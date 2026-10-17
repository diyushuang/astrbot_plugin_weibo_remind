"""微博时间线抓取。

网络层（游客身份三步流 / SSRF 校验 / 风控冷却 / 请求节流）沿用同作者的
astrbot_plugin_weibo_forward_to_qqgroup 实测实现并做精简：去掉相册、视频、
长文全文与多账号支持，仅保留单 UID 时间线抓取所需的最小闭包。
"""

from __future__ import annotations

import asyncio
import contextlib
import html as html_lib
import ipaddress
import json
import random
import re
import socket
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote, urlparse

import aiohttp

try:
    from .constants import (
        API_HEADERS,
        DNS_CACHE_TTL,
        RENEW_COOLDOWN,
        RENEW_FAIL_BACKOFF,
        RENEW_MAX_PER_HOUR,
        REQUEST_GAP,
        RISK_COOLDOWN_STEPS,
        RISK_ESCALATION_RESET,
        TIMELINE_MAX_PAGES,
        TIMELINE_PAGE_COUNT,
        TZ_CN_FIXED,
        UA_POOL,
        VISITOR_MAX_AGE,
        WEIBO_ALLOWED_HOSTS,
        WEIBO_GENVISITOR_URL,
        WEIBO_HOME_URL,
        WEIBO_INCARNATE_URL,
        WEIBO_INDEX_URL,
        WEIBO_VISITOR_REFERER,
    )
except ImportError:  # 独立脚本 / 测试环境
    from constants import (  # type: ignore
        API_HEADERS,
        DNS_CACHE_TTL,
        RENEW_COOLDOWN,
        RENEW_FAIL_BACKOFF,
        RENEW_MAX_PER_HOUR,
        REQUEST_GAP,
        RISK_COOLDOWN_STEPS,
        RISK_ESCALATION_RESET,
        TIMELINE_MAX_PAGES,
        TIMELINE_PAGE_COUNT,
        TZ_CN_FIXED,
        UA_POOL,
        VISITOR_MAX_AGE,
        WEIBO_ALLOWED_HOSTS,
        WEIBO_GENVISITOR_URL,
        WEIBO_HOME_URL,
        WEIBO_INCARNATE_URL,
        WEIBO_INDEX_URL,
        WEIBO_VISITOR_REFERER,
    )

_MONTH_MAP = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
_STD_TIME_RE = re.compile(r"^(\w{3}) (\w{3}) (\d{1,2}) (\d{2}):(\d{2}):(\d{2}) \+0800 (\d{4})$")
_JSONP_RE = re.compile(r"\((\{.*\})\)\s*;?\s*$")


class WeiboFetchError(Exception):
    """微博接口请求/解析失败。"""


class WeiboAuthError(WeiboFetchError):
    """游客身份失效，需要重新换取后再试。"""


class WeiboRiskError(WeiboFetchError):
    """触发微博 IP 级风控（HTTP 418/432、200 回 HTML），需要全局冷却。"""


class WeiboCookieExpiredError(WeiboFetchError):
    """自定义 Cookie 已失效。

    单独成类是为了让上层能**按类型**判定（而不是匹配错误文案）——
    「Cookie 过期要提醒用户更换」与「普通网络故障」的处理完全不同，
    靠字符串匹配一旦改文案就会静默失效。
    """


@dataclass
class WeiboPost:
    """解析后的一条微博（外层帖）。"""

    id: str
    uid: str
    author: str
    link: str
    created_at: str  # 可读时间，如 10-30 20:00
    created_ts: float  # epoch 秒；无法解析为 0
    text: str  # 已清洗的正文（含转发原文拼接由调用方决定）
    is_retweet: bool
    orig_text: str


def _clean_text(raw: str, limit: int) -> str:
    """清洗 m.weibo.cn 返回的 HTML 富文本，截断到 limit。"""
    if not raw:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", raw, flags=re.I)
    # 微博表情等内嵌图片，保留 alt 文本（如 [笑cry]）
    text = re.sub(r'<img[^>]*alt="([^"]*)"[^>]*>', r"\1", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    if limit > 0 and len(text) > limit:
        text = text[:limit].rstrip() + "…"
    return text


def _parse_created_epoch(raw: str) -> float:
    """把微博标准时间（Sat Mar 08 16:51:30 +0800 2025）解析为 Unix 时间戳。

    相对时间等无法解析的格式返回 0，由调用方采用宽松策略。
    """
    m = _STD_TIME_RE.match((raw or "").strip())
    if not m or m.group(2) not in _MONTH_MAP:
        return 0.0
    try:
        dt = datetime(
            int(m.group(7)),
            _MONTH_MAP[m.group(2)],
            int(m.group(3)),
            int(m.group(4)),
            int(m.group(5)),
            int(m.group(6)),
            tzinfo=TZ_CN_FIXED,
        )
        return dt.timestamp()
    except ValueError:
        return 0.0


def _format_time(raw: str) -> str:
    """把接口时间转成可读字符串；相对时间（如“5分钟前”）原样返回。"""
    raw = (raw or "").strip()
    if not raw:
        return ""
    m = _STD_TIME_RE.match(raw)
    if m and m.group(2) in _MONTH_MAP:
        mon, day = _MONTH_MAP[m.group(2)], int(m.group(3))
        return f"{m.group(7)}-{mon:02d}-{day:02d} {m.group(4)}:{m.group(5)}"
    return raw


def _jsonp_data(body: str) -> dict[str, Any] | None:
    """解析 gen_callback({...}) / cross_domain({...}) 之类的 JSONP 响应。"""
    m = _JSONP_RE.search((body or "").strip())
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


class WeiboFetcher:
    """微博时间线抓取器（单 UID，游客身份免登录）。

    config 仅读三键：custom_cookie / proxy / request_timeout。
    log 为调用方传入的日志器（插件内为 astrbot logger）。
    """

    def __init__(self, config: dict[str, Any], log: Any):
        self.config = config
        self.log = log
        self._http: aiohttp.ClientSession | None = None
        self._proxy = str(config.get("proxy") or "").strip() or None
        self._custom_cookies = self._parse_cookie_string(str(config.get("custom_cookie") or ""))
        self._timeout = max(5, int(config.get("request_timeout") or 15))
        self._ua = random.choice(UA_POOL)
        self._visitor_cookies: dict[str, str] = {}
        self._visitor_ts = 0.0
        self._renew_lock = asyncio.Lock()
        self._renew_times: list[float] = []
        self._renew_fail_count = 0
        self._renew_fail_until = 0.0
        self._last_renew_ts = 0.0
        self._blocked_until = 0.0
        self._risk_level = 0
        self._last_risk_ts = 0.0
        self._pace_lock = asyncio.Lock()
        self._last_request_mono = 0.0
        self._dns_cache: dict[str, tuple[list[tuple[Any, ...]], float]] = {}
        self._closed = False

    # ---------------- 生命周期 ----------------

    async def start(self) -> None:
        self._closed = False
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(
                cookie_jar=aiohttp.DummyCookieJar(),
                connector=aiohttp.TCPConnector(ttl_dns_cache=DNS_CACHE_TTL),
            )

    async def close(self) -> None:
        self._closed = True
        if self._http is not None and not self._http.closed:
            with contextlib.suppress(Exception):
                await self._http.close()
        self._http = None

    # ---------------- 对外接口 ----------------

    async def fetch_timeline(self, uid: str, known_ids: set[str] | None = None) -> list[WeiboPost]:
        """拉取用户最新微博，返回帖子列表（新→旧，跨页按 id 去重）。

        默认只拉第一页（单页 TIMELINE_PAGE_COUNT 条）；整页都是未见过的帖子时
        向后翻页补漏，最多 TIMELINE_MAX_PAGES 页。known_ids 为 None（建基线轮）
        时不翻页。

        **翻页失败不能丢掉本轮已取到的微博**：实测 m.weibo.cn 对 `page>=2` 一律回
        `ok=-100 接口要求登录`（只有自备登录态 custom_cookie 才放行）。若让异常冒泡，
        第一页那批新微博会连同本轮一起丢，而且它们没被标记已见，下一轮还会重演、
        形成永远失败的循环。所以除第一页外的失败一律按「没有更早的微博了」处理。
        """
        posts: list[WeiboPost] = []
        got: set[str] = set()
        for page in range(1, TIMELINE_MAX_PAGES + 1):
            try:
                batch = await self._fetch_timeline_page(uid, page)
            except WeiboFetchError as e:
                if page == 1:
                    raise
                self.log.info(f"翻到第 {page} 页失败（按没有更早微博处理，本轮结果保留）: {e}")
                break
            for post in batch:
                if post.id not in got:
                    got.add(post.id)
                    posts.append(post)
            # 本页为空，或页面里出现已见过的帖子（新旧边界已落在本页内）：
            # 更早的页全是旧帖，停止翻页
            if not batch or known_ids is None or any(p.id in known_ids for p in batch):
                break
        return posts

    def is_cooling_down(self) -> tuple[bool, int]:
        """（风控冷却中, 剩余秒）——状态指令展示用。"""
        now = time.time()
        if now < self._blocked_until:
            return True, int(self._blocked_until - now)
        return False, 0

    @property
    def visitor_alive(self) -> bool:
        """游客身份是否健康（有 SUB 且未超最长复用期）。"""
        if self._custom_cookies:
            return True
        return bool(self._visitor_cookies.get("SUB")) and (
            time.time() - self._visitor_ts < VISITOR_MAX_AGE
        )

    # ---------------- 网络原语 ----------------

    async def _resolve_host(self, host: str) -> list[tuple[Any, ...]]:
        """带 TTL 缓存的 getaddrinfo（仅 TCP 记录），供出网前的 SSRF 校验使用。"""
        now = time.monotonic()
        hit = self._dns_cache.get(host)
        if hit:
            infos, expires = hit
            if now < expires:
                return infos
            self._dns_cache.pop(host, None)
        infos = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
        self._dns_cache[host] = (infos, now + DNS_CACHE_TTL)
        return infos

    @staticmethod
    def _ensure_public_addrs(infos: list[tuple[Any, ...]], label: str) -> None:
        """解析结果里出现内网/保留地址就拦截。"""
        for info in infos:
            ip = ipaddress.ip_address(info[4][0])
            if (
                ip.is_loopback
                or ip.is_private
                or ip.is_reserved
                or ip.is_link_local
                or ip.is_multicast
                or ip.is_unspecified
            ):
                raise WeiboFetchError(f"{label}解析到受限地址，已拦截: {ip}")

    async def _validate_url(self, url: str) -> None:
        """出网前校验：仅 http/https、host 白名单、DNS 不解析到内网/保留地址。"""
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise WeiboFetchError(f"拒绝非 http(s) 协议: {parsed.scheme}")
        host = parsed.hostname or ""
        if host not in WEIBO_ALLOWED_HOSTS:
            raise WeiboFetchError(f"目标 host 不在白名单内: {host}")
        try:
            infos = await self._resolve_host(host)
        except OSError as e:
            raise WeiboFetchError(f"域名解析失败: {host} ({e})") from e
        self._ensure_public_addrs(infos, f"域名 {host} ")

    async def _pace(self) -> None:
        """全局请求间隔：任意两次微博请求之间至少间隔 REQUEST_GAP 内的随机秒数。"""
        async with self._pace_lock:
            gap = random.uniform(*REQUEST_GAP)
            wait = self._last_request_mono + gap - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_mono = time.monotonic()

    def _base_headers(self) -> dict[str, str]:
        return {
            "User-Agent": self._ua or UA_POOL[0],
            "Accept-Language": "zh-CN,zh;q=0.9",
        }

    async def _get_text(
        self,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        collect: dict[str, str] | None = None,
    ) -> str:
        await self._validate_url(url)
        await self._pace()
        assert self._http is not None
        timeout = aiohttp.ClientTimeout(total=self._timeout)
        last_err: Exception | None = None
        for attempt in range(3):
            try:
                async with self._http.get(
                    url, params=params, headers=headers, timeout=timeout, proxy=self._proxy
                ) as resp:
                    if resp.status in (418, 432):
                        raise WeiboRiskError(f"HTTP {resp.status}，触发微博 IP 级风控")
                    resp.raise_for_status()
                    if collect is not None:
                        self._merge_cookies(collect, resp)
                    return await resp.text()
            except (WeiboRiskError, WeiboAuthError):
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_err = e
                if attempt < 2:
                    await asyncio.sleep(1.5 * (2**attempt))
        raise WeiboFetchError(f"请求失败: {last_err}")

    # ---------------- API 层 ----------------

    async def _api_get(
        self, url: str, params: dict[str, Any], referer: str | None = None
    ) -> dict[str, Any]:
        """GET m.weibo.cn JSON 接口。

        ok=-100（游客身份过期）→ 换新身份重试一次；
        IP 级风控 → 记录全局冷却并换新身份重试一次；
        冷却期内直接快速失败，避免冷却期间继续向微博出网。
        """
        if time.time() < self._blocked_until:
            raise WeiboFetchError(
                f"微博风控冷却中（剩余 {int(self._blocked_until - time.time())} 秒）"
            )
        for attempt in (1, 2):
            try:
                data = await self._api_get_once(url, params, referer)
                # 成功即解除风控冷却
                self._risk_level = 0
                self._blocked_until = 0.0
                return data
            except WeiboRiskError as e:
                self._mark_risk_blocked(e)
                if attempt == 2:
                    raise
                await self._renew_quietly_force()
            except WeiboAuthError as e:
                if attempt == 2:
                    raise
                self.log.info(f"游客身份失效（{e}），正在自动换新…")
                await self._renew_visitor(force=True)
        raise WeiboFetchError("unreachable")  # pragma: no cover

    async def _api_get_once(
        self, url: str, params: dict[str, Any], referer: str | None
    ) -> dict[str, Any]:
        if self._http is None:
            raise WeiboFetchError("HTTP 会话未初始化")
        headers = self._base_headers()
        headers.update(API_HEADERS)
        headers["Referer"] = referer or WEIBO_HOME_URL
        if self._custom_cookies:
            cookies = self._custom_cookies
        else:
            if not self._visitor_cookies.get("SUB"):
                await self._renew_visitor()
            cookies = self._visitor_cookies
            xsrf = cookies.get("XSRF-TOKEN", "")
            if xsrf:
                headers["X-Xsrf-Token"] = xsrf
        headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in cookies.items())
        body = await self._get_text(url, params, headers)
        if body.lstrip().startswith("<"):
            # 风控拦截页与错误页照样回 200：JSON 接口拿到 HTML 说明这个 IP
            # 已经被墙了，按风控处理进入全局冷却
            raise WeiboRiskError("JSON 接口返回了 HTML 页面（疑似被风控拦截）")
        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            raise WeiboFetchError("接口返回的不是 JSON") from e
        if not isinstance(data, dict):
            raise WeiboFetchError("接口返回异常")
        if data.get("ok") == -100:
            if self._custom_cookies:
                # 自定义 Cookie 失效时换新游客身份没有意义，直接报错
                raise WeiboCookieExpiredError("自定义 Cookie 已失效（接口要求登录）")
            raise WeiboAuthError("接口要求登录（ok=-100）")
        if data.get("ok") != 1:
            raise WeiboFetchError(f"接口返回异常 ok={data.get('ok')}")
        return data

    def _mark_risk_blocked(self, e: Exception) -> None:
        """IP 级风控：全局冷却并按连续触发次数升级（10 → 30 → 60 分钟）。

        冷却期内重复触发不叠加升级；距上次触发超过 RISK_ESCALATION_RESET
        后梯度重置。
        """
        now = time.time()
        if now < self._blocked_until:
            return
        if now - self._last_risk_ts > RISK_ESCALATION_RESET:
            self._risk_level = 0
        self._last_risk_ts = now
        level = self._risk_level
        cooldown = RISK_COOLDOWN_STEPS[min(level, len(RISK_COOLDOWN_STEPS) - 1)]
        self._risk_level = min(level + 1, len(RISK_COOLDOWN_STEPS) - 1)
        self._blocked_until = now + cooldown
        self.log.warning(
            f"触发微博风控，全局冷却 {cooldown // 60} 分钟（连续第 {level + 1} 次触发）: {e}"
        )

    # ---------------- 游客身份 ----------------

    async def _renew_quietly_force(self) -> None:
        try:
            await self._renew_visitor(force=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.warning(f"换新游客身份失败: {e}")

    async def warmup(self) -> None:
        """启动预热：访问一次移动端主页刷新 _T_WM / XSRF-TOKEN 指纹。"""
        try:
            cookies: dict[str, str] = {}
            await self._get_text(WEIBO_HOME_URL, {}, self._base_headers(), collect=cookies)
            if cookies:
                self._visitor_cookies.update(cookies)
                self.log.info(f"启动预热完成（{sorted(cookies)}）")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.info(f"启动预热失败（不影响运行）: {e}")

    async def _renew_visitor(self, force: bool = False) -> None:
        """向微博游客系统换取新的匿名游客身份（SUB/SUBP，种在 .weibo.cn 域）。

        节流：每小时最多 RENEW_MAX_PER_HOUR 次；失败指数退避封顶 1 小时；
        非强制模式受 RENEW_COOLDOWN 冷却限制；加锁串行避免并发双重换新。
        """
        async with self._renew_lock:
            if self._http is None:
                raise WeiboFetchError("HTTP 会话未初始化")
            now = time.time()
            recent = [t for t in self._renew_times if now - t < 3600.0]
            self._renew_times = recent
            if len(recent) >= RENEW_MAX_PER_HOUR:
                raise WeiboFetchError(
                    f"游客身份续期已达每小时上限（{RENEW_MAX_PER_HOUR} 次），稍后自动重试"
                )
            if now < self._renew_fail_until:
                raise WeiboFetchError("上次续期失败，退避中，稍后自动重试")
            if (
                not force
                and self._visitor_cookies.get("SUB")
                and now - self._last_renew_ts < RENEW_COOLDOWN
            ):
                return  # 冷却期内且现有身份尚在，直接复用
            self._renew_times.append(now)
            self._last_renew_ts = now
            try:
                await self._do_renew()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._renew_fail_count += 1
                backoff = min(3600.0, RENEW_FAIL_BACKOFF * (2 ** (self._renew_fail_count - 1)))
                self._renew_fail_until = time.time() + backoff
                raise
            self._renew_fail_count = 0
            self._renew_fail_until = 0.0

    async def _do_renew(self) -> None:
        assert self._http is not None
        self.log.info("正在获取微博游客身份…")
        self._ua = random.choice(UA_POOL)
        cookies: dict[str, str] = {}
        common = self._base_headers()
        visitor_headers = dict(common)
        visitor_headers["Referer"] = WEIBO_VISITOR_REFERER

        # 1) 预热移动端主页，收集 _T_WM / XSRF-TOKEN 等游客 cookie
        await self._get_text(WEIBO_HOME_URL, {}, common, collect=cookies)

        # 2) genvisitor 获取 tid
        fp = json.dumps(
            {
                "os": "1",
                "browser": "Chrome136,136,0,0",
                "fonts": "undefined",
                "screenInfo": "1920*1080*30",
                "plugins": "",
            },
            separators=(",", ":"),
        )
        body = await self._get_text(
            WEIBO_GENVISITOR_URL,
            {"cb": "gen_callback", "fp": quote(fp, safe="")},
            visitor_headers,
        )
        gen = _jsonp_data(body) or {}
        if gen.get("retcode") != 20000000:
            raise WeiboFetchError(f"genvisitor 失败: {gen.get('retcode')} {gen.get('msg')}")
        tid = str((gen.get("data") or {}).get("tid") or "")
        confidence = (gen.get("data") or {}).get("confidence", 90)
        if not tid:
            raise WeiboFetchError("genvisitor 未返回 tid")

        # 3) incarnate 换取游客 SUB/SUBP
        inc_params = {
            "a": "incarnate",
            "t": tid,
            "w": "2",
            "c": str(confidence),
            "gc": "",
            "cb": "cross_domain",
            "from": "weibo",
            "_rand": f"{random.random():.17f}",
        }
        await self._get_text(WEIBO_INCARNATE_URL, inc_params, visitor_headers, collect=cookies)

        if "SUB" not in cookies:
            raise WeiboAuthError("游客身份获取失败（未拿到 SUB）")
        cookies.setdefault("MLOGIN", "0")
        self._visitor_cookies = cookies
        self._visitor_ts = time.time()
        self.log.info("微博游客身份已更新")

    # ---------------- 解析 ----------------

    async def _fetch_timeline_page(self, uid: str, page: int) -> list[WeiboPost]:
        """拉取用户时间线的指定页，返回解析后的帖子列表（新→旧）。"""
        data = await self._api_get(
            WEIBO_INDEX_URL,
            {
                "type": "uid",
                "value": uid,
                "containerid": f"107603{uid}",
                "page": page,
                "count": TIMELINE_PAGE_COUNT,
            },
            referer=f"https://m.weibo.cn/u/{uid}",
        )
        payload = data.get("data")
        if not isinstance(payload, dict):
            return []
        cards = payload.get("cards") or []
        posts: list[WeiboPost] = []
        for card in cards:
            # 接口对已删除/隐藏的微博会在 cards 里留 null 槽位
            if not isinstance(card, dict):
                continue
            if card.get("card_type") != 9:
                continue
            mb = card.get("mblog")
            if not isinstance(mb, dict):
                continue
            if self._is_pinned(card, mb):
                continue
            post = self._parse_post(mb, uid)
            if post:
                posts.append(post)
        return posts

    @staticmethod
    def _is_pinned(card: dict[str, Any], mb: dict[str, Any]) -> bool:
        for node in (card, mb):
            with contextlib.suppress(TypeError, ValueError):
                if int(node.get("mblogtype") or 0) == 2:
                    return True
            if node.get("isTop") or node.get("is_top"):
                return True
        title = mb.get("title")
        return isinstance(title, dict) and title.get("text") == "置顶"

    def _parse_post(self, mb: dict[str, Any], fallback_uid: str) -> WeiboPost | None:
        pid = str(mb.get("id") or mb.get("mid") or "").strip()
        if not pid:
            return None
        user = mb.get("user") or {}
        uid = str(user.get("id") or fallback_uid)
        bid = str(mb.get("bid") or pid)
        created_raw = str(mb.get("created_at") or "")
        limit = max(0, int(self.config.get("text_max_length") or 500))
        is_retweet = "retweeted_status" in mb
        rt_mb = mb.get("retweeted_status") if is_retweet else None
        orig_text = ""
        if isinstance(rt_mb, dict):
            orig_text = _clean_text(str(rt_mb.get("text") or ""), limit)
        return WeiboPost(
            id=pid,
            uid=uid,
            author=str(user.get("screen_name") or "") or uid,
            link=f"https://weibo.com/{uid}/{bid}",
            created_at=_format_time(created_raw),
            created_ts=_parse_created_epoch(created_raw),
            text=_clean_text(str(mb.get("text") or ""), limit),
            is_retweet=is_retweet,
            orig_text=orig_text,
        )

    @staticmethod
    def _parse_cookie_string(raw: str) -> dict[str, str]:
        """把 "k1=v1; k2=v2" 形式的 Cookie 字符串解析为字典。"""
        cookies: dict[str, str] = {}
        for part in (raw or "").split(";"):
            part = part.strip()
            if not part or "=" not in part:
                continue
            k, _, v = part.partition("=")
            k, v = k.strip(), v.strip()
            if k:
                cookies[k] = v
        return cookies

    @staticmethod
    def _merge_cookies(target: dict[str, str], resp: aiohttp.ClientResponse) -> None:
        """把响应 Set-Cookie 合入字典（仅保留 weibo 相关域）。"""
        for key, morsel in resp.cookies.items():
            domain = morsel.get("domain") or ""
            if domain and "weibo.cn" not in domain and "weibo.com" not in domain:
                continue
            target[str(key)] = str(morsel.value)
