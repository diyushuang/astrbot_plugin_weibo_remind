"""微博定时提醒 · AstrBot 插件主入口。

职责只有接线：装配 store / parser / fetcher / pusher / scheduler，跑两个常驻任务
（轮询抓取 + 到点推送），并暴露管理指令。业务逻辑都在各自模块里。

数据流：
    _poll_loop（poll_interval 秒）
      └─ fetcher.fetch_timeline(uid, known_ids)
           ├─ 首次运行（库里没有任何已见记录）→ 只建基线，不解析不推送
           └─ 新微博（旧→新逐条）→ _process_post
                ├─ 时效窗口过滤（check_window_minutes）
                ├─ parser.parse(正文, 基准=微博发布时间, now)   ← 基准取发布时间，
                │   防补发微博的「今晚8点」被当成已过去
                ├─ 无候选 且 疑似含时间安排 → 按 push_on_parse_fail 推原文
                └─ 每个候选 × 每个提前档位 → store.add_task（UNIQUE 去重）
    RemindScheduler.run（每 20 秒）
      └─ pop_due → pusher.send_remind → mark sent/failed
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools

try:
    from astrbot.core.utils.astrbot_path import get_astrbot_data_path
except ImportError:  # 旧版 AstrBot / 独立脚本
    get_astrbot_data_path = None

try:  # 插件在 AstrBot 内以 data.plugins.<name> 包形式加载
    from .constants import (
        BACKOFF_MAX,
        PLUGIN_NAME,
        PLUGIN_VERSION,
        POLL_JITTER,
        POLL_MIN_INTERVAL,
        SEEN_IDS_LIMIT,
        TZ_CN,
        USAGE,
    )
    from .fetcher import WeiboFetcher, WeiboFetchError, WeiboPost
    from .post_coverage import (
        KIND_LABELS,
        OUTCOME_AMBIGUOUS,
        OUTCOME_LABELS,
        OUTCOME_PAST,
        OUTCOME_PUSHED,
        OUTCOME_SILENT,
        OUTCOME_TASKED,
        CoverageTracker,
        PostOutcome,
        classify_post,
        has_pending_time,
    )
    from .pusher import Pusher
    from .scheduler import RemindScheduler
    from .store import SQLiteStore
    from .time_parser import WeiboTimeParser
except ImportError:  # 独立脚本 / 测试环境
    from constants import (  # type: ignore
        BACKOFF_MAX,
        PLUGIN_NAME,
        PLUGIN_VERSION,
        POLL_JITTER,
        POLL_MIN_INTERVAL,
        SEEN_IDS_LIMIT,
        TZ_CN,
        USAGE,
    )
    from fetcher import (  # type: ignore
        WeiboFetcher,
        WeiboFetchError,
        WeiboPost,
    )
    from post_coverage import (  # type: ignore
        KIND_LABELS,
        OUTCOME_AMBIGUOUS,
        OUTCOME_LABELS,
        OUTCOME_PAST,
        OUTCOME_PUSHED,
        OUTCOME_SILENT,
        OUTCOME_TASKED,
        CoverageTracker,
        PostOutcome,
        classify_post,
        has_pending_time,
    )
    from pusher import Pusher  # type: ignore
    from scheduler import RemindScheduler  # type: ignore
    from store import SQLiteStore  # type: ignore
    from time_parser import WeiboTimeParser  # type: ignore

DB_FILENAME = "weibo_remind.db"
DEFAULT_ADVANCE_MINUTES = (60, 10, 0)
# 任务触发时间早于「现在 + 这个秒数」就视为该档已过，静默跳过
TRIGGER_MIN_LEAD = 30


def _resolve_data_dir() -> Path:
    """插件数据目录（规范要求落盘在 AstrBot data 目录，不能写插件自身目录）。"""
    if get_astrbot_data_path is not None:
        try:
            return Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME
        except Exception:  # pragma: no cover - 取决于部署形态
            pass
    try:
        return Path(StarTools.get_data_dir(PLUGIN_NAME))
    except Exception:  # pragma: no cover
        return Path.cwd() / "data" / "plugin_data" / PLUGIN_NAME


class WeiboRemindPlugin(Star):
    """定时抓取微博并按正文时间推送提醒。"""

    def __init__(self, context: Context, config: dict[str, Any] | None = None):
        # 规范要求 config 可省略：star_manager 在无配置时只传 context 构造，
        # 写成必填会让「无配置」这条加载路径直接 TypeError
        super().__init__(context)
        self.config = config or {}
        # Star 自 v4.26.8 起才在 __init__ 里注入 self.logger；v4.24 ~ v4.26.0 没有该属性，
        # 直接取会让插件在加载阶段抛 AttributeError（整个插件装不上，且不会自愈）。
        # 这里退回 astrbot 的标准日志器，使声明的下限版本真正可用。
        if getattr(self, "logger", None) is None:
            self.logger = logger
        self.store: SQLiteStore | None = None
        self.parser = WeiboTimeParser(self.config, self.logger)
        self.fetcher: WeiboFetcher | None = None
        self.pusher: Pusher | None = None
        self.scheduler: RemindScheduler | None = None
        self.coverage: CoverageTracker | None = None
        self._data_dir: Path | None = None
        self._poll_task: asyncio.Task | None = None
        self._sched_task: asyncio.Task | None = None
        self._paused = False
        self._kick = asyncio.Event()
        self._fail_streak = 0
        self._stats: dict[str, Any] = {
            "fetch_rounds": 0,
            "new_posts": 0,
            "tasks_created": 0,
            "parse_fail_pushed": 0,
            "last_fetch_ts": 0.0,
            "started_ts": time.time(),
        }

    # ---------------- 生命周期 ----------------

    async def initialize(self) -> None:
        self._data_dir = _resolve_data_dir()
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self.store = SQLiteStore(self._data_dir / DB_FILENAME)
        # v0.4.0 之前的已见记录没有 uid 归属：旧版只支持单账号，
        # 这些记录必然来自旧配置的那个 uid，启动时一次性归属过去
        legacy_uid = self._uid()
        if legacy_uid:
            migrated = await self.store.migrate_legacy_uid(legacy_uid)
            if migrated:
                self.logger.info(f"已把 {migrated} 条旧版已见记录归属到原监控账号 uid={legacy_uid}")
        self.fetcher = WeiboFetcher(self.config, self.logger)
        await self.fetcher.start()
        self.pusher = Pusher(self.context, self.config, self.logger, counter=self.store)
        self.scheduler = RemindScheduler(
            self.store, self.pusher, self.config, self.logger, is_paused=lambda: self._paused
        )
        self.coverage = CoverageTracker(self._data_dir, self.logger)

        report = await self.scheduler.recover_on_startup()
        await self.store.prune_seen(keep=5000)
        await self.store.purge_finished(keep_days=30)
        await self.fetcher.warmup()

        self._poll_task = asyncio.create_task(self._poll_loop())
        self._sched_task = asyncio.create_task(self.scheduler.run())

        uids = self._uids()
        sessions = self.pusher.sessions()
        self.logger.info(
            f"{PLUGIN_NAME} {PLUGIN_VERSION} 已启动：监控账号 {len(uids)} 个"
            f"（{'、'.join(uids) if uids else '未配置'}），"
            f"轮询 {self._poll_interval()} 秒，推送目标 {len(sessions)} 个，"
            f"恢复待触发 {report['pending']} 条（中断复位 {report['recovered']}，"
            f"超期作废 {report['missed']}）"
        )
        if not sessions:
            self.logger.warning(
                "尚未配置推送目标：请在目标群发送「微博提醒 绑定」，或填写 push_group_ids"
            )
        if not self.parser.jionlp_ready:
            self.logger.warning("jionlp 未安装，中文自然语言时间解析不可用（pip install jionlp）")

    async def terminate(self) -> None:
        for task in (self._poll_task, self._sched_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        if self.scheduler is not None:
            self.scheduler.stop()
        if self.fetcher is not None:
            await self.fetcher.close()
        if self.store is not None:
            self.store.close()
        self.logger.info(f"{PLUGIN_NAME} 已停止")

    # ---------------- 配置读取 ----------------

    def _int_cfg(self, key: str, default: int, minimum: int = 0) -> int:
        value = self.config.get(key)
        if value is None or value == "":
            return default
        try:
            return max(minimum, int(value))
        except (TypeError, ValueError):
            return default

    def _uid(self) -> str:
        """旧版单账号配置（v0.4.0 前的字段），仅作兼容读取。"""
        raw = str(self.config.get("uid") or "").strip()
        return raw if raw.isdigit() else ""

    def _uids(self) -> list[str]:
        """监控账号列表：新配置 uids（列表）∪ 旧配置 uid（字符串），去重保序。

        只保留纯数字项；两个来源都为空时返回空列表（本轮跳过抓取）。
        """
        out: list[str] = []
        raw_list = self.config.get("uids")
        if isinstance(raw_list, (list, tuple)):
            for item in raw_list:
                value = str(item or "").strip()
                if value.isdigit() and value not in out:
                    out.append(value)
        legacy = self._uid()
        if legacy and legacy not in out:
            out.append(legacy)
        return out

    def _poll_interval(self) -> int:
        return self._int_cfg("poll_interval", 120, minimum=POLL_MIN_INTERVAL)

    def _advance_minutes(self) -> list[int]:
        """提前档位（分钟，降序）；非法输入回退默认 [60, 10, 0]。"""
        raw = self.config.get("remind_advance_minutes")
        if raw is None or not isinstance(raw, (list, tuple)):
            return list(DEFAULT_ADVANCE_MINUTES)
        out: list[int] = []
        for item in raw:
            try:
                value = max(0, int(item))
            except (TypeError, ValueError):
                continue
            if value not in out:
                out.append(value)
        return sorted(out, reverse=True) or [0]

    def _save_config(self) -> None:
        """落盘配置；config 可能是纯 dict（测试注入），无 save_config 可调。"""
        saver = getattr(self.config, "save_config", None)
        if callable(saver):
            saver()

    # ---------------- 轮询与解析 ----------------

    async def _poll_loop(self) -> None:
        while True:
            if not self._paused:
                try:
                    await self._check_once()
                    self._fail_streak = 0
                except asyncio.CancelledError:
                    raise
                except WeiboFetchError as e:
                    self._fail_streak += 1
                    backoff = min(BACKOFF_MAX, 60.0 * (2 ** (self._fail_streak - 1)))
                    # 只记首错，避免长时间故障刷屏
                    if self._fail_streak == 1:
                        self.logger.warning(f"抓取微博失败（第 1 次，退避 {backoff:.0f} 秒）: {e}")
                    await self._sleep_or_kick(backoff)
                    continue
                except Exception as e:
                    self._fail_streak += 1
                    self.logger.warning(f"检查轮次异常（第 {self._fail_streak} 次）: {e!r}")
                    await self._sleep_or_kick(min(BACKOFF_MAX, 60.0 * self._fail_streak))
                    continue
            await self._sleep_or_kick(self._poll_interval() + random.uniform(*POLL_JITTER))

    async def _sleep_or_kick(self, seconds: float) -> None:
        """等待间隔，或被「重抓」指令/停机提前唤醒。"""
        with_timeout = asyncio.wait_for(self._kick.wait(), timeout=max(1.0, seconds))
        try:
            await with_timeout
        except asyncio.TimeoutError:
            pass
        finally:
            self._kick.clear()

    async def _check_once(self) -> int:
        """抓取一轮（逐账号串行）并处理新微博，返回新建任务数。

        单个账号失败只跳过该账号、不中断其它账号；全部账号都失败才按
        整轮失败向外抛（让轮询进入退避）。账号间请求间隔由 fetcher 的
        全局节流（REQUEST_GAP）保证，无需额外 sleep。
        """
        assert self.fetcher is not None and self.store is not None
        uids = self._uids()
        if not uids:
            self.logger.warning("未配置有效的微博 uid（应为纯数字），跳过本轮抓取")
            return 0

        total_created = 0
        first_error: WeiboFetchError | None = None
        failed = 0
        for uid in uids:
            try:
                total_created += await self._check_uid_once(uid)
            except asyncio.CancelledError:
                raise
            except WeiboFetchError as e:
                failed += 1
                if first_error is None:
                    first_error = e
                # 单账号失败不影响其它账号；若触发的是风控冷却，
                # 后续账号会在冷却检查处快速失败，无害
                self.logger.warning(f"抓取 uid={uid} 失败（本轮跳过该账号）: {e}")
        if failed == len(uids) and first_error is not None:
            raise first_error
        self._stats["fetch_rounds"] += 1
        self._stats["last_fetch_ts"] = time.time()
        return total_created

    async def _check_uid_once(self, uid: str) -> int:
        """抓取单个账号并处理新微博，返回新建任务数。"""
        assert self.fetcher is not None and self.store is not None
        # 该账号在库里没有任何已见记录 = 该账号的首次运行
        baseline = await self.store.seen_count(uid) == 0
        known = set() if baseline else await self.store.known_ids(SEEN_IDS_LIMIT, uid=uid)
        posts = await self.fetcher.fetch_timeline(uid, known_ids=None if baseline else known)
        if not posts:
            self.logger.info(f"uid={uid} 本轮未取到微博（可能被风控或账号无公开微博）")
            return 0

        if baseline:
            return await self._handle_baseline(posts, uid)

        fresh = [p for p in posts if p.id not in known]
        if not fresh:
            return 0
        self._stats["new_posts"] += len(fresh)
        self.logger.info(f"uid={uid} 发现 {len(fresh)} 条新微博，开始解析时间")
        created = 0
        # 按发布先后处理（抓取结果是新→旧）
        for post in sorted(fresh, key=lambda p: p.created_ts):
            await self.store.mark_seen([post.id], {post.id: post.created_ts}, uid=uid)
            created += await self._process_post(post)
        self._stats["tasks_created"] += created
        await self._flush_coverage()
        return created

    async def _flush_coverage(self) -> None:
        """把覆盖自检统计落盘（跨重启累计，供「微博提醒 覆盖」查看）。"""
        if self.coverage is not None:
            await self.coverage.flush()

    async def _handle_baseline(self, posts: list[WeiboPost], uid: str) -> int:
        """单个账号首次运行：先把当前可见微博全部记为已见，再回补时效窗口内的那些。

        全部记为已见是为了避免下次运行把历史微博当成新提醒而刷屏；
        但窗口内（默认 12 小时）的微博仍要解析——刚装好插件时，最新一条
        「明天下午3点直播」正是用户最想要的提醒，不该被基线整轮吞掉。
        """
        assert self.store is not None
        now = time.time()
        await self.store.mark_seen(
            [p.id for p in posts], {p.id: p.created_ts for p in posts}, uid=uid
        )
        window = self._int_cfg("check_window_minutes", 720) * 60
        recent = [
            p for p in posts if not window or not p.created_ts or now - p.created_ts <= window
        ]
        self.logger.info(
            f"uid={uid} 首次运行：已记录 {len(posts)} 条可见微博为已见；"
            f"其中 {len(recent)} 条在时效窗口内，将解析时间"
        )
        created = 0
        for post in sorted(recent, key=lambda p: p.created_ts):
            created += await self._process_post(post)
        self._stats["new_posts"] += len(recent)
        self._stats["tasks_created"] += created
        return created

    async def _process_post(self, post: WeiboPost) -> int:
        """解析单条微博并创建任务，返回新建任务数。

        处理策略由**博文形态**决定（见 coverage.classify_post）：
        - 排期公告类：建任务；完全解析不出时间 → 推原文请群友确认
        - 声明打假/建议反馈/日常类：静默（实测这类博文若按「疑似含时间」推送，
          就是纯噪音——98 条语料里有 17 条是这种）
        - 提到时间但都已过去：静默（用户选定的口径）
        """
        assert self.store is not None and self.pusher is not None
        now = time.time()
        body = post.text if not post.is_retweet else f"{post.text}\n{post.orig_text}".strip()
        kind = classify_post(body)
        limit = self._int_cfg("text_max_length", 500)
        weibo_text = body[:limit] if limit > 0 else body

        window_minutes = self._int_cfg("check_window_minutes", 720)
        if post.created_ts and window_minutes > 0 and now - post.created_ts > window_minutes * 60:
            self.logger.info(f"跳过 {post.id}：发布已超过 {window_minutes} 分钟（时效窗口）")
            self._record_outcome(post, kind, OUTCOME_SILENT, 0, "超出时效窗口", weibo_text)
            return 0

        # 解析基准取微博发布时间：微博可能补发，用当前时间做基准会把
        # 「今晚8点」这类表达误判为已过去
        base_dt = (
            datetime.fromtimestamp(post.created_ts, tz=TZ_CN)
            if post.created_ts
            else datetime.now(tz=TZ_CN)
        )
        result = self.parser.parse(body, base_dt, now)

        if not result.candidates:
            return await self._handle_no_candidate(post, kind, result, weibo_text, now)

        advances = self._advance_minutes()
        cap = self._int_cfg("max_tasks_per_post", 3, minimum=1)
        created = 0
        for cand in result.candidates[:cap]:
            snapshot = {
                "author": post.author,
                "weibo": weibo_text,
                "link": post.link,
                "source_text": cand.source_text,
                "event_ts": cand.trigger_ts,
                "weibo_created_at": post.created_at,
                "post_kind": kind.kind,
            }
            for advance in advances:
                trigger_at = cand.trigger_ts - advance * 60
                if trigger_at <= now + TRIGGER_MIN_LEAD:
                    # 该档已过（例如抓到微博时已经离事件不足 60 分钟）
                    continue
                if await self.store.add_task(post.id, trigger_at, advance, snapshot):
                    created += 1
        if created:
            preview = "、".join(f"{c.source_text}" for c in result.candidates[:cap])
            self.logger.info(f"{post.id} 已创建 {created} 条提醒任务（时间点：{preview}）")
            self._record_outcome(
                post, kind, OUTCOME_TASKED, created, f"时间点：{preview}", weibo_text
            )
        else:
            self.logger.info(f"{post.id} 的时间点均已过或已有任务，未新建")
            self._record_outcome(post, kind, OUTCOME_PAST, 0, "命中的档位均已过", weibo_text)
        return created

    async def _handle_no_candidate(
        self, post: WeiboPost, kind: Any, result: Any, weibo_text: str, now: float
    ) -> int:
        """没有可用候选时按博文形态决定推原文还是静默。

        下面的判定顺序即优先级，且每条博文**只记一次**处理结果——否则同一条会
        既进「待补样本」又进「已推原文」，覆盖率统计跟着失真。
        纯数字歧义 → 排期类+时间待定 → 提到时间但已过去 → 排期类没识别出时间 → 静默
        """
        assert self.pusher is not None
        push_enabled = bool(self.config.get("push_on_parse_fail", True))
        pending = has_pending_time(weibo_text)
        # 覆盖缺口 = 排期类但没建出任务，且不是「识别到的时间都已过去/太远」那种
        # 正确跳过（停机后补看历史博文时很常见，算成没覆盖会让覆盖率莫名变 0）
        gap = bool(
            kind.schedule
            and result.ambiguous == 0
            and (result.raw_hits == 0 or result.past_rejected == 0)
        )
        if result.ambiguous:
            reason = f"{result.ambiguous} 处纯数字时间上下文不足"
            self.logger.info(f"{post.id} {reason}，未建任务")
            self._record_outcome(post, kind, OUTCOME_AMBIGUOUS, 0, reason, weibo_text)
            return 0
        if result.raw_hits and kind.schedule and pending and push_enabled:
            # 排期类 + 明说「时间待定」：时间还没定，推原文让群友知道有这回事
            await self.pusher.send_parse_fail(post.author, weibo_text, post.link)
            self._stats["parse_fail_pushed"] += 1
            self._record_outcome(post, kind, OUTCOME_PUSHED, 0, "时间待定，推原文", weibo_text)
            return 0
        if result.raw_hits:
            reason = "提到的时间都已过去或超出窗口"
            self.logger.info(f"{post.id} {reason}，静默跳过")
            self._record_outcome(post, kind, OUTCOME_PAST, 0, reason, weibo_text)
            return 0
        if kind.schedule and push_enabled:
            await self.pusher.send_parse_fail(post.author, weibo_text, post.link)
            self._stats["parse_fail_pushed"] += 1
            self.logger.info(f"{post.id} 排期类博文但未识别出时间，已推送原文供群友确认")
            self._record_outcome(
                post, kind, OUTCOME_PUSHED, 0, "排期类但未识别出时间", weibo_text, gap=gap
            )
            return 0
        reason = (
            f"{kind.label}且无时间安排" if not kind.schedule else "排期类解析失败（未开启推送）"
        )
        self.logger.info(f"{post.id} {reason}，静默跳过")
        self._record_outcome(post, kind, OUTCOME_SILENT, 0, reason, weibo_text, gap=gap)
        return 0

    def _record_outcome(
        self,
        post: WeiboPost,
        kind: Any,
        outcome: str,
        tasks: int,
        reason: str,
        weibo_text: str,
        gap: bool = False,
    ) -> None:
        """记一条覆盖自检结果（供「微博提醒 覆盖」查看真实覆盖率）。

        gap=True 表示这属于真正的**覆盖缺口**（排期类但没建出任务，且不是
        「识别到的时间都已过去」这种正确跳过），会被记进待补样本。
        """
        if self.coverage is None:
            return
        self.coverage.record(
            PostOutcome(
                post_id=post.id,
                created_at=post.created_at,
                kind=kind.kind,
                outcome=outcome,
                tasks_created=tasks,
                reason=reason,
                excerpt=weibo_text.replace("\n", " ")[:80],
                gap=gap,
            )
        )

    # ---------------- 指令 ----------------

    @filter.command_group("微博提醒")
    async def weibo_remind(self, event: AstrMessageEvent):
        """微博定时提醒指令组。"""
        yield event.plain_result(USAGE)

    @weibo_remind.command("帮助", alias={"help", "用法"})
    async def cmd_help(self, event: AstrMessageEvent):
        """查看指令用法。"""
        yield event.plain_result(USAGE)

    @weibo_remind.command("绑定")
    async def cmd_bind(self, event: AstrMessageEvent):
        """把当前群绑定为提醒推送目标。"""
        if not event.get_group_id():
            yield event.plain_result("请在要接收提醒的群聊中使用该指令")
            return
        umo = event.unified_msg_origin
        sessions = [str(s) for s in (self.config.get("push_sessions") or [])]
        if umo in sessions:
            yield event.plain_result("本群已在推送列表中，无需重复绑定")
            return
        sessions.append(umo)
        self.config["push_sessions"] = sessions
        self._save_config()
        self.logger.info(f"新增推送目标：{umo}")
        yield event.plain_result("绑定成功，本群将接收微博定时提醒")

    @weibo_remind.command("解绑")
    async def cmd_unbind(self, event: AstrMessageEvent):
        """解除当前群的提醒推送。"""
        umo = event.unified_msg_origin
        sessions = [str(s) for s in (self.config.get("push_sessions") or [])]
        if umo not in sessions:
            yield event.plain_result("本群不在推送列表中")
            return
        sessions.remove(umo)
        self.config["push_sessions"] = sessions
        self._save_config()
        self.logger.info(f"移除推送目标：{umo}")
        yield event.plain_result("已解除本群的提醒推送")

    @weibo_remind.command("状态")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_status(self, event: AstrMessageEvent):
        """查看运行状态（管理员）。"""
        assert self.store is not None and self.pusher is not None
        stats = await self.store.stats()
        cooling, remaining = (False, 0)
        if self.fetcher is not None:
            cooling, remaining = self.fetcher.is_cooling_down()
        last_fetch = (
            datetime.fromtimestamp(self._stats["last_fetch_ts"], tz=TZ_CN).strftime(
                "%m-%d %H:%M:%S"
            )
            if self._stats["last_fetch_ts"]
            else "尚未抓取"
        )
        next_trigger = (
            datetime.fromtimestamp(stats["next_trigger_at"], tz=TZ_CN).strftime("%Y-%m-%d %H:%M")
            if stats["next_trigger_at"]
            else "无"
        )
        uids = self._uids()
        lines = [
            f"微博定时提醒 {PLUGIN_VERSION}",
            f"监控账号：{'、'.join(uids) if uids else '未配置'}（{len(uids)} 个）",
            f"运行状态：{'已暂停' if self._paused else '运行中'}"
            f"｜轮询间隔 {self._poll_interval()} 秒",
            f"上次抓取：{last_fetch}｜累计轮次 {self._stats['fetch_rounds']}",
            f"新微博累计：{self._stats['new_posts']}｜建任务 {self._stats['tasks_created']}"
            f"｜解析失败推送 {self._stats['parse_fail_pushed']}",
            f"待触发任务：{stats['by_status'].get('pending', 0) + stats['by_status'].get('sending', 0)}"
            f"｜下次触发 {next_trigger}",
            f"已见微博记录：{stats['seen_count']}",
            f"提前档位：{self._advance_minutes()}",
            f"推送目标：{len(self.pusher.sessions())} 个",
            f"免打扰时段：{self.pusher.quiet().spec if self.pusher.quiet() else '未启用'}"
            f"｜推送抖动 {self.scheduler.jitter_seconds() if self.scheduler else 0} 秒"
            f"｜批内间隔 {self.scheduler.interval_seconds() if self.scheduler else 0} 秒",
            f"解析引擎：jionlp {'可用' if self.parser.jionlp_ready else '不可用（降级）'}",
            f"游客身份：{'有效' if self.fetcher and self.fetcher.visitor_alive else '待获取'}",
            f"风控冷却：{f'冷却中（剩余 {remaining} 秒）' if cooling else '正常'}",
        ]
        if self.pusher is not None and self.config.get("at_all"):
            used = await self._at_all_used_today()
            limit = self.pusher.at_all_daily_limit()
            lines.append(
                f"@全体成员：开（档位 {self.pusher.at_all_tiers() or '不限'}｜"
                f"今日 {used}/{limit if limit else '∞'} 次）"
            )
        else:
            lines.append("@全体成员：关")
        yield event.plain_result("\n".join(lines))

    @weibo_remind.command("列表")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_list(self, event: AstrMessageEvent):
        """查看待触发的提醒任务（管理员）。"""
        assert self.store is not None
        tasks = await self.store.list_pending(limit=20)
        if not tasks:
            yield event.plain_result("当前没有待触发的提醒任务")
            return
        lines = [f"待触发提醒（{len(tasks)} 条）："]
        for task in tasks:
            when = datetime.fromtimestamp(task.trigger_at, tz=TZ_CN).strftime("%m-%d %H:%M")
            label = WeiboTimeParser.advance_label(task.advance_minutes)
            summary = task.weibo.replace("\n", " ")[:24]
            lines.append(f"· {when}｜提前 {label}｜{task.source_text}｜{summary}")
        yield event.plain_result("\n".join(lines))

    @weibo_remind.command("覆盖")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_coverage(self, event: AstrMessageEvent):
        """查看博文覆盖自检：各形态分布、覆盖率、未覆盖样本（管理员）。"""
        if self.coverage is None:
            yield event.plain_result("覆盖自检尚未就绪（插件未完成初始化）")
            return
        stats = self.coverage.stats()
        if not stats["window"]:
            yield event.plain_result("还没有处理记录：等抓取到新微博后再看")
            return
        lines = [f"博文覆盖自检（最近 {stats['window']} 条）"]
        kind_parts = [f"{KIND_LABELS.get(k, k)} {v}" for k, v in sorted(stats["by_kind"].items())]
        lines.append("形态分布：" + "｜".join(kind_parts))
        outcome_parts = [
            f"{OUTCOME_LABELS.get(k, k)} {v}" for k, v in sorted(stats["by_outcome"].items())
        ]
        lines.append("处理结果：" + "｜".join(outcome_parts))
        lines.append(
            f"排期公告类 {stats['schedule_total']} 条：建出提醒 {stats['schedule_tasked']} 条"
            f"｜时间已过正确跳过 {stats['schedule_stale_skipped']} 条"
            f"｜待补 {stats['schedule_gaps']} 条"
        )
        lines.append(
            f"覆盖率 {stats['coverage_percent']}%（口径：建出提醒 ÷（建出提醒+待补），"
            "不含时间已过的博文）"
        )
        lines.append(f"待补样本池：{stats['uncovered_total']} 条")
        for row in stats["uncovered_recent"]:
            lines.append(
                f"· [{row.get('created_at')}] {row.get('reason')}｜{str(row.get('excerpt'))[:40]}"
            )
        lines.append("未覆盖样本明细见插件数据目录 uncovered.jsonl")
        yield event.plain_result("\n".join(lines))

    @weibo_remind.command("全体", alias={"@全体", "at全体"})
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_at_all(self, event: AstrMessageEvent):
        """切换提醒推送时是否 @全体成员：微博提醒 全体 开 / 关 / 状态（管理员）。"""
        assert self.pusher is not None
        arg = self._tail_text(event, 2).strip()
        sessions = self.pusher.sessions()
        groups = [s for s in sessions if Pusher.is_group_session(s)]
        if arg in ("开", "开启", "on", "打开"):
            self.config["at_all"] = True
            self._save_config()
            hint = ""
            if not groups:
                hint = "\n⚠️ 当前推送目标里没有群聊，「全体」只对群聊生效"
            yield event.plain_result(
                "已开启：到点提醒会 @全体成员\n"
                f"档位限定 {self.pusher.at_all_tiers() or '不限'}｜"
                f"每日上限 {self.pusher.at_all_daily_limit() or '不限'} 次"
                f"{hint}\n（需要机器人是群管理员，否则这次 @ 会失败并自动改为不带 @ 重发）"
            )
            return
        if arg in ("关", "关闭", "off", "取消"):
            self.config["at_all"] = False
            self._save_config()
            yield event.plain_result("已关闭：提醒推送不再 @全体成员")
            return
        if arg and arg not in ("状态", "status", "查看"):
            yield event.plain_result("用法：微博提醒 全体 开 / 关 / 状态")
            return
        used = await self._at_all_used_today()
        limit = self.pusher.at_all_daily_limit()
        lines = [
            "@全体成员设置",
            f"总开关：{'开' if self.config.get('at_all') else '关'}",
            f"档位限定：{self.pusher.at_all_tiers() or '不限（每档都 @）'}（0 表示到点档）",
            f"关键词限定：{self.pusher.at_all_keywords() or '不限'}",
            f"每日上限：{limit or '不限'}"
            + (f"｜今日已用 {used} 次" if limit else f"｜今日已用 {used} 次"),
            f"群聊推送目标：{len(groups)} 个",
            "备注：机器人需在群内有管理员权限；带 @ 发送失败会自动去掉 @ 重发一次",
        ]
        yield event.plain_result("\n".join(lines))

    async def _at_all_used_today(self) -> int:
        if self.store is None:
            return 0
        try:
            return await self.store.at_all_today()
        except Exception as e:  # pragma: no cover - 计数读取失败不影响指令回复
            self.logger.info(f"读取 @全体 计数失败（忽略）: {e!r}")
            return 0

    @weibo_remind.command("暂停")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_pause(self, event: AstrMessageEvent):
        """暂停抓取与提醒推送（管理员）。"""
        self._paused = True
        yield event.plain_result("已暂停：不再抓取新微博，也不再推送提醒（待触发任务保留）")

    @weibo_remind.command("恢复")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_resume(self, event: AstrMessageEvent):
        """恢复抓取与提醒推送（管理员）。"""
        self._paused = False
        self._kick.set()
        yield event.plain_result("已恢复运行")

    @weibo_remind.command("重抓")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_refetch(self, event: AstrMessageEvent):
        """立即抓取一轮，不等下个轮询间隔（管理员）。"""
        self._kick.set()
        yield event.plain_result("已触发立即抓取，结果见日志与「微博提醒 状态」")

    @weibo_remind.command("测试")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_test_parse(self, event: AstrMessageEvent):
        """调试时间解析：微博提醒 测试 <时间文本>（管理员）。"""
        text = self._tail_text(event, 2)
        if not text:
            yield event.plain_result(
                "用法：微博提醒 测试 <时间文本>\n例如：微博提醒 测试 今晚8点开播，明天下午3点复盘"
            )
            return
        now = time.time()
        result = self.parser.parse(text, datetime.now(tz=TZ_CN), now)
        lines = [f"解析「{text}」："]
        if result.candidates:
            for cand in result.candidates:
                when = WeiboTimeParser.human_time(cand.trigger_ts)
                lines.append(f"· {when}（{cand.engine}｜命中原文：{cand.source_text}）")
        else:
            lines.append("· 未得到可用时间点")
        lines.append(
            f"时间表达计数 {result.raw_hits}｜纯数字歧义 {result.ambiguous}"
            f"｜疑似含时间安排 {'是' if result.suspect else '否'}"
            f"｜判定解析失败 {'是' if result.parse_failed else '否'}"
        )
        yield event.plain_result("\n".join(lines))

    @staticmethod
    def _tail_text(event: AstrMessageEvent, skip_tokens: int) -> str:
        """取指令后的参数文本（跳过「微博提醒 测试」这两个 token）。"""
        raw = (event.message_str or "").strip()
        tokens = raw.split()
        if len(tokens) <= skip_tokens:
            return ""
        return " ".join(tokens[skip_tokens:]).strip()
