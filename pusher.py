"""推送层：把提醒渲染成文案，经 AstrBot 主动发到目标会话。

后台调度任务里没有 event 对象，主动推送走
`await context.send_message(unified_msg_origin, MessageChain)`——
与同作者的 astrbot_plugin_weibo_forward_to_qqgroup 同一条实测可用路径。
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime
from typing import Any

from astrbot.api.event import MessageChain
from astrbot.api.message_components import AtAll

try:
    from .constants import (
        DEFAULT_PARSE_FAIL_TEMPLATE,
        DEFAULT_REMIND_TEMPLATE,
        TZ_CN,
    )
    from .quiet_hours import QuietHours, parse_quiet_hours
    from .time_parser import WeiboTimeParser
except ImportError:  # 独立脚本 / 测试环境
    from constants import (  # type: ignore
        DEFAULT_PARSE_FAIL_TEMPLATE,
        DEFAULT_REMIND_TEMPLATE,
        TZ_CN,
    )
    from quiet_hours import QuietHours, parse_quiet_hours  # type: ignore
    from time_parser import WeiboTimeParser  # type: ignore

MAKEUP_PREFIX = "⏰ 补发提醒（原定时间已过）\n"
DEFERRED_PREFIX = "⏰ 顺延提醒（免打扰时段已过，原定时间未打扰）\n"


class Pusher:
    """提醒文案渲染与发送。"""

    def __init__(self, context: Any, config: dict[str, Any], log: Any, counter: Any = None):
        self.context = context
        self.config = config
        self.log = log
        # @全体 每日计数（通常是 SQLiteStore；测试可注入替身，None 表示不限额）
        self.counter = counter
        self._quiet: QuietHours | None = parse_quiet_hours(config.get("quiet_hours"))

    # ---------------- 免打扰 ----------------

    def quiet(self) -> QuietHours | None:
        """当前配置的免打扰时段（None 表示未开启）。"""
        return self._quiet

    def in_quiet_hours(self) -> bool:
        """此刻是否处于免打扰时段。

        定时提醒由调度层顺延（不在这里处理）；这条判定用于**非定时**的群发，
        例如「解析失败推原文」「Cookie 失效告警」——它们没有「到点」语义，
        顺延没有意义，深夜直接跳过并记日志。
        """
        return self._quiet is not None and self._quiet.contains(datetime.now(tz=TZ_CN))

    # ---------------- 目标会话 ----------------

    def sessions(self) -> list[str]:
        """推送目标会话列表（绑定指令写入的 + 手工群号拼装的，去重保序）。"""
        out: list[str] = []
        for item in self.config.get("push_sessions") or []:
            umo = str(item).strip()
            if umo and umo not in out:
                out.append(umo)
        platform = str(self.config.get("default_platform") or "aiocqhttp").strip() or "aiocqhttp"
        for item in self.config.get("push_group_ids") or []:
            gid = str(item).strip()
            if not gid:
                continue
            umo = f"{platform}:GroupMessage:{gid}"
            if umo not in out:
                out.append(umo)
        return out

    # ---------------- @全体成员 ----------------

    @staticmethod
    def is_group_session(session: str) -> bool:
        parts = session.split(":", 2)
        return len(parts) >= 2 and parts[1] == "GroupMessage"

    def at_all_tiers(self) -> list[int]:
        """允许 @全体 的提前档位（分钟）。

        默认只对「到点」那一档 @：多档提醒会各推一次，全带上 @全体 等于一小时内
        连炸三次群。空列表表示不限制档位。
        """
        raw = self.config.get("at_all_advance_minutes")
        if raw is None:
            return [0]
        if not isinstance(raw, (list, tuple)):
            return [0]
        out: list[int] = []
        for item in raw:
            try:
                value = max(0, int(item))
            except (TypeError, ValueError):
                continue
            if value not in out:
                out.append(value)
        return out

    def at_all_keywords(self) -> list[str]:
        raw = self.config.get("at_all_keywords")
        if not isinstance(raw, (list, tuple)):
            return []
        return [str(k).strip() for k in raw if str(k).strip()]

    def at_all_daily_limit(self) -> int:
        try:
            return max(0, int(self.config.get("at_all_daily_limit") or 0))
        except (TypeError, ValueError):
            return 0

    async def should_at_all(self, session: str, task: Any) -> bool:
        """这条提醒是否该 @全体。

        依次过四道闸门：群聊会话 → 总开关 → 档位/关键词限定 → 每日限额。
        每日限额是为了防 QQ 对 @全体 的频率限制（默认 5 次/天，超了自动降级为
        普通提醒，内容照发，只是不 @）。
        """
        if not self.is_group_session(session):
            return False
        if not self.config.get("at_all"):
            return False
        tiers = self.at_all_tiers()
        if tiers and getattr(task, "advance_minutes", None) not in tiers:
            return False
        keywords = self.at_all_keywords()
        if keywords:
            haystack = f"{getattr(task, 'weibo', '')}\n{getattr(task, 'source_text', '')}"
            if not any(kw in haystack for kw in keywords):
                return False
        limit = self.at_all_daily_limit()
        if limit > 0 and self.counter is not None:
            used = await self.counter.at_all_today()
            if used >= limit:
                self.log.info(f"今日 @全体 已达上限 {limit} 次，本条降级为普通提醒")
                return False
        return True

    # ---------------- 渲染 ----------------

    @staticmethod
    def _normalize(text: str) -> str:
        return re.sub(r"\n{3,}", "\n\n", text.replace("\\n", "\n")).strip()

    def render_remind(self, task: Any, *, is_makeup: bool = False) -> str:
        """渲染提醒文案。

        占位符用「单次正则替换」而不是逐个 str.replace：先替换 {weibo} 之后，
        正文里若恰好含有 {link} 之类的字样，逐个替换会把它也一并换掉（实测踩到）。
        """
        template = str(self.config.get("remind_template") or DEFAULT_REMIND_TEMPLATE)
        mapping = {
            "{name}": task.author or "微博",
            "{source}": task.source_text or "未标注",
            "{event_time}": WeiboTimeParser.human_time(task.event_ts),
            "{advance}": WeiboTimeParser.advance_label(task.advance_minutes),
            "{weibo}": task.weibo,
            "{link}": task.link,
        }
        text = re.sub(r"\{[a-z_]+\}", lambda m: mapping.get(m.group(0), m.group(0)), template)
        text = self._normalize(text)
        if is_makeup:
            return f"{MAKEUP_PREFIX}{text}"
        snapshot = getattr(task, "snapshot", None) or {}
        if isinstance(snapshot, dict) and snapshot.get("deferred"):
            # 免打扰时段顺延过来的：让群友知道这条本该更早到
            return f"{DEFERRED_PREFIX}{text}"
        return text

    def render_parse_fail(self, author: str, weibo: str, link: str) -> str:
        """渲染解析失败提示（含疑似时间安排但识别不出，交由群友确认）。"""
        template = str(self.config.get("parse_fail_template") or DEFAULT_PARSE_FAIL_TEMPLATE)
        mapping = {
            "{name}": author or "微博",
            "{weibo}": weibo,
            "{link}": link,
            "{time}": datetime.now(tz=TZ_CN).strftime("%m-%d %H:%M"),
        }
        text = re.sub(r"\{[a-z_]+\}", lambda m: mapping.get(m.group(0), m.group(0)), template)
        return self._normalize(text)

    # ---------------- 发送 ----------------

    def _timeout(self) -> int:
        try:
            return max(0, int(self.config.get("message_send_timeout") or 60))
        except (TypeError, ValueError):
            return 60

    def _build_chain(self, text: str, *, at_all: bool = False) -> MessageChain:
        chain = MessageChain().message(text)
        if at_all:
            # OneBot 的 at_all 段（qq="all"），必须放在消息最前面
            chain = MessageChain(chain=[AtAll(), *chain.chain])
        return chain

    async def _send_chain(self, session: str, chain: MessageChain) -> bool:
        timeout = self._timeout()
        try:
            if timeout > 0:
                ok = await asyncio.wait_for(
                    self.context.send_message(session, chain), timeout=timeout
                )
            else:
                ok = await self.context.send_message(session, chain)
            if ok is False:
                self.log.warning(f"推送失败：AstrBot 未找到匹配 {session} 的消息平台")
                return False
            return True
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            self.log.warning(f"推送超时（{timeout} 秒），平台是否已接收未知：{session}")
            return False
        except Exception as e:
            self.log.warning(f"推送异常：{session} → {e!r}")
            return False

    async def send_text(self, session: str, text: str, *, at_all: bool = False) -> bool:
        """向单个会话发送文本（带超时保护，单独失败不影响其它会话）。

        at_all=True 时消息以 at_all 段开头。**带 @ 发送失败会自动去掉 @ 重发一次**：
        群里机器人不是管理员、或撞上 QQ 对 @全体 的频控时，部分协议端会整条拒收，
        连正文都发不出去；降级重发能保证提醒正文一定到群（代价是这次不 @）。
        """
        if at_all and await self._send_chain(session, self._build_chain(text, at_all=True)):
            if self.counter is not None:
                try:
                    await self.counter.bump_at_all()
                except Exception as e:  # pragma: no cover - 计数失败不该影响推送
                    self.log.info(f"@全体 计数失败（忽略）: {e!r}")
            return True
        if at_all:
            self.log.warning(f"带 @全体 发送失败，改为不带 @ 重发：{session}")
        return await self._send_chain(session, self._build_chain(text, at_all=False))

    async def broadcast(
        self, text: str, *, at_all: bool = False, respect_quiet: bool = True
    ) -> tuple[int, int]:
        """向所有目标会话推送，返回（成功数, 总数）。

        注意 at_all 只对群聊会话生效（私聊没有 @全体 概念），由调用方按会话判定；
        这里仅在只有一个目标、且调用方明确要求时透传。
        respect_quiet=True 时，免打扰时段内的群发直接跳过（非定时消息顺延无意义）。
        """
        if respect_quiet and self.in_quiet_hours():
            spec = self._quiet.spec if self._quiet else ""
            self.log.info(f"免打扰时段 {spec}：跳过本条群发（{text.replace(chr(10), ' ')[:24]}）")
            return 0, 0
        sessions = self.sessions()
        if not sessions:
            self.log.warning("尚未配置推送目标：请在群内发送「微博提醒 绑定」或填写 push_group_ids")
            return 0, 0
        ok_count = 0
        for session in sessions:
            use_at = at_all and self.is_group_session(session)
            if await self.send_text(session, text, at_all=use_at):
                ok_count += 1
        return ok_count, len(sessions)

    async def send_remind(self, task: Any, *, is_makeup: bool = False) -> bool:
        """推送一条定时提醒到所有目标会话（至少一个成功即视为成功）。

        是否 @全体 逐会话判定（是否群聊、是否限定档位/关键词、今日额度是否用尽）。
        """
        sessions = self.sessions()
        if not sessions:
            self.log.warning("尚未配置推送目标：请在群内发送「微博提醒 绑定」或填写 push_group_ids")
            return False
        text = self.render_remind(task, is_makeup=is_makeup)
        ok_count = 0
        for session in sessions:
            use_at = await self.should_at_all(session, task)
            if await self.send_text(session, text, at_all=use_at):
                ok_count += 1
        return ok_count > 0

    async def send_parse_fail(self, author: str, weibo: str, link: str) -> bool:
        """推送解析失败提示。

        这类提醒不带 @全体：它不是到点事件，@ 全体只会打扰群友。
        """
        ok_count, total = await self.broadcast(self.render_parse_fail(author, weibo, link))
        return ok_count > 0 and total > 0
