"""LLM 兜底时间解析。

只在**规则三层都失败且博文形态为排期类**时调用（默认关闭，需显式开启）。
LLM 只做「从正文里抽时间」这一件事，输出必须过四道校验闸门才能建任务——
LLM 会自信地编造时间，这道闸门就是把「看起来像时间」挡在提醒之外：

1. **可解析**：`when` 必须能解析成 datetime（ISO 或 `YYYY-MM-DD HH:MM`）。
2. **年份合理**：落在「微博发布年 -1 ~ +3」内（防把【1700】读成公元 1700 年）。
3. **时间在窗口内**：不早于发布时间（容忍 5 分钟）、不超过 `max_future_days`。
4. **引用真实**：`source` 必须在正文里真的出现过（忽略空白后做子串匹配），
   编造的「10月24日晚8点」会被这条挡掉。

任何一条不过 → 该候选丢弃；全部丢弃 → 走原有的「推原文请群友确认」路径，
不因为 LLM 的加入而改变既有失败行为。
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

try:
    from .constants import TZ_CN
except ImportError:  # 独立脚本 / 测试环境
    from constants import TZ_CN  # type: ignore

# 低于该置信度的候选直接丢弃（LLM 自己都说不准）
MIN_CONFIDENCE = 0.5
# 允许「比发布时间早一点点」的容忍（时钟误差/补发场景），单位秒
PAST_TOLERANCE = 300
_YEAR_SPAN_BACK = 1
_YEAR_SPAN_FORWARD = 3

SYSTEM_PROMPT = """你是一个严格的时间抽取工具。任务：从一条微博正文里找出博主真正要执行/告知的时间点。

规则：
1. 只输出 JSON，不要输出任何解释、前言或 markdown 代码块标记。
2. 格式：{"times":[{"when":"YYYY-MM-DD HH:MM","source":"正文里对应的原文片段","confidence":0.0}]}
3. when 必须是完整的日期+时刻，按给定的微博发布时间和时区推算（"今晚8点"要换算成具体日期）。
4. source 必须逐字摘自正文（可以含少量标点），不得改写、不得拼接。
5. 只抽取「未来的安排」：发布/开播/上线/活动等时间。历史回顾、统计数字、价格、编号、期数一律忽略。
6. 如果没有明确的时间安排，返回 {"times":[]}。宁可返回空，也不要猜。
"""


@dataclass
class LLMCandidate:
    """通过校验闸门的一个时间点。"""

    trigger_ts: float
    source_text: str
    confidence: float = 0.0


def build_user_prompt(body: str, base_dt: datetime) -> str:
    return (
        f"微博发布时间：{base_dt.strftime('%Y-%m-%d %H:%M')}（Asia/Shanghai）\n"
        f"微博正文：\n{body}\n\n"
        "请按要求输出 JSON。"
    )


def _strip_code_fence(text: str) -> str:
    """去掉 ```json ... ``` 围栏（LLM 常见输出习惯）。"""
    cleaned = (text or "").strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", cleaned, flags=re.S | re.I)
    return fence.group(1).strip() if fence else cleaned


def parse_response(text: str) -> list[dict[str, Any]]:
    """从 LLM 输出里取出 times 数组；任何异常一律返回空列表（不抛）。"""
    raw = _strip_code_fence(text)
    if not raw:
        return []
    start, end = raw.find("{"), raw.rfind("}")
    if start < 0 or end <= start:
        return []
    try:
        data = json.loads(raw[start : end + 1])
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(data, dict):
        return []
    times = data.get("times")
    if not isinstance(times, list):
        return []
    return [item for item in times if isinstance(item, dict)]


def _parse_when(raw: Any) -> datetime | None:
    """把 when 解析成带时区的 datetime；无时区按 Asia/Shanghai。"""
    value = str(raw or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00").replace("/", "-")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        # 兜底几种常见写法：2026-10-24 20:00(已覆盖) / 2026年10月24日 20:00
        m = re.match(r"^(\d{4})年(\d{1,2})月(\d{1,2})日?\s*(\d{1,2}):(\d{2})$", value)
        if not m:
            return None
        try:
            dt = datetime(*(int(g) for g in m.groups()))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ_CN)
    return dt


def _squash(text: str) -> str:
    """去掉全部空白，用于「引用是否真的来自正文」的宽松比对。"""
    return re.sub(r"\s+", "", text or "")


def validate(
    items: list[dict[str, Any]],
    body: str,
    base_ts: float,
    now_ts: float,
    max_future_days: int,
) -> list[LLMCandidate]:
    """四道闸门全过才返回；结果按时间升序去重。"""
    base_dt = datetime.fromtimestamp(base_ts, tz=TZ_CN)
    body_squashed = _squash(body)
    out: list[LLMCandidate] = []
    seen: set[float] = set()
    for item in items:
        dt = _parse_when(item.get("when"))
        if dt is None:
            continue
        # 闸门 2：年份合理
        if not (base_dt.year - _YEAR_SPAN_BACK <= dt.year <= base_dt.year + _YEAR_SPAN_FORWARD):
            continue
        ts = dt.timestamp()
        # 闸门 3：时间窗口
        if ts < base_ts - PAST_TOLERANCE:
            continue
        if max_future_days > 0 and ts > base_ts + max_future_days * 86400:
            continue
        # 闸门 4：引用必须真的出现在正文里
        source = str(item.get("source") or "").strip()
        if not source or _squash(source) not in body_squashed:
            continue
        try:
            confidence = float(item.get("confidence", 1.0))
        except (TypeError, ValueError):
            confidence = 0.0
        if confidence < MIN_CONFIDENCE:
            continue
        if ts in seen:
            continue
        seen.add(ts)
        out.append(LLMCandidate(trigger_ts=ts, source_text=source, confidence=confidence))
    out.sort(key=lambda c: c.trigger_ts)
    return out


AskFn = Callable[[str, str], Awaitable[str]]


class LLMFallback:
    """LLM 兜底抽取器；ask 可注入，便于测试不触网。"""

    def __init__(self, config: dict[str, Any], log: Any, ask: AskFn | None = None):
        self.config = config
        self.log = log
        self._ask = ask
        self.fail_streak = 0
        self.disabled_reason = ""

    @staticmethod
    def enabled(config: dict[str, Any]) -> bool:
        return bool(config.get("llm_fallback"))

    def _timeout(self) -> int:
        try:
            return max(5, int(self.config.get("llm_timeout_seconds") or 25))
        except (TypeError, ValueError):
            return 25

    async def extract(self, body: str, base_dt: datetime, now_ts: float) -> list[LLMCandidate]:
        """返回通过校验的候选；任何失败都返回空列表（绝不抛异常）。"""
        if self._ask is None or self.disabled_reason:
            return []
        if self.fail_streak >= 3:
            return []
        prompt = build_user_prompt(body, base_dt)
        try:
            text = await asyncio.wait_for(self._ask(prompt, SYSTEM_PROMPT), timeout=self._timeout())
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            self.fail_streak += 1
            self.log.warning(
                f"LLM 兜底解析超时（{self._timeout()} 秒，连续第 {self.fail_streak} 次）"
            )
            return []
        except Exception as e:
            self.fail_streak += 1
            self.log.warning(f"LLM 兜底解析失败（连续第 {self.fail_streak} 次）: {e!r}")
            return []

        items = parse_response(text)
        if not items:
            self.log.info("LLM 未从正文中抽出时间（返回空或格式不符）")
            return []
        try:
            max_future_days = int(self.config.get("max_future_days") or 60)
        except (TypeError, ValueError):
            max_future_days = 60
        candidates = validate(items, body, base_dt.timestamp(), now_ts, max_future_days)
        dropped = len(items) - len(candidates)
        if dropped:
            self.log.info(f"LLM 给出 {len(items)} 个时间点，{dropped} 个未通过校验被丢弃")
        if self.fail_streak:
            self.fail_streak = 0
        return candidates
