"""周期表达的识别与续期计算。

只覆盖两类高频写法（其余交给 jionlp 解析「最近一次」，不自动续期）：
- ``每周五 / 每星期六 / 每周日`` → 每周同一时刻再提醒一次
- ``每天 / 每日`` → 每天同一时刻再提醒一次

**刻意不支持「每月 X 日」**：月份长度不一致，续期边界（31 日 → 2 月）容易算错，
而排期体微博里月度表达远少于周度。宁可明确不支持，也不做半对的续期。

续期的锚点只有「到点」档（advance=0）：多档提醒每档各续一次会重复建任务，
UNIQUE 虽然拦得住，但日志与状态会很乱。缺 0 档的配置不续期（文档已写明）。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

_DAY_SECONDS = 86400
_WEEK_SECONDS = 7 * _DAY_SECONDS

_WEEKDAY_MAP = {
    "一": 0,
    "二": 1,
    "三": 2,
    "四": 3,
    "五": 4,
    "六": 5,
    "日": 6,
    "天": 6,
    "1": 0,
    "2": 1,
    "3": 2,
    "4": 3,
    "5": 4,
    "6": 5,
    "7": 6,
}
_WEEKLY_RE = re.compile(r"每(?:周|星期|礼拜)\s*([一二三四五六日天1-7])")
_DAILY_RE = re.compile(r"每(?:天|日)")
# 「每天」的误判面比「每周五」大得多（"每日一图""每天不定时掉落"都会命中），
# 所以额外要求正文里出现明确的时刻标记才认（20:00 / 8点）。
# 代价是「每天八点」这类纯中文数字不续期——宁可漏续，也不每天误提醒。
_TIME_HINT_RE = re.compile(r"\d{1,2}\s*[:：]\s*\d{2}|\d{1,2}\s*点")


@dataclass(frozen=True)
class Recurrence:
    """一个可续期的周期规则（时/分取自本次解析出的候选时间点）。"""

    kind: str  # "daily" | "weekly"
    hour: int
    minute: int
    weekday: int | None = None  # 0=周一，仅 weekly 使用

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "hour": self.hour,
            "minute": self.minute,
            "weekday": self.weekday,
        }

    @staticmethod
    def from_dict(raw: Any) -> Recurrence | None:
        if not isinstance(raw, dict):
            return None
        kind = str(raw.get("kind") or "")
        if kind not in ("daily", "weekly"):
            return None
        try:
            hour = int(raw.get("hour"))
            minute = int(raw.get("minute"))
        except (TypeError, ValueError):
            return None
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None
        weekday = raw.get("weekday")
        if kind == "weekly":
            try:
                weekday = int(weekday)
            except (TypeError, ValueError):
                return None
            if not 0 <= weekday <= 6:
                return None
        else:
            weekday = None
        return Recurrence(kind=kind, hour=hour, minute=minute, weekday=weekday)

    def next_event_ts(self, event_ts: float) -> float:
        """下一个周期的同一时刻（上海无夏令时，直接加固定秒数即可）。"""
        return event_ts + (_WEEK_SECONDS if self.kind == "weekly" else _DAY_SECONDS)


def detect(body: str, event_dt: datetime) -> Recurrence | None:
    """识别正文里的周期表达；时刻取自本次解析出的候选时间点。"""
    text = body or ""
    m = _WEEKLY_RE.search(text)
    if m:
        weekday = _WEEKDAY_MAP.get(m.group(1))
        if weekday is None:
            return None
        return Recurrence(
            kind="weekly", hour=event_dt.hour, minute=event_dt.minute, weekday=weekday
        )
    if _DAILY_RE.search(text) and _TIME_HINT_RE.search(text):
        return Recurrence(kind="daily", hour=event_dt.hour, minute=event_dt.minute)
    return None
