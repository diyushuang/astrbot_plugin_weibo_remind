"""周期表达识别与续期计算测试。"""

from __future__ import annotations

from datetime import datetime

import pytest
from conftest import CN

from recurrence import Recurrence, detect

EVENT = datetime(2026, 10, 23, 20, 0, tzinfo=CN)  # 2026-10-23 是周五


@pytest.mark.parametrize(
    ("text", "weekday"),
    [
        ("每周五晚8点直播", 4),
        ("每星期六 20:00 直播", 5),
        ("每周日 20:00 开播", 6),
        ("每个周五晚8点", None),  # 「每个」不是支持的写法，不误判
        ("每周7 20:00 直播", 6),
    ],
)
def test_detect_weekly(text, weekday):
    rec = detect(text, EVENT)
    if weekday is None:
        assert rec is None
    else:
        assert rec is not None
        assert rec.kind == "weekly" and rec.weekday == weekday
        assert (rec.hour, rec.minute) == (20, 0)


def test_detect_daily():
    rec = detect("每天 20:00 直播", EVENT)
    assert rec is not None
    assert rec.kind == "daily" and rec.weekday is None
    assert (rec.hour, rec.minute) == (20, 0)


@pytest.mark.parametrize("text", ["每天晚8点直播", "每天 20：00 直播", "每日 21:30 掉落"])
def test_detect_daily_variants(text):
    rec = detect(text, EVENT)
    assert rec is not None and rec.kind == "daily"


@pytest.mark.parametrize(
    "text",
    [
        "每日一图，今天随手拍",
        "每天不定时掉落，具体看心情",
        "每天八点直播",  # 纯中文数字，宁可漏续也不误续
    ],
)
def test_daily_without_time_hint_is_not_recurring(text):
    """「每天」的误判面大：没有明确时刻标记就不认成周期提醒。"""
    assert detect(text, EVENT) is None


@pytest.mark.parametrize("text", ["下周五 20:00 直播", "10月24日 20:00 直播", "周一见", ""])
def test_detect_none(text):
    assert detect(text, EVENT) is None


def test_weekly_takes_priority_over_daily_word():
    """ "每天" 与 "每周五" 同现时按每周处理。"""
    rec = detect("每周五更新，每天不定时掉落", EVENT)
    assert rec is not None and rec.kind == "weekly"


def test_next_event_ts():
    weekly = Recurrence(kind="weekly", hour=20, minute=0, weekday=4)
    daily = Recurrence(kind="daily", hour=20, minute=0)
    base = EVENT.timestamp()
    assert weekly.next_event_ts(base) == base + 7 * 86400
    assert daily.next_event_ts(base) == base + 86400


def test_dict_roundtrip():
    rec = Recurrence(kind="weekly", hour=20, minute=30, weekday=4)
    assert Recurrence.from_dict(rec.to_dict()) == rec


@pytest.mark.parametrize(
    "raw",
    [
        None,
        {},
        {"kind": "monthly", "hour": 20, "minute": 0},
        {"kind": "daily", "hour": 24, "minute": 0},
        {"kind": "daily", "hour": 20, "minute": 99},
        {"kind": "weekly", "hour": 20, "minute": 0},  # 缺 weekday
        {"kind": "weekly", "hour": 20, "minute": 0, "weekday": 9},
        {"kind": "daily", "hour": "x", "minute": 0},
    ],
)
def test_from_dict_rejects_bad_data(raw):
    assert Recurrence.from_dict(raw) is None
