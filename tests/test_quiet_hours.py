"""免打扰时段解析测试。"""

from __future__ import annotations

from datetime import datetime

import pytest
from conftest import CN

from quiet_hours import parse_quiet_hours


def _dt(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=CN)


def test_parse_valid_specs():
    q = parse_quiet_hours("23:00-08:00")
    assert q is not None
    assert q.spec == "23:00-08:00"
    assert q.crosses_midnight is True

    q2 = parse_quiet_hours("12:00-14:30")
    assert q2 is not None and q2.crosses_midnight is False


@pytest.mark.parametrize("raw", ["", None, "  ", "23:00", "25:00-08:00", "23:70-08:00", "abc"])
def test_parse_invalid_returns_none(raw):
    assert parse_quiet_hours(raw) is None


def test_cross_midnight_contains():
    """23:00-08:00：深夜与凌晨都算时段内，白天不算。"""
    q = parse_quiet_hours("23:00-08:00")
    assert q is not None
    assert q.contains(_dt(17, 23, 0)) is True
    assert q.contains(_dt(17, 23, 59)) is True
    assert q.contains(_dt(18, 0, 0)) is True
    assert q.contains(_dt(18, 7, 59)) is True
    assert q.contains(_dt(18, 8, 0)) is False
    assert q.contains(_dt(18, 12, 0)) is False
    assert q.contains(_dt(18, 22, 59)) is False


def test_same_day_contains():
    q = parse_quiet_hours("12:00-14:00")
    assert q is not None
    assert q.contains(_dt(17, 11, 59)) is False
    assert q.contains(_dt(17, 12, 0)) is True
    assert q.contains(_dt(17, 13, 59)) is True
    assert q.contains(_dt(17, 14, 0)) is False


def test_end_after_cross_midnight():
    """23:00-08:00：刚过 23:00 时顺延到次日 08:00；凌晨 01:00 时顺延到当天 08:00。"""
    q = parse_quiet_hours("23:00-08:00")
    assert q is not None
    assert q.end_after(_dt(17, 23, 30)) == _dt(18, 8, 0)
    assert q.end_after(_dt(18, 1, 0)) == _dt(18, 8, 0)


def test_end_after_same_day():
    q = parse_quiet_hours("12:00-14:00")
    assert q is not None
    assert q.end_after(_dt(17, 12, 30)) == _dt(17, 14, 0)


def test_zero_length_spec_never_quiet():
    """start == end 视为 0 长度（不生效），避免用户误填成全天静音。"""
    q = parse_quiet_hours("08:00-08:00")
    assert q is not None
    assert q.contains(_dt(17, 8, 0)) is False
