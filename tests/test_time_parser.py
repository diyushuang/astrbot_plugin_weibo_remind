"""时间解析用例表。

真实文本取自被监控账号（UID 6902546888）的实际微博，覆盖排期体微博的
「日期 + 时刻分行」「【HHMM】」两种格式——这两种是 jionlp 覆盖不到的，
也正是自研结构化层存在的理由。
"""

from __future__ import annotations

from datetime import datetime

import pytest
from conftest import local, ts

from time_parser import WeiboTimeParser


@pytest.fixture
def parser(base_config, log):
    return WeiboTimeParser(base_config, log)


def parse(parser, text, base: datetime, now_ts: float):
    return parser.parse(text, base, now_ts)


# ---------------- 自然语言表达（jionlp 层） ----------------


@pytest.mark.parametrize(
    ("text", "base", "now", "expect"),
    [
        ("今晚8点直播", datetime(2026, 10, 16, 14, 0), ts(2026, 10, 16, 15, 0), "10-16 20:00"),
        ("晚上8点见", datetime(2026, 10, 16, 14, 0), ts(2026, 10, 16, 15, 0), "10-16 20:00"),
        ("今晚八点", datetime(2026, 10, 16, 14, 0), ts(2026, 10, 16, 15, 0), "10-16 20:00"),
        (
            "明天下午3点开奖",
            datetime(2026, 10, 16, 14, 0),
            ts(2026, 10, 16, 15, 0),
            "10-17 15:00",
        ),
        ("后天下午3点", datetime(2026, 10, 16, 14, 0), ts(2026, 10, 16, 15, 0), "10-18 15:00"),
        ("明晚8点半见", datetime(2026, 10, 16, 14, 0), ts(2026, 10, 16, 15, 0), "10-17 20:30"),
        (
            "晚上8点到10点有活动",
            datetime(2026, 10, 16, 14, 0),
            ts(2026, 10, 16, 15, 0),
            "10-16 20:00",
        ),
        (
            "双11上午10点开奖",
            datetime(2026, 10, 16, 8, 0),
            ts(2026, 10, 16, 9, 0),
            "11-11 10:00",
        ),
        ("20:00 开售", datetime(2026, 10, 16, 10, 0), ts(2026, 10, 16, 11, 0), "10-16 20:00"),
    ],
)
def test_natural_language(parser, text, base, now, expect):
    result = parse(parser, text, base, now)
    assert [local(c.trigger_ts) for c in result.candidates] == [expect]


def test_period_takes_nearest_occurrence(parser):
    """周期表达（每周五晚8点）只取最近一次：2026-10-16 是周五。"""
    result = parse(
        parser, "每周五晚8点团建", datetime(2026, 10, 16, 14, 0), ts(2026, 10, 16, 15, 0)
    )
    assert [local(c.trigger_ts) for c in result.candidates] == ["10-16 20:00"]


# ---------------- 结构化层：日期 + 时刻分行 ----------------


def test_real_post_date_then_clock_next_line(parser):
    """真实排期体：⏰10月17日 / 10:00 视频——时刻必须归属到前一行日期。

    jionlp 单独处理会把 10:00 判成「今天 10:00」（实测），结构化层负责纠正。
    """
    text = "📢商务物料 / @iCAR汽车 / 发布规划 / ⏰10月17日 / 10:00 邀约试驾视频 / ⏰10月18日 / 10:00 带你兜风视频"
    result = parse(parser, text, datetime(2026, 10, 16, 10, 0), ts(2026, 10, 16, 11, 0))
    assert [local(c.trigger_ts) for c in result.candidates] == ["10-17 10:00", "10-18 10:00"]
    assert all(c.engine == "structural" for c in result.candidates)


def test_real_post_bracket_clock(parser):
    """真实排期体：【0945】开播预告——括号内四位数字是 HH:MM。"""
    text = "📢9月28日《无可替代》发布节奏 / 【0945】开播预告 / 【0950】单人海报 / 【1010】单人海报"
    base = datetime(2026, 9, 27, 22, 50)
    result = parse(parser, text, base, ts(2026, 9, 27, 23, 0))
    assert [local(c.trigger_ts) for c in result.candidates] == [
        "09-28 09:45",
        "09-28 09:50",
        "09-28 10:10",
    ]


def test_real_post_date_only_uses_default_hour(parser):
    """真实行程博：「10月17日」只有日期没有时刻 → 按默认小时 9 点。"""
    text = "📢行程同步 / 10月17日 / 🫘直播 / 空降🫘@奈丝公主 官方旗舰店"
    result = parse(parser, text, datetime(2026, 10, 16, 10, 0), ts(2026, 10, 16, 11, 0))
    assert [local(c.trigger_ts) for c in result.candidates] == ["10-17 09:00"]
    assert result.candidates[0].date_only is True


def test_date_with_clock_suppresses_date_only_duplicate(parser):
    """同一天既有精确时刻又有纯日期候选时，只保留精确的那条。"""
    text = "10月17日 20:00 直播 / 10月17日 上新"
    result = parse(parser, text, datetime(2026, 10, 16, 10, 0), ts(2026, 10, 16, 11, 0))
    assert [local(c.trigger_ts) for c in result.candidates] == ["10-17 20:00"]


@pytest.mark.parametrize("hour", [0, 9, 23])
def test_date_only_default_hour_configurable(hour, log):
    parser = WeiboTimeParser({"date_only_default_hour": hour, "max_future_days": 60}, log)
    result = parse(parser, "10月30日发售", datetime(2026, 10, 16, 14, 0), ts(2026, 10, 16, 15, 0))
    assert local(result.candidates[0].trigger_ts) == f"10-30 {hour:02d}:00"


# ---------------- 纯数字层 ----------------


@pytest.mark.parametrize(
    ("text", "expect"),
    [
        ("1030上线", "10-30 09:00"),
        ("1030发售", "10-30 09:00"),
        ("1030预售开启", "10-30 09:00"),
        ("20261030上线", "10-30 09:00"),
    ],
)
def test_numeric_as_date(parser, text, expect):
    result = parse(parser, text, datetime(2026, 10, 16, 8, 0), ts(2026, 10, 16, 9, 0))
    assert [local(c.trigger_ts) for c in result.candidates] == [expect]


@pytest.mark.parametrize(
    ("text", "expect"),
    [
        ("1030开播", "10-16 10:30"),
        ("2130直播", "10-16 21:30"),
    ],
)
def test_numeric_as_clock(parser, text, expect):
    result = parse(parser, text, datetime(2026, 10, 16, 8, 0), ts(2026, 10, 16, 8, 30))
    assert [local(c.trigger_ts) for c in result.candidates] == [expect]


def test_numeric_ambiguous_creates_nothing(parser):
    """「1030」无上下文：既能读成 10:30 也能读成 10月30日 → 不建任务。"""
    result = parse(parser, "1030", datetime(2026, 10, 16, 8, 0), ts(2026, 10, 16, 8, 30))
    assert result.candidates == []
    assert result.ambiguous == 1


def test_numeric_clock_already_past_is_dropped(parser):
    """「1030开播」但当前已过 10:30 → 静默丢弃（不在候选里）。"""
    result = parse(parser, "1030开播", datetime(2026, 10, 16, 8, 0), ts(2026, 10, 16, 12, 0))
    assert result.candidates == []
    assert result.raw_hits > 0  # 识别到了时间，只是已过去


# ---------------- 噪音与失败判定 ----------------


def test_noise_only_text_is_not_a_parse_failure(parser):
    """纯噪音（话题/期号/价格）不该触发解析失败推送。"""
    result = parse(
        parser, "#1030# 第1030期 每人1000元", datetime(2026, 10, 16, 9, 0), ts(2026, 10, 16, 10, 0)
    )
    assert result.candidates == []
    assert result.suspect is False
    assert result.parse_failed is False


def test_plain_post_is_not_a_parse_failure(parser):
    """日常博文（没有时间安排）不该触发解析失败推送。"""
    result = parse(
        parser, "今天天气不错，大家早安", datetime(2026, 10, 16, 9, 0), ts(2026, 10, 16, 10, 0)
    )
    assert result.suspect is False
    assert result.parse_failed is False


def test_vague_relative_day_creates_nothing(parser):
    """ "今天有直播"这类含糊表达不建任务（否则会在 9 点误报）。"""
    result = parse(
        parser, "今天有直播，记得来看", datetime(2026, 10, 16, 8, 0), ts(2026, 10, 16, 8, 10)
    )
    assert result.candidates == []


def test_suspect_but_unparsable_marks_failure(parser):
    """含时间安排字样但识别不出时间 → 标记解析失败，交由主流程推送原文。"""
    text = "【待定】《无可替代》发布节奏 / 具体时间后续公布"
    result = parse(parser, text, datetime(2026, 10, 16, 9, 0), ts(2026, 10, 16, 10, 0))
    assert result.candidates == []
    assert result.parse_failed is True


# ---------------- 窗口与排序 ----------------


def test_far_future_beyond_max_days_is_dropped(base_config, log):
    parser = WeiboTimeParser({**base_config, "max_future_days": 10}, log)
    text = "12月31日跨年直播"
    result = parse(parser, text, datetime(2026, 10, 16, 9, 0), ts(2026, 10, 16, 10, 0))
    assert result.candidates == []


def test_candidates_sorted_ascending_and_deduped(parser):
    text = "10月17日 10:00 视频 / 10月17日 10:00 同一条微博重复提到 / 10月18日 20:00 直播"
    result = parse(parser, text, datetime(2026, 10, 16, 9, 0), ts(2026, 10, 16, 10, 0))
    assert [local(c.trigger_ts) for c in result.candidates] == ["10-17 10:00", "10-18 20:00"]


def test_past_time_is_silently_skipped(parser):
    """解析出的时间已过去 → 无候选，但有 raw_hits（不按解析失败处理）。"""
    result = parse(parser, "今天上午9点直播", datetime(2026, 10, 16, 8, 0), ts(2026, 10, 16, 12, 0))
    assert result.candidates == []
    assert result.raw_hits > 0


@pytest.mark.parametrize(
    ("minutes", "expect"),
    [(0, "到点"), (10, "10 分钟"), (60, "1 小时"), (120, "2 小时"), (1440, "1 天")],
)
def test_advance_label(minutes, expect):
    assert WeiboTimeParser.advance_label(minutes) == expect
