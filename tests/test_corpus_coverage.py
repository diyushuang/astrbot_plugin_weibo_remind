"""真实语料回归测试。

夹具 `corpus/weibo_posts.json` 全部取自被监控账号（UID 6902546888）的真实微博，
期望值是人工核对过的时间点，不是直接抄解析器输出。作用是：
**锁死已经支持的写法**，避免以后调规则时把「⏰日期+换行时刻」「括号时刻」
「紧凑日期 MMDD」这些实测过的能力弄回归。
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from post_coverage import KIND_DAILY, KIND_NOTICE, KIND_SCHEDULE, KIND_SUGGESTION, classify_post
from time_parser import WeiboTimeParser

TZ = ZoneInfo("Asia/Shanghai")
CORPUS = Path(__file__).resolve().parent / "corpus" / "weibo_posts.json"
CASES = json.loads(CORPUS.read_text(encoding="utf-8"))["cases"]


@pytest.fixture
def parser(log):
    return WeiboTimeParser(
        {"date_only_default_hour": 9, "max_future_days": 3650, "debug_parse": False}, log
    )


def _run(parser, case):
    base = datetime.strptime(case["created_at"], "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
    result = parser.parse(case["text"], base, base.timestamp())
    got = [
        datetime.fromtimestamp(c.trigger_ts, tz=TZ).strftime("%m-%d %H:%M")
        for c in result.candidates
    ]
    return result, got


@pytest.mark.parametrize("case", CASES, ids=lambda c: c["id"])
def test_kind_matches_corpus(parser, case):
    """每条真实博文的形态判定必须与夹具一致。"""
    assert classify_post(case["text"]).kind == case["kind"], case["note"]


@pytest.mark.parametrize("case", CASES, ids=lambda c: c["id"])
def test_candidates_match_corpus(parser, case):
    """解析出的时间点必须与人工核对的期望一致（顺序即时间升序）。"""
    _, got = _run(parser, case)
    assert got == case["expect_candidates"], f"{case['note']}\n实际：{got}"


@pytest.mark.parametrize("case", CASES, ids=lambda c: c["id"])
def test_silent_cases_produce_nothing(parser, case):
    """标记为静默的博文（建议博/打假声明/日常）不得产生任何任务。"""
    if not case.get("expect_silent"):
        pytest.skip("非静默样本")
    result, got = _run(parser, case)
    assert got == []
    assert not classify_post(case["text"]).schedule


def test_pending_time_case_is_recognised(parser):
    """「具体发布待定」必须被识别为待定，主流程据此推原文而不是静默丢弃。"""
    from post_coverage import has_pending_time

    case = next(c for c in CASES if c.get("expect_pending"))
    assert has_pending_time(case["text"]) is True
    assert classify_post(case["text"]).kind == KIND_SCHEDULE


def test_corpus_kinds_are_all_covered():
    """夹具要覆盖全部四种形态，否则分类规则缺少回归保护。"""
    kinds = {c["kind"] for c in CASES}
    assert {KIND_SCHEDULE, KIND_NOTICE, KIND_SUGGESTION, KIND_DAILY} <= kinds


def test_bracket_numbers_are_never_read_as_years(parser):
    """【1700】必须解析成 17:00——jionlp 会把它读成公元 1700 年（实测踩到）。"""
    case = next(c for c in CASES if c["expect_candidates"] and "1700" in c["text"])
    _, got = _run(parser, case)
    assert "09-28 17:00" in got
    assert all(not t.startswith("17") for t in got)  # 没有任何候选落在 1700 年


def test_naive_clock_rolls_to_next_day_for_evening_schedule(log):
    """前夜发次日排期：「7:28 发布…」发布于当天 16:01，应理解为次日 07:28。"""
    parser = WeiboTimeParser({"date_only_default_hour": 9, "max_future_days": 3650}, log)
    base = datetime(2025, 12, 26, 16, 1, tzinfo=TZ)
    text = "📢新商务官宣\n@奥利奥官方微博\n7:28 发布骄阳似我中插广告\n*剧集中插同步上线"
    result = parser.parse(text, base, base.timestamp())
    got = [
        datetime.fromtimestamp(c.trigger_ts, tz=TZ).strftime("%m-%d %H:%M")
        for c in result.candidates
    ]
    assert got == ["12-27 07:28"]


def test_unreasonable_jionlp_year_is_dropped(log):
    """jionlp 给出远超基准年的候选（年份噪声）必须丢弃，且不能抛异常。"""
    parser = WeiboTimeParser({"date_only_default_hour": 9, "max_future_days": 3650}, log)
    base = datetime(2026, 10, 16, 10, 0, tzinfo=TZ)
    result = parser.parse("【1700】二创剪辑 / 1999年怀旧特辑", base, base.timestamp())
    years = {datetime.fromtimestamp(c.trigger_ts, tz=TZ).year for c in result.candidates}
    assert all(2025 <= y <= 2029 for y in years)
