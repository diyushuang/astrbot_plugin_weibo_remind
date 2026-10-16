"""博文形态分类与覆盖自检测试。"""

from __future__ import annotations

import json

import pytest

from post_coverage import (
    KIND_DAILY,
    KIND_EMPTY,
    KIND_NOTICE,
    KIND_SCHEDULE,
    KIND_SUGGESTION,
    OUTCOME_PAST,
    OUTCOME_PUSHED,
    OUTCOME_SILENT,
    OUTCOME_TASKED,
    CoverageTracker,
    PostOutcome,
    classify_post,
    has_pending_time,
)


@pytest.mark.parametrize(
    ("text", "expect"),
    [
        # 排期/公告类
        ("📢9月28日《无可替代》发布节奏", KIND_SCHEDULE),
        ("📢商务物料 / 发布规划", KIND_SCHEDULE),
        ("新商务官宣\n@奈丝公主", KIND_SCHEDULE),
        ("⏰10月17日\n10:00 邀约试驾视频", KIND_SCHEDULE),
        ("📢行程同步 / 10月17日 / 直播", KIND_SCHEDULE),
        ("9月22日\n新代言官宣\n9:29 系列海报", KIND_SCHEDULE),
        # 建议反馈类（含「X月」也不该当时间信号）
        ("赵今麦 \n📭【7月建议博】", KIND_SUGGESTION),
        ("📭【6月建议博反馈】如下图👇", KIND_SUGGESTION),
        ("麦报已整理传达，感谢大家提出的建议！", KIND_SUGGESTION),
        # 声明打假类（含「行程」「通知」等排期词，优先级必须高于排期）
        ("📢通知\n网传活动行程为假，请大家以官方通知为准", KIND_NOTICE),
        ("麦报在此声明🎙️ 请勿轻信他人", KIND_NOTICE),
        ("关于某某的传言不实，属于断章取义。", KIND_NOTICE),
        # 日常类
        ("品牌的城市地广，欢迎大家积极打卡～", KIND_DAILY),
        ("经确认，由于剧组拍摄安排，麦麦不出席本次活动。", KIND_DAILY),
        ("这段采访是赵今麦接受其他媒体中提到的，谢绝断章取义。", KIND_NOTICE),
        # 空文本
        ("", KIND_EMPTY),
        ("   ", KIND_EMPTY),
    ],
)
def test_classify_post(text, expect):
    assert classify_post(text).kind == expect


def test_schedule_flag_only_for_schedule_kind():
    assert classify_post("发布节奏 10月17日").schedule is True
    for text in ("【7月建议博】", "网传为假 请勿轻信", "大家早安"):
        assert classify_post(text).schedule is False


def test_notice_beats_schedule_when_both_signals_present():
    """打假博文里同时有「行程」和「通知」，必须判为声明类（实测语料里的真实写法）。"""
    text = "📢通知\n网传剧宣行程为假，请大家以官方通知为准，避免财产损失。"
    kind = classify_post(text)
    assert kind.kind == KIND_NOTICE
    assert kind.evidence


@pytest.mark.parametrize(
    ("text", "expect"),
    [
        ("10月24日 官宣杀青物料\n\n具体发布待定。", True),
        ("发布时间另行通知", True),
        ("后续公布具体时间", True),
        ("10月17日 10:00 直播", False),
    ],
)
def test_has_pending_time(text, expect):
    assert has_pending_time(text) is expect


# ---------------- 覆盖自检 ----------------


def _rec(post_id: str, kind: str, outcome: str, tasks: int = 0, gap: bool = False) -> PostOutcome:
    return PostOutcome(
        post_id=post_id,
        created_at="10-16 10:00",
        kind=kind,
        outcome=outcome,
        tasks_created=tasks,
        reason="测试",
        excerpt="正文摘要",
        gap=gap,
    )


def test_stats_counts_kinds_and_outcomes(tmp_path, log):
    tracker = CoverageTracker(tmp_path, log)
    tracker.record(_rec("a", KIND_SCHEDULE, OUTCOME_TASKED, 3))
    tracker.record(_rec("b", KIND_SCHEDULE, OUTCOME_PAST))
    tracker.record(_rec("c", KIND_SUGGESTION, OUTCOME_SILENT))
    tracker.record(_rec("d", KIND_NOTICE, OUTCOME_SILENT))

    stats = tracker.stats()
    assert stats["window"] == 4
    assert stats["by_kind"][KIND_SCHEDULE] == 2
    assert stats["by_outcome"][OUTCOME_SILENT] == 2
    assert stats["schedule_total"] == 2
    assert stats["schedule_tasked"] == 1
    # 时间已过的那条属于正确跳过：不进分母，覆盖率仍是 100%
    assert stats["schedule_stale_skipped"] == 1
    assert stats["schedule_gaps"] == 0
    assert stats["coverage_percent"] == 100.0


def test_coverage_counts_only_real_gaps(tmp_path, log):
    """覆盖率分母 = 建出提醒 + 真正的缺口；「时间已过」不拉低覆盖率。"""
    tracker = CoverageTracker(tmp_path, log)
    for i in range(8):
        tracker.record(_rec(f"ok{i}", KIND_SCHEDULE, OUTCOME_TASKED, 1))
    for i in range(3):
        tracker.record(_rec(f"stale{i}", KIND_SCHEDULE, OUTCOME_PAST))  # 正确跳过
    tracker.record(_rec("gap0", KIND_SCHEDULE, OUTCOME_PUSHED, 0, gap=True))

    stats = tracker.stats()
    assert stats["schedule_tasked"] == 8
    assert stats["schedule_gaps"] == 1
    assert stats["schedule_stale_skipped"] == 3
    assert stats["coverage_percent"] == round(8 / 9 * 100, 1)


def test_uncovered_pool_requires_gap_flag(tmp_path, log):
    """只有带缺口标记的博文进待补样本；建议博/声明/时间已过都不该污染样本池。"""
    tracker = CoverageTracker(tmp_path, log)
    tracker.record(_rec("gap", KIND_SCHEDULE, OUTCOME_SILENT, gap=True))
    tracker.record(_rec("gap-pushed", KIND_SCHEDULE, OUTCOME_PUSHED, gap=True))
    tracker.record(_rec("stale", KIND_SCHEDULE, OUTCOME_PAST))
    tracker.record(_rec("sugg", KIND_SUGGESTION, OUTCOME_SILENT))
    tracker.record(_rec("ok", KIND_SCHEDULE, OUTCOME_TASKED, 2))

    stats = tracker.stats()
    assert stats["uncovered_total"] == 2
    ids = {row["post_id"] for row in stats["uncovered_recent"]}
    assert ids == {"gap", "gap-pushed"}


def test_uncovered_sample_deduped_by_post_id(tmp_path, log):
    tracker = CoverageTracker(tmp_path, log)
    tracker.record(_rec("dup", KIND_SCHEDULE, OUTCOME_SILENT, gap=True))
    tracker.record(_rec("dup", KIND_SCHEDULE, OUTCOME_SILENT, gap=True))
    assert tracker.stats()["uncovered_total"] == 1


def test_uncovered_jsonl_written(tmp_path, log):
    tracker = CoverageTracker(tmp_path, log)
    tracker.record(_rec("sched-fail", KIND_SCHEDULE, OUTCOME_SILENT, gap=True))
    lines = (tmp_path / "uncovered.jsonl").read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["post_id"] == "sched-fail"


def test_gap_flag_survives_reload(tmp_path, log):
    """缺口标记要随统计一起落盘，否则重启后待补样本与覆盖率都不准。"""
    tracker = CoverageTracker(tmp_path, log)
    tracker.record(_rec("gap", KIND_SCHEDULE, OUTCOME_SILENT, gap=True))
    tracker.record(_rec("ok", KIND_SCHEDULE, OUTCOME_TASKED, 1))
    tracker._dump()

    restarted = CoverageTracker(tmp_path, log)
    stats = restarted.stats()
    assert stats["schedule_gaps"] == 1
    assert stats["coverage_percent"] == 50.0


async def test_flush_and_reload_survives_restart(tmp_path, log):
    tracker = CoverageTracker(tmp_path, log)
    tracker.record(_rec("a", KIND_SCHEDULE, OUTCOME_TASKED, 1))
    tracker.record(_rec("b", KIND_SCHEDULE, OUTCOME_SILENT))
    await tracker.flush()

    restarted = CoverageTracker(tmp_path, log)
    stats = restarted.stats()
    assert stats["window"] == 2
    assert stats["by_kind"][KIND_SCHEDULE] == 2


def test_window_is_bounded(tmp_path, log):
    tracker = CoverageTracker(tmp_path, log, window=5)
    for i in range(20):
        tracker.record(_rec(f"p{i}", KIND_SCHEDULE, OUTCOME_TASKED, 1))
    assert tracker.stats()["window"] == 5


def test_tracker_works_without_data_dir(log):
    """不给数据目录时（纯内存模式）也不能报错。"""
    tracker = CoverageTracker(None, log)
    tracker.record(_rec("a", KIND_SCHEDULE, OUTCOME_TASKED, 1))
    assert tracker.stats()["window"] == 1


def test_corrupt_state_file_is_ignored(tmp_path, log):
    (tmp_path / "coverage.json").write_text("not json", encoding="utf-8")
    tracker = CoverageTracker(tmp_path, log)
    assert tracker.stats()["window"] == 0
