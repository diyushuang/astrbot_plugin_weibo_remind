"""博文形态分类与覆盖自检（模块名避开 PyPI 上常见的 `coverage` 包）。

两件事：

1. `classify_post()` —— 判断一条微博属于哪种形态，决定「解析不出时间时该怎么办」。
   语料实测（UID 6902546888 的 98 条可见博文）发现一个关键问题：原策略把
   「有没有时间信号」当作是否推送原文的依据，于是【7月建议博】、【5月建议博反馈】、
   「网传活动行程为假」这类**与提醒无关**的博文都会被当作「疑似含时间但解析失败」
   推给群友——17/98 条纯噪音。真正需要的判据是「这条博文是不是排期/公告类」：
   是，则解析失败时推原文请群友确认；不是，则静默。

2. `CoverageTracker` —— 每条博文的处理结果留档并统计，暴露「哪类博文没被覆盖」，
   未覆盖的排期类样本单独落盘，供后续补规则。对应指令「微博提醒 覆盖」。
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

KIND_SCHEDULE = "schedule"  # 排期/物料发布节奏：要建任务，解析不出则推原文
KIND_NOTICE = "notice"  # 声明/打假/澄清：静默
KIND_SUGGESTION = "suggestion"  # 建议博/反馈：静默
KIND_DAILY = "daily"  # 日常/打卡/互动：静默
KIND_EMPTY = "empty"  # 无正文

KIND_LABELS = {
    KIND_SCHEDULE: "排期公告",
    KIND_NOTICE: "声明打假",
    KIND_SUGGESTION: "建议反馈",
    KIND_DAILY: "日常其他",
    KIND_EMPTY: "无正文",
}

# 排期/公告类信号：命中即认定是「值得建提醒、也值得推原文」的博文
_SCHEDULE_WORDS = (
    "发布节奏",
    "发布计划",
    "物料",
    "官宣",
    "预告",
    "海报",
    "花絮",
    "分集",
    "节目单",
    "日程",
    "行程同步",
    "上线",
    "开播",
    "直播",
    "预售",
    "开售",
    "发售",
    "预热",
    "上线时间",
    "播出",
    "更新",
    "定档",
    "杀青",
    "见面会",
    "综艺",
    "采访",
    "CUT",
    "写真",
    "封面",
)
# 声明/打假类：与提醒无关，必须静默（实测这类最容易被误判成「疑似含时间」）
_NOTICE_WORDS = (
    "网传",
    "为假",
    "假的",
    "请勿",
    "谢绝",
    "声明",
    "打假",
    "断章取义",
    "不要相信",
    "谨防",
    "上当",
    "未收到",
    "并无",
    "辟谣",
    "非官方",
)
# 建议博/反馈类：每月例行博，与提醒无关
_SUGGESTION_WORDS = ("建议博", "反馈", "建议均已", "建议", "留言", "私信")
# 明确「时间待定」的表述：排期类博文命中它且没解析出时间时，同样值得推原文
_PENDING_WORDS = ("待定", "另行通知", "另通知", "后续公布", "稍后公布", "以实际为准", "不定时")
# 结构化时间信号：有明确时间安排的博文即使不含排期词，也按排期类对待
# （实测「⏰10月17日 / 10:00 邀约试驾视频」这类只写时间与事件、不含「发布/物料」词）
_TIME_MARKER_RE = re.compile(
    r"[⏰🕐🕙📅🗓]"
    r"|[【\[]\s*\d{3,4}\s*[】\]]"
    r"|\d{1,2}\s*月\s*\d{1,2}\s*[日号]"
    r"|\d{1,2}\s*[:：]\s*\d{2}"
    r"|(?:今晚|明晚|明天|后天)\s*[上下]?午?\s*\d{1,2}\s*[点時时]"
)


@dataclass
class PostKind:
    """博文形态判定结果。"""

    kind: str
    schedule: bool  # 是否按排期类处理（解析不出时间时可推原文）
    evidence: list[str] = field(default_factory=list)

    @property
    def label(self) -> str:
        return KIND_LABELS.get(self.kind, self.kind)


def classify_post(text: str) -> PostKind:
    """判断博文形态。

    优先级：建议反馈 > 声明打假 > 排期公告 > 日常。
    声明类放在排期之前，是因为打假博文里常出现「行程」「通知」等排期词
    （实测「网传活动行程为假，请大家以官方通知为准」），顺序反了就会被误判。
    """
    body = (text or "").strip()
    if not body:
        return PostKind(KIND_EMPTY, False)

    hit_suggestion = [w for w in _SUGGESTION_WORDS if w in body]
    if hit_suggestion:
        return PostKind(KIND_SUGGESTION, False, hit_suggestion)

    hit_notice = [w for w in _NOTICE_WORDS if w in body]
    if hit_notice:
        return PostKind(KIND_NOTICE, False, hit_notice)

    hit_schedule = [w for w in _SCHEDULE_WORDS if w in body]
    if hit_schedule:
        return PostKind(KIND_SCHEDULE, True, hit_schedule)
    if _TIME_MARKER_RE.search(body):
        return PostKind(KIND_SCHEDULE, True, ["结构化时间标记"])

    return PostKind(KIND_DAILY, False, [])


def has_pending_time(text: str) -> bool:
    """正文是否明说「时间待定」——排期类博文命中它时值得推原文提醒群友关注。"""
    return any(w in (text or "") for w in _PENDING_WORDS)


# ---------------- 覆盖自检 ----------------

OUTCOME_TASKED = "tasked"  # 已建提醒任务
OUTCOME_PUSHED = "pushed"  # 未建任务但按策略推送了原文
OUTCOME_PAST = "past"  # 提到时间但已过去/太远，静默
OUTCOME_AMBIGUOUS = "ambiguous"  # 纯数字歧义，静默
OUTCOME_SILENT = "silent"  # 无时间/非排期类，静默

OUTCOME_LABELS = {
    OUTCOME_TASKED: "已建任务",
    OUTCOME_PUSHED: "推原文确认",
    OUTCOME_PAST: "时间已过",
    OUTCOME_AMBIGUOUS: "数字歧义",
    OUTCOME_SILENT: "静默跳过",
}


@dataclass
class PostOutcome:
    """一条博文的处理结果（覆盖自检的最小记录单元）。"""

    post_id: str
    created_at: str
    kind: str
    outcome: str
    tasks_created: int
    reason: str
    excerpt: str
    gap: bool = False  # 是否算「覆盖缺口」（排期类且不是因时间已过而正确跳过）


class CoverageTracker:
    """博文处理结果统计 + 未覆盖样本留档。

    统计在内存里滚动保留最近 window 条，并可落盘（覆盖自检要跨重启看趋势）；
    未覆盖的**排期类**样本单独追加到 uncovered.jsonl，方便后续照样本补解析规则。
    """

    def __init__(
        self,
        data_dir: Path | None,
        log: Any,
        window: int = 300,
        uncovered_cap: int = 200,
    ):
        self.log = log
        self.window = window
        self.uncovered_cap = uncovered_cap
        self.records: deque[PostOutcome] = deque(maxlen=window)
        self._uncovered: deque[dict[str, Any]] = deque(maxlen=uncovered_cap)
        self._seen_uncovered: set[str] = set()
        self.data_dir = Path(data_dir) if data_dir else None
        self._state_file = self.data_dir / "coverage.json" if self.data_dir else None
        self._uncovered_file = self.data_dir / "uncovered.jsonl" if self.data_dir else None
        self._lock = asyncio.Lock()
        self._dirty = False
        self.load()

    # ---------------- 记录 ----------------

    def record(self, outcome: PostOutcome) -> None:
        """记一条处理结果（同步，调用点已在异步流程里，落盘走 flush）。

        只有真正的**覆盖缺口**才进未覆盖样本池：排期类且没建出任务、
        又不是「识别到的时间都已过去/太远」这种正确跳过的情况。
        否则停机几天后补看历史博文时，满屏「时间已过」会把待补样本刷爆。
        """
        self.records.append(outcome)
        self._dirty = True
        if outcome.gap:
            self._remember_uncovered(outcome)

    def _remember_uncovered(self, outcome: PostOutcome) -> None:
        """留档「排期类但没建出任务」的样本（去重，滚动覆盖）。"""
        if outcome.post_id in self._seen_uncovered:
            return
        self._seen_uncovered.add(outcome.post_id)
        self._uncovered.append(
            {
                "post_id": outcome.post_id,
                "created_at": outcome.created_at,
                "outcome": outcome.outcome,
                "reason": outcome.reason,
                "excerpt": outcome.excerpt,
            }
        )
        if self._uncovered_file is not None:
            try:
                self._uncovered_file.parent.mkdir(parents=True, exist_ok=True)
                with self._uncovered_file.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(self._uncovered[-1], ensure_ascii=False) + "\n")
            except OSError as e:  # pragma: no cover
                self.log.warning(f"未覆盖样本落盘失败: {e}")

    # ---------------- 统计 ----------------

    def stats(self) -> dict[str, Any]:
        """滚动窗口内的分布与覆盖率。

        覆盖率口径 = 排期公告类中「建出提醒」/「建出提醒 + 真正待补」。
        「识别到的时间都已过去」属于正确跳过，既不算覆盖也不算缺口——否则
        首轮补看历史博文时覆盖率会显示成 0%，看不出真实水平。
        """
        total = len(self.records)
        by_kind: dict[str, int] = {}
        by_outcome: dict[str, int] = {}
        for rec in self.records:
            by_kind[rec.kind] = by_kind.get(rec.kind, 0) + 1
            by_outcome[rec.outcome] = by_outcome.get(rec.outcome, 0) + 1
        schedule_total = by_kind.get(KIND_SCHEDULE, 0)
        schedule_tasked = sum(
            1 for r in self.records if r.kind == KIND_SCHEDULE and r.tasks_created > 0
        )
        schedule_gaps = sum(1 for r in self.records if r.gap)
        schedule_stale = sum(
            1
            for r in self.records
            if r.kind == KIND_SCHEDULE and r.outcome == OUTCOME_PAST and not r.gap
        )
        denom = schedule_tasked + schedule_gaps
        coverage = (schedule_tasked / denom * 100) if denom else 0.0
        return {
            "window": total,
            "by_kind": by_kind,
            "by_outcome": by_outcome,
            "schedule_total": schedule_total,
            "schedule_tasked": schedule_tasked,
            "schedule_stale_skipped": schedule_stale,
            "schedule_gaps": schedule_gaps,
            "coverage_percent": round(coverage, 1),
            "uncovered_total": len(self._uncovered),
            "uncovered_recent": list(self._uncovered)[-5:],
        }

    # ---------------- 持久化 ----------------

    def _load(self) -> None:
        if self._state_file is None or not self._state_file.is_file():
            return
        try:
            payload = json.loads(self._state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            self.log.info(f"覆盖统计读取失败（忽略）: {e}")
            return
        rows = payload.get("records") or []
        for row in rows[-self.window :]:
            if not isinstance(row, dict):
                continue
            try:
                self.records.append(
                    PostOutcome(
                        post_id=str(row.get("post_id") or ""),
                        created_at=str(row.get("created_at") or ""),
                        kind=str(row.get("kind") or ""),
                        outcome=str(row.get("outcome") or ""),
                        tasks_created=int(row.get("tasks_created") or 0),
                        reason=str(row.get("reason") or ""),
                        excerpt=str(row.get("excerpt") or ""),
                        gap=bool(row.get("gap")),
                    )
                )
                self._seen_uncovered.add(str(row.get("post_id") or ""))
            except (TypeError, ValueError):
                continue
        for row in payload.get("uncovered") or []:
            if isinstance(row, dict):
                self._uncovered.append(row)

    def load(self) -> None:
        self._load()

    def _dump(self) -> None:
        if self._state_file is None or not self._dirty:
            return
        payload = {
            "saved_at": time.time(),
            "records": [r.__dict__ for r in self.records],
            "uncovered": list(self._uncovered),
        }
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            self._state_file.write_text(
                json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
            )
            self._dirty = False
        except OSError as e:  # pragma: no cover
            self.log.warning(f"覆盖统计落盘失败: {e}")

    async def flush(self) -> None:
        """把统计落盘（由轮询循环在每轮结束后调用）。"""
        async with self._lock:
            await asyncio.to_thread(self._dump)
