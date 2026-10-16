"""定时任务的 SQLite 持久化。

两张表：
- seen_posts：已处理过的微博 id（去重，判「新微博」的唯一依据）
- remind_tasks：待触发/已触发的提醒任务，UNIQUE(post_id, trigger_at, advance_minutes)
  保证同一条微博的同一档提醒不会重复建、重复推

所有对外方法都是 async（内部 sqlite3 是同步阻塞调用，统一用 asyncio.to_thread
挪到线程池，避免卡住 AstrBot 事件循环）；连接单例 + 线程锁串行，配合
check_same_thread=False。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:  # 与主流程共用同一时区口径
    from .constants import TZ_CN
except ImportError:  # 独立脚本 / 测试环境
    from constants import TZ_CN  # type: ignore

STATUS_PENDING = "pending"
STATUS_SENDING = "sending"
STATUS_SENT = "sent"
STATUS_SKIPPED = "skipped"
STATUS_MISSED = "missed"
STATUS_FAILED = "failed"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_posts (
    post_id       TEXT PRIMARY KEY,
    created_ts    REAL NOT NULL DEFAULT 0,
    first_seen_ts REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS remind_tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id         TEXT NOT NULL,
    trigger_at      REAL NOT NULL,
    advance_minutes INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_ts      REAL NOT NULL,
    sent_ts         REAL,
    snapshot        TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_task
    ON remind_tasks (post_id, trigger_at, advance_minutes);
CREATE INDEX IF NOT EXISTS idx_task_due
    ON remind_tasks (status, trigger_at);
CREATE TABLE IF NOT EXISTS at_all_log (
    day   TEXT PRIMARY KEY,
    count INTEGER NOT NULL DEFAULT 0
);
"""


@dataclass
class RemindTask:
    """一条待触发的提醒任务。"""

    id: int
    post_id: str
    trigger_at: float
    advance_minutes: int
    status: str
    created_ts: float
    snapshot: dict[str, Any]
    sent_ts: float | None = None

    @property
    def author(self) -> str:
        return str(self.snapshot.get("author") or "")

    @property
    def event_ts(self) -> float:
        """事件本身的时间。

        优先用建任务时存进快照的 event_ts；老数据没有该字段时用
        「触发时间 + 提前分钟」反推（正常写入路径下两者等价）。
        """
        raw = self.snapshot.get("event_ts")
        try:
            value = float(raw)
        except (TypeError, ValueError):
            value = 0.0
        return value if value > 0 else self.trigger_at + self.advance_minutes * 60

    @property
    def source_text(self) -> str:
        return str(self.snapshot.get("source_text") or "")

    @property
    def weibo(self) -> str:
        return str(self.snapshot.get("weibo") or "")

    @property
    def link(self) -> str:
        return str(self.snapshot.get("link") or "")


def _row_to_task(row: sqlite3.Row) -> RemindTask:
    try:
        snapshot = json.loads(row["snapshot"]) or {}
    except (json.JSONDecodeError, TypeError):
        snapshot = {}
    if not isinstance(snapshot, dict):
        snapshot = {}
    return RemindTask(
        id=int(row["id"]),
        post_id=str(row["post_id"]),
        trigger_at=float(row["trigger_at"]),
        advance_minutes=int(row["advance_minutes"]),
        status=str(row["status"]),
        created_ts=float(row["created_ts"]),
        sent_ts=(float(row["sent_ts"]) if row["sent_ts"] is not None else None),
        snapshot=snapshot,
    )


class SQLiteStore:
    """任务与去重记录的持久化。"""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    # ---------------- 生命周期 ----------------

    def close(self) -> None:
        with self._lock, contextlib.suppress(sqlite3.Error):
            self._conn.close()

    # ---------------- 已见微博 ----------------

    def _is_seen(self, post_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM seen_posts WHERE post_id = ? LIMIT 1", (str(post_id),)
            ).fetchone()
        return row is not None

    async def is_seen(self, post_id: str) -> bool:
        return await asyncio.to_thread(self._is_seen, post_id)

    def _mark_seen(self, post_ids: list[str], created_map: dict[str, float]) -> None:
        now = time.time()
        rows = [(pid, float(created_map.get(pid, 0.0)), now) for pid in post_ids]
        if not rows:
            return
        with self._lock, self._conn:
            self._conn.executemany(
                "INSERT OR IGNORE INTO seen_posts (post_id, created_ts, first_seen_ts) "
                "VALUES (?, ?, ?)",
                rows,
            )

    async def mark_seen(self, post_ids: list[str], created_map: dict[str, float] | None = None):
        await asyncio.to_thread(self._mark_seen, list(post_ids), dict(created_map or {}))

    def _known_ids(self, limit: int) -> set[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT post_id FROM seen_posts ORDER BY first_seen_ts DESC LIMIT ?", (int(limit),)
            ).fetchall()
        return {str(r["post_id"]) for r in rows}

    async def known_ids(self, limit: int = 1000) -> set[str]:
        """最近见过的微博 id 集合（供抓取层判断翻页边界）。"""
        return await asyncio.to_thread(self._known_ids, limit)

    def _seen_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM seen_posts").fetchone()
        return int(row["n"]) if row else 0

    async def seen_count(self) -> int:
        return await asyncio.to_thread(self._seen_count)

    def _prune_seen(self, keep: int) -> int:
        with self._lock, self._conn:
            cur = self._conn.execute(
                "DELETE FROM seen_posts WHERE post_id NOT IN ("
                "  SELECT post_id FROM seen_posts ORDER BY first_seen_ts DESC LIMIT ?"
                ")",
                (int(keep),),
            )
        return cur.rowcount or 0

    async def prune_seen(self, keep: int = 5000) -> int:
        """已见记录只留最近 keep 条（防无限增长；微博 id 不会复用）。"""
        return await asyncio.to_thread(self._prune_seen, keep)

    # ---------------- 任务 ----------------

    def _add_task(
        self, post_id: str, trigger_at: float, advance_minutes: int, snapshot: dict[str, Any]
    ) -> bool:
        try:
            payload = json.dumps(snapshot, ensure_ascii=False)
        except (TypeError, ValueError):  # pragma: no cover - snapshot 由内部构造
            payload = "{}"
        with self._lock, self._conn:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO remind_tasks "
                "(post_id, trigger_at, advance_minutes, status, created_ts, snapshot) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    str(post_id),
                    float(trigger_at),
                    int(advance_minutes),
                    STATUS_PENDING,
                    time.time(),
                    payload,
                ),
            )
        return bool(cur.rowcount)

    async def add_task(
        self, post_id: str, trigger_at: float, advance_minutes: int, snapshot: dict[str, Any]
    ) -> bool:
        """建任务；同（微博, 触发时间, 档位）已存在时返回 False。"""
        return await asyncio.to_thread(
            self._add_task, post_id, trigger_at, advance_minutes, snapshot
        )

    def _pop_due(self, now_ts: float, limit: int) -> list[RemindTask]:
        with self._lock, self._conn:
            rows = self._conn.execute(
                "SELECT * FROM remind_tasks WHERE status = ? AND trigger_at <= ? "
                "ORDER BY trigger_at LIMIT ?",
                (STATUS_PENDING, float(now_ts), int(limit)),
            ).fetchall()
            if rows:
                self._conn.executemany(
                    "UPDATE remind_tasks SET status = ? WHERE id = ?",
                    [(STATUS_SENDING, int(r["id"])) for r in rows],
                )
        return [_row_to_task(r) for r in rows]

    async def pop_due(self, now_ts: float, limit: int = 10) -> list[RemindTask]:
        """原子取出到期任务并置为 sending（同一任务不会被取两次）。"""
        return await asyncio.to_thread(self._pop_due, now_ts, limit)

    def _tasks_by_status(self, status: str) -> list[RemindTask]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM remind_tasks WHERE status = ? ORDER BY trigger_at",
                (status,),
            ).fetchall()
        return [_row_to_task(r) for r in rows]

    async def tasks_by_status(self, status: str) -> list[RemindTask]:
        return await asyncio.to_thread(self._tasks_by_status, status)

    def _mark(self, task_id: int, status: str) -> None:
        sent_ts = time.time() if status == STATUS_SENT else None
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE remind_tasks SET status = ?, sent_ts = COALESCE(?, sent_ts) WHERE id = ?",
                (status, sent_ts, int(task_id)),
            )

    async def mark(self, task_id: int, status: str) -> None:
        """更新任务状态（sent / skipped / missed / failed）。"""
        await asyncio.to_thread(self._mark, task_id, status)

    def _pending(self, limit: int) -> list[RemindTask]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM remind_tasks WHERE status IN (?, ?) ORDER BY trigger_at LIMIT ?",
                (STATUS_PENDING, STATUS_SENDING, int(limit)),
            ).fetchall()
        return [_row_to_task(r) for r in rows]

    async def list_pending(self, limit: int = 20) -> list[RemindTask]:
        return await asyncio.to_thread(self._pending, limit)

    def _pending_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM remind_tasks WHERE status IN (?, ?)",
                (STATUS_PENDING, STATUS_SENDING),
            ).fetchone()
        return int(row["n"]) if row else 0

    async def pending_count(self) -> int:
        return await asyncio.to_thread(self._pending_count)

    def _purge_finished(self, keep_days: float) -> int:
        cutoff = time.time() - float(keep_days) * 86400
        with self._lock, self._conn:
            cur = self._conn.execute(
                "DELETE FROM remind_tasks WHERE status IN (?, ?, ?, ?) AND trigger_at < ?",
                (STATUS_SENT, STATUS_SKIPPED, STATUS_MISSED, STATUS_FAILED, cutoff),
            )
        return cur.rowcount or 0

    async def purge_finished(self, keep_days: float = 30) -> int:
        """清理历史任务（默认保留 30 天），防表无限增长。"""
        return await asyncio.to_thread(self._purge_finished, keep_days)

    # ---------------- @全体成员计数 ----------------

    def _at_all_today(self, day: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT count FROM at_all_log WHERE day = ?", (day,)
            ).fetchone()
        return int(row["count"]) if row else 0

    async def at_all_today(self, day: str | None = None) -> int:
        """今天已 @全体 的次数（day 为 YYYY-MM-DD，缺省按上海时区的今天）。"""
        key = day or datetime.now(tz=TZ_CN).strftime("%Y-%m-%d")
        return await asyncio.to_thread(self._at_all_today, key)

    def _bump_at_all(self, day: str, keep_days: int) -> int:
        cutoff = (datetime.now(tz=TZ_CN) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO at_all_log (day, count) VALUES (?, 1) "
                "ON CONFLICT(day) DO UPDATE SET count = count + 1",
                (day,),
            )
            row = self._conn.execute(
                "SELECT count FROM at_all_log WHERE day = ?", (day,)
            ).fetchone()
            # 顺手清理过期计数，防表无限增长
            self._conn.execute("DELETE FROM at_all_log WHERE day < ?", (cutoff,))
        return int(row["count"]) if row else 1

    async def bump_at_all(self, day: str | None = None, keep_days: int = 30) -> int:
        """记一次「已 @全体」，返回今天累计次数（用于每日限额判定）。"""
        key = day or datetime.now(tz=TZ_CN).strftime("%Y-%m-%d")
        return await asyncio.to_thread(self._bump_at_all, key, keep_days)

    # ---------------- 状态汇报 ----------------

    def _stats(self) -> dict[str, Any]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM remind_tasks GROUP BY status"
            ).fetchall()
            nxt = self._conn.execute(
                "SELECT MIN(trigger_at) AS t FROM remind_tasks WHERE status IN (?, ?)",
                (STATUS_PENDING, STATUS_SENDING),
            ).fetchone()
            seen = self._conn.execute("SELECT COUNT(*) AS n FROM seen_posts").fetchone()
            last = self._conn.execute(
                "SELECT MAX(sent_ts) AS t FROM remind_tasks WHERE status = ?", (STATUS_SENT,)
            ).fetchone()
        return {
            "by_status": {str(r["status"]): int(r["n"]) for r in rows},
            "next_trigger_at": (float(nxt["t"]) if nxt and nxt["t"] is not None else None),
            "seen_count": int(seen["n"]) if seen else 0,
            "last_sent_ts": (float(last["t"]) if last and last["t"] is not None else None),
        }

    async def stats(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._stats)
