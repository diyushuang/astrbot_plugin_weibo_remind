"""SQLite 持久化测试。"""

from __future__ import annotations

import time

import pytest

from store import STATUS_FAILED, STATUS_MISSED, STATUS_PENDING, STATUS_SENT, SQLiteStore


@pytest.fixture
def store(tmp_path):
    s = SQLiteStore(tmp_path / "state.db")
    yield s
    s.close()


async def test_seen_roundtrip(store):
    assert await store.is_seen("123") is False
    await store.mark_seen(["123", "456"], {"123": 1.0, "456": 2.0})
    assert await store.is_seen("123") is True
    assert await store.known_ids() == {"123", "456"}
    assert await store.seen_count() == 2
    # 重复标记不报错、不重复计数
    await store.mark_seen(["123"], {"123": 1.0})
    assert await store.seen_count() == 2


async def test_add_task_dedupes_on_same_post_time_advance(store):
    snapshot = {"weibo": "今晚8点直播", "link": "https://weibo.com/1/abc"}
    assert await store.add_task("post1", 1000.0, 60, snapshot) is True
    assert await store.add_task("post1", 1000.0, 60, snapshot) is False
    # 同一微博的其它档位是独立任务
    assert await store.add_task("post1", 1000.0, 0, snapshot) is True
    # 不同微博同时间也是独立任务
    assert await store.add_task("post2", 1000.0, 60, snapshot) is True
    assert await store.pending_count() == 3


async def test_pop_due_is_atomic(store):
    now = time.time()
    await store.add_task("p1", now - 10, 0, {"weibo": "a"})
    await store.add_task("p2", now + 3600, 0, {"weibo": "b"})

    first = await store.pop_due(now, limit=10)
    assert [t.post_id for t in first] == ["p1"]
    # 已取走的任务不会再次被取出
    assert await store.pop_due(now, limit=10) == []


async def test_pop_due_respects_limit_and_order(store):
    now = time.time()
    for i in range(5):
        await store.add_task(f"p{i}", now - (10 - i), 0, {"weibo": f"w{i}"})
    due = await store.pop_due(now, limit=3)
    assert [t.post_id for t in due] == ["p0", "p1", "p2"]


async def test_mark_status_and_stats(store):
    now = time.time()
    await store.add_task("p1", now - 1, 0, {"weibo": "a"})
    tasks = await store.pop_due(now)
    await store.mark(tasks[0].id, STATUS_SENT)

    stats = await store.stats()
    assert stats["by_status"].get(STATUS_SENT) == 1
    assert stats["last_sent_ts"] is not None
    assert stats["next_trigger_at"] is None
    assert await store.pending_count() == 0


async def test_list_pending_includes_sending(store):
    """重启恢复要看得到 sending 残留（崩溃时留在半路的任务）。"""
    now = time.time()
    await store.add_task("p1", now + 60, 0, {"weibo": "a"})
    await store.add_task("p2", now + 120, 0, {"weibo": "b"})
    popped = await store.pop_due(now + 3600)  # 两条都到期并被置为 sending
    assert len(popped) == 2
    pending = await store.list_pending()
    assert {t.post_id for t in pending} == {"p1", "p2"}
    assert all(t.status == "sending" for t in pending)


async def test_snapshot_persists_across_reopen(tmp_path):
    db = tmp_path / "state.db"
    s1 = SQLiteStore(db)
    await s1.add_task("p1", 5000.0, 10, {"weibo": "正文", "link": "https://x", "author": "麦报"})
    s1.close()

    s2 = SQLiteStore(db)
    try:
        tasks = await s2.list_pending()
        assert len(tasks) == 1
        assert tasks[0].weibo == "正文"
        assert tasks[0].author == "麦报"
        assert tasks[0].link == "https://x"
        assert tasks[0].advance_minutes == 10
        assert tasks[0].status == STATUS_PENDING
    finally:
        s2.close()


async def test_purge_finished_keeps_recent(store):
    now = time.time()
    await store.add_task("old", now - 100 * 86400, 0, {"weibo": "old"})
    await store.add_task("new", now - 1 * 86400, 0, {"weibo": "new"})
    for task in await store.pop_due(now + 1, limit=10):
        await store.mark(task.id, STATUS_MISSED)
    removed = await store.purge_finished(keep_days=30)
    assert removed == 1
    assert await store.pending_count() == 0


async def test_prune_seen_keeps_latest(store):
    for i in range(10):
        await store.mark_seen([f"p{i}"])
    await store.prune_seen(keep=3)
    assert await store.seen_count() == 3


async def test_broken_snapshot_does_not_break_read(tmp_path):
    db = tmp_path / "state.db"
    s = SQLiteStore(db)
    try:
        await s.add_task("p1", 5000.0, 0, {"weibo": "x"})
        # 直接写坏快照，读取时应回退为空 dict 而非抛异常
        with s._lock, s._conn:
            s._conn.execute("UPDATE remind_tasks SET snapshot = 'not-json'")
        tasks = await s.list_pending()
        assert tasks[0].snapshot == {}
    finally:
        s.close()


async def test_failed_status_recorded(store):
    now = time.time()
    await store.add_task("p1", now - 1, 0, {"weibo": "a"})
    task = (await store.pop_due(now))[0]
    await store.mark(task.id, STATUS_FAILED)
    stats = await store.stats()
    assert stats["by_status"].get(STATUS_FAILED) == 1


async def test_event_ts_prefers_snapshot_then_falls_back(store):
    """事件时间优先取快照里的 event_ts，老数据回退到「触发时间 + 提前分钟」。"""
    await store.add_task("p1", 1000.0, 60, {"weibo": "x", "event_ts": 5000.0})
    await store.add_task("p2", 1000.0, 60, {"weibo": "y"})
    tasks = {t.post_id: t for t in await store.list_pending()}
    assert tasks["p1"].event_ts == 5000.0
    assert tasks["p2"].event_ts == 1000.0 + 60 * 60

    await store.add_task("p3", 1000.0, 0, {"weibo": "z", "event_ts": "坏数据"})
    tasks = {t.post_id: t for t in await store.list_pending()}
    assert tasks["p3"].event_ts == 1000.0


# ---------------- @全体成员每日计数 ----------------


async def test_at_all_counter_starts_at_zero(store):
    assert await store.at_all_today("2026-10-16") == 0


async def test_at_all_counter_bumps_and_persists(store):
    assert await store.bump_at_all("2026-10-16") == 1
    assert await store.bump_at_all("2026-10-16") == 2
    assert await store.at_all_today("2026-10-16") == 2
    # 换一天重新计数（按自然日重置）
    assert await store.at_all_today("2026-10-17") == 0
    assert await store.bump_at_all("2026-10-17") == 1


async def test_at_all_counter_survives_reopen(tmp_path):
    db = tmp_path / "state.db"
    s1 = SQLiteStore(db)
    await s1.bump_at_all("2026-10-16")
    await s1.bump_at_all("2026-10-16")
    s1.close()

    s2 = SQLiteStore(db)
    try:
        assert await s2.at_all_today("2026-10-16") == 2
    finally:
        s2.close()


async def test_at_all_counter_prunes_old_days(store):
    assert await store.bump_at_all("2020-01-01", keep_days=30) == 1
    # 老记录被顺手清理，新的一天从 0 开始
    assert await store.at_all_today("2020-01-01") == 0
