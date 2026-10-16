"""调度器测试：多档触发、补发、失败标记、重启恢复。"""

from __future__ import annotations

import time

import pytest

from scheduler import MAKEUP_THRESHOLD, RemindScheduler
from store import STATUS_FAILED, STATUS_SENT, SQLiteStore


class FakePusher:
    """记录推送调用的假推送器。"""

    def __init__(self, ok: bool = True):
        self.calls: list[tuple[object, bool]] = []
        self.ok = ok

    async def send_remind(self, task, *, is_makeup: bool = False) -> bool:
        self.calls.append((task, is_makeup))
        return self.ok


@pytest.fixture
def store(tmp_path):
    s = SQLiteStore(tmp_path / "state.db")
    yield s
    s.close()


@pytest.fixture
def config():
    return {"missed_remind_grace_minutes": 10}


async def test_scan_pushes_due_task_and_marks_sent(store, config, log):
    now = time.time()
    await store.add_task("p1", now - 1, 0, {"weibo": "今晚8点直播", "author": "麦报"})
    pusher = FakePusher()
    sched = RemindScheduler(store, pusher, config, log)

    results = await sched.scan_once(now_ts=now)
    assert len(results) == 1
    assert results[0][1] == STATUS_SENT
    assert pusher.calls[0][1] is False  # 准点，不是补发
    stats = await store.stats()
    assert stats["by_status"].get(STATUS_SENT) == 1


async def test_scan_ignores_future_tasks(store, config, log):
    now = time.time()
    await store.add_task("p1", now + 600, 0, {"weibo": "稍后"})
    pusher = FakePusher()
    sched = RemindScheduler(store, pusher, config, log)

    assert await sched.scan_once(now_ts=now) == []
    assert pusher.calls == []
    assert await store.pending_count() == 1


async def test_push_failure_marks_failed_without_retry(store, config, log):
    """推送失败只标记 failed，不自动重试（避免「实际已送达却判超时」的重复推送）。"""
    now = time.time()
    await store.add_task("p1", now - 1, 0, {"weibo": "x"})
    pusher = FakePusher(ok=False)
    sched = RemindScheduler(store, pusher, config, log)

    results = await sched.scan_once(now_ts=now)
    assert results[0][1] == STATUS_FAILED
    # 后续扫描不再重试这条
    assert await sched.scan_once(now_ts=now + 3600) == []
    assert len(pusher.calls) == 1


async def test_multi_tier_tasks_all_fire_in_order(store, config, log):
    """多档提醒：同一条微博的 60/10/0 三档各推一次，按时间先后。"""
    now = time.time()
    event_ts = now + 3600
    for advance in (60, 10, 0):
        await store.add_task(
            "p1", event_ts - advance * 60, advance, {"weibo": "今晚8点直播", "author": "麦报"}
        )
    pusher = FakePusher()
    sched = RemindScheduler(store, pusher, config, log)

    # 时间推进到事件前一小时：只有提前 60 分钟那档到期
    first = await sched.scan_once(now_ts=event_ts - 3600)
    assert [t.advance_minutes for t, _ in first] == [60]
    second = await sched.scan_once(now_ts=event_ts - 600)
    assert [t.advance_minutes for t, _ in second] == [10]
    third = await sched.scan_once(now_ts=event_ts)
    assert [t.advance_minutes for t, _ in third] == [0]
    assert await store.pending_count() == 0


async def test_overdue_task_within_grace_is_makeup(store, config, log):
    """错过的任务在宽限窗内 → 补发（文案带补发标记）。"""
    now = time.time()
    await store.add_task("p1", now - MAKEUP_THRESHOLD - 60, 0, {"weibo": "x"})
    pusher = FakePusher()
    sched = RemindScheduler(store, pusher, config, log)

    await sched.scan_once(now_ts=now)
    assert pusher.calls[0][1] is True


async def test_recover_resets_sending_and_drops_expired(store, config, log):
    now = time.time()
    # 崩溃残留：已在 sending 状态，且已过期远超宽限
    await store.add_task("stale", now - 3600, 0, {"weibo": "stale"})
    await store.pop_due(now - 3500)  # 置为 sending
    # 宽限内错过：应保持 pending
    await store.add_task("grace", now - 60, 0, {"weibo": "grace"})
    # 未来任务
    await store.add_task("future", now + 600, 0, {"weibo": "future"})

    pusher = FakePusher()
    sched = RemindScheduler(store, pusher, config, log)
    report = await sched.recover_on_startup()

    assert report["recovered"] == 1
    assert report["missed"] == 1
    assert report["pending"] == 2
    pending = {t.post_id for t in await store.list_pending()}
    assert pending == {"grace", "future"}


async def test_zero_grace_drops_all_overdue(store, log):
    now = time.time()
    await store.add_task("p1", now - 1, 0, {"weibo": "x"})
    sched = RemindScheduler(store, FakePusher(), {"missed_remind_grace_minutes": 0}, log)
    report = await sched.recover_on_startup()
    assert report["missed"] == 1
    assert report["pending"] == 0


async def test_run_loop_stops_cleanly(store, config, log):
    """run 循环可被 stop 打断（terminate 时的清理路径）。"""
    import asyncio

    pusher = FakePusher()
    sched = RemindScheduler(store, pusher, config, log)
    task = asyncio.create_task(sched.run())
    await asyncio.sleep(0.05)
    sched.stop()
    await asyncio.wait_for(task, timeout=2.0)
    assert sched._stopped is True
