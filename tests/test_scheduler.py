"""调度器测试：多档触发、补发、失败标记、重启恢复。"""

from __future__ import annotations

import time

import pytest

from scheduler import MAKEUP_THRESHOLD, STATUS_DEFERRED, RemindScheduler
from store import STATUS_FAILED, STATUS_PENDING, STATUS_SENT, SQLiteStore


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


# ---------------- 防打扰 / 防风控（v0.4.1）----------------


async def test_quiet_hours_defers_due_task(store, log):
    """免打扰时段内到点的提醒 → 不推送，顺延到时段结束，状态回 pending。"""
    from conftest import ts

    config = {"missed_remind_grace_minutes": 10, "quiet_hours": "23:00-08:00"}
    now = ts(2026, 10, 17, 23, 30)
    await store.add_task("p1", now - 1, 0, {"weibo": "x", "author": "麦报"})
    pusher = FakePusher()
    sched = RemindScheduler(store, pusher, config, log)

    results = await sched.scan_once(now_ts=now)
    assert results[0][1] == STATUS_DEFERRED
    assert pusher.calls == [], "免打扰时段内不该真的发消息"
    tasks = await store.list_pending()
    assert len(tasks) == 1, "顺延后仍是待触发，不能丢"
    assert tasks[0].trigger_at == ts(2026, 10, 18, 8, 0)
    assert tasks[0].snapshot.get("deferred") is True, "顺延标记要落库，供文案标注"

    # 时段结束后的下一轮扫描：正常推送
    results2 = await sched.scan_once(now_ts=ts(2026, 10, 18, 8, 1))
    assert results2[0][1] == STATUS_SENT
    assert len(pusher.calls) == 1


async def test_outside_quiet_hours_pushes_normally(store, log):
    from conftest import ts

    config = {"quiet_hours": "23:00-08:00"}
    now = ts(2026, 10, 17, 20, 0)
    await store.add_task("p1", now - 1, 0, {"weibo": "x"})
    pusher = FakePusher()
    sched = RemindScheduler(store, pusher, config, log)

    results = await sched.scan_once(now_ts=now)
    assert results[0][1] == STATUS_SENT
    assert len(pusher.calls) == 1


async def test_invalid_quiet_hours_is_ignored(store, log):
    """配置写错不该让调度失效：按未启用处理，照常推送。"""
    config = {"quiet_hours": "晚上11点到早上8点"}
    await store.add_task("p1", time.time() - 1, 0, {"weibo": "x"})
    pusher = FakePusher()
    sched = RemindScheduler(store, pusher, config, log)

    assert sched._quiet is None
    results = await sched.scan_once()
    assert results[0][1] == STATUS_SENT


async def test_jitter_and_interval_sleeps(store, log, monkeypatch):
    """抖动每条一次、批内间隔从第二条起生效（防止同秒连发被平台风控）。"""
    config = {"push_jitter_seconds": 10, "push_interval_seconds": 3}
    sleeps: list[float] = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr("scheduler.asyncio.sleep", fake_sleep)
    now = time.time()
    for i in range(3):
        await store.add_task(f"p{i}", now - 1, 0, {"weibo": f"w{i}"})
    sched = RemindScheduler(store, FakePusher(), config, log)

    await sched.scan_once(now_ts=now)
    assert len(sleeps) == 5, "3 条任务 = 3 次抖动 + 2 次间隔"
    assert sleeps.count(3) == 2
    assert all(0 <= s <= 10 for s in sleeps if s != 3)


async def test_no_sleeps_when_disabled(store, log, monkeypatch):
    """两项都关闭（默认）时不应引入任何等待。"""
    sleeps: list[float] = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr("scheduler.asyncio.sleep", fake_sleep)
    now = time.time()
    await store.add_task("p1", now - 1, 0, {"weibo": "x"})
    sched = RemindScheduler(store, FakePusher(), {"quiet_hours": ""}, log)

    await sched.scan_once(now_ts=now)
    assert sleeps == []


async def test_deferred_task_is_not_marked_missed_on_restart(store, log):
    """顺延到未来的任务不应被重启恢复当成「超期错过」作废。

    免打扰时段按「当前时刻」动态构造，避免用例绑死某一天而随日期失效。
    """
    from datetime import datetime

    from conftest import CN

    now = time.time()
    clock = datetime.fromtimestamp(now, tz=CN)
    spec = f"{(clock.hour - 1) % 24:02d}:00-{(clock.hour + 2) % 24:02d}:00"
    config = {"missed_remind_grace_minutes": 10, "quiet_hours": spec}
    await store.add_task("p1", now - 1, 0, {"weibo": "x"})
    sched = RemindScheduler(store, FakePusher(), config, log)

    results = await sched.scan_once(now_ts=now)
    assert results[0][1] == STATUS_DEFERRED

    report = await RemindScheduler(store, FakePusher(), config, log).recover_on_startup()
    assert report["missed"] == 0
    assert await store.pending_count() == 1
    assert (await store.list_pending())[0].status == STATUS_PENDING


# ---------------- 周期续期（v0.5.1）----------------


def _rec_snapshot(now: float) -> dict:
    return {
        "weibo": "每周五晚8点直播",
        "author": "麦报",
        "source_text": "每周五 20:00",
        "event_ts": now,
        "recurrence": {"kind": "weekly", "hour": 20, "minute": 0, "weekday": 4},
    }


async def test_recurring_task_renews_next_period(store, log):
    """到点档发送成功后，按周期续建下一期的全套档位。"""
    now = time.time()
    await store.add_task("p1", now - 1, 0, _rec_snapshot(now))
    config = {"remind_advance_minutes": [60, 10, 0], "max_future_days": 60}
    sched = RemindScheduler(store, FakePusher(), config, log)

    results = await sched.scan_once(now_ts=now)
    assert results[0][1] == STATUS_SENT

    tasks = await store.list_pending()
    assert len(tasks) == 3, "下一期应建出三档"
    assert all(t.snapshot["event_ts"] == pytest.approx(now + 7 * 86400) for t in tasks)
    assert [t.advance_minutes for t in tasks] == [60, 10, 0]


async def test_only_zero_tier_renews(store, log):
    """非到点档不续期，避免同一条微博被续多次。"""
    now = time.time()
    snapshot = {**_rec_snapshot(now + 3600), "event_ts": now + 3600}
    await store.add_task("p1", now - 1, 60, snapshot)
    sched = RemindScheduler(store, FakePusher(), {"remind_advance_minutes": [60, 10, 0]}, log)

    await sched.scan_once(now_ts=now)
    assert await store.pending_count() == 0


async def test_renew_respects_switch_and_horizon(store, log):
    now = time.time()
    await store.add_task("p1", now - 1, 0, _rec_snapshot(now))

    # 开关关闭 → 不续
    sched = RemindScheduler(
        store, FakePusher(), {"recurrence_renew": False, "remind_advance_minutes": [0]}, log
    )
    await sched.scan_once(now_ts=now)
    assert await store.pending_count() == 0

    # 超出 max_future_days → 不续
    await store.add_task("p2", now - 1, 0, _rec_snapshot(now))
    sched2 = RemindScheduler(
        store,
        FakePusher(),
        {"max_future_days": 3, "remind_advance_minutes": [0]},
        log,
    )
    await sched2.scan_once(now_ts=now)
    assert await store.pending_count() == 0


async def test_renew_is_idempotent(store, log):
    """重复扫描不会把下一期建成多份（UNIQUE 兜底）。"""
    now = time.time()
    await store.add_task("p1", now - 1, 0, _rec_snapshot(now))
    sched = RemindScheduler(store, FakePusher(), {"remind_advance_minutes": [0]}, log)
    await sched.scan_once(now_ts=now)

    # 手动再触发一次续期（模拟重扫/重启后的重复调用）
    task = (await store.list_pending())[0]
    task.snapshot = _rec_snapshot(now)
    await sched.renew_recurring(task)
    assert await store.pending_count() == 1
