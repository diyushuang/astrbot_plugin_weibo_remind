"""到点任务的调度与重启恢复。

不用 APScheduler：任务本体就是 sqlite 里的一行记录，扫描循环每 20 秒取一次
到期任务即可，状态流转（pending → sending → sent/failed）与库里的记录天然一致；
APScheduler 的 jobstore 以 pickle 存 callable，插件重载后 job 引用旧实例会失效，
重启恢复反而要做自定义 jobstore，等于绕一圈回到自研。
"""

from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime
from typing import Any

try:
    from .constants import SCHEDULER_SCAN_INTERVAL, TZ_CN
    from .quiet_hours import QuietHours, parse_quiet_hours
    from .store import STATUS_FAILED, STATUS_MISSED, STATUS_PENDING, STATUS_SENDING, STATUS_SENT
except ImportError:  # 独立脚本 / 测试环境
    from constants import SCHEDULER_SCAN_INTERVAL, TZ_CN  # type: ignore
    from quiet_hours import QuietHours, parse_quiet_hours  # type: ignore
    from store import (  # type: ignore
        STATUS_FAILED,
        STATUS_MISSED,
        STATUS_PENDING,
        STATUS_SENDING,
        STATUS_SENT,
    )

# 超过这个秒数才推送的任务视为「补发」（AstrBot 重启或长时间卡顿导致错过）
MAKEUP_THRESHOLD = max(120.0, SCHEDULER_SCAN_INTERVAL * 3)
SCAN_BATCH_LIMIT = 20
# 免打扰顺延的任务在 scan_once 返回值里的状态（不入库，库里仍是 pending）
STATUS_DEFERRED = "deferred"


class RemindScheduler:
    """扫描到期任务并推送。"""

    def __init__(
        self,
        store: Any,
        pusher: Any,
        config: dict[str, Any],
        log: Any,
        is_paused: Any = None,
    ):
        self.store = store
        self.pusher = pusher
        self.config = config
        self.log = log
        self._is_paused = is_paused
        self._stop_event = asyncio.Event()
        self._stopped = False
        self._quiet: QuietHours | None = self._load_quiet()

    # ---------------- 配置 ----------------

    def _load_quiet(self) -> QuietHours | None:
        raw = self.config.get("quiet_hours")
        quiet = parse_quiet_hours(raw)
        if str(raw or "").strip() and quiet is None:
            self.log.warning(f"quiet_hours 配置非法（{raw!r}），应为 HH:MM-HH:MM，免打扰时段未启用")
        return quiet

    def _int_cfg(self, key: str, default: int = 0) -> int:
        try:
            return max(0, int(self.config.get(key) or default))
        except (TypeError, ValueError):
            return default

    def jitter_seconds(self) -> int:
        """到点推送的随机抖动上限（秒）——同秒齐发容易被 QQ 判成刷屏。"""
        return self._int_cfg("push_jitter_seconds", 0)

    def interval_seconds(self) -> int:
        """同一批到期任务之间的发送间隔（秒）。"""
        return self._int_cfg("push_interval_seconds", 0)

    # ---------------- 重启恢复 ----------------

    async def recover_on_startup(self) -> dict[str, int]:
        """启动时整理历史任务：sending 残留复位、超期任务标记 missed。

        宽限窗内（missed_remind_grace_minutes）错过的提醒保持 pending，
        由扫描循环按「补发」文案推出去；超过宽限的标记 missed 不再推送。
        """
        try:
            grace_minutes = max(0, int(self.config.get("missed_remind_grace_minutes") or 0))
        except (TypeError, ValueError):
            grace_minutes = 10
        grace_seconds = grace_minutes * 60
        now = time.time()

        recovered = 0
        missed = 0
        pending = 0
        for task in await self.store.list_pending(limit=1000):
            if task.status == STATUS_SENDING:
                # 上次运行在推送中途崩溃/被 kill：复位为 pending 重新评估
                await self.store.mark(task.id, STATUS_PENDING)
                recovered += 1
            if task.trigger_at < now - grace_seconds:
                await self.store.mark(task.id, STATUS_MISSED)
                missed += 1
            else:
                pending += 1
        if recovered or missed:
            self.log.info(
                f"启动恢复：复位 {recovered} 条中断任务，{missed} 条超期标记为 missed，"
                f"{pending} 条待触发"
            )
        return {"recovered": recovered, "missed": missed, "pending": pending}

    # ---------------- 扫描与推送 ----------------

    async def scan_once(self, now_ts: float | None = None, limit: int = SCAN_BATCH_LIMIT):
        """取一批到期任务并推送，返回 [(任务, 结果状态), ...]。

        顺序上先判免打扰：时段内的到期任务**不顺延到点直接发**，而是把
        trigger_at 改成时段结束时刻、状态放回 pending（结果里记 deferred）。
        顺延不再重复判定，时段结束后自然被下一轮扫描取到。
        """
        now = time.time() if now_ts is None else now_ts
        tasks = await self.store.pop_due(now, limit=limit)
        results: list[tuple[Any, str]] = []
        now_dt = datetime.fromtimestamp(now, tz=TZ_CN)
        jitter = self.jitter_seconds()
        interval = self.interval_seconds()
        attempted = False
        for task in tasks:
            # ① 免打扰时段：顺延到时段结束并标注，不深夜炸群也不丢提醒
            if self._quiet is not None and self._quiet.contains(now_dt):
                resume = self._quiet.end_after(now_dt)
                await self.store.reschedule(task.id, resume.timestamp(), mark_deferred=True)
                self.log.info(
                    f"免打扰时段 {self._quiet.spec}：提醒顺延到 "
                    f"{resume.strftime('%m-%d %H:%M')}（{task.author} {task.source_text}）"
                )
                results.append((task, STATUS_DEFERRED))
                continue
            # ② 抖动与间隔：防「同一秒连发多条」触发平台风控
            if jitter > 0:
                await asyncio.sleep(random.uniform(0, jitter))
            if attempted and interval > 0:
                await asyncio.sleep(interval)
            attempted = True

            is_makeup = (now - task.trigger_at) > MAKEUP_THRESHOLD
            try:
                ok = await self.pusher.send_remind(task, is_makeup=is_makeup)
            except asyncio.CancelledError:
                # 停机时把任务放回 pending，下次启动按补发处理，不能丢
                await self.store.mark(task.id, STATUS_PENDING)
                raise
            except Exception as e:
                self.log.warning(f"提醒推送异常（微博 {task.post_id}）: {e!r}")
                ok = False
            status = STATUS_SENT if ok else STATUS_FAILED
            await self.store.mark(task.id, status)
            results.append((task, status))
            if ok:
                self.log.info(
                    f"提醒已推送（{'补发' if is_makeup else '准点'}）"
                    f"：{task.author} {task.source_text} 提前 {task.advance_minutes} 分钟"
                )
        return results

    async def run(self) -> None:
        """常驻扫描循环（由插件 initialize 以 asyncio.create_task 拉起）。"""
        self.log.info(f"提醒调度已启动（扫描周期 {SCHEDULER_SCAN_INTERVAL:.0f} 秒）")
        while not self._stopped:
            try:
                if not (self._is_paused and self._is_paused()):
                    await self.scan_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log.warning(f"提醒调度扫描出错（下轮继续）: {e!r}")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=SCHEDULER_SCAN_INTERVAL)
            except asyncio.TimeoutError:
                continue

    def stop(self) -> None:
        """通知循环退出（terminate 时调用）。"""
        self._stopped = True
        self._stop_event.set()
