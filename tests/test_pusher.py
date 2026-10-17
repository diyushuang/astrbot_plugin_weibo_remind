"""推送层测试：目标会话合并、模板渲染、发送容错。"""

from __future__ import annotations

import asyncio

import pytest
from conftest import ts

from pusher import MAKEUP_PREFIX, Pusher
from store import RemindTask


class FakeContext:
    def __init__(self, result=True, exc: Exception | None = None):
        self.result = result
        self.exc = exc
        self.calls: list[tuple[str, object]] = []

    async def send_message(self, session, chain):
        self.calls.append((session, chain))
        if self.exc:
            raise self.exc
        return self.result


@pytest.fixture
def task() -> RemindTask:
    return RemindTask(
        id=1,
        post_id="5354957",
        # 事件时间 2026-10-17 20:00，提前 60 分钟 → 触发时间 19:00
        trigger_at=ts(2026, 10, 17, 19, 0),
        advance_minutes=60,
        status="pending",
        created_ts=0.0,
        snapshot={
            "author": "麦报的小行家",
            "source_text": "10月17日 20:00",
            "weibo": "📢10月17日 20:00 直播",
            "link": "https://weibo.com/6902546888/abc",
        },
    )


def test_sessions_merge_and_dedupe(log):
    config = {
        "push_sessions": ["aiocqhttp:GroupMessage:111", "aiocqhttp:GroupMessage:222"],
        "push_group_ids": ["222", "333"],
        "default_platform": "aiocqhttp",
    }
    assert Pusher(None, config, log).sessions() == [
        "aiocqhttp:GroupMessage:111",
        "aiocqhttp:GroupMessage:222",
        "aiocqhttp:GroupMessage:333",
    ]


def test_sessions_empty_by_default(log):
    assert Pusher(None, {}, log).sessions() == []


# ---------------- 免打扰时段（v0.4.1）----------------


def test_quiet_hours_parsed_from_config(log):
    p = Pusher(None, {"quiet_hours": "23:00-08:00"}, log)
    assert p.quiet() is not None and p.quiet().spec == "23:00-08:00"
    assert Pusher(None, {}, log).quiet() is None
    assert Pusher(None, {"quiet_hours": "乱填"}, log).quiet() is None


async def test_parse_fail_push_skipped_in_quiet_hours(log, monkeypatch):
    """解析失败提示是「非定时消息」，免打扰时段内直接跳过（顺延无意义）。"""
    ctx = FakeContext()
    config = {"push_sessions": ["aiocqhttp:GroupMessage:111"], "quiet_hours": "23:00-08:00"}
    p = Pusher(ctx, config, log)
    monkeypatch.setattr(Pusher, "in_quiet_hours", lambda self: True)

    assert await p.send_parse_fail("麦报", "正文", "https://x") is False
    assert ctx.calls == [], "免打扰时段内不该发出任何消息"


async def test_broadcast_respects_quiet_flag(log):
    """respect_quiet=False 时不受免打扰限制（用于需要立即送达的场景）。"""
    ctx = FakeContext()
    config = {"push_sessions": ["aiocqhttp:GroupMessage:111"], "quiet_hours": "23:00-08:00"}
    p = Pusher(ctx, config, log)

    ok, total = await p.broadcast("测试", respect_quiet=False)
    assert (ok, total) == (1, 1)
    assert len(ctx.calls) == 1


def test_render_remind_placeholders(log, task):
    config = {
        "remind_template": "【{name}】{source} 事件{event_time} 提前{advance}\n{weibo}\n{link}"
    }
    text = Pusher(None, config, log).render_remind(task)
    assert "麦报的小行家" in text
    assert "10月17日 20:00" in text
    assert "2026-10-17 20:00" in text
    assert "1 小时" in text
    assert "https://weibo.com/6902546888/abc" in text


def test_render_remind_default_template_and_makeup(log, task):
    text = Pusher(None, {}, log).render_remind(task)
    assert text.startswith("⏰ 微博提醒")
    makeup = Pusher(None, {}, log).render_remind(task, is_makeup=True)
    assert makeup.startswith(MAKEUP_PREFIX)


def test_render_remind_survives_braces_in_weibo(log, task):
    """正文里带花括号不能让模板渲染炸掉（用替换而非 str.format）。"""
    task.snapshot["weibo"] = "抽奖 {name} 与 {link} 都是占位符文本"
    text = Pusher(None, {"remind_template": "{weibo}"}, log).render_remind(task)
    assert text == "抽奖 {name} 与 {link} 都是占位符文本"


def test_render_remind_advance_label_at_zero(log, task):
    task.advance_minutes = 0
    text = Pusher(None, {"remind_template": "{advance}"}, log).render_remind(task)
    assert text == "到点"


def test_render_parse_fail(log):
    text = Pusher(None, {}, log).render_parse_fail("麦报", "正文", "https://x")
    assert "麦报" in text and "正文" in text and "https://x" in text


async def test_send_text_success(log):
    ctx = FakeContext(result=True)
    assert await Pusher(ctx, {}, log).send_text("aiocqhttp:GroupMessage:1", "hi") is True
    assert len(ctx.calls) == 1


async def test_send_text_returns_false_when_platform_missing(log):
    """AstrBot 返回 False 表示未找到匹配平台 → 记为失败而非异常。"""
    ctx = FakeContext(result=False)
    assert await Pusher(ctx, {}, log).send_text("aiocqhttp:GroupMessage:1", "hi") is False


async def test_send_text_timeout_is_swallowed(log):
    ctx = FakeContext(exc=asyncio.TimeoutError())
    assert await Pusher(ctx, {}, log).send_text("ug:GroupMessage:1", "hi") is False


async def test_send_text_exception_is_swallowed(log):
    ctx = FakeContext(exc=RuntimeError("adapter down"))
    assert await Pusher(ctx, {}, log).send_text("ug:GroupMessage:1", "hi") is False


async def test_broadcast_continues_after_one_failure(log):
    """单个会话发送失败不影响其它会话。"""
    config = {
        "push_sessions": ["aiocqhttp:GroupMessage:1", "aiocqhttp:GroupMessage:2"],
        "push_group_ids": ["3"],
    }

    class PartialContext(FakeContext):
        async def send_message(self, session, chain):
            self.calls.append((session, chain))
            return session.endswith(":2") is False  # 第 2 个会话失败

    ctx = PartialContext()
    ok, total = await Pusher(ctx, config, log).broadcast("hi")
    assert total == 3
    assert ok == 2


async def test_broadcast_without_target_returns_zero(log):
    ok, total = await Pusher(FakeContext(), {}, log).broadcast("hi")
    assert (ok, total) == (0, 0)


class FakeCounter:
    """@全体 每日计数替身。"""

    def __init__(self, used: int = 0):
        self.used = used
        self.bumps = 0

    async def at_all_today(self) -> int:
        return self.used

    async def bump_at_all(self) -> int:
        self.bumps += 1
        self.used += 1
        return self.used


def _has_at_all(chain) -> bool:
    return type(chain.chain[0]).__name__ == "AtAll"


async def test_at_all_only_for_group_sessions(log, task):
    """私聊没有 @全体 概念：即便总开关开着也不加 AtAll。"""
    config = {
        "at_all": True,
        "at_all_advance_minutes": [],
        "push_sessions": ["aiocqhttp:GroupMessage:1", "aiocqhttp:FriendMessage:9"],
        "remind_template": "{weibo}",
    }
    ctx = FakeContext()
    await Pusher(ctx, config, log).send_remind(task)
    assert _has_at_all(ctx.calls[0][1]) is True
    assert _has_at_all(ctx.calls[1][1]) is False


async def test_at_all_disabled_by_default(log, task):
    config = {"push_sessions": ["aiocqhttp:GroupMessage:1"], "remind_template": "{weibo}"}
    ctx = FakeContext()
    await Pusher(ctx, config, log).send_remind(task)
    assert _has_at_all(ctx.calls[0][1]) is False


async def test_at_all_default_only_on_the_due_tier(log, task):
    """默认只对「到点」档 @：多档提醒各推一次，全带上 @ 等于连炸三次群。"""
    config = {"at_all": True, "push_sessions": ["aiocqhttp:GroupMessage:1"]}
    ctx = FakeContext()
    pusher = Pusher(ctx, config, log)

    task.advance_minutes = 60
    await pusher.send_remind(task)
    assert _has_at_all(ctx.calls[-1][1]) is False  # 提前 60 分钟：不 @

    task.advance_minutes = 0
    await pusher.send_remind(task)
    assert _has_at_all(ctx.calls[-1][1]) is True  # 到点：@


async def test_at_all_tier_filter_configurable(log, task):
    config = {
        "at_all": True,
        "at_all_advance_minutes": [10, 0],
        "push_sessions": ["aiocqhttp:GroupMessage:1"],
    }
    ctx = FakeContext()
    pusher = Pusher(ctx, config, log)

    task.advance_minutes = 60
    await pusher.send_remind(task)
    assert _has_at_all(ctx.calls[-1][1]) is False

    task.advance_minutes = 10
    await pusher.send_remind(task)
    assert _has_at_all(ctx.calls[-1][1]) is True

    # 空列表 = 不限档位
    pusher.config["at_all_advance_minutes"] = []
    task.advance_minutes = 60
    await pusher.send_remind(task)
    assert _has_at_all(ctx.calls[-1][1]) is True


async def test_at_all_keyword_filter(log, task):
    config = {
        "at_all": True,
        "at_all_advance_minutes": [],  # 不限档位，单独验证关键词闸门
        "at_all_keywords": ["直播"],
        "push_sessions": ["aiocqhttp:GroupMessage:1"],
    }
    ctx = FakeContext()
    pusher = Pusher(ctx, config, log)

    task.snapshot["weibo"] = "10月17日 20:00 直播"
    await pusher.send_remind(task)
    assert _has_at_all(ctx.calls[-1][1]) is True

    task.snapshot["weibo"] = "10月17日 20:00 上新海报"
    await pusher.send_remind(task)
    assert _has_at_all(ctx.calls[-1][1]) is False


async def test_at_all_daily_limit_downgrades_but_still_sends(log, task):
    """超过每日 @ 上限后降级为普通提醒：内容照发，只是不 @。"""
    config = {
        "at_all": True,
        "at_all_advance_minutes": [],
        "at_all_daily_limit": 2,
        "push_sessions": ["aiocqhttp:GroupMessage:1"],
    }
    ctx = FakeContext()
    counter = FakeCounter(used=2)  # 今天已经用满
    pusher = Pusher(ctx, config, log, counter=counter)

    task.advance_minutes = 0
    assert await pusher.send_remind(task) is True  # 仍然发送成功
    assert _has_at_all(ctx.calls[-1][1]) is False
    assert counter.bumps == 0


async def test_at_all_counter_bumped_only_on_success(log, task):
    config = {
        "at_all": True,
        "at_all_advance_minutes": [],
        "push_sessions": ["aiocqhttp:GroupMessage:1"],
    }
    counter = FakeCounter()
    ok_ctx = FakeContext()
    task.advance_minutes = 0
    await Pusher(ok_ctx, config, log, counter=counter).send_remind(task)
    assert counter.bumps == 1

    fail_ctx = FakeContext(result=False)
    await Pusher(fail_ctx, config, log, counter=FakeCounter()).send_remind(task)
    assert counter.bumps == 1  # 失败的没记数


async def test_at_all_failure_falls_back_and_keeps_body(log, task):
    """带 @ 发送失败（机器人不是管理员 / 撞 @全体 频控）→ 去掉 @ 重发，正文必须到群。"""
    config = {
        "at_all": True,
        "at_all_advance_minutes": [],
        "push_sessions": ["aiocqhttp:GroupMessage:1"],
    }

    class FlakyContext(FakeContext):
        async def send_message(self, session, chain):
            self.calls.append((session, chain))
            return not _has_at_all(chain)  # 带 @ 的整条被协议端拒收

    ctx = FlakyContext()
    counter = FakeCounter()
    task.advance_minutes = 0
    assert await Pusher(ctx, config, log, counter=counter).send_remind(task) is True
    assert len(ctx.calls) == 2, "应当先试带 @，失败后去掉 @ 重发"
    assert _has_at_all(ctx.calls[0][1]) is True
    assert _has_at_all(ctx.calls[1][1]) is False
    assert counter.bumps == 0, "降级发送成功不该记 @ 次数"


async def test_no_at_all_when_all_sends_fail(log, task):
    config = {
        "at_all": True,
        "at_all_advance_minutes": [],
        "push_sessions": ["aiocqhttp:GroupMessage:1"],
    }
    ctx = FakeContext(result=False)
    task.advance_minutes = 0
    assert await Pusher(ctx, config, log).send_remind(task) is False


async def test_parse_fail_push_never_at_all(log):
    """解析失败提示不是到点事件，不该打扰全群。"""
    config = {"at_all": True, "push_sessions": ["aiocqhttp:GroupMessage:1"]}
    ctx = FakeContext()
    await Pusher(ctx, config, log).send_parse_fail("麦报", "正文", "https://x")
    assert _has_at_all(ctx.calls[0][1]) is False


async def test_send_remind_success_requires_a_target(log, task):
    config = {"push_sessions": ["aiocqhttp:GroupMessage:1"]}
    assert await Pusher(FakeContext(), config, log).send_remind(task) is True


def test_render_remind_marks_deferred_task(log, task):
    """免打扰顺延过来的任务，文案要标注「顺延提醒」，且优先级低于补发。"""
    p = Pusher(None, {}, log)
    assert "顺延提醒" not in p.render_remind(task)

    task.snapshot = {**task.snapshot, "deferred": True}
    assert "顺延提醒" in p.render_remind(task)

    # 同时是补发时以补发为准（顺延后的 trigger_at 在未来，理论上不会同时成立）
    out = p.render_remind(task, is_makeup=True)
    assert "补发提醒" in out and "顺延提醒" not in out
