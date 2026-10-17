"""端到端集成测试：抓取 → 解析 → 建任务 → 到点推送。

不触网：抓取层用假时间线替身，平台层用假 Context；
store / time_parser / scheduler / pusher 都是真实实现。
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import pytest
from conftest import CN, now_cn

from fetcher import WeiboFetchError, WeiboPost
from main import WeiboRemindPlugin
from scheduler import RemindScheduler
from store import STATUS_SENT


class FakeContext:
    """假 AstrBot Context：记录发出去的文本与消息链（链用来断言 @全体 段）。"""

    def __init__(self):
        self.sent: list[tuple[str, str]] = []
        self.calls: list[tuple[str, object]] = []

    async def send_message(self, session, chain):
        text = "".join(getattr(seg, "text", "") for seg in chain.chain)
        self.sent.append((session, text))
        self.calls.append((session, chain))
        return True


def _post(pid: str, text: str, created: datetime) -> WeiboPost:
    """构造一条微博；created 以**上海时区**为准。

    必须归一到带时区的 datetime：插件全流程按 `constants.TZ_CN` 解释正文时间，
    而裸 naive datetime 的 `.timestamp()` 跟随**运行机器的本地时区**。
    CI（UTC）与本地开发机（UTC+8）因此相差 8 小时，曾导致用例在 CI 上失败。
    """
    if created.tzinfo is None:
        created = created.replace(tzinfo=CN)
    return WeiboPost(
        id=pid,
        uid="6902546888",
        author="麦报的小行家",
        link=f"https://weibo.com/6902546888/{pid}",
        created_at=created.strftime("%Y-%m-%d %H:%M"),
        created_ts=created.timestamp(),
        text=text,
        is_retweet=False,
        orig_text="",
    )


@pytest.fixture
def plugin(tmp_path, monkeypatch, log):
    config = {
        "uid": "6902546888",
        "poll_interval": 3600,
        "check_window_minutes": 720,
        "remind_advance_minutes": [60, 10, 0],
        "date_only_default_hour": 9,
        "push_sessions": ["aiocqhttp:GroupMessage:123456"],
        "text_max_length": 500,
        "missed_remind_grace_minutes": 10,
    }
    ctx = FakeContext()
    inst = WeiboRemindPlugin(context=ctx, config=config)
    # 数据目录指到临时目录，避免污染真实插件数据
    monkeypatch.setattr("main._resolve_data_dir", lambda: tmp_path)
    # 抓取层的网络动作在集成测试里全部替换
    monkeypatch.setattr("fetcher.WeiboFetcher.start", lambda self: _noop())
    monkeypatch.setattr("fetcher.WeiboFetcher.warmup", lambda self: _noop())
    # 背景轮询循环关掉，避免与用例手动驱动的 _check_once 抢轮次
    monkeypatch.setattr(WeiboRemindPlugin, "_poll_loop", _noop_loop)
    # 后台调度循环同理关掉：`initialize()` 会拉起它，而它启动即扫一次，
    # 用例随后手动调 scan_once() 时任务可能已被后台抢走（曾偶发失败）。
    # 修的是用例的竞态，不是产品行为——线上只有这条后台循环。
    monkeypatch.setattr(RemindScheduler, "run", _noop_loop)
    return inst


async def _noop_loop(self) -> None:
    return None


async def _noop():
    return None


async def test_full_flow_creates_multi_tier_tasks_and_pushes(tmp_path, plugin, monkeypatch):
    """新微博 → 识别出时间 → 多档建任务 → 到点推送。

    事件时间用「未来某天 10:00」而不是「今天 10:00」：后者在下午跑用例时
    必然已被判为过期（既有用例的时间依赖缺陷，随手修掉）。
    """
    old = _post(
        "p-old",
        "📢行程同步 / 10月10日 / 已结束的活动",
        datetime(2026, 10, 10, 10, 0).replace(tzinfo=None),
    )
    fresh_post = _post(
        "p-new",
        f"📢商务物料 / 发布规划 / ⏰{_future_text()} / 10:00 邀约试驾视频",
        now_cn(),
    )
    rounds: list[list[WeiboPost]] = [[old], [fresh_post, old]]

    async def fake_fetch(self, uid, known_ids=None):
        return rounds.pop(0) if rounds else [fresh_post, old]

    monkeypatch.setattr("fetcher.WeiboFetcher.fetch_timeline", fake_fetch)
    await plugin.initialize()
    try:
        # 第一轮：首次运行 → 记全集为已见；窗口外（超 12 小时）的旧帖不解析
        assert await plugin._check_once() == 0
        assert await plugin.store.seen_count() == 1
        # 第二轮：出现新微博 → 建 3 档任务（提前 60/10/0）
        assert await plugin._check_once() == 3
        tasks = await plugin.store.list_pending()
        assert [t.advance_minutes for t in tasks] == [60, 10, 0]
        assert all(t.source_text == f"{_future_text()} 10:00" for t in tasks)
        assert all("邀约试驾视频" in t.weibo for t in tasks)

        # 把三档的触发时间全部改到过去，模拟到点
        now = time.time()
        with plugin.store._lock, plugin.store._conn:
            plugin.store._conn.execute(
                "UPDATE remind_tasks SET trigger_at = ? WHERE status = 'pending'", (now - 1,)
            )
        results = await plugin.scheduler.scan_once(now_ts=now)
        assert len(results) == 3
        assert all(status == STATUS_SENT for _, status in results)

        sent = plugin.context.sent
        assert len(sent) == 3
        assert sent[0][0] == "aiocqhttp:GroupMessage:123456"
        assert "微博提醒" in sent[0][1]
        event_day = now_cn() + timedelta(days=5)
        assert f"{event_day.year}-{event_day.month:02d}-{event_day.day:02d} 10:00" in sent[0][1]
        assert f"{_future_text()} 10:00" in sent[0][1]  # 命中原文
    finally:
        await plugin.terminate()


async def test_baseline_backfills_recent_post(plugin, monkeypatch):
    """首次运行：时效窗口内的最新公告要解析出任务，窗口外的历史帖只记已见。"""
    old = _post("p-history", "10月17日 20:00 直播", datetime(2026, 10, 1, 10, 0))
    recent = _post("p-recent", "明天下午3点直播", now_cn())
    monkeypatch.setattr(
        "fetcher.WeiboFetcher.fetch_timeline", lambda self, uid, known_ids=None: _ret([recent, old])
    )
    await plugin.initialize()
    try:
        created = await plugin._check_once()
        assert created > 0, "时效窗口内的微博应被回补解析"
        assert await plugin.store.seen_count() == 2, "全部可见微博都应记为已见"
        assert all(t.post_id == "p-recent" for t in await plugin.store.list_pending())
    finally:
        await plugin.terminate()


async def test_past_times_create_nothing_and_push_nothing(plugin, monkeypatch):
    """解析出的时间全部已过去 → 既无任务也不推送（用户选定的静默策略）。"""
    old = _post("p-old", "【0945】开播预告", datetime(2026, 9, 27, 22, 50))
    monkeypatch.setattr(
        "fetcher.WeiboFetcher.fetch_timeline", lambda self, uid, known_ids=None: _ret([old])
    )
    await plugin.initialize()
    try:
        await plugin._check_once()  # 基线（假抓取立刻给出该帖 → 视为基线，不解析）
        assert plugin.context.sent == []
        assert await plugin.store.pending_count() == 0
        # 直接走单帖处理路径：时间已过 → 不建任务、不推送
        assert await plugin._process_post(old) == 0
        assert plugin.context.sent == []
    finally:
        await plugin.terminate()


async def test_parse_failure_pushes_original_text(plugin, monkeypatch):
    """疑似含时间安排但解析不出 → 推送原文请群友确认。"""
    vague = _post(
        "p-vague",
        "📢《无可替代》发布节奏 / 具体时间后续公布 / 请大家留意",
        now_cn(),
    )
    monkeypatch.setattr(
        "fetcher.WeiboFetcher.fetch_timeline", lambda self, uid, known_ids=None: _ret([vague])
    )
    await plugin.initialize()
    try:
        created = await plugin._process_post(vague)
        assert created == 0
        assert len(plugin.context.sent) == 1
        assert "未能自动识别" in plugin.context.sent[0][1]
    finally:
        await plugin.terminate()


async def test_plain_post_is_silent(plugin, monkeypatch):
    """日常博文（不含时间安排）→ 静默，不推送。"""
    plain = _post("p-plain", "今天天气不错，大家早安", now_cn())
    await plugin.initialize()
    try:
        assert await plugin._process_post(plain) == 0
        assert plugin.context.sent == []
    finally:
        await plugin.terminate()


@pytest.mark.parametrize(
    ("pid", "text", "kind"),
    [
        (
            "p-suggestion",
            "赵今麦 \n📭【7月建议博】",
            "suggestion",
        ),
        (
            "p-notice",
            "📢通知\n网传活动行程为假，请大家以官方通知为准，避免财产损失。",
            "notice",
        ),
        (
            "p-daily",
            "品牌的城市地广，欢迎大家积极打卡～",
            "daily",
        ),
    ],
)
async def test_non_schedule_posts_are_silent(plugin, pid, text, kind):
    """建议博/打假声明/日常博不得触发「解析失败推原文」——实测语料里这类占 20/98，
    按原策略会被当疑似时间推给群友，是纯噪音。"""
    post = _post(pid, text, now_cn())
    await plugin.initialize()
    try:
        assert await plugin._process_post(post) == 0
        assert plugin.context.sent == [], f"{kind} 类博文不该推送原文"
        stats = plugin.coverage.stats()
        assert stats["by_kind"].get(kind) == 1
        assert stats["uncovered_total"] == 0, "非排期类不该进待补样本"
    finally:
        await plugin.terminate()


async def test_pending_schedule_post_pushes_original(plugin, monkeypatch):
    """明说「时间待定」的排期博文：没有可用时间点，但要推原文让群友知道有安排。"""
    past = now_cn() - timedelta(days=3)
    post = _post(
        "p-pending",
        f"通知📢\n@无可替代官微\n\n{past.month}月{past.day}日 官宣杀青物料\n\n具体发布待定。",
        now_cn(),
    )
    await plugin.initialize()
    try:
        assert await plugin._process_post(post) == 0
        assert len(plugin.context.sent) == 1
        assert "未能自动识别" in plugin.context.sent[0][1]
        assert plugin.coverage.stats()["uncovered_total"] == 0
    finally:
        await plugin.terminate()


async def test_schedule_post_without_any_time_pushes_original(plugin, monkeypatch):
    """排期类但完全没提时间：推原文请群友确认（这类博文的价值就是内容本身）。"""
    post = _post(
        "p-notime",
        "📢通知\n@无可替代官微\n\n官宣杀青物料，具体节奏稍后同步给大家。",
        now_cn(),
    )
    await plugin.initialize()
    try:
        assert await plugin._process_post(post) == 0
        assert len(plugin.context.sent) == 1
    finally:
        await plugin.terminate()


async def test_coverage_records_tasked_and_coverage_percent(plugin, monkeypatch):
    """覆盖自检：排期类建出任务后，覆盖率统计要反映出来。"""
    post = _post(
        "p-cover",
        f"📢商务物料 / 发布规划 / ⏰{_future_text()} / 10:00 邀约试驾视频",
        now_cn(),
    )
    await plugin.initialize()
    try:
        created = await plugin._process_post(post)
        assert created > 0
        stats = plugin.coverage.stats()
        assert stats["by_kind"]["schedule"] == 1
        assert stats["schedule_tasked"] == 1
        assert stats["coverage_percent"] == 100.0
        assert stats["uncovered_total"] == 0
    finally:
        await plugin.terminate()


async def test_old_post_outside_window_is_skipped(plugin, monkeypatch):
    """超过时效窗口的微博不处理（防基线/补漏把旧帖当新提醒）。"""
    old = _post("p-window", "今晚8点直播", datetime(2026, 10, 1, 10, 0))
    await plugin.initialize()
    try:
        assert await plugin._process_post(old) == 0
        assert plugin.context.sent == []
    finally:
        await plugin.terminate()


async def test_advance_tier_already_passed_is_skipped(plugin, monkeypatch):
    """抓到微博时离事件不足 60 分钟 → 提前 60 档静默跳过，其余照建。"""
    now = now_cn()
    event = now + timedelta(minutes=30)
    if event.date() != now.date():
        pytest.skip("临近午夜，跨日场景不适用本用例")
    post = _post("p-soon", f"【{event.strftime('%H%M')}】开播", now.replace(tzinfo=None))
    await plugin.initialize()
    try:
        created = await plugin._process_post(post)
        tasks = await plugin.store.list_pending()
        assert created == len(tasks) > 0
        assert 60 not in [t.advance_minutes for t in tasks]
    finally:
        await plugin.terminate()


async def test_restart_resends_missed_within_grace(tmp_path, plugin, monkeypatch):
    """重启后：宽限内的错过提醒补发（带补发标记），超期作废。"""
    post = _post("p-restart", "10月17日 20:00 直播", now_cn())
    await plugin.initialize()
    try:
        await plugin._process_post(post)
        assert await plugin.store.pending_count() > 0
        # 一档错过 5 分钟（宽限 10 分钟内 → 补发），一档错过 1 小时（→ 作废）
        now = time.time()
        tasks = sorted(await plugin.store.list_pending(), key=lambda t: t.advance_minutes)
        with plugin.store._lock, plugin.store._conn:
            plugin.store._conn.execute(
                "UPDATE remind_tasks SET trigger_at = ? WHERE id = ?", (now - 300, tasks[0].id)
            )
            plugin.store._conn.execute(
                "UPDATE remind_tasks SET trigger_at = ? WHERE id = ?", (now - 3600, tasks[1].id)
            )
    finally:
        await plugin.terminate()

    # 重新起一个插件实例（同一数据目录）→ 触发恢复逻辑
    fresh = WeiboRemindPlugin(context=FakeContext(), config=plugin.config)
    await fresh.initialize()
    try:
        results = await fresh.scheduler.scan_once()
        assert results, "宽限内的错过任务应被补发"
        assert "补发提醒" in fresh.context.sent[0][1]
        stats = await fresh.store.stats()
        assert stats["by_status"].get("missed", 0) >= 1, "超过宽限的任务应标记 missed"
    finally:
        await fresh.terminate()


async def _ret(value):
    return value


# ---------------- @全体成员指令 ----------------


class _CmdEvent:
    """指令 handler 的最小事件替身。"""

    def __init__(self, text: str = "", group_id: str = "123456"):
        self.unified_msg_origin = "aiocqhttp:GroupMessage:123456"
        self.message_str = text
        self._group_id = group_id

    def get_group_id(self):
        return self._group_id

    def plain_result(self, text):
        return _CmdResult(text)


class _CmdResult:
    def __init__(self, text: str):
        self.text = text


async def _run_cmd(plugin, name: str, event) -> str:
    out = []
    async for r in getattr(plugin, name)(event):
        out.append(r.text)
    return "\n".join(out)


async def test_at_all_command_toggles_and_persists(plugin):
    """微博提醒 全体 开/关 要真的改配置（并落盘），状态里能看到用量。"""
    await plugin.initialize()
    try:
        assert not plugin.config.get("at_all")

        out = await _run_cmd(plugin, "cmd_at_all", _CmdEvent("微博提醒 全体 开"))
        assert "已开启" in out
        assert plugin.config["at_all"] is True

        out = await _run_cmd(plugin, "cmd_at_all", _CmdEvent("微博提醒 全体 状态"))
        assert "总开关：开" in out
        assert "档位限定" in out and "每日上限" in out

        out = await _run_cmd(plugin, "cmd_at_all", _CmdEvent("微博提醒 全体 关"))
        assert "已关闭" in out
        assert plugin.config["at_all"] is False
    finally:
        await plugin.terminate()


async def test_at_all_command_usage_hint(plugin):
    await plugin.initialize()
    try:
        out = await _run_cmd(plugin, "cmd_at_all", _CmdEvent("微博提醒 全体 乱填"))
        assert "用法" in out
    finally:
        await plugin.terminate()


async def test_status_shows_at_all_state(plugin):
    await plugin.initialize()
    try:
        assert "@全体成员：关" in await _run_cmd(plugin, "cmd_status", _CmdEvent())
        plugin.config["at_all"] = True
        out = await _run_cmd(plugin, "cmd_status", _CmdEvent())
        assert "@全体成员：开" in out and "今日" in out
    finally:
        await plugin.terminate()


async def test_push_with_at_all_end_to_end(plugin):
    """端到端：到点提醒带 @全体 段发到群里，并记一次用量。"""
    post = _post(
        "p-atall",
        "📢商务物料 / 发布规划 / " + _future_text() + " / 10:00 邀约试驾视频",
        now_cn(),
    )
    plugin.config["at_all"] = True
    plugin.config["at_all_advance_minutes"] = [0]
    await plugin.initialize()
    try:
        created = await plugin._process_post(post)
        assert created > 0
        now = time.time()
        with plugin.store._lock, plugin.store._conn:
            plugin.store._conn.execute("UPDATE remind_tasks SET trigger_at = ?", (now - 1,))
        await plugin.scheduler.scan_once(now_ts=now)

        chains = [chain for _, chain in plugin.context.calls]
        assert chains, "应当有推送发出"
        assert any(type(c.chain[0]).__name__ == "AtAll" for c in chains), "@ 段应在消息最前"
        assert await plugin.store.at_all_today() == 1
    finally:
        await plugin.terminate()


def _future_text() -> str:
    """构造未来日期文本（月日），避免用例依赖运行日期。"""
    dt = now_cn() + timedelta(days=5)
    return f"{dt.month}月{dt.day}日"


# ---------------- 多账号监控（v0.4.0）----------------


def test_uids_merges_new_and_legacy_config():
    """uids 列表与旧版 uid 字段合并去重，剔除非数字项。"""
    inst = WeiboRemindPlugin(
        context=FakeContext(),
        config={"uid": "111", "uids": ["222", "111", "bad", 333, "", " 222 "]},
    )
    assert inst._uids() == ["222", "111", "333"]
    # 只剩旧字段 → 单账号（旧配置升级路径）
    assert WeiboRemindPlugin(context=FakeContext(), config={"uid": "111"})._uids() == ["111"]
    # 只剩新字段
    assert WeiboRemindPlugin(context=FakeContext(), config={"uids": ["222"]})._uids() == ["222"]
    # 全空
    assert WeiboRemindPlugin(context=FakeContext(), config={})._uids() == []


async def test_second_account_gets_own_baseline(plugin, monkeypatch):
    """账号 A 已有已见记录时，新加的账号 B 首轮必须走自己的基线路径。

    判定依据是 fetch_timeline 收到的 known_ids：基线轮传 None（不翻页），
    非基线轮传该账号的已见集合。若基线仍按全局记录数判定（v0.3.x 行为），
    B 的首轮会拿到 A 的已见集合，翻页边界判断就错了。
    """
    post_a = _post("a-seen", "今天天气不错", now_cn() - timedelta(hours=1))
    post_b = _post("b-fresh", f"{_future_text()} 20:00 直播预告", now_cn())
    calls: list[tuple[str, object]] = []

    async def fake_fetch(self, uid, known_ids=None):
        calls.append((uid, known_ids))
        return [post_a] if uid == "6902546888" else [post_b]

    monkeypatch.setattr("fetcher.WeiboFetcher.fetch_timeline", fake_fetch)
    await plugin.initialize()
    try:
        # 第一轮：单账号 A → 基线（known_ids=None）
        assert await plugin._check_once() == 0
        assert calls[-1] == ("6902546888", None)
        assert await plugin.store.seen_count("6902546888") == 1

        # 第二轮：新增账号 B → A 走增量（known 非空），B 走自己的基线（None）
        plugin.config["uids"] = ["6902546888", "99999"]
        created = await plugin._check_once()
        assert calls[-2] == ("6902546888", {"a-seen"})
        assert calls[-1] == ("99999", None)
        assert await plugin.store.seen_count("99999") == 1, "B 的帖子应记到 B 自己名下"
        # B 的窗口内排期帖在基线回补中被解析建任务
        assert created > 0
        assert all(t.post_id == "b-fresh" for t in await plugin.store.list_pending())
    finally:
        await plugin.terminate()


async def test_partial_fetch_failure_skips_only_that_account(plugin, monkeypatch):
    """一个账号抓取失败只跳过它，其它账号照常；全部失败才按整轮失败抛错。"""
    post_a = _post("a-ok", "今天天气不错", now_cn())
    post_b = _post("b-ok", f"{_future_text()} 20:00 直播预告", now_cn())
    state = {"fail_a": False}

    async def fake_fetch(self, uid, known_ids=None):
        if uid == "111" and state["fail_a"]:
            raise WeiboFetchError("账号 A 挂了")
        return [post_a] if uid == "111" else [post_b]

    # fixture 里带的是旧版单账号字段，这里显式清掉，只测 uids 两个账号的情形
    plugin.config.pop("uid", None)
    plugin.config["uids"] = ["111", "99999"]
    monkeypatch.setattr("fetcher.WeiboFetcher.fetch_timeline", fake_fetch)
    await plugin.initialize()
    try:
        # 第一轮：两个账号都正常 → 各自建基线（B 窗口内排期帖回补建任务）
        assert await plugin._check_once() > 0
        assert await plugin.store.seen_count("111") == 1
        assert await plugin.store.seen_count("99999") == 1

        # 第二轮：A 抓取失败、B 正常 → 本轮不抛错，B 的增量照常（无新帖）
        state["fail_a"] = True
        assert await plugin._check_once() == 0
    finally:
        await plugin.terminate()


async def test_all_accounts_failed_raises_for_backoff(plugin, monkeypatch):
    """全部账号都失败 → 抛出首个错误，让轮询层进入退避。"""

    async def fetch_fail(self, uid, known_ids=None):
        raise WeiboFetchError("风控冷却中")

    plugin.config["uids"] = ["111", "222"]
    monkeypatch.setattr("fetcher.WeiboFetcher.fetch_timeline", fetch_fail)
    await plugin.initialize()
    try:
        with pytest.raises(WeiboFetchError):
            await plugin._check_once()
    finally:
        await plugin.terminate()


async def test_status_lists_all_accounts(plugin):
    """状态指令要列出全部监控账号。"""
    plugin.config["uids"] = ["6902546888", "1234567890"]
    await plugin.initialize()
    try:
        out = await _run_cmd(plugin, "cmd_status", _CmdEvent())
        assert "6902546888" in out and "1234567890" in out
        assert "（2 个）" in out
    finally:
        await plugin.terminate()


async def test_legacy_uid_config_still_monitors(plugin, monkeypatch):
    """旧配置（只有 uid 字段）升级后无需改动即可继续工作。"""
    post = _post("p-legacy", f"{_future_text()} 20:00 直播预告", now_cn())
    monkeypatch.setattr(
        "fetcher.WeiboFetcher.fetch_timeline", lambda self, uid, known_ids=None: _ret([post])
    )
    await plugin.initialize()  # fixture 的 config 只有 uid=6902546888
    try:
        created = await plugin._check_once()
        assert created > 0, "基线回补窗口内的排期帖应建任务"
        assert await plugin.store.seen_count("6902546888") == 1, "已见记录归属到 legacy uid"
    finally:
        await plugin.terminate()


# ---------------- 声明下限的 AstrBot 兼容性 ----------------


def test_constructs_when_star_injects_no_logger(monkeypatch):
    """回归：Star 在 v4.26.8 之前不注入 self.logger，构造期不能因此崩溃。

    真实症状（CI 抓到）：工作流用 Python 3.11 + `pip install astrbot`，
    因 astrbot>=4.19.5 要求 Python>=3.12，pip 一路回退装到 4.14.6；
    该版本 Star 没有 logger 属性，构造期取 self.logger 直接 AttributeError,
    插件在加载阶段就起不来（且不会自愈）。此处用一个"不注入 logger 的 Star"
    复刻该形态——修复后应能退回 astrbot 的标准日志器正常构造。
    """
    from astrbot.api.star import Star

    def bare_init(self, context):  # 模拟旧版 Star：只存 context，不注入 logger
        self.context = context

    monkeypatch.setattr(Star, "__init__", bare_init)
    inst = WeiboRemindPlugin(context=FakeContext(), config={})
    assert inst.logger is not None, "缺少 logger 时应退回标准日志器，而不是崩溃"


def test_logger_is_kept_when_star_provides_one(monkeypatch):
    """反向用例：Star 注入了 logger 时必须原样保留，不能被回退逻辑覆盖。"""
    from astrbot.api.star import Star

    sentinel = object()

    def init_with_logger(self, context):  # 模拟 v4.26.8+ 的行为
        self.context = context
        self.logger = sentinel

    monkeypatch.setattr(Star, "__init__", init_with_logger)
    inst = WeiboRemindPlugin(context=FakeContext(), config={})
    assert inst.logger is sentinel
