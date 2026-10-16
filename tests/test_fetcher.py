"""抓取层测试：解析工具、置顶/cards 容错、风控与身份失效分支。

不触网：HTTP 层用注入的假 _get_text 替代。
"""

from __future__ import annotations

import time

import pytest

from fetcher import (
    WeiboAuthError,
    WeiboFetcher,
    WeiboFetchError,
    WeiboRiskError,
    _clean_text,
    _format_time,
    _jsonp_data,
    _parse_created_epoch,
)


@pytest.fixture
def fetcher(log) -> WeiboFetcher:
    f = WeiboFetcher({"text_max_length": 100, "request_timeout": 5}, log)
    # 标记 HTTP 会话已初始化；真实 HTTP 层在测试里被 _get_text 替身接管
    f._http = object()
    return f


# ---------------- 纯函数 ----------------


def test_clean_text_strips_html_and_keeps_emoji_alt():
    raw = '第一行<br/>第二行<img alt="[笑cry]" src="x"/> 结束'
    text = _clean_text(raw, 0)
    assert text == "第一行\n第二行[笑cry] 结束"


def test_clean_text_truncates():
    assert _clean_text("a" * 200, 10) == "a" * 10 + "…"


def test_parse_created_epoch():
    ts = _parse_created_epoch("Fri Oct 16 10:00:00 +0800 2026")
    assert _format_time("Fri Oct 16 10:00:00 +0800 2026") == "2026-10-16 10:00"
    assert ts > 0
    assert _parse_created_epoch("5分钟前") == 0.0


def test_format_time_passes_relative_through():
    assert _format_time("5分钟前") == "5分钟前"


def test_jsonp_data():
    assert _jsonp_data('gen_callback({"retcode":20000000,"data":{"tid":"abc"}});') == {
        "retcode": 20000000,
        "data": {"tid": "abc"},
    }
    assert _jsonp_data("不是 JSONP") is None


def test_parse_cookie_string(fetcher):
    assert fetcher._parse_cookie_string("SUB=abc; SUBP=def ; broken") == {
        "SUB": "abc",
        "SUBP": "def",
    }


# ---------------- 帖子解析 ----------------


def _mblog(**over):
    mb = {
        "id": "5354957523084",
        "bid": "RmU0fvz64",
        "created_at": "Thu Oct 15 18:00:00 +0800 2026",
        "text": "📢行程同步<br/>10月17日 20:00 直播",
        "user": {"id": 6902546888, "screen_name": "麦报的小行家"},
    }
    mb.update(over)
    return mb


def test_parse_post_basic(fetcher):
    post = fetcher._parse_post(_mblog(), "6902546888")
    assert post.id == "5354957523084"
    assert post.author == "麦报的小行家"
    assert post.link == "https://weibo.com/6902546888/RmU0fvz64"
    assert post.created_ts > 0
    assert "10月17日 20:00" in post.text
    assert post.is_retweet is False


def test_parse_post_retweet_keeps_original_text(fetcher):
    mb = _mblog(retweeted_status={"text": "原微博：今晚8点开播", "user": {"screen_name": "原作者"}})
    post = fetcher._parse_post(mb, "6902546888")
    assert post.is_retweet is True
    assert "今晚8点开播" in post.orig_text


def test_parse_post_without_id_is_skipped(fetcher):
    assert fetcher._parse_post({"text": "无 id"}, "1") is None


@pytest.mark.parametrize(
    "card,expect",
    [
        ({"mblogtype": 2}, True),
        ({"isTop": True}, True),
        ({}, False),
        ({"mblogtype": 0}, False),
    ],
)
def test_is_pinned(fetcher, card, expect):
    assert fetcher._is_pinned(card, {}) is expect


# ---------------- 时间线容错 ----------------


async def test_timeline_tolerates_null_slots_and_non_post_cards(fetcher, monkeypatch):
    """cards 里的 null 槽位与非微博卡片（card_type != 9）必须跳过而不是抛异常。"""

    async def fake_api_get(url, params, referer=None):
        return {
            "ok": 1,
            "data": {
                "cards": [
                    None,
                    {"card_type": 11, "title": "热搜"},
                    {"card_type": 9, "mblog": _mblog()},
                    {"card_type": 9, "mblog": None},
                    {"card_type": 9, "mblog": _mblog(id="2", mblogtype=2)},  # 置顶
                ]
            },
        }

    monkeypatch.setattr(fetcher, "_api_get", fake_api_get)
    posts = await fetcher._fetch_timeline_page("6902546888", 1)
    assert [p.id for p in posts] == ["5354957523084"]


async def test_fetch_timeline_pages_until_known_id(fetcher, monkeypatch):
    """翻页补漏：整页未见时继续翻，遇到已见过的 id 即停。"""
    pages: dict[int, list] = {
        1: [fetcher._parse_post(_mblog(id="10"), "1"), fetcher._parse_post(_mblog(id="9"), "1")],
        2: [fetcher._parse_post(_mblog(id="8"), "1"), fetcher._parse_post(_mblog(id="7"), "1")],
        3: [fetcher._parse_post(_mblog(id="6"), "1")],
    }

    async def fake_page(uid, page):
        return pages.get(page, [])

    monkeypatch.setattr(fetcher, "_fetch_timeline_page", fake_page)
    posts = await fetcher.fetch_timeline("1", known_ids={"7"})
    assert [p.id for p in posts] == ["10", "9", "8", "7"]  # 第 2 页出现已知 id 后停止
    posts_baseline = await fetcher.fetch_timeline("1", known_ids=None)
    assert [p.id for p in posts_baseline] == ["10", "9"]  # 建基线轮不翻页


async def test_later_page_failure_keeps_first_page(fetcher, monkeypatch):
    """翻页失败不能丢掉本轮第一页已取到的微博。

    实测 m.weibo.cn 对 page>=2 一律回 ok=-100（接口要求登录）。若让异常冒泡，
    第一页那批新微博会连同本轮一起丢，而且因为没被标记已见，下一轮还会重演、
    形成永远失败的循环。
    """
    first_page = [fetcher._parse_post(_mblog(id="10"), "1")]

    async def fake_page(uid, page):
        if page == 1:
            return first_page
        raise WeiboFetchError("接口要求登录（ok=-100）")

    monkeypatch.setattr(fetcher, "_fetch_timeline_page", fake_page)
    posts = await fetcher.fetch_timeline("1", known_ids={"999"})  # 第一页没有已知 id → 会翻页
    assert [p.id for p in posts] == ["10"]


async def test_first_page_failure_still_raises(fetcher, monkeypatch):
    """第一页失败是真故障（风控/网络），必须抛出去让轮询进入退避。"""

    async def fake_page(uid, page):
        raise WeiboFetchError("请求失败: boom")

    monkeypatch.setattr(fetcher, "_fetch_timeline_page", fake_page)
    with pytest.raises(WeiboFetchError):
        await fetcher.fetch_timeline("1", known_ids={"1"})


# ---------------- 风控与身份 ----------------


async def test_html_response_is_treated_as_risk(fetcher, monkeypatch):
    fetcher._visitor_cookies = {"SUB": "x"}  # 身份在位，避免走进续期分支

    async def fake_get_text(url, params, headers, collect=None):
        return "<html>访问过于频繁</html>"

    monkeypatch.setattr(fetcher, "_get_text", fake_get_text)
    with pytest.raises(WeiboRiskError):
        await fetcher._api_get_once("https://m.weibo.cn/api/container/getIndex", {}, None)


async def test_ok_minus_100_raises_auth_error(fetcher, monkeypatch):
    async def fake_get_text(url, params, headers, collect=None):
        return '{"ok": -100, "msg": "请先登录"}'

    monkeypatch.setattr(fetcher, "_get_text", fake_get_text)
    fetcher._visitor_cookies = {"SUB": "x"}
    with pytest.raises(WeiboAuthError):
        await fetcher._api_get_once("https://m.weibo.cn/api/container/getIndex", {}, None)


async def test_expired_custom_cookie_gives_actionable_error(log, monkeypatch):
    f = WeiboFetcher({"custom_cookie": "SUB=stale"}, log)
    f._http = object()

    async def fake_get_text(url, params, headers, collect=None):
        return '{"ok": -100}'

    monkeypatch.setattr(f, "_get_text", fake_get_text)
    with pytest.raises(WeiboFetchError, match="自定义 Cookie 已失效"):
        await f._api_get_once("https://m.weibo.cn/api/container/getIndex", {}, None)


async def test_risk_cooldown_blocks_further_requests(fetcher, monkeypatch):
    fetcher._visitor_cookies = {"SUB": "x"}

    async def fake_get_text(url, params, headers, collect=None):
        raise WeiboRiskError("HTTP 418")

    async def fake_renew(force: bool = False) -> None:
        return None

    monkeypatch.setattr(fetcher, "_get_text", fake_get_text)
    monkeypatch.setattr(fetcher, "_renew_visitor", fake_renew)
    with pytest.raises(WeiboRiskError):
        await fetcher._api_get("https://m.weibo.cn/api/container/getIndex", {})

    cooling, remaining = fetcher.is_cooling_down()
    assert cooling is True and remaining > 0
    # 冷却期内不再向微博出网，直接快速失败
    with pytest.raises(WeiboFetchError, match="风控冷却中"):
        await fetcher._api_get("https://m.weibo.cn/api/container/getIndex", {})


async def test_risk_escalates_then_resets(fetcher):
    """连续触发风控逐级升级冷却，长时间未触发后梯度重置。"""
    for _ in range(3):
        fetcher._blocked_until = 0.0  # 模拟冷却结束
        fetcher._mark_risk_blocked(RuntimeError("418"))
    assert fetcher._blocked_until - time.time() > 3500  # 已升到 60 分钟档

    fetcher._last_risk_ts = time.time() - 3 * 3600  # 距上次触发远超重置窗
    fetcher._blocked_until = 0.0
    fetcher._mark_risk_blocked(RuntimeError("418"))
    assert fetcher._risk_level == 1  # 梯度已重置


async def test_url_whitelist_rejects_foreign_host(fetcher):
    with pytest.raises(WeiboFetchError, match="白名单"):
        await fetcher._validate_url("https://evil.example.com/x")
    with pytest.raises(WeiboFetchError, match="非 http"):
        await fetcher._validate_url("file:///etc/passwd")


async def test_custom_cookie_skips_visitor_renewal(log, monkeypatch):
    """配了自定义 Cookie 就不再触发游客身份流程。"""
    f = WeiboFetcher({"custom_cookie": "SUB=mine"}, log)
    f._http = object()
    called = False

    async def fail_renew(force=False):
        nonlocal called
        called = True

    monkeypatch.setattr(f, "_renew_visitor", fail_renew)

    async def fake_get_text(url, params, headers, collect=None):
        return '{"ok": 1, "data": {}}'

    monkeypatch.setattr(f, "_get_text", fake_get_text)
    assert await f._api_get_once("https://m.weibo.cn/api/container/getIndex", {}, None) == {
        "ok": 1,
        "data": {},
    }
    assert called is False
