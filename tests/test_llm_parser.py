"""LLM 兜底解析测试：输出解析、四道校验闸门、失败降级。

这里不触网：LLM 调用一律用注入的假 ask 函数。
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime

import pytest
from conftest import CN

from llm_parser import LLMFallback, build_user_prompt, parse_response, validate

BASE_DT = datetime(2026, 10, 17, 16, 1, tzinfo=CN)
BASE_TS = BASE_DT.timestamp()
BODY = "📢新商务官宣 / 7:28 发布骄阳似我中插广告 / 具体以官方为准"


def _item(when: str, source: str, confidence: float = 0.9) -> dict:
    return {"when": when, "source": source, "confidence": confidence}


# ---------------- 输出解析 ----------------


def test_parse_response_plain_json():
    text = json.dumps({"times": [_item("2026-10-18 07:28", "7:28")]}, ensure_ascii=False)
    assert len(parse_response(text)) == 1


def test_parse_response_with_code_fence():
    text = '```json\n{"times":[{"when":"2026-10-18 07:28","source":"7:28"}]}\n```'
    assert len(parse_response(text)) == 1


def test_parse_response_with_chatter_around_json():
    text = '好的，结果如下：{"times":[{"when":"2026-10-18 07:28","source":"7:28"}]} 以上。'
    assert len(parse_response(text)) == 1


@pytest.mark.parametrize(
    "text",
    ["", "   ", "抱歉我没有找到", "{broken json", '{"times": "not-a-list"}', "[]", "{}"],
)
def test_parse_response_garbage_returns_empty(text):
    assert parse_response(text) == []


# ---------------- 校验闸门 ----------------


def test_validate_accepts_real_time():
    out = validate([_item("2026-10-18 07:28", "7:28")], BODY, BASE_TS, BASE_TS, 60)
    assert len(out) == 1
    assert out[0].source_text == "7:28"


def test_validate_rejects_hallucinated_time():
    """闸门 4：引用不在正文里 → 丢弃（LLM 编造时间的典型形态）。"""
    assert validate([_item("2026-10-18 07:28", "10月24日晚8点")], BODY, BASE_TS, BASE_TS, 60) == []


def test_validate_rejects_empty_source():
    assert validate([_item("2026-10-18 07:28", "")], BODY, BASE_TS, BASE_TS, 60) == []


def test_validate_rejects_out_of_range_year():
    """闸门 2：年份越界（如把【1700】读成公元 1700 年）。"""
    assert validate([_item("1700-01-01 00:00", "7:28")], BODY, BASE_TS, BASE_TS, 60) == []


def test_validate_rejects_too_far_future():
    """闸门 3：超过 max_future_days。"""
    assert validate([_item("2027-06-01 10:00", "7:28")], BODY, BASE_TS, BASE_TS, 60) == []


def test_validate_rejects_time_before_post():
    """闸门 3：早于微博发布时间（容忍 5 分钟）。"""
    assert validate([_item("2026-10-17 09:00", "7:28")], BODY, BASE_TS, BASE_TS, 60) == []


def test_validate_rejects_low_confidence():
    assert validate([_item("2026-10-18 07:28", "7:28", 0.2)], BODY, BASE_TS, BASE_TS, 60) == []


def test_validate_accepts_chinese_date_format():
    body = "10月24日 20:00 直播"
    items = [_item("2026年10月24日 20:00", "10月24日 20:00")]
    assert len(validate(items, body, BASE_TS, BASE_TS, 60)) == 1


def test_validate_dedupes_and_sorts():
    body = "10月24日 20:00 直播 / 10月25日 09:00 上线"
    items = [
        _item("2026-10-25 09:00", "10月25日 09:00"),
        _item("2026-10-24 20:00", "10月24日 20:00"),
        _item("2026-10-24 20:00", "10月24日 20:00"),
    ]
    out = validate(items, body, BASE_TS, BASE_TS, 60)
    assert [c.source_text for c in out] == ["10月24日 20:00", "10月25日 09:00"]


def test_validate_whitespace_insensitive_source_match():
    """正文与引用的换行/空格差异不该被判成编造。"""
    body = "10月24日\n20:00 直播"
    assert (
        len(validate([_item("2026-10-24 20:00", "10月24日 20:00")], body, BASE_TS, BASE_TS, 60))
        == 1
    )


def test_build_user_prompt_contains_post_time_and_body():
    prompt = build_user_prompt(BODY, BASE_DT)
    assert "2026-10-17 16:01" in prompt and BODY in prompt


# ---------------- 调用与降级 ----------------


def _llm(config: dict, ask) -> LLMFallback:
    return LLMFallback(config, logging.getLogger("t"), ask=ask)


async def test_extract_returns_validated_candidates():
    async def ask(prompt, system):
        return json.dumps({"times": [_item("2026-10-18 07:28", "7:28")]}, ensure_ascii=False)

    out = await _llm({}, ask).extract(BODY, BASE_DT, BASE_TS)
    assert len(out) == 1


async def test_extract_swallows_call_errors():
    """LLM 调用异常不能让抓取循环崩，返回空并记失败次数。"""

    async def ask(prompt, system):
        raise RuntimeError("provider 挂了")

    llm = _llm({}, ask)
    assert await llm.extract(BODY, BASE_DT, BASE_TS) == []
    assert llm.fail_streak == 1


async def test_extract_timeout_counted_then_swallowed(monkeypatch):
    """超时按失败计数并返回空，不向上抛。"""

    async def ask(prompt, system):
        return "{}"

    async def fake_wait_for(coro, timeout):
        coro.close()  # 避免 "coroutine was never awaited"
        raise asyncio.TimeoutError

    monkeypatch.setattr("llm_parser.asyncio.wait_for", fake_wait_for)
    llm = _llm({"llm_timeout_seconds": 5}, ask)
    assert await llm.extract(BODY, BASE_DT, BASE_TS) == []
    assert llm.fail_streak == 1


async def test_extract_disables_after_three_failures():
    """连续 3 次失败后本次运行不再调用，避免每条微博都等超时。"""
    calls = {"n": 0}

    async def ask(prompt, system):
        calls["n"] += 1
        raise RuntimeError("boom")

    llm = _llm({}, ask)
    for _ in range(5):
        await llm.extract(BODY, BASE_DT, BASE_TS)
    assert calls["n"] == 3, "失败 3 次后应停止调用"


async def test_extract_resets_streak_after_success():
    state = {"fail": True}

    async def ask(prompt, system):
        if state["fail"]:
            raise RuntimeError("boom")
        return json.dumps({"times": [_item("2026-10-18 07:28", "7:28")]}, ensure_ascii=False)

    llm = _llm({}, ask)
    await llm.extract(BODY, BASE_DT, BASE_TS)
    assert llm.fail_streak == 1
    state["fail"] = False
    assert len(await llm.extract(BODY, BASE_DT, BASE_TS)) == 1
    assert llm.fail_streak == 0


async def test_extract_skips_when_disabled_reason_set():
    calls = {"n": 0}

    async def ask(prompt, system):
        calls["n"] += 1
        return "{}"

    llm = _llm({}, ask)
    llm.disabled_reason = "未找到可用的 LLM provider"
    assert await llm.extract(BODY, BASE_DT, BASE_TS) == []
    assert calls["n"] == 0


def test_enabled_reads_config_flag():
    assert LLMFallback.enabled({"llm_fallback": True}) is True
    assert LLMFallback.enabled({"llm_fallback": False}) is False
    assert LLMFallback.enabled({}) is False
