"""Tests for TK-5 — LLM error classification and retry policies.

Audit source: THINK-DESIGN-AUDIT.md §4.1.

Coverage:
  * `classify_error(exc)` for each `LLMErrorClass` via all three
    classification surfaces (subclass, HTTP status, message heuristic).
  * `RETRY_POLICIES` budgets per class.
  * `RetryPolicy.delay_for(attempt)` computation.
  * `parse_retry_after` extraction from Retry-After header.
  * Integration: a caller driving the classification + policy loop
    against a fake provider that emits rate-limit → transient → succeed.
"""
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from lib.llm.provider import (
    LLMConfig,
    LLMContentViolationError,
    LLMErrorClass,
    LLMParseError,
    LLMPermanentError,
    LLMProvider,
    LLMRateLimitError,
    LLMTimeoutError,
    LLMTransientError,
    RETRY_POLICIES,
    RetryPolicy,
    classify_error,
    parse_retry_after,
    retry_policy_for,
)


# ---------------------------------------------------------------------
# Classification — subclass path
# ---------------------------------------------------------------------

def test_classify_parse_error():
    assert classify_error(LLMParseError("p")) == LLMErrorClass.PARSE_ERROR


def test_classify_rate_limit_subclass():
    assert classify_error(LLMRateLimitError("r")) == LLMErrorClass.RATE_LIMIT


def test_classify_timeout_subclass():
    assert classify_error(LLMTimeoutError("t")) == LLMErrorClass.TIMEOUT


def test_classify_content_violation_subclass():
    assert classify_error(LLMContentViolationError("c")) == LLMErrorClass.CONTENT_VIOLATION


def test_classify_permanent_subclass():
    assert classify_error(LLMPermanentError("p")) == LLMErrorClass.PERMANENT


def test_classify_transient_subclass():
    assert classify_error(LLMTransientError("t")) == LLMErrorClass.TRANSIENT


def test_classify_asyncio_timeout():
    assert classify_error(asyncio.TimeoutError()) == LLMErrorClass.TIMEOUT


# ---------------------------------------------------------------------
# Classification — HTTP status path
# ---------------------------------------------------------------------

class _ExcWithStatus(Exception):
    def __init__(self, msg: str, status_code: int):
        super().__init__(msg)
        self.status_code = status_code


def test_classify_http_429_is_rate_limit():
    assert classify_error(_ExcWithStatus("nope", 429)) == LLMErrorClass.RATE_LIMIT


def test_classify_http_400_is_permanent():
    assert classify_error(_ExcWithStatus("bad req", 400)) == LLMErrorClass.PERMANENT


def test_classify_http_401_is_permanent():
    assert classify_error(_ExcWithStatus("auth", 401)) == LLMErrorClass.PERMANENT


def test_classify_http_500_is_transient():
    assert classify_error(_ExcWithStatus("boom", 500)) == LLMErrorClass.TRANSIENT


def test_classify_http_503_is_transient():
    assert classify_error(_ExcWithStatus("svc", 503)) == LLMErrorClass.TRANSIENT


# ---------------------------------------------------------------------
# Classification — message-text heuristics
# ---------------------------------------------------------------------

def test_classify_rate_limit_by_message():
    class Exc(Exception):
        pass
    assert classify_error(Exc("rate limit hit")) == LLMErrorClass.RATE_LIMIT


def test_classify_timeout_by_message():
    class Exc(Exception):
        pass
    assert classify_error(Exc("request timed out")) == LLMErrorClass.TIMEOUT


def test_classify_content_policy_by_message():
    class Exc(Exception):
        pass
    assert classify_error(Exc("blocked by content policy")) == LLMErrorClass.CONTENT_VIOLATION


def test_classify_unknown_defaults_to_transient():
    class Exc(Exception):
        pass
    # Unknown exception with no status, no keyword → transient (retry).
    assert classify_error(Exc("some weird error")) == LLMErrorClass.TRANSIENT


# ---------------------------------------------------------------------
# RETRY_POLICIES budgets
# ---------------------------------------------------------------------

def test_rate_limit_policy_is_5_attempts():
    p = RETRY_POLICIES[LLMErrorClass.RATE_LIMIT]
    assert p.max_attempts == 5


def test_timeout_policy_is_2_attempts():
    p = RETRY_POLICIES[LLMErrorClass.TIMEOUT]
    assert p.max_attempts == 2


def test_content_violation_is_zero_retries():
    p = RETRY_POLICIES[LLMErrorClass.CONTENT_VIOLATION]
    assert p.max_attempts == 0
    assert p.requires_prompt_change is True


def test_parse_error_is_one_retry():
    p = RETRY_POLICIES[LLMErrorClass.PARSE_ERROR]
    assert p.max_attempts == 1
    assert p.requires_prompt_change is True


def test_transient_policy_is_2_attempts():
    p = RETRY_POLICIES[LLMErrorClass.TRANSIENT]
    assert p.max_attempts == 2


def test_permanent_policy_is_zero_retries():
    p = RETRY_POLICIES[LLMErrorClass.PERMANENT]
    assert p.max_attempts == 0


def test_retry_policy_delay_for_exponential():
    p = RetryPolicy(max_attempts=3, base_delay=1.0, backoff_multiplier=2.0)
    assert p.delay_for(0) == 0.0
    assert p.delay_for(1) == 1.0
    assert p.delay_for(2) == 2.0
    assert p.delay_for(3) == 4.0


def test_retry_policy_for_matches_classify():
    assert retry_policy_for(LLMRateLimitError("r")) is RETRY_POLICIES[LLMErrorClass.RATE_LIMIT]
    assert retry_policy_for(LLMParseError("p")) is RETRY_POLICIES[LLMErrorClass.PARSE_ERROR]


# ---------------------------------------------------------------------
# parse_retry_after
# ---------------------------------------------------------------------

class _Resp:
    def __init__(self, headers: dict[str, str]):
        self.headers = headers


class _ExcWithResp(Exception):
    def __init__(self, headers: dict[str, str]):
        super().__init__("rl")
        self.response = _Resp(headers)


def test_parse_retry_after_integer_seconds():
    exc = _ExcWithResp({"Retry-After": "30"})
    assert parse_retry_after(exc) == 30.0


def test_parse_retry_after_float_seconds():
    exc = _ExcWithResp({"Retry-After": "1.5"})
    assert parse_retry_after(exc) == 1.5


def test_parse_retry_after_missing_returns_default():
    exc = _ExcWithResp({})
    assert parse_retry_after(exc) == 1.0


def test_parse_retry_after_no_headers_returns_default():
    class Exc(Exception):
        pass
    assert parse_retry_after(Exc("x")) == 1.0


def test_parse_retry_after_bogus_returns_default():
    exc = _ExcWithResp({"Retry-After": "Thu, 21 Dec 2023 12:00:00 GMT"})
    # HTTP-date form not parsed — default.
    assert parse_retry_after(exc) == 1.0


# ---------------------------------------------------------------------
# Integration — caller loop driven by classify + policy
# ---------------------------------------------------------------------

class _Seq(LLMProvider):
    """Provider that returns a sequence of responses / raises."""

    def __init__(self, items: list[Any]):
        super().__init__(LLMConfig(
            provider="anthropic", api_key="k", model="m", max_retries=0,
        ))
        self.items = list(items)
        self.calls = 0

    async def _raw_call(self, **kw):
        self.calls += 1
        nxt = self.items.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        return nxt


async def _run_with_policy(provider: _Seq) -> str | None:
    """Minimal caller that drives classify + policy until success or budget exhausted.

    Returns the raw string on success, raises last exception otherwise.
    """
    attempts_per_class: dict[LLMErrorClass, int] = {}
    last_exc: BaseException | None = None
    # Hard ceiling to prevent test bugs from looping forever.
    for _ in range(20):
        try:
            return await provider._raw_call(
                system="s", user="u", temperature=0.0,
                max_tokens=10, schema_hint="{}",
            )
        except BaseException as e:
            last_exc = e
            cls = classify_error(e)
            attempts_per_class[cls] = attempts_per_class.get(cls, 0) + 1
            policy = RETRY_POLICIES[cls]
            if attempts_per_class[cls] > policy.max_attempts:
                raise
            await asyncio.sleep(0)
    # Ceiling hit — bubble last error.
    raise last_exc  # type: ignore[misc]


async def test_rate_limit_retries_then_succeeds():
    # 3 rate-limit errors (within the 5-attempt budget) then success.
    p = _Seq([
        LLMRateLimitError("slow"),
        LLMRateLimitError("slow"),
        LLMRateLimitError("slow"),
        "ok",
    ])
    out = await _run_with_policy(p)
    assert out == "ok"
    assert p.calls == 4


async def test_content_violation_does_not_retry():
    p = _Seq([LLMContentViolationError("blocked")])
    with pytest.raises(LLMContentViolationError):
        await _run_with_policy(p)
    # Zero retries → exactly one call.
    assert p.calls == 1


async def test_transient_retries_twice_then_succeeds():
    p = _Seq([
        LLMTransientError("blip"),
        LLMTransientError("blip"),
        "ok",
    ])
    out = await _run_with_policy(p)
    assert out == "ok"
    assert p.calls == 3


async def test_transient_exhausts_budget():
    # 3 transients in a row → first retry #1, #2, third is #3 which is > budget.
    p = _Seq([
        LLMTransientError("blip"),
        LLMTransientError("blip"),
        LLMTransientError("blip"),
    ])
    with pytest.raises(LLMTransientError):
        await _run_with_policy(p)
    # Each failure is counted; once > max_attempts the caller re-raises.
    # With max_attempts=2 that's exactly 3 calls (initial + 2 retries).
    assert p.calls == 3


async def test_permanent_error_does_not_retry():
    p = _Seq([_ExcWithStatus("bad req", 400)])
    with pytest.raises(_ExcWithStatus):
        await _run_with_policy(p)
    assert p.calls == 1


async def test_timeout_retries_then_succeeds():
    p = _Seq([
        LLMTimeoutError("slow"),
        LLMTimeoutError("slow"),
        "ok",
    ])
    out = await _run_with_policy(p)
    assert out == "ok"
    assert p.calls == 3
