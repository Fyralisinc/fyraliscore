"""M6.0 Phase 1 — retry helper tests.

The pattern-alignment requirement #3 (retry-logic-in-named-functions)
exists because A11 trigger #3 ("first multi-day debugging session
where the bisected root cause is 'asyncio service had no
introspectable history of decisions'") names structured per-attempt
logging as the missing introspection. These tests assert that every
helper emits the agreed log shape — attempt_number, error_class,
will_retry, next_delay_seconds — so production incident investigation
has a uniform grep target.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from services.ingestion.workflows.retry import (
    retry_indefinitely_on_transient,
    retry_with_backoff_on_429,
    retry_with_jitter_on_5xx,
)


pytestmark = [pytest.mark.timeout(20)]


class _Rate429(Exception):
    pass


class _Server5xx(Exception):
    pass


class _TransientDB(Exception):
    pass


class _Permanent(Exception):
    pass


# =====================================================================
# 1. retry_with_backoff_on_429: per-attempt structured logs.
#    Also confirms the helper sleeps the documented backoff math.
# =====================================================================

async def test_retry_with_backoff_on_429_logs_per_attempt(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LOAD-BEARING for the introspection contract: each attempt
    emits a single structured log line with the four required fields.
    """
    # Skip the real sleeps — we only care about decisions + logs.
    sleeps: list[float] = []

    async def _fake_sleep(d: float) -> None:
        sleeps.append(d)

    monkeypatch.setattr(
        "services.ingestion.workflows.retry.asyncio.sleep", _fake_sleep,
    )

    attempts: list[int] = []

    async def _fail_then_succeed() -> str:
        attempts.append(len(attempts) + 1)
        if len(attempts) < 3:
            raise _Rate429(f"too many requests {len(attempts)}")
        return "ok"

    caplog.set_level(logging.INFO, logger="services.ingestion.workflows.retry")

    result = await retry_with_backoff_on_429(
        _fail_then_succeed,
        retry_on=_Rate429,
        max_attempts=5,
        base_delay_seconds=1.0,
        max_delay_seconds=60.0,
    )
    assert result == "ok"
    assert attempts == [1, 2, 3]

    # Two failed attempts → two log records.
    attempt_records = [
        r for r in caplog.records
        if getattr(r, "helper", None) == "retry_with_backoff_on_429"
    ]
    assert len(attempt_records) == 2, (
        f"Expected 2 attempt logs (the two failures); got "
        f"{len(attempt_records)}. The retry helper must emit one "
        f"INFO-level log per failed attempt — that's the A11 "
        f"introspection contract."
    )

    for i, rec in enumerate(attempt_records, start=1):
        assert rec.attempt_number == i
        assert rec.max_attempts == 5
        assert rec.error_class == "_Rate429"
        assert rec.will_retry is True
        # next_delay_seconds = base * 2^(attempt-1) = 1.0, 2.0
        expected = 1.0 * (2 ** (i - 1))
        assert rec.next_delay_seconds == expected, (
            f"attempt {i}: next_delay_seconds={rec.next_delay_seconds}; "
            f"expected {expected}"
        )

    # Helper actually slept the documented durations.
    assert sleeps == [1.0, 2.0]


async def test_retry_with_backoff_on_429_exhausts_and_raises(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    async def _fake_sleep(_: float) -> None:
        pass

    monkeypatch.setattr(
        "services.ingestion.workflows.retry.asyncio.sleep", _fake_sleep,
    )

    async def _always_fail() -> Any:
        raise _Rate429("nope")

    caplog.set_level(logging.INFO, logger="services.ingestion.workflows.retry")

    with pytest.raises(_Rate429):
        await retry_with_backoff_on_429(
            _always_fail, retry_on=_Rate429, max_attempts=3,
            base_delay_seconds=0.5,
        )

    # Final attempt log has will_retry=False.
    attempt_records = [
        r for r in caplog.records
        if getattr(r, "helper", None) == "retry_with_backoff_on_429"
    ]
    assert len(attempt_records) == 3
    assert attempt_records[-1].will_retry is False
    assert attempt_records[-1].next_delay_seconds is None


async def test_retry_with_backoff_on_429_non_matching_exception_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-matching exception class MUST propagate immediately
    without retry (and without per-attempt log noise)."""
    async def _fake_sleep(_: float) -> None:
        pass

    monkeypatch.setattr(
        "services.ingestion.workflows.retry.asyncio.sleep", _fake_sleep,
    )

    async def _raise_permanent() -> Any:
        raise _Permanent("bad input")

    with pytest.raises(_Permanent):
        await retry_with_backoff_on_429(
            _raise_permanent, retry_on=_Rate429, max_attempts=5,
        )


# =====================================================================
# 2. retry_with_jitter_on_5xx: log shape + jitter bounds.
# =====================================================================

async def test_retry_with_jitter_on_5xx_logs_per_attempt(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_sleep(_: float) -> None:
        pass

    monkeypatch.setattr(
        "services.ingestion.workflows.retry.asyncio.sleep", _fake_sleep,
    )

    attempts = 0

    async def _fail_twice() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise _Server5xx("503")
        return "ok"

    caplog.set_level(logging.INFO, logger="services.ingestion.workflows.retry")

    result = await retry_with_jitter_on_5xx(
        _fail_twice, retry_on=_Server5xx,
        max_attempts=5, base_delay_seconds=0.5, jitter_range_seconds=0.5,
    )
    assert result == "ok"

    recs = [
        r for r in caplog.records
        if getattr(r, "helper", None) == "retry_with_jitter_on_5xx"
    ]
    assert len(recs) == 2
    for r in recs:
        # Delay must be in [base, base+jitter] = [0.5, 1.0].
        assert 0.5 <= r.next_delay_seconds <= 1.0


# =====================================================================
# 3. retry_indefinitely_on_transient: max_elapsed_seconds bound.
# =====================================================================

async def test_retry_indefinitely_on_transient_stops_at_max_elapsed(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """The 'indefinite' helper still respects max_elapsed_seconds so
    runaway retries can't accumulate. Time is monkeypatched so the
    test doesn't actually wait."""
    clock = [0.0]

    def _monotonic() -> float:
        return clock[0]

    async def _fake_sleep(seconds: float) -> None:
        clock[0] += seconds

    monkeypatch.setattr(
        "services.ingestion.workflows.retry.time.monotonic", _monotonic,
    )
    monkeypatch.setattr(
        "services.ingestion.workflows.retry.asyncio.sleep", _fake_sleep,
    )

    async def _always_transient() -> Any:
        raise _TransientDB("conn reset")

    caplog.set_level(logging.INFO, logger="services.ingestion.workflows.retry")

    with pytest.raises(_TransientDB):
        await retry_indefinitely_on_transient(
            _always_transient,
            transient_errors=_TransientDB,
            base_delay_seconds=1.0,
            max_delay_seconds=4.0,
            max_elapsed_seconds=5.0,
        )

    recs = [
        r for r in caplog.records
        if r.message == "workflow.retry.retry_indefinitely_on_transient"
    ]
    # Must have at least one will_retry=False record — the final
    # attempt that exceeded max_elapsed.
    final = [r for r in recs if getattr(r, "will_retry", None) is False]
    assert len(final) == 1
    assert final[0].elapsed_seconds >= 5.0
