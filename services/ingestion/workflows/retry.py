"""services/ingestion/workflows/retry.py
   — Named retry helpers for M6 asyncio workflow services.

Per [04-implementation-plan.md §M6 pattern-alignment requirement #3]
(retry-logic-in-named-functions). Inline `try/except` retry loops in
workflow service modules are forbidden by the pattern-alignment
static analyzer (M6.0 Phase 3); use the helpers below or add a new
helper here.

============================================================
WHY NAMED HELPERS, NOT CLASSES
============================================================
Each helper is a single async function. Reasons:
  - Temporal portability: Temporal's retry policies are declarative
    function-call-level configuration (RetryPolicy(...)). Named
    helpers map 1:1 to those policies when the [A11 trigger conditions]
    (../../../docs/ingestion/05-lld-amendments.md) fire and Temporal
    arrives. A class hierarchy doesn't.
  - Statelessness: each helper holds no state across attempts. A
    SIGTERM-restart during attempt 3 doesn't lose retry state because
    there IS no retry state to lose — the caller's workflow state
    row records "we're attempting this operation"; the helper just
    does the next attempt.
  - Logging consistency: every attempt emits structured logs with
    `attempt_number`, `error_class`, `will_retry`, and
    `next_delay_seconds`. This is what A11 trigger #3 (multi-day
    debugging session — "no introspectable history of decisions")
    names as the missing introspection; named helpers give every
    workflow service the same baseline.

============================================================
PATTERN-ALIGNMENT EXEMPTION
============================================================
This module is one of the substrate modules. It does NOT import
asyncpg or Kafka; it's pure retry math. The pattern-alignment static
analyzer treats it as a substrate-allowed module per Rule 1's
allowlist.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Awaitable, Callable, TypeVar


log = logging.getLogger(__name__)


T = TypeVar("T")


# Public type alias for callsite readability.
RetryableFn = Callable[[], Awaitable[T]]


def _log_attempt(
    *,
    helper_name: str,
    attempt: int,
    max_attempts: int,
    error: BaseException,
    will_retry: bool,
    next_delay_seconds: float | None,
) -> None:
    """Single log shape across every helper so future incident
    investigation has consistent fields to grep."""
    log.info(
        f"workflow.retry.{helper_name}",
        extra={
            "helper": helper_name,
            "attempt_number": attempt,
            "max_attempts": max_attempts,
            "error_class": type(error).__name__,
            "error_message": str(error)[:200],
            "will_retry": will_retry,
            "next_delay_seconds": next_delay_seconds,
        },
    )


async def retry_with_backoff_on_429(
    fn: RetryableFn[T],
    retry_on: type[BaseException] | tuple[type[BaseException], ...],
    *,
    max_attempts: int = 5,
    base_delay_seconds: float = 1.0,
    max_delay_seconds: float = 60.0,
) -> T:
    """Retry `fn` on rate-limit-shaped exceptions with exponential
    backoff. Each attempt's delay is `base_delay * 2 ** (attempt-1)`,
    clamped to `max_delay_seconds`.

    `retry_on` is the exception class (or tuple of classes) that
    indicates "this was a rate-limit-shaped error and should be
    retried." Anything else propagates immediately.

    Total worst-case wall-clock = sum(base * 2^k for k in range(max_attempts-1)).
    With defaults (max_attempts=5, base=1, max=60): 1 + 2 + 4 + 8 = 15s
    of sleeping before the 5th attempt; total deadline depends on the
    callable's own latency.

    Logs `attempt_number`, `error_class`, `will_retry`,
    `next_delay_seconds` per attempt.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return await fn()
        except retry_on as exc:
            is_last = attempt == max_attempts
            delay = (
                None if is_last
                else min(
                    base_delay_seconds * (2 ** (attempt - 1)),
                    max_delay_seconds,
                )
            )
            _log_attempt(
                helper_name="retry_with_backoff_on_429",
                attempt=attempt, max_attempts=max_attempts,
                error=exc, will_retry=not is_last,
                next_delay_seconds=delay,
            )
            if is_last:
                raise
            assert delay is not None
            await asyncio.sleep(delay)
    # Unreachable: max_attempts >= 1 guarantees the loop returns or raises.
    raise RuntimeError("retry_with_backoff_on_429 fell through; bug")


async def retry_with_jitter_on_5xx(
    fn: RetryableFn[T],
    retry_on: type[BaseException] | tuple[type[BaseException], ...],
    *,
    max_attempts: int = 5,
    base_delay_seconds: float = 0.5,
    jitter_range_seconds: float = 0.5,
) -> T:
    """Retry `fn` on server-error-shaped exceptions with linear backoff
    plus uniform random jitter.

    Each attempt's delay is `base_delay + uniform(0, jitter_range)`.
    Jitter prevents synchronised retry storms when many workflows
    hit the same upstream 5xx simultaneously.

    Total worst-case wall-clock (max_attempts=5, base=0.5, jitter=0.5):
    average ~0.75 * 4 = 3s of sleeping before the 5th attempt.

    Logs `attempt_number`, `error_class`, `will_retry`,
    `next_delay_seconds` (including the realised jitter) per attempt.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return await fn()
        except retry_on as exc:
            is_last = attempt == max_attempts
            delay = (
                None if is_last
                else base_delay_seconds + random.uniform(0.0, jitter_range_seconds)
            )
            _log_attempt(
                helper_name="retry_with_jitter_on_5xx",
                attempt=attempt, max_attempts=max_attempts,
                error=exc, will_retry=not is_last,
                next_delay_seconds=delay,
            )
            if is_last:
                raise
            assert delay is not None
            await asyncio.sleep(delay)
    raise RuntimeError("retry_with_jitter_on_5xx fell through; bug")


async def retry_indefinitely_on_transient(
    fn: RetryableFn[T],
    transient_errors: type[BaseException] | tuple[type[BaseException], ...],
    *,
    base_delay_seconds: float = 1.0,
    max_delay_seconds: float = 30.0,
    max_elapsed_seconds: float | None = None,
) -> T:
    """Retry `fn` forever (or until `max_elapsed_seconds`) on
    transient exceptions, with exponential-up-to-cap backoff.

    Use for operations whose business meaning is "this MUST succeed
    eventually and we have no recourse if it doesn't" — e.g. Postgres
    writes during recovery from a network blip. The caller's workflow
    state row encodes the "still trying" status; a SIGTERM during
    retry leaves the state row at the pre-attempt cursor, so the
    restarted worker resumes the same attempt.

    When `max_elapsed_seconds` is None (default), this retries
    forever — the caller is responsible for shutting the service
    down via SIGTERM if "forever" is wrong for them.

    Logs `attempt_number`, `error_class`, `will_retry`,
    `next_delay_seconds`, `elapsed_seconds` per attempt.
    """
    start = time.monotonic()
    attempt = 0
    while True:
        attempt += 1
        try:
            return await fn()
        except transient_errors as exc:
            elapsed = time.monotonic() - start
            will_retry = (
                max_elapsed_seconds is None
                or elapsed < max_elapsed_seconds
            )
            delay = min(
                base_delay_seconds * (2 ** min(attempt - 1, 16)),
                max_delay_seconds,
            ) if will_retry else None
            log.info(
                "workflow.retry.retry_indefinitely_on_transient",
                extra={
                    "helper": "retry_indefinitely_on_transient",
                    "attempt_number": attempt,
                    "max_attempts": None,
                    "max_elapsed_seconds": max_elapsed_seconds,
                    "elapsed_seconds": round(elapsed, 3),
                    "error_class": type(exc).__name__,
                    "error_message": str(exc)[:200],
                    "will_retry": will_retry,
                    "next_delay_seconds": delay,
                },
            )
            if not will_retry:
                raise
            assert delay is not None
            await asyncio.sleep(delay)


__all__ = [
    "RetryableFn",
    "retry_indefinitely_on_transient",
    "retry_with_backoff_on_429",
    "retry_with_jitter_on_5xx",
]
