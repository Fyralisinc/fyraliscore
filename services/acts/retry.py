"""
services/acts/retry.py — small deadlock-retry shim for transactional
write paths in the Acts store.

Under contention, PostgreSQL may raise DeadlockDetectedError when two
transactions acquire locks in conflicting orders. Per ARCHITECTURE-FINAL
§3 state transitions on Commitments and Goals are advisory-lockable —
but for Wave 1 we wrap writes with a short retry loop. Default: up to
3 attempts with linear 50ms backoff.

Callers: goals.create/transition, commitments.create/transition,
decisions.create/transition. Read-only helpers do not retry.
"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, TypeVar

import asyncpg


T = TypeVar("T")


async def with_deadlock_retry(
    fn: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 3,
    initial_backoff_ms: float = 25.0,
) -> T:
    """
    Run `fn()` inside a retry loop that catches asyncpg's
    DeadlockDetectedError and SerializationError. Any other exception
    (including InvariantViolation) propagates immediately.
    """
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return await fn()
        except (
            asyncpg.exceptions.DeadlockDetectedError,
            asyncpg.exceptions.SerializationError,
        ) as exc:
            last_exc = exc
            if attempt == max_attempts - 1:
                break
            await asyncio.sleep(
                (initial_backoff_ms / 1000.0) * (attempt + 1)
            )
    assert last_exc is not None  # for type-checker
    raise last_exc


__all__ = ["with_deadlock_retry"]
