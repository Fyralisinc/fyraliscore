"""Simple token-bucket rate limiter.

`TokenBucket` is safe for single-threaded async use. It refills at a constant
rate in requests-per-minute and blocks `acquire()` when empty. A monotonic
clock source is injectable so tests can run without real sleeps.
"""

from __future__ import annotations

import asyncio
import time as _time_module
from collections.abc import Awaitable, Callable


class TokenBucket:
    """Token-bucket limiter measured in requests per minute."""

    def __init__(
        self,
        rate_per_minute: float = 50.0,
        capacity: int | None = None,
        time_fn: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be positive")
        self._rate_per_sec = rate_per_minute / 60.0
        self._capacity = float(capacity if capacity is not None else rate_per_minute)
        self._tokens = self._capacity
        self._time_fn = time_fn or _time_module.monotonic
        self._sleep_fn = sleep_fn or asyncio.sleep
        self._last = self._time_fn()

    def _refill(self) -> None:
        now = self._time_fn()
        delta = now - self._last
        if delta <= 0:
            return
        self._tokens = min(
            self._capacity, self._tokens + delta * self._rate_per_sec
        )
        self._last = now

    async def acquire(self, n: float = 1.0) -> None:
        if n <= 0:
            return
        while True:
            self._refill()
            if self._tokens >= n:
                self._tokens -= n
                return
            needed = n - self._tokens
            wait = needed / self._rate_per_sec
            await self._sleep_fn(wait)

    @property
    def tokens(self) -> float:
        self._refill()
        return self._tokens


__all__ = ["TokenBucket"]
