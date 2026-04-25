"""services/gateway/rate_limit.py — in-process token-bucket rate limiter.

BUILD-PLAN §3 Prompt 2.A:
    "Rate limit middleware: per-tenant token bucket in Redis (or in-proc
     dict for Wave 2). 100 req/min default, configurable."

ARCHITECTURE §13 "Rate limits":
    - Signal ingestion: 1000/minute per actor (burst 2000)
    - Queries: 300/minute per actor
    - Explicit Think triggers: 10/minute per actor
    - WebSocket connections: 5 concurrent per actor

Wave 2-A scope:
- Redis is deferred to Wave 5 per the prompt. This module is a pure
  in-process dict keyed on (tenant_id, actor_id) → (tokens, last_refill).
- Two separate budget tiers: `DEFAULT` (100/min) and `SIGNAL_INGEST`
  (1000/min) per §13. Consumers call `consume(key, tier)`.
- Uses `asyncio.Lock` to avoid intra-process races on the bucket map.
- Monotonic clock from `time.monotonic()` so the limiter is unaffected
  by wall-clock jumps. Tests inject a clock for determinism.
"""
from __future__ import annotations

import asyncio
import enum
import time
from dataclasses import dataclass, field
from typing import Callable


class RateTier(enum.Enum):
    """Budget tier — maps to the ARCHITECTURE §13 numbers."""

    DEFAULT = "default"           # 100 req/min (BUILD-PLAN §3 2.A)
    SIGNAL_INGEST = "signal_ingest"  # 1000 req/min (§13)


# Budgets in (capacity, refill_per_second). Capacity doubles as burst
# allowance (you can spend up to `capacity` in a burst; `refill` tops
# the bucket up over time).
_BUDGETS: dict[RateTier, tuple[float, float]] = {
    RateTier.DEFAULT: (100.0, 100.0 / 60.0),          # 100/min
    RateTier.SIGNAL_INGEST: (1000.0, 1000.0 / 60.0),   # 1000/min
}


@dataclass
class _Bucket:
    tokens: float
    last_refill: float


@dataclass
class RateLimiter:
    """In-process per-key token-bucket limiter.

    Keys are caller-controlled — Gateway middleware uses
    `(tenant_id, actor_id, tier)` tuples. The limiter is thread-safe
    within an event loop (asyncio.Lock).
    """

    clock: Callable[[], float] = field(default=time.monotonic)
    _buckets: dict[tuple, _Bucket] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def consume(
        self, key: tuple, tier: RateTier = RateTier.DEFAULT, cost: float = 1.0
    ) -> bool:
        """Try to consume `cost` tokens from the bucket keyed by
        `(key, tier)`. Returns True on success (request allowed),
        False when the bucket is empty (request rejected).
        """
        capacity, refill_per_s = _BUDGETS[tier]
        full_key = (tier, key)
        now = self.clock()
        async with self._lock:
            b = self._buckets.get(full_key)
            if b is None:
                b = _Bucket(tokens=capacity, last_refill=now)
                self._buckets[full_key] = b
            else:
                elapsed = max(0.0, now - b.last_refill)
                b.tokens = min(capacity, b.tokens + elapsed * refill_per_s)
                b.last_refill = now
            if b.tokens >= cost:
                b.tokens -= cost
                return True
            return False

    def reset(self) -> None:
        """Drop all state. Tests use this between cases."""
        self._buckets.clear()

    def budget(self, tier: RateTier) -> tuple[float, float]:
        """Return (capacity, refill_per_second) for the tier.

        Exposed so tests can assert on configured budgets without
        reaching into `_BUDGETS` directly.
        """
        return _BUDGETS[tier]


__all__ = ["RateLimiter", "RateTier"]
