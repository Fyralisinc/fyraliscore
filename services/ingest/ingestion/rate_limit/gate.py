"""Per-(source, method) rate-limit gate for the backfill fetch path.

Wires the M1.3 `RateLimiter` (LLD §13) into ShardFetch's fetch loop so
every upstream page fetch acquires a token from the (source, method)
token bucket BEFORE the call — the missing *FetchPage* half of the rate
limiter, which until now only gated the embedding backlog (Ollama).

============================================================
WHAT THIS CLOSES
============================================================
`BUCKET_DEFAULTS` (LLD §13's published per-(source, method) budgets)
had zero non-test importers and `FetchPage` never called `.acquire()`.
This module is that importer: `FetchRateLimiter.acquire()` looks the
bucket up in `BUCKET_DEFAULTS` and consumes one token per page fetch.

============================================================
SCOPE — CONFIGURED SOURCES ONLY (no fabricated budgets)
============================================================
Only the four sources with a published budget in `BUCKET_DEFAULTS`
(slack / github / gmail / discord) are throttled. A source with no
entry in `PRIMARY_FETCH_METHOD` — or no matching `BUCKET_DEFAULTS`
row — is a **pass-through**: no bucket exists, so the gate does not
invent a limit for it. Adding a new source's throttle is a one-line
edit to `PRIMARY_FETCH_METHOD` here plus its `BUCKET_DEFAULTS` row.

The cost is one token per fetcher invocation. One fetcher call == one
upstream page request, so "one token before each upstream call" holds
at page granularity — the unit the bucket capacities were sized for.

============================================================
DENIAL / EXHAUSTION BEHAVIOUR
============================================================
On a denial the gate waits the bucket-reported `retry_after_ms` (or a
short recheck interval for the `-1` indefinite-lockout sentinel — the
operator pause / zero-refill state) and retries, up to a bounded
`max_wait_seconds`. Crossing that bound raises `RateLimitWaitExceeded`,
which ShardFetch treats as a clean fetch-loop exit (same shape as a
`CursorAdvanceFlushFailure`): the shard stays `in_progress` and the
orphan scan resumes it on a later tick, by which point the bucket has
refilled or the operator has lifted the pause. The bound also caps how
long one rate-limited shard can hold a tick, so it cannot starve the
orphan scan or the other claimed signals indefinitely.

Reporting an upstream `Retry-After` (429) lockout via
`RateLimiter.report_retry_after` stays the per-source fetcher's
concern — it is the one that sees the 429 status — exactly as the
fetch loop's docstring already assigns source-specific retry handling
to the fetchers.
"""
from __future__ import annotations

import asyncio
import time

from services.ingest.ingestion.rate_limit.buckets import BUCKET_DEFAULTS
from services.ingest.ingestion.rate_limit.client import RateLimiter


# Source -> the method string its page fetcher uses, matching a
# `BUCKET_DEFAULTS[(source, method)]` key. Sources absent here are
# pass-through (not throttled) — see the module docstring's SCOPE note.
#   slack    : conversations.history is the page-fetch call
#              (conversations.list / users.info buckets gate planner
#              paths, not the page loop).
#   github   : one logical bucket per app — rest_authenticated.
#   gmail    : Gmail's per-user quota.
#   discord  : channels.messages history reads.
PRIMARY_FETCH_METHOD: dict[str, str] = {
    "slack":   "conversations.history",
    "github":  "rest_authenticated",
    "gmail":   "per-user",
    "discord": "channels_messages",
}

# Defaults tuned so a single rate-limited shard cannot wedge a tick
# forever. SENTINEL recheck mirrors the embedding backlog's value so a
# QPS/refill bump takes effect within ~1s of an operator change.
DEFAULT_MAX_WAIT_SECONDS = 30.0
DEFAULT_SENTINEL_RECHECK_SEC = 1.0


def fetch_bucket_key(tenant_id: object, source: str, method: str) -> str:
    """`rate:<tenant>:<source>:<method>` — the key scheme `acquire.lua`
    documents and the embedding backlog already uses for its
    `rate:*system:ollama:embed` bucket."""
    return f"rate:{tenant_id}:{source}:{method}"


class RateLimitWaitExceeded(Exception):
    """Raised when a bucket stays empty past `max_wait_seconds`.

    ShardFetch catches this as a *transient* — it exits the fetch loop
    without marking the shard done/failed, leaving it `in_progress` for
    the orphan scan to resume (same recovery shape as a flush failure).
    """

    def __init__(
        self, *, source: str, method: str, bucket_key: str,
        waited_seconds: float,
    ) -> None:
        self.source = source
        self.method = method
        self.bucket_key = bucket_key
        self.waited_seconds = waited_seconds
        super().__init__(
            f"rate-limit bucket {bucket_key!r} for "
            f"(source={source!r}, method={method!r}) stayed empty for "
            f">{waited_seconds:.1f}s; exiting fetch loop for retry."
        )


class FetchRateLimiter:
    """Acquires one token per page fetch from the (source, method) bucket.

    Holds one `RateLimiter` (one Redis client). Safe under asyncio
    concurrency — the Lua bucket serialises concurrent acquires.
    """

    def __init__(
        self,
        limiter: RateLimiter,
        *,
        max_wait_seconds: float = DEFAULT_MAX_WAIT_SECONDS,
        sentinel_recheck_sec: float = DEFAULT_SENTINEL_RECHECK_SEC,
    ) -> None:
        self._limiter = limiter
        self._max_wait_seconds = max_wait_seconds
        self._sentinel_recheck_sec = sentinel_recheck_sec

    @staticmethod
    def method_for(source: str) -> str | None:
        """The throttled method for `source`, or None if pass-through."""
        return PRIMARY_FETCH_METHOD.get(source)

    async def acquire(self, *, source: str, tenant_id: object) -> bool:
        """Consume one token for `source`'s primary fetch method.

        Returns True once a token is granted, or immediately for a
        pass-through source (no `BUCKET_DEFAULTS` budget). Raises
        `RateLimitWaitExceeded` if the bucket stays empty past
        `max_wait_seconds`.
        """
        method = PRIMARY_FETCH_METHOD.get(source)
        if method is None:
            return True
        spec = BUCKET_DEFAULTS.get((source, method))
        if spec is None:
            # Configured method with no budget row — treat as
            # pass-through rather than guessing a capacity.
            return True

        key = fetch_bucket_key(tenant_id, source, method)
        deadline = time.monotonic() + self._max_wait_seconds
        while True:
            result = await self._limiter.acquire(
                key,
                capacity=spec.capacity,
                refill_per_sec=spec.refill_per_sec,
            )
            if result.granted:
                return True

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RateLimitWaitExceeded(
                    source=source, method=method, bucket_key=key,
                    waited_seconds=self._max_wait_seconds,
                )
            # -1 is the indefinite-lockout sentinel (zero refill / empty,
            # or an operator pause). Re-poll on a short cadence so a
            # config bump takes effect promptly without thrashing Redis.
            if result.retry_after_ms == -1:
                wait = self._sentinel_recheck_sec
            else:
                wait = max(0.001, result.retry_after_ms / 1000.0)
            await asyncio.sleep(min(wait, remaining))


__all__ = [
    "DEFAULT_MAX_WAIT_SECONDS",
    "DEFAULT_SENTINEL_RECHECK_SEC",
    "FetchRateLimiter",
    "PRIMARY_FETCH_METHOD",
    "RateLimitWaitExceeded",
    "fetch_bucket_key",
]
