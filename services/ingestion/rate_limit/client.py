"""Async client for the Lua token-bucket rate limiter.

Per ingestion LLD §13. The two Lua scripts are loaded once via
SCRIPT LOAD and then invoked with EVALSHA on every request — this
amortises the network round-trip for the script text away after the
first call.

Threading: a single `RateLimiter` instance is safe under asyncio
concurrency. The Lua atomicity guarantee on Redis ensures concurrent
acquires from a single bucket serialise correctly (test:
`test_rate_limiter_concurrent_acquires_serialize`).
"""
from __future__ import annotations

import pathlib
import time
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis


_SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent / "scripts"
_ACQUIRE_SCRIPT_PATH = _SCRIPTS_DIR / "acquire.lua"
_REPORT_SCRIPT_PATH = _SCRIPTS_DIR / "report_retry_after.lua"


@dataclass(frozen=True, slots=True)
class AcquireResult:
    """Return shape of `RateLimiter.acquire`.

    `granted`           — True if the bucket had capacity for the cost.
    `tokens_remaining`  — tokens left after deduction (or current
                          level if denied). Float because refill is
                          fractional.
    `retry_after_ms`    — for denials, milliseconds until the bucket
                          can serve `cost` tokens (or until lockout
                          expires, whichever is larger). 0 on grant.
    """

    granted: bool
    tokens_remaining: float
    retry_after_ms: int


class RateLimiter:
    """Per ingestion LLD §13. Holds one Redis client; loads Lua scripts
    lazily on first use and caches their SHAs.
    """

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._acquire_sha: str | None = None
        self._report_sha: str | None = None

    async def _load_scripts(self) -> None:
        if self._acquire_sha and self._report_sha:
            return
        acquire_src = _ACQUIRE_SCRIPT_PATH.read_text()
        report_src = _REPORT_SCRIPT_PATH.read_text()
        self._acquire_sha = await self._redis.script_load(acquire_src)
        self._report_sha = await self._redis.script_load(report_src)

    async def acquire(
        self,
        bucket_key: str,
        *,
        capacity: int,
        refill_per_sec: float,
        cost: int = 1,
    ) -> AcquireResult:
        """Attempt to consume `cost` tokens from `bucket_key`.

        Pure passthrough to acquire.lua. The Lua script is the
        authority on lockout + token math; this method only converts
        the return tuple into a typed dataclass.
        """
        await self._load_scripts()
        assert self._acquire_sha is not None  # for type-checker
        now_ms = int(time.time() * 1000)
        raw: Any = await self._redis.evalsha(
            self._acquire_sha,
            1,                  # numkeys
            bucket_key,         # KEYS[1]
            now_ms,             # ARGV[1]
            capacity,           # ARGV[2]
            refill_per_sec,     # ARGV[3]
            cost,               # ARGV[4]
        )
        return AcquireResult(
            granted=bool(raw[0]),
            tokens_remaining=float(raw[1]),
            retry_after_ms=int(raw[2]),
        )

    async def report_retry_after(
        self,
        bucket_key: str,
        retry_after_ms: int,
    ) -> None:
        """Record a `Retry-After`-driven lockout on `bucket_key`.

        Per LLD §13: when a source returns 429, the caller passes the
        upstream `Retry-After` value here. Subsequent `acquire` calls
        deny until the lockout expires, regardless of token math.
        """
        await self._load_scripts()
        assert self._report_sha is not None
        now_ms = int(time.time() * 1000)
        await self._redis.evalsha(
            self._report_sha,
            1,
            bucket_key,
            now_ms,
            retry_after_ms,
        )


__all__ = ["AcquireResult", "RateLimiter"]
