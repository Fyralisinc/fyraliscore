"""Async client for the Lua token-bucket rate limiter.

Per ingestion LLD §13. The Lua scripts are loaded once via
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
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Callable

from redis.asyncio import Redis


_SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent / "scripts"
_ACQUIRE_SCRIPT_PATH = _SCRIPTS_DIR / "acquire.lua"
_ACQUIRE_MANY_SCRIPT_PATH = _SCRIPTS_DIR / "acquire_many.lua"
_ACQUIRE_MANY_GUARDED_SCRIPT_PATH = (
    _SCRIPTS_DIR / "acquire_many_guarded.lua"
)
_REPORT_SCRIPT_PATH = _SCRIPTS_DIR / "report_retry_after.lua"
_CIRCUIT_FAILURE_SCRIPT_PATH = _SCRIPTS_DIR / "circuit_failure.lua"
_CIRCUIT_SUCCESS_SCRIPT_PATH = _SCRIPTS_DIR / "circuit_success.lua"


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


@dataclass(frozen=True, slots=True)
class BucketRequirement:
    bucket_key: str
    capacity: int
    refill_per_sec: float
    cost: int = 1


@dataclass(frozen=True, slots=True)
class MultiAcquireResult:
    granted: bool
    blocked_index: int | None
    retry_after_ms: int


@dataclass(frozen=True, slots=True)
class GuardedAcquireResult:
    granted: bool
    blocked_index: int | None
    retry_after_ms: int
    circuit_open: bool


class RateLimiter:
    """Per ingestion LLD §13. Holds one Redis client; loads Lua scripts
    lazily on first use and caches their SHAs.
    """

    def __init__(
        self,
        redis: Redis,
        *,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._redis = redis
        self._now = now
        self._acquire_sha: str | None = None
        self._acquire_many_sha: str | None = None
        self._acquire_many_guarded_sha: str | None = None
        self._report_sha: str | None = None
        self._circuit_failure_sha: str | None = None
        self._circuit_success_sha: str | None = None

    async def _load_scripts(self) -> None:
        if all(
            (
                self._acquire_sha,
                self._acquire_many_sha,
                self._acquire_many_guarded_sha,
                self._report_sha,
                self._circuit_failure_sha,
                self._circuit_success_sha,
            )
        ):
            return
        acquire_src = _ACQUIRE_SCRIPT_PATH.read_text()
        acquire_many_src = _ACQUIRE_MANY_SCRIPT_PATH.read_text()
        acquire_many_guarded_src = (
            _ACQUIRE_MANY_GUARDED_SCRIPT_PATH.read_text()
        )
        report_src = _REPORT_SCRIPT_PATH.read_text()
        circuit_failure_src = _CIRCUIT_FAILURE_SCRIPT_PATH.read_text()
        circuit_success_src = _CIRCUIT_SUCCESS_SCRIPT_PATH.read_text()
        self._acquire_sha = await self._redis.script_load(acquire_src)
        self._acquire_many_sha = await self._redis.script_load(acquire_many_src)
        self._acquire_many_guarded_sha = await self._redis.script_load(
            acquire_many_guarded_src
        )
        self._report_sha = await self._redis.script_load(report_src)
        self._circuit_failure_sha = await self._redis.script_load(
            circuit_failure_src
        )
        self._circuit_success_sha = await self._redis.script_load(
            circuit_success_src
        )

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
        now_ms = int(self._now() * 1000)
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

    async def acquire_many(
        self,
        requirements: Sequence[BucketRequirement],
    ) -> MultiAcquireResult:
        """Atomically charge all scopes or none of them."""

        if not requirements:
            return MultiAcquireResult(
                granted=True,
                blocked_index=None,
                retry_after_ms=0,
            )
        keys = [item.bucket_key for item in requirements]
        if len(keys) != len(set(keys)):
            raise ValueError("bucket requirements must have unique keys")
        await self._load_scripts()
        assert self._acquire_many_sha is not None
        args: list[int | float] = [int(self._now() * 1000)]
        for requirement in requirements:
            args.extend(
                (
                    requirement.capacity,
                    requirement.refill_per_sec,
                    requirement.cost,
                )
            )
        raw: Any = await self._redis.evalsha(
            self._acquire_many_sha,
            len(keys),
            *keys,
            *args,
        )
        blocked = int(raw[1])
        return MultiAcquireResult(
            granted=bool(raw[0]),
            blocked_index=None if blocked == 0 else blocked - 1,
            retry_after_ms=int(raw[2]),
        )

    async def acquire_many_guarded(
        self,
        requirements: Sequence[BucketRequirement],
        *,
        half_open_probe_lease_ms: int,
        circuit_state_retention_ms: int,
    ) -> GuardedAcquireResult:
        """Atomically gate circuits and charge all exact quota buckets."""

        if not requirements:
            return GuardedAcquireResult(
                granted=True,
                blocked_index=None,
                retry_after_ms=0,
                circuit_open=False,
            )
        quota_keys = [item.bucket_key for item in requirements]
        if len(quota_keys) != len(set(quota_keys)):
            raise ValueError("bucket requirements must have unique keys")
        if half_open_probe_lease_ms < 1:
            raise ValueError("half_open_probe_lease_ms must be >= 1")
        if circuit_state_retention_ms < half_open_probe_lease_ms:
            raise ValueError(
                "circuit_state_retention_ms must cover the probe lease"
            )
        circuit_keys = [
            self._circuit_key(bucket_key) for bucket_key in quota_keys
        ]
        await self._load_scripts()
        assert self._acquire_many_guarded_sha is not None
        args: list[int | float] = [
            int(self._now() * 1000),
            len(requirements),
            half_open_probe_lease_ms,
            circuit_state_retention_ms,
        ]
        for requirement in requirements:
            args.extend(
                (
                    requirement.capacity,
                    requirement.refill_per_sec,
                    requirement.cost,
                )
            )
        raw: Any = await self._redis.evalsha(
            self._acquire_many_guarded_sha,
            len(quota_keys) + len(circuit_keys),
            *quota_keys,
            *circuit_keys,
            *args,
        )
        blocked = int(raw[1])
        denial_kind = int(raw[3])
        return GuardedAcquireResult(
            granted=bool(raw[0]),
            blocked_index=None if blocked == 0 else blocked - 1,
            retry_after_ms=int(raw[2]),
            circuit_open=denial_kind == 2,
        )

    async def record_circuit_failure(
        self,
        bucket_keys: Sequence[str],
        *,
        consecutive_failure_threshold: int,
        open_duration_ms: int,
        circuit_state_retention_ms: int,
    ) -> None:
        """Record one retryable failure for each exact quota bucket."""

        keys = self._unique_circuit_keys(bucket_keys)
        if not keys:
            return
        if consecutive_failure_threshold < 1:
            raise ValueError(
                "consecutive_failure_threshold must be >= 1"
            )
        if open_duration_ms < 1:
            raise ValueError("open_duration_ms must be >= 1")
        if circuit_state_retention_ms < open_duration_ms:
            raise ValueError(
                "circuit_state_retention_ms must cover open_duration_ms"
            )
        await self._load_scripts()
        assert self._circuit_failure_sha is not None
        await self._redis.evalsha(
            self._circuit_failure_sha,
            len(keys),
            *keys,
            int(self._now() * 1000),
            consecutive_failure_threshold,
            open_duration_ms,
            circuit_state_retention_ms,
        )

    async def record_circuit_success(
        self,
        bucket_keys: Sequence[str],
        *,
        circuit_state_retention_ms: int,
    ) -> None:
        """Reset closed scopes or close a claimed half-open probe."""

        keys = self._unique_circuit_keys(bucket_keys)
        if not keys:
            return
        if circuit_state_retention_ms < 1:
            raise ValueError("circuit_state_retention_ms must be >= 1")
        await self._load_scripts()
        assert self._circuit_success_sha is not None
        await self._redis.evalsha(
            self._circuit_success_sha,
            len(keys),
            *keys,
            circuit_state_retention_ms,
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
        now_ms = int(self._now() * 1000)
        await self._redis.evalsha(
            self._report_sha,
            1,
            bucket_key,
            now_ms,
            retry_after_ms,
        )

    @staticmethod
    def _circuit_key(bucket_key: str) -> str:
        return f"{bucket_key}:circuit"

    @classmethod
    def _unique_circuit_keys(
        cls,
        bucket_keys: Sequence[str],
    ) -> tuple[str, ...]:
        keys = tuple(bucket_keys)
        if len(keys) != len(set(keys)):
            raise ValueError("circuit bucket keys must be unique")
        return tuple(cls._circuit_key(bucket_key) for bucket_key in keys)


__all__ = [
    "AcquireResult",
    "BucketRequirement",
    "GuardedAcquireResult",
    "MultiAcquireResult",
    "RateLimiter",
]
