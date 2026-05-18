"""services/integrations/discord/gateway/leader_lock.py — Redis lease.

Per ingestion LLD §1.5 + §13. M4.1.

The Discord Gateway is a stateful, single-instance ingestion surface.
Two pods connecting with the same bot token produce duplicate frame
delivery and (worse) "Already authenticated" rejections from Discord.
M4.1 prevents both via a Redis-backed lease: pods acquire before
connecting; non-holders wait.

=== Why Redis and not Postgres ===
Postgres advisory locks work but die with the connection. We need a
lease that survives the holder's death AND expires within a bounded
window so a crashed leader doesn't lock out the rest of the deploy.
Redis SET NX EX is exactly this shape. The `gateway_session_state`
table's `leader_lease_holder` / `leader_lease_expires_at` columns
mirror Redis for diagnostics — see LLD §1.5 "informational".

=== Why Lua for refresh + release ===
Naive Python-side check-then-act has a race window where another pod
can acquire between our GET and our EXPIRE/DEL. The Lua bundles the
check+act in a single Redis command turn, race-immune by construction.
The acquire script is also Lua (SET NX EX) so all three follow the
same deploy pattern: SCRIPT LOAD on first use, EVALSHA after — the
same shape as the M1.3 rate limiter.

=== Test-only knobs ===
TTL and refresh interval default to 30 / 10 seconds (per the M4.1
work order). Tests override these via constructor args to keep
test wall-clock manageable; production callers should never override.
"""
from __future__ import annotations

import logging
import pathlib
import uuid
from typing import Any

from redis.asyncio import Redis


log = logging.getLogger(__name__)


# Per the M4 plan + LLD §1.5: single shard, single key. The schema
# is `gateway:discord:leader_lock` (one global lease key). If
# multi-shard ships, the key becomes `gateway:discord:leader_lock:<shard_id>`.
LEASE_KEY = "gateway:discord:leader_lock"

# Production constants — the single source of truth. M4.1 work order
# pins 30s TTL refreshed every 10s. Do not scatter these.
DEFAULT_TTL_SECONDS = 30
DEFAULT_REFRESH_INTERVAL_SECONDS = 10


_SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent / "scripts"
_ACQUIRE_SCRIPT_PATH = _SCRIPTS_DIR / "acquire.lua"
_REFRESH_SCRIPT_PATH = _SCRIPTS_DIR / "refresh.lua"
_RELEASE_SCRIPT_PATH = _SCRIPTS_DIR / "release.lua"


class LeaderLock:
    """Per-process leader lease over Redis.

    Lifecycle:
        lock = LeaderLock(redis)
        if await lock.acquire():
            try:
                # ... do leader work ...
                # Periodically (every refresh_interval_seconds):
                if not await lock.refresh():
                    # Lease lost — another pod took over. Shut down.
                    break
            finally:
                await lock.release()
    """

    def __init__(
        self,
        redis: Redis,
        *,
        key: str = LEASE_KEY,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        # The per-process UUID. Default = fresh uuid7-style random;
        # tests override to a fixed value for assertion convenience.
        lease_value: str | None = None,
    ) -> None:
        self._redis = redis
        self._key = key
        self._ttl_seconds = ttl_seconds
        self._lease_value = lease_value or str(uuid.uuid4())
        self._held = False
        self._acquire_sha: str | None = None
        self._refresh_sha: str | None = None
        self._release_sha: str | None = None

    # ------------------------------------------------------------------
    # Read-only accessors (test-friendly + log-friendly)
    # ------------------------------------------------------------------
    @property
    def key(self) -> str:
        return self._key

    @property
    def ttl_seconds(self) -> int:
        return self._ttl_seconds

    @property
    def lease_value(self) -> str:
        return self._lease_value

    def is_held(self) -> bool:
        """Best-effort local view. Returns True iff this instance has
        successfully acquired AND not seen a refresh-loss / release.
        For authoritative "do I still hold it right now," call
        `refresh()` (Redis's view is the source of truth)."""
        return self._held

    # ------------------------------------------------------------------
    # Script loading
    # ------------------------------------------------------------------
    async def _load_scripts(self) -> None:
        if self._acquire_sha and self._refresh_sha and self._release_sha:
            return
        self._acquire_sha = await self._redis.script_load(
            _ACQUIRE_SCRIPT_PATH.read_text()
        )
        self._refresh_sha = await self._redis.script_load(
            _REFRESH_SCRIPT_PATH.read_text()
        )
        self._release_sha = await self._redis.script_load(
            _RELEASE_SCRIPT_PATH.read_text()
        )

    # ------------------------------------------------------------------
    # Operations
    # ------------------------------------------------------------------
    async def acquire(self) -> bool:
        """Attempt to acquire the lease. Non-blocking. Returns True
        if acquired, False if another holder has it.

        The caller decides whether to retry or wait — keeping this
        method synchronous (no internal sleep loop) makes the
        scheduling policy live in the caller, where it can interleave
        with shutdown signals."""
        await self._load_scripts()
        assert self._acquire_sha is not None
        raw: Any = await self._redis.evalsha(
            self._acquire_sha,
            1,
            self._key,
            self._lease_value,
            self._ttl_seconds,
        )
        acquired = bool(int(raw))
        if acquired:
            self._held = True
            log.info(
                "leader_lock.acquired",
                extra={
                    "key": self._key,
                    "lease_value": self._lease_value,
                    "ttl_seconds": self._ttl_seconds,
                },
            )
        return acquired

    async def refresh(self) -> bool:
        """Extend the lease TTL — but ONLY if we still own it.

        Returns True if refreshed; False if the lease has been lost
        (expired and re-acquired by another holder, OR force-deleted).
        On False, the caller MUST treat itself as no longer the
        leader. Drops the local `is_held()` flag.

        Atomic via refresh.lua — no Python-side race between GET and
        EXPIRE."""
        await self._load_scripts()
        assert self._refresh_sha is not None
        raw: Any = await self._redis.evalsha(
            self._refresh_sha,
            1,
            self._key,
            self._lease_value,
            self._ttl_seconds,
        )
        refreshed = bool(int(raw))
        if not refreshed:
            self._held = False
            log.warning(
                "leader_lock.refresh_lost",
                extra={
                    "key": self._key,
                    "lease_value": self._lease_value,
                },
            )
        return refreshed

    async def release(self) -> bool:
        """Delete the lease — but ONLY if we own it.

        Returns True if released; False if the key is absent OR
        belongs to another holder (someone else expired-and-re-acquired
        between our last refresh and this release). Idempotent: a
        second release after success returns False.

        Atomic via release.lua. Even if `is_held()` is True locally,
        Redis is the source of truth — a stale local flag does not
        clobber another holder's lease."""
        await self._load_scripts()
        assert self._release_sha is not None
        raw: Any = await self._redis.evalsha(
            self._release_sha,
            1,
            self._key,
            self._lease_value,
        )
        released = bool(int(raw))
        # `_held` flips to False regardless of release outcome — once
        # we have invoked release intent, we don't claim to hold any
        # more. (If we didn't own it, we already didn't hold it.)
        self._held = False
        if released:
            log.info(
                "leader_lock.released",
                extra={
                    "key": self._key,
                    "lease_value": self._lease_value,
                },
            )
        return released


__all__ = [
    "DEFAULT_REFRESH_INTERVAL_SECONDS",
    "DEFAULT_TTL_SECONDS",
    "LEASE_KEY",
    "LeaderLock",
]
