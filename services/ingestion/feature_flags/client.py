"""Tenant feature-flag reader for the ingestion data plane.

Per ingestion LLD §11 (cutover feature flags live in `tenant_flags`,
which was added in M1 migration 0050). Per M2 work-order §M2.1:

> "Add a feature flag `ingestion.shadow_write_enabled` in
> `tenant_flags`. Default true globally; can be flipped false
> per-tenant if a tenant's shadow path needs to be disabled in
> emergency. The router reads this flag with a 30-second TTL cache
> (matches LLD §11 pattern)."

This module is the per-process cache + DB reader. M5 will add
write-side helpers when the circuit breaker starts flipping flags.

Default-on semantics: when no row exists in `tenant_flags` for
(tenant_id, flag_name), the reader returns the supplied
`default_value`. Per LLD §1.7, missing rows mean "default behaviour"
which for `ingestion.shadow_write_enabled` is True.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


log = logging.getLogger(__name__)


# Public flag names — keep as constants so callers grep-find them.
SHADOW_WRITE_ENABLED = "ingestion.shadow_write_enabled"
KAFKA_PATH_ENABLED = "ingestion.kafka_path_enabled"  # M5 surface


@dataclass(frozen=True)
class _CacheEntry:
    value: bool
    expires_at: float  # monotonic time


@dataclass
class FlagCache:
    """Per-process in-memory cache for tenant_flags reads.

    Keyed on `(tenant_id, flag_name)`. TTL is the same for every
    entry; default 30s matches the M2 work order.

    Thread safety: protected by a single asyncio.Lock. The cache is
    small (one entry per active tenant × flag); concurrent writers
    block briefly but the read path is lock-free in the hot case
    via dict.get.
    """

    ttl_seconds: float = 30.0
    _entries: dict[tuple[UUID, str], _CacheEntry] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def invalidate(self, tenant_id: UUID, flag_name: str) -> None:
        """Drop a single entry. Used by tests; in production, entries
        expire naturally.
        """
        self._entries.pop((tenant_id, flag_name), None)

    def clear(self) -> None:
        self._entries.clear()

    def _fresh(self, key: tuple[UUID, str]) -> bool | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at <= time.monotonic():
            return None
        return entry.value

    def _store(self, key: tuple[UUID, str], value: bool) -> None:
        self._entries[key] = _CacheEntry(
            value=value,
            expires_at=time.monotonic() + self.ttl_seconds,
        )


class TenantFlags:
    """Async reader for `tenant_flags`. One instance per process,
    typically wired onto `app.state.tenant_flags` next to
    `tenant_resolver`.

    Holds a reference to an asyncpg Pool (or anything that exposes
    `fetchrow(query, *args) -> Record|None`); does not own the pool
    lifecycle.
    """

    def __init__(
        self,
        pool: Any,
        *,
        cache: FlagCache | None = None,
    ) -> None:
        self._pool = pool
        self._cache = cache or FlagCache()

    @property
    def cache(self) -> FlagCache:
        return self._cache

    async def get_bool(
        self,
        tenant_id: UUID,
        flag_name: str,
        *,
        default: bool,
    ) -> bool:
        """Return the boolean value of `flag_name` for `tenant_id`.

        Resolution order:
          1. Cache hit (within TTL).
          2. Read `tenant_flags` for the row; cache + return value.
          3. No row → cache the supplied `default` + return it.
        """
        key = (tenant_id, flag_name)
        cached = self._cache._fresh(key)
        if cached is not None:
            return cached

        async with self._cache._lock:
            # Re-check inside the lock to avoid a thundering herd:
            # the first waiter populates the cache; subsequent waiters
            # find it fresh.
            cached = self._cache._fresh(key)
            if cached is not None:
                return cached

            try:
                row = await self._pool.fetchrow(
                    """
                    SELECT flag_value
                      FROM tenant_flags
                     WHERE tenant_id = $1 AND flag_name = $2
                    """,
                    tenant_id, flag_name,
                )
            except Exception as exc:  # noqa: BLE001
                # Database read failure must NOT propagate — per M2's
                # prime directive, shadow-path readers cannot break
                # the inline path. Log + return the default.
                log.warning(
                    "tenant_flags_read_failed",
                    extra={
                        "tenant_id": str(tenant_id),
                        "flag_name": flag_name,
                        "error_type": type(exc).__name__,
                    },
                )
                return default

            value = bool(row["flag_value"]) if row is not None else default
            self._cache._store(key, value)
            return value


__all__ = [
    "KAFKA_PATH_ENABLED",
    "SHADOW_WRITE_ENABLED",
    "FlagCache",
    "TenantFlags",
]
