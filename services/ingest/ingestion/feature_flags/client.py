"""Small cached reader/writer for tenant-scoped platform feature flags."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class _CacheEntry:
    value: bool
    expires_at: float


@dataclass
class FlagCache:
    """Per-process cache for ``tenant_flags`` reads."""

    ttl_seconds: float = 30.0
    _entries: dict[tuple[UUID, str], _CacheEntry] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def invalidate(self, tenant_id: UUID, flag_name: str) -> None:
        self._entries.pop((tenant_id, flag_name), None)

    def clear(self) -> None:
        self._entries.clear()

    def _fresh(self, key: tuple[UUID, str]) -> bool | None:
        entry = self._entries.get(key)
        if entry is None or entry.expires_at <= time.monotonic():
            return None
        return entry.value

    def _store(self, key: tuple[UUID, str], value: bool) -> None:
        self._entries[key] = _CacheEntry(
            value=value,
            expires_at=time.monotonic() + self.ttl_seconds,
        )


class TenantFlags:
    """Read and update boolean flags in the common tenant control plane."""

    def __init__(self, pool: Any, *, cache: FlagCache | None = None) -> None:
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
        key = (tenant_id, flag_name)
        cached = self._cache._fresh(key)
        if cached is not None:
            return cached
        async with self._cache._lock:
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
                    tenant_id,
                    flag_name,
                )
            except Exception as exc:  # noqa: BLE001
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

    async def set_bool(
        self,
        tenant_id: UUID,
        flag_name: str,
        value: bool,
        *,
        set_by: str,
        note: str | None = None,
    ) -> None:
        await self._pool.execute(
            """
            INSERT INTO tenant_flags
                (tenant_id, flag_name, flag_value, set_by, note, set_at)
            VALUES ($1, $2, $3, $4, $5, now())
            ON CONFLICT (tenant_id, flag_name) DO UPDATE SET
                flag_value = EXCLUDED.flag_value,
                set_by = EXCLUDED.set_by,
                note = EXCLUDED.note,
                set_at = now()
            """,
            tenant_id,
            flag_name,
            value,
            set_by,
            note,
        )
        self._cache.invalidate(tenant_id, flag_name)


__all__ = ["FlagCache", "TenantFlags"]
