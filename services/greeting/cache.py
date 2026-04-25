"""services/greeting/cache.py — Phase 1.

Postgres-JSONB backed cache for the CEO view. Keys per CONTRACTS §3:

  'greeting' | 'cards' | 'query_grid' | 'status' | 'query_prefetch:<id>'

The `staleness_seconds` field returned by `get_cached` is measured at
read time (`now() - cached_at`); no clock skew correction here because
Postgres `now()` is authoritative for both the write and the read.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg


CACHE_KEYS = ("greeting", "cards", "query_grid", "status")
RecomputedReason = Literal["scheduled", "trigger_fired", "manual"]


@dataclass(frozen=True)
class CachedContent:
    tenant_id: UUID
    cache_key: str
    content: dict[str, Any]
    cached_at: datetime
    staleness_seconds: float
    recomputed_reason: str | None


class ViewCeoCacheRepo:
    """Thin repository around `view_ceo_cache`. No ORM, no abstraction
    beyond what's needed by the scheduler and HTTP layer.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    # -----------------------------------------------------------------
    # get
    # -----------------------------------------------------------------
    async def get_cached(
        self,
        tenant_id: UUID,
        cache_key: str,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> CachedContent | None:
        """Read one cache row. Returns None if the row is absent.

        `staleness_seconds` computed in-DB so the client and server
        don't drift.
        """
        sql = """
            SELECT tenant_id, cache_key, cached_content, cached_at,
                   recomputed_reason,
                   EXTRACT(EPOCH FROM (now() - cached_at)) AS staleness_seconds
            FROM view_ceo_cache
            WHERE tenant_id = $1 AND cache_key = $2
        """

        async def _run(c: asyncpg.Connection) -> CachedContent | None:
            row = await c.fetchrow(sql, tenant_id, cache_key)
            if row is None:
                return None
            content = row["cached_content"]
            if isinstance(content, (bytes, bytearray)):
                content = content.decode()
            if isinstance(content, str):
                content = json.loads(content)
            return CachedContent(
                tenant_id=row["tenant_id"],
                cache_key=row["cache_key"],
                content=content,
                cached_at=row["cached_at"],
                staleness_seconds=float(row["staleness_seconds"] or 0.0),
                recomputed_reason=row["recomputed_reason"],
            )

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)

    async def get_all(
        self,
        tenant_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> dict[str, CachedContent]:
        """Fetch every cache row for a tenant (keyed by cache_key)."""
        sql = """
            SELECT tenant_id, cache_key, cached_content, cached_at,
                   recomputed_reason,
                   EXTRACT(EPOCH FROM (now() - cached_at)) AS staleness_seconds
            FROM view_ceo_cache
            WHERE tenant_id = $1
        """

        async def _run(c: asyncpg.Connection) -> dict[str, CachedContent]:
            rows = await c.fetch(sql, tenant_id)
            out: dict[str, CachedContent] = {}
            for r in rows:
                content = r["cached_content"]
                if isinstance(content, (bytes, bytearray)):
                    content = content.decode()
                if isinstance(content, str):
                    content = json.loads(content)
                out[r["cache_key"]] = CachedContent(
                    tenant_id=r["tenant_id"],
                    cache_key=r["cache_key"],
                    content=content,
                    cached_at=r["cached_at"],
                    staleness_seconds=float(r["staleness_seconds"] or 0.0),
                    recomputed_reason=r["recomputed_reason"],
                )
            return out

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # set
    # -----------------------------------------------------------------
    async def set_cached(
        self,
        tenant_id: UUID,
        cache_key: str,
        content: dict[str, Any],
        *,
        reason: RecomputedReason = "scheduled",
        conn: asyncpg.Connection | None = None,
    ) -> datetime:
        """Upsert a cache row. Returns the `cached_at` stamp used."""
        payload = json.dumps(content, default=_default_json)
        sql = """
            INSERT INTO view_ceo_cache
              (tenant_id, cache_key, cached_content, cached_at, recomputed_reason)
            VALUES ($1, $2, $3::jsonb, now(), $4)
            ON CONFLICT (tenant_id, cache_key) DO UPDATE
            SET cached_content = EXCLUDED.cached_content,
                cached_at = EXCLUDED.cached_at,
                recomputed_reason = EXCLUDED.recomputed_reason
            RETURNING cached_at
        """

        async def _run(c: asyncpg.Connection) -> datetime:
            row = await c.fetchrow(sql, tenant_id, cache_key, payload, reason)
            assert row is not None
            return row["cached_at"]

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # invalidate
    # -----------------------------------------------------------------
    async def invalidate(
        self,
        tenant_id: UUID,
        cache_key: str,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> bool:
        """Delete a cache row. Returns True if a row was removed."""
        sql = "DELETE FROM view_ceo_cache WHERE tenant_id = $1 AND cache_key = $2"

        async def _run(c: asyncpg.Connection) -> bool:
            tag = await c.execute(sql, tenant_id, cache_key)
            # asyncpg returns e.g. 'DELETE 1'
            return tag.endswith("1")

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)

    async def invalidate_all(
        self,
        tenant_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> int:
        """Remove every cache row for a tenant. Returns the count."""
        sql = "DELETE FROM view_ceo_cache WHERE tenant_id = $1"

        async def _run(c: asyncpg.Connection) -> int:
            tag = await c.execute(sql, tenant_id)
            try:
                return int(tag.split(" ")[-1])
            except (ValueError, IndexError):
                return 0

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)


def _default_json(v: Any) -> Any:
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).isoformat()
    if hasattr(v, "isoformat"):
        return v.isoformat()
    if isinstance(v, (set, frozenset)):
        return sorted(v)
    raise TypeError(f"unserialisable type {type(v).__name__}")


__all__ = [
    "CACHE_KEYS",
    "CachedContent",
    "RecomputedReason",
    "ViewCeoCacheRepo",
]
