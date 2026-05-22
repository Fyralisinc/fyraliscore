"""services/greeting/viewer_state_repo.py — Track A.

Per-viewer last-seen tracking, mirroring `cache.py`'s repository
pattern. Backed by migration 0039's `viewer_state` table. Each
GET /view/ceo/home upserts the current visit timestamp and returns the
previous value (or None on first-ever visit), so the UI can render
delta indicators on a future "Map" surface.

The upsert is a single statement (INSERT ... ON CONFLICT ... RETURNING
the *old* value via a CTE) so concurrent GETs from the same viewer
don't race each other.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import asyncpg


class ViewerStateRepo:
    """Thin repository around `viewer_state`. No ORM. Methods mirror
    the shape of `ViewCeoCacheRepo` so call sites look uniform.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    # -----------------------------------------------------------------
    # get
    # -----------------------------------------------------------------
    async def get_last_seen(
        self,
        tenant_id: UUID,
        viewer_id: str,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> datetime | None:
        """Return the stored `last_seen_at` for (tenant_id, viewer_id),
        or None if no row exists yet.
        """
        sql = """
            SELECT last_seen_at
            FROM viewer_state
            WHERE tenant_id = $1 AND viewer_id = $2
        """

        async def _run(c: asyncpg.Connection) -> datetime | None:
            row = await c.fetchrow(sql, tenant_id, viewer_id)
            if row is None:
                return None
            return _ensure_utc(row["last_seen_at"])

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # upsert
    # -----------------------------------------------------------------
    async def upsert_last_seen(
        self,
        tenant_id: UUID,
        viewer_id: str,
        at: datetime,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> datetime | None:
        """Atomically record a new last-seen and return the previous one.

        Returns the previous `last_seen_at` (or None on first-ever
        visit). Implemented as a single CTE so two concurrent callers
        never read each other's writes mid-flight — the `prev` CTE
        captures the pre-image inside the same statement that performs
        the upsert.
        """
        at = _ensure_utc(at)
        # CTE strategy:
        #   * `prev` snapshots the row (if any) before we touch it.
        #   * `ins` does INSERT ... ON CONFLICT ... DO UPDATE.
        #   * Final SELECT pulls last_seen_at from `prev` so the
        #     returned value is the PREVIOUS one, not the just-written
        #     one. If no `prev` row existed, the LEFT JOIN yields NULL.
        sql = """
            WITH prev AS (
                SELECT last_seen_at
                FROM viewer_state
                WHERE tenant_id = $1 AND viewer_id = $2
            ),
            ins AS (
                INSERT INTO viewer_state (tenant_id, viewer_id, last_seen_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (tenant_id, viewer_id) DO UPDATE
                SET last_seen_at = EXCLUDED.last_seen_at
                RETURNING last_seen_at
            )
            SELECT (SELECT last_seen_at FROM prev) AS previous_last_seen_at
        """

        async def _run(c: asyncpg.Connection) -> datetime | None:
            row = await c.fetchrow(sql, tenant_id, viewer_id, at)
            if row is None:
                return None
            prev = row["previous_last_seen_at"]
            if prev is None:
                return None
            return _ensure_utc(prev)

        if conn is not None:
            return await _run(conn)
        async with self._pool.acquire() as owned:
            return await _run(owned)


def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


__all__ = ["ViewerStateRepo"]
