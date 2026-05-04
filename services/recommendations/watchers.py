"""
services/recommendations/watchers.py — async repo for "Watch for revision"
subscriptions on recommendation cards (model_watchers table, migration
0027).

A watch couples (actor, predicate) to a recommendation Model. The
substrate just persists; the T2 cascade work that detects predicate
firing lands later and will UPDATE `fired_at`.

The repo returns primitives (UUID, bool, set[UUID], None) — no
dataclasses — to match the style of services/recommendations/repo.py
and keep the wire layer above this module thin.
"""
from __future__ import annotations

from uuid import UUID

import asyncpg


# ---------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------


# Insert a watch. ON CONFLICT (tenant, actor, predicate) reactivates a
# previously cleared/fired row by nulling both timestamps and returning
# the existing id. Doing it in one statement keeps the re-watch path
# atomic and free of read-then-write races.
_CREATE_SQL = """
INSERT INTO model_watchers (
    tenant_id, recommendation_id, actor_id, predicate
) VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, actor_id, predicate) DO UPDATE
    SET cleared_at = NULL,
        fired_at   = NULL,
        recommendation_id = EXCLUDED.recommendation_id
RETURNING id
"""


# A watch is "active" when neither cleared nor fired. Once the cascade
# fires it, the row stays around for audit but is no longer surfaced as
# is_watched on the card.
_IS_WATCHING_SQL = """
SELECT 1
FROM model_watchers
WHERE tenant_id = $1
  AND recommendation_id = $2
  AND actor_id = $3
  AND cleared_at IS NULL
  AND fired_at IS NULL
LIMIT 1
"""


# Bulk read used by the Today aggregator: which of the given
# recommendation_ids does this actor have an active watch on?
_LIST_ACTIVE_SQL = """
SELECT recommendation_id
FROM model_watchers
WHERE tenant_id = $1
  AND actor_id  = $2
  AND recommendation_id = ANY($3::uuid[])
  AND cleared_at IS NULL
  AND fired_at IS NULL
"""


# Soft-delete: cancel the watch. We never hard-delete so the audit
# trail (created_at + cleared_at) survives, and a re-watch via
# _CREATE_SQL can reactivate the same row.
_CLEAR_SQL = """
UPDATE model_watchers
SET cleared_at = now()
WHERE tenant_id = $1
  AND recommendation_id = $2
  AND actor_id = $3
  AND cleared_at IS NULL
"""


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


async def create_watch(
    *,
    tenant_id: UUID,
    recommendation_id: UUID,
    actor_id: UUID,
    predicate: str,
    conn: asyncpg.Connection,
) -> UUID:
    """Insert (or reactivate) a watch and return its id."""
    row = await conn.fetchrow(
        _CREATE_SQL, tenant_id, recommendation_id, actor_id, predicate,
    )
    # ON CONFLICT DO UPDATE always returns a row.
    return row["id"]


async def is_watching(
    *,
    tenant_id: UUID,
    recommendation_id: UUID,
    actor_id: UUID,
    conn: asyncpg.Connection,
) -> bool:
    """True iff the actor has an active (not cleared, not fired) watch."""
    row = await conn.fetchrow(
        _IS_WATCHING_SQL, tenant_id, recommendation_id, actor_id,
    )
    return row is not None


async def list_active_watches(
    *,
    tenant_id: UUID,
    recommendation_ids: list[UUID],
    actor_id: UUID,
    conn: asyncpg.Connection,
) -> set[UUID]:
    """Return the subset of `recommendation_ids` that have an active
    watch for this actor. One query, used by the Today aggregator to
    fan `is_watched: true` onto cards post-loop."""
    if not recommendation_ids:
        return set()
    rows = await conn.fetch(
        _LIST_ACTIVE_SQL, tenant_id, actor_id, list(recommendation_ids),
    )
    return {r["recommendation_id"] for r in rows}


async def clear_watch(
    *,
    tenant_id: UUID,
    recommendation_id: UUID,
    actor_id: UUID,
    conn: asyncpg.Connection,
) -> None:
    """Set cleared_at = now() on the active watch, if any. No-op if the
    watch doesn't exist or is already cleared."""
    await conn.execute(
        _CLEAR_SQL, tenant_id, recommendation_id, actor_id,
    )


__all__ = [
    "create_watch",
    "is_watching",
    "list_active_watches",
    "clear_watch",
]
