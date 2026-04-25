"""
services/access_control/materialized.py — materialized view refresh.

Three views (migration 0014):
  - actor_visible_commitments
  - actor_visible_goals
  - actor_visible_models

Refresh strategy (per BUILD-PLAN §6 Prompt 5.A):
  * Incremental: on role grants/revokes, commitment.owner_id changes,
    commitment_contributors edits, and actors.metadata.manager_id
    updates, `enqueue_refresh` is called. The queue is a simple in-
    process set that the daily maintenance worker drains; production
    can later swap this for a durable table without touching the API.
  * Full: daily maintenance calls `refresh_all` nightly.

Wave 5-A wires the full rebuild into `services/workers/maintenance/
daily.py::run_daily`. The incremental path is an in-memory flag so
tests and operators can force a rebuild without waiting for the
scheduler.

Concurrency: we use `REFRESH MATERIALIZED VIEW CONCURRENTLY` when the
view has data (required for no-lock refresh). First-ever refresh must
be non-concurrent (PG limitation — nothing to diff against). We auto-
detect the empty case via `pg_matviews.ispopulated`.
"""
from __future__ import annotations

import logging
from uuid import UUID

import asyncpg


log = logging.getLogger(__name__)


# Canonical list of matviews maintained by Wave 5-A.
MATERIALIZED_VIEWS: tuple[str, ...] = (
    "actor_visible_commitments",
    "actor_visible_goals",
    "actor_visible_models",
)


# In-memory dirty flag. Per-process; production durable-queue swap is a
# Phase-5 concern. Simple set keeps incremental refresh cheap.
_DIRTY: set[str] = set()


def enqueue_refresh(view: str | None = None) -> None:
    """Mark a matview as dirty. `None` = all views. Idempotent."""
    if view is None:
        _DIRTY.update(MATERIALIZED_VIEWS)
        return
    if view not in MATERIALIZED_VIEWS:
        raise ValueError(
            f"unknown matview {view!r}; expected one of {MATERIALIZED_VIEWS}"
        )
    _DIRTY.add(view)


def _clear_dirty(view: str) -> None:
    _DIRTY.discard(view)


async def _is_populated(conn: asyncpg.Connection, view: str) -> bool:
    val = await conn.fetchval(
        "SELECT ispopulated FROM pg_matviews WHERE matviewname = $1",
        view,
    )
    return bool(val)


async def refresh_one(
    view: str,
    *,
    conn: asyncpg.Connection,
    concurrently: bool | None = None,
) -> None:
    """Refresh a single matview.

    * If the view has never been refreshed (`ispopulated=false`), we
      run the plain (non-concurrent) refresh first — PG requires it.
    * Otherwise, we use CONCURRENTLY so the refresh does not take an
      exclusive lock (hot-path queries keep seeing the previous data
      while the refresh runs).

    `concurrently=None` auto-detects; pass True/False to force.
    """
    if view not in MATERIALIZED_VIEWS:
        raise ValueError(f"unknown matview {view!r}")
    populated = await _is_populated(conn, view)
    if concurrently is None:
        concurrently = populated
    if concurrently and populated:
        stmt = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"
    else:
        stmt = f"REFRESH MATERIALIZED VIEW {view}"
    await conn.execute(stmt)
    _clear_dirty(view)
    log.info(
        "access_control matview refreshed",
        extra={
            "view": view,
            "concurrently": bool(concurrently and populated),
        },
    )


async def refresh_all(
    *,
    conn: asyncpg.Connection,
    concurrently: bool | None = None,
) -> dict[str, bool]:
    """Refresh every matview. Returns {view: True} for success. Errors
    in one view are logged but do not abort the others; the errored
    view stays dirty."""
    results: dict[str, bool] = {}
    for view in MATERIALIZED_VIEWS:
        try:
            await refresh_one(view, conn=conn, concurrently=concurrently)
            results[view] = True
        except Exception as e:
            log.warning(
                "access_control matview refresh failed",
                extra={"view": view, "error": str(e)},
            )
            results[view] = False
    return results


def dirty_views() -> frozenset[str]:
    """Diagnostic accessor — returns the current dirty set."""
    return frozenset(_DIRTY)


def clear_dirty_all() -> None:
    """Test hook: reset the dirty flag. Real refresh also clears."""
    _DIRTY.clear()


# ---------------------------------------------------------------------
# Membership helpers — hot path
# ---------------------------------------------------------------------


async def is_commitment_visible_to(
    actor_id: UUID,
    commitment_id: UUID,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> bool:
    """Indexed point-check on actor_visible_commitments."""
    val = await conn.fetchval(
        """
        SELECT 1 FROM actor_visible_commitments
        WHERE actor_id = $1 AND commitment_id = $2 AND tenant_id = $3
        LIMIT 1
        """,
        actor_id, commitment_id, tenant_id,
    )
    return val is not None


async def is_goal_visible_to(
    actor_id: UUID,
    goal_id: UUID,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> bool:
    val = await conn.fetchval(
        """
        SELECT 1 FROM actor_visible_goals
        WHERE actor_id = $1 AND goal_id = $2 AND tenant_id = $3
        LIMIT 1
        """,
        actor_id, goal_id, tenant_id,
    )
    return val is not None


async def is_model_visible_to(
    actor_id: UUID,
    model_id: UUID,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> bool:
    val = await conn.fetchval(
        """
        SELECT 1 FROM actor_visible_models
        WHERE actor_id = $1 AND model_id = $2 AND tenant_id = $3
        LIMIT 1
        """,
        actor_id, model_id, tenant_id,
    )
    return val is not None


__all__ = [
    "MATERIALIZED_VIEWS",
    "clear_dirty_all",
    "dirty_views",
    "enqueue_refresh",
    "is_commitment_visible_to",
    "is_goal_visible_to",
    "is_model_visible_to",
    "refresh_all",
    "refresh_one",
]
