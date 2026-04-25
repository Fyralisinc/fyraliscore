"""services/workers/maintenance/daily.py — Wave 4-D daily (+ hourly)
maintenance jobs.

Jobs exported here:

* ``hourly_decay_job`` — spec §2 / §12 / Wave 1-C decay. Thin wrapper
  around ``services.models.decay.hourly_decay``. Runs every hour.
* ``archive_decayed_job`` — spec §2 / Wave 1-C decay archival. Thin
  wrapper around ``services.models.decay.archive_decayed``. Runs daily.
* ``entity_alias_cleanup`` — drops aliases with ``confirmed_count = 0
  AND contested_count = 0 AND last_used_at < now() - 90 days``. (Spec
  says "usage_count"; S6.1 actually stores ``confirmed_count`` +
  ``contested_count`` — both must be zero for "zero usage".) Documented
  in BUILD-LOG Wave 4-D entry.
* ``orphan_detection`` — flags Observations older than 14 days that
  have NO downstream model or act referencing them. WRITES ONLY to
  ``orphan_log`` (migration 0013). Never deletes Observations (Phase 5
  work).
* ``think_runs_cleanup`` — counts ``think_runs`` older than 90 days.
  Wave-4 logs the count; Phase-5 archives.
* ``region_lock_log_cleanup`` — deletes rows from
  ``think_region_lock_log`` where ``acquired_at < now() - 30 days``.
  Kept recent for contention analysis.

All jobs accept an optional `conn` for test injection and return row
counts. `run_daily` composes them and returns a ``DailyReport``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.db import get_pool
from lib.shared.ids import uuid7
from services.models.decay import archive_decayed, hourly_decay


log = logging.getLogger(__name__)


# Defaults matching BUILD-PLAN §5 Prompt 4.D.
ALIAS_STALE_DAYS = 90
ORPHAN_GRACE_DAYS = 14
THINK_RUNS_OLD_DAYS = 90
REGION_LOCK_OLD_DAYS = 30


@dataclass
class DailyReport:
    """Row-count snapshot of one daily run. Truth for orphans lives in
    ``orphan_log``; everything else is DELETE/UPDATE counts."""

    run_id: UUID
    run_started_at: datetime
    decayed_rows: int = 0
    archived_rows: int = 0
    aliases_deleted: int = 0
    orphans_flagged: int = 0
    think_runs_old: int = 0   # counted, not deleted (Phase 5 archives)
    region_lock_rows_deleted: int = 0
    access_matviews_refreshed: dict[str, bool] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------
# Individual jobs
# ---------------------------------------------------------------------


async def hourly_decay_job(
    *, conn: asyncpg.Connection | None = None
) -> int:
    """One tick of hourly decay. Thin wrapper."""
    return await hourly_decay(conn=conn)


async def archive_decayed_job(
    *, conn: asyncpg.Connection | None = None
) -> int:
    """Archive Models whose activation < 0.05 AND stale retrieval > 30d.
    Thin wrapper around ``services.models.decay.archive_decayed``.
    """
    return await archive_decayed(conn=conn)


async def entity_alias_cleanup(
    *,
    conn: asyncpg.Connection | None = None,
    stale_days: int = ALIAS_STALE_DAYS,
) -> int:
    """Delete unused aliases. "Unused" = both counts zero AND stale
    last_used_at. Returns number of rows deleted.
    """
    runner: Any = conn if conn is not None else get_pool()
    tag = await runner.execute(
        """
        DELETE FROM entity_aliases
        WHERE confirmed_count = 0
          AND contested_count = 0
          AND last_used_at < (now() - ($1 || ' days')::interval)
        """,
        str(int(stale_days)),
    )
    return _rowcount(tag)


async def orphan_detection(
    *,
    conn: asyncpg.Connection | None = None,
    grace_days: int = ORPHAN_GRACE_DAYS,
) -> int:
    """Flag Observations with no downstream model / act reference.

    WRITES ONLY to ``orphan_log``. Observations are never deleted.
    Returns the number of rows inserted into ``orphan_log``.

    Detection rules:
    - Age > `grace_days` (default 14). The post-ingest Think run + any
      cascade may take hours; we don't flag anything younger than this.
    - No Model references via ``born_from_event_id`` or
      ``supporting_event_ids``.
    - No Act references via ``created_by_event_id`` (goals, commitments,
      decisions) or ``last_updated_by_event_id`` (resources) or
      ``resolved_by_event_ids`` (commitments).
    - No ``cause_id`` fan-in (another observation ALSO references this
      one downstream — if someone has this as their cause, it's not
      orphan).

    Uses a single INSERT...SELECT so the scan happens in one round trip.
    Every emitted `orphan_log.reason` is 'both' — Wave 4's predicate is
    strict "no downstream at all". Phase 5 may refine this.
    """
    runner: Any = conn if conn is not None else get_pool()
    # Pull orphan candidates in one pass. We use EXISTS subqueries
    # rather than join + distinct so the planner can use existing GIN /
    # btree indexes on each downstream column.
    sql = """
        INSERT INTO orphan_log (id, tenant_id, observation_id, reason)
        SELECT
          gen_random_uuid(),
          o.tenant_id,
          o.id,
          'both'::text AS reason
        FROM (
          SELECT
            o.id, o.tenant_id, o.occurred_at,
            EXISTS (
              SELECT 1 FROM models m
              WHERE m.tenant_id = o.tenant_id
                AND (m.born_from_event_id = o.id
                     OR o.id = ANY(m.supporting_event_ids))
            ) AS model_hit,
            (EXISTS (
               SELECT 1 FROM goals g
               WHERE g.tenant_id = o.tenant_id
                 AND g.created_by_event_id = o.id
             )
             OR EXISTS (
               SELECT 1 FROM commitments c
               WHERE c.tenant_id = o.tenant_id
                 AND (c.created_by_event_id = o.id
                      OR o.id = ANY(c.resolved_by_event_ids))
             )
             OR EXISTS (
               SELECT 1 FROM decisions d
               WHERE d.tenant_id = o.tenant_id
                 AND d.created_by_event_id = o.id
             )
             OR EXISTS (
               SELECT 1 FROM resources r
               WHERE r.tenant_id = o.tenant_id
                 AND r.last_updated_by_event_id = o.id
             )
             OR EXISTS (
               SELECT 1 FROM observations c
               WHERE c.tenant_id = o.tenant_id
                 AND c.cause_id = o.id
             )
            ) AS act_hit
          FROM observations o
          WHERE o.occurred_at < (now() - ($1 || ' days')::interval)
            -- Skip rows already flagged in the last run — we dedup
            -- in the application by checking the most recent row.
            AND NOT EXISTS (
              SELECT 1 FROM orphan_log ol
              WHERE ol.observation_id = o.id
                AND ol.detected_at > now() - interval '1 day'
            )
        ) o
        WHERE NOT model_hit AND NOT act_hit
    """
    tag = await runner.execute(sql, str(int(grace_days)))
    return _rowcount(tag)


async def think_runs_cleanup(
    *,
    conn: asyncpg.Connection | None = None,
    old_days: int = THINK_RUNS_OLD_DAYS,
) -> int:
    """Count ``think_runs`` older than `old_days`. Wave-4 does NOT delete
    — archival is Phase 5. Returns the count (for the log).
    """
    runner: Any = conn if conn is not None else get_pool()
    val = await runner.fetchval(
        """
        SELECT COUNT(*) FROM think_runs
        WHERE started_at < (now() - ($1 || ' days')::interval)
        """,
        str(int(old_days)),
    )
    return int(val or 0)


async def region_lock_log_cleanup(
    *,
    conn: asyncpg.Connection | None = None,
    old_days: int = REGION_LOCK_OLD_DAYS,
) -> int:
    """Delete old rows from ``think_region_lock_log``."""
    runner: Any = conn if conn is not None else get_pool()
    tag = await runner.execute(
        """
        DELETE FROM think_region_lock_log
        WHERE acquired_at < (now() - ($1 || ' days')::interval)
        """,
        str(int(old_days)),
    )
    return _rowcount(tag)


async def realtime_cursor_cleanup(
    *,
    conn: asyncpg.Connection | None = None,
    stale_days: int = 30,
) -> int:
    """Prune abandoned realtime_replay_cursors. Documented inline in
    migration 0012.
    """
    runner: Any = conn if conn is not None else get_pool()
    tag = await runner.execute(
        """
        DELETE FROM realtime_replay_cursors
        WHERE last_ack_at < (now() - ($1 || ' days')::interval)
        """,
        str(int(stale_days)),
    )
    return _rowcount(tag)


async def access_matview_refresh(
    *,
    conn: asyncpg.Connection | None = None,
    concurrently: bool | None = None,
) -> dict[str, bool]:
    """Wave 5-A materialized access-view refresh. Spec §26 +
    BUILD-PLAN §6 Prompt 5.A: "full nightly rebuild via the Wave 4-D
    daily maintenance worker".

    Concurrently=None auto-detects (use CONCURRENTLY when populated).
    """
    from services.access_control.materialized import refresh_all  # lazy
    runner: Any = conn if conn is not None else get_pool()
    if conn is None:
        async with runner.acquire() as held:
            return await refresh_all(conn=held, concurrently=concurrently)
    return await refresh_all(conn=runner, concurrently=concurrently)


# ---------------------------------------------------------------------
# Composer
# ---------------------------------------------------------------------


async def run_daily(
    *,
    pool: asyncpg.Pool | None = None,
) -> DailyReport:
    """Run every daily job in sequence. Errors in one job don't abort
    the rest; each error is appended to ``report.errors`` with the job
    name.
    """
    report = DailyReport(
        run_id=uuid7(),
        run_started_at=datetime.now(timezone.utc),
    )
    the_pool = pool or get_pool()

    async def _safe(name: str, coro) -> Any:
        try:
            return await coro
        except Exception as e:
            report.errors.append(f"{name}:{type(e).__name__}:{e}")
            log.warning("daily job %s failed: %s", name, e)
            return 0

    async with the_pool.acquire() as conn:
        report.decayed_rows = await _safe(
            "hourly_decay", hourly_decay_job(conn=conn)
        )
        report.archived_rows = await _safe(
            "archive_decayed", archive_decayed_job(conn=conn)
        )
        report.aliases_deleted = await _safe(
            "alias_cleanup", entity_alias_cleanup(conn=conn)
        )
        report.orphans_flagged = await _safe(
            "orphan_detection", orphan_detection(conn=conn)
        )
        report.think_runs_old = await _safe(
            "think_runs_cleanup", think_runs_cleanup(conn=conn)
        )
        report.region_lock_rows_deleted = await _safe(
            "region_lock_log_cleanup", region_lock_log_cleanup(conn=conn)
        )
        await _safe("realtime_cursor_cleanup", realtime_cursor_cleanup(conn=conn))
        # Wave 5-A: access-control matview refresh. Returns a dict and
        # we store it explicitly on the report (not a row count).
        try:
            report.access_matviews_refreshed = await access_matview_refresh(
                conn=conn,
            )
        except Exception as e:
            report.errors.append(
                f"access_matview_refresh:{type(e).__name__}:{e}"
            )
            log.warning("daily job access_matview_refresh failed: %s", e)
            report.access_matviews_refreshed = {}

    log.info(
        "daily maintenance complete",
        extra={
            "run_id": str(report.run_id),
            "decayed": report.decayed_rows,
            "archived": report.archived_rows,
            "aliases_deleted": report.aliases_deleted,
            "orphans_flagged": report.orphans_flagged,
            "think_runs_old": report.think_runs_old,
            "region_lock_rows_deleted": report.region_lock_rows_deleted,
            "errors": report.errors,
        },
    )
    return report


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _rowcount(tag: str) -> int:
    try:
        return int(tag.split()[-1])
    except (IndexError, ValueError):
        return 0


__all__ = [
    "DailyReport",
    "hourly_decay_job",
    "archive_decayed_job",
    "entity_alias_cleanup",
    "orphan_detection",
    "think_runs_cleanup",
    "region_lock_log_cleanup",
    "realtime_cursor_cleanup",
    "access_matview_refresh",
    "run_daily",
]
