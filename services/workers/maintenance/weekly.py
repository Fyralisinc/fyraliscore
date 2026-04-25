"""services/workers/maintenance/weekly.py — Wave 4-D weekly maintenance.

Jobs:

* Relationship maintenance — calls
  ``services.retrieval.maintenance.background_relationship_maintenance``
  per tenant.
* Calibration updater — calls ``services.workers.calibration_updater.
  worker.run_once`` if the module is importable. When Wave 4-C hasn't
  landed yet, this is a graceful no-op with an `errors` entry.
* Partition extension — 3 months ahead for both ``observations`` and
  ``resource_transactions``.
* ``signal_memory_fabric`` decay — delete unpromoted rows older than 30
  days.
* Contestation aggregation — count contestation observations per
  `content.entity_kind` (or fallback 'unknown') per tenant in the last
  7 days; log the summary.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.db import get_pool
from lib.shared.ids import uuid7
from services.observations.partitions import (
    ensure_next_n_months as ensure_obs_months,
)
from services.resources.partitions import (
    ensure_next_n_months as ensure_rtx_months,
)
from services.retrieval.maintenance import (
    background_relationship_maintenance,
)


log = logging.getLogger(__name__)


SMF_DECAY_DAYS = 30
CONTESTATION_WINDOW_DAYS = 7


@dataclass
class WeeklyReport:
    run_id: UUID
    run_started_at: datetime
    tenants_processed: int = 0
    relationship_orphans_flagged: int = 0
    relationship_outliers: int = 0
    relationship_archival_suggestions: int = 0
    calibration_status: str = "not_run"
    partitions_created: list[str] = field(default_factory=list)
    smf_rows_deleted: int = 0
    contestation_summary: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------


async def relationship_maintenance_per_tenant(
    *,
    conn: asyncpg.Connection,
    tenant_ids: list[UUID] | None = None,
) -> tuple[int, int, int, int]:
    """Run ``background_relationship_maintenance`` across tenants. Returns
    (tenants_processed, orphans, outliers, archival_suggestions).
    """
    if tenant_ids is None:
        rows = await conn.fetch(
            """
            SELECT DISTINCT tenant_id FROM models WHERE status = 'active'
            """
        )
        tenant_ids = [r["tenant_id"] for r in rows]
    processed = 0
    orphans = outliers = arch = 0
    for tid in tenant_ids:
        report = await background_relationship_maintenance(tid, conn)
        processed += 1
        orphans += report.orphans_flagged
        outliers += report.activation_outliers
        arch += report.archival_suggestions
    return processed, orphans, outliers, arch


async def calibration_updater_run_once(
    *, pool: asyncpg.Pool | None = None
) -> str:
    """Invoke ``services.workers.calibration_updater.worker.run_once``.

    Wave 4-C delivered this worker. The function accepts a pool
    argument (the updater needs it for the transaction + bulk-update
    pass); when not supplied we pull from `lib.shared.db.get_pool`.

    Returns a status string: 'ok', 'unavailable' (module missing
    entirely), or 'error:<reason>'. The 'unavailable' branch is
    retained for the unlikely case where a deployment has maintenance
    workers without the calibration updater (e.g., partial upgrade).
    """
    try:
        mod = __import__(
            "services.workers.calibration_updater.worker",
            fromlist=["run_once"],
        )
    except Exception:  # ImportError or a deeper failure
        return "unavailable"
    run_once = getattr(mod, "run_once", None)
    if run_once is None:
        return "unavailable"
    the_pool = pool or get_pool()
    try:
        await run_once(the_pool)
        return "ok"
    except Exception as e:
        return f"error:{type(e).__name__}:{e}"


async def extend_partitions_job(
    *,
    pool: asyncpg.Pool | None = None,
    months_ahead: int = 3,
) -> list[str]:
    """Extend both observations + resource_transactions partition windows.
    Returns the combined list of newly-created partition names.
    """
    the_pool = pool or get_pool()
    created = []
    created += await ensure_obs_months(the_pool, months_ahead)
    created += await ensure_rtx_months(the_pool, months_ahead)
    return created


async def signal_memory_fabric_decay(
    *,
    conn: asyncpg.Connection | None = None,
    stale_days: int = SMF_DECAY_DAYS,
) -> int:
    """Delete unpromoted rows older than `stale_days`."""
    runner: Any = conn if conn is not None else get_pool()
    tag = await runner.execute(
        """
        DELETE FROM signal_memory_fabric
        WHERE promoted_at IS NULL
          AND recorded_at < (now() - ($1 || ' days')::interval)
        """,
        str(int(stale_days)),
    )
    return _rowcount(tag)


async def contestation_aggregation_report(
    *,
    conn: asyncpg.Connection | None = None,
    window_days: int = CONTESTATION_WINDOW_DAYS,
) -> dict[str, int]:
    """Count contestation observations per (tenant, scope-key) over the
    last `window_days`. Returns a dict ``"<tenant>:<kind>" -> count`` for
    log use. No writes.

    "scope-key" is derived from `content.entity_kind`, fallback 'unknown'.
    """
    runner: Any = conn if conn is not None else get_pool()
    rows = await runner.fetch(
        """
        SELECT tenant_id,
               COALESCE(content->>'entity_kind', 'unknown') AS scope_kind,
               COUNT(*) AS n
        FROM observations
        WHERE kind = 'contestation'
          AND occurred_at > (now() - ($1 || ' days')::interval)
        GROUP BY tenant_id, COALESCE(content->>'entity_kind', 'unknown')
        """,
        str(int(window_days)),
    )
    out: dict[str, int] = {}
    for r in rows:
        key = f"{r['tenant_id']}:{r['scope_kind']}"
        out[key] = int(r["n"])
    return out


# ---------------------------------------------------------------------
# Composer
# ---------------------------------------------------------------------


async def run_weekly(
    *,
    pool: asyncpg.Pool | None = None,
) -> WeeklyReport:
    the_pool = pool or get_pool()
    report = WeeklyReport(
        run_id=uuid7(),
        run_started_at=datetime.now(timezone.utc),
    )

    # Relationship maintenance (per-tenant loop inside a single conn).
    try:
        async with the_pool.acquire() as conn:
            async with conn.transaction():
                processed, orph, out, arch = (
                    await relationship_maintenance_per_tenant(conn=conn)
                )
                report.tenants_processed = processed
                report.relationship_orphans_flagged = orph
                report.relationship_outliers = out
                report.relationship_archival_suggestions = arch
    except Exception as e:
        report.errors.append(f"relationship_maintenance:{type(e).__name__}:{e}")
        log.warning("weekly relationship_maintenance failed: %s", e)

    # Calibration updater (optional — Wave 4-C).
    try:
        report.calibration_status = await calibration_updater_run_once()
    except Exception as e:
        report.calibration_status = f"error:{type(e).__name__}:{e}"
        report.errors.append(f"calibration:{type(e).__name__}:{e}")

    # Partition extension.
    try:
        report.partitions_created = await extend_partitions_job(
            pool=the_pool, months_ahead=3
        )
    except Exception as e:
        report.errors.append(f"extend_partitions:{type(e).__name__}:{e}")

    # Signal memory fabric decay.
    try:
        async with the_pool.acquire() as conn:
            report.smf_rows_deleted = await signal_memory_fabric_decay(
                conn=conn
            )
    except Exception as e:
        report.errors.append(f"smf_decay:{type(e).__name__}:{e}")

    # Contestation aggregation.
    try:
        async with the_pool.acquire() as conn:
            report.contestation_summary = (
                await contestation_aggregation_report(conn=conn)
            )
    except Exception as e:
        report.errors.append(f"contestation_agg:{type(e).__name__}:{e}")

    log.info(
        "weekly maintenance complete",
        extra={
            "run_id": str(report.run_id),
            "tenants": report.tenants_processed,
            "orphans": report.relationship_orphans_flagged,
            "outliers": report.relationship_outliers,
            "archival_suggestions": (
                report.relationship_archival_suggestions
            ),
            "partitions_created": report.partitions_created,
            "smf_rows_deleted": report.smf_rows_deleted,
            "calibration_status": report.calibration_status,
            "contestation_summary_keys": list(
                report.contestation_summary.keys()
            ),
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
    "WeeklyReport",
    "relationship_maintenance_per_tenant",
    "calibration_updater_run_once",
    "extend_partitions_job",
    "signal_memory_fabric_decay",
    "contestation_aggregation_report",
    "run_weekly",
]
