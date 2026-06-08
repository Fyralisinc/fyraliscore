"""Polling worker for the SAGE topology optimizer.

The optimizer consumes `inquiry_outcome_events` and updates the Discovery
Utility Layer. Because those updates reinforce/decay utility state, this
worker records a durable one-row checkpoint per inquiry session before it
runs the optimizer. That keeps polling idempotent across restarts.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from services.reasoning.sage.topology_optimizer import optimize_topology


_log = structlog.get_logger(__name__)

DEFAULT_INTERVAL_S = float(
    os.environ.get("SAGE_TOPOLOGY_OPTIMIZER_INTERVAL_S", "60")
)
DEFAULT_LOOKBACK_HOURS = int(
    os.environ.get("SAGE_TOPOLOGY_OPTIMIZER_LOOKBACK_HOURS", "24")
)
DEFAULT_LIMIT = int(os.environ.get("SAGE_TOPOLOGY_OPTIMIZER_LIMIT", "50"))


@dataclass(slots=True)
class SessionOptimizationReport:
    """One inquiry session optimization attempt."""

    tenant_id: UUID
    inquiry_session_id: UUID
    status: str
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass(slots=True)
class RunReport:
    """Roll-up for one optimizer worker tick."""

    sessions: list[SessionOptimizationReport] = field(default_factory=list)

    @property
    def processed(self) -> int:
        return len(self.sessions)

    @property
    def completed(self) -> int:
        return sum(1 for s in self.sessions if s.status == "completed")

    @property
    def failed(self) -> int:
        return sum(1 for s in self.sessions if s.status == "failed")


async def _ensure_checkpoint_table(conn: asyncpg.Connection) -> bool:
    exists = await conn.fetchval(
        "SELECT to_regclass('public.sage_topology_optimizer_runs')"
    )
    if exists is not None:
        return True
    _log.warning(
        "sage_topology_optimizer.checkpoint_table_missing",
        hint="apply db/migrations/0070_sage_optimizer_worker_runs.sql",
    )
    return False


async def _claim_sessions(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID | None,
    lookback_hours: int,
    limit: int,
) -> list[asyncpg.Record]:
    """Claim unprocessed inquiry sessions with outcome events."""

    has_checkpoint = await _ensure_checkpoint_table(conn)
    if not has_checkpoint:
        return []

    rows = await conn.fetch(
        """
        WITH candidates AS (
          SELECT
            e.tenant_id,
            e.inquiry_session_id,
            max(e.created_at) AS last_event_at
          FROM inquiry_outcome_events e
          JOIN inquiry_sessions s
            ON s.id = e.inquiry_session_id
           AND s.tenant_id = e.tenant_id
          LEFT JOIN sage_topology_optimizer_runs r
            ON r.tenant_id = e.tenant_id
           AND r.inquiry_session_id = e.inquiry_session_id
          WHERE r.inquiry_session_id IS NULL
            AND ($1::uuid IS NULL OR e.tenant_id = $1)
            AND e.created_at >= now() - (($2::text)::interval)
            AND s.status IN ('completed', 'deferred', 'failed')
          GROUP BY e.tenant_id, e.inquiry_session_id
          ORDER BY max(e.created_at) ASC
          LIMIT $3
        )
        INSERT INTO sage_topology_optimizer_runs (
          tenant_id, inquiry_session_id, trigger_event, status
        )
        SELECT
          tenant_id, inquiry_session_id, 'worker_poll', 'running'
        FROM candidates
        ON CONFLICT (tenant_id, inquiry_session_id) DO NOTHING
        RETURNING tenant_id, inquiry_session_id
        """,
        tenant_id,
        f"{lookback_hours} hours",
        limit,
    )
    return list(rows)


async def _mark_completed(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    inquiry_session_id: UUID,
    metrics: dict[str, Any],
) -> None:
    await conn.execute(
        """
        UPDATE sage_topology_optimizer_runs
        SET status = 'completed',
            error = NULL,
            metrics = $3::jsonb,
            completed_at = now()
        WHERE tenant_id = $1
          AND inquiry_session_id = $2
        """,
        tenant_id,
        inquiry_session_id,
        metrics,
    )


async def _mark_failed(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    inquiry_session_id: UUID,
    error: str,
) -> None:
    await conn.execute(
        """
        UPDATE sage_topology_optimizer_runs
        SET status = 'failed',
            error = $3,
            completed_at = now()
        WHERE tenant_id = $1
          AND inquiry_session_id = $2
        """,
        tenant_id,
        inquiry_session_id,
        error[:4000],
    )


def _report_metrics(report: Any) -> dict[str, Any]:
    metrics = dict(getattr(report, "metrics", {}) or {})
    metrics.update(
        {
            "affordance_reinforces": getattr(
                report, "affordance_reinforces", 0
            ),
            "affordance_decays": getattr(report, "affordance_decays", 0),
            "shortcut_creates_or_bumps": getattr(
                report, "shortcut_creates_or_bumps", 0
            ),
            "shortcut_decays": getattr(report, "shortcut_decays", 0),
            "negative_memory_inserts": getattr(
                report, "negative_memory_inserts", 0
            ),
            "region_refreshes": getattr(report, "region_refreshes", 0),
            "question_policy_updates": getattr(
                report, "question_policy_updates", 0
            ),
            "canonical_merge_candidates": len(
                getattr(report, "canonical_merge_candidates", ()) or ()
            ),
            "canonical_split_candidates": len(
                getattr(report, "canonical_split_candidates", ()) or ()
            ),
            "canonical_promote_candidates": len(
                getattr(report, "canonical_promote_candidates", ()) or ()
            ),
            "canonical_demote_candidates": len(
                getattr(report, "canonical_demote_candidates", ()) or ()
            ),
        }
    )
    return metrics


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    limit: int = DEFAULT_LIMIT,
) -> RunReport:
    """Optimize newly completed inquiry sessions with outcome events."""

    report = RunReport()
    async with pool.acquire() as conn:
        claimed = await _claim_sessions(
            conn,
            tenant_id=tenant_id,
            lookback_hours=lookback_hours,
            limit=limit,
        )

    for row in claimed:
        tid: UUID = row["tenant_id"]
        session_id: UUID = row["inquiry_session_id"]
        try:
            opt_report = await optimize_topology(
                pool=pool,
                tenant_id=tid,
                inquiry_session_id=session_id,
                trigger_event="worker_poll",
            )
            metrics = _report_metrics(opt_report)
            async with pool.acquire() as conn:
                await _mark_completed(
                    conn,
                    tenant_id=tid,
                    inquiry_session_id=session_id,
                    metrics=metrics,
                )
            report.sessions.append(
                SessionOptimizationReport(
                    tenant_id=tid,
                    inquiry_session_id=session_id,
                    status="completed",
                    metrics=metrics,
                )
            )
            _log.info(
                "sage_topology_optimizer.session_done",
                tenant_id=str(tid),
                inquiry_session_id=str(session_id),
                **metrics,
            )
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"
            async with pool.acquire() as conn:
                await _mark_failed(
                    conn,
                    tenant_id=tid,
                    inquiry_session_id=session_id,
                    error=error,
                )
            report.sessions.append(
                SessionOptimizationReport(
                    tenant_id=tid,
                    inquiry_session_id=session_id,
                    status="failed",
                    error=error,
                )
            )
            _log.exception(
                "sage_topology_optimizer.session_failed",
                tenant_id=str(tid),
                inquiry_session_id=str(session_id),
                error=error,
            )
    return report


async def run_forever(
    pool: asyncpg.Pool,
    *,
    interval_s: float = DEFAULT_INTERVAL_S,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    limit: int = DEFAULT_LIMIT,
    shutdown: asyncio.Event | None = None,
) -> None:
    """Run optimizer ticks until shutdown is set."""

    shutdown = shutdown or asyncio.Event()
    while not shutdown.is_set():
        try:
            report = await run_once(
                pool,
                lookback_hours=lookback_hours,
                limit=limit,
            )
            _log.info(
                "sage_topology_optimizer.run_done",
                processed=report.processed,
                completed=report.completed,
                failed=report.failed,
            )
        except Exception as exc:  # noqa: BLE001
            _log.exception(
                "sage_topology_optimizer.run_failed",
                error=str(exc),
            )
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


__all__ = [
    "DEFAULT_INTERVAL_S",
    "DEFAULT_LIMIT",
    "DEFAULT_LOOKBACK_HOURS",
    "RunReport",
    "SessionOptimizationReport",
    "run_forever",
    "run_once",
]
