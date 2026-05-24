"""Background sweeper for the latent relationship topology field.

Insert-time topology generation catches local relationships around a
new Model. This worker periodically revisits a bounded frontier of
high-activation Models so older but still-important memory can form
new candidates as the organization changes.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from uuid import UUID

import asyncpg
import structlog

from services.relationships.repo import (
    RelationshipCandidateMetrics,
    RelationshipCandidatesRepo,
)
from services.topology import LatentTopologyService, TopologySweepReport


_log = structlog.get_logger(__name__)

DEFAULT_INTERVAL_S = float(os.environ.get("TOPOLOGY_SWEEPER_INTERVAL_S", "900"))
DEFAULT_LIMIT_PER_TENANT = int(
    os.environ.get("TOPOLOGY_SWEEPER_LIMIT_PER_TENANT", "50")
)
DEFAULT_MIN_ACTIVATION = float(
    os.environ.get("TOPOLOGY_SWEEPER_MIN_ACTIVATION", "0.15")
)


@dataclass
class RunReport:
    tenant_reports: dict[UUID, TopologySweepReport] = field(default_factory=dict)
    candidate_metrics_after: dict[UUID, RelationshipCandidateMetrics] = (
        field(default_factory=dict)
    )

    @property
    def candidates_inserted(self) -> int:
        return sum(r.candidates_inserted for r in self.tenant_reports.values())

    @property
    def think_triggers_enqueued(self) -> int:
        return sum(r.think_triggers_enqueued for r in self.tenant_reports.values())


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
    limit_per_tenant: int = DEFAULT_LIMIT_PER_TENANT,
    min_activation: float = DEFAULT_MIN_ACTIVATION,
    enqueue_think: bool = True,
    service: LatentTopologyService | None = None,
    relationship_repo: RelationshipCandidatesRepo | None = None,
) -> RunReport:
    service = service or LatentTopologyService()
    relationship_repo = relationship_repo or RelationshipCandidatesRepo()
    async with pool.acquire() as conn:
        if tenant_id is not None:
            tenant_ids = [tenant_id]
        else:
            rows = await conn.fetch(
                """
                SELECT tenant_id
                FROM models
                WHERE status = 'active'
                  AND embedding IS NOT NULL
                GROUP BY tenant_id
                ORDER BY max(created_at) DESC
                """
            )
            tenant_ids = [r["tenant_id"] for r in rows]

        report = RunReport()
        for tid in tenant_ids:
            async with conn.transaction():
                tenant_report = await service.sweep_tenant(
                    conn,
                    tenant_id=tid,
                    limit=limit_per_tenant,
                    min_activation=min_activation,
                    enqueue_think=enqueue_think,
                )
            report.tenant_reports[tid] = tenant_report
            metrics = await relationship_repo.metrics(conn, tenant_id=tid)
            report.candidate_metrics_after[tid] = metrics
            _log.info(
                "topology_sweeper.tenant_done",
                tenant_id=str(tid),
                models_seen=tenant_report.models_seen,
                models_skipped=tenant_report.models_skipped,
                neighbors_considered=tenant_report.neighbors_considered,
                candidates_ranked=tenant_report.candidates_ranked,
                candidates_inserted=tenant_report.candidates_inserted,
                duplicates_suppressed=tenant_report.duplicates_suppressed,
                think_triggers_enqueued=tenant_report.think_triggers_enqueued,
                open_candidates=metrics.open_count,
                candidate_acceptance_rate=metrics.acceptance_rate,
                oldest_open_age_seconds=metrics.oldest_open_age_seconds,
                errors=len(tenant_report.errors),
            )
        return report


async def run_forever(
    pool: asyncpg.Pool,
    *,
    interval_s: float = DEFAULT_INTERVAL_S,
    limit_per_tenant: int = DEFAULT_LIMIT_PER_TENANT,
    min_activation: float = DEFAULT_MIN_ACTIVATION,
    shutdown: asyncio.Event | None = None,
) -> None:
    shutdown = shutdown or asyncio.Event()
    while not shutdown.is_set():
        try:
            report = await run_once(
                pool,
                limit_per_tenant=limit_per_tenant,
                min_activation=min_activation,
            )
            _log.info(
                "topology_sweeper.run_done",
                tenants=len(report.tenant_reports),
                candidates_inserted=report.candidates_inserted,
                think_triggers_enqueued=report.think_triggers_enqueued,
                open_candidates=sum(
                    m.open_count for m in report.candidate_metrics_after.values()
                ),
            )
        except Exception as exc:  # noqa: BLE001
            _log.exception("topology_sweeper.run_failed", error=str(exc))
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


__all__ = [
    "DEFAULT_INTERVAL_S",
    "DEFAULT_LIMIT_PER_TENANT",
    "DEFAULT_MIN_ACTIVATION",
    "RunReport",
    "run_forever",
    "run_once",
]
