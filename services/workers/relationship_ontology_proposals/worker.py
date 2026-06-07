"""Periodic aggregation for relationship ontology proposals."""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from uuid import UUID

import asyncpg
import structlog

from services.relationships.ontology_proposals import (
    RelationshipOntologyProposalsRepo,
)


_log = structlog.get_logger(__name__)

DEFAULT_INTERVAL_S = float(
    os.environ.get("RELATIONSHIP_ONTOLOGY_PROPOSALS_INTERVAL_S", "900")
)
DEFAULT_MIN_EXAMPLES = int(
    os.environ.get("RELATIONSHIP_ONTOLOGY_PROPOSALS_MIN_EXAMPLES", "3")
)
DEFAULT_LIMIT_PER_TENANT = int(
    os.environ.get("RELATIONSHIP_ONTOLOGY_PROPOSALS_LIMIT_PER_TENANT", "500")
)


@dataclass(slots=True)
class TenantOntologyProposalReport:
    tenant_id: UUID
    proposals_upserted: int = 0
    review_ready: int = 0
    error: str | None = None


@dataclass(slots=True)
class RunReport:
    tenant_reports: dict[UUID, TenantOntologyProposalReport] = field(
        default_factory=dict
    )

    @property
    def tenants_scanned(self) -> int:
        return len(self.tenant_reports)

    @property
    def proposals_upserted(self) -> int:
        return sum(r.proposals_upserted for r in self.tenant_reports.values())

    @property
    def review_ready(self) -> int:
        return sum(r.review_ready for r in self.tenant_reports.values())

    @property
    def errors(self) -> int:
        return sum(1 for r in self.tenant_reports.values() if r.error)


async def _list_candidate_tenants(conn: asyncpg.Connection) -> list[UUID]:
    rows = await conn.fetch(
        """
        SELECT tenant_id
        FROM relationship_candidates
        WHERE candidate_kind = 'edge_type'
          AND review_status IN ('candidate', 'needs_review')
        GROUP BY tenant_id
        ORDER BY max(created_at) DESC
        """
    )
    return [r["tenant_id"] for r in rows]


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    exists = await conn.fetchval("SELECT to_regclass($1)", f"public.{table_name}")
    return exists is not None


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
    minimum_distinct_examples: int = DEFAULT_MIN_EXAMPLES,
    limit_per_tenant: int = DEFAULT_LIMIT_PER_TENANT,
    repo: RelationshipOntologyProposalsRepo | None = None,
) -> RunReport:
    repo = repo or RelationshipOntologyProposalsRepo()
    report = RunReport()
    async with pool.acquire() as conn:
        if not await _table_exists(conn, "relationship_ontology_proposals"):
            _log.warning(
                "relationship_ontology_proposals.table_missing",
                hint="apply db/migrations/0071_relationship_ontology_proposals.sql",
            )
            return report
        if not await _table_exists(conn, "relationship_candidates"):
            return report
        tenant_ids = (
            [tenant_id] if tenant_id is not None else await _list_candidate_tenants(conn)
        )
        for tid in tenant_ids:
            tenant_report = TenantOntologyProposalReport(tenant_id=tid)
            try:
                async with conn.transaction():
                    proposals = await repo.aggregate_from_edge_type_candidates(
                        conn,
                        tenant_id=tid,
                        minimum_distinct_examples=minimum_distinct_examples,
                        limit=limit_per_tenant,
                    )
                tenant_report.proposals_upserted = len(proposals)
                tenant_report.review_ready = sum(
                    1 for proposal in proposals
                    if proposal.get("status") == "review_ready"
                )
                _log.info(
                    "relationship_ontology_proposals.tenant_done",
                    tenant_id=str(tid),
                    proposals_upserted=tenant_report.proposals_upserted,
                    review_ready=tenant_report.review_ready,
                )
            except Exception as exc:  # noqa: BLE001
                tenant_report.error = f"{type(exc).__name__}: {exc}"
                _log.exception(
                    "relationship_ontology_proposals.tenant_failed",
                    tenant_id=str(tid),
                    error=tenant_report.error,
                )
            report.tenant_reports[tid] = tenant_report
    return report


async def run_forever(
    pool: asyncpg.Pool,
    *,
    interval_s: float = DEFAULT_INTERVAL_S,
    minimum_distinct_examples: int = DEFAULT_MIN_EXAMPLES,
    limit_per_tenant: int = DEFAULT_LIMIT_PER_TENANT,
    shutdown: asyncio.Event | None = None,
) -> None:
    shutdown = shutdown or asyncio.Event()
    while not shutdown.is_set():
        try:
            report = await run_once(
                pool,
                minimum_distinct_examples=minimum_distinct_examples,
                limit_per_tenant=limit_per_tenant,
            )
            _log.info(
                "relationship_ontology_proposals.run_done",
                tenants=report.tenants_scanned,
                proposals_upserted=report.proposals_upserted,
                review_ready=report.review_ready,
                errors=report.errors,
            )
        except Exception as exc:  # noqa: BLE001
            _log.exception(
                "relationship_ontology_proposals.run_failed",
                error=str(exc),
            )
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


__all__ = [
    "DEFAULT_INTERVAL_S",
    "DEFAULT_LIMIT_PER_TENANT",
    "DEFAULT_MIN_EXAMPLES",
    "RunReport",
    "TenantOntologyProposalReport",
    "run_forever",
    "run_once",
]

