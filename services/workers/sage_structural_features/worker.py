"""Periodic worker for SAGE structural-feature freshness.

SAGE read quality depends on `model_structural_features` and
`model_edge_structural_features` being reasonably current. The compute
job already exists under `services.reasoning.sage.structural_features`; this worker
turns it into a first-class polling loop.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from uuid import UUID

import asyncpg
import structlog

from services.reasoning.sage.structural_features.job import recompute_features_for_tenant


_log = structlog.get_logger(__name__)

DEFAULT_INTERVAL_S = float(
    os.environ.get("SAGE_STRUCTURAL_FEATURES_INTERVAL_S", "3600")
)


@dataclass(slots=True)
class TenantStructuralFeatureReport:
    """Per-tenant counters from one structural-feature recompute."""

    tenant_id: UUID
    models_written: int = 0
    edges_written: int = 0
    error: str | None = None


@dataclass(slots=True)
class RunReport:
    """Roll-up for one worker tick."""

    tenant_reports: dict[UUID, TenantStructuralFeatureReport] = field(
        default_factory=dict
    )

    @property
    def tenants_scanned(self) -> int:
        return len(self.tenant_reports)

    @property
    def models_written(self) -> int:
        return sum(r.models_written for r in self.tenant_reports.values())

    @property
    def edges_written(self) -> int:
        return sum(r.edges_written for r in self.tenant_reports.values())

    @property
    def errors(self) -> int:
        return sum(1 for r in self.tenant_reports.values() if r.error)


async def _list_active_model_tenants(conn: asyncpg.Connection) -> list[UUID]:
    rows = await conn.fetch(
        """
        SELECT tenant_id
        FROM models
        WHERE status = 'active'
        GROUP BY tenant_id
        ORDER BY max(created_at) DESC
        """
    )
    return [r["tenant_id"] for r in rows]


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
) -> RunReport:
    """Recompute structural features for one tenant or all active tenants."""

    report = RunReport()
    async with pool.acquire() as conn:
        tenant_ids = (
            [tenant_id]
            if tenant_id is not None
            else await _list_active_model_tenants(conn)
        )
        for tid in tenant_ids:
            tenant_report = TenantStructuralFeatureReport(tenant_id=tid)
            try:
                async with conn.transaction():
                    counters = await recompute_features_for_tenant(tid, conn)
                tenant_report.models_written = int(
                    counters.get("models_written") or 0
                )
                tenant_report.edges_written = int(
                    counters.get("edges_written") or 0
                )
                _log.info(
                    "sage_structural_features.tenant_done",
                    tenant_id=str(tid),
                    models_written=tenant_report.models_written,
                    edges_written=tenant_report.edges_written,
                )
            except Exception as exc:  # noqa: BLE001
                tenant_report.error = f"{type(exc).__name__}: {exc}"
                _log.exception(
                    "sage_structural_features.tenant_failed",
                    tenant_id=str(tid),
                    error=tenant_report.error,
                )
            report.tenant_reports[tid] = tenant_report
    return report


async def run_forever(
    pool: asyncpg.Pool,
    *,
    interval_s: float = DEFAULT_INTERVAL_S,
    shutdown: asyncio.Event | None = None,
) -> None:
    """Run structural-feature recompute ticks until shutdown is set."""

    shutdown = shutdown or asyncio.Event()
    while not shutdown.is_set():
        try:
            report = await run_once(pool)
            _log.info(
                "sage_structural_features.run_done",
                tenants=report.tenants_scanned,
                models_written=report.models_written,
                edges_written=report.edges_written,
                errors=report.errors,
            )
        except Exception as exc:  # noqa: BLE001
            _log.exception(
                "sage_structural_features.run_failed",
                error=str(exc),
            )
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_s)
        except asyncio.TimeoutError:
            pass


__all__ = [
    "DEFAULT_INTERVAL_S",
    "RunReport",
    "TenantStructuralFeatureReport",
    "run_forever",
    "run_once",
]

