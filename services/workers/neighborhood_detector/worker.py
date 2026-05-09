"""
services/workers/neighborhood_detector/worker.py — periodic
community-detection sweep over the active edge graph (S2,
migration 0032).

Loop
----
  Every INTERVAL_S (default 1h):
    For each tenant:
      - call NeighborhoodsRepo.recompute_for_tenant()
      - log RecomputeReport telemetry

The recompute is fully orchestrated inside the repo (load Models +
edges, detect communities, prune singletons, match to existing
neighborhoods for stable IDs, upsert + dissolve, refresh
membership). The worker is just the scheduler.

Public API
----------
  run_once(pool, *, tenant_id=None) -> dict[tenant_id, RecomputeReport]
      Single sweep. If tenant_id is None, processes every tenant
      with at least one active Model.

Tunable
-------
  - NEIGHBORHOOD_DETECTOR_INTERVAL_S (default 3600s = 1h) — sweep
    cadence
  - lib/topology/community.py constants — algorithm thresholds
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any
from uuid import UUID

import asyncpg

from services.topology.neighborhoods_repo import (
    NeighborhoodsRepo,
    RecomputeReport,
)


_log = logging.getLogger(__name__)

DEFAULT_INTERVAL_S = float(
    os.environ.get("NEIGHBORHOOD_DETECTOR_INTERVAL_S", "3600")
)


async def _list_tenants(conn: asyncpg.Connection) -> list[UUID]:
    rows = await conn.fetch(
        "SELECT DISTINCT tenant_id FROM models WHERE status = 'active'"
    )
    return [r["tenant_id"] for r in rows]


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
) -> dict[UUID, RecomputeReport]:
    """One detection sweep. Returns per-tenant reports."""
    repo = NeighborhoodsRepo(pool=pool)
    out: dict[UUID, RecomputeReport] = {}
    async with pool.acquire() as conn:
        if tenant_id is None:
            tenants = await _list_tenants(conn)
        else:
            tenants = [tenant_id]
        for tid in tenants:
            try:
                async with conn.transaction():
                    report = await repo.recompute_for_tenant(
                        conn, tenant_id=tid
                    )
                out[tid] = report
                if report.communities_after_prune > 0:
                    _log.info(
                        "neighborhood_detector recomputed",
                        extra={
                            "tenant_id": str(tid),
                            "models": report.models_seen,
                            "edges": report.edges_seen,
                            "communities": report.communities_after_prune,
                            "matched": report.matched_to_existing,
                            "new": report.new_neighborhoods,
                            "dissolved": report.dissolved_neighborhoods,
                        },
                    )
            except Exception:  # noqa: BLE001
                _log.exception(
                    "neighborhood_detector failed for tenant",
                    extra={"tenant_id": str(tid)},
                )
    return out


async def run_forever(
    pool: asyncpg.Pool,
    *,
    interval_s: float = DEFAULT_INTERVAL_S,
) -> None:
    _log.info(
        "neighborhood_detector started",
        extra={"interval_s": interval_s},
    )
    while True:
        try:
            await run_once(pool)
        except Exception:  # noqa: BLE001
            _log.exception("neighborhood_detector sweep crashed")
        await asyncio.sleep(interval_s)


__all__ = [
    "run_once",
    "run_forever",
    "DEFAULT_INTERVAL_S",
]
