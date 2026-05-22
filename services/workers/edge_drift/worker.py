"""
services/workers/edge_drift/worker.py — drift detector for the
unified Model-to-Model edge primitive (S1, migration 0031).

What it does
------------
Continuously samples active Models and verifies that the legacy
array columns (`supporting_model_ids`, `contributing_models`) are in
sync with the typed `model_edges` rows that should mirror them.

The contract is:

  supporting_model_ids[]         ==
        sources of incoming `supports` edges to this Model
        ∪
        targets of outgoing `instance_of` edges from this Model

  contributing_models[]          ==
        sources of incoming `contributes_to_resolution` edges
        to this Model

A non-empty symmetric difference on either side is "drift" — somebody
mutated an array column without going through the chokepoint helper
(`services.models.repo._set_model_relations`), or wrote an edge
without updating the corresponding array. Either way, dual-write
discipline broke. The drift detector emits a metric per kind so the
violation gets caught loudly during the dual-write phase.

Why a sampling worker (not a full-table check)
----------------------------------------------
At organizational scale (~100k Models / tenant), comparing every
Model on every tick costs more than it earns. The 200-Model random
sample (default) catches systematic drift within a few ticks while
the per-tick cost stays bounded. If sustained drift is detected,
operators can run the backfill (idempotent) to converge.

Why best-effort metrics, not page-on-divergence
-----------------------------------------------
S1 is the dual-write phase. Drift IS expected briefly during
deploys (a migration lands before the chokepoint refactor reaches
every site). The worker's job is to make drift OBSERVABLE, not to
panic. Operators decide whether sustained drift requires human
attention (the plan's gate is "14 consecutive days of zero drift
before considering Stage 2").

Public API
----------
  run_once(pool, *, tenant_id=None, sample_size=200) -> DriftReport
      Single sweep. Returns a structured report.

Usage
-----
Run as a periodic background task (every 30 min by default;
configurable via env EDGE_DRIFT_INTERVAL_S). Same pattern as the
calibration_updater worker.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from services.models.edges_repo import EdgesRepo


_log = logging.getLogger(__name__)

# Default sample size per tenant per tick. Override via the
# `sample_size` arg or env EDGE_DRIFT_SAMPLE_SIZE.
DEFAULT_SAMPLE_SIZE = int(os.environ.get("EDGE_DRIFT_SAMPLE_SIZE", "200"))


@dataclass
class TenantDriftReport:
    """Per-tenant drift accounting for one tick."""
    tenant_id: UUID
    models_sampled: int = 0
    # Number of Models whose supporting_model_ids array differs from
    # the union of incoming `supports` edges + outgoing `instance_of`
    # edge targets. Non-zero = drift.
    supports_drift_models: int = 0
    # Number of Models whose contributing_models array differs from
    # incoming `contributes_to_resolution` edges. Non-zero = drift.
    contributes_drift_models: int = 0
    # Sample of drifted model_ids for ad-hoc inspection. Capped to
    # avoid unbounded payload size in metrics.
    drift_examples: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DriftReport:
    """Roll-up of one drift sweep across all tenants checked."""
    tenants: list[TenantDriftReport] = field(default_factory=list)
    sample_size: int = 0

    @property
    def total_drift(self) -> int:
        return sum(
            t.supports_drift_models + t.contributes_drift_models
            for t in self.tenants
        )


# Maximum drift examples per tenant report (caps payload).
_MAX_EXAMPLES_PER_TENANT = 5


async def _list_tenants(conn: asyncpg.Connection) -> list[UUID]:
    """All tenants with at least one active Model."""
    rows = await conn.fetch(
        "SELECT DISTINCT tenant_id FROM models WHERE status = 'active'"
    )
    return [r["tenant_id"] for r in rows]


def _check_tenant_sample(rows: list[dict[str, Any]]) -> TenantDriftReport:
    """Pure-Python drift checker over a tenant's sample rows. Kept
    pure so unit tests can drive it directly without spinning up
    Postgres.
    """
    report = TenantDriftReport(
        tenant_id=rows[0]["tenant_id"] if rows else None,  # type: ignore[arg-type]
        models_sampled=len(rows),
    )
    for row in rows:
        supporting = set(row["supporting_array"] or [])
        sup_edges = set(row["supports_edges"] or [])
        inst_targets = set(row["instance_of_targets"] or [])
        contributing = set(row["contributing_array"] or [])
        contrib_edges = set(row["contributes_edges"] or [])

        # supporting_model_ids carries BOTH supporters AND pattern
        # back-links (legacy mixed semantics).
        sup_expected = sup_edges | inst_targets
        sup_diff = supporting.symmetric_difference(sup_expected)
        contrib_diff = contributing.symmetric_difference(contrib_edges)

        if sup_diff:
            report.supports_drift_models += 1
            if len(report.drift_examples) < _MAX_EXAMPLES_PER_TENANT:
                report.drift_examples.append({
                    "model_id": str(row["model_id"]),
                    "kind": "supports",
                    "missing_in_edges": sorted(
                        str(x) for x in supporting - sup_expected
                    ),
                    "extra_in_edges": sorted(
                        str(x) for x in sup_expected - supporting
                    ),
                })
        if contrib_diff:
            report.contributes_drift_models += 1
            if len(report.drift_examples) < _MAX_EXAMPLES_PER_TENANT:
                report.drift_examples.append({
                    "model_id": str(row["model_id"]),
                    "kind": "contributes_to_resolution",
                    "missing_in_edges": sorted(
                        str(x) for x in contributing - contrib_edges
                    ),
                    "extra_in_edges": sorted(
                        str(x) for x in contrib_edges - contributing
                    ),
                })
    return report


async def _check_tenant(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    sample_size: int,
    edges: EdgesRepo,
) -> TenantDriftReport:
    sample = await edges.get_drift_sample(
        conn, tenant_id=tenant_id, sample_size=sample_size
    )
    if not sample:
        return TenantDriftReport(tenant_id=tenant_id, models_sampled=0)
    # Inject tenant_id into rows so _check_tenant_sample sees it.
    for r in sample:
        r["tenant_id"] = tenant_id
    return _check_tenant_sample(sample)


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> DriftReport:
    """One sweep across all tenants (or one tenant if `tenant_id` is
    set). Returns a DriftReport.

    Logs a structured warning per tenant with non-zero drift,
    suitable for downstream metric aggregation.
    """
    edges = EdgesRepo(pool=pool)
    report = DriftReport(sample_size=sample_size)
    async with pool.acquire() as conn:
        if tenant_id is None:
            tenants = await _list_tenants(conn)
        else:
            tenants = [tenant_id]
        for tid in tenants:
            tr = await _check_tenant(
                conn, tid, sample_size=sample_size, edges=edges
            )
            report.tenants.append(tr)
            if (
                tr.supports_drift_models
                or tr.contributes_drift_models
            ):
                _log.warning(
                    "edge_drift detected",
                    extra={
                        "tenant_id": str(tid),
                        "sampled": tr.models_sampled,
                        "supports_drift": tr.supports_drift_models,
                        "contributes_drift": tr.contributes_drift_models,
                        "examples": tr.drift_examples,
                    },
                )
    return report


__all__ = [
    "DriftReport",
    "TenantDriftReport",
    "run_once",
    "_check_tenant_sample",  # exposed for tests
]
