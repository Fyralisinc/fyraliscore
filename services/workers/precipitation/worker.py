"""
services/workers/precipitation/worker.py — Wave 4-C nightly entry point.

Single public entry: `run_once(pool, *, tenant_id=None)`. Invoked by
the cron harness; tests call it directly with a per-test pool.
"""
from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import asyncpg

from services.workers.precipitation.clustering import (
    DENSITY_THRESHOLD,
    MIN_CLUSTER_SIZE,
    cluster_active_models,
)
from services.workers.precipitation.proposer import (
    enqueue_pattern_review_triggers,
    write_candidates,
)


@dataclass
class PrecipitationResult:
    """Bookkeeping returned by `run_once` for observability + tests."""
    tenant_id: UUID | None
    clusters_found: int
    candidates_written: int
    triggers_enqueued: int


async def run_once(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
    min_cluster_size: int = MIN_CLUSTER_SIZE,
    density_threshold: float = DENSITY_THRESHOLD,
) -> PrecipitationResult:
    """
    One pass of the precipitation pipeline.

    1. Cluster active hypothesis/concern Models via HDBSCAN.
    2. Write one `pattern_candidates` row per dense cluster.
    3. Enqueue a T4 `pattern_review` trigger per fresh candidate.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            clusters = await cluster_active_models(
                conn,
                tenant_id=tenant_id,
                min_cluster_size=min_cluster_size,
                density_threshold=density_threshold,
            )
            candidate_ids = await write_candidates(conn, clusters)
            # Only enqueue triggers for freshly-inserted candidates.
            # `write_candidates` returns existing ids for duplicates,
            # so `enqueue_pattern_review_triggers` filters those inside
            # based on the promoted_at/rejected_at state.
            trigger_ids = await enqueue_pattern_review_triggers(
                conn, candidate_ids
            )
    return PrecipitationResult(
        tenant_id=tenant_id,
        clusters_found=len(clusters),
        candidates_written=len(candidate_ids),
        triggers_enqueued=len(trigger_ids),
    )


__all__ = ["run_once", "PrecipitationResult"]
