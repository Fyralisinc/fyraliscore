"""services/reasoning/sage/topology_optimizer/api.py — Phase 13 entry-point wrapper.

A thin functional facade over `TopologyOptimizer.optimize` so callers
(post-commit hook in Synthesis, background sweep jobs, tests) can run
one optimization pass without manually wiring repos. Construct an
optimizer with default repos against `pool` + `tenant_id`, call
`.optimize`, return the `OptimizationRunReport`.

Use this wrapper unless you need to inject custom repos (e.g. a test
that wants to count calls, or a transaction-scoped run that shares
repos with surrounding code — pass a `conn` and let the bound repos
inherit it).
"""
from __future__ import annotations

from uuid import UUID

import asyncpg

from services.reasoning.sage.topology_optimizer.cadence import (
    OptimizationCadenceRequest,
    run_optimization_pass,
)
from services.reasoning.sage.topology_optimizer.types import OptimizationRunReport


async def optimize_topology(
    *,
    pool: asyncpg.Pool | None,
    tenant_id: UUID,
    inquiry_session_id: UUID,
    trigger_event: str,
    conn: asyncpg.Connection | None = None,
) -> OptimizationRunReport:
    """Construct a default `TopologyOptimizer` and run one optimization pass.

    Parameters mirror `TopologyOptimizer.optimize`. `pool` may be None
    only when the caller passes `conn`; otherwise repo default
    construction needs the pool for its acquire path.
    """
    return await run_optimization_pass(
        pool=pool,
        request=OptimizationCadenceRequest(
            tenant_id=tenant_id,
            inquiry_session_id=inquiry_session_id,
            trigger_event=trigger_event,
            source="api",
        ),
        conn=conn,
    )


__all__ = ["optimize_topology"]
