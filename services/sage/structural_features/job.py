"""services.sage.structural_features.job — recompute job entrypoint.

Skeleton for the Phase 5 acceptance criterion "periodic full
recomputation" (fyralis-sage-synthesis-self-evolution.md §1604-1644).
No scheduler is wired here — call sites are expected to drive this
from the existing background-worker loop.

Flow:
  1. Pull all active Models for the tenant.
  2. Pull all active edges among those Models from `model_edges`.
  3. Build the adjacency snapshot.
  4. Compute per-Model and per-edge features (pure async).
  5. Upsert via `StructuralFeaturesRepo`.

The compute layer is pure — all I/O is concentrated here so future
incremental variants (single-Model recompute, edge-delta recompute)
can reuse the same compute primitives.
"""
from __future__ import annotations

from typing import Optional
from uuid import UUID

import asyncpg
import structlog

from services.sage.structural_features.compute import (
    build_adjacency,
    compute_edge_features,
    compute_model_features,
)
from services.sage.structural_features.repo import StructuralFeaturesRepo
from services.sage.structural_features.types import StructuralEdge

_log = structlog.get_logger(__name__)


async def _fetch_active_model_ids(
    conn: asyncpg.Connection, tenant_id: UUID
) -> list[UUID]:
    rows = await conn.fetch(
        """
        SELECT id
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
        """,
        tenant_id,
    )
    return [r["id"] for r in rows]


async def _fetch_active_edges(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    model_ids: list[UUID],
) -> list[StructuralEdge]:
    if not model_ids:
        return []
    rows = await conn.fetch(
        """
        SELECT id, source_model_id, target_model_id, edge_kind, weight
        FROM model_edges
        WHERE tenant_id = $1
          AND status = 'active'
          AND source_model_id = ANY($2::uuid[])
          AND target_model_id = ANY($2::uuid[])
        """,
        tenant_id,
        model_ids,
    )
    return [
        StructuralEdge(
            edge_id=r["id"],
            source_model_id=r["source_model_id"],
            target_model_id=r["target_model_id"],
            edge_kind=r["edge_kind"],
            weight=r["weight"],
        )
        for r in rows
    ]


async def recompute_features_for_tenant(
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    pool: Optional[asyncpg.Pool] = None,
) -> dict[str, int]:
    """Full recompute of structural features for one tenant.

    `conn` is used for reads (and, if `pool` is None, for writes too
    via the repo's caller-supplied-conn path). When a `pool` is
    given, the repo uses it for writes — useful when the caller
    wants reads and writes on different connections.

    Returns a counters dict: {"models_written": n, "edges_written": m}.
    """
    log = _log.bind(tenant_id=str(tenant_id))
    log.info("sage.structural_features.recompute.start")

    model_ids = await _fetch_active_model_ids(conn, tenant_id)
    edges = await _fetch_active_edges(conn, tenant_id, model_ids)
    log.info(
        "sage.structural_features.recompute.snapshot",
        models=len(model_ids),
        edges=len(edges),
    )

    undirected, _out, _in = build_adjacency(model_ids, edges)
    model_rows = await compute_model_features(
        model_ids, edges, tenant_id=tenant_id
    )
    edge_rows = await compute_edge_features(
        edges, undirected, tenant_id=tenant_id
    )

    # If no separate pool is supplied, the repo writes on `conn` —
    # which keeps the entire recompute inside the caller's
    # transaction (desirable: full snapshot atomicity).
    repo_pool = pool if pool is not None else None
    repo = StructuralFeaturesRepo(repo_pool, tenant_id=tenant_id)  # type: ignore[arg-type]

    models_written = await repo.upsert_model_features(model_rows, conn=conn)
    edges_written = await repo.upsert_edge_features(edge_rows, conn=conn)

    log.info(
        "sage.structural_features.recompute.done",
        models_written=models_written,
        edges_written=edges_written,
    )
    return {
        "models_written": models_written,
        "edges_written": edges_written,
    }


__all__ = ["recompute_features_for_tenant"]
