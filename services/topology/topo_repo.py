"""
services/topology/topo_repo.py — repository for the positional
embedding layer (S2, migration 0032).

Owns:

  - `models.topo_embedding` reads/writes
  - `topo_dirty_queue` enqueue/dequeue/mark-processed
  - the alpha-anchored recompute orchestration

Public API
----------

  TopoRepo(pool=None)

  .set_initial_topo(conn, *, model_id, content_embedding,
                    tenant_id, enqueue_propagation=True)
      Called from ModelsRepo._insert_core when a Model is
      created. Computes content_anchor synchronously, writes it
      to models.topo_embedding, and (optionally) enqueues the
      Model in topo_dirty_queue at depth=0 so the topology
      updater can refine it once neighbors exist.

  .enqueue(conn, *, model_id, tenant_id, cause_model_id=None,
           hop_depth=0, delta_magnitude=None)
      Add a row to topo_dirty_queue. Idempotent on
      (tenant, model, processed_at IS NULL) — a duplicate enqueue
      while the previous row is still pending no-ops.

  .enqueue_neighbors(conn, *, model_id, tenant_id, hop_depth,
                     delta_magnitude)
      Enqueue every Model that's a neighbor of `model_id` via any
      active edge. Used by the propagation worker after a
      significant recompute. Damping is the worker's responsibility.

  .dequeue_pending(conn, *, tenant_id=None, limit=50)
      Fetch up to `limit` pending rows, ordered by delta_magnitude
      DESC, enqueued_at ASC. Marks them processed-pending via a
      lease pattern (caller updates processed_at on success).

  .recompute_topo(conn, *, model_id, tenant_id, alpha=ALPHA_DEFAULT)
      Read the Model's neighbors via EdgesRepo, compute new
      topo_embedding via the alpha-anchored rule, write it. Returns
      the (prev, new, delta) triple so the worker can decide
      whether to propagate.

  .mark_processed(conn, *, queue_row_id)
      Mark a topo_dirty_queue row processed_at = now().

Direction conventions
---------------------

A Model M's "topology neighbors" are every Model that M is connected
to via an active edge — regardless of edge direction or kind. We treat
the edge graph as undirected for the purposes of topology, because
arrangement is a symmetric concept (if A is positionally near B, B
is positionally near A). The edge_kind contributes weight (a
`supports` edge counts more than a future `co_activates_with` edge)
but doesn't determine direction of influence.

For asymmetric kinds where polarity matters (future `contradicts`),
the contributed weight is NEGATIVE — the contradicting Model
pushes this Model AWAY in topo space rather than toward.

Why this lives in services/topology/, not services/models/
----------------------------------------------------------

Topology is the substrate's emergent geometry, distinct from the
relational store. ModelsRepo manages the 9-step content pipeline;
EdgesRepo manages the relational graph; TopoRepo manages the
positional layer. Keeping it separate prevents the chokepoint
helper `_set_model_relations` from growing into a god-function.
"""
from __future__ import annotations

from typing import Any, Sequence
from uuid import UUID

import asyncpg

from lib.embeddings.ollama import EMBEDDING_DIM
from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import TOPO_EMBEDDING_DIM
from lib.topology.embeddings import (
    ALPHA_DEFAULT,
    DELTA_EPSILON,
    compute_topo_embedding,
    content_anchor,
    delta_magnitude,
)


# Edge kinds that contribute to topology with POSITIVE weight (the
# Model being connected pulls the topo embedding toward this
# neighbor). Ordered by influence strength: explicit `supports` and
# `instance_of` carry the most weight; pattern lineage and
# resolution-contributors carry less. `superseded_by` carries no
# topology weight by default — supersession changes the lifecycle,
# not the position.
_TOPO_EDGE_WEIGHTS: dict[str, float] = {
    "supports": 1.0,
    "instance_of": 1.0,
    "contributes_to_resolution": 0.6,
    "superseded_by": 0.0,
    # Reserved (S4): contradicts contributes NEGATIVE weight; the
    # contradicting Model's topo pushes this Model AWAY.
    # Implementation deferred until contradicts producer ships.
    "contradicts": 0.0,
    "weakens": 0.0,
}


class TopoRepoError(CompanyOSError):
    default_code = "topo_repo_error"


class TopoRepo:
    def __init__(self, pool: asyncpg.Pool | None = None) -> None:
        # Pool is optional for the same reason as EdgesRepo: every
        # public method takes `conn` so the caller's transaction
        # owns the write.
        self._pool = pool

    # =================================================================
    # set_initial_topo — called from ModelsRepo._insert_core
    # =================================================================
    async def set_initial_topo(
        self,
        conn: asyncpg.Connection,
        *,
        model_id: UUID,
        content_embedding: Sequence[float],
        tenant_id: UUID,
        enqueue_propagation: bool = True,
    ) -> list[float]:
        """Compute content_anchor and write it as the Model's
        initial topo_embedding. Synchronous so a freshly-inserted
        Model has a non-NULL topo_embedding before commit (Pathway F
        in S3 will require this).

        Optionally enqueues the Model in topo_dirty_queue at
        hop_depth=0 so the asynchronous topology updater can refine
        the position once edges exist.
        """
        if len(content_embedding) != EMBEDDING_DIM:
            raise ValidationError(
                f"set_initial_topo: content embedding dim "
                f"{len(content_embedding)} != {EMBEDDING_DIM}"
            )
        topo = content_anchor(content_embedding)
        await conn.execute(
            """
            UPDATE models
            SET topo_embedding = $1::vector,
                topo_updated_at = now()
            WHERE id = $2
            """,
            topo,
            model_id,
        )
        if enqueue_propagation:
            await self.enqueue(
                conn,
                model_id=model_id,
                tenant_id=tenant_id,
                hop_depth=0,
                delta_magnitude=float("inf"),
            )
        return topo

    # =================================================================
    # enqueue / enqueue_neighbors / dequeue / mark_processed
    # =================================================================
    async def enqueue(
        self,
        conn: asyncpg.Connection,
        *,
        model_id: UUID,
        tenant_id: UUID,
        cause_model_id: UUID | None = None,
        hop_depth: int = 0,
        delta_magnitude: float | None = None,
    ) -> None:
        """Add a row to topo_dirty_queue. Idempotent on the
        UNIQUE NULLS NOT DISTINCT (tenant, model, processed_at)
        constraint: while a previous unprocessed row exists for
        this (tenant, model), a second enqueue collapses into it.

        Once the previous row is processed (processed_at set),
        a new enqueue creates a fresh pending row.
        """
        # Use $5 for delta_magnitude so callers passing infinity
        # land as NULL (DB has no inf for FLOAT). Workers treat
        # NULL as max priority.
        mag = delta_magnitude
        if mag is not None and (mag != mag or mag == float("inf")):
            mag = None
        await conn.execute(
            """
            INSERT INTO topo_dirty_queue
              (id, tenant_id, model_id, cause_model_id, hop_depth,
               delta_magnitude)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT ON CONSTRAINT topo_dirty_queue_dedup
            DO NOTHING
            """,
            uuid7(),
            tenant_id,
            model_id,
            cause_model_id,
            hop_depth,
            mag,
        )

    async def enqueue_neighbors(
        self,
        conn: asyncpg.Connection,
        *,
        model_id: UUID,
        tenant_id: UUID,
        hop_depth: int,
        delta_magnitude: float,
    ) -> int:
        """Enqueue every Model directly connected to model_id via
        an active edge (any kind, any direction). Returns the count
        of neighbors enqueued (some may dedup).

        Used by the topology_updater after a significant recompute,
        and by EdgesRepo when a new edge changes one or both
        endpoints' neighborhoods.
        """
        rows = await conn.fetch(
            """
            SELECT DISTINCT
              CASE
                WHEN source_model_id = $1 THEN target_model_id
                ELSE source_model_id
              END AS neighbor_id
            FROM model_edges
            WHERE tenant_id = $2
              AND status = 'active'
              AND (source_model_id = $1 OR target_model_id = $1)
            """,
            model_id,
            tenant_id,
        )
        for row in rows:
            await self.enqueue(
                conn,
                model_id=row["neighbor_id"],
                tenant_id=tenant_id,
                cause_model_id=model_id,
                hop_depth=hop_depth + 1,
                delta_magnitude=delta_magnitude,
            )
        return len(rows)

    async def dequeue_pending(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch up to `limit` pending rows, highest delta_magnitude
        first (NULLs last so first-time / infinite-priority rows
        come first), then FIFO.

        Returns list of dicts with row contents; caller is responsible
        for invoking `mark_processed` after a successful recompute.

        Note: this method does NOT use FOR UPDATE SKIP LOCKED — v1
        runs a single topology updater worker. Move to row-level
        locking if multiple workers are deployed.
        """
        if tenant_id is not None:
            rows = await conn.fetch(
                """
                SELECT id, tenant_id, model_id, cause_model_id,
                       hop_depth, delta_magnitude, enqueued_at
                FROM topo_dirty_queue
                WHERE processed_at IS NULL AND tenant_id = $1
                ORDER BY delta_magnitude DESC NULLS FIRST, enqueued_at
                LIMIT $2
                """,
                tenant_id,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT id, tenant_id, model_id, cause_model_id,
                       hop_depth, delta_magnitude, enqueued_at
                FROM topo_dirty_queue
                WHERE processed_at IS NULL
                ORDER BY delta_magnitude DESC NULLS FIRST, enqueued_at
                LIMIT $1
                """,
                limit,
            )
        return [dict(r) for r in rows]

    async def mark_processed(
        self,
        conn: asyncpg.Connection,
        *,
        queue_row_id: UUID,
    ) -> None:
        await conn.execute(
            "UPDATE topo_dirty_queue SET processed_at = now() WHERE id = $1",
            queue_row_id,
        )

    async def mark_failed(
        self,
        conn: asyncpg.Connection,
        *,
        queue_row_id: UUID,
        error: str,
    ) -> None:
        """Increment attempts + record the error, but DON'T set
        processed_at — the worker will retry on the next pass."""
        await conn.execute(
            """
            UPDATE topo_dirty_queue
            SET attempts = attempts + 1,
                last_error = $2
            WHERE id = $1
            """,
            queue_row_id,
            error,
        )

    # =================================================================
    # recompute_topo — the alpha-anchored update for one Model
    # =================================================================
    async def recompute_topo(
        self,
        conn: asyncpg.Connection,
        *,
        model_id: UUID,
        tenant_id: UUID,
        alpha: float = ALPHA_DEFAULT,
    ) -> dict[str, Any]:
        """Recompute one Model's topo_embedding via the
        alpha-anchored neighbor-mean rule.

        Reads:
          - Model's content_embedding (for content_anchor)
          - Active neighbors' topo_embeddings (any edge kind)
          - Edge weights (per kind, from _TOPO_EDGE_WEIGHTS)

        Writes:
          - models.topo_embedding (if delta > 0)
          - models.topo_updated_at

        Returns:
          {model_id, prev_topo, new_topo, delta, neighbor_count}

        The caller (worker) decides whether to propagate based on
        delta vs. epsilon thresholds.
        """
        # Read Model's content + current topo.
        row = await conn.fetchrow(
            """
            SELECT embedding, topo_embedding
            FROM models
            WHERE id = $1
            """,
            model_id,
        )
        if row is None:
            raise ValidationError(
                f"recompute_topo: model {model_id} not found",
                model_id=str(model_id),
            )
        content_emb = row["embedding"]
        prev_topo = row["topo_embedding"]
        # asyncpg returns numpy arrays for VECTOR; coerce to list.
        if content_emb is not None:
            content_emb = list(float(x) for x in content_emb)
        prev_topo_list: list[float] | None = None
        if prev_topo is not None:
            prev_topo_list = [float(x) for x in prev_topo]

        if content_emb is None:
            raise ValidationError(
                f"recompute_topo: model {model_id} missing content embedding",
                model_id=str(model_id),
            )
        anchor = content_anchor(content_emb)

        # Fetch neighbors and their topo_embeddings + edge_kind for
        # weighting. Treat the edge graph as undirected for topology
        # purposes (see module docstring).
        neighbor_rows = await conn.fetch(
            """
            SELECT DISTINCT ON (neighbor_id)
              neighbor_id,
              neighbor_topo,
              edge_kind
            FROM (
              SELECT
                CASE
                  WHEN e.source_model_id = $1 THEN e.target_model_id
                  ELSE e.source_model_id
                END AS neighbor_id,
                m.topo_embedding AS neighbor_topo,
                e.edge_kind
              FROM model_edges e
              JOIN models m ON m.id = (
                CASE
                  WHEN e.source_model_id = $1 THEN e.target_model_id
                  ELSE e.source_model_id
                END
              )
              WHERE e.tenant_id = $2
                AND e.status = 'active'
                AND (e.source_model_id = $1 OR e.target_model_id = $1)
                AND m.status = 'active'
                AND m.topo_embedding IS NOT NULL
            ) AS sub
            """,
            model_id,
            tenant_id,
        )

        neighbor_topos: list[list[float]] = []
        weights: list[float] = []
        for nr in neighbor_rows:
            kind = nr["edge_kind"]
            weight = _TOPO_EDGE_WEIGHTS.get(kind, 0.0)
            if weight == 0.0:
                continue
            neighbor_topo = [float(x) for x in nr["neighbor_topo"]]
            neighbor_topos.append(neighbor_topo)
            weights.append(weight)

        new_topo = compute_topo_embedding(
            anchor,
            neighbor_topos,
            weights if weights else None,
            alpha=alpha,
        )
        delta = delta_magnitude(prev_topo_list, new_topo)

        # Only WRITE if the change is non-trivial; saves index churn
        # on no-op recomputes. We always update topo_updated_at
        # though, so the worker's "last touched" telemetry is
        # accurate.
        if delta > 0.0:
            await conn.execute(
                """
                UPDATE models
                SET topo_embedding = $1::vector,
                    topo_updated_at = now()
                WHERE id = $2
                """,
                new_topo,
                model_id,
            )
        else:
            await conn.execute(
                "UPDATE models SET topo_updated_at = now() WHERE id = $1",
                model_id,
            )

        return {
            "model_id": model_id,
            "prev_topo": prev_topo_list,
            "new_topo": new_topo,
            "delta": delta,
            "neighbor_count": len(neighbor_topos),
        }


__all__ = [
    "TopoRepo",
    "TopoRepoError",
    "_TOPO_EDGE_WEIGHTS",
]
