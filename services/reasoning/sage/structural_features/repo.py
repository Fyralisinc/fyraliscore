"""services.reasoning.sage.structural_features.repo — asyncpg repo.

Tenant-scoped CRUD over `model_structural_features` and
`model_edge_structural_features` (migration 0085). Pattern mirrors
`services/domain/models/repo.py`: pool-owning repo, optional
caller-supplied connection for transactional use, tenant_id bound at
construction.
"""
from __future__ import annotations

from typing import Iterable, Optional, Sequence
from uuid import UUID

import asyncpg
import structlog

from services.reasoning.sage.structural_features.types import (
    EdgeStructuralFeatures,
    ModelStructuralFeatures,
)

_log = structlog.get_logger(__name__)


_MODEL_COLS = (
    "model_id",
    "tenant_id",
    "degree_total",
    "degree_in",
    "degree_out",
    "clustering_coefficient",
    "core_number",
    "avg_neighbor_degree",
    "bridge_score",
    "hub_score",
    "community_id",
    "region_ids",
    "updated_at",
)

_EDGE_COLS = (
    "edge_id",
    "tenant_id",
    "source_model_id",
    "target_model_id",
    "degree_difference",
    "common_neighbors",
    "jaccard_overlap",
    "edge_betweenness_approx",
    "bridge_likelihood",
    "redundancy_score",
    "updated_at",
)


class StructuralFeaturesRepo:
    """Repo for the SAGE structural feature store.

    Construction:
        repo = StructuralFeaturesRepo(pool, tenant_id=tid)

    All write methods accept an optional `conn` to participate in an
    outer transaction (e.g. the recompute job runs everything under
    a single tenant-scoped transaction so the upsert is atomic with
    a stale-row cleanup).
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        tenant_id: UUID,
    ) -> None:
        self._pool = pool
        self._tenant_id = tenant_id

    # ------------------------------------------------------------------
    # Conn acquisition
    # ------------------------------------------------------------------

    async def _conn_ctx(self, conn: Optional[asyncpg.Connection]):
        """Yield `conn` if supplied, else acquire one from the pool."""
        if conn is not None:
            return _NoopAcquire(conn)
        return self._pool.acquire()

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    async def upsert_model_features(
        self,
        rows: Sequence[ModelStructuralFeatures],
        *,
        conn: Optional[asyncpg.Connection] = None,
    ) -> int:
        """Upsert a batch of per-Model feature rows. Returns count written."""
        if not rows:
            return 0
        params = [
            (
                r.model_id,
                self._tenant_id,
                r.degree_total,
                r.degree_in,
                r.degree_out,
                r.clustering_coefficient,
                r.core_number,
                r.avg_neighbor_degree,
                r.bridge_score,
                r.hub_score,
                r.community_id,
                list(r.region_ids or []),
            )
            for r in rows
        ]
        sql = """
            INSERT INTO model_structural_features (
              model_id, tenant_id,
              degree_total, degree_in, degree_out,
              clustering_coefficient, core_number, avg_neighbor_degree,
              bridge_score, hub_score,
              community_id, region_ids,
              updated_at
            )
            VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now()
            )
            ON CONFLICT (model_id) DO UPDATE SET
              tenant_id = EXCLUDED.tenant_id,
              degree_total = EXCLUDED.degree_total,
              degree_in = EXCLUDED.degree_in,
              degree_out = EXCLUDED.degree_out,
              clustering_coefficient = EXCLUDED.clustering_coefficient,
              core_number = EXCLUDED.core_number,
              avg_neighbor_degree = EXCLUDED.avg_neighbor_degree,
              bridge_score = EXCLUDED.bridge_score,
              hub_score = EXCLUDED.hub_score,
              community_id = EXCLUDED.community_id,
              region_ids = EXCLUDED.region_ids,
              updated_at = now()
        """
        async with await self._conn_ctx(conn) as c:
            await c.executemany(sql, params)
        return len(params)

    async def upsert_edge_features(
        self,
        rows: Sequence[EdgeStructuralFeatures],
        *,
        conn: Optional[asyncpg.Connection] = None,
    ) -> int:
        """Upsert a batch of per-edge feature rows. Returns count written."""
        if not rows:
            return 0
        params = [
            (
                r.edge_id,
                self._tenant_id,
                r.source_model_id,
                r.target_model_id,
                r.degree_difference,
                r.common_neighbors,
                r.jaccard_overlap,
                r.edge_betweenness_approx,
                r.bridge_likelihood,
                r.redundancy_score,
            )
            for r in rows
        ]
        sql = """
            INSERT INTO model_edge_structural_features (
              edge_id, tenant_id,
              source_model_id, target_model_id,
              degree_difference, common_neighbors, jaccard_overlap,
              edge_betweenness_approx, bridge_likelihood, redundancy_score,
              updated_at
            )
            VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now()
            )
            ON CONFLICT (edge_id) DO UPDATE SET
              tenant_id = EXCLUDED.tenant_id,
              source_model_id = EXCLUDED.source_model_id,
              target_model_id = EXCLUDED.target_model_id,
              degree_difference = EXCLUDED.degree_difference,
              common_neighbors = EXCLUDED.common_neighbors,
              jaccard_overlap = EXCLUDED.jaccard_overlap,
              edge_betweenness_approx = EXCLUDED.edge_betweenness_approx,
              bridge_likelihood = EXCLUDED.bridge_likelihood,
              redundancy_score = EXCLUDED.redundancy_score,
              updated_at = now()
        """
        async with await self._conn_ctx(conn) as c:
            await c.executemany(sql, params)
        return len(params)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    async def get_for_models(
        self,
        ids: Iterable[UUID],
        *,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[ModelStructuralFeatures]:
        id_list = list(ids)
        if not id_list:
            return []
        sql = f"""
            SELECT {', '.join(_MODEL_COLS)}
            FROM model_structural_features
            WHERE tenant_id = $1 AND model_id = ANY($2::uuid[])
        """
        async with await self._conn_ctx(conn) as c:
            records = await c.fetch(sql, self._tenant_id, id_list)
        return [_hydrate_model(r) for r in records]

    async def get_for_edges(
        self,
        ids: Iterable[UUID],
        *,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[EdgeStructuralFeatures]:
        id_list = list(ids)
        if not id_list:
            return []
        sql = f"""
            SELECT {', '.join(_EDGE_COLS)}
            FROM model_edge_structural_features
            WHERE tenant_id = $1 AND edge_id = ANY($2::uuid[])
        """
        async with await self._conn_ctx(conn) as c:
            records = await c.fetch(sql, self._tenant_id, id_list)
        return [_hydrate_edge(r) for r in records]

    async def top_hubs(
        self,
        limit: int,
        *,
        min_score: float = 0.0,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[ModelStructuralFeatures]:
        sql = f"""
            SELECT {', '.join(_MODEL_COLS)}
            FROM model_structural_features
            WHERE tenant_id = $1
              AND hub_score IS NOT NULL
              AND hub_score >= $2
            ORDER BY hub_score DESC NULLS LAST
            LIMIT $3
        """
        async with await self._conn_ctx(conn) as c:
            records = await c.fetch(sql, self._tenant_id, float(min_score), int(limit))
        return [_hydrate_model(r) for r in records]

    async def top_bridges(
        self,
        limit: int,
        *,
        min_score: float = 0.0,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[ModelStructuralFeatures]:
        sql = f"""
            SELECT {', '.join(_MODEL_COLS)}
            FROM model_structural_features
            WHERE tenant_id = $1
              AND bridge_score IS NOT NULL
              AND bridge_score >= $2
            ORDER BY bridge_score DESC NULLS LAST
            LIMIT $3
        """
        async with await self._conn_ctx(conn) as c:
            records = await c.fetch(sql, self._tenant_id, float(min_score), int(limit))
        return [_hydrate_model(r) for r in records]


# ----------------------------------------------------------------------
# Hydration + helpers
# ----------------------------------------------------------------------


def _hydrate_model(r: asyncpg.Record) -> ModelStructuralFeatures:
    return ModelStructuralFeatures(
        model_id=r["model_id"],
        tenant_id=r["tenant_id"],
        degree_total=r["degree_total"],
        degree_in=r["degree_in"],
        degree_out=r["degree_out"],
        clustering_coefficient=r["clustering_coefficient"],
        core_number=r["core_number"],
        avg_neighbor_degree=r["avg_neighbor_degree"],
        bridge_score=r["bridge_score"],
        hub_score=r["hub_score"],
        community_id=r["community_id"],
        region_ids=list(r["region_ids"] or []),
        updated_at=r["updated_at"],
    )


def _hydrate_edge(r: asyncpg.Record) -> EdgeStructuralFeatures:
    return EdgeStructuralFeatures(
        edge_id=r["edge_id"],
        tenant_id=r["tenant_id"],
        source_model_id=r["source_model_id"],
        target_model_id=r["target_model_id"],
        degree_difference=r["degree_difference"],
        common_neighbors=r["common_neighbors"],
        jaccard_overlap=r["jaccard_overlap"],
        edge_betweenness_approx=r["edge_betweenness_approx"],
        bridge_likelihood=r["bridge_likelihood"],
        redundancy_score=r["redundancy_score"],
        updated_at=r["updated_at"],
    )


class _NoopAcquire:
    """Async-context wrapper that yields a pre-existing connection.

    Lets the repo treat caller-supplied connections and pool-acquired
    connections identically:

        async with await self._conn_ctx(conn) as c: ...
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def __aenter__(self) -> asyncpg.Connection:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


__all__ = ["StructuralFeaturesRepo"]
