"""Integration tests for services.retrieval.pathways.pathway_f_topological
+ neighborhood expansion. Uses the topology test fixtures to seed
Models with topo embeddings and edges, then runs Pathway F end-to-end."""
from __future__ import annotations

import os

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from lib.shared.types import TOPO_EMBEDDING_DIM
from services.models.edges_repo import EdgesRepo
from services.retrieval.pathways import pathway_f_topological
from services.topology.neighborhoods_repo import NeighborhoodsRepo
from services.topology.topo_repo import TopoRepo


@pytest_asyncio.fixture
async def db_pool():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def tx_conn(db_pool):
    from pgvector.asyncpg import register_vector
    from services.models.repo import PGVECTOR_REGISTERED_POOL_IDS

    conn = await db_pool.acquire()
    try:
        await register_vector(conn)
        # Tell the retrieval layer the codec is live on this conn so
        # Pathway F binds vectors as numpy arrays (not string literals
        # — strings get rejected once the codec is installed).
        PGVECTOR_REGISTERED_POOL_IDS.add(id(conn))
        inner = getattr(conn, "_con", None)
        if inner is not None:
            PGVECTOR_REGISTERED_POOL_IDS.add(id(inner))
    except Exception:
        pass
    tx = conn.transaction()
    await tx.start()
    # Migration 0037 — tenant FKs deferred to commit.
    await conn.execute("SET CONSTRAINTS ALL DEFERRED")
    try:
        yield conn
    finally:
        try:
            await tx.rollback()
        finally:
            try:
                PGVECTOR_REGISTERED_POOL_IDS.discard(id(conn))
            except Exception:
                pass
            await db_pool.release(conn)


def _hash_embedding(text, dim=768):
    import hashlib
    import math
    import random
    seed = int.from_bytes(
        hashlib.sha256(text.encode()).digest()[:8], "big"
    )
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    n = math.sqrt(sum(x * x for x in v))
    if n == 0:
        return v
    return [x / n for x in v]


async def _seed_tenant_with_topology(conn):
    """Helper: stand up 4 Models forming 2 tiny clusters with topo
    embeddings + edges + materialized neighborhoods."""
    tenant = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    await conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, "
        "email, status, metadata, specification_id, created_at) "
        "VALUES ($1, $2, 'human_internal', 'pathway-f-test', null, "
        "'active', '{}'::jsonb, NULL, now())",
        actor_id, tenant,
    )
    await conn.execute(
        "INSERT INTO observations (id, tenant_id, occurred_at, kind, "
        "source_channel, actor_id, content, content_text, embedding, "
        "embedding_pending, trust_tier, external_id, "
        "entities_mentioned) VALUES ($1, $2, now(), 'signal', "
        "'test:f', $3, '{}'::jsonb, 'pathway f obs', NULL, TRUE, "
        "'authoritative', $4, '[]'::jsonb)",
        obs_id, tenant, actor_id, f"obs-{obs_id}",
    )
    a, b, c, d = (uuid7() for _ in range(4))
    for mid, name in ((a, "alpha"), (b, "beta"), (c, "gamma"), (d, "delta")):
        emb = _hash_embedding(name)
        await conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, falsifier, signal_readings,
                supporting_event_ids, supporting_model_ids,
                contributing_models, status, confidence_at_assertion
            ) VALUES (
                $1, $2, $3,
                '{"kind":"state","subject":"x","assertion":"y"}'::jsonb,
                $4, $5,
                '{}'::uuid[], '[]'::jsonb,
                '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
                0.6, NULL, '[]'::jsonb,
                '{}'::uuid[], '{}'::uuid[],
                '{}'::uuid[], 'active', 0.6
            )
            """,
            mid, tenant, obs_id, name, emb,
        )

    topo = TopoRepo()
    for mid, name in ((a, "alpha"), (b, "beta"), (c, "gamma"), (d, "delta")):
        await topo.set_initial_topo(
            conn, model_id=mid, content_embedding=_hash_embedding(name),
            tenant_id=tenant, enqueue_propagation=False,
        )

    edges = EdgesRepo()
    await edges.link(
        conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await edges.link(
        conn, source=c, target=d, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )

    nh_repo = NeighborhoodsRepo()
    await nh_repo.recompute_for_tenant(conn, tenant_id=tenant)
    return tenant, a, b, c, d, obs_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_with_seed_model_returns_neighborhood_members(
    tx_conn,
):
    tenant, a, b, c, d, _ = await _seed_tenant_with_topology(tx_conn)
    result = await pathway_f_topological(
        tenant, tx_conn,
        seed_model_id=a,
        k=5,
        expand_neighborhoods=True,
    )
    assert result.source_pathway == "F"
    ids = {m.id for m in result.models}
    # The seed Model itself is excluded (same-id filter).
    assert a not in ids
    # b is in seed's neighborhood → must surface via expansion.
    assert b in ids


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_with_precomputed_topo_vector(tx_conn):
    tenant, a, b, c, d, _ = await _seed_tenant_with_topology(tx_conn)
    # Read alpha's topo vector and reuse it as the seed.
    row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", a,
    )
    topo = list(float(x) for x in row["topo_embedding"])
    assert len(topo) == TOPO_EMBEDDING_DIM
    result = await pathway_f_topological(
        tenant, tx_conn,
        precomputed_topo_vector=topo,
        k=10,
        expand_neighborhoods=False,  # pure HNSW
    )
    ids = {m.id for m in result.models}
    # Without filter the seed itself surfaces (no seed_model_id given).
    assert a in ids
    # b should be the next-nearest because it sits in the same cluster.
    assert b in ids
    assert result.notes["expand_neighborhoods"] is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_returns_empty_on_empty_seed(tx_conn):
    tenant = uuid7()
    result = await pathway_f_topological(
        tenant, tx_conn,
    )
    assert result.source_pathway == "F"
    assert result.models == []
    assert result.notes["reason"] == "empty_seed"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_seed_model_missing_topo(tx_conn):
    """A model_id that doesn't exist (or has no topo) returns empty
    result with reason."""
    tenant = uuid7()
    bogus = uuid7()
    result = await pathway_f_topological(
        tenant, tx_conn,
        seed_model_id=bogus,
    )
    assert result.notes["reason"] == "seed_model_missing_topo"
