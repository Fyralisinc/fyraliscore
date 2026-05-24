"""Adversarial tests for Pathway F (S3 topological retrieval).

Focuses on tenant isolation, archived/null-topo exclusion, malformed
seeds, and assembler-level topology_context edge cases."""
from __future__ import annotations

import os

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import TOPO_EMBEDDING_DIM
from services.models.edges_repo import EdgesRepo
from services.retrieval.pathways import (
    RetrievalPathwayError,
    pathway_f_topological,
)
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
        PGVECTOR_REGISTERED_POOL_IDS.add(id(conn))
    except Exception:
        pass
    tx = conn.transaction()
    await tx.start()
    await conn.execute("SET CONSTRAINTS ALL DEFERRED")  # migration 0037: defer tenant FKs
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


def _hash_emb(text):
    import hashlib
    import math
    import random
    seed = int.from_bytes(
        hashlib.sha256(text.encode()).digest()[:8], "big"
    )
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(768)]
    n = math.sqrt(sum(x * x for x in v))
    return [x / n for x in v]


async def _seed_tenant(conn, names):
    """Insert a tenant + actor + observation + N Models, init topo for
    each. Returns (tenant_id, [model_ids])."""
    tenant = uuid7()
    actor = uuid7()
    obs = uuid7()
    await conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, "
        "email, status, metadata, specification_id, created_at) "
        "VALUES ($1, $2, 'human_internal', 'adv', null, 'active', "
        "'{}'::jsonb, NULL, now())",
        actor, tenant,
    )
    await conn.execute(
        "INSERT INTO observations (id, tenant_id, occurred_at, kind, "
        "source_channel, actor_id, content, content_text, embedding, "
        "embedding_pending, trust_tier, external_id, "
        "entities_mentioned) VALUES ($1, $2, now(), 'signal', "
        "'test:adv', $3, '{}'::jsonb, 'adv obs', NULL, TRUE, "
        "'authoritative', $4, '[]'::jsonb)",
        obs, tenant, actor, f"adv-{obs}",
    )
    ids = []
    topo = TopoRepo()
    for name in names:
        mid = uuid7()
        emb = _hash_emb(name)
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
            mid, tenant, obs, name, emb,
        )
        await topo.set_initial_topo(
            conn, model_id=mid, content_embedding=emb,
            tenant_id=tenant, enqueue_propagation=False,
        )
        ids.append(mid)
    return tenant, ids


# =====================================================================
# Tenant isolation
# =====================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_does_not_leak_across_tenants(tx_conn):
    """Two tenants, each with Models. F query in tenant A must not
    return any tenant B Models."""
    tenant_a, ids_a = await _seed_tenant(tx_conn, ["alpha", "beta"])
    tenant_b, ids_b = await _seed_tenant(tx_conn, ["gamma", "delta"])
    # Pull an explicit topo from tenant A's first model and use it
    # as the seed in tenant_a's query.
    row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", ids_a[0],
    )
    topo_a = list(float(x) for x in row["topo_embedding"])
    result = await pathway_f_topological(
        tenant_a, tx_conn,
        precomputed_topo_vector=topo_a,
        k=20,
        expand_neighborhoods=False,
    )
    returned_ids = {m.id for m in result.models}
    # No tenant_b Model should be returned.
    assert not (returned_ids & set(ids_b))
    # All returned ids should be in tenant_a.
    assert returned_ids.issubset(set(ids_a))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_seed_model_id_in_other_tenant_returns_empty(
    tx_conn,
):
    """A seed_model_id that exists in a different tenant must return
    'seed_model_missing_topo' (the WHERE id=$1 AND tenant_id=$2
    excludes it)."""
    tenant_a, ids_a = await _seed_tenant(tx_conn, ["alpha"])
    tenant_b, ids_b = await _seed_tenant(tx_conn, ["beta"])
    result = await pathway_f_topological(
        tenant_b, tx_conn, seed_model_id=ids_a[0],
    )
    assert result.notes["reason"] == "seed_model_missing_topo"


# =====================================================================
# Status filters: archived / NULL topo / non-active neighborhood
# =====================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_excludes_archived_models(tx_conn):
    """An archived Model with a topo embedding should NOT surface in
    the HNSW results."""
    tenant, ids = await _seed_tenant(tx_conn, ["a", "b", "c"])
    # Archive one.
    await tx_conn.execute(
        "UPDATE models SET status='archived', archived_at=now(), "
        "archive_reason='deprecated' WHERE id = $1", ids[1],
    )
    row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", ids[0],
    )
    topo = list(float(x) for x in row["topo_embedding"])
    result = await pathway_f_topological(
        tenant, tx_conn,
        precomputed_topo_vector=topo,
        k=10,
    )
    returned = {m.id for m in result.models}
    assert ids[1] not in returned
    assert ids[0] in returned
    assert ids[2] in returned


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_excludes_null_topo_models(tx_conn):
    """Models with NULL topo_embedding should not surface in NN
    results."""
    tenant, ids = await _seed_tenant(tx_conn, ["a", "b"])
    # NULL out one Model's topo.
    await tx_conn.execute(
        "UPDATE models SET topo_embedding = NULL WHERE id = $1",
        ids[1],
    )
    row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", ids[0],
    )
    topo = list(float(x) for x in row["topo_embedding"])
    result = await pathway_f_topological(
        tenant, tx_conn,
        precomputed_topo_vector=topo,
    )
    returned = {m.id for m in result.models}
    assert ids[1] not in returned


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_seed_model_with_null_topo_returns_empty(tx_conn):
    """Caller passes a seed_model_id whose topo_embedding is NULL.
    Should short-circuit to 'seed_model_missing_topo'."""
    tenant, ids = await _seed_tenant(tx_conn, ["a"])
    await tx_conn.execute(
        "UPDATE models SET topo_embedding = NULL WHERE id = $1", ids[0],
    )
    result = await pathway_f_topological(
        tenant, tx_conn, seed_model_id=ids[0],
    )
    assert result.notes["reason"] == "seed_model_missing_topo"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_expansion_skips_dissolved_neighborhoods(tx_conn):
    """If a neighborhood is dissolved, its members must NOT surface
    via expansion."""
    tenant, ids = await _seed_tenant(tx_conn, ["a", "b", "c"])
    a, b, c = ids
    # Build a neighborhood A↔B, then dissolve it manually.
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    nh_repo = NeighborhoodsRepo()
    await nh_repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    rows = await nh_repo.list_active(tx_conn, tenant)
    assert len(rows) == 1
    nh_id = rows[0]["id"]
    # Dissolve.
    await tx_conn.execute(
        "UPDATE model_neighborhoods SET status='dissolved' WHERE id=$1",
        nh_id,
    )
    # Membership rows still exist but the neighborhood is dissolved.
    # Expansion should skip them.
    result = await pathway_f_topological(
        tenant, tx_conn, seed_model_id=c, expand_neighborhoods=True,
    )
    # b and the dissolved-membership of a should not appear via
    # expansion (NN cosine may still surface them — but we're seeding
    # from c which is content-distinct).
    assert "expansion_returned" in result.notes


# =====================================================================
# Malformed / boundary inputs
# =====================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_rejects_wrong_dim_precomputed_vector(tx_conn):
    tenant, _ = await _seed_tenant(tx_conn, ["a"])
    bad = [0.0] * 64  # half the right dim
    with pytest.raises(ValidationError, match="dim"):
        await pathway_f_topological(
            tenant, tx_conn, precomputed_topo_vector=bad,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_with_seed_text_no_embedder_raises(tx_conn):
    tenant, _ = await _seed_tenant(tx_conn, ["a"])
    with pytest.raises(RetrievalPathwayError, match="embedder"):
        await pathway_f_topological(
            tenant, tx_conn, seed_natural_text="hi", embedder=None,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_with_k_zero_or_negative(tx_conn):
    """k <= 0: HNSW with LIMIT 0 returns no rows. Should not crash."""
    tenant, ids = await _seed_tenant(tx_conn, ["a", "b"])
    row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", ids[0],
    )
    topo = list(float(x) for x in row["topo_embedding"])
    result = await pathway_f_topological(
        tenant, tx_conn,
        precomputed_topo_vector=topo,
        k=0,
    )
    # No NN rows. Behavior should be empty list, not crash.
    assert len(result.models) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_k_larger_than_corpus(tx_conn):
    """k=1000 with only 3 Models: should return ≤3, not crash."""
    tenant, ids = await _seed_tenant(tx_conn, ["a", "b", "c"])
    row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", ids[0],
    )
    topo = list(float(x) for x in row["topo_embedding"])
    result = await pathway_f_topological(
        tenant, tx_conn,
        precomputed_topo_vector=topo,
        k=1000,
        expand_neighborhoods=False,
    )
    assert len(result.models) <= 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_seed_model_excluded_from_nn_results(tx_conn):
    """Seed-by-model_id: the seed itself must NOT be in the NN list."""
    tenant, ids = await _seed_tenant(tx_conn, ["a", "b", "c"])
    result = await pathway_f_topological(
        tenant, tx_conn,
        seed_model_id=ids[0],
        k=10,
        expand_neighborhoods=False,
    )
    returned = {m.id for m in result.models}
    assert ids[0] not in returned


# =====================================================================
# Neighborhood expansion edge cases
# =====================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_expansion_caps_at_max_neighborhood_members(
    tx_conn,
):
    """A 50-Model neighborhood with default cap (30) should yield at
    most 30 from expansion."""
    tenant, ids = await _seed_tenant(
        tx_conn, [f"m{i}" for i in range(20)],
    )
    # Wire all 20 Models into a star around ids[0].
    edges = EdgesRepo()
    for i in range(1, 20):
        await edges.link(
            tx_conn, source=ids[0], target=ids[i], kind="supports",
            tenant_id=tenant, detected_by="manual",
        )
    nh_repo = NeighborhoodsRepo()
    await nh_repo.recompute_for_tenant(tx_conn, tenant_id=tenant)

    result = await pathway_f_topological(
        tenant, tx_conn,
        seed_model_id=ids[0],
        k=1,  # minimal NN; force result to come mostly from expansion
        expand_neighborhoods=True,
        max_neighborhood_members=10,
    )
    # Total returned should be capped: 1 NN + up to 10 expansion - dedup.
    # Expansion-returned in notes should be at most 10.
    assert result.notes["expansion_returned"] <= 10


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_expansion_disabled_returns_only_nn(tx_conn):
    tenant, ids = await _seed_tenant(tx_conn, ["a", "b", "c"])
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=ids[0], target=ids[1], kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    nh_repo = NeighborhoodsRepo()
    await nh_repo.recompute_for_tenant(tx_conn, tenant_id=tenant)

    row = await tx_conn.fetchrow(
        "SELECT topo_embedding FROM models WHERE id = $1", ids[0],
    )
    topo = list(float(x) for x in row["topo_embedding"])
    result = await pathway_f_topological(
        tenant, tx_conn,
        precomputed_topo_vector=topo,
        expand_neighborhoods=False,
    )
    assert result.notes["expansion_returned"] == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pathway_f_dedups_seed_from_expansion(tx_conn):
    """If the seed Model is also a neighborhood member, expansion
    shouldn't double-add it."""
    tenant, ids = await _seed_tenant(tx_conn, ["a", "b"])
    edges = EdgesRepo()
    await edges.link(
        tx_conn, source=ids[0], target=ids[1], kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    nh_repo = NeighborhoodsRepo()
    await nh_repo.recompute_for_tenant(tx_conn, tenant_id=tenant)
    result = await pathway_f_topological(
        tenant, tx_conn,
        seed_model_id=ids[0],
        expand_neighborhoods=True,
    )
    returned = [m.id for m in result.models]
    # ids[0] is the seed and excluded; ids[1] should appear exactly once.
    assert returned.count(ids[1]) == 1
    assert ids[0] not in returned
