"""
services/workers/neighborhood_detector/tests/test_worker.py — single
integration test that the worker invokes the repo over a real DB.
The repo's behavior is exhaustively tested in
services/topology/tests/test_neighborhoods_repo.py; here we just
verify the scheduler glue.
"""
from __future__ import annotations

import os

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from services.models.edges_repo import EdgesRepo
from services.topology.topo_repo import TopoRepo
from services.workers.neighborhood_detector.worker import run_once


@pytest_asyncio.fixture
async def pool():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    p = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    try:
        yield p
    finally:
        await p.close()


@pytest_asyncio.fixture
async def seeded_tenant(pool):
    """Two Models with one supports edge between them."""
    import hashlib
    import math
    import random as rng_mod
    from pgvector.asyncpg import register_vector

    def _emb(seed: str) -> list[float]:
        s = int.from_bytes(
            hashlib.sha256(seed.encode()).digest()[:8], "big"
        )
        rng = rng_mod.Random(s)
        v = [rng.gauss(0.0, 1.0) for _ in range(768)]
        n = math.sqrt(sum(x * x for x in v))
        return [x / n for x in v] if n else v

    tenant = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    a = uuid7()
    b = uuid7()
    async with pool.acquire() as conn:
        try:
            await register_vector(conn)
        except Exception:
            pass
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO actors (id, tenant_id, type, display_name, "
                "email, status, metadata, specification_id, created_at) "
                "VALUES ($1, $2, 'human_internal', 'NB worker test', null, "
                "'active', '{}'::jsonb, NULL, now())",
                actor_id, tenant,
            )
            await conn.execute(
                "INSERT INTO observations (id, tenant_id, occurred_at, kind, "
                "source_channel, actor_id, content, content_text, "
                "embedding, embedding_pending, trust_tier, external_id, "
                "entities_mentioned) VALUES ($1, $2, now(), 'signal', "
                "'test:nb-worker', $3, '{}'::jsonb, 'nb obs', NULL, TRUE, "
                "'authoritative', $4, '[]'::jsonb)",
                obs_id, tenant, actor_id, f"nb-{obs_id}",
            )
            for mid, name in ((a, "A"), (b, "B")):
                await conn.execute(
                    """
                    INSERT INTO models (
                        id, tenant_id, born_from_event_id,
                        proposition, "natural", embedding,
                        scope_actors, scope_entities, scope_temporal,
                        confidence, falsifier, signal_readings,
                        supporting_event_ids, supporting_model_ids,
                        contributing_models, status,
                        confidence_at_assertion
                    ) VALUES (
                        $1, $2, $3,
                        '{"kind":"state","subject":"x","assertion":"y"}'::jsonb,
                        $4, $5,
                        '{}'::uuid[], '[]'::jsonb,
                        '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
                        0.6, NULL, '[]'::jsonb,
                        '{}'::uuid[], '{}'::uuid[],
                        '{}'::uuid[], 'active',
                        0.6
                    )
                    """,
                    mid, tenant, obs_id, name, _emb(name),
                )
            topo = TopoRepo()
            for mid, name in ((a, "A"), (b, "B")):
                await topo.set_initial_topo(
                    conn, model_id=mid,
                    content_embedding=_emb(name),
                    tenant_id=tenant,
                    enqueue_propagation=False,
                )
            edges = EdgesRepo()
            await edges.link(
                conn, source=a, target=b, kind="supports",
                tenant_id=tenant, detected_by="manual",
            )
    try:
        yield tenant, a, b
    finally:
        async with pool.acquire() as conn:
            for sql in (
                "DELETE FROM model_neighborhood_membership WHERE tenant_id = $1",
                "DELETE FROM model_neighborhoods WHERE tenant_id = $1",
                "DELETE FROM topo_dirty_queue WHERE tenant_id = $1",
                "DELETE FROM model_edges WHERE tenant_id = $1",
                "DELETE FROM model_reeval_queue WHERE tenant_id = $1",
                "DELETE FROM models WHERE tenant_id = $1",
                "DELETE FROM observations WHERE tenant_id = $1",
                "DELETE FROM actors WHERE tenant_id = $1",
            ):
                try:
                    await conn.execute(sql, tenant)
                except Exception:
                    pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_run_once_produces_neighborhood(pool, seeded_tenant):
    tenant, a, b = seeded_tenant
    out = await run_once(pool, tenant_id=tenant)
    assert tenant in out
    report = out[tenant]
    assert report.communities_after_prune == 1
    assert report.new_neighborhoods == 1
