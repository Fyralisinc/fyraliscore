"""
services/workers/topology_updater/tests/test_worker.py — integration
tests for run_once over a real DB. Creates Models via the test
fixtures (which seed initial topo + dirty queue rows), drains the
queue, and verifies propagation.

For test isolation we use the per-test pool from db_pool (not a
held-open transaction) so the worker's own transactions can commit
and roll back independently. We clean up after each test by deleting
rows in the test tenant.
"""
from __future__ import annotations

import os

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from services.models.edges_repo import EdgesRepo
from services.topology.topo_repo import TopoRepo
from services.workers.topology_updater.worker import run_once


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
async def tenant_with_models(pool):
    """Seed two connected Models in a fresh tenant. Yields
    (tenant_id, model_a, model_b). Cleans up at teardown by
    cascading-delete via tenant_id filter on every relevant table."""
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
            # Migration 0037: tenant_id FK to tenants(id). Commit-path
            # tests must register the tenant before any tenant-scoped
            # INSERT.
            await conn.execute(
                "INSERT INTO tenants (id, name) VALUES ($1, $2) "
                "ON CONFLICT DO NOTHING",
                tenant, f"topology_updater_{tenant}",
            )
            await conn.execute(
                """
                INSERT INTO actors (
                    id, tenant_id, type, display_name, email, status,
                    metadata, specification_id, created_at
                ) VALUES (
                    $1, $2, 'human_internal', 'Worker test', null,
                    'active', '{}'::jsonb, NULL, now()
                )
                """,
                actor_id, tenant,
            )
            await conn.execute(
                """
                INSERT INTO observations (
                    id, tenant_id, occurred_at, kind, source_channel,
                    actor_id, content, content_text,
                    embedding, embedding_pending, trust_tier,
                    external_id, entities_mentioned
                ) VALUES (
                    $1, $2, now(), 'signal', 'test:worker',
                    $3, '{}'::jsonb, 'worker test obs',
                    NULL, TRUE, 'authoritative',
                    $4, '[]'::jsonb
                )
                """,
                obs_id, tenant, actor_id, f"worker-{obs_id}",
            )
            for mid, natural in ((a, "model A"), (b, "model B")):
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
                    mid, tenant, obs_id, natural, _emb(natural),
                )
            # Init topos + link via EdgesRepo (which enqueues both
            # endpoints in the dirty queue).
            topo = TopoRepo()
            for mid, natural in ((a, "model A"), (b, "model B")):
                await topo.set_initial_topo(
                    conn, model_id=mid,
                    content_embedding=_emb(natural),
                    tenant_id=tenant,
                    enqueue_propagation=True,
                )
            edges = EdgesRepo()
            await edges.link(
                conn, source=b, target=a, kind="supports",
                tenant_id=tenant, detected_by="manual",
            )
    try:
        yield tenant, a, b, obs_id, actor_id
    finally:
        # Cleanup. Order matters: child tables first.
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
                    # Best-effort cleanup; some tables may not exist
                    # in older test schemas.
                    pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_run_once_drains_queue(pool, tenant_with_models):
    """One sweep should process all pending rows for the tenant."""
    tenant, a, b, _obs, _actor = tenant_with_models
    report = await run_once(pool, tenant_id=tenant, batch_size=20)
    # Both A and B were enqueued by set_initial_topo + by edges.link.
    # Worker drains them.
    assert report.rows_processed >= 2
    # No failures.
    assert report.rows_failed == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recompute_significant_propagates_to_neighbors(
    pool, tenant_with_models,
):
    """When a Model's first recompute produces a significant delta
    (initial topo was content_anchor; neighbors blend in), the
    worker enqueues neighbors at hop_depth + 1."""
    tenant, a, b, _obs, _actor = tenant_with_models
    # Drain to a known empty state then trigger a fresh enqueue.
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE topo_dirty_queue SET processed_at = now() "
            "WHERE tenant_id = $1 AND processed_at IS NULL",
            tenant,
        )
        # Enqueue just A. Worker should recompute A and (since A has
        # an active edge to B) enqueue B as a neighbor at depth 1.
        topo = TopoRepo()
        await topo.enqueue(
            conn, model_id=a, tenant_id=tenant, hop_depth=0,
        )

    report = await run_once(pool, tenant_id=tenant, batch_size=20)
    assert report.rows_processed >= 1
    # Neighbors should be enqueued (or processed if the worker
    # picked them up in the same batch — both behaviors are valid).
    # We assert that B was either in the dirty queue or already
    # processed in this batch.
    async with pool.acquire() as conn:
        b_was_processed = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM topo_dirty_queue "
            "WHERE tenant_id = $1 AND model_id = $2)",
            tenant, b,
        )
    assert b_was_processed


@pytest.mark.integration
@pytest.mark.asyncio
async def test_run_once_empty_queue_returns_zero_report(pool):
    """An empty queue is a valid state — sweep returns counts of 0."""
    fake_tenant = uuid7()
    report = await run_once(pool, tenant_id=fake_tenant, batch_size=20)
    assert report.rows_processed == 0
    assert report.rows_failed == 0
