"""services/model_trace/tests/conftest.py — fixtures for trace walks.

Mirrors services/topology/tests/conftest.py: each test holds a single
transaction open and rolls back at teardown. Tenant UUID is the
hermetic boundary.
"""
from __future__ import annotations

import os
import uuid
from collections.abc import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7


@pytest.fixture
def tenant() -> uuid.UUID:
    return uuid7()


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping integration test.")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def tx_conn(
    db_pool: asyncpg.Pool,
) -> AsyncGenerator[asyncpg.Connection, None]:
    from pgvector.asyncpg import register_vector

    conn = await db_pool.acquire()
    try:
        await register_vector(conn)
    except Exception:
        pass
    tx = conn.transaction()
    await tx.start()
    await conn.execute("SET CONSTRAINTS ALL DEFERRED")
    try:
        yield conn
    finally:
        try:
            await tx.rollback()
        finally:
            await db_pool.release(conn)


@pytest_asyncio.fixture
async def actor_id(
    tx_conn: asyncpg.Connection, tenant: uuid.UUID,
) -> uuid.UUID:
    aid = uuid7()
    await tx_conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status,
            metadata, specification_id, created_at, last_seen_at
        ) VALUES (
            $1, $2, 'human_internal', 'Trace Test', 'trace@example.com',
            'active', '{}'::jsonb, NULL, now(), NULL
        )
        """,
        aid, tenant,
    )
    return aid


@pytest_asyncio.fixture
async def born_from_event(
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
) -> uuid.UUID:
    oid = uuid7()
    await tx_conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, now(), 'signal', 'test:signal',
            $3, '{}'::jsonb, 'trace obs',
            NULL, TRUE, 'authoritative',
            $4, '[]'::jsonb
        )
        """,
        oid, tenant, actor_id, f"trace-obs-{oid}",
    )
    return oid


def _hash_embedding(text: str, dim: int = 768) -> list[float]:
    import hashlib
    import math
    import random
    seed = int.from_bytes(
        hashlib.sha256(text.encode()).digest()[:8], "big"
    )
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = math.sqrt(sum(x * x for x in v))
    if norm == 0:
        return v
    return [x / norm for x in v]


@pytest_asyncio.fixture
async def make_model(
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
):
    """Insert a Model directly via SQL with a chosen proposition_kind.
    Returns the model_id. `kind` defaults to 'state'."""
    async def _make(
        natural: str,
        kind: str = "state",
        proposition: dict | None = None,
    ) -> uuid.UUID:
        import json
        mid = uuid7()
        emb = _hash_embedding(natural)
        prop = proposition or {
            "kind": kind,
            "subject": "x",
            "assertion": natural,
        }
        await tx_conn.execute(
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
                $6::jsonb, $4, $5,
                '{}'::uuid[], '[]'::jsonb,
                '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
                0.6, NULL, '[]'::jsonb,
                '{}'::uuid[], '{}'::uuid[],
                '{}'::uuid[], 'active',
                0.6
            )
            """,
            mid, tenant, born_from_event, natural, emb, json.dumps(prop),
        )
        return mid
    return _make


@pytest_asyncio.fixture
async def link_edge(tx_conn: asyncpg.Connection, tenant: uuid.UUID):
    """Insert a model_edges row directly. Bypasses EdgesRepo so we can
    stage graph shapes without DAG / weight rules getting in the way."""
    async def _link(
        source: uuid.UUID,
        target: uuid.UUID,
        kind: str = "supports",
        weight: float | None = None,
    ) -> None:
        await tx_conn.execute(
            """
            INSERT INTO model_edges (
                id, tenant_id, source_model_id, target_model_id,
                edge_kind, weight, metadata, status, detected_by
            ) VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, 'active', 'test')
            ON CONFLICT ON CONSTRAINT model_edges_unique DO NOTHING
            """,
            uuid7(), tenant, source, target, kind, weight,
        )
    return _link
