"""
services/topology/tests/conftest.py — fixtures for topology
integration tests (S2).

Mirrors services/models/tests/conftest.py: per-test asyncpg pool,
single transaction wrapping each test that gets ROLLBACK at
teardown so no residue leaks. Tenant UUID isolation is the
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
async def tx_conn(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    """A connection with a transaction held open for the test body;
    ROLLBACK at teardown so all writes vanish. Registers pgvector
    codec so VECTOR-typed parameters pass through cleanly."""
    from pgvector.asyncpg import register_vector

    conn = await db_pool.acquire()
    try:
        await register_vector(conn)
    except Exception:
        pass
    tx = conn.transaction()
    await tx.start()
    try:
        yield conn
    finally:
        try:
            await tx.rollback()
        finally:
            await db_pool.release(conn)


@pytest_asyncio.fixture
async def actor_id(tx_conn: asyncpg.Connection, tenant: uuid.UUID) -> uuid.UUID:
    aid = uuid7()
    await tx_conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status,
            metadata, specification_id, created_at, last_seen_at
        ) VALUES (
            $1, $2, 'human_internal', 'Test Topo', 'topo@example.com',
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
            $3, '{}'::jsonb, 'test obs',
            NULL, TRUE, 'authoritative',
            $4, '[]'::jsonb
        )
        """,
        oid, tenant, actor_id, f"test-topo-{oid}",
    )
    return oid


def _hash_embedding(text: str, dim: int = 768) -> list[float]:
    """Deterministic 768-d unit vector. Same scheme as
    services/models/tests/conftest.py — same text → same vector."""
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
    """Insert a Model directly via SQL (bypasses ModelsRepo's
    9-step pipeline so we can stage many Models without async
    embedding round-trips). Yields a callable: make_model("name")
    -> model_id."""
    async def _make(natural: str) -> uuid.UUID:
        mid = uuid7()
        emb = _hash_embedding(natural)
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
            mid, tenant, born_from_event, natural, emb,
        )
        return mid
    return _make
