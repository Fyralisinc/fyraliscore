"""
Shared fixtures for Wave 4-C calibration updater tests.

Same hermetic pattern as services/models/tests/conftest.py:
  * per-test pool
  * per-test transaction that rolls back
  * tenant UUID isolation
  * deterministic 768-d embeddings (no Ollama round-trip)

We DO NOT TRUNCATE — tenant UUID isolation is the boundary.
"""
from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7


_RESETTING_FILTER = "ignore::pytest.PytestUnraisableExceptionWarning"


def pytest_collection_modifyitems(config, items):
    for item in items:
        path_str = str(item.fspath)
        if (
            "services/workers/calibration_updater/tests/" in path_str
            or "services/workers/precipitation/tests/" in path_str
            or "services/contestability/tests/" in path_str
            or "services/falsifiers/tests/" in path_str
        ):
            item.add_marker(pytest.mark.filterwarnings(_RESETTING_FILTER))


@pytest.fixture
def tenant() -> uuid.UUID:
    return uuid7()


@pytest.fixture
def other_tenant() -> uuid.UUID:
    return uuid7()


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping integration test.")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def fresh_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """Tenant isolation, not TRUNCATE — consistent with Wave 1-C pattern."""
    yield db_pool


@pytest_asyncio.fixture
async def tx_conn(fresh_db: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    from pgvector.asyncpg import register_vector

    conn = await fresh_db.acquire()
    try:
        await register_vector(conn)
    except Exception:
        pass
    tx = conn.transaction()
    await tx.start()
    # Migration 0037: defer tenant FK to commit (rollback teardown
    # never triggers the check).
    await conn.execute("SET CONSTRAINTS ALL DEFERRED")
    try:
        yield conn
    finally:
        try:
            await tx.rollback()
        finally:
            await fresh_db.release(conn)


# ---------------------------------------------------------------------
# Actor / observation builders
# ---------------------------------------------------------------------


async def insert_actor(
    conn: asyncpg.Connection,
    tenant: uuid.UUID,
    *,
    display_name: str = "Test Alice",
    email: str | None = None,
    actor_type: str = "human_internal",
) -> uuid.UUID:
    aid = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status,
            metadata, specification_id, created_at, last_seen_at
        ) VALUES (
            $1, $2, $3, $4, $5, 'active',
            '{}'::jsonb, NULL, now(), NULL
        )
        """,
        aid, tenant, actor_type, display_name,
        email or f"{display_name.lower().replace(' ', '.')}-{aid}@example.com",
    )
    return aid


async def insert_observation(
    conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID | None = None,
    *,
    kind: str = "signal",
    source_channel: str = "test:signal",
    content_text: str = "test observation",
) -> uuid.UUID:
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, now(), $3, $4, $5,
            '{}'::jsonb, $6,
            NULL, TRUE, 'authoritative',
            $7, '[]'::jsonb
        )
        """,
        oid, tenant, kind, source_channel, actor_id,
        content_text, f"external-{oid}",
    )
    return oid


@pytest_asyncio.fixture
async def actor_id(tx_conn: asyncpg.Connection, tenant: uuid.UUID) -> uuid.UUID:
    return await insert_actor(tx_conn, tenant)


@pytest_asyncio.fixture
async def born_from_event(
    tx_conn: asyncpg.Connection, tenant: uuid.UUID, actor_id: uuid.UUID
) -> uuid.UUID:
    return await insert_observation(tx_conn, tenant, actor_id)


# ---------------------------------------------------------------------
# Deterministic 768-d embeddings — same algorithm as Wave 1-C tests.
# ---------------------------------------------------------------------


def make_embedding(text: str, *, dim: int = 768) -> list[float]:
    import random

    seed = int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8], "big"
    )
    rng = random.Random(seed)
    vec = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0:
        return vec
    return [x / norm for x in vec]


def similar_embedding(base: list[float], *, jitter: float = 0.05) -> list[float]:
    import random

    rng = random.Random(42)
    v = [x + rng.gauss(0.0, jitter) for x in base]
    norm = sum(x * x for x in v) ** 0.5
    return [x / norm for x in v] if norm else v


# ---------------------------------------------------------------------
# Raw-SQL Model inserter (pool-less — we can't use ModelsRepo without
# an embedder + transaction plumbing).
# ---------------------------------------------------------------------


async def insert_model(
    conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    born_from_event_id: uuid.UUID,
    proposition: dict,
    natural: str,
    embedding: list[float],
    scope_actors: list[uuid.UUID] | None = None,
    scope_entities: list | None = None,
    confidence: float = 0.6,
    confidence_at_assertion: float | None = None,
    evaluate_at=None,
    resolved_at=None,
    resolution_outcome: bool | None = None,
    status: str = "active",
    supporting_model_ids: list[uuid.UUID] | None = None,
    falsifier: dict | None = None,
) -> uuid.UUID:
    import json

    mid = uuid7()
    cfa = confidence_at_assertion if confidence_at_assertion is not None else confidence
    await conn.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, activation, falsifier,
            signal_readings, reading_contestable,
            supporting_event_ids, supporting_model_ids, evidential_weight,
            status, evaluate_at, resolution_criteria,
            contributing_models, visible_to_subjects,
            confidence_at_assertion, activation_coefficient,
            resolved_at, resolution_outcome
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            $7::uuid[], $8::jsonb, $9::jsonb,
            $10, 1.0, $11::jsonb,
            '[]'::jsonb, TRUE,
            '{}', $12::uuid[], 0.5,
            $13, $14, NULL,
            '{}', TRUE,
            $15, 1.0,
            $16, $17
        )
        """,
        mid,
        tenant,
        born_from_event_id,
        json.dumps(proposition, sort_keys=True),
        natural,
        embedding,
        scope_actors or [],
        json.dumps(scope_entities or []),
        json.dumps({"kind": "open_ended"}),
        confidence,
        json.dumps(falsifier) if falsifier is not None else None,
        supporting_model_ids or [],
        status,
        evaluate_at,
        cfa,
        resolved_at,
        resolution_outcome,
    )
    return mid


__all__ = [
    "insert_actor",
    "insert_observation",
    "insert_model",
    "make_embedding",
    "similar_embedding",
]
