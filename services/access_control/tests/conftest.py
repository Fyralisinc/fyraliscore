"""
Wave 5-A access control test fixtures.

Follows the Wave 4-C hermetic pattern:
  * per-test pool
  * per-test transaction that rolls back (tenant UUID isolation)
  * helpers reused from the calibration conftest for actors,
    observations, models

We also expose access-control-specific helpers:
  * seed_commitment
  * seed_goal
  * seed_resource
  * seed_decision
"""
from __future__ import annotations

import hashlib
import json
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
        if "services/access_control/tests/" in path_str:
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
    """Tenant isolation — no TRUNCATE. Tests use fresh tenant UUIDs."""
    yield db_pool


@pytest_asyncio.fixture
async def tx_conn(
    fresh_db: asyncpg.Pool,
) -> AsyncGenerator[asyncpg.Connection, None]:
    from pgvector.asyncpg import register_vector

    conn = await fresh_db.acquire()
    try:
        await register_vector(conn)
    except Exception:
        pass
    tx = conn.transaction()
    await tx.start()
    # Migration 0037: defer tenant FK to commit (which never fires for
    # the rollback teardown).
    await conn.execute("SET CONSTRAINTS ALL DEFERRED")
    try:
        yield conn
    finally:
        try:
            await tx.rollback()
        finally:
            await fresh_db.release(conn)


# ---------------------------------------------------------------------
# Commit-path fixtures — for the matview refresh tests which REQUIRE
# committed data (matviews see the last-committed snapshot, not the
# uncommitted in-progress transaction). These tests explicitly clean
# up their tenant's rows at the end instead of relying on rollback.
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def committed_conn(
    fresh_db: asyncpg.Pool,
) -> AsyncGenerator[asyncpg.Connection, None]:
    from pgvector.asyncpg import register_vector

    conn = await fresh_db.acquire()
    try:
        await register_vector(conn)
    except Exception:
        pass
    try:
        yield conn
    finally:
        await fresh_db.release(conn)


# ---------------------------------------------------------------------
# Entity builders — raw SQL, no repo dependencies
# ---------------------------------------------------------------------


async def _ensure_tenant(conn: asyncpg.Connection, tenant: uuid.UUID) -> None:
    """Migration 0037 added an FK from every tenant_id column to
    tenants(id). Tests that COMMIT (committed_conn path) need a real
    tenants row to satisfy the FK; tests that ROLLBACK (tx_conn) have
    SET CONSTRAINTS ALL DEFERRED so this is a redundant no-op for them.
    Idempotent via ON CONFLICT."""
    await conn.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        tenant, f"test_tenant_{tenant}",
    )


async def insert_actor(
    conn: asyncpg.Connection,
    tenant: uuid.UUID,
    *,
    display_name: str | None = None,
    email: str | None = None,
    actor_type: str = "human_internal",
    metadata: dict | None = None,
    status: str = "active",
) -> uuid.UUID:
    await _ensure_tenant(conn, tenant)
    aid = uuid7()
    nm = display_name or f"Actor-{str(aid)[:8]}"
    await conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status,
            metadata, specification_id, created_at, last_seen_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7::jsonb, NULL, now(), NULL
        )
        """,
        aid, tenant, actor_type, nm,
        email or f"{nm.lower().replace(' ', '.')}-{aid}@example.com",
        status,
        json.dumps(metadata or {}),
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
    entities_mentioned: list[dict] | None = None,
    source_actor_ref: str | None = None,
) -> uuid.UUID:
    await _ensure_tenant(conn, tenant)
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned, source_actor_ref
        ) VALUES (
            $1, $2, now(), $3, $4, $5,
            '{}'::jsonb, $6,
            NULL, TRUE, 'authoritative',
            $7, $8::jsonb, $9
        )
        """,
        oid, tenant, kind, source_channel, actor_id,
        content_text, f"external-{oid}",
        json.dumps(entities_mentioned or []),
        source_actor_ref,
    )
    return oid


async def insert_commitment(
    conn: asyncpg.Connection,
    tenant: uuid.UUID,
    *,
    owner_id: uuid.UUID | None = None,
    title: str = "test commitment",
    state: str = "active",
    created_by_event_id: uuid.UUID | None = None,
) -> uuid.UUID:
    cid = uuid7()
    if created_by_event_id is None:
        created_by_event_id = await insert_observation(
            conn, tenant, owner_id, kind="state_change",
            source_channel="internal:state_change",
            content_text="commitment created",
        )
    await conn.execute(
        """
        INSERT INTO commitments (
            id, tenant_id, title, description, state, owner_id,
            due_date, ambition_level, priority, success_criteria,
            resolved_by_event_ids, external_counterparty_ref,
            estimated_capacity, created_at, last_state_change_at,
            terminal_at, created_by_event_id, last_confidence_basis
        ) VALUES (
            $1, $2, $3, NULL, $4, $5,
            NULL, 'base', 5, NULL,
            '{}', NULL, NULL, now(), now(), NULL, $6, NULL
        )
        """,
        cid, tenant, title, state, owner_id, created_by_event_id,
    )
    return cid


async def insert_contributor(
    conn: asyncpg.Connection,
    commitment_id: uuid.UUID,
    actor_id: uuid.UUID,
    *,
    role: str = "contributor",
) -> None:
    await conn.execute(
        """
        INSERT INTO commitment_contributors (commitment_id, actor_id, role)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        """,
        commitment_id, actor_id, role,
    )


async def insert_goal(
    conn: asyncpg.Connection,
    tenant: uuid.UUID,
    *,
    title: str = "test goal",
    created_by_event_id: uuid.UUID | None = None,
) -> uuid.UUID:
    gid = uuid7()
    if created_by_event_id is None:
        created_by_event_id = await insert_observation(
            conn, tenant, None, kind="state_change",
            source_channel="internal:state_change",
            content_text="goal created",
        )
    await conn.execute(
        """
        INSERT INTO goals (
            id, tenant_id, title, description, state, target_date,
            parent_goal_id, altitude, success_criteria, cached_health,
            cached_health_computed_at, created_at, last_state_change_at,
            created_by_event_id, archived_at
        ) VALUES (
            $1, $2, $3, NULL, 'active', NULL,
            NULL, 'operational', NULL, 'healthy',
            NULL, now(), now(), $4, NULL
        )
        """,
        gid, tenant, title, created_by_event_id,
    )
    return gid


async def insert_decision(
    conn: asyncpg.Connection,
    tenant: uuid.UUID,
    *,
    title: str = "test decision",
    created_by_event_id: uuid.UUID | None = None,
) -> uuid.UUID:
    did = uuid7()
    if created_by_event_id is None:
        created_by_event_id = await insert_observation(
            conn, tenant, None, kind="state_change",
            source_channel="internal:state_change",
            content_text="decision drafted",
        )
    await conn.execute(
        """
        INSERT INTO decisions (
            id, tenant_id, title, decision_text, rationale, state,
            scope, revisit_triggers, created_at, last_state_change_at,
            created_by_event_id, archived_at
        ) VALUES (
            $1, $2, $3, 'we chose X', NULL, 'active',
            NULL, NULL, now(), now(), $4, NULL
        )
        """,
        did, tenant, title, created_by_event_id,
    )
    return did


async def insert_resource(
    conn: asyncpg.Connection,
    tenant: uuid.UUID,
    *,
    kind: str = "capacity",
    identity: str | None = None,
    metadata: dict | None = None,
    last_updated_by_event_id: uuid.UUID | None = None,
) -> uuid.UUID:
    rid = uuid7()
    if last_updated_by_event_id is None:
        last_updated_by_event_id = await insert_observation(
            conn, tenant, None, kind="state_change",
            source_channel="internal:state_change",
            content_text="resource created",
        )
    await conn.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, description,
            current_value, valuation_confidence, utilization_state,
            controllability, temporal_character, metadata,
            created_at, last_updated_at, last_updated_by_event_id,
            archived_at
        ) VALUES (
            $1, $2, $3, $4, NULL,
            '{"qty":1}'::jsonb, 1.0, 'available',
            'owned', 'permanent', $5::jsonb,
            now(), now(), $6, NULL
        )
        """,
        rid, tenant, kind, identity or f"{kind}-{str(rid)[:8]}",
        json.dumps(metadata or {}),
        last_updated_by_event_id,
    )
    return rid


async def insert_contributes_to(
    conn: asyncpg.Connection,
    commitment_id: uuid.UUID,
    goal_id: uuid.UUID,
    *,
    is_critical_path: bool = False,
) -> None:
    await conn.execute(
        """
        INSERT INTO contributes_to (commitment_id, goal_id, is_critical_path)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        """,
        commitment_id, goal_id, is_critical_path,
    )


async def insert_deployment(
    conn: asyncpg.Connection,
    resource_id: uuid.UUID,
    commitment_id: uuid.UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO resource_deployments (
            resource_id, commitment_id, deployed_quantity,
            deployed_at, released_at
        ) VALUES ($1, $2, '{"qty":1}'::jsonb, now(), NULL)
        ON CONFLICT DO NOTHING
        """,
        resource_id, commitment_id,
    )


async def insert_model(
    conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    born_from_event_id: uuid.UUID,
    proposition: dict | None = None,
    natural: str = "alice is fast",
    embedding: list[float] | None = None,
    scope_actors: list[uuid.UUID] | None = None,
    scope_entities: list | None = None,
    visible_to_subjects: bool = True,
    confidence: float = 0.6,
    status: str = "active",
) -> uuid.UUID:
    await _ensure_tenant(conn, tenant)
    mid = uuid7()
    if embedding is None:
        embedding = make_embedding(natural)
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
            confidence_at_assertion
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            $7::uuid[], $8::jsonb, $9::jsonb,
            $10, 1.0, NULL,
            '[]'::jsonb, TRUE,
            '{}', '{}', 0.5,
            $11, NULL, NULL,
            '{}', $12,
            $13
        )
        """,
        mid, tenant, born_from_event_id,
        json.dumps(proposition or {"kind": "state", "subject": "test", "assertion": natural}),
        natural, embedding,
        scope_actors or [],
        json.dumps(scope_entities or []),
        json.dumps({"kind": "open_ended"}),
        confidence,
        status,
        visible_to_subjects,
        confidence,
    )
    return mid


# ---------------------------------------------------------------------
# Deterministic 768-d embeddings
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


__all__ = [
    "insert_actor",
    "insert_commitment",
    "insert_contributes_to",
    "insert_contributor",
    "insert_decision",
    "insert_deployment",
    "insert_goal",
    "insert_model",
    "insert_observation",
    "insert_resource",
    "make_embedding",
]
