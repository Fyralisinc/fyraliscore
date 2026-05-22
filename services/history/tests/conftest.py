"""services/history/tests/conftest.py — fixtures for history aggregator
+ summary tests.

Mirrors services/topology/tests/conftest.py: per-test asyncpg pool,
single transaction wrapping each test, ROLLBACK at teardown. Tenant
UUID isolation is the hermetic boundary.
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
            $1, $2, 'human_internal', 'Hist Test', 'hist@example.com',
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
            $3, '{}'::jsonb, 'hist obs',
            NULL, TRUE, 'authoritative',
            $4, '[]'::jsonb
        )
        """,
        oid, tenant, actor_id, f"hist-obs-{oid}",
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
async def make_prediction_model(
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
):
    """Insert a Model with proposition_kind='prediction'. Allows setting
    created_at and resolved_at + resolution_outcome."""
    async def _make(
        natural: str = "things will happen",
        *,
        created_at_offset_days: int = 0,
        resolved: bool | None = None,
        outcome: bool | None = None,
    ) -> uuid.UUID:
        import json
        from datetime import datetime, timedelta, timezone

        mid = uuid7()
        emb = _hash_embedding(natural)
        prop = {
            "kind": "prediction",
            "expected": natural,
            "resolution": "manual review",
        }
        created_at = datetime.now(timezone.utc) - timedelta(
            days=created_at_offset_days
        )
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, falsifier, signal_readings,
                supporting_event_ids, supporting_model_ids,
                contributing_models, status,
                confidence_at_assertion, created_at
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, $5, $6,
                '{}'::uuid[], '[]'::jsonb,
                '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
                0.7, NULL, '[]'::jsonb,
                '{}'::uuid[], '{}'::uuid[],
                '{}'::uuid[], 'active',
                0.7, $7
            )
            """,
            mid, tenant, born_from_event, json.dumps(prop), natural, emb,
            created_at,
        )
        if resolved is True:
            await tx_conn.execute(
                """
                UPDATE models
                SET resolved_at = now(), resolution_outcome = $2
                WHERE id = $1
                """,
                mid, bool(outcome),
            )
        return mid
    return _make


@pytest_asyncio.fixture
async def insert_state_change(
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
):
    """Insert an observation row with kind='state_change'. Lets tests
    seed action_taken events deterministically."""
    async def _insert(
        *,
        entity_kind: str = "commitment",
        new_state: str = "doneverified",
        occurred_offset_days: int = 0,
        title: str = "ship the thing",
    ) -> uuid.UUID:
        import json
        from datetime import datetime, timedelta, timezone

        # Seed a target commitment / decision row so the aggregator can
        # hydrate the title. We only need a minimum-viable row.
        entity_id = uuid7()
        if entity_kind == "commitment":
            await tx_conn.execute(
                """
                INSERT INTO commitments (
                    id, tenant_id, title, state, owner_id,
                    created_at, last_state_change_at, created_by_event_id
                ) VALUES ($1, $2, $3, $4, $5, now(), now(), $6)
                ON CONFLICT (id) DO NOTHING
                """,
                entity_id, tenant, title, new_state, actor_id,
                born_from_event,
            )
        elif entity_kind == "decision":
            await tx_conn.execute(
                """
                INSERT INTO decisions (
                    id, tenant_id, title, decision_text, state,
                    created_at, last_state_change_at, created_by_event_id
                ) VALUES ($1, $2, $3, $4, $5, now(), now(), $6)
                ON CONFLICT (id) DO NOTHING
                """,
                entity_id, tenant, title, f"decided: {title}",
                new_state, born_from_event,
            )

        oid = uuid7()
        content = {
            "entity_kind": entity_kind,
            "entity_id": str(entity_id),
            "state_change_kind": f"{entity_kind}_{new_state}",
        }
        occurred = datetime.now(timezone.utc) - timedelta(
            days=occurred_offset_days
        )
        await tx_conn.execute(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                actor_id, content, content_text,
                embedding, embedding_pending, trust_tier,
                external_id, entities_mentioned
            ) VALUES (
                $1, $2, $3, 'state_change', 'test:state',
                $4, $5::jsonb, 'state change',
                NULL, TRUE, 'authoritative',
                $6, '[]'::jsonb
            )
            """,
            oid, tenant, occurred, actor_id,
            json.dumps(content), f"sc-{oid}",
        )
        return oid
    return _insert
