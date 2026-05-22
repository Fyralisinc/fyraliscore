"""services/workers/anomaly_processor/tests/conftest.py — Wave 4-B test helpers.

Same pattern as services/think/tests/conftest.py: per-test asyncpg
pool with pgvector/JSONB codec, tenant-UUID hermetic boundary, and
rich cleanup. Anomaly tests also cleanup `signal_memory_fabric`
(new to Wave 4-B).

No mocks for Postgres per BUILD-PLAN §0.5 non-negotiable #4.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
from pgvector.asyncpg import register_vector

from lib.shared.ids import uuid7


pytestmark = pytest.mark.integration


EMBED_DIM = 768


# ---------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping anomaly_processor integration tests.")
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=10, init=_init_connection,
    )
    try:
        yield pool
    finally:
        await pool.close()


async def _init_connection(conn: asyncpg.Connection) -> None:
    try:
        await register_vector(conn)
    except Exception:
        pass


@pytest_asyncio.fixture
async def fresh_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """Override root `fresh_db` — per-tenant isolation."""
    yield db_pool


# ---------------------------------------------------------------------
# Tenants
# ---------------------------------------------------------------------


@pytest.fixture
def tenant() -> UUID:
    return uuid7()


@pytest.fixture
def other_tenant() -> UUID:
    return uuid7()


# ---------------------------------------------------------------------
# Per-tenant cleanup
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def tenant_cleanup(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    request,
):
    """
    After-test cleanup for one tenant. Tests that create a second
    tenant (tenant isolation tests) should use `two_tenant_cleanup`.
    """
    yield
    await _cleanup_tenant(fresh_db, tenant)


@pytest_asyncio.fixture
async def two_tenant_cleanup(
    fresh_db: asyncpg.Pool,
    tenant: UUID,
    other_tenant: UUID,
):
    yield
    await _cleanup_tenant(fresh_db, tenant)
    await _cleanup_tenant(fresh_db, other_tenant)


async def _cleanup_tenant(pool: asyncpg.Pool, tenant: UUID) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM signal_memory_fabric WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM think_anomalies_raw WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM think_trigger_queue WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM applied_triggers WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM think_runs WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM think_region_lock_log WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM model_reeval_dead_letter WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM model_reeval_queue WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM customer_commitments WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM resource_deployments WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM contributes_to WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM depends_on WHERE dependent_commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM constrained_by WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM commitment_contributors WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM commitments WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM goals WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM decisions WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM resource_transactions WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM resources WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM models WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM observations WHERE tenant_id = $1", tenant,
        )
        await conn.execute(
            "DELETE FROM actor_identity_mappings WHERE actor_id IN "
            "(SELECT id FROM actors WHERE tenant_id = $1)", tenant,
        )
        await conn.execute(
            "DELETE FROM actors WHERE tenant_id = $1", tenant,
        )


# ---------------------------------------------------------------------
# Test-fixture builders (raw SQL so we don't depend on the full
# repo pipelines — those have heavyweight insert invariants we don't
# need for anomaly-detector unit tests).
# ---------------------------------------------------------------------


def make_embedding(text: str, *, dim: int = EMBED_DIM) -> list[float]:
    seed = int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    n = sum(x * x for x in v) ** 0.5
    return [x / n for x in v] if n > 0 else v


async def insert_actor(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    display_name: str = "Test Actor",
) -> UUID:
    aid = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', $3, 'active')
        """,
        aid, tenant_id, display_name,
    )
    return aid


async def insert_observation(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    kind: str = "signal",
    source_channel: str = "test:channel",
    actor_id: UUID | None = None,
    content: dict[str, Any] | None = None,
    content_text: str | None = None,
    trust_tier: str = "reputable",
    occurred_at: datetime | None = None,
    external_id: str | None = None,
    entities_mentioned: list[dict[str, Any]] | None = None,
    cause_id: UUID | None = None,
) -> UUID:
    oid = uuid7()
    occurred_at = occurred_at or datetime.now(timezone.utc)
    text = content_text or (content or {}).get("text") or kind
    content = content if content is not None else {"text": text}
    emb = make_embedding(text)
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, actor_id,
           content, content_text, embedding, embedding_pending,
           trust_tier, external_id, cause_id, entities_mentioned)
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, FALSE,
                $10, $11, $12, $13::jsonb)
        """,
        oid, tenant_id, occurred_at, kind, source_channel, actor_id,
        json.dumps(content, default=str), text, emb, trust_tier,
        external_id, cause_id, json.dumps(entities_mentioned or []),
    )
    return oid


async def insert_minimal_model(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    born_from_event_id: UUID,
    proposition: dict[str, Any] | None = None,
    scope_actors: list[UUID] | None = None,
    scope_entities: list[dict[str, Any]] | None = None,
    confidence: float = 0.6,
    confidence_at_assertion: float = 0.6,
    activation: float = 1.0,
    supporting_event_ids: list[UUID] | None = None,
    natural: str = "Test belief",
) -> UUID:
    mid = uuid7()
    proposition = proposition or {"kind": "belief", "text": natural}
    scope_temporal = {"kind": "indefinite"}
    emb = make_embedding(natural)
    await conn.execute(
        f"""
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, activation,
            status, confidence_at_assertion, activation_coefficient,
            supporting_event_ids
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            $7::uuid[], $8::jsonb, $9::jsonb,
            $10, $11,
            'active', $12, 1.0,
            $13::uuid[]
        )
        """,
        mid, tenant_id, born_from_event_id,
        json.dumps(proposition), natural, emb,
        list(scope_actors or []), json.dumps(scope_entities or []),
        json.dumps(scope_temporal),
        confidence, activation,
        confidence_at_assertion,
        list(supporting_event_ids or []),
    )
    return mid


async def insert_goal(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    title: str = "Goal",
    created_by_event_id: UUID,
) -> UUID:
    gid = uuid7()
    await conn.execute(
        """
        INSERT INTO goals (id, tenant_id, title, state, altitude,
                           created_by_event_id)
        VALUES ($1, $2, $3, 'active', 'operational', $4)
        """,
        gid, tenant_id, title, created_by_event_id,
    )
    return gid


async def insert_commitment(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    title: str = "Commitment",
    owner_id: UUID | None = None,
    state: str = "active",
    due_date: datetime | None = None,
    created_by_event_id: UUID,
    external_counterparty_ref: dict[str, Any] | None = None,
    estimated_capacity: dict[str, Any] | None = None,
) -> UUID:
    cid = uuid7()
    await conn.execute(
        """
        INSERT INTO commitments
          (id, tenant_id, title, state, owner_id, due_date,
           ambition_level, priority, created_by_event_id,
           external_counterparty_ref, estimated_capacity)
        VALUES ($1, $2, $3, $4, $5, $6, 'base', 5, $7, $8::jsonb, $9::jsonb)
        """,
        cid, tenant_id, title, state, owner_id, due_date,
        created_by_event_id,
        json.dumps(external_counterparty_ref) if external_counterparty_ref is not None else None,
        json.dumps(estimated_capacity) if estimated_capacity is not None else None,
    )
    return cid


async def insert_contributes_to(
    conn: asyncpg.Connection,
    *,
    commitment_id: UUID,
    goal_id: UUID,
    is_critical_path: bool = False,
) -> None:
    await conn.execute(
        """
        INSERT INTO contributes_to (commitment_id, goal_id, is_critical_path)
        VALUES ($1, $2, $3)
        """,
        commitment_id, goal_id, is_critical_path,
    )


async def insert_resource(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    kind: str = "capacity",
    identity: str = "engineer-hours",
    current_value: dict[str, Any] | None = None,
) -> UUID:
    rid = uuid7()
    current_value = current_value or {"total_units": 100.0, "deployed_units": 0.0, "available_units": 100.0}
    await conn.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, current_value,
            utilization_state, controllability, temporal_character
        ) VALUES ($1, $2, $3, $4, $5::jsonb, 'available', 'owned', 'permanent')
        """,
        rid, tenant_id, kind, identity, json.dumps(current_value),
    )
    return rid


async def insert_resource_deployment(
    conn: asyncpg.Connection,
    *,
    resource_id: UUID,
    commitment_id: UUID,
    deployed_quantity: dict[str, Any] | None = None,
) -> None:
    await conn.execute(
        """
        INSERT INTO resource_deployments
          (resource_id, commitment_id, deployed_quantity)
        VALUES ($1, $2, $3::jsonb)
        """,
        resource_id, commitment_id,
        json.dumps(deployed_quantity or {"units": 1}),
    )
