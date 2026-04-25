"""services/bridge/tests/conftest.py — per-test asyncpg pool with
JSONB + vector codecs installed, migrations applied + TRUNCATE between
tests. Mirrors services/resources/tests/conftest.py.

We intentionally duplicate the conftest pattern instead of importing
`services.resources.tests.conftest` so the two test packages can run
independently in parallel CI shards.
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared import db as _db_module
from lib.shared.ids import uuid7


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]

pytestmark = pytest.mark.integration


async def _install_json_codec(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: json.dumps(v) if not isinstance(v, str) else v,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=lambda v: json.dumps(v) if not isinstance(v, str) else v,
        decoder=json.loads,
        schema="pg_catalog",
    )


@pytest_asyncio.fixture
async def bridge_db() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set — skipping bridge integration test.")

    async def _wait_idle(max_wait_ms: float = 2000.0) -> None:
        start = asyncio.get_event_loop().time()
        while True:
            probe = await asyncpg.connect(dsn)
            try:
                active = await probe.fetchval(
                    """
                    SELECT COUNT(*) FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND state IN ('active', 'idle in transaction',
                                    'idle in transaction (aborted)')
                      AND pid <> pg_backend_pid()
                    """
                )
            finally:
                await probe.close()
            if (active or 0) == 0:
                return
            if (asyncio.get_event_loop().time() - start) * 1000 > max_wait_ms:
                return
            await asyncio.sleep(0.01)

    await _wait_idle()

    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=12,
        init=_install_json_codec,
    )
    async with pool.acquire() as conn:
        migration_files = sorted((REPO_ROOT / "db" / "migrations").glob("*.sql"))
        for path in migration_files:
            await conn.execute(path.read_text())
        rows = await conn.fetch(
            """
            SELECT c.relname FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind IN ('r', 'p')
              AND c.relispartition = FALSE
            """
        )
        tables = [r["relname"] for r in rows]
        if tables:
            table_list = ", ".join(f'"{t}"' for t in tables)
            await conn.execute(
                f"TRUNCATE {table_list} RESTART IDENTITY CASCADE"
            )

    prior = _db_module._pool
    _db_module._pool = pool
    try:
        yield pool
    finally:
        _db_module._pool = prior
        try:
            await asyncio.wait_for(pool.close(), timeout=1.0)
        except (asyncio.TimeoutError, Exception):
            pool.terminate()


# ---------------------------------------------------------------------
# Constants + factories (mirror resources/tests/conftest)
# ---------------------------------------------------------------------

TENANT_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
TENANT_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


async def make_actor(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    display_name: str = "Test Actor",
) -> UUID:
    actor_id = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, 'human_internal', $3, 'active')
            """,
            actor_id, tenant_id, display_name,
        )
    return actor_id


async def make_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
) -> UUID:
    obs_id = uuid7()
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel,
              content, content_text, trust_tier
            ) VALUES ($1, $2, $3, 'signal', 'test:harness',
                      '{}'::jsonb, 'test', 'authoritative')
            """,
            obs_id, tenant_id, now,
        )
    return obs_id


async def make_goal(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    title: str = "Test Goal",
    parent_goal_id: UUID | None = None,
    cached_health: str = "healthy",
    event_id: UUID | None = None,
) -> UUID:
    if event_id is None:
        event_id = await make_observation(pool, tenant_id=tenant_id)
    gid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO goals (
              id, tenant_id, title, parent_goal_id, cached_health, created_by_event_id
            ) VALUES ($1, $2, $3, $4, $5, $6)
            """,
            gid, tenant_id, title, parent_goal_id, cached_health, event_id,
        )
    return gid


async def make_commitment(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    title: str = "C",
    owner_id: UUID | None = None,
    state: str = "active",
    due_date: datetime | None = None,
    event_id: UUID | None = None,
    contributes_to_goal_id: UUID | None = None,
    is_critical_path: bool = False,
) -> UUID:
    if event_id is None:
        event_id = await make_observation(pool, tenant_id=tenant_id)
    if owner_id is None:
        owner_id = await make_actor(pool, tenant_id=tenant_id)
    cid = uuid7()
    if due_date is None:
        due_date = datetime.now(timezone.utc) + timedelta(days=30)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO commitments (
              id, tenant_id, title, state, owner_id, due_date, created_by_event_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            cid, tenant_id, title, state, owner_id, due_date, event_id,
        )
        if contributes_to_goal_id is not None:
            await conn.execute(
                """
                INSERT INTO contributes_to (commitment_id, goal_id, is_critical_path)
                VALUES ($1, $2, $3)
                ON CONFLICT (commitment_id, goal_id) DO UPDATE
                  SET is_critical_path = EXCLUDED.is_critical_path
                """,
                cid, contributes_to_goal_id, is_critical_path,
            )
    return cid


async def make_customer(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    identity: str = "customer:acme",
    arr_cents: int = 100_00,
    event_id: UUID | None = None,
) -> UUID:
    if event_id is None:
        event_id = await make_observation(pool, tenant_id=tenant_id)
    rid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO resources (
              id, tenant_id, kind, identity, current_value,
              last_updated_by_event_id
            ) VALUES ($1, $2, 'relational', $3, $4::jsonb, $5)
            """,
            rid, tenant_id, identity,
            json.dumps({"arr_cents": arr_cents, "strength": "strong"}),
            event_id,
        )
    return rid


async def make_capacity_resource(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    identity: str = "eng",
    total_units: int = 10,
    deployed_units: int = 0,
    event_id: UUID | None = None,
) -> UUID:
    if event_id is None:
        event_id = await make_observation(pool, tenant_id=tenant_id)
    rid = uuid7()
    available = total_units - deployed_units
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO resources (
              id, tenant_id, kind, identity, current_value,
              last_updated_by_event_id
            ) VALUES ($1, $2, 'capacity', $3, $4::jsonb, $5)
            """,
            rid, tenant_id, identity,
            json.dumps(
                {
                    "total_units": total_units,
                    "deployed_units": deployed_units,
                    "available_units": available,
                }
            ),
            event_id,
        )
    return rid


async def make_decision(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    state: str = "active",
    event_id: UUID | None = None,
) -> UUID:
    if event_id is None:
        event_id = await make_observation(pool, tenant_id=tenant_id)
    did = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO decisions (
              id, tenant_id, title, decision_text, state, created_by_event_id
            ) VALUES ($1, $2, 'D', 'decision text', $3, $4)
            """,
            did, tenant_id, state, event_id,
        )
    return did


async def set_commitment_state(
    pool: asyncpg.Pool, commitment_id: UUID, new_state: str
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE commitments SET state = $2, last_state_change_at = now() WHERE id = $1",
            commitment_id, new_state,
        )


async def link_commitment_row(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    customer_resource_id: UUID,
    commitment_id: UUID,
    revenue_at_risk_usd=None,
    criticality: str = "medium",
    relationship_kind: str = "delivers",
) -> None:
    """Raw-SQL link helper (bypasses public link_commitment for tests
    that want to seed rows with arbitrary values)."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO customer_commitments (
              id, tenant_id, customer_resource_id, commitment_id,
              relationship_kind, revenue_at_risk_usd, criticality
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (customer_resource_id, commitment_id) DO UPDATE
              SET revenue_at_risk_usd = EXCLUDED.revenue_at_risk_usd,
                  criticality = EXCLUDED.criticality,
                  relationship_kind = EXCLUDED.relationship_kind
            """,
            uuid7(), tenant_id, customer_resource_id, commitment_id,
            relationship_kind, revenue_at_risk_usd, criticality,
        )


async def insert_prediction_model(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    commitment_id: UUID,
    direction: str = "will_slip",
    confidence: float = 0.8,
    event_id: UUID | None = None,
) -> UUID:
    if event_id is None:
        event_id = await make_observation(pool, tenant_id=tenant_id)
    mid = uuid7()
    async with pool.acquire() as conn:
        # Build a 768-dim zero embedding — pgvector parses the string literal.
        embedding = "[" + ",".join(["0"] * 768) + "]"
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id,
              proposition, "natural", embedding,
              scope_actors, scope_entities, scope_temporal,
              confidence, activation, confidence_at_assertion,
              status
            ) VALUES (
              $1, $2, $3, $4::jsonb, 'predicts slip', $5::vector,
              '{}'::uuid[], $6::jsonb, '{}'::jsonb,
              $7, 1.0, $7, 'active'
            )
            """,
            mid, tenant_id, event_id,
            json.dumps(
                {
                    "kind": "prediction",
                    "subject": {"type": "commitment", "id": str(commitment_id)},
                    "direction": direction,
                }
            ),
            embedding,
            json.dumps(
                [{"type": "commitment", "id": str(commitment_id)}]
            ),
            float(confidence),
        )
    return mid


async def seed_state_change_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    commitment_id: UUID,
    new_state: str,
    occurred_at: datetime,
) -> UUID:
    oid = uuid7()
    content = {
        "entity_kind": "commitment",
        "entity_id": str(commitment_id),
        "metadata": {"new_state": new_state},
    }
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel,
              content, content_text, trust_tier
            ) VALUES ($1, $2, $3, 'state_change', 'test:cascade',
                      $4::jsonb, $5, 'authoritative')
            """,
            oid, tenant_id, occurred_at,
            json.dumps(content),
            f"state -> {new_state}",
        )
    return oid


@pytest_asyncio.fixture
async def event_id(bridge_db: asyncpg.Pool) -> UUID:
    return await make_observation(bridge_db)


__all__ = [
    "TENANT_A",
    "TENANT_B",
    "bridge_db",
    "event_id",
    "make_actor",
    "make_observation",
    "make_goal",
    "make_commitment",
    "make_customer",
    "make_capacity_resource",
    "make_decision",
    "set_commitment_state",
    "link_commitment_row",
    "insert_prediction_model",
    "seed_state_change_observation",
]
