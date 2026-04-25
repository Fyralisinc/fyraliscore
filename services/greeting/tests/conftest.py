"""Fixtures for services/greeting tests.

Pattern mirrors services/acts/tests/conftest.py — real Postgres, per-
test pool with JSONB codec, migrations applied once per pool.

Two tenant UUIDs are preset so tests can use them without inventing.

Helpers for seeding the substrate (Model, Commitment, Resource,
state_change, anomaly) are provided at the raw-SQL level because the
greeting service is not allowed to import other agents' repos at
module load time (parallel agents — avoid cross-owner imports).
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


pytestmark = pytest.mark.integration


TENANT_A = UUID("33333333-3333-3333-3333-333333333333")
TENANT_B = UUID("44444444-4444-4444-4444-444444444444")


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
async def greeting_db() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set — skipping greeting integration test.")

    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=4,
        init=_install_json_codec,
    )
    async with pool.acquire() as conn:
        for path in sorted((REPO_ROOT / "db" / "migrations").glob("*.sql")):
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
    try:
        yield pool
    finally:
        try:
            await asyncio.wait_for(pool.close(), timeout=1.0)
        except (asyncio.TimeoutError, Exception):
            pool.terminate()


# =====================================================================
# Seed helpers — raw SQL so we don't depend on other-agent repos
# =====================================================================


async def seed_actor(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    display_name: str = "Dogfood CEO",
) -> UUID:
    aid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, 'human_internal', $3, 'active')
            """,
            aid,
            tenant_id,
            display_name,
        )
    return aid


async def seed_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    kind: str = "signal",
    content: dict[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> UUID:
    obs_id = uuid7()
    when = occurred_at or datetime.now(timezone.utc)
    content = content or {}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel,
              content, content_text, trust_tier
            ) VALUES (
              $1, $2, $3, $4, 'test:greeting', $5::jsonb, 'seed', 'authoritative'
            )
            """,
            obs_id,
            tenant_id,
            when,
            kind,
            json.dumps(content, default=str),
        )
    return obs_id


async def seed_state_change(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    entity_id: UUID | None = None,
    entity_kind: str = "model",
    change_kind: str = "insert_model",
    occurred_at: datetime | None = None,
) -> UUID:
    eid = entity_id or uuid7()
    content = {
        "entity_id": str(eid),
        "entity_kind": entity_kind,
        "kind": change_kind,
        "metadata": {"seed": True},
    }
    return await seed_observation(
        pool,
        tenant_id=tenant_id,
        kind="state_change",
        content=content,
        occurred_at=occurred_at,
    )


async def seed_model(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    natural: str = "test model proposition",
    confidence: float = 0.8,
    confidence_at_assertion: float | None = None,
    born_from_event_id: UUID | None = None,
) -> UUID:
    model_id = uuid7()
    event_id = born_from_event_id or await seed_observation(pool, tenant_id=tenant_id)
    cfa = confidence_at_assertion if confidence_at_assertion is not None else confidence
    # minimal embedding — 768 zeros to satisfy the pgvector column
    embedding = [0.0] * 768
    emb_str = "[" + ",".join(f"{x:.6f}" for x in embedding) + "]"
    prop = {"kind": "state", "subject": "test", "predicate": "active"}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id,
              proposition, "natural", embedding,
              scope_actors, scope_entities, scope_temporal,
              confidence, activation, falsifier,
              signal_readings, reading_contestable,
              supporting_event_ids, supporting_model_ids, evidential_weight,
              status, confidence_at_assertion, activation_coefficient
            ) VALUES (
              $1, $2, $3,
              $4::jsonb, $5, $6::vector,
              '{}'::uuid[], '[]'::jsonb, '{}'::jsonb,
              $7, 1.0, NULL,
              '[]'::jsonb, TRUE,
              '{}'::uuid[], '{}'::uuid[], 0.5,
              'active', $8, 1.0
            )
            """,
            model_id,
            tenant_id,
            event_id,
            json.dumps(prop),
            natural,
            emb_str,
            confidence,
            cfa,
        )
    return model_id


async def seed_commitment(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    title: str = "Test commitment",
    state: str = "active",
    due_days: int | None = 3,
    priority: int = 3,
    owner_id: UUID | None = None,
    created_by_event_id: UUID | None = None,
    is_critical_path: bool = False,
    goal_id: UUID | None = None,
) -> UUID:
    commit_id = uuid7()
    event_id = created_by_event_id or await seed_observation(pool, tenant_id=tenant_id)
    due = (
        datetime.now(timezone.utc) + timedelta(days=due_days)
        if due_days is not None
        else None
    )
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO commitments (
              id, tenant_id, title, state, owner_id, due_date,
              priority, created_by_event_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            commit_id,
            tenant_id,
            title,
            state,
            owner_id,
            due,
            priority,
            event_id,
        )
        if is_critical_path and goal_id is not None:
            await conn.execute(
                """
                INSERT INTO contributes_to (commitment_id, goal_id, is_critical_path)
                VALUES ($1, $2, TRUE)
                """,
                commit_id,
                goal_id,
            )
    return commit_id


async def seed_goal(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    title: str = "Seed goal",
    created_by_event_id: UUID | None = None,
) -> UUID:
    goal_id = uuid7()
    event_id = created_by_event_id or await seed_observation(pool, tenant_id=tenant_id)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO goals (
              id, tenant_id, title, created_by_event_id
            ) VALUES ($1, $2, $3, $4)
            """,
            goal_id,
            tenant_id,
            title,
            event_id,
        )
    return goal_id


async def seed_resource(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    kind: str = "relational",
    identity: str = "Acme Corp",
    utilization_state: str = "depleted",
    health: str | None = "degraded",
) -> UUID:
    rid = uuid7()
    cv: dict[str, Any] = {"health": health} if health else {}
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO resources (
              id, tenant_id, kind, identity, current_value,
              utilization_state, controllability, temporal_character
            ) VALUES (
              $1, $2, $3, $4, $5::jsonb, $6, 'owned', 'permanent'
            )
            """,
            rid,
            tenant_id,
            kind,
            identity,
            json.dumps(cv),
            utilization_state,
        )
    return rid


async def seed_anomaly(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    kind: str = "customer_health_degraded",
    significance: float = 0.85,
    region: dict[str, Any] | None = None,
    published_at: datetime | None = None,
) -> UUID:
    aid = uuid7()
    when = published_at or datetime.now(timezone.utc)
    region_json = json.dumps(region or {"resource_id": str(uuid7())})
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO think_anomalies_raw (
              id, tenant_id, think_run_id, kind, region,
              significance, triggering_op, published_at
            ) VALUES (
              $1, $2, $3, $4, $5::jsonb, $6, '{}'::jsonb, $7
            )
            """,
            aid,
            tenant_id,
            uuid7(),
            kind,
            region_json,
            significance,
            when,
        )
    return aid


async def seed_post_commit_action(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    action_kind: str = "publish_anomalies",
    created_at: datetime | None = None,
) -> UUID:
    row_id = uuid7()
    when = created_at or datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO pending_post_commit_actions (
              id, tenant_id, trigger_id, action_kind,
              action_payload, created_at, scheduled_at
            ) VALUES ($1, $2, $3, $4, '{}'::jsonb, $5, $5)
            """,
            row_id,
            tenant_id,
            uuid7(),
            action_kind,
            when,
        )
    return row_id


__all__ = [
    "TENANT_A",
    "TENANT_B",
    "greeting_db",
    "seed_actor",
    "seed_observation",
    "seed_state_change",
    "seed_model",
    "seed_commitment",
    "seed_goal",
    "seed_resource",
    "seed_anomaly",
    "seed_post_commit_action",
]
