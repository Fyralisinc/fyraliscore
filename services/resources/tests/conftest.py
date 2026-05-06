"""Fixtures for services/resources tests.

Mirrors Wave 1-D's conftest pattern:
  - per-test asyncpg pool with JSONB codec installed on every acquired
    connection (the shared lib/shared/db.py helpers don't install one)
  - migrations applied + TRUNCATE between tests
  - deterministic TENANT_A / TENANT_B + raw-SQL factory helpers for
    actor/observation/commitment/goal/decision fixture rows (Agent 2-C
    avoids importing actors/observations public APIs so parallel
    agents don't step on each other's module boundaries — but DOES
    import services.acts.commitments for the few tests that need a
    real Commitment lifecycle, since Wave 1-D is already complete per
    BUILD-LOG).

No mocks for Postgres — per BUILD-PLAN §0.5 non-negotiable #4 and
Prompt 2.C hard constraint.
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
    """Register JSONB/JSON codec so asyncpg returns dicts, not strings."""
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
async def resources_db() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set — skipping resources integration test.")

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
        max_size=12,  # enough for the concurrent-deploy tests
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
# Constants / raw-SQL helpers
# ---------------------------------------------------------------------

TENANT_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
TENANT_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


async def make_actor(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    display_name: str = "Test Actor",
    type_: str = "human_internal",
    status: str = "active",
) -> UUID:
    actor_id = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, $3, $4, $5)
            """,
            actor_id,
            tenant_id,
            type_,
            display_name,
            status,
        )
    return actor_id


async def make_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    kind: str = "signal",
    source_channel: str = "test:harness",
    trust_tier: str = "authoritative",
) -> UUID:
    obs_id = uuid7()
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel,
              content, content_text, trust_tier
            ) VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, 'test', $6)
            """,
            obs_id,
            tenant_id,
            now,
            kind,
            source_channel,
            trust_tier,
        )
    return obs_id


async def make_goal(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    title: str = "Test Goal",
    event_id: UUID | None = None,
) -> UUID:
    if event_id is None:
        event_id = await make_observation(pool, tenant_id=tenant_id)
    gid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO goals (id, tenant_id, title, created_by_event_id)
            VALUES ($1, $2, $3, $4)
            """,
            gid,
            tenant_id,
            title,
            event_id,
        )
    return gid


async def make_commitment(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    title: str = "Serve customer",
    owner_id: UUID | None = None,
    event_id: UUID | None = None,
    state: str = "active",
    contributes_to_goal_id: UUID | None = None,
    maintenance: bool = True,
) -> UUID:
    if event_id is None:
        event_id = await make_observation(pool, tenant_id=tenant_id)
    if owner_id is None:
        owner_id = await make_actor(pool, tenant_id=tenant_id)
    cid = uuid7()
    due = datetime.now(timezone.utc) + timedelta(days=30)
    ec = {"maintenance": True} if maintenance else None
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO commitments (
              id, tenant_id, title, state, owner_id, due_date,
              estimated_capacity, created_by_event_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            """,
            cid,
            tenant_id,
            title,
            state,
            owner_id,
            due,
            json.dumps(ec) if ec is not None else None,
            event_id,
        )
        if contributes_to_goal_id is not None:
            await conn.execute(
                """
                INSERT INTO contributes_to (commitment_id, goal_id)
                VALUES ($1, $2) ON CONFLICT DO NOTHING
                """,
                cid,
                contributes_to_goal_id,
            )
    return cid


async def set_commitment_state(
    pool: asyncpg.Pool, commitment_id: UUID, new_state: str
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE commitments SET state = $2, last_state_change_at = now() WHERE id = $1",
            commitment_id,
            new_state,
        )


async def make_decision(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID = TENANT_A,
    state: str = "active",
    event_id: UUID | None = None,
    title: str = "D",
) -> UUID:
    if event_id is None:
        event_id = await make_observation(pool, tenant_id=tenant_id)
    did = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO decisions (id, tenant_id, title, decision_text, state, created_by_event_id)
            VALUES ($1, $2, $3, 'decision text', $4, $5)
            """,
            did,
            tenant_id,
            title,
            state,
            event_id,
        )
    return did


@pytest_asyncio.fixture
async def actor_id(resources_db: asyncpg.Pool) -> UUID:
    return await make_actor(resources_db)


@pytest_asyncio.fixture
async def event_id(resources_db: asyncpg.Pool) -> UUID:
    return await make_observation(resources_db)


__all__ = [
    "TENANT_A",
    "TENANT_B",
    "resources_db",
    "actor_id",
    "event_id",
    "make_actor",
    "make_observation",
    "make_goal",
    "make_commitment",
    "set_commitment_state",
    "make_decision",
]
