"""
Fixtures for services/acts tests.

Per BUILD-PLAN §0.5 non-negotiable #4 — no mocks for Postgres. Every
test here runs against the real local Postgres via the fresh_db
fixture inherited from the top-level conftest.py.

Because the top-level `fresh_db` yields an `asyncpg.Pool` but our
services use `lib.shared.db.get_pool()`, we bind the pool into the
shared module for each test. The acts module opens its own
transactions off that pool.
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
async def acts_db() -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Per-test fresh pool. Uses a single dedicated connection (min=max=1)
    with the JSONB codec installed, runs migrations + TRUNCATE, and
    yields. On teardown, terminate is used instead of close() so the
    backend process is freed eagerly (avoids the "previous test's
    backend still holds a share lock on observations" race that caused
    intermittent DeadlockDetectedError).
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set — skipping acts integration test.")

    # Wait out any leftover transactions from a previous test whose
    # pool-close is still finalizing on the server side. If a prior
    # test's connections haven't released all their locks yet, our
    # TRUNCATE would deadlock against them.
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
        max_size=4,
        init=_install_json_codec,
    )
    async with pool.acquire() as conn:
        from lib.shared.migrations import apply_migrations_dir
        await apply_migrations_dir(conn, REPO_ROOT / "db" / "migrations")
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
        # Graceful close; asyncpg will wait for in-flight work.
        # A 1s timeout stops teardown from stalling test transitions
        # if something went wrong in the test body.
        try:
            await asyncio.wait_for(pool.close(), timeout=1.0)
        except (asyncio.TimeoutError, Exception):
            pool.terminate()


# ---------------------------------------------------------------------
# Raw-SQL helpers for fixtures. Agent 1-D is not allowed to import
# services/actors/repo.py or services/observations/repo.py (parallel
# agents), so we INSERT directly.
# ---------------------------------------------------------------------

TENANT_A = UUID("11111111-1111-1111-1111-111111111111")
TENANT_B = UUID("22222222-2222-2222-2222-222222222222")


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
            INSERT INTO actors (
              id, tenant_id, type, display_name, status
            ) VALUES ($1, $2, $3, $4, $5)
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
    """
    Insert a real observation row so tests that need FK enforcement
    work. `observations` has partitioned PK (id, occurred_at) but the
    app-layer FKs from goals/commitments/decisions *are not enforced*
    at the DB level (per Wave 0 BUILD-LOG). Still, seeding a real row
    keeps the record consistent.
    """
    obs_id = uuid7()
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel, content,
              content_text, trust_tier
            ) VALUES (
              $1, $2, $3, $4, $5, '{}'::jsonb, 'test', $6
            )
            """,
            obs_id,
            tenant_id,
            now,
            kind,
            source_channel,
            trust_tier,
        )
    return obs_id


@pytest_asyncio.fixture
async def actor_id(acts_db: asyncpg.Pool) -> UUID:
    """A single active actor in TENANT_A."""
    return await make_actor(acts_db)


@pytest_asyncio.fixture
async def actor_id2(acts_db: asyncpg.Pool) -> UUID:
    """A second active actor in TENANT_A."""
    return await make_actor(acts_db, display_name="Alt Actor")


@pytest_asyncio.fixture
async def inactive_actor_id(acts_db: asyncpg.Pool) -> UUID:
    return await make_actor(acts_db, display_name="Inactive", status="inactive")


@pytest_asyncio.fixture
async def event_id(acts_db: asyncpg.Pool) -> UUID:
    """A seeded observation to use as created_by_event_id."""
    return await make_observation(acts_db)


@pytest_asyncio.fixture
async def event_id2(acts_db: asyncpg.Pool) -> UUID:
    return await make_observation(acts_db)


def future_due(hours: int = 24) -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=hours)


def past_due(hours: int = 24) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


__all__ = [
    "TENANT_A",
    "TENANT_B",
    "make_actor",
    "make_observation",
    "future_due",
    "past_due",
    "acts_db",
    "actor_id",
    "actor_id2",
    "inactive_actor_id",
    "event_id",
    "event_id2",
]
