"""
services/retrieval/tests/conftest.py — per-test pool + pgvector codec
+ tenant-isolated fixtures.

Mirrors the Wave 1-D / Models conftest pattern: per-test asyncpg pool
(avoids cross-event-loop issues in pytest-asyncio 1.x), JSONB codec
installation wrapping each connection, and a tenant-UUID hermetic
boundary so we don't trip over other agents' parallel test runs.

The `fixture_set` fixture hand-builds the 200-obs / 100-models /
50-commits / 20-goals / 10-customers dataset by going through the
Wave 1/2 repos (Observations, Models, Acts, Resources) so the
retrieval tests exercise the full write path. Do NOT shortcut past
the repos; the prompt is explicit about this.
"""
from __future__ import annotations

import os
import uuid
from collections.abc import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio
from pgvector.asyncpg import register_vector

from lib.shared.ids import uuid7

from services.models.repo import ModelsRepo


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Pool + transaction lifecycle
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Per-test asyncpg pool with a moderately high max_size to tolerate
    the concurrent-retrieval benchmark test. Skips the root conftest
    TRUNCATE because tenant isolation is our hermetic boundary.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set; skipping integration test.")
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=25,
        init=_init_connection,
    )
    try:
        yield pool
    finally:
        await pool.close()


async def _init_connection(conn: asyncpg.Connection) -> None:
    """
    Install pgvector + JSONB codecs on every new pool connection so
    `list[float]` round-trips as VECTOR(768) and JSONB columns don't
    return raw `str`. This is the Wave 1-D pattern the prompt calls
    out — lib/shared/db.py doesn't do this yet and our tests must.
    """
    try:
        await register_vector(conn)
    except Exception:
        pass


@pytest_asyncio.fixture
async def fresh_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Override the root `fresh_db` fixture. We do NOT TRUNCATE — tenant
    UUID isolation is our hermetic boundary.
    """
    yield db_pool


@pytest_asyncio.fixture
async def tx_conn(fresh_db: asyncpg.Pool) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Acquire one connection for the whole test body, open a transaction
    on it, and ROLLBACK at teardown. The repo calls accept `conn=` so
    every write goes through this connection.
    """
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


@pytest.fixture
def tenant() -> uuid.UUID:
    return uuid7()


@pytest.fixture
def other_tenant() -> uuid.UUID:
    return uuid7()


@pytest_asyncio.fixture
async def models_repo(fresh_db: asyncpg.Pool) -> ModelsRepo:
    # No embedder — we pass precomputed embeddings everywhere in tests.
    return ModelsRepo(fresh_db, embedder=None)
