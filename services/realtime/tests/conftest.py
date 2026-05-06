"""Wave 4-D Realtime tests — shared fixtures.

Design:

* Real Postgres (per BUILD-PLAN non-negotiable: "No mocks for Postgres").
* Real `fastapi.testclient.TestClient` so WebSocket lifecycle is honored
  by the ASGI app just as it is in production. `TestClient.websocket_connect`
  is synchronous in the `requests`-based default; we use it inside a
  thread pool so an async test can drive the server without blocking
  the event loop. Documented in BUILD-LOG.

Fixtures:

* ``realtime_pool`` — per-test asyncpg pool, migrations + TRUNCATE.
* ``seeded_actor`` + ``valid_session`` — same pattern as the Gateway
  suite so auth works in-flow.
* ``dispatcher_app`` — a standalone FastAPI app with the realtime
  sub-router mounted, dispatcher started. Skips the full Gateway
  middleware stack because the WS path bypasses it.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
from collections.abc import AsyncGenerator
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
from fastapi import FastAPI

from lib.shared.ids import uuid7
from services.gateway.auth import create_session
from services.realtime.main import configure_realtime


pytestmark = pytest.mark.integration


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]


async def _run_migrations(conn: asyncpg.Connection) -> None:
    for p in sorted((REPO_ROOT / "db" / "migrations").glob("*.sql")):
        await conn.execute(p.read_text())


async def _truncate_all(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        SELECT c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
        """
    )
    names = [r["relname"] for r in rows]
    if not names:
        return
    lst = ", ".join(f'"{t}"' for t in names)
    await conn.execute("SET lock_timeout = '1s'")
    for _ in range(5):
        try:
            await conn.execute(f"TRUNCATE {lst} RESTART IDENTITY CASCADE")
            return
        except (
            asyncpg.exceptions.DeadlockDetectedError,
            asyncpg.exceptions.LockNotAvailableError,
        ):
            await asyncio.sleep(0.2)
    await conn.execute(f"TRUNCATE {lst} RESTART IDENTITY CASCADE")


async def _wait_idle(dsn: str, max_wait_ms: float = 2000.0) -> None:
    import time as _t

    start = _t.monotonic()
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
        if (_t.monotonic() - start) * 1000 > max_wait_ms:
            return
        await asyncio.sleep(0.02)


@pytest_asyncio.fixture
async def realtime_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    await _wait_idle(dsn)
    pool = await asyncpg.create_pool(dsn, min_size=2, max_size=20)
    async with pool.acquire() as conn:
        await _run_migrations(conn)
        await _truncate_all(conn)
    try:
        yield pool
    finally:
        try:
            pool.terminate()
        except Exception:
            pass


@pytest.fixture
def tenant_id() -> UUID:
    return uuid7()


@pytest.fixture
def tenant_id_b() -> UUID:
    return uuid7()


@pytest_asyncio.fixture
async def seeded_actor(
    realtime_pool: asyncpg.Pool, tenant_id: UUID
) -> UUID:
    actor_id = uuid7()
    await realtime_pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Alice', 'active')
        """,
        actor_id,
        tenant_id,
    )
    return actor_id


@pytest_asyncio.fixture
async def seeded_actor_b(
    realtime_pool: asyncpg.Pool, tenant_id_b: UUID
) -> UUID:
    actor_id = uuid7()
    await realtime_pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Bob', 'active')
        """,
        actor_id,
        tenant_id_b,
    )
    return actor_id


@pytest_asyncio.fixture
async def valid_session(
    realtime_pool: asyncpg.Pool,
    seeded_actor: UUID,
    tenant_id: UUID,
) -> tuple[str, UUID]:
    token, ctx = await create_session(
        realtime_pool,
        actor_id=seeded_actor,
        tenant_id=tenant_id,
    )
    return token, ctx.actor_id


@pytest_asyncio.fixture
async def valid_session_b(
    realtime_pool: asyncpg.Pool,
    seeded_actor_b: UUID,
    tenant_id_b: UUID,
) -> tuple[str, UUID]:
    token, ctx = await create_session(
        realtime_pool,
        actor_id=seeded_actor_b,
        tenant_id=tenant_id_b,
    )
    return token, ctx.actor_id


@pytest_asyncio.fixture
async def dispatcher_app(
    realtime_pool: asyncpg.Pool,
) -> AsyncGenerator[tuple[FastAPI, "services.realtime.dispatcher.Dispatcher"], None]:  # noqa: F821
    """Standalone FastAPI app with realtime mounted.

    Starts + stops the dispatcher around the yield.
    """
    from services.realtime.dispatcher import Dispatcher

    app = FastAPI()
    disp = Dispatcher(realtime_pool)
    deps = configure_realtime(app, pool=realtime_pool, dispatcher=disp, start=False)
    await disp.start()
    try:
        yield app, disp
    finally:
        await disp.stop()
