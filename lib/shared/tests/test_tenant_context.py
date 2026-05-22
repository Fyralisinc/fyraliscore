"""Tests for lib/shared/tenant_context.py.

Validates that:
  - tenant_transaction sets app.current_tenant for the life of the tx
  - the setting vanishes after the tx ends (no pool leak)
  - bind_tenant works with caller-owned transactions
  - TenantContext quacks as a Connection for repo-style usage
  - rollback also clears the setting (Postgres handles this; we verify)
  - non-UUID tenant_id raises TenantContextError early
"""
from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from lib.shared.tenant_context import (
    TenantContext,
    TenantContextError,
    bind_tenant,
    current_tenant,
    tenant_transaction,
)


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def db_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
    try:
        yield pool
    finally:
        await pool.close()


# ---------------------------------------------------------------------
# Happy path: app.current_tenant set inside the transaction
# ---------------------------------------------------------------------

async def test_tenant_transaction_sets_current_tenant(db_pool: asyncpg.Pool):
    tid = uuid7()
    async with tenant_transaction(tid, pool=db_pool) as tctx:
        seen = await tctx.fetchval(
            "SELECT current_setting('app.current_tenant', true)"
        )
        assert seen == str(tid)
        # current_tenant() helper round-trips the UUID.
        assert await current_tenant(tctx.conn) == tid


# ---------------------------------------------------------------------
# Setting vanishes after the transaction (per-tx scope)
# ---------------------------------------------------------------------

async def test_setting_vanishes_after_commit(db_pool: asyncpg.Pool):
    tid = uuid7()
    async with tenant_transaction(tid, pool=db_pool):
        pass  # commit
    # Acquire a fresh connection and check the setting is empty.
    async with db_pool.acquire() as conn:
        seen = await conn.fetchval(
            "SELECT current_setting('app.current_tenant', true)"
        )
        assert seen == ""  # current_setting returns '' for missing


async def test_setting_vanishes_after_rollback(db_pool: asyncpg.Pool):
    tid = uuid7()
    with pytest.raises(RuntimeError):
        async with tenant_transaction(tid, pool=db_pool) as tctx:
            seen = await tctx.fetchval(
                "SELECT current_setting('app.current_tenant', true)"
            )
            assert seen == str(tid)
            raise RuntimeError("force rollback")
    # Pool acquire returns clean connection.
    async with db_pool.acquire() as conn:
        seen = await conn.fetchval(
            "SELECT current_setting('app.current_tenant', true)"
        )
        assert seen == ""


# ---------------------------------------------------------------------
# bind_tenant — caller owns the transaction
# ---------------------------------------------------------------------

async def test_bind_tenant_in_caller_transaction(db_pool: asyncpg.Pool):
    tid = uuid7()
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tid) as tctx:
                seen = await tctx.fetchval(
                    "SELECT current_setting('app.current_tenant', true)"
                )
                assert seen == str(tid)


async def test_bind_tenant_setting_scoped_to_caller_tx(db_pool: asyncpg.Pool):
    tid = uuid7()
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tid):
                pass
        # End of inner tx — setting cleared
        seen = await conn.fetchval(
            "SELECT current_setting('app.current_tenant', true)"
        )
        assert seen == ""


# ---------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------

async def test_non_uuid_tenant_id_raises():
    with pytest.raises(TenantContextError):
        async with tenant_transaction("not-a-uuid"):  # type: ignore[arg-type]
            pass


# ---------------------------------------------------------------------
# Connection-like surface
# ---------------------------------------------------------------------

async def test_tenant_context_quacks_as_conn(db_pool: asyncpg.Pool):
    """Repos that take `conn` should accept a TenantContext unchanged."""
    tid = uuid7()
    async with tenant_transaction(tid, pool=db_pool) as tctx:
        # execute returns a status string
        status = await tctx.execute("SELECT 1")
        assert status.startswith("SELECT")
        # fetchrow returns a Record
        row = await tctx.fetchrow("SELECT 42 AS x")
        assert row["x"] == 42
        # fetchval is unwrapped scalar
        val = await tctx.fetchval("SELECT 'hello'")
        assert val == "hello"
        # fetch returns list[Record]
        rows = await tctx.fetch("SELECT generate_series(1, 3) AS i")
        assert [r["i"] for r in rows] == [1, 2, 3]


async def test_two_concurrent_transactions_have_isolated_tenants(
    db_pool: asyncpg.Pool,
):
    """Each tenant_transaction acquires its own pool connection, so
    concurrent users see their own tenant — no cross-talk."""
    import asyncio

    tid_a = uuid7()
    tid_b = uuid7()

    async def _check(tid: UUID) -> str:
        async with tenant_transaction(tid, pool=db_pool) as tctx:
            await asyncio.sleep(0.01)
            return await tctx.fetchval(
                "SELECT current_setting('app.current_tenant', true)"
            )

    seen_a, seen_b = await asyncio.gather(_check(tid_a), _check(tid_b))
    assert seen_a == str(tid_a)
    assert seen_b == str(tid_b)


# ---------------------------------------------------------------------
# current_tenant helper edge cases
# ---------------------------------------------------------------------

async def test_current_tenant_unset_returns_none(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        assert await current_tenant(conn) is None


async def test_current_tenant_returns_uuid_when_set(db_pool: asyncpg.Pool):
    tid = uuid7()
    async with tenant_transaction(tid, pool=db_pool) as tctx:
        recovered = await current_tenant(tctx.conn)
        assert recovered == tid
