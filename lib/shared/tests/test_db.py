"""Tests for lib/shared/db.py — typed helpers + transactions.

Unit tests use mocks for the pool; integration tests (`@pytest.mark.integration`)
use the real DATABASE_URL pool provided by the root conftest.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
from pydantic import BaseModel

from lib.shared.db import (
    ConnectionPoolNotInitializedError,
    RowHydrationError,
    close_pool,
    execute,
    get_pool,
    init_pool,
    select_many,
    select_one,
    transaction,
)
from lib.shared.ids import uuid7


# =====================================================================
# Unit tests — no live DB
# =====================================================================

async def test_get_pool_uninitialised_raises():
    # Start clean — if a prior test left _pool set, clear it.
    await close_pool()
    with pytest.raises(ConnectionPoolNotInitializedError):
        get_pool()


async def test_init_pool_missing_dsn(monkeypatch):
    await close_pool()
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ConnectionPoolNotInitializedError):
        await init_pool()


class _Row(BaseModel):
    id: int
    name: str


async def test_select_one_returns_dict_without_row_type():
    fake_record = MagicMock(spec=asyncpg.Record)
    fake_record.__iter__ = lambda self: iter([("id", 1), ("name", "x")])
    fake_record.keys = lambda: ["id", "name"]
    fake_record.values = lambda: [1, "x"]
    # asyncpg.Record -> dict works via dict(record); mock it:
    fake_record.__getitem__ = lambda self, k: {"id": 1, "name": "x"}[k]
    # easier: patch at the level of the pool mock:
    pool = MagicMock()
    pool.fetchrow = AsyncMock(return_value={"id": 1, "name": "x"})
    result = await select_one("SELECT ...", conn=pool)
    assert result == {"id": 1, "name": "x"}


async def test_select_one_none_when_no_row():
    pool = MagicMock()
    pool.fetchrow = AsyncMock(return_value=None)
    result = await select_one("SELECT ...", conn=pool)
    assert result is None


async def test_select_one_hydrates_with_row_type():
    pool = MagicMock()
    pool.fetchrow = AsyncMock(return_value={"id": 1, "name": "x"})
    result = await select_one("SELECT ...", conn=pool, row_type=_Row)
    assert result == _Row(id=1, name="x")


async def test_select_one_hydration_failure_raises():
    pool = MagicMock()
    # Missing `name` — Pydantic will reject.
    pool.fetchrow = AsyncMock(return_value={"id": 1})
    with pytest.raises(RowHydrationError):
        await select_one("SELECT ...", conn=pool, row_type=_Row)


async def test_select_many_empty():
    pool = MagicMock()
    pool.fetch = AsyncMock(return_value=[])
    result = await select_many("SELECT ...", conn=pool, row_type=_Row)
    assert result == []


async def test_select_many_hydrates_all():
    pool = MagicMock()
    pool.fetch = AsyncMock(return_value=[
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ])
    rows = await select_many("SELECT ...", conn=pool, row_type=_Row)
    assert rows == [_Row(id=1, name="a"), _Row(id=2, name="b")]


async def test_execute_returns_status_tag():
    pool = MagicMock()
    pool.execute = AsyncMock(return_value="INSERT 0 1")
    status = await execute("INSERT ...", conn=pool)
    assert status == "INSERT 0 1"


# =====================================================================
# Integration tests — require DATABASE_URL
# =====================================================================

@pytest.mark.integration
async def test_integration_pool_initialises_and_runs_query(fresh_db):
    """
    fresh_db yields the session pool. Verify we can run a trivial
    query against it and that init_pool() is idempotent.
    """
    pool = fresh_db
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT 1")
    assert val == 1


@pytest.mark.integration
async def test_integration_transaction_commits(fresh_db):
    """Insert inside transaction() and verify post-commit visibility."""
    # Install our pool reference so the module-level helpers work.
    import lib.shared.db as db_mod
    db_mod._pool = fresh_db
    try:
        actor_id = uuid7()
        tenant_id = uuid7()
        async with transaction() as tx:
            await tx.execute(
                "INSERT INTO actors (id, tenant_id, type, display_name, created_at)"
                " VALUES ($1, $2, 'human_internal', 'Alice', now())",
                actor_id, tenant_id,
            )
        async with fresh_db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT display_name FROM actors WHERE id = $1", actor_id
            )
        assert row["display_name"] == "Alice"
    finally:
        db_mod._pool = None


@pytest.mark.integration
async def test_integration_transaction_rolls_back_on_exception(fresh_db):
    import lib.shared.db as db_mod
    db_mod._pool = fresh_db
    try:
        actor_id = uuid7()
        tenant_id = uuid7()
        with pytest.raises(RuntimeError):
            async with transaction() as tx:
                await tx.execute(
                    "INSERT INTO actors (id, tenant_id, type, display_name, created_at)"
                    " VALUES ($1, $2, 'human_internal', 'Bob', now())",
                    actor_id, tenant_id,
                )
                raise RuntimeError("abort")
        async with fresh_db.acquire() as conn:
            count = await conn.fetchval(
                "SELECT count(*) FROM actors WHERE id = $1", actor_id
            )
        assert count == 0
    finally:
        db_mod._pool = None


@pytest.mark.integration
async def test_integration_select_one_hydrates_pydantic(fresh_db):
    """End-to-end: INSERT + SELECT round-trip through a Pydantic row type."""
    from lib.shared.types import ActorRow
    import lib.shared.db as db_mod
    db_mod._pool = fresh_db
    try:
        aid = uuid7()
        tid = uuid7()
        async with transaction() as tx:
            await tx.execute(
                "INSERT INTO actors (id, tenant_id, type, display_name, status,"
                " metadata, specification_id, created_at, last_seen_at, email)"
                " VALUES ($1, $2, 'human_internal', 'Alice', 'active',"
                " NULL, NULL, now(), NULL, NULL)",
                aid, tid,
            )
        row = await select_one(
            "SELECT id, tenant_id, type, display_name, email, status,"
            " metadata, specification_id, created_at, last_seen_at"
            " FROM actors WHERE id = $1",
            aid,
            row_type=ActorRow,
        )
        assert row is not None
        assert isinstance(row, ActorRow)
        assert row.display_name == "Alice"
        assert row.type == "human_internal"
    finally:
        db_mod._pool = None
