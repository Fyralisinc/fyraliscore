"""Integration tests for lib/shared/migrations.py.

Spec for T3: a migration that fails on its second statement must
roll back the first statement's effects, so the database is
indistinguishable from "migration never ran". The previous
hand-rolled `await conn.execute(file_text)` did NOT do this — it
left statement 1 committed and statement 2's failure poisoned the
connection's transaction state for every subsequent migration.
"""
from __future__ import annotations

import os
import pathlib
import tempfile

import asyncpg
import pytest

from lib.shared.migrations import (
    MigrationError,
    apply_migration,
    apply_migrations_dir,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    return dsn


@pytest.fixture()
async def conn():
    pool = await asyncpg.create_pool(_dsn(), min_size=1, max_size=2)
    async with pool.acquire() as c:
        # Make sure no leftover test table from a prior failed run.
        await c.execute(
            "DROP TABLE IF EXISTS migrations_t3_marker CASCADE"
        )
        yield c
        # Clean up after the test.
        await c.execute(
            "DROP TABLE IF EXISTS migrations_t3_marker CASCADE"
        )
    await pool.close()


async def test_failing_second_statement_rolls_back_first(conn):
    """A multi-statement migration whose second statement is bad must
    roll back the first. Without the wrapping transaction, the
    `CREATE TABLE` would commit and the subsequent error would only
    affect later operations.
    """
    bad_migration = """
    CREATE TABLE migrations_t3_marker (id INT PRIMARY KEY, name TEXT);
    INSERT INTO migrations_t3_marker (id, NOT_A_REAL_COLUMN) VALUES (1, 'hi');
    """
    with pytest.raises(MigrationError) as ei:
        await apply_migration(conn, bad_migration, name="bad.sql")
    assert ei.value.filename == "bad.sql"
    # Connection is clean: no aborted-transaction state. Issuing a
    # fresh query immediately works (this is the key regression — the
    # old runner would have left the connection in error state and
    # this query would crash with "current transaction is aborted").
    exists = await conn.fetchval(
        "SELECT to_regclass('public.migrations_t3_marker') IS NOT NULL"
    )
    assert exists is False, (
        "table created by statement 1 should NOT survive — "
        "the failing second statement must roll back the whole file"
    )


async def test_succeeding_migration_persists(conn):
    good_migration = """
    CREATE TABLE migrations_t3_marker (id INT PRIMARY KEY, name TEXT);
    INSERT INTO migrations_t3_marker (id, name) VALUES (1, 'kept');
    """
    await apply_migration(conn, good_migration, name="good.sql")
    row = await conn.fetchrow(
        "SELECT name FROM migrations_t3_marker WHERE id = 1"
    )
    assert row["name"] == "kept"


async def test_apply_migrations_dir_stops_on_first_failure(conn, tmp_path):
    """`on_error='stop'` re-raises the first failure and the previous
    successful migrations must persist (each migration is its own
    transaction). The failing one must NOT have left side effects."""
    (tmp_path / "01_first.sql").write_text(
        "CREATE TABLE migrations_t3_marker (id INT PRIMARY KEY);"
    )
    (tmp_path / "02_bad.sql").write_text(
        "CREATE TABLE migrations_t3_marker_bad (id INT PRIMARY KEY); "
        "INSERT INTO no_such_table VALUES (1);"
    )
    with pytest.raises(MigrationError) as ei:
        await apply_migrations_dir(conn, tmp_path, on_error="stop")
    assert ei.value.filename == "02_bad.sql"
    # First migration's effect persists.
    assert await conn.fetchval(
        "SELECT to_regclass('public.migrations_t3_marker') IS NOT NULL"
    )
    # Second migration's first statement must NOT persist.
    assert not await conn.fetchval(
        "SELECT to_regclass('public.migrations_t3_marker_bad') IS NOT NULL"
    )
    # Cleanup
    await conn.execute("DROP TABLE IF EXISTS migrations_t3_marker_bad")


async def test_apply_migrations_dir_warn_continues(conn, tmp_path):
    """`on_error='warn'` skips failing migrations and applies the rest.
    The harness uses this against long-lived dev DBs where most
    migrations are already applied."""
    (tmp_path / "01_bad.sql").write_text(
        "CREATE TABLE no_op (); INSERT INTO no_such_table VALUES (1);"
    )
    (tmp_path / "02_good.sql").write_text(
        "CREATE TABLE migrations_t3_marker (id INT PRIMARY KEY);"
    )
    applied = await apply_migrations_dir(conn, tmp_path, on_error="warn")
    assert applied == ["02_good.sql"]
    assert await conn.fetchval(
        "SELECT to_regclass('public.migrations_t3_marker') IS NOT NULL"
    )


# ---------------------------------------------------------------------
# Non-transactional migrations (ingestion LLD §1.6).
# `CREATE INDEX CONCURRENTLY` cannot run inside an explicit BEGIN/COMMIT.
# The runner detects either the keyword or the
# `-- migration:no-transaction` directive and skips the txn wrapper.
# Pure-unit detection tests live in test_migrations_unit.py; this file
# carries the end-to-end behaviour against a real Postgres.
# ---------------------------------------------------------------------

async def test_concurrently_index_runs_outside_transaction(conn):
    """CREATE INDEX CONCURRENTLY would fail with SQLSTATE 25001 if
    `apply_migration` wrapped it in a transaction. This test proves
    the keyword-detection path bypasses the wrapper end-to-end.

    Skip note: requires Postgres; uses a transient table.
    """
    await conn.execute(
        "CREATE TABLE IF NOT EXISTS migrations_no_txn_marker "
        "(id INT PRIMARY KEY, name TEXT)"
    )
    try:
        await apply_migration(
            conn,
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
            "migrations_no_txn_marker_name_idx "
            "ON migrations_no_txn_marker (name);",
            name="0049_test.sql",
        )
        idx_exists = await conn.fetchval(
            "SELECT to_regclass('public.migrations_no_txn_marker_name_idx') "
            "IS NOT NULL"
        )
        assert idx_exists is True
    finally:
        await conn.execute(
            "DROP TABLE IF EXISTS migrations_no_txn_marker CASCADE"
        )


async def test_no_transaction_directive_path(conn):
    """Files carrying an explicit `-- migration:no-transaction`
    directive must also bypass the txn wrapper, even without the
    CONCURRENTLY keyword. This is the manual escape hatch.
    """
    sql = (
        "-- migration:no-transaction\n"
        "CREATE TABLE IF NOT EXISTS migrations_t3_marker (id INT PRIMARY KEY);"
    )
    try:
        await apply_migration(conn, sql, name="directive_test.sql")
        assert await conn.fetchval(
            "SELECT to_regclass('public.migrations_t3_marker') IS NOT NULL"
        )
    finally:
        await conn.execute("DROP TABLE IF EXISTS migrations_t3_marker CASCADE")
