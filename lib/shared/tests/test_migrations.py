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
import uuid

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
        await c.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
              filename text PRIMARY KEY,
              applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await c.execute(
            "DELETE FROM schema_migrations WHERE filename LIKE 'test_migrations_%'"
        )
        yield c
        # Clean up after the test.
        await c.execute(
            "DROP TABLE IF EXISTS migrations_t3_marker CASCADE"
        )
        await c.execute(
            "DELETE FROM schema_migrations WHERE filename LIKE 'test_migrations_%'"
        )
    await pool.close()


def _migration_name(label: str) -> str:
    return f"test_migrations_{label}_{uuid.uuid4().hex}.sql"


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
    first = _migration_name("01_first")
    bad = _migration_name("02_bad")
    (tmp_path / first).write_text(
        "CREATE TABLE migrations_t3_marker (id INT PRIMARY KEY);"
    )
    (tmp_path / bad).write_text(
        "CREATE TABLE migrations_t3_marker_bad (id INT PRIMARY KEY); "
        "INSERT INTO no_such_table VALUES (1);"
    )
    with pytest.raises(MigrationError) as ei:
        await apply_migrations_dir(conn, tmp_path, on_error="stop")
    assert ei.value.filename == bad
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
    bad = _migration_name("01_bad")
    good = _migration_name("02_good")
    (tmp_path / bad).write_text(
        "CREATE TABLE no_op (); INSERT INTO no_such_table VALUES (1);"
    )
    (tmp_path / good).write_text(
        "CREATE TABLE migrations_t3_marker (id INT PRIMARY KEY);"
    )
    applied = await apply_migrations_dir(conn, tmp_path, on_error="warn")
    assert applied == [good]
    assert await conn.fetchval(
        "SELECT to_regclass('public.migrations_t3_marker') IS NOT NULL"
    )


async def test_apply_migrations_dir_skips_recorded_files(conn, tmp_path):
    skipped = _migration_name("01_skipped")
    applied_name = _migration_name("02_applied")
    await conn.execute(
        "INSERT INTO schema_migrations(filename) VALUES ($1)",
        skipped,
    )
    (tmp_path / skipped).write_text(
        "CREATE TABLE migrations_t3_marker_skipped (id INT PRIMARY KEY);"
    )
    (tmp_path / applied_name).write_text(
        "CREATE TABLE migrations_t3_marker (id INT PRIMARY KEY);"
    )

    applied = await apply_migrations_dir(conn, tmp_path, on_error="stop")

    assert applied == [applied_name]
    assert not await conn.fetchval(
        "SELECT to_regclass('public.migrations_t3_marker_skipped') IS NOT NULL"
    )
    assert await conn.fetchval(
        "SELECT to_regclass('public.migrations_t3_marker') IS NOT NULL"
    )
