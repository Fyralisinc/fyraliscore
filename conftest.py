"""
Top-level pytest conftest for Company OS.

Provides the fresh-database fixture mandated by BUILD-PLAN.md §0.1
step 6. Each integration test gets a clean database: all user tables
are truncated between tests; the schema itself is loaded once per
session from db/migrations/*.sql.

Usage:

    @pytest.mark.integration
    async def test_something(db_pool):
        async with db_pool.acquire() as conn:
            ...
"""
from __future__ import annotations

import os
import pathlib
from collections.abc import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio
from dotenv import load_dotenv


REPO_ROOT = pathlib.Path(__file__).resolve().parent
MIGRATIONS_DIR = REPO_ROOT / "db" / "migrations"


def _load_env() -> None:
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file)


_load_env()


def _database_url() -> str | None:
    return os.environ.get("DATABASE_URL")


def _requires_db(request: pytest.FixtureRequest) -> str:
    dsn = _database_url()
    if not dsn:
        pytest.skip(
            "DATABASE_URL not set — skipping integration test. "
            "Start docker-compose up and copy .env.example to .env."
        )
    return dsn


async def _run_migrations(conn: asyncpg.Connection) -> None:
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    if not migration_files:
        raise RuntimeError(f"No migrations found in {MIGRATIONS_DIR}")
    for path in migration_files:
        sql = path.read_text()
        await conn.execute(sql)


async def _tables_to_truncate(conn: asyncpg.Connection) -> list[str]:
    # Exclude tables that are seeded only by migrations (no test mutates
    # them) — truncating would wipe the seed and migrations don't re-run
    # between tests, so dependent tests would see an empty table.
    seed_only = ("demo_configs",)
    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
          AND c.relname <> ALL($1::text[])
        """,
        list(seed_only),
    )
    return [r["relname"] for r in rows]


# ---------------------------------------------------------------------
# Fresh-DB fixtures
# ---------------------------------------------------------------------
# Pool scoped to "function" to avoid cross-event-loop issues with
# pytest-asyncio 1.x — each async test gets its own loop, and an
# asyncpg pool bound to one loop can't be used from another. Creating
# a pool per test costs ~30ms, which is acceptable.
@pytest_asyncio.fixture(scope="function")
async def db_pool(request) -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Per-test asyncpg pool. Migrations are idempotent (IF NOT EXISTS),
    so the first test in a run applies them and subsequent tests
    no-op against a fresh TRUNCATE.
    """
    dsn = _requires_db(request)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await _run_migrations(conn)
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture(scope="function")
async def fresh_db(db_pool: asyncpg.Pool) -> AsyncGenerator[asyncpg.Pool, None]:
    """
    Guarantees a clean database state. Truncates every base table in
    public schema (CASCADE) before the test starts. Do not share
    state between tests through the database.
    """
    async with db_pool.acquire() as conn:
        tables = await _tables_to_truncate(conn)
        if tables:
            table_list = ", ".join(f'"{t}"' for t in tables)
            await conn.execute(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE")
    yield db_pool


# ---------------------------------------------------------------------
# Marker collection: auto-skip integration tests when DATABASE_URL
# is absent. This keeps `pytest` green in environments without a DB
# (CI doc builds, etc.) while integration work must run with a real DB.
# ---------------------------------------------------------------------
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _database_url():
        return
    skip_marker = pytest.mark.skip(
        reason="DATABASE_URL not set; skipping integration tests"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_marker)
