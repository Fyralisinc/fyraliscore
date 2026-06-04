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
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio
from dotenv import load_dotenv

from lib.shared.migrations import schema_bootstrap_lock


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


# ---------------------------------------------------------------------
# RLS test role.
#
# A SUPERUSER / BYPASSRLS connection sees through every row-level-security
# policy regardless of FORCE, so RLS isolation tests connected as such a
# role can only *skip* (they cannot prove isolation). Many local/dev DBs
# connect as a superuser. This fixture provisions a dedicated NON-super,
# NON-bypassrls app role (idempotent) and yields a pool connected as it, so
# the RLS policies are actually exercised. If the configured role already
# can't bypass RLS (e.g. CI), it is used directly.
#
# Credentials are for a local test DB only — never a production secret.
# ---------------------------------------------------------------------
RLS_TEST_ROLE = "fyralis_rls_test"
RLS_TEST_PW = "rls_test_pw"


async def _provision_rls_app_role(super_dsn: str) -> str:
    conn = await asyncpg.connect(super_dsn)
    try:
        async with schema_bootstrap_lock(conn):
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_roles WHERE rolname = $1", RLS_TEST_ROLE
            )
            if not exists:
                await conn.execute(
                    f'CREATE ROLE "{RLS_TEST_ROLE}" LOGIN PASSWORD '
                    f"'{RLS_TEST_PW}' NOSUPERUSER NOBYPASSRLS"
                )
            await conn.execute(f'GRANT USAGE ON SCHEMA public TO "{RLS_TEST_ROLE}"')
            await conn.execute(
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER "
                f'ON ALL TABLES IN SCHEMA public TO "{RLS_TEST_ROLE}"'
            )
            await conn.execute(
                "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public "
                f'TO "{RLS_TEST_ROLE}"'
            )
    finally:
        await conn.close()
    host_tail = super_dsn.split("@", 1)[1]  # host:port/db[?params]
    return f"postgresql://{RLS_TEST_ROLE}:{RLS_TEST_PW}@{host_tail}"


@pytest_asyncio.fixture
async def rls_app_pool() -> AsyncGenerator[asyncpg.Pool, None]:
    """Pool whose role does NOT bypass RLS, so policies are enforced.

    Skips cleanly if DATABASE_URL is unset, or if the non-super role cannot
    be provisioned (e.g. a locked-down environment without CREATEROLE)."""
    dsn = _database_url()
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    probe = await asyncpg.connect(dsn)
    try:
        bypasses = await probe.fetchval(
            "SELECT usesuper OR usebypassrls FROM pg_user WHERE usename = current_user"
        )
    finally:
        await probe.close()
    if bypasses:
        try:
            app_dsn = await _provision_rls_app_role(dsn)
        except asyncpg.PostgresError as e:
            pytest.skip(f"cannot provision non-super RLS test role: {e}")
    else:
        app_dsn = dsn  # already a non-super role (e.g. CI)
    pool = await asyncpg.create_pool(app_dsn, min_size=1, max_size=3)
    try:
        yield pool
    finally:
        await pool.close()


async def _run_migrations(conn: asyncpg.Connection) -> None:
    # T3: each migration runs in its own transaction so partial
    # failures roll back cleanly instead of poisoning the
    # connection. See lib/shared/migrations.py.
    from lib.shared.migrations import apply_migrations_dir
    await apply_migrations_dir(conn, MIGRATIONS_DIR)


async def _install_test_tenant_auto_register(conn: asyncpg.Connection) -> None:
    """
    Test harness compatibility for migration 0037.

    Production now requires every tenant-scoped write to reference a
    registered `tenants` row. A large amount of older integration test
    fixture code still creates fresh UUID tenants inline through raw SQL
    helpers. Rather than weakening the production FK, tests install a
    small DB-local trigger that registers placeholder tenants before
    tenant-scoped inserts. This keeps the broad suite useful while the
    application schema remains strict.
    """
    await conn.execute(
        """
        CREATE OR REPLACE FUNCTION _test_auto_register_tenant()
        RETURNS trigger AS $$
        BEGIN
          IF NEW.tenant_id IS NOT NULL THEN
            INSERT INTO tenants (id, name, is_demo)
            VALUES (
              NEW.tenant_id,
              'test_auto_' || NEW.tenant_id::text,
              FALSE
            )
            ON CONFLICT (id) DO NOTHING;
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    all_tenant_tables = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
          AND a.attname = 'tenant_id'
          AND NOT a.attisdropped
          AND c.relname <> 'tenants'
        """
    )
    for row in all_tenant_tables:
        table = row["relname"]
        await conn.execute(
            f'DROP TRIGGER IF EXISTS _test_auto_register_tenant ON "{table}"'
        )

    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
          AND a.attname = 'tenant_id'
          AND NOT a.attisdropped
          AND c.relname <> 'tenants'
        """
    )
    for row in rows:
        table = row["relname"]
        await conn.execute(
            f"""
            CREATE TRIGGER _test_auto_register_tenant
            BEFORE INSERT ON "{table}"
            FOR EACH ROW
            EXECUTE FUNCTION _test_auto_register_tenant()
            """
        )


async def _seed_demo_configs(conn: asyncpg.Connection) -> None:
    """Restore migration-seeded demo companies after `fresh_db` TRUNCATE."""
    await conn.execute(
        """
        INSERT INTO demo_configs (
          id, company_id, name, description, tagline, snapshot_uri,
          model_routing, cost_cap_usd_per_session, determinism_seed
        ) VALUES
          (
            '00000000-0000-7d23-8000-000000000001'::uuid,
            'truss',
            'Truss',
            '40-person AI-native developer infrastructure company.',
            'Series A, founder at full cognitive load',
            'demo/snapshots/truss-v1.sql.zst',
            '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
            5.00,
            42
          ),
          (
            '00000000-0000-7d23-8000-000000000002'::uuid,
            'northwind',
            'Northwind Software',
            'Series B SaaS, 180 employees, $14M ARR, growing 80% YoY.',
            'Series B, healthy growth, normal Tuesday',
            'demo/snapshots/northwind-v1.sql.zst',
            '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
            5.00,
            43
          ),
          (
            '00000000-0000-7d23-8000-000000000003'::uuid,
            'meridian',
            'Meridian Industrial',
            'Series C enterprise software, 1100 employees, $85M ARR.',
            'Series C, $4.2M ARR customer escalating',
            'demo/snapshots/meridian-v1.sql.zst',
            '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
            7.50,
            44
          ),
          (
            '00000000-0000-7d23-8000-000000000004'::uuid,
            'pelago',
            'Pelago',
            'Series A B2B SaaS revenue-intelligence platform.',
            'Series A, multi-shock year, founder running on signals',
            'demo/snapshots/pelago-v1.sql.zst',
            '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
            5.00,
            42
          )
        ON CONFLICT (company_id) DO UPDATE
          SET name = EXCLUDED.name,
              description = EXCLUDED.description,
              tagline = EXCLUDED.tagline,
              snapshot_uri = EXCLUDED.snapshot_uri,
              model_routing = EXCLUDED.model_routing,
              cost_cap_usd_per_session = EXCLUDED.cost_cap_usd_per_session,
              determinism_seed = EXCLUDED.determinism_seed;
        """
    )


async def _seed_test_baseline(conn: asyncpg.Connection) -> None:
    default_tenant = os.environ.get("DEFAULT_TENANT_ID")
    tenant_ids: set[UUID] = {
        UUID("11111111-1111-1111-1111-111111111111"),
        UUID("22222222-2222-2222-2222-222222222222"),
    }
    if default_tenant:
        try:
            tenant_ids.add(UUID(default_tenant))
        except ValueError:
            pass
    for tenant_id in tenant_ids:
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, $2, FALSE)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
            f"test_baseline_{tenant_id}",
        )
    await _seed_demo_configs(conn)


async def _tables_to_truncate(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
        """
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
        async with schema_bootstrap_lock(conn):
            await _run_migrations(conn)
            await _install_test_tenant_auto_register(conn)
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
        async with schema_bootstrap_lock(conn):
            tables = await _tables_to_truncate(conn)
            if tables:
                table_list = ", ".join(f'"{t}"' for t in tables)
                await conn.execute(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE")
            await _seed_test_baseline(conn)
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
