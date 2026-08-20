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

import base64
import hashlib
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


# Operational secrets the test suite needs but that are NOT test-specific
# assertions: a Fernet KEK for the encrypted-secrets store and an
# application-level Discord bot token. Locally these come from `.env`;
# CI (and a fresh clone without a `.env`) has neither, so the gateway's
# secret-store wiring used to warn-then-fail under `filterwarnings=error`
# and the Discord client raised `discord_secret_unavailable` before any
# API call. `setdefault` means a real value (from `.env` or a CI secret)
# always wins; absent one we fall back to a deterministic dev value so the
# suite is hermetic. Negative-path tests that exercise "unset" do their own
# `monkeypatch.delenv`, so these defaults don't mask them.
def _ensure_test_secrets() -> None:
    # A valid (url-safe base64, 32-byte) Fernet key — FernetSecretStore
    # validates the key shape at construction, so a placeholder string
    # would fail-fast. sha256 gives exactly 32 bytes, deterministically.
    test_kek = base64.urlsafe_b64encode(
        hashlib.sha256(b"fyralis-test-master-kek").digest()
    ).decode("ascii")
    os.environ.setdefault("MASTER_KEK", test_kek)
    os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-bot-token")


_load_env()
_ensure_test_secrets()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-cutover-dryrun",
        action="store_true",
        default=False,
        help=(
            "Run the staging-only one-hour webhook cutover dry run. "
            "Requires CUTOVER_DRYRUN_TARGET_URL and provider secrets."
        ),
    )


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
    # on_error="warn": local dev/test databases are long-lived, and older runs
    # may predate the schema_migrations ledger. Warn-and-continue lets later
    # migrations re-establish the correct final schema when a stale DB replays an
    # already-superseded migration. Once recorded, the ledger is preserved across
    # per-test TRUNCATEs by _tables_to_truncate below.
    await apply_migrations_dir(conn, MIGRATIONS_DIR, on_error="warn")


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


async def _tables_to_truncate(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
          AND c.relname <> 'schema_migrations'
          AND c.relname NOT LIKE 'schema_migrations_ext_%'
        """
    )
    return [r["relname"] for r in rows]


async def _truncate_public_tables(conn: asyncpg.Connection) -> None:
    tables = await _tables_to_truncate(conn)
    if not tables:
        return
    table_list = ", ".join(f'"{t}"' for t in tables)
    await conn.execute(f"TRUNCATE {table_list} RESTART IDENTITY CASCADE")


async def _prune_empty_out_of_window_partitions(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        WITH bounds AS (
          SELECT
            (date_trunc('month', CURRENT_DATE)::date - INTERVAL '36 months')::date AS keep_start,
            (date_trunc('month', CURRENT_DATE)::date + INTERVAL '7 months')::date AS keep_end
        ),
        parts AS (
          SELECT
            parent.relname AS parent_name,
            child.relname AS partition_name,
            to_date(substring(child.relname FROM '_(\\d{4}_\\d{2})$'), 'YYYY_MM') AS month_start
          FROM pg_inherits inh
          JOIN pg_class child ON child.oid = inh.inhrelid
          JOIN pg_class parent ON parent.oid = inh.inhparent
          JOIN pg_namespace n ON n.oid = parent.relnamespace
          WHERE n.nspname = 'public'
            AND parent.relname IN ('observations', 'resource_transactions')
            AND child.relkind = 'r'
            AND child.relname ~ '_(\\d{4}_\\d{2})$'
        )
        SELECT parent_name, partition_name
        FROM parts, bounds
        WHERE month_start < keep_start OR month_start >= keep_end
        ORDER BY parent_name, partition_name
        """
    )
    for row in rows:
        partition_name = row["partition_name"]
        row_count = await conn.fetchval(f'SELECT count(*) FROM "{partition_name}"')
        if row_count == 0:
            await conn.execute(f'DROP TABLE IF EXISTS "{partition_name}"')


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
            await _truncate_public_tables(conn)
            await _prune_empty_out_of_window_partitions(conn)
            await _seed_test_baseline(conn)
            try:
                yield db_pool
            finally:
                await _truncate_public_tables(conn)
                await _prune_empty_out_of_window_partitions(conn)


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
