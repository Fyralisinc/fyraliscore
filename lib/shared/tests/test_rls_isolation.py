"""End-to-end tests of the RLS permissive-default policy from migration
0036. Verifies that:

  - Without app.current_tenant set, queries see all rows (permissive default).
  - With app.current_tenant set via tenant_transaction, queries see ONLY
    rows for that tenant.
  - INSERT into a tenant other than current is rejected.
  - The policy applies to every covered table — sample a representative
    handful to keep test runtime sane.

These tests are the load-bearing proof that the migration's intent is
realized at the database, not just on paper."""
from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from lib.shared.tenant_context import bind_tenant, tenant_transaction


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


async def _seed_actor(conn: asyncpg.Connection, tenant: UUID, name: str) -> UUID:
    # Migration 0037 added FK actors.tenant_id -> tenants(id). Make
    # sure the tenant exists before the insert.
    await conn.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        tenant, f"rls_test_{tenant}",
    )
    aid = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status, created_at)
        VALUES ($1, $2, 'human_internal', $3, 'active', now())
        """,
        aid, tenant, name,
    )
    return aid


# ---------------------------------------------------------------------
# Permissive default: no tenant set → see everything
# ---------------------------------------------------------------------

async def test_no_tenant_set_sees_all_rows(db_pool: asyncpg.Pool):
    tenant_a = uuid7()
    tenant_b = uuid7()
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await _seed_actor(conn, tenant_a, "rls-perm-a")
            await _seed_actor(conn, tenant_b, "rls-perm-b")
            # Two distinct tenants, both visible from outside any RLS context.
            seen = await conn.fetch(
                "SELECT tenant_id FROM actors WHERE tenant_id = ANY($1::uuid[])",
                [tenant_a, tenant_b],
            )
            tenants = {r["tenant_id"] for r in seen}
            assert tenants == {tenant_a, tenant_b}


# ---------------------------------------------------------------------
# With tenant set → only that tenant's rows are visible
# ---------------------------------------------------------------------

async def test_with_tenant_set_sees_only_own_rows(db_pool: asyncpg.Pool):
    tenant_a = uuid7()
    tenant_b = uuid7()
    # Pre-seed both tenants from outside any RLS context.
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await _seed_actor(conn, tenant_a, "rls-iso-a")
            await _seed_actor(conn, tenant_b, "rls-iso-b")

    # Now read from inside tenant_a's context.
    async with tenant_transaction(tenant_a, pool=db_pool) as tctx:
        rows = await tctx.fetch(
            "SELECT display_name FROM actors WHERE display_name LIKE 'rls-iso-%'"
        )
        names = {r["display_name"] for r in rows}
        assert names == {"rls-iso-a"}

    # And from tenant_b's context.
    async with tenant_transaction(tenant_b, pool=db_pool) as tctx:
        rows = await tctx.fetch(
            "SELECT display_name FROM actors WHERE display_name LIKE 'rls-iso-%'"
        )
        names = {r["display_name"] for r in rows}
        assert names == {"rls-iso-b"}


# ---------------------------------------------------------------------
# WITH CHECK rejects INSERT into the wrong tenant
# ---------------------------------------------------------------------

async def test_insert_with_wrong_tenant_rejected(db_pool: asyncpg.Pool):
    tenant_a = uuid7()
    tenant_b = uuid7()
    # Pre-register both tenants so the only failure mode tested is the
    # RLS WITH CHECK clause, not the tenants FK.
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'a'), ($2, 'b') "
            "ON CONFLICT DO NOTHING",
            tenant_a, tenant_b,
        )
    async with tenant_transaction(tenant_a, pool=db_pool) as tctx:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await tctx.execute(
                """
                INSERT INTO actors (id, tenant_id, type, display_name, status, created_at)
                VALUES ($1, $2, 'human_internal', 'wrong-tenant', 'active', now())
                """,
                uuid7(), tenant_b,  # mismatch — current is A, insert is B
            )


async def test_insert_with_correct_tenant_succeeds(db_pool: asyncpg.Pool):
    tenant_a = uuid7()
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'rls-correct') "
            "ON CONFLICT DO NOTHING",
            tenant_a,
        )
    async with tenant_transaction(tenant_a, pool=db_pool) as tctx:
        await tctx.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status, created_at)
            VALUES ($1, $2, 'human_internal', 'rls-correct-tenant', 'active', now())
            """,
            uuid7(), tenant_a,
        )


# ---------------------------------------------------------------------
# Coverage spot check — RLS applied to multiple covered tables
# ---------------------------------------------------------------------

async def test_rls_enabled_on_models(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        relrowsecurity = await conn.fetchval(
            "SELECT relrowsecurity FROM pg_class WHERE relname = 'models'"
        )
        assert relrowsecurity is True


async def test_rls_enabled_on_observations(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        relrowsecurity = await conn.fetchval(
            "SELECT relrowsecurity FROM pg_class WHERE relname = 'observations'"
        )
        assert relrowsecurity is True


async def test_rls_enabled_on_topology_events(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        relrowsecurity = await conn.fetchval(
            "SELECT relrowsecurity FROM pg_class WHERE relname = 'topology_events'"
        )
        assert relrowsecurity is True


async def test_policy_named_correctly(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        policy_name = await conn.fetchval(
            """
            SELECT polname FROM pg_policy
            WHERE polrelid = 'actors'::regclass
            """
        )
        assert policy_name == "tenant_isolation"


# ---------------------------------------------------------------------
# Cross-tenant leak attempt is blocked
# ---------------------------------------------------------------------

async def test_cross_tenant_select_blocked_via_rls(db_pool: asyncpg.Pool):
    """A bug that forgets `WHERE tenant_id = $1` in tenant_a's context
    must not leak tenant_b's rows."""
    tenant_a = uuid7()
    tenant_b = uuid7()
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await _seed_actor(conn, tenant_a, "leak-test-a")
            await _seed_actor(conn, tenant_b, "leak-test-b-secret")

    async with tenant_transaction(tenant_a, pool=db_pool) as tctx:
        # Buggy query: no WHERE tenant_id
        rows = await tctx.fetch(
            "SELECT display_name FROM actors WHERE display_name LIKE 'leak-test-%'"
        )
        names = {r["display_name"] for r in rows}
        # tenant_b's row must not appear, even though the query has no tenant filter.
        assert "leak-test-b-secret" not in names
        assert "leak-test-a" in names


# ---------------------------------------------------------------------
# bind_tenant produces the same isolation
# ---------------------------------------------------------------------

async def test_bind_tenant_enforces_isolation(db_pool: asyncpg.Pool):
    tenant_a = uuid7()
    tenant_b = uuid7()
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await _seed_actor(conn, tenant_a, "bind-iso-a")
            await _seed_actor(conn, tenant_b, "bind-iso-b")

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_a) as tctx:
                rows = await tctx.fetch(
                    "SELECT display_name FROM actors WHERE display_name LIKE 'bind-iso-%'"
                )
                names = {r["display_name"] for r in rows}
                assert names == {"bind-iso-a"}
