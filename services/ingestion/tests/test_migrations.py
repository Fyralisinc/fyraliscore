"""Gate tests for ingestion migrations 0045-0050 (M1.1).

Verifies:
- All six new migrations apply against a fresh DB, are idempotent on
  re-apply (the project's "rollback" surrogate — see M1 sub-block 1.1
  decision; there is no down-migration infrastructure to test against).
- `onboarding_shards.onboarding_run_id` FK cascades on parent delete.
- RLS policies on `onboarding_runs` isolate rows by `app.current_tenant`.
- The functional index from LLD §1.6 is used by the batched alias query.

Spec: docs/ingestion/03-low-level-design.md §1.1-§1.7.
"""
from __future__ import annotations

import json
import pathlib
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migration


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
MIGRATIONS_DIR = REPO_ROOT / "db" / "migrations"

# The six migrations under test. Order matters: 0045 must precede 0046
# (FK), and 0049 is the CONCURRENTLY one which exercises the runner's
# non-transactional code path.
M1_MIGRATIONS = [
    "0045_onboarding_runs_and_shards.sql",
    "0046_ingestion_failures.sql",
    "0047_onboarding_triggers_outbox.sql",
    "0048_gateway_session_state.sql",
    "0049_entity_aliases_normalized_index.sql",
    "0050_tenant_flags.sql",
]


async def _seed_tenant(conn: asyncpg.Connection) -> UUID:
    """Insert a `tenants` row (FK target for the new tables) and
    return its id. The `db_pool` fixture's TRUNCATE between tests
    cleans up.
    """
    tenant_id = uuid7()
    await conn.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) "
        "ON CONFLICT (id) DO NOTHING",
        tenant_id,
        f"test-{tenant_id}",
    )
    return tenant_id


# ---------------------------------------------------------------------
# Idempotency — the project's substitute for forward/rollback/forward.
# `db_pool` runs migrations once via apply_migrations_dir; we then
# re-apply each M1 migration manually and assert no errors. Every
# migration uses IF NOT EXISTS / DROP POLICY IF EXISTS, so this is the
# integrity check that re-running the runner is safe.
# ---------------------------------------------------------------------

async def test_m1_migrations_idempotent_on_reapply(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        for fname in M1_MIGRATIONS:
            sql = (MIGRATIONS_DIR / fname).read_text()
            # Re-apply directly through the runner. If any DDL lacks
            # IF NOT EXISTS / IF EXISTS guards, this raises.
            await apply_migration(conn, sql, name=fname)


# ---------------------------------------------------------------------
# Schema presence — each migration's tables must exist after the
# fixture's migration pass.
# ---------------------------------------------------------------------

@pytest.mark.parametrize(
    "table",
    [
        "onboarding_runs",
        "onboarding_shards",
        "ingestion_failures",
        "onboarding_triggers",
        "gateway_session_state",
        "tenant_flags",
    ],
)
async def test_m1_table_exists(db_pool: asyncpg.Pool, table: str):
    async with db_pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT to_regclass($1) IS NOT NULL",
            f"public.{table}",
        )
        assert exists is True, f"table {table} not created by 0045-0050"


# ---------------------------------------------------------------------
# §1.2 — onboarding_shards FK cascade.
# ---------------------------------------------------------------------

async def test_onboarding_shards_fk_cascade(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        tenant_id = await _seed_tenant(conn)
        run_id = uuid7()
        await conn.execute(
            """
            INSERT INTO onboarding_runs (
                id, tenant_id, trigger_kind, workflow_id, sources_enabled
            ) VALUES ($1, $2, 'install', $3, $4)
            """,
            run_id, tenant_id, f"wf-{run_id}", ["slack"],
        )
        shard_id = uuid7()
        await conn.execute(
            """
            INSERT INTO onboarding_shards (
                id, onboarding_run_id, tenant_id, source,
                shard_kind, shard_identifier, recency_score
            ) VALUES ($1, $2, $3, 'slack', 'channel', $4::jsonb, 1.0)
            """,
            shard_id, run_id, tenant_id, json.dumps({"channel_id": "C0001"}),
        )
        # Sanity: the shard is there.
        assert await conn.fetchval(
            "SELECT 1 FROM onboarding_shards WHERE id = $1", shard_id
        )
        # Delete the parent run; the shard must go with it.
        await conn.execute("DELETE FROM onboarding_runs WHERE id = $1", run_id)
        assert await conn.fetchval(
            "SELECT 1 FROM onboarding_shards WHERE id = $1", shard_id
        ) is None


# ---------------------------------------------------------------------
# §1.1 — RLS isolation on onboarding_runs.
#
# Structure check: every M1 table that should be tenant-scoped has
# ENABLE + FORCE row-level security and a `tenant_isolation` policy.
# This is what the migration controls and must pass everywhere.
#
# Behaviour check (separate test): the policy actually blocks
# cross-tenant reads. Postgres bypasses RLS for SUPERUSER /
# BYPASSRLS roles regardless of `FORCE`; the local dev DB's
# `company_os` user has both attributes, so the behavioural test is
# skipped there (matches the same skip pattern that affects the
# pre-existing lib/shared/tests/test_rls_isolation.py suite). CI is
# expected to use a non-superuser role.
# ---------------------------------------------------------------------

@pytest.mark.parametrize(
    "table",
    [
        "onboarding_runs",
        "onboarding_shards",
        "ingestion_failures",
    ],
)
async def test_rls_enabled_and_forced(db_pool: asyncpg.Pool, table: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT relrowsecurity, relforcerowsecurity "
            "FROM pg_class WHERE relname = $1",
            table,
        )
        assert row["relrowsecurity"] is True, f"RLS not enabled on {table}"
        assert row["relforcerowsecurity"] is True, (
            f"FORCE RLS not set on {table}"
        )
        policy = await conn.fetchval(
            "SELECT polname FROM pg_policy "
            "WHERE polrelid = $1::regclass AND polname = 'tenant_isolation'",
            table,
        )
        assert policy == "tenant_isolation", (
            f"tenant_isolation policy missing on {table}"
        )


async def test_rls_policy_isolates_by_tenant(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        is_super = await conn.fetchval(
            "SELECT usesuper OR usebypassrls FROM pg_user "
            "WHERE usename = current_user"
        )
        if is_super:
            pytest.skip(
                "Connecting role is SUPERUSER/BYPASSRLS — Postgres bypasses "
                "RLS regardless of FORCE. Same skip applies to "
                "lib/shared/tests/test_rls_isolation.py in this dev env; "
                "CI runs as a non-super role."
            )

        tenant_a = await _seed_tenant(conn)
        tenant_b = await _seed_tenant(conn)
        run_a = uuid7()
        # Insert tenant_a's row without `app.current_tenant` set —
        # the permissive policy default allows it.
        await conn.execute(
            """
            INSERT INTO onboarding_runs (
                id, tenant_id, trigger_kind, workflow_id, sources_enabled
            ) VALUES ($1, $2, 'install', $3, $4)
            """,
            run_a, tenant_a, f"wf-rls-{run_a}", ["slack"],
        )

        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(tenant_b),
            )
            visible = await conn.fetch(
                "SELECT id FROM onboarding_runs WHERE id = $1", run_a
            )
            assert visible == [], (
                "RLS leak: tenant_b sees tenant_a's onboarding_runs row"
            )

        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(tenant_a),
            )
            visible = await conn.fetch(
                "SELECT id FROM onboarding_runs WHERE id = $1", run_a
            )
            assert len(visible) == 1


# ---------------------------------------------------------------------
# §1.6 — functional index for batched alias lookup. Gate: the LLD's
# canonical query plan must name `entity_aliases_normalized_idx`. If
# this fails, 0049 is wrong: the expression does not match the one
# `EntityAliasRepo` issues.
# ---------------------------------------------------------------------

async def test_functional_index_used_in_explain(db_pool: asyncpg.Pool):
    async with db_pool.acquire() as conn:
        # Force the planner to prefer index scans over the seq scan
        # that an empty table would otherwise choose.
        await conn.execute("SET LOCAL enable_seqscan = OFF")
        tenant_id = uuid7()
        plan = await conn.fetchval(
            r"""
            EXPLAIN (FORMAT JSON)
            SELECT actor_id FROM entity_aliases
            WHERE tenant_id = $1
              AND regexp_replace(lower(alias_text), '\s+', ' ', 'g')
                  = ANY($2::text[])
            """,
            tenant_id,
            ["acme corp", "big feature"],
        )
        plan_text = json.dumps(plan)
        assert "entity_aliases_normalized_idx" in plan_text, (
            "0049 functional index not used by the LLD §1.6 canonical "
            "query. Expression mismatch with normalize_phrase()? "
            "Plan was: " + plan_text
        )
