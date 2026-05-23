"""Foundation tests for the model_signal_readings sidecar table from
0038. Verifies the table accepts the four reading kinds, rejects
unknown ones, cascades on model archival, and respects RLS."""
from __future__ import annotations

import json
import os
import uuid

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction


pytestmark = pytest.mark.integration


_RLS_ROLE = "rls_test_user"


@pytest_asyncio.fixture
async def rls_pool():
    """A pool connected as a non-superuser, non-BYPASSRLS role so the
    RLS policy actually fires (superusers/owners bypass RLS, and CI
    connects as a superuser). Only the cross-tenant RLS test needs this;
    the rest of the suite uses the package's superuser `db_pool`."""
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set")
    admin = await asyncpg.create_pool(dsn, min_size=1, max_size=1)
    try:
        async with admin.acquire() as c:
            await c.execute(
                f"""
                DO $$ BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM pg_roles WHERE rolname = '{_RLS_ROLE}'
                  ) THEN
                    CREATE ROLE {_RLS_ROLE} LOGIN PASSWORD '{_RLS_ROLE}'
                      NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE;
                  END IF;
                END $$;
                """
            )
            await c.execute(f"GRANT USAGE ON SCHEMA public TO {_RLS_ROLE}")
            await c.execute(
                "GRANT SELECT, INSERT, UPDATE, DELETE "
                f"ON ALL TABLES IN SCHEMA public TO {_RLS_ROLE}"
            )
            await c.execute(
                "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public "
                f"TO {_RLS_ROLE}"
            )
    finally:
        await admin.close()
    pool = await asyncpg.create_pool(
        dsn, user=_RLS_ROLE, password=_RLS_ROLE, min_size=1, max_size=3
    )
    try:
        yield pool
    finally:
        await pool.close()


async def _seed_model(
    conn: asyncpg.Connection,
    tenant: uuid.UUID,
) -> uuid.UUID:
    """Minimal model + dependencies for the sidecar FK to be satisfied."""
    actor_id = uuid7()
    obs_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status, created_at)
        VALUES ($1, $2, 'human_internal', 'sr-test', 'active', now())
        """,
        actor_id, tenant,
    )
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text, embedding,
            embedding_pending, trust_tier, external_id, entities_mentioned
        ) VALUES (
            $1, $2, now(), 'signal', 'sr:t', $3, '{}'::jsonb, 'sr',
            NULL, TRUE, 'authoritative', $4, '[]'::jsonb
        )
        """,
        obs_id, tenant, actor_id, f"sr-ext-{obs_id}",
    )
    mid = uuid7()
    await conn.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, falsifier, signal_readings,
            supporting_event_ids, supporting_model_ids,
            contributing_models, status, confidence_at_assertion
        ) VALUES (
            $1, $2, $3,
            '{"kind":"state","subject":"x","assertion":"y"}'::jsonb,
            'sr-model', $4,
            '{}'::uuid[], '[]'::jsonb,
            '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
            0.6, NULL, '[]'::jsonb,
            '{}'::uuid[], '{}'::uuid[],
            '{}'::uuid[], 'active', 0.6
        )
        """,
        mid, tenant, obs_id, [0.0] * 768,
    )
    return mid


async def _insert_reading(
    conn: asyncpg.Connection,
    *,
    model_id: uuid.UUID,
    tenant: uuid.UUID,
    reading_kind: str,
    detail: dict | None = None,
    source_event_id: uuid.UUID | None = None,
) -> uuid.UUID:
    rid = uuid7()
    await conn.execute(
        """
        INSERT INTO model_signal_readings (
            id, model_id, tenant_id, reading_kind,
            source_event_id, detail
        ) VALUES (
            $1, $2, $3, $4, $5, $6::jsonb
        )
        """,
        rid, model_id, tenant, reading_kind,
        source_event_id, json.dumps(detail or {}),
    )
    return rid


# =====================================================================
# Schema
# =====================================================================

@pytest.mark.asyncio
async def test_accepts_all_four_reading_kinds(tx_conn: asyncpg.Connection):
    tenant = uuid7()
    await tx_conn.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'sr') ON CONFLICT DO NOTHING",
        tenant,
    )
    mid = await _seed_model(tx_conn, tenant)
    for kind in ("confirm", "contest", "observe", "falsify"):
        await _insert_reading(
            tx_conn, model_id=mid, tenant=tenant, reading_kind=kind,
        )


@pytest.mark.asyncio
async def test_rejects_unknown_reading_kind(tx_conn: asyncpg.Connection):
    tenant = uuid7()
    await tx_conn.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'sr') ON CONFLICT DO NOTHING",
        tenant,
    )
    mid = await _seed_model(tx_conn, tenant)
    with pytest.raises(asyncpg.CheckViolationError):
        await _insert_reading(
            tx_conn, model_id=mid, tenant=tenant, reading_kind="bogus",
        )


@pytest.mark.asyncio
async def test_observed_at_defaults_to_now(tx_conn: asyncpg.Connection):
    tenant = uuid7()
    await tx_conn.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'sr') ON CONFLICT DO NOTHING",
        tenant,
    )
    mid = await _seed_model(tx_conn, tenant)
    rid = await _insert_reading(
        tx_conn, model_id=mid, tenant=tenant, reading_kind="confirm",
    )
    observed_at = await tx_conn.fetchval(
        "SELECT observed_at FROM model_signal_readings WHERE id = $1", rid,
    )
    assert observed_at is not None


# =====================================================================
# Cascade
# =====================================================================

@pytest.mark.asyncio
async def test_readings_cascade_when_model_deleted(tx_conn: asyncpg.Connection):
    tenant = uuid7()
    await tx_conn.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'sr') ON CONFLICT DO NOTHING",
        tenant,
    )
    mid = await _seed_model(tx_conn, tenant)
    await _insert_reading(
        tx_conn, model_id=mid, tenant=tenant, reading_kind="confirm",
    )
    await _insert_reading(
        tx_conn, model_id=mid, tenant=tenant, reading_kind="contest",
    )
    # CASCADE on model delete.
    await tx_conn.execute(
        "DELETE FROM models WHERE id = $1", mid,
    )
    count = await tx_conn.fetchval(
        "SELECT count(*) FROM model_signal_readings WHERE model_id = $1", mid,
    )
    assert count == 0


# =====================================================================
# RLS
# =====================================================================

@pytest.mark.asyncio
async def test_rls_blocks_cross_tenant_select(rls_pool: asyncpg.Pool):
    from pgvector.asyncpg import register_vector

    tenant_a = uuid7()
    tenant_b = uuid7()
    async with rls_pool.acquire() as conn:
        try:
            await register_vector(conn)
        except Exception:
            pass
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO tenants (id, name) VALUES ($1, 'a'), ($2, 'b') "
                "ON CONFLICT DO NOTHING",
                tenant_a, tenant_b,
            )
            mid_a = await _seed_model(conn, tenant_a)
            mid_b = await _seed_model(conn, tenant_b)
            await _insert_reading(
                conn, model_id=mid_a, tenant=tenant_a, reading_kind="confirm",
            )
            await _insert_reading(
                conn, model_id=mid_b, tenant=tenant_b, reading_kind="contest",
            )

    async with tenant_transaction(tenant_a, pool=rls_pool) as tctx:
        rows = await tctx.fetch(
            "SELECT reading_kind FROM model_signal_readings WHERE model_id = $1",
            mid_a,
        )
        kinds_a = [r["reading_kind"] for r in rows]
        assert "confirm" in kinds_a

        # Looking for tenant_b's reading by id should return nothing.
        rows = await tctx.fetch(
            "SELECT reading_kind FROM model_signal_readings WHERE model_id = $1",
            mid_b,
        )
        assert rows == []
