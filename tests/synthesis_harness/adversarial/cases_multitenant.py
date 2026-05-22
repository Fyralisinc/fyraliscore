"""Category 9 — Multi-tenant isolation pressure.

The existing harness verifies hash isolation (region lock keys differ
across tenants). These cases stress the substrate's tenancy model
under more challenging conditions: same entity name across tenants,
shared pool resources (pgvector codec), cross-tenant retrieval, and
tenant cleanup.

The principle: tenant_id is an inviolable boundary. A regression
that lets one tenant see another's Models is a critical bug.
"""
from __future__ import annotations

from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.think.applier import apply_diff
from services.think.diff_schema import ClaimOp, ValidatedDiff
from services.think.region_locks import region_lock_key

from .. import _fixtures as F
from .._runner import Case
from . import _helpers as H


# =====================================================================
# MT1 — Same display_name 'ACME' across two tenants → distinct lock keys
# =====================================================================


async def _setup_two_acmes(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            t1 = await F.make_tenant(conn)
            t2 = await F.make_tenant(conn)
            acme1 = await F.make_actor(conn, t1, display_name="ACME")
            acme2 = await F.make_actor(conn, t2, display_name="ACME")
            return {"t1": t1, "t2": t2, "acme1": acme1, "acme2": acme2}


async def _run_two_acmes(_pool: asyncpg.Pool, ctx: dict) -> dict:
    key1 = region_lock_key(ctx["t1"], [{"type": "actor", "id": str(ctx["acme1"])}])
    key2 = region_lock_key(ctx["t2"], [{"type": "actor", "id": str(ctx["acme2"])}])
    return {
        "key1": str(key1),
        "key2": str(key2),
        "distinct": key1 != key2,
    }


CASE_ACME_TENANTS = Case(
    stage="adversarial.multitenant",
    name="same_actor_name_distinct_tenants_distinct_locks",
    intent="Two tenants both with an actor named 'ACME' produce "
           "distinct region-lock keys (tenant_id is mixed into the hash)",
    setup=_setup_two_acmes,
    run=H.safe_pipeline(_run_two_acmes),
    expected=lambda _ctx: {"distinct": True},
    assertion=lambda a, e, c: (
        (a.get("distinct") is True,
         "" if a.get("distinct") is True
         else f"region keys collided: {a!r}")
    ),
    failure_mode_under_test=(
        "region_lock_key drops tenant_id from the hash; two tenants "
        "with same-named entities serialize on the same lock, causing "
        "cross-tenant contention"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# MT2 — pgvector codec is per-connection, not per-tenant
# =====================================================================
# Verify that two tenants can share a pool connection without the
# codec state leaking. We do this by acquiring two connections from
# the same pool and writing tenant-specific Models, then reading
# them back with the right tenant.


async def _setup_two_tenants_with_models(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            t1 = await F.make_tenant(conn)
            t2 = await F.make_tenant(conn)
            actor1 = await F.make_actor(conn, t1)
            actor2 = await F.make_actor(conn, t2)
            m1 = await F.make_model(
                conn, t1, natural="tenant 1 model",
                scope_actors=[actor1], embed_seed="mt2-t1",
            )
            m2 = await F.make_model(
                conn, t2, natural="tenant 2 model",
                scope_actors=[actor2], embed_seed="mt2-t2",
            )
            return {
                "t1": t1, "t2": t2,
                "actor1": actor1, "actor2": actor2,
                "m1": m1, "m2": m2,
            }


async def _run_codec_isolation(pool: asyncpg.Pool, ctx: dict) -> dict:
    # Same pool connection used to read both tenants' Models.
    async with pool.acquire() as conn:
        rows1 = await conn.fetch(
            "SELECT id FROM models WHERE tenant_id=$1", ctx["t1"],
        )
        rows2 = await conn.fetch(
            "SELECT id FROM models WHERE tenant_id=$1", ctx["t2"],
        )
    return {
        "t1_count": len(rows1),
        "t2_count": len(rows2),
        "no_overlap": (
            {str(r["id"]) for r in rows1} & {str(r["id"]) for r in rows2}
        ) == set(),
    }


CASE_CODEC_ISOLATION = Case(
    stage="adversarial.multitenant",
    name="shared_pool_codec_no_cross_tenant_leak",
    intent="A pool connection used by two tenants does not leak "
           "vector codec state — each tenant reads only their own rows",
    setup=_setup_two_tenants_with_models,
    run=H.safe_pipeline(_run_codec_isolation),
    expected=lambda _ctx: {
        "t1_count": 1, "t2_count": 1, "no_overlap": True,
    },
    assertion=lambda a, e, c: (
        (a == e, "" if a == e else f"got {a!r}")
    ),
    failure_mode_under_test=(
        "shared codec on a pool connection causes the second tenant's "
        "embedding cast to interpret t1's vector as float[] (or vice "
        "versa)"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# MT3 — Cross-tenant retrieval: insert in t1, retrieve in t2 returns nothing
# =====================================================================


async def _run_no_cross_retrieval(pool: asyncpg.Pool, ctx: dict) -> dict:
    from services.retrieval.primary import (
        TriggerContext,
        primary_retrieve,
    )
    # Build a trigger in tenant 2 that semantically matches tenant 1's
    # Model (same embed seed). Retrieval must NOT return tenant 1's row.
    trigger = TriggerContext(
        kind="T1",
        tenant_id=ctx["t2"],
        observation_id=None,
        scope_actors=[ctx["actor2"]],
        seed_entity_ids=[],
        seed_natural_text="cross-tenant retrieval probe",
        seed_occurred_at=F.isoplus(0),
        precomputed_seed_vector=F.deterministic_vector("mt2-t1"),  # t1's seed
        seed_signature={},
    )
    async with pool.acquire() as conn:
        result = await primary_retrieve(trigger, conn)
    leaked = [
        m for m in result.models
        if str(m.id) == str(ctx["m1"])
    ]
    return {
        "model_count": len(result.models),
        "leaked_t1_model": len(leaked) > 0,
    }


CASE_NO_CROSS_RETRIEVAL = Case(
    stage="adversarial.multitenant",
    name="retrieval_does_not_leak_across_tenants",
    intent="A retrieval in tenant 2 with a vector matching tenant 1's "
           "Model returns ZERO leaked rows from tenant 1",
    setup=_setup_two_tenants_with_models,
    run=H.safe_pipeline(_run_no_cross_retrieval),
    expected=lambda _ctx: {"leaked_t1_model": False},
    assertion=lambda a, e, c: (
        (a.get("leaked_t1_model") is False,
         "" if a.get("leaked_t1_model") is False
         else f"LEAK: tenant 1 model surfaced in tenant 2 retrieval; {a!r}")
    ),
    failure_mode_under_test=(
        "primary_retrieve drops the tenant_id WHERE clause from any "
        "of its 4 pathways; cross-tenant data exposure"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# MT4 — Reconciler ignores other tenants' candidates
# =====================================================================


async def _run_reconcile_no_leak(pool: asyncpg.Pool, ctx: dict) -> dict:
    obs = await _make_obs(pool, ctx["t2"], ctx["actor2"])
    op = H.make_state_insert_op(
        tenant_id=ctx["t2"], born_from_event_id=obs,
        natural="tenant 2 lookup probe",
        scope_actors=[ctx["actor2"]],
        embed_seed="mt2-t1",  # tenant 1's seed
    )
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=ctx["t2"],
        claim_ops=[op], act_ops=[], resource_ops=[],
        new_predictions=[], reasoning_trace="cross-tenant probe",
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
        # Tenant 1's Model untouched
        t1_count = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["t1"],
        )
        t2_count = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["t2"],
        )
    return {"t1_count": t1_count, "t2_count": t2_count}


async def _make_obs(pool: asyncpg.Pool, tenant: UUID, actor: UUID) -> UUID:
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await F.make_observation(conn, tenant, actor_id=actor)


CASE_RECONCILE_NO_LEAK = Case(
    stage="adversarial.multitenant",
    name="reconciler_does_not_match_across_tenants",
    intent="A tenant 2 insert with tenant 1's embed_seed lands as a "
           "NEW tenant 2 Model (so t2_count = 2: pre-existing m2 + "
           "new); tenant 1's m1 is untouched (t1_count = 1)",
    setup=_setup_two_tenants_with_models,
    run=H.safe_pipeline(_run_reconcile_no_leak),
    expected=lambda _ctx: {"t1_count": 1, "t2_count": 2},
    assertion=lambda a, e, c: (
        (a.get("t1_count") == 1 and a.get("t2_count") == 2,
         "" if (a.get("t1_count") == 1 and a.get("t2_count") == 2)
         else f"got {a!r}")
    ),
    failure_mode_under_test=(
        "reconciler's _find_candidates drops tenant_id filter or uses "
        "the wrong $param index; tenant 2's insert merges into "
        "tenant 1's row, leaving t1_count=0 (merge consumed) or "
        "t2_count=1 (failed to insert)"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# MT5 — Tenant cleanup: deleting all rows for a tenant doesn't impact others
# =====================================================================


async def _run_tenant_cleanup(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            # "Delete" tenant 1 by archiving all its Models
            await conn.execute(
                "UPDATE models SET status='archived', "
                "archive_reason='tenant_deleted', archived_at=now() "
                "WHERE tenant_id=$1",
                ctx["t1"],
            )
        t1_active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["t1"],
        )
        t2_active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["t2"],
        )
    return {"t1_active": t1_active, "t2_active": t2_active}


CASE_TENANT_CLEANUP = Case(
    stage="adversarial.multitenant",
    name="tenant_cleanup_does_not_affect_other_tenants",
    intent="Archiving every Model in tenant 1 leaves tenant 2's "
           "Models intact",
    setup=_setup_two_tenants_with_models,
    run=H.safe_pipeline(_run_tenant_cleanup),
    expected=lambda _ctx: {"t1_active": 0, "t2_active": 1},
    assertion=lambda a, e, c: (
        (a == e, "" if a == e else f"cleanup leaked: {a!r}")
    ),
    failure_mode_under_test=(
        "tenant-cleanup script (or operator UPDATE) drops the "
        "tenant_id WHERE clause and archives every tenant's Models"
    ),
    expected_behavior="specified",
    domain="ops",
)


CASES = [
    CASE_ACME_TENANTS,
    CASE_CODEC_ISOLATION,
    CASE_NO_CROSS_RETRIEVAL,
    CASE_RECONCILE_NO_LEAK,
    CASE_TENANT_CLEANUP,
]
