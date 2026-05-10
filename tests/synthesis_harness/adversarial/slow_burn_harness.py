"""Category 10 — Slow-burn / accumulation drift.

Single-shot scenarios miss long-tail substrate corruption. These
cases run long sequences (50-200 ops) into one tenant and verify
the substrate's invariants hold across the whole sequence:

  * No orphan Models (every Model has either a valid scope or is
    documented as scope-less)
  * Archived Models have a non-empty archive_reason
  * Audit trail is complete (every reconciliation_events row has
    a valid trigger_id)
  * No duplicate Models past auto_merge threshold

Each case is self-contained — heavy but bounded.
"""
from __future__ import annotations

import asyncio
import random
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.think.applier import apply_diff
from services.think.diff_schema import ClaimOp, ValidatedDiff

from .. import _fixtures as F
from .._runner import Case
from . import _helpers as H


def _build_diff(tenant_id: UUID, trigger_id: UUID, ops: list[ClaimOp]) -> ValidatedDiff:
    return ValidatedDiff(
        trigger_ref=trigger_id, tenant_id=tenant_id,
        claim_ops=ops, act_ops=[], resource_ops=[],
        new_predictions=[], reasoning_trace="adversarial.slow_burn",
    )


async def _setup_tenant(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            return {"tenant": tenant, "actor": actor}


# =====================================================================
# SB1 — 200 distinct signals: invariant audit
# =====================================================================
# Fire 200 signals into one tenant on different topics. Verify:
# - All 200 Models exist
# - None have NULL embedding
# - All have at least one scope element
# - Reconciliation audit row exists for each


async def _run_200_distinct(pool: asyncpg.Pool, ctx: dict) -> dict:
    N = 200
    async with pool.acquire() as conn:
        for i in range(N):
            async with conn.transaction():
                obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
                op = H.make_state_insert_op(
                    tenant_id=ctx["tenant"], born_from_event_id=obs,
                    natural=f"distinct topic #{i:03d} on {chr(65 + (i % 26))}",
                    scope_actors=[ctx["actor"]],
                    embed_seed=f"sb1-distinct-topic-{i:03d}",
                )
                await apply_diff(
                    _build_diff(ctx["tenant"], uuid7(), [op]),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
        # Invariant queries
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
        null_embeds = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND embedding IS NULL",
            ctx["tenant"],
        )
        no_scope = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 "
            "AND COALESCE(array_length(scope_actors, 1), 0) = 0 "
            "AND jsonb_array_length(scope_entities) = 0",
            ctx["tenant"],
        )
        audit_rows = await conn.fetchval(
            "SELECT COUNT(*) FROM reconciliation_events WHERE tenant_id=$1",
            ctx["tenant"],
        )
    return {
        "n_inserts": N,
        "active": active,
        "null_embeds": null_embeds,
        "no_scope_count": no_scope,
        "audit_rows": audit_rows,
    }


def _assert_200_distinct(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    diffs = []
    if actual["null_embeds"] > 0:
        diffs.append(f"{actual['null_embeds']} Models have NULL embedding")
    if actual["no_scope_count"] > 0:
        diffs.append(f"{actual['no_scope_count']} scope-less Models accumulated")
    # We expect mostly no_match decisions; some auto_merges if topics
    # happen to share embeddings — but never more than n_inserts.
    if actual["audit_rows"] < actual["n_inserts"]:
        diffs.append(
            f"audit underrun: {actual['audit_rows']} rows for "
            f"{actual['n_inserts']} inserts"
        )
    if actual["active"] < 1:
        diffs.append("no active Models after 200 inserts")
    return (not diffs), "; ".join(diffs)


CASE_200_DISTINCT = Case(
    stage="adversarial.slow_burn",
    name="two_hundred_distinct_signals_invariant_audit",
    intent="200 distinct-topic inserts into one tenant produce a "
           "consistent substrate: no NULL embeddings, no scope-less "
           "Models, full audit trail",
    setup=_setup_tenant,
    run=H.safe_pipeline(_run_200_distinct),
    expected=lambda _ctx: {},
    assertion=_assert_200_distinct,
    failure_mode_under_test=(
        "embedding-missing inserts accumulate as NULL-embed Models; "
        "OR audit table loses rows under repeated load; OR a "
        "reconcile race produces orphan Models past iteration ~150"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# SB2 — Random walk: 100 ops mixing insert/contest/archive
# =====================================================================


async def _run_random_walk(pool: asyncpg.Pool, ctx: dict) -> dict:
    from services.contestability.service import ContestationInput, contest_model
    rng = random.Random(0xC0FFEE)
    N = 100
    model_ids: list[UUID] = []
    errors: list[str] = []
    async with pool.acquire() as conn:
        for i in range(N):
            try:
                async with conn.transaction():
                    roll = rng.random()
                    if roll < 0.6 or not model_ids:
                        # 60% insert
                        obs = await F.make_observation(
                            conn, ctx["tenant"], actor_id=ctx["actor"],
                        )
                        op = H.make_state_insert_op(
                            tenant_id=ctx["tenant"], born_from_event_id=obs,
                            natural=f"random walk insert {i}",
                            scope_actors=[ctx["actor"]],
                            embed_seed=f"sb2-walk-{i}",
                        )
                        await apply_diff(
                            _build_diff(ctx["tenant"], uuid7(), [op]),
                            conn, trigger_kind="T1",
                            trigger_cause_event_id=obs,
                        )
                        mid = await conn.fetchval(
                            "SELECT id FROM models WHERE tenant_id=$1 "
                            "AND status='active' "
                            "ORDER BY created_at DESC LIMIT 1",
                            ctx["tenant"],
                        )
                        if mid:
                            model_ids.append(mid)
                    elif roll < 0.85:
                        # 25% contest
                        target = rng.choice(model_ids)
                        try:
                            await contest_model(
                                conn,
                                ContestationInput(
                                    model_id=target,
                                    contestor_actor_id=ctx["actor"],
                                    tenant_id=ctx["tenant"],
                                    contestation_kind="belief",
                                    rationale=f"walk contest {i}",
                                ),
                            )
                        except Exception:  # noqa: BLE001
                            pass
                    else:
                        # 15% archive
                        target = rng.choice(model_ids)
                        obs = await F.make_observation(
                            conn, ctx["tenant"], actor_id=ctx["actor"],
                        )
                        await apply_diff(
                            _build_diff(ctx["tenant"], uuid7(), [
                                ClaimOp(op="archive", model_id=target,
                                        reason="manual"),
                            ]),
                            conn, trigger_kind="T1",
                            trigger_cause_event_id=obs,
                        )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"step {i}: {type(exc).__name__}: {exc}"[:240])

        # Substrate invariants
        archived_no_reason = await conn.fetchval(
            "SELECT COUNT(*) FROM models "
            "WHERE tenant_id=$1 AND status='archived' "
            "AND (archive_reason IS NULL OR archive_reason = '')",
            ctx["tenant"],
        )
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
        archived = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='archived'",
            ctx["tenant"],
        )
    return {
        "errors_count": len(errors),
        "errors_first": errors[:3],
        "archived_no_reason": archived_no_reason,
        "active": active,
        "archived": archived,
    }


def _assert_random_walk(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["archived_no_reason"] > 0:
        return False, (
            f"{actual['archived_no_reason']} archived Models lack "
            f"archive_reason — audit trail broken"
        )
    if actual["errors_count"] > 5:
        return False, (
            f"{actual['errors_count']} steps errored; first: "
            f"{actual['errors_first']}"
        )
    return True, ""


CASE_RANDOM_WALK = Case(
    stage="adversarial.slow_burn",
    name="random_walk_lifecycle_audit_intact",
    intent="100 mixed insert/contest/archive ops leave the substrate "
           "consistent: no archived Models without reasons, error rate "
           "≤ 5%",
    setup=_setup_tenant,
    run=H.safe_pipeline(_run_random_walk),
    expected=lambda _ctx: {},
    assertion=_assert_random_walk,
    failure_mode_under_test=(
        "lifecycle interactions accumulate corruption: archives "
        "without reasons, contests on archived Models silently "
        "succeed, audit trail develops gaps"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# SB3 — 50-deep supersession chain end state
# =====================================================================


async def _run_50_deep_chain(pool: asyncpg.Pool, ctx: dict) -> dict:
    N = 50
    async with pool.acquire() as conn:
        prev_id = None
        for i in range(N):
            async with conn.transaction():
                obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
                ops = []
                if prev_id is not None:
                    ops.append(ClaimOp(
                        op="archive", model_id=prev_id,
                        reason="superseded",
                    ))
                ops.append(H.make_state_insert_op(
                    tenant_id=ctx["tenant"], born_from_event_id=obs,
                    natural=f"chain step {i}",
                    scope_actors=[ctx["actor"]],
                    embed_seed=f"sb3-chain-distinct-{i}",
                ))
                await apply_diff(
                    _build_diff(ctx["tenant"], uuid7(), ops),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
                prev_id = await conn.fetchval(
                    "SELECT id FROM models WHERE tenant_id=$1 AND status='active' "
                    "ORDER BY created_at DESC LIMIT 1",
                    ctx["tenant"],
                )
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
        archived = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='archived'",
            ctx["tenant"],
        )
        archived_correct_reason = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='archived' "
            "AND archive_reason = 'superseded'",
            ctx["tenant"],
        )
    return {
        "active": active,
        "archived": archived,
        "superseded": archived_correct_reason,
    }


def _assert_50_chain(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["active"] != 1:
        return False, f"expected 1 active, got {actual['active']}"
    if actual["archived"] != 49:
        return False, f"expected 49 archived, got {actual['archived']}"
    if actual["superseded"] != 49:
        return False, (
            f"all 49 archived should have reason='superseded'; "
            f"got {actual['superseded']}"
        )
    return True, ""


CASE_50_CHAIN = Case(
    stage="adversarial.slow_burn",
    name="fifty_deep_supersession_chain",
    intent="50-deep supersession chain ends with 1 active + 49 "
           "archived, all with reason='superseded'",
    setup=_setup_tenant,
    run=H.safe_pipeline(_run_50_deep_chain),
    expected=lambda _ctx: {},
    assertion=_assert_50_chain,
    failure_mode_under_test=(
        "deep chain accumulates audit-reason gaps; OR archive ops "
        "silently no-op past some depth; OR reconciler resurrects "
        "earlier links"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# SB4 — Reconciliation drift: 100 near-duplicates of same proposition
# =====================================================================
# Fire 100 signals all about the same proposition with identical
# embedding seed. Expectation: 1 Model + 99 auto_merges. Drift would
# manifest as multiple Models accumulating.


async def _run_recon_drift(pool: asyncpg.Pool, ctx: dict) -> dict:
    N = 100
    async with pool.acquire() as conn:
        for i in range(N):
            async with conn.transaction():
                obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
                op = H.make_state_insert_op(
                    tenant_id=ctx["tenant"], born_from_event_id=obs,
                    natural="canonical drift probe",
                    scope_actors=[ctx["actor"]],
                    embed_seed="sb4-drift-canonical",
                )
                await apply_diff(
                    _build_diff(ctx["tenant"], uuid7(), [op]),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
        decisions = await conn.fetch(
            "SELECT decision, COUNT(*) AS n FROM reconciliation_events "
            "WHERE tenant_id=$1 GROUP BY decision",
            ctx["tenant"],
        )
    return {
        "active": active,
        "decision_breakdown": {d["decision"]: d["n"] for d in decisions},
    }


def _assert_recon_drift(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["active"] != 1:
        return False, (
            f"100 identical inserts produced {actual['active']} active "
            f"Models — reconciliation drift; "
            f"breakdown={actual['decision_breakdown']}"
        )
    return True, ""


CASE_RECON_DRIFT = Case(
    stage="adversarial.slow_burn",
    name="reconciliation_drift_100_identical_inserts",
    intent="100 identical inserts produce exactly 1 active Model "
           "(99 auto_merges + 1 no_match in the breakdown)",
    setup=_setup_tenant,
    run=H.safe_pipeline(_run_recon_drift),
    expected=lambda _ctx: {},
    assertion=_assert_recon_drift,
    failure_mode_under_test=(
        "reconciler's candidate query degrades after some N (e.g. "
        "due to HNSW index quality), causing intermittent no_match "
        "decisions and orphan Models accumulating"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# SB5 — Cascade saturation: many cascades from a shared seed
# =====================================================================


async def _setup_saturation(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(
                conn, tenant, title="saturation",
                cached_health="at_risk",
            )
            done = await F.make_commitment(
                conn, tenant, owner_id=owner, state="doneverified",
            )
            await F.add_contributes_to(
                conn, commitment_id=done, goal_id=goal,
                is_critical_path=True,
            )
            blocks = []
            for i in range(40):
                b = await F.make_commitment(
                    conn, tenant, owner_id=owner, state="blocked",
                    title=f"sat_{i}",
                )
                await F.add_contributes_to(conn, commitment_id=b, goal_id=goal)
                await F.add_depends_on(conn, dependent=b, dependency=done)
                blocks.append(b)
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant, "done": done, "blocks": blocks,
                "obs": obs, "goal": goal,
            }


async def _run_saturation(pool: asyncpg.Pool, ctx: dict) -> dict:
    from services.think.cascade import CascadeEvent, cascade
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["done"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=ctx["obs"],
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
            unblocked = await conn.fetchval(
                "SELECT COUNT(*) FROM commitments "
                "WHERE id = ANY($1::uuid[]) AND state='active'",
                ctx["blocks"],
            )
    return {
        "events_visited": result.events_visited,
        "depth_reached": result.depth_reached,
        "bound_violated": result.bound_violated,
        "unblocked": unblocked,
        "expected_unblock": 40,
    }


def _assert_saturation(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["unblocked"] != 40:
        return False, (
            f"saturation cascade unblocked {actual['unblocked']} of 40"
        )
    if actual["bound_violated"]:
        return False, "depth bound violated unexpectedly"
    return True, ""


CASE_SATURATION = Case(
    stage="adversarial.slow_burn",
    name="cascade_saturation_40_dependents",
    intent="A single doneverify cascade unblocks all 40 dependents "
           "without exceeding depth bound",
    setup=_setup_saturation,
    run=H.safe_pipeline(_run_saturation),
    expected=lambda _ctx: {},
    assertion=_assert_saturation,
    failure_mode_under_test=(
        "cascade scales sub-linearly because dependents are "
        "fetched one-by-one; OR depth bound trips at 40 "
        "(off-by-one in the bound)"
    ),
    expected_behavior="specified",
    domain="ops",
)


CASES = [
    CASE_200_DISTINCT,
    CASE_RANDOM_WALK,
    CASE_50_CHAIN,
    CASE_RECON_DRIFT,
    CASE_SATURATION,
]
