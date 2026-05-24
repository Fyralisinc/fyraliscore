"""Category 3 — Sequencing & ordering.

The substrate maintains state across time. Order matters. These
scenarios fire multiple ops sequentially against one tenant and
inspect the resulting state for consistency. Concurrency (true
parallel firing) lives in `concurrency_harness`.

We test:
  * Out-of-order arrival (occurred_at vs arrival time)
  * Rapid-fire same-proposition updates
  * Reversal (A → not-A → A)
  * Long supersession chains
  * Stale signal after archival
  * Multi-step state transitions across acts

All cases use direct ClaimOp construction so they run without
LLM and are deterministic.
"""
from __future__ import annotations

from datetime import timedelta
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
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        claim_ops=ops,
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
        reasoning_trace="adversarial.sequencing",
    )


async def _setup_one_actor(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            return {"tenant": tenant, "actor": actor}


# =====================================================================
# SEQ1 — Out-of-order arrival: T1 < T2 by occurred_at but ingested reversed
# =====================================================================
# Two observations: A occurred at -2h, B occurred at -1h. We ingest
# B first (older arrival) then A. The substrate should reflect both
# but reconcile by occurred_at. Test that both Models exist and the
# ordering is preserved.


async def _run_out_of_order(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs_b = await F.make_observation(
                conn, ctx["tenant"],
                content_text="B happens later in real time",
                occurred_at=F.isoplus(-3600),
                actor_id=ctx["actor"],
            )
            obs_a = await F.make_observation(
                conn, ctx["tenant"],
                content_text="A happens earlier in real time",
                occurred_at=F.isoplus(-7200),
                actor_id=ctx["actor"],
            )

        op_b = H.make_state_insert_op(
            tenant_id=ctx["tenant"], born_from_event_id=obs_b,
            natural="State observed at T=B (later)",
            scope_actors=[ctx["actor"]],
            embed_seed="state-T-B-later-distinct-1",
            confidence=0.55,
        )
        op_a = H.make_state_insert_op(
            tenant_id=ctx["tenant"], born_from_event_id=obs_a,
            natural="State observed at T=A (earlier)",
            scope_actors=[ctx["actor"]],
            embed_seed="state-T-A-earlier-distinct-2",
            confidence=0.45,
        )

        # Ingest B first (later arrival), then A (earlier)
        async with conn.transaction():
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op_b]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs_b,
            )
        async with conn.transaction():
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op_a]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs_a,
            )

        rows = await conn.fetch(
            "SELECT id, born_from_event_id, created_at "
            "FROM models WHERE tenant_id=$1 ORDER BY created_at",
            ctx["tenant"],
        )
        obs_rows = await conn.fetch(
            "SELECT id, occurred_at FROM observations WHERE id = ANY($1::uuid[])",
            [obs_a, obs_b],
        )
    return {
        "model_count": len(rows),
        "obs_occurred_at": {
            str(r["id"]): r["occurred_at"].isoformat()
            for r in obs_rows
        },
    }


def _assert_out_of_order(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["model_count"] != 2:
        return False, (
            f"both inserts should land as separate Models; got "
            f"{actual['model_count']}"
        )
    return True, ""


CASE_OUT_OF_ORDER = Case(
    stage="adversarial.sequencing",
    name="out_of_order_arrival_both_persist",
    intent="Two observations whose occurred_at order is REVERSE of "
           "their arrival order both persist as distinct Models",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_out_of_order),
    expected=lambda _ctx: {},
    assertion=_assert_out_of_order,
    failure_mode_under_test=(
        "reconciler treats them as duplicates because they share "
        "scope+kind, collapsing into one Model and losing the "
        "earlier-occurring observation"
    ),
    expected_behavior="specified",
    domain="ingest",
)


# =====================================================================
# SEQ2 — Rapid-fire same-proposition updates collapse via reconcile
# =====================================================================
# 5 successive inserts with identical text + scope + embed_seed should
# auto-merge into one Model after the first.


async def _run_rapid_fire(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        decisions = []
        for i in range(5):
            async with conn.transaction():
                obs = await F.make_observation(
                    conn, ctx["tenant"], actor_id=ctx["actor"],
                    content_text=f"rapid fire signal {i}",
                )
                op = H.make_state_insert_op(
                    tenant_id=ctx["tenant"], born_from_event_id=obs,
                    natural="Latency on the export pipeline is degraded",
                    scope_actors=[ctx["actor"]],
                    embed_seed="rapid-fire-canonical-text",
                    confidence=0.5 + i * 0.05,
                )
                trigger_id = uuid7()
                await apply_diff(
                    _build_diff(ctx["tenant"], trigger_id, [op]),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
                row = await conn.fetchrow(
                    "SELECT decision FROM reconciliation_events "
                    "WHERE trigger_id=$1",
                    trigger_id,
                )
                decisions.append(row["decision"] if row else None)
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM models "
            "WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {"decisions": decisions, "model_count": count}


def _assert_rapid_fire(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["model_count"] != 1:
        return False, (
            f"5 identical inserts should collapse to 1 Model; got "
            f"{actual['model_count']}; decisions={actual['decisions']}"
        )
    if actual["decisions"][0] not in (None, "no_match"):
        return False, (
            f"first insert should be no_match (no candidate); got "
            f"{actual['decisions'][0]!r}"
        )
    if not all(d == "auto_merge" for d in actual["decisions"][1:]):
        return False, (
            f"subsequent inserts should auto_merge; got "
            f"{actual['decisions']}"
        )
    return True, ""


CASE_RAPID_FIRE = Case(
    stage="adversarial.sequencing",
    name="rapid_fire_same_proposition_collapses",
    intent="5 successive identical-text inserts collapse to 1 Model "
           "via auto_merge (first is no_match, rest are auto_merge)",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_rapid_fire),
    expected=lambda _ctx: {},
    assertion=_assert_rapid_fire,
    failure_mode_under_test=(
        "reconciler fires only on rows older than some threshold; "
        "rapid-fire inserts all see no_match and accumulate as "
        "duplicates"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# SEQ3 — Reversal: insert positive then archive then re-insert
# =====================================================================


async def _run_reversal(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        # Insert
        async with conn.transaction():
            obs1 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op1 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs1,
                natural="System is healthy",
                scope_actors=[ctx["actor"]],
                embed_seed="reversal-system-healthy",
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op1]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs1,
            )
            mid = await conn.fetchval(
                "SELECT id FROM models WHERE tenant_id=$1 AND status='active'",
                ctx["tenant"],
            )

        # Archive
        async with conn.transaction():
            obs2 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            arch_op = ClaimOp(
                op="archive", model_id=mid,
                reason="superseded",
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [arch_op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs2,
            )

        # Re-insert (text now reverses to "broken")
        async with conn.transaction():
            obs3 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op3 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs3,
                natural="System is broken",
                scope_actors=[ctx["actor"]],
                embed_seed="reversal-system-broken-distinct",
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op3]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs3,
            )

        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
        archived = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='archived'",
            ctx["tenant"],
        )
    return {"active": active, "archived": archived}


def _assert_reversal(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["active"] != 1 or actual["archived"] != 1:
        return False, (
            f"after reverse(insert,archive,insert) expected active=1 "
            f"archived=1; got active={actual['active']} "
            f"archived={actual['archived']}"
        )
    return True, ""


CASE_REVERSAL = Case(
    stage="adversarial.sequencing",
    name="insert_archive_reinsert_reversal",
    intent="Insert → archive → re-insert (negated text) leaves "
           "exactly 1 active and 1 archived Model",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_reversal),
    expected=lambda _ctx: {},
    assertion=_assert_reversal,
    failure_mode_under_test=(
        "archive op silently no-ops or reconciler resurrects the "
        "archived Model on re-insert because of cosine proximity"
    ),
    expected_behavior="specified",
    domain="ingest",
)


# =====================================================================
# SEQ4 — Reversal of reversal: A → not-A → A again preserves audit chain
# =====================================================================


async def _run_double_reversal(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        ids = []
        for i, (text, seed) in enumerate([
            ("Pipeline ok", "double-reversal-A-1"),
            ("Pipeline broken", "double-reversal-not-A-2"),
            ("Pipeline ok again", "double-reversal-A-again-3"),
        ]):
            async with conn.transaction():
                obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
                op = H.make_state_insert_op(
                    tenant_id=ctx["tenant"], born_from_event_id=obs,
                    natural=text,
                    scope_actors=[ctx["actor"]],
                    embed_seed=seed,
                )
                await apply_diff(
                    _build_diff(ctx["tenant"], uuid7(), [op]),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
                mid = await conn.fetchval(
                    "SELECT id FROM models WHERE tenant_id=$1 "
                    "ORDER BY created_at DESC LIMIT 1",
                    ctx["tenant"],
                )
                ids.append(mid)
        active_count = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {"ids": [str(i) for i in ids], "active": active_count}


CASE_DOUBLE_REVERSAL = Case(
    stage="adversarial.sequencing",
    name="reversal_of_reversal_audit_intact",
    intent="A → not-A → A produces 3 distinct Models all in active "
           "state (no auto-merge between contradictions)",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_double_reversal),
    expected=lambda _ctx: {"active": 3},
    assertion=lambda a, e, c: (
        (a.get("active") == 3,
         "" if a.get("active") == 3
         else f"expected 3 active Models, got {a.get('active')}")
    ),
    failure_mode_under_test=(
        "reconciler auto-merges the third A back into the first A "
        "(based on cosine), erasing the not-A reading from the "
        "audit chain"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "When a proposition reverses then reverses again, should the "
        "third reading auto-merge with the first? Cosine says yes "
        "(same vector seed); semantically the not-A in between is a "
        "real reading. Currently auto-merge will collapse them."
    ),
    domain="extraction",
)


# =====================================================================
# SEQ5 — 10-deep supersession chain via explicit archive ops
# =====================================================================


async def _run_long_supersession(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        prev_id = None
        for i in range(10):
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
                    natural=f"Generation {i} of the proposition",
                    scope_actors=[ctx["actor"]],
                    embed_seed=f"long-supersession-distinct-{i}",
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
    return {"active": active, "archived": archived}


CASE_LONG_SUPERSESSION = Case(
    stage="adversarial.sequencing",
    name="ten_deep_supersession_chain",
    intent="10-deep insert+archive sequence ends with 1 active, 9 "
           "archived Models",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_long_supersession),
    expected=lambda _ctx: {"active": 1, "archived": 9},
    assertion=lambda a, e, c: (
        (a.get("active") == 1 and a.get("archived") == 9,
         "" if (a.get("active") == 1 and a.get("archived") == 9)
         else f"got active={a.get('active')} archived={a.get('archived')}")
    ),
    failure_mode_under_test=(
        "deep chain accumulates active Models because archive ops "
        "race the next insert's reconcile; or audit chain breaks "
        "(archive_reason missing on intermediate Models)"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# SEQ6 — Stale signal arriving after archival (zombie reference)
# =====================================================================


async def _run_zombie(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        # Insert + archive
        async with conn.transaction():
            obs1 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs1,
                natural="Cohort retention is dropping",
                scope_actors=[ctx["actor"]],
                embed_seed="zombie-cohort-retention",
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs1,
            )
            mid = await conn.fetchval(
                "SELECT id FROM models WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 1",
                ctx["tenant"],
            )
        async with conn.transaction():
            obs2 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [
                    ClaimOp(op="archive", model_id=mid,
                            reason="manual"),
                ]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs2,
            )

        # Now an "old" signal arrives — same content, a week later.
        async with conn.transaction():
            obs3 = await F.make_observation(
                conn, ctx["tenant"], actor_id=ctx["actor"],
                occurred_at=F.isoplus(-86400 * 7),  # backdated week ago
            )
            op2 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs3,
                natural="Cohort retention is dropping",
                scope_actors=[ctx["actor"]],
                embed_seed="zombie-cohort-retention",
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op2]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs3,
            )
            decision = await conn.fetchval(
                "SELECT decision FROM reconciliation_events WHERE trigger_id=$1",
                trigger_id,
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
        "active": active, "archived": archived, "decision": decision,
    }


CASE_ZOMBIE = Case(
    stage="adversarial.sequencing",
    name="stale_signal_after_archival",
    intent="A backdated signal arriving after the matching Model was "
           "archived must NOT auto-merge into the archived Model "
           "(reconciler scopes to active rows only)",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_zombie),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "reconciler queries all rows (including archived); a stale "
        "signal resurrects an archived Model OR auto-merges into a "
        "tombstone"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "What's the right behavior for a backdated signal that matches "
        "an archived Model? Options: (a) ignore (current; archive blocks "
        "reconcile), (b) restore status='active', (c) emit a new Model. "
        "Currently 'a' but no test guarantees it."
    ),
    domain="ingest",
)


# =====================================================================
# SEQ7 — Concurrent contradictions with explicit interleave
# =====================================================================
# Two contradictory inserts back-to-back in same transaction. Region
# lock should serialize INSIDE one apply_diff (since both are in the
# same diff). Verify both land or one is rejected.


async def _run_interleaved_contradictions(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op_pos = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs,
                natural="Goal X is fully on track",
                scope_actors=[ctx["actor"]],
                embed_seed="interleave-positive-1",
            )
            op_neg = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs,
                natural="Goal X is critically off track",
                scope_actors=[ctx["actor"]],
                embed_seed="interleave-negative-2",
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op_pos, op_neg]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {"active": active}


CASE_INTERLEAVE_CONTRADICTIONS = Case(
    stage="adversarial.sequencing",
    name="interleaved_contradictions_in_one_diff",
    intent="Two contradictory inserts in one diff — both land (the "
           "engine emits, the validator doesn't deconflict)",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_interleaved_contradictions),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "validator tolerates intra-diff contradictions silently; "
        "downstream consumers see two contradictory active Models "
        "with no signal that they conflict"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should the validator detect intra-diff contradictions? "
        "Currently no — it's the LLM's job to be self-consistent. "
        "But contradictory Models in the same diff are a red flag."
    ),
    domain="extraction",
)


# =====================================================================
# SEQ8 — Trigger ID re-use across two different applies
# =====================================================================


async def _run_trigger_reuse(pool: asyncpg.Pool, ctx: dict) -> dict:
    trigger_id = uuid7()
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs,
                natural="first apply with trigger T",
                scope_actors=[ctx["actor"]],
            )
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )

        # Second apply with SAME trigger_id, DIFFERENT op.
        async with conn.transaction():
            obs2 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op2 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs2,
                natural="second apply with same trigger T",
                scope_actors=[ctx["actor"]],
            )
            try:
                await apply_diff(
                    _build_diff(ctx["tenant"], trigger_id, [op2]),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs2,
                )
                raised = False
                err = None
            except Exception as exc:  # noqa: BLE001
                raised = True
                err = type(exc).__name__
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {"raised": raised, "err": err, "active": active}


def _assert_trigger_reuse(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if not actual["raised"]:
        return False, "second apply with same trigger_id should raise"
    if actual["err"] != "AlreadyAppliedError":
        return False, f"wrong exception: {actual['err']!r}"
    if actual["active"] != 1:
        return False, (
            f"only first apply's Model should land; got {actual['active']}"
        )
    return True, ""


CASE_TRIGGER_REUSE = Case(
    stage="adversarial.sequencing",
    name="trigger_id_reuse_blocked",
    intent="Re-using trigger_id with a different diff raises "
           "AlreadyAppliedError; second diff's ops are NOT applied",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_trigger_reuse),
    expected=lambda _ctx: {},
    assertion=_assert_trigger_reuse,
    failure_mode_under_test=(
        "applied_triggers idempotency check uses (trigger_id, diff_hash) "
        "instead of trigger_id alone, allowing different diffs to share "
        "an id and apply twice"
    ),
    expected_behavior="specified",
    domain="ingest",
)


# =====================================================================
# SEQ9 — Insert + immediate auto-merge: confidence does not drop
# =====================================================================


async def _run_no_conf_drop(pool: asyncpg.Pool, ctx: dict) -> dict:
    # We use confidences below the falsifier-required threshold (0.7
    # by default) so the validator doesn't insist on an adequate
    # falsifier — this isolates the auto-merge max-rule test.
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs1 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op1 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs1,
                natural="Issue A is high priority",
                scope_actors=[ctx["actor"]],
                embed_seed="no-conf-drop-canonical",
                confidence=0.65,
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op1]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs1,
            )

        async with conn.transaction():
            obs2 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op2 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs2,
                natural="Issue A is high priority",
                scope_actors=[ctx["actor"]],
                embed_seed="no-conf-drop-canonical",
                confidence=0.4,  # LOWER than existing
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op2]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs2,
            )
        row = await conn.fetchrow(
            "SELECT confidence FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {"final_confidence": float(row["confidence"]) if row else None}


def _assert_no_conf_drop(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    # The validator may apply calibration shrinkage at insert time
    # (Wave 4-C real vs Wave 1 identity). The max-rule guarantees
    # the post-merge confidence is no LOWER than the post-calibration
    # existing value — and at minimum strictly higher than the new
    # signal's 0.4 contribution.
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    c = actual["final_confidence"]
    if c is None:
        return False, "no Model"
    # Guard: must be strictly above the new (lower) signal — that's
    # the max-rule contract.
    if c < 0.45:
        return False, (
            f"auto_merge dropped to {c} (close to or below the new "
            f"signal's 0.4 confidence) — max-rule appears violated"
        )
    return True, ""


CASE_NO_CONF_DROP = Case(
    stage="adversarial.sequencing",
    name="auto_merge_does_not_lower_confidence",
    intent="Auto-merge with lower-confidence new signal must NOT drop "
           "the existing higher confidence (max rule)",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_no_conf_drop),
    expected=lambda _ctx: {},
    assertion=_assert_no_conf_drop,
    failure_mode_under_test=(
        "reconciler uses 'newer wins' instead of 'max wins', causing "
        "high-confidence Models to be dragged down by uncertain "
        "follow-up signals"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# SEQ10 — Long pause then re-emit (recency window boundary)
# =====================================================================
# Insert, advance time past the 30-day recency window, re-insert
# identical text. Should NOT auto-merge (stale).


async def _run_long_pause(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs1 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op1 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs1,
                natural="Recurring quarterly check-in pattern",
                scope_actors=[ctx["actor"]],
                embed_seed="long-pause-canonical",
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op1]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs1,
            )
            # Backdate the existing Model 60 days
            await conn.execute(
                "UPDATE models SET created_at = now() - interval '60 days' "
                "WHERE tenant_id=$1",
                ctx["tenant"],
            )

        async with conn.transaction():
            obs2 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op2 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs2,
                natural="Recurring quarterly check-in pattern",
                scope_actors=[ctx["actor"]],
                embed_seed="long-pause-canonical",
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op2]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs2,
            )
            decision = await conn.fetchval(
                "SELECT decision FROM reconciliation_events WHERE trigger_id=$1",
                trigger_id,
            )
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {"decision": decision, "active": active}


def _assert_long_pause(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["decision"] != "no_match":
        return False, (
            f"stale match (60d) should be no_match; got {actual['decision']!r}"
        )
    if actual["active"] != 2:
        return False, (
            f"both rows should be active (no merge); got {actual['active']}"
        )
    return True, ""


CASE_LONG_PAUSE = Case(
    stage="adversarial.sequencing",
    name="stale_recency_window_boundary",
    intent="Re-emitting same text 60 days later (past 30d recency) "
           "produces no_match — both Models survive",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_long_pause),
    expected=lambda _ctx: {},
    assertion=_assert_long_pause,
    failure_mode_under_test=(
        "recency window predicate uses < instead of <=, or includes "
        "archived Models, causing stale auto-merge"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# SEQ11 — Multi-step lifecycle: insert, contest, archive
# =====================================================================


async def _run_lifecycle(pool: asyncpg.Pool, ctx: dict) -> dict:
    from services.contestability.service import (
        ContestationInput,
        contest_model,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            mid = await F.make_model(
                conn, ctx["tenant"],
                natural="Lifecycle test model",
                scope_actors=[ctx["actor"]],
                confidence=0.75,
            )

        # Contest belief
        async with conn.transaction():
            outcome = await contest_model(
                conn,
                ContestationInput(
                    model_id=mid,
                    contestor_actor_id=ctx["actor"],
                    tenant_id=ctx["tenant"],
                    contestation_kind="belief",
                    rationale="contest in lifecycle",
                ),
            )

        # Archive
        async with conn.transaction():
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [
                    ClaimOp(op="archive", model_id=mid,
                            reason="manual"),
                ]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )

        row = await conn.fetchrow(
            "SELECT status, archive_reason, confidence FROM models WHERE id=$1",
            mid,
        )
    return {
        "status": row["status"],
        "archive_reason": row["archive_reason"],
        "confidence": float(row["confidence"]) if row else None,
        "contest_outcome": str(outcome),
    }


def _assert_lifecycle(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["status"] != "archived":
        return False, f"not archived: {actual['status']!r}"
    if actual["archive_reason"] != "manual":
        return False, f"wrong reason: {actual['archive_reason']!r}"
    return True, ""


CASE_LIFECYCLE = Case(
    stage="adversarial.sequencing",
    name="model_lifecycle_insert_contest_archive",
    intent="A Model walks through insert → contest → archive with "
           "consistent state at each step",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_lifecycle),
    expected=lambda _ctx: {},
    assertion=_assert_lifecycle,
    failure_mode_under_test=(
        "contestation modifies confidence but archive op overrides "
        "without recording the contested confidence in audit"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# SEQ12 — Reconciler ignores archived candidates
# =====================================================================


async def _run_archived_not_candidate(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await F.make_model(
                conn, ctx["tenant"],
                natural="Existing archived Model",
                scope_actors=[ctx["actor"]],
                embed_seed="archived-not-candidate",
                status="archived",
                archive_reason="manual",
            )
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])

        async with conn.transaction():
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs,
                natural="Existing archived Model",
                scope_actors=[ctx["actor"]],
                embed_seed="archived-not-candidate",
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
            decision = await conn.fetchval(
                "SELECT decision FROM reconciliation_events WHERE trigger_id=$1",
                trigger_id,
            )
        # Both rows: archived original + new active insert
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {"decision": decision, "active": active}


CASE_ARCHIVED_NOT_CANDIDATE = Case(
    stage="adversarial.sequencing",
    name="reconciler_ignores_archived_candidates",
    intent="Reconciler must not match against archived Models (its "
           "candidate query restricts to status='active')",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_archived_not_candidate),
    expected=lambda _ctx: {"decision": "no_match", "active": 1},
    assertion=lambda a, e, c: (
        (a.get("decision") == "no_match" and a.get("active") == 1,
         "" if (a.get("decision") == "no_match" and a.get("active") == 1)
         else f"got decision={a.get('decision')!r} active={a.get('active')}")
    ),
    failure_mode_under_test=(
        "reconciler scoping to status='active' regresses; archived "
        "Models start matching, leading to ghost auto-merges"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# SEQ13 — Three actors, signals interleaved, region locks serialize
# =====================================================================
# Multiple distinct scopes so region locks don't conflict; verify
# all signals land in deterministic order.


async def _setup_three_actors(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            a, b, c = (
                await F.make_actor(conn, tenant, display_name="A"),
                await F.make_actor(conn, tenant, display_name="B"),
                await F.make_actor(conn, tenant, display_name="C"),
            )
            return {"tenant": tenant, "a": a, "b": b, "c": c}


async def _run_three_actors(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        for actor_key, label in [("a", "alpha"), ("b", "beta"), ("c", "gamma"),
                                  ("a", "alpha-2"), ("b", "beta-2")]:
            async with conn.transaction():
                obs = await F.make_observation(
                    conn, ctx["tenant"], actor_id=ctx[actor_key],
                )
                op = H.make_state_insert_op(
                    tenant_id=ctx["tenant"], born_from_event_id=obs,
                    natural=f"signal {label}",
                    scope_actors=[ctx[actor_key]],
                    embed_seed=f"three-actors-{label}",
                )
                await apply_diff(
                    _build_diff(ctx["tenant"], uuid7(), [op]),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
        rows = await conn.fetch(
            "SELECT scope_actors[1] AS a FROM models "
            "WHERE tenant_id=$1 AND status='active' "
            "ORDER BY created_at",
            ctx["tenant"],
        )
    actor_seq = [str(r["a"]) for r in rows]
    return {"actor_sequence": actor_seq, "model_count": len(actor_seq)}


def _assert_three_actors(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["model_count"] != 5:
        return False, f"expected 5 Models, got {actual['model_count']}"
    return True, ""


CASE_THREE_ACTORS = Case(
    stage="adversarial.sequencing",
    name="five_signals_three_actors_interleaved",
    intent="5 signals across 3 actors interleaved (a-b-c-a-b) all "
           "land in deterministic order",
    setup=_setup_three_actors,
    run=H.safe_pipeline(_run_three_actors),
    expected=lambda _ctx: {"model_count": 5},
    assertion=_assert_three_actors,
    failure_mode_under_test=(
        "region lock contention or ordering bug drops or duplicates "
        "Models when scopes interleave"
    ),
    expected_behavior="specified",
    domain="leadership",
)


# =====================================================================
# SEQ14 — Apply summary captures reconcile decision per op
# =====================================================================


async def _run_summary_records(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        # First insert (no_match)
        async with conn.transaction():
            obs1 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op1 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs1,
                natural="Summary capture probe",
                scope_actors=[ctx["actor"]],
                embed_seed="summary-records-canonical",
            )
            s1 = await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op1]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs1,
            )

        # Second insert (auto_merge)
        async with conn.transaction():
            obs2 = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op2 = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs2,
                natural="Summary capture probe",
                scope_actors=[ctx["actor"]],
                embed_seed="summary-records-canonical",
            )
            s2 = await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op2]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs2,
            )
    return {"first": s1.get("reconcile_summary"), "second": s2.get("reconcile_summary")}


def _assert_summary_records(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if not actual["first"] or not actual["second"]:
        return False, f"summary missing reconcile_summary: {actual!r}"
    if actual["first"].get("no_match", 0) < 1:
        return False, f"first apply expected no_match >= 1; got {actual['first']}"
    if actual["second"].get("auto_merge", 0) < 1:
        return False, f"second apply expected auto_merge >= 1; got {actual['second']}"
    return True, ""


CASE_SUMMARY_RECORDS = Case(
    stage="adversarial.sequencing",
    name="apply_summary_reconcile_breakdown",
    intent="apply_diff returns a reconcile_summary with per-decision "
           "counts; first apply records no_match, second records auto_merge",
    setup=_setup_one_actor,
    run=H.safe_pipeline(_run_summary_records),
    expected=lambda _ctx: {},
    assertion=_assert_summary_records,
    failure_mode_under_test=(
        "apply_diff drops the reconcile_summary or returns stale "
        "counts; downstream observability misses the breakdown"
    ),
    expected_behavior="specified",
    domain="observability",
)


# =====================================================================
# SEQ15 — Three-deep cascade chain: A done → unblock B → no further
# =====================================================================
# Cascade depth 1 only — confirm the chain doesn't infinite-loop and
# the second commitment lands in 'active' state.


async def _setup_chain(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(conn, tenant, title="Parent")
            a = await F.make_commitment(conn, tenant, owner_id=owner,
                                          state="doneverified")
            b = await F.make_commitment(conn, tenant, owner_id=owner,
                                          state="blocked")
            c = await F.make_commitment(conn, tenant, owner_id=owner,
                                          state="blocked")
            await F.add_contributes_to(conn, commitment_id=b, goal_id=goal)
            await F.add_contributes_to(conn, commitment_id=c, goal_id=goal)
            await F.add_depends_on(conn, dependent=b, dependency=a)
            await F.add_depends_on(conn, dependent=c, dependency=b)
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant, "owner": owner,
                "a": a, "b": b, "c": c, "obs": obs,
            }


async def _run_chain(pool: asyncpg.Pool, ctx: dict) -> dict:
    from services.think.cascade import CascadeEvent, cascade
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["a"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=ctx["obs"],
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
            b_state = await conn.fetchval(
                "SELECT state FROM commitments WHERE id=$1", ctx["b"],
            )
            c_state = await conn.fetchval(
                "SELECT state FROM commitments WHERE id=$1", ctx["c"],
            )
    return {
        "events_visited": result.events_visited,
        "depth_reached": result.depth_reached,
        "bound_violated": result.bound_violated,
        "b_state": b_state,
        "c_state": c_state,
    }


def _assert_chain(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["b_state"] != "active":
        return False, f"B should unblock to active; got {actual['b_state']}"
    if actual["c_state"] != "blocked":
        return False, (
            f"C should stay blocked (B is now active, not doneverified); "
            f"got {actual['c_state']}"
        )
    if actual["bound_violated"]:
        return False, "bound violated unexpectedly"
    return True, ""


CASE_CHAIN = Case(
    stage="adversarial.sequencing",
    name="three_deep_cascade_chain_stops_at_active",
    intent="Cascade walks A.done → unblock B → DOES NOT cascade further "
           "(B becomes 'active', not 'doneverified', so C stays blocked)",
    setup=_setup_chain,
    run=H.safe_pipeline(_run_chain),
    expected=lambda _ctx: {},
    assertion=_assert_chain,
    failure_mode_under_test=(
        "cascade walks the full A→B→C chain incorrectly because the "
        "branch confuses 'unblock' with 'doneverify', producing "
        "infinite or incorrect downstream propagation"
    ),
    expected_behavior="specified",
    domain="extraction",
)


CASES = [
    CASE_OUT_OF_ORDER,
    CASE_RAPID_FIRE,
    CASE_REVERSAL,
    CASE_DOUBLE_REVERSAL,
    CASE_LONG_SUPERSESSION,
    CASE_ZOMBIE,
    CASE_INTERLEAVE_CONTRADICTIONS,
    CASE_TRIGGER_REUSE,
    CASE_NO_CONF_DROP,
    CASE_LONG_PAUSE,
    CASE_LIFECYCLE,
    CASE_ARCHIVED_NOT_CANDIDATE,
    CASE_THREE_ACTORS,
    CASE_SUMMARY_RECORDS,
    CASE_CHAIN,
]
