"""Category 4 — Reconciliation pressure.

The existing reconciliation suite covers happy paths (auto_merge,
human_review, no_match across the four match signals). These cases
push the boundaries: same-proposition different phrasing, scope
precision mismatches, hierarchical entity overlap, conflicting
falsifiers, human_review→upgrade, and the auto-merge contradiction
boundary.

All deterministic (no LLM); fixtures use the deterministic_vector
helper to land cosine in specific bands.
"""
from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.think.applier import apply_diff
from services.think.diff_schema import ClaimOp, ValidatedDiff
from services.think.reconciler import (
    ReconcilerConfig,
    reconcile_claim_op,
)

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
        reasoning_trace="adversarial.reconciliation_pressure",
    )


async def _setup_with_actor(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            return {"tenant": tenant, "actor": actor}


# =====================================================================
# RP1 — Same proposition, different paraphrase
# =====================================================================
# "ACME is at risk of churning" vs "We might lose ACME as a customer".
# Semantically equivalent; embeddings will be similar but not identical.
# Whether they auto-merge depends on the actual cosine — likely <0.85.


async def _run_paraphrase(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await F.make_model(
                conn, ctx["tenant"],
                natural="ACME is at risk of churning",
                scope_actors=[ctx["actor"]],
                embed_seed="paraphrase-acme-churn-v1",
            )
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])

        async with conn.transaction():
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs,
                natural="We might lose ACME as a customer",
                scope_actors=[ctx["actor"]],
                embed_seed="paraphrase-acme-churn-v2",  # different seed
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
            row = await conn.fetchrow(
                "SELECT decision, cosine_similarity::float8 AS c "
                "FROM reconciliation_events WHERE trigger_id=$1",
                trigger_id,
            )
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models "
            "WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {
        "decision": row["decision"] if row else None,
        "cosine": float(row["c"]) if row and row["c"] is not None else None,
        "active": active,
    }


CASE_PARAPHRASE = Case(
    stage="adversarial.reconciliation_pressure",
    name="paraphrase_same_proposition_different_words",
    intent="Two paraphrases of the same proposition: deterministic "
           "embeddings produce different vectors → no_match (current "
           "behavior); ideally human_review",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_paraphrase),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "Synonymous phrasings produce two distinct Models because the "
        "deterministic embedding doesn't model semantic equivalence — "
        "the substrate has duplicate Models for one underlying truth"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should reconcile widen its candidate net for paraphrase "
        "(e.g. add an LLM-based same-proposition test below cosine "
        "0.70)? Real production text uses paraphrase constantly."
    ),
    domain="sales",
)


# =====================================================================
# RP2 — Same proposition, different scope precision
# =====================================================================
# "ACME is at risk" (entity: customer:acme)
# vs "The ACME Corp deal is in trouble" (entity: deal:acme-renewal-q3)


async def _setup_scope_precision(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            customer_ent = {"type": "actor", "id": str(actor)}
            deal_ent = {"type": "commitment", "id": str(uuid7())}
            await F.make_model(
                conn, tenant,
                natural="ACME is at risk",
                scope_entities=[customer_ent],
                embed_seed="scope-precision-canonical",
            )
            obs = await F.make_observation(conn, tenant, actor_id=actor)
            return {
                "tenant": tenant, "actor": actor, "obs": obs,
                "deal_ent": deal_ent,
            }


async def _run_scope_precision(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=ctx["obs"],
                natural="The ACME Corp deal is in trouble",
                scope_entities=[ctx["deal_ent"]],
                embed_seed="scope-precision-canonical",  # same vec
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            row = await conn.fetchrow(
                "SELECT decision FROM reconciliation_events "
                "WHERE trigger_id=$1",
                trigger_id,
            )
    return {"decision": row["decision"] if row else None}


CASE_SCOPE_PRECISION = Case(
    stage="adversarial.reconciliation_pressure",
    name="scope_precision_mismatch",
    intent="Same vector but different scope precision (customer vs "
           "deal entity) → no_match (scope predicate filters out the "
           "candidate)",
    setup=_setup_scope_precision,
    run=H.safe_pipeline(_run_scope_precision),
    expected=lambda _ctx: {"decision": "no_match"},
    assertion=lambda a, e, c: (
        (a.get("decision") == "no_match",
         "" if a.get("decision") == "no_match"
         else f"got {a.get('decision')!r}")
    ),
    failure_mode_under_test=(
        "scope predicate uses && (any-overlap) loosely and matches "
        "across precision boundaries; the customer-level Model is "
        "merged with the deal-level Model"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should the substrate represent that customer-level and "
        "deal-level Models cover the same underlying truth at "
        "different granularities? Currently they are independent."
    ),
    domain="sales",
)


# =====================================================================
# RP3 — Confidence disagreement on near-duplicate
# =====================================================================
# Existing 0.6, new 0.85 — auto-merge takes max → 0.85.
# Current behavior is correct, but confirm.


async def _run_conf_disagreement(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await F.make_model(
                conn, ctx["tenant"],
                natural="Renewal at high risk",
                confidence=0.6,
                scope_actors=[ctx["actor"]],
                embed_seed="conf-disagreement-canonical",
            )
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])

        async with conn.transaction():
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs,
                natural="Renewal at high risk",
                scope_actors=[ctx["actor"]],
                embed_seed="conf-disagreement-canonical",
                confidence=0.85,
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
            row = await conn.fetchrow(
                "SELECT confidence FROM models WHERE id=$1", existing,
            )
    return {"merged_confidence": float(row["confidence"]) if row else None}


CASE_CONF_DISAGREE = Case(
    stage="adversarial.reconciliation_pressure",
    name="confidence_disagreement_max_rule",
    intent="Existing 0.6 + new 0.85 → merged confidence is max (0.85)",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_conf_disagreement),
    expected=lambda _ctx: {"merged_confidence_at_least": 0.85 - 1e-6},
    assertion=lambda a, e, c: (
        (a.get("merged_confidence", 0.0) >= 0.85 - 1e-6,
         "" if a.get("merged_confidence", 0.0) >= 0.85 - 1e-6
         else f"got {a.get('merged_confidence')}, expected ≥0.85")
    ),
    failure_mode_under_test=(
        "auto_merge takes new confidence (overwrite) instead of max; "
        "high-confidence existing Models get downgraded"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# RP4 — Partial overlap multi-entity scope
# =====================================================================
# Existing scope=[A, B], new scope=[A]. Cosine high. The reconciler's
# scope predicate is "any overlap" — they should auto-merge.
# But this collapses information: the new Model only mentions A, the
# existing covers both. After merge, scope is the existing's [A, B].


async def _setup_multi_entity(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            a = await F.make_actor(conn, tenant, display_name="A")
            b = await F.make_actor(conn, tenant, display_name="B")
            existing = await F.make_model(
                conn, tenant,
                natural="A and B are both at risk",
                scope_actors=[a, b],
                embed_seed="multi-entity-canonical",
            )
            obs = await F.make_observation(conn, tenant, actor_id=a)
            return {
                "tenant": tenant, "a": a, "b": b,
                "existing": existing, "obs": obs,
            }


async def _run_multi_entity(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=ctx["obs"],
                natural="A is at risk",
                scope_actors=[ctx["a"]],
                embed_seed="multi-entity-canonical",
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await conn.fetchrow(
                "SELECT decision FROM reconciliation_events WHERE trigger_id=$1",
                trigger_id,
            )
            scope_a = await conn.fetchval(
                "SELECT array_length(scope_actors, 1) FROM models WHERE id=$1",
                ctx["existing"],
            )
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {
        "decision": audit["decision"] if audit else None,
        "scope_actors_count": scope_a,
        "active": active,
    }


CASE_MULTI_ENTITY = Case(
    stage="adversarial.reconciliation_pressure",
    name="partial_overlap_multi_entity_scope",
    intent="Auto-merge of [A] into existing [A,B] scope: scope of "
           "merged Model retains [A,B] (no information loss)",
    setup=_setup_multi_entity,
    run=H.safe_pipeline(_run_multi_entity),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "auto_merge replaces existing scope_actors with the new "
        "(narrower) scope, dropping B from the merged Model — silent "
        "scope shrinkage"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "When auto-merging, should scope_actors be unioned (current?) "
        "or replaced? The reconciler issues an UPDATE that only "
        "changes confidence, but the design doc doesn't lock this in."
    ),
    domain="sales",
)


# =====================================================================
# RP5 — Hierarchical entity reconciliation: team vs leader
# =====================================================================
# A signal scoped to a team manager and a signal scoped to the team
# itself. Are these the same proposition? Currently no — different
# scope_actors → no_match.


async def _setup_hierarchy(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            sarah = await F.make_actor(
                conn, tenant, display_name="Sarah (lead)",
            )
            team = await F.make_actor(
                conn, tenant, display_name="Engineering team",
                actor_type="group",
            )
            await F.make_model(
                conn, tenant,
                natural="Engineering capacity is constrained this quarter",
                scope_actors=[team],
                embed_seed="hierarchy-canonical",
            )
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant, "sarah": sarah, "team": team, "obs": obs,
            }


async def _run_hierarchy(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=ctx["obs"],
                natural="Engineering capacity is constrained this quarter",
                scope_actors=[ctx["sarah"]],
                embed_seed="hierarchy-canonical",
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await conn.fetchrow(
                "SELECT decision FROM reconciliation_events WHERE trigger_id=$1",
                trigger_id,
            )
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {
        "decision": audit["decision"] if audit else None,
        "active": active,
    }


CASE_HIERARCHY = Case(
    stage="adversarial.reconciliation_pressure",
    name="hierarchical_entity_team_vs_leader",
    intent="Signal scoped to team-leader vs signal scoped to team "
           "itself — are these the same proposition?",
    setup=_setup_hierarchy,
    run=H.safe_pipeline(_run_hierarchy),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "no hierarchy resolution: a Model about Sarah and a Model "
        "about Sarah's team are duplicates from a substrate POV but "
        "currently flow through as two distinct Models"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Does the substrate model actor hierarchies (manager chains, "
        "team-of-membership)? If yes, should reconcile traverse them? "
        "Currently no hierarchy table is consulted."
    ),
    domain="leadership",
)


# =====================================================================
# RP6 — Auto-merge boundary: cosine exactly at the threshold
# =====================================================================
# Build a vector with cosine very close to 0.85 (the auto_merge
# threshold). Current behavior: ≥0.85 → auto_merge. Verify the
# strict comparison.


async def _run_threshold_boundary(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await F.make_model(
                conn, ctx["tenant"],
                natural="Threshold boundary canonical",
                scope_actors=[ctx["actor"]],
                embed_seed="threshold-boundary-base",
            )
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])

        # cosine ≈ 0.85: w / sqrt(w² + (1-w)²) = 0.85  →  w ≈ 0.617
        base = F.deterministic_vector("threshold-boundary-base")
        other = F.deterministic_vector("threshold-boundary-noise")
        blend = H.blend_vectors(base, other, w=0.617)

        async with conn.transaction():
            op = ClaimOp(
                op="insert",
                entry={
                    "tenant_id": str(ctx["tenant"]),
                    "born_from_event_id": str(obs),
                    "proposition": {
                        "kind": "state",
                        "subject": "Threshold boundary canonical",
                        "assertion": "Threshold boundary canonical",
                    },
                    "natural": "Threshold boundary canonical",
                    "embedding": blend,
                    "scope_actors": [str(ctx["actor"])],
                    "scope_entities": [],
                    "scope_temporal": {
                        "valid_from": F.isoplus(0).isoformat(),
                        "valid_until": None,
                    },
                    "confidence": 0.6,
                    "confidence_at_assertion": 0.6,
                },
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
            row = await conn.fetchrow(
                "SELECT decision, cosine_similarity::float8 AS c "
                "FROM reconciliation_events WHERE trigger_id=$1",
                trigger_id,
            )
    return {
        "decision": row["decision"] if row else None,
        "cosine": float(row["c"]) if row and row["c"] is not None else None,
    }


def _assert_threshold_boundary(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    cos = actual.get("cosine")
    if cos is None:
        return False, "no audit row written"
    if cos >= 0.85 and actual["decision"] != "auto_merge":
        return False, f"cosine {cos:.4f} ≥ 0.85 should be auto_merge"
    if cos < 0.85 and cos >= 0.70 and actual["decision"] != "human_review":
        return False, f"cosine {cos:.4f} ∈ [0.70, 0.85) should be human_review"
    return True, ""


CASE_THRESHOLD_BOUNDARY = Case(
    stage="adversarial.reconciliation_pressure",
    name="auto_merge_threshold_boundary_strict",
    intent="At cosine ≈ 0.85, the comparison must be strict (≥) — "
           "any drift in the comparator silently changes auto_merge "
           "rate",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_threshold_boundary),
    expected=lambda _ctx: {},
    assertion=_assert_threshold_boundary,
    failure_mode_under_test=(
        "comparator regresses to >0.85 (strict greater); rows at "
        "exactly the threshold get bumped to human_review unexpectedly"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# RP7 — Auto-merge with conflicting falsifiers
# =====================================================================
# Existing Model has falsifier F1 (e.g. observation_pattern). New
# insert has falsifier F2 (e.g. prediction_deadline). Auto-merge
# only updates confidence — falsifier doesn't change. The new
# falsifier is dropped silently.


async def _setup_conflicting_falsifiers(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            existing = await F.make_model(
                conn, tenant,
                natural="Latency p95 trending up",
                scope_actors=[actor],
                embed_seed="conflicting-falsifiers",
                falsifier={
                    "kind": "observation_pattern",
                    "pattern": "any p95 reading above 800ms in 7d window",
                    "within_window": "P7D",
                },
            )
            obs = await F.make_observation(conn, tenant, actor_id=actor)
            return {
                "tenant": tenant, "actor": actor,
                "existing": existing, "obs": obs,
            }


async def _run_conflicting_falsifiers(pool: asyncpg.Pool, ctx: dict) -> dict:
    new_falsifier = {
        "kind": "prediction_deadline",
        "evaluate_at": F.isoplus(86400 * 14).isoformat(),
        "check": f"Model {uuid7()} confidence < 0.5",
    }
    async with pool.acquire() as conn:
        async with conn.transaction():
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=ctx["obs"],
                natural="Latency p95 trending up",
                scope_actors=[ctx["actor"]],
                embed_seed="conflicting-falsifiers",
                falsifier=new_falsifier,
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            row = await conn.fetchrow(
                "SELECT falsifier FROM models WHERE id=$1",
                ctx["existing"],
            )
    if row and row["falsifier"]:
        import json as _json
        retained = _json.loads(row["falsifier"]) if isinstance(row["falsifier"], str) else dict(row["falsifier"])
    else:
        retained = None
    return {"retained_falsifier_kind": retained.get("kind") if retained else None}


CASE_CONFLICTING_FALSIFIERS = Case(
    stage="adversarial.reconciliation_pressure",
    name="auto_merge_conflicting_falsifiers",
    intent="When auto-merging, the new falsifier is silently dropped "
           "because the UPDATE only sets confidence — known limitation",
    setup=_setup_conflicting_falsifiers,
    run=H.safe_pipeline(_run_conflicting_falsifiers),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "the new claim's falsifier (prediction_deadline) is lost; "
        "the existing observation_pattern remains as the only "
        "falsifier — corroborating signal arrived with a different "
        "epistemic check that's now invisible"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "When two falsifiers are both adequate, should auto_merge "
        "union them, replace, or flag for human_review? Today the "
        "new one is dropped, which means a more rigorous falsifier "
        "from a corroborating signal is lost."
    ),
    domain="extraction",
)


# =====================================================================
# RP8 — Three near-duplicates create one Model (idempotent reconcile)
# =====================================================================


async def _run_three_near_dups(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        decisions = []
        for i in range(3):
            async with conn.transaction():
                obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
                op = H.make_state_insert_op(
                    tenant_id=ctx["tenant"], born_from_event_id=obs,
                    natural="Idempotent reconcile probe",
                    scope_actors=[ctx["actor"]],
                    embed_seed="three-near-dups-canonical",
                )
                trigger_id = uuid7()
                await apply_diff(
                    _build_diff(ctx["tenant"], trigger_id, [op]),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
                row = await conn.fetchrow(
                    "SELECT decision FROM reconciliation_events WHERE trigger_id=$1",
                    trigger_id,
                )
                decisions.append(row["decision"] if row else None)
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {"decisions": decisions, "active": active}


def _assert_three_near_dups(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["active"] != 1:
        return False, (
            f"3 identical inserts → 1 Model expected; got {actual['active']}"
        )
    return True, ""


CASE_THREE_NEAR_DUPS = Case(
    stage="adversarial.reconciliation_pressure",
    name="three_identical_inserts_idempotent",
    intent="3 identical inserts → 1 Model (1 no_match + 2 auto_merge)",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_three_near_dups),
    expected=lambda _ctx: {},
    assertion=_assert_three_near_dups,
    failure_mode_under_test=(
        "race condition or stale candidate query causes the third "
        "insert to no_match instead of auto_merge into the existing "
        "Model"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# RP9 — Reconcile with empty embedding skips cleanly
# =====================================================================


async def _run_no_embedding(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(ctx["tenant"]),
            "born_from_event_id": str(uuid7()),
            "proposition": {"kind": "state", "subject": "x", "assertion": "x"},
            "natural": "no embedding probe",
            # embedding deliberately missing
            "scope_actors": [str(ctx["actor"])],
            "scope_entities": [],
            "scope_temporal": {
                "valid_from": F.isoplus(0).isoformat(),
                "valid_until": None,
            },
            "confidence": 0.6,
            "confidence_at_assertion": 0.6,
        },
    )
    async with pool.acquire() as conn:
        result = await reconcile_claim_op(
            op, conn,
            tenant_id=ctx["tenant"],
            trigger_id=uuid7(),
        )
    return {"decision": result.decision}


CASE_NO_EMBEDDING = Case(
    stage="adversarial.reconciliation_pressure",
    name="reconcile_missing_embedding_skips",
    intent="A claim_op without an embedding short-circuits to "
           "decision='skipped' (no candidate query, no false match)",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_no_embedding),
    expected=lambda _ctx: {"decision": "skipped"},
    assertion=lambda a, e, c: (
        (a.get("decision") == "skipped",
         "" if a.get("decision") == "skipped"
         else f"got {a.get('decision')!r}")
    ),
    failure_mode_under_test=(
        "missing-embedding contract regresses; reconciler tries to "
        "compare against a None vector and either crashes or "
        "matches against zero vectors"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# RP10 — Reconcile with non-insert op skips cleanly
# =====================================================================


async def _run_non_insert(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = ClaimOp(
        op="update", model_id=uuid7(),
        changes={"confidence": 0.7},
    )
    async with pool.acquire() as conn:
        result = await reconcile_claim_op(
            op, conn,
            tenant_id=ctx["tenant"],
            trigger_id=uuid7(),
        )
    return {"decision": result.decision}


CASE_NON_INSERT = Case(
    stage="adversarial.reconciliation_pressure",
    name="reconcile_non_insert_skips",
    intent="claim_op.update or .archive bypass reconcile entirely",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_non_insert),
    expected=lambda _ctx: {"decision": "skipped"},
    assertion=lambda a, e, c: (
        (a.get("decision") == "skipped",
         "" if a.get("decision") == "skipped"
         else f"got {a.get('decision')!r}")
    ),
    failure_mode_under_test=(
        "reconcile fires on update/archive ops, attempting to find "
        "candidates and write spurious audit rows"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# RP11 — Custom (very high) auto_merge threshold demotes auto_merges
# =====================================================================


async def _run_high_threshold(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await F.make_model(
                conn, ctx["tenant"],
                natural="custom-threshold canonical",
                scope_actors=[ctx["actor"]],
                embed_seed="custom-threshold",
            )
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])

        op = H.make_state_insert_op(
            tenant_id=ctx["tenant"], born_from_event_id=obs,
            natural="custom-threshold canonical",
            scope_actors=[ctx["actor"]],
            embed_seed="custom-threshold",
        )
        # Identical embedding gives cosine ≈ 1.0; with auto_merge=0.99
        # the case should still be auto_merge but with 0.999 threshold
        # any noise would demote.
        config = ReconcilerConfig(
            enabled=True,
            auto_merge_cosine=0.999,  # very strict
            human_review_cosine=0.70,
            recency_window_days=30,
            log_no_match=True,
        )
        result = await reconcile_claim_op(
            op, conn,
            tenant_id=ctx["tenant"],
            trigger_id=uuid7(),
            config=config,
        )
    return {"decision": result.decision, "cosine": result.cosine_similarity}


CASE_HIGH_THRESHOLD = Case(
    stage="adversarial.reconciliation_pressure",
    name="custom_high_auto_merge_threshold",
    intent="Setting auto_merge_cosine=0.999 forces near-identical "
           "vectors (cosine ≈ 1.0) to still auto_merge; tighter "
           "thresholds would route to human_review",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_high_threshold),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "config injection bypassed; reconciler reads only env "
        "rather than the config arg"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# RP12 — Reconcile against a Model with NULL embedding skipped
# =====================================================================


async def _run_null_existing_embedding(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await F.make_model(
                conn, ctx["tenant"],
                natural="null-emb canonical",
                scope_actors=[ctx["actor"]],
                embed_seed="null-emb-canonical",
            )
            await conn.execute(
                "UPDATE models SET embedding = NULL WHERE id=$1", existing,
            )
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])

        async with conn.transaction():
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs,
                natural="null-emb canonical",
                scope_actors=[ctx["actor"]],
                embed_seed="null-emb-canonical",
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
            audit = await conn.fetchrow(
                "SELECT decision FROM reconciliation_events WHERE trigger_id=$1",
                trigger_id,
            )
    return {"decision": audit["decision"] if audit else None}


CASE_NULL_EXISTING_EMB = Case(
    stage="adversarial.reconciliation_pressure",
    name="existing_model_null_embedding_blocked_by_schema",
    intent="The schema's NOT NULL constraint on `embedding` blocks "
           "any path that would write a Model with no embedding — "
           "this scenario verifies the schema, not the reconciler",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_null_existing_embedding),
    expected=lambda _ctx: {},
    assertion=lambda a, e, c: (
        # We expect a crash here (NotNullViolationError) — that's the
        # protective behavior. Failure mode would be the UPDATE silently
        # succeeding and producing a NULL-embedding Model.
        (a.get("crashed") is True
         and "NotNull" in str(a.get("error_type", ""))
         + str(a.get("error", "")),
         "" if (a.get("crashed") is True
                and "NotNull" in (str(a.get("error_type", ""))
                                  + str(a.get("error", ""))))
         else f"expected NotNullViolationError; got {a!r}")
    ),
    failure_mode_under_test=(
        "schema NOT NULL constraint on embedding column regresses; "
        "rows with NULL embeddings can land and silently match "
        "everything (or nothing) during reconciliation"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# RP13 — Reconcile across propositions with shared kind but different content
# =====================================================================


async def _run_shared_kind_diff_content(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await F.make_model(
                conn, ctx["tenant"],
                natural="API latency rising",
                scope_actors=[ctx["actor"]],
                embed_seed="shared-kind-diff-1",
                proposition={
                    "kind": "state",
                    "subject": "API latency",
                    "assertion": "rising",
                },
            )
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])

        async with conn.transaction():
            op = ClaimOp(
                op="insert",
                entry={
                    "tenant_id": str(ctx["tenant"]),
                    "born_from_event_id": str(obs),
                    "proposition": {
                        "kind": "state",
                        "subject": "Hiring pipeline",
                        "assertion": "running smooth",
                    },
                    "natural": "Hiring pipeline running smooth",
                    "embedding": F.deterministic_vector("shared-kind-diff-2"),
                    "scope_actors": [str(ctx["actor"])],
                    "scope_entities": [],
                    "scope_temporal": {
                        "valid_from": F.isoplus(0).isoformat(),
                        "valid_until": None,
                    },
                    "confidence": 0.6,
                    "confidence_at_assertion": 0.6,
                },
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
            audit = await conn.fetchrow(
                "SELECT decision FROM reconciliation_events WHERE trigger_id=$1",
                trigger_id,
            )
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {
        "decision": audit["decision"] if audit else None,
        "active": active,
    }


CASE_SHARED_KIND_DIFF = Case(
    stage="adversarial.reconciliation_pressure",
    name="shared_kind_different_content",
    intent="Two different state propositions on the same scope must "
           "both persist (no_match) — the kind-match alone is "
           "insufficient",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_shared_kind_diff_content),
    expected=lambda _ctx: {"decision": "no_match", "active": 2},
    assertion=lambda a, e, c: (
        (a.get("decision") == "no_match" and a.get("active") == 2,
         "" if (a.get("decision") == "no_match" and a.get("active") == 2)
         else f"got {a!r}")
    ),
    failure_mode_under_test=(
        "scope-and-kind alone trigger auto_merge against orthogonal "
        "content because cosine is wide; collapsed Models lose "
        "distinct propositions"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# RP14 — Reconciler audit row written even on no_match (tuning data)
# =====================================================================


async def _run_no_match_audit(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs,
                natural="No-match audit probe",
                scope_actors=[ctx["actor"]],
                embed_seed="no-match-audit-canonical",
            )
            trigger_id = uuid7()
            await apply_diff(
                _build_diff(ctx["tenant"], trigger_id, [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
        cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM reconciliation_events WHERE trigger_id=$1",
            trigger_id,
        )
    return {"audit_rows": cnt}


CASE_NO_MATCH_AUDIT = Case(
    stage="adversarial.reconciliation_pressure",
    name="no_match_audit_row_written",
    intent="With log_no_match=True (default), a no_match decision "
           "still writes 1 audit row for tuning data",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_no_match_audit),
    expected=lambda _ctx: {"audit_rows": 1},
    assertion=lambda a, e, c: (
        (a.get("audit_rows") == 1,
         "" if a.get("audit_rows") == 1
         else f"got {a.get('audit_rows')} audit rows")
    ),
    failure_mode_under_test=(
        "RECONCILE_LOG_NO_MATCH default flips silently; tuning data "
        "vanishes from production"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# RP15 — Auto-merge does not reset retrieval activation
# =====================================================================


async def _run_no_activation_reset(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await F.make_model(
                conn, ctx["tenant"],
                natural="Activation reset probe",
                scope_actors=[ctx["actor"]],
                embed_seed="activation-reset-canonical",
                activation=0.95,
            )
            obs = await F.make_observation(conn, ctx["tenant"], actor_id=ctx["actor"])

        async with conn.transaction():
            op = H.make_state_insert_op(
                tenant_id=ctx["tenant"], born_from_event_id=obs,
                natural="Activation reset probe",
                scope_actors=[ctx["actor"]],
                embed_seed="activation-reset-canonical",
            )
            await apply_diff(
                _build_diff(ctx["tenant"], uuid7(), [op]),
                conn, trigger_kind="T1",
                trigger_cause_event_id=obs,
            )
            row = await conn.fetchrow(
                "SELECT activation FROM models WHERE id=$1", existing,
            )
    return {"activation": float(row["activation"]) if row else None}


def _assert_no_activation_reset(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    a = actual.get("activation")
    if a is None:
        return False, "no row"
    if a < 0.95 - 1e-6:
        return False, f"activation dropped from 0.95 to {a}"
    return True, ""


CASE_NO_ACTIVATION_RESET = Case(
    stage="adversarial.reconciliation_pressure",
    name="auto_merge_does_not_reset_activation",
    intent="auto_merge UPDATE only changes confidence; activation "
           "must NOT regress",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_no_activation_reset),
    expected=lambda _ctx: {},
    assertion=_assert_no_activation_reset,
    failure_mode_under_test=(
        "auto_merge UPDATE accidentally sets activation back to "
        "default 0.5, dropping a high-attention Model from retrieval"
    ),
    expected_behavior="specified",
    domain="extraction",
)


CASES = [
    CASE_PARAPHRASE,
    CASE_SCOPE_PRECISION,
    CASE_CONF_DISAGREE,
    CASE_MULTI_ENTITY,
    CASE_HIERARCHY,
    CASE_THRESHOLD_BOUNDARY,
    CASE_CONFLICTING_FALSIFIERS,
    CASE_THREE_NEAR_DUPS,
    CASE_NO_EMBEDDING,
    CASE_NON_INSERT,
    CASE_HIGH_THRESHOLD,
    CASE_NULL_EXISTING_EMB,
    CASE_SHARED_KIND_DIFF,
    CASE_NO_MATCH_AUDIT,
    CASE_NO_ACTIVATION_RESET,
]
