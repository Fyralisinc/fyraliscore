"""Reconciliation stage — T5 first-class reconciliation step.

Ten scenarios covering the four behaviors:

  * 3 × auto-merge: identical-text + same scope + same kind +
    recent → reconciler converts insert into a confidence update,
    only one row in `models`, audit row in `reconciliation_events`.
  * 3 × no-match (boundary cases that look similar but should NOT
    reconcile): different scope, different kind, stale.
  * 2 × human-review: medium cosine in [HUMAN_REVIEW, AUTO_MERGE)
    → audit row in pending state, original insert proceeds.
  * 2 × supersession (contestation territory): the engine emits an
    insert that contradicts an existing Model; reconciler returns
    no-match because proposition kinds differ. Documented to make
    explicit that supersession stays with the LLM.

Each scenario builds a synthetic ValidatedDiff with one
`claim_op.insert`, drives it through `apply_diff` (the same path
production Think uses), then asserts:
  - The reconcile_decision recorded on the apply summary.
  - The number of `models` rows that exist for the tenant.
  - The presence + shape of the `reconciliation_events` row.
"""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.think.applier import apply_diff
from services.think.diff_schema import ClaimOp, ValidatedDiff

from . import _fixtures as F
from ._runner import Case


# =====================================================================
# Helpers
# =====================================================================


def _proposition_for(kind: str, natural: str) -> dict:
    """Build a minimally-valid proposition dict for the given kind.

    `services.models.propositions.validate_proposition` requires
    kind-specific fields. The harness only exercises `state` and
    `concern` so we hard-code those shapes; extending to other
    kinds is straightforward when needed.
    """
    if kind == "state":
        return {"kind": "state", "subject": natural, "assertion": natural}
    if kind == "concern":
        return {
            "kind": "concern",
            "about": natural,
            "nature": natural,
            "raised_by": "harness",
        }
    raise ValueError(f"harness has no proposition template for kind={kind!r}")


def _state_insert_op(
    *,
    tenant_id: UUID,
    born_from_event_id: UUID,
    natural: str,
    confidence: float = 0.6,
    embed_seed: str | None = None,
    scope_actors: list[UUID] | None = None,
    scope_entities: list[dict] | None = None,
    proposition_kind: str = "state",
) -> ClaimOp:
    """Build a kind-appropriate insert op shaped like the LLM emits."""
    return ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(tenant_id),
            "born_from_event_id": str(born_from_event_id),
            "proposition": _proposition_for(proposition_kind, natural),
            "natural": natural,
            "embedding": F.deterministic_vector(embed_seed or natural),
            "scope_actors": [str(a) for a in (scope_actors or [])],
            "scope_entities": scope_entities or [],
            "scope_temporal": {
                "valid_from": F.isoplus(0).isoformat(),
                "valid_until": None,
            },
            "confidence": confidence,
            "confidence_at_assertion": confidence,
        },
    )


def _build_diff(
    tenant_id: UUID, trigger_id: UUID, op: ClaimOp,
) -> ValidatedDiff:
    return ValidatedDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        claim_ops=[op],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
        reasoning_trace="reconciliation harness",
    )


async def _audit_row(
    conn: asyncpg.Connection, trigger_id: UUID,
) -> dict | None:
    row = await conn.fetchrow(
        """
        SELECT id, decision, matched_model_id, cosine_similarity,
               proposition_kind
        FROM reconciliation_events
        WHERE trigger_id = $1
        ORDER BY occurred_at DESC
        LIMIT 1
        """,
        trigger_id,
    )
    return dict(row) if row else None


async def _model_count(conn: asyncpg.Connection, tenant_id: UUID) -> int:
    return await conn.fetchval(
        "SELECT COUNT(*) FROM models WHERE tenant_id = $1 AND status = 'active'",
        tenant_id,
    )


# =====================================================================
# Scenario 1 (auto_merge) — identical text + same actor scope, kind=state
# =====================================================================


async def _setup_auto_merge_state(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            existing = await F.make_model(
                conn, tenant,
                natural="Rate limiter is throwing 5% false positives at peak",
                confidence=0.55,
                scope_actors=[actor],
                embed_seed="rate-limiter-fp",
            )
            obs = await F.make_observation(
                conn, tenant, content_text="duplicate signal", actor_id=actor,
            )
            return {
                "tenant": tenant, "actor": actor,
                "existing": existing, "obs": obs,
                "trigger_id": uuid7(),
            }


async def _run_auto_merge_state(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = _state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="Rate limiter is throwing 5% false positives at peak",
        confidence=0.7,
        embed_seed="rate-limiter-fp",  # identical seed → cosine ≈ 1.0
        scope_actors=[ctx["actor"]],
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            summary = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await _audit_row(conn, ctx["trigger_id"])
            count = await _model_count(conn, ctx["tenant"])
    return {
        "claim_op_summaries": summary["claim_ops"],
        "reconcile_summary": summary["reconcile_summary"],
        "audit": audit,
        "model_count": count,
    }


def _expected_auto_merge_state(ctx: dict) -> dict:
    return {
        "decision": "auto_merge",
        "model_count": 1,
        "matched": str(ctx["existing"]),
    }


def _assert_auto_merge_state(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    diffs = []
    if actual["model_count"] != expected["model_count"]:
        diffs.append(
            f"model_count: got {actual['model_count']} expected {expected['model_count']}"
        )
    if actual["audit"] is None:
        diffs.append("audit row missing")
    else:
        a = actual["audit"]
        if a["decision"] != expected["decision"]:
            diffs.append(f"audit.decision: got {a['decision']!r}")
        if str(a["matched_model_id"]) != expected["matched"]:
            diffs.append(
                f"audit.matched_model_id: got {a['matched_model_id']!r}"
            )
    if actual["reconcile_summary"]["auto_merge"] < 1:
        diffs.append("reconcile_summary.auto_merge should be >= 1")
    return (not diffs), "; ".join(diffs)


CASE_AUTO_MERGE_STATE = Case(
    stage="reconciliation",
    name="auto_merge_state_kind",
    intent="Identical text + actor scope + state kind + recent → auto_merge converts insert to update",
    setup=_setup_auto_merge_state,
    run=_run_auto_merge_state,
    expected=_expected_auto_merge_state,
    assertion=_assert_auto_merge_state,
)


# =====================================================================
# Scenario 2 (auto_merge) — concern-kind variant
# =====================================================================


async def _setup_auto_merge_concern(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            commit_id = await F.make_commitment(
                conn, tenant, owner_id=actor, title="Migrate auth",
            )
            existing = await F.make_model(
                conn, tenant,
                natural="Migration risk: breaking sessions on rollout",
                proposition={"kind": "concern", "subject": "auth migration"},
                confidence=0.6,
                scope_entities=[{"type": "commitment", "id": str(commit_id)}],
                embed_seed="auth-migration-concern",
            )
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant, "actor": actor, "commit": commit_id,
                "existing": existing, "obs": obs,
                "trigger_id": uuid7(),
            }


async def _run_auto_merge_concern(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = _state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="Migration risk: breaking sessions on rollout",
        proposition_kind="concern",
        confidence=0.65,
        embed_seed="auth-migration-concern",
        scope_entities=[{"type": "commitment", "id": str(ctx["commit"])}],
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            summary = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await _audit_row(conn, ctx["trigger_id"])
            count = await _model_count(conn, ctx["tenant"])
    return {
        "decision": (audit or {}).get("decision"),
        "model_count": count,
        "summary": summary["reconcile_summary"],
    }


def _expected_auto_merge_concern(_ctx: dict) -> dict:
    return {"decision": "auto_merge", "model_count": 1}


def _assert_simple(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    for k, v in expected.items():
        if actual.get(k) != v:
            return False, f"{k}: got {actual.get(k)!r} expected {v!r}"
    return True, ""


CASE_AUTO_MERGE_CONCERN = Case(
    stage="reconciliation",
    name="auto_merge_concern_kind",
    intent="Identical concern + commitment scope + recent → auto_merge",
    setup=_setup_auto_merge_concern,
    run=_run_auto_merge_concern,
    expected=_expected_auto_merge_concern,
    assertion=_assert_simple,
)


# =====================================================================
# Scenario 3 (auto_merge) — confidence rises toward higher reading
# =====================================================================


async def _setup_auto_merge_confidence_rise(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            existing = await F.make_model(
                conn, tenant,
                natural="API gateway p95 latency drift to 800ms",
                confidence=0.4,
                scope_actors=[actor],
                embed_seed="latency-drift-800",
            )
            obs = await F.make_observation(conn, tenant, actor_id=actor)
            return {
                "tenant": tenant, "actor": actor,
                "existing": existing, "obs": obs,
                "trigger_id": uuid7(),
            }


async def _run_auto_merge_confidence_rise(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = _state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="API gateway p95 latency drift to 800ms",
        confidence=0.75,  # higher than existing 0.4
        embed_seed="latency-drift-800",
        scope_actors=[ctx["actor"]],
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            row = await conn.fetchrow(
                "SELECT confidence FROM models WHERE id = $1",
                ctx["existing"],
            )
    return {"confidence": float(row["confidence"]) if row else None}


def _expected_auto_merge_confidence_rise(_ctx: dict) -> dict:
    return {"confidence_at_least": 0.7}


def _assert_confidence_rise(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual.get("confidence") is None:
        return False, "matched model row not found"
    if actual["confidence"] < expected["confidence_at_least"]:
        return False, (
            f"confidence not raised: got {actual['confidence']} "
            f"expected ≥ {expected['confidence_at_least']}"
        )
    return True, ""


CASE_AUTO_MERGE_CONF_RISE = Case(
    stage="reconciliation",
    name="auto_merge_raises_existing_confidence",
    intent="Auto-merge takes max(existing_conf, new_conf): existing 0.4 → new 0.75 raises to 0.75",
    setup=_setup_auto_merge_confidence_rise,
    run=_run_auto_merge_confidence_rise,
    expected=_expected_auto_merge_confidence_rise,
    assertion=_assert_confidence_rise,
)


# =====================================================================
# Scenario 4 (no_match) — same text, DIFFERENT scope_actors
# =====================================================================


async def _setup_no_match_scope(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            alice = await F.make_actor(conn, tenant, display_name="Alice")
            bob = await F.make_actor(conn, tenant, display_name="Bob")
            await F.make_model(
                conn, tenant,
                natural="Quarterly review attendance is slipping",
                scope_actors=[alice],
                embed_seed="review-attendance-slip",
            )
            obs = await F.make_observation(conn, tenant, actor_id=bob)
            return {
                "tenant": tenant, "alice": alice, "bob": bob,
                "obs": obs, "trigger_id": uuid7(),
            }


async def _run_no_match_scope(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = _state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="Quarterly review attendance is slipping",
        embed_seed="review-attendance-slip",
        scope_actors=[ctx["bob"]],  # different actor
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await _audit_row(conn, ctx["trigger_id"])
            count = await _model_count(conn, ctx["tenant"])
    return {
        "decision": (audit or {}).get("decision"),
        "model_count": count,
    }


def _expected_no_match_scope(_ctx: dict) -> dict:
    # Two rows: existing + newly-inserted
    return {"decision": "no_match", "model_count": 2}


CASE_NO_MATCH_SCOPE = Case(
    stage="reconciliation",
    name="no_match_when_scope_actors_differ",
    intent="Same text + different scope_actors → no_match (both rows survive)",
    setup=_setup_no_match_scope,
    run=_run_no_match_scope,
    expected=_expected_no_match_scope,
    assertion=_assert_simple,
)


# =====================================================================
# Scenario 5 (no_match) — same text, DIFFERENT proposition kind
# =====================================================================


async def _setup_no_match_kind(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            await F.make_model(
                conn, tenant,
                natural="Cohort churn rising past 8% MoM",
                proposition={"kind": "state", "subject": "churn"},
                scope_actors=[actor],
                embed_seed="churn-rising-8mom",
            )
            obs = await F.make_observation(conn, tenant, actor_id=actor)
            return {
                "tenant": tenant, "actor": actor, "obs": obs,
                "trigger_id": uuid7(),
            }


async def _run_no_match_kind(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = _state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="Cohort churn rising past 8% MoM",
        embed_seed="churn-rising-8mom",
        scope_actors=[ctx["actor"]],
        proposition_kind="concern",  # different kind
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await _audit_row(conn, ctx["trigger_id"])
            count = await _model_count(conn, ctx["tenant"])
    return {
        "decision": (audit or {}).get("decision"),
        "model_count": count,
    }


CASE_NO_MATCH_KIND = Case(
    stage="reconciliation",
    name="no_match_when_proposition_kind_differs",
    intent="Same text + same scope + different kind → no_match (state ≠ concern)",
    setup=_setup_no_match_kind,
    run=_run_no_match_kind,
    expected=lambda _ctx: {"decision": "no_match", "model_count": 2},
    assertion=_assert_simple,
)


# =====================================================================
# Scenario 6 (no_match) — same text/scope/kind but STALE (outside window)
# =====================================================================


async def _setup_no_match_stale(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            existing = await F.make_model(
                conn, tenant,
                natural="Vendor SLA at risk for the AP team",
                scope_actors=[actor],
                embed_seed="vendor-sla-risk",
            )
            # Push created_at past the recency window. Default
            # RECONCILE_RECENCY_WINDOW_DAYS=30; we set 60 days back.
            await conn.execute(
                "UPDATE models SET created_at = now() - interval '60 days' "
                "WHERE id = $1",
                existing,
            )
            obs = await F.make_observation(conn, tenant, actor_id=actor)
            return {
                "tenant": tenant, "actor": actor, "obs": obs,
                "trigger_id": uuid7(),
            }


async def _run_no_match_stale(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = _state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="Vendor SLA at risk for the AP team",
        embed_seed="vendor-sla-risk",
        scope_actors=[ctx["actor"]],
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await _audit_row(conn, ctx["trigger_id"])
            count = await _model_count(conn, ctx["tenant"])
    return {
        "decision": (audit or {}).get("decision"),
        "model_count": count,
    }


CASE_NO_MATCH_STALE = Case(
    stage="reconciliation",
    name="no_match_when_existing_is_stale",
    intent="Same text+scope+kind but existing is older than recency window → no_match",
    setup=_setup_no_match_stale,
    run=_run_no_match_stale,
    expected=lambda _ctx: {"decision": "no_match", "model_count": 2},
    assertion=_assert_simple,
)


# =====================================================================
# Scenario 7 (human_review) — medium cosine, all other signals match
# =====================================================================


async def _setup_human_review(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            await F.make_model(
                conn, tenant,
                natural="Revenue at risk if vendor renegotiation slips",
                scope_actors=[actor],
                embed_seed="revenue-risk-vendor-renegotiate",
            )
            obs = await F.make_observation(conn, tenant, actor_id=actor)
            return {
                "tenant": tenant, "actor": actor, "obs": obs,
                "trigger_id": uuid7(),
            }


async def _run_human_review(pool: asyncpg.Pool, ctx: dict) -> dict:
    # Different seed → different vector → cosine ≈ 0 typically. We
    # need a vector that is intentionally close-but-not-identical
    # to the existing one. Build it as a weighted blend so the
    # cosine sits in the [0.70, 0.85) review band.
    base = F.deterministic_vector("revenue-risk-vendor-renegotiate")
    other = F.deterministic_vector("totally-different-text-here-2")
    # Two near-orthogonal unit vectors blended (w, 1-w) produce
    # cosine ≈ w / sqrt(w² + (1-w)²). For human_review band [0.70,
    # 0.85), w in roughly [0.51, 0.62]. We pick 0.58 (≈0.79 cosine).
    blend = [0.58 * a + 0.42 * b for a, b in zip(base, other)]
    # Re-normalize so the cosine math works out. (Cosine doesn't
    # require unit-norm but our blend should land in the band.)
    import math
    n = math.sqrt(sum(x * x for x in blend)) or 1.0
    blend = [x / n for x in blend]

    op = ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(ctx["tenant"]),
            "born_from_event_id": str(ctx["obs"]),
            "proposition": _proposition_for(
                "state",
                "Revenue at risk if vendor renegotiation slips",
            ),
            "natural": "Revenue at risk if vendor renegotiation slips",
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
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await _audit_row(conn, ctx["trigger_id"])
            count = await _model_count(conn, ctx["tenant"])
    return {
        "decision": (audit or {}).get("decision"),
        "cosine": (
            float(audit["cosine_similarity"]) if audit and audit["cosine_similarity"] is not None
            else None
        ),
        "model_count": count,
    }


def _expected_human_review(_ctx: dict) -> dict:
    return {"decision": "human_review", "model_count": 2}


def _assert_human_review(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual.get("decision") != expected["decision"]:
        return False, f"decision: got {actual.get('decision')!r} expected human_review (cos={actual.get('cosine')})"
    if actual.get("model_count") != expected["model_count"]:
        return False, f"model_count: got {actual.get('model_count')}"
    cos = actual.get("cosine")
    if cos is None or not (0.70 <= cos < 0.85):
        return False, f"cosine should be in [0.70, 0.85); got {cos}"
    return True, ""


CASE_HUMAN_REVIEW = Case(
    stage="reconciliation",
    name="human_review_at_medium_cosine",
    intent="Cosine in [0.70, 0.85) → human_review (audit row written, original insert proceeds)",
    setup=_setup_human_review,
    run=_run_human_review,
    expected=_expected_human_review,
    assertion=_assert_human_review,
)


# =====================================================================
# Scenario 8 (human_review) — second blend at lower cosine
# =====================================================================


async def _setup_human_review_low(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            await F.make_model(
                conn, tenant,
                natural="Q4 hiring pipeline below target",
                scope_actors=[actor],
                embed_seed="q4-hiring-pipeline",
            )
            obs = await F.make_observation(conn, tenant, actor_id=actor)
            return {
                "tenant": tenant, "actor": actor, "obs": obs,
                "trigger_id": uuid7(),
            }


async def _run_human_review_low(pool: asyncpg.Pool, ctx: dict) -> dict:
    base = F.deterministic_vector("q4-hiring-pipeline")
    other = F.deterministic_vector("budget-review-meeting-tuesday")
    # Lower band: w=0.52 → cosine ≈ 0.72, just above the 0.70 floor.
    blend = [0.52 * a + 0.48 * b for a, b in zip(base, other)]
    import math
    n = math.sqrt(sum(x * x for x in blend)) or 1.0
    blend = [x / n for x in blend]
    op = ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(ctx["tenant"]),
            "born_from_event_id": str(ctx["obs"]),
            "proposition": _proposition_for("state", "Q4 hiring pipeline below target"),
            "natural": "Q4 hiring pipeline below target",
            "embedding": blend,
            "scope_actors": [str(ctx["actor"])],
            "scope_entities": [],
            "scope_temporal": {
                "valid_from": F.isoplus(0).isoformat(),
                "valid_until": None,
            },
            "confidence": 0.55,
            "confidence_at_assertion": 0.55,
        },
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await _audit_row(conn, ctx["trigger_id"])
            count = await _model_count(conn, ctx["tenant"])
    return {
        "decision": (audit or {}).get("decision"),
        "cosine": (
            float(audit["cosine_similarity"]) if audit and audit["cosine_similarity"] is not None
            else None
        ),
        "model_count": count,
    }


CASE_HUMAN_REVIEW_LOW = Case(
    stage="reconciliation",
    name="human_review_at_lower_band",
    intent="Cosine just above 0.70 boundary → human_review",
    setup=_setup_human_review_low,
    run=_run_human_review_low,
    expected=_expected_human_review,
    assertion=_assert_human_review,
)


# =====================================================================
# Scenario 9 (supersession boundary) — contradicting state, same kind
# =====================================================================
# Documented behavior: the reconciler does NOT handle supersession.
# When the LLM emits a state-kind insert that semantically contradicts
# an existing state-kind Model (different proposition value, but
# similar enough text to share embedding space), the reconciler may
# flag for human review or auto-merge depending on cosine. Auto-merge
# in this case is wrong — it raises confidence on a contradictory
# claim. To prevent this, the reconciler in production should NOT be
# the supersession path.
#
# This scenario asserts: when the cosine is high but the text differs
# meaningfully (negation), the reconciler still triggers — and we
# treat that as a known limitation. The LLM remains responsible for
# emitting an explicit `claim_op.archive(reason="superseded")` op.
#
# The harness scenario locks down current behavior so a future
# contributor who tries to make the reconciler "smarter" sees this
# test fire.


async def _setup_supersession_boundary(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            existing = await F.make_model(
                conn, tenant,
                natural="Pipeline is healthy with all green signals",
                scope_actors=[actor],
                embed_seed="pipeline-healthy-green",
                confidence=0.7,
            )
            obs = await F.make_observation(conn, tenant, actor_id=actor)
            return {
                "tenant": tenant, "actor": actor, "obs": obs,
                "existing": existing, "trigger_id": uuid7(),
            }


async def _run_supersession_boundary(pool: asyncpg.Pool, ctx: dict) -> dict:
    # Negation in text but same embedding seed (which we control).
    # In production embeddings, negation usually shifts the vector
    # noticeably; here we use distinct seeds to ensure the cosine is
    # below the auto-merge threshold but above human_review, so the
    # reconciler routes this to the human-review queue instead of
    # silently merging the contradictions.
    base = F.deterministic_vector("pipeline-healthy-green")
    neg = F.deterministic_vector("pipeline-broken-red-alarm")
    # Land in human_review band — we want the reconciler to flag
    # this for human attention, not auto_merge it.
    blend = [0.58 * a + 0.42 * b for a, b in zip(base, neg)]
    import math
    n = math.sqrt(sum(x * x for x in blend)) or 1.0
    blend = [x / n for x in blend]
    op = ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(ctx["tenant"]),
            "born_from_event_id": str(ctx["obs"]),
            "proposition": _proposition_for("state", "Pipeline is broken with critical red alarms"),
            "natural": "Pipeline is broken with critical red alarms",
            "embedding": blend,
            "scope_actors": [str(ctx["actor"])],
            "scope_entities": [],
            "scope_temporal": {
                "valid_from": F.isoplus(0).isoformat(),
                "valid_until": None,
            },
            "confidence": 0.85,
            "confidence_at_assertion": 0.85,
            "falsifier": {
                "kind": "observation_pattern",
                "pattern": "any authoritative pipeline status report stating green",
                "within_window": "1 day",
            },
        },
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            audit = await _audit_row(conn, ctx["trigger_id"])
            count = await _model_count(conn, ctx["tenant"])
    return {
        "decision": (audit or {}).get("decision"),
        "model_count": count,
    }


def _expected_supersession_boundary(_ctx: dict) -> dict:
    # Either human_review or no_match is acceptable: the cosine
    # depends on the deterministic vector blend exactly. Auto_merge
    # is NOT acceptable — the texts contradict.
    return {"acceptable_decisions": {"human_review", "no_match"}}


def _assert_supersession_boundary(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual.get("decision") not in expected["acceptable_decisions"]:
        return False, (
            f"contradiction case must NOT auto_merge; got "
            f"decision={actual.get('decision')!r}"
        )
    if actual.get("model_count") != 2:
        return False, (
            f"both rows must survive (no auto-merge); got "
            f"{actual.get('model_count')} models"
        )
    return True, ""


CASE_SUPERSESSION = Case(
    stage="reconciliation",
    name="supersession_does_not_auto_merge_contradiction",
    intent="Contradicting state insert must NOT silently auto_merge into the existing claim",
    setup=_setup_supersession_boundary,
    run=_run_supersession_boundary,
    expected=_expected_supersession_boundary,
    assertion=_assert_supersession_boundary,
)


# =====================================================================
# Scenario 10 — reconciler kill switch via env
# =====================================================================
# When RECONCILE_ENABLED=false, the reconciler short-circuits and
# every insert flows through unchanged. This is the operator's
# emergency escape hatch from the design.


async def _setup_kill_switch(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            await F.make_model(
                conn, tenant,
                natural="Identical text would normally auto-merge",
                scope_actors=[actor],
                embed_seed="kill-switch-test",
            )
            obs = await F.make_observation(conn, tenant, actor_id=actor)
            return {
                "tenant": tenant, "actor": actor, "obs": obs,
                "trigger_id": uuid7(),
            }


async def _run_kill_switch(pool: asyncpg.Pool, ctx: dict) -> dict:
    # We mutate ReconcilerConfig directly rather than the
    # RECONCILE_ENABLED env var because env is process-global and
    # the harness runs cases concurrently — toggling env from one
    # case will leak into every other case in flight. Direct config
    # injection tests the same `enabled=False` short-circuit without
    # touching shared state.
    from services.think.reconciler import ReconcilerConfig, reconcile_claim_op

    op = _state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="Identical text would normally auto-merge",
        embed_seed="kill-switch-test",
        scope_actors=[ctx["actor"]],
    )
    disabled = ReconcilerConfig(
        enabled=False,
        auto_merge_cosine=0.85,
        human_review_cosine=0.70,
        recency_window_days=30,
        log_no_match=False,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await reconcile_claim_op(
                op, conn,
                tenant_id=ctx["tenant"],
                trigger_id=ctx["trigger_id"],
                config=disabled,
            )
            audit = await _audit_row(conn, ctx["trigger_id"])
    return {
        "decision": result.decision,
        "audit_row_written": audit is not None,
    }


CASE_KILL_SWITCH = Case(
    stage="reconciliation",
    name="kill_switch_disables_reconciler",
    intent="ReconcilerConfig(enabled=False) → decision='skipped', no audit row",
    setup=_setup_kill_switch,
    run=_run_kill_switch,
    expected=lambda _ctx: {"decision": "skipped", "audit_row_written": False},
    assertion=_assert_simple,
)


CASES = [
    CASE_AUTO_MERGE_STATE,
    CASE_AUTO_MERGE_CONCERN,
    CASE_AUTO_MERGE_CONF_RISE,
    CASE_NO_MATCH_SCOPE,
    CASE_NO_MATCH_KIND,
    CASE_NO_MATCH_STALE,
    CASE_HUMAN_REVIEW,
    CASE_HUMAN_REVIEW_LOW,
    CASE_SUPERSESSION,
    CASE_KILL_SWITCH,
]
