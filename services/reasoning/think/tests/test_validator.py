"""services/reasoning/think/tests/test_validator.py — validator unit + integration tests.

Falsifier adequacy, confidence clipping, state-machine checks, trust-
tier gate on doneverified, tenant-bound reference safety, and advisory
retrieval regions.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from lib.shared.ids import uuid7

from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.diff_schema import (
    ActOp,
    ClaimOp,
    EdgeOp,
    FormationResolutionOp,
    MemoryLifecycleOp,
    OntologyGapOp,
    RawDiff,
    RelationClaimOp,
    RelationFrameOp,
    RelationFrameParticipantOp,
)
from services.reasoning.think.validator import (
    ValidationFailure, validate,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _retrieval_result(tenant_id):
    return RetrievalResult(
        trigger=TriggerContext(kind="T1", tenant_id=tenant_id),
        models=[], observations=[], acts={"goals": [], "commitments": [], "decisions": []},
        resources=[], pathway_results=[], notes={}, model_scores={},
    )


async def _make_model(fresh_db, tenant_id, *, confidence: float, prop_kind: str = "state"):
    """Insert a raw Model directly via SQL so we can craft basis rows fast."""
    from services.reasoning.think.tests.conftest import make_embedding
    mid = uuid7()
    # Need a dummy actor+observation to satisfy FKs.
    async with fresh_db.acquire() as conn:
        aid = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            aid, tenant_id,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant_id, aid, make_embedding("x"),
        )
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                    $9::jsonb, $10, 1.0, 'active', $10, 1.0)
            """,
            mid, tenant_id, oid,
            json.dumps({"kind": prop_kind, "text": "x"}), "x",
            make_embedding("x"), [], "[]", "{}",
            float(confidence),
        )
        return mid, oid


async def test_validate_rejects_insert_without_falsifier_when_conf_high(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    mid, _ = await _make_model(fresh_db, tenant, confidence=0.5)
    # Insert with confidence 0.8 but no falsifier.
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {"kind": "state", "subject": "x", "assertion": "y"},
                    "natural": "x",
                    "embedding": [0.0] * 768,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.8,
                    "confidence_at_assertion": 0.8,
                }),
                # Valid op to bring total ops up so error rate check
                # doesn't nuke the whole diff.
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.5}),
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.4}),
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.3}),
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.2}),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        # One op dropped (the bad insert), four succeed.
        assert len(validated.claim_ops) == 4


async def test_validate_accepts_insert_with_good_falsifier_at_high_conf(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        future = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {"kind": "prediction", "expected": "x", "resolution": "y"},
                    "natural": "x",
                    "embedding": [0.0] * 768,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.8,
                    "confidence_at_assertion": 0.8,
                    "falsifier": {
                        "kind": "prediction_deadline",
                        "evaluate_at": future,
                        "check": "X must be done by Y",
                    },
                    "evaluate_at": future,
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert len(validated.claim_ops) == 1


async def test_validate_accepts_memory_lifecycle_confirm_with_evidence(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    model_id, observation_id = await _make_model(
        fresh_db,
        tenant,
        confidence=0.6,
        prop_kind="prediction",
    )
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            memory_lifecycle_ops=[
                MemoryLifecycleOp(
                    model_id=model_id,
                    action="confirm",
                    evidence_event_ids=[observation_id],
                    rationale="The new observation confirms the predicted outcome.",
                )
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.memory_lifecycle_ops) == 1
    assert validated.memory_lifecycle_ops[0].action == "confirm"
    assert validated.dropped_op_count == 0


async def test_validate_accepts_known_formation_resolution_with_existing_model(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    model_id, _ = await _make_model(fresh_db, tenant, confidence=0.72)
    candidate_id = f"formation:employee.capability:{uuid7()}:abc123"
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            formation_resolutions=[
                FormationResolutionOp(
                    candidate_id=candidate_id,
                    resolution="already_covered",
                    rationale="The selected Model already captures this capability.",
                    output_model_ids=[model_id],
                )
            ],
        )

        validated = await validate(
            diff,
            rr,
            conn,
            allowed_region=None,
            formation_candidate_ids={candidate_id},
        )

    assert len(validated.formation_resolutions) == 1
    assert validated.formation_resolutions[0].output_model_ids == [model_id]
    assert validated.dropped_op_count == 0


async def test_validate_drops_unknown_formation_resolution_but_keeps_valid_ops(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    model_id, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    known_id = f"formation:employee.support_need:{uuid7()}:known"
    unknown_id = f"formation:employee.support_need:{uuid7()}:unknown"
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            formation_resolutions=[
                FormationResolutionOp(
                    candidate_id=unknown_id,
                    resolution="rejected",
                    rationale="This should be dropped because the candidate was not prompted.",
                )
            ],
            claim_ops=[
                ClaimOp(op="update", model_id=model_id, changes={"confidence": 0.61})
            ],
        )

        validated = await validate(
            diff,
            rr,
            conn,
            allowed_region=None,
            formation_candidate_ids={known_id},
        )

    assert validated.formation_resolutions == []
    assert len(validated.claim_ops) == 1
    assert validated.dropped_op_count == 1
    assert "unknown candidate_id" in validated.dropped_op_errors[0]


async def test_validate_drops_memory_lifecycle_without_evidence_but_keeps_valid_ops(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    model_id, _ = await _make_model(
        fresh_db,
        tenant,
        confidence=0.6,
        prop_kind="prediction",
    )
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            memory_lifecycle_ops=[
                MemoryLifecycleOp(
                    model_id=model_id,
                    action="confirm",
                    rationale="No evidence is cited, so this must be dropped.",
                )
            ],
            claim_ops=[
                ClaimOp(op="update", model_id=model_id, changes={"confidence": 0.61})
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.memory_lifecycle_ops == []
    assert len(validated.claim_ops) == 1
    assert validated.dropped_op_count == 1


async def test_validate_accepts_registered_archive_reason(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    model_id, _ = await _make_model(fresh_db, tenant, confidence=0.5)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="archive", model_id=model_id, reason=" decay "),
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.claim_ops) == 1
    assert validated.claim_ops[0].op == "archive"
    assert validated.claim_ops[0].reason == "decay"


async def test_validate_drops_free_text_archive_reason(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    model_id, _ = await _make_model(fresh_db, tenant, confidence=0.5)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="archive",
                    model_id=model_id,
                    reason="stale memory cleanup",
                ),
                ClaimOp(op="update", model_id=model_id, changes={"confidence": 0.4}),
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert [op.op for op in validated.claim_ops] == ["update"]
    assert validated.dropped_op_count == 1
    assert "registered lifecycle reason" in validated.dropped_op_errors[0]


async def test_validate_clips_confidence(fresh_db, tenant):
    """
    Confidence clipped to [0.05, 0.95] on insert even when LLM proposes
    0.99 or 0.03. Using confidence <= 0.7 so no falsifier required.
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        future = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {"kind": "prediction", "expected": "x", "resolution": "y"},
                    "natural": "x",
                    "embedding": [0.0] * 768,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.99,   # above cap
                    "confidence_at_assertion": 0.99,
                    "falsifier": {
                        "kind": "prediction_deadline",
                        "evaluate_at": future,
                        "check": "X will happen by Y",
                    },
                    "evaluate_at": future,
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert validated.claim_ops[0].entry["confidence"] == 0.95


async def test_validate_caps_confidence_for_hedged_source_text(fresh_db, tenant):
    """
    A source message can be visibly hedged while the LLM overstates the
    simplified claim. The validator deterministically caps confidence
    below the high-confidence threshold so prompt variance does not
    leak into production calibration.
    """
    rr = _retrieval_result(tenant)
    rr.observations = [
        SimpleNamespace(
            content_text=(
                "If leadership ever funds this team, we'd love to maybe "
                "ship Q4 multi-region. No promises."
            )
        )
    ]
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {
                        "kind": "state",
                        "subject": "multi-region",
                        "assertion": "team lacks funding",
                    },
                    "natural": "The team lacks funding for multi-region.",
                    "embedding": [0.0] * 768,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.9,
                    "confidence_at_assertion": 0.9,
                    "falsifier": {
                        "kind": "observation_pattern",
                        "pattern": "leadership funds multi-region work",
                        "within_window": "P90D",
                    },
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert validated.claim_ops[0].entry["confidence"] == 0.69


async def test_validate_repairs_non_situation_composite_claim_before_drop(fresh_db, tenant):
    """
    Live LLMs sometimes describe a multi-clause signal as a composite
    concern/fact. Composite meaning belongs to situation Models, but the
    splitter runs after validation, so validator must preserve the op by
    normalizing the atomic role's abstraction level.
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "concern",
                        "abstraction_level": "composite",
                        "about": "Atlas renewal",
                        "nature": (
                            "Security review is blocked, exec sponsor confidence "
                            "is dropping, and renewal timing is at risk."
                        ),
                    },
                    "natural": (
                        "Security review is blocked, exec sponsor confidence is "
                        "dropping, and renewal timing is at risk."
                    ),
                    "embedding": [0.0] * 768,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.6,
                    "confidence_at_assertion": 0.6,
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.dropped_op_count == 0
    prop = validated.claim_ops[0].entry["proposition"]
    assert prop["claim_role"] == "concern"
    assert prop["abstraction_level"] == "atomic"


async def test_validate_repairs_non_situation_wrong_abstraction_level(fresh_db, tenant):
    """Live LLMs also emit concern/fact with relationship/pattern levels."""
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "fact",
                        "abstraction_level": "relationship",
                        "subject": "DeltaFleet reliability signal",
                        "assertion": "DeltaFleet has the same freshness issue.",
                    },
                    "natural": "DeltaFleet has the same freshness issue.",
                    "embedding": [0.0] * 768,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.6,
                    "confidence_at_assertion": 0.6,
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.dropped_op_count == 0
    prop = validated.claim_ops[0].entry["proposition"]
    assert prop["claim_role"] == "fact"
    assert prop["abstraction_level"] == "atomic"


async def test_validate_repairs_hypothesis_assertion_alias(fresh_db, tenant):
    """Providers sometimes express a hypothesis body as assertion text."""
    rr = _retrieval_result(tenant)
    assertion = "A bounded off-sensor transition likely occurred before confirmation."
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "hypothesis",
                        "abstraction_level": "atomic",
                        "assertion": assertion,
                    },
                    "natural": assertion,
                    "embedding": [0.0] * 768,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.58,
                    "confidence_at_assertion": 0.58,
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.dropped_op_count == 0
    prop = validated.claim_ops[0].entry["proposition"]
    assert prop["claim_role"] == "hypothesis"
    assert prop["hypothesis_text"] == assertion


async def test_validate_marks_empty_situation_members_pending(fresh_db, tenant):
    """
    Sparse retrieval can leave a live LLM with no existing Model ids to
    cite even when it correctly identifies an explicit situation. Keep
    the transient shape alive so splitter/applier can bind members.
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "situation",
                        "abstraction_level": "composite",
                        "situation": "Reliability issue is cross-customer pressure",
                        "summary": (
                            "Atlas renewal risk, DeltaFleet freshness issues, "
                            "and support saturation share one mechanism."
                        ),
                        "member_model_ids": [],
                        "relationship_summary": "Customer signals share a reliability mechanism.",
                        "pressure_type": "revenue",
                        "shared_mechanism": "The same reliability issue is gating multiple customers.",
                    },
                    "natural": (
                        "Atlas renewal risk is rising, DeltaFleet has the same "
                        "freshness issue, and support capacity is saturated."
                    ),
                    "embedding": [0.0] * 768,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.6,
                    "confidence_at_assertion": 0.6,
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.dropped_op_count == 0
    entry = validated.claim_ops[0].entry
    assert entry["member_model_pending"] is True
    assert entry["proposition"]["_pending_members"] is True


async def test_validate_marks_one_member_situation_members_pending(
    fresh_db,
    tenant,
):
    """
    The 400-wave long-horizon run exposed live-LLM situations that cited
    only one Model id. Treat that as the same deferred-binding shape as
    an empty member list so the splitter/applier can form concrete atomics
    instead of failing the whole run before retry.
    """
    rr = _retrieval_result(tenant)
    lonely_member = uuid7()
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "situation",
                        "abstraction_level": "composite",
                        "situation": "Northstar discount bridge is now visible",
                        "summary": (
                            "A later finance signal connects prior pricing "
                            "concerns to the renewal decision path."
                        ),
                        "member_model_ids": [str(lonely_member)],
                        "relationship_summary": (
                            "The cited concern needs another member before "
                            "the situation can be persisted as composition."
                        ),
                        "pressure_type": "decision",
                        "shared_mechanism": (
                            "Pricing evidence and decision timing point at "
                            "the same approval mechanism."
                        ),
                    },
                    "natural": (
                        "Northstar later confirmed the pricing bridge, but "
                        "the model only cited one prior member."
                    ),
                    "embedding": [0.0] * 768,
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.6,
                    "confidence_at_assertion": 0.6,
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.dropped_op_count == 0
    entry = validated.claim_ops[0].entry
    assert entry["member_model_pending"] is True
    assert entry["proposition"]["member_model_ids"] == []
    assert entry["proposition"]["_pending_members"] is True


async def test_validate_rejects_update_to_confidence_at_assertion(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    mid, _ = await _make_model(fresh_db, tenant, confidence=0.5)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="update", model_id=mid, changes={"confidence_at_assertion": 0.9}),
                # Ballast
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.4}),
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.6}),
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.5}),
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.3}),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        # The bad op is filtered; ballast passes.
        assert all(
            "confidence_at_assertion" not in (op.changes or {})
            for op in validated.claim_ops
        )


async def test_validate_allows_tenant_local_model_outside_allowed_region(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    mid, _ = await _make_model(fresh_db, tenant, confidence=0.5)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.6}),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=[])
        assert len(validated.claim_ops) == 1


async def test_validate_rejects_cross_tenant_model_reference(
    fresh_db,
    tenant,
    other_tenant,
):
    rr = _retrieval_result(tenant)
    mid, _ = await _make_model(fresh_db, tenant, confidence=0.5)
    other_mid, _ = await _make_model(fresh_db, other_tenant, confidence=0.5)
    try:
        async with fresh_db.acquire() as conn:
            diff = RawDiff(
                trigger_ref=uuid7(), tenant_id=tenant,
                claim_ops=[
                    ClaimOp(op="update", model_id=other_mid, changes={"confidence": 0.8}),
                    ClaimOp(op="update", model_id=mid, changes={"confidence": 0.6}),
                ],
            )
            validated = await validate(diff, rr, conn, allowed_region=None)
            assert [op.model_id for op in validated.claim_ops] == [mid]
            assert validated.dropped_op_count == 1
            assert any("not found" in err for err in validated.dropped_op_errors)
    finally:
        async with fresh_db.acquire() as conn:
            await conn.execute("DELETE FROM models WHERE tenant_id = $1", other_tenant)
            await conn.execute(
                "DELETE FROM observations WHERE tenant_id = $1", other_tenant
            )
            await conn.execute("DELETE FROM actors WHERE tenant_id = $1", other_tenant)
            await conn.execute("DELETE FROM tenants WHERE id = $1", other_tenant)


async def test_validate_within_region_passes(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    mid, _ = await _make_model(fresh_db, tenant, confidence=0.5)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.6}),
            ],
        )
        validated = await validate(
            diff, rr, conn,
            allowed_region=[("model", str(mid))],
        )
        assert len(validated.claim_ops) == 1


async def test_validate_doneverified_requires_authoritative_evidence(fresh_db, tenant):
    """
    C3 + spec §7 trust-tier gate: transitioning a commitment to
    doneverified with a non-authoritative resolved_by_event MUST raise
    TrustTierError (we deliberately let this be fatal per the "too
    many errors" threshold path — it's wrapped in validator).
    """
    from services.reasoning.think.tests.conftest import _insert_observation
    rr = _retrieval_result(tenant)
    mid, oid = await _make_model(fresh_db, tenant, confidence=0.9)
    # Create a non-authoritative observation.
    async with fresh_db.acquire() as conn:
        bad_obs = await _insert_observation(
            conn, tenant, content_text="I think it's done",
            trust_tier="inferential",
            external_id="inferential-1",
        )
    # Insert a commitment.
    async with fresh_db.acquire() as conn:
        # Use actor + owner.
        actor_id = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            actor_id, tenant,
        )
        cid = uuid7()
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at)
            VALUES ($1, $2, 'x', 'doneunverified', $3, $4, now())
            """,
            cid, tenant, actor_id, oid,
        )
        # Now submit transition_commitment_to_doneverified with the
        # inferential obs as resolved_by_event.
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=mid,
                    entity={
                        "id": str(cid),
                        "new_state": "doneverified",
                        "resolved_by_event_ids": [str(bad_obs)],
                    },
                ),
            ],
        )
        # Single-op diff with the op failing → error rate = 100% > 25%;
        # validate() raises ValidationFailure (the underlying TrustTierError
        # is in errors).
        with pytest.raises((ValidationFailure,)):
            await validate(diff, rr, conn, allowed_region=None)


async def test_validate_doneverified_authoritative_evidence_passes(fresh_db, tenant):
    from services.reasoning.think.tests.conftest import _insert_observation
    rr = _retrieval_result(tenant)
    mid, oid = await _make_model(fresh_db, tenant, confidence=0.95)
    async with fresh_db.acquire() as conn:
        good_obs = await _insert_observation(
            conn, tenant, content_text="PR merged — build passed",
            trust_tier="authoritative",
            external_id="auth-1",
        )
        actor_id = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            actor_id, tenant,
        )
        cid = uuid7()
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at)
            VALUES ($1, $2, 'x', 'doneunverified', $3, $4, now())
            """,
            cid, tenant, actor_id, oid,
        )
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=mid,
                    entity={
                        "id": str(cid),
                        "new_state": "doneverified",
                        "resolved_by_event_ids": [str(good_obs)],
                    },
                ),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert len(validated.act_ops) == 1


async def test_validate_rejects_cross_tenant_commitment_reference(
    fresh_db,
    tenant,
    other_tenant,
):
    rr = _retrieval_result(tenant)
    mid, oid = await _make_model(fresh_db, tenant, confidence=0.95)
    _, other_oid = await _make_model(fresh_db, other_tenant, confidence=0.95)
    try:
        async with fresh_db.acquire() as conn:
            actor_id = uuid7()
            await conn.execute(
                "INSERT INTO actors (id, tenant_id, type, display_name, status) "
                "VALUES ($1, $2, 'human_internal', 'x', 'active')",
                actor_id, tenant,
            )
            cid = uuid7()
            await conn.execute(
                """
                INSERT INTO commitments
                  (id, tenant_id, title, state, owner_id, created_by_event_id,
                   last_state_change_at)
                VALUES ($1, $2, 'tenant commitment', 'active', $3, $4, now())
                """,
                cid, tenant, actor_id, oid,
            )
            other_actor_id = uuid7()
            await conn.execute(
                "INSERT INTO actors (id, tenant_id, type, display_name, status) "
                "VALUES ($1, $2, 'human_internal', 'other', 'active')",
                other_actor_id, other_tenant,
            )
            other_cid = uuid7()
            await conn.execute(
                """
                INSERT INTO commitments
                  (id, tenant_id, title, state, owner_id, created_by_event_id,
                   last_state_change_at)
                VALUES ($1, $2, 'other commitment', 'active', $3, $4, now())
                """,
                other_cid, other_tenant, other_actor_id, other_oid,
            )
            diff = RawDiff(
                trigger_ref=uuid7(),
                tenant_id=tenant,
                act_ops=[
                    ActOp(
                        op="transition_commitment",
                        confidence_basis=mid,
                        entity={"id": str(other_cid), "new_state": "paused"},
                    ),
                    ActOp(
                        op="transition_commitment",
                        confidence_basis=mid,
                        entity={"id": str(cid), "new_state": "paused"},
                    ),
                ],
            )

            validated = await validate(diff, rr, conn, allowed_region=None)

        assert [op.entity["id"] for op in validated.act_ops] == [str(cid)]
        assert validated.dropped_op_count == 1
        assert any("not found" in err for err in validated.dropped_op_errors)
    finally:
        async with fresh_db.acquire() as conn:
            await conn.execute(
                "DELETE FROM commitments WHERE tenant_id = $1", other_tenant
            )
            await conn.execute("DELETE FROM models WHERE tenant_id = $1", other_tenant)
            await conn.execute(
                "DELETE FROM observations WHERE tenant_id = $1", other_tenant
            )
            await conn.execute("DELETE FROM actors WHERE tenant_id = $1", other_tenant)
            await conn.execute("DELETE FROM tenants WHERE id = $1", other_tenant)


async def test_validate_canonicalizes_unsupported_blocked_to_paused(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    mid, oid = await _make_model(fresh_db, tenant, confidence=0.95)
    async with fresh_db.acquire() as conn:
        actor_id = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            actor_id, tenant,
        )
        cid = uuid7()
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at)
            VALUES ($1, $2, 'x', 'active', $3, $4, now())
            """,
            cid, tenant, actor_id, oid,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=mid,
                    entity={
                        "id": str(cid),
                        "new_state": "blocked",
                    },
                ),
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.act_ops) == 1
    entity = validated.act_ops[0].entity
    assert entity["new_state"] == "paused"
    assert entity["canonicalized_from_state"] == "blocked"
    assert (
        entity["canonicalization_reason"]
        == "blocked_without_dependency_or_revisited_decision"
    )


async def test_validate_neutralizes_paused_blocked_without_dependency(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    mid, oid = await _make_model(fresh_db, tenant, confidence=0.95)
    async with fresh_db.acquire() as conn:
        actor_id = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            actor_id, tenant,
        )
        cid = uuid7()
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at)
            VALUES ($1, $2, 'x', 'paused', $3, $4, now())
            """,
            cid, tenant, actor_id, oid,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=mid,
                    entity={
                        "id": str(cid),
                        "new_state": "blocked",
                    },
                ),
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.act_ops == []
    assert validated.dropped_op_count == 0
    assert validated.dropped_op_errors == []


@pytest.mark.parametrize("new_state", ["paused", "blocked"])
async def test_validate_neutralizes_proposed_commitment_runtime_state(
    fresh_db,
    tenant,
    new_state,
):
    rr = _retrieval_result(tenant)
    mid, oid = await _make_model(fresh_db, tenant, confidence=0.95)
    async with fresh_db.acquire() as conn:
        actor_id = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            actor_id, tenant,
        )
        cid = uuid7()
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at)
            VALUES ($1, $2, 'x', 'proposed', $3, $4, now())
            """,
            cid, tenant, actor_id, oid,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=mid,
                    entity={
                        "id": str(cid),
                        "new_state": new_state,
                    },
                ),
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.act_ops == []
    assert validated.dropped_op_count == 0
    assert validated.dropped_op_errors == []


async def test_validate_canonicalizes_drafted_decision_revisited_to_active(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    mid, oid = await _make_model(fresh_db, tenant, confidence=0.95)
    async with fresh_db.acquire() as conn:
        did = uuid7()
        await conn.execute(
            """
            INSERT INTO decisions (
              id, tenant_id, title, decision_text, state, created_by_event_id
            ) VALUES ($1, $2, 'Adopt Kafka', 'Use Kafka', 'drafted', $3)
            """,
            did,
            tenant,
            oid,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="transition_decision",
                    confidence_basis=mid,
                    entity={
                        "id": str(did),
                        "new_state": "revisited",
                    },
                ),
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.act_ops) == 1
    entity = validated.act_ops[0].entity
    assert entity["new_state"] == "active"
    assert entity["canonicalized_from_state"] == "revisited"
    assert (
        entity["canonicalization_reason"]
        == "drafted_decision_cannot_be_revisited"
    )


async def test_validate_canonicalizes_create_decision_missing_decision_text(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    mid, _ = await _make_model(fresh_db, tenant, confidence=0.95)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="create_decision",
                    confidence_basis=mid,
                    entity={
                        "title": "Decide Granite Insurance go/no-go",
                        "rationale": "Legal evidence and reviewer capacity now collide.",
                    },
                ),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.act_ops) == 1
    entity = validated.act_ops[0].entity
    assert entity["decision_text"] == "Legal evidence and reviewer capacity now collide."
    assert entity["canonicalized_missing_decision_text"] is True


async def test_validate_neutralizes_commitment_basis_below_threshold(fresh_db, tenant):
    """
    ActOp transition_commitment_to_doneverified requires threshold
    0.80 for non-external, non-critical basis. A 0.70 basis fails.
    """
    rr = _retrieval_result(tenant)
    mid, oid = await _make_model(fresh_db, tenant, confidence=0.70)
    async with fresh_db.acquire() as conn:
        actor_id = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            actor_id, tenant,
        )
        cid = uuid7()
        from services.reasoning.think.tests.conftest import _insert_observation
        good_obs = await _insert_observation(
            conn, tenant, content_text="evidence", trust_tier="authoritative",
            external_id="ev-below-thresh",
        )
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at)
            VALUES ($1, $2, 'x', 'doneunverified', $3, $4, now())
            """,
            cid, tenant, actor_id, oid,
        )
        op = ActOp(
            op="transition_commitment",
            confidence_basis=mid,
            entity={
                "id": str(cid),
                "new_state": "doneverified",
                "resolved_by_event_ids": [str(good_obs)],
            },
        )
        # Pad with ballast so the diff doesn't fail the 25% rate gate.
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            act_ops=[op],
            claim_ops=[
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.6}),
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.65}),
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.55}),
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.50}),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        # act_op neutralized (confidence too low), claim_ops survive.
        assert len(validated.act_ops) == 0
        assert len(validated.claim_ops) == 4
        assert validated.dropped_op_count == 0


async def test_validate_all_low_confidence_act_ops_returns_empty_diff(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    mid, oid = await _make_model(fresh_db, tenant, confidence=0.60)
    async with fresh_db.acquire() as conn:
        did = uuid7()
        await conn.execute(
            """
            INSERT INTO decisions (
              id, tenant_id, title, decision_text, state, created_by_event_id
            ) VALUES ($1, $2, 'x', 'x', 'active', $3)
            """,
            did, tenant, oid,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            act_ops=[
                ActOp(
                    op="transition_decision",
                    confidence_basis=mid,
                    entity={"id": str(did), "new_state": "revisited"},
                )
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.act_ops == []
    assert validated.dropped_op_count == 0
    assert validated.dropped_op_errors == []


async def test_validate_all_bad_ops_raises_failure(fresh_db, tenant):
    """
    Post-partial-accept policy: when the LLM submitted ops and EVERY
    one failed validation, `validate()` still raises `ValidationFailure`
    (there's nothing left to apply and silently returning empty would
    mask an upstream bug). Mixed-result diffs go through partial-accept
    and are covered by `test_validate_partial_accept_keeps_good_ops`
    below.
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        bad_op = ClaimOp(op="insert", entry={
            "tenant_id": str(tenant),
            "born_from_event_id": str(uuid7()),
            "proposition": {"kind": "state", "subject": "x", "assertion": "y"},
            "natural": "x",
            "embedding": [0.0] * 768,
            "scope_actors": [],
            "scope_entities": [],
            "scope_temporal": {},
            "confidence": 0.8,  # no falsifier → validator drops
            "confidence_at_assertion": 0.8,
        })
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[bad_op, bad_op, bad_op],
        )
        with pytest.raises(ValidationFailure):
            await validate(diff, rr, conn, allowed_region=None)


async def test_validate_partial_accept_keeps_good_ops(fresh_db, tenant):
    """
    Post-partial-accept policy: mixed-result diffs should keep the
    survivors and record the dropped count. The prior 25% hard-limit
    would have rejected this case outright.
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        bad_op = ClaimOp(op="insert", entry={
            "tenant_id": str(tenant),
            "born_from_event_id": str(uuid7()),
            "proposition": {"kind": "state", "subject": "x", "assertion": "y"},
            "natural": "x",
            "embedding": [0.0] * 768,
            "scope_actors": [],
            "scope_entities": [],
            "scope_temporal": {},
            "confidence": 0.8,  # no falsifier → validator drops
            "confidence_at_assertion": 0.8,
        })
        good_op = ClaimOp(op="insert", entry={
            "tenant_id": str(tenant),
            "born_from_event_id": str(uuid7()),
            "proposition": {"kind": "state", "subject": "x", "assertion": "y"},
            "natural": "x is y",
            "embedding": [0.0] * 768,
            "scope_actors": [],
            "scope_entities": [],
            "scope_temporal": {},
            "confidence": 0.5,  # below falsifier threshold
            "confidence_at_assertion": 0.5,
        })
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[bad_op, good_op],  # 1 bad, 1 good → 50% failure
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert len(validated.claim_ops) == 1
        assert validated.dropped_op_count == 1
        assert len(validated.dropped_op_errors) == 1


async def test_validate_drops_malformed_proposition_but_keeps_good_claim(
    fresh_db, tenant,
):
    """
    Live reasoner can emit one malformed recommendation beside a valid
    claim. The validator should partial-accept instead of letting the
    applier fail the whole Think transaction.
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        bad_recommendation = ClaimOp(op="insert", entry={
            "tenant_id": str(tenant),
            "born_from_event_id": str(uuid7()),
            "proposition": {
                "kind": "recommendation",
                "target_act_ref": None,
                "proposed_change": {
                    "operation": "create",
                    "payload": {"title": "Fix latency"},
                },
                "qualitative_impact": "Avoid escalation",
                # Missing required target_actor_id.
            },
            "natural": "Someone should fix the latency escalation.",
            "embedding": [0.0] * 768,
            "scope_actors": [],
            "scope_entities": [],
            "scope_temporal": {},
            "confidence": 0.5,
            "confidence_at_assertion": 0.5,
        })
        good_claim = ClaimOp(op="insert", entry={
            "tenant_id": str(tenant),
            "born_from_event_id": str(uuid7()),
            "proposition": {
                "kind": "concern",
                "about": "ACME latency",
                "nature": "Escalation risk",
                "raised_by": "support",
            },
            "natural": "ACME latency is creating escalation risk.",
            "embedding": [0.0] * 768,
            "scope_actors": [],
            "scope_entities": [],
            "scope_temporal": {},
            "confidence": 0.5,
            "confidence_at_assertion": 0.5,
        })
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[bad_recommendation, good_claim],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert [op.entry["proposition"]["kind"] for op in validated.claim_ops] == [
            "concern"
        ]
        assert validated.dropped_op_count == 1
        assert "recommendation.target_actor_id" in validated.dropped_op_errors[0]


async def test_validate_accepts_evidence_backed_edge_op(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    a, obs_id = await _make_model(fresh_db, tenant, confidence=0.6)
    b, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="contradicts",
                    weight=0.7,
                    confidence=0.85,
                    evidence_event_ids=[obs_id],
                    explanation=(
                        "The two Models make incompatible claims about the "
                        "same operating state."
                    ),
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert len(validated.edge_ops) == 1
        assert validated.edge_ops[0].edge_kind == "contradicts"
        assert validated.edge_ops[0].evidence_event_ids == [obs_id]


async def test_validate_promotes_precise_evidence_backed_candidate_edge(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    a, obs_id = await _make_model(fresh_db, tenant, confidence=0.6)
    b, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="weakens",
                    weight=0.45,
                    confidence=0.76,
                    evidence_event_ids=[obs_id],
                    explanation=(
                        "The fresh telemetry is counter-evidence and weakens "
                        "confidence in the original launch-readiness model."
                    ),
                    review_status="candidate",
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.edge_ops) == 1
    edge = validated.edge_ops[0]
    assert edge.edge_kind == "weakens"
    assert edge.review_status == "accepted"
    assert edge.metadata["review_status_promoted_by"] == "edge_semantic_refiner"


async def test_validate_promotes_precise_candidate_edge_with_verified_endpoints(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    a, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    b, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="blocks",
                    weight=0.75,
                    confidence=0.78,
                    explanation="The DPA approval blocks the HubSpot import.",
                    review_status="candidate",
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.edge_ops) == 1
    edge = validated.edge_ops[0]
    assert edge.edge_kind == "blocks"
    assert edge.review_status == "accepted"
    assert edge.metadata["review_status_promoted_by"] == "edge_semantic_refiner"
    assert edge.metadata["review_status_promoted_evidence"] == "verified_endpoint_models"


async def test_validate_promotes_bound_relation_claim_to_accepted_edge_policy(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    a, obs_id = await _make_model(fresh_db, tenant, confidence=0.6)
    b, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="blocks",
                    edge_kind="blocks",
                    endpoint_binding_status="bound",
                    write_policy="candidate",
                    status="candidate",
                    confidence=0.74,
                    binding_confidence=0.88,
                    evidence_event_ids=[obs_id],
                    explanation="The DPA approval blocks the HubSpot import.",
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.relation_claim_ops) == 1
    relation = validated.relation_claim_ops[0]
    assert relation.edge_kind == "blocks"
    assert relation.endpoint_binding_status == "bound"
    assert relation.write_policy == "accepted_edge"
    assert relation.status == "accepted"


async def test_validate_adds_required_weight_for_accepted_relation_claim(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    a, obs_id = await _make_model(fresh_db, tenant, confidence=0.6)
    b, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=a,
                    target_model_id=b,
                    subject_ref={"kind": "model", "model_id": str(a)},
                    object_ref={"kind": "model", "model_id": str(b)},
                    predicate="weakens",
                    edge_kind="weakens",
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.72,
                    binding_confidence=0.88,
                    evidence_event_ids=[obs_id],
                    evidence_model_ids=[a, b],
                    explanation=(
                        "The fresh counterevidence weakens confidence in the "
                        "original readiness model."
                    ),
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.relation_claim_ops) == 1
    relation = validated.relation_claim_ops[0]
    assert relation.edge_kind == "weakens"
    assert relation.weight == 0.72
    assert relation.write_policy == "accepted_edge"
    assert relation.status == "accepted"


async def test_validate_refines_generic_supports_edge_to_blocks(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    source_id, _ = await _make_model(fresh_db, tenant, confidence=0.7)
    target_id, _ = await _make_model(fresh_db, tenant, confidence=0.7)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=source_id,
                    target_model_id=target_id,
                    edge_kind="supports",
                    explanation=(
                        "The source blocks the target until approval is recorded."
                    ),
                ),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.edge_ops) == 1
    edge = validated.edge_ops[0]
    assert edge.edge_kind == "blocks"
    assert edge.weight == 0.75
    assert edge.metadata["canonicalized_from_edge_kind"] == "supports"


async def test_validate_refines_generic_supports_relation_claim_to_blocks(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    source_id, _ = await _make_model(fresh_db, tenant, confidence=0.7)
    target_id, _ = await _make_model(fresh_db, tenant, confidence=0.7)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            relation_claim_ops=[
                RelationClaimOp(
                    op="upsert",
                    source_model_id=source_id,
                    target_model_id=target_id,
                    predicate="supports",
                    edge_kind="supports",
                    endpoint_binding_status="bound",
                    write_policy="candidate",
                    confidence=0.72,
                    evidence_model_ids=[source_id, target_id],
                    explanation=(
                        "The source blocks the target until approval is recorded."
                    ),
                ),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.relation_claim_ops) == 1
    relation = validated.relation_claim_ops[0]
    assert relation.edge_kind == "blocks"
    assert relation.predicate == "blocks"
    assert relation.write_policy == "accepted_edge"
    assert relation.status == "accepted"
    assert relation.metadata["canonicalized_from_edge_kind"] == "supports"


async def test_validate_relation_frame_accepts_bound_projectable_frame(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    blocker, oid = await _make_model(fresh_db, tenant, confidence=0.8)
    work, _ = await _make_model(fresh_db, tenant, confidence=0.8)
    risk, _ = await _make_model(fresh_db, tenant, confidence=0.8)
    rr = _retrieval_result(tenant)
    diff = RawDiff(
        trigger_ref=uuid7(),
        tenant_id=tenant,
        relation_frame_ops=[
            RelationFrameOp(
                relation_kind="Blocked Workstream",
                status="candidate",
                participant_binding_status="partially_bound",
                write_policy="project_edges",
                confidence=0.82,
                participants=[
                    RelationFrameParticipantOp(
                        model_id=blocker,
                        role="Blocker",
                        binding_confidence=0.9,
                    ),
                    RelationFrameParticipantOp(
                        model_id=work,
                        role="Blocked Work",
                        binding_confidence=0.9,
                    ),
                    RelationFrameParticipantOp(
                        model_id=risk,
                        role="Downstream Risk",
                        binding_confidence=0.84,
                    ),
                ],
                evidence_event_ids=[oid],
                evidence_model_ids=[blocker, work, risk],
                evidence_text="DPA approval blocks the HubSpot import.",
            )
        ],
    )
    async with fresh_db.acquire() as conn:
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.relation_frame_ops) == 1
    frame = validated.relation_frame_ops[0]
    assert frame.relation_kind == "blocked_workstream"
    assert frame.status == "accepted"
    assert frame.participant_binding_status == "bound"
    assert {participant.role for participant in frame.participants} == {
        "blocked_work",
        "blocker",
        "downstream_risk",
    }


async def test_validate_relation_frame_drops_oversized_participant_set(
    fresh_db,
    tenant,
    tenant_cleanup,
):
    model_ids = []
    for _ in range(13):
        model_id, _oid = await _make_model(fresh_db, tenant, confidence=0.8)
        model_ids.append(model_id)
    rr = _retrieval_result(tenant)
    diff = RawDiff(
        trigger_ref=uuid7(),
        tenant_id=tenant,
        relation_frame_ops=[
            RelationFrameOp(
                relation_kind="oversized_relation",
                participants=[
                    RelationFrameParticipantOp(
                        model_id=model_id,
                        role=f"role_{index}",
                    )
                    for index, model_id in enumerate(model_ids)
                ],
                evidence_model_ids=model_ids,
                evidence_text="Too many participants.",
            )
        ],
    )
    async with fresh_db.acquire() as conn:
        with pytest.raises(ValidationFailure):
            await validate(diff, rr, conn, allowed_region=None)


async def test_validate_accepts_accepted_dynamic_edge_kind(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    a, obs_id = await _make_model(fresh_db, tenant, confidence=0.6)
    b, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO relationship_ontology_proposals (
              id, tenant_id, proposed_edge_kind, status,
              description, relationship_summary,
              nearest_existing_kind, retrieval_fallback_kind, directionality,
              example_count, promotion_criteria
            )
            VALUES (
              $1, $2, 'gated_by_decision', 'accepted',
              'Progress depends on an explicit approval decision.',
              'The target cannot progress until the source decision is made.',
              'blocks', 'blocks', 'directed',
              3, '{"minimum_distinct_examples":3}'::jsonb
            )
            """,
            uuid7(),
            tenant,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="gated_by_decision",
                    weight=0.7,
                    confidence=0.85,
                    evidence_event_ids=[obs_id],
                    explanation="B is gated by a decision represented by A.",
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.edge_ops) == 1
    assert validated.edge_ops[0].edge_kind == "gated_by_decision"
    assert validated.edge_ops[0].evidence_event_ids == [obs_id]


async def test_validate_accepts_ontology_gap_op_for_unknown_edge_type(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    source, obs_id = await _make_model(fresh_db, tenant, confidence=0.6)
    target, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            ontology_gap_ops=[
                OntologyGapOp(
                    source_model_id=source,
                    target_model_id=target,
                    proposed_edge_kind="gated_by_decision",
                    description="Progress depends on an explicit approval decision.",
                    relationship_summary=(
                        "The blocker cannot resolve until the approval decision is made."
                    ),
                    parent_kind="blocks",
                    nearest_existing_kind="blocks",
                    directionality="directed",
                    dropped_dimensions=[
                        "authority surface",
                        "approval state",
                    ],
                    evidence_event_ids=[obs_id],
                    confidence=0.7,
                    impact=0.9,
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.ontology_gap_ops) == 1
    op = validated.ontology_gap_ops[0]
    assert op.proposed_edge_kind == "gated_by_decision"
    assert op.parent_kind == "blocks"
    assert op.evidence_event_ids == [obs_id]


async def test_validate_accepts_diverse_ontology_gap_matrix(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    shapes = [
        (
            "gated_by_decision",
            "blocks",
            "Progress depends on a specific approval decision.",
            "The work is blocked until the approval decision is made.",
            ["authority surface", "approval state"],
        ),
        (
            "depends_on_assumption",
            "supports",
            "The plan rests on an assumption that may later fail.",
            "The target assumption underpins whether the source plan remains valid.",
            ["assumption dependency", "future fragility"],
        ),
        (
            "transfers_risk_to",
            "early_warning_for",
            "One mitigation reduces local risk by moving it elsewhere.",
            "The source action creates an early warning on the target risk surface.",
            ["risk recipient", "second order consequence"],
        ),
        (
            "competes_for_priority_with",
            "blocks",
            "Two initiatives draw from the same finite decision capacity.",
            "The source can delay the target because both compete for priority.",
            ["shared priority budget", "capacity conflict"],
        ),
        (
            "accountable_for",
            "explains",
            "One model names the owner accountable for another outcome.",
            "The target explains who is accountable for the source outcome.",
            ["ownership", "accountability surface"],
        ),
        (
            "proxy_for",
            "predicts",
            "A measurable signal stands in for a harder-to-measure state.",
            "Movement in the source signal predicts the target latent state.",
            ["latent variable", "measurement proxy"],
        ),
    ]

    source_models = []
    evidence_ids = []
    for _shape in shapes:
        source, obs_id = await _make_model(fresh_db, tenant, confidence=0.66)
        target, _ = await _make_model(fresh_db, tenant, confidence=0.67)
        source_models.append((source, target))
        evidence_ids.append(obs_id)

    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            ontology_gap_ops=[
                OntologyGapOp(
                    source_model_id=source,
                    target_model_id=target,
                    proposed_edge_kind=kind,
                    description=description,
                    relationship_summary=summary,
                    parent_kind=fallback,
                    nearest_existing_kind=fallback,
                    directionality="directed",
                    dropped_dimensions=dropped,
                    evidence_event_ids=[obs_id],
                    confidence=0.75,
                    impact=0.82,
                    actionability=0.71,
                    urgency=0.62,
                    uncertainty=0.56,
                    authority_required=0.4,
                    novelty=0.91,
                )
                for (
                    (source, target),
                    obs_id,
                    (kind, fallback, description, summary, dropped),
                ) in zip(source_models, evidence_ids, shapes, strict=True)
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert [op.proposed_edge_kind for op in validated.ontology_gap_ops] == [
        shape[0] for shape in shapes
    ]
    assert [op.parent_kind for op in validated.ontology_gap_ops] == [
        shape[1] for shape in shapes
    ]
    assert validated.dropped_op_count == 0


async def test_validate_drops_ontology_gap_op_for_registered_edge_kind(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    source, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    target, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="update",
                    model_id=source,
                    changes={"confidence": 0.61},
                )
            ],
            ontology_gap_ops=[
                OntologyGapOp(
                    source_model_id=source,
                    target_model_id=target,
                    proposed_edge_kind="blocks",
                    description="This should be a normal registered edge.",
                    relationship_summary="The relation already fits blocks.",
                    parent_kind="blocks",
                    nearest_existing_kind="blocks",
                    dropped_dimensions=["none"],
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.ontology_gap_ops == []
    assert len(validated.claim_ops) == 1
    assert validated.dropped_op_count == 1
    assert "already exists" in validated.dropped_op_errors[0]


async def test_validate_drops_ontology_gap_op_for_accepted_dynamic_edge_kind(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    source, obs_id = await _make_model(fresh_db, tenant, confidence=0.6)
    target, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO relationship_ontology_proposals (
              id, tenant_id, proposed_edge_kind, status,
              description, relationship_summary,
              nearest_existing_kind, retrieval_fallback_kind, directionality,
              example_count, promotion_criteria
            )
            VALUES (
              $1, $2, 'gated_by_decision', 'accepted',
              'Progress depends on an explicit approval decision.',
              'The target cannot progress until the source decision is made.',
              'blocks', 'blocks', 'directed',
              3, '{"minimum_distinct_examples":3}'::jsonb
            )
            """,
            uuid7(),
            tenant,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="update",
                    model_id=source,
                    changes={"confidence": 0.61},
                )
            ],
            ontology_gap_ops=[
                OntologyGapOp(
                    source_model_id=source,
                    target_model_id=target,
                    proposed_edge_kind="gated_by_decision",
                    description="Progress depends on an explicit approval decision.",
                    relationship_summary=(
                        "The blocker cannot resolve until the approval decision is made."
                    ),
                    parent_kind="blocks",
                    nearest_existing_kind="blocks",
                    directionality="directed",
                    dropped_dimensions=["authority surface"],
                    evidence_event_ids=[obs_id],
                    confidence=0.7,
                    impact=0.9,
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.ontology_gap_ops == []
    assert len(validated.claim_ops) == 1
    assert validated.dropped_op_count == 1
    assert "already exists" in validated.dropped_op_errors[0]


async def test_validate_neutralizes_existing_graph_cycle_edge(fresh_db, tenant):
    from services.domain.models.edges_repo import EdgesRepo

    rr = _retrieval_result(tenant)
    a, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    b, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        await EdgesRepo().link(
            conn,
            source=a,
            target=b,
            kind="supports",
            tenant_id=tenant,
            detected_by="think_edge_op",
            confidence=0.8,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=b,
                    target_model_id=a,
                    edge_kind="supports",
                    confidence=0.8,
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert validated.edge_ops == []
    assert validated.dropped_op_count == 0
    assert validated.dropped_op_errors == []


async def test_validate_neutralizes_same_diff_cycle_edge(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    a, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    b, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="supports",
                    confidence=0.8,
                ),
                EdgeOp(
                    op="add",
                    source_model_id=b,
                    target_model_id=a,
                    edge_kind="supports",
                    confidence=0.8,
                ),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.edge_ops) == 1
    assert validated.edge_ops[0].source_model_id == a
    assert validated.edge_ops[0].target_model_id == b
    assert validated.dropped_op_count == 0
    assert validated.dropped_op_errors == []


async def test_validate_allows_same_diff_insert_as_edge_and_act_basis(
    fresh_db,
    tenant,
):
    rr = _retrieval_result(tenant)
    existing_model, born_obs = await _make_model(fresh_db, tenant, confidence=0.72)
    new_event = uuid7()
    async with fresh_db.acquire() as conn:
        actor_id = uuid7()
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            actor_id, tenant,
        )
        commitment_id = uuid7()
        await conn.execute(
            """
            INSERT INTO commitments
              (id, tenant_id, title, state, owner_id, created_by_event_id,
               last_state_change_at)
            VALUES ($1, $2, 'x', 'active', $3, $4, now())
            """,
            commitment_id, tenant, actor_id, born_obs,
        )
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(
                    op="insert",
                    entry={
                        "born_from_event_id": str(new_event),
                        "proposition": {
                            "kind": "state",
                            "subject": "x",
                            "assertion": "done",
                        },
                        "natural": "x is done",
                        "scope_actors": [],
                        "scope_entities": [],
                        "scope_temporal": {},
                        "confidence": 0.82,
                        "falsifier": {
                            "kind": "observation_pattern",
                            "pattern": "x is reopened or marked incomplete",
                            "within_window": "P14D",
                        },
                    },
                )
            ],
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=new_event,
                    target_model_id=existing_model,
                    edge_kind="superseded_by",
                    confidence=0.8,
                    explanation="The new done state supersedes the older state.",
                    detected_by="system",
                )
            ],
            act_ops=[
                ActOp(
                    op="transition_commitment",
                    confidence_basis=new_event,
                    entity={
                        "id": str(commitment_id),
                        "new_state": "doneunverified",
                    },
                )
            ],
        )

        validated = await validate(diff, rr, conn, allowed_region=None)

    assert len(validated.claim_ops) == 1
    assert len(validated.edge_ops) == 1
    assert validated.edge_ops[0].detected_by is None
    assert len(validated.act_ops) == 1
    assert validated.dropped_op_count == 0


async def test_validate_drops_edge_op_missing_explanation_but_keeps_claim(
    fresh_db, tenant,
):
    rr = _retrieval_result(tenant)
    a, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    b, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="update", model_id=a, changes={"confidence": 0.55}),
            ],
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=b,
                    edge_kind="contradicts",
                    weight=0.5,
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert len(validated.claim_ops) == 1
        assert validated.edge_ops == []
        assert validated.dropped_op_count == 1
        assert "requires explanation" in validated.dropped_op_errors[0]


async def test_validate_drops_edge_op_missing_endpoint_but_keeps_claim(
    fresh_db, tenant,
):
    rr = _retrieval_result(tenant)
    a, _ = await _make_model(fresh_db, tenant, confidence=0.6)
    missing = uuid4()
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="update", model_id=a, changes={"confidence": 0.55}),
            ],
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=a,
                    target_model_id=missing,
                    edge_kind="weakens",
                    weight=0.5,
                    explanation="The first Model weakens the missing endpoint.",
                )
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert len(validated.claim_ops) == 1
        assert validated.edge_ops == []
        assert validated.dropped_op_count == 1
        assert "missing model" in validated.dropped_op_errors[0]
