"""services/think/tests/test_validator.py — validator unit + integration tests.

Falsifier adequacy, confidence clipping, state-machine checks, trust-
tier gate on doneverified, and region-containment.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from lib.shared.errors import (
    FalsifierInadequateError, InvariantViolation, TrustTierError,
)
from lib.shared.ids import uuid7

from services.retrieval.primary import RetrievalResult, TriggerContext
from services.think.diff_schema import ActOp, ClaimOp, RawDiff, ResourceOp
from services.think.validator import (
    OutOfRegionError, ValidationFailure, validate,
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
    from services.think.tests.conftest import make_embedding
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
                    "proposition": {"kind": "state", "text": "x"},
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
                    "proposition": {"kind": "prediction", "text": "x"},
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
                    "proposition": {"kind": "prediction", "text": "x"},
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


async def test_validate_out_of_region_raises(fresh_db, tenant):
    rr = _retrieval_result(tenant)
    mid, _ = await _make_model(fresh_db, tenant, confidence=0.5)
    async with fresh_db.acquire() as conn:
        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="update", model_id=mid, changes={"confidence": 0.6}),
            ],
        )
        # Region is empty — touching mid is out of region.
        with pytest.raises(OutOfRegionError):
            await validate(diff, rr, conn, allowed_region=[])


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
    from services.think.tests.conftest import _insert_observation
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
    from services.think.tests.conftest import _insert_observation
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


async def test_validate_rejects_commitment_basis_below_threshold(fresh_db, tenant):
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
        from services.think.tests.conftest import _insert_observation
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
        # act_op rejected (confidence too low), claim_ops survive.
        assert len(validated.act_ops) == 0
        assert len(validated.claim_ops) == 4


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
            "proposition": {"kind": "state", "text": "x"},
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
            "proposition": {"kind": "state", "text": "x"},
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
