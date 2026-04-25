"""services/think/tests/test_calibration_ordering.py — TK-2 audit fix.

Source: THINK-DESIGN-AUDIT.md §5.2.

`services/models/calibration.apply_calibration` CAN inflate confidence
(the formula is `clip(raw * offset, 0.05, 0.95)` with
`offset ∈ [OFFSET_MIN=0.3, OFFSET_MAX=1.5]`). This test enforces the
fixed ordering in validator._validate_claim_op:

    clip → calibrate → clip → falsifier check

A raw confidence of 0.65 with a calibration offset of 1.2 lands at
0.78 post-calibration, which is ABOVE the 0.7 falsifier threshold. The
Model must therefore require an adequate falsifier — a Model without
one must be dropped (before this fix, the old ordering ran the
falsifier check on the pre-calibration 0.65 value and happily admitted
the Model without a falsifier).

The inverse case (calibration drops confidence from 0.72 to 0.60)
does NOT require a falsifier, because the post-calibration confidence
is below the threshold.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from lib.shared.ids import uuid7
from services.retrieval.primary import RetrievalResult, TriggerContext
from services.think.diff_schema import ClaimOp, RawDiff
from services.think.validator import validate


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _retrieval_result(tenant_id):
    return RetrievalResult(
        trigger=TriggerContext(kind="T1", tenant_id=tenant_id),
        models=[], observations=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        resources=[], pathway_results=[], notes={}, model_scores={},
    )


async def _seed_actor(conn, tenant_id):
    aid = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'calib-actor', 'active')
        """,
        aid, tenant_id,
    )
    return aid


async def _install_calibration_offset(
    conn,
    tenant_id,
    actor_id,
    proposition_kind: str,
    offset: float,
    bucket_low: float = 0.0,
    bucket_high: float = 1.0,
) -> None:
    await conn.execute(
        """
        INSERT INTO calibration_offsets
          (tenant_id, actor_id, proposition_kind,
           bucket_low, bucket_high, "offset", sample_size)
        VALUES ($1, $2, $3, $4, $5, $6, 100)
        ON CONFLICT (tenant_id, actor_id, proposition_kind, bucket_low)
        DO UPDATE SET bucket_high = EXCLUDED.bucket_high,
                      "offset" = EXCLUDED."offset"
        """,
        tenant_id, actor_id, proposition_kind,
        bucket_low, bucket_high, offset,
    )


async def test_inflating_calibration_requires_falsifier(fresh_db, tenant, tenant_cleanup):
    """
    TK-2 core test.

    Raw confidence 0.65 (below falsifier threshold 0.7), but an
    inflating calibration offset of 1.2 pushes post-calibration
    confidence to 0.78 (above threshold). Without a falsifier, the
    op MUST be dropped — proving the falsifier check runs AFTER
    calibration (new ordering).
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        actor = await _seed_actor(conn, tenant)
        await _install_calibration_offset(
            conn, tenant, actor, proposition_kind="state", offset=1.2,
        )

        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {"kind": "state", "text": "x"},
                    "natural": "x",
                    "embedding": [0.0] * 768,
                    "scope_actors": [str(actor)],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.65,
                    "confidence_at_assertion": 0.65,
                    # no falsifier — the whole point of the test
                }),
            ],
        )
        # No survivors -> ValidationFailure (partial-accept gate). The
        # insert is dropped because post-calibration conf (0.78)
        # exceeds the falsifier threshold and the entry has no
        # adequate falsifier.
        from services.think.validator import ValidationFailure
        with pytest.raises(ValidationFailure):
            await validate(diff, rr, conn, allowed_region=None)


async def test_inflating_calibration_with_adequate_falsifier_passes(
    fresh_db, tenant, tenant_cleanup,
):
    """
    Same inflating calibration (0.65 → 0.78) but with an adequate
    prediction_deadline falsifier. The op must pass — falsifier check
    is post-calibration, so a falsifier that is adequate for a
    high-confidence Model admits the Model.
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        actor = await _seed_actor(conn, tenant)
        await _install_calibration_offset(
            conn, tenant, actor, proposition_kind="prediction", offset=1.2,
        )

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
                    "scope_actors": [str(actor)],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.65,
                    "confidence_at_assertion": 0.65,
                    "falsifier": {
                        "kind": "prediction_deadline",
                        "evaluate_at": future,
                        "check": "outcome observable by deadline",
                    },
                    "evaluate_at": future,
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert len(validated.claim_ops) == 1
        # Post-calibration confidence is stored on the entry.
        post_conf = validated.claim_ops[0].entry["confidence"]
        assert post_conf > 0.7, (
            f"expected post-calibration conf > 0.7, got {post_conf}"
        )


async def test_deflating_calibration_does_not_require_falsifier(
    fresh_db, tenant, tenant_cleanup,
):
    """
    Raw confidence 0.72 (above threshold) but calibration offset 0.8
    drops it to 0.576 (below threshold). No falsifier required. The
    Model passes — the new ordering clips confidence, applies
    calibration (0.72 → 0.576), clips again, then runs the falsifier
    check against the post-calibration value.
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        actor = await _seed_actor(conn, tenant)
        await _install_calibration_offset(
            conn, tenant, actor, proposition_kind="state", offset=0.8,
        )

        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {"kind": "state", "text": "x"},
                    "natural": "x",
                    "embedding": [0.0] * 768,
                    "scope_actors": [str(actor)],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.72,
                    "confidence_at_assertion": 0.72,
                    # no falsifier — below-threshold post-calibration
                }),
            ],
        )
        validated = await validate(diff, rr, conn, allowed_region=None)
        assert len(validated.claim_ops) == 1
        post_conf = validated.claim_ops[0].entry["confidence"]
        assert post_conf < 0.7, (
            f"expected post-calibration conf < 0.7, got {post_conf}"
        )


async def test_no_calibration_row_is_identity(fresh_db, tenant, tenant_cleanup):
    """
    When no calibration offset matches (no row for the tuple), the
    confidence is unchanged. A high raw confidence above the falsifier
    threshold still requires a falsifier.
    """
    rr = _retrieval_result(tenant)
    async with fresh_db.acquire() as conn:
        actor = await _seed_actor(conn, tenant)
        # No calibration row installed.

        diff = RawDiff(
            trigger_ref=uuid7(), tenant_id=tenant,
            claim_ops=[
                ClaimOp(op="insert", entry={
                    "tenant_id": str(tenant),
                    "born_from_event_id": str(uuid7()),
                    "proposition": {"kind": "state", "text": "x"},
                    "natural": "x",
                    "embedding": [0.0] * 768,
                    "scope_actors": [str(actor)],
                    "scope_entities": [],
                    "scope_temporal": {},
                    "confidence": 0.8,
                    "confidence_at_assertion": 0.8,
                    # no falsifier
                }),
            ],
        )
        from services.think.validator import ValidationFailure
        with pytest.raises(ValidationFailure):
            await validate(diff, rr, conn, allowed_region=None)
