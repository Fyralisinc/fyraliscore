from __future__ import annotations

from uuid import uuid4

from services.reasoning.think.diff_schema import ClaimOp, ValidatedDiff
from services.reasoning.think.post_commit import _predictions_payload


def test_predictions_payload_includes_claim_op_prediction_inserts() -> None:
    tenant_id = uuid4()
    trigger_ref = uuid4()
    diff = ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "natural": "Atlas renewal risk should recover by Friday.",
                    "confidence": 0.66,
                    "evaluate_at": "2026-06-20T00:00:00+00:00",
                    "proposition": {
                        "kind": "prediction",
                        "claim_role": "prediction",
                        "expected": "Atlas renewal risk recovers.",
                    },
                    "scope_actors": [],
                    "scope_entities": [],
                    "scope_temporal": {
                        "valid_from": "2026-06-17T00:00:00+00:00",
                        "valid_until": None,
                    },
                    "falsifier": {
                        "kind": "prediction_deadline",
                        "evaluate_at": "2026-06-20T00:00:00+00:00",
                        "check": {"field": "renewal_risk", "state": "not_recovered"},
                    },
                },
            )
        ],
        new_predictions=[],
    )

    payload = _predictions_payload(diff)

    assert len(payload["predictions"]) == 1
    prediction = payload["predictions"][0]
    assert prediction["tenant_id"] == str(tenant_id)
    assert prediction["trigger_ref"] == str(trigger_ref)
    assert prediction["source"] == "claim_ops"
    assert prediction["evaluate_at"] == "2026-06-20T00:00:00+00:00"


def test_predictions_payload_dedupes_legacy_new_prediction_bucket() -> None:
    tenant_id = uuid4()
    trigger_ref = uuid4()
    entry = {
        "natural": "Atlas renewal risk should recover by Friday.",
        "confidence": 0.66,
        "evaluate_at": "2026-06-20T00:00:00+00:00",
        "proposition": {
            "kind": "prediction",
            "claim_role": "prediction",
            "expected": "Atlas renewal risk recovers.",
        },
        "scope_actors": [],
        "scope_entities": [],
        "scope_temporal": {
            "valid_from": "2026-06-17T00:00:00+00:00",
            "valid_until": None,
        },
    }
    op = ClaimOp(op="insert", entry=dict(entry))
    diff = ValidatedDiff(
        trigger_ref=trigger_ref,
        tenant_id=tenant_id,
        claim_ops=[op],
        new_predictions=[ClaimOp(op="insert", entry=dict(entry))],
    )

    payload = _predictions_payload(diff)

    assert len(payload["predictions"]) == 1
    assert payload["predictions"][0]["source"] == "new_predictions"
