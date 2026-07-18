from __future__ import annotations

from lib.evaluation.epistemic_repair.p6_postfreeze_evidence import _signal_fate_rows


def test_postfreeze_maps_candidate_receipt_without_canonical_promotion() -> None:
    observation_id = "11111111-1111-4111-8111-111111111111"
    receipt = {
        "decision_id": "receipt-1",
        "context_item_kind": "candidate",
        "decision_fate": "justified_noop",
        "result_object_kind": "open_question",
        "source_signal_ids": ["P6-B01-S02"],
    }

    fates, dispositions = _signal_fate_rows(
        {observation_id: "P6-B01-S02"},
        observed_ids={observation_id},
        boundary_by_signal={},
        mention_rows=[],
        claims=[],
        context_items=[receipt],
    )

    assert dispositions == [receipt]
    assert fates == [{
        "signal_id": "P6-B01-S02",
        "observation_id": observation_id,
        "boundary_fate": None,
        "mention_fate": "no_mention",
        "mutation_fate": "open_question",
        "mutation_reason": "nonassertable_signal_retained_outside_truth",
        "disposition_decision_id": "receipt-1",
    }]


def test_canonical_promotion_remains_visible_over_candidate_receipt() -> None:
    observation_id = "22222222-2222-4222-8222-222222222222"
    fates, _ = _signal_fate_rows(
        {observation_id: "P6-B01-S04"},
        observed_ids={observation_id},
        boundary_by_signal={},
        mention_rows=[],
        claims=[{"evidence_signal_ids": ["P6-B01-S04"]}],
        context_items=[{
            "decision_id": "receipt-2",
            "context_item_kind": "candidate",
            "decision_fate": "justified_noop",
            "result_object_kind": "clarification_residual",
            "source_signal_ids": ["P6-B01-S04"],
        }],
    )

    assert fates[0]["mutation_fate"] == "canonical_mutation"

