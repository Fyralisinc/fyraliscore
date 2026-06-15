from __future__ import annotations

from datetime import datetime, timezone

from services.reasoning.think.prediction_lifecycle import prepare_prediction_entry


def test_prepare_prediction_entry_reads_falsifier_evaluate_at() -> None:
    evaluate_at = "2026-06-20T12:00:00+00:00"
    entry = prepare_prediction_entry({
        "claim_role": "prediction",
        "proposition": {
            "kind": "prediction",
            "expected": "Atlas renewal risk improves",
            "resolution": "Risk has moved down.",
        },
        "scope_temporal": {},
        "falsifier": {
            "kind": "prediction_deadline",
            "evaluate_at": evaluate_at,
            "check": "risk moved down",
        },
    })

    assert entry["evaluate_at"] == datetime.fromisoformat(evaluate_at)


def test_prepare_prediction_entry_within_window_uses_valid_from_not_born_id() -> None:
    entry = prepare_prediction_entry({
        "claim_role": "prediction",
        "born_from_event_id": "018f0000-0000-7000-8000-000000000001",
        "proposition": {
            "kind": "prediction",
            "expected": "Review clears",
            "resolution": "Review clears within a week.",
        },
        "scope_temporal": {
            "valid_from": "2026-06-10T00:00:00Z",
        },
        "falsifier": {
            "kind": "observation_pattern",
            "pattern": "Review remains blocked",
            "within_window": "P7D",
        },
    })

    assert entry["evaluate_at"] == datetime(
        2026, 6, 17, 0, 0, tzinfo=timezone.utc
    )
