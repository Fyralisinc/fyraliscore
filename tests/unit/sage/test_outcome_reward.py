from __future__ import annotations

from uuid import uuid4

from services.reasoning.sage.outcome_reward import build_reward_features


def test_reward_features_give_high_credit_for_durable_used_context() -> None:
    outcome = build_reward_features(
        packet={"budget": {"estimated_tokens_used": 3000}},
        ops_applied={
            "claim_ops": [{"op": "insert", "model_id": str(uuid4())}],
            "state_changes_emitted": 1,
            "context_use": {
                "selected_context_count": 2,
                "selected_context_used": True,
            },
        },
        evidence_items=[{}, {}],  # type: ignore[list-item]
        omitted_rows=[],
        used_evidence_ids={uuid4()},
        used_node_ids=[uuid4()],
        run_status="success",
        counterevidence_retrieved=0,
        counterevidence_in_packet=0,
        duplicate_evidence=0,
        packet_tokens=3000,
    )

    features = outcome.reward_features
    assert features["durable_fate_rate"] == 1.0
    assert features["selected_context_use"] == 1.0
    assert features["selected_unused_rate"] == 0.0
    assert features["validation_drop_rate"] == 0.0
    assert features["residual_creation_rate"] == 0.0
    assert features["retrieval_outcome_reward"] > 0.70


def test_reward_features_downshift_selected_unused_noop() -> None:
    outcome = build_reward_features(
        packet={"budget": {"estimated_tokens_used": 12000}},
        ops_applied={
            "claim_ops": [],
            "edge_ops": [],
            "context_use": {
                "selected_context_count": 3,
                "selected_context_used": False,
                "context_use_grade": "unused_selected_context",
            },
        },
        evidence_items=[{}, {}, {}],  # type: ignore[list-item]
        omitted_rows=[],
        used_evidence_ids=set(),
        used_node_ids=[],
        run_status="success",
        counterevidence_retrieved=0,
        counterevidence_in_packet=0,
        duplicate_evidence=0,
        packet_tokens=12000,
    )

    features = outcome.reward_features
    assert features["durable_fate_rate"] == 0.0
    assert features["selected_context_use"] == 0.0
    assert features["selected_unused_rate"] == 1.0
    assert features["retrieval_outcome_reward"] < 0.20


def test_reward_features_penalize_residual_creation_and_validation_drops() -> None:
    outcome = build_reward_features(
        packet={},
        ops_applied={
            "residual_creations": {"count": 1},
            "apply_dropped_op_count": 1,
            "context_use": {
                "selected_context_count": 1,
                "selected_context_used": True,
            },
        },
        evidence_items=[{}],  # type: ignore[list-item]
        omitted_rows=[],
        used_evidence_ids={uuid4()},
        used_node_ids=[],
        run_status="success",
        counterevidence_retrieved=0,
        counterevidence_in_packet=0,
        duplicate_evidence=0,
        packet_tokens=1000,
        validation_error_count=1,
    )

    features = outcome.reward_features
    assert features["durable_fate_rate"] == 0.0
    assert features["residual_creation_rate"] == 1.0
    assert features["validation_drop_rate"] > 0.60
    assert features["retrieval_outcome_reward"] < 0.30


def test_reward_features_track_omitted_later_requested_rate() -> None:
    outcome = build_reward_features(
        packet={},
        ops_applied={"omitted_later_requested": {"count": 1}},
        evidence_items=[{}],  # type: ignore[list-item]
        omitted_rows=[{"id": "a"}, {"id": "b"}],  # type: ignore[list-item]
        used_evidence_ids=set(),
        used_node_ids=[],
        run_status="success",
        counterevidence_retrieved=0,
        counterevidence_in_packet=0,
        duplicate_evidence=0,
        packet_tokens=1000,
    )

    assert outcome.reward_features["omitted_later_requested_rate"] == 0.5
