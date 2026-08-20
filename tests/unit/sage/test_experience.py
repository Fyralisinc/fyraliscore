from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from services.reasoning.sage.experience import build_experience_loop_report
from services.reasoning.sage.inquiry_traces.types import OutcomeEventRow
from services.reasoning.sage.topology_optimizer.optimizer import (
    _experience_policy_effects_from_events,
    _optimizer_experience_loop_report,
)


def test_experience_report_is_idle_without_events_or_policy_effects() -> None:
    report = build_experience_loop_report(
        [],
        policy_effects={},
    )

    assert report.status == "idle"
    assert report.closed is False
    assert report.closure_score == 0.0
    assert report.blockers == (
        "no_outcome_events",
        "no_evaluation_events",
        "no_policy_effects",
        "no_future_behavior_levers",
    )
    assert report.optimizer_metrics()["experience_loop_closed"] == 0.0


def test_experience_report_distinguishes_sensed_from_evaluated() -> None:
    sensed = build_experience_loop_report(
        [{"event_type": "retrieved_evidence_used_in_packet"}],
        policy_effects={},
    )
    evaluated = build_experience_loop_report(
        [{"event_type": "node_used_in_valid_diff"}],
        policy_effects={},
    )

    assert sensed.status == "sensed"
    assert sensed.closure_score == 0.2
    assert "no_evaluation_events" in sensed.blockers
    assert evaluated.status == "evaluated"
    assert evaluated.closure_score == 0.5
    assert "no_evaluation_events" not in evaluated.blockers
    assert "no_policy_effects" in evaluated.blockers


def test_experience_report_closes_when_outcomes_become_future_policy() -> None:
    report = build_experience_loop_report(
        [
            SimpleNamespace(event_type="node_used_in_valid_diff"),
            SimpleNamespace(event_type="outcome_quality_assessed"),
            {"event_type": "reader_decision_low_value"},
        ],
        policy_effects={
            "affordance_reinforces": 1,
            "negative_memory_inserts": 2,
            "question_policy_updates": 1,
            "region_refreshes": 3,
        },
        canonical_candidate_count=2,
    )

    assert report.status == "metabolized"
    assert report.closed is True
    assert report.closure_score == 1.0
    assert report.outcome_event_count == 3
    assert report.evaluation_event_count == 3
    assert report.policy_effect_count == 7
    assert report.canonical_candidate_count == 2
    assert report.future_behavior_levers == (
        "affordance_policy",
        "negative_memory",
        "question_policy",
    )
    metrics = report.optimizer_metrics()
    assert metrics["experience_policy_effects"] == 7.0
    assert metrics["experience_future_behavior_levers"] == 3.0
    assert metrics["experience_loop_closed"] == 1.0


def test_experience_report_ignores_unknown_policy_effect_keys() -> None:
    report = build_experience_loop_report(
        [{"event_type": "outcome_quality_assessed"}],
        policy_effects={"diagnostic_only_counter": 5},
    )

    assert report.status == "evaluated"
    assert report.policy_effect_count == 5
    assert report.future_behavior_levers == ()
    assert "no_policy_effects" not in report.blockers
    assert "no_future_behavior_levers" in report.blockers


def test_optimizer_extracts_policy_effects_from_outcome_events() -> None:
    session_id = uuid4()
    event = OutcomeEventRow(
        inquiry_session_id=session_id,
        event_type="outcome_quality_assessed",
        payload={
            "policy_effects": {
                "negative_memory_inserts": 1,
                "question_policy_updates": 2,
                "diagnostic_only_counter": 99,
                "shortcut_decays": "bad",
            }
        },
    )

    effects = _experience_policy_effects_from_events([event])

    assert effects["negative_memory_inserts"] == 1
    assert effects["question_policy_updates"] == 2
    assert "diagnostic_only_counter" not in effects
    assert "shortcut_decays" not in effects


def test_optimizer_experience_adapter_reports_metabolized_loop() -> None:
    session_id = uuid4()
    event = OutcomeEventRow(
        inquiry_session_id=session_id,
        event_type="node_used_in_valid_diff",
        payload={"model_id": str(uuid4())},
    )

    report = _optimizer_experience_loop_report(
        [event],
        affordance_reinforces=1,
        affordance_decays=0,
        shortcut_creates_or_bumps=0,
        shortcut_decays=0,
        negative_memory_inserts=0,
        question_policy_updates=1,
        region_refreshes=0,
        structural_models_written=1,
        structural_edges_written=0,
        canonical_validation_enqueued=1,
    )

    assert report.status == "metabolized"
    assert report.closed is True
    assert report.future_behavior_levers == (
        "affordance_policy",
        "question_policy",
        "structural_features",
    )
    assert report.optimizer_metrics()["experience_closure_score"] == 1.0
