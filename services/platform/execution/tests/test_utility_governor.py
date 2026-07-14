from __future__ import annotations

import pytest

from lib.shared.utility_governor import (
    downstream_trigger_utility,
    question_planning_utility,
)


def test_question_planning_governor_skips_when_primary_context_is_strong() -> None:
    decision = question_planning_utility(
        trigger_kind="T1",
        trigger_text="HarborRail renewal risk has enough audit evidence",
        deterministic_primitives=[
            "COUNTEREVIDENCE",
            "DEPENDENCY",
            "COMMITMENT",
            "OWNERSHIP",
            "GOAL_IMPACT",
        ],
        deterministic_count=5,
        evidence_count=16,
        model_count=7,
        unknown_count=1,
        round_index=2,
    )

    assert decision.decision == "suppress"
    assert decision.reason == "deterministic_questions_cover_high_value_uncertainty"
    assert decision.score >= 0.68
    assert decision.features["critical_primitive_coverage"] == 4


@pytest.mark.parametrize(
    ("evidence_count", "model_count", "unknown_count"),
    [(0, 0, 2), (4, 8, 1), (8, 1, 6)],
)
def test_question_planning_governor_runs_when_context_is_still_sparse(
    evidence_count: int,
    model_count: int,
    unknown_count: int,
) -> None:
    decision = question_planning_utility(
        trigger_kind="T1",
        trigger_text="HarborRail blocker needs ownership clarity",
        deterministic_primitives=[
            "COUNTEREVIDENCE",
            "DEPENDENCY",
            "COMMITMENT",
            "OWNERSHIP",
        ],
        deterministic_count=4,
        evidence_count=evidence_count,
        model_count=model_count,
        unknown_count=unknown_count,
        round_index=1,
    )

    assert decision.decision == "run"


def test_downstream_governor_runs_high_leverage_causal_edges() -> None:
    decision = downstream_trigger_utility(
        candidate_kind="edge",
        edge_kind="blocks",
        basis="causal_hypothesis",
        source="latent_topology",
        leverage_score=0.57,
        member_count=2,
        metadata={
            "topology": {
                "score_components": {
                    "actionability": 0.72,
                    "business_leverage": 0.66,
                }
            }
        },
    )

    assert decision.decision == "run"
    assert decision.score >= 0.66
    assert decision.features["high_leverage_edge"] is True


def test_downstream_governor_suppresses_generic_low_specificity_edges() -> None:
    decision = downstream_trigger_utility(
        candidate_kind="edge",
        edge_kind="same_issue_as",
        basis="topology_suggested",
        source="latent_topology",
        leverage_score=0.69,
        member_count=2,
        metadata={
            "topology": {
                "score_components": {
                    "actionability": 0.20,
                    "business_leverage": 0.25,
                }
            }
        },
    )

    assert decision.decision == "suppress"
    assert decision.reason == "candidate_is_low_specificity_or_redundant_for_followup_think"
    assert decision.features["generic_edge"] is True


def test_downstream_governor_suppresses_weak_latent_topology_edges() -> None:
    decision = downstream_trigger_utility(
        candidate_kind="edge",
        edge_kind="blocks",
        basis="topology_suggested",
        source="latent_topology",
        leverage_score=0.66,
        member_count=2,
        metadata={
            "topology": {
                "score_components": {
                    "actionability": 0.18,
                    "business_leverage": 0.24,
                    "evidence_quality": 0.42,
                    "novelty": 0.36,
                }
            }
        },
    )

    assert decision.decision == "suppress"
    assert decision.features["is_latent_topology"] is True
    assert decision.features["high_leverage_edge"] is True


def test_downstream_governor_suppresses_borderline_latent_edges_without_action_surface() -> None:
    decision = downstream_trigger_utility(
        candidate_kind="edge",
        edge_kind="early_warning_for",
        basis="topology_suggested",
        source="latent_topology",
        leverage_score=0.72,
        member_count=2,
        metadata={
            "topology": {
                "score_components": {
                    "actionability": 0.45,
                    "business_leverage": 0.48,
                    "evidence_quality": 0.52,
                    "novelty": 0.50,
                }
            }
        },
    )

    assert decision.decision == "suppress"
    assert decision.score < decision.features["run_threshold"]
    assert decision.features["high_leverage_edge"] is True


def test_downstream_governor_runs_exceptionally_strong_situations() -> None:
    decision = downstream_trigger_utility(
        candidate_kind="situation",
        edge_kind=None,
        basis="topology_suggested",
        source="latent_topology",
        leverage_score=0.78,
        member_count=5,
        metadata={
            "topology": {
                "score_components": {
                    "actionability": 0.69,
                    "business_leverage": 0.74,
                    "structural_surprise": 0.61,
                }
            }
        },
    )

    assert decision.decision == "run"
