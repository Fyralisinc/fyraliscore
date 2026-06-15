from __future__ import annotations

from uuid import uuid4

from services.platform.execution import inquiry, retrieval_plan
from services.platform.execution.config import InquiryConfig
from services.platform.execution.question_policy import policy_budget
from services.platform.execution.types import (
    InquiryQuestion,
    LearnedRetrievalMotif,
    QuestionPolicySignal,
    RetrievalAction,
)
from services.reasoning.retrieval.primary import TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[{"type": "customer", "label": "AcmeAtlas"}],
        seed_natural_text="AcmeAtlas launch is blocked by review capacity.",
    )


def _question(
    primitive: str = "CONSTRAINT",
    *,
    question_id: str = "Q_CONSTRAINT",
) -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question="Which review capacity constraint blocks AcmeAtlas?",
        primitive=primitive,
        tests_hypotheses=("H1",),
        expected_value=0.9,
        expected_cost=0.2,
        retrieval_target="constraint_evidence",
        stop_condition="constraint found",
        score=0.7,
    )


def _policy_signal() -> QuestionPolicySignal:
    return QuestionPolicySignal(
        signal_type="T1",
        question_primitive="CONSTRAINT",
        attempts=10,
        successes=8,
        utility_score=1.6,
        total_credit=20.0,
        total_cost=4.0,
    )


def test_retrieval_plan_helpers_keep_legacy_inquiry_identity() -> None:
    assert (
        inquiry._compile_motif_retrieval_plan
        is retrieval_plan.compile_motif_retrieval_plan
    )
    assert inquiry._compile_retrieval_plan is retrieval_plan.compile_retrieval_plan
    assert (
        inquiry._compile_static_retrieval_plan
        is retrieval_plan.compile_static_retrieval_plan
    )
    assert inquiry._focused_index_actions is retrieval_plan.focused_index_actions


def test_static_retrieval_plan_includes_focused_and_constraint_actions() -> None:
    cfg = InquiryConfig(focused_index_terms=4, focused_index_max_candidates=40)
    signal = _policy_signal()

    actions = retrieval_plan.compile_static_retrieval_plan(
        _question(),
        _trigger(),
        cfg,
        policy_signal=signal,
    )

    assert [action.path for action in actions] == [
        "focused_index",
        "structural",
        "model_edge",
        "temporal",
        "semantic",
    ]
    focused = actions[0]
    assert focused.target == "question_answerability_scope"
    assert focused.filters["primitive"] == "CONSTRAINT"
    assert focused.filters["seed_entities"] == [
        {"type": "customer", "label": "AcmeAtlas"}
    ]
    assert focused.budget == policy_budget(40, signal)
    assert any(action.target == "constraint_evidence" for action in actions)
    assert any(action.target == "recent_constraint_observations" for action in actions)


def test_focused_index_actions_respect_config_gate() -> None:
    disabled = retrieval_plan.focused_index_actions(
        _question(),
        _trigger(),
        InquiryConfig(focused_index_enabled=False),
        policy_signal=None,
    )

    assert disabled == []


def test_motif_retrieval_plan_overlays_only_safe_static_actions() -> None:
    question = _question()
    cfg = InquiryConfig(retrieval_motif_max_actions=3)
    static_actions = [
        RetrievalAction("Q_CONSTRAINT", "structural", "goal_resource_bridge"),
        RetrievalAction("Q_CONSTRAINT", "semantic", "constraint_evidence", budget=24),
    ]
    motif = LearnedRetrievalMotif(
        id=uuid4(),
        signature={},
        question_primitive="CONSTRAINT",
        plan={
            "actions": [
                {"path": "structural", "target": "goal_resource_bridge", "budget": 8},
                {
                    "path": "semantic",
                    "target": "constraint_evidence",
                    "budget": 5,
                    "stage": 2,
                    "bind_previous_scope": True,
                },
                {"path": "made_up", "target": "unsafe", "budget": 99},
            ]
        },
        utility_score=1.23456,
        success_count=4,
        match_score=0.87654,
    )

    actions = retrieval_plan.compile_motif_retrieval_plan(
        question,
        static_actions,
        motif,
        cfg,
    )

    assert [(action.path, action.target, action.budget) for action in actions] == [
        ("structural", "goal_resource_bridge", 8),
        ("semantic", "constraint_evidence", 5),
    ]
    assert actions[1].filters["_bind_previous_scope"] is True
    assert actions[1].filters["_motif_id"] == str(motif.id)
    assert actions[1].filters["_motif_match_score"] == 0.8765
    assert actions[1].filters["_motif_utility_score"] == 1.2346


def test_compile_retrieval_plan_falls_back_to_static_for_invalid_motif() -> None:
    question = _question("OWNERSHIP", question_id="Q_OWNER")
    cfg = InquiryConfig(focused_index_enabled=False)
    motif = LearnedRetrievalMotif(
        id=uuid4(),
        signature={},
        question_primitive="OWNERSHIP",
        plan={"actions": [{"path": "unsafe", "target": "nope"}]},
        utility_score=1.0,
        success_count=1,
        match_score=0.9,
    )

    cold = retrieval_plan.compile_static_retrieval_plan(question, _trigger(), cfg)
    hot = retrieval_plan.compile_retrieval_plan(
        question,
        _trigger(),
        cfg,
        learned_motif=motif,
    )

    assert hot == cold
