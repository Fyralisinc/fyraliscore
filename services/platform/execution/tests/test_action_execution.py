from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from services.platform.execution import action_execution, inquiry
from services.platform.execution.types import InquiryQuestion, RetrievalAction
from services.reasoning.retrieval.pathways import PathwayResult


def _question() -> InquiryQuestion:
    return InquiryQuestion(
        question_id="Q1",
        question="Which dependency blocks launch?",
        primitive="DEPENDENCY",
        tests_hypotheses=("H1",),
        expected_value=0.8,
        expected_cost=0.2,
        retrieval_target="blocker",
        stop_condition="dependency found",
        score=0.7,
    )


def test_inquiry_private_aliases_point_to_action_execution_module() -> None:
    assert inquiry._execute_action is action_execution._execute_action
    assert (
        inquiry._execute_question_retrieval_actions
        is action_execution._execute_question_retrieval_actions
    )
    assert inquiry._action_timing_note is action_execution._action_timing_note
    assert inquiry._QuestionRetrievalPlan is action_execution._QuestionRetrievalPlan
    assert inquiry._ActionExecutionRecord is action_execution._ActionExecutionRecord


def test_action_timing_note_includes_cache_and_motif_details() -> None:
    action = RetrievalAction(
        "Q1",
        "semantic",
        "constraint_evidence",
        filters={
            "_motif_id": str(uuid4()),
            "_motif_stage": 2,
            "_motif_match_score": 0.8,
            "_motif_utility_score": 1.2,
            "_bound_scope": {"model_count": 1},
        },
    )
    result = PathwayResult(
        models=[SimpleNamespace()],
        observations=[SimpleNamespace()],
        resources=[SimpleNamespace()],
        source_pathway="B",
    )

    note = action_execution._action_timing_note(
        action,
        result,
        elapsed_ms=12,
        cache_hit=True,
    )

    assert note["question_id"] == "Q1"
    assert note["models"] == 1
    assert note["source_pathway"] == "B"
    assert note["cache_hit"] is True
    assert note["motif_stage"] == 2
    assert note["bound_scope"] == {"model_count": 1}


def test_action_timing_note_includes_reconstruction_details() -> None:
    action = RetrievalAction(
        "Q1",
        "semantic",
        "owner_evidence",
        filters={
            "_reconstruction_stage": 2,
            "_reconstruction_round": 3,
            "_reconstruction_active_cues": ["owner", "audit"],
            "_reconstruction_cue_count": 2,
            "_bound_scope": {"model_count": 2},
        },
    )

    note = action_execution._action_timing_note(
        action,
        PathwayResult(source_pathway="B"),
        elapsed_ms=4,
        cache_hit=False,
    )

    assert action_execution._action_stage(action) == 2
    assert note["reconstruction_stage"] == 2
    assert note["reconstruction_round"] == 3
    assert note["reconstruction_cue_count"] == 2
    assert note["reconstruction_active_cues"] == ["owner", "audit"]
    assert note["bound_scope"] == {"model_count": 2}


def test_question_retrieval_plan_defaults_to_empty_action_lists() -> None:
    plan = action_execution._QuestionRetrievalPlan(question=_question())

    assert plan.actions_to_run == []
    assert plan.skipped_timing_notes == []
    assert plan.learned_motif is None
