from __future__ import annotations

from uuid import uuid4

import pytest

from services.evaluation.epistemic_repair.p7_retrieval_policy import (
    _strip_models,
    assert_no_model_context,
    production_retrieval_policy,
)
from lib.shared.errors import InvariantViolation
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.context_planner import ContextPlan
from services.reasoning.think.reasoning_frame import ReasoningFrame


class _Model:
    def __init__(self, model_id):
        self.id = model_id


def _plan() -> ContextPlan:
    model = _Model(uuid4())
    trigger = TriggerContext(kind="T1", tenant_id=uuid4())
    result = RetrievalResult(
        trigger=trigger,
        models=[model],
        pathway_results=[PathwayResult(models=[model], source_pathway="A")],
        model_scores={model.id: 0.9},
    )
    frame = ReasoningFrame.from_trigger(trigger, retrieval_result=result)
    return ContextPlan(retrieval_result=result, inquiry_result=None, reasoning_frame=frame)


def test_hidden_policy_removes_models_from_every_prompt_facing_surface() -> None:
    plan = _strip_models(_plan())
    assert_no_model_context(plan)
    assert plan.retrieval_result.models == []
    assert plan.retrieval_result.pathway_results[0].models == []
    assert plan.retrieval_result.model_scores == {}
    assert plan.reasoning_frame.candidate_model_ids == ()
    assert plan.retrieval_result.notes["p7_evaluator_retrieval_policy"] == "hide_models"


def test_hidden_policy_assertion_fails_closed_on_leak() -> None:
    with pytest.raises(InvariantViolation, match="allowed Models to enter"):
        assert_no_model_context(_plan())


@pytest.mark.asyncio
async def test_p7_scope_pins_question_planning_to_source_and_disables_fallback() -> None:
    from services.platform.execution import question_planning

    source = object()
    failed = object()
    async with production_retrieval_policy("normal"):
        assert question_planning.select_question_planning_provider(source) is source
        assert (
            question_planning.select_question_planning_fallback_provider(source, failed)
            is None
        )
