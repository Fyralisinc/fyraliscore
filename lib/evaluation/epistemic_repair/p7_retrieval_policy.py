"""Explicit evaluator-owned retrieval intervention for the P7 hidden arm.

The intervention is installed only around an evaluator invocation.  It wraps
the production context-planning boundary, is task-local through a ContextVar,
and has no environment switch or production launch path.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from typing import AsyncIterator, Literal

from lib.shared.errors import InvariantViolation
from services.reasoning.think.context_planner import ContextPlan
from services.reasoning.think.reasoning_frame import ReasoningFrame


P7RetrievalPolicy = Literal["normal", "hide_models"]
_policy: ContextVar[P7RetrievalPolicy] = ContextVar(
    "p7_evaluator_retrieval_policy", default="normal"
)
_single_provider: ContextVar[bool] = ContextVar(
    "p7_evaluator_single_provider", default=False
)
_installed = False


def _strip_models(plan: ContextPlan) -> ContextPlan:
    result = plan.retrieval_result
    result.models.clear()
    result.model_scores.clear()
    for pathway in result.pathway_results:
        pathway.models.clear()
    result.notes["p7_evaluator_retrieval_policy"] = "hide_models"
    frame = ReasoningFrame.from_trigger(result.trigger, retrieval_result=result)
    result.notes["reasoning_frame"] = frame.to_dict()
    return replace(plan, reasoning_frame=frame)


def assert_no_model_context(plan: ContextPlan) -> None:
    """Fail closed if any model survives the hidden-memory intervention."""

    leaked_pathways = [
        getattr(pathway, "source_pathway", "unknown")
        for pathway in plan.retrieval_result.pathway_results
        if pathway.models
    ]
    candidate_ids = tuple(plan.reasoning_frame.candidate_model_ids)
    seed_ids = tuple(plan.reasoning_frame.seed_model_ids)
    if (
        plan.retrieval_result.models
        or plan.retrieval_result.model_scores
        or leaked_pathways
        or candidate_ids
        or seed_ids
    ):
        raise InvariantViolation(
            "P7_HIDDEN_MODEL_CONTEXT_LEAK",
            "memory-hidden arm allowed Models to enter production reasoning context",
            retrieved_models=len(plan.retrieval_result.models),
            pathway_leaks=leaked_pathways,
            candidate_model_ids=tuple(map(str, candidate_ids)),
            seed_model_ids=tuple(map(str, seed_ids)),
        )


def install_production_retrieval_policy_dispatch() -> None:
    """Install one task-local dispatch wrapper at Think's imported boundary."""

    global _installed
    if _installed:
        return
    from services.reasoning.think import run_pipeline
    from services.reasoning.think import context_planner
    from services.platform.execution import question_planning

    original = run_pipeline.plan_context
    original_assemble = run_pipeline.assemble_reasoning_context
    original_question_provider = context_planner.retrieval_question_planning_provider
    original_fallback_provider = (
        question_planning.select_question_planning_fallback_provider
    )

    def governed_question_provider(provider):
        if _single_provider.get():
            return provider
        return original_question_provider(provider)

    def governed_fallback_provider(*args, **kwargs):
        if _single_provider.get():
            return None
        return original_fallback_provider(*args, **kwargs)

    async def governed_plan_context(*args, **kwargs):
        plan = await original(*args, **kwargs)
        if _policy.get() == "hide_models":
            plan = _strip_models(plan)
            assert_no_model_context(plan)
        return plan

    async def governed_assemble_reasoning_context(*args, **kwargs):
        context = await original_assemble(*args, **kwargs)
        if _policy.get() == "hide_models" and context.bundle.models:
            raise InvariantViolation(
                "P7_HIDDEN_MODEL_CONTEXT_LEAK",
                "memory-hidden arm allowed Models into the assembled production prompt",
                assembled_model_ids=tuple(
                    str(model.id) for model in context.bundle.models
                ),
            )
        return context

    run_pipeline.plan_context = governed_plan_context
    run_pipeline.assemble_reasoning_context = governed_assemble_reasoning_context
    context_planner.retrieval_question_planning_provider = governed_question_provider
    question_planning.select_question_planning_fallback_provider = (
        governed_fallback_provider
    )
    question_planning.select_question_planning_provider = governed_question_provider
    _installed = True


@asynccontextmanager
async def production_retrieval_policy(
    policy: P7RetrievalPolicy,
) -> AsyncIterator[None]:
    """Explicitly scope a P7 policy to the current asynchronous arm task."""

    install_production_retrieval_policy_dispatch()
    token = _policy.set(policy)
    provider_token = _single_provider.set(True)
    try:
        yield
    finally:
        _single_provider.reset(provider_token)
        _policy.reset(token)


__all__ = [
    "P7RetrievalPolicy",
    "assert_no_model_context",
    "install_production_retrieval_policy_dispatch",
    "production_retrieval_policy",
]
