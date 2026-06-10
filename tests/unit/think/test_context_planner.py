from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from lib.llm.provider import CodexProvider, LLMConfig
from services.platform.execution import question_planning_provider
from services.reasoning.retrieval.assembler import AccessContext, ContextBundle
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.reasoning_frame import ReasoningFrame
from services.reasoning.think import context_planner


class _CustomProvider:
    pass


class _ProductionLikeProvider:
    __module__ = "lib.llm.provider"


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        observation_id=uuid4(),
        seed_natural_text="customer onboarding signal",
    )


def test_retrieval_question_planning_provider_gates_custom_doubles(monkeypatch):
    custom = _CustomProvider()
    production_like = _ProductionLikeProvider()

    monkeypatch.delenv("INQUIRY_ALLOW_CUSTOM_LLM_QUESTION_PROVIDER", raising=False)

    assert context_planner.retrieval_question_planning_provider(None) is None
    assert (
        context_planner.retrieval_question_planning_provider(custom)
        is None
    )
    assert (
        context_planner.retrieval_question_planning_provider(production_like)
        is production_like
    )

    monkeypatch.setenv("INQUIRY_ALLOW_CUSTOM_LLM_QUESTION_PROVIDER", "1")

    assert (
        context_planner.retrieval_question_planning_provider(custom)
        is custom
    )


def test_retrieval_question_planning_provider_routes_codex_to_spark_low_effort(
    monkeypatch,
):
    monkeypatch.setattr(
        question_planning_provider,
        "_QUESTION_PLANNING_PROVIDER_CACHE",
        None,
    )
    monkeypatch.setattr(
        question_planning_provider,
        "_QUESTION_PLANNING_PROVIDER_CACHE_KEY",
        None,
    )
    monkeypatch.delenv(
        "INQUIRY_CODEX_LOW_EFFORT_QUESTION_PLANNING",
        raising=False,
    )

    codex = CodexProvider(
        LLMConfig(
            provider="codex",
            api_key="test-codex-key",
            model="gpt-5.3-codex",
            reasoning_effort="medium",
        )
    )

    provider = context_planner.retrieval_question_planning_provider(codex)

    assert isinstance(provider, CodexProvider)
    assert provider is not codex
    assert provider.config.provider == "codex"
    assert provider.config.model == "gpt-5.3-codex-spark"
    assert provider.config.api_key == "test-codex-key"
    assert provider.config.reasoning_effort == "low"
    assert provider.config.timeout_s == 24.0
    assert provider.config.max_retries == 0


def test_retrieval_question_planning_provider_respects_codex_model_override(
    monkeypatch,
):
    monkeypatch.setattr(
        question_planning_provider,
        "_QUESTION_PLANNING_PROVIDER_CACHE",
        None,
    )
    monkeypatch.setattr(
        question_planning_provider,
        "_QUESTION_PLANNING_PROVIDER_CACHE_KEY",
        None,
    )
    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_MODEL", "gpt-5.3-codex")

    codex = CodexProvider(
        LLMConfig(
            provider="codex",
            api_key="test-codex-key",
            model="gpt-5.5",
            reasoning_effort="medium",
        )
    )

    provider = context_planner.retrieval_question_planning_provider(codex)

    assert isinstance(provider, CodexProvider)
    assert provider.config.model == "gpt-5.3-codex"
    assert provider.config.reasoning_effort == "low"


def test_retrieval_question_planning_provider_can_disable_low_effort(monkeypatch):
    monkeypatch.setenv("INQUIRY_CODEX_LOW_EFFORT_QUESTION_PLANNING", "0")
    codex = CodexProvider(
        LLMConfig(
            provider="codex",
            api_key="test-codex-key",
            model="gpt-5.3-codex",
            reasoning_effort="medium",
        )
    )

    assert context_planner.retrieval_question_planning_provider(codex) is codex


@pytest.mark.asyncio
async def test_plan_context_builds_frame_and_preserves_retrieval_notes(
    monkeypatch,
):
    trigger = _trigger()
    retrieval_result = RetrievalResult(trigger=trigger)
    seen_provider = object()

    async def fake_retrieve_for_execution(
        actual_trigger,
        conn,
        *,
        embedder=None,
        llm_provider=None,
        mode=None,
    ):
        assert actual_trigger is trigger
        assert conn == "conn"
        assert embedder == "embedder"
        assert llm_provider is None
        assert mode == "deep"
        return retrieval_result

    def fake_should_run_second_pass(*args, **kwargs):
        return SimpleNamespace(
            run=False,
            trigger_condition="sufficient",
            suggested_dimensions=(),
            reason_detail={"why": "unit-test"},
        )

    async def fake_detect_dynamic_signals(*args, **kwargs):
        return []

    monkeypatch.delenv("INQUIRY_ALLOW_CUSTOM_LLM_QUESTION_PROVIDER", raising=False)
    monkeypatch.setattr(
        context_planner,
        "retrieve_for_execution",
        fake_retrieve_for_execution,
    )
    monkeypatch.setattr(
        context_planner,
        "should_run_second_pass",
        fake_should_run_second_pass,
    )
    monkeypatch.setattr(
        context_planner,
        "log_second_pass_decision",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        context_planner,
        "detect_dynamic_signals",
        fake_detect_dynamic_signals,
    )

    plan = await context_planner.plan_context(
        trigger,
        "conn",  # type: ignore[arg-type]
        embedder="embedder",
        llm_provider=seen_provider,  # type: ignore[arg-type]
    )

    assert plan.inquiry_result is None
    assert plan.retrieval_result is retrieval_result
    assert plan.reasoning_frame.trigger_kind == "T1"
    assert retrieval_result.notes["second_pass_decision"]["run"] is False
    assert retrieval_result.notes["reasoning_frame"]["trigger_kind"] == "T1"


@pytest.mark.asyncio
async def test_assemble_reasoning_context_expands_region_with_augmented_acts(
    monkeypatch,
):
    trigger = _trigger()
    commitment_id = uuid4()
    decision_id = uuid4()
    bundle = ContextBundle()
    retrieval_result = RetrievalResult(trigger=trigger)
    context_plan = context_planner.ContextPlan(
        retrieval_result=retrieval_result,
        inquiry_result=None,
        reasoning_frame=ReasoningFrame.from_trigger(
            trigger,
            retrieval_result=retrieval_result,
        ),
    )

    class FakeConn:
        async def fetch(self, query, tenant_id):
            assert tenant_id == trigger.tenant_id
            if "FROM commitments" in query:
                return [
                    {
                        "id": commitment_id,
                        "tenant_id": tenant_id,
                        "title": "Ship onboarding checklist",
                        "state": "open",
                        "owner_id": None,
                        "due_date": None,
                        "last_state_change_at": None,
                        "created_at": None,
                    }
                ]
            if "FROM decisions" in query:
                return [
                    {
                        "id": decision_id,
                        "tenant_id": tenant_id,
                        "title": "Choose launch segment",
                        "state": "open",
                        "created_at": None,
                        "last_state_change_at": None,
                    }
                ]
            raise AssertionError(query)

    async def fake_assemble_context(retrieval_result_arg, access, conn):
        assert retrieval_result_arg is retrieval_result
        assert isinstance(access, AccessContext)
        assert access.tenant_id == trigger.tenant_id
        assert isinstance(conn, FakeConn)
        return bundle

    async def fake_load_actor_operating_context(*args, **kwargs):
        assert kwargs["tenant_id"] == trigger.tenant_id
        assert kwargs["actor_ids"] == []
        return ["ctx"]

    monkeypatch.setattr(
        context_planner,
        "assemble_context",
        fake_assemble_context,
    )
    monkeypatch.setattr(
        context_planner,
        "load_actor_operating_context",
        fake_load_actor_operating_context,
    )
    monkeypatch.setattr(
        context_planner,
        "summarize_actor_operating_context",
        lambda contexts: "actor-summary",
    )

    reasoning_context = await context_planner.assemble_reasoning_context(
        context_plan,
        trigger,
        FakeConn(),  # type: ignore[arg-type]
        expanded_region={("goal", "existing")},
    )

    assert reasoning_context.bundle is bundle
    assert reasoning_context.actor_operating_summary == "actor-summary"
    assert ("goal", "existing") in reasoning_context.allowed_region
    assert (
        "commitment",
        str(commitment_id),
    ) in reasoning_context.allowed_region
    assert ("decision", str(decision_id)) in reasoning_context.allowed_region
    assert bundle.acts_summary["commitments"][0].title == (
        "Ship onboarding checklist"
    )
    assert bundle.acts_summary["decisions"][0].title == "Choose launch segment"
