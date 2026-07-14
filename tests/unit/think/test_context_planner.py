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
        is None
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


def test_think_inquiry_config_defaults_to_models_only(monkeypatch):
    monkeypatch.delenv("THINK_INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE", raising=False)

    cfg = context_planner._think_inquiry_config()

    assert cfg.context_packet_evidence_mode == "models_only"


def test_think_inquiry_config_allows_explicit_rollback(monkeypatch):
    monkeypatch.setenv("THINK_INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE", "model_first")

    cfg = context_planner._think_inquiry_config()

    assert cfg.context_packet_evidence_mode == "model_first"


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


def test_retrieval_question_planning_provider_rejects_non_codex_provider():
    provider = _ProductionLikeProvider()
    provider.config = SimpleNamespace(provider="deepseek")

    assert context_planner.retrieval_question_planning_provider(provider) is None


def test_retrieval_question_planning_provider_always_uses_low_effort_codex():
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
    assert provider.config.reasoning_effort == "low"


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
        read_pool=None,
        mode=None,
        config=None,
    ):
        assert actual_trigger is trigger
        assert conn == "conn"
        assert embedder == "embedder"
        assert llm_provider is None
        assert read_pool is None
        assert mode == "deep"
        assert config.context_packet_evidence_mode == "models_only"
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
    candidate_actor_id = uuid4()
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

    async def fake_assemble_context(
        retrieval_result_arg,
        access,
        conn,
        *,
        config=None,
        read_pool=None,
    ):
        assert retrieval_result_arg is retrieval_result
        assert isinstance(access, AccessContext)
        assert access.tenant_id == trigger.tenant_id
        assert isinstance(conn, FakeConn)
        assert config is not None
        assert read_pool is None
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

    async def fake_build_substrate_candidates(
        conn,
        *,
        tenant_id,
        observations,
        models,
        run_id=None,
    ):
        assert isinstance(conn, FakeConn)
        assert tenant_id == trigger.tenant_id
        assert observations is bundle.observations
        assert models is bundle.models
        assert run_id is None
        return [
            {
                "id": str(candidate_actor_id),
                "kind": "actor",
                "label": "Candidate Actor",
                "scope_ref": {
                    "type": "candidate_actor",
                    "id": str(candidate_actor_id),
                },
            }
        ]

    monkeypatch.setattr(
        context_planner,
        "build_substrate_candidates",
        fake_build_substrate_candidates,
    )

    async def fake_augment_context(*, conn, trigger, bundle, allowed_region):
        commitments = await conn.fetch("FROM commitments", trigger.tenant_id)
        decisions = await conn.fetch("FROM decisions", trigger.tenant_id)
        bundle.acts_summary["commitments"] = [
            SimpleNamespace(title=row["title"]) for row in commitments
        ]
        bundle.acts_summary["decisions"] = [
            SimpleNamespace(title=row["title"]) for row in decisions
        ]
        return sorted(
            set(allowed_region)
            | {("commitment", str(commitment_id)), ("decision", str(decision_id))}
        )

    monkeypatch.setattr(
        context_planner,
        "augment_context",
        fake_augment_context,
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
    assert (
        "candidate_actor",
        str(candidate_actor_id),
    ) in reasoning_context.allowed_region
    assert bundle.notes["substrate_candidate_region_count"] == 1
    assert bundle.notes["substrate_candidates"][0]["kind"] == "actor"
    assert bundle.acts_summary["commitments"][0].title == (
        "Ship onboarding checklist"
    )
    assert bundle.acts_summary["decisions"][0].title == "Choose launch segment"
