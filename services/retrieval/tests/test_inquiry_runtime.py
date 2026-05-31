from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import pytest

from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.types import ModelCreate
from services.models.repo import ModelsRepo
from services.execution.inquiry import (
    Hypothesis,
    InquiryConfig,
    InquiryQuestion,
    _candidate_questions_for_round,
    _has_broad_signal_language,
    _merge_llm_and_safety_questions,
    run_inquiry_retrieval,
)
from services.retrieval.assembler import AccessContext, assemble_context
from services.retrieval.primary import TriggerContext
from services.retrieval.primary import RetrievalResult
from services.retrieval.tests._fixtures import build_fixture, make_embedding


pytestmark = pytest.mark.integration


class _BadQuestionEmbedder:
    async def embed(self, _text: str) -> list[float]:
        return [0.0]


class _ScriptedQuestionProvider(LLMProvider):
    def __init__(self, responses: list[str]):
        super().__init__(LLMConfig(provider="anthropic", api_key="test", model="m"))
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        self.calls.append(
            {
                "system": system,
                "user": user,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "schema_hint": schema_hint,
            }
        )
        return self.responses.pop(0)


class _SlowQuestionProvider(LLMProvider):
    def __init__(self):
        super().__init__(LLMConfig(provider="anthropic", api_key="test", model="m"))

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        await asyncio.sleep(1.0)
        return "{}"


def test_broad_signal_language_does_not_treat_split_across_as_portfolio():
    assert not _has_broad_signal_language(
        "the named owner is split across escalations and the delivery date is slipping"
    )
    assert _has_broad_signal_language(
        "board update across all enterprise customers: renewal risk is rising"
    )


async def test_question_planning_timeout_falls_back_to_deterministic(monkeypatch, tenant):
    monkeypatch.setenv("INQUIRY_LLM_QUESTION_TIMEOUT_SECONDS", "0.01")
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_natural_text="customer launch depends on security approval",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
    )
    baseline = RetrievalResult(trigger=trigger)
    questions, note = await _candidate_questions_for_round(
        trigger,
        baseline,
        (
            Hypothesis(
                id="H1",
                claim="The approval dependency may block launch.",
                confidence=0.5,
                impact_if_true="high",
            ),
        ),
        {},
        {"counterevidence"},
        llm_provider=_SlowQuestionProvider(),
        config=InquiryConfig(),
        round_index=1,
    )
    assert note["mode"] == "deterministic_fallback"
    assert note["reason"] == "TimeoutError"
    assert questions


def test_safety_question_boosts_same_primitive_llm_priority():
    llm_goal = InquiryQuestion(
        question_id="Q_GOAL_IMPACT",
        question="Does this affect a customer goal?",
        primitive="GOAL_IMPACT",
        tests_hypotheses=("H1",),
        expected_value=0.55,
        expected_cost=0.30,
        retrieval_target="goal_resource_bridge",
        stop_condition="goal impact known",
        score=0.37,
    )
    safety_goal = InquiryQuestion(
        question_id="Q_GOAL_IMPACT",
        question="Which goal, customer, or resource is affected?",
        primitive="GOAL_IMPACT",
        tests_hypotheses=("H1",),
        expected_value=0.86,
        expected_cost=0.20,
        retrieval_target="goal_resource_bridge",
        stop_condition="goal impact known",
        score=0.81,
    )

    merged, added = _merge_llm_and_safety_questions([llm_goal], [safety_goal])

    assert added == 0
    assert merged[0].primitive == "GOAL_IMPACT"
    assert merged[0].question == llm_goal.question
    assert merged[0].score == safety_goal.score


def test_high_value_safety_question_is_added_when_llm_omits_it():
    llm_questions = [
        InquiryQuestion(
            question_id="Q_COUNTEREVIDENCE",
            question="What evidence would disprove this?",
            primitive="COUNTEREVIDENCE",
            tests_hypotheses=("H1",),
            expected_value=0.7,
            expected_cost=0.2,
            retrieval_target="counterevidence",
            stop_condition="counterevidence known",
            score=0.56,
        ),
        InquiryQuestion(
            question_id="Q_COMMITMENT",
            question="Which promises are implicated?",
            primitive="COMMITMENT",
            tests_hypotheses=("H1",),
            expected_value=0.6,
            expected_cost=0.2,
            retrieval_target="commitments",
            stop_condition="commitments known",
            score=0.48,
        ),
        InquiryQuestion(
            question_id="Q_OWNERSHIP",
            question="Who owns the work?",
            primitive="OWNERSHIP",
            tests_hypotheses=("H1",),
            expected_value=0.58,
            expected_cost=0.2,
            retrieval_target="owners",
            stop_condition="owners known",
            score=0.46,
        ),
        InquiryQuestion(
            question_id="Q_RECURRENCE",
            question="Has this happened before?",
            primitive="RECURRENCE",
            tests_hypotheses=("H1",),
            expected_value=0.62,
            expected_cost=0.2,
            retrieval_target="recurrence",
            stop_condition="recurrence known",
            score=0.50,
        ),
    ]
    safety_dependency = InquiryQuestion(
        question_id="Q_CRITICAL_PATH",
        question="What dependency could make this block the critical path?",
        primitive="DEPENDENCY",
        tests_hypotheses=("H1",),
        expected_value=0.9,
        expected_cost=0.2,
        retrieval_target="critical_path",
        stop_condition="dependency known",
        score=0.66,
    )

    merged, added = _merge_llm_and_safety_questions(
        llm_questions,
        [safety_dependency],
    )

    assert added == 1
    assert "DEPENDENCY" in {question.primitive for question in merged}


def test_safety_question_boosts_existing_llm_expected_value():
    llm_dependency = InquiryQuestion(
        question_id="Q_CRITICAL_PATH",
        question="What dependency is involved?",
        primitive="DEPENDENCY",
        tests_hypotheses=("H1",),
        expected_value=0.52,
        expected_cost=0.2,
        retrieval_target="critical_path",
        stop_condition="dependency known",
        score=0.70,
    )
    safety_dependency = InquiryQuestion(
        question_id="Q_CRITICAL_PATH",
        question="What dependency could make this block the critical path?",
        primitive="DEPENDENCY",
        tests_hypotheses=("H1",),
        expected_value=0.9,
        expected_cost=0.24,
        retrieval_target="critical_path",
        stop_condition="dependency known",
        score=0.66,
    )

    merged, added = _merge_llm_and_safety_questions(
        [llm_dependency],
        [safety_dependency],
    )

    assert added == 0
    assert merged[0].primitive == "DEPENDENCY"
    assert merged[0].question == llm_dependency.question
    assert merged[0].expected_value == safety_dependency.expected_value
    assert merged[0].score == llm_dependency.score


async def test_inquiry_runtime_builds_questions_reservoir_and_packet(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[
            {"type": "commitment", "id": str(fs.hero_commitment_id)},
            {"type": "goal", "id": str(fs.hero_goal_id)},
        ],
        seed_natural_text=(
            "Acme cannot launch without SSO, and Sales promised "
            "go-live this month."
        ),
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=make_embedding("Acme launch SSO blocker"),
    )

    result = await run_inquiry_retrieval(
        trigger,
        tx_conn,
        config=InquiryConfig(max_rounds=1, questions_per_round=2, persist=False),
    )

    assert result.route == "DEEP_INQUIRY_PATH"
    assert {h.id for h in result.hypotheses} >= {"H1", "H0"}
    assert len(result.questions) == 2
    assert result.retrieval_actions
    assert result.evidence_cards
    assert result.context_packet["sufficiency_verdict"]["evidence_count"] == len(
        result.evidence_cards
    )
    assert result.retrieval_result.notes["execution_engine"] == "inquiry"
    assert result.retrieval_result.notes["inquiry"]["context_packet"][
        "question_path"
    ]

    bundle = await assemble_context(
        result.retrieval_result,
        AccessContext(tenant_id=tenant, requestor_actor_id=fs.hero_actor_id),
        tx_conn,
    )
    assert "inquiry_context_packet" in bundle.notes


async def test_inquiry_runtime_uses_llm_for_question_planning(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    provider = _ScriptedQuestionProvider(
        [
            json.dumps(
                {
                    "rationale": "The signal makes a material launch claim.",
                    "questions": [
                        {
                            "primitive": "COUNTEREVIDENCE",
                            "question": (
                                "What fresh evidence would show the Acme SSO "
                                "issue is not blocking launch?"
                            ),
                            "retrieval_target": (
                                "semantic_counterevidence+recent_observations"
                            ),
                            "expected_value": 0.91,
                            "expected_cost": 0.22,
                            "tests_hypotheses": ["H1", "H0"],
                            "stop_condition": "credible counterevidence found",
                        },
                        {
                            "primitive": "DEPENDENCY",
                            "question": (
                                "Which Acme launch dependency is actually blocked "
                                "by the SSO permission edge case?"
                            ),
                            "retrieval_target": "commitment_graph+recent_observations",
                            "expected_value": 0.88,
                            "expected_cost": 0.26,
                            "tests_hypotheses": ["H1"],
                            "stop_condition": "critical dependency found",
                        },
                    ],
                }
            )
        ]
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[
            {"type": "commitment", "id": str(fs.hero_commitment_id)},
            {"type": "goal", "id": str(fs.hero_goal_id)},
        ],
        seed_natural_text="Acme cannot launch because SSO permissions are blocked.",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=make_embedding("Acme launch SSO permissions blocked"),
    )

    result = await run_inquiry_retrieval(
        trigger,
        tx_conn,
        llm_provider=provider,
        config=InquiryConfig(max_rounds=1, questions_per_round=2, persist=False),
    )

    assert len(provider.calls) == 1
    assert result.notes["question_planning"][0]["mode"] == "llm"
    assert result.notes["question_planning"][0]["llm_primitives"] == [
        "COUNTEREVIDENCE",
        "DEPENDENCY",
    ]
    assert [
        question.question
        for question in result.questions
    ] == [
        "What fresh evidence would show the Acme SSO issue is not blocking launch?",
        "Which Acme launch dependency is actually blocked by the SSO permission edge case?",
    ]


async def test_fast_path_inquiry_stops_after_baseline(tx_conn, fresh_db, tenant):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="What happened with Acme?",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=make_embedding("What happened with Acme?"),
    )

    result = await run_inquiry_retrieval(
        trigger,
        tx_conn,
        route="FAST_PATH",
        mode="fast",
        config=InquiryConfig(persist=False, fast_path_evidence_limit=10),
    )

    assert result.route == "FAST_PATH"
    assert result.questions == ()
    assert len(result.evidence_cards) <= 10
    assert result.context_packet["source_metadata"]["route"] == "FAST_PATH"
    assert result.sufficiency.status in {
        "sufficient_for_reasoning",
        "no_update_needed",
    }


async def test_fast_path_baseline_evidence_is_classified_against_hypotheses(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    repo = ModelsRepo(
        fresh_db,
        embedder=None,
        run_topology_on_insert=False,
    )
    await repo.insert(
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=fs.observation_ids[0],
            proposition={
                "kind": "concern",
                "about": "customer-0 churn",
                "nature": "blocked launch risk",
                "raised_by": "test",
            },
            natural="customer-0 churn risk high is blocked by launch dependency.",
            embedding=make_embedding("customer-0 churn risk high"),
            scope_actors=[fs.hero_actor_id],
            scope_entities=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
            scope_temporal={"type": "now"},
            confidence=0.6,
            confidence_at_assertion=0.6,
        ),
        conn=tx_conn,
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text=(
            "customer-0 churn risk high is blocked and Sales promised "
            "go-live this month."
        ),
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=make_embedding("customer-0 churn risk high"),
    )

    result = await run_inquiry_retrieval(
        trigger,
        tx_conn,
        route="FAST_PATH",
        mode="fast",
        config=InquiryConfig(persist=False, fast_path_evidence_limit=80),
    )

    assert result.questions == ()
    assert any(card.supports_hypotheses for card in result.evidence_cards)
    assert any(
        group.get("claim_supported")
        for group in result.context_packet["tiers"]["supporting_evidence_groups"]
    )


async def test_inquiry_applies_result_and_action_budget_caps(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="customer-0 churn risk high is blocked.",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=make_embedding("customer-0 churn risk high"),
    )

    result = await run_inquiry_retrieval(
        trigger,
        tx_conn,
        top_n=80,
        config=InquiryConfig(
            max_rounds=1,
            questions_per_round=2,
            result_model_limit=9,
            action_model_budget_limit=6,
            action_observation_budget_limit=5,
            persist=False,
        ),
    )

    assert len(result.retrieval_result.models) <= 9
    assert result.notes["requested_top_n"] == 80
    assert result.notes["effective_top_n"] == 9
    assert result.notes["action_model_budget_limit"] == 6
    assert result.notes["action_observation_budget_limit"] == 5


async def test_inquiry_skips_bad_question_embedding_without_aborting(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="customer-0 churn risk high is blocked.",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=make_embedding("customer-0 churn risk high"),
    )

    result = await run_inquiry_retrieval(
        trigger,
        tx_conn,
        embedder=_BadQuestionEmbedder(),
        config=InquiryConfig(max_rounds=1, questions_per_round=2, persist=False),
    )

    assert result.questions
    assert result.retrieval_actions
    assert result.evidence_cards
    assert result.sufficiency.status in {
        "sufficient_for_reasoning",
        "budget_exhausted",
        "human_validation_required",
    }


async def test_inquiry_context_packet_budget_applies_to_all_evidence_tiers(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="customer-0 churn risk high is blocked.",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=make_embedding("customer-0 churn risk high"),
    )

    result = await run_inquiry_retrieval(
        trigger,
        tx_conn,
        route="FAST_PATH",
        mode="fast",
        config=InquiryConfig(
            persist=False,
            fast_path_evidence_limit=10,
            reasoning_packet_token_budget=1,
        ),
    )

    packet = result.context_packet
    assert packet["budget"]["estimated_tokens_used"] <= 1
    assert any(
        item.get("reason") == "context packet token budget reached"
        for item in packet["tiers"]["omission_ledger"]
    )
