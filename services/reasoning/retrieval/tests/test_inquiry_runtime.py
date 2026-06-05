from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pytest

from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.types import ModelCreate, ModelRow
from services.domain.models.repo import ModelsRepo
from services.platform.execution.inquiry import (
    Hypothesis,
    InquiryConfig,
    InquiryQuestion,
    ModelRelevance,
    _apply_relevance_diversity,
    _adaptive_baseline_top_n,
    _adaptive_evidence_limit,
    _cap_pathway_models,
    _candidate_questions_for_round,
    _has_broad_signal_language,
    _merge_llm_and_safety_questions,
    run_inquiry_retrieval,
)
from services.reasoning.retrieval.assembler import AccessContext, assemble_context
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.primary import RetrievalResult
from services.reasoning.retrieval.tests._fixtures import build_fixture, make_embedding


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


def _model_row_for_compaction(
    *,
    natural: str,
    claim_role: str,
    domain_tags: list[str],
    scope_entities: list[dict[str, str]],
    supporting_model_ids: list[Any] | None = None,
) -> ModelRow:
    now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    return ModelRow(
        id=uuid4(),
        tenant_id=uuid4(),
        born_from_event_id=uuid4(),
        proposition={"kind": "belief", "assertion": natural},
        natural=natural,
        embedding=[0.1, 0.2, 0.3],
        scope_actors=[],
        scope_entities=scope_entities,
        scope_temporal={"type": "now"},
        confidence=0.7,
        activation=0.6,
        supporting_event_ids=[],
        supporting_model_ids=list(supporting_model_ids or []),
        evidential_weight=0.5,
        status="active",
        archived_at=None,
        archive_reason=None,
        created_at=now,
        last_retrieved_at=None,
        retrieval_count=0,
        evaluate_at=None,
        resolution_criteria=None,
        contributing_models=[],
        visible_to_subjects=True,
        proposition_kind="belief",
        claim_role=claim_role,
        abstraction_level="atomic",
        time_mode="current",
        modality="inferred",
        polarity="negative" if claim_role == "concern" else "neutral",
        domain_tags=domain_tags,
        memory_grammar_version="v1",
        confirmed_count=0,
        contested_count=0,
        last_confirmed_at=None,
        confidence_at_assertion=0.7,
        resolved_at=None,
        resolution_outcome=None,
        activation_coefficient=1.0,
        target_actor_id=None,
        caused_act_change_id=None,
    )


def _relevance_for_model(model: ModelRow, score: float) -> ModelRelevance:
    return ModelRelevance(
        model_id=model.id,
        final_score=score,
        base_score=0.12,
        lexical_score=0.2,
        scope_score=0.16,
        path_score=0.06,
        evidence_score=0.06,
        provenance_score=0.04,
        penalty=0.0,
        reasons=("test",),
    )


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


def test_coverage_compaction_caps_dense_material_neighborhoods():
    same_customer = str(uuid4())
    pairs: list[tuple[ModelRow, ModelRelevance]] = []
    for idx in range(54):
        model = _model_row_for_compaction(
            natural=f"same customer revenue risk duplicate {idx}",
            claim_role="concern",
            domain_tags=["revenue", "customers"],
            scope_entities=[{"type": "customer", "id": same_customer}],
        )
        pairs.append((model, _relevance_for_model(model, 0.62 - idx * 0.001)))
    for idx, role in enumerate(("pattern", "situation", "fact", "relation")):
        support_id = uuid4()
        model = _model_row_for_compaction(
            natural=f"novel {role} coverage for adjacent customer {idx}",
            claim_role=role,
            domain_tags=["execution", "customers"],
            scope_entities=[{"type": "customer", "id": str(uuid4())}],
            supporting_model_ids=[support_id],
        )
        pairs.append((model, _relevance_for_model(model, 0.46 - idx * 0.01)))

    compacted, dropped, notes = _apply_relevance_diversity(
        pairs,
        top_n=64,
        weak_signal=False,
        broad_signal=False,
        threshold=0.30,
        min_keep=4,
        model_pathways={model.id: {"semantic"} for model, _ in pairs},
        model_questions={model.id: {"Q_GOAL_IMPACT"} for model, _ in pairs},
    )

    roles = {model.claim_role for model, _ in compacted}
    assert len(compacted) <= 36
    assert dropped >= 20
    assert {"pattern", "situation"} <= roles
    assert notes["strategy"] == "coverage_aware"


def test_coverage_compaction_uses_larger_broad_portfolio_budget():
    pairs: list[tuple[ModelRow, ModelRelevance]] = []
    for idx in range(64):
        model = _model_row_for_compaction(
            natural=f"portfolio renewal risk customer {idx}",
            claim_role="concern",
            domain_tags=["portfolio", "revenue"],
            scope_entities=[{"type": "customer", "id": str(uuid4())}],
        )
        pairs.append((model, _relevance_for_model(model, 0.58 - idx * 0.001)))

    compacted, dropped, notes = _apply_relevance_diversity(
        pairs,
        top_n=64,
        weak_signal=False,
        broad_signal=True,
        threshold=0.24,
        min_keep=18,
        model_pathways={model.id: {"semantic", "temporal"} for model, _ in pairs},
        model_questions={model.id: {"Q_GOAL_IMPACT"} for model, _ in pairs},
    )

    assert 24 <= len(compacted) <= 48
    assert dropped >= 16
    assert notes["target_limit"] == 48


def test_adaptive_budget_limits_by_signal_class():
    cfg = InquiryConfig(evidence_reservoir_limit=700, fast_path_evidence_limit=80)

    assert _adaptive_baseline_top_n(220, "weak") == 80
    assert _adaptive_baseline_top_n(220, "material") == 150
    assert _adaptive_baseline_top_n(220, "broad") == 220
    assert (
        _adaptive_evidence_limit(
            cfg,
            route="DEEP_INQUIRY_PATH",
            mode="deep",
            signal_class="material",
        )
        == 360
    )
    assert (
        _adaptive_evidence_limit(
            cfg,
            route="DEEP_INQUIRY_PATH",
            mode="deep",
            signal_class="broad",
        )
        == 560
    )
    assert (
        _adaptive_evidence_limit(
            cfg,
            route="DEEP_INQUIRY_PATH",
            mode="deep",
            signal_class="weak",
        )
        == 80
    )


def test_pathway_model_cap_records_adaptive_trim():
    models = [
        _model_row_for_compaction(
            natural=f"candidate {idx}",
            claim_role="concern",
            domain_tags=["execution"],
            scope_entities=[{"type": "customer", "id": str(uuid4())}],
        )
        for idx in range(12)
    ]
    result = PathwayResult(models=models, source_pathway="A")

    _cap_pathway_models(result, 5)

    assert len(result.models) == 5
    assert result.notes["models_before_adaptive_cap"] == 12
    assert result.notes["models_after_adaptive_cap"] == 5


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
    tiers = result.context_packet["tiers"]
    assert any(
        item.get("supports_hypotheses")
        for item in tiers["decisive_evidence"]
    ) or any(
        group.get("claim_supported")
        for group in tiers["supporting_evidence_groups"]
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
