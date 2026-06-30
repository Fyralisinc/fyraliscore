from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pytest

from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.types import ModelCreate, ModelRow
from services.domain.models.address import build_belief_address
from services.domain.models.repo import ModelsRepo
from services.platform.execution.inquiry import (
    EvidenceCard,
    Hypothesis,
    InquiryConfig,
    InquiryQuestion,
    ModelRelevance,
    QuestionAnswer,
    RetrievalAction,
    SufficiencyVerdict,
    _append_structural_closure,
    _apply_relevance_diversity,
    _adaptive_baseline_top_n,
    _adaptive_evidence_limit,
    _answer_question,
    _cap_pathway_models,
    _candidate_questions_for_round,
    _classify_hypothesis_links,
    _cold_weak_noop_gate,
    _compile_context_packet,
    _execute_focused_index_action,
    _execute_semantic_hybrid_action,
    _focused_index_terms,
    _has_broad_signal_language,
    _hybrid_lexical_model_scan,
    _hybrid_lexical_terms,
    _hybrid_sparse_lookup_terms,
    _merge_llm_and_safety_questions,
    _merge_hybrid_semantic_lexical_models,
    _model_coverage_features,
    _model_relevance_cluster_key,
    _pack_structural_links,
    _select_minimal_sufficient_evidence,
    _signal_class_for_trigger,
    _upsert_evidence,
    run_inquiry_retrieval,
)
from services.reasoning.retrieval.assembler import AccessContext, assemble_context
from services.reasoning.retrieval.pathways import PathwayResult, pathway_b_semantic
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.primary import RetrievalResult
from services.reasoning.retrieval.tests._fixtures import build_fixture, make_embedding
from services.reasoning.sage.reader import _fetch_sparse_term_matches


pytestmark = pytest.mark.integration


def test_hybrid_sparse_lookup_terms_stays_bounded_for_long_raw_query():
    terms = _hybrid_sparse_lookup_terms(
        ["alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo"]
    )

    assert terms == [
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
    ]


class _BadQuestionEmbedder:
    async def embed(self, _text: str) -> list[float]:
        return [0.0]


class _ScriptedQuestionProvider(LLMProvider):
    def __init__(self, responses: list[str]):
        super().__init__(
            LLMConfig(
                provider="codex",
                api_key="test",
                model="gpt-5.3-codex",
                reasoning_effort="low",
            )
        )
        self._is_question_planning_provider = True
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
        super().__init__(
            LLMConfig(
                provider="codex",
                api_key="test",
                model="gpt-5.3-codex",
                timeout_s=0.01,
                reasoning_effort="low",
            )
        )
        self._is_question_planning_provider = True

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
    abstraction_level: str = "atomic",
    polarity: str | None = None,
    proposition_extra: dict[str, Any] | None = None,
) -> ModelRow:
    now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    proposition = {"kind": "belief", "assertion": natural}
    if proposition_extra:
        proposition.update(proposition_extra)
    return ModelRow(
        id=uuid4(),
        tenant_id=uuid4(),
        born_from_event_id=uuid4(),
        proposition=proposition,
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
        abstraction_level=abstraction_level,
        time_mode="current",
        modality="inferred",
        polarity=polarity or ("negative" if claim_role == "concern" else "neutral"),
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


def _belief_address_for_test(
    *,
    subject: str,
    assertion: str,
    claim_role: str = "concern",
) -> dict[str, Any]:
    return build_belief_address(
        {
            "kind": "belief",
            "claim_role": claim_role,
            "subject": subject,
            "assertion": assertion,
        }
    )


def test_broad_signal_language_does_not_treat_split_across_as_portfolio():
    assert not _has_broad_signal_language(
        "the named owner is split across escalations and the delivery date is slipping"
    )
    assert _has_broad_signal_language(
        "board update across all enterprise customers: renewal risk is rising"
    )


async def test_question_planning_timeout_falls_back_to_deterministic(
    monkeypatch, tenant
):
    monkeypatch.delenv("INQUIRY_CODEX_QUESTION_TIMEOUT_SECONDS", raising=False)
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


async def test_question_planning_expands_llm_belief_deltas_without_questions(tenant):
    provider = _ScriptedQuestionProvider(
        [
            json.dumps(
                {
                    "rationale": "The signal implies a specific belief delta.",
                    "belief_deltas": [
                        {
                            "delta_id": "D_AUDIT_EXPORT",
                            "claim_atom": (
                                "Atlas renewal approval is blocked by missing "
                                "customer-visible audit export evidence"
                            ),
                            "delta_type": "update",
                            "affected_entities": ["Atlas", "renewal"],
                            "uncertainty_slots": [
                                "who owns customer-visible audit export evidence",
                                "what evidence would weaken the existing SAML blocker model",
                                "which active renewal commitment is at risk",
                            ],
                            "evidence_needed": [
                                "security review thread",
                                "Atlas renewal commitment",
                                "prior SAML blocker model",
                            ],
                            "impact_if_true": "high",
                            "confidence": 0.68,
                        }
                    ],
                    "questions": [],
                }
            )
        ]
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_natural_text=(
            "Security review asks for SOC2, audit export, SAML mapping, and "
            "data residency evidence before Atlas will approve renewal."
        ),
        seed_entity_ids=[{"type": "customer", "id": "Atlas"}],
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
    )
    baseline = RetrievalResult(trigger=trigger)
    hypotheses = (
        Hypothesis("H1", "The signal is a real blocker.", 0.7, "high"),
        Hypothesis("H2", "An active commitment is affected.", 0.4, "medium"),
        Hypothesis("H0", "The signal is already captured.", 0.2, "low"),
    )

    questions, note = await _candidate_questions_for_round(
        trigger,
        baseline,
        hypotheses,
        {},
        {"counterevidence", "responsible owner"},
        llm_provider=provider,
        config=InquiryConfig(),
        round_index=1,
    )

    question_text = " ".join(question.question for question in questions)
    assert note["mode"] == "llm_delta"
    assert note["belief_delta_count"] == 1
    assert note["belief_delta_question_count"] >= 3
    assert "audit export evidence" in question_text
    assert {"OWNERSHIP", "COUNTEREVIDENCE", "COMMITMENT"} <= {
        question.primitive for question in questions
    }


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


async def test_semantic_hybrid_lexical_promotes_exact_anchor_within_budget(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    repo = ModelsRepo(
        fresh_db,
        embedder=None,
        run_topology_on_insert=False,
    )
    dense_models: list[ModelRow] = []
    for idx in range(3):
        dense_models.append(
            await repo.insert(
                ModelCreate(
                    tenant_id=tenant,
                    born_from_event_id=fs.observation_ids[0],
                    proposition={
                        "kind": "belief",
                        "subject": f"generic launch blocker {idx}",
                        "assertion": "semantic candidate for launch blocker retrieval",
                    },
                    natural=f"Generic launch blocker from semantic path {idx}.",
                    embedding=make_embedding(f"semantic dense candidate {idx}"),
                    scope_actors=[fs.hero_actor_id],
                    scope_entities=[
                        {"type": "commitment", "id": str(fs.hero_commitment_id)}
                    ],
                    scope_temporal={"type": "now"},
                    confidence=0.6,
                    confidence_at_assertion=0.6,
                ),
                conn=tx_conn,
            )
        )
    exact_model = await repo.insert(
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=fs.observation_ids[0],
            proposition={
                "kind": "belief",
                "subject": "SOC2-RISK-77 escrow dependency",
                "assertion": "exact risk anchor identifies the escrow dependency",
            },
            natural="SOC2-RISK-77 vendor escrow dependency blocks the Acme launch.",
            embedding=make_embedding("orthogonal lexical-only model"),
            scope_actors=[fs.hero_actor_id],
            scope_entities=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
            scope_temporal={"type": "now"},
            confidence=0.6,
            confidence_at_assertion=0.6,
        ),
        conn=tx_conn,
    )
    sparse_rows = await tx_conn.fetchval(
        """
        SELECT count(*)::int
        FROM model_sparse_terms
        WHERE model_id = $1
          AND tenant_id = $2
          AND term = 'soc2-risk-77'
          AND status = 'active'
        """,
        exact_model.id,
        tenant,
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="Does SOC2-RISK-77 block the Acme launch?",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=make_embedding("semantic dense candidate 0"),
    )

    terms = _hybrid_lexical_terms(
        "Which dependency mentions SOC2-RISK-77?",
        trigger,
        max_terms=4,
    )
    hits = await _hybrid_lexical_model_scan(
        trigger,
        tx_conn,
        terms=terms,
        limit=4,
        per_term_limit=4,
    )
    merged = _merge_hybrid_semantic_lexical_models(
        dense_models,
        hits,
        limit=3,
    )

    assert any("soc2-risk-77" in term for term in terms)
    assert sparse_rows == 1
    assert exact_model.id in {model.id for model, _match_count in hits}
    assert len(merged) == 3
    assert exact_model.id in {model.id for model in merged}


async def test_semantic_hybrid_action_rescues_model_semantic_terms(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db, n_models=6)
    repo = ModelsRepo(
        fresh_db,
        embedder=None,
        run_topology_on_insert=False,
    )
    query = "Which model explains refund replay drift for the launch risk?"
    query_vector = make_embedding("semantic dense launch readiness")
    dense_models: list[ModelRow] = []
    for idx in range(3):
        dense_models.append(
            await repo.insert(
                ModelCreate(
                    tenant_id=tenant,
                    born_from_event_id=fs.observation_ids[0],
                    proposition={
                        "kind": "belief",
                        "subject": f"generic launch readiness {idx}",
                        "assertion": "ordinary dense retrieval candidate",
                    },
                    natural=f"Generic launch readiness candidate {idx}.",
                    embedding=query_vector,
                    scope_actors=[fs.hero_actor_id],
                    scope_entities=[
                        {"type": "commitment", "id": str(fs.hero_commitment_id)}
                    ],
                    scope_temporal={"type": "now"},
                    confidence=0.61,
                    confidence_at_assertion=0.61,
                ),
                conn=tx_conn,
            )
        )
    tagged_model = await repo.insert(
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=fs.observation_ids[0],
            proposition={
                "kind": "belief",
                "subject": "quiet launch risk",
                "assertion": "payment correction review is delaying release",
            },
            natural="Payment correction review is delaying the release train.",
            embedding=make_embedding("orthogonal sidecar semantic term candidate"),
            scope_actors=[fs.hero_actor_id],
            scope_entities=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
            scope_temporal={"type": "now"},
            confidence=0.61,
            confidence_at_assertion=0.61,
        ),
        conn=tx_conn,
    )
    await tx_conn.execute(
        """
        INSERT INTO model_semantic_terms (
          tenant_id, model_id, semantic_terms
        ) VALUES ($1, $2, $3::text[])
        ON CONFLICT (tenant_id, model_id) DO UPDATE
        SET semantic_terms = EXCLUDED.semantic_terms
        """,
        tenant,
        tagged_model.id,
        ["refund replay drift"],
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="Launch risk asks about refund replay drift.",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=query_vector,
    )

    dense_result = await pathway_b_semantic(
        query,
        tenant,
        tx_conn,
        k=3,
        precomputed_vector=query_vector,
        event_actors=trigger.scope_actors,
        event_entities=trigger.seed_entity_ids,
    )
    result = await _execute_semantic_hybrid_action(
        RetrievalAction(
            "Q_SEMANTIC_TERMS",
            "semantic",
            "semantic_term_rescue",
            query=query,
            budget=3,
        ),
        trigger,
        tx_conn,
        None,
        InquiryConfig(),
        model_limit=3,
    )

    dense_ids = {model.id for model in dense_result.models}
    returned_ids = {model.id for model in result.models}
    semantic_note = result.notes.get("semantic_hybrid_semantic_terms")
    if semantic_note is None:
        semantic_note = (result.notes.get("semantic_hybrid_substrates") or {}).get(
            "semantic_terms"
        )

    assert {model.id for model in dense_models}.issubset(dense_ids)
    assert tagged_model.id not in dense_ids
    assert tagged_model.id in returned_ids
    assert isinstance(semantic_note, dict), result.notes
    semantic_note_blob = json.dumps(semantic_note, sort_keys=True, default=str)
    assert "refund replay drift" in semantic_note_blob
    assert str(tagged_model.id) in semantic_note_blob


async def test_sage_sparse_lookup_returns_partial_bounded_term_hits(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    repo = ModelsRepo(
        fresh_db,
        embedder=None,
        run_topology_on_insert=False,
    )
    target = await repo.insert(
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=fs.observation_ids[0],
            proposition={
                "kind": "belief",
                "assertion": "procurement renewal security evidence is the blocker",
            },
            natural=(
                "Procurement renewal security evidence packet is the blocker "
                "for enterprise approval."
            ),
            embedding=make_embedding("sparse partial lexical candidate"),
            scope_actors=[fs.hero_actor_id],
            scope_entities=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
            scope_temporal={"type": "now"},
            confidence=0.67,
            confidence_at_assertion=0.67,
        ),
        conn=tx_conn,
    )
    noisy_singleton = await repo.insert(
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=fs.observation_ids[0],
            proposition={
                "kind": "belief",
                "assertion": "security posture reminder",
            },
            natural="Security posture reminder for routine review.",
            embedding=make_embedding("sparse noisy singleton candidate"),
            scope_actors=[fs.hero_actor_id],
            scope_entities=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
            scope_temporal={"type": "now"},
            confidence=0.67,
            confidence_at_assertion=0.67,
        ),
        conn=tx_conn,
    )

    rows = await _fetch_sparse_term_matches(
        tx_conn,
        tenant_id=tenant,
        terms=["Atlas Retail Group renewal security evidence procurement"],
        limit=4,
        max_terms=8,
        per_term_limit=4,
    )

    assert target.id in {row["id"] for row in rows}
    assert noisy_singleton.id not in {row["id"] for row in rows}
    assert max(int(row["match_count"] or 0) for row in rows) >= 4


async def test_focused_index_uses_question_terms_answerability_and_scope(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    repo = ModelsRepo(
        fresh_db,
        embedder=None,
        run_topology_on_insert=False,
    )
    exact_model = await repo.insert(
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=fs.observation_ids[0],
            proposition={
                "kind": "belief",
                "claim_role": "relation",
                "abstraction_level": "relationship",
                "subject": "Acme launch",
                "relation": "depends_on",
                "object": "SOC2-RISK-77 vendor escrow",
            },
            natural="SOC2-RISK-77 vendor escrow dependency blocks the Acme launch.",
            embedding=make_embedding("focused scoped answerability model"),
            scope_actors=[fs.hero_actor_id],
            scope_entities=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
            scope_temporal={"type": "now"},
            confidence=0.6,
            confidence_at_assertion=0.6,
        ),
        conn=tx_conn,
    )
    off_scope_model = await repo.insert(
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=fs.observation_ids[0],
            proposition={
                "kind": "belief",
                "claim_role": "relation",
                "abstraction_level": "relationship",
                "subject": "Other launch",
                "relation": "depends_on",
                "object": "SOC2-RISK-77 vendor escrow",
            },
            natural="SOC2-RISK-77 vendor escrow dependency blocks another launch.",
            embedding=make_embedding("focused off scope answerability model"),
            scope_actors=[fs.hero_actor_id],
            scope_entities=[{"type": "commitment", "id": str(uuid4())}],
            scope_temporal={"type": "now"},
            confidence=0.6,
            confidence_at_assertion=0.6,
        ),
        conn=tx_conn,
    )
    answerability_rows = await tx_conn.fetchval(
        """
        SELECT count(*)::int
        FROM model_answerability_index
        WHERE model_id = $1
          AND tenant_id = $2
          AND primitive = 'DEPENDENCY'
          AND term = 'soc2-risk-77'
          AND status = 'active'
        """,
        exact_model.id,
        tenant,
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="Does SOC2-RISK-77 block the Acme launch?",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
    )
    terms = _focused_index_terms(
        "Which dependency mentions SOC2-RISK-77 for the Acme launch?",
        trigger,
        max_terms=8,
    )
    result = await _execute_focused_index_action(
        RetrievalAction(
            "Q_CRITICAL_PATH",
            "focused_index",
            "question_answerability_scope",
            query="Which dependency mentions SOC2-RISK-77 for the Acme launch?",
            filters={"primitive": "DEPENDENCY", "terms": terms},
            budget=8,
        ),
        trigger,
        tx_conn,
        InquiryConfig(),
        model_limit=8,
    )

    assert result is not None
    returned_ids = [model.id for model in result.models]
    top_hit = result.notes["top_hits"][0]

    assert answerability_rows == 1
    assert any("soc2-risk-77" in term for term in terms)
    assert returned_ids[0] == exact_model.id
    assert off_scope_model.id not in returned_ids
    assert top_hit["model_id"] == str(exact_model.id)
    assert "answerability_index" in top_hit["sources"]
    assert "scope_sparse" in top_hit["sources"]
    assert top_hit["scope_overlap"] >= 1


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


def test_belief_address_distinguishes_compaction_clusters_for_similar_text():
    same_customer = str(uuid4())
    first_address = _belief_address_for_test(
        subject="Beacon renewal",
        assertion="SOC2 owner is missing",
    )
    second_address = _belief_address_for_test(
        subject="Beacon renewal",
        assertion="legal approval is blocked",
    )
    first = _model_row_for_compaction(
        natural="Beacon renewal risk needs attention.",
        claim_role="concern",
        domain_tags=["revenue", "customers"],
        scope_entities=[{"type": "customer", "id": same_customer}],
        proposition_extra={
            "belief_address": first_address,
            "semantic_address": first_address,
        },
    )
    second = _model_row_for_compaction(
        natural="Beacon renewal risk needs attention.",
        claim_role="concern",
        domain_tags=["revenue", "customers"],
        scope_entities=[{"type": "customer", "id": same_customer}],
        proposition_extra={
            "belief_address": second_address,
            "semantic_address": second_address,
        },
    )
    duplicate = _model_row_for_compaction(
        natural="Beacon renewal risk needs attention.",
        claim_role="concern",
        domain_tags=["revenue", "customers"],
        scope_entities=[{"type": "customer", "id": same_customer}],
        proposition_extra={
            "belief_address": first_address,
            "semantic_address": first_address,
        },
    )

    assert _model_relevance_cluster_key(first) != _model_relevance_cluster_key(second)
    assert _model_relevance_cluster_key(first) == _model_relevance_cluster_key(
        duplicate
    )
    features = {
        feature
        for feature, _weight in _model_coverage_features(
            first,
            {"sage_reader"},
            {"Q_CONSTRAINT"},
        )
    }
    assert f"belief_fingerprint:{first_address['fingerprint']}" in features
    assert any(feature.startswith("belief_obligation:spo:") for feature in features)
    assert "answerable:CONSTRAINT" in features


def test_compaction_preserves_distinct_belief_obligations_under_duplicate_pressure():
    same_customer = str(uuid4())
    duplicate_address = _belief_address_for_test(
        subject="Beacon renewal",
        assertion="SOC2 owner is missing",
    )
    pairs: list[tuple[ModelRow, ModelRelevance]] = []
    for idx in range(36):
        model = _model_row_for_compaction(
            natural="Beacon renewal risk needs attention.",
            claim_role="concern",
            domain_tags=["revenue", "customers"],
            scope_entities=[{"type": "customer", "id": same_customer}],
            proposition_extra={
                "belief_address": duplicate_address,
                "semantic_address": duplicate_address,
            },
        )
        pairs.append((model, _relevance_for_model(model, 0.64 - idx * 0.001)))

    unique_ids: set[Any] = set()
    for idx in range(8):
        address = _belief_address_for_test(
            subject="Beacon renewal",
            assertion=f"independent blocker {idx} needs owner",
        )
        model = _model_row_for_compaction(
            natural="Beacon renewal risk needs attention.",
            claim_role="concern",
            domain_tags=["revenue", "customers"],
            scope_entities=[{"type": "customer", "id": same_customer}],
            proposition_extra={
                "belief_address": address,
                "semantic_address": address,
            },
        )
        unique_ids.add(model.id)
        pairs.append((model, _relevance_for_model(model, 0.50 - idx * 0.002)))

    compacted, dropped, notes = _apply_relevance_diversity(
        pairs,
        top_n=44,
        weak_signal=False,
        broad_signal=False,
        threshold=0.30,
        min_keep=4,
        model_pathways={model.id: {"semantic", "sage_reader"} for model, _ in pairs},
        model_questions={model.id: {"Q_CONSTRAINT"} for model, _ in pairs},
    )

    compacted_ids = {model.id for model, _ in compacted}
    assert unique_ids <= compacted_ids
    assert dropped >= 8
    assert notes["cluster_count"] >= 9


def test_compaction_preserves_diverse_obligations_across_roles_and_entities():
    shared_customer = str(uuid4())
    pairs: list[tuple[ModelRow, ModelRelevance]] = []
    duplicate_address = _belief_address_for_test(
        subject="Beacon renewal",
        assertion="generic renewal risk needs attention",
    )

    for idx in range(90):
        model = _model_row_for_compaction(
            natural="Portfolio renewal risk needs attention.",
            claim_role="concern",
            domain_tags=["portfolio", "revenue"],
            scope_entities=[{"type": "customer", "id": shared_customer}],
            proposition_extra={
                "belief_address": duplicate_address,
                "semantic_address": duplicate_address,
            },
        )
        pairs.append((model, _relevance_for_model(model, 0.68 - idx * 0.0005)))

    must_keep: set[Any] = set()
    diverse_specs = [
        (
            "Beacon renewal",
            "SOC2 evidence blocks enterprise launch",
            "relation",
            "relationship",
        ),
        ("Beacon renewal", "owner is missing for security review", "concern", "atomic"),
        ("Beacon renewal", "month-end export stalls recur", "pattern", "atomic"),
        ("Beacon renewal", "signed order contradicts churn risk", "concern", "atomic"),
        ("Northstar renewal", "legal approval blocks close plan", "concern", "atomic"),
        (
            "Orion launch",
            "sandbox quota exhaustion blocks release",
            "concern",
            "atomic",
        ),
        ("Vela import", "owner is platform enablement", "fact", "atomic"),
        ("HelioWorks handoff", "customer goal is at risk", "prediction", "atomic"),
        ("Atlas workflow", "assign owner for mitigation", "recommendation", "atomic"),
        (
            "Kestrel system",
            "maps invoice questions to evidence",
            "capability",
            "atomic",
        ),
    ]
    for idx, (subject, assertion, role, level) in enumerate(diverse_specs):
        address = _belief_address_for_test(
            subject=subject,
            assertion=assertion,
            claim_role=role,
        )
        model = _model_row_for_compaction(
            natural="Portfolio renewal risk needs attention.",
            claim_role=role,
            abstraction_level=level,
            domain_tags=["portfolio", "revenue", "execution"],
            scope_entities=[
                {
                    "type": "customer",
                    "id": shared_customer if idx < 4 else str(uuid4()),
                }
            ],
            proposition_extra={
                "belief_address": address,
                "semantic_address": address,
            },
        )
        must_keep.add(model.id)
        pairs.append((model, _relevance_for_model(model, 0.47 - idx * 0.01)))

    compacted, dropped, notes = _apply_relevance_diversity(
        pairs,
        top_n=64,
        weak_signal=False,
        broad_signal=False,
        threshold=0.30,
        min_keep=4,
        model_pathways={model.id: {"semantic", "sage_reader"} for model, _ in pairs},
        model_questions={
            model.id: {"Q_CONSTRAINT", "Q_COUNTEREVIDENCE", "Q_GOAL_IMPACT"}
            for model, _ in pairs
        },
    )

    compacted_ids = {model.id for model, _ in compacted}
    assert must_keep <= compacted_ids
    assert len(compacted) <= 18
    assert dropped >= 50
    assert notes["cluster_count"] >= len(diverse_specs) + 1


def test_material_compaction_does_not_keep_unique_fingerprints_after_answer_saturation():
    same_customer = str(uuid4())
    pairs: list[tuple[ModelRow, ModelRelevance]] = []
    for idx in range(80):
        address = _belief_address_for_test(
            subject="Beacon renewal",
            assertion=f"minor renewal risk detail {idx}",
        )
        model = _model_row_for_compaction(
            natural="Beacon renewal risk needs attention.",
            claim_role="concern",
            domain_tags=["revenue", "customers"],
            scope_entities=[{"type": "customer", "id": same_customer}],
            proposition_extra={
                "belief_address": address,
                "semantic_address": address,
            },
        )
        pairs.append((model, _relevance_for_model(model, 0.64 - idx * 0.001)))

    compacted, dropped, notes = _apply_relevance_diversity(
        pairs,
        top_n=64,
        weak_signal=False,
        broad_signal=False,
        threshold=0.30,
        min_keep=4,
        model_pathways={model.id: {"semantic", "sage_reader"} for model, _ in pairs},
        model_questions={model.id: {"Q_CONSTRAINT"} for model, _ in pairs},
    )

    assert len(compacted) <= 18
    assert dropped >= 62
    assert notes["target_limit"] == 18


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

    assert 32 <= len(compacted) <= 48
    assert dropped >= 16
    assert notes["target_limit"] == 48


def test_broad_compaction_preserves_repeated_trend_breadth():
    same_customer = str(uuid4())
    pairs: list[tuple[ModelRow, ModelRelevance]] = []
    for idx in range(64):
        model = _model_row_for_compaction(
            natural=(
                "portfolio renewal risk: legal review, security approvals, "
                f"capacity pressure, and billing disputes affect account {idx}"
            ),
            claim_role="concern",
            domain_tags=["portfolio", "revenue"],
            scope_entities=[{"type": "customer", "id": same_customer}],
        )
        pairs.append((model, _relevance_for_model(model, 0.58 - idx * 0.001)))

    compacted, dropped, notes = _apply_relevance_diversity(
        pairs,
        top_n=64,
        weak_signal=False,
        broad_signal=True,
        threshold=0.24,
        min_keep=3,
        model_pathways={model.id: {"semantic", "sage_reader"} for model, _ in pairs},
        model_questions={model.id: {"Q_CONSTRAINT"} for model, _ in pairs},
    )

    assert len(compacted) >= 32
    assert len(compacted) <= 48
    assert dropped <= 32
    assert notes["floor"] == 32
    assert notes["target_limit"] == 48


def test_structural_closure_keeps_linked_counterevidence_and_relation_only():
    same_customer = str(uuid4())
    root = _model_row_for_compaction(
        natural="Acme launch is blocked by integration readiness risk.",
        claim_role="concern",
        domain_tags=["execution", "risk"],
        scope_entities=[{"type": "customer", "id": same_customer}],
    )
    selected_pairs = [(root, _relevance_for_model(root, 0.84))]

    redundant_sibling = _model_row_for_compaction(
        natural="Acme launch has another ordinary readiness risk note.",
        claim_role="concern",
        domain_tags=["execution", "risk"],
        scope_entities=[{"type": "customer", "id": same_customer}],
        supporting_model_ids=[root.id],
        polarity="negative",
    )
    counter = _model_row_for_compaction(
        natural=(
            "A mitigation exists, but it does not remove the active risk "
            "and should not erase the blocker."
        ),
        claim_role="concern",
        domain_tags=["execution", "risk"],
        scope_entities=[{"type": "customer", "id": same_customer}],
        supporting_model_ids=[root.id],
        polarity="mixed",
    )
    relation = _model_row_for_compaction(
        natural="Latent invariant B-17 explains the dependency.",
        claim_role="relation",
        domain_tags=["graph", "dependencies"],
        scope_entities=[],
        supporting_model_ids=[root.id],
        abstraction_level="relationship",
        polarity="neutral",
        proposition_extra={
            "subject": str(root.id),
            "relation": "explains",
            "object": "latent invariant B-17",
        },
    )
    candidates = [
        *selected_pairs,
        (redundant_sibling, _relevance_for_model(redundant_sibling, 0.52)),
        (counter, _relevance_for_model(counter, 0.28)),
        (relation, _relevance_for_model(relation, 0.02)),
    ]

    closed, notes = _append_structural_closure(
        selected_pairs,
        candidates,
        top_n=12,
        weak_signal=False,
        broad_signal=False,
        threshold=0.30,
        model_pathways={
            root.id: {"semantic"},
            redundant_sibling.id: {"semantic"},
            counter.id: {"semantic"},
            relation.id: {"G", "model_edge"},
        },
        model_questions={
            root.id: {"Q_CONSTRAINT"},
            redundant_sibling.id: {"Q_CONSTRAINT"},
            counter.id: {"Q_COUNTEREVIDENCE"},
            relation.id: {"Q_CRITICAL_PATH"},
        },
    )

    closed_ids = {model.id for model, _ in closed}
    assert counter.id in closed_ids
    assert relation.id in closed_ids
    assert redundant_sibling.id not in closed_ids
    assert notes["added"] == 2
    assert notes["reasons"][str(counter.id)] == "linked_counterevidence"
    assert notes["reasons"][str(relation.id)] == "linked_relation"


def test_structural_link_packing_places_late_relation_next_to_anchor():
    same_customer = str(uuid4())
    root = _model_row_for_compaction(
        natural="Acme launch is blocked by integration readiness risk.",
        claim_role="concern",
        domain_tags=["execution", "risk"],
        scope_entities=[{"type": "customer", "id": same_customer}],
    )
    fillers = [
        _model_row_for_compaction(
            natural=f"Acme launch has ordinary adjacent note {idx}.",
            claim_role="concern",
            domain_tags=["execution", "risk"],
            scope_entities=[{"type": "customer", "id": same_customer}],
        )
        for idx in range(16)
    ]
    relation = _model_row_for_compaction(
        natural="Readiness risk is caused by the shared identity provider dependency.",
        claim_role="relation",
        domain_tags=["graph", "dependencies"],
        scope_entities=[],
        supporting_model_ids=[root.id],
        abstraction_level="relationship",
        polarity="neutral",
        proposition_extra={
            "subject": str(root.id),
            "relation": "caused_by",
            "object": "shared identity provider dependency",
        },
    )
    selected_pairs = [
        (root, _relevance_for_model(root, 0.90)),
        *((model, _relevance_for_model(model, 0.70)) for model in fillers),
        (relation, _relevance_for_model(relation, 0.42)),
    ]

    packed, notes = _pack_structural_links(selected_pairs)

    packed_ids = [model.id for model, _ in packed]
    assert packed_ids[0] == root.id
    assert packed_ids[1] == relation.id
    assert set(packed_ids) == {model.id for model, _ in selected_pairs}
    assert notes["moved"] == 1


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


@pytest.mark.asyncio
async def test_cold_weak_noop_gate_abstains_without_scope_anchor(
    tx_conn,
    tenant,
):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[],
        scope_actors=[],
        seed_natural_text=(
            "Workspace chatter for customer-40: lunch notes, travel plans, "
            "and general team coordination. Marker RTS-011-weak_workspace_noise."
        ),
        seed_occurred_at=datetime.now(timezone.utc),
    )

    gate = _cold_weak_noop_gate(trigger, "weak")
    assert gate["used"] is True

    result = await run_inquiry_retrieval(
        trigger,
        tx_conn,
        mode="deep",
        top_n=64,
        config=InquiryConfig(persist=False, max_rounds=1),
    )

    assert result.sufficiency.status == "no_update_needed"
    assert result.retrieval_result.models == []
    assert result.evidence_cards == ()
    assert result.notes["cold_weak_noop_gate"]["used"] is True


def test_cold_weak_noop_gate_ignores_learned_scope_for_explicit_noop(tenant):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[uuid4()],
        scope_actors=["customer-40"],
        seed_natural_text=(
            "Workspace chatter for customer-40: lunch notes, travel plans, "
            "and general team coordination. Marker RTS-011-weak_workspace_noise."
        ),
        seed_occurred_at=datetime.now(timezone.utc),
    )

    gate = _cold_weak_noop_gate(trigger, "weak")

    assert gate["used"] is True


def test_weak_noop_declaration_dominates_generic_followup_boilerplate(tenant):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[uuid4()],
        scope_actors=["customer-40"],
        seed_natural_text=(
            "Workspace chatter for customer-40: lunch notes, travel plans, "
            "and general team coordination. Follow up: identify the current "
            "blocker, dependency, owner constraint, counterevidence, and next "
            "action for the same scope."
        ),
        seed_occurred_at=datetime.now(timezone.utc),
    )
    signal_class = _signal_class_for_trigger(trigger)
    gate = _cold_weak_noop_gate(trigger, signal_class)

    assert signal_class == "weak"
    assert gate["used"] is True


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


def test_premise_challenge_language_answers_counterevidence_question():
    hypotheses = (
        Hypothesis(
            id="H1",
            claim="Acme is blocked only by SSO.",
            confidence=0.5,
            impact_if_true="high",
        ),
        Hypothesis(
            id="H2",
            claim="An active commitment, owner, or promised outcome is affected.",
            confidence=0.35,
            impact_if_true="medium",
        ),
        Hypothesis(
            id="H0",
            claim="The premise is stale or incomplete.",
            confidence=0.2,
            impact_if_true="low",
        ),
    )
    trigger_text = "Why is Acme blocked by SSO?"
    summary = (
        "Acme SSO is one blocker, but it is not the only blocker: "
        "data migration is also active, and no explicit owner is represented."
    )

    supports, weakens, contradicts = _classify_hypothesis_links(
        summary,
        hypotheses,
        trigger_text=trigger_text,
    )

    assert "H1" in weakens | contradicts
    assert "H2" in weakens

    evidence_by_key: dict[tuple[str, str], EvidenceCard] = {}
    _upsert_evidence(
        evidence_by_key,
        key=("observation", "premise-challenge"),
        source_type="observation",
        source_ref_id=None,
        summary=summary,
        trust_tier="authoritative",
        timestamp=datetime(2026, 4, 1, 18, 5, tzinfo=timezone.utc),
        path="semantic",
        question_id="Q_COUNTEREVIDENCE",
        hypotheses=hypotheses,
        score=0.9,
        raw_content_ref="observation:premise-challenge",
        trigger_text=trigger_text,
    )
    answer = _answer_question(
        InquiryQuestion(
            question_id="Q_COUNTEREVIDENCE",
            question="What premise in the question is stale or incomplete?",
            primitive="COUNTEREVIDENCE",
            tests_hypotheses=("H1", "H0"),
            expected_value=0.9,
            expected_cost=0.2,
            retrieval_target="counterevidence",
            stop_condition="premise checked",
            score=0.7,
        ),
        evidence_by_key,
        trigger_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
        stale_after_days=30,
    )

    assert answer.answer_status in {"supported", "partially_supported"}
    assert answer.counterevidence


def test_context_packet_preserves_state_workflow_gotcha_and_premise_under_distractors(
    tenant,
):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "customer", "id": str(uuid4())}],
        seed_natural_text=(
            "Why is Acme onboarding blocked by SSO and still marked Commit?"
        ),
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, tzinfo=timezone.utc),
    )
    hypotheses = (
        Hypothesis(
            id="H1",
            claim="Acme onboarding is blocked by SSO and Commit remains justified.",
            confidence=0.52,
            impact_if_true="high",
        ),
        Hypothesis(
            id="H2",
            claim="An active commitment, owner, or promised outcome is affected.",
            confidence=0.36,
            impact_if_true="medium",
        ),
        Hypothesis(
            id="H3",
            claim="The signal may be part of a broader recurring pattern.",
            confidence=0.28,
            impact_if_true="high",
        ),
        Hypothesis(
            id="H0",
            claim="The premise is stale, incomplete, or already captured.",
            confidence=0.18,
            impact_if_true="low",
        ),
    )
    evidence_by_key: dict[tuple[str, str], EvidenceCard] = {}

    important = {
        "state": "Acme state changed from Commit to At Risk after data migration became active.",
        "workflow": "Acme onboarding is missing the security review owner assignment step before procurement can move forward.",
        "gotcha": "Recurring trap: security review stalls unless ownership is assigned early.",
        "premise": "SSO is not the only blocker; data migration is also active and the CRM Commit premise is unsupported.",
    }
    for key, summary in important.items():
        _upsert_evidence(
            evidence_by_key,
            key=("observation", key),
            source_type="observation",
            source_ref_id=None,
            summary=summary,
            trust_tier="authoritative",
            timestamp=datetime(2026, 4, 1, 18, 10, tzinfo=timezone.utc),
            path="capability_probe",
            question_id={
                "state": "Q_CRITICAL_PATH",
                "workflow": "Q_ACTIVE_COMMITMENT",
                "gotcha": "Q_RECURRENCE",
                "premise": "Q_COUNTEREVIDENCE",
            }[key],
            hypotheses=hypotheses,
            score=0.95,
            raw_content_ref=f"observation:{key}",
            trigger_text=trigger.seed_natural_text,
        )
    for idx in range(80):
        _upsert_evidence(
            evidence_by_key,
            key=("model", f"distractor-{idx}"),
            source_type="model",
            source_ref_id=None,
            summary=f"Unrelated noisy model about account {idx} lunch notes.",
            trust_tier="model",
            timestamp=datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
            path="semantic",
            question_id="Q0",
            hypotheses=hypotheses,
            score=0.01,
            raw_content_ref=f"model:distractor-{idx}",
            trigger_text=trigger.seed_natural_text,
        )

    by_raw_ref = {card.raw_content_ref: card for card in evidence_by_key.values()}
    questions = [
        InquiryQuestion(
            question_id="Q_CRITICAL_PATH",
            question="What changed in Acme's state?",
            primitive="DEPENDENCY",
            tests_hypotheses=("H1",),
            expected_value=0.9,
            expected_cost=0.25,
            retrieval_target="state",
            stop_condition="state transition found",
            score=0.65,
        ),
        InquiryQuestion(
            question_id="Q_ACTIVE_COMMITMENT",
            question="What workflow step is missing?",
            primitive="COMMITMENT",
            tests_hypotheses=("H2",),
            expected_value=0.85,
            expected_cost=0.2,
            retrieval_target="workflow",
            stop_condition="missing step found",
            score=0.65,
        ),
        InquiryQuestion(
            question_id="Q_RECURRENCE",
            question="What recurring local trap applies?",
            primitive="RECURRENCE",
            tests_hypotheses=("H3",),
            expected_value=0.8,
            expected_cost=0.3,
            retrieval_target="gotcha",
            stop_condition="gotcha found",
            score=0.5,
        ),
        InquiryQuestion(
            question_id="Q_COUNTEREVIDENCE",
            question="What premise is incomplete?",
            primitive="COUNTEREVIDENCE",
            tests_hypotheses=("H1", "H0"),
            expected_value=0.9,
            expected_cost=0.2,
            retrieval_target="premise",
            stop_condition="premise checked",
            score=0.7,
        ),
    ]
    answers = [
        QuestionAnswer(
            question_id="Q_CRITICAL_PATH",
            answer_status="supported",
            summary="State transition found.",
            supporting_evidence=(str(by_raw_ref["observation:state"].evidence_id),),
        ),
        QuestionAnswer(
            question_id="Q_ACTIVE_COMMITMENT",
            answer_status="supported",
            summary="Workflow gap found.",
            supporting_evidence=(str(by_raw_ref["observation:workflow"].evidence_id),),
        ),
        QuestionAnswer(
            question_id="Q_RECURRENCE",
            answer_status="supported",
            summary="Recurring gotcha found.",
            supporting_evidence=(str(by_raw_ref["observation:gotcha"].evidence_id),),
        ),
        QuestionAnswer(
            question_id="Q_COUNTEREVIDENCE",
            answer_status="supported",
            summary="Premise challenge found.",
            counterevidence=(str(by_raw_ref["observation:premise"].evidence_id),),
        ),
    ]

    selected, notes = _select_minimal_sufficient_evidence(
        list(evidence_by_key.values()),
        hypotheses=hypotheses,
        questions=questions,
        answers=answers,
        route="DEEP_INQUIRY_PATH",
        mode="deep",
        evidence_limit=12,
    )
    packet = _compile_context_packet(
        trigger,
        "DEEP_INQUIRY_PATH",
        hypotheses,
        questions,
        answers,
        selected,
        SufficiencyVerdict(
            "sufficient_for_reasoning",
            "capability packet probe",
            len(selected),
            len(answers),
            (),
        ),
        token_budget=4000,
    )

    selected_refs = {card.raw_content_ref for card in selected}
    decisive_refs = {
        item["raw_content_ref"] for item in packet["tiers"]["decisive_evidence"]
    }
    assert {f"observation:{key}" for key in important} <= selected_refs
    assert {f"observation:{key}" for key in important} <= decisive_refs
    assert notes["coverage"]["questions"] == 1.0
    assert packet["answer_obligations"]["premise_status"] == "stale_or_incomplete"
    assert {
        "current_blocker",
        "current_stage",
        "dynamic_state",
        "premise_challenge",
        "recurring_gotcha",
        "workflow_missing_step",
    } <= set(packet["state_contract"]["covered_slots"])
    assert packet["state_contract"]["premise_check"]["counterevidence_refs"]
    assert packet["budget"]["reservoir_evidence_count"] <= 12


def test_context_packet_model_first_suppresses_redundant_observation_evidence():
    tenant = uuid4()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_natural_text="Customer escalation says renewal risk increased.",
    )
    hypotheses = (
        Hypothesis(
            id="H1",
            claim="Renewal risk increased",
            confidence=0.72,
            impact_if_true="Account team needs an owner and mitigation plan.",
        ),
    )
    question = InquiryQuestion(
        question_id="Q_RISK",
        question="Is renewal risk increasing?",
        primitive="CONSTRAINT",
        tests_hypotheses=("H1",),
        expected_value=0.8,
        expected_cost=0.1,
        retrieval_target="model",
        stop_condition="risk evidence found",
        score=0.8,
    )
    model_card = EvidenceCard(
        evidence_id=uuid4(),
        source_type="model",
        source_ref="model:risk",
        source_ref_id=uuid4(),
        summary="Model says renewal risk increased because security review is blocked.",
        trust_tier="model",
        timestamp=datetime.now(timezone.utc),
        retrieval_paths={"focused_index"},
        retrieved_for_questions={"Q_RISK"},
        supports_hypotheses={"H1"},
        raw_content_ref="model:risk",
        token_estimate=16,
        score=0.9,
    )
    redundant_observation = EvidenceCard(
        evidence_id=uuid4(),
        source_type="observation",
        source_ref="observation:duplicate",
        source_ref_id=uuid4(),
        summary="Raw message repeats that renewal risk increased.",
        trust_tier="authoritative",
        timestamp=datetime.now(timezone.utc),
        retrieval_paths={"sage_reader"},
        retrieved_for_questions={"Q_RISK"},
        supports_hypotheses={"H1"},
        raw_content_ref="observation:duplicate",
        token_estimate=14,
        score=0.7,
    )
    counter_observation = EvidenceCard(
        evidence_id=uuid4(),
        source_type="observation",
        source_ref="observation:counter",
        source_ref_id=uuid4(),
        summary="Latest AE note says the customer accepted the mitigation.",
        trust_tier="authoritative",
        timestamp=datetime.now(timezone.utc),
        retrieval_paths={"sage_reader"},
        retrieved_for_questions={"Q_RISK"},
        weakens_hypotheses={"H1"},
        raw_content_ref="observation:counter",
        token_estimate=14,
        score=0.8,
    )
    answers = [
        QuestionAnswer(
            question_id="Q_RISK",
            answer_status="supported",
            summary="Model-backed risk evidence was found, with a counter-signal.",
            supporting_evidence=(str(model_card.evidence_id),),
            counterevidence=(str(counter_observation.evidence_id),),
        )
    ]

    packet = _compile_context_packet(
        trigger,
        "DEEP_INQUIRY_PATH",
        hypotheses,
        [question],
        answers,
        [model_card, redundant_observation, counter_observation],
        SufficiencyVerdict(
            "sufficient_for_reasoning",
            "model-first evidence test",
            3,
            1,
            (),
        ),
        token_budget=4000,
        evidence_mode="model_first",
    )

    decisive_refs = {
        item["raw_content_ref"] for item in packet["tiers"]["decisive_evidence"]
    }
    supporting_refs = {
        ref
        for group in packet["tiers"]["supporting_evidence_groups"]
        for ref in group["source_refs"]
    }
    assert "model:risk" in supporting_refs
    assert "observation:counter" in decisive_refs
    assert "observation:duplicate" not in decisive_refs | supporting_refs
    policy = packet["budget"]["evidence_policy"]
    assert policy["mode"] == "model_first"
    assert policy["packet_evidence_count"] == 2
    assert policy["suppressed_observation_count"] == 1


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
            "Acme cannot launch without SSO, and Sales promised " "go-live this month."
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
    assert result.retrieval_result.notes["inquiry"]["context_packet"]["question_path"]

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
        config=InquiryConfig(
            max_rounds=1,
            questions_per_round=2,
            persist=False,
            utility_governor_enabled=False,
        ),
    )

    assert len(provider.calls) == 1
    assert result.notes["question_planning"][0]["mode"] == "llm"
    assert result.notes["question_planning"][0]["llm_primitives"] == [
        "COUNTEREVIDENCE",
        "DEPENDENCY",
    ]
    assert [question.primitive for question in result.questions] == [
        "OWNERSHIP",
        "COUNTEREVIDENCE",
    ]
    assert result.questions[1].question == (
        "What fresh evidence would show the Acme SSO issue is not blocking launch?"
    )


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
        item.get("supports_hypotheses") for item in tiers["decisive_evidence"]
    ) or any(
        group.get("claim_supported") for group in tiers["supporting_evidence_groups"]
    )


async def test_inquiry_applies_result_and_action_budget_caps(tx_conn, fresh_db, tenant):
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
