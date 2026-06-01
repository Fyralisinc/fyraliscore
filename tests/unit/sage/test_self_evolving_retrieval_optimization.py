"""Iterative optimization tests for the self-evolving retrieval loop."""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.execution.inquiry import (
    EvidenceCard,
    Hypothesis,
    InquiryConfig,
    InquiryQuestion,
    QuestionAnswer,
    _select_minimal_sufficient_evidence,
    _candidate_questions,
    _compile_retrieval_plan,
    _apply_question_policy,
    _select_questions,
    QuestionPolicySignal,
)
from services.retrieval.primary import TriggerContext
from services.sage.affordances.repo import AffordanceProfilesRepo
from services.sage.affordances.types import RetrievalAffordanceProfile
from services.sage.outcome_evaluator import OutcomeEvaluator
from services.sage.reader import ReaderBudget, SynthesisReader
from services.sage.topology_optimizer import TopologyOptimizer
from tests.unit.sage._seed import ZERO_EMBEDDING, seed_model, seed_observation


pytestmark = pytest.mark.integration


def test_question_planner_prioritizes_owner_when_owner_is_the_bottleneck(
    tenant_id: UUID,
):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_natural_text="HelioWorks DataBridge handoff lacks a clear owner.",
        seed_entity_ids=[
            {"type": "customer", "id": "HelioWorks"},
            {"type": "system", "id": "DataBridge"},
        ],
    )
    candidates = _candidate_questions(
        trigger,
        _hypotheses(),
        evidence_by_key={},
        unknowns={"responsible owner", "affected commitment", "counterevidence"},
    )

    selected = _select_questions(
        candidates,
        questions_per_round=2,
        round_index=0,
        already_asked=set(),
    )

    assert selected[0].primitive == "OWNERSHIP"
    assert "OWNERSHIP" in {q.primitive for q in selected}


def test_question_planner_promotes_first_class_constraint_questions(
    tenant_id: UUID,
):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_natural_text=(
            "OrionHealth PatientSync goal is constrained by sandbox quota "
            "and limited integration capacity."
        ),
        seed_entity_ids=[
            {"type": "customer", "id": "OrionHealth"},
            {"type": "system", "id": "PatientSync"},
        ],
    )
    candidates = _candidate_questions(
        trigger,
        _hypotheses(),
        evidence_by_key={},
        unknowns={"blocking constraint", "counterevidence"},
    )

    selected = _select_questions(
        candidates,
        questions_per_round=2,
        round_index=0,
        already_asked=set(),
    )
    plan = _compile_retrieval_plan(selected[0], trigger, InquiryConfig())

    assert selected[0].primitive == "CONSTRAINT"
    assert any(action.target == "constraint_evidence" for action in plan)
    assert any(action.path == "temporal" for action in plan)


def test_question_planner_treats_cadence_as_recurrence_not_broad_portfolio(
    tenant_id: UUID,
):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_natural_text="VelaRetail ImportFlow stalls recur every month-end close.",
        seed_entity_ids=[
            {"type": "customer", "id": "VelaRetail"},
            {"type": "system", "id": "ImportFlow"},
        ],
    )
    candidates = _candidate_questions(
        trigger,
        _hypotheses(),
        evidence_by_key={},
        unknowns={"whether this is part of a broader recurring pattern"},
    )

    selected = _select_questions(
        candidates,
        questions_per_round=1,
        round_index=0,
        already_asked=set(),
    )

    assert selected[0].primitive == "RECURRENCE"


def test_learned_policy_overrides_static_question_order_when_utility_is_clear(
    tenant_id: UUID,
):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_natural_text="AcmeAtlas SsoRelay launch is blocked by review capacity.",
        seed_entity_ids=[],
    )
    candidates = _candidate_questions(
        trigger,
        _hypotheses(),
        evidence_by_key={},
        unknowns={"whether the dependency is binding", "counterevidence"},
    )

    policy = {
        "CONSTRAINT": QuestionPolicySignal(
            signal_type="T1",
            question_primitive="CONSTRAINT",
            attempts=12,
            successes=9,
            utility_score=1.8,
            total_credit=24.0,
            total_cost=3.0,
        ),
        "DEPENDENCY": QuestionPolicySignal(
            signal_type="T1",
            question_primitive="DEPENDENCY",
            attempts=12,
            successes=1,
            utility_score=-0.8,
            total_credit=2.0,
            total_cost=12.0,
        ),
    }
    policy_candidates = _apply_question_policy(candidates, question_policy=policy)
    selected = _select_questions(
        policy_candidates,
        questions_per_round=1,
        round_index=0,
        already_asked=set(),
    )

    assert selected[0].primitive == "CONSTRAINT"
    assert selected[0].score > next(
        q.score for q in policy_candidates if q.primitive == "DEPENDENCY"
    )


def test_policy_scales_retrieval_budgets_without_changing_plan_shape(
    tenant_id: UUID,
):
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_natural_text="VelaRetail ImportFlow stalls recur every month-end close.",
        seed_entity_ids=[],
    )
    question = next(
        q for q in _candidate_questions(
            trigger,
            _hypotheses(),
            evidence_by_key={},
            unknowns={"whether this is part of a broader recurring pattern"},
        )
        if q.primitive == "RECURRENCE"
    )
    cfg = InquiryConfig(semantic_budget=30)
    cold_plan = _compile_retrieval_plan(question, trigger, cfg)
    hot_plan = _compile_retrieval_plan(
        question,
        trigger,
        cfg,
        policy_signal=QuestionPolicySignal(
            signal_type="T1",
            question_primitive="RECURRENCE",
            attempts=20,
            successes=16,
            utility_score=1.6,
            total_credit=40.0,
            total_cost=8.0,
        ),
    )
    cold_targets = [action.target for action in cold_plan]
    hot_targets = [action.target for action in hot_plan]

    assert hot_targets == cold_targets
    assert sum(action.budget for action in hot_plan) > sum(
        action.budget for action in cold_plan
    )


def test_minimal_packet_preserves_value_while_dropping_redundant_evidence(
    tenant_id: UUID,
):
    questions = [
        _question("Q_DEPENDENCY", "DEPENDENCY"),
        _question("Q_COUNTEREVIDENCE", "COUNTEREVIDENCE"),
        _question("Q_OWNER", "OWNERSHIP"),
    ]
    support = _card(
        "Acme launch blocker depends on reviewer capacity",
        paths={"sage_reader"},
        questions={"Q_DEPENDENCY"},
        supports={"H1"},
        score=1.2,
    )
    counter = _card(
        "Signed expansion contradicts the stale churn risk",
        paths={"sage_reader"},
        questions={"Q_COUNTEREVIDENCE"},
        contradicts={"H1"},
        supports={"H0"},
        score=1.1,
    )
    owner = _card(
        "commitment launch owner=platform enablement",
        source_type="commitment",
        paths={"structural"},
        questions={"Q_OWNER"},
        supports={"H2"},
        score=0.9,
    )
    noise = [
        _card(
            f"Generic dashboard duplicate dependency noise {idx % 4}",
            source_type="model",
            paths={"semantic"},
            questions={"Q_DEPENDENCY"},
            score=0.55 - idx * 0.004,
        )
        for idx in range(42)
    ]
    cards = [support, counter, owner, *noise]
    selected, report = _select_minimal_sufficient_evidence(
        cards,
        hypotheses=(
            Hypothesis("H1", "The signal is a real blocker.", 0.7, "high"),
            Hypothesis("H0", "The signal is stale.", 0.2, "low"),
            Hypothesis("H2", "An owner/action anchor exists.", 0.4, "medium"),
        ),
        questions=questions,
        answers=[
            QuestionAnswer(
                "Q_DEPENDENCY",
                "supported",
                "Dependency answered.",
                supporting_evidence=(str(support.evidence_id),),
            ),
            QuestionAnswer(
                "Q_COUNTEREVIDENCE",
                "supported",
                "Counterevidence answered.",
                counterevidence=(str(counter.evidence_id),),
            ),
            QuestionAnswer(
                "Q_OWNER",
                "supported",
                "Owner answered.",
                supporting_evidence=(str(owner.evidence_id),),
            ),
        ],
        route="DEEP_INQUIRY_PATH",
        mode="deep",
        evidence_limit=80,
    )

    selected_ids = {card.evidence_id for card in selected}
    assert support.evidence_id in selected_ids
    assert counter.evidence_id in selected_ids
    assert owner.evidence_id in selected_ids
    assert len(selected) <= 21
    assert report["dropped_count"] >= 24
    assert report["drop_ratio"] >= 0.50
    assert report["coverage"]["questions"] == 1.0
    assert report["coverage"]["supported_answers"] == 1.0
    assert report["coverage"]["has_counterevidence"] is True
    assert report["coverage"]["has_action_anchor"] is True


@pytest.mark.asyncio
async def test_reader_writer_feedback_lifts_useful_node_into_next_read(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
):
    obs_id = await seed_observation(
        gateway_pool,
        tenant_id=tenant_id,
        content_text="Writer validated the hidden dependency model.",
    )
    target_id = await seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=obs_id,
        natural="Hidden relay record maps the release dependency to reviewer capacity",
        supporting_event_ids=[obs_id],
        signal_readings=[{"kind": "observe", "event_id": str(obs_id), "weight": 1.0}],
    )
    decoy_ids = [
        await seed_model(
            gateway_pool,
            tenant_id=tenant_id,
            born_from_event_id=obs_id,
            natural=f"Decoy dependency profile {idx} for generic release tracking",
            supporting_event_ids=[obs_id],
        )
        for idx in range(12)
    ]
    aff_repo = AffordanceProfilesRepo(gateway_pool, tenant_id=tenant_id)
    for decoy_id in decoy_ids:
        await aff_repo.upsert(
            RetrievalAffordanceProfile(
                model_id=decoy_id,
                tenant_id=tenant_id,
                answers_question_primitives=["DEPENDENCY"],
                utility_score=2.40,
            )
        )
    seed_profile = await aff_repo.upsert(
        RetrievalAffordanceProfile(
            model_id=target_id,
            tenant_id=tenant_id,
            answers_question_primitives=["DEPENDENCY"],
            utility_score=2.35,
        )
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        seed_natural_text="Release dependency needs investigation.",
        seed_entity_ids=[],
        precomputed_seed_vector=ZERO_EMBEDDING,
    )

    async with gateway_pool.acquire() as conn:
        before = await SynthesisReader(
            budget=ReaderBudget(
                max_nodes=8,
                max_edges=8,
                lexical_candidates=0,
                affordance_candidates=12,
            )
        ).read(
            conn=conn,
            tenant_id=tenant_id,
            trigger=trigger,
            question_id="Q_CRITICAL_PATH",
            question="Which dependency is binding?",
            question_primitive="DEPENDENCY",
        )

        session_id = await _seed_writer_success_session(
            conn,
            tenant_id=tenant_id,
            used_model_id=target_id,
            primitive="DEPENDENCY",
        )
        summary = await OutcomeEvaluator(
            pool=None,
            tenant_id=tenant_id,
        ).evaluate(inquiry_session_id=session_id, conn=conn)
        report = await TopologyOptimizer(
            pool=None,
            tenant_id=tenant_id,
        ).optimize(
            inquiry_session_id=session_id,
            trigger_event="validated_synthesis_diff_applied",
            conn=conn,
        )
        after = await SynthesisReader(
            budget=ReaderBudget(
                max_nodes=8,
                max_edges=8,
                lexical_candidates=0,
                affordance_candidates=12,
            )
        ).read(
            conn=conn,
            tenant_id=tenant_id,
            trigger=trigger,
            question_id="Q_CRITICAL_PATH",
            question="Which dependency is binding?",
            question_primitive="DEPENDENCY",
        )
        updated_profile = await aff_repo.get(target_id, conn=conn)
        attribution = await conn.fetchrow(
            """
            SELECT writer_used, writer_credit_score
            FROM sage_reader_decision_attributions
            WHERE tenant_id = $1
              AND inquiry_session_id = $2
              AND model_id = $3
            """,
            tenant_id,
            session_id,
            target_id,
        )
        policy_stats = await conn.fetchrow(
            """
            SELECT attempts, successes, total_credit, utility_score
            FROM sage_question_policy_stats
            WHERE tenant_id = $1
              AND signal_type = 'T1'
              AND question_primitive = 'DEPENDENCY'
            """,
            tenant_id,
        )

    assert target_id not in {trace.model_id for trace in before.activations}
    assert summary.events_by_type.get("node_used_in_valid_diff") == 1
    assert summary.events_by_type.get("reader_decision_used_in_valid_diff") == 1
    assert report.affordance_reinforces == 1
    assert report.question_policy_updates >= 1
    assert updated_profile is not None
    assert updated_profile.utility_score > seed_profile.utility_score
    assert attribution is not None
    assert attribution["writer_used"] is True
    assert attribution["writer_credit_score"] > 0
    assert policy_stats is not None
    assert policy_stats["attempts"] >= 1
    assert policy_stats["successes"] >= 1
    assert policy_stats["total_credit"] > 0
    assert policy_stats["utility_score"] > 0
    assert target_id in {trace.model_id for trace in after.activations}
    assert target_id in {model.id for model in after.models}


def _hypotheses() -> tuple[Hypothesis, ...]:
    return (
        Hypothesis("H1", "The signal requires a synthesis update.", 0.72, "high"),
        Hypothesis("H0", "The signal is already captured.", 0.18, "low"),
    )


def _question(question_id: str, primitive: str) -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question=f"What should Fyralis know for {primitive}?",
        primitive=primitive,
        tests_hypotheses=("H1",),
        expected_value=0.8,
        expected_cost=0.2,
        retrieval_target="evidence",
        stop_condition="answer",
        score=0.8,
    )


def _card(
    summary: str,
    *,
    source_type: str = "observation",
    paths: set[str],
    questions: set[str],
    supports: set[str] | None = None,
    weakens: set[str] | None = None,
    contradicts: set[str] | None = None,
    score: float,
) -> EvidenceCard:
    source_ref_id = uuid7()
    card = EvidenceCard(
        evidence_id=uuid7(),
        source_type=source_type,
        source_ref=f"{source_type}:{source_ref_id}",
        source_ref_id=source_ref_id,
        summary=summary,
        trust_tier="authoritative" if source_type != "model" else "model",
        timestamp=None,
        retrieval_paths=set(paths),
        retrieved_for_questions=set(questions),
        supports_hypotheses=set(supports or set()),
        weakens_hypotheses=set(weakens or set()),
        contradicts_hypotheses=set(contradicts or set()),
        raw_content_ref=f"{source_type}:{source_ref_id}",
        token_estimate=max(1, len(summary) // 4),
        score=score,
    )
    return card


async def _seed_writer_success_session(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    used_model_id: UUID,
    primitive: str,
) -> UUID:
    trigger_id = uuid7()
    think_run_id = uuid7()
    session_id = uuid7()
    await conn.execute(
        """
        INSERT INTO applied_triggers (
          trigger_id, tenant_id, diff_hash, trigger_kind, outcome
        ) VALUES ($1, $2, $3, 'T1', 'success')
        """,
        trigger_id,
        tenant_id,
        f"self-evolving-loop-{trigger_id}",
    )
    await conn.execute(
        """
        INSERT INTO think_runs (
          id, tenant_id, trigger_id, trigger_kind, status, ops_applied
        ) VALUES ($1, $2, $3, 'T1', 'success', $4::jsonb)
        """,
        think_run_id,
        tenant_id,
        trigger_id,
        json.dumps({
            "claim_ops": [{"op": "update", "model_id": str(used_model_id)}],
            "edge_ops": [],
            "act_ops": [],
            "resource_ops": [],
        }),
    )
    await conn.execute(
        """
        INSERT INTO inquiry_sessions (
          id, tenant_id, signal_ref_type, signal_ref_id,
          route, status, stop_status, context_packet, think_run_id
        ) VALUES (
          $1, $2, 'internal', NULL,
          'DEEP_INQUIRY_PATH', 'completed', 'sufficient_for_reasoning',
          $3::jsonb, $4
        )
        """,
        session_id,
        tenant_id,
        json.dumps({
            "source_metadata": {"trigger_kind": "T1"},
            "question_path": [{"primitive": primitive}],
            "resolved_entities": [],
            "budget": {"estimated_tokens_used": 1000},
        }),
        think_run_id,
    )
    await conn.execute(
        """
        INSERT INTO sage_reader_decision_attributions (
          id, tenant_id, inquiry_session_id,
          question_id, question_primitive, question,
          question_score, expected_value, expected_cost,
          signal_type, entities, model_id,
          selected, selection_rank, activation_score,
          activation_reasons, source_breakdown, retrieval_actions,
          projected_evidence_refs, evidence_in_packet_count
        ) VALUES (
          $1, $2, $3,
          'Q_CRITICAL_PATH', $4, 'Which dependency is binding?',
          0.80, 0.90, 0.24,
          'T1', '[]'::jsonb, $5,
          TRUE, 0, 0.82,
          '["affordance:DEPENDENCY"]'::jsonb,
          '{"affordance": 0.82}'::jsonb,
          '[{"path": "sage_reader", "target": "synthesis_reader"}]'::jsonb,
          '[]'::jsonb, 0
        )
        """,
        uuid7(),
        tenant_id,
        session_id,
        primitive,
        used_model_id,
    )
    return session_id
