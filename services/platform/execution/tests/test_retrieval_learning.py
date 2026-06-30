from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.platform.execution import inquiry, retrieval_learning
from services.platform.execution.config import InquiryConfig
from services.platform.execution.motif_utils import motif_signature_for
from services.platform.execution.types import (
    EvidenceCard,
    InquiryQuestion,
    RetrievalAction,
)
from services.reasoning.retrieval.primary import TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[{"type": "customer", "id": "HarborRail"}],
        seed_natural_text="HarborRail procurement blocker needs audit evidence",
    )


def _question() -> InquiryQuestion:
    return InquiryQuestion(
        question_id="Q_CONSTRAINT",
        question="Which procurement constraint blocks HarborRail renewal?",
        primitive="CONSTRAINT",
        tests_hypotheses=("H1",),
        expected_value=0.9,
        expected_cost=0.2,
        retrieval_target="constraint_evidence",
        stop_condition="constraint found",
        score=0.7,
        round_index=1,
    )


def _evidence_card(question_id: str) -> EvidenceCard:
    return EvidenceCard(
        evidence_id=uuid4(),
        source_type="model",
        source_ref=f"model:{uuid4()}",
        source_ref_id=uuid4(),
        summary="HarborRail procurement blocker with explicit audit evidence",
        trust_tier="model",
        timestamp=None,
        retrieval_paths={"structural", "semantic"},
        retrieved_for_questions={question_id},
        supports_hypotheses={"H1"},
        score=0.8,
    )


def test_retrieval_learning_helpers_keep_legacy_inquiry_identity() -> None:
    assert (
        inquiry._decay_sage_route_utilities
        is retrieval_learning.decay_sage_route_utilities
    )
    assert (
        inquiry._load_question_policy_stats
        is retrieval_learning.load_question_policy_stats
    )
    assert (
        inquiry._load_retrieval_motifs_for_questions
        is retrieval_learning.load_retrieval_motifs_for_questions
    )
    assert inquiry._learn_retrieval_motifs is retrieval_learning.learn_retrieval_motifs
    assert (
        inquiry._learn_sage_route_utilities
        is retrieval_learning.learn_sage_route_utilities
    )
    assert (
        inquiry._load_sage_route_utilities
        is retrieval_learning.load_sage_route_utilities
    )
    assert (
        inquiry._penalize_retrieval_motifs
        is retrieval_learning.penalize_retrieval_motifs
    )
    assert (
        inquiry._motif_failure_penalties is retrieval_learning.motif_failure_penalties
    )
    assert (
        inquiry._is_low_value_model_noise is retrieval_learning.is_low_value_model_noise
    )


@pytest.mark.asyncio
async def test_load_question_policy_stats_normalizes_rows() -> None:
    tenant_id = uuid4()

    class FakeConn:
        async def fetchval(self, query, *args):
            assert "sage_question_policy_stats" in query
            return "sage_question_policy_stats"

        async def fetch(self, query, *args):
            assert args == (tenant_id, "T1")
            return [
                {
                    "signal_type": "T1",
                    "question_primitive": "constraint",
                    "attempts": 3,
                    "successes": 2,
                    "utility_score": "1.25",
                    "total_credit": "4.0",
                    "total_cost": "1.5",
                },
                {
                    "signal_type": "T1",
                    "question_primitive": "",
                    "attempts": 99,
                    "successes": 99,
                    "utility_score": 99,
                    "total_credit": 99,
                    "total_cost": 99,
                },
            ]

    stats = await retrieval_learning.load_question_policy_stats(
        FakeConn(),  # type: ignore[arg-type]
        tenant_id=tenant_id,
        signal_type="T1",
    )

    assert set(stats) == {"CONSTRAINT"}
    signal = stats["CONSTRAINT"]
    assert signal.question_primitive == "CONSTRAINT"
    assert signal.attempts == 3
    assert signal.successes == 2
    assert signal.utility_score == pytest.approx(1.25)


@pytest.mark.asyncio
async def test_load_sage_route_utilities_normalizes_rows() -> None:
    trigger = _trigger()

    class FakeConn:
        async def fetchval(self, query, *args):
            assert "sage_retrieval_route_utilities" in query
            return "sage_retrieval_route_utilities"

        async def fetch(self, query, *args):
            assert args[0] == trigger.tenant_id
            assert args[1] == "T1"
            return [
                {
                    "signature_hash": "abc",
                    "path": "semantic",
                    "signal_type": "T1",
                    "subkind": None,
                    "question_primitive": "CONSTRAINT",
                    "attempts": 5,
                    "wins": 3,
                    "skips": 1,
                    "returned_models": 12,
                    "returned_observations": 2,
                    "selected_evidence": 4,
                    "elapsed_ms_total": 120,
                    "latency_ms_p95": 50,
                    "budget_total": 90,
                    "total_cost": 1.4,
                    "total_quality_credit": 3.2,
                    "utility_score": 0.61,
                    "confidence": 0.52,
                }
            ]

    utilities = await retrieval_learning.load_sage_route_utilities(
        FakeConn(),  # type: ignore[arg-type]
        trigger,
        question_primitives=("CONSTRAINT",),
    )

    assert len(utilities) == 1
    utility = utilities[0]
    assert utility.path == "semantic"
    assert utility.question_primitive == "CONSTRAINT"
    assert utility.utility_score == pytest.approx(0.61)
    assert utility.avg_budget == pytest.approx(18.0)


@pytest.mark.asyncio
async def test_load_retrieval_motifs_for_questions_filters_by_signature() -> None:
    trigger = _trigger()
    question = _question()
    matching_motif_id = uuid4()

    class FakeConn:
        async def fetchval(self, query, *args):
            assert "retrieval_motifs" in query
            return "retrieval_motifs"

        async def fetch(self, query, *args):
            assert args[0] == trigger.tenant_id
            assert args[1] == ["CONSTRAINT"]
            return [
                {
                    "id": uuid4(),
                    "signature": {
                        "signal_type": "T1",
                        "question_primitive": "CONSTRAINT",
                    },
                    "question_primitive": "CONSTRAINT",
                    "plan": {"version": 1, "actions": []},
                    "utility_score": 99.0,
                    "success_count": 99,
                },
                {
                    "id": matching_motif_id,
                    "signature": motif_signature_for(trigger, question.primitive),
                    "question_primitive": "CONSTRAINT",
                    "plan": {"version": 1, "actions": [{"path": "semantic"}]},
                    "utility_score": 2.0,
                    "success_count": 3,
                },
            ]

    matches = await retrieval_learning.load_retrieval_motifs_for_questions(
        FakeConn(),  # type: ignore[arg-type]
        trigger,
        [question],
        InquiryConfig(),
    )

    assert matches[question.question_id].id == matching_motif_id
    assert matches[question.question_id].match_score >= 0.8


@pytest.mark.asyncio
async def test_learn_retrieval_motifs_writes_successful_staged_plan() -> None:
    trigger = _trigger()
    question = _question()
    card = _evidence_card(question.question_id)
    calls: list[tuple[str, tuple[object, ...]]] = []

    class FakeConn:
        async def fetchval(self, query, *args):
            assert "retrieval_motifs" in query
            return "retrieval_motifs"

        async def execute(self, query, *args):
            calls.append((query, args))

    result = SimpleNamespace(
        questions=(question,),
        retrieval_actions=(
            RetrievalAction(
                question.question_id,
                "structural",
                "goal_resource_bridge",
                budget=20,
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "constraint_evidence",
                budget=30,
            ),
        ),
        evidence_cards=(card,),
    )

    await retrieval_learning.learn_retrieval_motifs(
        FakeConn(),  # type: ignore[arg-type]
        result,  # type: ignore[arg-type]
        trigger,
    )

    assert len(calls) == 1
    query, args = calls[0]
    assert "INSERT INTO retrieval_motifs" in query
    assert "ON CONFLICT" in query
    assert args[1] == trigger.tenant_id
    assert args[4] == "CONSTRAINT"
    plan = json.loads(str(args[5]))
    assert plan["execution"] == "staged"
    assert len(plan["actions"]) == 2
    assert args[7] > 0


@pytest.mark.asyncio
async def test_learn_sage_route_utilities_writes_action_and_primary_outcomes() -> None:
    trigger = _trigger()
    question = _question()
    card = _evidence_card(question.question_id)
    calls: list[list[tuple[object, ...]]] = []

    class FakeConn:
        async def fetchval(self, query, *args):
            assert "sage_retrieval_route_utilities" in query
            return "sage_retrieval_route_utilities"

        async def executemany(self, query, params):
            assert "INSERT INTO sage_retrieval_route_utilities" in query
            assert "ON CONFLICT" in query
            calls.append(list(params))

    result = SimpleNamespace(
        session_id=uuid4(),
        questions=(question,),
        retrieval_actions=(
            RetrievalAction(
                question.question_id,
                "semantic",
                "constraint_evidence",
                budget=30,
            ),
        ),
        evidence_cards=(card,),
        notes={
            "retrieval_action_timings": [
                {
                    "question_id": question.question_id,
                    "path": "semantic",
                    "target": "constraint_evidence",
                    "elapsed_ms": 42,
                    "returned": True,
                    "models": 8,
                    "observations": 1,
                }
            ],
            "retrieval_stage_timings": [
                {
                    "stage": "primary_retrieve",
                    "primary_pathway_timings": [
                        {
                            "stage": "pathway_L",
                            "elapsed_ms": 8,
                            "models": 6,
                            "observations": 0,
                        }
                    ],
                }
            ],
        },
    )

    await retrieval_learning.learn_sage_route_utilities(
        FakeConn(),  # type: ignore[arg-type]
        result,  # type: ignore[arg-type]
        trigger,
    )

    assert len(calls) == 1
    paths = {str(params[5]) for params in calls[0]}
    assert paths == {"semantic", "L"}
    semantic = next(params for params in calls[0] if params[5] == "semantic")
    assert semantic[1] == "T1"
    assert semantic[3] == "CONSTRAINT"
    assert semantic[6] == 1
    assert semantic[11] >= 1
    assert float(semantic[17]) > 0


@pytest.mark.asyncio
async def test_decay_sage_route_utilities_scopes_tenant_and_returns_count() -> None:
    tenant_id = uuid4()
    calls: list[tuple[str, tuple[object, ...]]] = []

    class FakeConn:
        async def fetchval(self, query, *args):
            assert "sage_retrieval_route_utilities" in query
            return "sage_retrieval_route_utilities"

        async def execute(self, query, *args):
            calls.append((query, args))
            return "UPDATE 3"

    count = await retrieval_learning.decay_sage_route_utilities(
        FakeConn(),  # type: ignore[arg-type]
        tenant_id=tenant_id,
        stale_after_days=7,
        factor=0.9,
    )

    assert count == 3
    query, args = calls[0]
    assert "WHERE tenant_id = $2" in query
    assert args == (0.9, tenant_id, 7)


def test_is_low_value_model_noise_requires_unlinked_model_evidence() -> None:
    linked = _evidence_card("Q_CONSTRAINT")
    unlinked = _evidence_card("Q_CONSTRAINT")
    unlinked.supports_hypotheses.clear()

    assert not retrieval_learning.is_low_value_model_noise(linked)
    assert retrieval_learning.is_low_value_model_noise(unlinked)
