from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.execution import inquiry
from services.execution.inquiry import (
    EvidenceCard,
    InquiryConfig,
    InquiryQuestion,
    LearnedRetrievalMotif,
    RetrievalAction,
    _bind_action_to_previous_results,
    _compile_motif_retrieval_plan,
    _compile_static_retrieval_plan,
    _execute_question_retrieval_actions,
    _load_retrieval_motifs_for_questions,
    _motif_failure_penalties,
    _motif_signature_for,
    _motif_plan_from_actions,
    _penalize_retrieval_motifs,
)
from services.retrieval.pathways import PathwayResult
from services.retrieval.primary import TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[{"type": "customer", "id": str(uuid4())}],
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


def test_motif_compiler_overlays_static_safe_actions():
    trigger = _trigger()
    question = _question()
    static_actions = _compile_static_retrieval_plan(
        question,
        trigger,
        InquiryConfig(),
    )
    motif = LearnedRetrievalMotif(
        id=uuid4(),
        signature={},
        question_primitive="CONSTRAINT",
        plan={
            "version": 1,
            "actions": [
                {
                    "path": "structural",
                    "target": "goal_resource_bridge",
                    "budget": 11,
                    "stage": 1,
                },
                {
                    "path": "semantic",
                    "target": "constraint_evidence",
                    "budget": 7,
                    "stage": 2,
                    "bind_previous_scope": True,
                },
                {
                    "path": "made_up",
                    "target": "unsafe",
                    "budget": 99,
                    "stage": 1,
                },
            ],
        },
        utility_score=1.5,
        success_count=3,
        match_score=0.8,
    )

    actions = _compile_motif_retrieval_plan(
        question,
        static_actions,
        motif,
        InquiryConfig(),
    )

    assert [(a.path, a.target, a.budget) for a in actions] == [
        ("structural", "goal_resource_bridge", 11),
        ("semantic", "constraint_evidence", 7),
    ]
    assert actions[1].filters["_bind_previous_scope"] is True
    assert actions[1].filters["_motif_id"] == str(motif.id)


def test_binding_adds_prior_models_and_scope_entities():
    trigger = _trigger()
    model_id = uuid4()
    scoped_entity_id = uuid4()
    prior = PathwayResult(
        models=[
            SimpleNamespace(
                id=model_id,
                scope_entities=[
                    {"type": "commitment", "id": str(scoped_entity_id)}
                ],
            )
        ],
        resources=[],
        acts={"goals": [], "commitments": [], "decisions": []},
        source_pathway="A",
    )
    action = RetrievalAction(
        "Q_CONSTRAINT",
        "semantic",
        "constraint_evidence",
        query="constraint",
        filters={"_bind_previous_scope": True, "_motif_stage": 2},
        budget=10,
    )

    bound = _bind_action_to_previous_results(action, trigger, [prior])

    assert str(model_id) in bound.filters["seed_model_ids"]
    assert {"type": "commitment", "id": str(scoped_entity_id)} in bound.filters[
        "seed_entities"
    ]
    assert bound.filters["_bound_scope"]["model_count"] == 1


@pytest.mark.asyncio
async def test_staged_motif_execution_binds_second_stage(monkeypatch):
    trigger = _trigger()
    question = _question()
    model_id = uuid4()
    seen_actions: list[RetrievalAction] = []

    async def fake_execute_action(
        action,
        trigger_arg,
        conn,
        embedder,
        cfg,
        *,
        read_pool=None,
    ):
        seen_actions.append(action)
        if action.path == "structural":
            return PathwayResult(
                models=[
                    SimpleNamespace(
                        id=model_id,
                        scope_entities=[
                            {"type": "commitment", "id": str(uuid4())}
                        ],
                    )
                ],
                source_pathway="A",
            )
        return PathwayResult(models=[], source_pathway="B")

    monkeypatch.setattr(inquiry, "_execute_action", fake_execute_action)
    plan = inquiry._QuestionRetrievalPlan(
        question=question,
        actions_to_run=[
            RetrievalAction(
                question.question_id,
                "structural",
                "goal_resource_bridge",
                filters={"_motif_id": str(uuid4()), "_motif_stage": 1},
                budget=10,
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "constraint_evidence",
                query="constraint",
                filters={
                    "_motif_id": str(uuid4()),
                    "_motif_stage": 2,
                    "_bind_previous_scope": True,
                },
                budget=10,
            ),
        ],
    )

    records = await _execute_question_retrieval_actions(
        [plan],
        trigger,
        "conn",  # type: ignore[arg-type]
        None,
        InquiryConfig(question_action_parallel_enabled=False),
        {},
        read_pool=None,
    )

    assert len(records[question.question_id]) == 2
    assert seen_actions[1].filters["_bound_scope"]["model_count"] == 1
    assert str(model_id) in seen_actions[1].filters["seed_model_ids"]


def test_motif_plan_from_actions_creates_staged_recipe():
    actions = [
        RetrievalAction("Q", "structural", "goal_resource_bridge", budget=20),
        RetrievalAction("Q", "semantic", "constraint_evidence", budget=30),
        RetrievalAction("Q", "temporal", "recent_constraint_observations", budget=15),
    ]

    plan = _motif_plan_from_actions(actions)

    assert plan["execution"] == "staged"
    assert plan["actions"][0]["stage"] == 1
    later = [a for a in plan["actions"] if a["stage"] == 2]
    assert later
    assert all(a["bind_previous_scope"] for a in later)


@pytest.mark.asyncio
async def test_load_retrieval_motif_matches_signature():
    trigger = _trigger()
    question = _question()
    motif_id = uuid4()

    class FakeConn:
        async def fetchval(self, query, *args):
            assert "retrieval_motifs" in query
            return "retrieval_motifs"

        async def fetch(self, query, *args):
            assert args[0] == trigger.tenant_id
            return [
                {
                    "id": motif_id,
                    "signature": _motif_signature_for(trigger, question.primitive),
                    "question_primitive": question.primitive,
                    "plan": {
                        "version": 1,
                        "actions": [
                            {
                                "path": "structural",
                                "target": "goal_resource_bridge",
                                "budget": 10,
                                "stage": 1,
                            }
                        ],
                    },
                    "utility_score": 2.0,
                    "success_count": 3,
                }
            ]

    matches = await _load_retrieval_motifs_for_questions(
        FakeConn(),  # type: ignore[arg-type]
        trigger,
        [question],
        InquiryConfig(),
    )

    assert matches[question.question_id].id == motif_id


def test_motif_failure_penalty_detects_wide_no_packet_value():
    motif_id = uuid4()
    result = SimpleNamespace(
        retrieval_actions=(
            RetrievalAction(
                "Q_CONSTRAINT",
                "semantic",
                "constraint_evidence",
                filters={"_motif_id": str(motif_id)},
                budget=40,
            ),
        ),
        evidence_cards=(),
        context_packet={"tiers": {}},
        notes={
            "retrieval_action_timings": [
                {
                    "question_id": "Q_CONSTRAINT",
                    "path": "semantic",
                    "motif_id": str(motif_id),
                    "models": 96,
                    "observations": 0,
                }
            ]
        },
    )

    penalties = _motif_failure_penalties(result)  # type: ignore[arg-type]

    assert len(penalties) == 1
    penalty = penalties[0]
    assert penalty.motif_id == motif_id
    assert "no_packet_evidence" in penalty.reasons
    assert "wide_motif_selection" in penalty.reasons
    assert penalty.cost > 1.0


def test_motif_failure_penalty_does_not_punish_useful_broad_return():
    motif_id = uuid4()
    cards = []
    decisive = []
    for _ in range(4):
        card = EvidenceCard(
            evidence_id=uuid4(),
            source_type="model",
            source_ref=f"model:{uuid4()}",
            source_ref_id=uuid4(),
            summary="HarborRail procurement blocker with explicit owner evidence",
            trust_tier="model",
            timestamp=None,
            retrieval_paths={"semantic"},
            retrieved_for_questions={"Q_CONSTRAINT"},
            supports_hypotheses={"H1"},
            score=0.8,
        )
        cards.append(card)
        decisive.append({"evidence_id": str(card.evidence_id)})
    result = SimpleNamespace(
        retrieval_actions=(
            RetrievalAction(
                "Q_CONSTRAINT",
                "semantic",
                "constraint_evidence",
                filters={"_motif_id": str(motif_id)},
                budget=40,
            ),
        ),
        evidence_cards=tuple(cards),
        context_packet={"tiers": {"decisive_evidence": decisive}},
        notes={
            "retrieval_action_timings": [
                {
                    "question_id": "Q_CONSTRAINT",
                    "path": "semantic",
                    "motif_id": str(motif_id),
                    "models": 120,
                    "observations": 0,
                }
            ]
        },
    )

    assert _motif_failure_penalties(result) == []  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_penalize_retrieval_motifs_updates_failure_accounting():
    trigger = _trigger()
    motif_id = uuid4()
    calls = []

    class FakeConn:
        async def fetchval(self, query, *args):
            assert "retrieval_motifs" in query
            return "retrieval_motifs"

        async def execute(self, query, *args):
            calls.append((query, args))

    result = SimpleNamespace(
        retrieval_actions=(
            RetrievalAction(
                "Q_CONSTRAINT",
                "semantic",
                "constraint_evidence",
                filters={"_motif_id": str(motif_id)},
                budget=40,
            ),
        ),
        evidence_cards=(),
        context_packet={"tiers": {}},
        notes={
            "retrieval_action_timings": [
                {
                    "question_id": "Q_CONSTRAINT",
                    "motif_id": str(motif_id),
                    "models": 96,
                    "observations": 0,
                }
            ]
        },
    )

    await _penalize_retrieval_motifs(  # type: ignore[arg-type]
        FakeConn(), result, trigger
    )

    assert len(calls) == 1
    query, args = calls[0]
    assert "failure_count = failure_count + 1" in query
    assert "maturity = CASE" in query
    assert args[0] == trigger.tenant_id
    assert args[1] == motif_id
    assert args[2] > 1.0
    assert args[3] == 3
    assert args[4] == 0.0
