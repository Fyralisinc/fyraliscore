from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.platform.execution import inquiry, reflective_learning
from services.platform.execution.types import (
    EvidenceCard,
    InquiryQuestion,
    RetrievalAction,
    SufficiencyVerdict,
)
from services.reasoning.retrieval.primary import TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[{"type": "customer", "label": "DeltaCo"}],
        seed_natural_text=(
            "DeltaCo SAML launch is blocked again; PR may already be deployed."
        ),
    )


def _question(
    primitive: str,
    *,
    question_id: str,
    score: float,
) -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question=f"What should we ask for {primitive.lower()}?",
        primitive=primitive,
        tests_hypotheses=("H1",),
        expected_value=0.8,
        expected_cost=0.2,
        retrieval_target="test",
        stop_condition="resolved",
        score=score,
    )


def _card(question_id: str, *, path: str = "temporal") -> EvidenceCard:
    return EvidenceCard(
        evidence_id=uuid4(),
        source_type="observation",
        source_ref=f"observation:{uuid4()}",
        source_ref_id=uuid4(),
        summary="PR #847 was merged and deployed; rollback was not needed.",
        trust_tier="authoritative",
        timestamp=None,
        retrieval_paths={path},
        retrieved_for_questions={question_id},
        supports_hypotheses={"H0"},
        weakens_hypotheses={"H1"},
        score=0.92,
    )


def _result(*, rule_id=None) -> SimpleNamespace:
    owner = _question("OWNERSHIP", question_id="Q_OWNER", score=0.85)
    counter = _question("COUNTEREVIDENCE", question_id="Q_COUNTER", score=0.62)
    card = _card(counter.question_id)
    filters = (
        {"_reflective_rule_ids": [str(rule_id)], "_reflective_rule_match_score": 0.9}
        if rule_id
        else {}
    )
    return SimpleNamespace(
        session_id=uuid4(),
        questions=(owner, counter),
        retrieval_actions=(
            RetrievalAction(owner.question_id, "semantic", "owner_lookup", budget=40),
            RetrievalAction(counter.question_id, "semantic", "counter", budget=40),
            RetrievalAction(
                counter.question_id,
                "temporal",
                "recent_counterevidence",
                query="blocked",
                filters=filters,
                budget=30,
            ),
        ),
        evidence_cards=(card,),
        context_packet={
            "tiers": {"decisive_evidence": [{"evidence_id": str(card.evidence_id)}]}
        },
        sufficiency=SufficiencyVerdict(
            status="sufficient_for_reasoning",
            reason="fresh counterevidence found",
            evidence_count=1,
            answered_questions=1,
            remaining_unknowns=("current status",),
        ),
        notes={},
    )


def test_reflective_learning_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._learn_reflective_rules is reflective_learning.learn_reflective_rules
    assert (
        inquiry._propose_reflective_rule_candidates
        is reflective_learning.propose_reflective_rule_candidates
    )
    assert (
        inquiry._replay_reflective_rule_candidate
        is reflective_learning.replay_reflective_rule_candidate
    )


def test_propose_reflective_rule_candidate_from_successful_trace() -> None:
    candidates = reflective_learning.propose_reflective_rule_candidates(
        _result(),  # type: ignore[arg-type]
        _trigger(),
    )

    assert candidates
    candidate = candidates[0]
    assert candidate.signature["signal_type"] == "T1"
    assert candidate.rule_pack["question_rules"][0]["prefer_primitive"] == (
        "COUNTEREVIDENCE"
    )
    assert candidate.rule_pack["avoid_rules"][0]["primitive"] == "OWNERSHIP"
    action_rule = candidate.rule_pack["action_rules"][0]
    assert action_rule["prefer_paths"] == ["temporal"]
    assert {"deployed", "rollback"} & set(action_rule["semantic_terms"])


def test_replay_reflective_rule_candidate_promotes_when_order_improves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INQUIRY_REFLECTIVE_RULE_PROMOTION_MIN_DELTA", "0.01")
    result = _result()
    candidate = reflective_learning.propose_reflective_rule_candidates(
        result,  # type: ignore[arg-type]
        _trigger(),
    )[0]

    replay = reflective_learning.replay_reflective_rule_candidate(
        result,  # type: ignore[arg-type]
        _trigger(),
        candidate,
    )

    assert replay.utility_delta > 0
    assert replay.decision == "promoted"
    assert replay.candidate_score > replay.baseline_score


def test_reflective_rule_attributions_score_rule_tagged_actions() -> None:
    rule_id = uuid4()
    attributions = reflective_learning.reflective_rule_attributions_from_result(
        _result(rule_id=rule_id),  # type: ignore[arg-type]
    )

    assert len(attributions) == 1
    attribution = attributions[0]
    assert attribution.rule_id == rule_id
    assert attribution.question_primitive == "COUNTEREVIDENCE"
    assert attribution.action_path == "temporal"
    assert attribution.selected_evidence_count == 1
    assert attribution.credit > attribution.cost


@pytest.mark.asyncio
async def test_persist_attribution_and_apply_credit_write_expected_rows() -> None:
    trigger = _trigger()
    result = _result(rule_id=uuid4())
    attributions = reflective_learning.reflective_rule_attributions_from_result(
        result,  # type: ignore[arg-type]
    )
    calls: list[tuple[str, tuple[object, ...]]] = []
    batches: list[tuple[str, list[tuple[object, ...]]]] = []

    class FakeConn:
        async def fetchval(self, query, *args):
            assert "reflective_rule_attributions" in query
            return "reflective_rule_attributions"

        async def executemany(self, query, args):
            batches.append((query, list(args)))

        async def execute(self, query, *args):
            calls.append((query, args))

    conn = FakeConn()
    await reflective_learning.persist_reflective_rule_attributions(
        conn,  # type: ignore[arg-type]
        result,  # type: ignore[arg-type]
        trigger,
        attributions,
    )
    await reflective_learning.apply_reflective_rule_credit(
        conn,  # type: ignore[arg-type]
        trigger,
        attributions,
    )

    assert len(batches) == 1
    assert "INSERT INTO reflective_rule_attributions" in batches[0][0]
    assert batches[0][1][0][1] == trigger.tenant_id
    assert len(calls) == 1
    assert "UPDATE reflective_retrieval_rules" in calls[0][0]
    assert calls[0][1][0] == trigger.tenant_id


@pytest.mark.asyncio
async def test_learn_reflective_rules_noops_when_rule_table_missing() -> None:
    class FakeConn:
        async def fetchval(self, query, *args):
            assert "reflective_retrieval_rules" in query
            return None

    await reflective_learning.learn_reflective_rules(
        FakeConn(),  # type: ignore[arg-type]
        _result(),  # type: ignore[arg-type]
        _trigger(),
    )
