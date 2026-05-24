from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from services.relationships.candidates import (
    JudgmentScores,
    ModelSignal,
    generate_scope_overlap_candidates,
    make_edge_candidate,
    make_situation_candidate,
    rank_candidates,
)


def test_situation_candidate_carries_first_class_proposition() -> None:
    tenant_id = uuid7()
    m1 = uuid7()
    m2 = uuid7()

    candidate = make_situation_candidate(
        tenant_id=tenant_id,
        situation="Beacon renewal risk is becoming cross-functional",
        summary="Delivery delay and pricing pressure are compounding.",
        relationship_summary="The delay increases the impact of pricing concern.",
        member_model_ids=(m1, m2, m1),
        basis="inferred",
        scores=JudgmentScores(
            impact=0.9,
            uncertainty=0.6,
            urgency=0.7,
            authority_required=0.8,
            actionability=0.6,
            novelty=0.5,
            confidence=0.7,
        ),
    )

    assert candidate.candidate_kind == "situation"
    assert candidate.member_model_ids == (m1, m2)
    assert candidate.proposed_proposition is not None
    assert candidate.proposed_proposition["kind"] == "situation"
    assert candidate.proposed_proposition["member_model_ids"] == [
        str(m1),
        str(m2),
    ]
    assert candidate.judgment_leverage_score > 0.6


def test_edge_candidate_rejects_self_edge() -> None:
    model_id = uuid7()

    with pytest.raises(ValueError):
        make_edge_candidate(
            tenant_id=uuid7(),
            source_model_id=model_id,
            target_model_id=model_id,
            edge_kind="supports",
            basis="inferred",
            explanation="bad self-edge",
            scores=JudgmentScores(),
        )


def test_causal_candidate_requires_mechanism() -> None:
    with pytest.raises(ValueError):
        make_edge_candidate(
            tenant_id=uuid7(),
            source_model_id=uuid7(),
            target_model_id=uuid7(),
            edge_kind="blocks",
            basis="causal_hypothesis",
            explanation="causal but underspecified",
            scores=JudgmentScores(),
        )


def test_generate_scope_overlap_candidates_prioritizes_warning_edges() -> None:
    tenant_id = uuid7()
    customer_id = uuid7()
    concern = ModelSignal(
        id=uuid7(),
        natural="Beacon renewal risk is rising because pricing is unresolved.",
        proposition_kind="concern",
        confidence=0.72,
        activation=0.9,
        scope_entities=(("customer", customer_id),),
    )
    prediction = ModelSignal(
        id=uuid7(),
        natural="Beacon churn prediction depends on May procurement approval.",
        proposition_kind="prediction",
        confidence=0.64,
        activation=0.8,
        scope_entities=(("customer", customer_id),),
    )
    unrelated = ModelSignal(
        id=uuid7(),
        natural="Internal docs cleanup is progressing.",
        proposition_kind="state",
        confidence=0.8,
        activation=0.2,
        scope_entities=(("resource", uuid7()),),
    )

    candidates = generate_scope_overlap_candidates(
        tenant_id=tenant_id,
        models=[concern, prediction, unrelated],
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.edge_kind == "early_warning_for"
    assert candidate.source_model_id == concern.id
    assert candidate.target_model_id == prediction.id
    assert candidate.metadata["scope"] == {
        "type": "customer",
        "id": str(customer_id),
    }


def test_scope_overlap_blocker_includes_causal_metadata() -> None:
    tenant_id = uuid7()
    commitment_id = uuid7()
    blocker = ModelSignal(
        id=uuid7(),
        natural="The launch is blocked waiting on security approval.",
        proposition_kind="concern",
        confidence=0.8,
        activation=0.9,
        scope_entities=(("commitment", commitment_id),),
    )
    target = ModelSignal(
        id=uuid7(),
        natural="The launch is expected this week.",
        proposition_kind="prediction",
        confidence=0.7,
        activation=0.8,
        scope_entities=(("commitment", commitment_id),),
    )

    candidate = generate_scope_overlap_candidates(
        tenant_id=tenant_id,
        models=[blocker, target],
    )[0]

    assert candidate.edge_kind == "blocks"
    assert candidate.basis == "causal_hypothesis"
    assert candidate.metadata["causal"]["mechanism_summary"]


def test_rank_candidates_orders_by_judgment_leverage() -> None:
    tenant_id = uuid7()
    low = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="co_occurs_with",
        basis="inferred",
        explanation="low leverage",
        scores=JudgmentScores(impact=0.2, confidence=0.9),
    )
    high = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="blocks",
        basis="inferred",
        explanation="high leverage",
        scores=JudgmentScores(
            impact=0.9,
            urgency=0.8,
            actionability=0.8,
            authority_required=0.7,
            uncertainty=0.6,
            confidence=0.6,
        ),
    )

    assert rank_candidates([low, high]) == [high, low]
