from __future__ import annotations

from datetime import datetime, timezone

from lib.shared.ids import uuid7
from services.reasoning.relationships.ontology_proposals import (
    aggregate_edge_type_candidates,
    normalize_proposed_edge_kind,
)


def _candidate(
    *,
    tenant_id,
    candidate_id,
    proposed_edge_kind: str,
    dropped_dimensions: list[str],
    score: float,
    fallback: str = "blocks",
):
    return {
        "id": candidate_id,
        "tenant_id": tenant_id,
        "candidate_kind": "edge_type",
        "review_status": "needs_review",
        "proposed_proposition": {
            "kind": "ontology_gap",
            "proposed_edge_kind": proposed_edge_kind,
            "description": "Progress depends on an explicit decision gate.",
            "relationship_summary": (
                "A model cannot progress until a specific decision is made."
            ),
            "parent_kind": fallback,
            "nearest_existing_kind": fallback,
            "directionality": "directed",
            "dropped_dimensions": dropped_dimensions,
            "promotion_criteria": {
                "minimum_distinct_examples": 3,
                "requires_human_or_llm_adjudication": True,
            },
        },
        "metadata": {
            "ontology_gap": {
                "retrieval_fallback_kind": fallback,
            }
        },
        "evidence_model_ids": [uuid7()],
        "evidence_event_ids": [uuid7()],
        "judgment_leverage_score": score,
        "confidence_score": 0.6,
        "created_at": datetime.now(timezone.utc),
    }


def test_normalize_proposed_edge_kind_cleans_input() -> None:
    assert normalize_proposed_edge_kind(" Gated By Decision ") == "gated_by_decision"
    assert normalize_proposed_edge_kind("x") is None


def test_aggregate_edge_type_candidates_promotes_repeated_gap_to_review_ready() -> None:
    tenant_id = uuid7()
    candidates = [
        _candidate(
            tenant_id=tenant_id,
            candidate_id=uuid7(),
            proposed_edge_kind="gated_by_decision",
            dropped_dimensions=["authority surface"],
            score=0.7,
        ),
        _candidate(
            tenant_id=tenant_id,
            candidate_id=uuid7(),
            proposed_edge_kind="Gated By Decision",
            dropped_dimensions=["decision dependency"],
            score=0.9,
        ),
        _candidate(
            tenant_id=tenant_id,
            candidate_id=uuid7(),
            proposed_edge_kind="gated_by_decision",
            dropped_dimensions=["approval state"],
            score=0.8,
        ),
    ]

    proposals = aggregate_edge_type_candidates(candidates)

    assert len(proposals) == 1
    proposal = proposals[0]
    assert proposal.tenant_id == tenant_id
    assert proposal.proposed_edge_kind == "gated_by_decision"
    assert proposal.status == "review_ready"
    assert proposal.example_count == 3
    assert proposal.retrieval_fallback_kind == "blocks"
    assert proposal.directionality == "directed"
    assert proposal.max_judgment_leverage_score == 0.9
    assert proposal.dropped_dimensions == (
        "authority surface",
        "decision dependency",
        "approval state",
    )


def test_aggregate_edge_type_candidates_keeps_sparse_gap_as_draft() -> None:
    tenant_id = uuid7()

    proposals = aggregate_edge_type_candidates([
        _candidate(
            tenant_id=tenant_id,
            candidate_id=uuid7(),
            proposed_edge_kind="depends_on_assumption",
            dropped_dimensions=["conditional truth"],
            score=0.75,
            fallback="supports",
        )
    ])

    assert len(proposals) == 1
    assert proposals[0].status == "draft"
    assert proposals[0].proposed_edge_kind == "depends_on_assumption"

