from __future__ import annotations

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.reasoning.relationships import (
    JudgmentScores,
    RelationshipCandidatesRepo,
    RelationshipOntologyProposal,
    RelationshipOntologyProposalsRepo,
    make_edge_candidate,
    make_edge_type_candidate,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_relationship_candidates_repo_round_trips(fresh_db: asyncpg.Pool) -> None:
    tenant_id = uuid7()
    edge_id = uuid7()
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="blocks",
        basis="inferred",
        explanation="One model indicates work is blocked by the other.",
        scores=JudgmentScores(
            impact=0.8,
            urgency=0.7,
            actionability=0.6,
            uncertainty=0.5,
            authority_required=0.6,
            confidence=0.7,
        ),
    )
    repo = RelationshipCandidatesRepo()

    async with fresh_db.acquire() as conn:
        inserted = await repo.insert(conn, candidate)
        listed = await repo.list_for_review(conn, tenant_id=tenant_id)
        decided = await repo.mark_decided(
            conn,
            candidate_id=candidate.id,
            tenant_id=tenant_id,
            review_status="accepted",
            accepted_edge_ids=[edge_id],
        )
        after_decision = await repo.list_for_review(conn, tenant_id=tenant_id)

    assert inserted["id"] == candidate.id
    assert inserted["edge_kind"] == "blocks"
    assert listed and listed[0]["id"] == candidate.id
    assert decided is not None
    assert decided["review_status"] == "accepted"
    assert decided["accepted_edge_ids"] == [edge_id]
    assert after_decision == []


async def test_relationship_candidate_metrics_tracks_lifecycle(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    repo = RelationshipCandidatesRepo()
    accepted = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="blocks",
        basis="topology_suggested",
        explanation="Candidate accepted.",
        scores=JudgmentScores(impact=0.9, confidence=0.8),
        source="latent_topology",
    )
    rejected = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="same_issue_as",
        basis="topology_suggested",
        explanation="Candidate rejected.",
        scores=JudgmentScores(impact=0.4, confidence=0.5),
        source="latent_topology",
    )
    open_candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="early_warning_for",
        basis="topology_suggested",
        explanation="Candidate still open.",
        scores=JudgmentScores(impact=0.6, confidence=0.6),
        source="latent_topology",
    )

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, accepted)
        await repo.insert(conn, rejected)
        await repo.insert(conn, open_candidate)
        await repo.mark_decided(
            conn,
            candidate_id=accepted.id,
            tenant_id=tenant_id,
            review_status="accepted",
        )
        await repo.mark_decided(
            conn,
            candidate_id=rejected.id,
            tenant_id=tenant_id,
            review_status="rejected",
        )
        metrics = await repo.metrics(conn, tenant_id=tenant_id)

    assert metrics.total == 3
    assert metrics.accepted == 1
    assert metrics.rejected == 1
    assert metrics.open_count == 1
    assert metrics.acceptance_rate == 0.5
    assert metrics.by_source == {"latent_topology": 3}


async def test_relationship_candidates_repo_round_trips_edge_type_gap(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()
    candidate = make_edge_type_candidate(
        tenant_id=tenant_id,
        proposed_edge_kind="trades_off_with",
        description="Improving one model predictably worsens another.",
        relationship_summary=(
            "The two models are both true but compete along an optimization frontier."
        ),
        parent_kind="contradicts",
        nearest_existing_kind="contradicts",
        directionality="symmetric",
        dropped_dimensions=("both can be true", "choice cost"),
        example_source_model_id=source_model_id,
        example_target_model_id=target_model_id,
        scores=JudgmentScores(
            impact=0.9,
            uncertainty=0.8,
            urgency=0.6,
            actionability=0.7,
            novelty=0.9,
            confidence=0.6,
        ),
    )
    repo = RelationshipCandidatesRepo()

    async with fresh_db.acquire() as conn:
        inserted = await repo.insert(conn, candidate)
        listed = await repo.list_for_review(conn, tenant_id=tenant_id)
        metrics = await repo.metrics(conn, tenant_id=tenant_id)

    assert inserted["candidate_kind"] == "edge_type"
    assert inserted["basis"] == "ontology_gap"
    assert inserted["edge_kind"] is None
    assert inserted["source_model_id"] is None
    assert inserted["target_model_id"] is None
    assert inserted["member_model_ids"] == [source_model_id, target_model_id]
    assert inserted["proposed_proposition"]["proposed_edge_kind"] == "trades_off_with"
    assert listed and listed[0]["id"] == candidate.id
    assert metrics.by_kind == {"edge_type": 1}


async def test_relationship_ontology_proposal_review_accepts_dynamic_kind(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    repo = RelationshipOntologyProposalsRepo()
    proposal = RelationshipOntologyProposal(
        tenant_id=tenant_id,
        proposed_edge_kind="gated_by_decision",
        status="review_ready",
        description="Progress depends on an explicit approval decision.",
        relationship_summary=(
            "The target cannot progress until the source decision is made."
        ),
        nearest_existing_kind="blocks",
        retrieval_fallback_kind="blocks",
        directionality="directed",
        example_count=3,
        promotion_criteria={"minimum_distinct_examples": 3},
    )

    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, 'relationship ontology proposal test', false)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
        )
        inserted = await repo.upsert(conn, proposal)
        reviewed = await repo.review(
            conn,
            tenant_id=tenant_id,
            proposal_id=inserted["id"],
            status="accepted",
            reviewed_by="test",
            note="Repeated decision-gate evidence.",
        )
        accepted = await repo.get_accepted(
            conn,
            tenant_id=tenant_id,
            proposed_edge_kind="gated_by_decision",
        )

    assert reviewed is not None
    assert reviewed["status"] == "accepted"
    assert reviewed["promoted_at"] is not None
    assert reviewed["metadata"]["last_review"]["reviewed_by"] == "test"
    assert accepted is not None
    assert accepted["proposed_edge_kind"] == "gated_by_decision"
