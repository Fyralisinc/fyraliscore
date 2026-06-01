from __future__ import annotations

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.reasoning.relationships import (
    JudgmentScores,
    RelationshipCandidatesRepo,
    make_edge_candidate,
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
