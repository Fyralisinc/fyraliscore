from __future__ import annotations

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.relationships import (
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
