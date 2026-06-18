from __future__ import annotations

import json

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.reasoning.relationships import (
    JudgmentScores,
    RelationshipCandidatesRepo,
    make_edge_candidate,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _jsonb(value):
    return json.loads(value) if isinstance(value, str) else value


async def test_candidate_acceptance_reinforces_pair_evidence(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=source_model_id,
        target_model_id=target_model_id,
        edge_kind="blocks",
        basis="observed",
        explanation="DPA approval blocks import.",
        scores=JudgmentScores(impact=0.9, confidence=0.8),
    )
    repo = RelationshipCandidatesRepo()

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        await repo.mark_decided(
            conn,
            candidate_id=candidate.id,
            tenant_id=tenant_id,
            review_status="accepted",
            decision_metadata={
                "decision_reason": "accepted_with_justification",
                "reason": "think_promoted_candidate_to_durable_memory",
            },
        )
        pair = await conn.fetchrow(
            """
            SELECT t4_accept_count, positive_outcome_count, edge_kind_votes,
                   direction_votes
            FROM model_pair_evidence
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        relation_count = await conn.fetchval(
            "SELECT COUNT(*) FROM relation_evidence WHERE tenant_id = $1",
            tenant_id,
        )

    assert pair is not None
    assert pair["t4_accept_count"] == 1
    assert pair["positive_outcome_count"] == 1
    assert _jsonb(pair["edge_kind_votes"])["blocks"] == 1
    assert sum(_jsonb(pair["direction_votes"]).values()) == 1
    assert relation_count == 1


async def test_candidate_rejection_records_negative_pair_evidence(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    candidate = make_edge_candidate(
        tenant_id=tenant_id,
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="blocks",
        basis="inferred",
        explanation="Noisy candidate.",
        scores=JudgmentScores(impact=0.5, confidence=0.4),
    )
    repo = RelationshipCandidatesRepo()

    async with fresh_db.acquire() as conn:
        await repo.insert(conn, candidate)
        await repo.mark_decided(
            conn,
            candidate_id=candidate.id,
            tenant_id=tenant_id,
            review_status="rejected",
            decision_metadata={"decision_reason": "rejected_no_match"},
        )
        pair = await conn.fetchrow(
            """
            SELECT t4_reject_count, negative_outcome_count, no_edge_count,
                   edge_kind_votes
            FROM model_pair_evidence
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        relation_count = await conn.fetchval(
            "SELECT COUNT(*) FROM relation_evidence WHERE tenant_id = $1",
            tenant_id,
        )

    assert pair is not None
    assert pair["t4_reject_count"] == 1
    assert pair["negative_outcome_count"] == 1
    assert pair["no_edge_count"] == 1
    assert _jsonb(pair["edge_kind_votes"])["blocks"] == 1
    assert relation_count == 0
