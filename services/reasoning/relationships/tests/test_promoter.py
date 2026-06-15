from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import pytest
from pgvector.asyncpg import register_vector

from lib.shared.ids import uuid7
from services.reasoning.relationships import (
    JudgmentScores,
    RelationshipCandidatesRepo,
    make_edge_candidate,
)
from services.reasoning.relationships.promoter import promote_high_confidence_edges


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _insert_observation(conn: asyncpg.Connection, tenant_id: UUID) -> UUID:
    obs_id = uuid7()
    now = datetime.now(timezone.utc)
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, ingested_at, kind, source_channel,
          content, content_text, trust_tier
        )
        VALUES ($1, $2, $3, $3, 'signal', 'test',
                $4::jsonb, $5, 'derived')
        """,
        obs_id,
        tenant_id,
        now,
        json.dumps({"text": "relationship promotion evidence"}),
        "relationship promotion evidence",
    )
    return obs_id


async def _insert_model(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    born_from_event_id: UUID,
    natural: str,
) -> UUID:
    model_id = uuid7()
    await conn.execute(
        """
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural",
          embedding, scope_actors, scope_entities, scope_temporal,
          confidence, falsifier, signal_readings, supporting_event_ids,
          supporting_model_ids, contributing_models, status,
          confidence_at_assertion
        )
        VALUES (
          $1, $2, $3,
          '{"kind":"state","subject":"promotion","assertion":"true"}'::jsonb,
          $4, $5,
          '{}'::uuid[], '[]'::jsonb, '{}'::jsonb,
          0.82, NULL, '[]'::jsonb, ARRAY[$3]::uuid[],
          '{}'::uuid[], '{}'::uuid[], 'active',
          0.82
        )
        """,
        model_id,
        tenant_id,
        born_from_event_id,
        natural,
        [1.0, *([0.0] * 767)],
    )
    return model_id


async def test_promoter_preserves_null_weight_for_resolution_edges(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    repo = RelationshipCandidatesRepo()

    async with fresh_db.acquire() as conn:
        await register_vector(conn)
        obs_id = await _insert_observation(conn, tenant_id)
        resolver = await _insert_model(
            conn,
            tenant_id=tenant_id,
            born_from_event_id=obs_id,
            natural="Customer signed the renewal amendment.",
        )
        target = await _insert_model(
            conn,
            tenant_id=tenant_id,
            born_from_event_id=obs_id,
            natural="Renewal risk needs resolution.",
        )
        candidate = make_edge_candidate(
            tenant_id=tenant_id,
            source_model_id=resolver,
            target_model_id=target,
            edge_kind="contributes_to_resolution",
            basis="observed",
            explanation="The amendment directly helps resolve the renewal risk.",
            scores=JudgmentScores(
                impact=0.95,
                actionability=0.95,
                urgency=0.90,
                authority_required=0.90,
                novelty=0.70,
                reversibility=0.50,
                confidence=0.91,
                uncertainty=0.20,
            ),
            evidence_event_ids=(obs_id,),
            source="test",
        )
        await repo.insert(conn, candidate)

        report = await promote_high_confidence_edges(
            conn,
            tenant_id=tenant_id,
            min_confidence=0.80,
            min_leverage=0.60,
            max_uncertainty=0.50,
        )
        edge = await conn.fetchrow(
            """
            SELECT edge_kind, weight, review_status
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = $2
              AND target_model_id = $3
              AND edge_kind = 'contributes_to_resolution'
            """,
            tenant_id,
            resolver,
            target,
        )
        decided = await repo.get(conn, candidate_id=candidate.id, tenant_id=tenant_id)

    assert report.promoted_candidates == 1
    assert report.failed_candidates == 0
    assert edge is not None
    assert edge["weight"] is None
    assert edge["review_status"] == "accepted"
    assert decided is not None
    assert decided["review_status"] == "accepted"
