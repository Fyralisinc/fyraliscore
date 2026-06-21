from __future__ import annotations

import json

import asyncpg
import pytest
from pgvector.asyncpg import register_vector

from lib.shared.ids import uuid7
from services.reasoning.edge_intelligence import promote_pair_evidence_candidates
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.diff_schema import EdgeOp, ValidatedDiff
from services.reasoning.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _insert_observation(
    conn: asyncpg.Connection,
    tenant_id,
    content_text: str,
):
    obs_id = uuid7()
    await register_vector(conn)
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, ingested_at, kind, source_channel,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), now(), 'signal', 'test',
                $3::jsonb, $4, $5, FALSE, 'authoritative')
        """,
        obs_id,
        tenant_id,
        json.dumps({"text": content_text}),
        content_text,
        make_embedding(content_text),
    )
    return obs_id


async def _insert_model(
    conn: asyncpg.Connection,
    tenant_id,
    observation_id,
    *,
    natural: str,
    scope_entities: list[dict],
):
    model_id = uuid7()
    await register_vector(conn)
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], $7::jsonb,
                '{}'::jsonb, 0.74, 1.0, 'active', 0.74, 1.0)
        """,
        model_id,
        tenant_id,
        observation_id,
        json.dumps(
            {
                "kind": "belief",
                "claim_role": "fact",
                "abstraction_level": "atomic",
                "assertion": natural,
            }
        ),
        natural,
        make_embedding(natural),
        json.dumps(scope_entities),
    )
    return model_id


def _jsonb(value):
    return json.loads(value) if isinstance(value, str) else value


async def test_valid_diff_records_edge_intelligence_without_sage_trace_and_skips_existing_edge(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    customer_id = uuid7()
    scope = [{"type": "customer", "id": str(customer_id)}]
    signal_text = "DPA approval blocks HubSpot import."

    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, 'edge intelligence e2e', FALSE)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
        )
        observation_id = await _insert_observation(conn, tenant_id, signal_text)
        source_model_id = await _insert_model(
            conn,
            tenant_id,
            observation_id,
            natural="DPA approval is pending.",
            scope_entities=scope,
        )
        target_model_id = await _insert_model(
            conn,
            tenant_id,
            observation_id,
            natural="HubSpot import is blocked.",
            scope_entities=scope,
        )
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant_id,
            edge_ops=[
                EdgeOp(
                    op="add",
                    source_model_id=source_model_id,
                    target_model_id=target_model_id,
                    edge_kind="blocks",
                    confidence=0.86,
                    evidence_event_ids=[observation_id],
                    evidence_model_ids=[source_model_id, target_model_id],
                    explanation="DPA approval blocks HubSpot import.",
                    detected_by="think_edge_op",
                )
            ],
        )

        async with conn.transaction():
            await apply_diff(
                diff,
                conn,
                trigger_kind="T1",
                trigger_cause_event_id=observation_id,
            )

        relation_rows = await conn.fetch(
            """
            SELECT predicate, extraction_method, source_model_id, target_model_id
            FROM relation_evidence
            WHERE tenant_id = $1
            ORDER BY extraction_method
            """,
            tenant_id,
        )
        pair = await conn.fetchrow(
            """
            SELECT explicit_relation_count, think_edge_op_count,
                   co_used_valid_diff_count, positive_outcome_count,
                   edge_kind_votes, direction_votes
            FROM model_pair_evidence
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        promotion = await promote_pair_evidence_candidates(conn, tenant_id=tenant_id)
        candidate_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM relationship_candidates
            WHERE tenant_id = $1
              AND source = 'edge_intelligence_kernel'
            """,
            tenant_id,
        )

    methods = {row["extraction_method"] for row in relation_rows}
    assert methods == {"deterministic_signal_relation", "think_edge_op"}
    assert any(
        row["source_model_id"] == source_model_id
        and row["target_model_id"] == target_model_id
        for row in relation_rows
    )
    assert pair is not None
    assert pair["explicit_relation_count"] == 1
    assert pair["think_edge_op_count"] == 1
    assert pair["co_used_valid_diff_count"] == 1
    assert pair["positive_outcome_count"] == 1
    assert _jsonb(pair["edge_kind_votes"])["blocks"] == 1
    assert sum(_jsonb(pair["direction_votes"]).values()) == 1
    assert promotion.scanned_pair_evidence == 1
    assert promotion.candidates_inserted == 0
    assert promotion.candidates_skipped == 1
    assert candidate_count == 0
