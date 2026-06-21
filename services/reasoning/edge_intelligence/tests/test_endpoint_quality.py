from __future__ import annotations

import json

import asyncpg
import pytest
from pgvector.asyncpg import register_vector

from lib.shared.ids import uuid7
from services.reasoning.edge_intelligence.endpoint_quality import endpoint_quality_gate
from services.reasoning.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _insert_model(
    conn: asyncpg.Connection,
    tenant_id,
    *,
    natural: str,
    proposition_kind: str = "state",
    scope_entities: list[dict] | None = None,
    status: str = "active",
):
    model_id = uuid7()
    observation_id = uuid7()
    await register_vector(conn)
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, ingested_at, kind, source_channel,
          content, content_text, trust_tier
        )
        VALUES ($1, $2, now(), now(), 'signal', 'test',
                $3::jsonb, $4, 'derived')
        """,
        observation_id,
        tenant_id,
        json.dumps({"text": natural}),
        natural,
    )
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], $7::jsonb,
                '{}'::jsonb, 0.72, 1.0, $8, 0.72, 1.0)
        """,
        model_id,
        tenant_id,
        observation_id,
        json.dumps(
            {
                "kind": "belief",
                "claim_role": "situation" if proposition_kind == "situation" else "fact",
                "subject": natural,
                "assertion": "true",
                **(
                    {
                        "pressure_type": "execution",
                        "shared_mechanism": natural,
                    }
                    if proposition_kind == "situation"
                    else {}
                ),
            }
        ),
        natural,
        make_embedding(natural),
        json.dumps(scope_entities or []),
        status,
    )
    return model_id


async def test_endpoint_quality_allows_shared_concrete_scope(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    customer_id = uuid7()
    scope = [{"type": "customer", "id": str(customer_id)}]
    async with fresh_db.acquire() as conn:
        left = await _insert_model(conn, tenant_id, natural="DPA pending", scope_entities=scope)
        right = await _insert_model(conn, tenant_id, natural="Import blocked", scope_entities=scope)
        decision = await endpoint_quality_gate(
            conn,
            tenant_id=tenant_id,
            source_model_id=left,
            target_model_id=right,
            edge_kind="blocks",
        )

    assert decision.allowed is True


async def test_endpoint_quality_rejects_broad_scope_only(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with fresh_db.acquire() as conn:
        left = await _insert_model(
            conn,
            tenant_id,
            natural="Launch risk",
            scope_entities=[{"type": "goal", "id": str(uuid7())}],
        )
        right = await _insert_model(
            conn,
            tenant_id,
            natural="Import risk",
            scope_entities=[{"type": "goal", "id": str(uuid7())}],
        )
        decision = await endpoint_quality_gate(
            conn,
            tenant_id=tenant_id,
            source_model_id=left,
            target_model_id=right,
            edge_kind="blocks",
        )

    assert decision.allowed is False
    assert "missing_shared_concrete_scope" in decision.reasons


async def test_endpoint_quality_rejects_composite_endpoint(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    customer_id = uuid7()
    scope = [{"type": "customer", "id": str(customer_id)}]
    async with fresh_db.acquire() as conn:
        left = await _insert_model(
            conn,
            tenant_id,
            natural="Composite situation",
            proposition_kind="situation",
            scope_entities=scope,
        )
        right = await _insert_model(conn, tenant_id, natural="Import blocked", scope_entities=scope)
        decision = await endpoint_quality_gate(
            conn,
            tenant_id=tenant_id,
            source_model_id=left,
            target_model_id=right,
            edge_kind="blocks",
        )

    assert decision.allowed is False
    assert "composite_endpoint" in decision.reasons
