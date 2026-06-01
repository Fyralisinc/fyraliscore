"""tests/unit/sage/_seed.py — shared seed helpers for SAGE integration tests.

Centralizes the model/observation/edge seed fixtures so the four
Wave-1/Wave-2 test files don't drift on schema details. The hazards
this helper exists to absorb:

  * `models.embedding` is a pgvector column. The gateway_pool fixture
    installs `pgvector.asyncpg.register_vector`, so the right way to
    bind it is a `list[float]`, NOT a stringified `'[0,0,...,0]'` (the
    codec then chokes trying to convert the string to a numpy array).
    Do NOT add `::vector` casts either — let the codec handle it.

  * `models.confidence_at_assertion` is NOT NULL with no default. Every
    seed helper must set it (typically equal to `confidence`).

  * `models.born_from_event_id` is NOT NULL and references
    `observations(id)`. Callers usually want a one-shot helper that
    inserts an observation first and returns its id, but a `born_from`
    override is supported for tests that need to share an observation
    across multiple seeded models.

  * The gateway_pool fixture installs `_test_auto_register_tenant`
    triggers on every tenant_id-bearing table, so manual tenant inserts
    are unnecessary — the first row carrying a tenant_id auto-creates
    the tenants row.

If you need different defaults than these, pass kwargs; don't reach
back into raw SQL inside the test file.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


ZERO_EMBEDDING: list[float] = [0.0] * 768


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


async def seed_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    kind: str = "signal",
    source_channel: str = "test_seed",
    content_text: str = "synthetic observation",
    trust_tier: str = "authoritative",
    occurred_at: datetime | None = None,
    conn: asyncpg.Connection | None = None,
) -> UUID:
    """Insert a minimal observations row, return its id."""
    obs_id = uuid7()
    occurred = occurred_at or datetime.now(tz=timezone.utc)
    sql = """
        INSERT INTO observations (
            id, tenant_id, occurred_at, ingested_at, kind,
            source_channel, source_actor_ref, actor_id,
            content, content_text,
            embedding, embedding_pending,
            trust_tier, external_id, cause_id, entities_mentioned
        ) VALUES (
            $1, $2, $3, $3, $4,
            $5, NULL, NULL,
            $6::jsonb, $7,
            NULL, TRUE,
            $8, $9, NULL, '[]'::jsonb
        )
    """
    args = (
        obs_id, tenant_id, occurred, kind,
        source_channel,
        json.dumps({"content_text": content_text}),
        content_text,
        trust_tier,
        f"test-seed-{obs_id}",
    )
    if conn is not None:
        await conn.execute(sql, *args)
    else:
        await pool.execute(sql, *args)
    return obs_id


async def seed_model(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    born_from_event_id: UUID | None = None,
    proposition: dict | None = None,
    natural: str = "test model",
    confidence: float = 0.5,
    supporting_event_ids: list[UUID] | None = None,
    supporting_model_ids: list[UUID] | None = None,
    signal_readings: list[dict] | None = None,
    falsifier: dict | None = None,
    embedding: list[float] | None = None,
    conn: asyncpg.Connection | None = None,
) -> UUID:
    """Insert a minimal models row, return its id.

    Defaults are chosen so the row passes every NOT NULL + CHECK constraint
    on the models table. Caller can override any field via kwargs.
    """
    model_id = uuid7()
    if born_from_event_id is None:
        born_from_event_id = await seed_observation(
            pool, tenant_id=tenant_id, conn=conn,
        )
    prop = proposition or {"kind": "belief", "subject": natural}
    emb = embedding if embedding is not None else ZERO_EMBEDDING
    scope_temporal = {
        "valid_from": _utc_now_iso(),
        "valid_until": None,
    }
    sql = """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, confidence_at_assertion, activation,
            falsifier, signal_readings,
            supporting_event_ids, supporting_model_ids,
            status
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            '{}'::uuid[], '[]'::jsonb, $7::jsonb,
            $8, $8, 1.0,
            $9::jsonb, $10::jsonb,
            $11::uuid[], $12::uuid[],
            'active'
        )
    """
    args = (
        model_id, tenant_id, born_from_event_id,
        json.dumps(prop), natural, emb,
        json.dumps(scope_temporal),
        float(confidence),
        json.dumps(falsifier) if falsifier is not None else None,
        json.dumps(signal_readings or []),
        supporting_event_ids or [],
        supporting_model_ids or [],
    )
    if conn is not None:
        await conn.execute(sql, *args)
    else:
        await pool.execute(sql, *args)
    return model_id


async def seed_edge(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source_model_id: UUID,
    target_model_id: UUID,
    edge_kind: str = "supports",
    detected_by: str = "test_seed",
    conn: asyncpg.Connection | None = None,
) -> UUID:
    """Insert a minimal model_edges row, return its id."""
    edge_id = uuid7()
    sql = """
        INSERT INTO model_edges (
            id, tenant_id, source_model_id, target_model_id,
            edge_kind, status, detected_by
        ) VALUES ($1, $2, $3, $4, $5, 'active', $6)
    """
    args = (edge_id, tenant_id, source_model_id, target_model_id, edge_kind, detected_by)
    if conn is not None:
        await conn.execute(sql, *args)
    else:
        await pool.execute(sql, *args)
    return edge_id


__all__ = ["ZERO_EMBEDDING", "seed_observation", "seed_model", "seed_edge"]
