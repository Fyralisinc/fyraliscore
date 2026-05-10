"""Verify the CHECK constraint added by 0035_proposition_kind_constraints.sql.

The constraint enforces that proposition_kind (a generated column derived
from proposition->>'kind') is non-NULL and one of the 11 known kinds. A
direct INSERT of a Model with an unknown kind in the proposition JSONB
must be rejected by Postgres."""
from __future__ import annotations

import uuid

import asyncpg
import pytest

from lib.shared.ids import uuid7

pytestmark = pytest.mark.integration


async def _seed_observation(conn: asyncpg.Connection, tenant: uuid.UUID) -> uuid.UUID:
    actor_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status, created_at)
        VALUES ($1, $2, 'human_internal', 'pk-test', 'active', now())
        """,
        actor_id, tenant,
    )
    obs_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, now(), 'signal', 'pk:test',
            $3, '{}'::jsonb, 'pk obs',
            NULL, TRUE, 'authoritative',
            $4, '[]'::jsonb
        )
        """,
        obs_id, tenant, actor_id, f"pk-ext-{obs_id}",
    )
    return obs_id


def _zero_embedding() -> list[float]:
    return [0.0] * 768


async def _insert_model(
    conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    kind: str,
) -> None:
    proposition = f'{{"kind":"{kind}","subject":"x","assertion":"y"}}'
    await conn.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, falsifier, signal_readings,
            supporting_event_ids, supporting_model_ids,
            contributing_models, status,
            confidence_at_assertion
        ) VALUES (
            $1, $2, $3,
            $4::jsonb,
            'pk-test', $5,
            '{}'::uuid[], '[]'::jsonb,
            '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
            0.6, NULL, '[]'::jsonb,
            '{}'::uuid[], '{}'::uuid[],
            '{}'::uuid[], 'active',
            0.6
        )
        """,
        uuid7(), tenant, born_from_event,
        proposition, _zero_embedding(),
    )


@pytest.mark.asyncio
async def test_constraint_accepts_known_kind(tx_conn: asyncpg.Connection):
    tenant = uuid7()
    obs = await _seed_observation(tx_conn, tenant)
    # All 11 known kinds should pass.
    for kind in [
        "state", "relation", "prediction", "pattern", "pattern_instance",
        "capability_assessment", "hypothesis", "concern",
        "market_assessment", "environmental_trend", "recommendation",
    ]:
        await _insert_model(tx_conn, tenant=tenant, born_from_event=obs, kind=kind)


@pytest.mark.asyncio
async def test_constraint_rejects_unknown_kind(tx_conn: asyncpg.Connection):
    tenant = uuid7()
    obs = await _seed_observation(tx_conn, tenant)
    with pytest.raises(asyncpg.CheckViolationError) as exc_info:
        await _insert_model(
            tx_conn, tenant=tenant, born_from_event=obs, kind="bogus_kind"
        )
    assert "models_proposition_kind_valid" in str(exc_info.value)


@pytest.mark.asyncio
async def test_constraint_rejects_missing_kind(tx_conn: asyncpg.Connection):
    tenant = uuid7()
    obs = await _seed_observation(tx_conn, tenant)
    # proposition with no 'kind' key → generated column is NULL → CHECK
    # rejects (NULL fails IN-membership test).
    with pytest.raises(asyncpg.CheckViolationError):
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, falsifier, signal_readings,
                supporting_event_ids, supporting_model_ids,
                contributing_models, status,
                confidence_at_assertion
            ) VALUES (
                $1, $2, $3,
                '{"subject":"x"}'::jsonb,
                'pk-test', $4,
                '{}'::uuid[], '[]'::jsonb,
                '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
                0.6, NULL, '[]'::jsonb,
                '{}'::uuid[], '{}'::uuid[],
                '{}'::uuid[], 'active',
                0.6
            )
            """,
            uuid7(), tenant, obs, _zero_embedding(),
        )
