"""
services/recommendations/tests/conftest.py — shared fixtures.

Reuses the gateway test fixtures (`gateway_pool`, `client`,
`valid_session`, etc.) so the recommendation API tests get a fully
wired FastAPI app with a real Postgres connection. Recommendation
seeding helpers live here.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import pytest_asyncio

from lib.shared.ids import uuid7


# Pull the gateway shared fixtures into this scope. Pytest discovers
# them by re-export.
from services.gateway.tests.conftest import (  # noqa: F401
    SLACK_TEST_SECRET,
    _DeterministicEmbedder,
    app_deps,
    build_slack_payload,
    client,
    gateway_pool,
    rate_limiter,
    seeded_actor,
    seeded_actor_b,
    sign_slack,
    tenant_id,
    tenant_id_b,
    valid_session,
    valid_session_b,
)


async def seed_observation(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    actor_id: UUID | None = None,
) -> UUID:
    obs_id = uuid7()
    await pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, now(), 'signal', 'test:signal',
            $3, '{}'::jsonb, 'seed observation',
            NULL, TRUE, 'authoritative',
            $4, '[]'::jsonb
        )
        """,
        obs_id, tenant, actor_id, f"test-external-{obs_id}",
    )
    return obs_id


async def seed_commitment(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    owner_id: UUID,
    born_from_event: UUID,
    state: str = "active",
    title: str = "Build rate limiter",
) -> UUID:
    cid = uuid7()
    await pool.execute(
        """
        INSERT INTO commitments (
            id, tenant_id, title, description, state, owner_id,
            created_by_event_id
        ) VALUES (
            $1, $2, $3, NULL, $4, $5, $6
        )
        """,
        cid, tenant, title, state, owner_id, born_from_event,
    )
    return cid


async def seed_recommendation_model(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    target_actor_id: UUID,
    born_from_event: UUID,
    proposition: dict[str, Any],
    natural: str = "Pause the rate limiter commitment until capacity opens up.",
    confidence: float = 0.6,
    expected_impact: float | None = 340000.0,
) -> UUID:
    """Insert a recommendation Model directly via SQL for test setup.

    Bypasses the ModelsRepo pipeline so DB-only tests can pre-seed
    rows without invoking calibration / falsifier / embedder code.
    The proposition JSONB is what the GENERATED `target_actor_id`
    extracts from, so the field MUST be present in `proposition`.
    """
    mid = uuid7()
    embedding = [0.0] * 768
    embedding[0] = 1.0
    await pool.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, activation,
            confidence_at_assertion, activation_coefficient,
            status
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            $7::uuid[], '[]'::jsonb, $8::jsonb,
            $9, 1.0,
            $9, 1.0,
            'active'
        )
        """,
        mid, tenant, born_from_event,
        json.dumps(proposition),
        natural,
        embedding,
        [target_actor_id],
        json.dumps({"valid_from": "2026-04-26T00:00:00Z", "valid_until": None}),
        confidence,
    )
    return mid


def make_recommendation_proposition(
    *,
    target_actor_id: UUID,
    target_type: str,
    target_id: UUID,
    operation: str = "transition",
    payload: dict[str, Any] | None = None,
    expected_impact: float | None = 340000.0,
    qualitative_impact: str | None = None,
) -> dict[str, Any]:
    return {
        "kind": "recommendation",
        "target_act_ref": {"type": target_type, "id": str(target_id)},
        "proposed_change": {
            "operation": operation,
            "payload": payload or {"new_state": "paused"},
        },
        "expected_impact": expected_impact,
        "qualitative_impact": qualitative_impact,
        "target_actor_id": str(target_actor_id),
    }


__all__ = [
    "seed_observation",
    "seed_commitment",
    "seed_recommendation_model",
    "make_recommendation_proposition",
]
