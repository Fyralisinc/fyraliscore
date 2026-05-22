"""Shared fixtures for services/decision_deltas tests.

Re-exports the gateway fixtures (gateway_pool, client, valid_session,
tenant_id, seeded_actor) so the router-level tests get a fully wired
FastAPI app + real Postgres. Also exposes small helpers to seed
decision deltas and source recommendations directly via SQL.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import pytest_asyncio

from lib.shared.ids import uuid7


# Retry transient deadlocks / serialization failures while migrations
# race against other test pools (same hazard documented in
# services/gateway/tests/conftest.py).
_TRANSIENT_ERRORS = (
    "DeadlockDetectedError",
    "SerializationError",
    "InFailedSQLTransactionError",
    "MigrationError",
    "deadlock detected",
)


def pytest_runtest_protocol(item, nextitem):
    from _pytest.runner import runtestprotocol

    max_attempts = 4
    for attempt in range(max_attempts):
        reports = runtestprotocol(item, nextitem=nextitem, log=False)
        failed = any(r.failed for r in reports)
        transient = any(
            r.failed
            and r.longrepr is not None
            and any(err in repr(r.longrepr) for err in _TRANSIENT_ERRORS)
            for r in reports
        )
        if (not failed or not transient) or attempt == max_attempts - 1:
            for r in reports:
                item.ihook.pytest_runtest_logreport(report=r)
            return True
    return True

# Re-export the gateway fixtures so the router tests work the same
# way the recommendation router tests do.
from services.gateway.tests.conftest import (  # noqa: F401
    SLACK_TEST_SECRET,
    _DeterministicEmbedder,
    app_deps,
    client,
    gateway_pool,
    rate_limiter,
    tenant_id,
    tenant_id_b,
)


# The gateway's seeded_actor / seeded_actor_b fixtures insert rows
# into `actors`, which carries a FK to `tenants` (migration 0037). We
# pre-seed the tenants registry here so those fixtures don't trip the
# FK on a fresh DB. The migration 0037 backfill only fires for already-
# present tenant_ids, not the per-test uuid7() ones.
async def _ensure_tenant(pool: asyncpg.Pool, tenant: UUID) -> None:
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) "
        "ON CONFLICT (id) DO NOTHING",
        tenant, f"dd-test-{tenant}",
    )


@pytest_asyncio.fixture
async def seeded_actor(gateway_pool: asyncpg.Pool, tenant_id: UUID) -> UUID:
    await _ensure_tenant(gateway_pool, tenant_id)
    actor_id = uuid7()
    await gateway_pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Alice', 'active')
        """,
        actor_id, tenant_id,
    )
    return actor_id


@pytest_asyncio.fixture
async def seeded_actor_b(
    gateway_pool: asyncpg.Pool, tenant_id_b: UUID,
) -> UUID:
    await _ensure_tenant(gateway_pool, tenant_id_b)
    actor_id = uuid7()
    await gateway_pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', 'Bob', 'active')
        """,
        actor_id, tenant_id_b,
    )
    return actor_id


@pytest_asyncio.fixture
async def valid_session(
    gateway_pool: asyncpg.Pool, seeded_actor: UUID, tenant_id: UUID,
):
    """Return (bearer_token, actor_id) — same shape as the gateway fixture."""
    from services.gateway.auth import create_session
    token, ctx = await create_session(
        gateway_pool, actor_id=seeded_actor, tenant_id=tenant_id,
    )
    return token, ctx.actor_id


@pytest_asyncio.fixture
async def valid_session_b(
    gateway_pool: asyncpg.Pool, seeded_actor_b: UUID, tenant_id_b: UUID,
):
    from services.gateway.auth import create_session
    token, ctx = await create_session(
        gateway_pool, actor_id=seeded_actor_b, tenant_id=tenant_id_b,
    )
    return token, ctx.actor_id


async def seed_decision_delta(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    main_assertion: str = "Salesforce sync failures threaten anchor renewals.",
    status: str = "proposed",
    label: str = "needs_review",
    target_node_kind: str | None = None,
    target_node_id: UUID | None = None,
    confidence: float | None = 0.62,
    category: str | None = "customer_risk",
    impact: dict[str, Any] | None = None,
    consequence_preview: dict[str, Any] | None = None,
    current_state: dict[str, Any] | None = None,
    suggested_update: dict[str, Any] | None = None,
    falsification_condition: str | None = None,
    source_recommendation_id: UUID | None = None,
    evidence: list[dict[str, Any]] | None = None,
) -> UUID:
    await _ensure_tenant(pool, tenant)
    delta_id = uuid7()
    impact_v = (
        impact
        if impact is not None
        else {"arr_at_risk": 720_000, "accounts": 3}
    )
    consequence_v = (
        consequence_preview
        if consequence_preview is not None
        else {
            "creates": [],
            "updates": [
                {
                    "target_kind": target_node_kind,
                    "target_id": (
                        str(target_node_id) if target_node_id else None
                    ),
                }
            ],
            "archives": [],
            "notifies": [],
            "re_evaluates_in": "48h",
        }
    )
    suggested_v = (
        suggested_update
        if suggested_update is not None
        else {"label": "Customer risk", "value": "Critical"}
    )
    current_v = (
        current_state
        if current_state is not None
        else {"label": "Customer risk", "value": "Watch"}
    )

    await pool.execute(
        """
        INSERT INTO decision_deltas (
          id, tenant_id, status, label, main_assertion,
          current_state, suggested_update,
          target_node_kind, target_node_id,
          confidence, confidence_basis,
          falsification_condition, consequence_preview, impact,
          category, source_recommendation_id
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6::jsonb, $7::jsonb,
          $8, $9,
          $10, 'seeded',
          $11, $12::jsonb, $13::jsonb,
          $14, $15
        )
        """,
        delta_id, tenant, status, label, main_assertion,
        json.dumps(current_v), json.dumps(suggested_v),
        target_node_kind, target_node_id,
        confidence,
        falsification_condition, json.dumps(consequence_v),
        json.dumps(impact_v),
        category, source_recommendation_id,
    )

    if evidence:
        for i, ev in enumerate(evidence):
            await pool.execute(
                """
                INSERT INTO decision_delta_evidence (
                  delta_id, source, title, ts, trust_tier,
                  excerpt, weight, ordinal
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                delta_id,
                ev.get("source", "fyralis_reasoning"),
                ev.get("title", "evidence"),
                ev.get("ts", datetime.now(timezone.utc)),
                ev.get("trust_tier"),
                ev.get("excerpt"),
                ev.get("weight"),
                ev.get("ordinal", i),
            )

    return delta_id


async def seed_observation_minimal(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    actor_id: UUID | None = None,
    source_channel: str = "slack:beacon",
    content_text: str = "Beacon reported recurring Salesforce sync failures.",
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
            $1, $2, now(), 'signal', $3,
            $4, '{}'::jsonb, $5,
            NULL, TRUE, 'reputable',
            $6, '[]'::jsonb
        )
        """,
        obs_id, tenant, source_channel, actor_id, content_text,
        f"test-external-{obs_id}",
    )
    return obs_id


async def seed_recommendation_for_promotion(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    target_actor_id: UUID,
    target_commitment_id: UUID,
    supporting_event_ids: list[UUID] | None = None,
    confidence: float = 0.55,
) -> UUID:
    """Insert a kind='recommendation' Model row suitable for the
    promote_from_recommendation path."""
    mid = uuid7()
    embedding = [0.0] * 768
    embedding[0] = 1.0
    born_event = (
        supporting_event_ids[0]
        if supporting_event_ids
        else uuid7()
    )
    proposition = {
        "kind": "recommendation",
        "target_act_ref": {
            "type": "commitment",
            "id": str(target_commitment_id),
        },
        "proposed_change": {
            "operation": "transition",
            "payload": {"new_state": "paused"},
        },
        "expected_impact": 340_000.0,
        "qualitative_impact": "high",
        "target_actor_id": str(target_actor_id),
    }
    # Need a real born_from_event observation row to satisfy the FK
    # on models.born_from_event_id.
    if not supporting_event_ids:
        obs_id = await seed_observation_minimal(
            pool, tenant=tenant, actor_id=target_actor_id,
        )
        supporting_event_ids = [obs_id]
        born_event = obs_id

    await pool.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, activation,
            confidence_at_assertion, activation_coefficient,
            status, supporting_event_ids
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            $7::uuid[], '[]'::jsonb, $8::jsonb,
            $9, 1.0,
            $9, 1.0,
            'active', $10::uuid[]
        )
        """,
        mid, tenant, born_event,
        json.dumps(proposition),
        "Pause the rate limiter commitment until capacity opens up.",
        embedding,
        [target_actor_id],
        json.dumps({"valid_from": "2026-04-26T00:00:00Z", "valid_until": None}),
        confidence,
        supporting_event_ids,
    )
    return mid


async def seed_commitment_for_target(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    owner_id: UUID,
    born_from_event: UUID,
    title: str = "Rate limiter rollout",
    state: str = "active",
) -> UUID:
    cid = uuid7()
    await pool.execute(
        """
        INSERT INTO commitments (
            id, tenant_id, title, description, state, owner_id,
            created_by_event_id
        ) VALUES ($1, $2, $3, NULL, $4, $5, $6)
        """,
        cid, tenant, title, state, owner_id, born_from_event,
    )
    return cid


__all__ = [
    "seed_decision_delta",
    "seed_observation_minimal",
    "seed_recommendation_for_promotion",
    "seed_commitment_for_target",
]
