"""Integration tests for the hypothesis surface on `/v1/recommendations`.

The repo filter was widened in Phase 3 to include `claim_role='hypothesis'`
so system-imputed intermediate-state Models surface in the same CEO
action list alongside normative recommendations.

Coverage:
  - GET /v1/recommendations includes active hypothesis Models.
  - The hypothesis-specific fields (claim_role, is_system_hypothesis,
    hypothesis_text) are populated correctly.
  - Hypothesis Models with low confidence rank BELOW high-impact
    recommendations even when both surface in the list.
  - Archived hypothesis Models are excluded from the list (mirrors the
    existing recommendation filter).
  - POST /v1/recommendations/{id}/ratify happy path through the gateway.
"""
from __future__ import annotations

import asyncpg
import httpx
import json
import pytest
from uuid import UUID

from lib.shared.ids import uuid7

from .conftest import (
    make_recommendation_proposition,
    seed_commitment,
    seed_observation,
    seed_recommendation_model,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


async def _latest_product_action(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    action: str,
    resource_id: UUID,
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        """
        SELECT action, resource_type, resource_id, metadata
        FROM product_action_audit_log
        WHERE tenant_id = $1
          AND actor_id = $2
          AND action = $3
          AND resource_type = 'recommendation'
          AND resource_id = $4
        ORDER BY occurred_at DESC
        LIMIT 1
        """,
        tenant_id,
        actor_id,
        action,
        resource_id,
    )


async def _seed_hypothesis_model(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    target_actor_id: UUID,
    born_from_event: UUID,
    confidence: float = 0.30,
    hypothesis_text: str = "Hypothesized intermediate state X happened",
    is_system_hypothesis: bool = True,
) -> UUID:
    mid = uuid7()
    embedding = [0.0] * 768
    embedding[0] = 1.0
    proposition = {
        "kind": "belief",
        "legacy_kind": "hypothesis",
        "hypothesis_text": hypothesis_text,
        "is_system_hypothesis": is_system_hypothesis,
        "imputation_source": "missing_transition_detector_v1",
        "bracketed_event_ids": [None, None],
        "differing_fields": ["status"],
    }
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
        hypothesis_text,
        embedding,
        [target_actor_id],
        json.dumps({"valid_from": "2026-04-26T00:00:00Z", "valid_until": None}),
        confidence,
    )
    return mid


# =====================================================================
# Listing: hypothesis Models surface
# =====================================================================


@pytest.mark.asyncio
async def test_list_includes_active_hypothesis_models(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, _ = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    hyp_id = await _seed_hypothesis_model(
        gateway_pool, tenant=tenant_id,
        target_actor_id=seeded_actor, born_from_event=obs_id,
    )

    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    items = resp.json()["items"]
    ids = [i["id"] for i in items]
    assert str(hyp_id) in ids

    hyp_item = next(i for i in items if i["id"] == str(hyp_id))
    assert hyp_item["claim_role"] == "hypothesis"
    assert hyp_item["is_system_hypothesis"] is True
    assert hyp_item["hypothesis_text"].startswith("Hypothesized intermediate")
    # No target_act_ref on hypotheses.
    assert hyp_item.get("target_act_ref") is None


@pytest.mark.asyncio
async def test_list_excludes_archived_hypothesis_models(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, _ = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    hyp_id = await _seed_hypothesis_model(
        gateway_pool, tenant=tenant_id,
        target_actor_id=seeded_actor, born_from_event=obs_id,
    )
    await gateway_pool.execute(
        "UPDATE models SET status='archived', archived_at=now(), "
        "archive_reason='hypothesis_dismissed_by_user' WHERE id = $1",
        hyp_id,
    )
    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )
    ids = [i["id"] for i in resp.json()["items"]]
    assert str(hyp_id) not in ids


@pytest.mark.asyncio
async def test_hypothesis_ranks_below_high_impact_recommendation(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    """The CEO's action list shows high-impact recommendations first.
    Low-confidence hypotheses (rank_score = 0 * conf = 0) sit at the
    bottom — naturally implementing the 'Uncertainty band' UX without
    a special sort path."""
    token, _ = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id,
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id, target_actor_id=seeded_actor,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
            expected_impact=500_000.0,
        ),
        confidence=0.85,
        natural="High-impact recommendation",
    )
    hyp_id = await _seed_hypothesis_model(
        gateway_pool, tenant=tenant_id,
        target_actor_id=seeded_actor, born_from_event=obs_id,
        confidence=0.30,
    )

    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )
    items = resp.json()["items"]
    ids = [i["id"] for i in items]
    assert ids.index(str(rec_id)) < ids.index(str(hyp_id))


@pytest.mark.asyncio
async def test_recommendation_row_has_no_hypothesis_fields(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    """Recommendation rows must NOT spuriously set the hypothesis-
    specific fields. The UI keys off these to pick which action chips
    to render."""
    token, _ = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id,
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id, target_actor_id=seeded_actor,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
        ),
    )
    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )
    rec_item = next(
        i for i in resp.json()["items"] if i["id"] == str(rec_id)
    )
    assert rec_item["claim_role"] == "recommendation"
    assert rec_item["is_system_hypothesis"] is False
    assert rec_item["hypothesis_text"] is None


# =====================================================================
# POST /v1/recommendations/{id}/ratify
# =====================================================================


@pytest.mark.asyncio
async def test_ratify_endpoint_dismiss_happy_path(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, actor_id = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    hyp_id = await _seed_hypothesis_model(
        gateway_pool, tenant=tenant_id,
        target_actor_id=seeded_actor, born_from_event=obs_id,
    )

    resp = await client.post(
        f"/v1/recommendations/{hyp_id}/ratify",
        headers={"Authorization": f"Bearer {token}"},
        json={"action": "dismiss", "explanation": "noise"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "dismiss"
    assert body["archived"] is True
    assert body["trigger_id"] is None

    status = await gateway_pool.fetchval(
        "SELECT status FROM models WHERE id = $1", hyp_id,
    )
    assert status == "archived"
    audit = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="recommendation.ratify",
        resource_id=hyp_id,
    )
    assert audit is not None
    assert audit["metadata"]["ratify_action"] == "dismiss"
    assert audit["metadata"]["archived"] is True
    assert audit["metadata"]["explanation_chars"] == len("noise")
    assert "noise" not in str(audit["metadata"])


@pytest.mark.asyncio
async def test_ratify_endpoint_approve_emits_t2_trigger(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, _ = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    hyp_id = await _seed_hypothesis_model(
        gateway_pool, tenant=tenant_id,
        target_actor_id=seeded_actor, born_from_event=obs_id,
    )

    resp = await client.post(
        f"/v1/recommendations/{hyp_id}/ratify",
        headers={"Authorization": f"Bearer {token}"},
        json={"action": "approve"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "approve"
    assert body["archived"] is False
    assert body["trigger_id"] is not None
    trig_id = UUID(body["trigger_id"])
    row = await gateway_pool.fetchrow(
        "SELECT trigger_kind, trigger_subkind, model_id "
        "FROM think_trigger_queue WHERE id = $1",
        trig_id,
    )
    assert row["trigger_kind"] == "T2"
    assert row["trigger_subkind"] == "hypothesis_approved"
    assert row["model_id"] == hyp_id


@pytest.mark.asyncio
async def test_ratify_endpoint_rejects_invalid_action(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, _ = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    hyp_id = await _seed_hypothesis_model(
        gateway_pool, tenant=tenant_id,
        target_actor_id=seeded_actor, born_from_event=obs_id,
    )
    resp = await client.post(
        f"/v1/recommendations/{hyp_id}/ratify",
        headers={"Authorization": f"Bearer {token}"},
        json={"action": "bogus"},
    )
    assert resp.status_code == 400
    assert resp.json()["error"] == "invalid_action"


@pytest.mark.asyncio
async def test_ratify_endpoint_404_for_unknown_model(
    client: httpx.AsyncClient,
    valid_session,
):
    token, _ = valid_session
    resp = await client.post(
        f"/v1/recommendations/{uuid7()}/ratify",
        headers={"Authorization": f"Bearer {token}"},
        json={"action": "dismiss", "explanation": "x"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_ratify_endpoint_409_for_already_archived(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, _ = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    hyp_id = await _seed_hypothesis_model(
        gateway_pool, tenant=tenant_id,
        target_actor_id=seeded_actor, born_from_event=obs_id,
    )
    await gateway_pool.execute(
        "UPDATE models SET status='archived', archived_at=now(), "
        "archive_reason='manual' WHERE id = $1",
        hyp_id,
    )
    resp = await client.post(
        f"/v1/recommendations/{hyp_id}/ratify",
        headers={"Authorization": f"Bearer {token}"},
        json={"action": "approve"},
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_ratify_endpoint_rejects_invalid_model_id(
    client: httpx.AsyncClient,
    valid_session,
):
    token, _ = valid_session
    resp = await client.post(
        "/v1/recommendations/not-a-uuid/ratify",
        headers={"Authorization": f"Bearer {token}"},
        json={"action": "dismiss"},
    )
    assert resp.status_code == 400
