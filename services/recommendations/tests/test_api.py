"""
services/recommendations/tests/test_api.py — gateway-level tests for
the three recommendation endpoints.

  GET  /v1/recommendations[?actor_id=&limit=]
  POST /v1/recommendations/{id}/act
  POST /v1/recommendations/{id}/dismiss
"""
from __future__ import annotations

import asyncpg
import httpx
import pytest

from lib.shared.ids import uuid7

from .conftest import (
    make_recommendation_proposition,
    seed_commitment,
    seed_observation,
    seed_recommendation_model,
)


pytestmark = pytest.mark.integration


# =====================================================================
# GET /v1/recommendations
# =====================================================================


@pytest.mark.asyncio
async def test_list_returns_empty_for_actor_with_no_recommendations(
    client: httpx.AsyncClient,
    valid_session,
):
    token, _ = valid_session
    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["items"] == []
    assert body["count"] == 0


@pytest.mark.asyncio
async def test_list_ranks_by_impact_times_confidence(
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
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id,
    )

    # Lower-scoring recommendation (impact=100, conf=0.4 → rank 40).
    low = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
            expected_impact=100.0,
        ),
        confidence=0.4,
    )
    # Higher-scoring recommendation (impact=1000, conf=0.6 → rank 600).
    high = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
            expected_impact=1000.0,
        ),
        confidence=0.6,
        natural="Pause the rate limiter, severe impact.",
    )

    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert [i["id"] for i in items] == [str(high), str(low)]
    assert items[0]["rank_score"] > items[1]["rank_score"]


@pytest.mark.asyncio
async def test_list_filters_recommendations_with_archived_target(
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
    # Closed commitment — recommendation about it is moot.
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id, state="closed",
    )
    await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
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
    assert resp.status_code == 200
    assert resp.json()["items"] == []


@pytest.mark.asyncio
async def test_list_respects_limit_param(
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
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id,
    )
    for i in range(3):
        await seed_recommendation_model(
            gateway_pool,
            tenant=tenant_id,
            target_actor_id=seeded_actor,
            born_from_event=obs_id,
            proposition=make_recommendation_proposition(
                target_actor_id=seeded_actor,
                target_type="commitment",
                target_id=cid,
                expected_impact=100.0 * (i + 1),
            ),
        )
    resp = await client.get(
        "/v1/recommendations?limit=2",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    assert len(resp.json()["items"]) == 2


@pytest.mark.asyncio
async def test_list_rejects_cross_actor_access(
    client: httpx.AsyncClient,
    valid_session,
):
    token, _ = valid_session
    other = uuid7()
    resp = await client.get(
        f"/v1/recommendations?actor_id={other}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403


# =====================================================================
# POST /v1/recommendations/{id}/act
# =====================================================================


@pytest.mark.asyncio
async def test_act_transitions_target_commitment_and_archives(
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
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id, state="active",
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
            payload={"new_state": "paused"},
        ),
    )

    resp = await client.post(
        f"/v1/recommendations/{rec_id}/act",
        json={"notes": "queue is fully booked through Q2"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["recommendation_id"] == str(rec_id)
    assert body["target_act_change_kind"] == "transition_commitment"
    assert body["target_act_change_id"] == str(cid)

    # Verify side effects: commitment moved to paused; rec archived.
    state_row = await gateway_pool.fetchrow(
        "SELECT state FROM commitments WHERE id = $1", cid,
    )
    assert state_row["state"] == "paused"
    rec_row = await gateway_pool.fetchrow(
        "SELECT status, archive_reason, caused_act_change_id "
        "FROM models WHERE id = $1",
        rec_id,
    )
    assert rec_row["status"] == "archived"
    assert rec_row["archive_reason"] == "acted_upon"
    assert rec_row["caused_act_change_id"] == cid


@pytest.mark.asyncio
async def test_act_twice_returns_409(
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
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id, state="active",
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
        ),
    )
    first = await client.post(
        f"/v1/recommendations/{rec_id}/act",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first.status_code == 200
    second = await client.post(
        f"/v1/recommendations/{rec_id}/act",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_act_with_unreachable_transition_rolls_back(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    """If the target's state changed since recommendation insert,
    the act handler's underlying commitment transition fails;
    the whole transaction rolls back so the recommendation stays
    active and the commitment retains its current state."""
    token, _ = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id, state="active",
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
            payload={"new_state": "doneverified"},  # active→doneverified illegal
        ),
    )
    resp = await client.post(
        f"/v1/recommendations/{rec_id}/act",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code in (400, 422), resp.text
    rec_row = await gateway_pool.fetchrow(
        "SELECT status, archive_reason FROM models WHERE id = $1",
        rec_id,
    )
    assert rec_row["status"] == "active"
    assert rec_row["archive_reason"] is None
    cm = await gateway_pool.fetchrow(
        "SELECT state FROM commitments WHERE id = $1", cid,
    )
    assert cm["state"] == "active"


# =====================================================================
# POST /v1/recommendations/{id}/dismiss
# =====================================================================


@pytest.mark.asyncio
async def test_dismiss_archives_without_acting(
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
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id, state="active",
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
        ),
    )
    resp = await client.post(
        f"/v1/recommendations/{rec_id}/dismiss",
        json={"reason": "different priority this quarter"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    rec_row = await gateway_pool.fetchrow(
        "SELECT status, archive_reason FROM models WHERE id = $1",
        rec_id,
    )
    assert rec_row["status"] == "archived"
    assert rec_row["archive_reason"] == "dismissed_by_user"
    cm = await gateway_pool.fetchrow(
        "SELECT state FROM commitments WHERE id = $1", cid,
    )
    assert cm["state"] == "active"


@pytest.mark.asyncio
async def test_dismiss_requires_reason(
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
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id,
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
        ),
    )
    resp = await client.post(
        f"/v1/recommendations/{rec_id}/dismiss",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 400
