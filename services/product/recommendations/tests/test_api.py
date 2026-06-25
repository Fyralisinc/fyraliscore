"""
services/product/recommendations/tests/test_api.py — gateway-level tests for
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
    grant_actor_role,
    make_recommendation_proposition,
    seed_commitment,
    seed_observation,
    seed_recommendation_model,
    seed_resource,
)


pytestmark = pytest.mark.integration


async def _latest_product_action(
    pool: asyncpg.Pool,
    *,
    tenant_id,
    actor_id,
    action: str,
    resource_id,
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


@pytest.mark.asyncio
async def test_list_filters_recommendations_with_hidden_target_resource(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=actor_id,
    )
    resource_id = await seed_resource(gateway_pool, tenant=tenant_id)
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=actor_id,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=actor_id,
            target_type="resource",
            target_id=resource_id,
        ),
    )

    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200, resp.text
    ids = {item["id"] for item in resp.json()["items"]}
    assert str(rec_id) not in ids


@pytest.mark.asyncio
async def test_list_allows_recommendation_with_resource_viewer_role(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=actor_id,
    )
    resource_id = await seed_resource(gateway_pool, tenant=tenant_id)
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=actor_id,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=actor_id,
            target_type="resource",
            target_id=resource_id,
        ),
    )
    await grant_actor_role(
        gateway_pool,
        tenant=tenant_id,
        actor_id=actor_id,
        entity_type="resource",
        entity_id=resource_id,
        role="viewer",
    )

    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200, resp.text
    items = resp.json()["items"]
    assert str(rec_id) in {item["id"] for item in items}
    target = next(item["target_entity"] for item in items if item["id"] == str(rec_id))
    assert target["id"] == str(resource_id)


@pytest.mark.asyncio
async def test_dismiss_hidden_target_recommendation_is_forbidden(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=actor_id,
    )
    resource_id = await seed_resource(gateway_pool, tenant=tenant_id)
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=actor_id,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=actor_id,
            target_type="resource",
            target_id=resource_id,
        ),
    )

    resp = await client.post(
        f"/v1/recommendations/{rec_id}/dismiss",
        json={"reason": "not now"},
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 403
    assert resp.json()["reason"] == "resource_out_of_scope:financial"
    row = await gateway_pool.fetchrow(
        "SELECT status, archive_reason FROM models WHERE id = $1",
        rec_id,
    )
    assert row["status"] == "active"
    assert row["archive_reason"] is None


@pytest.mark.asyncio
async def test_list_admin_override_for_target_resource_is_audited(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=actor_id,
    )
    resource_id = await seed_resource(gateway_pool, tenant=tenant_id)
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=actor_id,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=actor_id,
            target_type="resource",
            target_id=resource_id,
        ),
    )
    await grant_actor_role(
        gateway_pool,
        tenant=tenant_id,
        actor_id=actor_id,
        entity_type="tenant",
        entity_id=None,
        role="admin",
    )

    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200, resp.text
    assert str(rec_id) in {item["id"] for item in resp.json()["items"]}
    audit_row = await gateway_pool.fetchrow(
        """
        SELECT override_kind, reason
        FROM access_override_log
        WHERE tenant_id = $1
          AND actor_id = $2
          AND entity_type = 'resource'
          AND entity_id = $3
        ORDER BY occurred_at DESC
        LIMIT 1
        """,
        tenant_id,
        actor_id,
        resource_id,
    )
    assert audit_row is not None
    assert audit_row["override_kind"] == "admin"
    assert audit_row["reason"] == "admin_override"


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
    token, actor_id = valid_session
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
    audit = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="recommendation.act",
        resource_id=rec_id,
    )
    assert audit is not None
    assert audit["metadata"]["notes_chars"] == len("queue is fully booked through Q2")
    assert audit["metadata"]["target_act_change_kind"] == "transition_commitment"
    assert audit["metadata"]["target_act_change_id"] == str(cid)
    assert "queue is fully booked" not in str(audit["metadata"])


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
    token, actor_id = valid_session
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
    audit = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="recommendation.dismiss",
        resource_id=rec_id,
    )
    assert audit is not None
    assert audit["metadata"]["reason_chars"] == len("different priority this quarter")
    assert "different priority" not in str(audit["metadata"])


@pytest.mark.asyncio
async def test_watch_and_unwatch_are_product_action_audited(
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

    predicate = "tell me if the owner moves this back to active"
    watch = await client.post(
        f"/v1/recommendations/{rec_id}/watch",
        json={"predicate": predicate},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert watch.status_code == 200, watch.text
    watch_audit = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="recommendation.watch",
        resource_id=rec_id,
    )
    assert watch_audit is not None
    assert watch_audit["metadata"]["watch_id"] == watch.json()["watch_id"]
    assert watch_audit["metadata"]["predicate_chars"] == len(predicate)
    assert "tell me if" not in str(watch_audit["metadata"])

    unwatch = await client.delete(
        f"/v1/recommendations/{rec_id}/watch",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert unwatch.status_code == 200, unwatch.text
    unwatch_audit = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="recommendation.unwatch",
        resource_id=rec_id,
    )
    assert unwatch_audit is not None


@pytest.mark.asyncio
async def test_triage_route_is_product_action_audited_without_raw_reason(
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

    reason = "route this to the CSM owner"
    routed_to = "customer-success"
    resp = await client.post(
        f"/v1/recommendations/{rec_id}/triage",
        json={"action": "route", "reason": reason, "routed_to": routed_to},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    audit = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="recommendation.triage",
        resource_id=rec_id,
    )
    assert audit is not None
    assert audit["metadata"]["triage_action"] == "route"
    assert audit["metadata"]["reason_chars"] == len(reason)
    assert audit["metadata"]["routed_to_chars"] == len(routed_to)
    assert reason not in str(audit["metadata"])
    assert routed_to not in str(audit["metadata"])


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
