"""
services/product/decision_deltas/tests/test_router.py — request-level tests.

The router is NOT yet registered in services/app/gateway/main.py (that
file is in this agent's forbidden zone for Phase 1). To exercise the
HTTP surface we build the gateway app with the same fixtures the
recommendation tests use and call include_router on the decision-delta
router for the duration of the test.

Once the gateway owner adds the registration line, this preamble can
be removed; the test bodies will keep working unchanged.
"""
from __future__ import annotations

from typing import AsyncGenerator
from uuid import UUID

import asyncpg
import httpx
import pytest
import pytest_asyncio

from services.product.decision_deltas.router import build_router
from services.app.gateway.main import build_app

from .conftest import (
    grant_actor_role,
    seed_commitment_for_target,
    seed_decision_delta,
    seed_observation_minimal,
    seed_recommendation_for_promotion,
    seed_resource_for_target,
)


pytestmark = pytest.mark.integration


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
          AND resource_type = 'decision_delta'
          AND resource_id = $4
        ORDER BY occurred_at DESC
        LIMIT 1
        """,
        tenant_id,
        actor_id,
        action,
        resource_id,
    )


@pytest_asyncio.fixture
async def dd_client(app_deps) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Gateway app + decision_deltas router mounted."""
    app = build_app(
        pool=app_deps.pool,
        actor_repo=app_deps.actor_repo,
        alias_repo=app_deps.alias_repo,
        embedder=app_deps.embedder,
        rate_limiter=app_deps.rate_limiter,
        configure_logging=False,
    )
    app.include_router(build_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test",
    ) as c:
        yield c


# =====================================================================
# GET /v1/decision_deltas/
# =====================================================================


@pytest.mark.asyncio
async def test_list_empty_for_new_tenant(
    dd_client: httpx.AsyncClient, valid_session,
):
    token, _ = valid_session
    resp = await dd_client.get(
        "/v1/decision_deltas/",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {"items": [], "count": 0}


@pytest.mark.asyncio
async def test_list_returns_seeded_deltas(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, _ = valid_session
    a = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
        category="customer_risk",
    )
    b = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
        category="capacity",
    )
    resp = await dd_client.get(
        "/v1/decision_deltas/?status=proposed",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    items = resp.json()["items"]
    ids = {item["id"] for item in items}
    assert {str(a), str(b)} <= ids
    assert all(i["status"] == "proposed" for i in items)


@pytest.mark.asyncio
async def test_list_isolates_by_tenant(
    dd_client: httpx.AsyncClient,
    valid_session,
    valid_session_b,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    tenant_id_b,
):
    await seed_decision_delta(
        gateway_pool, tenant=tenant_id,
        main_assertion="A tenant delta",
    )
    await seed_decision_delta(
        gateway_pool, tenant=tenant_id_b,
        main_assertion="B tenant delta",
    )
    token_a, _ = valid_session
    token_b, _ = valid_session_b
    a_resp = await dd_client.get(
        "/v1/decision_deltas/",
        headers={"Authorization": f"Bearer {token_a}"},
    )
    b_resp = await dd_client.get(
        "/v1/decision_deltas/",
        headers={"Authorization": f"Bearer {token_b}"},
    )
    a_items = a_resp.json()["items"]
    b_items = b_resp.json()["items"]
    assert all(i["main_assertion"] == "A tenant delta" for i in a_items)
    assert all(i["main_assertion"] == "B tenant delta" for i in b_items)
    assert {i["id"] for i in a_items}.isdisjoint(
        {i["id"] for i in b_items},
    )


# =====================================================================
# Target access
# =====================================================================


@pytest.mark.asyncio
async def test_list_filters_invisible_target_delta(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, _ = valid_session
    visible = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        status="proposed",
        main_assertion="Targetless delta remains visible.",
    )
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    hidden = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        status="proposed",
        target_node_kind="resource",
        target_node_id=resource_id,
        main_assertion="Restricted financial resource delta.",
    )

    resp = await dd_client.get(
        "/v1/decision_deltas/?status=proposed",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200, resp.text
    ids = {item["id"] for item in resp.json()["items"]}
    assert str(visible) in ids
    assert str(hidden) not in ids


@pytest.mark.asyncio
async def test_get_target_delta_forbidden_without_access(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, _ = valid_session
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    delta_id = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        target_node_kind="resource",
        target_node_id=resource_id,
    )

    resp = await dd_client.get(
        f"/v1/decision_deltas/{delta_id}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 403
    assert resp.json()["detail"] == {
        "error": "forbidden",
        "reason": "resource_out_of_scope:financial",
    }


@pytest.mark.asyncio
async def test_accept_target_delta_forbidden_without_access(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, _ = valid_session
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    delta_id = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        status="proposed",
        target_node_kind="resource",
        target_node_id=resource_id,
    )

    resp = await dd_client.post(
        f"/v1/decision_deltas/{delta_id}/accept",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )

    assert resp.status_code == 403
    stored_status = await gateway_pool.fetchval(
        "SELECT status FROM decision_deltas WHERE id = $1",
        delta_id,
    )
    assert stored_status == "proposed"


@pytest.mark.asyncio
async def test_get_target_delta_allowed_with_resource_viewer_role(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    delta_id = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        target_node_kind="resource",
        target_node_id=resource_id,
    )
    await grant_actor_role(
        gateway_pool,
        tenant=tenant_id,
        actor_id=actor_id,
        entity_type="resource",
        entity_id=resource_id,
        role="viewer",
    )

    resp = await dd_client.get(
        f"/v1/decision_deltas/{delta_id}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["target_node_id"] == str(resource_id)


@pytest.mark.asyncio
async def test_get_target_delta_admin_override_is_audited(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    delta_id = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        target_node_kind="resource",
        target_node_id=resource_id,
    )
    await grant_actor_role(
        gateway_pool,
        tenant=tenant_id,
        actor_id=actor_id,
        entity_type="tenant",
        entity_id=None,
        role="admin",
    )

    resp = await dd_client.get(
        f"/v1/decision_deltas/{delta_id}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200, resp.text
    row = await gateway_pool.fetchrow(
        """
        SELECT override_kind, reason, entity_type, entity_id
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
    assert row is not None
    assert row["override_kind"] == "admin"
    assert row["reason"] == "admin_override"


# =====================================================================
# GET /v1/decision_deltas/{delta_id}
# =====================================================================


@pytest.mark.asyncio
async def test_get_one_returns_evidence(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, _ = valid_session
    from datetime import datetime, timezone
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id,
        evidence=[
            {
                "source": "crm",
                "title": "Account flagged at-risk",
                "ts": datetime.now(timezone.utc),
                "trust_tier": "authoritative",
            },
        ],
    )
    resp = await dd_client.get(
        f"/v1/decision_deltas/{delta_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == str(delta_id)
    assert len(body["evidence"]) == 1
    assert body["evidence"][0]["source"] == "crm"


@pytest.mark.asyncio
async def test_get_one_404_for_unknown(
    dd_client: httpx.AsyncClient, valid_session,
):
    token, _ = valid_session
    from uuid import uuid4
    resp = await dd_client.get(
        f"/v1/decision_deltas/{uuid4()}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404


# =====================================================================
# POST /v1/decision_deltas/{delta_id}/accept
# =====================================================================


@pytest.mark.asyncio
async def test_accept_marks_accepted(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
    )
    resp = await dd_client.post(
        f"/v1/decision_deltas/{delta_id}/accept",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["delta"]["status"] == "accepted"
    assert body["delta"]["accepted_by"] is not None
    assert body["triggered"]["target_event_id"] is not None
    row = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="decision_delta.accept",
        resource_id=delta_id,
    )
    assert row is not None
    assert row["resource_type"] == "decision_delta"
    assert row["metadata"]["status_before"] == "proposed"
    assert row["metadata"]["status_after"] == "accepted"
    assert row["metadata"]["target_event_id"] == body["triggered"]["target_event_id"]


@pytest.mark.asyncio
async def test_accept_404_for_unknown(
    dd_client: httpx.AsyncClient, valid_session,
):
    token, _ = valid_session
    from uuid import uuid4
    resp = await dd_client.post(
        f"/v1/decision_deltas/{uuid4()}/accept",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )
    assert resp.status_code == 404


# =====================================================================
# POST /v1/decision_deltas/{delta_id}/delegate
# =====================================================================


@pytest.mark.asyncio
async def test_delegate_requires_owner_id(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, _ = valid_session
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
    )
    resp = await dd_client.post(
        f"/v1/decision_deltas/{delta_id}/delegate",
        headers={"Authorization": f"Bearer {token}"},
        json={"note": "please own this"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_delegate_transitions_and_records_owner(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, actor_id = valid_session
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
    )
    resp = await dd_client.post(
        f"/v1/decision_deltas/{delta_id}/delegate",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "owner_id": str(seeded_actor),
            "note": "Please handle by EOW.",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["delta"]["status"] == "delegated"
    assert body["delta"]["impact"]["delegation"]["owner_id"] == str(seeded_actor)
    row = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="decision_delta.delegate",
        resource_id=delta_id,
    )
    assert row is not None
    assert row["metadata"]["delegate_to_actor_id"] == str(seeded_actor)
    assert row["metadata"]["note_chars"] == len("Please handle by EOW.")
    assert "Please handle" not in str(row["metadata"])


# =====================================================================
# POST /v1/decision_deltas/{delta_id}/contest
# =====================================================================


@pytest.mark.asyncio
async def test_contest_requires_reason(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, _ = valid_session
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
    )
    resp = await dd_client.post(
        f"/v1/decision_deltas/{delta_id}/contest",
        headers={"Authorization": f"Bearer {token}"},
        json={"reason": "   "},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_contest_records_reason(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
    )
    resp = await dd_client.post(
        f"/v1/decision_deltas/{delta_id}/contest",
        headers={"Authorization": f"Bearer {token}"},
        json={"reason": "Disagree with evidence weighting."},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["delta"]["status"] == "contested"
    assert body["delta"]["impact"]["contest"]["reason"] == (
        "Disagree with evidence weighting."
    )
    row = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="decision_delta.contest",
        resource_id=delta_id,
    )
    assert row is not None
    assert row["metadata"]["reason_chars"] == len("Disagree with evidence weighting.")
    assert "Disagree with evidence" not in str(row["metadata"])


# =====================================================================
# POST /v1/decision_deltas/{delta_id}/add_context
# =====================================================================


@pytest.mark.asyncio
async def test_add_context_appends_note(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    delta_id = await seed_decision_delta(
        gateway_pool, tenant=tenant_id, status="proposed",
    )
    r1 = await dd_client.post(
        f"/v1/decision_deltas/{delta_id}/add_context",
        headers={"Authorization": f"Bearer {token}"},
        json={"note": "Anchor CSM confirmed via call."},
    )
    assert r1.status_code == 200, r1.text
    r2 = await dd_client.post(
        f"/v1/decision_deltas/{delta_id}/add_context",
        headers={"Authorization": f"Bearer {token}"},
        json={"note": "Additional context: customer has 30d to switch."},
    )
    assert r2.status_code == 200, r2.text
    body = r2.json()
    notes = body["delta"]["impact"]["context_notes"]
    assert len(notes) == 2
    assert notes[0]["note"].startswith("Anchor")
    assert notes[1]["note"].startswith("Additional")
    rows = await gateway_pool.fetch(
        """
        SELECT metadata
        FROM product_action_audit_log
        WHERE tenant_id = $1
          AND actor_id = $2
          AND action = 'decision_delta.add_context'
          AND resource_id = $3
        ORDER BY occurred_at ASC
        """,
        tenant_id,
        actor_id,
        delta_id,
    )
    assert len(rows) == 2
    assert rows[0]["metadata"]["note_chars"] == len("Anchor CSM confirmed via call.")
    assert rows[1]["metadata"]["note_chars"] == len(
        "Additional context: customer has 30d to switch."
    )
    assert "Anchor CSM" not in str(rows[0]["metadata"])
    assert "Additional context" not in str(rows[1]["metadata"])


# =====================================================================
# POST /v1/decision_deltas/from_recommendation/{recommendation_id}
# =====================================================================


@pytest.mark.asyncio
async def test_promote_from_recommendation_creates_delta(
    dd_client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, actor_id = valid_session

    # Seed an observation + commitment + recommendation row that we
    # can promote.
    obs_id = await seed_observation_minimal(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    commitment_id = await seed_commitment_for_target(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs_id,
    )
    rec_id = await seed_recommendation_for_promotion(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        target_commitment_id=commitment_id,
        supporting_event_ids=[obs_id],
    )

    resp = await dd_client.post(
        f"/v1/decision_deltas/from_recommendation/{rec_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["delta"]["source_recommendation_id"] == str(rec_id)
    assert body["delta"]["target_node_kind"] == "commitment"
    assert body["delta"]["target_node_id"] == str(commitment_id)
    # The promotion should attach the supporting observation as
    # evidence.
    assert len(body["delta"]["evidence"]) >= 1
    row = await _latest_product_action(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        action="decision_delta.promote_from_recommendation",
        resource_id=UUID(body["delta"]["id"]),
    )
    assert row is not None
    assert row["metadata"]["source_recommendation_id"] == str(rec_id)


# =====================================================================
# Auth
# =====================================================================


@pytest.mark.asyncio
async def test_unauthorized_without_token(
    dd_client: httpx.AsyncClient,
):
    resp = await dd_client.get("/v1/decision_deltas/")
    assert resp.status_code == 401
