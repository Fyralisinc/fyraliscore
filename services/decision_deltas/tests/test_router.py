"""
services/decision_deltas/tests/test_router.py — request-level tests.

The router is NOT yet registered in services/gateway/main.py (that
file is in this agent's forbidden zone for Phase 1). To exercise the
HTTP surface we build the gateway app with the same fixtures the
recommendation tests use and call include_router on the decision-delta
router for the duration of the test.

Once the gateway owner adds the registration line, this preamble can
be removed; the test bodies will keep working unchanged.
"""
from __future__ import annotations

from typing import AsyncGenerator

import asyncpg
import httpx
import pytest
import pytest_asyncio

from services.decision_deltas.router import build_router
from services.gateway.main import build_app

from .conftest import (
    seed_commitment_for_target,
    seed_decision_delta,
    seed_observation_minimal,
    seed_recommendation_for_promotion,
)


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def dd_client(app_deps) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Gateway app + decision_deltas router mounted."""
    app = build_app(
        pool=app_deps.pool,
        actor_repo=app_deps.actor_repo,
        alias_repo=app_deps.alias_repo,
        embedder=app_deps.embedder,
        rate_limiter=app_deps.rate_limiter,
        slack_signing_secret=app_deps.slack_signing_secret,
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
    token, _ = valid_session
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
    token, _ = valid_session
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
    token, _ = valid_session
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
    token, _ = valid_session
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
    token, _ = valid_session

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


# =====================================================================
# Auth
# =====================================================================


@pytest.mark.asyncio
async def test_unauthorized_without_token(
    dd_client: httpx.AsyncClient,
):
    resp = await dd_client.get("/v1/decision_deltas/")
    assert resp.status_code == 401
