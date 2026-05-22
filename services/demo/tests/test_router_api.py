"""services/demo/tests/test_router_api.py — gateway-level smoke for
the demo router endpoints (/v1/demo/companies, /v1/demo/sessions/*)."""
from __future__ import annotations

import asyncpg
import httpx
import pytest


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_list_companies_is_public(client: httpx.AsyncClient):
    resp = await client.get("/v1/demo/companies")
    assert resp.status_code == 200
    body = resp.json()
    company_ids = {c["company_id"] for c in body["items"]}
    assert company_ids == {"truss", "northwind", "meridian"}
    for item in body["items"]:
        assert item["name"]
        assert item["description"]
        assert item["tagline"]


@pytest.mark.asyncio
async def test_start_session_returns_token_and_provisions_tenant(
    client: httpx.AsyncClient, gateway_pool: asyncpg.Pool,
):
    resp = await client.post(
        "/v1/demo/sessions/start",
        json={"company_id": "truss"},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["company_id"] == "truss"
    assert body["auth_token"]
    assert body["tenant_id"]
    assert body["session_id"]

    # The new tenant exists in the registry, marked as demo.
    row = await gateway_pool.fetchrow(
        "SELECT is_demo FROM tenants WHERE id = $1",
        body["tenant_id"],
    )
    assert row is not None
    assert row["is_demo"] is True


@pytest.mark.asyncio
async def test_start_session_rejects_unknown_company(
    client: httpx.AsyncClient,
):
    resp = await client.post(
        "/v1/demo/sessions/start",
        json={"company_id": "fakeco"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_session_info_requires_auth(
    client: httpx.AsyncClient,
):
    resp = await client.post("/v1/demo/sessions/start",
                              json={"company_id": "northwind"})
    body = resp.json()
    sid = body["session_id"]

    # Without bearer token: 401
    no_auth = await client.get(f"/v1/demo/sessions/{sid}")
    assert no_auth.status_code == 401

    # With the minted token: 200 + sane shape
    authed = await client.get(
        f"/v1/demo/sessions/{sid}",
        headers={"Authorization": f"Bearer {body['auth_token']}",
                 "X-Tenant-Id": body["tenant_id"]},
    )
    assert authed.status_code == 200, authed.text
    info = authed.json()
    assert info["id"] == sid
    assert info["signals_injected"] == 0
    assert info["actions_taken"] == 0


@pytest.mark.asyncio
async def test_end_session_marks_ended(client: httpx.AsyncClient):
    resp = await client.post("/v1/demo/sessions/start",
                              json={"company_id": "meridian"})
    body = resp.json()
    sid, token, tid = body["session_id"], body["auth_token"], body["tenant_id"]

    end_resp = await client.post(
        f"/v1/demo/sessions/{sid}/end",
        headers={"Authorization": f"Bearer {token}", "X-Tenant-Id": tid},
    )
    assert end_resp.status_code == 200
    assert end_resp.json() == {"ended": True}
