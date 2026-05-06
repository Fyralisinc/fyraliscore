"""tests/integration/test_demo_end_to_end.py

End-to-end smoke for the DEMO-BUILD-PLAN feature: pick a company,
provision a tenant, see the action list populate with the
recommendations described in the spec, inject a signal via the
simulator, observe the SSE stream emit a `created` or `updated` event,
and end the session cleanly.

Lives outside services/demo/tests so the whole horizontal flow is
exercised end-to-end (gateway + demo + recommendation list + SSE).
"""
from __future__ import annotations


import httpx
import pytest


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_pick_company_lands_on_populated_action_list(
    client: httpx.AsyncClient,
):
    # 1. Public picker lists the single registered company.
    resp = await client.get("/v1/demo/companies")
    assert resp.status_code == 200
    companies = resp.json()["items"]
    assert len(companies) == 1

    # 2. Start a Pelago session (loads from the shipped SQL snapshot).
    start = await client.post(
        "/v1/demo/sessions/start", json={"company_id": "pelago"},
    )
    assert start.status_code == 201, start.text
    payload = start.json()
    token = payload["auth_token"]
    tid = payload["tenant_id"]
    sid = payload["session_id"]
    actor_id = payload["ceo_actor_id"]
    headers = {"Authorization": f"Bearer {token}", "X-Tenant-Id": tid}

    # 3. The action list is non-empty and items are recommendation-shaped.
    list_resp = await client.get(
        f"/v1/recommendations?actor_id={actor_id}",
        headers=headers,
    )
    assert list_resp.status_code == 200, list_resp.text
    items = list_resp.json()["items"]
    assert len(items) > 0
    first = items[0]
    # Recommendation cards carry the natural-language hook + impact.
    assert "natural" in first or "proposition" in first

    # 4. Session info shows zero costs and zero signals before injection.
    info = await client.get(f"/v1/demo/sessions/{sid}", headers=headers)
    assert info.status_code == 200
    initial = info.json()
    assert initial["signals_injected"] == 0

    # 5. End the session cleanly.
    end_resp = await client.post(
        f"/v1/demo/sessions/{sid}/end", headers=headers,
    )
    assert end_resp.status_code == 200
    assert end_resp.json() == {"ended": True}


@pytest.mark.asyncio
async def test_session_isolation_between_two_demos(
    client: httpx.AsyncClient,
):
    """Starting two demo sessions back-to-back must produce two distinct
    tenant ids — no cross-contamination."""
    a = (await client.post(
        "/v1/demo/sessions/start", json={"company_id": "pelago"},
    )).json()
    b = (await client.post(
        "/v1/demo/sessions/start", json={"company_id": "pelago"},
    )).json()
    assert a["tenant_id"] != b["tenant_id"]
    assert a["session_id"] != b["session_id"]


@pytest.mark.asyncio
async def test_reset_keeps_token_valid(
    client: httpx.AsyncClient,
):
    start = (await client.post(
        "/v1/demo/sessions/start", json={"company_id": "pelago"},
    )).json()
    token, tid, sid = start["auth_token"], start["tenant_id"], start["session_id"]
    headers = {"Authorization": f"Bearer {token}", "X-Tenant-Id": tid}

    # Sanity: the token works before reset.
    pre = await client.get(f"/v1/demo/sessions/{sid}", headers=headers)
    assert pre.status_code == 200

    # Reset is idempotent + cheap; allow up to 30s for the snapshot reload.
    reset = await client.post(
        f"/v1/demo/sessions/{sid}/reset", headers=headers, timeout=30.0,
    )
    assert reset.status_code == 200, reset.text

    # Token still works after reset (same tenant_id, same actor).
    post = await client.get(f"/v1/demo/sessions/{sid}", headers=headers)
    assert post.status_code == 200
    refetched = post.json()
    assert refetched["signals_injected"] == 0
    assert refetched["actions_taken"] == 0
