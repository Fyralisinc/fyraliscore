"""
services/today/tests/test_today_api.py — gateway-level smoke tests for
the Fyralis Today aggregator.

  GET  /v1/today
  POST /v1/today/brand
  POST /v1/recommendations/{id}/triage
"""
from __future__ import annotations

import asyncpg
import httpx
import pytest

from services.recommendations.tests.conftest import (  # noqa: F401
    make_recommendation_proposition,
    seed_commitment,
    seed_observation,
    seed_recommendation_model,
    # Pulls in the gateway fixtures (client, valid_session, etc.)
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


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_today_returns_full_payload_for_actor_with_no_recommendations(
    client: httpx.AsyncClient,
    valid_session,
):
    token, _ = valid_session
    resp = await client.get(
        "/v1/today",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["cards"] == []
    # All required top-level keys present
    for key in (
        "brand", "page", "signal_strip", "vitals", "nav",
        "cards", "ask_suggestions",
    ):
        assert key in body
    # Signal strip always returns four metrics
    assert len(body["signal_strip"]) == 4
    # Empty state surfaces when no cards
    assert body.get("empty_state") is not None
    # Page header tone is quiet/clear when nothing pressing
    assert body["page"]["state_tone"] in {"quiet", "clear"}


@pytest.mark.asyncio
async def test_today_lists_recommendations_with_severity_and_card_shape(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, _ = valid_session
    obs = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs,
    )
    await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
            expected_impact=0.95,
        ),
        confidence=0.95,
        natural="Pause the rate limiter — three weeks of slipping deliverables.",
    )

    resp = await client.get(
        "/v1/today",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["cards"]) == 1
    card = body["cards"][0]
    # Severity derived from impact * confidence (0.95 * 0.95 = 0.9025 → critical,
    # bucket boundary: >= 0.80; see services/today/aggregator.py).
    assert card["severity"] == "critical"
    assert card["category"] in ("operational", "strategic")
    assert card["kind_label"]
    assert card["headline_html"]
    assert isinstance(card["actions"], list) and "act" in card["actions"]
    assert "stats" in card and len(card["stats"]) >= 1
    assert card["detail"]["paths"]


@pytest.mark.asyncio
async def test_triage_hold_archives_with_manual_reason(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, _ = valid_session
    obs = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    cid = await seed_commitment(
        gateway_pool, tenant=tenant_id, owner_id=seeded_actor,
        born_from_event=obs,
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
        ),
    )

    resp = await client.post(
        f"/v1/recommendations/{rec_id}/triage",
        headers={"Authorization": f"Bearer {token}"},
        json={"action": "hold"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"ok": True, "recommendation_id": str(rec_id), "action": "hold"}

    # Recommendation is archived with archive_reason='manual'
    row = await gateway_pool.fetchrow(
        "SELECT status, archive_reason FROM models WHERE id = $1", rec_id,
    )
    assert row["status"] == "archived"
    assert row["archive_reason"] == "manual"


@pytest.mark.asyncio
async def test_triage_rejects_act(
    client: httpx.AsyncClient,
    valid_session,
):
    token, _ = valid_session
    resp = await client.post(
        f"/v1/recommendations/{__import__('uuid').uuid4()}/triage",
        headers={"Authorization": f"Bearer {token}"},
        json={"action": "act"},
    )
    assert resp.status_code == 400
    assert resp.json()["error"] == "invalid_action"


@pytest.mark.asyncio
async def test_brand_rename_persists_for_tenant(
    client: httpx.AsyncClient,
    valid_session,
):
    token, _ = valid_session
    resp = await client.post(
        "/v1/today/brand",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "Atlas"},
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "Atlas"

    # Subsequent /v1/today reads back the new name
    resp = await client.get(
        "/v1/today",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200
    assert resp.json()["brand"]["name"] == "Atlas"
    assert resp.json()["brand"]["mark"] == "A"
