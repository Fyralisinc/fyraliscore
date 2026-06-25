"""
services/product/today/tests/test_today_api.py — gateway-level smoke tests for
the Fyralis Today aggregator.

  GET  /v1/today
  POST /v1/today/brand
  POST /v1/recommendations/{id}/triage
"""
# ruff: noqa: F811
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import httpx
import pytest

from lib.shared.ids import uuid7
from services.platform.access_control.roles import grant_role
from services.product.recommendations.tests.conftest import (  # noqa: F401
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


def _embedding() -> list[float]:
    embedding = [0.0] * 768
    embedding[0] = 1.0
    return embedding


async def _seed_same_tenant_actor(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    display_name: str = "Mallory",
) -> UUID:
    actor_id = uuid7()
    await pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', $3, 'active')
        """,
        actor_id,
        tenant_id,
        display_name,
    )
    return actor_id


async def _seed_today_signal(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    actor_id: UUID | None,
    content_text: str,
) -> UUID:
    oid = uuid7()
    await pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, now(), 'signal', 'today:test',
            $3, '{}'::jsonb, $4,
            NULL, TRUE, 'authoritative',
            $5, '[]'::jsonb
        )
        """,
        oid,
        tenant_id,
        actor_id,
        content_text,
        f"today-test-obs-{oid}",
    )
    return oid


async def _seed_today_model(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    born_from_event: UUID,
    natural: str,
    scope_actor: UUID,
    visible_to_subjects: bool = False,
) -> UUID:
    mid = uuid7()
    proposition = {
        "kind": "belief",
        "claim_role": "fact",
        "subject": "today-test",
        "assertion": natural,
    }
    await pool.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, activation,
            confidence_at_assertion, activation_coefficient,
            visible_to_subjects, status
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            $7::uuid[], '[]'::jsonb, $8::jsonb,
            0.7, 1.0,
            0.7, 1.0,
            $9, 'active'
        )
        """,
        mid,
        tenant_id,
        born_from_event,
        json.dumps(proposition),
        natural,
        _embedding(),
        [scope_actor],
        json.dumps({"valid_from": "2026-04-26T00:00:00Z", "valid_until": None}),
        visible_to_subjects,
    )
    return mid


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
    # Severity derived from impact * confidence (0.95 * 0.95 = 0.9025 -> critical)
    assert card["severity"] == "critical"
    assert card["category"] in ("operational", "strategic")
    assert card["kind_label"]
    assert card["headline_html"]
    assert isinstance(card["actions"], list) and "act" in card["actions"]
    assert "stats" in card and len(card["stats"]) >= 1
    assert card["detail"]["paths"]


@pytest.mark.asyncio
async def test_today_filters_recommendations_with_hidden_targets(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, actor_id = valid_session
    hidden_actor = await _seed_same_tenant_actor(gateway_pool, tenant_id)
    obs = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=actor_id,
    )
    hidden_commitment = await seed_commitment(
        gateway_pool,
        tenant=tenant_id,
        owner_id=hidden_actor,
        born_from_event=obs,
        title="Hidden target commitment",
    )
    await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=obs,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=hidden_commitment,
            expected_impact=0.95,
        ),
        confidence=0.95,
        natural="Hidden target recommendation must not reach Today.",
    )

    resp = await client.get(
        "/v1/today",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["cards"] == []
    assert body.get("empty_state") is not None


@pytest.mark.asyncio
async def test_today_filters_card_evidence_and_recent_signals_by_row_access(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, actor_id = valid_session
    hidden_actor = await _seed_same_tenant_actor(gateway_pool, tenant_id)
    visible_signal = await _seed_today_signal(
        gateway_pool,
        tenant_id,
        actor_id=actor_id,
        content_text="visible today signal",
    )
    hidden_signal = await _seed_today_signal(
        gateway_pool,
        tenant_id,
        actor_id=hidden_actor,
        content_text="hidden today signal",
    )
    cid = await seed_commitment(
        gateway_pool,
        tenant=tenant_id,
        owner_id=actor_id,
        born_from_event=visible_signal,
        title="Visible target commitment",
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=seeded_actor,
        born_from_event=visible_signal,
        proposition=make_recommendation_proposition(
            target_actor_id=seeded_actor,
            target_type="commitment",
            target_id=cid,
            expected_impact=0.95,
        ),
        confidence=0.95,
        natural="Visible recommendation with mixed evidence.",
    )
    await gateway_pool.execute(
        """
        UPDATE models
        SET supporting_event_ids = $2::uuid[]
        WHERE id = $1
        """,
        rec_id,
        [visible_signal, hidden_signal],
    )

    resp = await client.get(
        "/v1/today",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert len(body["cards"]) == 1
    evidence_quotes = [
        item["quote_html"]
        for item in body["cards"][0]["detail"].get("evidence", [])
    ]
    assert evidence_quotes == ["visible today signal"]
    signal_titles = [
        item["title"] for item in body["recent_signals"]["signals"]
    ]
    assert "visible today signal" in signal_titles
    assert "hidden today signal" not in signal_titles
    assert body["recent_signals"]["total"] == len(signal_titles)


@pytest.mark.asyncio
async def test_today_financial_metrics_require_resource_access(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    event_id = await _seed_today_signal(
        gateway_pool,
        tenant_id,
        actor_id=actor_id,
        content_text="financial seed event",
    )
    await gateway_pool.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, current_value, metadata,
            last_updated_by_event_id
        ) VALUES (
            $1, $2, 'financial', 'ARR snapshot',
            $3::jsonb, '{}'::jsonb, $4
        )
        """,
        uuid7(),
        tenant_id,
        json.dumps({"value": "123M", "unit": "USD"}),
        event_id,
    )

    resp = await client.get(
        "/v1/today",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200
    arr = next(item for item in resp.json()["signal_strip"] if item["id"] == "arr")
    assert arr["unavailable"] is True
    assert arr["value"] == "—"


@pytest.mark.asyncio
async def test_today_filters_just_updated_models_by_row_access(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    hidden_actor = await _seed_same_tenant_actor(gateway_pool, tenant_id)
    visible_signal = await _seed_today_signal(
        gateway_pool,
        tenant_id,
        actor_id=actor_id,
        content_text="visible model event",
    )
    hidden_signal = await _seed_today_signal(
        gateway_pool,
        tenant_id,
        actor_id=hidden_actor,
        content_text="hidden model event",
    )
    await _seed_today_model(
        gateway_pool,
        tenant_id,
        born_from_event=visible_signal,
        natural="visible learned private model",
        scope_actor=actor_id,
    )
    await _seed_today_model(
        gateway_pool,
        tenant_id,
        born_from_event=hidden_signal,
        natural="hidden learned private model",
        scope_actor=hidden_actor,
    )

    resp = await client.get(
        "/v1/today",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200
    html = resp.json()["just_updated"]["text_html"]
    assert "visible learned private model" in html
    assert "hidden learned private model" not in html


@pytest.mark.asyncio
async def test_today_admin_override_for_recent_signal_is_audited(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
):
    token, actor_id = valid_session
    hidden_actor = await _seed_same_tenant_actor(gateway_pool, tenant_id)
    hidden_signal = await _seed_today_signal(
        gateway_pool,
        tenant_id,
        actor_id=hidden_actor,
        content_text="admin-visible hidden today signal",
    )
    async with gateway_pool.acquire() as conn:
        await grant_role(
            actor_id,
            "tenant",
            None,
            "admin",
            actor_id,
            conn=conn,
            tenant_id=tenant_id,
        )

    resp = await client.get(
        "/v1/today",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert resp.status_code == 200
    signal_titles = [
        item["title"] for item in resp.json()["recent_signals"]["signals"]
    ]
    assert "admin-visible hidden today signal" in signal_titles
    row = await gateway_pool.fetchrow(
        """
        SELECT override_kind, reason
        FROM access_override_log
        WHERE tenant_id = $1
          AND actor_id = $2
          AND entity_type = 'observation'
          AND entity_id = $3
        ORDER BY occurred_at DESC
        LIMIT 1
        """,
        tenant_id,
        actor_id,
        hidden_signal,
    )
    assert row is not None
    assert row["override_kind"] == "admin"
    assert row["reason"] == "admin_override"


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
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, actor_id = valid_session
    async with gateway_pool.acquire() as conn:
        await grant_role(
            actor_id,
            "tenant",
            None,
            "admin",
            actor_id,
            conn=conn,
            tenant_id=tenant_id,
        )
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
