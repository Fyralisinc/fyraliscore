"""
tests/integration/test_recommendation_flow.py — Stage 1 decision support
end-to-end smoke (Session 6 of RECOMMENDATION-BUILD-PLAN).

Covers the full recommendation lifecycle through the gateway HTTP API:

  setup → recommendation Model lands → CEO lists → CEO acts →
  Commitment transitions → recommendation archives + state_change
  Observation chain ties everything together.

The Think reasoning path itself is NOT exercised here (that requires
a live LLM + replay of a fixture trigger, which is covered by
services/think/tests/test_llm_reason.py and the real_llm/ suite). We
seed a recommendation Model directly to keep this test deterministic
and fast — its purpose is to assert the surrounding plumbing (storage,
ranker, act handler, dismiss handler, audit chain) is wired correctly.
"""
from __future__ import annotations

import asyncpg
import httpx
import pytest


# Reuse the gateway test fixtures via the recommendations conftest —
# both modules pull from services/gateway/tests/conftest.py, so the
# same client / pool / session machinery is available here.
from services.recommendations.tests.conftest import (  # noqa: F401
    _DeterministicEmbedder,
    app_deps,
    client,
    gateway_pool,
    make_recommendation_proposition,
    rate_limiter,
    seed_commitment,
    seed_observation,
    seed_recommendation_model,
    seeded_actor,
    tenant_id,
    valid_session,
)


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_full_recommendation_flow_lists_acts_archives(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    """End-to-end: a CEO sees a recommendation about a slipping
    Commitment, acts on it, the Commitment transitions, the
    recommendation is archived, and the audit chain links everything."""
    token, ceo_actor_id = valid_session
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    cid = await seed_commitment(
        gateway_pool,
        tenant=tenant_id,
        owner_id=seeded_actor,
        born_from_event=obs_id,
        state="active",
        title="Build rate limiter for API",
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id,
        target_actor_id=ceo_actor_id,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=ceo_actor_id,
            target_type="commitment",
            target_id=cid,
            payload={"new_state": "paused"},
            expected_impact=340_000.0,
        ),
        natural=(
            "Pause the rate limiter commitment — Alice signaled a 2-week slip "
            "and customer-impact horizon shifts revenue at risk to $340K."
        ),
        confidence=0.7,
    )

    # 1. The CEO lists their action list — recommendation is the top item.
    resp = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body["items"]) == 1
    item = body["items"][0]
    assert item["id"] == str(rec_id)
    assert item["target_act_ref"]["type"] == "commitment"
    assert item["target_act_ref"]["id"] == str(cid)
    assert item["target_entity"]["title"] == "Build rate limiter for API"
    assert item["target_entity"]["state"] == "active"

    # 2. The CEO acts on the recommendation.
    act_resp = await client.post(
        f"/v1/recommendations/{rec_id}/act",
        json={"notes": "queue is fully booked through Q2"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert act_resp.status_code == 200, act_resp.text
    act_body = act_resp.json()
    assert act_body["target_act_change_kind"] == "transition_commitment"
    assert act_body["target_act_change_id"] == str(cid)

    # 3. The Commitment transitioned to paused.
    cm_state = await gateway_pool.fetchval(
        "SELECT state FROM commitments WHERE id = $1", cid,
    )
    assert cm_state == "paused"

    # 4. The recommendation Model is archived with reason='acted_upon',
    #    and caused_act_change_id points at the Commitment.
    rec_row = await gateway_pool.fetchrow(
        "SELECT status, archive_reason, caused_act_change_id "
        "FROM models WHERE id = $1",
        rec_id,
    )
    assert rec_row["status"] == "archived"
    assert rec_row["archive_reason"] == "acted_upon"
    assert rec_row["caused_act_change_id"] == cid

    # 5. The recommendation no longer surfaces in the action list.
    resp2 = await client.get(
        "/v1/recommendations",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp2.status_code == 200
    assert resp2.json()["items"] == []

    # 6. Audit chain: the act emits a `recommendation_acted_upon`
    #    state_change observation referencing the recommendation.
    audit_row = await gateway_pool.fetchrow(
        """
        SELECT content_text, content
        FROM observations
        WHERE tenant_id = $1
          AND kind = 'state_change'
          AND content->>'state_change_kind' = 'recommendation_acted_upon'
          AND content->>'entity_id' = $2
        """,
        tenant_id,
        str(rec_id),
    )
    assert audit_row is not None


@pytest.mark.asyncio
async def test_dismiss_archives_without_act_change(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, ceo_actor_id = valid_session
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
        target_actor_id=ceo_actor_id,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=ceo_actor_id,
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

    # Commitment unchanged.
    state = await gateway_pool.fetchval(
        "SELECT state FROM commitments WHERE id = $1", cid,
    )
    assert state == "active"

    # Recommendation archived with the dismissal reason.
    rec_row = await gateway_pool.fetchrow(
        "SELECT status, archive_reason FROM models WHERE id = $1", rec_id,
    )
    assert rec_row["status"] == "archived"
    assert rec_row["archive_reason"] == "dismissed_by_user"


@pytest.mark.asyncio
async def test_act_after_archive_returns_409(
    client: httpx.AsyncClient,
    valid_session,
    gateway_pool: asyncpg.Pool,
    tenant_id,
    seeded_actor,
):
    token, ceo_actor_id = valid_session
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
        target_actor_id=ceo_actor_id,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=ceo_actor_id,
            target_type="commitment",
            target_id=cid,
        ),
    )
    # First act succeeds.
    first = await client.post(
        f"/v1/recommendations/{rec_id}/act",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first.status_code == 200
    # Second act on the now-archived recommendation: 409 Conflict.
    second = await client.post(
        f"/v1/recommendations/{rec_id}/act",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert second.status_code == 409
