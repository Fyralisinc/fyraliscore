"""Integration tests for gateway contestability access controls."""
from __future__ import annotations

from uuid import UUID

import asyncpg
import httpx
import pytest

from services.app.gateway.tests.test_map_routes import (  # type: ignore
    _auth,
    _make_model_private,
    _seed_model,
)


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_contest_private_out_of_scope_model_is_denied_before_mutation(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, _actor_id = valid_session
    model_id = await _seed_model(
        gateway_pool,
        tenant_id,
        natural="Hidden model should not be contestable by this actor.",
        confidence=0.8,
    )
    await _make_model_private(gateway_pool, tenant_id, model_id)

    resp = await client.post(
        f"/contest/{model_id}",
        headers=_auth(token),
        json={
            "contestation_kind": "belief",
            "rationale": "This should be rejected before contestation.",
        },
    )

    assert resp.status_code == 403, resp.text
    assert resp.json() == {
        "error": "access_denied",
        "reason": "model_out_of_scope",
    }
    row = await gateway_pool.fetchrow(
        "SELECT contested_count FROM models WHERE id = $1",
        model_id,
    )
    assert row["contested_count"] == 0
    obs_count = await gateway_pool.fetchval(
        """
        SELECT COUNT(*) FROM observations
        WHERE tenant_id = $1
          AND kind = 'contestation'
          AND content->>'contested_model_id' = $2
        """,
        tenant_id,
        str(model_id),
    )
    assert obs_count == 0


@pytest.mark.asyncio
async def test_contest_private_in_scope_model_succeeds(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, actor_id = valid_session
    model_id = await _seed_model(
        gateway_pool,
        tenant_id,
        natural="Scoped model can be contested by the scoped actor.",
        confidence=0.8,
    )
    await _make_model_private(gateway_pool, tenant_id, model_id, actor_id)

    resp = await client.post(
        f"/contest/{model_id}",
        headers=_auth(token),
        json={
            "contestation_kind": "belief",
            "rationale": "I am the subject and this belief is wrong.",
        },
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["standing_basis"] == "scope"
    assert body["override_applied"] is True
    assert body["observation_id"]
    row = await gateway_pool.fetchrow(
        "SELECT contested_count, confidence FROM models WHERE id = $1",
        model_id,
    )
    assert row["contested_count"] == 1
    assert float(row["confidence"]) == pytest.approx(0.24)
