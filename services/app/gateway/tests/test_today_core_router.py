"""Integration tests for the legacy Today core and artifact drawer routes."""
from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import httpx
import pytest

from services.app.gateway.tests.test_dashboard_router import (
    _auth,
    _seed_commitment,
    _seed_resource,
)
from services.platform.access_control.roles import grant_role


pytestmark = pytest.mark.integration


async def _deploy_with_quantity(
    pool: asyncpg.Pool,
    resource_id: UUID,
    commitment_id: UUID,
    *,
    value: float,
) -> None:
    await pool.execute(
        """
        INSERT INTO resource_deployments (
            resource_id, commitment_id, deployed_quantity
        ) VALUES ($1, $2, $3::jsonb)
        """,
        resource_id,
        commitment_id,
        json.dumps({"value": value}),
    )


@pytest.mark.asyncio
async def test_artifact_drawer_denies_hidden_commitment(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, _ = valid_session
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden drawer commitment",
    )

    resp = await client.get(
        f"/v1/artifacts/commitment/{hidden_commitment}",
        headers=_auth(token),
    )

    assert resp.status_code == 403, resp.text
    assert resp.json()["reason"] == "commitment_out_of_scope"


@pytest.mark.asyncio
async def test_artifact_resource_drawer_filters_hidden_consumers(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, actor_id = valid_session
    resource_id = await _seed_resource(
        gateway_pool,
        tenant_id,
        kind="human",
        identity="Platform pod",
        current_value={"label": "Platform pod", "capacity": 10, "unit": "FTE"},
        actor_id=actor_id,
    )
    async with gateway_pool.acquire() as conn:
        await grant_role(
            actor_id,
            "resource",
            resource_id,
            "viewer",
            actor_id,
            conn=conn,
            tenant_id=tenant_id,
        )
    visible_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Visible consumer",
        owner_id=actor_id,
    )
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden consumer",
    )
    await _deploy_with_quantity(
        gateway_pool,
        resource_id,
        visible_commitment,
        value=3,
    )
    await _deploy_with_quantity(
        gateway_pool,
        resource_id,
        hidden_commitment,
        value=7,
    )

    resp = await client.get(
        f"/v1/artifacts/resource/{resource_id}",
        headers=_auth(token),
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    top_consumers = next(
        section for section in body["sections"] if section["title"] == "Top consumers"
    )
    assert [item["primary"] for item in top_consumers["items"]] == [
        "Visible consumer"
    ]
    fields = next(
        section for section in body["sections"] if section["title"] == "At a glance"
    )
    assert {row["label"]: row["value"] for row in fields["rows"]}[
        "Active commitments"
    ] == "1"


@pytest.mark.asyncio
async def test_today_brand_update_requires_admin_or_leadership(
    client: httpx.AsyncClient,
    valid_session,
) -> None:
    token, _ = valid_session

    resp = await client.post(
        "/v1/today/brand",
        headers=_auth(token),
        json={"name": "Customer Intelligence"},
    )

    assert resp.status_code == 403, resp.text
    assert resp.json() == {
        "error": "forbidden",
        "reason": "brand_update_requires_admin_or_leadership",
    }


@pytest.mark.asyncio
async def test_today_brand_update_allows_leadership_actor(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, actor_id = valid_session
    async with gateway_pool.acquire() as conn:
        await grant_role(
            actor_id,
            "tenant",
            None,
            "leadership",
            actor_id,
            conn=conn,
            tenant_id=tenant_id,
        )

    resp = await client.post(
        "/v1/today/brand",
        headers=_auth(token),
        json={"name": "Customer Intelligence"},
    )

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"ok": True, "name": "Customer Intelligence"}

    row = await gateway_pool.fetchrow(
        """
        SELECT current_value
        FROM resources
        WHERE tenant_id = $1
          AND kind = 'ip'
          AND identity = 'fyralis.brand_name'
          AND archived_at IS NULL
        """,
        tenant_id,
    )
    assert row is not None
    current_value = row["current_value"]
    if isinstance(current_value, str):
        current_value = json.loads(current_value)
    assert current_value == {"name": "Customer Intelligence"}
