"""Actor-scope regression tests for Structure routes."""
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


async def _grant_resource_viewer(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    resource_id: UUID,
) -> None:
    async with pool.acquire() as conn:
        await grant_role(
            actor_id,
            "resource",
            resource_id,
            "viewer",
            actor_id,
            conn=conn,
            tenant_id=tenant_id,
        )


@pytest.mark.asyncio
async def test_structure_overlay_denies_hidden_commitment(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, _ = valid_session
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden structure commitment",
    )

    resp = await client.get(
        f"/v1/structure/overlay/{hidden_commitment}",
        headers=_auth(token),
    )

    assert resp.status_code == 403, resp.text
    assert resp.json()["reason"] == "commitment_out_of_scope"


@pytest.mark.asyncio
async def test_structure_recent_filters_hidden_commitments_and_people(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, actor_id = valid_session
    visible_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Visible structure commitment",
        owner_id=actor_id,
    )
    await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden structure commitment",
    )

    resp = await client.get(
        "/v1/structure/recent?since_minutes=0",
        headers=_auth(token),
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [c["label"] for c in body["commitments"]] == [
        "Visible structure commitment"
    ]
    assert body["commitments"][0]["id"] == str(visible_commitment)
    assert [p["id"] for p in body["people"]] == [str(actor_id)]


@pytest.mark.asyncio
async def test_structure_resource_routes_filter_hidden_consumers(
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
        identity="Structure platform pod",
        current_value={
            "label": "Structure platform pod",
            "capacity": 10,
            "unit": "FTE",
        },
        actor_id=actor_id,
    )
    await _grant_resource_viewer(
        gateway_pool,
        tenant_id=tenant_id,
        actor_id=actor_id,
        resource_id=resource_id,
    )
    await _seed_resource(
        gateway_pool,
        tenant_id,
        kind="human",
        identity="Hidden structure pod",
        current_value={"label": "Hidden structure pod", "capacity": 5},
    )
    visible_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Visible structure consumer",
        owner_id=actor_id,
    )
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden structure consumer",
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

    aggregate_resp = await client.get(
        "/v1/structure/resources/aggregate",
        headers=_auth(token),
    )
    overlay_resp = await client.get(
        f"/v1/structure/resources/{resource_id}/overlay",
        headers=_auth(token),
    )

    assert aggregate_resp.status_code == 200, aggregate_resp.text
    resources = aggregate_resp.json()["resources"]
    assert [r["label"] for r in resources] == ["Structure platform pod"]
    assert resources[0]["deployed"] == 3
    assert resources[0]["deployments_count"] == 1
    assert [c["label"] for c in resources[0]["top_consumers"]] == [
        "Visible structure consumer"
    ]

    assert overlay_resp.status_code == 200, overlay_resp.text
    overlay = overlay_resp.json()
    assert overlay["resource"]["deployed"] == 3
    assert [c["label"] for c in overlay["consumers"]] == [
        "Visible structure consumer"
    ]
