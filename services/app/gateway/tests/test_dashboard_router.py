"""Integration tests for actor-scoped dashboard surfaces."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import asyncpg
import httpx
import pytest

from lib.shared.ids import uuid7


pytestmark = pytest.mark.integration


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _seed_observation(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    actor_id: UUID | None = None,
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
            $1, $2, now(), 'signal', 'dashboard:test',
            $3, '{}'::jsonb, 'dashboard seed',
            NULL, TRUE, 'authoritative',
            $4, '[]'::jsonb
        )
        """,
        oid,
        tenant_id,
        actor_id,
        f"dashboard-test-obs-{oid}",
    )
    return oid


async def _seed_resource(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    kind: str,
    identity: str,
    current_value: dict,
    metadata: dict | None = None,
    actor_id: UUID | None = None,
) -> UUID:
    rid = uuid7()
    event_id = await _seed_observation(pool, tenant_id, actor_id)
    await pool.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, current_value, metadata,
            last_updated_by_event_id
        ) VALUES (
            $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7
        )
        """,
        rid,
        tenant_id,
        kind,
        identity,
        json.dumps(current_value),
        json.dumps(metadata or {}),
        event_id,
    )
    return rid


async def _seed_customer(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    identity: str,
    account_owner_id: UUID | None = None,
) -> UUID:
    metadata = (
        {"account_owner_id": str(account_owner_id)}
        if account_owner_id is not None
        else {}
    )
    return await _seed_resource(
        pool,
        tenant_id,
        kind="relational",
        identity=identity,
        current_value={"arr_cents": 1_200_000},
        metadata=metadata,
        actor_id=account_owner_id,
    )


async def _seed_capacity(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    identity: str,
    team_actor_id: UUID | None = None,
) -> UUID:
    metadata = (
        {"team_ids": [str(team_actor_id)]}
        if team_actor_id is not None
        else {}
    )
    return await _seed_resource(
        pool,
        tenant_id,
        kind="capacity",
        identity=identity,
        current_value={
            "available_units": 1,
            "deployed_units": 9,
            "total_units": 10,
        },
        metadata=metadata,
        actor_id=team_actor_id,
    )


async def _seed_commitment(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    title: str,
    owner_id: UUID | None = None,
    state: str = "blocked",
) -> UUID:
    cid = uuid7()
    event_id = await _seed_observation(pool, tenant_id, owner_id)
    await pool.execute(
        """
        INSERT INTO commitments (
            id, tenant_id, title, state, owner_id, due_date,
            created_by_event_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        )
        """,
        cid,
        tenant_id,
        title,
        state,
        owner_id,
        datetime.now(timezone.utc) + timedelta(days=1),
        event_id,
    )
    return cid


async def _seed_goal(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    title: str,
) -> UUID:
    gid = uuid7()
    event_id = await _seed_observation(pool, tenant_id)
    await pool.execute(
        """
        INSERT INTO goals (
            id, tenant_id, title, created_by_event_id
        ) VALUES ($1, $2, $3, $4)
        """,
        gid,
        tenant_id,
        title,
        event_id,
    )
    return gid


async def _link_customer_commitment(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    customer_id: UUID,
    commitment_id: UUID,
    *,
    revenue_at_risk_usd: Decimal,
) -> None:
    await pool.execute(
        """
        INSERT INTO customer_commitments (
            customer_resource_id, commitment_id, tenant_id,
            relationship_kind, revenue_at_risk_usd, criticality
        ) VALUES ($1, $2, $3, 'delivers', $4, 'high')
        """,
        customer_id,
        commitment_id,
        tenant_id,
        revenue_at_risk_usd,
    )


async def _link_goal_commitment(
    pool: asyncpg.Pool,
    goal_id: UUID,
    commitment_id: UUID,
    *,
    critical: bool = True,
) -> None:
    await pool.execute(
        """
        INSERT INTO contributes_to (
            commitment_id, goal_id, is_critical_path
        ) VALUES ($1, $2, $3)
        """,
        commitment_id,
        goal_id,
        critical,
    )


async def _deploy_resource(
    pool: asyncpg.Pool,
    resource_id: UUID,
    commitment_id: UUID,
) -> None:
    await pool.execute(
        """
        INSERT INTO resource_deployments (
            resource_id, commitment_id, deployed_quantity
        ) VALUES ($1, $2, '{"units":1}'::jsonb)
        """,
        resource_id,
        commitment_id,
    )


@pytest.mark.asyncio
async def test_revenue_at_risk_filters_hidden_customers_and_mixed_rows(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, actor_id = valid_session
    visible_customer = await _seed_customer(
        gateway_pool,
        tenant_id,
        identity="Visible Customer",
        account_owner_id=actor_id,
    )
    hidden_customer = await _seed_customer(
        gateway_pool,
        tenant_id,
        identity="Hidden Customer",
    )
    mixed_customer = await _seed_customer(
        gateway_pool,
        tenant_id,
        identity="Mixed Customer",
        account_owner_id=actor_id,
    )
    visible_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Visible renewal blocker",
        owner_id=actor_id,
    )
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden renewal blocker",
    )
    mixed_visible_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Visible mixed blocker",
        owner_id=actor_id,
    )
    mixed_hidden_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden mixed blocker",
    )
    await _link_customer_commitment(
        gateway_pool,
        tenant_id,
        visible_customer,
        visible_commitment,
        revenue_at_risk_usd=Decimal("1250.00"),
    )
    await _link_customer_commitment(
        gateway_pool,
        tenant_id,
        hidden_customer,
        hidden_commitment,
        revenue_at_risk_usd=Decimal("9000.00"),
    )
    await _link_customer_commitment(
        gateway_pool,
        tenant_id,
        mixed_customer,
        mixed_visible_commitment,
        revenue_at_risk_usd=Decimal("100.00"),
    )
    await _link_customer_commitment(
        gateway_pool,
        tenant_id,
        mixed_customer,
        mixed_hidden_commitment,
        revenue_at_risk_usd=Decimal("200.00"),
    )

    resp = await client.get("/dashboard/revenue-at-risk", headers=_auth(token))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    customers = body["report"]["customers"]
    assert [c["customer_name"] for c in customers] == ["Visible Customer"]
    assert body["top_at_risk_customers"] == [str(visible_customer)]
    assert Decimal(str(body["report"]["grand_total_usd"])) == Decimal("1250.00")


@pytest.mark.asyncio
async def test_capacity_filters_hidden_resources_and_deployments(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, actor_id = valid_session
    visible_resource = await _seed_capacity(
        gateway_pool,
        tenant_id,
        identity="Visible Data Team",
        team_actor_id=actor_id,
    )
    hidden_resource = await _seed_capacity(
        gateway_pool,
        tenant_id,
        identity="Hidden Platform Team",
    )
    visible_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Visible deployment",
        owner_id=actor_id,
    )
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden deployment",
    )
    await _deploy_resource(gateway_pool, visible_resource, visible_commitment)
    await _deploy_resource(gateway_pool, visible_resource, hidden_commitment)
    await _deploy_resource(gateway_pool, hidden_resource, hidden_commitment)

    resp = await client.get("/dashboard/capacity", headers=_auth(token))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    risks = body["at_risk"]
    assert [risk["resource_name"] for risk in risks] == ["Visible Data Team"]
    assert risks[0]["resource_id"] == str(visible_resource)
    assert risks[0]["deploying_commitment_ids"] == [str(visible_commitment)]


@pytest.mark.asyncio
async def test_goals_filters_hidden_goal_and_critical_path_entries(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, actor_id = valid_session
    visible_goal = await _seed_goal(
        gateway_pool,
        tenant_id,
        title="Visible goal",
    )
    hidden_goal = await _seed_goal(
        gateway_pool,
        tenant_id,
        title="Hidden goal",
    )
    visible_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Visible critical path",
        owner_id=actor_id,
    )
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden critical path",
    )
    hidden_goal_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden goal only",
    )
    await _link_goal_commitment(gateway_pool, visible_goal, visible_commitment)
    await _link_goal_commitment(gateway_pool, visible_goal, hidden_commitment)
    await _link_goal_commitment(
        gateway_pool,
        hidden_goal,
        hidden_goal_commitment,
    )

    resp = await client.get("/dashboard/goals", headers=_auth(token))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    goals = body["goals"]
    assert [goal["title"] for goal in goals] == ["Visible goal"]
    critical_path = goals[0]["critical_path"]
    assert [entry["commitment"]["title"] for entry in critical_path] == [
        "Visible critical path"
    ]


@pytest.mark.asyncio
async def test_customer_detail_suppresses_aggregates_when_served_rows_hidden(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, actor_id = valid_session
    customer = await _seed_customer(
        gateway_pool,
        tenant_id,
        identity="Visible Customer",
        account_owner_id=actor_id,
    )
    visible_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Visible served commitment",
        owner_id=actor_id,
    )
    hidden_commitment = await _seed_commitment(
        gateway_pool,
        tenant_id,
        title="Hidden served commitment",
    )
    visible_capacity = await _seed_capacity(
        gateway_pool,
        tenant_id,
        identity="Visible Capacity",
        team_actor_id=actor_id,
    )
    hidden_capacity = await _seed_capacity(
        gateway_pool,
        tenant_id,
        identity="Hidden Capacity",
    )
    await _link_customer_commitment(
        gateway_pool,
        tenant_id,
        customer,
        visible_commitment,
        revenue_at_risk_usd=Decimal("50.00"),
    )
    await _link_customer_commitment(
        gateway_pool,
        tenant_id,
        customer,
        hidden_commitment,
        revenue_at_risk_usd=Decimal("75.00"),
    )
    await _deploy_resource(gateway_pool, visible_capacity, visible_commitment)
    await _deploy_resource(gateway_pool, hidden_capacity, hidden_commitment)

    resp = await client.get(
        f"/dashboard/customer/{customer}",
        headers=_auth(token),
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [c["title"] for c in body["served_commitments"]] == [
        "Visible served commitment"
    ]
    assert Decimal(str(body["revenue_at_risk_usd"])) == Decimal("0")
    assert body["health_timeline"] == []
    assert body["active_deployments"] == [str(visible_capacity)]
