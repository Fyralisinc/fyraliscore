"""Integration tests for the Resolution Tracker backend."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import httpx
import pytest

from services.product.decision_deltas.tests.conftest import (  # type: ignore
    grant_actor_role,
    seed_decision_delta,
    seed_resource_for_target,
)
from services.product.resolution_threads import repo as resolution_repo


pytestmark = pytest.mark.integration


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _thread_payload() -> dict:
    return {
        "id": "rt-acme-deal-reality",
        "title": "Restore Acme Expansion to a supportable late-stage path",
        "status": "active",
        "current_state": "Commit forecast is unsupported.",
        "target_state": "Security review scheduled.",
        "owner": "AE + RevOps",
        "success_criteria": ["Security owner assigned."],
        "steps": [
            {
                "id": "step-security-owner",
                "label": "Assign internal security owner",
                "owner": "VP Sales Ops",
                "status": "waiting",
                "proof_needed": "Named owner appears in Slack.",
            }
        ],
        "watched_signals": [
            {
                "id": "watch-calendar",
                "label": "Buyer alignment meeting",
                "source_type": "Calendar",
                "expected": "CFO + security call appears this week.",
                "status": "watching",
            }
        ],
        "escalation_triggers": ["No buyer alignment meeting scheduled."],
    }


async def _seed_resolution_thread(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    created_by: UUID,
    target_node_kind: str | None = None,
    target_node_id: UUID | None = None,
) -> resolution_repo.ResolutionThread:
    async with pool.acquire() as conn:
        return await resolution_repo.create_thread(
            conn,
            tenant_id=tenant_id,
            payload=_thread_payload(),
            target_node_kind=target_node_kind,
            target_node_id=target_node_id,
            created_by=created_by,
        )


@pytest.mark.asyncio
async def test_accept_delta_creates_persisted_resolution_thread(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    did = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        main_assertion="Move Acme Expansion forecast from Commit to Best Case",
        label="authority_required",
        confidence=0.78,
        falsification_condition="Security review is scheduled.",
        impact={
            "arr_at_risk": 1_200_000,
            "accounts_affected": 1,
            "resolution_thread": _thread_payload(),
        },
        evidence=[
            {
                "source": "calendar",
                "title": "Buyer alignment meeting scheduled",
                "ts": datetime.now(timezone.utc),
                "trust_tier": "verified",
                "excerpt": "CFO + security call appears this week.",
            }
        ],
    )

    resp = await client.post(f"/today/deltas/{did}/apply", headers=_auth(token))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["triggered"]["resolution_thread_created"] is True
    thread = body["updatedDelta"]["resolutionThread"]
    assert thread["title"].startswith("Restore Acme Expansion")
    assert thread["sourceDecisionDeltaId"] == str(did)
    assert thread["steps"][0]["label"] == "Assign internal security owner"

    second = await client.post(f"/today/deltas/{did}/apply", headers=_auth(token))
    assert second.status_code == 200, second.text
    assert second.json()["triggered"]["resolution_thread_created"] is False

    listed = await client.get(
        f"/v1/resolution_threads/?source_decision_delta_id={did}",
        headers=_auth(token),
    )
    assert listed.status_code == 200, listed.text
    assert listed.json()["count"] == 1


@pytest.mark.asyncio
async def test_resolution_thread_evaluate_and_complete_step(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    did = await seed_decision_delta(
        gateway_pool,
        tenant=tenant_id,
        main_assertion="Move Acme Expansion forecast from Commit to Best Case",
        label="authority_required",
        confidence=0.78,
        falsification_condition="Security review is scheduled.",
        impact={"resolution_thread": _thread_payload()},
        evidence=[
            {
                "source": "calendar",
                "title": "Buyer alignment meeting scheduled",
                "ts": datetime.now(timezone.utc),
                "trust_tier": "verified",
                "excerpt": "CFO + security call appears this week.",
            }
        ],
    )
    applied = await client.post(f"/today/deltas/{did}/apply", headers=_auth(token))
    assert applied.status_code == 200, applied.text
    thread = applied.json()["updatedDelta"]["resolutionThread"]
    thread_id = thread["id"]
    step_id = thread["steps"][0]["id"]

    evaluated = await client.post(
        f"/v1/resolution_threads/{thread_id}/evaluate",
        headers=_auth(token),
    )
    assert evaluated.status_code == 200, evaluated.text
    evaluated_body = evaluated.json()
    assert evaluated_body["evaluation"]["signalsSeen"] == 1
    assert evaluated_body["thread"]["watchedSignals"][0]["status"] == "seen"

    completed = await client.patch(
        f"/v1/resolution_threads/{thread_id}/steps/{step_id}",
        headers=_auth(token),
        json={"status": "done"},
    )
    assert completed.status_code == 200, completed.text
    body = completed.json()
    assert body["thread"]["steps"][0]["status"] == "done"
    assert body["thread"]["status"] == "confirmed"


@pytest.mark.asyncio
async def test_resolution_threads_filter_hidden_target_resource(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, actor_id = valid_session
    visible = await _seed_resolution_thread(
        gateway_pool, tenant_id=tenant_id, created_by=actor_id,
    )
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    hidden = await _seed_resolution_thread(
        gateway_pool,
        tenant_id=tenant_id,
        created_by=actor_id,
        target_node_kind="resource",
        target_node_id=resource_id,
    )

    resp = await client.get("/v1/resolution_threads/", headers=_auth(token))

    assert resp.status_code == 200, resp.text
    ids = {item["id"] for item in resp.json()["items"]}
    assert str(visible.id) in ids
    assert str(hidden.id) not in ids


@pytest.mark.asyncio
async def test_resolution_thread_get_hidden_target_forbidden(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, actor_id = valid_session
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    hidden = await _seed_resolution_thread(
        gateway_pool,
        tenant_id=tenant_id,
        created_by=actor_id,
        target_node_kind="resource",
        target_node_id=resource_id,
    )

    resp = await client.get(
        f"/v1/resolution_threads/{hidden.id}",
        headers=_auth(token),
    )

    assert resp.status_code == 403
    assert resp.json()["detail"]["reason"] == "resource_out_of_scope:financial"


@pytest.mark.asyncio
async def test_resolution_thread_update_hidden_target_forbidden(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, actor_id = valid_session
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    hidden = await _seed_resolution_thread(
        gateway_pool,
        tenant_id=tenant_id,
        created_by=actor_id,
        target_node_kind="resource",
        target_node_id=resource_id,
    )

    resp = await client.patch(
        f"/v1/resolution_threads/{hidden.id}/status",
        headers=_auth(token),
        json={"status": "resolved"},
    )

    assert resp.status_code == 403
    stored_status = await gateway_pool.fetchval(
        "SELECT status FROM resolution_threads WHERE id = $1",
        hidden.id,
    )
    assert stored_status == "active"


@pytest.mark.asyncio
async def test_resolution_thread_create_hidden_target_forbidden(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    payload = {
        **_thread_payload(),
        "targetNodeKind": "resource",
        "targetNodeId": str(resource_id),
    }

    resp = await client.post(
        "/v1/resolution_threads/",
        headers=_auth(token),
        json=payload,
    )

    assert resp.status_code == 403
    assert resp.json()["detail"]["reason"] == "resource_out_of_scope:financial"


@pytest.mark.asyncio
async def test_resolution_thread_admin_override_is_audited(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, actor_id = valid_session
    resource_id = await seed_resource_for_target(gateway_pool, tenant=tenant_id)
    hidden = await _seed_resolution_thread(
        gateway_pool,
        tenant_id=tenant_id,
        created_by=actor_id,
        target_node_kind="resource",
        target_node_id=resource_id,
    )
    await grant_actor_role(
        gateway_pool,
        tenant=tenant_id,
        actor_id=actor_id,
        entity_type="tenant",
        entity_id=None,
        role="admin",
    )

    resp = await client.get(
        f"/v1/resolution_threads/{hidden.id}",
        headers=_auth(token),
    )

    assert resp.status_code == 200, resp.text
    audit_row = await gateway_pool.fetchrow(
        """
        SELECT override_kind, reason
        FROM access_override_log
        WHERE tenant_id = $1
          AND actor_id = $2
          AND entity_type = 'resource'
          AND entity_id = $3
        ORDER BY occurred_at DESC
        LIMIT 1
        """,
        tenant_id,
        actor_id,
        resource_id,
    )
    assert audit_row is not None
    assert audit_row["override_kind"] == "admin"
    assert audit_row["reason"] == "admin_override"
