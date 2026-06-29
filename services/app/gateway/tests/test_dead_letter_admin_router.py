from __future__ import annotations

import json
from uuid import UUID

import asyncpg
import httpx
import pytest

from lib.shared.ids import uuid7
from services.app.gateway.tests.test_map_routes import _seed_model


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _grant_admin(pool: asyncpg.Pool, *, tenant_id: UUID, actor_id: UUID) -> None:
    await pool.execute(
        """
        INSERT INTO actor_roles (
            tenant_id, actor_id, entity_type, entity_id, role,
            granted_by, granted_at, revoked_at
        ) VALUES ($1, $2, 'tenant', NULL, 'admin', $2, now(), NULL)
        ON CONFLICT ON CONSTRAINT actor_roles_dedup DO NOTHING
        """,
        tenant_id,
        actor_id,
    )


async def _seed_post_commit_dead_letter(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    error: str = "handler exploded",
) -> UUID:
    action_id = uuid7()
    await pool.execute(
        """
        INSERT INTO pending_post_commit_actions (
            id, tenant_id, trigger_id, action_kind, action_payload,
            attempts, last_error, dead_lettered_at
        ) VALUES (
            $1, $2, $3, 'publish_anomalies', $4::jsonb,
            5, $5, now()
        )
        """,
        action_id,
        tenant_id,
        uuid7(),
        json.dumps({"raw_customer_email": "alice@example.com"}),
        error,
    )
    return action_id


async def _seed_think_trigger_dead_letter(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    error: str = "validator failed",
) -> UUID:
    trigger_id = uuid7()
    await pool.execute(
        """
        INSERT INTO think_trigger_queue (
            id, tenant_id, trigger_kind, trigger_subkind, payload,
            attempts, completed_at, last_error
        ) VALUES (
            $1, $2, 'T4', 'model_reeval', '{}'::jsonb,
            5, now(), $3
        )
        """,
        trigger_id,
        tenant_id,
        error,
    )
    return trigger_id


async def _seed_model_reeval_dead_letter(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
) -> UUID:
    model_id = await _seed_model(
        pool,
        tenant_id,
        natural="A model needs re-evaluation after supporting evidence changed.",
    )
    dead_letter_id = uuid7()
    await pool.execute(
        """
        INSERT INTO model_reeval_dead_letter (
            id, tenant_id, original_queue_id, model_id, cause_model_id,
            cause_kind, attempts, last_error, enqueued_at
        ) VALUES (
            $1, $2, $3, $4, NULL,
            'supporting_archived', 5, 'reeval failed', now()
        )
        """,
        dead_letter_id,
        tenant_id,
        uuid7(),
        model_id,
    )
    return dead_letter_id


async def _operator_log_count(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    action: str,
) -> int:
    value = await pool.fetchval(
        """
        SELECT count(*)::int
        FROM operator_action_log
        WHERE tenant_id = $1 AND action = $2
        """,
        tenant_id,
        action,
    )
    return int(value or 0)


@pytest.mark.asyncio
async def test_dead_letter_list_requires_admin_and_omits_raw_payload(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, actor_id = valid_session
    action_id = await _seed_post_commit_dead_letter(
        gateway_pool,
        tenant_id=tenant_id,
        error="handler failed after reading alice@example.com",
    )

    non_admin = await client.get(
        "/api/admin/dead-letters?queue=post_commit",
        headers=_auth(token),
    )
    assert non_admin.status_code == 403

    await _grant_admin(gateway_pool, tenant_id=tenant_id, actor_id=actor_id)
    response = await client.get(
        "/api/admin/dead-letters?queue=post_commit",
        headers=_auth(token),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["id"] == str(action_id)
    assert body["items"][0]["queue"] == "post_commit"
    assert body["items"][0]["action_kind"] == "publish_anomalies"
    assert "action_payload" not in body["items"][0]
    assert "raw_customer_email" not in response.text
    assert await _operator_log_count(
        gateway_pool,
        tenant_id=tenant_id,
        action="dead_letter.list",
    ) == 1


@pytest.mark.asyncio
async def test_dead_letter_retry_requeues_post_commit_action_and_audits(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, actor_id = valid_session
    await _grant_admin(gateway_pool, tenant_id=tenant_id, actor_id=actor_id)
    action_id = await _seed_post_commit_dead_letter(gateway_pool, tenant_id=tenant_id)

    response = await client.post(
        f"/api/admin/dead-letters/post_commit/{action_id}/retry",
        headers=_auth(token),
        json={"reason": "handler fixed"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "retry_scheduled",
        "queue": "post_commit",
        "id": str(action_id),
    }
    row = await gateway_pool.fetchrow(
        """
        SELECT attempts, dead_lettered_at, last_error, quarantined_at
        FROM pending_post_commit_actions
        WHERE id = $1
        """,
        action_id,
    )
    assert row is not None
    assert row["attempts"] == 0
    assert row["dead_lettered_at"] is None
    assert row["last_error"] is None
    assert row["quarantined_at"] is None
    assert await _operator_log_count(
        gateway_pool,
        tenant_id=tenant_id,
        action="dead_letter.retry",
    ) == 1


@pytest.mark.asyncio
async def test_dead_letter_quarantine_hides_trigger_by_default_and_audits(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, actor_id = valid_session
    await _grant_admin(gateway_pool, tenant_id=tenant_id, actor_id=actor_id)
    trigger_id = await _seed_think_trigger_dead_letter(
        gateway_pool,
        tenant_id=tenant_id,
    )

    response = await client.post(
        f"/api/admin/dead-letters/think_trigger/{trigger_id}/quarantine",
        headers=_auth(token),
        json={"reason": "non-retryable bad payload"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "quarantined",
        "queue": "think_trigger",
        "id": str(trigger_id),
    }
    default_list = await client.get(
        "/api/admin/dead-letters?queue=think_trigger",
        headers=_auth(token),
    )
    assert default_list.status_code == 200
    assert default_list.json()["items"] == []

    with_quarantined = await client.get(
        "/api/admin/dead-letters?queue=think_trigger&include_quarantined=true",
        headers=_auth(token),
    )
    assert with_quarantined.status_code == 200
    item = with_quarantined.json()["items"][0]
    assert item["id"] == str(trigger_id)
    assert item["state"] == "quarantined"
    assert item["quarantine_reason"] == "non-retryable bad payload"
    assert await _operator_log_count(
        gateway_pool,
        tenant_id=tenant_id,
        action="dead_letter.quarantine",
    ) == 1


@pytest.mark.asyncio
async def test_dead_letter_retry_model_reeval_creates_new_queue_row(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, actor_id = valid_session
    await _grant_admin(gateway_pool, tenant_id=tenant_id, actor_id=actor_id)
    dead_letter_id = await _seed_model_reeval_dead_letter(
        gateway_pool,
        tenant_id=tenant_id,
    )

    response = await client.post(
        f"/api/admin/dead-letters/model_reeval/{dead_letter_id}/retry",
        headers=_auth(token),
        json={"reason": "model prompt fixed"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "retry_scheduled"
    assert body["queue"] == "model_reeval"
    retry_queue_id = UUID(body["retry_queue_id"])

    queued = await gateway_pool.fetchrow(
        """
        SELECT attempts, processed_at, last_error
        FROM model_reeval_queue
        WHERE id = $1 AND tenant_id = $2
        """,
        retry_queue_id,
        tenant_id,
    )
    assert queued is not None
    assert queued["attempts"] == 0
    assert queued["processed_at"] is None
    assert queued["last_error"] is None

    dead_letter = await gateway_pool.fetchrow(
        """
        SELECT retried_at, retried_by, retry_queue_id
        FROM model_reeval_dead_letter
        WHERE id = $1
        """,
        dead_letter_id,
    )
    assert dead_letter is not None
    assert dead_letter["retried_at"] is not None
    assert dead_letter["retried_by"] == actor_id
    assert dead_letter["retry_queue_id"] == retry_queue_id
    assert await _operator_log_count(
        gateway_pool,
        tenant_id=tenant_id,
        action="dead_letter.retry",
    ) == 1
