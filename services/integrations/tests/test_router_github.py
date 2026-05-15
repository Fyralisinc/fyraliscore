"""IN-13 end-to-end router tests for GitHub webhook deliveries.

Covers T028–T034 (US1 ingest happy path), T032a (FR-022 ping bootstrap),
T051–T055 (US3 cross-tenant routing), T060–T067 (US4 uninstall via
lifecycle), T070–T076 (US5 repo filter), T084–T085 (US6 replay drop).

Requires live Postgres (Constitution §IV). The full gateway app is
built so signature verification, tenant resolution, replay cache, repo
filter, lifecycle dispatch, and ingestion are all exercised.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from uuid import UUID

import pytest

from lib.shared.ids import uuid7


pytestmark = pytest.mark.integration


_APP_SECRET = "test-app-level-webhook-secret-IN-13"


def _sign(body: bytes, secret: str = _APP_SECRET) -> str:
    return "sha256=" + hmac.new(
        secret.encode(), body, hashlib.sha256,
    ).hexdigest()


@pytest.fixture(autouse=True)
def _wire_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEBHOOK_SECRET_GITHUB", _APP_SECRET)


@pytest.fixture
async def seeded_installation(db_pool):
    """Seed a tenant + enabled GitHub installation row mapped to it."""
    tenant_id = uuid7()
    await db_pool.execute(
        """
        INSERT INTO tenants (id, slug, display_name)
        VALUES ($1, $2, 'Test') ON CONFLICT (id) DO NOTHING
        """,
        tenant_id,
        f"t-{tenant_id.hex[:8]}",
    )
    installation_row_id = uuid7()
    await db_pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref,
             enabled, selected_repositories)
        VALUES ($1, $2, 'github', '12345678', NULL, TRUE, $3::jsonb)
        """,
        installation_row_id,
        tenant_id,
        json.dumps(["octo/repo-a", "octo/repo-b"]),
    )
    return {
        "tenant_id": tenant_id,
        "installation_row_id": installation_row_id,
        "installation_id": "12345678",
    }


# ---------------------------------------------------------------------
# US1: verified ingest happy path (T028)
# ---------------------------------------------------------------------


async def test_verified_pull_request_lands_as_observation(
    gateway_client, db_pool, seeded_installation,
) -> None:
    body = json.dumps({
        "action": "opened",
        "pull_request": {
            "id": 1,
            "node_id": "PR_test_node_id_1",
            "number": 42,
            "title": "Add rate limiter",
            "base": {"ref": "main"},
            "updated_at": "2026-05-15T10:00:00Z",
        },
        "installation": {"id": 12345678},
        "repository": {"full_name": "octo/repo-a"},
        "sender": {"login": "alice"},
    }).encode()

    response = await gateway_client.post(
        "/webhooks/github/events",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": _sign(body),
            "X-GitHub-Event": "pull_request",
            "X-GitHub-Delivery": "11111111-2222-3333-4444-555555555555",
        },
    )
    assert response.status_code in (200, 201)
    obs = await db_pool.fetchrow(
        """
        SELECT tenant_id, source_channel, external_id
          FROM observations
         WHERE source_channel = 'github:webhook'
           AND external_id = 'PR_test_node_id_1'
        """,
    )
    assert obs is not None
    assert obs["tenant_id"] == seeded_installation["tenant_id"]


# ---------------------------------------------------------------------
# US1: FR-022 ping bootstrap (T032a)
# ---------------------------------------------------------------------


async def test_ping_bootstrap_no_installation(gateway_client) -> None:
    body = json.dumps({"zen": "Half measures are as bad as nothing at all."}).encode()
    response = await gateway_client.post(
        "/webhooks/github/events",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": _sign(body),
            "X-GitHub-Event": "ping",
            "X-GitHub-Delivery": "ping-bootstrap-uuid",
        },
    )
    assert response.status_code == 200
    assert response.json().get("handled") == "ping"


# ---------------------------------------------------------------------
# US3: signature failure first (T052)
# ---------------------------------------------------------------------


async def test_forged_signature_returns_401(
    gateway_client, seeded_installation,
) -> None:
    body = json.dumps({
        "action": "opened",
        "installation": {"id": 12345678},
        "repository": {"full_name": "octo/repo-a"},
    }).encode()
    response = await gateway_client.post(
        "/webhooks/github/events",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": _sign(body, secret="wrong-secret"),
            "X-GitHub-Event": "pull_request",
            "X-GitHub-Delivery": "forged-uuid",
        },
    )
    assert response.status_code == 401


# ---------------------------------------------------------------------
# US3: unknown installation collapses with disabled (T053-T054)
# ---------------------------------------------------------------------


async def test_unknown_installation_returns_401(gateway_client) -> None:
    body = json.dumps({
        "action": "opened",
        "installation": {"id": 99999999},
        "repository": {"full_name": "octo/repo-x"},
    }).encode()
    response = await gateway_client.post(
        "/webhooks/github/events",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": _sign(body),
            "X-GitHub-Event": "pull_request",
            "X-GitHub-Delivery": "unknown-uuid",
        },
    )
    assert response.status_code == 401


# ---------------------------------------------------------------------
# US5: repo filter drops unlisted (T074)
# ---------------------------------------------------------------------


async def test_repo_filter_drops_unlisted(
    gateway_client, db_pool, seeded_installation,
) -> None:
    body = json.dumps({
        "action": "opened",
        "pull_request": {
            "id": 2, "node_id": "PR_other_node",
            "number": 1, "title": "Out of scope",
            "base": {"ref": "main"},
        },
        "installation": {"id": 12345678},
        "repository": {"full_name": "octo/repo-c"},  # NOT in selection
        "sender": {"login": "bob"},
    }).encode()
    response = await gateway_client.post(
        "/webhooks/github/events",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": _sign(body),
            "X-GitHub-Event": "pull_request",
            "X-GitHub-Delivery": "repo-filter-uuid",
        },
    )
    assert response.status_code == 200
    assert response.json().get("handled") == "filtered_repo"
    obs = await db_pool.fetchrow(
        "SELECT 1 FROM observations WHERE external_id = 'PR_other_node'",
    )
    assert obs is None


# ---------------------------------------------------------------------
# US6: replay drop within TTL (T084)
# ---------------------------------------------------------------------


async def test_replay_short_circuit_within_5_min(
    gateway_client, db_pool, seeded_installation,
) -> None:
    body = json.dumps({
        "action": "opened",
        "pull_request": {
            "id": 3, "node_id": "PR_replay_node",
            "number": 7, "title": "Once",
            "base": {"ref": "main"},
        },
        "installation": {"id": 12345678},
        "repository": {"full_name": "octo/repo-a"},
        "sender": {"login": "carol"},
    }).encode()
    headers = {
        "Content-Type": "application/json",
        "X-Hub-Signature-256": _sign(body),
        "X-GitHub-Event": "pull_request",
        "X-GitHub-Delivery": "replay-stable-uuid",
    }

    resp1 = await gateway_client.post(
        "/webhooks/github/events", content=body, headers=headers,
    )
    assert resp1.status_code in (200, 201)

    resp2 = await gateway_client.post(
        "/webhooks/github/events", content=body, headers=headers,
    )
    assert resp2.status_code == 200
    assert resp2.json().get("handled") == "replay"


# ---------------------------------------------------------------------
# US4: lifecycle installation.deleted disables row (T038)
# ---------------------------------------------------------------------


async def test_installation_deleted_disables_row(
    gateway_client, db_pool, seeded_installation,
) -> None:
    body = json.dumps({
        "action": "deleted",
        "installation": {"id": 12345678, "account": {"login": "octo"}},
    }).encode()
    response = await gateway_client.post(
        "/webhooks/github/events",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": _sign(body),
            "X-GitHub-Event": "installation",
            "X-GitHub-Delivery": "uninstall-uuid",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body.get("handled") == "installation_deleted"
    row = await db_pool.fetchrow(
        "SELECT enabled FROM provider_installations "
        "WHERE id = $1",
        seeded_installation["installation_row_id"],
    )
    assert row is not None
    assert row["enabled"] is False
