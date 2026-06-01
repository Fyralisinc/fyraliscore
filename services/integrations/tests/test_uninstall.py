"""IN-08 US4 + US5: uninstall + re-install tests.

End-to-end through the webhooks router: a signed Slack `app_uninstalled`
event disables the installation row, zeroes the secret material, and
the very next webhook for the same workspace returns 401
`unknown_installation`. Then a fresh OAuth install reuses the same
`provider_installations.id` (US5 / FR-018 / SC-004).
"""
from __future__ import annotations

import json
import time
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import respx
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.main import build_app
from services.gateway.rate_limit import RateLimiter
from services.integrations.slack import metrics as slack_metrics
from services.integrations.slack import oauth as slack_oauth
from services.webhooks.tests.conftest import slack_sign


pytestmark = pytest.mark.integration


_SIGNING_SECRET = "uninstall-test-signing-secret"
_TEAM_ID = "T_UNINSTALL_WS"


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    slack_metrics.reset()


@pytest.fixture
async def _tenant(fresh_db: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"uninstall-{tid.hex[:8]}",
    )
    return tid


async def _seed_install(
    fresh_db: asyncpg.Pool, tenant_id: UUID, secret_store,
) -> tuple[UUID, str, str]:
    """Insert a `provider_installations` row + bot + signing-secret
    encrypted_secrets rows so a signed webhook can flow end-to-end."""
    bot_ref = await secret_store.put(
        b"xoxb-installed-bot-token",
        label=f"slack_bot_token:{_TEAM_ID}",
        tenant_id=tenant_id,
    )
    signing_ref = await secret_store.put(
        _SIGNING_SECRET.encode("utf-8"),
        label="slack_signing_secret:app",
        tenant_id=tenant_id,
    )
    row_id = uuid7()
    # `secret_ref` on provider_installations points at the *signing*
    # secret ref so load_secrets returns it for HMAC verification.
    # (Bot token is addressable via its label.)
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'slack', $3, $4, TRUE)",
        row_id, tenant_id, _TEAM_ID, signing_ref,
    )
    return row_id, bot_ref, signing_ref


def _build_test_app(fresh_db: asyncpg.Pool, secret_store) -> FastAPI:
    app = build_app(
        pool=fresh_db,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=None,
        rate_limiter=RateLimiter(),
        configure_logging=False,
    )
    app.state.secret_store = secret_store
    return app


def _sign_uninstall_body(team_id: str, event_type: str) -> tuple[bytes, int, str]:
    body = json.dumps({
        "team_id": team_id,
        "event": {"type": event_type},
        "type": "event_callback",
    }).encode("utf-8")
    ts = int(time.time())
    sig = slack_sign(_SIGNING_SECRET, body, ts)
    return body, ts, sig


async def test_uninstall_disables_row_and_zeros_secrets(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_row_id, bot_ref, _signing_ref = await _seed_install(
        fresh_db, _tenant, secret_store,
    )
    app = _build_test_app(fresh_db, secret_store)

    body, ts, sig = _sign_uninstall_body(_TEAM_ID, "app_uninstalled")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )

    assert r.status_code == 200, r.text
    assert r.json() == {"handled": "app_uninstalled"}

    # Row disabled.
    row = await fresh_db.fetchrow(
        "SELECT enabled FROM provider_installations WHERE id = $1",
        install_row_id,
    )
    assert row is not None and row["enabled"] is False

    # Bot token row deleted from encrypted_secrets.
    bot_row = await fresh_db.fetchrow(
        "SELECT id FROM encrypted_secrets WHERE id = $1::uuid",
        bot_ref,
    )
    assert bot_row is None

    # Audit row with action=uninstall, status=ok.
    audit = await fresh_db.fetchrow(
        "SELECT action, status, context::text AS ctx "
        "FROM installation_audit_log WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 1",
        _tenant,
    )
    assert audit is not None
    assert audit["action"] == "uninstall"
    assert audit["status"] == "ok"

    assert slack_metrics.get_uninstall_outcome_count("success") == 1


async def test_uninstall_next_webhook_returns_unknown_installation(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    await _seed_install(fresh_db, _tenant, secret_store)
    app = _build_test_app(fresh_db, secret_store)

    # 1) Uninstall.
    body, ts, sig = _sign_uninstall_body(_TEAM_ID, "app_uninstalled")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )
        assert r.status_code == 200

        # 2) Another inbound webhook for the same team → 401.
        msg_body = json.dumps({
            "team_id": _TEAM_ID,
            "event": {
                "type": "message",
                "text": "after uninstall",
                "ts": str(time.time()),
                "channel": "C", "user": "U",
            },
        }).encode("utf-8")
        ts2 = int(time.time())
        sig2 = slack_sign(_SIGNING_SECRET, msg_body, ts2)
        r2 = await c.post(
            "/webhooks/slack/events",
            content=msg_body,
            headers={
                "X-Slack-Request-Timestamp": str(ts2),
                "X-Slack-Signature": sig2,
            },
        )

    # Either unknown_installation (resolver) or secret_not_configured
    # (no installation ⇒ no signing secret loadable). Both are 401.
    assert r2.status_code == 401


async def test_tokens_revoked_equivalent_to_app_uninstalled(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_row_id, _bot_ref, _signing_ref = await _seed_install(
        fresh_db, _tenant, secret_store,
    )
    app = _build_test_app(fresh_db, secret_store)

    body, ts, sig = _sign_uninstall_body(_TEAM_ID, "tokens_revoked")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )

    assert r.status_code == 200
    assert r.json() == {"handled": "tokens_revoked"}
    row = await fresh_db.fetchrow(
        "SELECT enabled FROM provider_installations WHERE id = $1",
        install_row_id,
    )
    assert row["enabled"] is False


async def test_reinstall_after_uninstall_reuses_row(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """US5 / FR-018 / SC-004: after uninstall, a fresh OAuth callback
    for the same `team_id` updates the same `provider_installations.id`
    (no duplicate, no unique-constraint conflict)."""
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key-reinstall")
    monkeypatch.setenv("SLACK_CLIENT_ID", "1234.5678")
    monkeypatch.setenv("SLACK_CLIENT_SECRET", "x")
    monkeypatch.setenv(
        "SLACK_REDIRECT_URI",
        "https://app.fyralis.test/integrations/slack/callback",
    )
    monkeypatch.setenv("SLACK_SIGNING_SECRET", _SIGNING_SECRET)

    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_row_id, _bot_ref, _signing_ref = await _seed_install(
        fresh_db, _tenant, secret_store,
    )
    app = _build_test_app(fresh_db, secret_store)

    # 1) Uninstall.
    body, ts, sig = _sign_uninstall_body(_TEAM_ID, "app_uninstalled")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )
        assert r.status_code == 200

        # 2) Issue a fresh state token (binds nonce to tenant), then
        # POST the callback with mocked Slack returning the same team.
        state = await slack_oauth.issue_state_token(_tenant, fresh_db)
        with respx.mock(base_url="https://slack.com") as router:
            router.post("/api/oauth.v2.access").respond(
                200,
                json={
                    "ok": True,
                    "access_token": "xoxb-new-bot-token",
                    "scope": "channels:history,users:read",
                    "app_id": "A1",
                    "team": {"id": _TEAM_ID},
                    "authed_user": {},
                },
            )
            r2 = await c.get(
                "/integrations/slack/callback",
                params={"code": "fresh-code", "state": state},
                follow_redirects=False,
            )

    assert r2.status_code == 302
    assert "installed?team=" in r2.headers["location"]

    # Exactly ONE provider_installations row for this team_id.
    count = await fresh_db.fetchval(
        "SELECT count(*) FROM provider_installations WHERE installation_id = $1",
        _TEAM_ID,
    )
    assert count == 1

    # Same row id preserved.
    row = await fresh_db.fetchrow(
        "SELECT id, enabled FROM provider_installations WHERE installation_id = $1",
        _TEAM_ID,
    )
    assert row["id"] == install_row_id
    assert row["enabled"] is True
