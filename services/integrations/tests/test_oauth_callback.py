"""IN-08 US3: callback-handler tests.

GET /integrations/slack/callback covers:
  - state_invalid (HMAC mismatch)
  - state_expired (nonce past expires_at)
  - state_consumed (replayed nonce)
  - slack_oauth_error (Slack returns ok=false)
  - installation_collision (cross-tenant rebind attempt)
  - secret_store_unavailable
  - success (fresh install)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import respx
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.integrations.router import build_integrations_router
from services.integrations.slack import metrics as slack_metrics
from services.integrations.slack import oauth as slack_oauth


pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _set_oauth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_CLIENT_ID", "1234.5678")
    monkeypatch.setenv("SLACK_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv(
        "SLACK_REDIRECT_URI", "https://app.fyralis.test/integrations/slack/callback",
    )
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "test-slack-signing-secret")
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key-callback")
    slack_metrics.reset()


@pytest.fixture(autouse=True)
def _mock_slack_oauth_endpoint() -> object:
    """Default mock: Slack returns a happy-path token bundle. Tests
    that need a different response override via `respx.post(...).mock(...)`
    on the same router."""
    with respx.mock(
        assert_all_called=False, base_url="https://slack.com",
    ) as router:
        router.post("/api/oauth.v2.access").respond(
            200,
            json={
                "ok": True,
                "access_token": "xoxb-test-bot-token",
                "token_type": "bot",
                "scope": "channels:history,users:read,team:read",
                "app_id": "A_TEST_APP",
                "team": {"id": "T_TEST_WS", "name": "TestWS"},
                "authed_user": {
                    "id": "U_INSTALLER",
                    "scope": "",
                    "access_token": "xoxp-test-user-token",
                    "token_type": "user",
                },
            },
        )
        yield router


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"cb-{tid.hex[:8]}",
    )
    return tid


def _make_app(pool: asyncpg.Pool, secret_store) -> FastAPI:
    app = FastAPI()
    app.include_router(build_integrations_router())
    app.state.pool = pool
    app.state.secret_store = secret_store
    return app


async def test_callback_state_invalid_hmac(fresh_db: asyncpg.Pool) -> None:
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/slack/callback",
            params={"code": "anycode", "state": "notbase64.alsonotbase64"},
            follow_redirects=False,
        )
    assert r.status_code == 302
    assert "reason=state_invalid" in r.headers["location"]


async def test_callback_state_expired(fresh_db: asyncpg.Pool) -> None:
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    # Issue a state token, then manually expire its nonce row.
    state = await slack_oauth.issue_state_token(tenant, fresh_db)
    await fresh_db.execute(
        "UPDATE oauth_install_states SET expires_at = $1",
        datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/slack/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )
    assert "reason=state_expired" in r.headers["location"]


async def test_callback_state_consumed_replay(fresh_db: asyncpg.Pool) -> None:
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await slack_oauth.issue_state_token(tenant, fresh_db)
    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.get(
            "/integrations/slack/callback",
            params={"code": "code-A", "state": state},
            follow_redirects=False,
        )
        # First call succeeds (302 to success).
        assert r1.headers["location"].startswith("/integrations/slack/installed")
        # Second call: nonce already consumed.
        r2 = await c.get(
            "/integrations/slack/callback",
            params={"code": "code-B", "state": state},
            follow_redirects=False,
        )
    assert "reason=state_consumed" in r2.headers["location"]


async def test_callback_unknown_nonce_is_state_invalid(
    fresh_db: asyncpg.Pool,
) -> None:
    """A correctly-signed state token whose nonce was never issued is
    treated as `state_invalid` (could be forged by an attacker who
    somehow knows the HMAC key, but the DB ledger is authoritative)."""
    import base64
    import hashlib
    import hmac
    import json

    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    # Forge a perfectly-signed state token for a nonce that doesn't exist.
    payload = {
        "tenant_id": str(uuid4()),
        "nonce": "never-issued-nonce-xyz",
        "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
    }
    payload_b64 = (
        base64.urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":")).encode("utf-8")
        )
        .rstrip(b"=")
        .decode("ascii")
    )
    sig = hmac.new(
        b"test-hmac-key-callback", payload_b64.encode("ascii"), hashlib.sha256,
    ).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")
    state = f"{payload_b64}.{sig_b64}"

    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/slack/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )
    assert "reason=state_invalid" in r.headers["location"]


async def test_callback_success_fresh_install(fresh_db: asyncpg.Pool) -> None:
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await slack_oauth.issue_state_token(tenant, fresh_db)
    app = _make_app(fresh_db, secret_store)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/slack/callback",
            params={"code": "valid-code", "state": state},
            follow_redirects=False,
        )

    assert r.status_code == 302
    loc = r.headers["location"]
    assert loc.startswith("/integrations/slack/installed?team=")
    # `?team=` is a short hash, NOT the raw team_id "T_TEST_WS".
    assert "T_TEST_WS" not in loc

    # provider_installations row exists, enabled, secret_ref populated.
    row = await fresh_db.fetchrow(
        "SELECT tenant_id, secret_ref, enabled "
        "FROM provider_installations WHERE installation_id = $1",
        "T_TEST_WS",
    )
    assert row is not None
    assert row["tenant_id"] == tenant
    assert row["enabled"] is True
    assert row["secret_ref"] is not None

    # installation_audit_log carries install/ok.
    audit = await fresh_db.fetchrow(
        "SELECT action, status, context::text AS ctx "
        "FROM installation_audit_log WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 1",
        tenant,
    )
    assert audit is not None
    assert audit["action"] == "install"
    assert audit["status"] == "ok"

    # encrypted_secrets has the bot + user tokens (signing secret too).
    refs = await fresh_db.fetch(
        "SELECT label FROM encrypted_secrets WHERE tenant_id = $1",
        tenant,
    )
    labels = {r["label"] for r in refs}
    assert "slack_bot_token:T_TEST_WS" in labels

    # The success metric fired.
    assert slack_metrics.get_install_outcome_count("success") == 1


async def test_callback_slack_oauth_error(
    fresh_db: asyncpg.Pool, _mock_slack_oauth_endpoint: object,
) -> None:
    """Slack returns `ok=false` → 302 with reason=slack_oauth_error."""
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await slack_oauth.issue_state_token(tenant, fresh_db)

    # Override the default mock with a Slack error response.
    _mock_slack_oauth_endpoint.post("/api/oauth.v2.access").mock(
        return_value=httpx.Response(
            200, json={"ok": False, "error": "invalid_code"},
        )
    )

    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/slack/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )

    assert "reason=slack_oauth_error" in r.headers["location"]
    # No provider_installations row was created.
    count = await fresh_db.fetchval(
        "SELECT count(*) FROM provider_installations WHERE tenant_id = $1",
        tenant,
    )
    assert count == 0
    # Audit row with status=error.
    audit = await fresh_db.fetchrow(
        "SELECT status FROM installation_audit_log WHERE tenant_id = $1",
        tenant,
    )
    assert audit is not None
    assert audit["status"] == "error"


async def test_callback_installation_collision(fresh_db: asyncpg.Pool) -> None:
    """An installation already bound to tenant A; tenant B attempts to
    install for the same team_id → 302 reason=installation_collision."""
    tenant_a = await _seed_tenant(fresh_db)
    tenant_b = await _seed_tenant(fresh_db)
    # Pre-seed: T_TEST_WS bound to tenant_a.
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'slack', 'T_TEST_WS', $3, TRUE)",
        uuid7(), tenant_a, "stale-ref-tenant-a",
    )
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    # Tenant B issues an install for the same team via OAuth.
    state = await slack_oauth.issue_state_token(tenant_b, fresh_db)

    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/slack/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )

    assert "reason=installation_collision" in r.headers["location"]
    # Tenant A's row is unchanged (still bound to tenant_a).
    row = await fresh_db.fetchrow(
        "SELECT tenant_id FROM provider_installations WHERE installation_id = 'T_TEST_WS'",
    )
    assert row is not None and row["tenant_id"] == tenant_a
    # Audit row with status=rejected_collision.
    audit = await fresh_db.fetchrow(
        "SELECT status FROM installation_audit_log "
        "WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 1",
        tenant_b,
    )
    assert audit is not None
    assert audit["status"] == "rejected_collision"


async def test_callback_no_team_id_in_logs(
    fresh_db: asyncpg.Pool,
    caplog: pytest.LogCaptureFixture,
    _mock_slack_oauth_endpoint: object,
) -> None:
    """SC-007 + clarification Q3: the response and structured logs
    MUST NOT carry the conflicting `team_id` or the foreign tenant.
    """
    tenant_a = await _seed_tenant(fresh_db)
    tenant_b = await _seed_tenant(fresh_db)
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'slack', 'T_SECRET_WS', $3, TRUE)",
        uuid7(), tenant_a, "ref",
    )
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await slack_oauth.issue_state_token(tenant_b, fresh_db)

    # Override the autouse mock to return T_SECRET_WS as the team.
    _mock_slack_oauth_endpoint.post("/api/oauth.v2.access").mock(
        return_value=httpx.Response(
            200,
            json={
                "ok": True,
                "access_token": "xoxb-fake",
                "team": {"id": "T_SECRET_WS"},
                "scope": "users:read",
                "app_id": "A",
                "authed_user": {},
            },
        )
    )

    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        with caplog.at_level("INFO"):
            r = await c.get(
                "/integrations/slack/callback",
                params={"code": "x", "state": state},
                follow_redirects=False,
            )

    assert "reason=installation_collision" in r.headers["location"]
    # The redirect URL itself contains no team_id substring.
    assert "T_SECRET_WS" not in r.headers["location"]
    # The captured structured logs do not carry the team_id either.
    for record in caplog.records:
        assert "T_SECRET_WS" not in record.getMessage()


async def test_callback_secret_store_unavailable(
    fresh_db: asyncpg.Pool,
) -> None:
    """If the secret store raises, the callback returns 302 with
    reason=secret_store_unavailable."""
    tenant = await _seed_tenant(fresh_db)

    class _BrokenStore:
        async def put(self, *_, **__):
            from lib.shared.errors import SecretStoreError
            raise SecretStoreError("simulated", reason="db_down")

        async def get(self, *_, **__): pass
        async def delete(self, *_, **__): pass

    state = await slack_oauth.issue_state_token(tenant, fresh_db)
    app = _make_app(fresh_db, _BrokenStore())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/slack/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )
    assert "reason=secret_store_unavailable" in r.headers["location"]
