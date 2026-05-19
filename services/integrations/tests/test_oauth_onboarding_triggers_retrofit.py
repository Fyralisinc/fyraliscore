"""X1 F4 OAuth retrofit — onboarding_triggers atomic-write tests.

Per A20: every OAuth callback (Gmail / Slack / GitHub / Discord) writes
an `onboarding_triggers` row in the SAME transaction as the install row
insert. Idempotent on retry via migration 0057's partial unique indexes
on `(tenant_id, source, installation_row_id)` (Slack/GitHub/Discord) and
`(tenant_id, source, gmail_installation_id)` (Gmail).

This file verifies the retrofit's load-bearing invariants:
  1. Each callback writes a trigger row on first install.
  2. Idempotent: a second callback for the same install produces zero
     new trigger rows.
  3. Atomic: if the trigger insert is forced to fail mid-transaction,
     the install row also rolls back.
  4. Regression-shaped: existing OAuth callback test suites
     (`test_oauth_callback*.py`, `test_oauth_github.py`,
     `test_oauth_*_discord.py`) still pass — verified by running them
     separately, not by this file.
"""
from __future__ import annotations

import json
from typing import Any
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


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Shared seeding helper.
# ---------------------------------------------------------------------
async def _seed_tenant(pool: asyncpg.Pool, slug: str = "x1") -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"{slug}-{tid.hex[:8]}",
    )
    return tid


async def _count_triggers(
    pool: asyncpg.Pool, *, tenant_id: UUID, source: str,
) -> int:
    return int(await pool.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = $2",
        tenant_id, source,
    ))


# =====================================================================
# Slack.
# =====================================================================
@pytest.fixture
def _slack_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLACK_CLIENT_ID", "1234.5678")
    monkeypatch.setenv("SLACK_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv(
        "SLACK_REDIRECT_URI",
        "https://app.fyralis.test/integrations/slack/callback",
    )
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "test-slack-signing-secret")
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key-x1-slack")
    from services.integrations.slack import metrics as slack_metrics
    slack_metrics.reset()


@pytest.fixture
def _mock_slack() -> object:
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
                "team": {"id": "T_X1_SLACK", "name": "TestWS"},
                "authed_user": {
                    "id": "U_INSTALLER", "scope": "",
                    "access_token": "xoxp-test-user-token",
                    "token_type": "user",
                },
            },
        )
        yield router


def _make_slack_app(pool: asyncpg.Pool, secret_store: Any) -> FastAPI:
    app = FastAPI()
    app.include_router(build_integrations_router())
    app.state.pool = pool
    app.state.secret_store = secret_store
    return app


async def test_slack_oauth_callback_writes_onboarding_trigger(
    fresh_db: asyncpg.Pool, _slack_env: None, _mock_slack: object,
) -> None:
    from services.integrations.slack import oauth as slack_oauth

    tenant = await _seed_tenant(fresh_db, "slack")
    secret_store = FernetSecretStore(
        fresh_db, master_kek=Fernet.generate_key(),
    )
    state = await slack_oauth.issue_state_token(tenant, fresh_db)
    app = _make_slack_app(fresh_db, secret_store)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.get(
            "/integrations/slack/callback",
            params={"code": "valid-code", "state": state},
            follow_redirects=False,
        )
    assert r.status_code == 302

    # Install row written.
    install = await fresh_db.fetchrow(
        "SELECT id, tenant_id FROM provider_installations "
        "WHERE provider='slack' AND installation_id='T_X1_SLACK'",
    )
    assert install is not None
    assert install["tenant_id"] == tenant

    # Trigger row written, references install via installation_row_id,
    # gmail_installation_id is NULL.
    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id, gmail_installation_id, "
        "payload::text AS payload "
        "FROM onboarding_triggers "
        "WHERE tenant_id=$1 AND source='slack'",
        tenant,
    )
    assert trig is not None
    assert trig["trigger_kind"] == "install"
    assert trig["installation_row_id"] == install["id"]
    assert trig["gmail_installation_id"] is None
    assert "team_id" in trig["payload"]


# =====================================================================
# GitHub.
# =====================================================================
@pytest.fixture
def _github_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization

    monkeypatch.setenv("GITHUB_APP_SLUG", "fyralis-x1-test")
    monkeypatch.setenv("GITHUB_APP_ID", "999999")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", pem.decode())
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key-x1-github")


@pytest.fixture
def _mock_github() -> object:
    with respx.mock(
        base_url="https://api.github.com", assert_all_called=False,
    ) as r:
        r.post("/app/installations/77777777/access_tokens").mock(
            return_value=httpx.Response(
                201,
                json={"token": "ghs_x1_test_token",
                      "expires_at": "2099-12-31T23:59:59Z"},
            ),
        )
        r.get(url__regex=r"/installation/repositories(\?.*)?").mock(
            return_value=httpx.Response(
                200, json={"total_count": 0,
                           "repository_selection": "all",
                           "repositories": []},
            ),
        )
        yield r


def _make_github_app(pool: asyncpg.Pool, tenant_id: UUID) -> FastAPI:
    from services.integrations.github.client import GithubClient

    app = FastAPI()
    app.state.pool = pool
    app.state.github_client = GithubClient(pool=pool)

    @app.middleware("http")
    async def _inject_auth(request, call_next):
        class _A: pass
        a = _A()
        a.tenant_id = tenant_id
        request.state.auth = a
        return await call_next(request)

    app.include_router(build_integrations_router())
    return app


def _github_state_from_location(location: str) -> str:
    from urllib.parse import urlparse, parse_qs
    return parse_qs(urlparse(location).query)["state"][0]


async def test_github_oauth_callback_writes_onboarding_trigger(
    fresh_db: asyncpg.Pool, _github_env: None, _mock_github: object,
) -> None:
    tenant = await _seed_tenant(fresh_db, "github")
    app = _make_github_app(fresh_db, tenant)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r1 = await c.get("/integrations/github/install")
        state = _github_state_from_location(r1.headers["Location"])
        r2 = await c.get(
            "/integrations/github/callback"
            f"?installation_id=77777777&setup_action=install&state={state}",
        )
    assert r2.status_code == 302

    install = await fresh_db.fetchrow(
        "SELECT id, tenant_id FROM provider_installations "
        "WHERE provider='github' AND installation_id='77777777'",
    )
    assert install is not None

    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id, gmail_installation_id "
        "FROM onboarding_triggers "
        "WHERE tenant_id=$1 AND source='github'",
        tenant,
    )
    assert trig is not None
    assert trig["trigger_kind"] == "install"
    assert trig["installation_row_id"] == install["id"]
    assert trig["gmail_installation_id"] is None


# =====================================================================
# Discord.
# =====================================================================
_DISCORD_PUBLIC_KEY_HEX = "a" * 64


@pytest.fixture
def _discord_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DISCORD_CLIENT_ID", "discord-client-id-x1")
    monkeypatch.setenv("DISCORD_CLIENT_SECRET", "discord-client-secret-x1")
    monkeypatch.setenv(
        "DISCORD_REDIRECT_URI",
        "https://app.fyralis.test/integrations/discord/callback",
    )
    monkeypatch.setenv("DISCORD_APPLICATION_ID", "A_X1_DISCORD_APP")
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", _DISCORD_PUBLIC_KEY_HEX)
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key-x1-discord")
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "x1-bot-token-app-level")
    from services.integrations.discord import metrics as discord_metrics
    discord_metrics.reset()


@pytest.fixture
def _mock_discord() -> object:
    with respx.mock(
        assert_all_called=False, base_url="https://discord.com",
    ) as router:
        router.post("/api/v10/oauth2/token").respond(
            200,
            json={
                "access_token": "discord-bot-token-x1",
                "token_type": "Bearer",
                "scope": "applications.commands bot",
                "guild": {"id": "G_X1_GUILD"},
                "application": {"id": "A_X1_DISCORD_APP"},
            },
        )
        router.post(
            "/api/v10/applications/A_X1_DISCORD_APP/commands",
        ).respond(
            200,
            json={"id": "CMD_X1", "application_id": "A_X1_DISCORD_APP",
                  "name": "fyralis"},
        )
        yield router


def _make_discord_app(pool: asyncpg.Pool, secret_store: Any) -> FastAPI:
    app = FastAPI()
    app.include_router(build_integrations_router())
    app.state.pool = pool
    app.state.secret_store = secret_store
    return app


async def test_discord_oauth_callback_writes_onboarding_trigger(
    fresh_db: asyncpg.Pool, _discord_env: None, _mock_discord: object,
) -> None:
    from services.integrations.discord import oauth as discord_oauth

    tenant = await _seed_tenant(fresh_db, "discord")
    secret_store = FernetSecretStore(
        fresh_db, master_kek=Fernet.generate_key(),
    )
    state = await discord_oauth.issue_state_token(tenant, fresh_db)
    app = _make_discord_app(fresh_db, secret_store)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.get(
            "/integrations/discord/callback",
            params={"code": "valid-code", "state": state},
            follow_redirects=False,
        )
    assert r.status_code == 302

    install = await fresh_db.fetchrow(
        "SELECT id, tenant_id FROM provider_installations "
        "WHERE provider='discord' AND installation_id='G_X1_GUILD'",
    )
    assert install is not None

    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id, gmail_installation_id "
        "FROM onboarding_triggers "
        "WHERE tenant_id=$1 AND source='discord'",
        tenant,
    )
    assert trig is not None
    assert trig["trigger_kind"] == "install"
    assert trig["installation_row_id"] == install["id"]
    assert trig["gmail_installation_id"] is None


# =====================================================================
# Gmail.
# =====================================================================
def _make_gmail_app(pool: asyncpg.Pool, tenant_id: UUID) -> FastAPI:
    """Build a FastAPI app wired to Gmail's POST /connect/finalize.

    The Gmail callback uses `tenant_transaction()` which calls
    `get_pool()` to acquire a connection. Tests monkeypatch
    `tenant_context.get_pool` to point at `fresh_db`. Auth is injected
    via middleware (same shape as the GitHub OAuth tests).

    Gmail's router isn't part of `build_integrations_router()`; it's
    mounted directly in `services/gateway/main.py`. The test imports
    the router module and includes it explicitly.
    """
    from services.integrations.gmail.oauth import (
        router as gmail_oauth_router,
    )

    app = FastAPI()

    @app.middleware("http")
    async def _inject_auth(request, call_next):
        class _A: pass
        a = _A()
        a.tenant_id = tenant_id
        request.state.auth = a
        return await call_next(request)

    app.include_router(gmail_oauth_router)
    return app


@pytest.fixture
def _gmail_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "GMAIL_SERVICE_ACCOUNT_CLIENT_ID", "test-sa-client-id",
    )
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key-x1-gmail")


async def test_gmail_oauth_callback_writes_onboarding_trigger(
    fresh_db: asyncpg.Pool, _gmail_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Gmail's connect_finalize uses tenant_transaction(tenant_id), which
    # internally calls get_pool(). Wire it for the test.
    from lib.shared import tenant_context
    monkeypatch.setattr(
        tenant_context, "get_pool", lambda: fresh_db,
    )
    # Patch dwd.get_minter to return a stub so we don't need a real
    # Google service-account JSON.
    from services.integrations.gmail import dwd
    class _StubMinter:
        service_account_email = "sa@test-project.iam.gserviceaccount.com"
    monkeypatch.setattr(dwd, "get_minter", lambda: _StubMinter())

    tenant = await _seed_tenant(fresh_db, "gmail")
    app = _make_gmail_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/gmail/connect/finalize",
            json={
                "workspace_domain": "test.example.com",
                "admin_email": "admin@test.example.com",
                "scope": "gmail.metadata",
                "inclusion_spec": {"mode": "all"},
            },
        )
    assert r.status_code == 200, r.text
    body = r.json()
    install_id = UUID(body["installation_id"])

    # Trigger written, references install via gmail_installation_id.
    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id, gmail_installation_id "
        "FROM onboarding_triggers "
        "WHERE tenant_id=$1 AND source='gmail'",
        tenant,
    )
    assert trig is not None
    assert trig["trigger_kind"] == "install"
    assert trig["installation_row_id"] is None
    assert trig["gmail_installation_id"] == install_id


# =====================================================================
# Atomic rollback — install + trigger fail together.
# =====================================================================
async def test_oauth_callback_atomic_rollback_includes_trigger(
    fresh_db: asyncpg.Pool, _slack_env: None, _mock_slack: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inject a failure into _emit_onboarding_trigger; assert the install
    row also rolls back. Verifies the X1.1 atomicity contract: no
    install-succeeded-but-no-trigger failure mode."""
    from services.integrations.slack import oauth as slack_oauth

    async def _explode(*args, **kwargs):
        raise RuntimeError("simulated trigger insert failure")

    monkeypatch.setattr(
        slack_oauth, "_emit_onboarding_trigger", _explode,
    )

    tenant = await _seed_tenant(fresh_db, "atomic")
    secret_store = FernetSecretStore(
        fresh_db, master_kek=Fernet.generate_key(),
    )
    state = await slack_oauth.issue_state_token(tenant, fresh_db)
    app = _make_slack_app(fresh_db, secret_store)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        # Callback will raise; the response shape depends on FastAPI's
        # error handling, but the load-bearing assertion is on DB state.
        try:
            await c.get(
                "/integrations/slack/callback",
                params={"code": "valid-code", "state": state},
                follow_redirects=False,
            )
        except Exception:
            pass

    # Neither install nor trigger should be present.
    install = await fresh_db.fetchrow(
        "SELECT id FROM provider_installations "
        "WHERE provider='slack' AND installation_id='T_X1_SLACK'",
    )
    assert install is None, (
        "Install row should have rolled back when trigger insert failed"
    )
    n_triggers = await _count_triggers(
        fresh_db, tenant_id=tenant, source="slack",
    )
    assert n_triggers == 0


# =====================================================================
# Idempotency — retry produces exactly one trigger.
# =====================================================================
async def test_oauth_callback_idempotent_on_retry_with_unique_constraint(
    fresh_db: asyncpg.Pool, _slack_env: None, _mock_slack: object,
) -> None:
    """Two callback invocations for the same install identity produce
    exactly one trigger row. Verifies the X1.2 idempotency contract
    (UNIQUE + ON CONFLICT DO NOTHING)."""
    from services.integrations.slack import oauth as slack_oauth

    tenant = await _seed_tenant(fresh_db, "idem")
    secret_store = FernetSecretStore(
        fresh_db, master_kek=Fernet.generate_key(),
    )

    app = _make_slack_app(fresh_db, secret_store)

    # First callback (install).
    state1 = await slack_oauth.issue_state_token(tenant, fresh_db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r1 = await c.get(
            "/integrations/slack/callback",
            params={"code": "valid-code", "state": state1},
            follow_redirects=False,
        )
    assert r1.status_code == 302

    # Second callback (reinstall — same team_id).
    state2 = await slack_oauth.issue_state_token(tenant, fresh_db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r2 = await c.get(
            "/integrations/slack/callback",
            params={"code": "valid-code", "state": state2},
            follow_redirects=False,
        )
    assert r2.status_code == 302

    # Exactly ONE trigger row across the two invocations.
    n_triggers = await _count_triggers(
        fresh_db, tenant_id=tenant, source="slack",
    )
    assert n_triggers == 1, (
        f"Expected exactly 1 trigger row after retry; got {n_triggers}"
    )
