"""IN-14: Notion OAuth install + callback tests (real DB + respx).

Covers:
  - install: 302 to api.notion.com/v1/oauth/authorize with tenant-bound state
  - install: unconfigured → 500 notion_unconfigured
  - callback: first install persists bot token + provider_installations row +
    onboarding_trigger + audit, then 302 to installed
  - callback: cross-tenant collision → install-error
  - callback: state_invalid HMAC
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import respx
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.ingest.integrations.notion import metrics as notion_metrics
from services.ingest.integrations.notion import oauth as notion_oauth
from services.ingest.integrations.router import build_integrations_router


pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTION_CLIENT_ID", "notion-client-id")
    monkeypatch.setenv("NOTION_CLIENT_SECRET", "notion-client-secret")
    monkeypatch.setenv(
        "NOTION_REDIRECT_URI",
        "https://app.fyralis.test/integrations/notion/callback",
    )
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "notion-test-hmac-key")
    notion_metrics.reset()


class _Auth:
    def __init__(self, tenant_id: UUID) -> None:
        self.tenant_id = tenant_id
        self.actor_id = uuid4()


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)", tid, f"nt-{tid.hex[:8]}",
    )
    return tid


def _make_app(pool, secret_store=None, auth: _Auth | None = None) -> FastAPI:
    app = FastAPI()
    app.include_router(build_integrations_router())
    app.state.pool = pool
    if secret_store is not None:
        app.state.secret_store = secret_store

    @app.middleware("http")
    async def _inject_auth(request, call_next):
        if auth is not None:
            request.state.auth = auth
        return await call_next(request)

    return app


async def test_install_redirects_to_notion(fresh_db: asyncpg.Pool) -> None:
    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, auth=_Auth(tenant))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/integrations/notion/install", follow_redirects=False)
    assert r.status_code == 302
    loc = r.headers["location"]
    assert loc.startswith("https://api.notion.com/v1/oauth/authorize")
    assert "client_id=notion-client-id" in loc
    assert "state=" in loc
    row = await fresh_db.fetchrow(
        "SELECT provider, consumed_at FROM oauth_install_states WHERE tenant_id = $1",
        tenant,
    )
    assert row is not None and row["provider"] == "notion" and row["consumed_at"] is None


async def test_install_unconfigured_returns_500(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NOTION_CLIENT_ID", raising=False)
    tenant = await _seed_tenant(fresh_db)
    app = _make_app(fresh_db, auth=_Auth(tenant))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get("/integrations/notion/install", follow_redirects=False)
    assert r.status_code == 500
    assert r.json()["code"] == "notion_unconfigured"


def _mock_token_endpoint(workspace_id: str = "ws-acme"):
    router = respx.mock(assert_all_called=False, base_url="https://api.notion.com")
    router.post("/v1/oauth/token").respond(
        200,
        json={
            "access_token": "secret_notion_bot_token",
            "workspace_id": workspace_id,
            "workspace_name": "Acme",
            "bot_id": "bot-1",
        },
    )
    return router


async def _issue_state(tenant: UUID, pool: asyncpg.Pool) -> str:
    return await notion_oauth.issue_state_token(tenant, pool, provider="notion")


async def test_callback_first_install_persists_everything(fresh_db: asyncpg.Pool) -> None:
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await _issue_state(tenant, fresh_db)
    app = _make_app(fresh_db, secret_store=secret_store)
    transport = httpx.ASGITransport(app=app)
    with _mock_token_endpoint("ws-acme"):
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.get(
                "/integrations/notion/callback",
                params={"code": "auth-code", "state": state},
                follow_redirects=False,
            )
    assert r.status_code == 302
    assert "/integrations/notion/installed" in r.headers["location"]

    inst = await fresh_db.fetchrow(
        "SELECT tenant_id, secret_ref, enabled FROM provider_installations "
        "WHERE provider = 'notion' AND installation_id = $1",
        "ws-acme",
    )
    assert inst is not None and inst["tenant_id"] == tenant and inst["enabled"] is True
    # the secret_ref resolves to the bot token we stored.
    token = await secret_store.get(inst["secret_ref"], tenant_id=tenant)
    assert token.decode() == "secret_notion_bot_token"
    # an onboarding trigger + audit row landed.
    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind FROM onboarding_triggers WHERE tenant_id = $1 AND source = 'notion'",
        tenant,
    )
    assert trig is not None and trig["trigger_kind"] == "install"
    audit = await fresh_db.fetchrow(
        "SELECT action, status FROM installation_audit_log "
        "WHERE tenant_id = $1 AND provider = 'notion' AND status = 'ok'",
        tenant,
    )
    assert audit is not None and audit["action"] == "install"
    assert notion_metrics.get_install_outcome_count("success") == 1


async def test_callback_cross_tenant_collision(fresh_db: asyncpg.Pool) -> None:
    # Workspace already bound to tenant A.
    tenant_a = await _seed_tenant(fresh_db)
    await fresh_db.execute(
        "INSERT INTO provider_installations (id, tenant_id, provider, installation_id, "
        "secret_ref, enabled) VALUES ($1, $2, 'notion', 'ws-acme', $3, TRUE)",
        uuid7(), tenant_a, str(uuid4()),
    )
    # Tenant B tries to install the same workspace.
    tenant_b = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await _issue_state(tenant_b, fresh_db)
    app = _make_app(fresh_db, secret_store=secret_store)
    transport = httpx.ASGITransport(app=app)
    with _mock_token_endpoint("ws-acme"):
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.get(
                "/integrations/notion/callback",
                params={"code": "auth-code", "state": state},
                follow_redirects=False,
            )
    assert r.status_code == 302
    assert "reason=installation_collision" in r.headers["location"]
    # tenant A's binding is untouched.
    owner = await fresh_db.fetchval(
        "SELECT tenant_id FROM provider_installations "
        "WHERE provider = 'notion' AND installation_id = 'ws-acme'",
    )
    assert owner == tenant_a


async def test_callback_state_invalid(fresh_db: asyncpg.Pool) -> None:
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    app = _make_app(fresh_db, secret_store=secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/notion/callback",
            params={"code": "x", "state": "bogus.nothmac"},
            follow_redirects=False,
        )
    assert r.status_code == 302
    assert "reason=state_invalid" in r.headers["location"]
