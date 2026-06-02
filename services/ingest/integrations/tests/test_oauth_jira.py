"""Gateway router tests for the Jira admin connect wizard (IN-17).

Covers the production install surface added in
`services/ingest/integrations/jira/oauth.py` — the gap that previously made
`finalize_install` reachable only from `scripts/sandbox_jira*.py`. Unlike the
Google DWD sources, Jira submits credentials (account email + API token), so
these tests fake the outbound `JiraClient` and use a real `FernetSecretStore`
to prove the token is persisted encrypted-at-rest (only an opaque ref reaches
the install tables).

Marked `integration` (real Postgres, auto-skipped when DATABASE_URL is unset).
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.errors import JiraApiError
from lib.shared.secrets import FernetSecretStore


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
async def _clean_jira_rows(fresh_db: asyncpg.Pool):
    """Remove any `jira` rows these tests leave behind.

    The conftest `db_pool` fixture re-applies ALL migrations at the start of
    every test, BEFORE `fresh_db` truncates. Migration 0059's source CHECK
    predates `jira`, so a surviving `jira` row makes the next test's
    `ADD CONSTRAINT` re-run fail validation. Production is forward-only and
    unaffected (same guard as the Google source router tests).
    """
    yield
    await fresh_db.execute("DELETE FROM onboarding_triggers WHERE source = 'jira'")
    await fresh_db.execute("DELETE FROM jira_projects")
    await fresh_db.execute("DELETE FROM jira_installations")
    await fresh_db.execute("DELETE FROM provider_installations WHERE provider = 'jira'")


# ---------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------
class _FakeJiraClient:
    """Stand-in for the outbound JiraClient. Configured per-test via class
    attributes set on a small factory closure (see `_install_fake`)."""

    projects = [
        {"key": "ENG", "id": "10001", "name": "Engineering"},
        {"key": "OPS", "id": "10002", "name": "Operations"},
    ]
    fail_unauthorized = False

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def myself(self):
        if self.fail_unauthorized:
            raise JiraApiError("401 token rejected", code="jira_api_unauthorized")
        return {
            "accountId": "5b10ac8d82e05b22cc7d4ef5",
            "displayName": "Admin User",
            "emailAddress": "admin@acme.com",
        }

    async def list_projects(self, *, start_at=0, max_results=50):
        # Single page.
        return list(self.projects), None, len(self.projects)

    async def aclose(self):
        return None


def _install_fake(monkeypatch, *, projects=None, fail_unauthorized=False):
    from services.ingest.integrations.jira import oauth as jira_oauth

    class _Client(_FakeJiraClient):
        pass

    if projects is not None:
        _Client.projects = projects
    _Client.fail_unauthorized = fail_unauthorized
    monkeypatch.setattr(jira_oauth, "JiraClient", _Client)


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'jira-router-test')", tid,
    )
    return tid


def _make_app(pool: asyncpg.Pool, tenant_id: UUID) -> tuple[FastAPI, FernetSecretStore]:
    """Minimal app: Jira oauth router + injected auth + pinned pool + a real
    Fernet secret store (so we can assert the token is stored encrypted)."""
    from services.ingest.integrations.jira.oauth import router

    store = FernetSecretStore(pool, master_kek=Fernet.generate_key())

    app = FastAPI()
    app.state.pool = pool
    app.state.secret_store = store

    @app.middleware("http")
    async def _inject_auth(request, call_next):  # type: ignore[no-untyped-def]
        class _A:
            pass

        a = _A()
        a.tenant_id = tenant_id
        request.state.auth = a
        return await call_next(request)

    app.include_router(router)
    return app, store


_CREDS = {
    "base_url": "https://acme.atlassian.net",
    "account_email": "admin@acme.com",
    "api_token": "atlassian-api-token-xyz",
}


async def test_preflight_verifies_and_enumerates(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/jira/connect/preflight", json=_CREDS)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["site_host"] == "acme.atlassian.net"
    assert body["account"]["display_name"] == "Admin User"
    assert [p["key"] for p in body["projects"]] == ["ENG", "OPS"]


async def test_preflight_auth_failure_returns_structured_400(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch, fail_unauthorized=True)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/jira/connect/preflight", json=_CREDS)
    assert r.status_code == 400, r.text
    body = r.json()
    assert body["ok"] is False
    assert body["error_code"] == "jira_auth_failed"
    # The token must never be echoed back.
    assert "atlassian-api-token-xyz" not in r.text


async def test_finalize_writes_install_projects_trigger_and_webhook(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, store = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/jira/connect/finalize",
            json={**_CREDS, "webhook_secret": "whsec-jira-123"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["project_count"] == 2
    assert body["webhook_registered"] is True
    install_id = UUID(body["installation_id"])

    # Install row — secret_ref is an opaque ref, NOT the plaintext token.
    install = await fresh_db.fetchrow(
        "SELECT base_url, account_email, secret_ref, webhook_secret_ref "
        "FROM jira_installations WHERE id = $1", install_id,
    )
    assert install["base_url"] == "https://acme.atlassian.net"
    assert install["account_email"] == "admin@acme.com"
    assert install["secret_ref"] and install["secret_ref"] != _CREDS["api_token"]

    # The stored secret decrypts back to the submitted token.
    decrypted = await store.get(install["secret_ref"], tenant_id=tenant)
    assert decrypted.decode() == _CREDS["api_token"]

    # One project row per enumerated project.
    projects = await fresh_db.fetch(
        "SELECT project_key FROM jira_projects "
        "WHERE jira_installation_id = $1 ORDER BY project_key", install_id,
    )
    assert [p["project_key"] for p in projects] == ["ENG", "OPS"]

    # Onboarding trigger (source='jira', install id in installation_row_id).
    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = 'jira'", tenant,
    )
    assert trig["trigger_kind"] == "install"
    assert trig["installation_row_id"] == install_id

    # Webhook edge row in provider_installations keyed by the site host.
    prov = await fresh_db.fetchrow(
        "SELECT tenant_id, secret_ref, enabled FROM provider_installations "
        "WHERE provider = 'jira' AND installation_id = 'acme.atlassian.net'",
    )
    assert prov["tenant_id"] == tenant
    assert prov["enabled"] is True
    assert (await store.get(prov["secret_ref"], tenant_id=tenant)).decode() == "whsec-jira-123"


async def test_finalize_without_webhook_secret_skips_provider_row(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/jira/connect/finalize", json=_CREDS)
    assert r.status_code == 200, r.text
    assert r.json()["webhook_registered"] is False

    n = await fresh_db.fetchval(
        "SELECT count(*) FROM provider_installations WHERE provider = 'jira'",
    )
    assert n == 0


async def test_finalize_uses_pinned_project_subset(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When project_keys is supplied, the enumerator is not consulted."""
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/jira/connect/finalize",
            json={**_CREDS, "project_keys": ["ENG"]},
        )
    assert r.status_code == 200, r.text
    assert r.json()["project_count"] == 1
    install_id = UUID(r.json()["installation_id"])
    projects = await fresh_db.fetch(
        "SELECT project_key FROM jira_projects WHERE jira_installation_id = $1",
        install_id,
    )
    assert [p["project_key"] for p in projects] == ["ENG"]


async def test_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid token → 400 and NO secret / install rows (verify-before-write)."""
    _install_fake(monkeypatch, fail_unauthorized=True)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/jira/connect/finalize", json=_CREDS)
    assert r.status_code == 400
    assert r.json()["error_code"] == "jira_auth_failed"

    assert await fresh_db.fetchval(
        "SELECT count(*) FROM jira_installations WHERE tenant_id = $1", tenant,
    ) == 0
    assert await fresh_db.fetchval(
        "SELECT count(*) FROM encrypted_secrets WHERE tenant_id = $1", tenant,
    ) == 0


async def test_finalize_is_idempotent_on_reinstall(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r1 = await c.post("/integrations/jira/connect/finalize", json=_CREDS)
        r2 = await c.post("/integrations/jira/connect/finalize", json=_CREDS)
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json()["installation_id"] == r2.json()["installation_id"]

    n_triggers = await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = 'jira'", tenant,
    )
    assert n_triggers == 1


async def test_finalize_rejects_bad_base_url(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/jira/connect/finalize",
            json={**_CREDS, "base_url": "not-a-url"},
        )
    assert r.status_code == 400


async def test_unauthenticated_request_is_rejected(
    fresh_db: asyncpg.Pool,
) -> None:
    from services.ingest.integrations.jira.oauth import router

    app = FastAPI()
    app.state.pool = fresh_db
    app.state.secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    app.include_router(router)  # NB: no auth middleware

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/jira/connect/finalize", json=_CREDS)
    assert r.status_code == 401
