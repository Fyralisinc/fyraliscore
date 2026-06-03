"""Gateway router tests for the Mercury admin connect wizard (finance).

Covers the production install surface added in
`services/ingest/integrations/mercury/oauth.py` — the gap that previously left
Mercury install reachable only through the synthetic `finance_router` dev panel.
Mirrors the Jira router tests: the outbound `MercuryClient` is faked and a real
`FernetSecretStore` proves the API token is persisted encrypted-at-rest.

Marked `integration` (real Postgres, auto-skipped when DATABASE_URL is unset).
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.errors import MercuryApiError
from lib.shared.secrets import FernetSecretStore


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
async def _clean_mercury_rows(fresh_db: asyncpg.Pool):
    """Remove `mercury` rows so the next test's migration re-run (which
    re-applies 0059's pre-mercury source CHECK before truncate) doesn't choke."""
    yield
    await fresh_db.execute("DELETE FROM onboarding_triggers WHERE source = 'mercury'")
    await fresh_db.execute("DELETE FROM mercury_accounts")
    await fresh_db.execute("DELETE FROM mercury_installations")
    await fresh_db.execute("DELETE FROM provider_installations WHERE provider = 'mercury'")


class _FakeMercuryClient:
    accounts = [
        {"id": "acc-checking", "name": "Operating Checking", "type": "checking"},
        {"id": "acc-savings", "name": "Reserve Savings", "type": "savings"},
    ]
    fail_unauthorized = False

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def list_accounts(self):
        if self.fail_unauthorized:
            raise MercuryApiError("401 token rejected", code="mercury_api_unauthorized")
        return list(self.accounts)

    async def aclose(self):
        return None


def _install_fake(monkeypatch, *, accounts=None, fail_unauthorized=False):
    from services.ingest.integrations.mercury import oauth as mercury_oauth

    class _Client(_FakeMercuryClient):
        pass

    if accounts is not None:
        _Client.accounts = accounts
    _Client.fail_unauthorized = fail_unauthorized
    monkeypatch.setattr(mercury_oauth, "MercuryClient", _Client)


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'mercury-router-test')", tid,
    )
    return tid


def _make_app(pool: asyncpg.Pool, tenant_id: UUID) -> tuple[FastAPI, FernetSecretStore]:
    from services.ingest.integrations.mercury.oauth import router

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


_TOKEN = {"api_token": "mercury-secret-token-abc"}


async def test_preflight_verifies_and_enumerates(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/mercury/connect/preflight", json=_TOKEN)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert [a["account_id"] for a in body["accounts"]] == ["acc-checking", "acc-savings"]
    assert body["accounts"][0]["account_kind"] == "checking"


async def test_preflight_auth_failure_returns_structured_400(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch, fail_unauthorized=True)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/mercury/connect/preflight", json=_TOKEN)
    assert r.status_code == 400
    body = r.json()
    assert body["error_code"] == "mercury_auth_failed"
    assert "mercury-secret-token-abc" not in r.text


async def test_finalize_writes_install_accounts_trigger_and_webhook(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, store = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/mercury/connect/finalize",
            json={**_TOKEN, "organization_id": "org-acme",
                  "webhook_secret": "whsec-mercury"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["account_count"] == 2
    assert body["webhook_registered"] is True
    install_id = UUID(body["installation_id"])

    install = await fresh_db.fetchrow(
        "SELECT base_url, secret_ref, organization_id FROM mercury_installations "
        "WHERE id = $1", install_id,
    )
    assert install["organization_id"] == "org-acme"
    assert install["secret_ref"] and install["secret_ref"] != _TOKEN["api_token"]
    assert (await store.get(install["secret_ref"], tenant_id=tenant)).decode() == _TOKEN["api_token"]

    accts = await fresh_db.fetch(
        "SELECT account_id FROM mercury_accounts "
        "WHERE mercury_installation_id = $1 ORDER BY account_id", install_id,
    )
    assert [a["account_id"] for a in accts] == ["acc-checking", "acc-savings"]

    trig = await fresh_db.fetchrow(
        "SELECT trigger_kind, installation_row_id FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = 'mercury'", tenant,
    )
    assert trig["installation_row_id"] == install_id

    prov = await fresh_db.fetchrow(
        "SELECT tenant_id, enabled FROM provider_installations "
        "WHERE provider = 'mercury' AND installation_id = 'org-acme'",
    )
    assert prov["tenant_id"] == tenant and prov["enabled"] is True


async def test_finalize_without_webhook_skips_provider_row(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/mercury/connect/finalize", json=_TOKEN)
    assert r.status_code == 200
    assert r.json()["webhook_registered"] is False
    assert await fresh_db.fetchval(
        "SELECT count(*) FROM provider_installations WHERE provider = 'mercury'",
    ) == 0


async def test_finalize_account_subset(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/mercury/connect/finalize",
            json={**_TOKEN, "account_ids": ["acc-checking"]},
        )
    assert r.status_code == 200
    assert r.json()["account_count"] == 1


async def test_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch, fail_unauthorized=True)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/mercury/connect/finalize", json=_TOKEN)
    assert r.status_code == 400
    assert await fresh_db.fetchval(
        "SELECT count(*) FROM mercury_installations WHERE tenant_id = $1", tenant,
    ) == 0
    assert await fresh_db.fetchval(
        "SELECT count(*) FROM encrypted_secrets WHERE tenant_id = $1", tenant,
    ) == 0


async def test_finalize_idempotent(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r1 = await c.post("/integrations/mercury/connect/finalize", json=_TOKEN)
        r2 = await c.post("/integrations/mercury/connect/finalize", json=_TOKEN)
    assert r1.json()["installation_id"] == r2.json()["installation_id"]
    assert await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = 'mercury'", tenant,
    ) == 1


async def test_unauthenticated_rejected(fresh_db: asyncpg.Pool) -> None:
    from services.ingest.integrations.mercury.oauth import router

    app = FastAPI()
    app.state.pool = fresh_db
    app.state.secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    app.include_router(router)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/mercury/connect/finalize", json=_TOKEN)
    assert r.status_code == 401
