"""Gateway router tests for the QuickBooks admin connect wizard (finance).

Covers the production install surface added in
`services/ingest/integrations/quickbooks/oauth.py` — the gap that previously
left QBO install reachable only through the synthetic `finance_router` dev
panel. The outbound `QuickBooksClient` is faked; a real `FernetSecretStore`
proves the access + refresh tokens are persisted encrypted-at-rest.

Marked `integration` (real Postgres, auto-skipped when DATABASE_URL is unset).
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.errors import QuickBooksApiError
from lib.shared.secrets import FernetSecretStore


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
async def _clean_qbo_rows(fresh_db: asyncpg.Pool):
    """Remove `quickbooks` rows so the next test's migration re-run (which
    re-applies 0059's pre-quickbooks source CHECK before truncate) doesn't choke."""
    yield
    await fresh_db.execute("DELETE FROM onboarding_triggers WHERE source = 'quickbooks'")
    await fresh_db.execute("DELETE FROM quickbooks_entities")
    await fresh_db.execute("DELETE FROM quickbooks_installations")
    await fresh_db.execute("DELETE FROM provider_installations WHERE provider = 'quickbooks'")


class _FakeQboClient:
    company_name = "Acme Inc"
    fail_unauthorized = False

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def company_info(self):
        if self.fail_unauthorized:
            raise QuickBooksApiError(
                "401 token rejected", code="quickbooks_api_unauthorized",
            )
        return {"CompanyInfo": {"CompanyName": self.company_name}}

    async def aclose(self):
        return None


def _install_fake(monkeypatch, *, fail_unauthorized=False):
    from services.ingest.integrations.quickbooks import oauth as qbo_oauth

    class _Client(_FakeQboClient):
        pass

    _Client.fail_unauthorized = fail_unauthorized
    monkeypatch.setattr(qbo_oauth, "QuickBooksClient", _Client)


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'qbo-router-test')", tid,
    )
    return tid


def _make_app(pool: asyncpg.Pool, tenant_id: UUID) -> tuple[FastAPI, FernetSecretStore]:
    from services.ingest.integrations.quickbooks.oauth import router

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


_CREDS = {"realm_id": "4620816365", "access_token": "qbo-access-token-xyz"}


async def test_preflight_verifies_company(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/quickbooks/connect/preflight", json=_CREDS)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["ok"] is True
    assert body["realm_id"] == "4620816365"
    assert body["company_name"] == "Acme Inc"
    assert "Invoice" in body["entities"]


async def test_preflight_auth_failure(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch, fail_unauthorized=True)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/quickbooks/connect/preflight", json=_CREDS)
    assert r.status_code == 400
    assert r.json()["error_code"] == "quickbooks_auth_failed"
    assert "qbo-access-token-xyz" not in r.text


async def test_finalize_writes_install_entities_trigger_tokens_and_webhook(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, store = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/quickbooks/connect/finalize",
            json={**_CREDS, "refresh_token": "qbo-refresh-token-123",
                  "webhook_verifier_token": "qbo-verifier-456"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["entity_count"] == 4
    assert body["webhook_registered"] is True
    install_id = UUID(body["installation_id"])

    install = await fresh_db.fetchrow(
        "SELECT realm_id, secret_ref, refresh_secret_ref, webhook_secret_ref "
        "FROM quickbooks_installations WHERE id = $1", install_id,
    )
    assert install["realm_id"] == "4620816365"
    # Access + refresh tokens both stored encrypted and decrypt back.
    assert (await store.get(install["secret_ref"], tenant_id=tenant)).decode() == "qbo-access-token-xyz"
    assert (await store.get(install["refresh_secret_ref"], tenant_id=tenant)).decode() == "qbo-refresh-token-123"

    ents = await fresh_db.fetch(
        "SELECT entity_type FROM quickbooks_entities "
        "WHERE quickbooks_installation_id = $1", install_id,
    )
    assert {e["entity_type"] for e in ents} == {"Invoice", "Bill", "BillPayment", "Payment"}

    trig = await fresh_db.fetchrow(
        "SELECT installation_row_id FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = 'quickbooks'", tenant,
    )
    assert trig["installation_row_id"] == install_id

    prov = await fresh_db.fetchrow(
        "SELECT enabled FROM provider_installations "
        "WHERE provider = 'quickbooks' AND installation_id = '4620816365'",
    )
    assert prov["enabled"] is True


async def test_finalize_without_webhook_skips_provider_row(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/quickbooks/connect/finalize", json=_CREDS)
    assert r.status_code == 200
    assert r.json()["webhook_registered"] is False
    assert await fresh_db.fetchval(
        "SELECT count(*) FROM provider_installations WHERE provider = 'quickbooks'",
    ) == 0


async def test_finalize_custom_entities(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/quickbooks/connect/finalize",
            json={**_CREDS, "entities": ["Invoice"]},
        )
    assert r.status_code == 200
    assert r.json()["entity_count"] == 1
    install_id = UUID(r.json()["installation_id"])
    ents = await fresh_db.fetch(
        "SELECT entity_type FROM quickbooks_entities WHERE quickbooks_installation_id = $1",
        install_id,
    )
    assert [e["entity_type"] for e in ents] == ["Invoice"]


async def test_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch, fail_unauthorized=True)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/quickbooks/connect/finalize", json=_CREDS)
    assert r.status_code == 400
    assert await fresh_db.fetchval(
        "SELECT count(*) FROM quickbooks_installations WHERE tenant_id = $1", tenant,
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
        r1 = await c.post("/integrations/quickbooks/connect/finalize", json=_CREDS)
        r2 = await c.post("/integrations/quickbooks/connect/finalize", json=_CREDS)
    assert r1.json()["installation_id"] == r2.json()["installation_id"]
    assert await fresh_db.fetchval(
        "SELECT count(*) FROM onboarding_triggers "
        "WHERE tenant_id = $1 AND source = 'quickbooks'", tenant,
    ) == 1


async def test_finalize_requires_realm_and_token(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake(monkeypatch)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post(
            "/integrations/quickbooks/connect/finalize",
            json={"access_token": "x"},  # missing realm_id
        )
    assert r.status_code == 400


async def test_unauthenticated_rejected(fresh_db: asyncpg.Pool) -> None:
    from services.ingest.integrations.quickbooks.oauth import router

    app = FastAPI()
    app.state.pool = fresh_db
    app.state.secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    app.include_router(router)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t",
    ) as c:
        r = await c.post("/integrations/quickbooks/connect/finalize", json=_CREDS)
    assert r.status_code == 401
