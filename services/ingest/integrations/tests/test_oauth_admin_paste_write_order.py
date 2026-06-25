"""Credential verification order tests for admin-paste source onboarding.

These routers accept customer-provided credential material, so the important
production invariant is simple: bad credentials must fail before any durable
install state or encrypted secret is written.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.errors import (
    CartaApiError,
    GustoApiError,
    LinkedinApiError,
    RampApiError,
)
from lib.shared.secrets import FernetSecretStore


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


_SOURCES = ("gusto", "ramp", "carta", "linkedin")


@pytest.fixture(autouse=True)
async def _clean_source_rows(fresh_db: asyncpg.Pool):
    yield
    await fresh_db.execute(
        "DELETE FROM onboarding_triggers WHERE source = ANY($1::text[])",
        list(_SOURCES),
    )
    await fresh_db.execute(
        "DELETE FROM provider_installations WHERE provider = ANY($1::text[])",
        list(_SOURCES),
    )
    await fresh_db.execute("DELETE FROM gusto_entities")
    await fresh_db.execute("DELETE FROM ramp_entities")
    await fresh_db.execute("DELETE FROM carta_entities")
    await fresh_db.execute("DELETE FROM linkedin_entities")
    await fresh_db.execute("DELETE FROM gusto_installations")
    await fresh_db.execute("DELETE FROM ramp_installations")
    await fresh_db.execute("DELETE FROM carta_installations")
    await fresh_db.execute("DELETE FROM linkedin_installations")
    await fresh_db.execute("DELETE FROM encrypted_secrets")
    await fresh_db.execute("DELETE FROM tenants WHERE name = 'admin-paste-write-order-test'")


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'admin-paste-write-order-test')",
        tid,
    )
    return tid


def _make_app(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    router,
) -> tuple[FastAPI, FernetSecretStore]:
    store = FernetSecretStore(pool, master_kek=Fernet.generate_key())
    app = FastAPI()
    app.state.pool = pool
    app.state.secret_store = store

    @app.middleware("http")
    async def _inject_auth(request, call_next):  # type: ignore[no-untyped-def]
        class _Auth:
            pass

        auth = _Auth()
        auth.tenant_id = tenant_id
        request.state.auth = auth
        return await call_next(request)

    app.include_router(router)
    return app, store


async def _post_json(
    app: FastAPI,
    path: str,
    payload: dict[str, object],
) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://t",
    ) as client:
        return await client.post(path, json=payload)


async def _assert_no_durable_state(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    install_table: str,
    source: str,
) -> None:
    assert await pool.fetchval(
        f"SELECT count(*) FROM {install_table} WHERE tenant_id = $1", tenant_id,
    ) == 0
    assert await pool.fetchval(
        "SELECT count(*) FROM encrypted_secrets WHERE tenant_id = $1", tenant_id,
    ) == 0
    assert await pool.fetchval(
        "SELECT count(*) FROM onboarding_triggers WHERE tenant_id = $1 AND source = $2",
        tenant_id,
        source,
    ) == 0
    assert await pool.fetchval(
        "SELECT count(*) FROM provider_installations WHERE tenant_id = $1 AND provider = $2",
        tenant_id,
        source,
    ) == 0


async def test_gusto_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.gusto import oauth as gusto_oauth

    class _FailingGustoClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def company(self):
            raise GustoApiError("401 token rejected", code="gusto_api_unauthorized")

        async def aclose(self):
            return None

    monkeypatch.setattr(gusto_oauth, "GustoClient", _FailingGustoClient)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, gusto_oauth.router)

    payload = {
        "company_uuid": "gusto-company-bad",
        "access_token": "bad-gusto-access-token",
        "refresh_token": "bad-gusto-refresh-token",
        "webhook_verifier_token": "bad-gusto-webhook-token",
    }
    response = await _post_json(app, "/integrations/gusto/connect/finalize", payload)

    assert response.status_code == 400
    assert response.json()["error_code"] == "gusto_auth_failed"
    assert "bad-gusto-access-token" not in response.text
    assert "bad-gusto-refresh-token" not in response.text
    assert "bad-gusto-webhook-token" not in response.text
    await _assert_no_durable_state(fresh_db, tenant, "gusto_installations", "gusto")


async def test_ramp_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.ramp import oauth as ramp_oauth

    class _FailingRampClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def business(self):
            raise RampApiError("401 token rejected", code="ramp_api_unauthorized")

        async def aclose(self):
            return None

    monkeypatch.setattr(ramp_oauth, "RampClient", _FailingRampClient)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, ramp_oauth.router)

    payload = {
        "access_token": "bad-ramp-access-token",
        "webhook_verifier_token": "bad-ramp-webhook-token",
    }
    response = await _post_json(app, "/integrations/ramp/connect/finalize", payload)

    assert response.status_code == 400
    assert response.json()["error_code"] == "ramp_auth_failed"
    assert "bad-ramp-access-token" not in response.text
    assert "bad-ramp-webhook-token" not in response.text
    await _assert_no_durable_state(fresh_db, tenant, "ramp_installations", "ramp")


async def test_carta_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.carta import oauth as carta_oauth

    class _FailingCartaClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def list_issuers(self, *, page_size: int = 50):
            raise CartaApiError("401 token rejected", code="carta_api_unauthorized")

        async def aclose(self):
            return None

    monkeypatch.setattr(carta_oauth, "CartaClient", _FailingCartaClient)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, carta_oauth.router)

    payload = {
        "access_token": "bad-carta-access-token",
        "client_secret": "bad-carta-client-secret",
    }
    response = await _post_json(app, "/integrations/carta/connect/finalize", payload)

    assert response.status_code == 400
    assert response.json()["error_code"] == "carta_auth_failed"
    assert "bad-carta-access-token" not in response.text
    assert "bad-carta-client-secret" not in response.text
    await _assert_no_durable_state(fresh_db, tenant, "carta_installations", "carta")


async def test_linkedin_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.linkedin import oauth as linkedin_oauth

    class _FailingLinkedinClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def get_organization(self):
            raise LinkedinApiError(
                "401 token rejected",
                code="linkedin_api_unauthorized",
            )

        async def aclose(self):
            return None

    monkeypatch.setattr(linkedin_oauth, "LinkedinClient", _FailingLinkedinClient)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, linkedin_oauth.router)

    payload = {
        "organization_urn": "urn:li:organization:bad",
        "access_token": "bad-linkedin-access-token",
        "refresh_token": "bad-linkedin-refresh-token",
    }
    response = await _post_json(
        app,
        "/integrations/linkedin/connect/finalize",
        payload,
    )

    assert response.status_code == 400
    assert response.json()["error_code"] == "linkedin_auth_failed"
    assert "bad-linkedin-access-token" not in response.text
    assert "bad-linkedin-refresh-token" not in response.text
    await _assert_no_durable_state(
        fresh_db,
        tenant,
        "linkedin_installations",
        "linkedin",
    )
