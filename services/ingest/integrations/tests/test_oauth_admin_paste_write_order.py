"""Credential verification order tests for admin-paste source onboarding.

These routers accept customer-provided credential material, so the important
production invariant is simple: bad credentials must fail before any durable
install state or encrypted secret is written.
"""
from __future__ import annotations

import json
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.errors import (
    BrexApiError,
    CartaApiError,
    DeelApiError,
    FigmaApiError,
    FirefliesApiError,
    GustoApiError,
    LinkedinApiError,
    MiroApiError,
    RampApiError,
)
from lib.shared.secrets import FernetSecretStore


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


_SOURCES = (
    "brex",
    "carta",
    "deel",
    "figma",
    "fireflies",
    "gusto",
    "linkedin",
    "miro",
    "ramp",
)


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
    await fresh_db.execute("DELETE FROM brex_accounts")
    await fresh_db.execute("DELETE FROM deel_contracts")
    await fresh_db.execute("DELETE FROM miro_boards")
    await fresh_db.execute("DELETE FROM figma_files")
    await fresh_db.execute("DELETE FROM gusto_installations")
    await fresh_db.execute("DELETE FROM ramp_installations")
    await fresh_db.execute("DELETE FROM carta_installations")
    await fresh_db.execute("DELETE FROM linkedin_installations")
    await fresh_db.execute("DELETE FROM brex_installations")
    await fresh_db.execute("DELETE FROM deel_installations")
    await fresh_db.execute("DELETE FROM fireflies_installations")
    await fresh_db.execute("DELETE FROM miro_installations")
    await fresh_db.execute("DELETE FROM figma_installations")
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


async def test_brex_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.brex import oauth as brex_oauth

    class _FailingBrexClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def list_accounts(self):
            raise BrexApiError("401 token rejected", code="brex_api_unauthorized")

        async def aclose(self):
            return None

    monkeypatch.setattr(brex_oauth, "BrexClient", _FailingBrexClient)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, brex_oauth.router)

    payload = {
        "api_token": "bad-brex-api-token",
        "organization_id": "brex-org-bad",
        "webhook_secret": "bad-brex-webhook-secret",
    }
    response = await _post_json(app, "/integrations/brex/connect/finalize", payload)

    assert response.status_code == 400
    assert response.json()["error_code"] == "brex_auth_failed"
    assert "bad-brex-api-token" not in response.text
    assert "bad-brex-webhook-secret" not in response.text
    await _assert_no_durable_state(fresh_db, tenant, "brex_installations", "brex")


async def test_deel_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.deel import oauth as deel_oauth

    class _FailingDeelClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def list_contracts(self):
            raise DeelApiError("401 token rejected", code="deel_api_unauthorized")

        async def aclose(self):
            return None

    monkeypatch.setattr(deel_oauth, "DeelClient", _FailingDeelClient)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, deel_oauth.router)

    payload = {
        "api_token": "bad-deel-api-token",
        "organization_id": "deel-org-bad",
        "webhook_secret": "bad-deel-webhook-secret",
    }
    response = await _post_json(app, "/integrations/deel/connect/finalize", payload)

    assert response.status_code == 400
    assert response.json()["error_code"] == "deel_auth_failed"
    assert "bad-deel-api-token" not in response.text
    assert "bad-deel-webhook-secret" not in response.text
    await _assert_no_durable_state(fresh_db, tenant, "deel_installations", "deel")


async def test_fireflies_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.fireflies import oauth as fireflies_oauth

    class _FailingFirefliesClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def get_workspace(self):
            raise FirefliesApiError(
                "401 token rejected",
                code="fireflies_api_unauthorized",
            )

        async def aclose(self):
            return None

    monkeypatch.setattr(fireflies_oauth, "FirefliesClient", _FailingFirefliesClient)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, fireflies_oauth.router)

    payload = {
        "api_token": "bad-fireflies-api-token",
        "webhook_secret": "bad-fireflies-webhook-secret",
    }
    response = await _post_json(
        app,
        "/integrations/fireflies/connect/finalize",
        payload,
    )

    assert response.status_code == 400
    assert response.json()["error_code"] == "fireflies_auth_failed"
    assert "bad-fireflies-api-token" not in response.text
    assert "bad-fireflies-webhook-secret" not in response.text
    await _assert_no_durable_state(
        fresh_db,
        tenant,
        "fireflies_installations",
        "fireflies",
    )


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


async def test_ramp_finalize_stores_client_credentials_ref(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.ramp import oauth as ramp_oauth

    class _SuccessfulRampClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def mint_token(self):
            return {"access_token": "fresh-ramp-access", "expires_in": 3600}

        async def business(self):
            return {"id": "ramp-business-1", "business_name_legal": "Ramp Test"}

        async def aclose(self):
            return None

    monkeypatch.setattr(ramp_oauth, "RampClient", _SuccessfulRampClient)
    tenant = await _seed_tenant(fresh_db)
    app, store = _make_app(fresh_db, tenant, ramp_oauth.router)

    payload = {
        "client_id": "ramp-client-id",
        "client_secret": "ramp-client-secret",
        "webhook_verifier_token": "ramp-webhook-secret",
    }
    response = await _post_json(app, "/integrations/ramp/connect/finalize", payload)

    assert response.status_code == 200
    row = await fresh_db.fetchrow(
        """
        SELECT business_id, secret_ref, refresh_secret_ref, webhook_secret_ref
          FROM ramp_installations
         WHERE tenant_id = $1
        """,
        tenant,
    )
    assert row is not None
    assert row["business_id"] == "ramp-business-1"
    assert row["secret_ref"] and row["secret_ref"] != "fresh-ramp-access"
    assert row["refresh_secret_ref"]
    assert row["webhook_secret_ref"]

    access_token = await store.get(row["secret_ref"], tenant_id=tenant)
    credential_payload = await store.get(row["refresh_secret_ref"], tenant_id=tenant)
    webhook_secret = await store.get(row["webhook_secret_ref"], tenant_id=tenant)

    assert access_token.decode("utf-8") == "fresh-ramp-access"
    assert json.loads(credential_payload.decode("utf-8")) == {
        "client_id": "ramp-client-id",
        "client_secret": "ramp-client-secret",
    }
    assert webhook_secret.decode("utf-8") == "ramp-webhook-secret"


async def test_miro_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.miro import oauth as miro_oauth

    class _FailingMiroClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def list_boards(self):
            raise MiroApiError("401 token rejected", code="miro_api_unauthorized")

        async def aclose(self):
            return None

    monkeypatch.setattr(miro_oauth, "MiroClient", _FailingMiroClient)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, miro_oauth.router)

    payload = {
        "api_token": "bad-miro-api-token",
        "org_id": "miro-org-bad",
    }
    response = await _post_json(app, "/integrations/miro/connect/finalize", payload)

    assert response.status_code == 400
    assert response.json()["error_code"] == "miro_auth_failed"
    assert "bad-miro-api-token" not in response.text
    await _assert_no_durable_state(fresh_db, tenant, "miro_installations", "miro")


async def test_miro_finalize_rejects_retired_webhook_secret_without_writes(
    fresh_db: asyncpg.Pool,
) -> None:
    from services.ingest.integrations.miro import oauth as miro_oauth

    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, miro_oauth.router)
    response = await _post_json(
        app,
        "/integrations/miro/connect/finalize",
        {
            "api_token": "miro-api-token",
            "org_id": "miro-org",
            "webhook_secret": "retired-miro-webhook-secret",
        },
    )

    assert response.status_code == 400
    assert "not supported for poll-only Miro" in response.json()["detail"]
    assert "retired-miro-webhook-secret" not in response.text
    await _assert_no_durable_state(
        fresh_db,
        tenant,
        "miro_installations",
        "miro",
    )


async def test_miro_finalize_persists_only_exact_poll_installation(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.miro import oauth as miro_oauth

    class _SuccessfulMiroClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def list_boards(self):
            return [
                {
                    "id": "miro-board-1",
                    "name": "Product",
                    "type": "board",
                },
            ]

        async def aclose(self):
            return None

    monkeypatch.setattr(miro_oauth, "MiroClient", _SuccessfulMiroClient)
    tenant = await _seed_tenant(fresh_db)
    app, store = _make_app(fresh_db, tenant, miro_oauth.router)
    response = await _post_json(
        app,
        "/integrations/miro/connect/finalize",
        {
            "api_token": "miro-api-token",
            "org_id": "  miro-org-1  ",
            "board_ids": ["miro-board-1"],
        },
    )

    assert response.status_code == 200
    assert "webhook_registered" not in response.json()
    row = await fresh_db.fetchrow(
        """
        SELECT id, org_id, secret_ref, webhook_secret_ref
          FROM miro_installations
         WHERE tenant_id = $1
        """,
        tenant,
    )
    assert row is not None
    assert row["org_id"] == "miro-org-1"
    assert row["secret_ref"]
    assert row["webhook_secret_ref"] is None
    assert await fresh_db.fetchval(
        """
        SELECT count(*)
          FROM provider_installations
         WHERE tenant_id = $1 AND provider = 'miro'
        """,
        tenant,
    ) == 0
    assert await fresh_db.fetchval(
        """
        SELECT count(*)
          FROM miro_boards
         WHERE tenant_id = $1
           AND miro_installation_id = $2
           AND board_id = 'miro-board-1'
        """,
        tenant,
        row["id"],
    ) == 1
    api_token = await store.get(row["secret_ref"], tenant_id=tenant)
    assert api_token.decode("utf-8") == "miro-api-token"


async def test_figma_finalize_bad_credentials_writes_nothing(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.figma import oauth as figma_oauth

    class _FailingFigmaClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def list_files(self, team_id: str):
            raise FigmaApiError("401 token rejected", code="figma_api_unauthorized")

        async def aclose(self):
            return None

    monkeypatch.setattr(figma_oauth, "FigmaClient", _FailingFigmaClient)
    tenant = await _seed_tenant(fresh_db)
    app, _ = _make_app(fresh_db, tenant, figma_oauth.router)

    payload = {
        "api_token": "bad-figma-api-token",
        "team_id": "figma-team-bad",
        "webhook_id": "figma-webhook-bad",
        "webhook_secret": "bad-figma-webhook-secret",
    }
    response = await _post_json(app, "/integrations/figma/connect/finalize", payload)

    assert response.status_code == 400
    assert response.json()["error_code"] == "figma_auth_failed"
    assert "bad-figma-api-token" not in response.text
    assert "bad-figma-webhook-secret" not in response.text
    await _assert_no_durable_state(fresh_db, tenant, "figma_installations", "figma")


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
