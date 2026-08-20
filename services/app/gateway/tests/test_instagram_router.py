from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from lib.shared.tenant_context import tenant_transaction
from services.app.gateway import instagram_router
from services.app.gateway.instagram_router import build_instagram_router
from services.ingest.integrations.router import build_integrations_router
from services.ingest.integrations.instagram import oauth as instagram_oauth
from services.ingest.integrations.instagram.signature import sign_payload


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def instagram_client(
    gateway_pool: asyncpg.Pool,
    app_deps: Any,
) -> AsyncGenerator[tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore], None]:
    app = FastAPI()
    app.state.deps = app_deps
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())
    app.state.secret_store = store
    app.include_router(build_instagram_router())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, gateway_pool, store


async def _register(
    pool: asyncpg.Pool,
    store: FernetSecretStore,
    *,
    tenant_id: UUID | None = None,
    ig_business_account_id: str | None = None,
    app_secret: str = "ig-app-secret",
    verify_token: str = "ig-verify-token",
) -> tuple[UUID, str]:
    tenant_id = tenant_id or uuid4()
    business_id = ig_business_account_id or f"ig-{uuid4().hex}"
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            tenant_id,
            "instagram-test",
        )
    app_secret_ref = await store.put(
        app_secret,
        label=f"instagram_app_secret:{business_id}",
        tenant_id=tenant_id,
    )
    verify_token_ref = await store.put(
        verify_token,
        label=f"instagram_verify_token:{business_id}",
        tenant_id=tenant_id,
    )
    access_token_ref = await store.put(
        "ig-access-token",
        label=f"instagram_access_token:{business_id}",
        tenant_id=tenant_id,
    )
    async with tenant_transaction(tenant_id, pool=pool) as conn:
        install_id = uuid7()
        await conn.execute(
            """
            INSERT INTO instagram_installations (
                id, tenant_id, base_url, ig_business_account_id, page_id,
                app_secret_ref, verify_token_ref, access_token_ref
            ) VALUES ($1, $2, 'https://graph.facebook.com', $3, 'page-1', $4, $5, $6)
            """,
            install_id,
            tenant_id,
            business_id,
            app_secret_ref,
            verify_token_ref,
            access_token_ref,
        )
        await conn.execute(
            """
            INSERT INTO instagram_webhook_routes (
                id, resolved_tenant_id, instagram_installation_id, ig_business_account_id,
                page_id, app_secret_ref, verify_token_ref, enabled
            ) VALUES ($1, $2, $3, $4, 'page-1', $5, $6, TRUE)
            """,
            uuid7(),
            tenant_id,
            install_id,
            business_id,
            app_secret_ref,
            verify_token_ref,
        )
    return tenant_id, business_id


async def test_webhook_verify_matches_verify_token_secret_ref(
    instagram_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
) -> None:
    client, pool, store = instagram_client
    await _register(pool, store, verify_token="db-backed-ig-token")

    response = await client.get(
        "/integrations/instagram/webhook",
        params={
            "hub.mode": "subscribe",
            "hub.verify_token": "db-backed-ig-token",
            "hub.challenge": "challenge-ok",
        },
    )

    assert response.status_code == 200
    assert response.text == "challenge-ok"


async def test_signed_webhook_fans_out_to_inline_ingest(
    instagram_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, pool, store = instagram_client
    tenant_id, business_id = await _register(pool, store, app_secret="signed-secret")
    captured: list[dict[str, Any]] = []

    async def fake_ingest_item(
        deps: Any,
        tenant_id: UUID,
        item_payload: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, Any]:
        captured.append({
            "tenant_id": tenant_id,
            "payload": item_payload,
            "headers": headers,
        })
        return {
            "channel": "instagram:message",
            "observation_id": str(uuid4()),
            "deduped": False,
        }

    monkeypatch.setattr(instagram_router, "_ingest_item", fake_ingest_item)
    payload = {
        "object": "instagram",
        "entry": [
            {
                "id": business_id,
                "messaging": [
                    {
                        "sender": {"id": "cust-1"},
                        "recipient": {"id": business_id},
                        "timestamp": 1781000000000,
                        "message": {"mid": "mid-1", "text": "hello"},
                    }
                ],
            }
        ],
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    response = await client.post(
        "/integrations/instagram/webhook",
        content=raw,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": sign_payload("signed-secret", raw),
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["ingested"] == 1
    assert captured[0]["tenant_id"] == tenant_id
    assert captured[0]["headers"] == {
        "x-instagram-business-account-id": business_id,
    }
    assert captured[0]["payload"]["message_id"] == "mid-1"
    contact_count = await pool.fetchval(
        "SELECT count(*) FROM instagram_contacts WHERE tenant_id = $1",
        tenant_id,
    )
    replay_count = await pool.fetchval(
        """
        SELECT count(*) FROM onboarding_triggers
         WHERE tenant_id = $1
           AND source = 'instagram'
           AND trigger_kind = 'manual_replay'
           AND consumed_at IS NULL
        """,
        tenant_id,
    )
    assert contact_count == 1
    assert replay_count == 1


async def test_bad_signature_rejected(
    instagram_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
) -> None:
    client, pool, store = instagram_client
    _tenant_id, business_id = await _register(pool, store, app_secret="signed-secret")
    payload = {"entry": [{"id": business_id, "messaging": []}]}

    response = await client.post(
        "/integrations/instagram/webhook",
        json=payload,
        headers={"X-Hub-Signature-256": "sha256=bad"},
    )

    assert response.status_code == 401
    assert response.json()["status"] == "signature_invalid"


async def test_unknown_or_disabled_route_is_acknowledged_after_signature_check(
    instagram_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, pool, store = instagram_client
    _tenant_id, business_id = await _register(pool, store, app_secret="deployment-secret")
    monkeypatch.setenv("INSTAGRAM_APP_SECRET", "deployment-secret")
    await pool.execute(
        "UPDATE instagram_webhook_routes SET enabled = FALSE WHERE ig_business_account_id = $1",
        business_id,
    )
    raw = json.dumps({"entry": [{"id": business_id, "messaging": []}]}).encode()

    response = await client.post(
        "/integrations/instagram/webhook",
        content=raw,
        headers={"X-Hub-Signature-256": sign_payload("deployment-secret", raw)},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ignored"


async def test_signed_webhook_binds_meta_delivery_account_alias(
    instagram_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, pool, store = instagram_client
    tenant_id, business_id = await _register(pool, store, app_secret="deployment-secret")
    delivery_account_id = "meta-delivery-account-id"
    captured: list[dict[str, Any]] = []
    monkeypatch.setenv("INSTAGRAM_APP_SECRET", "deployment-secret")

    class FakeInstagramClient:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        async def validate_account(self, account_id: str) -> dict[str, str]:
            assert account_id == delivery_account_id
            return {"id": business_id}

        async def aclose(self) -> None:
            return None

    async def fake_ingest_item(
        _deps: Any,
        _tenant_id: UUID,
        item_payload: dict[str, Any],
        _headers: dict[str, str],
    ) -> dict[str, Any]:
        captured.append(item_payload)
        return {
            "channel": "instagram:message",
            "observation_id": str(uuid4()),
            "deduped": False,
        }

    monkeypatch.setattr(instagram_router, "InstagramClient", FakeInstagramClient)
    monkeypatch.setattr(instagram_router, "_ingest_item", fake_ingest_item)
    payload = {
        "object": "instagram",
        "entry": [{
            "id": delivery_account_id,
            "messaging": [{
                "sender": {"id": "customer-1"},
                "recipient": {"id": delivery_account_id},
                "timestamp": 1781000000000,
                "message": {"mid": "mid-alias", "text": "hello"},
            }],
        }],
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    response = await client.post(
        "/integrations/instagram/webhook",
        content=raw,
        headers={"X-Hub-Signature-256": sign_payload("deployment-secret", raw)},
    )

    assert response.status_code == 200, response.text
    assert captured[0]["ig_business_account_id"] == business_id
    assert await pool.fetchval(
        """
        SELECT webhook_delivery_account_id
          FROM instagram_webhook_routes
         WHERE resolved_tenant_id = $1
        """,
        tenant_id,
    ) == delivery_account_id


async def test_disconnect_disables_route_and_deletes_tenant_token(
    gateway_pool: asyncpg.Pool,
) -> None:
    tenant_id = uuid4()
    app = FastAPI()
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())
    app.state.pool = gateway_pool
    app.state.secret_store = store

    @app.middleware("http")
    async def authenticated_tenant(request, call_next):
        request.state.auth = SimpleNamespace(tenant_id=tenant_id)
        return await call_next(request)

    app.include_router(instagram_oauth.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        _tenant, business_id = await _register(
            gateway_pool,
            store,
            tenant_id=tenant_id,
        )
        response = await client.post("/integrations/instagram/disconnect")

    assert response.status_code == 200
    assert response.json() == {"ok": True, "disconnected": True}
    install = await gateway_pool.fetchrow(
        """
        SELECT disabled_at, access_token_ref, connection_status
          FROM instagram_installations
         WHERE tenant_id = $1 AND ig_business_account_id = $2
        """,
        tenant_id,
        business_id,
    )
    route_enabled = await gateway_pool.fetchval(
        "SELECT enabled FROM instagram_webhook_routes WHERE ig_business_account_id = $1",
        business_id,
    )
    assert install["disabled_at"] is not None
    assert install["access_token_ref"] is None
    assert install["connection_status"] == "revoked"
    assert route_enabled is False


async def test_status_serializes_installation_timestamps(
    gateway_pool: asyncpg.Pool,
) -> None:
    tenant_id = uuid4()
    app = FastAPI()
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())
    app.state.pool = gateway_pool
    app.state.secret_store = store

    @app.middleware("http")
    async def authenticated_tenant(request, call_next):
        request.state.auth = SimpleNamespace(tenant_id=tenant_id)
        return await call_next(request)

    app.include_router(instagram_oauth.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        _tenant, business_id = await _register(
            gateway_pool,
            store,
            tenant_id=tenant_id,
        )
        await gateway_pool.execute(
            """
            UPDATE instagram_installations
               SET token_expires_at = now(), webhook_subscribed_at = now(),
                   conversation_discovered_at = now(), last_error_at = now()
             WHERE tenant_id = $1 AND ig_business_account_id = $2
            """,
            tenant_id,
            business_id,
        )
        response = await client.get("/integrations/instagram/status")

    assert response.status_code == 200
    body = response.json()
    assert body["connected"] is True
    assert body["installation"]["token_expires_at"]
    assert body["installation"]["webhook_subscribed_at"]
    assert body["installation"]["conversation_discovered_at"]
    assert body["installation"]["last_error_at"]


async def test_oauth_callback_creates_a_routable_installation(
    gateway_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    app = FastAPI()
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())
    app.state.pool = gateway_pool
    app.state.secret_store = store
    monkeypatch.setenv("INSTAGRAM_APP_ID", "app-123")
    monkeypatch.setenv("INSTAGRAM_APP_SECRET", "app-secret")
    monkeypatch.setenv("INSTAGRAM_OAUTH_REDIRECT_URI", "http://test/integrations/instagram/callback")

    @app.middleware("http")
    async def authenticated_tenant(request, call_next):
        request.state.auth = SimpleNamespace(tenant_id=tenant_id)
        return await call_next(request)

    async def fake_exchange(_code: str) -> dict[str, object]:
        return {
            "access_token": "tenant-ig-token",
            "expires_in": 3600,
            "user_id": "meta-delivery-account-id",
        }

    async def fake_discover_and_subscribe(**_kwargs):
        return (
            {"id": "ig-business", "username": "fyralis-test", "name": "Fyralis Test"},
            [{
                "id": "conv-1",
                "updated_time": "2026-07-10T12:00:00+00:00",
                "participants": {"data": [
                    {"id": "ig-business"},
                    {"id": "customer-1", "username": "customer"},
                ]},
            }],
            ["messages", "messaging_seen"],
        )

    monkeypatch.setattr(instagram_oauth, "_exchange_code", fake_exchange)
    monkeypatch.setattr(
        instagram_oauth,
        "_discover_and_subscribe",
        fake_discover_and_subscribe,
    )
    app.include_router(build_integrations_router())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        await gateway_pool.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2)",
            tenant_id,
            "instagram-oauth-test",
        )
        install_response = await client.get("/integrations/instagram/install")
        state = parse_qs(urlparse(install_response.headers["location"]).query)["state"][0]
        callback_response = await client.get(
            "/integrations/instagram/callback",
            params={"code": "authorization-code", "state": state},
        )

    assert install_response.status_code == 302
    assert callback_response.status_code == 200
    install = await gateway_pool.fetchrow(
        """
        SELECT id, access_token_ref, business_actor_id, connection_status
          FROM instagram_installations
         WHERE tenant_id = $1 AND ig_business_account_id = 'ig-business'
        """,
        tenant_id,
    )
    assert install is not None
    assert install["business_actor_id"] is not None
    assert install["connection_status"] == "active"
    route_alias = await gateway_pool.fetchval(
        """
        SELECT webhook_delivery_account_id
          FROM instagram_webhook_routes
         WHERE instagram_installation_id = $1
        """,
        install["id"],
    )
    assert route_alias == "meta-delivery-account-id"
    assert (await store.get(install["access_token_ref"], tenant_id=tenant_id)).decode() == "tenant-ig-token"
    assert await gateway_pool.fetchval(
        "SELECT enabled FROM instagram_webhook_routes WHERE instagram_installation_id = $1",
        install["id"],
    ) is True
    assert await gateway_pool.fetchval(
        "SELECT count(*) FROM instagram_conversations WHERE instagram_installation_id = $1",
        install["id"],
    ) == 1
    assert await gateway_pool.fetchval(
        "SELECT count(*) FROM onboarding_triggers WHERE tenant_id = $1 AND source = 'instagram'",
        tenant_id,
    ) == 1
