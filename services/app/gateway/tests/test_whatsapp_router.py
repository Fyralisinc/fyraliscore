from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.secrets import FernetSecretStore
from services.app.gateway import whatsapp_router
from services.app.gateway.whatsapp_router import build_whatsapp_router
from services.ingest.integrations.whatsapp.signature import sign_payload


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def whatsapp_client(
    gateway_pool: asyncpg.Pool,
    app_deps: Any,
) -> AsyncGenerator[tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore], None]:
    app = FastAPI()
    app.state.deps = app_deps
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())
    app.state.secret_store = store
    app.include_router(build_whatsapp_router(debug_endpoints_enabled=True))

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        yield client, gateway_pool, store


async def _register(
    client: httpx.AsyncClient,
    *,
    tenant_id: UUID | None = None,
    phone_number_id: str | None = None,
    app_secret: str = "app-secret-123",
    verify_token: str = "verify-token-123",
    access_token: str = "graph-token-123",
) -> tuple[UUID, str, dict[str, Any]]:
    tenant_id = tenant_id or uuid4()
    phone_number_id = phone_number_id or f"phone-{uuid4().hex}"
    response = await client.post(
        "/debug/whatsapp/register",
        json={
            "tenant_id": str(tenant_id),
            "phone_number_id": phone_number_id,
            "waba_id": f"waba-{phone_number_id}",
            "display_phone_number": "+15550000000",
            "app_secret": app_secret,
            "verify_token": verify_token,
            "access_token": access_token,
        },
    )
    assert response.status_code == 200, response.text
    return tenant_id, phone_number_id, response.json()


async def test_debug_viewer_uses_nonce_csp_and_security_headers(
    whatsapp_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
) -> None:
    client, _pool, _store = whatsapp_client

    response = await client.get("/debug/whatsapp")

    assert response.status_code == 200
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert response.headers["cache-control"] == "no-store"
    body = response.text
    assert "__CSP_NONCE__" not in body
    nonce = body.split('<script nonce="', 1)[1].split('"', 1)[0]
    assert f"script-src 'nonce-{nonce}'" in response.headers["content-security-policy"]
    assert f"style-src 'nonce-{nonce}'" in response.headers["content-security-policy"]


async def test_debug_register_stores_whatsapp_credentials_as_secret_refs(
    whatsapp_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
) -> None:
    client, pool, store = whatsapp_client

    tenant_id, phone_number_id, body = await _register(client)

    installation = body["installation"]
    assert installation["has_app_secret_ref"] is True
    assert installation["has_verify_token_ref"] is True
    assert installation["has_access_token_ref"] is True
    assert "app_secret_ref" not in installation
    assert "verify_token_ref" not in installation
    assert "access_token_ref" not in installation

    row = await pool.fetchrow(
        """
        SELECT app_secret, verify_token, access_token,
               app_secret_ref, verify_token_ref, access_token_ref
          FROM whatsapp_installations
         WHERE phone_number_id = $1
        """,
        phone_number_id,
    )
    assert row is not None
    assert row["app_secret"] is None
    assert row["verify_token"] is None
    assert row["access_token"] is None

    assert (
        await store.get(row["app_secret_ref"], tenant_id=tenant_id)
        == b"app-secret-123"
    )
    assert (
        await store.get(row["verify_token_ref"], tenant_id=tenant_id)
        == b"verify-token-123"
    )
    assert (
        await store.get(row["access_token_ref"], tenant_id=tenant_id)
        == b"graph-token-123"
    )


async def test_webhook_verify_matches_verify_token_secret_ref(
    whatsapp_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _pool, _store = whatsapp_client
    monkeypatch.delenv("WHATSAPP_VERIFY_TOKEN", raising=False)
    await _register(client, verify_token="db-backed-verify-token")

    response = await client.get(
        "/integrations/whatsapp/webhook",
        params={
            "hub.mode": "subscribe",
            "hub.verify_token": "db-backed-verify-token",
            "hub.challenge": "challenge-ok",
        },
    )

    assert response.status_code == 200
    assert response.text == "challenge-ok"


async def test_signed_webhook_resolves_app_secret_ref(
    whatsapp_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _pool, _store = whatsapp_client
    _tenant_id, phone_number_id, _body = await _register(
        client,
        app_secret="db-backed-app-secret",
    )
    captured: list[dict[str, Any]] = []

    async def fake_ingest_item(
        deps: Any,
        tenant_id: UUID,
        channel: str,
        item_payload: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, Any]:
        captured.append(
            {
                "tenant_id": tenant_id,
                "channel": channel,
                "payload": item_payload,
                "headers": headers,
            }
        )
        return {
            "channel": channel,
            "observation_id": str(uuid4()),
            "deduped": False,
        }

    monkeypatch.setattr(whatsapp_router, "_ingest_item", fake_ingest_item)
    payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "contacts": [
                                {
                                    "wa_id": "15551234567",
                                    "profile": {"name": "Ava"},
                                }
                            ],
                            "messages": [
                                {
                                    "id": "wamid.TEST1",
                                    "from": "15551234567",
                                    "timestamp": "1700000000",
                                    "type": "text",
                                    "text": {"body": "hello"},
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    response = await client.post(
        "/integrations/whatsapp/webhook",
        content=raw,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": sign_payload("db-backed-app-secret", raw),
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["ingested"] == 1
    assert len(captured) == 1
    assert captured[0]["headers"] == {
        "x-whatsapp-phone-number-id": phone_number_id,
    }


async def test_production_ignores_whatsapp_verify_token_env_fallback(
    whatsapp_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _pool, _store = whatsapp_client
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("WHATSAPP_VERIFY_TOKEN", "env-token-must-not-work")

    response = await client.get(
        "/integrations/whatsapp/webhook",
        params={
            "hub.mode": "subscribe",
            "hub.verify_token": "env-token-must-not-work",
            "hub.challenge": "challenge",
        },
    )

    assert response.status_code == 403


async def test_production_ignores_unsigned_webhook_bypass(
    whatsapp_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _pool, _store = whatsapp_client
    monkeypatch.setenv("FYRALIS_ENV", "prod")
    monkeypatch.setenv("WHATSAPP_ALLOW_UNSIGNED", "1")
    _tenant_id, phone_number_id, _body = await _register(client)
    payload = {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": phone_number_id},
                            "messages": [{"id": "wamid.TEST2", "type": "text"}],
                        }
                    }
                ]
            }
        ]
    }

    response = await client.post("/integrations/whatsapp/webhook", json=payload)

    assert response.status_code == 401
    assert response.json()["status"] == "signature_invalid"
