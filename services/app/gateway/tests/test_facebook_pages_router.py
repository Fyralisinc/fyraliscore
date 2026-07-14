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
from services.app.gateway import facebook_pages_router
from services.app.gateway.facebook_pages_router import build_facebook_pages_router
from services.ingest.integrations.whatsapp.signature import sign_payload


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def facebook_pages_client(
    gateway_pool: asyncpg.Pool,
    app_deps: Any,
) -> AsyncGenerator[tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore], None]:
    app = FastAPI()
    app.state.deps = app_deps
    store = FernetSecretStore(gateway_pool, master_kek=Fernet.generate_key())
    app.state.secret_store = store
    app.include_router(build_facebook_pages_router())

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, gateway_pool, store


async def _register(
    pool: asyncpg.Pool,
    store: FernetSecretStore,
    *,
    tenant_id: UUID | None = None,
    page_id: str | None = None,
    app_secret: str = "app-secret-123",
    verify_token: str = "verify-token-123",
) -> tuple[UUID, str]:
    tenant_id = tenant_id or uuid4()
    page_id = page_id or f"page-{uuid4().hex}"
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        tenant_id,
        "facebook-pages-test",
    )
    token_ref = await store.put(
        "page-token",
        label=f"facebook_pages_page_token:{page_id}",
        tenant_id=tenant_id,
    )
    app_secret_ref = await store.put(
        app_secret,
        label=f"facebook_pages_app_secret:{page_id}",
        tenant_id=tenant_id,
    )
    verify_token_ref = await store.put(
        verify_token,
        label=f"facebook_pages_verify_token:{page_id}",
        tenant_id=tenant_id,
    )
    await pool.execute(
        """
        INSERT INTO facebook_page_installations (
            tenant_id, page_id, page_name, page_access_token_ref,
            app_secret_ref, verify_token_ref, enabled
        ) VALUES ($1,$2,'Acme Page',$3,$4,$5,true)
        ON CONFLICT (page_id) DO UPDATE SET
            tenant_id = EXCLUDED.tenant_id,
            page_access_token_ref = EXCLUDED.page_access_token_ref,
            app_secret_ref = EXCLUDED.app_secret_ref,
            verify_token_ref = EXCLUDED.verify_token_ref,
            enabled = true
        """,
        tenant_id,
        page_id,
        token_ref,
        app_secret_ref,
        verify_token_ref,
    )
    return tenant_id, page_id


async def test_verify_challenge_uses_db_verify_token(
    facebook_pages_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, pool, store = facebook_pages_client
    monkeypatch.delenv("FACEBOOK_WEBHOOK_VERIFY_TOKEN", raising=False)
    await _register(pool, store, verify_token="db-token")

    response = await client.get(
        "/integrations/facebook_pages/webhook",
        params={
            "hub.mode": "subscribe",
            "hub.verify_token": "db-token",
            "hub.challenge": "challenge-ok",
        },
    )

    assert response.status_code == 200
    assert response.text == "challenge-ok"


async def test_signed_webhook_fans_out_messages(
    facebook_pages_client: tuple[httpx.AsyncClient, asyncpg.Pool, FernetSecretStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, pool, store = facebook_pages_client
    tenant_id, page_id = await _register(
        pool,
        store,
        app_secret="db-backed-app-secret",
    )
    captured: list[dict[str, Any]] = []

    async def fake_ingest_item(
        deps: Any,
        tenant_id: UUID,
        item_payload: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, Any]:
        captured.append(
            {
                "tenant_id": tenant_id,
                "payload": item_payload,
                "headers": headers,
            }
        )
        return {
            "channel": "facebook_pages:message",
            "observation_id": str(uuid4()),
            "deduped": False,
        }

    monkeypatch.setattr(facebook_pages_router, "_ingest_item", fake_ingest_item)
    payload = {
        "entry": [
            {
                "id": page_id,
                "messaging": [
                    {
                        "sender": {"id": "PSID1"},
                        "recipient": {"id": page_id},
                        "timestamp": 1_704_067_200_000,
                        "message": {"mid": "m_1", "text": "hello"},
                    },
                    {"sender": {"id": "PSID1"}, "delivery": {"mids": ["m_1"]}},
                ],
            }
        ]
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    response = await client.post(
        "/integrations/facebook_pages/webhook",
        content=raw,
        headers={
            "Content-Type": "application/json",
            "X-Hub-Signature-256": sign_payload("db-backed-app-secret", raw),
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["ingested"] == 1
    assert captured[0]["tenant_id"] == tenant_id
    assert captured[0]["headers"] == {"x-facebook-page-id": page_id}
    assert captured[0]["payload"]["message"]["mid"] == "m_1"
