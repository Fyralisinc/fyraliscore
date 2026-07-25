"""Exact-installation contract for shared OAuth native-connect finalization."""
from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import httpx
from fastapi import FastAPI, Request

from services.ingest.integrations.oauth_native_connect import (
    build_oauth_native_connect_router,
)


class _Pool:
    def __init__(self, row=None) -> None:
        self.row = row
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query: str, *args: object):
        self.calls.append((query, args))
        return self.row


def _app(pool: _Pool, *, tenant_id) -> FastAPI:
    app = FastAPI()
    app.state.pool = pool

    @app.middleware("http")
    async def _auth(request: Request, call_next):
        request.state.auth = SimpleNamespace(tenant_id=tenant_id)
        return await call_next(request)

    async def _handoff(_tenant_id, _pool, _request, _body):
        return {
            "install_url": "https://provider.test/install",
            "missing_configuration": [],
        }

    app.include_router(
        build_oauth_native_connect_router(
            source="slack",
            authorization_mode="oauth",
            provider_console_url="https://provider.test/console",
            payload_fields=[],
            build_handoff=_handoff,
        )
    )
    return app


async def test_finalize_without_identity_never_selects_latest_installation() -> None:
    pool = _Pool(
        row={
            "installation_id": "sibling-install",
            "enabled": True,
            "installed_at": None,
        }
    )
    app = _app(pool, tenant_id=uuid4())

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gateway.test",
    ) as client:
        response = await client.post(
            "/integrations/slack/connect/finalize",
            json={},
        )

    assert response.status_code == 202
    assert response.json()["state"] == "waiting_for_provider_callback"
    assert pool.calls == []


async def test_finalize_loads_only_the_named_provider_installation() -> None:
    tenant_id = uuid4()
    pool = _Pool(
        row={
            "installation_id": "T_EXACT",
            "enabled": True,
            "installed_at": None,
        }
    )
    app = _app(pool, tenant_id=tenant_id)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://gateway.test",
    ) as client:
        response = await client.post(
            "/integrations/slack/connect/finalize",
            json={"installation_id": "T_EXACT"},
        )

    assert response.status_code == 200
    assert response.json()["installation_id"] == "T_EXACT"
    [(query, args)] = pool.calls
    assert "installation_id = $3" in query
    assert "LIMIT 1" not in query.upper()
    assert args == (tenant_id, "slack", "T_EXACT")
