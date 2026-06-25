from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from services.product.conversations.api import build_router


class _Decision:
    allowed = True
    reason = None


class _Repo:
    async def card_access_decision(self, **kwargs):
        return _Decision()


class _Handler:
    async def probe(self, request):
        raise ValueError("query required for ask probes password=super-secret")


async def test_probe_validation_error_is_bounded() -> None:
    app = FastAPI()

    @app.middleware("http")
    async def _inject_auth(request, call_next):
        request.state.auth = type(
            "Auth",
            (),
            {"tenant_id": uuid4(), "actor_id": uuid4()},
        )()
        return await call_next(request)

    app.include_router(build_router(repo=_Repo(), handler=_Handler()))

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            f"/v1/cards/{uuid4()}/probe",
            json={"kind": "ask"},
        )

    assert response.status_code == 400
    assert response.json() == {"detail": "invalid_probe"}
    assert "super-secret" not in response.text
