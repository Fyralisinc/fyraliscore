from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.testclient import TestClient

from services.app.gateway.structure_router import build_structure_router


class _Acquire:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class _Pool:
    def __init__(self, conn: object) -> None:
        self.conn = conn

    def acquire(self) -> _Acquire:
        return _Acquire(self.conn)


def _client(
    *,
    tenant_id=None,
    authenticated: bool = True,
    conn: object | None = None,
) -> TestClient:
    tenant_id = tenant_id or uuid4()
    app = FastAPI()
    app.state.deps = SimpleNamespace(pool=_Pool(conn or object()))

    if authenticated:

        class _StubAuthMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request: Request, call_next):
                request.state.auth = SimpleNamespace(
                    tenant_id=tenant_id,
                    actor_id=uuid4(),
                )
                return await call_next(request)

        app.add_middleware(_StubAuthMiddleware)

    app.include_router(build_structure_router())
    return TestClient(app)


def test_structure_overlay_requires_authentication() -> None:
    client = _client(authenticated=False)

    response = client.get(f"/v1/structure/overlay/{uuid4()}")

    assert response.status_code == 401
    assert response.json() == {
        "error": "unauthorized",
        "reason": "missing_bearer",
    }


def test_structure_overlay_rejects_invalid_commitment_id() -> None:
    client = _client()

    response = client.get("/v1/structure/overlay/not-a-uuid")

    assert response.status_code == 400
    assert response.json() == {"error": "invalid_commitment_id"}


def test_structure_resource_overlay_rejects_invalid_resource_id() -> None:
    client = _client()

    response = client.get("/v1/structure/resources/not-a-uuid/overlay")

    assert response.status_code == 400
    assert response.json() == {"error": "invalid_resource_id"}


def test_structure_overlay_delegates_to_overlay_fetcher(monkeypatch) -> None:
    tenant_id = uuid4()
    commitment_id = uuid4()
    conn = object()
    captured = {}

    async def fake_fetch(commitment_uuid, tenant_uuid, acquired_conn):
        captured.update(
            {
                "commitment_id": str(commitment_uuid),
                "tenant_id": str(tenant_uuid),
                "conn": acquired_conn,
            }
        )
        return {
            "commitment": {"id": str(commitment_uuid), "label": "Launch plan"},
            "goals": [],
            "people": [],
            "customers": [],
            "decisions": [],
            "resources": [],
        }

    monkeypatch.setattr(
        "services.app.gateway.structure_router.fetch_commitment_overlay",
        fake_fetch,
    )
    client = _client(tenant_id=tenant_id, conn=conn)

    response = client.get(f"/v1/structure/overlay/{commitment_id}")

    assert response.status_code == 200
    assert response.json()["commitment"]["label"] == "Launch plan"
    assert captured == {
        "commitment_id": str(commitment_id),
        "tenant_id": str(tenant_id),
        "conn": conn,
    }
