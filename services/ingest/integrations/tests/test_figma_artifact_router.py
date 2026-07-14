"""Tenant-bound retrieval tests for durable Figma design artifacts."""
from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI, Request

from services.ingest.integrations.figma import artifact_router


pytestmark = pytest.mark.asyncio


class _Auth:
    def __init__(self, tenant_id: UUID) -> None:
        self.tenant_id = tenant_id


class _Conn:
    def __init__(self, row):
        self.row = row
        self.calls: list[tuple[str, tuple]] = []

    async def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        return self.row


def _app(*, tenant_id: UUID | None, conn: _Conn | None) -> FastAPI:
    app = FastAPI()
    app.state.pool = object()

    @app.middleware("http")
    async def inject_auth(request: Request, call_next):  # type: ignore[no-untyped-def]
        if tenant_id is not None:
            request.state.auth = _Auth(tenant_id)
        return await call_next(request)

    app.include_router(artifact_router.router)
    return app


def _patch_transaction(monkeypatch: pytest.MonkeyPatch, conn: _Conn) -> None:
    @asynccontextmanager
    async def fake_transaction(tenant_id, *, pool):  # type: ignore[no-untyped-def]
        assert pool is not None
        yield conn

    monkeypatch.setattr(artifact_router, "tenant_transaction", fake_transaction)


async def test_returns_json_only_after_tenant_scoped_snapshot_link_check(monkeypatch):
    tenant_id = uuid4()
    observation_id = uuid4()
    blob_id = uuid4()
    conn = _Conn({
        "bucket": "private-fyralis-blobs",
        "object_key": "prod/artifacts/figma/private-design.json",
        "content_hash": "blake2b:abc123",
        "content_type": "application/json",
        "content_encoding": None,
        "size_bytes": 22,
    })
    _patch_transaction(monkeypatch, conn)
    seen: dict[str, str] = {}

    async def fake_load(*, bucket: str, object_key: str, expected_content_hash: str) -> bytes:
        seen.update({
            "bucket": bucket,
            "object_key": object_key,
            "hash": expected_content_hash,
        })
        return b'{"name":"Checkout"}'

    monkeypatch.setattr(artifact_router, "_load_artifact_bytes", fake_load)
    app = _app(tenant_id=tenant_id, conn=conn)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        response = await client.get(
            f"/observations/{observation_id}/artifacts/{blob_id}",
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert response.headers["cache-control"] == "private, no-store"
    assert response.json() == {"name": "Checkout"}
    # The storage locator is used only inside the server-side read helper.
    assert "private-fyralis-blobs" not in response.text
    assert "private-design.json" not in response.text
    assert seen == {
        "bucket": "private-fyralis-blobs",
        "object_key": "prod/artifacts/figma/private-design.json",
        "hash": "abc123",
    }
    query, args = conn.calls[0]
    assert "observation_artifacts" in query
    assert "figma:file_snapshot" in query
    assert args == (tenant_id, observation_id, blob_id)


async def test_missing_or_cross_tenant_snapshot_is_a_single_not_found(monkeypatch):
    tenant_id = uuid4()
    conn = _Conn(None)
    _patch_transaction(monkeypatch, conn)

    async def must_not_load(**kwargs):
        raise AssertionError("no S3 read without a tenant-authorized catalog row")

    monkeypatch.setattr(artifact_router, "_load_artifact_bytes", must_not_load)
    app = _app(tenant_id=tenant_id, conn=conn)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        response = await client.get(f"/observations/{uuid4()}/artifacts/{uuid4()}")

    assert response.status_code == 404
    assert response.json() == {"detail": "artifact not found"}


async def test_missing_authentication_is_rejected_before_database_lookup():
    app = _app(tenant_id=None, conn=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        response = await client.get(f"/observations/{uuid4()}/artifacts/{uuid4()}")

    assert response.status_code == 401
    assert response.json() == {"detail": "unauthenticated"}


async def test_storage_or_integrity_failure_does_not_expose_locator(monkeypatch):
    tenant_id = uuid4()
    conn = _Conn({
        "bucket": "private-fyralis-blobs",
        "object_key": "prod/artifacts/figma/private-design.json",
        "content_hash": "blake2b:abc123",
        "content_type": "application/json",
        "content_encoding": None,
        "size_bytes": 22,
    })
    _patch_transaction(monkeypatch, conn)

    async def fail_load(**kwargs):
        raise ValueError("hash mismatch: private-fyralis-blobs/private-design.json")

    monkeypatch.setattr(artifact_router, "_load_artifact_bytes", fail_load)
    app = _app(tenant_id=tenant_id, conn=conn)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        response = await client.get(f"/observations/{uuid4()}/artifacts/{uuid4()}")

    assert response.status_code == 502
    assert response.json() == {"detail": "artifact unavailable"}
    assert "private-fyralis-blobs" not in response.text
    assert "private-design.json" not in response.text
