"""services/integrations/notion/webhook.py tests (IN-14).

Covers the two router-invoked entry points:
  * verification handshake detection + ack
  * event path: page fetch → shadow-write, with the ignore/skip branches
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import UUID

import pytest

from lib.shared.errors import NotionApiError
from services.integrations.notion import webhook


TENANT = UUID("00000000-0000-0000-0000-000000000001")


# ---------------------------------------------------------------------
# Handshake
# ---------------------------------------------------------------------

def test_is_verification_handshake_true() -> None:
    assert webhook.is_verification_handshake({"verification_token": "secret_x"})


def test_is_verification_handshake_false() -> None:
    assert not webhook.is_verification_handshake({"verification_token": ""})
    assert not webhook.is_verification_handshake({"verification_token": None})
    assert not webhook.is_verification_handshake({"entity": {"id": "p1"}})
    assert not webhook.is_verification_handshake(None)
    assert not webhook.is_verification_handshake("not-a-dict")


def test_handle_verification_handshake_acks_200() -> None:
    resp = webhook.handle_verification_handshake(
        {"verification_token": "secret_tok"},
    )
    assert resp.status_code == 200
    assert json.loads(resp.body) == {"handled": "verification"}


# ---------------------------------------------------------------------
# Event path — fixtures
# ---------------------------------------------------------------------

class _FakeClient:
    def __init__(self, page: dict | None = None, error: Exception | None = None):
        self._page = page
        self._error = error
        self.closed = False
        self.requested_id: str | None = None

    async def retrieve_page(self, page_id: str) -> dict:
        self.requested_id = page_id
        if self._error is not None:
            raise self._error
        return dict(self._page or {})

    async def aclose(self) -> None:
        self.closed = True


def _request_with_shadow_deps() -> SimpleNamespace:
    """A request whose app.state has shadow deps wired and no flag store
    (so the shadow write is attempted)."""
    state = SimpleNamespace(
        kafka_producer=object(),
        s3_raw_client=object(),
        tenant_flags=None,
    )
    return SimpleNamespace(app=SimpleNamespace(state=state))


def _outcome() -> SimpleNamespace:
    return SimpleNamespace(tenant_id=TENANT, secret_ref="install:ref")


@pytest.fixture
def patch_client(monkeypatch):
    """Patch build_notion_client to return a supplied fake client."""
    def _install(client: _FakeClient) -> None:
        async def _build(install, *, pool=None):
            return client
        monkeypatch.setattr(
            "services.ingestion.fetchers._clients.build_notion_client", _build,
        )
    return _install


@pytest.fixture
def capture_shadow(monkeypatch):
    """Capture shadow_write_raw kwargs instead of touching S3/Kafka."""
    calls: list[dict] = []

    async def _fake(**kwargs):
        calls.append(kwargs)
        return "s3://fake/key"

    monkeypatch.setattr(webhook, "shadow_write_raw", _fake)
    return calls


# ---------------------------------------------------------------------
# Event path — cases
# ---------------------------------------------------------------------

@pytest.mark.asyncio
async def test_page_event_fetches_and_shadow_writes(patch_client, capture_shadow) -> None:
    page = {"object": "page", "id": "page-123", "last_edited_time": "2026-05-01T00:00:00Z"}
    client = _FakeClient(page=page)
    patch_client(client)

    resp = await webhook.handle_notion_event(
        request=_request_with_shadow_deps(),
        outcome=_outcome(),
        payload={
            "workspace_id": "ws-1",
            "type": "page.content_updated",
            "entity": {"id": "page-123", "type": "page"},
        },
    )

    assert resp.status_code == 200
    body = json.loads(resp.body)
    assert body["handled"] == "event"
    assert body["shadow_write"] is True
    assert client.requested_id == "page-123"
    assert client.closed is True

    # One shadow write of the fetched page, enriched with workspace id.
    assert len(capture_shadow) == 1
    call = capture_shadow[0]
    assert call["source"] == "notion"
    assert call["ingress_kind"] == "webhook"
    assert call["tenant_id"] == TENANT
    written = json.loads(call["raw_body"])
    assert written["id"] == "page-123"
    assert written["_fyralis_workspace_id"] == "ws-1"
    assert call["ingress_metadata"]["event_type"] == "page.content_updated"


@pytest.mark.asyncio
async def test_non_page_entity_ignored(patch_client, capture_shadow) -> None:
    client = _FakeClient(page={})
    patch_client(client)

    resp = await webhook.handle_notion_event(
        request=_request_with_shadow_deps(),
        outcome=_outcome(),
        payload={
            "workspace_id": "ws-1",
            "type": "comment.created",
            "entity": {"id": "c-1", "type": "comment"},
        },
    )
    assert resp.status_code == 200
    assert json.loads(resp.body)["reason"] == "unsupported_entity"
    assert client.requested_id is None  # never fetched
    assert capture_shadow == []  # never shadow-written


@pytest.mark.asyncio
async def test_fetch_404_acks_without_shadow_write(patch_client, capture_shadow) -> None:
    client = _FakeClient(error=NotionApiError(
        "not found", code="notion_api_not_found", context={"http_status": 404},
    ))
    patch_client(client)

    resp = await webhook.handle_notion_event(
        request=_request_with_shadow_deps(),
        outcome=_outcome(),
        payload={
            "workspace_id": "ws-1",
            "type": "page.deleted",
            "entity": {"id": "gone", "type": "page"},
        },
    )
    assert resp.status_code == 200
    assert json.loads(resp.body)["reason"] == "fetch_failed"
    assert client.closed is True
    assert capture_shadow == []


@pytest.mark.asyncio
async def test_shadow_skipped_when_deps_unwired(patch_client, capture_shadow) -> None:
    """No kafka/s3 on app.state ⇒ shadow_write is skipped, still 200."""
    page = {"object": "page", "id": "p9"}
    client = _FakeClient(page=page)
    patch_client(client)

    state = SimpleNamespace(kafka_producer=None, s3_raw_client=None, tenant_flags=None)
    request = SimpleNamespace(app=SimpleNamespace(state=state))

    resp = await webhook.handle_notion_event(
        request=request,
        outcome=_outcome(),
        payload={"workspace_id": "ws", "type": "page.created", "entity": {"id": "p9", "type": "page"}},
    )
    assert resp.status_code == 200
    assert json.loads(resp.body)["shadow_write"] is False
    assert capture_shadow == []
