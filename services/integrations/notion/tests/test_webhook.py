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


def _request_with_deps() -> SimpleNamespace:
    """A request whose app.state.deps carries the inline-ingest deps."""
    deps = SimpleNamespace(
        pool=object(),
        actor_repo=object(),
        alias_repo=object(),
        embedder=None,
    )
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(deps=deps)))


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
def capture_ingest(monkeypatch):
    """Capture inline ingest() calls instead of touching the DB."""
    calls: list[dict] = []

    async def _fake(channel, payload, **kwargs):
        calls.append({"channel": channel, "payload": payload, **kwargs})
        return SimpleNamespace(
            observation=SimpleNamespace(id=UUID("11111111-1111-1111-1111-111111111111")),
            deduped=False,
            trigger_queue_id=None,
        )

    monkeypatch.setattr(webhook, "ingest", _fake)
    return calls


# ---------------------------------------------------------------------
# Event path — cases
# ---------------------------------------------------------------------

@pytest.mark.asyncio
async def test_page_event_fetches_and_ingests(patch_client, capture_ingest) -> None:
    page = {"object": "page", "id": "page-123", "last_edited_time": "2026-05-01T00:00:00Z"}
    client = _FakeClient(page=page)
    patch_client(client)

    resp = await webhook.handle_notion_event(
        request=_request_with_deps(),
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
    assert body["observation_id"] == "11111111-1111-1111-1111-111111111111"
    assert body["deduped"] is False
    assert client.requested_id == "page-123"
    assert client.closed is True

    # One inline ingest of the fetched page through the notion:object
    # handler, enriched with the workspace id (matches the fetcher).
    assert len(capture_ingest) == 1
    call = capture_ingest[0]
    assert call["channel"] == "notion:object"
    assert call["tenant_id"] == TENANT
    assert call["payload"]["id"] == "page-123"
    assert call["payload"]["_fyralis_workspace_id"] == "ws-1"


@pytest.mark.asyncio
async def test_non_page_entity_ignored(patch_client, capture_ingest) -> None:
    client = _FakeClient(page={})
    patch_client(client)

    resp = await webhook.handle_notion_event(
        request=_request_with_deps(),
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
    assert capture_ingest == []  # never ingested


@pytest.mark.asyncio
async def test_fetch_404_acks_without_ingest(patch_client, capture_ingest) -> None:
    client = _FakeClient(error=NotionApiError(
        "not found", code="notion_api_not_found", context={"http_status": 404},
    ))
    patch_client(client)

    resp = await webhook.handle_notion_event(
        request=_request_with_deps(),
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
    assert capture_ingest == []


@pytest.mark.asyncio
async def test_deduped_page_reports_dedup(patch_client, monkeypatch) -> None:
    """A page already ingested (e.g. via backfill) dedups on
    notion:page:{id}; the response surfaces deduped=true."""
    page = {"object": "page", "id": "p-dup"}
    client = _FakeClient(page=page)
    patch_client(client)

    async def _fake(channel, payload, **kwargs):
        return SimpleNamespace(
            observation=SimpleNamespace(id=UUID("22222222-2222-2222-2222-222222222222")),
            deduped=True,
            trigger_queue_id=None,
        )

    monkeypatch.setattr(webhook, "ingest", _fake)

    resp = await webhook.handle_notion_event(
        request=_request_with_deps(),
        outcome=_outcome(),
        payload={"workspace_id": "ws", "type": "page.content_updated", "entity": {"id": "p-dup", "type": "page"}},
    )
    assert resp.status_code == 200
    assert json.loads(resp.body)["deduped"] is True
