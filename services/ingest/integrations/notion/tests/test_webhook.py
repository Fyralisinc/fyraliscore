"""services/ingest/integrations/notion/webhook.py tests (IN-14).

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
from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.integrations.notion import webhook


TENANT = UUID("00000000-0000-0000-0000-000000000001")
INSTALLATION = UUID("00000000-0000-0000-0000-000000000002")


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
        self.install: dict | None = None

    async def retrieve_page(self, page_id: str) -> dict:
        self.requested_id = page_id
        if self._error is not None:
            raise self._error
        return dict(self._page or {})

    async def aclose(self) -> None:
        self.closed = True


class _FakeProducer:
    def __init__(self) -> None:
        self.produced: list[dict] = []

    async def produce(self, *, topic, value, key):
        self.produced.append({"topic": topic, "value": value, "key": key})

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        return 0  # all delivered


class _FakeS3:
    def __init__(self) -> None:
        self.puts: list[tuple[str, bytes]] = []

    async def put_if_absent(self, key: str, body: bytes) -> None:
        self.puts.append((key, body))


def _request_with_data_plane() -> tuple[SimpleNamespace, _FakeProducer, _FakeS3]:
    """A request whose app.state.notion_data_plane carries fake producer +
    S3 client (the Notion-scoped data plane)."""
    producer, s3 = _FakeProducer(), _FakeS3()
    ndp = SimpleNamespace(producer=producer, s3_client=s3)
    state = SimpleNamespace(notion_data_plane=ndp)
    return SimpleNamespace(app=SimpleNamespace(state=state)), producer, s3


def _outcome() -> SimpleNamespace:
    return SimpleNamespace(
        tenant_id=TENANT,
        installation_row_id=INSTALLATION,
        secret_ref="install:ref",
    )


@pytest.fixture
def patch_client(monkeypatch):
    """Patch build_notion_client to return a supplied fake client."""
    def _install(client: _FakeClient) -> None:
        async def _build(install, *, pool=None):
            client.install = install
            return client
        monkeypatch.setattr(
            "services.ingest.ingestion.fetchers._clients.build_notion_client", _build,
        )
    return _install


# ---------------------------------------------------------------------
# Event path — cases
# ---------------------------------------------------------------------

@pytest.mark.asyncio
async def test_page_event_fetches_and_shadow_writes(patch_client) -> None:
    page = {"object": "page", "id": "page-123", "last_edited_time": "2026-05-01T00:00:00Z"}
    client = _FakeClient(page=page)
    patch_client(client)
    request, producer, s3 = _request_with_data_plane()

    resp = await webhook.handle_notion_event(
        request=request,
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
    assert body["inline_fallback"] is False
    assert client.requested_id == "page-123"
    assert client.closed is True
    assert client.install is not None
    assert client.install["id"] == INSTALLATION
    assert client.install["tenant_id"] == TENANT

    # The fetched page (enriched with workspace id) was PUT to S3 and the
    # envelope published to ingestion.raw keyed by tenant.
    assert len(s3.puts) == 1
    written = json.loads(s3.puts[0][1])
    assert written["id"] == "page-123"
    assert written["_fyralis_workspace_id"] == "ws-1"
    assert len(producer.produced) == 1
    # Per-source raw topic (source-isolation): notion -> notion lane.
    assert producer.produced[0]["topic"] == "ingestion.raw.notion"
    assert producer.produced[0]["key"] == str(TENANT).encode()


@pytest.mark.asyncio
async def test_non_page_entity_ignored(patch_client) -> None:
    client = _FakeClient(page={})
    patch_client(client)
    request, producer, s3 = _request_with_data_plane()

    resp = await webhook.handle_notion_event(
        request=request,
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
    assert producer.produced == [] and s3.puts == []  # never written


@pytest.mark.asyncio
async def test_fetch_404_acks_without_write(patch_client) -> None:
    client = _FakeClient(error=NotionApiError(
        "not found", code="notion_api_not_found", context={"http_status": 404},
    ))
    patch_client(client)
    request, producer, s3 = _request_with_data_plane()

    resp = await webhook.handle_notion_event(
        request=request,
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
    assert producer.produced == [] and s3.puts == []


@pytest.mark.asyncio
async def test_retry_later_propagates_without_success_ack(patch_client) -> None:
    retry = RetryLater.after(
        request_context=RequestContext(
            source="notion",
            operation="pages.retrieve",
            tenant_id=str(TENANT),
            installation_id=str(INSTALLATION),
        ),
        delay_seconds=60,
        reason=RetryReason.RATE_LIMIT,
    )
    client = _FakeClient(error=retry)
    patch_client(client)
    request, producer, s3 = _request_with_data_plane()

    with pytest.raises(RetryLater) as raised:
        await webhook.handle_notion_event(
            request=request,
            outcome=_outcome(),
            payload={
                "workspace_id": "ws-1",
                "type": "page.content_updated",
                "entity": {"id": "page-123", "type": "page"},
            },
        )

    assert raised.value is retry
    assert client.closed is True
    assert producer.produced == [] and s3.puts == []


@pytest.mark.asyncio
async def test_data_plane_unwired_falls_back_to_uniform_inline_ingest(
    patch_client,
    monkeypatch,
) -> None:
    """No Notion data plane must persist inline before returning 200."""
    page = {"object": "page", "id": "p9"}
    client = _FakeClient(page=page)
    patch_client(client)

    calls = []

    async def _ingest(channel, payload, **kwargs):
        calls.append((channel, payload, kwargs))

    monkeypatch.setattr(webhook, "ingest", _ingest)
    deps = SimpleNamespace(
        pool=object(),
        actor_repo=object(),
        alias_repo=object(),
        embedder=None,
    )
    state = SimpleNamespace(notion_data_plane=None, deps=deps)
    request = SimpleNamespace(app=SimpleNamespace(state=state))

    resp = await webhook.handle_notion_event(
        request=request,
        outcome=_outcome(),
        payload={"workspace_id": "ws", "type": "page.created", "entity": {"id": "p9", "type": "page"}},
    )
    assert resp.status_code == 200
    body = json.loads(resp.body)
    assert body["shadow_write"] is False
    assert body["inline_fallback"] is True
    assert len(calls) == 1
    channel, payload, kwargs = calls[0]
    assert channel == "notion:object"
    assert payload["id"] == "p9"
    assert payload["_fyralis_workspace_id"] == "ws"
    assert kwargs["tenant_id"] == TENANT
    assert kwargs["pool"] is deps.pool
