"""NotionWebhookGenerator — synthetic Notion webhook deliveries (IN-14).

Drives the PRODUCTION Notion live path in-process:

  Generator → POST /webhooks/notion  (thin {type, workspace_id, entity})
                 signed `X-Notion-Signature: sha256=<hex>` with the
                 app-level NOTION_WEBHOOK_VERIFICATION_TOKEN
            → router verify (signatures/notion.py) + tenant resolve
                 (workspace_id → provider_installations(provider='notion'))
            → notion_webhook.handle_notion_event → build_notion_client
                 (here monkeypatched) → client.retrieve_page(entity_id)
            → _shadow_write_page → ingestion.raw.notion (S3 + Kafka via
                 app.state.notion_data_plane) → normalizer → observation_writer.

Notion is NOT a 202 Kafka-cutover provider; its live path fetches the changed
page and shadow-writes it onto the data plane. So the live observation flows
through the SAME normalizer→observation_writer Kafka chain as backfill, landing
in `observations` while backfill is still in flight.

Each `simulate_event` mints a brand-new page (unique id + a current
partition-window timestamp), so N events ⇒ N distinct observations
(`external_id = notion:page:{id}`).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import httpx
from fastapi import FastAPI


log = logging.getLogger(__name__)

_LIVE_BASE_MS = 1781000000000


def _iso(ms: int) -> str:
    return (
        time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000.0))
        + f".{ms % 1000:03d}Z"
    )


@dataclass
class NotionWebhookResult:
    http_status: int
    external_hint: str
    shadow_written: bool
    tenant_id: UUID | None = None
    was_tamper: bool = False


class _LiveNotionClient:
    """Minimal NotionClient surface the webhook path calls: `retrieve_page`
    + `aclose`. Returns a fresh page object keyed by the requested id so the
    handler derives `external_id = notion:page:{id}`."""

    def __init__(self, pages: dict[str, dict[str, Any]]) -> None:
        self._pages = pages

    async def retrieve_page(self, page_id: str) -> dict[str, Any]:
        return self._pages[page_id]

    async def aclose(self) -> None:
        return None


class NotionWebhookGenerator:
    """Drives Notion live webhook ingress against the shared gateway app."""

    def __init__(
        self,
        *,
        app: FastAPI,
        kafka_producer: Any,
        s3_raw_client: Any,
        verification_token: str,
    ) -> None:
        self._app = app
        self._producer = kafka_producer
        self._s3 = s3_raw_client
        self._token = verification_token
        self._exit_stack = AsyncExitStack()
        self._client: httpx.AsyncClient | None = None
        self._seq = 0
        self._pages: dict[str, dict[str, Any]] = {}
        self._patches: list[tuple[Any, str, Any]] = []
        self._prev_ndp: Any = None
        self._had_ndp = False

    async def __aenter__(self) -> "NotionWebhookGenerator":
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self._app),
            base_url="http://live-notion",
        )
        await self._exit_stack.enter_async_context(self._client)

        # Wire the Notion-scoped data plane the shadow-write reads.
        self._had_ndp = hasattr(self._app.state, "notion_data_plane")
        self._prev_ndp = getattr(self._app.state, "notion_data_plane", None)
        self._app.state.notion_data_plane = SimpleNamespace(
            producer=self._producer, s3_client=self._s3,
        )

        # Monkeypatch build_notion_client so retrieve_page returns our mint.
        from services.ingest.ingestion.fetchers import _clients as _c
        live_client = _LiveNotionClient(self._pages)

        async def _build(_install):  # noqa: ANN001, ANN202
            return live_client

        self._patches.append((_c, "build_notion_client", _c.build_notion_client))
        _c.build_notion_client = _build  # type: ignore[assignment]
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        for mod, name, original in reversed(self._patches):
            setattr(mod, name, original)
        self._patches.clear()
        if self._had_ndp:
            self._app.state.notion_data_plane = self._prev_ndp
        await self._exit_stack.aclose()

    def _sign(self, body: bytes) -> str:
        mac = hmac.new(self._token.encode("utf-8"), body, hashlib.sha256)
        return "sha256=" + mac.hexdigest()

    def _mint_page(self, workspace_id: str) -> str:
        self._seq += 1
        ms = _LIVE_BASE_MS + self._seq * 1000
        page_id = f"live-{workspace_id}-{self._seq}"
        self._pages[page_id] = {
            "object": "page",
            "id": page_id,
            "created_time": _iso(ms),
            "last_edited_time": _iso(ms),
            "created_by": {"object": "user", "id": f"user-{workspace_id}"},
            "last_edited_by": {"object": "user", "id": f"user-{workspace_id}"},
            "parent": {"type": "workspace", "workspace": True},
            "url": f"https://notion.so/{page_id}",
            "properties": {
                "Name": {
                    "type": "title",
                    "title": [{
                        "type": "text",
                        "text": {"content": f"live page {self._seq}"},
                        "plain_text": f"live page {self._seq}",
                    }],
                },
            },
        }
        return page_id

    async def simulate_event(
        self, *, target: "Any", tamper_signature: bool = False,
    ) -> NotionWebhookResult:
        """Mint a fresh page + POST a signed `page.content_updated` event."""
        assert self._client is not None
        workspace_id = target.notion_workspace_id
        page_id = self._mint_page(workspace_id)
        payload = {
            "type": "page.content_updated",
            "workspace_id": workspace_id,
            "entity": {"id": page_id, "type": "page"},
        }
        body = json.dumps(payload).encode("utf-8")
        signature = (
            "sha256=" + ("f" * 64) if tamper_signature else self._sign(body)
        )
        response = await self._client.post(
            "/webhooks/notion",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Notion-Signature": signature,
            },
        )
        try:
            data = response.json()
        except Exception:  # noqa: BLE001
            data = {}
        return NotionWebhookResult(
            http_status=response.status_code,
            external_hint=f"notion:page:{page_id}",
            shadow_written=bool(data.get("shadow_write")) if isinstance(data, dict) else False,
            tenant_id=getattr(target, "tenant_id", None),
            was_tamper=tamper_signature,
        )


__all__ = ["NotionWebhookGenerator", "NotionWebhookResult"]
