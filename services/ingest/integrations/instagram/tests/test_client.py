from __future__ import annotations

import httpx
import pytest

from services.ingest.integrations.instagram.client import InstagramClient


pytestmark = pytest.mark.asyncio


async def test_conversations_uses_instagram_login_host_and_cursor():
    seen: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["authorization"] = request.headers.get("Authorization")
        return httpx.Response(
            200,
            json={
                "data": [{"id": "conv-1"}],
                "paging": {"cursors": {"after": "next-cursor"}, "next": "https://next"},
            },
        )

    client = InstagramClient(
        access_token="token-value",
        graph_version="v24.0",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    records, after = await client.list_conversations(
        ig_business_account_id="ig-business",
        after="previous-cursor",
    )

    assert records == [{"id": "conv-1"}]
    assert after == "next-cursor"
    assert str(seen["url"]).startswith(
        "https://graph.instagram.com/v24.0/ig-business/conversations?"
    )
    assert "after=previous-cursor" in str(seen["url"])
    assert seen["authorization"] == "Bearer token-value"


async def test_subscribe_webhooks_sends_fields_as_graph_query_params():
    seen: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        seen["method"] = request.method
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"success": True})

    client = InstagramClient(
        access_token="token-value",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )
    await client.subscribe_webhooks(
        ig_business_account_id="ig-business",
        fields=["messages", "messaging_seen"],
    )

    assert seen["method"] == "POST"
    assert "subscribed_fields=messages%2Cmessaging_seen" in str(seen["url"])
