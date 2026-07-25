"""Contract test: the Fireflies client speaks GraphQL (finding #5).

The real Fireflies API is a single GraphQL endpoint (POST /graphql) with a
`transcripts` query returning `data.transcripts` — NOT the REST GET paths the
client was cloned with from Brex (which 404 on a real install). The Phase-3 fix
adds the GraphQL read surface (`_graphql` / `list_transcripts_graphql`). This
test pins the request shape (POST /graphql with a `transcripts` query) and the
`data.transcripts` response parsing against a doc-sourced fixture, with no
network and no real token.
"""
from __future__ import annotations

import httpx
import pytest

from lib.shared.provider_transport import (
    RequestPolicy,
    RetryLater,
    RetryReason,
)
from services.ingest.integrations.fireflies.client import FirefliesClient
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.asyncio


def _fixture():
    return load_fixture("fireflies", "api_response", "transcripts_query")


def _client(http: httpx.AsyncClient) -> FirefliesClient:
    return FirefliesClient(
        base_url="https://api.fireflies.ai/graphql",
        api_token="test-token",          # preset → no secret_store needed
        http_client=http,
        request_policy=RequestPolicy(max_attempts=1),
    )


@pytest.mark.contract
async def test_fireflies_graphql_request_and_response_shape():
    fx = _fixture()
    resp_body = fx.response["body"]
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["json"] = httpx.Response(200, content=request.content).json()
        return httpx.Response(int(fx.response["status"]), json=resp_body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        items, next_skip = await _client(http).list_transcripts_graphql(
            limit=50, skip=0,
        )

    # --- request is a GraphQL POST to /graphql with a transcripts query ---
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/graphql")
    assert "transcripts" in captured["json"]["query"]
    assert captured["json"]["variables"]["limit"] == 50
    assert captured["json"]["variables"]["skip"] == 0

    # --- response parsed from data.transcripts (NOT a REST `transcripts` key) ---
    assert [t["id"] for t in items] == [
        t["id"] for t in resp_body["data"]["transcripts"]
    ]
    assert items[0]["title"] == "Weekly Eng Sync"
    # a short page (2 < limit 50) is terminal
    assert next_skip is None


@pytest.mark.contract
async def test_fireflies_graphql_errors_raise():
    """The real API returns 200 with an `errors` array on failure (incl. rate
    limit) — that must raise, not be parsed as an empty transcript list."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "data": None,
            "errors": [{"message": "Rate limit", "extensions": {"code": "too_many_requests"}}],
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        with pytest.raises(RetryLater) as exc:
            await _client(http).list_transcripts_graphql(limit=50, skip=0)
    assert exc.value.reason is RetryReason.RATE_LIMIT
    assert exc.value.request_context.operation == "transcripts.list"
