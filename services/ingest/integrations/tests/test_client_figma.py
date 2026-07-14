"""Figma REST client contract tests for the real event-read surface."""
from __future__ import annotations

import httpx
import pytest

from services.ingest.integrations.figma.client import FigmaClient


pytestmark = pytest.mark.asyncio


async def test_list_events_derives_records_from_versions_and_comments_only():
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/files/file-1/versions":
            return httpx.Response(200, json={
                "versions": [{
                    "id": "v-1",
                    "created_at": "2026-06-01T10:00:00Z",
                    "label": "Release",
                    "user": {"handle": "ada"},
                }],
            })
        if request.url.path == "/v1/files/file-1/comments":
            return httpx.Response(200, json={
                "comments": [{
                    "id": "c-1",
                    "created_at": "2026-06-02T10:00:00Z",
                    "message": "Looks good",
                    "user": {"handle": "lin"},
                }],
            })
        return httpx.Response(404, json={"error": "unexpected path"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = FigmaClient(
            base_url="https://api.figma.com",
            api_token="oauth-token",
            auth_kind="oauth",
            http_client=http,
        )
        events, next_offset, total = await client.list_events("file-1", limit=10)

    assert [request.url.path for request in requests] == [
        "/v1/files/file-1/versions",
        "/v1/files/file-1/comments",
    ]
    assert all(request.headers["Authorization"] == "Bearer oauth-token" for request in requests)
    assert total == 2
    assert next_offset is None
    # Newest source event is first after the client-side merge/sort.
    assert [event["event_type"] for event in events] == [
        "FILE_COMMENT",
        "FILE_VERSION_UPDATE",
    ]
