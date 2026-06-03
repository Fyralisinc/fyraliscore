"""Tests for services/ingest/integrations/notion/client.py (IN-14).

respx mocks api.notion.com only; no DB boundary is involved here (the bot
token is injected directly).
"""
from __future__ import annotations

import httpx
import pytest
import respx

from lib.shared.errors import NotionApiError
from services.ingest.integrations.notion.client import NotionClient, short_workspace_hash


pytestmark = pytest.mark.asyncio


def _client() -> NotionClient:
    return NotionClient(
        bot_token="secret-bot-token",
        http_client=httpx.AsyncClient(),
        api_base_url="https://api.notion.com",
    )


async def test_search_returns_results_cursor_has_more():
    with respx.mock(base_url="https://api.notion.com") as router:
        router.post("/v1/search").respond(
            200,
            json={
                "results": [{"object": "database", "id": "db-1"}],
                "next_cursor": "cursor-2",
                "has_more": True,
            },
        )
        c = _client()
        results, cursor, has_more = await c.search(object_filter="database")
        assert results == [{"object": "database", "id": "db-1"}]
        assert cursor == "cursor-2"
        assert has_more is True


async def test_query_database_paginates_per_call():
    with respx.mock(base_url="https://api.notion.com") as router:
        router.post("/v1/databases/db-1/query").respond(
            200, json={"results": [{"object": "page", "id": "p1"}],
                       "next_cursor": None, "has_more": False},
        )
        c = _client()
        rows, cursor, has_more = await c.query_database("db-1")
        assert [r["id"] for r in rows] == ["p1"]
        assert cursor is None and has_more is False


async def test_bot_token_is_sent_as_bearer():
    captured = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers.get("Authorization")
        captured["version"] = request.headers.get("Notion-Version")
        return httpx.Response(200, json={"results": [], "next_cursor": None, "has_more": False})

    with respx.mock(base_url="https://api.notion.com") as router:
        router.get("/v1/comments").mock(side_effect=_capture)
        await _client().list_comments("page-1")
    assert captured["auth"] == "Bearer secret-bot-token"
    assert captured["version"]  # a pinned Notion-Version header is always sent


async def test_401_maps_to_unauthorized():
    with respx.mock(base_url="https://api.notion.com") as router:
        router.post("/v1/search").respond(401, json={"code": "unauthorized", "message": "x"})
        with pytest.raises(NotionApiError) as ei:
            await _client().search()
    assert ei.value.code == "notion_api_unauthorized"


async def test_429_retries_then_succeeds(monkeypatch):
    monkeypatch.setenv("NOTION_RL_MAX_SLEEP_SEC", "0")
    monkeypatch.setenv("NOTION_RL_MAX_ATTEMPTS", "4")
    responses = [
        httpx.Response(429, headers={"Retry-After": "0"}),
        httpx.Response(200, json={"results": [{"object": "page", "id": "p1"}],
                                  "next_cursor": None, "has_more": False}),
    ]

    def _side_effect(request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    with respx.mock(base_url="https://api.notion.com") as router:
        router.post("/v1/databases/db/query").mock(side_effect=_side_effect)
        rows, _cursor, _more = await _client().query_database("db")
    assert [r["id"] for r in rows] == ["p1"]
    assert responses == []  # both responses consumed (one retry happened)


async def test_429_exhausts_budget_raises_rate_limited(monkeypatch):
    monkeypatch.setenv("NOTION_RL_MAX_SLEEP_SEC", "0")
    monkeypatch.setenv("NOTION_RL_MAX_ATTEMPTS", "2")
    with respx.mock(base_url="https://api.notion.com") as router:
        router.post("/v1/search").respond(429, headers={"Retry-After": "0"})
        with pytest.raises(NotionApiError) as ei:
            await _client().search()
    assert ei.value.code == "notion_api_rate_limited"


async def test_latest_database_edit_probe():
    with respx.mock(base_url="https://api.notion.com") as router:
        router.post("/v1/databases/db-1/query").respond(
            200, json={"results": [{"id": "p1", "last_edited_time": "2025-05-01T00:00:00Z"}],
                       "next_cursor": None, "has_more": False},
        )
        latest = await _client().latest_database_edit("db-1")
    assert latest == "2025-05-01T00:00:00Z"


async def test_workspace_hash_deterministic_16_hex():
    h = short_workspace_hash("ws-prod")
    assert len(h) == 16 and short_workspace_hash("ws-prod") == h
    assert short_workspace_hash("ws-other") != h
