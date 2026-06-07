"""Tests for services/ingest/integrations/notion/client.py (IN-14).

respx mocks api.notion.com only; no DB boundary is involved here (the bot
token is injected directly).
"""
from __future__ import annotations

from uuid import uuid4

import httpx
import pytest
import respx

from lib.shared.errors import NotionApiError
from services.ingest.integrations.notion.client import (
    NotionClient,
    _api_error_from_response,
    short_workspace_hash,
)


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


# ---------------------------------------------------------------------
# IN-14 worker-crash hardening: recoverable classification + chokepoint.
# ---------------------------------------------------------------------

def _resp(status: int) -> httpx.Response:
    return httpx.Response(status, json={"object": "error", "code": "x"})


async def test_recoverable_classification_by_status():
    # 401 (revoked, chokepoint-disabled), 429, 5xx, transport → park/retry.
    assert _api_error_from_response(_resp(401), "/p").recoverable is True
    assert _api_error_from_response(_resp(429), "/p").recoverable is True
    assert _api_error_from_response(_resp(500), "/p").recoverable is True
    assert _api_error_from_response(_resp(503), "/p").recoverable is True
    # 404 (genuine not-found) and other 4xx → fail fast.
    assert _api_error_from_response(_resp(404), "/p").recoverable is False
    assert _api_error_from_response(_resp(400), "/p").recoverable is False
    assert _api_error_from_response(_resp(403), "/p").recoverable is False


class _RecordingConn:
    def __init__(self):
        self.disabled = False

    def transaction(self):
        class _Txn:
            async def __aenter__(self_):
                return None

            async def __aexit__(self_, *a):
                return False

        return _Txn()

    async def fetchrow(self, sql, *args):
        self.disabled = True
        return {"id": uuid4()}

    async def execute(self, sql, *args):
        return None


class _RecordingPool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Acq:
            async def __aenter__(self_):
                return conn

            async def __aexit__(self_, *a):
                return False

        return _Acq()


async def test_401_fires_revocation_chokepoint_and_is_recoverable():
    conn = _RecordingConn()
    client = NotionClient(
        bot_token="secret-bot-token",
        http_client=httpx.AsyncClient(),
        api_base_url="https://api.notion.com",
        pool=_RecordingPool(conn),
        tenant_id=uuid4(),
        workspace_id="ws-revoked",
    )
    with respx.mock(base_url="https://api.notion.com") as router:
        router.post("/v1/search").respond(
            401, json={"object": "error", "code": "unauthorized"},
        )
        with pytest.raises(NotionApiError) as ei:
            await client.search()
    # The install was disabled (chokepoint fired) ...
    assert conn.disabled is True
    # ... and the raised 401 is recoverable so the shard PARKS (not fail).
    assert ei.value.code == "notion_api_unauthorized"
    assert ei.value.recoverable is True


async def test_401_without_db_context_skips_chokepoint_no_crash():
    # No pool/tenant/workspace (spammer mode / OAuth probe): chokepoint is
    # skipped, but the 401 still raises cleanly (and stays recoverable).
    client = NotionClient(
        bot_token="secret-bot-token",
        http_client=httpx.AsyncClient(),
        api_base_url="https://api.notion.com",
    )
    with respx.mock(base_url="https://api.notion.com") as router:
        router.post("/v1/search").respond(401, json={"code": "unauthorized"})
        with pytest.raises(NotionApiError) as ei:
            await client.search()
    assert ei.value.code == "notion_api_unauthorized"
    assert ei.value.recoverable is True
