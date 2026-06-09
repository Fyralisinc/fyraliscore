"""Contract test: the Notion reconciler probe paginates a REAL /v1/search list.

Guards the Phase-3 drift fix (finding #36): real Notion (api.notion.com)
`POST /v1/search` returns a paginated list envelope
`{"object":"list","results":[...],"has_more":bool,"next_cursor":str|null,"type":...}`.
The old `NotionClient.latest_page_edit()` issued ONE call (page_size 50) and
returned None when that single page was all database rows — so in a workspace
whose newest objects are database rows, every loose page sat past page 1 and
the page_tree gap probe silently went blind.

The fix is ADDITIVE: when `has_more` is true the client loops, threading
`next_cursor` back as `start_cursor`, until it finds a loose (non-database-row)
page or `has_more` is false. A response with `has_more` absent/false still stops
after one call (the single-call fallback the synthetic spammer relies on).

This test drives the REAL `NotionClient` over a fake httpx transport fed the
doc-shaped fixture, and asserts the loop CONTINUES past the all-database-rows
first page and STOPS on the terminal page. Verified against
developers.notion.com/reference/post-search.
"""
from __future__ import annotations

import json

import httpx
import pytest

from services.ingest.integrations.notion.client import NotionClient, _unwrap_list
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract


def _fixture():
    return load_fixture("notion", "api_response", "search_page")


class _FakeResponse:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code = status
        self.headers: dict[str, str] = {}
        self._body = body

    def json(self):
        return self._body


class _SequencedTransport:
    """Stand-in for httpx.AsyncClient. Returns search pages keyed off the
    `start_cursor` the REAL client threads into the request body — so the loop
    is exercised exactly as it would be against api.notion.com."""

    def __init__(self, page1: dict, page2: dict) -> None:
        self._page1 = page1
        self._page2 = page2
        self.calls: list[dict] = []

    async def request(self, method, url, *, headers=None, json=None, params=None):
        self.calls.append({"method": method, "url": url, "json": json})
        # page 1 has next_cursor == page2's lookup key.
        cursor = (json or {}).get("start_cursor")
        if cursor is None:
            return _FakeResponse(200, self._page1)
        assert cursor == self._page1["next_cursor"], (
            "client must thread page-1 next_cursor back as start_cursor"
        )
        return _FakeResponse(200, self._page2)

    async def aclose(self):
        return None


def _client(transport) -> NotionClient:
    return NotionClient(bot_token="test-notion-token", http_client=transport)


def test_fixture_is_doc_shaped_search_list():
    """The fixture body is the real Notion list envelope, and `_unwrap_list`
    (the production parser) reads results/next_cursor/has_more from it."""
    fx = _fixture()
    body = fx.response_body
    assert body["object"] == "list"
    assert body["type"] == "page_or_database"
    results, next_cursor, has_more = _unwrap_list(body)
    assert has_more is True
    assert next_cursor == body["next_cursor"]
    # Page 1 is intentionally ALL database rows -> latest_page_edit must paginate.
    assert results, "page 1 must carry results"
    assert all(
        p["parent"]["type"] == "database_id" for p in results
    ), "page 1 must be entirely database rows to force pagination"

    page2 = fx.response["body_page2"]
    r2, c2, more2 = _unwrap_list(page2)
    assert more2 is False and c2 is None
    assert r2[0]["parent"]["type"] != "database_id", "page 2 carries the loose page"


@pytest.mark.asyncio
async def test_latest_page_edit_follows_next_cursor():
    """REAL code: page 1 is all database rows + has_more:true, so the probe must
    follow next_cursor to page 2 and return the loose page's last_edited_time."""
    fx = _fixture()
    page1 = fx.response_body
    page2 = fx.response["body_page2"]
    transport = _SequencedTransport(page1, page2)

    result = await _client(transport).latest_page_edit()

    loose = page2["results"][0]
    assert result == loose["last_edited_time"]
    # Drift proof: it took TWO calls (it did NOT stop at the all-db-rows page 1).
    assert len(transport.calls) == 2
    assert transport.calls[1]["json"]["start_cursor"] == page1["next_cursor"]
    # The original 50-row cap is gone; we request Notion's max page size.
    assert transport.calls[0]["json"]["page_size"] == 100


@pytest.mark.asyncio
async def test_single_call_fallback_when_has_more_false():
    """Fallback (unchanged synthetic shape): a terminal page (has_more:false /
    next_cursor:null) stops after ONE call — no infinite loop, no extra fetch."""
    fx = _fixture()
    page2 = fx.response["body_page2"]  # has_more:false, carries a loose page
    transport = _SequencedTransport(page2, page2)

    result = await _client(transport).latest_page_edit()

    assert result == page2["results"][0]["last_edited_time"]
    assert len(transport.calls) == 1  # stopped on the terminal page


@pytest.mark.asyncio
async def test_returns_none_without_unbounded_loop_when_all_db_rows():
    """If EVERY page is database rows and the final page says has_more:false,
    the probe returns None after exhausting pages (terminates — no runaway)."""
    fx = _fixture()
    page1 = dict(fx.response_body)  # all db rows, has_more:true -> next_cursor
    # A terminal all-db-rows page: same db rows but has_more:false/next_cursor:null.
    terminal = {
        "object": "list",
        "results": page1["results"],
        "next_cursor": None,
        "has_more": False,
        "type": "page_or_database",
    }
    transport = _SequencedTransport(page1, terminal)

    result = await _client(transport).latest_page_edit()

    assert result is None
    assert len(transport.calls) == 2  # followed cursor once, then stopped


def test_raw_body_round_trips_as_json():
    """Sanity: the fixture body is valid JSON (what api.notion.com would send)."""
    fx = _fixture()
    assert json.loads(json.dumps(fx.response_body))["object"] == "list"
