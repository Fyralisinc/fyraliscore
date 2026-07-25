"""Contract test: the Brex client parses a REAL cursor-paginated transactions page.

Guards the Phase-3 drift fix (finding #3): the REAL Brex transactions API
(`GET /v2/transactions/card/primary` and `/v2/transactions/cash/{account_id}`)
returns CURSOR pagination —
`{"items": [...], "next_cursor": "<token or null>"}` with NO `total` field. The
pre-fix `BrexClient.list_transactions` used Mercury-style offset/`total`
pagination (`resp.get('total', len(txns))`), so it stopped after page 1 and
silently dropped every later page of live Brex transactions.

The fix is ADDITIVE: `_parse_transactions_page` reads `items` + `next_cursor`
(real shape) and recognizes a null/absent `next_cursor` as terminal, while
`list_transactions` follows the cursor across pages when one is present — and
still serves the legacy offset/`total` single-page shape (the synthetic mock
Provider Lab) unchanged as a fallback. Verified against developer.brex.com
(Transactions API + Pagination docs).
"""
from __future__ import annotations


import httpx
import pytest

from services.ingest.integrations.brex.client import (
    BrexClient,
    _parse_transactions_page,
)
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract


def _fixture():
    return load_fixture("brex", "api_response", "transactions_page")


def _body() -> dict:
    return _fixture().response_body


def _client(transport: httpx.MockTransport) -> BrexClient:
    http = httpx.AsyncClient(transport=transport)
    return BrexClient(
        base_url="https://platform.brexapis.com",
        api_token="t",
        http_client=http,
    )


# --- shape sanity: the fixture really is the doc cursor shape ---------------

def test_fixture_is_real_cursor_shape_no_total():
    body = _body()
    assert isinstance(body["items"], list) and len(body["items"]) >= 2
    # Real Brex: cursor pagination, and the terminal page carries next_cursor=null.
    assert "next_cursor" in body
    assert body["next_cursor"] is None
    # There is NO `total` field on the real response — that was the Mercury clone.
    assert "total" not in body
    assert "transactions" not in body  # real key is `items`, not `transactions`


# --- the page-parsing helper the fix introduced -----------------------------

def test_parse_helper_reads_items_and_treats_null_cursor_as_terminal():
    items, next_cursor, total = _parse_transactions_page(_body())
    # Items are read from the real `items` key.
    assert [it["id"] for it in items] == [
        "pste_00000000000000000000000001",
        "pste_00000000000000000000000002",
    ]
    # null next_cursor => no follow token => TERMINAL.
    assert next_cursor is None
    # Real shape has no total.
    assert total is None


def test_parse_helper_non_terminal_cursor_is_followed():
    page = {"items": [{"id": "pste_x"}], "next_cursor": "cursor-token-abc"}
    items, next_cursor, total = _parse_transactions_page(page)
    assert [it["id"] for it in items] == ["pste_x"]
    assert next_cursor == "cursor-token-abc"  # present => keep walking
    assert total is None


def test_parse_helper_synthetic_offset_shape_still_works():
    """ADDITIVE fallback: the synthetic `{transactions, total}` offset shape is
    parsed unchanged (no next_cursor key => offset/total path)."""
    page = {"transactions": [{"id": "t1"}, {"id": "t2"}], "total": 7}
    items, next_cursor, total = _parse_transactions_page(page)
    assert [it["id"] for it in items] == ["t1", "t2"]
    assert next_cursor is None  # no cursor field at all
    assert total == 7


# --- the production client, end-to-end through _request ---------------------

async def test_client_terminal_cursor_page_is_single_call():
    body = _body()
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=body)

    client = _client(httpx.MockTransport(handler))
    txns, next_offset, total = await client.list_transactions("acc_primary")

    # next_cursor was null => exactly one API call, and the walk is terminal.
    assert len(calls) == 1
    assert next_offset is None  # terminal for the fetcher (`is_last`)
    assert [t["id"] for t in txns] == [it["id"] for it in body["items"]]
    assert total == len(body["items"])
    # cursor pagination => the query carries `cursor`-style params, no `total`
    # bookkeeping leaked into the request.
    assert calls[0].url.path == "/v2/transactions/cash/acc_primary"
    assert dict(httpx.QueryParams(calls[0].url.query))["limit"] == "100"
    await client.aclose()


async def test_client_follows_next_cursor_until_null():
    """A non-terminal page (next_cursor set) is followed; the doc fixture is the
    terminal page. The client must concatenate both and stop at null."""
    terminal = _body()
    first_page = {
        "items": [{"id": "pste_page1_a"}, {"id": "pste_page1_b"}],
        "next_cursor": "go-to-page-2",
    }
    seen_cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = dict(httpx.QueryParams(request.url.query)).get("cursor")
        seen_cursors.append(cursor)
        if cursor == "go-to-page-2":
            return httpx.Response(200, json=terminal)
        return httpx.Response(200, json=first_page)

    client = _client(httpx.MockTransport(handler))
    txns, next_offset, total = await client.list_transactions("acc_primary")

    # First call had no cursor; second followed "go-to-page-2"; then null => stop.
    assert seen_cursors == [None, "go-to-page-2"]
    assert next_offset is None
    assert [t["id"] for t in txns] == [
        "pste_page1_a",
        "pste_page1_b",
        "pste_00000000000000000000000001",
        "pste_00000000000000000000000002",
    ]
    assert total == 4
    await client.aclose()


async def test_client_uses_card_primary_route_for_card_account_kind():
    body = _body()
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=body)

    client = _client(httpx.MockTransport(handler))
    await client.list_transactions("ignored-card-id", account_kind="card")

    assert calls[0].url.path == "/v2/transactions/card/primary"
    await client.aclose()


async def test_client_synthetic_offset_shape_unchanged():
    """ADDITIVE guarantee: against the legacy `{transactions, total}` server the
    client still returns the original offset/total single-page contract."""
    page = {
        "transactions": [{"id": "t1"}, {"id": "t2"}],
        "total": 5,  # more pages exist by offset bookkeeping
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=page)

    client = _client(httpx.MockTransport(handler))
    txns, next_offset, total = await client.list_transactions(
        "acc", limit=2, offset=0,
    )
    assert [t["id"] for t in txns] == ["t1", "t2"]
    assert total == 5
    assert next_offset == 2  # offset advanced; NOT terminal (5 > 2)
    await client.aclose()
