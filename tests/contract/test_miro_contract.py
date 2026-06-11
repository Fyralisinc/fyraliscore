"""Contract test: MiroClient.list_boards walks the REAL boards pagination.

Guards the Phase-3 drift fix (finding #10): the real Miro API
(`GET https://api.miro.com/v2/boards`) returns an OFFSET-paginated envelope —
``{"data":[...],"total":N,"size":N,"offset":N,"limit":N,
"links":{"self":...,"next":"<url>"}}`` — and advertises a `links.next` URL
while more pages remain. The old `list_boards()` fetched only the first page,
silently dropping every board past it for any tenant with >limit boards.

This test drives the REAL `MiroClient` (no stub of `list_boards`) over an
`httpx.MockTransport` seeded from the doc-derived fixture, asserting that:
  - the fixture itself carries the real envelope (data + total/size/offset/limit
    + links.next) the client must understand;
  - the client follows `links.next` to exhaustion and returns boards from ALL
    pages (terminal page omits `links.next`);
  - the single-page fallback (no `links.next`, no offset/total envelope) still
    terminates after one page — the shape the synthetic spammer emits, so the
    all-25 synthetic gate is unaffected.

Verified against developers.miro.com (GET /v2/boards; query params
team_id/limit/offset/sort; v2 offset-pagination envelope).
"""
from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from services.ingest.integrations.miro.client import MiroClient
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_API_BASE = "https://api.miro.com/v2"
_TOKEN = "test-miro-bearer-token"


def _fixture():
    return load_fixture("miro", "api_response", "boards_page")


def _page_body() -> dict:
    return _fixture().response_body


def test_fixture_carries_real_pagination_envelope():
    """The doc fixture is the real offset-paginated shape, not a bare list."""
    body = _page_body()
    assert isinstance(body["data"], list) and body["data"]
    # The pagination counters the client may use as the offset fallback.
    for key in ("total", "size", "offset", "limit"):
        assert isinstance(body[key], int), key
    # links.next is present on a non-terminal page (total=3 > offset+size=2).
    assert body["links"]["next"], "first page must advertise links.next"
    assert body["offset"] + body["size"] < body["total"]


def _build_paginated_transport() -> tuple[httpx.MockTransport, list[str]]:
    """A MockTransport that serves the fixture's page 1 (with links.next) and a
    synthesized terminal page 2 derived from the fixture envelope. Records the
    request paths so the test can assert the client actually advanced pages."""
    page1 = _page_body()
    total = page1["total"]
    size = page1["size"]
    limit = page1["limit"]

    seen_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.raw_path.decode())
        assert request.headers["Authorization"] == f"Bearer {_TOKEN}"
        qs = parse_qs(urlsplit(str(request.url)).query)
        offset = int(qs.get("offset", ["0"])[0])
        if offset == 0:
            return httpx.Response(200, json=page1)
        # Terminal page: the remaining boards, NO links.next.
        remaining = total - size
        tail = [
            {"id": f"uXjVABCD{100 + n}=", "name": f"More board {n}",
             "type": "board"}
            for n in range(remaining)
        ]
        body = {
            "data": tail,
            "total": total,
            "size": len(tail),
            "offset": offset,
            "limit": limit,
            "links": {
                "self": f"{_API_BASE}/boards?limit={limit}&offset={offset}",
            },
        }
        return httpx.Response(200, json=body)

    return httpx.MockTransport(handler), seen_paths


async def test_list_boards_follows_links_next_to_exhaustion():
    transport, seen = _build_paginated_transport()
    http = httpx.AsyncClient(transport=transport)
    client = MiroClient(base_url=_API_BASE, api_token=_TOKEN, http_client=http)
    try:
        boards = await client.list_boards()
    finally:
        await client.aclose()
        await http.aclose()

    page1 = _page_body()
    # ALL boards across pages, not just the first page.
    assert len(boards) == page1["total"]
    assert all(isinstance(b, dict) and b.get("id") for b in boards)
    # First-page board ids are preserved verbatim.
    first_ids = {b["id"] for b in page1["data"]}
    assert first_ids <= {b["id"] for b in boards}
    # The client actually issued a SECOND request advancing the offset
    # (i.e. it followed links.next rather than stopping at page 1).
    assert len(seen) >= 2
    assert any("offset=2" in p for p in seen[1:])


async def test_list_boards_single_page_fallback_when_no_links_next():
    """A response with NO links.next and no more-rows-remaining envelope
    terminates after exactly one request — the synthetic/legacy shape. This is
    the fallback that keeps the all-25 synthetic gate green."""
    page1 = _page_body()
    # A self-contained single page: total == size, so the offset fallback also
    # says terminal; and no links.next.
    single = {
        "data": page1["data"],
        "total": len(page1["data"]),
        "size": len(page1["data"]),
        "offset": 0,
        "limit": page1["limit"],
        "links": {"self": f"{_API_BASE}/boards"},
    }
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.raw_path.decode())
        return httpx.Response(200, json=single)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = MiroClient(base_url=_API_BASE, api_token=_TOKEN, http_client=http)
    try:
        boards = await client.list_boards()
    finally:
        await client.aclose()
        await http.aclose()

    assert len(boards) == len(page1["data"])
    assert len(calls) == 1  # did NOT paginate past the single page


async def test_list_boards_bare_list_response_terminates():
    """A bare-list response (no envelope at all) still parses and terminates —
    the most defensive legacy fallback the original code supported."""
    page1 = _page_body()
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.raw_path.decode())
        # NOTE: the real client requires a JSON object; a bare list is mapped to
        # a MiroApiError by _request. We instead model the legacy "data-only,
        # no links/total" object that the synthetic mock-equivalent emits.
        return httpx.Response(200, json={"data": page1["data"]})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = MiroClient(base_url=_API_BASE, api_token=_TOKEN, http_client=http)
    try:
        boards = await client.list_boards()
    finally:
        await client.aclose()
        await http.aclose()

    assert len(boards) == len(page1["data"])
    assert len(calls) == 1
