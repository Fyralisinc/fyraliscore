"""Tests for the LinkedIn vertical: client wire contract (Community Management
API smoke), fetchers/linkedin.py, and the handler's external_id derivation."""
from __future__ import annotations

import httpx
import pytest

from services.ingest.integrations.linkedin.client import LinkedinClient
from services.ingest.ingestion.fetchers import linkedin as linkedin_fetcher
from services.ingest.ingestion.fetchers.linkedin import (
    LinkedinCursor,
    SHARD_KIND_ENTITY,
    fetch_page_linkedin,
)
from services.ingest.ingestion.handlers.linkedin import handle_linkedin_object


pytestmark = pytest.mark.asyncio


_ORG = "urn:li:organization:123"
_BASE = "https://api.linkedin.com/rest"


def _post(n: int, modified_ms: int) -> dict:
    return {
        "id": f"urn:li:share:{n}",
        "author": _ORG,
        "commentary": f"post {n}",
        "lifecycleState": "PUBLISHED",
        "lifecycleStateInfo": {"isEditedByAuthor": False},
        "visibility": "PUBLIC",
        "createdAt": modified_ms - 1000,
        "lastModifiedAt": modified_ms,
    }


def _stat_bucket(start_ms: int) -> dict:
    return {
        "organizationalEntity": _ORG,
        "timeRange": {"start": start_ms, "end": start_ms + 86_400_000},
        "totalShareStatistics": {"impressionCount": 42, "likeCount": 4},
    }


def _client_with(captured: list, body: dict) -> LinkedinClient:
    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=body)

    return LinkedinClient(
        base_url=_BASE,
        organization_urn=_ORG,
        access_token="test-token",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )


# ---------------------------------------------------------------------
# Client wire-contract smoke tests
# ---------------------------------------------------------------------

async def test_first_request_is_rest_posts_author_finder_with_required_headers():
    """The read surface is the REAL Rest.li posts finder — NOT the old
    `/v1/organizations/{org}/query` placeholder — and BOTH required headers
    ride on every request."""
    captured: list[httpx.Request] = []
    client = _client_with(
        captured, {"elements": [], "paging": {"start": 0, "count": 10}},
    )
    rows, next_start = await client.list_posts(start=0, count=10)
    await client.aclose()

    assert rows == [] and next_start is None
    req = captured[0]
    assert req.method == "GET"
    assert req.url.path == "/rest/posts"
    assert "/v1/organizations" not in str(req.url)
    assert req.url.params["q"] == "author"
    assert req.url.params["author"] == _ORG
    # Rest.li-2.0 URN encoding on the wire.
    assert "urn%3Ali%3Aorganization%3A123" in str(req.url)
    # BOTH required headers (missing -> 400/426 on the real API).
    assert req.headers["X-Restli-Protocol-Version"] == "2.0.0"
    assert len(req.headers["LinkedIn-Version"]) == 6
    assert req.headers["LinkedIn-Version"].isdigit()


async def test_posts_offset_pagination_short_page_terminal():
    captured: list[httpx.Request] = []
    full = [_post(i, 1_700_000_000_000 - i) for i in range(3)]
    client = _client_with(
        captured,
        {"elements": full, "paging": {"start": 0, "count": 3, "links": []}},
    )
    rows, next_start = await client.list_posts(start=0, count=3)
    assert len(rows) == 3
    assert next_start == 3  # full page -> advance start by len(elements)

    rows, next_start = await client.list_posts(start=3, count=5)
    assert next_start is None  # short page, no `next` link -> terminal
    await client.aclose()


async def test_share_statistics_time_intervals_restli_encoding():
    """`timeIntervals` rides as the documented Rest.li-2.0 form — parens raw,
    inner colons/commas percent-encoded."""
    captured: list[httpx.Request] = []
    client = _client_with(captured, {"elements": [_stat_bucket(1_700_000_000_000)]})
    elements = await client.share_statistics(
        start_ms=1_700_000_000_000, end_ms=1_700_086_400_000, granularity="DAY",
    )
    await client.aclose()

    assert len(elements) == 1
    url = str(captured[0].url)
    assert captured[0].url.path == "/rest/organizationalEntityShareStatistics"
    assert "q=organizationalEntity" in url
    assert "organizationalEntity=urn%3Ali%3Aorganization%3A123" in url
    assert (
        "timeIntervals=(timeRange%3A(start%3A1700000000000%2C"
        "end%3A1700086400000)%2CtimeGranularityType%3ADAY)" in url
    )
    assert captured[0].headers["X-Restli-Protocol-Version"] == "2.0.0"


async def test_get_organization_probe_uses_numeric_id_path():
    captured: list[httpx.Request] = []
    client = _client_with(captured, {"id": 123, "localizedName": "Acme"})
    info = await client.get_organization()
    await client.aclose()
    assert info["localizedName"] == "Acme"
    assert captured[0].url.path == "/rest/organizations/123"


# ---------------------------------------------------------------------
# Fetcher tests (client seam rebound)
# ---------------------------------------------------------------------

class _FakeClient:
    """Implements the LinkedinClient read surface the fetcher uses."""

    def __init__(self, posts=None, stats=None):
        # posts: DESC by lastModifiedAt, as the real finder serves them.
        self._posts = posts or []
        self._stats = stats or []
        self.calls: list[dict] = []

    async def list_posts(self, *, start=0, count=100, sort_by="LAST_MODIFIED"):
        self.calls.append({"method": "list_posts", "start": start, "count": count})
        page = self._posts[start:start + count]
        is_last = len(page) < count or not page
        return page, (None if is_last else start + len(page))

    async def share_statistics(self, *, start_ms=None, end_ms=None,
                               granularity="DAY"):
        self.calls.append({"method": "share_statistics", "start_ms": start_ms})
        return [
            e for e in self._stats
            if start_ms is None or e["timeRange"]["start"] >= start_ms
        ]

    async def follower_statistics(self, *, start_ms=None, end_ms=None,
                                  granularity="DAY"):
        self.calls.append({"method": "follower_statistics", "start_ms": start_ms})
        return [
            e for e in self._stats
            if start_ms is None or e["timeRange"]["start"] >= start_ms
        ]


class _FakeInst:
    _d = {"organization_urn": _ORG, "base_url": _BASE, "tenant_id": None,
          "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(linkedin_fetcher, "_open_linkedin_client", _open)


async def test_full_posts_backfill_tags_records_and_tracks_epoch_high_water(
    monkeypatch,
):
    posts = [_post(2, 1_700_000_200_000), _post(1, 1_700_000_100_000)]
    client = _FakeClient(posts=posts)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "post"}
    res = await fetch_page_linkedin(_FakeInst(), shard, None)

    assert len(res.records) == 2
    assert all(r["_fyralis_record_type"] == "post" for r in res.records)
    assert all(r["_fyralis_org_urn"] == _ORG for r in res.records)
    assert res.end_of_data is True
    cur = LinkedinCursor.model_validate(res.next_cursor)
    assert cur.high_water_ms == 1_700_000_200_000


async def test_incremental_posts_early_stop_at_floor(monkeypatch):
    # DESC order: one new post, then two at/under the floor.
    posts = [_post(3, 1_700_000_300_000), _post(2, 1_700_000_200_000),
             _post(1, 1_700_000_100_000)]
    client = _FakeClient(posts=posts)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "post",
             "updated_cursor": 1_700_000_200_000}
    res = await fetch_page_linkedin(_FakeInst(), shard, None)

    assert [r["entity"]["id"] for r in res.records] == ["urn:li:share:3"]
    assert res.end_of_data is True
    cur = LinkedinCursor.model_validate(res.next_cursor)
    assert cur.high_water_ms == 1_700_000_300_000


async def test_statistics_shard_is_single_call_and_floor_is_exclusive(
    monkeypatch,
):
    stats = [_stat_bucket(1_700_000_000_000), _stat_bucket(1_700_086_400_000)]
    client = _FakeClient(stats=stats)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "share_statistics",
             "updated_cursor": "1700000000000"}
    res = await fetch_page_linkedin(_FakeInst(), shard, None)

    # The statistics finder is unpaginated: one call, terminal.
    assert res.end_of_data is True
    assert client.calls[0]["method"] == "share_statistics"
    # timeRange.start is inclusive on the wire -> the fetcher asks from floor+1.
    assert client.calls[0]["start_ms"] == 1_700_000_000_001
    assert len(res.records) == 1
    assert res.records[0]["entity"]["timeRange"]["start"] == 1_700_086_400_000
    cur = LinkedinCursor.model_validate(res.next_cursor)
    assert cur.high_water_ms == 1_700_086_400_000


async def test_missing_entity_type_is_noop(monkeypatch):
    client = _FakeClient()
    _wire(monkeypatch, client)
    res = await fetch_page_linkedin(
        _FakeInst(), {"shard_kind": SHARD_KIND_ENTITY}, None,
    )
    assert res.records == []
    assert res.end_of_data is True


# ---------------------------------------------------------------------
# Handler external_id / timestamp decoding
# ---------------------------------------------------------------------

async def test_handler_post_external_id_and_epoch_ms_timestamp():
    record = {
        "_fyralis_record_type": "post",
        "_fyralis_org_urn": "li-org-1",
        "entity": _post(1000, 1_700_000_000_000),
    }
    draft = await handle_linkedin_object(record, {})
    assert draft.external_id == "linkedin:li-org-1:post:urn:li:share:1000"
    assert int(draft.occurred_at.timestamp() * 1000) == 1_700_000_000_000
    assert draft.kind == "signal"


async def test_handler_statistics_external_id_versioned_by_time_bucket():
    record = {
        "_fyralis_record_type": "share_statistics",
        "_fyralis_org_urn": "li-org-1",
        "entity": _stat_bucket(1_700_000_000_000),
    }
    draft = await handle_linkedin_object(record, {})
    assert draft.external_id == (
        "linkedin:li-org-1:share_statistics:1700000000000"
    )
    assert draft.content["impression_count"] == 42
