"""Tests for services/ingest/ingestion/fetchers/aws.py (IN-AWS)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import FETCHER_DISPATCH
from services.ingest.ingestion.fetchers import aws as af
from services.ingest.ingestion.fetchers.aws import (
    SHARD_KIND_ACCOUNT_EVENTS,
    AwsCursor,
    fetch_page_aws,
)


pytestmark = pytest.mark.asyncio


class _FakeInst:
    _d = {"account_id": "123456789012", "region": "us-east-1", "tenant_id": None}

    def __getitem__(self, k):
        return self._d[k]

    def __contains__(self, k):
        return k in self._d


def _evt(eid, time_ms, **extra):
    return {"eventId": eid, "eventTime": time_ms, "eventName": f"E{eid}", **extra}


class _FakeAwsClient:
    """Newest-first event window query with opaque off:<n> token paging."""

    def __init__(self, events, per_page=50):
        self._events = list(events)
        self._per_page = per_page
        self.calls: list[dict] = []

    async def list_events(self, *, account_id, region, from_ms=None, to_ms=None,
                          cursor=None, limit=50):
        self.calls.append({"from_ms": from_ms, "to_ms": to_ms, "cursor": cursor})
        rows = [
            e for e in self._events
            if (from_ms is None or e["eventTime"] >= from_ms)
            and (to_ms is None or e["eventTime"] <= to_ms)
        ]
        rows.sort(key=lambda e: e["eventTime"], reverse=True)  # newest-first
        start = int(cursor[4:]) if cursor and cursor.startswith("off:") else 0
        per = min(limit, self._per_page)
        end = start + per
        page = rows[start:end]
        is_last = end >= len(rows)
        return {"events": page, "next_cursor": None if is_last else f"off:{end}"}


def _patch_client(monkeypatch, client):
    async def _open(_install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(af, "_open_aws_client", _open)


async def test_dispatch_wired():
    assert FETCHER_DISPATCH["aws"] is fetch_page_aws


async def test_full_backfill_pages_by_token_and_tracks_high_water(monkeypatch):
    monkeypatch.setenv("AWS_BACKFILL_WINDOW_DAYS", "0")  # all-time floor=None
    monkeypatch.setattr(af, "_page_size", lambda: 2)
    client = _FakeAwsClient(
        [_evt("a", 1000), _evt("b", 2000), _evt("c", 3000)], per_page=2,
    )
    _patch_client(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ACCOUNT_EVENTS, "installation_id": "i1"}

    # Page 1: newest two (3000, 2000); more remain (token returned).
    res1 = await fetch_page_aws(_FakeInst(), shard, None)
    assert res1.end_of_data is False
    assert len(res1.records) == 2
    assert all(r["_fyralis_record_type"] == "event" for r in res1.records)
    assert all(r["_fyralis_account_id"] == "123456789012" for r in res1.records)
    assert all(r["_fyralis_region"] == "us-east-1" for r in res1.records)
    cur = AwsCursor.model_validate(res1.next_cursor)
    assert cur.high_water_time_ms == 3000
    assert cur.events_cursor == "off:2"
    assert client.calls[0]["from_ms"] is None  # full walk -> no floor

    # Page 2: the remaining oldest (1000); terminal (no token).
    res2 = await fetch_page_aws(_FakeInst(), shard, res1.next_cursor)
    assert client.calls[1]["cursor"] == "off:2"
    assert res2.end_of_data is True
    assert len(res2.records) == 1
    cur2 = AwsCursor.model_validate(res2.next_cursor)
    assert cur2.high_water_time_ms == 3000  # unchanged by the older page
    assert cur2.events_seen == 3


async def test_warm_start_sets_incremental_floor(monkeypatch):
    monkeypatch.setattr(af, "_page_size", lambda: 50)
    client = _FakeAwsClient([_evt("a", 1000), _evt("b", 2000), _evt("c", 3000)])
    _patch_client(monkeypatch, client)
    shard = {
        "shard_kind": SHARD_KIND_ACCOUNT_EVENTS,
        "installation_id": "i1",
        "updated_cursor": 2500,  # prior high-water (epoch ms)
    }
    res = await fetch_page_aws(_FakeInst(), shard, None)
    # Only the event newer than the floor (3000) comes back.
    assert client.calls[0]["from_ms"] == 2500
    assert len(res.records) == 1
    assert res.end_of_data is True
    cur = AwsCursor.model_validate(res.next_cursor)
    assert cur.high_water_time_ms == 3000
    assert cur.floor_ms == 2500


async def test_empty_account_ends_cleanly(monkeypatch):
    monkeypatch.setenv("AWS_BACKFILL_WINDOW_DAYS", "0")
    _patch_client(monkeypatch, _FakeAwsClient([]))
    res = await fetch_page_aws(
        _FakeInst(), {"shard_kind": SHARD_KIND_ACCOUNT_EVENTS}, None,
    )
    assert res.end_of_data is True
    assert res.records == []


async def test_throttle_leaves_cursor_unadvanced(monkeypatch):
    from services.ingest.integrations.aws.client import AwsApiError

    class _Throttled:
        async def list_events(self, **_kw):
            raise AwsApiError("throttled", code="aws_api_throttled")

    monkeypatch.setenv("AWS_BACKFILL_WINDOW_DAYS", "0")
    _patch_client(monkeypatch, _Throttled())
    res = await fetch_page_aws(
        _FakeInst(), {"shard_kind": SHARD_KIND_ACCOUNT_EVENTS}, None,
    )
    assert res.records == []
    assert res.end_of_data is False  # re-enter next tick
