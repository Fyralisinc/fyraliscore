"""Tests for services/ingest/ingestion/fetchers/grafana.py (IN-GRAFANA)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import FETCHER_DISPATCH
from services.ingest.ingestion.fetchers import grafana as gf
from services.ingest.ingestion.fetchers.grafana import (
    SHARD_KIND_ORG_ANNOTATIONS,
    GrafanaCursor,
    fetch_page_grafana,
)
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel


pytestmark = pytest.mark.asyncio


class _FakeInst:
    _d = {"base_url": "https://acme.grafana.net", "org_id": "1"}

    def __getitem__(self, k):
        return self._d[k]

    def __contains__(self, k):
        return k in self._d


def _ann(aid, time_ms, **extra):
    return {"id": aid, "time": time_ms, "text": f"ann-{aid}", "tags": [], **extra}


class _FakeGrafanaClient:
    """Newest-first annotation window query, recording the (from,to) it saw."""

    def __init__(self, annotations):
        self._anns = list(annotations)
        self.calls: list[dict] = []

    async def list_annotations(self, *, from_ms=None, to_ms=None, limit=100, type_filter=None):
        self.calls.append({"from_ms": from_ms, "to_ms": to_ms, "limit": limit})
        rows = [
            a for a in self._anns
            if (from_ms is None or a["time"] >= from_ms)
            and (to_ms is None or a["time"] <= to_ms)
        ]
        rows.sort(key=lambda a: a["time"], reverse=True)  # newest-first
        return rows[:limit]


def _patch_client(monkeypatch, client):
    async def _open(_install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(gf, "_open_grafana_client", _open)


async def test_dispatch_and_channel_wired():
    assert FETCHER_DISPATCH["grafana"] is fetch_page_grafana
    assert resolve_channel("grafana", "backfill") == "grafana:annotation"
    assert resolve_channel("grafana", "poll") == "grafana:annotation"
    assert resolve_channel("grafana", "webhook") == "grafana:alert"


async def test_full_backfill_walks_backward_and_tracks_high_water(monkeypatch):
    monkeypatch.setenv("GRAFANA_BACKFILL_WINDOW_DAYS", "0")  # all-time floor=None
    monkeypatch.setattr(gf, "_page_size", lambda: 2)
    client = _FakeGrafanaClient([_ann(1, 1000), _ann(2, 2000), _ann(3, 3000)])
    _patch_client(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ORG_ANNOTATIONS, "installation_id": "i1"}

    # Page 1: newest two (3000, 2000); more remain.
    res1 = await fetch_page_grafana(_FakeInst(), shard, None)
    assert res1.end_of_data is False
    assert len(res1.records) == 2
    assert all(r["_fyralis_record_type"] == "annotation" for r in res1.records)
    assert all(r["_fyralis_instance"] == "acme.grafana.net" for r in res1.records)
    cur = GrafanaCursor.model_validate(res1.next_cursor)
    assert cur.high_water_time_ms == 3000
    assert cur.page_to_ms == 1999  # min seen (2000) - 1, walking backward
    assert client.calls[0]["from_ms"] is None  # full walk -> no floor

    # Page 2: the remaining oldest (1000); terminal (short page).
    res2 = await fetch_page_grafana(_FakeInst(), shard, res1.next_cursor)
    assert client.calls[1]["to_ms"] == 1999
    assert res2.end_of_data is True
    assert len(res2.records) == 1
    cur2 = GrafanaCursor.model_validate(res2.next_cursor)
    assert cur2.high_water_time_ms == 3000  # unchanged by the older page
    assert cur2.annotations_seen == 3


async def test_warm_start_sets_incremental_floor(monkeypatch):
    monkeypatch.setattr(gf, "_page_size", lambda: 100)
    client = _FakeGrafanaClient([_ann(1, 1000), _ann(2, 2000), _ann(3, 3000)])
    _patch_client(monkeypatch, client)
    shard = {
        "shard_kind": SHARD_KIND_ORG_ANNOTATIONS,
        "installation_id": "i1",
        "updated_cursor": 2500,  # prior high-water (epoch ms)
    }
    res = await fetch_page_grafana(_FakeInst(), shard, None)
    # Only the annotation newer than the floor (3000) comes back.
    assert client.calls[0]["from_ms"] == 2500
    assert len(res.records) == 1
    assert res.end_of_data is True
    cur = GrafanaCursor.model_validate(res.next_cursor)
    assert cur.high_water_time_ms == 3000
    assert cur.floor_ms == 2500


async def test_empty_org_ends_cleanly(monkeypatch):
    monkeypatch.setenv("GRAFANA_BACKFILL_WINDOW_DAYS", "0")
    _patch_client(monkeypatch, _FakeGrafanaClient([]))
    res = await fetch_page_grafana(
        _FakeInst(), {"shard_kind": SHARD_KIND_ORG_ANNOTATIONS}, None,
    )
    assert res.end_of_data is True
    assert res.records == []
