"""Tests for services/ingest/ingestion/fetchers/figma.py (design)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import figma as figma_fetcher
from services.ingest.ingestion.fetchers.figma import (
    FigmaCursor,
    SHARD_KIND_FILE_EVENTS,
    fetch_page_figma,
)


pytestmark = pytest.mark.asyncio


_FILE = "file-abc"
_TEAM = "team-1"


class _FakeClient:
    """Implements the FigmaClient read surface the fetcher uses."""

    def __init__(self, file_meta, full, delta):
        self._file = file_meta
        self._full = full
        self._delta = delta
        self.calls: list[dict] = []

    async def get_file(self, file_key):
        return self._file

    async def list_events(self, file_key, *, limit=100, offset=0, start=None):
        self.calls.append({"offset": offset, "start": start})
        pool = self._delta if start else self._full
        page = pool[offset:offset + limit]
        next_offset = offset + len(page)
        total = len(pool)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total


class _FakeInst:
    _d = {"base_url": "https://api.figma.com", "tenant_id": None,
          "secret_ref": None, "team_id": _TEAM}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(figma_fetcher, "_open_figma_client", _open)


async def test_full_backfill_emits_one_record_per_event(monkeypatch):
    file_meta = {"key": _FILE, "name": "Design", "lastModified": "2026-05-02T00:00:00Z"}
    events = [
        {"id": "e1", "event_type": "FILE_VERSION_UPDATE", "version": "v-1",
         "team_id": _TEAM, "file_key": _FILE, "createdAt": "2026-05-01T00:00:00Z"},
        {"id": "e2", "event_type": "FILE_COMMENT", "version": "v-1",
         "team_id": _TEAM, "file_key": _FILE, "createdAt": "2026-05-02T00:00:00Z"},
    ]
    client = _FakeClient(file_meta, events, [])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_FILE_EVENTS, "file_key": _FILE, "team_id": _TEAM}
    res = await fetch_page_figma(_FakeInst(), shard, None)

    # No snapshot — exactly one record per event.
    kinds = [r["_fyralis_record_type"] for r in res.records]
    assert kinds == ["event", "event"]
    assert all(r["_fyralis_team_id"] == _TEAM for r in res.records)
    assert res.end_of_data is True
    cur = FigmaCursor.model_validate(res.next_cursor)
    assert cur.seeded is True
    assert cur.high_water_created == "2026-05-02T00:00:00Z"


async def test_incremental_warm_start_uses_start_param(monkeypatch):
    file_meta = {"key": _FILE}
    delta = [{"id": "e1", "event_type": "FILE_VERSION_UPDATE", "version": "v-2",
              "team_id": _TEAM, "file_key": _FILE, "createdAt": "2026-05-10T00:00:00Z"}]
    client = _FakeClient(file_meta, [], delta)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_FILE_EVENTS, "file_key": _FILE, "team_id": _TEAM,
             "event_cursor": "2026-05-09T00:00:00Z"}
    res = await fetch_page_figma(_FakeInst(), shard, None)

    # Warm start -> incremental: list_events called with start=date.
    assert client.calls[0]["start"] == "2026-05-09"
    event_records = [r for r in res.records if r["_fyralis_record_type"] == "event"]
    assert len(event_records) == 1
    assert event_records[0]["event"]["version"] == "v-2"


async def test_empty_file_terminates(monkeypatch):
    client = _FakeClient({"key": _FILE}, [], [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_FILE_EVENTS, "file_key": _FILE, "team_id": _TEAM}
    res = await fetch_page_figma(_FakeInst(), shard, None)
    assert res.end_of_data is True
    assert res.records == []


async def test_missing_file_key_is_noop(monkeypatch):
    client = _FakeClient({}, [], [])
    _wire(monkeypatch, client)
    res = await fetch_page_figma(_FakeInst(), {"shard_kind": SHARD_KIND_FILE_EVENTS}, None)
    assert res.records == []
    assert res.end_of_data is True
