"""Tests for services/ingest/ingestion/fetchers/fireflies.py."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import fireflies as fireflies_fetcher
from services.ingest.ingestion.fetchers.fireflies import (
    FirefliesCursor,
    SHARD_KIND_TRANSCRIPTS,
    fetch_page_fireflies,
)


pytestmark = pytest.mark.asyncio


_WS = "ws-acme"


class _FakeClient:
    """Implements the FirefliesClient read surface the fetcher uses."""

    def __init__(self, full, delta):
        self._full = full
        self._delta = delta
        self.calls: list[dict] = []

    async def list_transcripts(self, *, limit=50, offset=0, start=None):
        self.calls.append({"offset": offset, "start": start})
        pool = self._delta if start else self._full
        page = pool[offset:offset + limit]
        next_offset = offset + len(page)
        total = len(pool)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total


class _FakeInst:
    _d = {"base_url": "https://api.fireflies.ai", "tenant_id": None,
          "secret_ref": None, "workspace_id": _WS}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(fireflies_fetcher, "_open_fireflies_client", _open)


async def test_full_backfill_emits_one_record_per_transcript(monkeypatch):
    txns = [
        {"id": "t1", "title": "Sync", "dateTime": "2026-05-01T00:00:00Z"},
        {"id": "t2", "title": "Demo", "dateTime": "2026-05-02T00:00:00Z"},
    ]
    client = _FakeClient(txns, [])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_TRANSCRIPTS, "workspace_id": _WS}
    res = await fetch_page_fireflies(_FakeInst(), shard, None)

    # NO snapshot record — one transcript record per transcript.
    kinds = [r["_fyralis_record_type"] for r in res.records]
    assert kinds == ["transcript", "transcript"]
    assert res.end_of_data is True
    cur = FirefliesCursor.model_validate(res.next_cursor)
    assert cur.seeded is True
    assert cur.high_water_created == "2026-05-02T00:00:00Z"


async def test_incremental_warm_start_uses_start_param(monkeypatch):
    delta = [{"id": "t9", "title": "Late call", "dateTime": "2026-05-10T00:00:00Z"}]
    client = _FakeClient([], delta)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_TRANSCRIPTS, "workspace_id": _WS,
             "transcript_cursor": "2026-05-09T00:00:00Z"}
    res = await fetch_page_fireflies(_FakeInst(), shard, None)

    # Warm start -> incremental: list_transcripts called with the ISO floor.
    assert client.calls[0]["start"] == "2026-05-09T00:00:00Z"
    txn_records = [r for r in res.records if r["_fyralis_record_type"] == "transcript"]
    assert len(txn_records) == 1
    assert txn_records[0]["transcript"]["id"] == "t9"


async def test_epoch_millis_date_bumps_high_water(monkeypatch):
    epoch_ms = 1_777_593_600_000
    client = _FakeClient([{"id": "t-ms", "title": "Millis", "date": epoch_ms}], [])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_TRANSCRIPTS, "workspace_id": _WS}
    res = await fetch_page_fireflies(_FakeInst(), shard, None)

    cur = FirefliesCursor.model_validate(res.next_cursor)
    assert cur.high_water_created == "2026-05-01T00:00:00+00:00"


async def test_empty_workspace_terminates(monkeypatch):
    client = _FakeClient([], [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_TRANSCRIPTS, "workspace_id": _WS}
    res = await fetch_page_fireflies(_FakeInst(), shard, None)
    assert res.end_of_data is True
    assert res.records == []


async def test_missing_workspace_id_is_noop(monkeypatch):
    client = _FakeClient([], [])
    _wire(monkeypatch, client)
    res = await fetch_page_fireflies(
        _FakeInst(), {"shard_kind": SHARD_KIND_TRANSCRIPTS}, None,
    )
    assert res.records == []
    assert res.end_of_data is True
