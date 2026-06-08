"""Tests for services/ingest/ingestion/fetchers/carta.py (cap-table)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import carta as carta_fetcher
from services.ingest.ingestion.fetchers.carta import (
    CartaCursor,
    SHARD_KIND_ENTITY,
    fetch_page_carta,
)


pytestmark = pytest.mark.asyncio


_FIRM = "firm_9341452000000001"


class _FakeClient:
    """Implements the CartaClient.query surface the fetcher uses."""

    def __init__(self, full, delta):
        self._full = full
        self._delta = delta
        self.calls: list[dict] = []

    async def query(self, entity, *, where=None, order_by=None,
                    start_position=1, max_results=100):
        self.calls.append({"entity": entity, "where": where,
                           "start_position": start_position})
        pool = self._delta if where else self._full
        page = pool[start_position - 1: start_position - 1 + max_results]
        next_start = start_position + len(page)
        is_last = len(page) < max_results or not page
        return page, (None if is_last else next_start)


class _FakeInst:
    _d = {"firm_id": _FIRM, "base_url": "https://x", "tenant_id": None,
          "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(carta_fetcher, "_open_carta_client", _open)


def _grant(gid, sync, updated, status="active"):
    return {"Id": gid, "SyncToken": str(sync), "Status": status,
            "Quantity": 1000, "StrikePrice": 0.25, "DocNumber": gid,
            "MetaData": {"LastUpdatedTime": updated}}


async def test_full_backfill_tags_records_with_entity_type(monkeypatch):
    rows = [_grant("1", 0, "2026-05-01T00:00:00-08:00"),
            _grant("2", 0, "2026-05-02T00:00:00-08:00")]
    client = _FakeClient(rows, [])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "OptionGrant",
             "firm_id": _FIRM}
    res = await fetch_page_carta(_FakeInst(), shard, None)

    assert len(res.records) == 2
    assert all(r["_fyralis_record_type"] == "optiongrant" for r in res.records)
    assert all(r["_fyralis_firm_id"] == _FIRM for r in res.records)
    assert res.end_of_data is True
    cur = CartaCursor.model_validate(res.next_cursor)
    assert cur.high_water_updated == "2026-05-02T00:00:00-08:00"
    # FULL mode: no WHERE filter.
    assert client.calls[0]["where"] is None


async def test_incremental_warm_start_adds_where_filter(monkeypatch):
    delta = [_grant("1", 1, "2026-05-10T00:00:00-08:00", status="exercised")]
    client = _FakeClient([], delta)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "OptionGrant",
             "firm_id": _FIRM, "updated_cursor": "2026-05-09T00:00:00-08:00"}
    res = await fetch_page_carta(_FakeInst(), shard, None)

    assert "LastUpdatedTime > '2026-05-09T00:00:00-08:00'" in client.calls[0]["where"]
    assert len(res.records) == 1
    assert res.records[0]["entity"]["SyncToken"] == "1"
    assert res.records[0]["entity"]["Status"] == "exercised"


async def test_empty_entity_terminates(monkeypatch):
    client = _FakeClient([], [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "SafeNote",
             "firm_id": _FIRM}
    res = await fetch_page_carta(_FakeInst(), shard, None)
    assert res.records == []
    assert res.end_of_data is True


async def test_missing_entity_type_is_noop(monkeypatch):
    client = _FakeClient([], [])
    _wire(monkeypatch, client)
    res = await fetch_page_carta(_FakeInst(), {"shard_kind": SHARD_KIND_ENTITY}, None)
    assert res.records == []
    assert res.end_of_data is True
