"""Tests for services/ingest/ingestion/fetchers/gusto.py (finance)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import gusto as gusto_fetcher
from services.ingest.ingestion.fetchers.gusto import (
    GustoCursor,
    SHARD_KIND_ENTITY,
    fetch_page_gusto,
)


pytestmark = pytest.mark.asyncio


_COMPANY = "9341452000000001"


class _FakeClient:
    """Implements the GustoClient.query surface the fetcher uses."""

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
    _d = {"company_uuid": _COMPANY, "base_url": "https://x", "tenant_id": None,
          "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(gusto_fetcher, "_open_gusto_client", _open)


def _inv(iid, sync, updated, balance=100.0):
    return {"Id": iid, "SyncToken": str(sync), "TotalAmt": 100.0,
            "Balance": balance, "DocNumber": iid,
            "MetaData": {"LastUpdatedTime": updated}}


async def test_full_backfill_tags_records_with_entity_type(monkeypatch):
    rows = [_inv("1", 0, "2026-05-01T00:00:00-08:00"),
            _inv("2", 0, "2026-05-02T00:00:00-08:00")]
    client = _FakeClient(rows, [])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "Invoice",
             "company_uuid": _COMPANY}
    res = await fetch_page_gusto(_FakeInst(), shard, None)

    assert len(res.records) == 2
    assert all(r["_fyralis_record_type"] == "invoice" for r in res.records)
    assert all(r["_fyralis_company_uuid"] == _COMPANY for r in res.records)
    assert res.end_of_data is True
    cur = GustoCursor.model_validate(res.next_cursor)
    assert cur.high_water_updated == "2026-05-02T00:00:00-08:00"
    # FULL mode: no WHERE filter.
    assert client.calls[0]["where"] is None


async def test_incremental_warm_start_adds_where_filter(monkeypatch):
    delta = [_inv("1", 1, "2026-05-10T00:00:00-08:00", balance=0.0)]
    client = _FakeClient([], delta)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "Invoice",
             "company_uuid": _COMPANY, "updated_cursor": "2026-05-09T00:00:00-08:00"}
    res = await fetch_page_gusto(_FakeInst(), shard, None)

    assert "LastUpdatedTime > '2026-05-09T00:00:00-08:00'" in client.calls[0]["where"]
    assert len(res.records) == 1
    assert res.records[0]["entity"]["SyncToken"] == "1"


async def test_empty_entity_terminates(monkeypatch):
    client = _FakeClient([], [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "Bill",
             "company_uuid": _COMPANY}
    res = await fetch_page_gusto(_FakeInst(), shard, None)
    assert res.records == []
    assert res.end_of_data is True


async def test_missing_entity_type_is_noop(monkeypatch):
    client = _FakeClient([], [])
    _wire(monkeypatch, client)
    res = await fetch_page_gusto(_FakeInst(), {"shard_kind": SHARD_KIND_ENTITY}, None)
    assert res.records == []
    assert res.end_of_data is True
