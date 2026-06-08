"""Tests for services/ingest/ingestion/fetchers/deel.py (finance)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import deel as deel_fetcher
from services.ingest.ingestion.fetchers.deel import (
    DeelCursor,
    SHARD_KIND_CONTRACT_PAYMENTS,
    fetch_page_deel,
)


pytestmark = pytest.mark.asyncio


_CONTRACT = "con-eng-monthly"


class _FakeClient:
    """Implements the DeelClient read surface the fetcher uses."""

    def __init__(self, contract, full, delta):
        self._contract = contract
        self._full = full
        self._delta = delta
        self.calls: list[dict] = []

    async def get_contract(self, contract_id):
        return self._contract

    async def list_payments(self, contract_id, *, limit=100, offset=0, start=None):
        self.calls.append({"offset": offset, "start": start})
        pool = self._delta if start else self._full
        page = pool[offset:offset + limit]
        next_offset = offset + len(page)
        total = len(pool)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total


class _FakeInst:
    _d = {"base_url": "https://api.letsdeel.com", "tenant_id": None,
          "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(deel_fetcher, "_open_deel_client", _open)


async def test_full_backfill_emits_snapshot_plus_payments(monkeypatch):
    contract = {"id": _CONTRACT, "name": "Eng Monthly", "rate": 8500.0,
                "status": "in_progress", "type": "ongoing_time_based"}
    payments = [
        {"id": "p1", "amount": -10.0, "status": "sent",
         "counterpartyName": "A", "createdAt": "2026-05-01T00:00:00Z"},
        {"id": "p2", "amount": 20.0, "status": "sent",
         "counterpartyName": "B", "createdAt": "2026-05-02T00:00:00Z"},
    ]
    client = _FakeClient(contract, payments, [])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_CONTRACT_PAYMENTS, "contract_id": _CONTRACT}
    res = await fetch_page_deel(_FakeInst(), shard, None)

    # 1 contract_snapshot + 2 payments.
    kinds = [r["_fyralis_record_type"] for r in res.records]
    assert kinds.count("contract_snapshot") == 1
    assert kinds.count("payment") == 2
    assert res.end_of_data is True
    cur = DeelCursor.model_validate(res.next_cursor)
    assert cur.seeded is True
    assert cur.high_water_created == "2026-05-02T00:00:00Z"


async def test_incremental_warm_start_uses_start_param(monkeypatch):
    contract = {"id": _CONTRACT, "rate": 0.0, "status": "in_progress"}
    delta = [{"id": "p1", "amount": -10.0, "status": "failed",
              "counterpartyName": "A", "createdAt": "2026-05-10T00:00:00Z"}]
    client = _FakeClient(contract, [], delta)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_CONTRACT_PAYMENTS, "contract_id": _CONTRACT,
             "payment_cursor": "2026-05-09T00:00:00Z"}
    res = await fetch_page_deel(_FakeInst(), shard, None)

    # Warm start -> incremental: list_payments called with start=date.
    assert client.calls[0]["start"] == "2026-05-09"
    payment_records = [r for r in res.records if r["_fyralis_record_type"] == "payment"]
    assert len(payment_records) == 1
    assert payment_records[0]["payment"]["status"] == "failed"


async def test_empty_contract_terminates(monkeypatch):
    client = _FakeClient({"id": _CONTRACT, "rate": 0.0}, [], [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_CONTRACT_PAYMENTS, "contract_id": _CONTRACT}
    res = await fetch_page_deel(_FakeInst(), shard, None)
    # Only the snapshot, no payments; end_of_data on first page.
    assert res.end_of_data is True
    assert all(r["_fyralis_record_type"] == "contract_snapshot" for r in res.records)


async def test_missing_contract_id_is_noop(monkeypatch):
    client = _FakeClient({}, [], [])
    _wire(monkeypatch, client)
    res = await fetch_page_deel(_FakeInst(), {"shard_kind": SHARD_KIND_CONTRACT_PAYMENTS}, None)
    assert res.records == []
    assert res.end_of_data is True
