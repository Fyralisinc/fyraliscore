"""Tests for services/ingest/ingestion/fetchers/mercury.py (finance)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import mercury as mercury_fetcher
from services.ingest.ingestion.fetchers.mercury import (
    MercuryCursor,
    SHARD_KIND_ACCOUNT_TXNS,
    fetch_page_mercury,
)


pytestmark = pytest.mark.asyncio


_ACCT = "acc-checking"


class _FakeClient:
    """Implements the MercuryClient read surface the fetcher uses."""

    def __init__(self, account, full, delta):
        self._account = account
        self._full = full
        self._delta = delta
        self.calls: list[dict] = []

    async def get_account(self, account_id):
        return self._account

    async def list_transactions(self, account_id, *, limit=100, offset=0, start=None):
        self.calls.append({"offset": offset, "start": start})
        pool = self._delta if start == "2026-05-09" else self._full
        page = pool[offset:offset + limit]
        next_offset = offset + len(page)
        total = len(pool)
        is_last = next_offset >= total or not page
        return page, (None if is_last else next_offset), total


class _FakeInst:
    _d = {"base_url": "https://api.mercury.com/api/v1", "tenant_id": None,
          "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(mercury_fetcher, "_open_mercury_client", _open)


async def test_full_backfill_emits_snapshot_plus_transactions(monkeypatch):
    account = {"id": _ACCT, "name": "Checking", "availableBalance": 100.0,
               "currentBalance": 100.0, "type": "checking"}
    txns = [
        {"id": "t1", "amount": -10.0, "status": "sent",
         "counterpartyName": "A", "createdAt": "2026-05-01T00:00:00Z"},
        {"id": "t2", "amount": 20.0, "status": "sent",
         "counterpartyName": "B", "createdAt": "2026-05-02T00:00:00Z"},
    ]
    client = _FakeClient(account, txns, [])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ACCOUNT_TXNS, "account_id": _ACCT}
    res = await fetch_page_mercury(_FakeInst(), shard, None)

    # 1 account_snapshot + 2 transactions.
    assert client.calls[0]["start"] is not None
    kinds = [r["_fyralis_record_type"] for r in res.records]
    assert kinds.count("account_snapshot") == 1
    assert kinds.count("transaction") == 2
    assert res.end_of_data is True
    cur = MercuryCursor.model_validate(res.next_cursor)
    assert cur.seeded is True
    assert cur.high_water_created == "2026-05-02T00:00:00Z"


async def test_incremental_warm_start_uses_start_param(monkeypatch):
    account = {"id": _ACCT, "availableBalance": 0.0, "currentBalance": 0.0}
    delta = [{"id": "t1", "amount": -10.0, "status": "failed",
              "counterpartyName": "A", "createdAt": "2026-05-10T00:00:00Z"}]
    client = _FakeClient(account, [], delta)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ACCOUNT_TXNS, "account_id": _ACCT,
             "txn_cursor": "2026-05-09T00:00:00Z"}
    res = await fetch_page_mercury(_FakeInst(), shard, None)

    # Warm start -> incremental: list_transactions called with start=date.
    assert client.calls[0]["start"] == "2026-05-09"
    txn_records = [r for r in res.records if r["_fyralis_record_type"] == "transaction"]
    assert len(txn_records) == 1
    assert txn_records[0]["transaction"]["status"] == "failed"


async def test_empty_account_terminates(monkeypatch):
    client = _FakeClient({"id": _ACCT, "availableBalance": 0.0}, [], [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ACCOUNT_TXNS, "account_id": _ACCT}
    res = await fetch_page_mercury(_FakeInst(), shard, None)
    # Only the snapshot, no transactions; end_of_data on first page.
    assert res.end_of_data is True
    assert all(r["_fyralis_record_type"] == "account_snapshot" for r in res.records)


async def test_missing_account_id_is_noop(monkeypatch):
    client = _FakeClient({}, [], [])
    _wire(monkeypatch, client)
    res = await fetch_page_mercury(_FakeInst(), {"shard_kind": SHARD_KIND_ACCOUNT_TXNS}, None)
    assert res.records == []
    assert res.end_of_data is True
