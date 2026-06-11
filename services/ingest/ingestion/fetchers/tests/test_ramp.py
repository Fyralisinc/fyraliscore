"""Tests for services/ingest/ingestion/fetchers/ramp.py (finance)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import ramp as ramp_fetcher
from services.ingest.ingestion.fetchers.ramp import (
    RampCursor,
    SHARD_KIND_ENTITY,
    fetch_page_ramp,
)


pytestmark = pytest.mark.asyncio


_BUSINESS = "bus-9341452000000001"
_BASE = "https://api.ramp.com/developer/v1"


class _FakeClient:
    """Implements the RampClient list surface the fetcher uses (real keyset
    semantics: `page.next` URL embedding start=<last id>, null at EOF)."""

    def __init__(self, full, delta, page_size=100):
        self._full = full
        self._delta = delta
        self._page_size = page_size
        self.calls: list[dict] = []

    def _page(self, resource, pool, page_size, page_url):
        pos = 0
        if page_url:
            # start=<id> embedded in the next URL
            start = page_url.rsplit("start=", 1)[-1].split("&", 1)[0]
            for i, row in enumerate(pool):
                if row["id"] == start:
                    pos = i + 1
                    break
        per_page = min(page_size, self._page_size)
        page = pool[pos:pos + per_page]
        if len(page) < per_page or not page:
            return page, None
        return page, f"{_BASE}/{resource}?start={page[-1]['id']}&page_size={per_page}"

    async def list_transactions(self, *, from_date=None, to_date=None,
                                state=None, page_size=100, start=None,
                                page_url=None):
        self.calls.append({"resource": "transactions", "from_date": from_date,
                           "page_url": page_url})
        pool = self._delta if from_date else self._full
        return self._page("transactions", pool, page_size, page_url)

    async def list_reimbursements(self, *, updated_after=None, from_date=None,
                                  page_size=100, start=None, page_url=None):
        self.calls.append({"resource": "reimbursements",
                           "updated_after": updated_after,
                           "page_url": page_url})
        pool = self._delta if updated_after else self._full
        return self._page("reimbursements", pool, page_size, page_url)

    async def list_cards(self, *, page_size=100, start=None, page_url=None):
        self.calls.append({"resource": "cards", "page_url": page_url})
        return self._page("cards", self._full, page_size, page_url)

    async def list_users(self, *, page_size=100, start=None, page_url=None):
        self.calls.append({"resource": "users", "page_url": page_url})
        return self._page("users", self._full, page_size, page_url)


class _FakeInst:
    _d = {"business_id": _BUSINESS, "base_url": _BASE, "tenant_id": None,
          "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(ramp_fetcher, "_open_ramp_client", _open)


def _txn(tid, when, state="CLEARED"):
    return {"id": tid, "state": state, "amount": 100.0,
            "currency_code": "USD", "merchant_name": "Acme",
            "user_transaction_time": when}


async def test_full_backfill_tags_records_with_entity_type(monkeypatch):
    rows = [_txn("t-1", "2026-05-01T00:00:00+00:00"),
            _txn("t-2", "2026-05-02T00:00:00+00:00")]
    client = _FakeClient(rows, [])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "transaction",
             "business_id": _BUSINESS}
    res = await fetch_page_ramp(_FakeInst(), shard, None)

    assert len(res.records) == 2
    assert all(r["_fyralis_record_type"] == "transaction" for r in res.records)
    assert all(r["_fyralis_business_id"] == _BUSINESS for r in res.records)
    assert res.end_of_data is True
    cur = RampCursor.model_validate(res.next_cursor)
    # high-water tracks user_transaction_time for the transaction stream.
    assert cur.high_water_updated == "2026-05-02T00:00:00+00:00"
    # FULL mode: no server-side window.
    assert client.calls[0]["from_date"] is None


async def test_keyset_pagination_persists_and_follows_next_url(monkeypatch):
    rows = [_txn(f"t-{i}", f"2026-05-0{i}T00:00:00+00:00") for i in range(1, 6)]
    client = _FakeClient(rows, [], page_size=2)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "transaction",
             "business_id": _BUSINESS}

    # Page 1: full page -> a page.next URL embedding start=<last id>.
    res1 = await fetch_page_ramp(_FakeInst(), shard, None)
    assert [r["entity"]["id"] for r in res1.records] == ["t-1", "t-2"]
    assert res1.end_of_data is False
    cur1 = RampCursor.model_validate(res1.next_cursor)
    assert cur1.next_page_url is not None and "start=t-2" in cur1.next_page_url

    # Page 2: resumes FROM the persisted keyset URL.
    res2 = await fetch_page_ramp(_FakeInst(), shard, res1.next_cursor)
    assert client.calls[-1]["page_url"] == cur1.next_page_url
    assert [r["entity"]["id"] for r in res2.records] == ["t-3", "t-4"]
    assert res2.end_of_data is False

    # Page 3: short page -> page.next null -> terminal.
    res3 = await fetch_page_ramp(_FakeInst(), shard, res2.next_cursor)
    assert [r["entity"]["id"] for r in res3.records] == ["t-5"]
    assert res3.end_of_data is True
    cur3 = RampCursor.model_validate(res3.next_cursor)
    assert cur3.next_page_url is None
    assert cur3.high_water_updated == "2026-05-05T00:00:00+00:00"


async def test_incremental_warm_start_passes_from_date_window(monkeypatch):
    delta = [_txn("t-9", "2026-05-10T00:00:00+00:00")]
    client = _FakeClient([], delta)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "transaction",
             "business_id": _BUSINESS,
             "updated_cursor": "2026-05-09T00:00:00+00:00"}
    res = await fetch_page_ramp(_FakeInst(), shard, None)

    assert client.calls[0]["from_date"] == "2026-05-09T00:00:00+00:00"
    assert len(res.records) == 1
    assert res.records[0]["entity"]["id"] == "t-9"


async def test_reimbursement_warm_start_uses_updated_after(monkeypatch):
    delta = [{"id": "r-1", "state": "REIMBURSED", "amount": 50.0,
              "currency": "USD", "updated_at": "2026-05-10T00:00:00+00:00"}]
    client = _FakeClient([], delta)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "reimbursement",
             "business_id": _BUSINESS,
             "updated_cursor": "2026-05-09T00:00:00+00:00"}
    res = await fetch_page_ramp(_FakeInst(), shard, None)

    assert client.calls[0]["updated_after"] == "2026-05-09T00:00:00+00:00"
    assert len(res.records) == 1
    cur = RampCursor.model_validate(res.next_cursor)
    assert cur.high_water_updated == "2026-05-10T00:00:00+00:00"


async def test_card_stream_has_no_window_and_rewalks_in_full(monkeypatch):
    rows = [{"id": "c-1", "state": "ACTIVE",
             "created_at": "2026-04-01T00:00:00+00:00"}]
    client = _FakeClient(rows, [])
    _wire(monkeypatch, client)

    # Warm-started card shard: NO server-side window exists — full re-walk
    # (idempotent via the state-versioned external_id).
    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "card",
             "business_id": _BUSINESS,
             "updated_cursor": "2026-05-09T00:00:00+00:00"}
    res = await fetch_page_ramp(_FakeInst(), shard, None)

    assert client.calls[0]["resource"] == "cards"
    assert "from_date" not in client.calls[0]
    assert len(res.records) == 1
    assert res.end_of_data is True


async def test_empty_entity_terminates(monkeypatch):
    client = _FakeClient([], [])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "user",
             "business_id": _BUSINESS}
    res = await fetch_page_ramp(_FakeInst(), shard, None)
    assert res.records == []
    assert res.end_of_data is True


async def test_missing_entity_type_is_noop(monkeypatch):
    client = _FakeClient([], [])
    _wire(monkeypatch, client)
    res = await fetch_page_ramp(_FakeInst(), {"shard_kind": SHARD_KIND_ENTITY}, None)
    assert res.records == []
    assert res.end_of_data is True
