"""Tests for services/ingest/ingestion/fetchers/carta.py (cap-table)."""
from __future__ import annotations

import pytest

from lib.shared.errors import CartaApiError
from services.ingest.ingestion.fetchers import carta as carta_fetcher
from services.ingest.ingestion.fetchers.carta import (
    CartaCursor,
    SHARD_KIND_ENTITY,
    fetch_page_carta,
)


pytestmark = pytest.mark.asyncio


_ISSUER = "f6e1d4a0-0000-4000-8000-000000000001"


class _FakeClient:
    """Implements the CartaClient.list_entity surface the fetcher uses
    (AIP-158 pageToken pagination)."""

    def __init__(self, full, delta=()):
        self._full = list(full)
        self._delta = list(delta)
        self.calls: list[dict] = []

    async def list_entity(self, entity_type, *, page_size=50,
                          page_token=None, modified_after=None):
        self.calls.append({"entity_type": entity_type,
                           "page_token": page_token,
                           "modified_after": modified_after})
        pool = self._delta if modified_after is not None else self._full
        offset = int(page_token) if page_token else 0
        page = pool[offset:offset + page_size]
        end = offset + len(page)
        next_token = str(end) if page and end < len(pool) else None
        return page, next_token


class _FakeInst:
    _d = {"firm_id": _ISSUER, "base_url": "https://x", "tenant_id": None,
          "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(carta_fetcher, "_open_carta_client", _open)


def _grant(gid, modified, *, exercised="0"):
    """A v1alpha1 option grant (wrapper-shaped decimals/datetimes)."""
    return {"id": gid, "issuerId": _ISSUER, "securityLabel": f"OG-{gid}",
            "quantity": {"value": "1000"},
            "exercisedQuantity": {"value": exercised},
            "exercisePrice": {"currencyCode": {"value": "USD"},
                              "amount": {"value": "0.25"}},
            "lastModifiedDatetime": {"value": modified}}


def _stakeholder(sid):
    return {"id": sid, "issuerId": _ISSUER, "fullName": f"Holder {sid}",
            "relationship": "EMPLOYEE"}


async def test_full_backfill_tags_records_with_entity_type(monkeypatch):
    rows = [_grant("1", "2026-05-01T00:00:00Z"),
            _grant("2", "2026-05-02T00:00:00Z")]
    client = _FakeClient(rows)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "optionGrant",
             "firm_id": _ISSUER}
    res = await fetch_page_carta(_FakeInst(), shard, None)

    assert len(res.records) == 2
    assert all(r["_fyralis_record_type"] == "optiongrant" for r in res.records)
    assert all(r["_fyralis_firm_id"] == _ISSUER for r in res.records)
    assert res.end_of_data is True
    cur = CartaCursor.model_validate(res.next_cursor)
    assert cur.high_water_modified == "2026-05-02T00:00:00Z"
    # FULL mode: no lastModifiedDatetimeAfter bound.
    assert client.calls[0]["modified_after"] is None


async def test_multi_page_pagination_threads_page_token(monkeypatch):
    monkeypatch.setenv("CARTA_BACKFILL_PAGE_SIZE", "2")
    rows = [_grant(str(i), f"2026-05-0{i}T00:00:00Z") for i in range(1, 4)]
    client = _FakeClient(rows)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "optionGrant",
             "firm_id": _ISSUER}
    first = await fetch_page_carta(_FakeInst(), shard, None)
    assert len(first.records) == 2
    assert first.end_of_data is False
    assert CartaCursor.model_validate(first.next_cursor).page_token == "2"

    second = await fetch_page_carta(_FakeInst(), shard, first.next_cursor)
    assert len(second.records) == 1
    assert second.end_of_data is True
    # The opaque nextPageToken from page 1 was passed straight through.
    assert client.calls[1]["page_token"] == "2"
    cur = CartaCursor.model_validate(second.next_cursor)
    assert cur.page_token is None
    assert cur.rows_seen == 3


async def test_incremental_warm_start_passes_modified_after(monkeypatch):
    delta = [_grant("1", "2026-05-10T00:00:00Z", exercised="1000")]
    client = _FakeClient([], delta)
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "optionGrant",
             "firm_id": _ISSUER, "updated_cursor": "2026-05-09T00:00:00Z"}
    res = await fetch_page_carta(_FakeInst(), shard, None)

    assert client.calls[0]["modified_after"] == "2026-05-09T00:00:00Z"
    assert len(res.records) == 1
    assert res.records[0]["entity"]["exercisedQuantity"]["value"] == "1000"
    cur = CartaCursor.model_validate(res.next_cursor)
    # The high-water advanced past the warm-start floor.
    assert cur.high_water_modified == "2026-05-10T00:00:00Z"
    assert cur.incremental_floor == "2026-05-09T00:00:00Z"


async def test_warm_start_is_full_rewalk_for_non_delta_collections(monkeypatch):
    """Only optionGrants has lastModifiedDatetimeAfter; a warm-started
    stakeholder shard must re-walk FULL (no modified_after bound)."""
    client = _FakeClient([_stakeholder("7")])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "stakeholder",
             "firm_id": _ISSUER, "updated_cursor": "2026-05-09T00:00:00Z"}
    res = await fetch_page_carta(_FakeInst(), shard, None)

    assert client.calls[0]["modified_after"] is None
    assert len(res.records) == 1
    assert res.records[0]["_fyralis_record_type"] == "stakeholder"


async def test_rate_limited_yields_same_cursor_not_end_of_data(monkeypatch):
    class _RateLimited:
        async def list_entity(self, *a, **k):
            raise CartaApiError(
                "429", code="carta_api_rate_limited",
                context={"http_status": 429},
            )
    _wire(monkeypatch, _RateLimited())

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "optionGrant",
             "firm_id": _ISSUER}
    res = await fetch_page_carta(_FakeInst(), shard, None)
    assert res.records == []
    assert res.end_of_data is False
    # Cursor preserved (seeded but not advanced) so ShardFetch retries later.
    cur = CartaCursor.model_validate(res.next_cursor)
    assert cur.page_token is None
    assert cur.rows_seen == 0


async def test_empty_entity_terminates(monkeypatch):
    client = _FakeClient([])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "convertibleNote",
             "firm_id": _ISSUER}
    res = await fetch_page_carta(_FakeInst(), shard, None)
    assert res.records == []
    assert res.end_of_data is True


async def test_missing_entity_type_is_noop(monkeypatch):
    client = _FakeClient([])
    _wire(monkeypatch, client)
    res = await fetch_page_carta(_FakeInst(), {"shard_kind": SHARD_KIND_ENTITY}, None)
    assert res.records == []
    assert res.end_of_data is True


async def test_unknown_entity_type_is_noop(monkeypatch):
    client = _FakeClient([_grant("1", "2026-05-01T00:00:00Z")])
    _wire(monkeypatch, client)
    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "SafeNote",
             "firm_id": _ISSUER}
    res = await fetch_page_carta(_FakeInst(), shard, None)
    assert res.records == []
    assert res.end_of_data is True
