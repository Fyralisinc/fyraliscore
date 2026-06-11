"""Tests for services/ingest/ingestion/fetchers/hibob.py."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import hibob as hibob_fetcher
from services.ingest.ingestion.fetchers.hibob import (
    HibobCursor,
    SHARD_KIND_ENTITY,
    fetch_page_hibob,
)


pytestmark = pytest.mark.asyncio

_COMPANY = "co-1"


class _FakeClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls: list[dict] = []

    async def list_entities(
        self, entity_type, *, limit=100, offset=0, page_cursor=None,
        modified_since=None,
    ):
        self.calls.append({
            "entity_type": entity_type,
            "offset": offset,
            "page_cursor": page_cursor,
            "modified_since": modified_since,
        })
        return self.pages.pop(0)


class _FakeInst:
    _d = {"company_id": _COMPANY, "tenant_id": None, "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(hibob_fetcher, "_open_hibob_client", _open)


async def test_offset_page_cursor_round_trips(monkeypatch):
    client = _FakeClient([
        ([{"id": "e1", "modified": "2026-05-01T00:00:00Z"}], "1"),
    ])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "employee"}
    res = await fetch_page_hibob(_FakeInst(), shard, None)

    assert res.end_of_data is False
    cur = HibobCursor.model_validate(res.next_cursor)
    assert cur.offset == 1
    assert cur.page_cursor is None
    assert cur.high_water_updated == "2026-05-01T00:00:00Z"


async def test_opaque_page_cursor_round_trips(monkeypatch):
    client = _FakeClient([
        ([{"id": "s1", "modified": "2026-05-02T00:00:00Z"}], "opaque-next"),
    ])
    _wire(monkeypatch, client)

    shard = {"shard_kind": SHARD_KIND_ENTITY, "entity_type": "payroll"}
    res = await fetch_page_hibob(_FakeInst(), shard, None)

    cur = HibobCursor.model_validate(res.next_cursor)
    assert cur.page_cursor == "opaque-next"
    assert cur.offset == 0


async def test_warm_start_passes_modified_since(monkeypatch):
    client = _FakeClient([([], None)])
    _wire(monkeypatch, client)

    shard = {
        "shard_kind": SHARD_KIND_ENTITY,
        "entity_type": "employee",
        "updated_cursor": "2026-05-09T00:00:00Z",
    }
    await fetch_page_hibob(_FakeInst(), shard, None)

    assert client.calls[0]["modified_since"] == "2026-05-09T00:00:00Z"
