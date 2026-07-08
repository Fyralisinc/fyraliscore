"""Tests for services/ingest/ingestion/fetchers/ashby.py."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import ashby as ashby_fetcher
from services.ingest.ingestion.fetchers.ashby import (
    AshbyCursor,
    SHARD_KIND_ENTITY,
    fetch_page_ashby,
)


pytestmark = pytest.mark.asyncio

_ORG = "org-1"


class _FakeClient:
    def __init__(self, pages):
        self.pages = pages
        self.calls: list[dict] = []

    async def list_entities(
        self, category, *, cursor=None, sync_token=None, limit=100,
    ):
        self.calls.append({
            "category": category,
            "cursor": cursor,
            "sync_token": sync_token,
            "limit": limit,
        })
        return self.pages.pop(0)


class _FakeInst:
    _d = {"org_id": _ORG, "tenant_id": None, "secret_ref": None}

    def __getitem__(self, k): return self._d[k]
    def __contains__(self, k): return k in self._d


def _wire(monkeypatch, client):
    async def _open(install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(ashby_fetcher, "_open_ashby_client", _open)


async def test_expanded_entity_record_type_round_trips(monkeypatch):
    client = _FakeClient([
        (
            [{"id": "fb-1", "submittedAt": "2026-05-21T14:00:00Z"}],
            None,
            "sync-1",
        ),
    ])
    _wire(monkeypatch, client)

    shard = {
        "shard_kind": SHARD_KIND_ENTITY,
        "entity_type": "application_feedback",
    }
    res = await fetch_page_ashby(_FakeInst(), shard, None)

    assert res.end_of_data is True
    assert res.records == [
        {
            "_fyralis_record_type": "application_feedback",
            "_fyralis_org_id": _ORG,
            "entity": {"id": "fb-1", "submittedAt": "2026-05-21T14:00:00Z"},
        }
    ]
    cur = AshbyCursor.model_validate(res.next_cursor)
    assert cur.sync_token == "sync-1"
    assert cur.high_water_updated == "2026-05-21T14:00:00Z"


async def test_warm_start_passes_sync_token_for_new_entity(monkeypatch):
    client = _FakeClient([([], None, "sync-2")])
    _wire(monkeypatch, client)

    shard = {
        "shard_kind": SHARD_KIND_ENTITY,
        "entity_type": "user",
        "sync_cursor": "sync-1",
    }
    await fetch_page_ashby(_FakeInst(), shard, None)

    assert client.calls[0]["category"] == "user"
    assert client.calls[0]["sync_token"] == "sync-1"
