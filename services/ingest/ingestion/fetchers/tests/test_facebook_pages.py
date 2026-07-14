from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.ingestion.fetchers import FETCHER_DISPATCH
from services.ingest.ingestion.fetchers import facebook_pages as fb
from services.ingest.ingestion.fetchers.facebook_pages import (
    FacebookPagesCursor,
    SHARD_KIND_PAGE_HISTORY,
    fetch_page_facebook_pages,
)
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel


pytestmark = pytest.mark.asyncio


class _FakeInstall:
    def __init__(self) -> None:
        self._data = {
            "id": uuid4(),
            "tenant_id": uuid4(),
            "page_id": "PAGE1",
            "page_name": "Acme",
        }

    def __getitem__(self, key):
        return self._data[key]

    def __contains__(self, key):
        return key in self._data


class _FakeFacebookClient:
    def __init__(self) -> None:
        self.conversation_calls: list[dict] = []
        self.message_calls: list[dict] = []

    async def list_conversations(self, *, page_id, after=None, limit=100):
        self.conversation_calls.append({"page_id": page_id, "after": after})
        if after is None:
            return [{"id": "c1"}, {"id": "c2"}], None
        return [], None

    async def list_messages(self, *, conversation_id, after=None, limit=100):
        self.message_calls.append({"conversation_id": conversation_id, "after": after})
        if conversation_id == "c1" and after is None:
            return [
                {
                    "id": "m2",
                    "created_time": "2024-01-02T00:00:00+0000",
                    "message": "second",
                    "from": {"id": "PSID1"},
                    "to": {"data": [{"id": "PAGE1"}]},
                }
            ], "after-c1"
        if conversation_id == "c1" and after == "after-c1":
            return [
                {
                    "id": "m1",
                    "created_time": "2024-01-01T00:00:00+0000",
                    "message": "first",
                    "from": {"id": "PSID1"},
                    "to": {"data": [{"id": "PAGE1"}]},
                }
            ], None
        if conversation_id == "c2":
            return [], None
        return [], None

    async def upsert_conversation_state(self, **kwargs):
        return None

    async def mark_conversation_exhausted(self, **kwargs):
        return None


def _patch_client(monkeypatch, client):
    async def _open(_install):
        async def _close():
            return None

        return client, _close

    monkeypatch.setattr(fb, "_open_facebook_pages_client", _open)


def _shard(install: _FakeInstall):
    return {
        "shard_kind": SHARD_KIND_PAGE_HISTORY,
        "installation_id": str(install["id"]),
        "page_id": "PAGE1",
        "page_name": "Acme",
    }


async def test_dispatch_and_channel_wired():
    assert FETCHER_DISPATCH["facebook_pages"] is fetch_page_facebook_pages
    assert resolve_channel("facebook_pages", "webhook") == "facebook_pages:message"
    assert resolve_channel("facebook_pages", "backfill") == "facebook_pages:message"


async def test_nested_cursor_walks_all_available_history_without_relisting(monkeypatch):
    install = _FakeInstall()
    client = _FakeFacebookClient()
    _patch_client(monkeypatch, client)

    r1 = await fetch_page_facebook_pages(install, _shard(install), None)
    assert r1.records == []
    cur1 = FacebookPagesCursor.model_validate(r1.next_cursor)
    assert cur1.conversation_listing_exhausted is True
    assert len(cur1.pending_conversations) == 2

    r2 = await fetch_page_facebook_pages(install, _shard(install), r1.next_cursor)
    assert [r["id"] for r in r2.records] == ["m2"]
    assert r2.next_cursor["message_after"] == "after-c1"

    r3 = await fetch_page_facebook_pages(install, _shard(install), r2.next_cursor)
    assert [r["id"] for r in r3.records] == ["m1"]
    assert r3.next_cursor["current_conversation"] is None
    assert r3.next_cursor["conversation_count"] == 1

    r4 = await fetch_page_facebook_pages(install, _shard(install), r3.next_cursor)
    assert r4.records == []
    assert r4.next_cursor["conversation_count"] == 2

    r5 = await fetch_page_facebook_pages(install, _shard(install), r4.next_cursor)
    assert r5.end_of_data is True
    assert r5.next_cursor["exhausted_reason"] == (
        "all_available_history_graph_pagination_exhausted"
    )
    assert client.conversation_calls == [{"page_id": "PAGE1", "after": None}]
