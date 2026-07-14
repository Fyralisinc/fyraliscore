"""Tests for Instagram backfill dispatch and fetcher behavior."""
from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from services.ingest.ingestion.fetchers import FETCHER_DISPATCH
from services.ingest.ingestion.fetchers import instagram as fetcher_mod
from services.ingest.ingestion.fetchers.instagram import (
    SHARD_KIND_CONVERSATION_HISTORY,
    fetch_page_instagram,
)
from services.ingest.ingestion.kafka.topics import INGESTION_SOURCES
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.ingestion.planners import PLANNER_DISPATCH
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.instagram import plan_shards_instagram
from services.ingest.ingestion.reconcilers import RECONCILER_DISPATCH


pytestmark = pytest.mark.asyncio


class _FakeInstall:
    def __init__(self) -> None:
        self._data = {
            "id": "inst-1",
            "tenant_id": "11111111-1111-1111-1111-111111111111",
            "ig_business_account_id": "ig-business",
            "page_id": "page-1",
        }

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data


class _FakeClient:
    async def list_conversation_messages(
        self,
        *,
        conversation_id: str,
        limit: int,
        after: str | None = None,
    ):
        assert conversation_id == "conv-1"
        assert after is None
        return [
            {
                "id": "mid-1",
                "created_time": "2026-06-09T12:00:00+00:00",
                "message": "hi",
                "from": {"id": "cust-1"},
                "to": {"data": [{"id": "ig-business"}]},
            }
        ], None


class _Record(dict):
    pass


async def test_registry_and_channel_wired():
    assert "instagram" in INGESTION_SOURCES
    assert "instagram" in PLANNER_DISPATCH
    assert FETCHER_DISPATCH["instagram"] is fetch_page_instagram
    assert "instagram" in RECONCILER_DISPATCH
    assert resolve_channel("instagram", "webhook") == "instagram:message"
    assert resolve_channel("instagram", "backfill") == "instagram:message"
    assert resolve_channel("instagram", "poll") == "instagram:message"


async def test_fetcher_emits_canonical_history_records(monkeypatch: pytest.MonkeyPatch):
    async def _open(_install):
        async def _close():
            return None
        return _FakeClient(), _close

    monkeypatch.setattr(fetcher_mod, "_open_instagram_client", _open)
    result = await fetch_page_instagram(
        _FakeInstall(),  # type: ignore[arg-type]
        {
            "shard_kind": SHARD_KIND_CONVERSATION_HISTORY,
            "provider_conversation_id": "conv-1",
            "ig_business_account_id": "ig-business",
            "page_id": "page-1",
            "participant_id": "cust-1",
        },
        None,
    )

    assert result.end_of_data is True
    assert result.records[0]["_fyralis_record_type"] == "message"
    assert result.records[0]["message_id"] == "mid-1"
    assert result.records[0]["conversation_id"] == "ig-business:cust-1"
    assert result.records[0]["provider_conversation_id"] == "conv-1"
    assert result.next_cursor["high_water_message_id"] == "mid-1"


async def test_fetcher_treats_delivery_alias_as_business_sender(
    monkeypatch: pytest.MonkeyPatch,
):
    class _AliasClient:
        async def list_conversation_messages(self, **_kwargs):
            return [{
                "id": "mid-outbound",
                "created_time": "2026-07-10T12:00:00+00:00",
                "message": "business reply",
                "from": {"id": "meta-delivery-id"},
                "to": {"data": [{"id": "cust-1"}]},
            }], None

    async def _open(_install):
        async def _close():
            return None
        return _AliasClient(), _close

    monkeypatch.setattr(fetcher_mod, "_open_instagram_client", _open)
    result = await fetch_page_instagram(
        _FakeInstall(),  # type: ignore[arg-type]
        {
            "shard_kind": SHARD_KIND_CONVERSATION_HISTORY,
            "provider_conversation_id": "conv-1",
            "ig_business_account_id": "ig-business",
            "webhook_delivery_account_id": "meta-delivery-id",
            "participant_id": "cust-1",
        },
        None,
    )

    assert result.records[0]["direction"] == "outbound"
    assert result.records[0]["thread_key"] == "ig-business:cust-1"


async def test_planner_respects_history_lookback_window():
    ctx = PlannerContext(
        tenant_id=uuid4(),
        install=_Record(
            id="inst-1",
            ig_business_account_id="ig-business",
            page_id="page-1",
            history_lookback_days=90,
            conversations=[
                {
                    "provider_conversation_id": "recent",
                    "last_message_at": "2026-06-09T12:00:00+00:00",
                },
                {
                    "provider_conversation_id": "old",
                    "last_message_at": "2025-01-01T12:00:00+00:00",
                },
            ],
        ),  # type: ignore[arg-type]
        conn=None,  # type: ignore[arg-type]
    )

    shards = await plan_shards_instagram(ctx)

    assert [s.shard_identifier["provider_conversation_id"] for s in shards] == ["recent"]


async def test_planner_discovers_conversations_for_a_replay_run():
    class _DiscoveryClient:
        async def list_conversations(self, **_kwargs):
            return [{
                "id": "conv-2",
                "updated_time": "2026-07-10T12:00:00+00:00",
                "participants": {"data": [
                    {"id": "meta-delivery-id"},
                    {"id": "cust-2", "username": "customer", "name": "Customer"},
                ]},
            }], None

    ctx = PlannerContext(
        tenant_id=uuid4(),
        install=_Record(
            id="inst-1",
            ig_business_account_id="ig-business",
            page_id="page-1",
            webhook_delivery_account_id="meta-delivery-id",
            history_lookback_days=90,
            conversations=[],
        ),  # type: ignore[arg-type]
        conn=None,  # type: ignore[arg-type]
        source_client=_DiscoveryClient(),
    )

    shards = await plan_shards_instagram(ctx)

    assert len(shards) == 1
    assert shards[0].shard_identifier["provider_conversation_id"] == "conv-2"
    assert shards[0].shard_identifier["thread_key"] == "ig-business:cust-2"
    assert shards[0].shard_identifier["webhook_delivery_account_id"] == "meta-delivery-id"


async def test_poll_stops_at_previous_high_water(monkeypatch: pytest.MonkeyPatch):
    class _PollClient:
        async def list_conversation_messages(self, **_kwargs):
            return [
                {
                    "id": "new-mid",
                    "created_time": "2026-07-10T12:00:00+00:00",
                    "message": "new",
                    "from": {"id": "cust-1"},
                    "to": {"data": [{"id": "ig-business"}]},
                },
                {
                    "id": "old-mid",
                    "created_time": "2026-07-10T11:00:00+00:00",
                    "message": "old",
                    "from": {"id": "cust-1"},
                    "to": {"data": [{"id": "ig-business"}]},
                },
            ], "next-page"

    async def _open(_install):
        async def _close():
            return None
        return _PollClient(), _close

    monkeypatch.setattr(fetcher_mod, "_open_instagram_client", _open)
    result = await fetch_page_instagram(
        _FakeInstall(),  # type: ignore[arg-type]
        {
            "shard_kind": SHARD_KIND_CONVERSATION_HISTORY,
            "provider_conversation_id": "conv-1",
            "ig_business_account_id": "ig-business",
            "participant_id": "cust-1",
            "gap_baseline_message_id": "old-mid",
        },
        None,
    )

    assert result.end_of_data is True
    assert [record["message_id"] for record in result.records] == ["new-mid"]
