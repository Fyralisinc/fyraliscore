"""Tests for Instagram poll reconciliation."""
from __future__ import annotations

import json
from uuid import UUID

import pytest

from services.ingest.ingestion.reconcilers import instagram as ig
from services.ingest.ingestion.reconcilers.instagram import (
    SHARD_KIND_CONVERSATION_HISTORY,
    reconcile_instagram,
    set_pool_provider,
)


pytestmark = pytest.mark.asyncio


class _FakePool:
    def __init__(self, installs):
        self.installs = installs

    async def fetch(self, _sql, *_args):
        return self.installs


class _FakeClient:
    async def list_conversation_messages(
        self,
        *,
        conversation_id: str,
        limit: int,
    ):
        assert conversation_id == "conv-1"
        assert limit == 1
        return [{"id": "new-mid"}], None


def _shard():
    return {
        "id": UUID("11111111-1111-1111-1111-111111111111"),
        "state": "done",
        "shard_identifier": json.dumps({
            "shard_kind": SHARD_KIND_CONVERSATION_HISTORY,
            "installation_id": "22222222-2222-2222-2222-222222222222",
            "ig_business_account_id": "ig-business",
            "conversation_id": "conv-1",
        }),
    }


async def test_instagram_gap_reshare_uses_poll_ingress(
    monkeypatch: pytest.MonkeyPatch,
):
    install = {
        "id": UUID("22222222-2222-2222-2222-222222222222"),
        "tenant_id": UUID("33333333-3333-3333-3333-333333333333"),
        "ig_business_account_id": "ig-business",
        "base_url": "https://graph.facebook.com",
        "page_id": "page-1",
        "access_token_ref": "secret-ref",
    }
    set_pool_provider(_FakePool([install]))

    async def _open(_install):
        async def _close():
            return None
        return _FakeClient(), _close

    async def _high_water(_pool, _shard_id):
        return "old-mid"

    monkeypatch.setattr(ig, "_open_instagram_client", _open)
    monkeypatch.setattr(ig, "_load_high_water", _high_water)

    decision = await reconcile_instagram(
        [_shard()], {"tenant_id": UUID("33333333-3333-3333-3333-333333333333")},
    )

    assert decision.has_gaps is True
    reshared = decision.new_shards[0]
    assert reshared.shard.shard_identifier["ingress_kind"] == "poll"
    assert reshared.shard.shard_identifier["messages_cursor"] is None
    assert reshared.shard.shard_identifier["gap_baseline_message_id"] == "old-mid"


async def test_reconciler_discovers_a_new_conversation(
    monkeypatch: pytest.MonkeyPatch,
):
    install = {
        "id": UUID("22222222-2222-2222-2222-222222222222"),
        "tenant_id": UUID("33333333-3333-3333-3333-333333333333"),
        "ig_business_account_id": "ig-business",
        "base_url": "https://graph.instagram.com",
        "page_id": "page-1",
        "webhook_delivery_account_id": "meta-delivery-id",
        "access_token_ref": "secret-ref",
    }
    set_pool_provider(_FakePool([install]))

    class _Client:
        async def list_conversations(self, **_kwargs):
            return [{
                "id": "conv-2",
                "participants": {"data": [
                    {"id": "meta-delivery-id"}, {"id": "cust-2"},
                ]},
            }], None

        async def list_conversation_messages(self, **_kwargs):
            return [{"id": "old-mid"}], None

    async def _open(_install):
        async def _close():
            return None
        return _Client(), _close

    async def _high_water(_pool, _shard_id):
        return "old-mid"

    persisted: list[dict[str, object]] = []

    async def _persist(_pool, **kwargs):
        persisted.extend(kwargs["conversations"])
        return len(kwargs["conversations"])

    monkeypatch.setattr(ig, "_open_instagram_client", _open)
    monkeypatch.setattr(ig, "_load_high_water", _high_water)
    monkeypatch.setattr(ig, "upsert_discovered_conversations", _persist)

    decision = await reconcile_instagram(
        [_shard()], {"tenant_id": UUID("33333333-3333-3333-3333-333333333333")},
    )

    assert decision.has_gaps is True
    assert persisted == [{
        "id": "conv-2",
        "participants": {"data": [
            {"id": "meta-delivery-id"}, {"id": "cust-2"},
        ]},
    }]
    assert any(
        reshared.shard.shard_identifier.get("provider_conversation_id") == "conv-2"
        for reshared in decision.new_shards
    )
    discovered = next(
        reshared.shard.shard_identifier
        for reshared in decision.new_shards
        if reshared.shard.shard_identifier.get("provider_conversation_id") == "conv-2"
    )
    assert discovered["thread_key"] == "ig-business:cust-2"
    assert discovered["webhook_delivery_account_id"] == "meta-delivery-id"
