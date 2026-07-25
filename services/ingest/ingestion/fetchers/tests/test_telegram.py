"""Tests for services/ingest/ingestion/fetchers/telegram.py (IN-TELEGRAM).

Drives the REAL fetcher against the REAL MockTelegramClient + make_telegram
fixture (highest fidelity), covering: dispatch/channel wiring, backward paging on
offset_id, cursor round-trip, one-message-one-record fan-out, end_of_data,
durable FLOOD_WAIT propagation, incremental min_id warm-start, and the
empty-dialog terminal case.
"""
from __future__ import annotations


import pytest

from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.source_contract.runtime import resolve_fetcher
from services.ingest.ingestion.fetchers import telegram as tf
from services.ingest.ingestion.fetchers.telegram import (
    SHARD_KIND_DIALOG_HISTORY,
    TelegramCursor,
    fetch_page_telegram,
)
from services.ingest.ingestion.kafka.topics import INGESTION_SOURCES
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.source_contract.runtime import resolve_planner
from services.ingest.source_contract.runtime import resolve_reconciler
from services.ingest.ingestion.reconcilers.telegram import reconcile_telegram
from services.ingest.synthetic.fixtures import make_telegram
from services.ingest.synthetic.mock_clients import MockTelegramClient


pytestmark = pytest.mark.asyncio


class _FakeInst:
    def __init__(self, tenant_id="11111111-1111-1111-1111-111111111111"):
        self._d = {"id": "inst-uuid", "tenant_id": tenant_id}

    def __getitem__(self, k):
        return self._d[k]

    def __contains__(self, k):
        return k in self._d


def _patch_client(monkeypatch, client):
    async def _open(_install):
        async def _close():
            return None
        return client, _close
    monkeypatch.setattr(tf, "_open_telegram_client", _open)


def _shard(fixture, *, offset_cursor=None):
    did = fixture["dialog_order"][0]
    d = fixture["dialogs"][str(did)]
    return {
        "shard_kind": SHARD_KIND_DIALOG_HISTORY,
        "dialog_id": did,
        "dialog_kind": d["dialog_kind"],
        "access_hash": d["access_hash"],
        "dialog_title": d["title"],
        "installation_id": "inst-uuid",
        "offset_id_cursor": offset_cursor,
    }


async def test_dispatch_and_channel_wired():
    assert resolve_fetcher("telegram") is fetch_page_telegram
    assert resolve_channel("telegram", "backfill") == "telegram:message"
    assert resolve_channel("telegram", "gateway") == "telegram:message"


async def test_contract_drift_telegram_present():
    # Drift guard: Telegram must remain in the wire-source literal and expose
    # all three historical roles through its SourceDefinition.
    assert "telegram" in INGESTION_SOURCES
    assert callable(resolve_planner("telegram"))
    assert callable(resolve_fetcher("telegram"))
    assert callable(resolve_reconciler("telegram"))


async def test_onboarding_and_reconciler_cover_telegram():
    """Drift guard for the two hardcoded per-source enumerations the all-12 gate
    caught (both would strand telegram backfill in production):

      1. tenant_onboarding's applicable-source UNION must read telegram_installations
         (else telegram installs are invisible → no source_onboarding_run).
      2. the reconciler startup must register telegram's pool provider (else
         reconcile_telegram raises RuntimeError → the source run fails).
    """
    import inspect

    from services.ingest.ingestion.workflows import reconciler as _rec
    from services.ingest.ingestion.workflows.tenant_onboarding import VALID_SOURCES
    from services.ingest.source_contract.runtime import (
        resolve_installation_loader,
    )

    assert "telegram" in VALID_SOURCES
    assert callable(resolve_installation_loader("telegram"))
    assert resolve_reconciler("telegram") is reconcile_telegram
    rec_src = inspect.getsource(_rec)
    assert "register_pool_provider(pool)" in rec_src


async def test_backward_paging_full_sweep(monkeypatch):
    fixture = make_telegram(dialogs=1, messages_per_dialog=5, page_size=2, seed="t1")
    _patch_client(monkeypatch, MockTelegramClient(fixture=fixture))
    install, shard = _FakeInst(), _shard(fixture)

    records: list[dict] = []
    cursor = None
    pages = 0
    while True:
        res = await fetch_page_telegram(install, shard, cursor)
        records.extend(res.records)
        cursor = res.next_cursor
        pages += 1
        if res.end_of_data:
            break
        assert pages < 10  # guard against a runaway loop

    # 5 messages → 5 records across ceil(5/2)=3 pages, oldest..newest covered.
    assert pages == 3
    ids = sorted(int(r["id"]) for r in records)
    assert ids == [1, 2, 3, 4, 5]
    # Every record carries the canonical dialog context for the handler.
    assert all(r["_fyralis_record_type"] == "message" for r in records)
    assert all(r["_fyralis_dialog_id"] == shard["dialog_id"] for r in records)
    # Cursor high-water == the newest id seen.
    assert TelegramCursor.model_validate(cursor).high_water_max_id == 5


async def test_one_message_one_record(monkeypatch):
    fixture = make_telegram(dialogs=1, messages_per_dialog=3, page_size=100, seed="t2")
    _patch_client(monkeypatch, MockTelegramClient(fixture=fixture))
    res = await fetch_page_telegram(_FakeInst(), _shard(fixture), None)
    assert res.end_of_data is True
    assert len(res.records) == 3  # one record per message, no fan-out


async def test_flood_wait_propagates_durable_retry_later(monkeypatch):
    fixture = make_telegram(dialogs=1, messages_per_dialog=5, seed="t3")

    class _FloodWait:
        async def get_history(self, **_kwargs):
            raise RetryLater.after(
                request_context=RequestContext(
                    source="telegram",
                    operation="get_history",
                ),
                delay_seconds=30,
                reason=RetryReason.RATE_LIMIT,
            )

    _patch_client(monkeypatch, _FloodWait())
    with pytest.raises(RetryLater) as raised:
        await fetch_page_telegram(_FakeInst(), _shard(fixture), None)
    assert raised.value.context["operation"] == "get_history"


async def test_incremental_warm_start_min_id(monkeypatch):
    # Warm-start at high-water 3 → only messages 4,5 come back (incremental).
    fixture = make_telegram(dialogs=1, messages_per_dialog=5, page_size=100, seed="t4")
    _patch_client(monkeypatch, MockTelegramClient(fixture=fixture))
    res = await fetch_page_telegram(
        _FakeInst(), _shard(fixture, offset_cursor=3), None,
    )
    ids = sorted(int(r["id"]) for r in res.records)
    assert ids == [4, 5]
    assert res.end_of_data is True


async def test_empty_dialog_terminal(monkeypatch):
    fixture = make_telegram(dialogs=1, messages_per_dialog=0, seed="t5")
    _patch_client(monkeypatch, MockTelegramClient(fixture=fixture))
    res = await fetch_page_telegram(_FakeInst(), _shard(fixture), None)
    assert res.records == []
    assert res.end_of_data is True
