"""Tests for services/ingest/ingestion/fetchers/telegram.py (IN-TELEGRAM).

Drives the REAL fetcher against the REAL MockTelegramClient + make_telegram
fixture (highest fidelity), covering: dispatch/channel wiring, backward paging on
offset_id, cursor round-trip, one-message-one-record fan-out, end_of_data, the
FLOOD_WAIT backoff (empty page, cursor unadvanced), incremental min_id warm-start,
and the empty-dialog terminal case.
"""
from __future__ import annotations

import os

import pytest

from services.ingest.ingestion.fetchers import FETCHER_DISPATCH
from services.ingest.ingestion.fetchers import telegram as tf
from services.ingest.ingestion.fetchers.telegram import (
    SHARD_KIND_DIALOG_HISTORY,
    TelegramCursor,
    fetch_page_telegram,
)
from services.ingest.ingestion.kafka.topics import INGESTION_SOURCES
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.ingestion.planners import PLANNER_DISPATCH
from services.ingest.ingestion.reconcilers import RECONCILER_DISPATCH
from services.ingest.synthetic.fixtures import make_telegram
from services.ingest.synthetic.fault_profiles import FaultProfile
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
    assert FETCHER_DISPATCH["telegram"] is fetch_page_telegram
    assert resolve_channel("telegram", "backfill") == "telegram:message"
    assert resolve_channel("telegram", "gateway") == "telegram:message"


async def test_registry_drift_telegram_present():
    # Drift guard: telegram must be in every per-source registry (the
    # SourceLiteral-derived INGESTION_SOURCES + the three dispatch tables).
    assert "telegram" in INGESTION_SOURCES
    assert "telegram" in PLANNER_DISPATCH
    assert "telegram" in FETCHER_DISPATCH
    assert "telegram" in RECONCILER_DISPATCH


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
    from services.ingest.ingestion.workflows.tenant_onboarding import (
        _LOAD_ACTIVE_SOURCES_SQL,
    )

    assert "telegram_installations" in _LOAD_ACTIVE_SOURCES_SQL
    rec_src = inspect.getsource(_rec)
    assert "telegram_reconciler_mod.set_pool_provider" in rec_src


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


async def test_flood_wait_returns_empty_unadvanced(monkeypatch):
    fixture = make_telegram(dialogs=1, messages_per_dialog=5, seed="t3")
    # rate_limit_after_n_requests=0 → the first get_history raises FLOOD_WAIT.
    mock = MockTelegramClient(
        fixture=fixture, profile=FaultProfile(rate_limit_after_n_requests=0),
    )
    _patch_client(monkeypatch, mock)
    res = await fetch_page_telegram(_FakeInst(), _shard(fixture), None)
    # Backoff posture: no records, NOT terminal (ShardFetch re-enters), cursor
    # unadvanced (offset_id still 0).
    assert res.records == []
    assert res.end_of_data is False
    assert TelegramCursor.model_validate(res.next_cursor).offset_id == 0


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
