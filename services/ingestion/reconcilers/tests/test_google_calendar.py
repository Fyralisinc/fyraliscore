"""Tests for services/ingestion/reconcilers/google_calendar.py (IN-15)."""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.ingestion.reconcilers import RECONCILER_DISPATCH
from services.ingestion.reconcilers import google_calendar as rec
from services.ingestion.reconcilers.google_calendar import (
    SHARD_KIND_EVENTS,
    reconcile_google_calendar,
    set_pool_provider,
)


pytestmark = pytest.mark.asyncio


class _Row(dict):
    pass


class _FakePool:
    async def fetchrow(self, *a, **k):
        return _Row(id=uuid4(), tenant_id=uuid4(), scope="calendar.readonly")


class _FakeCalClient:
    def __init__(self, has_updates):
        self._has = has_updates
        self.last_updated_min = None

    async def has_updates_since(self, **kw):
        self.last_updated_min = kw.get("updated_min")
        return self._has


def _shard(shard_id):
    return _Row(
        id=shard_id,
        state="done",
        shard_identifier=json.dumps({
            "shard_kind": SHARD_KIND_EVENTS,
            "calendar_id": "alice@acme.com",
            "owner_email": "alice@acme.com",
        }),
    )


def _wire(monkeypatch, *, has_updates, high_water="2026-04-20T10:00:00.000Z"):
    set_pool_provider(_FakePool())
    client = _FakeCalClient(has_updates)

    async def fake_open(install):
        async def close(): return None
        return client, close
    monkeypatch.setattr(rec, "_open_calendar_client", fake_open)

    class _State:
        state_data = {"cursor": {"high_water_updated": high_water}}

    async def fake_load_state(pool, kind, sid):
        return _State()
    monkeypatch.setattr(rec, "load_state", fake_load_state)
    return client


async def test_clean_when_no_live_updates(monkeypatch):
    _wire(monkeypatch, has_updates=False)
    decision = await reconcile_google_calendar(
        [_shard(uuid4())], _Row(tenant_id=uuid4()),
    )
    assert decision.has_gaps is False
    assert decision.new_shards == []


async def test_gap_emits_reshared_shard(monkeypatch):
    _wire(monkeypatch, has_updates=True)
    sid = uuid4()
    decision = await reconcile_google_calendar(
        [_shard(sid)], _Row(tenant_id=uuid4()),
    )
    assert decision.has_gaps is True
    assert len(decision.new_shards) == 1
    reshared = decision.new_shards[0]
    assert reshared.parent_shard_id == sid
    assert reshared.shard.recency_score == rec.RESHARE_RECENCY_SCORE
    assert reshared.shard.shard_identifier["parent_shard_id"] == str(sid)


async def test_probe_uses_exclusive_floor(monkeypatch):
    # The probe must be sent at high_water + 1ms so Calendar's INCLUSIVE
    # updatedMin doesn't re-match the boundary event forever (runaway reshare).
    client = _wire(
        monkeypatch, has_updates=False,
        high_water="2026-04-20T10:00:00.000Z",
    )
    await reconcile_google_calendar([_shard(uuid4())], _Row(tenant_id=uuid4()))
    assert client.last_updated_min == "2026-04-20T10:00:00.001Z"


async def test_unparseable_high_water_skips_probe(monkeypatch):
    _wire(monkeypatch, has_updates=True, high_water="not-a-timestamp")
    decision = await reconcile_google_calendar(
        [_shard(uuid4())], _Row(tenant_id=uuid4()),
    )
    assert decision.has_gaps is False


async def test_no_high_water_skips_probe(monkeypatch):
    _wire(monkeypatch, has_updates=True, high_water=None)
    decision = await reconcile_google_calendar(
        [_shard(uuid4())], _Row(tenant_id=uuid4()),
    )
    # without a reference point we never reshare.
    assert decision.has_gaps is False


async def test_no_done_shards_is_clean(monkeypatch):
    set_pool_provider(_FakePool())
    pending = _Row(id=uuid4(), state="pending", shard_identifier="{}")
    decision = await reconcile_google_calendar([pending], _Row(tenant_id=uuid4()))
    assert decision.has_gaps is False


async def test_dispatch_wired():
    assert RECONCILER_DISPATCH["google_calendar"] is reconcile_google_calendar
