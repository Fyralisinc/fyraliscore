"""Tests for services/ingestion/reconcilers/discord.py (M6.6)."""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingestion.reconcilers import discord as dc_rec
from services.ingestion.reconcilers.discord import (
    RESHARE_RECENCY_SCORE,
    SHARD_KIND_CHANNEL_WINDOW,
    reconcile_discord,
)
from services.ingestion.workflows.state import WorkflowState


pytestmark = pytest.mark.asyncio


class _FakeRec:
    def __init__(self, **f):
        self._f = f
    def __getitem__(self, k):
        return self._f[k]
    def get(self, k, default=None):
        return self._f.get(k, default)


class _FakeDC:
    def __init__(self, *, after_returns=None):
        self.after_returns = after_returns or []
        self.calls = 0
    async def get_messages(self, *, channel_id, before=None, after=None, limit=None):
        self.calls += 1
        return self.after_returns


class _FakePool:
    def __init__(self, install=None):
        self.install = install
    async def fetchrow(self, _sql, *a):
        return self.install


def _shard(*, state="done", is_sampled=True, shard_id=None):
    sid = shard_id or uuid4()
    return _FakeRec(
        id=sid, onboarding_run_id=uuid4(), tenant_id=uuid4(),
        source="discord", shard_kind=SHARD_KIND_CHANNEL_WINDOW,
        shard_identifier={
            "shard_kind": SHARD_KIND_CHANNEL_WINDOW,
            "guild_id": "G", "channel_id": "C",
            "is_sampled": is_sampled,
            "installation_id": "I",
        },
        state=state, parent_shard_id=None, last_error=None,
        observations_seen=0, pages_fetched=1,
        started_at=None, completed_at=None,
    )


def _install():
    return _FakeRec(id=uuid4(), tenant_id=uuid4(),
                    provider="discord", installation_id="I", enabled=True)


def _run():
    return _FakeRec(
        onboarding_run_id=uuid4(), source="discord",
        tenant_id=uuid4(), status="completed",
        reconciled_at=None, reconciliation_pass_count=0,
    )


def _stub_state(monkeypatch, cursors):
    async def fake_load(_pool, kind, wid):
        if wid not in cursors:
            return None
        return WorkflowState(
            workflow_kind=kind, workflow_id=wid, tenant_id=None,
            state_data={"cursor": cursors[wid]},
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
    monkeypatch.setattr(dc_rec, "load_state", fake_load)


def _stub_client(monkeypatch, fake):
    async def fake_open(install):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(dc_rec, "_open_discord_client", fake_open)


def _wire_pool(monkeypatch, pool):
    monkeypatch.setattr(dc_rec, "_pool_provider", pool)


async def test_only_sampled_channels_checked(monkeypatch):
    """LOAD-BEARING for M6.6: non-sampled shards are skipped entirely."""
    sampled = _shard(is_sampled=True)
    unsampled = _shard(is_sampled=False)
    pool = _FakePool(install=_install())
    fake = _FakeDC(after_returns=[])
    _stub_state(monkeypatch, {
        str(sampled["id"]): {"newest_seen_snowflake": "1000"},
        str(unsampled["id"]): {"newest_seen_snowflake": "1000"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    await reconcile_discord([sampled, unsampled], _run())
    # Only the sampled shard probed.
    assert fake.calls == 1


async def test_reshares_when_newer_messages(monkeypatch):
    sid = uuid4()
    s = _shard(shard_id=sid)
    pool = _FakePool(install=_install())
    fake = _FakeDC(after_returns=[{"id": "2000"}])
    _stub_state(monkeypatch, {
        str(sid): {"newest_seen_snowflake": "1000"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_discord([s], _run())
    assert decision.has_gaps is True
    rs = decision.new_shards[0]
    assert rs.parent_shard_id == sid
    assert rs.shard.recency_score == RESHARE_RECENCY_SCORE
    assert rs.shard.shard_identifier["gap_baseline_snowflake"] == "1000"
    assert rs.shard.shard_identifier["is_sampled"] is True


async def test_clean_when_no_newer(monkeypatch):
    s = _shard()
    pool = _FakePool(install=_install())
    fake = _FakeDC(after_returns=[])
    _stub_state(monkeypatch, {
        str(s["id"]): {"newest_seen_snowflake": "1000"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_discord([s], _run())
    assert decision.has_gaps is False


async def test_resharded_failed_excluded(monkeypatch):
    a = _shard(state="done")
    b = _shard(state="reconciliation_resharded")
    c = _shard(state="failed")
    pool = _FakePool(install=_install())
    fake = _FakeDC(after_returns=[])
    _stub_state(monkeypatch, {
        str(a["id"]): {"newest_seen_snowflake": "1000"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    await reconcile_discord([a, b, c], _run())
    assert fake.calls == 1


async def test_dispatch_wired():
    assert RECONCILER_DISPATCH["discord"] is reconcile_discord
