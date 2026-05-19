"""Tests for services/ingestion/reconcilers/slack.py (M6.5)."""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingestion.reconcilers import slack as sl_rec
from services.ingestion.reconcilers.slack import (
    RESHARE_RECENCY_SCORE,
    SHARD_KIND_CHANNEL_WINDOW,
    reconcile_slack,
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


class _FakeClient:
    def __init__(self, *, newer=None):
        self.newer = newer or []
        self.calls = 0
    async def conversations_history(
        self, *, channel, cursor=None, oldest=None, limit=None,
    ):
        self.calls += 1
        return self.newer, None


class _FakePool:
    def __init__(self, install=None):
        self.install = install
    async def fetchrow(self, _sql, *a):
        return self.install


def _shard(state="done", shard_id=None):
    sid = shard_id or uuid4()
    return _FakeRec(
        id=sid, onboarding_run_id=uuid4(), tenant_id=uuid4(),
        source="slack", shard_kind=SHARD_KIND_CHANNEL_WINDOW,
        shard_identifier={
            "shard_kind": SHARD_KIND_CHANNEL_WINDOW,
            "channel_id": "C1", "channel_name": "general",
            "team_id": "T", "installation_id": "T",
        },
        state=state, parent_shard_id=None, last_error=None,
        observations_seen=0, pages_fetched=1,
        started_at=None, completed_at=None,
    )


def _install():
    return _FakeRec(id=uuid4(), tenant_id=uuid4(),
                    provider="slack", installation_id="T", enabled=True)


def _run():
    return _FakeRec(
        onboarding_run_id=uuid4(), source="slack",
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
    monkeypatch.setattr(sl_rec, "load_state", fake_load)


def _stub_client(monkeypatch, fake):
    async def fake_open(install):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(sl_rec, "_open_slack_client", fake_open)


def _wire_pool(monkeypatch, pool):
    monkeypatch.setattr(sl_rec, "_pool_provider", pool)


async def test_clean_when_no_newer_messages(monkeypatch):
    s = _shard()
    pool = _FakePool(install=_install())
    fake = _FakeClient(newer=[])
    _stub_state(monkeypatch, {
        str(s["id"]): {"newest_seen_ts": "1700000.999"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_slack([s], _run())
    assert decision.has_gaps is False
    assert fake.calls == 1


async def test_reshares_when_newer_messages(monkeypatch):
    sid = uuid4()
    s = _shard(shard_id=sid)
    pool = _FakePool(install=_install())
    fake = _FakeClient(newer=[{"ts": "1800000.000"}])
    _stub_state(monkeypatch, {
        str(sid): {"newest_seen_ts": "1700000.999"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_slack([s], _run())
    assert decision.has_gaps is True
    rs = decision.new_shards[0]
    assert rs.parent_shard_id == sid
    assert rs.shard.recency_score == RESHARE_RECENCY_SCORE
    assert rs.shard.shard_identifier["gap_baseline_ts"] == "1700000.999"


async def test_resharded_failed_excluded(monkeypatch):
    a = _shard(state="done")
    b = _shard(state="reconciliation_resharded")
    c = _shard(state="failed")
    pool = _FakePool(install=_install())
    fake = _FakeClient(newer=[])
    _stub_state(monkeypatch, {
        str(a["id"]): {"newest_seen_ts": "1700000.999"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    await reconcile_slack([a, b, c], _run())
    assert fake.calls == 1  # only the 'done' shard checked


async def test_no_done_shards_clean_without_install_load(monkeypatch):
    pool = _FakePool(install=None)
    _wire_pool(monkeypatch, pool)
    d = await reconcile_slack([_shard(state="failed")], _run())
    assert d.has_gaps is False


async def test_dispatch_wired():
    assert RECONCILER_DISPATCH["slack"] is reconcile_slack
