"""Tests for the `slack_dm_window` gap-check branch of reconcilers/slack.py.

DM shards gap-probe under the consenting user's xoxp token (a bot token can't
read DMs); a re-shared DM gap carries shard_kind='slack_dm_window' + the
per-user identity so the re-fetch routes back through the DM fetch branch.
"""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from services.ingest.ingestion.reconcilers import slack as sl_rec
from services.ingest.ingestion.reconcilers.slack import (
    SHARD_KIND_DM_WINDOW,
    reconcile_slack,
)
from services.ingest.ingestion.workflows.state import WorkflowState


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


def _dm_shard(state="done", shard_id=None):
    sid = shard_id or uuid4()
    return _FakeRec(
        id=sid, onboarding_run_id=uuid4(), tenant_id=uuid4(),
        source="slack", shard_kind=SHARD_KIND_DM_WINDOW,
        shard_identifier={
            "shard_kind": SHARD_KIND_DM_WINDOW,
            "channel_id": "D0COWORKER1",
            "channel_type": "im",
            "consenting_user_id": "U_ALICE",
            "counterpart_user_id": "U_BOB",
            "team_id": "T_DEMO", "installation_id": "T_DEMO",
        },
        state=state,
    )


def _install():
    return _FakeRec(id=uuid4(), tenant_id=uuid4(),
                    provider="slack", installation_id="T_DEMO", enabled=True)


def _run():
    return _FakeRec(
        onboarding_run_id=uuid4(), source="slack",
        tenant_id=uuid4(), status="completed",
        reconciliation_pass_count=0,
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


def _stub_bot(monkeypatch):
    async def fake_open(install):
        async def close():
            return None
        return _FakeClient(newer=[]), close
    monkeypatch.setattr(sl_rec, "_open_slack_client", fake_open)


def _stub_user(monkeypatch, fake):
    opened = {"count": 0}

    async def fake_open(install, ident):
        opened["count"] += 1
        async def close():
            return None
        return fake, close
    monkeypatch.setattr(sl_rec, "_open_slack_user_client", fake_open)
    return opened


async def test_dm_gap_uses_user_client_and_reshares_dm(monkeypatch):
    sid = uuid4()
    s = _dm_shard(shard_id=sid)
    pool = _FakePool(install=_install())
    user_fake = _FakeClient(newer=[{"ts": "1800000.000"}])
    _stub_state(monkeypatch, {str(sid): {"newest_seen_ts": "1700000.999"}})
    _stub_bot(monkeypatch)
    opened = _stub_user(monkeypatch, user_fake)
    monkeypatch.setattr(sl_rec, "_pool_provider", pool)

    decision = await reconcile_slack([s], _run())

    assert decision.has_gaps is True
    assert opened["count"] == 1  # DM shard probed via the USER client
    assert user_fake.calls == 1
    rs = decision.new_shards[0]
    gid = rs.shard.shard_identifier
    assert rs.shard.shard_kind == SHARD_KIND_DM_WINDOW
    assert gid["shard_kind"] == SHARD_KIND_DM_WINDOW
    assert gid["channel_type"] == "im"
    assert gid["consenting_user_id"] == "U_ALICE"
    assert gid["counterpart_user_id"] == "U_BOB"
    assert gid["gap_baseline_ts"] == "1700000.999"


async def test_dm_clean_when_no_newer(monkeypatch):
    sid = uuid4()
    s = _dm_shard(shard_id=sid)
    pool = _FakePool(install=_install())
    user_fake = _FakeClient(newer=[])
    _stub_state(monkeypatch, {str(sid): {"newest_seen_ts": "1700000.999"}})
    _stub_bot(monkeypatch)
    _stub_user(monkeypatch, user_fake)
    monkeypatch.setattr(sl_rec, "_pool_provider", pool)

    decision = await reconcile_slack([s], _run())
    assert decision.has_gaps is False
    assert user_fake.calls == 1
