"""Tests for services/ingestion/reconcilers/github.py (M6.4)."""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingestion.reconcilers import github as gh_rec
from services.ingestion.reconcilers.github import (
    RESHARE_RECENCY_SCORE,
    SHARD_KIND_REPO_EVENTS,
    reconcile_github,
)
from services.ingestion.workflows.state import WorkflowState


pytestmark = pytest.mark.asyncio


class _FakeRecord:
    def __init__(self, **f):
        self._f = f

    def __getitem__(self, k):
        return self._f[k]

    def get(self, k, default=None):
        return self._f.get(k, default)


class _FakeClient:
    def __init__(self, *, head_changes=False, head_etag="W/new",
                 list_page=None):
        self.head_changes = head_changes
        self.head_etag = head_etag
        self.list_page = list_page or []
        self.head_calls = 0
        self.list_calls = 0

    async def head_repo_events(self, *, owner, repo, event_type, etag):
        self.head_calls += 1
        return self.head_changes, self.head_etag

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
    ):
        self.list_calls += 1
        return self.list_page, self.head_etag, None


class _FakePool:
    def __init__(self, install=None):
        self.install = install

    async def fetchrow(self, _sql, *args):
        return self.install


def _shard(state="done", last_seen="2025-01-01T00:00:00Z",
           etag="W/old", shard_id=None):
    sid = shard_id or uuid4()
    return _FakeRecord(
        id=sid,
        onboarding_run_id=uuid4(),
        tenant_id=uuid4(),
        source="github",
        shard_kind=SHARD_KIND_REPO_EVENTS,
        shard_identifier={
            "shard_kind": SHARD_KIND_REPO_EVENTS,
            "owner": "acme", "repo": "api",
            "repo_full_name": "acme/api",
            "event_type": "issues",
            "installation_id": "42",
        },
        state=state,
        parent_shard_id=None, last_error=None,
        observations_seen=0, pages_fetched=1,
        started_at=None, completed_at=None,
    )


def _install():
    return _FakeRecord(
        id=uuid4(), tenant_id=uuid4(),
        provider="github", installation_id="42", enabled=True,
    )


def _run():
    return _FakeRecord(
        onboarding_run_id=uuid4(), source="github",
        tenant_id=uuid4(), status="completed",
        reconciled_at=None, reconciliation_pass_count=0,
    )


def _stub_state(monkeypatch, cursors):
    async def fake_load(_pool, kind, wid):
        if kind != "shard_fetch" or wid not in cursors:
            return None
        return WorkflowState(
            workflow_kind=kind, workflow_id=wid, tenant_id=None,
            state_data={"cursor": cursors[wid]},
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
    monkeypatch.setattr(gh_rec, "load_state", fake_load)


def _stub_client(monkeypatch, fake):
    async def fake_open(install):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(gh_rec, "_open_github_client", fake_open)


def _wire_pool(monkeypatch, pool):
    monkeypatch.setattr(gh_rec, "_pool_provider", pool)


async def test_etag_fastpath_clean(monkeypatch):
    """Etag matches (head returns has_changes=False) → no gap."""
    shard = _shard()
    pool = _FakePool(install=_install())
    fake = _FakeClient(head_changes=False)
    _stub_state(monkeypatch, {
        str(shard["id"]): {"etag": "W/old", "last_seen_updated_at": "2025-01-01T00:00:00Z"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_github([shard], _run())
    assert decision.has_gaps is False
    assert fake.head_calls == 1
    assert fake.list_calls == 0


async def test_reshares_when_newer_updated_at(monkeypatch):
    """Head says changes; first page has record newer than baseline → gap."""
    sid = uuid4()
    shard = _shard(shard_id=sid, last_seen="2025-01-01T00:00:00Z")
    pool = _FakePool(install=_install())
    fake = _FakeClient(
        head_changes=True,
        list_page=[{"id": 99, "updated_at": "2025-02-01T00:00:00Z"}],
    )
    _stub_state(monkeypatch, {
        str(sid): {"etag": "W/old", "last_seen_updated_at": "2025-01-01T00:00:00Z"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_github([shard], _run())
    assert decision.has_gaps is True
    assert len(decision.new_shards) == 1
    rs = decision.new_shards[0]
    assert rs.parent_shard_id == sid
    assert rs.shard.shard_kind == SHARD_KIND_REPO_EVENTS
    assert rs.shard.recency_score == RESHARE_RECENCY_SCORE
    assert rs.shard.shard_identifier["gap_baseline_updated_at"] == \
        "2025-01-01T00:00:00Z"


async def test_changes_but_no_newer_records_still_clean(monkeypatch):
    """Head says changes, but first page's newest is OLDER than baseline → clean."""
    sid = uuid4()
    shard = _shard(shard_id=sid)
    pool = _FakePool(install=_install())
    fake = _FakeClient(
        head_changes=True,
        list_page=[{"id": 1, "updated_at": "2024-01-01T00:00:00Z"}],
    )
    _stub_state(monkeypatch, {
        str(sid): {"etag": "W/old", "last_seen_updated_at": "2025-01-01T00:00:00Z"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_github([shard], _run())
    assert decision.has_gaps is False


async def test_resharded_failed_shards_excluded(monkeypatch):
    a = _shard(state="done")
    b = _shard(state="reconciliation_resharded")
    c = _shard(state="failed")
    pool = _FakePool(install=_install())
    fake = _FakeClient(head_changes=False)
    _stub_state(monkeypatch, {
        str(a["id"]): {"etag": "W/old", "last_seen_updated_at": "2025-01-01T00:00:00Z"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_github([a, b, c], _run())
    assert decision.has_gaps is False
    assert fake.head_calls == 1  # only the 'done' shard checked


async def test_no_done_shards_returns_clean_without_install_load(monkeypatch):
    pool = _FakePool(install=None)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_github([_shard(state="failed")], _run())
    assert decision.has_gaps is False


async def test_dispatch_wired():
    assert RECONCILER_DISPATCH["github"] is reconcile_github
