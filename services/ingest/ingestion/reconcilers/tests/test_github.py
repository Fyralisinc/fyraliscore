"""Tests for services/ingest/ingestion/reconcilers/github.py (M6.4)."""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from services.ingest.source_contract.runtime import resolve_reconciler
from services.ingest.ingestion.reconcilers import github as gh_rec
from services.ingest.ingestion.reconcilers.github import (
    RESHARE_RECENCY_SCORE,
    SHARD_KIND_REPO_EVENTS,
    reconcile_github,
)
from services.ingest.ingestion.workflows.state import WorkflowState


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
                 list_page=None, head_raises=False):
        self.head_changes = head_changes
        self.head_etag = head_etag
        self.list_page = list_page or []
        self.head_raises = head_raises
        self.head_calls = 0
        self.list_calls = 0
        self.list_directions: list[str] = []

    async def head_repo_events(self, *, owner, repo, event_type, etag):
        self.head_calls += 1
        if self.head_raises:
            from lib.shared.errors import GithubApiError
            raise GithubApiError("transient head probe error",
                                 code="github_api_error")
        return self.head_changes, self.head_etag

    async def list_repo_events(
        self, *, owner, repo, event_type, page, per_page, etag,
        direction="asc",
    ):
        self.list_calls += 1
        self.list_directions.append(direction)
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


async def test_confirm_probe_reads_newest_via_desc(monkeypatch):
    """REGRESSION (P1): the gap-confirm probe must page direction=desc so it
    inspects the NEWEST records. The old asc probe read page-1 oldest records
    (always <= last_seen) and dismissed every gap whose new items sort beyond
    page 1 — i.e. essentially all of them. Assert desc is used AND the gap is
    detected."""
    sid = uuid4()
    shard = _shard(shard_id=sid, last_seen="2025-01-01T00:00:00Z")
    pool = _FakePool(install=_install())
    # Newest record (what a desc page 1 surfaces) is newer than baseline.
    fake = _FakeClient(
        head_changes=True,
        list_page=[{"id": 99, "updated_at": "2026-06-02T00:00:00Z"}],
    )
    _stub_state(monkeypatch, {
        str(sid): {"etag": "W/old", "last_seen_updated_at": "2025-01-01T00:00:00Z"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_github([shard], _run())
    assert decision.has_gaps is True
    assert fake.list_directions == ["desc"], (
        "gap-confirm must page direction=desc (newest-first), not asc"
    )


async def test_head_probe_failure_falls_through_to_confirm(monkeypatch):
    """REGRESSION (P2): a transient head-probe error must NOT be treated as
    'clean' (silent gap loss). It falls through to the authoritative desc
    confirm, which still detects the gap."""
    sid = uuid4()
    shard = _shard(shard_id=sid, last_seen="2025-01-01T00:00:00Z")
    pool = _FakePool(install=_install())
    fake = _FakeClient(
        head_raises=True,
        list_page=[{"id": 99, "updated_at": "2026-06-02T00:00:00Z"}],
    )
    _stub_state(monkeypatch, {
        str(sid): {"etag": "W/old", "last_seen_updated_at": "2025-01-01T00:00:00Z"},
    })
    _stub_client(monkeypatch, fake)
    _wire_pool(monkeypatch, pool)
    decision = await reconcile_github([shard], _run())
    assert decision.has_gaps is True       # gap detected despite head failure
    assert fake.list_calls == 1            # confirm ran (no silent return)
    assert fake.list_directions == ["desc"]


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
    assert resolve_reconciler("github") is reconcile_github
