"""Tests for services/ingest/ingestion/reconcilers/notion.py (IN-14)."""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from services.ingest.ingestion.reconcilers import RECONCILER_DISPATCH
from services.ingest.ingestion.reconcilers import notion as nt_rec
from services.ingest.ingestion.reconcilers.notion import (
    RESHARE_RECENCY_SCORE,
    SHARD_KIND_DATABASE,
    SHARD_KIND_PAGE_TREE,
    reconcile_notion,
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
    def __init__(self, *, db_latest=None, page_latest=None):
        self.db_latest = db_latest
        self.page_latest = page_latest

    async def latest_database_edit(self, database_id):
        return self.db_latest

    async def latest_page_edit(self):
        return self.page_latest


class _FakePool:
    def __init__(self, install):
        self.install = install

    async def fetchrow(self, _sql, *args):
        return self.install


def _db_shard(state="done", shard_id=None):
    return _FakeRecord(
        id=shard_id or uuid4(),
        state=state,
        shard_identifier={
            "shard_kind": SHARD_KIND_DATABASE,
            "database_id": "db-1", "workspace_id": "ws-1",
        },
    )


def _install():
    return _FakeRecord(
        id=uuid4(), tenant_id=uuid4(), provider="notion",
        installation_id="ws-1", secret_ref=str(uuid4()), enabled=True,
    )


def _run():
    return _FakeRecord(tenant_id=uuid4(), source="notion", status="completed")


def _stub_state(monkeypatch, cursors):
    async def fake_load(_pool, kind, wid):
        if kind != "shard_fetch" or wid not in cursors:
            return None
        return WorkflowState(
            workflow_kind=kind, workflow_id=wid, tenant_id=None,
            state_data={"cursor": cursors[wid]},
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
    monkeypatch.setattr(nt_rec, "load_state", fake_load)


def _stub_client(monkeypatch, fake):
    async def fake_open(install):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(nt_rec, "_open_notion_client", fake_open)


def _wire_pool(monkeypatch, pool):
    monkeypatch.setattr(nt_rec, "_pool_provider", pool)


async def test_clean_when_latest_not_newer(monkeypatch):
    sid = uuid4()
    shard = _db_shard(shard_id=sid)
    _stub_state(monkeypatch, {str(sid): {"last_edited_at": "2025-03-01T00:00:00Z"}})
    _stub_client(monkeypatch, _FakeClient(db_latest="2025-03-01T00:00:00Z"))
    _wire_pool(monkeypatch, _FakePool(_install()))
    decision = await reconcile_notion([shard], _run())
    assert decision.has_gaps is False


async def test_gap_when_database_edited_after_backfill(monkeypatch):
    sid = uuid4()
    shard = _db_shard(shard_id=sid)
    _stub_state(monkeypatch, {str(sid): {"last_edited_at": "2025-03-01T00:00:00Z"}})
    _stub_client(monkeypatch, _FakeClient(db_latest="2025-04-01T00:00:00Z"))
    _wire_pool(monkeypatch, _FakePool(_install()))
    decision = await reconcile_notion([shard], _run())
    assert decision.has_gaps is True
    assert len(decision.new_shards) == 1
    rs = decision.new_shards[0]
    assert rs.parent_shard_id == sid
    assert rs.shard.shard_kind == SHARD_KIND_DATABASE
    assert rs.shard.recency_score == RESHARE_RECENCY_SCORE
    assert rs.shard.shard_identifier["gap_baseline_edited_at"] == "2025-03-01T00:00:00Z"


async def test_page_tree_gap(monkeypatch):
    sid = uuid4()
    shard = _FakeRecord(
        id=sid, state="done",
        shard_identifier={"shard_kind": SHARD_KIND_PAGE_TREE, "workspace_id": "ws-1"},
    )
    _stub_state(monkeypatch, {str(sid): {"last_edited_at": "2025-01-01T00:00:00Z"}})
    _stub_client(monkeypatch, _FakeClient(page_latest="2025-02-01T00:00:00Z"))
    _wire_pool(monkeypatch, _FakePool(_install()))
    decision = await reconcile_notion([shard], _run())
    assert decision.has_gaps is True


async def test_non_done_shards_excluded(monkeypatch):
    _wire_pool(monkeypatch, _FakePool(_install()))
    decision = await reconcile_notion([_db_shard(state="failed")], _run())
    assert decision.has_gaps is False


async def test_no_install_returns_clean(monkeypatch):
    _wire_pool(monkeypatch, _FakePool(None))
    decision = await reconcile_notion([_db_shard()], _run())
    assert decision.has_gaps is False


async def test_dispatch_wired():
    assert RECONCILER_DISPATCH["notion"] is reconcile_notion
