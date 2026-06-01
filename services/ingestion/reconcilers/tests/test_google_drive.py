"""Tests for services/ingestion/reconcilers/google_drive.py (IN-16)."""
from __future__ import annotations

import json

import pytest

from services.ingestion.reconcilers import RECONCILER_DISPATCH
from services.ingestion.reconcilers import google_drive as gd
from services.ingestion.reconcilers.google_drive import (
    SHARD_KIND_FILES,
    reconcile_google_drive,
    set_pool_provider,
)


pytestmark = pytest.mark.asyncio


class _FakePool:
    def __init__(self, install):
        self._install = install

    async def fetchrow(self, sql, *args):
        return self._install


class _FakeClient:
    def __init__(self, has_changes):
        self._has = has_changes
        self.calls = []

    async def has_changes_since(self, **kw):
        self.calls.append(kw)
        return self._has


def _shard(state="done", shard_id="11111111-1111-1111-1111-111111111111"):
    return {
        "id": shard_id,
        "state": state,
        "shard_identifier": json.dumps({
            "shard_kind": SHARD_KIND_FILES,
            "drive_kind": "my_drive", "drive_id": "my-drive",
            "owner_email": "alice@acme.com",
        }),
    }


def _patch(monkeypatch, *, install, client, start_token):
    set_pool_provider(_FakePool(install))

    async def fake_open(inst):
        async def close():
            return None
        return client, close
    monkeypatch.setattr(gd, "_open_drive_client", fake_open)

    async def fake_load(pool, shard_id):
        return start_token
    monkeypatch.setattr(gd, "_load_shard_start_token", fake_load)


_INSTALL = {"id": "i1", "tenant_id": "t1", "scope": "drive.readonly",
            "workspace_domain": "acme.com", "service_account_email": "sa@x",
            "disabled_at": None}
_RUN = {"tenant_id": "t1"}


async def test_dispatch_registered():
    assert RECONCILER_DISPATCH["google_drive"] is reconcile_google_drive


async def test_gap_detected_reshares(monkeypatch):
    client = _FakeClient(has_changes=True)
    _patch(monkeypatch, install=_INSTALL, client=client, start_token="spt-9")
    decision = await reconcile_google_drive([_shard()], _RUN)
    assert decision.has_gaps is True
    assert len(decision.new_shards) == 1
    reshared = decision.new_shards[0]
    assert reshared.shard.recency_score == gd.RESHARE_RECENCY_SCORE
    # Warm-started from the captured token so it re-walks incrementally.
    assert reshared.shard.shard_identifier["start_page_token"] == "spt-9"


async def test_no_gap_no_reshare(monkeypatch):
    client = _FakeClient(has_changes=False)
    _patch(monkeypatch, install=_INSTALL, client=client, start_token="spt-9")
    decision = await reconcile_google_drive([_shard()], _RUN)
    assert decision.has_gaps is False


async def test_no_token_skips_probe(monkeypatch):
    client = _FakeClient(has_changes=True)
    _patch(monkeypatch, install=_INSTALL, client=client, start_token=None)
    decision = await reconcile_google_drive([_shard()], _RUN)
    assert decision.has_gaps is False
    assert client.calls == []


async def test_non_done_shards_ignored(monkeypatch):
    client = _FakeClient(has_changes=True)
    _patch(monkeypatch, install=_INSTALL, client=client, start_token="spt-9")
    decision = await reconcile_google_drive([_shard(state="in_progress")], _RUN)
    assert decision.has_gaps is False


async def test_no_install_no_gaps(monkeypatch):
    client = _FakeClient(has_changes=True)
    _patch(monkeypatch, install=None, client=client, start_token="spt-9")
    decision = await reconcile_google_drive([_shard()], _RUN)
    assert decision.has_gaps is False
