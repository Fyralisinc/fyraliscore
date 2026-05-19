"""Tests for services/ingestion/reconcilers/gmail.py (M6.3 Phase 1B).

Covers:
  - Clean decision when getProfile.historyId == shard's final_history_id.
  - Re-share decision when getProfile.historyId > final_history_id.
  - Multi-mailbox install with mixed gaps.
  - NULL final_history_id is treated as clean (no reference point).
  - Reshared shards carry parent_shard_id linkage + recency_score=1.5.
  - Reshared shard_kind is "gmail_history_gap".
  - reconciliation_resharded + failed shards are excluded from check.
  - RECONCILER_DISPATCH['gmail'] wire-in.
"""
from __future__ import annotations

import datetime as dt
from typing import Any
from uuid import UUID, uuid4

import pytest

from services.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
    ResharedShard,
)
from services.ingestion.reconcilers import gmail as gmail_reconciler
from services.ingestion.reconcilers.gmail import (
    RESHARE_RECENCY_SCORE,
    SHARD_KIND_HISTORY_GAP,
    SHARD_KIND_MAILBOX_WINDOW,
    reconcile_gmail,
)
from services.ingestion.workflows.state import WorkflowState


pytestmark = pytest.mark.asyncio


class _FakeRecord:
    def __init__(self, **fields):
        self._fields = fields

    def __getitem__(self, key):
        return self._fields[key]

    def get(self, key, default=None):
        return self._fields.get(key, default)


class _FakeGmailClient:
    """Same shape as the fetcher tests' fake but only needs get_profile."""

    def __init__(self, *, profile_by_email: dict[str, dict]):
        self.profile_by_email = profile_by_email
        self.profile_calls: list[dict] = []

    async def get_profile(self, *, user_email, scope):
        self.profile_calls.append({"user_email": user_email, "scope": scope})
        return dict(self.profile_by_email.get(
            user_email, {"historyId": "0"},
        ))


class _FakePool:
    """Stand-in for an asyncpg.Pool.

    - `fetchrow` returns whatever the test injected for the install row.
    - The pool is also handed to `load_state` (from workflows.state),
      which uses .acquire(); to avoid wiring a real connection, we
      intercept load_state at the module seam in each test.
    """

    def __init__(self, *, install: _FakeRecord | None = None):
        self.install_to_return = install
        self.fetchrow_calls: list[Any] = []

    async def fetchrow(self, _sql, tenant_id):
        self.fetchrow_calls.append(tenant_id)
        return self.install_to_return


def _install(scope: str = "gmail.metadata") -> _FakeRecord:
    return _FakeRecord(
        id=uuid4(),
        tenant_id=uuid4(),
        workspace_domain="acme.com",
        service_account_email="svc@acme.iam.gserviceaccount.com",
        scope=scope,
        disabled_at=None,
    )


def _shard(
    *, shard_id=None, mailbox="alice@acme.com", state="done",
    shard_kind=SHARD_KIND_MAILBOX_WINDOW,
) -> _FakeRecord:
    return _FakeRecord(
        id=shard_id or uuid4(),
        onboarding_run_id=uuid4(),
        tenant_id=uuid4(),
        source="gmail",
        shard_kind=shard_kind,
        shard_identifier={
            "shard_kind": shard_kind,
            "mailbox_email": mailbox,
            "user_id": "1",
            "initial_history_id": "100",
        },
        state=state,
        parent_shard_id=None,
        last_error=None,
        observations_seen=0,
        pages_fetched=1,
        started_at=None,
        completed_at=None,
    )


def _run(tenant_id=None) -> _FakeRecord:
    return _FakeRecord(
        onboarding_run_id=uuid4(),
        source="gmail",
        tenant_id=tenant_id or uuid4(),
        status="completed",
        reconciled_at=None,
        reconciliation_pass_count=0,
    )


def _stub_cursor_load(monkeypatch, *, cursors_by_shard: dict):
    """Replace load_state() with a stub that returns canned cursors."""
    async def fake_load_state(_pool, workflow_kind, workflow_id):
        if workflow_kind != "shard_fetch":
            return None
        cursor = cursors_by_shard.get(workflow_id)
        if cursor is None:
            return None
        return WorkflowState(
            workflow_kind=workflow_kind,
            workflow_id=workflow_id,
            tenant_id=None,
            state_data={"cursor": cursor},
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
    monkeypatch.setattr(gmail_reconciler, "load_state", fake_load_state)


def _stub_open_client(monkeypatch, fake_client: _FakeGmailClient):
    async def fake_open(install):
        async def close():
            pass
        return fake_client, close
    monkeypatch.setattr(gmail_reconciler, "_open_gmail_client", fake_open)


def _install_pool(monkeypatch, pool):
    gmail_reconciler.set_pool_provider(pool)
    # restore after test:
    monkeypatch.setattr(gmail_reconciler, "_pool_provider", pool)


# =====================================================================
# Clean path
# =====================================================================
async def test_clean_when_profile_history_id_matches_final(monkeypatch):
    install = _install()
    pool = _FakePool(install=install)
    shard = _shard(mailbox="alice@acme.com")
    fake = _FakeGmailClient(profile_by_email={
        "alice@acme.com": {"historyId": "500"},
    })
    _stub_open_client(monkeypatch, fake)
    _stub_cursor_load(monkeypatch, cursors_by_shard={
        str(shard["id"]): {"final_history_id": "500"},
    })
    _install_pool(monkeypatch, pool)

    decision = await reconcile_gmail([shard], _run(tenant_id=install["tenant_id"]))

    assert decision.has_gaps is False
    assert decision.new_shards == []
    assert len(fake.profile_calls) == 1


async def test_clean_when_profile_history_id_less_than_final(monkeypatch):
    """Defensive: getProfile briefly inconsistent with stored cursor
    (eventual-consistency style) — treat as clean. The reconciler is
    not infallible; periodic re-reconciliation (Phase 5+) catches it."""
    install = _install()
    pool = _FakePool(install=install)
    shard = _shard()
    fake = _FakeGmailClient(profile_by_email={
        "alice@acme.com": {"historyId": "400"},
    })
    _stub_open_client(monkeypatch, fake)
    _stub_cursor_load(monkeypatch, cursors_by_shard={
        str(shard["id"]): {"final_history_id": "500"},
    })
    _install_pool(monkeypatch, pool)

    decision = await reconcile_gmail([shard], _run(tenant_id=install["tenant_id"]))
    assert decision.has_gaps is False


# =====================================================================
# Re-share path
# =====================================================================
async def test_reshare_when_profile_history_id_ahead(monkeypatch):
    install = _install()
    pool = _FakePool(install=install)
    shard_id = uuid4()
    shard = _shard(shard_id=shard_id, mailbox="alice@acme.com")
    fake = _FakeGmailClient(profile_by_email={
        "alice@acme.com": {"historyId": "750"},
    })
    _stub_open_client(monkeypatch, fake)
    _stub_cursor_load(monkeypatch, cursors_by_shard={
        str(shard_id): {"final_history_id": "500"},
    })
    _install_pool(monkeypatch, pool)

    decision = await reconcile_gmail(
        [shard], _run(tenant_id=install["tenant_id"]),
    )

    assert decision.has_gaps is True
    assert len(decision.new_shards) == 1
    reshared = decision.new_shards[0]
    assert isinstance(reshared, ResharedShard)
    assert reshared.parent_shard_id == shard_id
    # Per A17: gap-fill shards get recency boost.
    assert reshared.shard.recency_score == RESHARE_RECENCY_SCORE
    # Per M6.3 convention: gap shards use shard_kind="gmail_history_gap".
    assert reshared.shard.shard_kind == SHARD_KIND_HISTORY_GAP
    ident = reshared.shard.shard_identifier
    assert ident["shard_kind"] == SHARD_KIND_HISTORY_GAP
    assert ident["mailbox_email"] == "alice@acme.com"
    assert ident["start_history_id"] == "500"
    assert ident["end_history_id"] == "750"


async def test_multi_mailbox_mixed_clean_and_gappy(monkeypatch):
    """Multiple done shards under one run; some clean, some need
    re-share. Decision aggregates: has_gaps=True with one new shard
    per gappy mailbox."""
    install = _install()
    pool = _FakePool(install=install)

    a_id, b_id, c_id = uuid4(), uuid4(), uuid4()
    shards = [
        _shard(shard_id=a_id, mailbox="alice@acme.com"),
        _shard(shard_id=b_id, mailbox="bob@acme.com"),
        _shard(shard_id=c_id, mailbox="carol@acme.com"),
    ]
    fake = _FakeGmailClient(profile_by_email={
        "alice@acme.com": {"historyId": "100"},  # clean
        "bob@acme.com":   {"historyId": "350"},  # gap (current > final 200)
        "carol@acme.com": {"historyId": "500"},  # gap (current > final 300)
    })
    _stub_open_client(monkeypatch, fake)
    _stub_cursor_load(monkeypatch, cursors_by_shard={
        str(a_id): {"final_history_id": "100"},
        str(b_id): {"final_history_id": "200"},
        str(c_id): {"final_history_id": "300"},
    })
    _install_pool(monkeypatch, pool)

    decision = await reconcile_gmail(
        shards, _run(tenant_id=install["tenant_id"]),
    )

    assert decision.has_gaps is True
    assert len(decision.new_shards) == 2
    by_parent = {rs.parent_shard_id: rs for rs in decision.new_shards}
    assert b_id in by_parent
    assert c_id in by_parent
    assert a_id not in by_parent
    # Each gap-shard's range is correct:
    assert by_parent[b_id].shard.shard_identifier["start_history_id"] == "200"
    assert by_parent[b_id].shard.shard_identifier["end_history_id"] == "350"
    assert by_parent[c_id].shard.shard_identifier["start_history_id"] == "300"
    assert by_parent[c_id].shard.shard_identifier["end_history_id"] == "500"


# =====================================================================
# Edge cases
# =====================================================================
async def test_null_final_history_id_treated_as_clean(monkeypatch):
    install = _install()
    pool = _FakePool(install=install)
    shard = _shard()
    fake = _FakeGmailClient(profile_by_email={
        "alice@acme.com": {"historyId": "999"},
    })
    _stub_open_client(monkeypatch, fake)
    # Cursor exists but final_history_id is missing.
    _stub_cursor_load(monkeypatch, cursors_by_shard={
        str(shard["id"]): {"final_history_id": None},
    })
    _install_pool(monkeypatch, pool)

    decision = await reconcile_gmail([shard], _run(tenant_id=install["tenant_id"]))
    assert decision.has_gaps is False
    # Without a reference point, getProfile is not called either —
    # the per-shard check short-circuits before the API call.
    assert len(fake.profile_calls) == 0


async def test_resharded_and_failed_shards_excluded_from_check(monkeypatch):
    install = _install()
    pool = _FakePool(install=install)
    a_id = uuid4()
    b_id = uuid4()
    c_id = uuid4()
    shards = [
        _shard(shard_id=a_id, mailbox="alice@acme.com", state="done"),
        _shard(shard_id=b_id, mailbox="bob@acme.com", state="reconciliation_resharded"),
        _shard(shard_id=c_id, mailbox="carol@acme.com", state="failed"),
    ]
    fake = _FakeGmailClient(profile_by_email={
        "alice@acme.com": {"historyId": "100"},
    })
    _stub_open_client(monkeypatch, fake)
    _stub_cursor_load(monkeypatch, cursors_by_shard={
        str(a_id): {"final_history_id": "100"},
    })
    _install_pool(monkeypatch, pool)

    decision = await reconcile_gmail(shards, _run(tenant_id=install["tenant_id"]))

    # Only alice's profile is fetched; bob (resharded) + carol (failed)
    # are excluded.
    assert {c["user_email"] for c in fake.profile_calls} == {"alice@acme.com"}
    assert decision.has_gaps is False


async def test_no_done_shards_returns_clean_without_install_load(monkeypatch):
    """All shards failed → nothing to reconcile → clean without
    even loading the install. M6.2b's reconciler service hands off
    the failure handling."""
    pool = _FakePool(install=None)
    shards = [_shard(state="failed")]
    _install_pool(monkeypatch, pool)
    decision = await reconcile_gmail(shards, _run())
    assert decision.has_gaps is False
    assert len(pool.fetchrow_calls) == 0  # install never loaded


async def test_install_missing_treated_as_clean(monkeypatch):
    """Install was disabled mid-flight. Reconciler treats as clean."""
    pool = _FakePool(install=None)  # install lookup returns nothing
    shard = _shard()
    _stub_cursor_load(monkeypatch, cursors_by_shard={
        str(shard["id"]): {"final_history_id": "500"},
    })
    _install_pool(monkeypatch, pool)
    decision = await reconcile_gmail([shard], _run())
    assert decision.has_gaps is False


async def test_non_numeric_history_id_treated_as_clean(monkeypatch):
    install = _install()
    pool = _FakePool(install=install)
    shard = _shard()
    fake = _FakeGmailClient(profile_by_email={
        "alice@acme.com": {"historyId": "not-a-number"},
    })
    _stub_open_client(monkeypatch, fake)
    _stub_cursor_load(monkeypatch, cursors_by_shard={
        str(shard["id"]): {"final_history_id": "500"},
    })
    _install_pool(monkeypatch, pool)
    decision = await reconcile_gmail([shard], _run(tenant_id=install["tenant_id"]))
    assert decision.has_gaps is False


# =====================================================================
# Wire-in
# =====================================================================
async def test_dispatch_table_has_gmail_wired_in():
    assert RECONCILER_DISPATCH["gmail"] is reconcile_gmail
    # Stub message should no longer be emitted by this entry.


async def test_pool_provider_unset_raises_explicit_error(monkeypatch):
    monkeypatch.setattr(gmail_reconciler, "_pool_provider", None)
    shard = _shard(state="done")
    with pytest.raises(RuntimeError, match="pool provider not registered"):
        await reconcile_gmail([shard], _run())
