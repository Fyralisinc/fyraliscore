"""Tests for services/ingestion/planners/gmail.py (M6.3 Phase 1B).

Covers:
  - One shard per active mailbox (single + multi-mailbox installs).
  - Empty-mailbox install returns empty list.
  - shard_kind value is "gmail_mailbox_window".
  - shard_identifier carries the discriminator + per-mailbox fields.
  - Inactive mailboxes (excluded by the S1 loader) don't appear here.
  - Mailboxes with null history_id propagate as None (no synthesis).
  - PLANNER_DISPATCH['gmail'] is wired in.
"""
from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.gmail import (
    SHARD_KIND_MAILBOX_WINDOW,
    plan_shards_gmail,
)


pytestmark = pytest.mark.asyncio


class _FakeRecord:
    """Minimal asyncpg.Record-shaped fake (supports __getitem__ and .get)."""

    def __init__(self, **fields):
        self._fields = fields

    def __getitem__(self, key):
        return self._fields[key]

    def get(self, key, default=None):
        return self._fields.get(key, default)


def _install(*, mailboxes_json: str) -> _FakeRecord:
    return _FakeRecord(
        id=uuid4(),
        tenant_id=uuid4(),
        workspace_domain="acme.com",
        service_account_email="svc@acme-fyralis.iam.gserviceaccount.com",
        scope="gmail.metadata",
        disabled_at=None,
        mailboxes=mailboxes_json,
    )


async def test_single_mailbox_install_returns_one_shard():
    install = _install(mailboxes_json=json.dumps([
        {
            "email_address": "alice@acme.com",
            "google_user_id": "118273645",
            "history_id": "42",
        },
    ]))
    shards = await plan_shards_gmail(uuid4(), install)
    assert len(shards) == 1
    s = shards[0]
    assert isinstance(s, Shard)
    assert s.shard_kind == SHARD_KIND_MAILBOX_WINDOW
    assert s.shard_identifier == {
        "shard_kind": SHARD_KIND_MAILBOX_WINDOW,
        "mailbox_email": "alice@acme.com",
        "user_id": "118273645",
        "initial_history_id": "42",
    }
    assert s.recency_score == 1.0
    assert s.window_start is None and s.window_end is None


async def test_multi_mailbox_install_returns_one_shard_per_mailbox():
    install = _install(mailboxes_json=json.dumps([
        {"email_address": "alice@acme.com", "google_user_id": "1", "history_id": "10"},
        {"email_address": "bob@acme.com",   "google_user_id": "2", "history_id": "20"},
        {"email_address": "carol@acme.com", "google_user_id": "3", "history_id": "30"},
    ]))
    shards = await plan_shards_gmail(uuid4(), install)
    assert len(shards) == 3
    emails = {s.shard_identifier["mailbox_email"] for s in shards}
    assert emails == {"alice@acme.com", "bob@acme.com", "carol@acme.com"}
    for s in shards:
        assert s.shard_kind == SHARD_KIND_MAILBOX_WINDOW
        assert s.shard_identifier["shard_kind"] == SHARD_KIND_MAILBOX_WINDOW


async def test_empty_mailboxes_install_returns_empty_list():
    install = _install(mailboxes_json="[]")
    shards = await plan_shards_gmail(uuid4(), install)
    assert shards == []


async def test_null_history_id_propagates_as_none():
    # Mailbox with pending watch: history_id is NULL. The planner
    # must not synthesize a value — the reconciler handles NULL
    # final_history_id by skipping gap check for that shard.
    install = _install(mailboxes_json=json.dumps([
        {"email_address": "alice@acme.com", "google_user_id": None, "history_id": None},
    ]))
    shards = await plan_shards_gmail(uuid4(), install)
    assert len(shards) == 1
    assert shards[0].shard_identifier["initial_history_id"] is None
    assert shards[0].shard_identifier["user_id"] is None


async def test_planner_decodes_mailboxes_when_passed_as_list():
    # asyncpg may return a JSON aggregate as a parsed list if a
    # global JSON codec is registered. The planner must handle
    # both shapes (str + list).
    install = _install(mailboxes_json=[
        {"email_address": "a@acme.com", "google_user_id": "1", "history_id": "1"},
    ])
    shards = await plan_shards_gmail(uuid4(), install)
    assert len(shards) == 1
    assert shards[0].shard_identifier["mailbox_email"] == "a@acme.com"


async def test_dispatch_table_has_gmail_wired_in():
    # The module-level wire-in must register the planner. This is
    # the cross-cutting "M6.3 plumbed correctly" assertion that
    # M6.4-M6.6 will mirror for their sources.
    assert PLANNER_DISPATCH["gmail"] is plan_shards_gmail
    # And it's no longer the not-implemented stub:
    assert plan_shards_gmail.__name__ == "plan_shards_gmail"


async def test_planner_skips_mailbox_with_missing_email_address():
    # Malformed loader output (shouldn't happen given the loader's
    # JOIN, but defensive): mailboxes without email_address are
    # silently skipped rather than producing a shard with
    # mailbox_email=None.
    install = _install(mailboxes_json=json.dumps([
        {"email_address": "alice@acme.com", "google_user_id": "1", "history_id": "1"},
        {"google_user_id": "2", "history_id": "2"},  # no email_address
    ]))
    shards = await plan_shards_gmail(uuid4(), install)
    assert len(shards) == 1
    assert shards[0].shard_identifier["mailbox_email"] == "alice@acme.com"
