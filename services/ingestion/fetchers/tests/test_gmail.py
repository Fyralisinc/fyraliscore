"""Tests for services/ingestion/fetchers/gmail.py (M6.3 Phase 1B).

Covers:
  - users.messages.list pagination + cursor advance.
  - Last page stamps final_history_id from users.getProfile.
  - Record envelope shape matches A18's two-path coexistence framing.
  - users.history.list gap-fill path.
  - Dispatch on shard_kind (mailbox_window vs history_gap).
  - FETCHER_DISPATCH['gmail'] wire-in.
  - N1 invariant verification at service level (separate file —
    test_gmail_n1_invariant.py).
"""
from __future__ import annotations

from typing import Any

import pytest

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.fetchers import gmail as gmail_fetcher
from services.ingestion.fetchers.gmail import (
    GmailCursor,
    SHARD_KIND_HISTORY_GAP,
    SHARD_KIND_MAILBOX_WINDOW,
    fetch_page_gmail,
)


pytestmark = pytest.mark.asyncio


class _FakeGmailClient:
    """In-process Gmail client capturing every API call + returning
    pre-canned responses. Indexed by call site (list, get, profile,
    history) so tests can simulate multi-page paging deterministically.
    """

    def __init__(
        self,
        *,
        list_pages: list[dict] | None = None,
        history_pages: list[dict] | None = None,
        messages: dict[str, dict] | None = None,
        profile: dict | None = None,
    ):
        self.list_pages = list(list_pages or [])
        self.history_pages = list(history_pages or [])
        self.messages = dict(messages or {})
        self.profile = dict(profile or {"historyId": "1"})
        self.list_calls: list[dict] = []
        self.history_calls: list[dict] = []
        self.get_calls: list[dict] = []
        self.profile_calls: list[dict] = []

    async def messages_list(self, **kwargs):
        self.list_calls.append(kwargs)
        if not self.list_pages:
            return {"messages": [], "nextPageToken": None}
        return self.list_pages.pop(0)

    async def history_list(self, **kwargs):
        self.history_calls.append(kwargs)
        if not self.history_pages:
            return {"history": [], "nextPageToken": None, "historyId": "1"}
        return self.history_pages.pop(0)

    async def get_message(self, *, user_email, scope, message_id):
        self.get_calls.append({"message_id": message_id, "user_email": user_email})
        return self.messages.get(
            message_id,
            {"id": message_id, "threadId": f"thread-{message_id}", "snippet": "..."},
        )

    async def get_profile(self, **kwargs):
        self.profile_calls.append(kwargs)
        return dict(self.profile)


class _FakeInstall:
    """asyncpg.Record stand-in for gmail_installations rows."""

    def __init__(self, **fields):
        from uuid import uuid4
        defaults = {
            "id": uuid4(),
            "tenant_id": uuid4(),
            "workspace_domain": "acme.com",
            "service_account_email": "svc@acme.iam.gserviceaccount.com",
            "scope": "gmail.metadata",
        }
        defaults.update(fields)
        self._fields = defaults

    def __getitem__(self, key):
        return self._fields[key]


def _patch_client(monkeypatch, fake: _FakeGmailClient):
    """Replace the module-level _open_gmail_client with a fake."""
    close_calls: list[bool] = []

    async def _fake_open(install):
        async def close():
            close_calls.append(True)
        return fake, close

    monkeypatch.setattr(gmail_fetcher, "_open_gmail_client", _fake_open)
    return close_calls


# =====================================================================
# users.messages.list backfill path
# =====================================================================
async def test_initial_backfill_first_page_advances_cursor(monkeypatch):
    fake = _FakeGmailClient(
        list_pages=[
            {
                "messages": [
                    {"id": "m1", "threadId": "t1"},
                    {"id": "m2", "threadId": "t2"},
                ],
                "nextPageToken": "p2",
            },
        ],
        profile={"historyId": "9999"},
    )
    _patch_client(monkeypatch, fake)
    install = _FakeInstall()
    shard_id = {
        "shard_kind": SHARD_KIND_MAILBOX_WINDOW,
        "mailbox_email": "alice@acme.com",
        "user_id": "1",
        "initial_history_id": "100",
    }
    result = await fetch_page_gmail(install, shard_id, cursor=None)

    assert isinstance(result, FetchResult)
    assert len(result.records) == 2
    assert result.end_of_data is False
    # Cursor advances to next page; final_history_id NOT yet stamped
    # (only on the last page).
    assert result.next_cursor["page_token"] == "p2"
    assert result.next_cursor["final_history_id"] is None
    assert result.next_cursor["messages_seen"] == 2


async def test_initial_backfill_last_page_stamps_final_history_id(monkeypatch):
    fake = _FakeGmailClient(
        list_pages=[
            {
                "messages": [{"id": "m9"}],
                "nextPageToken": None,  # last page
            },
        ],
        profile={"historyId": "5000"},
    )
    _patch_client(monkeypatch, fake)
    install = _FakeInstall()
    shard_id = {
        "shard_kind": SHARD_KIND_MAILBOX_WINDOW,
        "mailbox_email": "alice@acme.com",
        "user_id": "1",
    }
    cursor = {"page_token": "pN", "messages_seen": 5, "final_history_id": None}
    result = await fetch_page_gmail(install, shard_id, cursor=cursor)

    assert result.end_of_data is True
    # final_history_id stamped from getProfile — this is the
    # reconciler's reference point.
    assert result.next_cursor["final_history_id"] == "5000"
    assert len(fake.profile_calls) == 1
    assert fake.profile_calls[0]["user_email"] == "alice@acme.com"


async def test_record_envelope_matches_handler_payload_shape(monkeypatch):
    """A18 two-path coexistence: the framework record shape MUST
    match the inline handler's raw_payload shape so a future
    normalizer can read either source."""
    fake = _FakeGmailClient(
        list_pages=[{"messages": [{"id": "m1"}], "nextPageToken": None}],
        messages={"m1": {
            "id": "m1", "threadId": "t1", "snippet": "hi",
            "payload": {"headers": [{"name": "Subject", "value": "Test"}]},
        }},
        profile={"historyId": "1"},
    )
    _patch_client(monkeypatch, fake)
    install = _FakeInstall(scope="gmail.readonly")
    install_id = str(install["id"])
    shard_id = {
        "shard_kind": SHARD_KIND_MAILBOX_WINDOW,
        "mailbox_email": "alice@acme.com",
        "user_id": "1",
    }
    result = await fetch_page_gmail(install, shard_id, cursor=None)

    rec = result.records[0]
    assert set(rec.keys()) == {
        "message_resource", "mailbox_email", "scope_used",
        "gmail_installation_id", "read_path",
    }
    assert rec["mailbox_email"] == "alice@acme.com"
    assert rec["scope_used"] == "gmail.readonly"
    assert rec["gmail_installation_id"] == install_id
    # A27.3 — the gmail: handler validates read_path ∈ {push, poll};
    # backfill conforms as "poll" (external_id is install+message_id,
    # independent of read_path).
    assert rec["read_path"] == "poll"
    assert rec["message_resource"]["id"] == "m1"
    assert "payload" in rec["message_resource"]


async def test_get_message_failure_skips_individual_message(monkeypatch):
    """A single message that fails (e.g., 404 because it was deleted
    after listing) is skipped; the rest of the page still goes through.
    Same shape as the inline fetcher's exception swallow."""
    from services.integrations.gmail.client import GoogleApiError

    class _OneFails(_FakeGmailClient):
        async def get_message(self, *, user_email, scope, message_id):
            if message_id == "bad":
                raise GoogleApiError("404", status=404)
            return await super().get_message(
                user_email=user_email, scope=scope, message_id=message_id,
            )

    fake = _OneFails(
        list_pages=[{
            "messages": [{"id": "good1"}, {"id": "bad"}, {"id": "good2"}],
            "nextPageToken": None,
        }],
        profile={"historyId": "1"},
    )
    _patch_client(monkeypatch, fake)
    install = _FakeInstall()
    shard_id = {
        "shard_kind": SHARD_KIND_MAILBOX_WINDOW,
        "mailbox_email": "alice@acme.com",
    }
    result = await fetch_page_gmail(install, shard_id, cursor=None)
    ids = [r["message_resource"]["id"] for r in result.records]
    assert ids == ["good1", "good2"]
    assert result.end_of_data is True


# =====================================================================
# users.history.list gap-fill path
# =====================================================================
async def test_gap_fill_dispatches_to_history_list(monkeypatch):
    fake = _FakeGmailClient(
        history_pages=[
            {
                "history": [
                    {
                        "id": "h1",
                        "messagesAdded": [
                            {"message": {"id": "gm1"}},
                            {"message": {"id": "gm2"}},
                        ],
                    },
                ],
                "historyId": "200",
                "nextPageToken": None,
            },
        ],
    )
    _patch_client(monkeypatch, fake)
    install = _FakeInstall()
    shard_id = {
        "shard_kind": SHARD_KIND_HISTORY_GAP,
        "mailbox_email": "alice@acme.com",
        "user_id": "1",
        "start_history_id": "100",
        "end_history_id": "200",
        "parent_shard_id": "00000000-0000-0000-0000-000000000001",
    }
    result = await fetch_page_gmail(install, shard_id, cursor=None)

    # Must call history.list, NOT messages.list.
    assert len(fake.history_calls) == 1
    assert len(fake.list_calls) == 0
    assert fake.history_calls[0]["start_history_id"] == "100"
    assert len(result.records) == 2
    for rec in result.records:
        # A27.3 — gap-fill records also conform to the handler as
        # "poll" (the producing path is diagnostic, kept in the cursor).
        assert rec["read_path"] == "poll"
    assert result.end_of_data is True


async def test_gap_fill_terminates_when_reaching_end_history_id(monkeypatch):
    # historyId on the response >= end_history_id triggers end-of-data
    # even if there are still pages on Gmail's side.
    fake = _FakeGmailClient(
        history_pages=[
            {
                "history": [{"id": "h1", "messagesAdded": []}],
                "historyId": "250",  # past end
                "nextPageToken": "still-more",
            },
        ],
    )
    _patch_client(monkeypatch, fake)
    install = _FakeInstall()
    shard_id = {
        "shard_kind": SHARD_KIND_HISTORY_GAP,
        "mailbox_email": "a@acme.com",
        "start_history_id": "100",
        "end_history_id": "200",
    }
    result = await fetch_page_gmail(install, shard_id, cursor=None)
    assert result.end_of_data is True


# =====================================================================
# Dispatch + cursor handling
# =====================================================================
async def test_default_dispatch_is_mailbox_window(monkeypatch):
    """Backward-compat: shard_identifier without shard_kind defaults
    to mailbox_window (calls messages.list, not history.list)."""
    fake = _FakeGmailClient(
        list_pages=[{"messages": [], "nextPageToken": None}],
        profile={"historyId": "1"},
    )
    _patch_client(monkeypatch, fake)
    install = _FakeInstall()
    shard_id = {"mailbox_email": "a@acme.com"}  # no shard_kind key
    result = await fetch_page_gmail(install, shard_id, cursor=None)
    assert len(fake.list_calls) == 1
    assert len(fake.history_calls) == 0
    assert result.end_of_data is True


async def test_cursor_round_trips_through_pydantic_model(monkeypatch):
    fake = _FakeGmailClient(
        list_pages=[{"messages": [{"id": "m1"}], "nextPageToken": "pX"}],
        profile={"historyId": "1"},
    )
    _patch_client(monkeypatch, fake)
    install = _FakeInstall()
    shard_id = {"shard_kind": SHARD_KIND_MAILBOX_WINDOW, "mailbox_email": "a@a"}
    cursor_in = {
        "page_token": "pIn", "messages_seen": 3, "final_history_id": None,
        "start_history_id": None, "end_history_id": None,
    }
    result = await fetch_page_gmail(install, shard_id, cursor=cursor_in)
    # Verify the call used the input page_token (cursor was decoded
    # then re-used on the API call).
    assert fake.list_calls[0]["page_token"] == "pIn"
    # Verify the returned next_cursor matches GmailCursor's shape.
    assert "page_token" in result.next_cursor
    assert "messages_seen" in result.next_cursor


async def test_unknown_scope_raises_value_error(monkeypatch):
    fake = _FakeGmailClient()
    _patch_client(monkeypatch, fake)
    install = _FakeInstall(scope="bogus.scope")
    shard_id = {"shard_kind": SHARD_KIND_MAILBOX_WINDOW, "mailbox_email": "a@a"}
    with pytest.raises(ValueError, match="unknown scope alias"):
        await fetch_page_gmail(install, shard_id, cursor=None)


# =====================================================================
# Wire-in assertion
# =====================================================================
async def test_dispatch_table_has_gmail_wired_in():
    assert FETCHER_DISPATCH["gmail"] is fetch_page_gmail


async def test_gmail_cursor_model_rejects_extra_fields():
    # extra='forbid' is load-bearing — silently-accepted unknown
    # fields would mask cursor-schema drift between fetcher and
    # reconciler.
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        GmailCursor.model_validate({"page_token": None, "bogus_field": True})
