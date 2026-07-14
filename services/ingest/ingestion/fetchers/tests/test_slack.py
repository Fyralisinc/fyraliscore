"""Tests for services/ingest/ingestion/fetchers/slack.py (M6.5)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import FETCHER_DISPATCH
from services.ingest.ingestion.fetchers import slack as sl
from services.ingest.ingestion.fetchers.slack import (
    SHARD_KIND_CHANNEL_WINDOW,
    SlackCursor,
    fetch_page_slack,
)


pytestmark = pytest.mark.asyncio


class _FakeSlackClient:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = 0

    async def conversations_history(self, *, channel, cursor=None, oldest=None, limit=None):
        self.calls += 1
        if not self.pages:
            return [], None
        page_msgs, next_cursor = self.pages.pop(0)
        return page_msgs, next_cursor


class _FakeInst:
    def __getitem__(self, k):
        return {"id": "row"}.get(k, "row")


def _patch(monkeypatch, fake):
    async def fake_open(install):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(sl, "_open_slack_client", fake_open)


def _patch_user(monkeypatch, fake):
    async def fake_open(install, shard_identifier):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(sl, "_open_slack_user_client", fake_open)


async def test_first_page_advances(monkeypatch):
    fake = _FakeSlackClient([
        ([{"ts": "1700000.000001", "text": "hi"},
          {"ts": "1700000.000002", "text": "hi2"}], "p2"),
    ])
    _patch(monkeypatch, fake)
    r = await fetch_page_slack(
        _FakeInst(),
        {"shard_kind": SHARD_KIND_CHANNEL_WINDOW, "channel_id": "C1",
         "installation_id": "T"}, cursor=None,
    )
    assert len(r.records) == 2
    assert r.end_of_data is False
    assert r.next_cursor["next_cursor"] == "p2"
    assert r.next_cursor["newest_seen_ts"] == "1700000.000002"


async def test_channel_with_consenting_user_uses_user_client(monkeypatch):
    fake = _FakeSlackClient([
        ([{"ts": "1700000.000001", "text": "hello1"}], None),
    ])
    _patch_user(monkeypatch, fake)
    called = {"bot": False}

    async def fake_bot_open(install):
        called["bot"] = True
        async def close(): return None
        return fake, close

    monkeypatch.setattr(sl, "_open_slack_client", fake_bot_open)
    r = await fetch_page_slack(
        _FakeInst(),
        {
            "shard_kind": SHARD_KIND_CHANNEL_WINDOW,
            "channel_id": "C_GENERAL",
            "channel_type": "public_channel",
            "consenting_user_id": "U_ALICE",
            "team_id": "T_DEMO",
            "installation_id": "T_DEMO",
        },
        cursor=None,
    )
    assert called["bot"] is False
    assert r.records[0]["event"]["text"] == "hello1"
    assert r.records[0]["event"]["channel_type"] == "public_channel"


async def test_multi_page(monkeypatch):
    fake = _FakeSlackClient([
        ([{"ts": "1700000.001"}], "p2"),
        ([{"ts": "1700000.002"}], None),
    ])
    _patch(monkeypatch, fake)
    r1 = await fetch_page_slack(
        _FakeInst(), {"channel_id": "C1"}, cursor=None,
    )
    assert r1.end_of_data is False
    r2 = await fetch_page_slack(
        _FakeInst(), {"channel_id": "C1"}, cursor=r1.next_cursor,
    )
    assert r2.end_of_data is True


async def test_record_envelope_shape(monkeypatch):
    """A27.3 — records are emitted in the slack:message event_callback
    shape with `channel` injected into the event, so external_id
    ("{channel}:{ts}") matches the live webhook."""
    fake = _FakeSlackClient([
        ([{"ts": "1700000.001", "text": "hi", "user": "U1"}], None),
    ])
    _patch(monkeypatch, fake)
    r = await fetch_page_slack(
        _FakeInst(),
        {"channel_id": "C1", "team_id": "T", "installation_id": "T"},
        cursor=None,
    )
    rec = r.records[0]
    assert set(rec.keys()) == {"type", "team_id", "event"}
    assert rec["type"] == "event_callback"
    assert rec["event"]["channel"] == "C1"
    assert rec["event"]["ts"] == "1700000.001"
    assert rec["event"]["text"] == "hi"


async def test_cursor_strict():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        SlackCursor.model_validate({"next_cursor": None, "extra": True})


async def test_dispatch_wired():
    assert FETCHER_DISPATCH["slack"] is fetch_page_slack
