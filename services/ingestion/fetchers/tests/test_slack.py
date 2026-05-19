"""Tests for services/ingestion/fetchers/slack.py (M6.5)."""
from __future__ import annotations

import pytest

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.fetchers import slack as sl
from services.ingestion.fetchers.slack import (
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
    fake = _FakeSlackClient([
        ([{"ts": "1700000.001", "text": "hi"}], None),
    ])
    _patch(monkeypatch, fake)
    r = await fetch_page_slack(
        _FakeInst(),
        {"channel_id": "C1", "team_id": "T", "installation_id": "T"},
        cursor=None,
    )
    rec = r.records[0]
    assert set(rec.keys()) == {
        "channel_id", "team_id", "installation_id", "message", "read_path",
    }
    assert rec["read_path"] == "backfill"


async def test_cursor_strict():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        SlackCursor.model_validate({"next_cursor": None, "extra": True})


async def test_dispatch_wired():
    assert FETCHER_DISPATCH["slack"] is fetch_page_slack
