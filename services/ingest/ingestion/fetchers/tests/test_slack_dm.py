"""Tests for the `slack_dm_window` branch of fetchers/slack.py.

A DM shard is fetched under the consenting user's xoxp client; each emitted
record is in the same `event_callback` shape the `slack:message` handler
consumes, with `channel` injected (so external_id matches the live webhook)
AND `channel_type` (im/mpim) injected (parity with the live DM event + inline
backfill, which both stamp content.channel_type). The end-to-end check feeds a
fetched DM record through the REAL handler and asserts the external_id +
channel_type the observation would carry.
"""
from __future__ import annotations

import pytest

from services.ingest.ingestion.fetchers import slack as sl
from services.ingest.ingestion.fetchers.slack import (
    SHARD_KIND_DM_WINDOW,
    fetch_page_slack,
)
from services.ingest.ingestion.handlers.slack import handle_slack_message
from services.ingest.synthetic.fixtures import make_slack_dm_workspace
from services.ingest.synthetic.mock_clients import MockSlackUserClient


pytestmark = pytest.mark.asyncio


class _FakeInst:
    def __init__(self, tenant_id="t-uuid"):
        self._f = {"tenant_id": tenant_id, "id": "row"}

    def __getitem__(self, k):
        return self._f.get(k, "row")


def _patch_user(monkeypatch, client):
    async def fake_open(install, shard_identifier):
        async def close():
            return None
        return client, close
    monkeypatch.setattr(sl, "_open_slack_user_client", fake_open)


def _im_shard(channel_id, user="U_ALICE", team="T_DEMO"):
    return {
        "shard_kind": SHARD_KIND_DM_WINDOW,
        "channel_id": channel_id,
        "channel_type": "im",
        "consenting_user_id": user,
        "counterpart_user_id": "U_BOB",
        "team_id": team,
        "installation_id": team,
    }


async def test_dm_records_carry_channel_and_channel_type(monkeypatch):
    fixture = make_slack_dm_workspace(
        team_id="T_DEMO", user_id="U_ALICE", messages_per_dm=4,
    )
    client = MockSlackUserClient(fixture=fixture)
    _patch_user(monkeypatch, client)

    # Pick a real im conversation id from the fixture.
    im_conv = next(
        c for c in fixture["dm_users"][0]["conversations"]
        if c["channel_type"] == "im"
    )
    r = await fetch_page_slack(_FakeInst(), _im_shard(im_conv["id"]), cursor=None)

    assert r.records, "expected DM messages"
    for rec in r.records:
        ev = rec["event"]
        assert ev["channel"] == im_conv["id"]      # injected for external_id
        assert ev["channel_type"] == "im"          # injected for parity
        assert rec["type"] == "event_callback"


async def test_dm_uses_user_client_not_bot(monkeypatch):
    """The DM branch must open the per-user client; the bot opener must NOT
    be touched for a slack_dm_window shard."""
    fixture = make_slack_dm_workspace(team_id="T_DEMO", user_id="U_ALICE")
    client = MockSlackUserClient(fixture=fixture)
    _patch_user(monkeypatch, client)

    called = {"bot": False}

    async def _bot_open(install):
        called["bot"] = True
        async def close():
            return None
        return client, close
    monkeypatch.setattr(sl, "_open_slack_client", _bot_open)

    im_conv = next(
        c for c in fixture["dm_users"][0]["conversations"]
        if c["channel_type"] == "im"
    )
    await fetch_page_slack(_FakeInst(), _im_shard(im_conv["id"]), cursor=None)
    assert called["bot"] is False


async def test_fetched_dm_record_handler_parity(monkeypatch):
    """A DM record produced by the worker fetcher, run through the real
    handler, yields external_id="{channel}:{ts}" and channel_type='im' — the
    same observation the inline backfill produces."""
    fixture = make_slack_dm_workspace(
        team_id="T_DEMO", user_id="U_ALICE", messages_per_dm=2,
    )
    client = MockSlackUserClient(fixture=fixture)
    _patch_user(monkeypatch, client)
    im_conv = next(
        c for c in fixture["dm_users"][0]["conversations"]
        if c["channel_type"] == "im"
    )
    r = await fetch_page_slack(_FakeInst(), _im_shard(im_conv["id"]), cursor=None)

    seen = set()
    for rec in r.records:
        draft = await handle_slack_message(rec, {})
        ts = rec["event"]["ts"]
        assert draft.source_channel == "slack:message"
        assert draft.external_id == f"{im_conv['id']}:{ts}"
        assert draft.content["channel_type"] == "im"
        seen.add(draft.external_id)
    assert len(seen) == len(r.records)  # no collisions


async def test_mpim_shard_stamps_mpim(monkeypatch):
    fixture = make_slack_dm_workspace(team_id="T_DEMO", user_id="U_ALICE")
    client = MockSlackUserClient(fixture=fixture)
    _patch_user(monkeypatch, client)
    mpim_conv = next(
        c for c in fixture["dm_users"][0]["conversations"]
        if c["channel_type"] == "mpim"
    )
    shard = {
        "shard_kind": SHARD_KIND_DM_WINDOW,
        "channel_id": mpim_conv["id"],
        "channel_type": "mpim",
        "consenting_user_id": "U_ALICE",
        "counterpart_user_id": None,
        "team_id": "T_DEMO",
        "installation_id": "T_DEMO",
    }
    r = await fetch_page_slack(_FakeInst(), shard, cursor=None)
    assert r.records
    assert all(rec["event"]["channel_type"] == "mpim" for rec in r.records)
