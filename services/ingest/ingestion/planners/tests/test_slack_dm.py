"""Tests for the per-user DM shard planning in planners/slack.py.

The DM planner emits one `slack_dm_window` shard per im/mpim conversation per
CONSENTING user (rows in slack_dm_installations), enumerated under that user's
xoxp token. A revoked token for one user is a partial (logged) gap, not a
whole-plan failure. Channel shards (bot) are emitted alongside.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.ingestion.planners import slack as planner_mod
from services.ingest.ingestion.planners.context import PlannerContext
from services.ingest.ingestion.planners.slack import (
    SHARD_KIND_CHANNEL_WINDOW,
    SHARD_KIND_DM_WINDOW,
    plan_shards_slack,
)
from services.ingest.synthetic.fixtures import make_slack_dm_workspace
from services.ingest.synthetic.mock_clients import MockSlackUserClient


pytestmark = pytest.mark.asyncio


class _FakeRec:
    def __init__(self, **f):
        self._f = f

    def __getitem__(self, k):
        return self._f[k]


class _FakeBotClient:
    def __init__(self, channels):
        self._channels = channels

    async def conversations_list(self):
        return self._channels


class _FakeConn:
    """Returns the given slack_dm_installations rows from `.fetch`."""

    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, _sql, _tenant_id):
        return self._rows


def _dm_row(user_id, team_id="T_DEMO", base_url=None):
    return _FakeRec(
        id=uuid4(), team_id=team_id, user_id=user_id, base_url=base_url,
    )


def _ctx(*, channels, dm_rows, user_clients):
    """Build a PlannerContext with a fake bot client + a fake conn yielding
    consenting-user rows, and monkeypatch the per-user client factory to
    return MockSlackUserClient instances keyed by user_id."""
    install = _FakeRec(
        id=uuid4(), tenant_id=uuid4(), provider="slack",
        installation_id="T_DEMO", enabled=True,
    )
    return PlannerContext(
        tenant_id=uuid4(), install=install,
        conn=_FakeConn(dm_rows),
        source_client=_FakeBotClient(channels),
    )


@pytest.fixture
def patch_user_clients(monkeypatch):
    def _install(clients_by_user):
        async def _factory(*, tenant_id, team_id, user_id, base_url):
            client = clients_by_user.get(user_id)
            if client is None:
                raise RuntimeError(f"no mock user client for {user_id}")
            return client
        monkeypatch.setattr(
            planner_mod, "_open_slack_user_client", _factory,
        )
    return _install


async def test_dm_shards_emitted_per_conversation(patch_user_clients):
    fixture = make_slack_dm_workspace(
        team_id="T_DEMO", user_id="U_ALICE", messages_per_dm=3,
    )
    patch_user_clients({"U_ALICE": MockSlackUserClient(fixture=fixture)})

    ctx = _ctx(
        channels=[{"id": "C1", "name": "general", "team_id": "T_DEMO"}],
        dm_rows=[_dm_row("U_ALICE")],
        user_clients=None,
    )
    shards = await plan_shards_slack(ctx)

    dm = [s for s in shards if s.shard_kind == SHARD_KIND_DM_WINDOW]
    chan = [s for s in shards if s.shard_kind == SHARD_KIND_CHANNEL_WINDOW]
    assert len(chan) == 1
    # 3 im (counterparts) + 1 mpim = 4 DM conversations → 4 DM shards.
    assert len(dm) == 4
    types = {s.shard_identifier["channel_type"] for s in dm}
    assert types == {"im", "mpim"}
    for s in dm:
        assert s.shard_identifier["shard_kind"] == SHARD_KIND_DM_WINDOW
        assert s.shard_identifier["consenting_user_id"] == "U_ALICE"
        assert s.shard_identifier["channel_id"]
        # im carries the counterpart; mpim does not.
        if s.shard_identifier["channel_type"] == "im":
            assert s.shard_identifier["counterpart_user_id"] is not None


async def test_channel_shards_prefer_user_visible_channels(monkeypatch):
    class _UserVisible:
        async def conversations_list(self, *, types="im,mpim"):
            if types == "public_channel,private_channel":
                return [
                    {
                        "id": "C_GENERAL",
                        "name": "general",
                        "team_id": "T_DEMO",
                        "channel_type": "public_channel",
                    },
                    {
                        "id": "G_SECRET",
                        "name": "secret",
                        "team_id": "T_DEMO",
                        "channel_type": "private_channel",
                    },
                ]
            return []

    async def _factory(*, tenant_id, team_id, user_id, base_url):
        return _UserVisible()

    monkeypatch.setattr(planner_mod, "_open_slack_user_client", _factory)
    ctx = _ctx(
        channels=[{"id": "C_BOT_ONLY", "name": "bot", "team_id": "T_DEMO"}],
        dm_rows=[_dm_row("U_ALICE", base_url="https://slack.test/api")],
        user_clients=None,
    )

    shards = await plan_shards_slack(ctx)

    chan = [s for s in shards if s.shard_kind == SHARD_KIND_CHANNEL_WINDOW]
    assert {s.shard_identifier["channel_id"] for s in chan} == {
        "C_GENERAL", "G_SECRET",
    }
    assert all(
        s.shard_identifier["consenting_user_id"] == "U_ALICE" for s in chan
    )
    assert all(
        s.shard_identifier["base_url"] == "https://slack.test/api" for s in chan
    )


async def test_dm_channel_ids_match_inline_console_scheme(patch_user_clients):
    """The worker-fetch DM channel id must equal the gateway console's
    `_dm_channel(user, counterpart)` so a worker-backfilled DM and its inline
    twin dedup to one observation."""
    from services.app.gateway.slack_router import _dm_channel

    fixture = make_slack_dm_workspace(
        team_id="T_DEMO", user_id="U_ALICE",
        counterparts=("U_BOB",), mpim_users=("U_BOB", "U_CAROL", "U_DAVE"),
    )
    patch_user_clients({"U_ALICE": MockSlackUserClient(fixture=fixture)})
    ctx = _ctx(channels=[], dm_rows=[_dm_row("U_ALICE")], user_clients=None)
    shards = await plan_shards_slack(ctx)
    im = [
        s for s in shards
        if s.shard_identifier.get("channel_type") == "im"
    ]
    assert im[0].shard_identifier["channel_id"] == _dm_channel(
        "U_ALICE", "U_BOB",
    )


async def test_multiple_consenting_users(patch_user_clients):
    fa = make_slack_dm_workspace(team_id="T_DEMO", user_id="U_ALICE")
    fb = make_slack_dm_workspace(
        team_id="T_DEMO", user_id="U_BOB", counterparts=("U_EVE",),
    )
    patch_user_clients({
        "U_ALICE": MockSlackUserClient(fixture=fa, user_id="U_ALICE"),
        "U_BOB": MockSlackUserClient(fixture=fb, user_id="U_BOB"),
    })
    ctx = _ctx(
        channels=[], dm_rows=[_dm_row("U_ALICE"), _dm_row("U_BOB")],
        user_clients=None,
    )
    shards = await plan_shards_slack(ctx)
    by_user = {}
    for s in shards:
        if s.shard_kind != SHARD_KIND_DM_WINDOW:
            continue
        by_user.setdefault(s.shard_identifier["consenting_user_id"], 0)
        by_user[s.shard_identifier["consenting_user_id"]] += 1
    assert set(by_user) == {"U_ALICE", "U_BOB"}
    assert all(n > 0 for n in by_user.values())


async def test_revoked_user_token_is_partial_not_fatal(monkeypatch):
    """One user's enumeration failure → that user is skipped; the other
    user's DM shards + the channel shards still land."""
    fixture = make_slack_dm_workspace(team_id="T_DEMO", user_id="U_ALICE")

    class _Revoked:
        async def conversations_list(self, *, types="im,mpim"):
            from services.ingest.integrations.slack.client import SlackApiError
            raise SlackApiError("invalid_auth")

    async def _factory(*, tenant_id, team_id, user_id, base_url):
        if user_id == "U_BAD":
            return _Revoked()
        return MockSlackUserClient(fixture=fixture, user_id="U_ALICE")

    monkeypatch.setattr(planner_mod, "_open_slack_user_client", _factory)
    ctx = _ctx(
        channels=[{"id": "C1", "name": "g", "team_id": "T_DEMO"}],
        dm_rows=[_dm_row("U_BAD"), _dm_row("U_ALICE")],
        user_clients=None,
    )
    shards = await plan_shards_slack(ctx)

    dm_users = {
        s.shard_identifier["consenting_user_id"]
        for s in shards if s.shard_kind == SHARD_KIND_DM_WINDOW
    }
    assert dm_users == {"U_ALICE"}  # U_BAD skipped, not fatal
    assert any(s.shard_kind == SHARD_KIND_CHANNEL_WINDOW for s in shards)


async def test_no_consenting_users_channel_only(patch_user_clients):
    patch_user_clients({})
    ctx = _ctx(
        channels=[{"id": "C1", "name": "g", "team_id": "T_DEMO"}],
        dm_rows=[], user_clients=None,
    )
    shards = await plan_shards_slack(ctx)
    assert all(s.shard_kind == SHARD_KIND_CHANNEL_WINDOW for s in shards)
    assert len(shards) == 1
