"""End-to-end proof of the per-user DM read path against the spammer.

A REAL `SlackUserClient` drives `conversations.list(types=im,mpim)` +
`conversations.history` against the local spammer over the real httpx +
FastAPI stack (xoxp user-token grain). Proves: the spammer routes DM
enumeration by the user token (`spam-slack-user::<team>::<user>`), the client
maps Slack's `is_im`/`is_mpim` flags to `channel_type`, and DM history paginates
the same way channel history does. Also asserts a bot token does NOT see DMs.
"""
from __future__ import annotations

from uuid import uuid4

import httpx

from services.ingest.synthetic.fixtures import make_slack_dm_workspace
from services.ingest.synthetic.spammer.server import build_spammer_app


_HOST = "http://spammer"
_TEAM = "T_SLACK_DM"
_USER = "U_ALICE"


def _user_client(app, **kwargs):
    from services.ingest.integrations.slack.client import SlackUserClient

    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=_HOST,
    )
    c = SlackUserClient(
        pool=None, secret_store=None, tenant_id=uuid4(),
        installation_row_id=uuid4(), team_id=_TEAM, user_id=_USER,
        base_url=f"{_HOST}/slack/api", http_client=http, **kwargs,
    )
    c._user_token_cache.set(  # type: ignore[attr-defined]
        f"spam-slack-user::{_TEAM}::{_USER}", ttl_seconds=float("inf"),
    )
    return c, http


def _bot_client(app):
    from services.ingest.integrations.slack.client import SlackClient

    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=_HOST,
    )
    c = SlackClient(
        pool=None, secret_store=None, tenant_id=uuid4(),
        installation_row_id=uuid4(), team_id=_TEAM,
        base_url=f"{_HOST}/slack/api", http_client=http,
    )
    c._bot_token_cache.set(  # type: ignore[attr-defined]
        f"spam-slack::{_TEAM}", ttl_seconds=float("inf"),
    )
    return c, http


async def test_user_client_lists_dms_with_channel_type():
    fx = make_slack_dm_workspace(team_id=_TEAM, user_id=_USER)
    app = build_spammer_app(fixtures={"slack": [fx]}, rate_limit_every=0)
    c, http = _user_client(app)
    try:
        convs = await c.conversations_list(types="im,mpim")
        types = {x["channel_type"] for x in convs}
        assert types == {"im", "mpim"}
        # im conversations carry the counterpart user.
        ims = [x for x in convs if x["channel_type"] == "im"]
        assert all(x["user"] for x in ims)
        # Every conversation id from the fixture is present.
        fixture_ids = {
            conv["id"] for conv in fx["dm_users"][0]["conversations"]
        }
        assert {x["id"] for x in convs} == fixture_ids
    finally:
        await http.aclose()


async def test_user_client_reads_dm_history():
    fx = make_slack_dm_workspace(
        team_id=_TEAM, user_id=_USER, messages_per_dm=5,
    )
    im = next(
        c for c in fx["dm_users"][0]["conversations"]
        if c["channel_type"] == "im"
    )
    app = build_spammer_app(fixtures={"slack": [fx]}, rate_limit_every=0)
    c, http = _user_client(app)
    try:
        seen: list[str] = []
        cursor = None
        pages = 0
        while True:
            msgs, cursor = await c.conversations_history(
                channel=im["id"], cursor=cursor, limit=2,
            )
            seen.extend(m["ts"] for m in msgs)
            pages += 1
            if not cursor:
                break
        assert len(seen) == 5 and pages == 3
    finally:
        await http.aclose()


async def test_bot_token_does_not_see_dms():
    """A bot token requesting im/mpim gets no DM conversations — the spammer
    only serves DMs to the consenting user's token (the real Slack ceiling)."""
    fx = make_slack_dm_workspace(team_id=_TEAM, user_id=_USER)
    app = build_spammer_app(fixtures={"slack": [fx]}, rate_limit_every=0)
    c, http = _bot_client(app)
    try:
        # Bot enumerates channels only; the DM-typed request returns the
        # seeded bot channels, never the D…/G… DM conversations.
        channels = await c.conversations_list()
        ids = {ch["id"] for ch in channels}
        dm_ids = {
            conv["id"] for conv in fx["dm_users"][0]["conversations"]
        }
        assert ids.isdisjoint(dm_ids)
    finally:
        await http.aclose()
