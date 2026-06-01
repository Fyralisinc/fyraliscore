"""End-to-end proof of the outbound-API shift for Slack.

A REAL SlackClient drives conversations_list / conversations_history
against the local spammer over the real httpx + FastAPI stack. Pointing
it at the spammer is pure config (base_url / SLACK_API_BASE_URL). Proves
cursor pagination and that a spammer 429 is absorbed by SlackClient's
own Retry-After backoff (the production rate-limit path).
"""
from __future__ import annotations

from uuid import uuid4

import httpx

from services.synthetic.fixtures import make_slack_workspace
from services.synthetic.spammer.server import build_spammer_app


_HOST = "http://spammer"
_TEAM = "T_SLACK"


def _client(app, **kwargs):
    from services.integrations.slack.client import SlackClient

    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url=_HOST)
    c = SlackClient(
        pool=None, secret_store=None, tenant_id=uuid4(),
        installation_row_id=uuid4(), team_id=_TEAM,
        base_url=f"{_HOST}/slack/api", http_client=http, **kwargs,
    )
    c._bot_token = f"spam-slack::{_TEAM}"  # bypass secret-store resolution
    return c, http


async def test_real_slack_client_lists_and_paginates(monkeypatch):
    fx = make_slack_workspace(team_id=_TEAM, channels=1,
                              messages_per_channel=5)
    cid = fx["channels"][0]["id"]
    app = build_spammer_app(fixtures={"slack": [fx]}, rate_limit_every=0)
    c, http = _client(app)
    try:
        channels = await c.conversations_list()
        assert [ch["id"] for ch in channels] == [cid]

        # Drain the channel at limit=2 → 3 pages (2 + 2 + 1).
        seen: list[str] = []
        cursor = None
        pages = 0
        while True:
            msgs, cursor = await c.conversations_history(
                channel=cid, cursor=cursor, limit=2)
            seen.extend(m["ts"] for m in msgs)
            pages += 1
            if not cursor:
                break
        assert len(seen) == 5 and pages == 3
    finally:
        await http.aclose()


async def test_slack_429_absorbed_by_client_retry(monkeypatch):
    fx = make_slack_workspace(team_id=_TEAM, channels=1,
                              messages_per_channel=3)
    cid = fx["channels"][0]["id"]
    # 429 on every 2nd data request; SlackClient retries within budget.
    app = build_spammer_app(fixtures={"slack": [fx]}, rate_limit_every=2,
                            retry_after_s=0)
    c, http = _client(app)
    try:
        # First call: 200. Second call hits a 429 mid-flight and the
        # client's Retry-After backoff retries it to success.
        await c.conversations_history(channel=cid, limit=10)
        msgs, _ = await c.conversations_history(channel=cid, limit=10)
        assert len(msgs) == 3
    finally:
        await http.aclose()
