"""End-to-end proof of the outbound-API shift for Discord (REST).

A REAL DiscordClient drives list_guilds / list_guild_channels /
get_messages against the local spammer over the real httpx + FastAPI
stack. Pointing it at the spammer is pure config (base_url /
DISCORD_API_BASE_URL). Proves snowflake `before` pagination and that a
spammer 429 is absorbed by DiscordClient's own Retry-After backoff.

(The live Discord Gateway is WebSocket, not REST — see the gateway WSS
mock + its test for that path. This covers the REST backfill surface.)
"""
from __future__ import annotations

from uuid import uuid4

import httpx

from services.synthetic.fixtures import make_discord_guild
from services.synthetic.spammer.server import build_spammer_app


_HOST = "http://spammer"
_GUILD = "900000000000000001"


def _client(app, **kwargs):
    from services.integrations.discord.client import DiscordClient

    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url=_HOST)
    c = DiscordClient(
        pool=None, secret_store=None, tenant_id=uuid4(),
        installation_row_id=uuid4(), guild_id=_GUILD,
        base_url=f"{_HOST}/discord/api/v10", http_client=http, **kwargs,
    )
    return c, http


async def test_real_discord_client_paginates(monkeypatch):
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "spam-bot-token")
    fx = make_discord_guild(guild_id=_GUILD, channels=1,
                            messages_per_channel=5)
    cid = fx["channels"][0]["id"]
    app = build_spammer_app(fixtures={"discord": [fx]}, rate_limit_every=0)
    c, http = _client(app)
    try:
        guilds = await c.list_guilds()
        assert [g["id"] for g in guilds] == [_GUILD]

        channels = await c.list_guild_channels(_GUILD)
        assert [ch["id"] for ch in channels] == [cid]

        # Page older via `before` at limit=2 → 2 + 2 + 1.
        seen: list[str] = []
        before = None
        pages = 0
        while True:
            msgs = await c.get_messages(channel_id=cid, before=before, limit=2)
            if not msgs:
                break
            seen.extend(m["id"] for m in msgs)
            before = msgs[-1]["id"]  # oldest in page (newest-first order)
            pages += 1
            if len(msgs) < 2:
                break
        assert len(set(seen)) == 5 and pages == 3
    finally:
        await http.aclose()


async def test_discord_429_absorbed_by_client_retry(monkeypatch):
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "spam-bot-token")
    fx = make_discord_guild(guild_id=_GUILD, channels=1,
                            messages_per_channel=3)
    cid = fx["channels"][0]["id"]
    # 429 on every 2nd data request; DiscordClient retries within budget.
    app = build_spammer_app(fixtures={"discord": [fx]}, rate_limit_every=2,
                            retry_after_s=0)
    c, http = _client(app)
    try:
        await c.get_messages(channel_id=cid, limit=100)  # #1 → 200
        msgs = await c.get_messages(channel_id=cid, limit=100)  # #2 429→retry
        assert len(msgs) == 3
    finally:
        await http.aclose()
