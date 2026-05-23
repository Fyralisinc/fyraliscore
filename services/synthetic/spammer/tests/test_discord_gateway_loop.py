"""End-to-end proof of the LIVE Discord path against the WSS gateway mock.

The REAL DiscordGatewayClient resolves the gateway URL via the REST
spammer (`/gateway/bot` → ws://…), opens a real WebSocket to the local
`DiscordGatewayMock`, completes the HELLO → IDENTIFY → READY handshake,
exchanges heartbeats, and receives op-0 MESSAGE_CREATE DISPATCH frames —
all the real protocol code, driven on synthetic messages. Pointing the
client at the mock is pure config (DISCORD_GATEWAY_BOT_URL +
SPAMMER_DISCORD_WSS_URL).
"""
from __future__ import annotations

import asyncio

import httpx

from services.synthetic.fixtures import make_discord_guild
from services.synthetic.spammer.discord_gateway import DiscordGatewayMock
from services.synthetic.spammer.server import build_spammer_app


async def test_real_gateway_client_receives_dispatched_messages(monkeypatch):
    fx = make_discord_guild(guild_id="G1", channels=1, messages_per_channel=4)
    msgs = fx["channels"][0]["messages"]

    mock = DiscordGatewayMock(messages=msgs, heartbeat_interval_ms=200)
    server = await mock.serve(port=0)
    port = server.sockets[0].getsockname()[1]

    monkeypatch.setenv("SPAMMER_DISCORD_WSS_URL", f"ws://127.0.0.1:{port}")
    monkeypatch.setenv(
        "DISCORD_GATEWAY_BOT_URL",
        "http://disc/discord/api/v10/gateway/bot",
    )

    app = build_spammer_app(rate_limit_every=0)
    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url="http://disc")

    from services.integrations.discord.gateway.client import (
        DiscordGatewayClient,
    )

    received: list[str] = []

    async def handler(frame) -> None:
        if frame.get("t") == "MESSAGE_CREATE":
            received.append(frame["d"]["id"])

    client = DiscordGatewayClient(
        bot_token="spam-bot-token", dispatch_handler=handler,
        http_client=http,
    )
    task = asyncio.create_task(client.run())
    try:
        for _ in range(60):
            if len(received) >= len(msgs):
                break
            await asyncio.sleep(0.05)
        assert set(received) == {m["id"] for m in msgs}
    finally:
        client.request_shutdown()
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        await client.aclose()
        server.close()
        await server.wait_closed()
        await http.aclose()
