"""Local Discord **Gateway** mock — a WebSocket server speaking the v10
opcode protocol, the live-ingestion counterpart to the REST spammer.

Discord live ingestion is a WSS Gateway, not REST: the real
`DiscordGatewayClient` (services/integrations/discord/gateway/client.py)
connects, awaits HELLO, sends IDENTIFY, awaits READY, then receives
op-0 DISPATCH frames (MESSAGE_CREATE) and exchanges heartbeats. This mock
implements the server half of exactly that handshake so the real client
can be pointed at it and driven on synthetic messages — the WSS analogue
of pointing the REST clients at `server.py`.

Protocol implemented (server → client unless noted):
  op 10 HELLO          — first frame; carries heartbeat_interval_ms
  op 2  IDENTIFY        — (client → server) → we reply with READY
  op 0  DISPATCH READY  — session_id + resume_gateway_url + application.id
  op 0  DISPATCH MESSAGE_CREATE — one per seeded message, increasing `s`
  op 1  HEARTBEAT       — (client → server) → we reply HEARTBEAT_ACK
  op 11 HEARTBEAT_ACK
  op 6  RESUME          — (client → server) → we re-push (replay)

Run standalone for a real-port load run via `serve()`; the test
(`tests/test_discord_gateway_loop.py`) starts it on an ephemeral port and
runs the real client against it in-process.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import websockets


# Opcodes (mirror the client's constants).
_OP_DISPATCH = 0
_OP_HEARTBEAT = 1
_OP_IDENTIFY = 2
_OP_RESUME = 6
_OP_HELLO = 10
_OP_HEARTBEAT_ACK = 11


class DiscordGatewayMock:
    """Server half of the Discord Gateway handshake over WSS.

    `messages` is a list of MESSAGE_CREATE `d` payloads (the Discord
    message objects); each is pushed as an op-0 DISPATCH with an
    increasing sequence number once the client is READY.
    """

    def __init__(
        self,
        *,
        messages: list[dict[str, Any]],
        heartbeat_interval_ms: int = 200,
        session_id: str = "spam-session",
        resume_gateway_url: str | None = None,
        application_id: str = "spam-app",
    ) -> None:
        self._messages = messages
        self._hb = heartbeat_interval_ms
        self._session_id = session_id
        self._resume_url = resume_gateway_url
        self._application_id = application_id

    async def handler(self, ws: Any) -> None:
        seq = 0
        # 1. HELLO immediately on connect.
        await ws.send(json.dumps(
            {"op": _OP_HELLO, "d": {"heartbeat_interval": self._hb}},
        ))

        async def push_messages() -> None:
            nonlocal seq
            for m in self._messages:
                seq += 1
                await ws.send(json.dumps({
                    "op": _OP_DISPATCH, "s": seq,
                    "t": "MESSAGE_CREATE", "d": m,
                }))

        try:
            async for raw in ws:
                frame = json.loads(raw)
                op = frame.get("op")
                if op == _OP_IDENTIFY:
                    seq += 1
                    await ws.send(json.dumps({
                        "op": _OP_DISPATCH, "s": seq, "t": "READY",
                        "d": {
                            "session_id": self._session_id,
                            "resume_gateway_url": self._resume_url,
                            "application": {"id": self._application_id},
                        },
                    }))
                    asyncio.create_task(push_messages())
                elif op == _OP_RESUME:
                    # Replay the seeded messages (idempotent downstream via
                    # content_hash / external_id dedup).
                    asyncio.create_task(push_messages())
                elif op == _OP_HEARTBEAT:
                    await ws.send(json.dumps({"op": _OP_HEARTBEAT_ACK}))
        except websockets.exceptions.ConnectionClosed:
            return

    def serve(self, host: str = "127.0.0.1", port: int = 0) -> Any:
        """Return the websockets.serve(...) awaitable/context-manager
        bound to this mock's handler. `port=0` picks an ephemeral port;
        read it back from `server.sockets[0].getsockname()[1]`."""
        return websockets.serve(self.handler, host, port)


__all__ = ["DiscordGatewayMock"]
