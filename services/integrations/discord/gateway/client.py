"""services/integrations/discord/gateway/client.py — Discord Gateway WSS client.

Speaks Discord's documented gateway opcode protocol (v10, JSON encoding):

  op 0  DISPATCH         — server-pushed event payload
  op 1  HEARTBEAT        — bidirectional, we send every interval*0.7
  op 2  IDENTIFY         — we send once per session after HELLO
  op 6  RESUME           — we send on resumable reconnect
  op 7  RECONNECT        — server asks us to reconnect+resume
  op 9  INVALID_SESSION  — server invalidated our session
  op 10 HELLO            — server's first frame; carries heartbeat_interval
  op 11 HEARTBEAT_ACK    — server's reply to our heartbeat

Intent bitmask sent in IDENTIFY: GUILDS (1) | GUILD_MESSAGES (1<<9) |
MESSAGE_CONTENT (1<<15) = 33281. MESSAGE_CONTENT is privileged and MUST be
enabled in the Discord Developer Portal (Bot tab) before the worker can
deploy; otherwise Discord closes the connection with code 4014 and we
exit fatally (FR-005, FR-018; Clarifications: no degraded mode).

This module is the protocol layer only. The dispatch routing
(MESSAGE_CREATE → ingestion handler) lives in dispatch.py.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable

import httpx
import structlog
import websockets
from websockets.exceptions import ConnectionClosed

from services.integrations.discord.gateway import metrics


log = structlog.get_logger("integrations.discord.gateway.client")


_GATEWAY_BOT_URL = "https://discord.com/api/v10/gateway/bot"
_GATEWAY_VERSION = 10
_GATEWAY_ENCODING = "json"

# Intent bitmask per research R3:
#   GUILDS         (1<<0)  = 1      — receive GUILD_CREATE / GUILD_DELETE
#   GUILD_MESSAGES (1<<9)  = 512    — receive MESSAGE_CREATE in guild channels
#   MESSAGE_CONTENT (1<<15) = 32768 — populated `content` field (privileged)
INTENTS = (1 << 0) | (1 << 9) | (1 << 15)  # 33281

# Discord ops we send.
_OP_HEARTBEAT = 1
_OP_IDENTIFY = 2
_OP_RESUME = 6

# Discord ops we receive.
_OP_DISPATCH = 0
_OP_RECONNECT = 7
_OP_INVALID_SESSION = 9
_OP_HELLO = 10
_OP_HEARTBEAT_ACK = 11

# Close-code classifier per research R5.
_RESUMABLE_CLOSE_CODES = frozenset({1006, 4000, 4001, 4002, 4005, 4008})
_FULL_RECONNECT_CLOSE_CODES = frozenset({4003, 4007, 4009})
_FATAL_CLOSE_CODES = frozenset({4004, 4010, 4011, 4012, 4013, 4014})


class ReconnectAction(Enum):
    RESUME = "resume"
    IDENTIFY = "identify"
    FATAL_EXIT = "fatal_exit"


def classify_close_code(code: int | None) -> ReconnectAction:
    """Map a WSS close code to the action the worker should take next."""
    if code is None:
        return ReconnectAction.RESUME  # treat as 1006-equivalent
    if code in _FATAL_CLOSE_CODES:
        return ReconnectAction.FATAL_EXIT
    if code in _FULL_RECONNECT_CLOSE_CODES:
        return ReconnectAction.IDENTIFY
    if code in _RESUMABLE_CLOSE_CODES:
        return ReconnectAction.RESUME
    # Conservative default: try to resume; if the server rejects we'll
    # get INVALID_SESSION (d=false) and fall back to full reconnect.
    return ReconnectAction.RESUME


@dataclass
class GatewaySessionState:
    """In-memory session state. Reset on full reconnect; preserved across
    resumable reconnects."""
    session_id: str | None = None
    resume_gateway_url: str | None = None
    last_seq: int | None = None
    heartbeat_interval_ms: int = 0
    last_heartbeat_ack: float = field(default_factory=time.monotonic)
    application_id: str | None = None


class FatalGatewayError(Exception):
    """Raised when Discord closes the connection with a fatal code
    (4004, 4010..4014). The worker loop re-raises; the supervisor MUST
    NOT auto-restart (FR-005)."""

    def __init__(self, code: int, reason: str) -> None:
        super().__init__(f"fatal close {code}: {reason}")
        self.code = code
        self.reason = reason


DispatchHandler = Callable[[dict[str, Any]], Awaitable[None]]


class DiscordGatewayClient:
    """A single Discord Gateway WSS connection.

    Lifecycle:
        1. `run()` opens GET /gateway/bot → captures wss URL.
        2. Connects WSS, awaits HELLO, captures heartbeat_interval.
        3. Sends IDENTIFY (op 2), awaits READY DISPATCH.
        4. Spawns heartbeat task; enters dispatch loop.
        5. On resumable close → reopens to `resume_gateway_url`, sends RESUME.
        6. On fatal close → raises FatalGatewayError; caller exits 1.
    """

    def __init__(
        self,
        *,
        bot_token: str,
        dispatch_handler: DispatchHandler,
        application_id: str | None = None,
        ws_module: Any = websockets,
        http_client: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not bot_token:
            raise ValueError("bot_token is required")
        self._bot_token = bot_token
        self._dispatch_handler = dispatch_handler
        self._state = GatewaySessionState(application_id=application_id)
        self._ws_module = ws_module
        self._owns_http = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=15.0)
        self._clock = clock
        self._sleep = sleep
        self._ws: Any = None  # websockets.WebSocketClientProtocol
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._shutdown = asyncio.Event()

    async def aclose(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        if self._ws is not None:
            await self._ws.close(code=1000)
        if self._owns_http:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # Public surface
    # ------------------------------------------------------------------

    def request_shutdown(self) -> None:
        """Signal the run loop to exit cleanly on the next opportunity."""
        self._shutdown.set()

    async def run(self) -> None:
        """Outer run loop: connect → identify/resume → dispatch → reconnect.

        Returns normally when `request_shutdown()` is called and the
        dispatch loop exits. Raises `FatalGatewayError` when Discord
        closes with a code in _FATAL_CLOSE_CODES.
        """
        while not self._shutdown.is_set():
            try:
                if self._state.session_id and self._state.resume_gateway_url:
                    await self._connect_and_resume()
                else:
                    await self._connect_and_identify()
                await self._dispatch_loop()
            except FatalGatewayError:
                raise
            except ConnectionClosed as exc:
                action = classify_close_code(exc.rcvd.code if exc.rcvd else None)
                metrics.inc(
                    "discord_gateway_reconnect_total",
                    reason=f"close_{exc.rcvd.code if exc.rcvd else 'unknown'}",
                )
                if action is ReconnectAction.FATAL_EXIT:
                    raise FatalGatewayError(
                        code=exc.rcvd.code if exc.rcvd else -1,
                        reason=exc.rcvd.reason if exc.rcvd else "",
                    ) from exc
                if action is ReconnectAction.IDENTIFY:
                    self._state.session_id = None
                    self._state.resume_gateway_url = None
                # else RESUME — keep state, loop will reconnect via resume path.
            finally:
                if self._heartbeat_task is not None:
                    self._heartbeat_task.cancel()
                    self._heartbeat_task = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def _connect_and_identify(self) -> None:
        url = await self._fetch_gateway_url()
        await self._open_ws(url)
        hello = await self._await_op(_OP_HELLO)
        self._state.heartbeat_interval_ms = int(hello["d"]["heartbeat_interval"])
        self._start_heartbeat(initial_jitter=True)
        await self._send_identify()
        await self._await_ready()
        metrics.set_gauge("discord_gateway_connection_state", 1, state="connected")
        log.info("discord_gateway_ready", session_id=self._state.session_id)

    async def _connect_and_resume(self) -> None:
        url = self._state.resume_gateway_url or await self._fetch_gateway_url()
        await self._open_ws(url)
        hello = await self._await_op(_OP_HELLO)
        self._state.heartbeat_interval_ms = int(hello["d"]["heartbeat_interval"])
        self._start_heartbeat(initial_jitter=False)
        await self._send_resume()
        metrics.set_gauge("discord_gateway_connection_state", 1, state="resuming")
        log.info(
            "discord_gateway_resuming",
            session_id=self._state.session_id, seq=self._state.last_seq,
        )

    async def _fetch_gateway_url(self) -> str:
        r = await self._http.get(
            _GATEWAY_BOT_URL,
            headers={"Authorization": f"Bot {self._bot_token}"},
        )
        r.raise_for_status()
        url = r.json()["url"]
        return f"{url}?v={_GATEWAY_VERSION}&encoding={_GATEWAY_ENCODING}"

    async def _open_ws(self, url: str) -> None:
        self._ws = await self._ws_module.connect(url, max_size=2**20)

    # ------------------------------------------------------------------
    # Protocol frames
    # ------------------------------------------------------------------

    async def _send_identify(self) -> None:
        payload = {
            "op": _OP_IDENTIFY,
            "d": {
                "token": self._bot_token,
                "intents": INTENTS,
                "properties": {
                    "$os": "linux",
                    "$browser": "fyralis-gateway-worker",
                    "$device": "fyralis-gateway-worker",
                },
            },
        }
        await self._ws.send(json.dumps(payload))

    async def _send_resume(self) -> None:
        payload = {
            "op": _OP_RESUME,
            "d": {
                "token": self._bot_token,
                "session_id": self._state.session_id,
                "seq": self._state.last_seq,
            },
        }
        await self._ws.send(json.dumps(payload))

    async def _send_heartbeat(self) -> None:
        payload = {"op": _OP_HEARTBEAT, "d": self._state.last_seq}
        await self._ws.send(json.dumps(payload))

    async def _await_op(self, want_op: int) -> dict[str, Any]:
        while True:
            raw = await self._ws.recv()
            frame = json.loads(raw)
            if frame.get("op") == want_op:
                return frame
            # Stash seq for any DISPATCH that arrives before HELLO/READY
            # (shouldn't happen per Discord protocol, but be defensive).
            if frame.get("s") is not None:
                self._state.last_seq = frame["s"]

    async def _await_ready(self) -> None:
        while True:
            raw = await self._ws.recv()
            frame = json.loads(raw)
            if frame.get("s") is not None:
                self._state.last_seq = frame["s"]
            if frame.get("op") == _OP_DISPATCH and frame.get("t") == "READY":
                d = frame.get("d") or {}
                self._state.session_id = d.get("session_id")
                self._state.resume_gateway_url = d.get("resume_gateway_url")
                application = d.get("application") or {}
                if isinstance(application, dict):
                    self._state.application_id = (
                        application.get("id") or self._state.application_id
                    )
                return
            if frame.get("op") == _OP_INVALID_SESSION:
                raise ConnectionClosed(None, None)

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def _start_heartbeat(self, *, initial_jitter: bool) -> None:
        self._state.last_heartbeat_ack = self._clock()
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(initial_jitter=initial_jitter)
        )

    async def _heartbeat_loop(self, *, initial_jitter: bool) -> None:
        interval_s = self._state.heartbeat_interval_ms / 1000.0
        if initial_jitter:
            await self._sleep(interval_s * random.random())
        while True:
            try:
                await self._send_heartbeat()
            except Exception:  # noqa: BLE001
                return  # connection dead; outer loop will handle
            await self._sleep(interval_s * 0.7)
            # If two intervals passed without ACK, close the socket
            # (the outer loop will reconnect-and-resume).
            since_ack = self._clock() - self._state.last_heartbeat_ack
            if since_ack > interval_s * 1.5:
                metrics.inc("discord_gateway_heartbeat_miss_total")
                log.warning("discord_gateway_heartbeat_miss", since_ack=since_ack)
                if self._ws is not None:
                    try:
                        await self._ws.close(code=4000)
                    except Exception:  # noqa: BLE001
                        pass
                return

    # ------------------------------------------------------------------
    # Dispatch loop
    # ------------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        while not self._shutdown.is_set():
            raw = await self._ws.recv()
            frame = json.loads(raw)
            op = frame.get("op")
            if frame.get("s") is not None:
                self._state.last_seq = frame["s"]

            if op == _OP_HEARTBEAT_ACK:
                self._state.last_heartbeat_ack = self._clock()
                continue
            if op == _OP_HEARTBEAT:
                # Discord asks us to send one immediately.
                await self._send_heartbeat()
                continue
            if op == _OP_RECONNECT:
                # Discord asks us to reconnect+resume.
                metrics.inc("discord_gateway_reconnect_total", reason="op_7_reconnect")
                if self._ws is not None:
                    await self._ws.close(code=4000)
                return
            if op == _OP_INVALID_SESSION:
                d_can_resume = bool(frame.get("d") is True)
                metrics.inc(
                    "discord_gateway_reconnect_total",
                    reason=f"invalid_session_d_{d_can_resume}",
                )
                if not d_can_resume:
                    self._state.session_id = None
                    self._state.resume_gateway_url = None
                if self._ws is not None:
                    await self._ws.close(code=4000)
                return
            if op == _OP_DISPATCH:
                event_name = frame.get("t") or ""
                metrics.inc("discord_gateway_dispatch_total", event=event_name)
                # Re-bind READY application_id if it shifted.
                if event_name == "READY":
                    d = frame.get("d") or {}
                    app = d.get("application") or {}
                    if isinstance(app, dict) and app.get("id"):
                        self._state.application_id = app["id"]
                # Hand to the dispatch module; it owns the business logic.
                try:
                    await self._dispatch_handler(frame)
                except Exception:  # noqa: BLE001
                    log.exception(
                        "discord_gateway_dispatch_handler_error",
                        event=event_name,
                    )


def _random_jitter(scale: float) -> float:
    """Public for tests."""
    return scale * (0.75 + 0.5 * random.random())


__all__ = [
    "DiscordGatewayClient",
    "GatewaySessionState",
    "ReconnectAction",
    "FatalGatewayError",
    "classify_close_code",
    "INTENTS",
    "_random_jitter",
]
