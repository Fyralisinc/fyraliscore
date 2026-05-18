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
        # M4.2 — when provided, the client seeds its in-memory state
        # from `initial_state` and (on the first reconnect attempt) tries
        # RESUME instead of fresh IDENTIFY. The worker layer is
        # responsible for deciding RESUME-vs-IDENTIFY based on the
        # persisted state's staleness (LLD §1.5, STALENESS_THRESHOLD).
        initial_state: "GatewaySessionState | None" = None,
        # M4.2 — fire-and-forget save hook. Called AFTER each
        # `dispatch_handler` returns durably for an op-0 DISPATCH frame
        # carrying a `seq` (s != None). See module docstring + LLD §1.5
        # for the save-after-handle ordering rationale.
        on_dispatched: "Callable[[GatewaySessionState], Awaitable[None]] | None" = None,
        # A6 — broker-ack durability barrier. When provided, the
        # dispatch loop calls `kafka_producer.flush(timeout_seconds=2)`
        # between the dispatch handler returning and the
        # `on_dispatched` save task being scheduled. This closes the
        # produce-to-broker-ack window where a SIGKILL would lose
        # frames from librdkafka's local queue while Postgres already
        # has the advanced `last_seq`. See
        # docs/ingestion/05-lld-amendments.md §A6 +
        # docs/decisions/a6-resolution.md. Optional — `None` (default)
        # is a no-op, used by tests that don't wire the shadow path.
        # Must be the same `IdempotentProducer` instance that the
        # dispatch handler's `shadow_write_raw` enqueues to;
        # otherwise the flush guarantees nothing about the frame.
        kafka_producer: Any = None,
    ) -> None:
        if not bot_token:
            raise ValueError("bot_token is required")
        self._bot_token = bot_token
        self._dispatch_handler = dispatch_handler
        if initial_state is not None:
            # Seed from the persisted snapshot. application_id from the
            # constructor wins only if the persisted state didn't carry
            # one (defensive — should never happen in production).
            self._state = initial_state
            if not self._state.application_id and application_id:
                self._state.application_id = application_id
        else:
            self._state = GatewaySessionState(application_id=application_id)
        self._ws_module = ws_module
        self._owns_http = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=15.0)
        self._clock = clock
        self._sleep = sleep
        self._ws: Any = None  # websockets.WebSocketClientProtocol
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._shutdown = asyncio.Event()
        self._on_dispatched = on_dispatched
        self._kafka_producer = kafka_producer

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
                # M4.2 — SAVE-AFTER-HANDLE call site. AFTER the
                # dispatch handler returned (success OR caught
                # exception above; both branches reach here), fire the
                # session-state save. Fire-and-forget: the save's
                # latency must NOT block frame ingestion (the next
                # frame's save will catch up the last_seq; losing one
                # save means re-processing one frame, safe under M2
                # content_hash dedup). Saving BEFORE the dispatch
                # would risk RESUMing past a frame that was never
                # handled — silent N1 breach. See session_state.py
                # module docstring "Save-after-handle ordering."
                #
                # A6 — BROKER-ACK DURABILITY BARRIER (Option 1: per-
                # frame flush). The dispatch handler enqueues the
                # frame to librdkafka's local queue via
                # `shadow_write_raw`. `Producer.produce()` returns on
                # local-enqueue, NOT broker-ack — so a SIGKILL between
                # produce-return and broker-ack would lose the frame
                # from the producer's in-memory queue while Postgres
                # already advanced `last_seq=N`. The next worker
                # RESUMEs past N, Discord doesn't replay frame N,
                # frame N is lost (silent N1 breach).
                #
                # Fix: flush the producer here so the save below only
                # persists `last_seq=N` after the broker has acked
                # frame N. On flush failure (timeout / broker
                # unreachable), DO NOT save — the next worker will
                # RESUME from the previously-saved seq and re-process
                # this frame, safe under M2 dedup. See
                # docs/ingestion/05-lld-amendments.md §A6 +
                # docs/decisions/a6-resolution.md.
                if self._on_dispatched is not None and frame.get("s") is not None:
                    try:
                        await self._pre_save_flush(timeout_seconds=2.0)
                    except Exception as exc:  # noqa: BLE001
                        metrics.inc(
                            "discord_gateway_pre_save_flush_failures_total",
                        )
                        log.warning(
                            "discord_gateway_pre_save_flush_failed",
                            seq=frame.get("s"),
                            error_type=type(exc).__name__,
                            error_message=str(exc)[:200],
                        )
                    else:
                        # Capture a snapshot — _state is mutated by
                        # the next frame's seq update, and a queued
                        # save task could otherwise persist a future
                        # seq value.
                        snapshot = GatewaySessionState(
                            session_id=self._state.session_id,
                            resume_gateway_url=self._state.resume_gateway_url,
                            last_seq=self._state.last_seq,
                            heartbeat_interval_ms=self._state.heartbeat_interval_ms,
                            last_heartbeat_ack=self._state.last_heartbeat_ack,
                            application_id=self._state.application_id,
                        )
                        asyncio.create_task(self._safe_save(snapshot))

    async def _safe_save(self, state: "GatewaySessionState") -> None:
        """Wrap the on_dispatched hook in exception logging so a save
        failure does not crash the gateway worker (per M4.2 PRIME
        DIRECTIVE)."""
        if self._on_dispatched is None:
            return
        try:
            await self._on_dispatched(state)
        except Exception:  # noqa: BLE001
            log.exception(
                "discord_gateway_session_save_failed",
                seq=state.last_seq,
            )

    async def _pre_save_flush(self, *, timeout_seconds: float) -> None:
        """Thin wrapper around the module-level :func:`pre_save_flush`,
        bound to this client's `kafka_producer`. The method form is
        retained so the dispatch-loop call site reads naturally; the
        actual flush logic is the free function so the M4.3
        cross-process test simulation (`tests/_subprocess_entrypoint.py`)
        can call the same code path. See A6 Phase 3 refactor note in
        the free function's docstring.
        """
        await pre_save_flush(
            self._kafka_producer, timeout_seconds=timeout_seconds,
        )


# ---------------------------------------------------------------------
# A6 — broker-ack durability barrier (free function).
# ---------------------------------------------------------------------
async def pre_save_flush(
    kafka_producer: Any,
    *,
    timeout_seconds: float,
) -> None:
    """Flush the Kafka producer so the broker has acked all in-flight
    shadow-write messages before the caller advances the persisted
    `last_seq`. Raises `TimeoutError` when the flush returns with
    messages still in queue (timeout exhausted); raises whatever the
    producer raises on broker errors. The caller decides what to do
    on failure (production client: skip save + increment failure
    metric; test simulation: let exception propagate to fail the test
    loudly).

    No-op when `kafka_producer` is None — used by tests that don't
    wire the shadow producer.

    See:
      - docs/ingestion/05-lld-amendments.md §A6 (the finding)
      - docs/decisions/a6-resolution.md (Phase 1 decision + Phase 3
        refactor note)
      - DiscordGatewayClient._dispatch_loop (production call site)
      - services/integrations/discord/gateway/tests/_subprocess_entrypoint.py
        (test simulation call site — uses the same function, no
        independent re-implementation)

    A6 Phase 3 refactor: this was originally a method on
    `DiscordGatewayClient`. It was extracted to a free function so
    the load-bearing `test_no_frames_lost_across_sigkill` test
    (which simulates the dispatch loop in a subprocess that does
    NOT instantiate `DiscordGatewayClient`) actually exercises the
    production code path. Previously the subprocess had a manual
    workaround flush; the workaround masked the fact that no test
    proved the production fix at the simulation surface.
    """
    if kafka_producer is None:
        return
    remaining = await kafka_producer.flush(timeout_seconds)
    if remaining > 0:
        raise TimeoutError(
            f"Kafka flush returned with {remaining} message(s) still "
            f"in queue after {timeout_seconds}s timeout"
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
    "pre_save_flush",
    "INTENTS",
    "_random_jitter",
]
