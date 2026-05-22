"""services/realtime/main.py — WebSocket sub-router + app wiring.

Exposes:
  * `realtime_router` — an APIRouter with `WS /stream`.
  * `RealtimeDeps` — process-wide dispatcher + pool bundle attached to
    `app.state.realtime`.
  * `configure_realtime(app, pool)` — convenience to wire the dispatcher
    into any FastAPI app at startup/shutdown.

The WS handshake performs Bearer-token auth against `actor_sessions`
using `services.gateway.auth.validate_token`. Tokens can be passed via
`Authorization: Bearer <tok>` header (Starlette WebSockets support
custom headers) OR via a `?token=<tok>` query param (for browser clients
that can't set headers on `new WebSocket()`).

On handshake failure: WS close code 1008 (policy violation) with a JSON
reason frame just before close.

Subscribe protocol:
    {"action": "subscribe", "topics": ["tenant:...", "goal:..."]}
    {"action": "unsubscribe", "topics": [...]}
    {"action": "replay", "since_sequence_num": 123}

Control frames the server may emit:
    {"kind": "ready", "subscription_id": "<uuid>",
     "actor_id": "<uuid>", "tenant_id": "<uuid>"}
    {"kind": "subscribed", "topics": [...]}
    {"kind": "unsubscribed", "topics": [...]}
    {"kind": "stream_lagged", "dropped": N}
    {"kind": "replay_complete", "pushed": N, "since_sequence_num": X}
    {"kind": "error", "message": "..."}
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg
from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect, status

from lib.shared.ids import uuid7
from services.realtime.dispatcher import Dispatcher, _ClientState


def _validate_token_lazy():
    """Lazy import to avoid the `services.gateway.__init__` → `main`
    module-load cycle. See BUILD-LOG Wave 4-D entry.
    """
    from services.gateway.auth import validate_token

    return validate_token


log = logging.getLogger(__name__)


# Close codes — aligned with Starlette + RFC 6455.
CLOSE_POLICY_VIOLATION = 1008
CLOSE_NORMAL = 1000


@dataclass
class RealtimeDeps:
    """Sub-app dependency bundle.

    Attached to `app.state.realtime` by `configure_realtime`.
    """

    pool: asyncpg.Pool
    dispatcher: Dispatcher


# ---------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------


realtime_router = APIRouter()


@realtime_router.websocket("/stream")
async def ws_stream(ws: WebSocket) -> None:
    """WS endpoint. Mounted on the Gateway app at `/stream`.

    The handshake runs BEFORE `ws.accept()` returns — we have to accept
    first (Starlette's WS API requires accept before reading the first
    message or sending close with a custom payload). We accept, then
    look at headers/query string, and if auth fails we close with 1008.
    """
    deps = _realtime_deps(ws)

    # Accept first; sending a close code pre-accept isn't supported on
    # all client libs. 1008 with a json rationale post-accept is the
    # common pattern.
    await ws.accept()

    # --- Auth: Bearer header OR ?token=... query param -------------
    token: str | None = None
    auth_hdr = ws.headers.get("authorization") or ws.headers.get("Authorization")
    if auth_hdr and auth_hdr.startswith("Bearer "):
        token = auth_hdr[len("Bearer "):].strip()
    if not token:
        token = ws.query_params.get("token")
    if not token:
        await _close_with(ws, CLOSE_POLICY_VIOLATION, "missing_token")
        return

    ctx = await _validate_token_lazy()(deps.pool, token)
    if ctx is None:
        await _close_with(ws, CLOSE_POLICY_VIOLATION, "invalid_or_expired_token")
        return

    # --- Register a client + drain task -----------------------------
    state = deps.dispatcher.register_client(
        tenant_id=ctx.tenant_id,
        actor_id=ctx.actor_id,
    )
    # Send a ready frame with the subscription_id so the client can
    # reference it in replay cursors.
    await _safe_send(
        ws,
        {
            "kind": "ready",
            "subscription_id": str(state.sub.subscription_id),
            "actor_id": str(ctx.actor_id),
            "tenant_id": str(ctx.tenant_id),
        },
    )

    # Drain task — loops reading from state.queue and writing to the WS.
    drain_task = asyncio.create_task(
        state.drain_to(
            _send_json_for(ws),
            control_frame_factory=_lag_frame,
        )
    )

    try:
        await _reader_loop(ws, state, deps)
    except WebSocketDisconnect:
        pass
    except Exception as e:  # pragma: no cover
        log.warning("realtime: WS reader error: %s", e)
    finally:
        state.closed = True
        # Unblock drain by pushing a sentinel-equivalent — closing the
        # queue via unregister.
        await deps.dispatcher.unregister_client(state.sub.subscription_id)
        drain_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await drain_task
        with contextlib.suppress(Exception):
            await ws.close()


# ---------------------------------------------------------------------
# Reader loop
# ---------------------------------------------------------------------


async def _reader_loop(
    ws: WebSocket,
    state: _ClientState,
    deps: RealtimeDeps,
) -> None:
    """Read subscribe/unsubscribe/replay messages from the client."""
    while True:
        msg = await ws.receive_text()
        try:
            parsed = json.loads(msg)
        except json.JSONDecodeError:
            await _safe_send(ws, {"kind": "error", "message": "invalid_json"})
            continue
        if not isinstance(parsed, dict):
            await _safe_send(ws, {"kind": "error", "message": "not_object"})
            continue
        action = parsed.get("action")
        if action == "subscribe":
            topics = _coerce_topics(parsed.get("topics"))
            if not topics:
                await _safe_send(ws, {"kind": "error", "message": "no_topics"})
                continue
            state.sub.topics.update(topics)
            await _safe_send(
                ws, {"kind": "subscribed", "topics": sorted(topics)}
            )
        elif action == "unsubscribe":
            topics = _coerce_topics(parsed.get("topics"))
            if topics:
                state.sub.topics.difference_update(topics)
            await _safe_send(
                ws, {"kind": "unsubscribed", "topics": sorted(topics)}
            )
        elif action == "replay":
            since = parsed.get("since_sequence_num")
            try:
                since_i = int(since)
            except (TypeError, ValueError):
                await _safe_send(
                    ws, {"kind": "error", "message": "bad_since_sequence_num"}
                )
                continue
            pushed = await deps.dispatcher.replay_since(
                state, since_sequence_num=since_i
            )
            # Best-effort cursor persist — used by cold-restart to
            # reconstruct a cursor if the client disconnects without
            # sending a new replay. Failure here is non-fatal.
            with contextlib.suppress(Exception):
                await _upsert_cursor(
                    deps.pool,
                    tenant_id=state.sub.tenant_id,
                    actor_id=state.sub.actor_id,
                    subscription_id=state.sub.subscription_id,
                    last_delivered_sequence_num=since_i + pushed,
                )
            await _safe_send(
                ws,
                {
                    "kind": "replay_complete",
                    "pushed": pushed,
                    "since_sequence_num": since_i,
                },
            )
        elif action == "ping":
            await _safe_send(ws, {"kind": "pong"})
        else:
            await _safe_send(
                ws, {"kind": "error", "message": f"unknown_action:{action}"}
            )


def _coerce_topics(raw: Any) -> set[str]:
    if not isinstance(raw, list):
        return set()
    out: set[str] = set()
    for t in raw:
        if isinstance(t, str) and ":" in t:
            out.add(t)
    return out


# ---------------------------------------------------------------------
# Control frames + send helpers
# ---------------------------------------------------------------------


def _send_json_for(ws: WebSocket) -> Callable[[dict[str, Any]], Awaitable[None]]:
    async def _send(obj: dict[str, Any]) -> None:
        await ws.send_text(json.dumps(obj, default=str))

    return _send


async def _safe_send(ws: WebSocket, payload: dict[str, Any]) -> None:
    try:
        await ws.send_text(json.dumps(payload, default=str))
    except Exception:
        # WS may be closed mid-send; caller handles cleanup.
        pass


def _lag_frame(dropped: int) -> dict[str, Any]:
    return {"kind": "stream_lagged", "dropped": dropped}


async def _close_with(ws: WebSocket, code: int, reason: str) -> None:
    with contextlib.suppress(Exception):
        await ws.send_text(json.dumps({"kind": "error", "message": reason}))
    with contextlib.suppress(Exception):
        await ws.close(code=code)


# ---------------------------------------------------------------------
# Cursor persistence (0012_realtime_replay_cursors)
# ---------------------------------------------------------------------


async def _upsert_cursor(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    subscription_id: UUID,
    last_delivered_sequence_num: int,
) -> None:
    await pool.execute(
        """
        INSERT INTO realtime_replay_cursors (
            tenant_id, actor_id, subscription_id,
            last_delivered_sequence_num, last_ack_at
        ) VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (tenant_id, actor_id, subscription_id) DO UPDATE
        SET last_delivered_sequence_num = EXCLUDED.last_delivered_sequence_num,
            last_ack_at = EXCLUDED.last_ack_at
        """,
        tenant_id,
        actor_id,
        subscription_id,
        int(last_delivered_sequence_num),
    )


# ---------------------------------------------------------------------
# App wiring
# ---------------------------------------------------------------------


def _realtime_deps(ws_or_request) -> RealtimeDeps:
    app = ws_or_request.app
    deps = getattr(app.state, "realtime", None)
    if deps is None:
        raise RuntimeError(
            "RealtimeDeps not configured on app.state.realtime — call "
            "services.realtime.configure_realtime(app, pool) at startup"
        )
    return deps


def configure_realtime(
    app: FastAPI,
    *,
    pool: asyncpg.Pool,
    dispatcher: Dispatcher | None = None,
    start: bool = True,
) -> RealtimeDeps:
    """Attach a Dispatcher to the FastAPI app. Tests pass
    ``start=False`` and call ``dispatcher.start()`` themselves.

    Returns the RealtimeDeps; also set on ``app.state.realtime``.
    """
    dispatcher = dispatcher or Dispatcher(pool)
    deps = RealtimeDeps(pool=pool, dispatcher=dispatcher)
    app.state.realtime = deps
    if start:
        # FastAPI >= 0.110 removed `add_event_handler`; `on_event` is
        # deprecated under filterwarnings=error. Start/stop is the
        # caller's responsibility when `start=True` — that path is only
        # used by explicit lifespan hooks in the calling app.
        raise ValueError(
            "start=True is unsupported in this FastAPI version; "
            "call dispatcher.start()/stop() from your app's lifespan"
        )
    # Mount the router if the caller hasn't already.
    if not any(
        getattr(r, "name", None) == "ws_stream" for r in app.router.routes
    ):
        app.include_router(realtime_router)
    return deps


__all__ = [
    "realtime_router",
    "ws_stream",
    "RealtimeDeps",
    "configure_realtime",
]
