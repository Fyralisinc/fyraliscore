"""services/greeting/stream.py — Phase 5.

`ViewCeoStreamManager` — in-process fan-out of cache-write events to
WebSocket subscribers of `/view/ceo/stream`. Message shapes per
CONTRACTS §1.4.

Wire contract:
  server → client:
    { "type": "hello", "tenant_id": "<uuid>" }
    { "type": "greeting_updated", "greeting": {...} }
    { "type": "cards_updated", "cards": [...] }
    { "type": "query_grid_updated", "query_grid": {...} }
    { "type": "status_updated", "status": {...} }
    { "type": "heartbeat", "at": "<iso>" }

  client → server:
    { "action": "ping" }  # optional — server responds with heartbeat

Auth: simple static token (query param `?token=<tok>` OR
`Authorization: Bearer <tok>`). Real auth is deferred; the token maps
to a tenant_id via an env-configurable registry so dogfood can wire a
single tenant without a full gateway-style auth path.

Tenant isolation: each connection is scoped to exactly one tenant_id
at handshake; the manager only delivers messages for that tenant.

Heartbeats every 30s; server tolerates silent clients.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable
from uuid import UUID, uuid4

from fastapi import APIRouter, WebSocket, WebSocketDisconnect


log = logging.getLogger(__name__)


HEARTBEAT_INTERVAL_S = 30.0
CLIENT_QUEUE_MAX = 200
CLOSE_POLICY_VIOLATION = 1008


# =====================================================================
# Token → tenant resolver (pluggable)
# =====================================================================


@dataclass
class StaticTenantTokenMap:
    """Maps static tokens to tenant_ids. Source: constructor arg or
    env var `VIEW_CEO_STATIC_TOKENS=tok1:<uuid>,tok2:<uuid>`.

    Intended only for dogfood. Real auth replaces this in Wave 5-adj.
    """

    tokens: dict[str, UUID] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "StaticTenantTokenMap":
        raw = os.environ.get("VIEW_CEO_STATIC_TOKENS", "").strip()
        out: dict[str, UUID] = {}
        if raw:
            for item in raw.split(","):
                item = item.strip()
                if not item or ":" not in item:
                    continue
                tok, uid = item.split(":", 1)
                try:
                    out[tok.strip()] = UUID(uid.strip())
                except (ValueError, TypeError):
                    continue
        return cls(tokens=out)

    def resolve(self, token: str) -> UUID | None:
        return self.tokens.get(token)


# =====================================================================
# Per-client state
# =====================================================================


@dataclass
class _ClientState:
    id: UUID
    tenant_id: UUID
    queue: asyncio.Queue[dict[str, Any]]
    closed: bool = False


# =====================================================================
# Manager
# =====================================================================


class ViewCeoStreamManager:
    """Process-local fan-out. `publish(tenant_id, message)` routes the
    message to every subscriber whose tenant matches.

    Scheduler wires itself to `self.publish` so cache writes produce
    client-visible updates.
    """

    def __init__(
        self,
        token_map: StaticTenantTokenMap | None = None,
    ):
        self._clients: dict[UUID, _ClientState] = {}
        self._lock = asyncio.Lock()
        self._token_map = token_map or StaticTenantTokenMap.from_env()

    # -----------------------------------------------------------------
    # Handshake / register
    # -----------------------------------------------------------------
    async def register(self, tenant_id: UUID) -> _ClientState:
        state = _ClientState(
            id=uuid4(),
            tenant_id=tenant_id,
            queue=asyncio.Queue(maxsize=CLIENT_QUEUE_MAX),
        )
        async with self._lock:
            self._clients[state.id] = state
        return state

    async def unregister(self, client_id: UUID) -> None:
        async with self._lock:
            state = self._clients.pop(client_id, None)
        if state is not None:
            state.closed = True
            # Wake any pending drain.
            with contextlib.suppress(asyncio.QueueFull, Exception):
                state.queue.put_nowait({"type": "__closed__"})

    def resolve_token(self, token: str) -> UUID | None:
        return self._token_map.resolve(token)

    # -----------------------------------------------------------------
    # Publish API (scheduler-side)
    # -----------------------------------------------------------------
    async def publish(
        self,
        tenant_id: UUID,
        message: dict[str, Any],
    ) -> int:
        """Fan-out to all subscribers for `tenant_id`. Returns the count
        of queues the message was placed on. When a queue is full we
        drop the oldest item and log — consistent with the realtime
        dispatcher's oldest-drop policy."""
        async with self._lock:
            targets = [
                s for s in self._clients.values()
                if s.tenant_id == tenant_id and not s.closed
            ]
        delivered = 0
        for state in targets:
            try:
                state.queue.put_nowait(message)
                delivered += 1
            except asyncio.QueueFull:
                # Drop oldest, push again.
                try:
                    _ = state.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    state.queue.put_nowait(message)
                    delivered += 1
                    log.warning(
                        "ceo_stream.lagged",
                        extra={
                            "tenant_id": str(tenant_id),
                            "client_id": str(state.id),
                        },
                    )
                except Exception:
                    pass
        return delivered

    # -----------------------------------------------------------------
    # Observability
    # -----------------------------------------------------------------
    def connected_clients(self, tenant_id: UUID | None = None) -> int:
        if tenant_id is None:
            return len(self._clients)
        return sum(
            1 for s in self._clients.values() if s.tenant_id == tenant_id
        )


# =====================================================================
# FastAPI router
# =====================================================================


def build_ceo_stream_router(manager: ViewCeoStreamManager) -> APIRouter:
    """Router that exposes WS `/view/ceo/stream`. Caller mounts on
    their FastAPI app."""
    router = APIRouter()

    @router.websocket("/view/ceo/stream")
    async def ws_ceo_stream(ws: WebSocket) -> None:
        await ws.accept()

        # --- Auth: Bearer header OR ?token= query --------------------
        token: str | None = None
        auth_hdr = ws.headers.get("authorization") or ws.headers.get("Authorization")
        if auth_hdr and auth_hdr.lower().startswith("bearer "):
            token = auth_hdr[len("Bearer "):].strip()
        if not token:
            token = ws.query_params.get("token")
        if not token:
            await _close_with(ws, CLOSE_POLICY_VIOLATION, "missing_token")
            return

        tenant_id = manager.resolve_token(token)
        if tenant_id is None:
            # Fall back to actor_sessions (demo / real auth tokens).
            try:
                from services.gateway.auth import validate_token
                deps = getattr(ws.app.state, "deps", None)
                pool = deps.pool if deps else None
                if pool:
                    ctx = await validate_token(pool, token)
                    if ctx is not None:
                        tenant_id = ctx.tenant_id
            except Exception:
                pass
        if tenant_id is None:
            await _close_with(ws, CLOSE_POLICY_VIOLATION, "invalid_token")
            return

        state = await manager.register(tenant_id)

        # Send initial hello so the client can correlate.
        await _safe_send(
            ws,
            {
                "type": "hello",
                "tenant_id": str(tenant_id),
                "client_id": str(state.id),
            },
        )

        drain_task = asyncio.create_task(_drain_to_ws(ws, state))
        hb_task = asyncio.create_task(_heartbeat_loop(ws, state))

        try:
            await _reader_loop(ws, state)
        except WebSocketDisconnect:
            pass
        except Exception as e:  # pragma: no cover
            log.warning("ceo_stream.reader_error", extra={"error": str(e)})
        finally:
            state.closed = True
            drain_task.cancel()
            hb_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await drain_task
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await hb_task
            await manager.unregister(state.id)
            with contextlib.suppress(Exception):
                await ws.close()

    return router


# ---------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------


async def _drain_to_ws(ws: WebSocket, state: _ClientState) -> None:
    while not state.closed:
        try:
            msg = await state.queue.get()
        except asyncio.CancelledError:
            raise
        if msg.get("type") == "__closed__":
            return
        try:
            await ws.send_text(json.dumps(msg, default=_default_json))
        except Exception:
            return


async def _heartbeat_loop(ws: WebSocket, state: _ClientState) -> None:
    try:
        while not state.closed:
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)
            if state.closed:
                return
            await _safe_send(
                ws,
                {
                    "type": "heartbeat",
                    "at": datetime.now(timezone.utc).isoformat(),
                },
            )
    except asyncio.CancelledError:
        raise


async def _reader_loop(ws: WebSocket, state: _ClientState) -> None:
    """The reader primarily keeps the WS alive; messages are
    client-initiated pings. We accept and ignore anything else."""
    while not state.closed:
        try:
            raw = await ws.receive_text()
        except WebSocketDisconnect:
            return
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            await _safe_send(ws, {"type": "error", "message": "invalid_json"})
            continue
        if isinstance(parsed, dict) and parsed.get("action") == "ping":
            await _safe_send(
                ws,
                {
                    "type": "heartbeat",
                    "at": datetime.now(timezone.utc).isoformat(),
                },
            )


async def _safe_send(ws: WebSocket, payload: dict[str, Any]) -> None:
    try:
        await ws.send_text(json.dumps(payload, default=_default_json))
    except Exception:
        pass


async def _close_with(ws: WebSocket, code: int, reason: str) -> None:
    with contextlib.suppress(Exception):
        await ws.send_text(
            json.dumps({"type": "error", "message": reason})
        )
    with contextlib.suppress(Exception):
        await ws.close(code=code)


def _default_json(v: Any) -> Any:
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).isoformat()
    if hasattr(v, "isoformat"):
        return v.isoformat()
    raise TypeError(f"unserialisable {type(v).__name__}")


__all__ = [
    "ViewCeoStreamManager",
    "StaticTenantTokenMap",
    "build_ceo_stream_router",
    "HEARTBEAT_INTERVAL_S",
]
