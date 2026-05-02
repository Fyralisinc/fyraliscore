"""services/demo/sse.py — Server-Sent Events bus for the action list.

DEMO-BUILD-PLAN Session 3:
  GET /v1/recommendations/stream → SSE stream of recommendation events
  for a single (tenant, actor). Whenever a recommendation Model is
  inserted, archived, or updated for that actor, an event is pushed to
  the open connection.

Event shape:
  data: {"event":"created","recommendation_id":"...","actor_id":"...",
         "summary":{...}}\n\n

The bus is in-process pub/sub via asyncio queues. For multi-instance
deployments, swap `_PROCESS_BUS` for a Redis-backed implementation;
the producer/subscriber API stays the same.

Producers (Think applier, recommendation handlers) call:
  await publish_recommendation_event(tenant_id, actor_id, event)

Subscribers (gateway SSE handler) iterate `subscribe(...)` to get a
stream of events for the (tenant, actor) pair.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID


SSE_HEARTBEAT_SECONDS = 15
SSE_MAX_QUEUE = 100


@dataclass
class _Subscriber:
    queue: asyncio.Queue[str | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=SSE_MAX_QUEUE)
    )


class _RecommendationBus:
    """In-process pub/sub for recommendation lifecycle events.

    Topics keyed on (tenant_id, actor_id). Multiple subscribers per
    topic supported (e.g., the same CEO has two browser tabs)."""

    def __init__(self) -> None:
        self._subs: dict[tuple[UUID, UUID], list[_Subscriber]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(
        self, tenant_id: UUID, actor_id: UUID
    ) -> _Subscriber:
        async with self._lock:
            sub = _Subscriber()
            self._subs.setdefault((tenant_id, actor_id), []).append(sub)
            return sub

    async def unsubscribe(
        self, tenant_id: UUID, actor_id: UUID, sub: _Subscriber
    ) -> None:
        async with self._lock:
            subs = self._subs.get((tenant_id, actor_id))
            if subs is None:
                return
            try:
                subs.remove(sub)
            except ValueError:
                pass
            if not subs:
                self._subs.pop((tenant_id, actor_id), None)
        # Wake the drainer so it can exit cleanly.
        try:
            sub.queue.put_nowait(None)
        except asyncio.QueueFull:
            pass

    async def publish(
        self, tenant_id: UUID, actor_id: UUID, payload: dict[str, Any]
    ) -> int:
        """Deliver `payload` to every subscriber for (tenant, actor).
        Returns the fan-out count. Drops payloads when a subscriber's
        queue is full (slow client; sender-side drop is the right call
        for streaming)."""
        body = json.dumps(payload, default=str)
        sent = 0
        async with self._lock:
            subs = list(self._subs.get((tenant_id, actor_id), ()))
        for sub in subs:
            try:
                sub.queue.put_nowait(body)
                sent += 1
            except asyncio.QueueFull:
                # Slow consumer — drop. Client will refetch on reconnect.
                pass
        return sent


_PROCESS_BUS = _RecommendationBus()


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


async def publish_recommendation_event(
    *,
    tenant_id: UUID,
    actor_id: UUID,
    event: str,
    recommendation_id: UUID,
    summary: dict[str, Any] | None = None,
) -> None:
    """Producer entry point. `event` is one of:
      - 'created'   (a new recommendation Model was inserted)
      - 'updated'   (confidence / impact / proposition changed)
      - 'archived'  (acted upon, dismissed, situation_resolved, etc.)
    """
    await _PROCESS_BUS.publish(
        tenant_id,
        actor_id,
        {
            "event": event,
            "recommendation_id": str(recommendation_id),
            "actor_id": str(actor_id),
            "tenant_id": str(tenant_id),
            "summary": summary or {},
            "ts": datetime.now(timezone.utc).isoformat(),
        },
    )


async def stream_for_actor(
    *,
    tenant_id: UUID,
    actor_id: UUID,
) -> AsyncIterator[bytes]:
    """SSE byte stream. Yields properly-formatted `data:` frames.
    Sends a heartbeat comment every SSE_HEARTBEAT_SECONDS so proxies
    don't time out the connection.

    The handler in services/demo/router.py wires this into a
    StreamingResponse(media_type="text/event-stream").
    """
    sub = await _PROCESS_BUS.subscribe(tenant_id, actor_id)

    # Initial ready frame so the client knows the connection is live.
    ready = json.dumps({
        "event": "ready",
        "actor_id": str(actor_id),
        "tenant_id": str(tenant_id),
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    yield f"event: ready\ndata: {ready}\n\n".encode("utf-8")

    try:
        while True:
            try:
                payload = await asyncio.wait_for(
                    sub.queue.get(), timeout=SSE_HEARTBEAT_SECONDS,
                )
            except asyncio.TimeoutError:
                yield b": heartbeat\n\n"
                continue
            if payload is None:
                # Sentinel — unsubscribe drained.
                break
            yield f"data: {payload}\n\n".encode("utf-8")
    finally:
        await _PROCESS_BUS.unsubscribe(tenant_id, actor_id, sub)


def get_bus_for_test() -> _RecommendationBus:
    """Test hook — lets unit tests replace the bus with a fresh one."""
    return _PROCESS_BUS


__all__ = [
    "publish_recommendation_event",
    "stream_for_actor",
    "SSE_HEARTBEAT_SECONDS",
    "get_bus_for_test",
]
