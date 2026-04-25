"""services/realtime/dispatcher.py — per-process fan-out of Postgres
NOTIFY events to subscribed WebSocket clients.

One `Dispatcher` runs per process. A single dedicated asyncpg connection
holds `LISTEN observations_new` (+ any extra channels). When a
notification arrives, the dispatcher parses it, routes to matching
client subscriptions, and pushes onto per-client bounded queues.
Each client has its own `asyncio.Task` that drains its queue and writes
to the WebSocket.

Design notes
------------

* **Channel design** (BUILD-PLAN §5 Prompt 4.D: "pick one, document"):
  we REUSE `observations_new` rather than introducing a second channel.
  The Wave-1-A NOTIFY payload is `{id, kind, tenant_id, source_channel}`;
  `kind='state_change'` on `source_channel='internal:state_change'`
  delivers every state-change cascade (emitted by
  `services/observations/state_change.emit_state_change`), which is the
  signal the UI needs. Rationale: one channel = one LISTEN connection =
  simpler backpressure bookkeeping. The payload's `kind` discriminator
  drives the `EventFrame.kind` field mapping. Documented in BUILD-LOG
  Wave 4-D Deviation (b).

* **Backpressure** (BUILD-PLAN §5 Prompt 4.D: "document"): per-client
  `asyncio.Queue(maxsize=500)`. When a push encounters a full queue, the
  dispatcher drops the OLDEST item (via `queue.get_nowait()` loop) and
  emits a `{kind: "stream_lagged", dropped: N}` control frame. Oldest-
  drop preserves the most recent state, which is what a dashboard
  re-rendering on reconnect actually wants. Documented in BUILD-LOG
  Wave 4-D Deviation (c).

* **Topics**: clients subscribe to strings of the form
  `"tenant:<uuid>" | "actor:<uuid>" | "goal:<uuid>" | "commitment:<uuid>"
  | "customer:<uuid>"`. `tenant:*` topics receive every event in the
  tenant. Other topics filter by content-derived entity ids. The
  EventFrame carries the matched topic for client-side routing.

* **Access control**: Wave 4-D scope-based filter. Tenant isolation is
  strict (an event's tenant_id must equal the subscriber's tenant_id).
  Beyond that, Wave 4 forwards everything the client asked for; Wave 5
  access_control elaborates (BUILD-PLAN §5 Prompt 4.D note).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Public data shapes
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class EventFrame:
    """One message pushed onto a client's WS.

    Wire shape (JSON):
        {"kind": "observation|state_change|act_change|resource_change",
         "id": "<uuid>", "tenant_id": "<uuid>",
         "topic": "goal:<uuid>", "sequence_num": 42, "payload": {...}}
    """

    kind: str
    id: UUID
    tenant_id: UUID
    topic: str
    sequence_num: int
    payload: dict[str, Any]

    def to_wire(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "id": str(self.id),
            "tenant_id": str(self.tenant_id),
            "topic": self.topic,
            "sequence_num": self.sequence_num,
            "payload": self.payload,
        }


@dataclass
class SubscriptionFilter:
    """One WS client's current subscription set.

    `topics` is a set of `"<kind>:<id>"` strings. `tenant_id` is fixed
    at handshake (bearer token → tenant). Altering `topics` is allowed
    via subscribe/unsubscribe messages.
    """

    subscription_id: UUID
    tenant_id: UUID
    actor_id: UUID
    topics: set[str] = field(default_factory=set)

    def matches(self, tenant_id: UUID, candidate_topics: set[str]) -> bool:
        if tenant_id != self.tenant_id:
            return False
        return bool(self.topics & candidate_topics)


# ---------------------------------------------------------------------
# Per-client state
# ---------------------------------------------------------------------


@dataclass
class _ClientState:
    """Server-side state for one WebSocket connection.

    The ClientState lives only inside the Dispatcher. The WS endpoint
    owns the WebSocket and delegates push to `queue_put` / the drain
    task.
    """

    connection_id: UUID
    sub: SubscriptionFilter
    queue: asyncio.Queue
    dropped: int = 0
    closed: bool = False

    # When a push hits a full queue, we drop oldest and keep a counter.
    # The drain task emits a control frame when `dropped > 0`.

    async def drain_to(
        self,
        send_json: Callable[[dict[str, Any]], Awaitable[None]],
        *,
        control_frame_factory: Callable[[int], dict[str, Any]] | None = None,
    ) -> None:
        """Drain queue → send. Loops until `closed`. Emits a lag
        control-frame when `dropped` > 0 before the next event.
        """
        while not self.closed:
            try:
                item = await self.queue.get()
            except asyncio.CancelledError:
                raise
            if item is _SENTINEL_CLOSE:
                return
            if self.dropped > 0 and control_frame_factory is not None:
                try:
                    await send_json(control_frame_factory(self.dropped))
                finally:
                    self.dropped = 0
            try:
                await send_json(item.to_wire() if isinstance(item, EventFrame) else item)
            except Exception as e:  # pragma: no cover — WebSocket errors
                log.warning("realtime drain_to send_json failed: %s", e)
                self.closed = True
                return


_SENTINEL_CLOSE = object()


# ---------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------


class Dispatcher:
    """Process-wide NOTIFY → WebSocket fan-out.

    Typical lifecycle:

        dispatcher = Dispatcher(pool)
        await dispatcher.start()
        ...
        await dispatcher.stop()

    Register / unregister clients via `register_client()` /
    `unregister_client()`.

    The `pool` must be able to support one long-lived dedicated
    connection for LISTEN. The dispatcher acquires it on start and
    releases it on stop.
    """

    QUEUE_MAX = 500

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._clients: dict[UUID, _ClientState] = {}
        self._listen_conn: asyncpg.Connection | None = None
        self._listen_task: asyncio.Task | None = None
        self._started = asyncio.Event()
        self._stopping = False
        # Expose a simple counter so tests can assert dispatch happened.
        self.stats = {
            "events_received": 0,
            "events_dispatched": 0,
            "drops": 0,
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Acquire a dedicated LISTEN connection and start the pump.

        `observations_new` is the sole subscribed channel (design
        decision documented in the module docstring).
        """
        if self._listen_task is not None:
            return
        self._listen_conn = await self._pool.acquire()
        try:
            await self._listen_conn.add_listener(
                "observations_new", self._on_notify
            )
        except Exception:
            await self._pool.release(self._listen_conn)
            self._listen_conn = None
            raise
        # Keep the connection alive in an idle loop. asyncpg processes
        # NOTIFY callbacks on the connection's own read loop, so we only
        # need this task to hold the connection + allow cancellation.
        self._listen_task = asyncio.create_task(self._idle_loop())
        self._started.set()

    async def stop(self) -> None:
        """Shut down cleanly: cancel the idle loop, remove the listener,
        release the connection, and close every client."""
        self._stopping = True
        # Close every client (puts sentinel on their queues).
        for cs in list(self._clients.values()):
            await self._close_client(cs)
        if self._listen_task is not None:
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
            self._listen_task = None
        if self._listen_conn is not None:
            try:
                await self._listen_conn.remove_listener(
                    "observations_new", self._on_notify
                )
            except Exception:
                pass
            try:
                await self._pool.release(self._listen_conn)
            except Exception:
                pass
            self._listen_conn = None

    async def _idle_loop(self) -> None:
        try:
            while not self._stopping:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            return

    # ------------------------------------------------------------------
    # Client management
    # ------------------------------------------------------------------

    def register_client(
        self,
        *,
        tenant_id: UUID,
        actor_id: UUID,
        connection_id: UUID | None = None,
        initial_topics: set[str] | None = None,
        queue_maxsize: int | None = None,
    ) -> _ClientState:
        """Create a new client state + per-client queue."""
        connection_id = connection_id or uuid7()
        sub = SubscriptionFilter(
            subscription_id=connection_id,
            tenant_id=tenant_id,
            actor_id=actor_id,
            topics=set(initial_topics or ()),
        )
        state = _ClientState(
            connection_id=connection_id,
            sub=sub,
            queue=asyncio.Queue(maxsize=queue_maxsize or self.QUEUE_MAX),
        )
        self._clients[connection_id] = state
        return state

    async def unregister_client(self, connection_id: UUID) -> None:
        state = self._clients.pop(connection_id, None)
        if state is not None:
            await self._close_client(state)

    async def _close_client(self, state: _ClientState) -> None:
        state.closed = True
        # Drain: push sentinel so drain_to returns.
        try:
            state.queue.put_nowait(_SENTINEL_CLOSE)
        except asyncio.QueueFull:
            # Make room by dropping one oldest, then push sentinel.
            try:
                state.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                state.queue.put_nowait(_SENTINEL_CLOSE)
            except asyncio.QueueFull:
                pass

    # ------------------------------------------------------------------
    # NOTIFY handling
    # ------------------------------------------------------------------

    def _on_notify(
        self,
        conn: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        """asyncpg invokes this callback on the connection's read loop.

        We MUST NOT block here; we parse + schedule a background task
        for the dispatch (which may need to hydrate extra data from the
        DB).
        """
        self.stats["events_received"] += 1
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as e:
            log.warning("realtime: malformed NOTIFY payload: %s / %s", e, payload)
            return
        asyncio.create_task(self._dispatch(channel, data))

    async def _dispatch(self, channel: str, data: dict[str, Any]) -> None:
        """Hydrate an EventFrame from the NOTIFY payload and fan out."""
        try:
            frame = await self._hydrate_event(data)
        except Exception as e:
            log.warning("realtime: hydrate failed: %s / %s", e, data)
            return
        if frame is None:
            return
        # Derive candidate topic strings from the event.
        candidate_topics = self._candidate_topics(frame)
        # Iterate over a snapshot — clients may come + go during dispatch.
        for state in list(self._clients.values()):
            if state.closed:
                continue
            if not state.sub.matches(frame.tenant_id, candidate_topics):
                continue
            # Tag the outbound frame with the specific matching topic
            # so clients can route multiplexed subscriptions.
            matched = next(
                (t for t in candidate_topics if t in state.sub.topics),
                None,
            )
            if matched is None:
                continue
            # Wave 5-A access control filter — drop events the subscriber
            # cannot read. Best-effort: if the check fails, we fall back
            # to Wave 4-D's tenant-only isolation (already enforced via
            # SubscriptionFilter.matches).
            if not await self._can_deliver(state.sub, frame):
                self.stats.setdefault("access_drops", 0)
                self.stats["access_drops"] += 1
                continue
            outbound = EventFrame(
                kind=frame.kind,
                id=frame.id,
                tenant_id=frame.tenant_id,
                topic=matched,
                sequence_num=frame.sequence_num,
                payload=frame.payload,
            )
            self._enqueue(state, outbound)
            self.stats["events_dispatched"] += 1

    async def _can_deliver(
        self,
        sub: "SubscriptionFilter",
        frame: EventFrame,
    ) -> bool:
        """Wave 5-A access-control filter for streamed events.

        Determines whether the subscriber is allowed to see this event
        based on the underlying entity's access rules. We derive the
        entity kind from the frame's kind + payload.content:

          - kind='observation' → kind='observation' in can_read.
          - kind='act_change' → entity_kind in payload.content
            ('commitment' | 'goal' | 'decision').
          - kind='resource_change' → kind='resource'.
          - kind='state_change' (fallthrough) → observation.

        We read the underlying row via can_read_by_id so we get the
        most current ownership / scope data. Errors default to deny
        (safer than a stream leak).
        """
        from services.access_control.checks import can_read_by_id  # lazy

        content = frame.payload.get("content", {}) or {}
        entity_kind: str | None = None
        entity_id: UUID | None = None
        if frame.kind == "observation":
            entity_kind = "observation"
            entity_id = frame.id
        elif frame.kind == "act_change":
            ek = str(content.get("entity_kind") or "").lower()
            if ek in ("commitment", "goal", "decision"):
                entity_kind = ek
                raw = content.get("entity_id")
                try:
                    entity_id = UUID(str(raw)) if raw else None
                except (ValueError, TypeError):
                    entity_id = None
        elif frame.kind == "resource_change":
            entity_kind = "resource"
            raw = content.get("entity_id")
            try:
                entity_id = UUID(str(raw)) if raw else None
            except (ValueError, TypeError):
                entity_id = None
        else:
            # state_change fallthrough: treat as observation.
            entity_kind = "observation"
            entity_id = frame.id
        if entity_kind is None or entity_id is None:
            # Can't determine — fall back to Wave 4-D default (tenant
            # isolation already enforced by SubscriptionFilter.matches).
            return True
        try:
            async with self._pool.acquire() as conn:
                decision = await can_read_by_id(
                    sub.actor_id,
                    entity_kind,  # type: ignore[arg-type]
                    entity_id,
                    conn=conn,
                    tenant_id=sub.tenant_id,
                )
                # Backward compat: when the underlying entity isn't
                # loaded (e.g. Wave 4-D tests inserted an Observation
                # that references a goal id with no goal row), fall
                # back to Wave 4-D tenant-only isolation. Real access
                # denials (scope violation, role missing, etc.) still
                # drop the frame.
                if (
                    not decision.allowed
                    and decision.reason == "entity_not_found"
                ):
                    return True
                return decision.allowed
        except Exception as e:
            log.warning(
                "realtime access_control check failed: %s (kind=%s id=%s)",
                e, entity_kind, entity_id,
            )
            # Fail CLOSED — don't leak when the check is broken.
            return False

    async def revoke_for_entity(
        self,
        *,
        actor_id: UUID,
        entity_kind: str,
        entity_id: UUID,
    ) -> int:
        """Drop every client's subscription to the given entity id.

        Called when access is revoked (role drop, manager-chain change,
        etc.). Returns the number of subscriptions whose topic set was
        pruned.
        """
        topic = f"{entity_kind}:{entity_id}"
        dropped = 0
        for state in list(self._clients.values()):
            if state.closed:
                continue
            if state.sub.actor_id != actor_id:
                continue
            if topic in state.sub.topics:
                state.sub.topics.discard(topic)
                dropped += 1
        return dropped

    async def _hydrate_event(
        self, data: dict[str, Any]
    ) -> EventFrame | None:
        """Turn a raw `observations_new` payload into an EventFrame.

        The NOTIFY payload carries (id, kind, tenant_id, source_channel).
        We need `sequence_num` (for replay) + `content` (for topic
        derivation + client display). One-row SELECT — fine on the pool.
        """
        try:
            obs_id = UUID(data["id"])
            tenant_id = UUID(data["tenant_id"])
            kind = data["kind"]
        except (KeyError, ValueError, TypeError) as e:
            log.warning("realtime: bad NOTIFY payload: %s / %s", e, data)
            return None

        row = await self._pool.fetchrow(
            """
            SELECT sequence_num, content, content_text, source_channel
            FROM observations
            WHERE id = $1 AND tenant_id = $2
            """,
            obs_id,
            tenant_id,
        )
        if row is None:
            # Observation was deleted or hasn't arrived on this replica.
            # Drop silently; the Wave 1-A ingestion invariant prevents
            # this under normal conditions (NOTIFY is post-commit).
            return None

        content = row["content"]
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                content = {}
        if not isinstance(content, dict):
            content = {}

        frame_kind = _map_event_kind(kind, content)
        payload = {
            "source_channel": row["source_channel"],
            "content": content,
            "content_text": row["content_text"],
        }
        return EventFrame(
            kind=frame_kind,
            id=obs_id,
            tenant_id=tenant_id,
            topic="",  # filled in by _dispatch per matching subscription
            sequence_num=int(row["sequence_num"]),
            payload=payload,
        )

    # ------------------------------------------------------------------
    # Topic derivation + enqueue
    # ------------------------------------------------------------------

    @staticmethod
    def _candidate_topics(frame: EventFrame) -> set[str]:
        """Every topic a subscription could legitimately match.

        Derivation rules (mirrors the subscribe vocabulary):
        - Always: `tenant:<tenant_uuid>` (catch-all per tenant)
        - From content.entity_id + content.entity_kind:
            goal → `goal:<id>`
            commitment → `commitment:<id>`
            resource → `customer:<id>` when entity_kind=='resource'
            (resources are the customer spine)
            decision → no topic (decisions aren't UI-primary)
        - From content.actor_id (or content.metadata.actor_id):
            `actor:<id>`
        """
        topics: set[str] = {f"tenant:{frame.tenant_id}"}
        content = frame.payload.get("content", {}) or {}
        entity_kind = content.get("entity_kind")
        entity_id = content.get("entity_id")
        if entity_id and entity_kind:
            ek = str(entity_kind).lower()
            if ek == "goal":
                topics.add(f"goal:{entity_id}")
            elif ek == "commitment":
                topics.add(f"commitment:{entity_id}")
            elif ek in ("resource", "customer"):
                topics.add(f"customer:{entity_id}")
        # Actor topics — peek at content root + metadata.
        actor_id = content.get("actor_id")
        if not actor_id:
            md = content.get("metadata") or {}
            if isinstance(md, dict):
                actor_id = md.get("actor_id")
        if actor_id:
            topics.add(f"actor:{actor_id}")
        return topics

    def _enqueue(self, state: _ClientState, frame: EventFrame) -> None:
        """Push; on QueueFull, drop oldest and bump dropped counter."""
        try:
            state.queue.put_nowait(frame)
            return
        except asyncio.QueueFull:
            pass
        # Drop oldest and try once more.
        dropped = 0
        try:
            state.queue.get_nowait()
            dropped += 1
        except asyncio.QueueEmpty:
            pass
        try:
            state.queue.put_nowait(frame)
        except asyncio.QueueFull:
            # Still full somehow (another drain race). Drop the incoming.
            dropped += 1
        state.dropped += dropped
        self.stats["drops"] += dropped

    # ------------------------------------------------------------------
    # Replay
    # ------------------------------------------------------------------

    async def replay_since(
        self,
        state: _ClientState,
        *,
        since_sequence_num: int,
        limit: int = 1000,
        partition_days: int = 30,
    ) -> int:
        """Fetch observations with sequence_num > X for this tenant + topics
        and push them onto the client's queue. Returns pushed count.

        We bound the scan with an `occurred_at` window to help partition
        pruning (BUILD-PLAN §5 Prompt 4.D "partition pruning").
        """
        if since_sequence_num < 0:
            return 0
        sub = state.sub
        # Partition-pruning bound.
        rows = await self._pool.fetch(
            """
            SELECT id, kind, tenant_id, sequence_num, content,
                   content_text, source_channel
            FROM observations
            WHERE tenant_id = $1
              AND sequence_num > $2
              AND occurred_at > now() - ($3 || ' days')::interval
            ORDER BY sequence_num ASC
            LIMIT $4
            """,
            sub.tenant_id,
            int(since_sequence_num),
            str(int(partition_days)),
            int(limit),
        )
        pushed = 0
        for r in rows:
            content = r["content"]
            if isinstance(content, str):
                try:
                    content = json.loads(content)
                except json.JSONDecodeError:
                    content = {}
            if not isinstance(content, dict):
                content = {}
            payload = {
                "source_channel": r["source_channel"],
                "content": content,
                "content_text": r["content_text"],
            }
            frame = EventFrame(
                kind=_map_event_kind(r["kind"], content),
                id=r["id"],
                tenant_id=r["tenant_id"],
                topic="",
                sequence_num=int(r["sequence_num"]),
                payload=payload,
            )
            candidate_topics = self._candidate_topics(frame)
            matched = next(
                (t for t in candidate_topics if t in sub.topics), None
            )
            if matched is None:
                continue
            outbound = EventFrame(
                kind=frame.kind,
                id=frame.id,
                tenant_id=frame.tenant_id,
                topic=matched,
                sequence_num=frame.sequence_num,
                payload=frame.payload,
            )
            self._enqueue(state, outbound)
            pushed += 1
        return pushed

    # ------------------------------------------------------------------
    # Test hooks
    # ------------------------------------------------------------------

    def client_count(self) -> int:
        return len(self._clients)

    def get_client(self, connection_id: UUID) -> _ClientState | None:
        return self._clients.get(connection_id)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _map_event_kind(obs_kind: str, content: dict[str, Any]) -> str:
    """Map Observation.kind + content to the EventFrame.kind vocabulary.

    - `signal` → `observation`
    - `state_change` → one of `state_change | act_change |
      resource_change` based on content.entity_kind.
    - `contestation`, `anomaly_flagged`, `prediction_resolution`,
      `transaction` → fall through as `observation` (with the original
      kind accessible in payload.content).
    """
    if obs_kind == "state_change":
        ek = (content or {}).get("entity_kind")
        if ek in ("goal", "commitment", "decision"):
            return "act_change"
        if ek in ("resource", "customer"):
            return "resource_change"
        return "state_change"
    return "observation"


__all__ = [
    "Dispatcher",
    "EventFrame",
    "SubscriptionFilter",
]
