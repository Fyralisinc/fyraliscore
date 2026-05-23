"""services/observations/events.py — post-commit NOTIFY emission.

BUILD-PLAN.md §2 Prompt 1.A item 2:
    "events.py — NOTIFY helper that emits 'observations_new' with
     (id, kind, tenant_id, source_channel) as JSON. Wrapped in the
     transaction commit hook."

Spec §1 "Process":
    1-6: INSERT inside transaction. 7: NOTIFY 'observations_new' with
    (id, kind, tenant_id, source_channel) AFTER the transaction
    commits.

Why post-commit matters:
- Subscribers listening via LISTEN consume the payload *immediately*
  and may attempt to SELECT the new row. If NOTIFY fires inside the
  transaction, the listener's SELECT runs before COMMIT and misses
  the row. Hours of debugging for anyone who gets this wrong.
- Postgres itself defers NOTIFY delivery until COMMIT (it queues
  notifications in the transaction and releases on commit). But if
  you issue NOTIFY on a pooled connection and then abort the
  transaction, the NOTIFY is silently dropped — fine for our use
  case, as long as we only notify on successful commits.

Design decision:
- We do NOT use Postgres's in-transaction NOTIFY because asyncpg's
  connection pool can return the connection mid-transaction to
  another caller before COMMIT. Instead, we record the notification
  intent in a list bound to the transaction, and issue the NOTIFY
  *after* the transaction context exits cleanly (no exception). This
  is what `schedule_notify` + `emit_pending_notifications` implement.
- Alternative path: Postgres statement-level `NOTIFY` inside the
  transaction works per spec (Postgres defers delivery to COMMIT),
  but we run the NOTIFY on a fresh connection from the pool, after
  commit, so the behavior is identical and explicit from the
  application's perspective. This is what `notify_new_observation`
  implements for the common case.
"""
from __future__ import annotations

import contextvars
import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg


OBSERVATIONS_CHANNEL = "observations_new"


# ---------------------------------------------------------------------
# Pending-notification buffer, scoped to a transaction.
# ---------------------------------------------------------------------
# A ContextVar holds the current buffer. Callers use `notify_scope()` to
# open a buffer before entering their transaction; `schedule_notify()`
# appends to it mid-transaction; `emit_pending_notifications()` is
# called after commit to flush them.
#
# Using a ContextVar rather than passing the buffer explicitly keeps the
# repo API small — `insert()` schedules its own notification without
# callers having to thread a buffer through every call site.

@dataclass(frozen=True)
class NewObservationEvent:
    id: UUID
    kind: str
    tenant_id: UUID
    source_channel: str

    def to_payload(self) -> str:
        return json.dumps(
            {
                "id": str(self.id),
                "kind": self.kind,
                "tenant_id": str(self.tenant_id),
                "source_channel": self.source_channel,
            },
            sort_keys=True,
        )


_pending: contextvars.ContextVar[list[NewObservationEvent] | None] = contextvars.ContextVar(
    "observations_pending_notifications", default=None
)


class _NotifyScope:
    """
    Context manager that binds a fresh pending-notification list for
    the duration of a block. On successful exit, notifications are
    returned so the caller can flush them post-commit; on exception,
    the buffer is discarded.

    Usage:

        async with transaction() as tx, notify_scope() as scope:
            await repo.insert(tx, obs)
        # transaction committed; scope.events holds what to NOTIFY
        await emit_pending_notifications(pool, scope.events)
    """

    def __init__(self) -> None:
        self._token: contextvars.Token | None = None
        self.events: list[NewObservationEvent] = []

    def __enter__(self) -> "_NotifyScope":
        self._token = _pending.set(self.events)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._token is not None:
            _pending.reset(self._token)
            self._token = None
        if exc is not None:
            # Drop on exception — nothing committed.
            self.events = []


def notify_scope() -> _NotifyScope:
    return _NotifyScope()


def schedule_notify(event: NewObservationEvent) -> bool:
    """
    Append `event` to the current notify scope's buffer. Returns True
    if a scope was active (event scheduled), False otherwise. Callers
    who skip the scope simply don't get notifications — useful for
    one-off scripts that shouldn't emit.
    """
    buf = _pending.get()
    if buf is None:
        return False
    buf.append(event)
    return True


# ---------------------------------------------------------------------
# Emission — runs on a fresh connection, AFTER the transaction
# commits. Postgres itself also defers NOTIFY until COMMIT, but by
# running on a separate connection we guarantee the listener sees the
# row post-commit even if the caller reuses the transaction connection.
# ---------------------------------------------------------------------

async def notify_new_observation(
    conn_or_pool: asyncpg.Connection | asyncpg.Pool,
    event: NewObservationEvent,
) -> None:
    """
    Emit a single `observations_new` NOTIFY. The payload is the JSON
    (id, kind, tenant_id, source_channel). Always fires a NOTIFY that
    commits immediately (asyncpg auto-commits execute() outside a
    transaction).

    Uses asyncpg's parameterized NOTIFY via pg_notify(channel,
    payload). Channel names containing non-identifier characters are
    legal with pg_notify but not with LISTEN; we use `observations_new`
    which is a valid identifier.
    """
    if isinstance(conn_or_pool, asyncpg.Pool):
        async with conn_or_pool.acquire() as conn:
            await conn.execute(
                "SELECT pg_notify($1, $2)",
                OBSERVATIONS_CHANNEL,
                event.to_payload(),
            )
        return
    await conn_or_pool.execute(
        "SELECT pg_notify($1, $2)",
        OBSERVATIONS_CHANNEL,
        event.to_payload(),
    )


async def emit_pending_notifications(
    conn_or_pool: asyncpg.Connection | asyncpg.Pool,
    events: list[NewObservationEvent],
) -> None:
    """
    Fire all buffered notifications. Callers pass the list returned by
    a `_NotifyScope`. Errors are NOT swallowed — if the DB is
    unreachable at notify time, the caller needs to know (the data is
    committed, but subscribers missed the wakeup).
    """
    for e in events:
        await notify_new_observation(conn_or_pool, e)


def event_from_row(row: dict[str, Any]) -> NewObservationEvent:
    """Helper used by tests + repo.insert to build an event from the
    inserted row's minimal fields."""
    return NewObservationEvent(
        id=row["id"] if isinstance(row["id"], UUID) else UUID(str(row["id"])),
        kind=row["kind"],
        tenant_id=(
            row["tenant_id"]
            if isinstance(row["tenant_id"], UUID)
            else UUID(str(row["tenant_id"]))
        ),
        source_channel=row["source_channel"],
    )


__all__ = [
    "OBSERVATIONS_CHANNEL",
    "NewObservationEvent",
    "notify_scope",
    "schedule_notify",
    "notify_new_observation",
    "emit_pending_notifications",
    "event_from_row",
]
