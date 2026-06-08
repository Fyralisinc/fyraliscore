"""Signal threads/messages fixture generator (IN-SIGNAL).

`make_signal(threads=N, messages_per_thread=M, ...)` produces a deterministic
Signal install fixture shaped to feed `MockSignalClient`. It mirrors the
telegram/mercury generators: every field is derived via `hashlib` (stable across
runs), timestamps land in 2026-01, and the shape is exactly what the mock client
pages over.

Fixture shape (consumed by `MockSignalClient(fixture=...)` AND by the harness's
signal install-seed branch, which INSERTs one signal_threads row per thread):
    {
      "threads": {
        "<thread_id>": {                       # key is str(thread_id) (JSON)
          "thread_id": <int>,
          "thread_kind": "direct"|"group",
          "title": "...",
          # messages sorted by id ASCENDING (oldest..newest); the mock pages
          # them BACKWARD on offset_id, the way thread history does.
          "messages": [ {id, date, edit_date, message, out, from_id}, ... ],
        },
        ...
      },
      "thread_order": [<int>, ...],            # planner shard order
      "page_size": 100,
    }

The fetcher (services/ingest/ingestion/fetchers/signal.py) emits ONE record per
message, so the observation count per thread is exactly `messages_per_thread`.
Default 1 thread x 5 messages = exactly 5 backfill observations/tenant.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


# Thread kinds cycled across threads (mirrors signal_threads.thread_kind).
_THREAD_KINDS = ("direct", "group")


def make_signal(
    *,
    threads: int = 1,
    messages_per_thread: int = 5,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
    seed: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic Signal install fixture.

    Args:
      threads: Number of threads (one shard each in the planner).
      messages_per_thread: Messages on each thread's history.
      base_iso: Anchor timestamp (2026-01); messages are spaced forward from it
        so id-ascending order matches time-ascending order.
      page_size: The mock client's per-page cap for `get_history`.
      seed: Optional namespacing salt mixed into the synthetic `thread_id`.
        Signal's external_id is already install-namespaced
        (`signal:{installation_id}:{thread_id}:…`, and each tenant has a distinct
        signal_installations id), so cross-tenant collision on the global
        observations UNIQUE index cannot happen even with identical thread ids.
        The seed is belt-and-suspenders: a per-tenant value (the tenant slug)
        also makes the thread ids themselves tenant-distinct, matching production
        where each linked account sees distinct threads.

    Returns:
      Fixture dict consumable by `MockSignalClient(fixture=...)`.
    """
    seed = seed or ""
    base_epoch = int(
        datetime.fromisoformat(base_iso.replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .timestamp()
    )

    threads_map: dict[str, dict[str, Any]] = {}
    thread_order: list[int] = []
    for t in range(threads):
        parts = ("signal-thread", t) if not seed else ("signal-thread", seed, t)
        # Positive 48-bit int — fits BIGINT and a Signal thread-id range.
        thread_id = int(_digest(*parts)[:12], 16)
        # Guard against a (vanishingly unlikely) zero / collision within a run.
        if thread_id == 0 or thread_id in thread_order:
            thread_id += t + 1
        thread_order.append(thread_id)
        kind = _THREAD_KINDS[t % len(_THREAD_KINDS)]
        messages = [
            _message(thread_id, idx, base_epoch)
            for idx in range(messages_per_thread)
        ]
        threads_map[str(thread_id)] = {
            "thread_id": thread_id,
            "thread_kind": kind,
            "title": f"Thread {t + 1}" + (f" ({seed})" if seed else ""),
            "messages": messages,
        }

    return {
        "threads": threads_map,
        "thread_order": thread_order,
        "page_size": page_size,
    }


def _message(thread_id: int, idx: int, base_epoch: int) -> dict[str, Any]:
    """One deterministic Signal message. id is 1-based and monotonic; date is
    spaced one hour apart forward from base so id-asc == time-asc (all 2026-01).
    Edits are unsupported in v1 → edit_date is always None."""
    message_id = idx + 1
    date = base_epoch + idx * 3600
    # A small rotating cast of senders per thread.
    sender_id = int(_digest("sender", thread_id, idx % 3)[:10], 16)
    return {
        "id": message_id,
        "date": date,
        "edit_date": None,
        "message": f"message {message_id} in thread {thread_id}",
        "out": False,
        "from_id": {"user_id": sender_id},
        "sender_username": f"user{idx % 3}_{_digest('u', thread_id)[:5]}",
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_signal"]
