"""services/ingest/ingestion/fetchers/signal.py — Signal backfill fetcher (IN-SIGNAL).

Per A18 (per-source backfill = net-new code) + A16/N1 (cursor advanced by
ShardFetch, opaque to it) + A27.3 (records shaped for the handler). Cloned from
the Telegram fetcher (its archetype).

============================================================
ONE SHARD KIND, BACKWARD-PAGED HISTORY
============================================================
A `signal_thread_history` shard streams one thread's message history. ShardFetch
calls this fetcher in a loop, persisting the returned cursor between calls. Like
Telegram's `messages.getHistory`, Signal's thread history pages BACKWARD from the
newest message: each call returns up to `limit` messages older than `offset_id`
(0 = start at the newest), and the OLDEST id in the page becomes the next page's
`offset_id`.

  - FULL (initial backfill): offset_id=0, min_id=0; page backward until a short/
    empty page (`is_last`), i.e. the start of history.
  - INCREMENTAL (reconciler re-walk): warm-started with `offset_id_cursor` (the
    prior high-water max id) → `min_id` bounds the walk to messages NEWER than
    the high-water, so only the changed tail comes back.

`high_water_max_id` (the MAX message id seen this run) is the reconciler's gap
reference point — the reconciler probes `has_history_since(min_id=high_water)`.

============================================================
ONE MESSAGE -> ONE RECORD (A27.3 handler conformance)
============================================================
One Signal message is one record. The fetcher builds the canonical record via
`integrations/signal/records.build_message_record` — the SAME builder the live
gateway worker uses — so a backfilled message and its live twin derive an
identical external_id (`signal:{install}:{thread}:{id}:none`) and collapse to one
observation.

RATE LIMIT: the client raises `SignalApiError(signal_api_rate_limited)` carrying
the server's `retry_after`. The fetcher leaves the cursor unadvanced and ends the
round empty (end_of_data=False) so ShardFetch re-enters next tick.

TODO(human): confirm Signal's real rate-limit signal + history pagination shape
against vendor docs (signal-cli / libsignal). The cursor/shard/record wiring here
is real; only the client transport is stubbed.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import SignalApiError
from services.ingest.ingestion.fetchers import FetchResult
from services.ingest.integrations.signal.records import build_message_record


log = logging.getLogger(__name__)


SHARD_KIND_THREAD_HISTORY = "signal_thread_history"
_DEFAULT_PAGE_SIZE = 100  # TODO(human): confirm Signal history page cap.


def _page_size() -> int:
    try:
        return min(100, max(1, int(os.environ.get("SIGNAL_BACKFILL_PAGE_SIZE", "100"))))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class SignalCursor(BaseModel):
    """Cursor for one thread shard. Round-trips through the opaque dict in
    workflow_states.state_data per the M6.2a contract.

    - offset_id          : the next (older) page boundary; 0 = start at newest.
    - min_id             : incremental floor (warm-start high-water); 0 = full.
    - high_water_max_id  : MAX message id seen this run — the reconciler's gap
                           reference point.
    - messages_seen      : diagnostic.
    - seeded             : whether the first-call warm-start setup has run.
    """

    model_config = ConfigDict(extra="forbid")

    offset_id: int = 0
    min_id: int = 0
    high_water_max_id: int = 0
    messages_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> SignalCursor:
    if c is None:
        return SignalCursor()
    return SignalCursor.model_validate(c)


def _encode_cursor(c: SignalCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real SignalClient on the install's backfill
# session; the mock harness / tests rebind this symbol to inject a fake.
async def _open_signal_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_signal_client
    return await open_signal_client(install)


async def fetch_page_signal(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of a thread's history (one record per message) + next cursor."""
    thread_id = shard_identifier.get("thread_id")
    if not isinstance(thread_id, int):
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    thread_kind = shard_identifier.get("thread_kind") or "direct"
    thread_title = shard_identifier.get("thread_title")
    installation_id = shard_identifier.get("installation_id") or (
        str(install["id"]) if "id" in install else ""
    )

    cur = _decode_cursor(cursor)
    if not cur.seeded:
        warm = shard_identifier.get("offset_id_cursor")
        if isinstance(warm, int) and warm > 0:
            cur.min_id = warm  # warm start -> incremental re-walk above high-water
            cur.high_water_max_id = warm
        cur.seeded = True

    page_size = _page_size()
    client, close = await _open_signal_client(install)
    try:
        try:
            messages, next_offset_id, is_last = await client.get_history(
                thread_id=thread_id,
                thread_kind=thread_kind,
                offset_id=cur.offset_id,
                min_id=cur.min_id,
                limit=page_size,
            )
        except SignalApiError as exc:
            if getattr(exc, "code", None) == "signal_api_rate_limited":
                # Retry budget deferred to ShardFetch — leave the cursor
                # unadvanced, end this round empty so it re-enters next tick.
                log.info(
                    "signal_backfill_rate_limited",
                    extra={
                        "thread_id": thread_id,
                        "retry_after": (exc.context or {}).get("retry_after"),
                    },
                )
                return FetchResult(
                    records=[], next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        records: list[dict[str, Any]] = []
        for msg in messages:
            mid = msg.get("id")
            if not isinstance(mid, int) or mid <= 0:
                continue
            if mid > cur.high_water_max_id:
                cur.high_water_max_id = mid
            records.append(build_message_record(
                msg,
                installation_id=installation_id,
                thread_id=thread_id,
                thread_kind=thread_kind,
                thread_title=thread_title,
            ))

        cur.messages_seen += len(records)
        # Advance the backward-paging cursor to the oldest id in this page.
        if isinstance(next_offset_id, int) and next_offset_id > 0:
            cur.offset_id = next_offset_id

        log.info(
            "signal_backfill_page",
            extra={
                "thread_id": thread_id,
                "messages": len(messages),
                "records": len(records),
                "is_last": is_last,
            },
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()




__all__ = [
    "SHARD_KIND_THREAD_HISTORY",
    "SignalCursor",
    "fetch_page_signal",
]
