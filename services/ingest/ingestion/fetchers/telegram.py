"""services/ingest/ingestion/fetchers/telegram.py — Telegram backfill fetcher (IN-TELEGRAM).

Per A18 (per-source backfill = net-new code) + A16/N1 (cursor advanced by
ShardFetch, opaque to it) + A27.3 (records shaped for the handler).

============================================================
ONE SHARD KIND, BACKWARD-PAGED HISTORY
============================================================
A `telegram_dialog_history` shard streams one dialog's message history.
ShardFetch calls this fetcher in a loop, persisting the returned cursor between
calls. Telegram's `messages.getHistory` pages BACKWARD from the newest message:
each call returns up to `limit` messages older than `offset_id` (0 = start at
the newest), and the OLDEST id in the page becomes the next page's `offset_id`.

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
Unlike Jira (which fans an issue into issue/transition/comment records), one
Telegram message is one record. The fetcher builds the canonical record via
`integrations/telegram/records.build_message_record` — the SAME builder the live
gateway worker uses — so a backfilled message and its live `updateNewMessage`
twin derive an identical external_id (`telegram:{install}:{dialog}:{id}:{edit}`)
and collapse to one observation.

FLOOD_WAIT (error 420): the client raises `TelegramApiError(telegram_api_flood_wait)`
carrying the server's `retry_after`. The fetcher leaves the cursor unadvanced and
ends the round empty (end_of_data=False) so ShardFetch re-enters next tick — the
canonical Telegram backoff (wait the server's value, do not client-choose).
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import TelegramApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingest.integrations.telegram.records import build_message_record


log = logging.getLogger(__name__)


SHARD_KIND_DIALOG_HISTORY = "telegram_dialog_history"
_DEFAULT_PAGE_SIZE = 100  # messages.getHistory caps the limit at 100.


def _page_size() -> int:
    try:
        return min(100, max(1, int(os.environ.get("TELEGRAM_BACKFILL_PAGE_SIZE", "100"))))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class TelegramCursor(BaseModel):
    """Cursor for one dialog shard. Round-trips through the opaque dict in
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


def _decode_cursor(c: dict[str, Any] | None) -> TelegramCursor:
    if c is None:
        return TelegramCursor()
    return TelegramCursor.model_validate(c)


def _encode_cursor(c: TelegramCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real TelegramClient on the install's backfill
# session; the mock harness / tests rebind this symbol to inject a fake.
async def _open_telegram_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_telegram_client
    return await open_telegram_client(install)


async def fetch_page_telegram(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of a dialog's history (one record per message) + next cursor."""
    dialog_id = shard_identifier.get("dialog_id")
    if not isinstance(dialog_id, int):
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    dialog_kind = shard_identifier.get("dialog_kind") or "chat"
    access_hash = shard_identifier.get("access_hash")
    dialog_title = shard_identifier.get("dialog_title")
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
    client, close = await _open_telegram_client(install)
    try:
        try:
            messages, next_offset_id, is_last = await client.get_history(
                dialog_id=dialog_id,
                access_hash=access_hash,
                dialog_kind=dialog_kind,
                offset_id=cur.offset_id,
                min_id=cur.min_id,
                limit=page_size,
            )
        except TelegramApiError as exc:
            if getattr(exc, "code", None) == "telegram_api_flood_wait":
                # Retry budget deferred to ShardFetch — leave the cursor
                # unadvanced, end this round empty so it re-enters next tick.
                log.info(
                    "telegram_backfill_flood_wait",
                    extra={
                        "dialog_id": dialog_id,
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
                dialog_id=dialog_id,
                dialog_kind=dialog_kind,
                dialog_title=dialog_title,
            ))

        cur.messages_seen += len(records)
        # Advance the backward-paging cursor to the oldest id in this page.
        if isinstance(next_offset_id, int) and next_offset_id > 0:
            cur.offset_id = next_offset_id

        log.info(
            "telegram_backfill_page",
            extra={
                "dialog_id": dialog_id,
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


FETCHER_DISPATCH["telegram"] = fetch_page_telegram


__all__ = [
    "SHARD_KIND_DIALOG_HISTORY",
    "TelegramCursor",
    "fetch_page_telegram",
]
