"""services/ingest/ingestion/fetchers/miro.py — Miro backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `miro_board_items` shard streams one board's items.

  - FULL (initial backfill): walk `GET /boards/{id}/items` from the start via
    the opaque cursor, paginated. ONE observation per item — no extra board
    snapshot — so a board with N items yields exactly N backfill observations.
  - INCREMENTAL (poll): when the shard is warm-started with an `item_cursor`
    (the high-water item `modifiedAt`), the fetcher resumes from the saved
    opaque cursor; the overlap re-fetch dedups via the versioned external_id.

============================================================
FAN-OUT: ONE BOARD -> N ITEM RECORDS
============================================================
The `miro:item` handler produces ONE observation per record. The fetcher emits:
  - "item" : one per board item.

Each record is tagged with a private `_fyralis_record_type` the handler branches
on. external_id parity (set by the handler) collapses a backfilled record and
its live-webhook twin to one observation. Because an item MUTATES (a sticky
note's text/position is edited), its external_id is versioned by `version`.

CONFIRMED (developers.miro.com): `GET /v2/boards/{id}/items` is CURSOR-paginated
(`limit` 10-50 + `cursor`; the response returns the next `cursor`) — this
fetcher's opaque-cursor handling is correct. NOTE: the parent `GET /v2/boards`
listing is OFFSET-paginated (`limit`/`offset`), a different paginator. Miro has
NO "modified since" filter on items, so the warm-start high-water rides the
cursor for the reconciler's gap reference; rate-limit signal is 429 +
`X-RateLimit-*` headers. (Miro webhooks were discontinued 2025-12-05 → live edge
must become poll-only; see signatures/miro.py.) Page
size is overridable via `MIRO_BACKFILL_PAGE_SIZE`.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import MiroApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_BOARD_ITEMS = "miro_board_items"
_DEFAULT_PAGE_SIZE = 50


def _page_size() -> int:
    try:
        return min(50, int(os.environ.get("MIRO_BACKFILL_PAGE_SIZE", "50")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class MiroCursor(BaseModel):
    """Cursor for one board shard. Round-trips through the opaque dict in
    workflow_states.state_data.

    - page_cursor        : the opaque Miro list cursor within a run. None on the
                           first page; `next_cursor is None` is terminal.
    - high_water_modified : max item `modifiedAt` (ISO) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - incremental_floor  : the modified-since lower bound frozen for this run
                          (None in FULL mode).
    - items_seen         : diagnostic.
    - seeded             : whether the first-call setup (board snapshot) ran.
    """

    model_config = ConfigDict(extra="forbid")

    page_cursor: str | None = None
    high_water_modified: str | None = None
    incremental_floor: str | None = None
    items_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> MiroCursor:
    if c is None:
        return MiroCursor()
    return MiroCursor.model_validate(c)


def _encode_cursor(c: MiroCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real MiroClient against the install's auth;
# the mock harness / tests rebind this symbol to inject a fake.
async def _open_miro_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_miro_client
    return await open_miro_client(install)


def _bump_high_water(cur: MiroCursor, modified: Any) -> None:
    if isinstance(modified, str) and (
        cur.high_water_modified is None or modified > cur.high_water_modified
    ):
        cur.high_water_modified = modified


def _item_modified(item: dict[str, Any]) -> Any:
    """The item's last-modified timestamp (Miro nests it under `modifiedAt` or
    `updatedAt`, sometimes inside `createdAt` fallback)."""
    return item.get("modifiedAt") or item.get("updatedAt") or item.get("createdAt")


async def fetch_page_miro(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of board items (+ a board snapshot on the first page) + cursor."""
    board_id = shard_identifier.get("board_id")
    if not isinstance(board_id, str) or not board_id:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    # The org id namespaces every observation's external_id (`miro:{org_id}:…`).
    # The planner threads it onto the shard_identifier; fall back to the board id
    # when absent so a record is never emitted unnamespaced.
    org_id = shard_identifier.get("org_id")
    if not isinstance(org_id, str) or not org_id:
        org_id = board_id

    cur = _decode_cursor(cursor)
    records: list[dict[str, Any]] = []

    client, close = await _open_miro_client(install)
    try:
        # First-call setup: warm-start mode. Unlike the Brex archetype (which
        # emits a per-shard balance snapshot), Miro emits ONE observation per
        # board item and NO extra board-snapshot record — so a board with N
        # items yields exactly N backfill observations.
        if not cur.seeded:
            warm = shard_identifier.get("item_cursor")
            if isinstance(warm, str) and warm:
                cur.incremental_floor = warm  # warm start -> incremental
                cur.high_water_modified = warm
            cur.seeded = True

        try:
            items, next_cursor, total = await client.list_items(
                board_id,
                limit=_page_size(),
                cursor=cur.page_cursor,
            )
        except MiroApiError as exc:
            if (exc.context or {}).get("code") == "miro_api_rate_limited" or \
               getattr(exc, "_code", None) == "miro_api_rate_limited":
                log.info("miro_backfill_rate_limited",
                         extra={"board_id": board_id})
                return FetchResult(
                    records=records, next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        for item in items:
            records.append({
                "_fyralis_record_type": "item",
                "_fyralis_board_id": board_id,
                "_fyralis_org_id": org_id,
                "item": item,
            })
            _bump_high_water(cur, _item_modified(item))

        cur.items_seen += len(items)
        is_last = next_cursor is None
        cur.page_cursor = next_cursor

        log.info(
            "miro_backfill_page",
            extra={"board_id": board_id, "items": len(items),
                   "records": len(records), "is_last": is_last},
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()


FETCHER_DISPATCH["miro"] = fetch_page_miro


__all__ = [
    "SHARD_KIND_BOARD_ITEMS",
    "MiroCursor",
    "fetch_page_miro",
]
