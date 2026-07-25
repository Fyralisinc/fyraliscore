"""services/ingest/ingestion/fetchers/linkedin.py — LinkedIn backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, THREE STREAMS, TWO SYNC MODES
============================================================
A `linkedin_entity` shard streams one Community-Management stream for the
organization (entity_type ∈ post | share_statistics | follower_statistics):

  - `post` — `GET /rest/posts?q=author&author={org URN}`, OFFSET-paginated via
    `start`/`count` over the `{"elements": [...], "paging": {...}}` envelope,
    sorted DESC by `lastModifiedAt` (epoch millis).
      * FULL: walk every page from start=0.
      * INCREMENTAL: warm-started with `updated_cursor` (the epoch-millis
        high-water); the finder has NO modified-since filter, so the fetcher
        relies on the DESC ordering — it keeps elements with
        `lastModifiedAt > floor` and early-terminates at the first element
        at/under the floor.
  - `share_statistics` / `follower_statistics` — snapshot-style, NOT paginated.
    The fetcher always requests TIME-BOUND buckets (`timeIntervals`, DAY
    granularity by default, env LINKEDIN_STATS_GRANULARITY) so every element is
    window-keyed by `timeRange.start` — that epoch-millis bucket IS the
    deterministic id the handler versions the external_id with (re-polling a
    snapshot dedups; a new bucket is a new observation).
      * FULL: window = the API's rolling 12-month maximum.
      * INCREMENTAL: window starts at floor+1 (timeRange.start is inclusive).

============================================================
RECORDS
============================================================
Each element is emitted as one record tagged with the private
`_fyralis_record_type` = the entity type, plus `_fyralis_org_urn`. The
`linkedin:object` handler builds ONE observation per record; external_id is
`linkedin:{org}:{kind}:{id}` (post id = the post URN; statistics id = the
`timeRange.start` bucket).

LinkedIn is poll-only and partner-gated; page size is env-overridable via
LINKEDIN_BACKFILL_PAGE_SIZE (the posts finder caps count at 100).
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.ingestion.fetchers import FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ENTITY = "linkedin_entity"
_DEFAULT_PAGE_SIZE = 100
# The statistics APIs only serve a rolling ~12-month window.
_STATS_BACKFILL_WINDOW_MS = 365 * 24 * 3600 * 1000
_STATISTICS_TYPES = ("share_statistics", "follower_statistics")


def _page_size() -> int:
    try:
        return max(
            1, min(100, int(os.environ.get("LINKEDIN_BACKFILL_PAGE_SIZE", "100"))),
        )
    except ValueError:
        return _DEFAULT_PAGE_SIZE


def _stats_granularity() -> str:
    return os.environ.get("LINKEDIN_STATS_GRANULARITY", "DAY")


class LinkedinCursor(BaseModel):
    """Cursor for one stream shard.

    - start            : the posts finder's OFFSET (`start` param, 0-based).
    - high_water_ms    : max epoch-millis watermark observed — posts use
                         `lastModifiedAt`, statistics use `timeRange.start`.
                         The warm-start / incremental lower bound AND the
                         reconciler's gap reference point.
    - incremental_floor_ms : the floor frozen for this run (None in FULL mode).
    - rows_seen        : diagnostic.
    - seeded           : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    start: int = 0
    high_water_ms: int | None = None
    incremental_floor_ms: int | None = None
    rows_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> LinkedinCursor:
    if c is None:
        return LinkedinCursor()
    return LinkedinCursor.model_validate(c)


def _encode_cursor(c: LinkedinCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real LinkedinClient; tests rebind this.
async def _open_linkedin_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_linkedin_client
    return await open_linkedin_client(install)


def _coerce_ms(value: Any) -> int | None:
    """Epoch-millis int from an int/str cursor or wire value (None otherwise)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _last_modified_ms(post: dict[str, Any]) -> int | None:
    return _coerce_ms(post.get("lastModifiedAt")) or _coerce_ms(post.get("createdAt"))


def _bucket_start_ms(element: dict[str, Any]) -> int | None:
    tr = element.get("timeRange")
    if isinstance(tr, dict):
        return _coerce_ms(tr.get("start"))
    return None


def _bump_high_water(cur: LinkedinCursor, value_ms: int | None) -> None:
    if value_ms is not None and (
        cur.high_water_ms is None or value_ms > cur.high_water_ms
    ):
        cur.high_water_ms = value_ms


def _org_urn_of(install: asyncpg.Record) -> str:
    return str(install["organization_urn"]) if "organization_urn" in install else ""


def _record(
    entity_type: str, element: dict[str, Any], organization_urn: str,
) -> dict[str, Any]:
    return {
        "_fyralis_record_type": entity_type,
        "_fyralis_org_urn": organization_urn,
        "entity": element,
    }


async def fetch_page_linkedin(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of stream elements + next cursor."""
    entity_type = shard_identifier.get("entity_type")
    if not isinstance(entity_type, str) or not entity_type:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)
    entity_type = entity_type.lower()

    cur = _decode_cursor(cursor)
    if not cur.seeded:
        warm = _coerce_ms(shard_identifier.get("updated_cursor"))
        if warm is not None:
            cur.incremental_floor_ms = warm
            cur.high_water_ms = warm
        cur.seeded = True

    organization_urn = _org_urn_of(install)

    client, close = await _open_linkedin_client(install)
    try:
        # RetryLater must reach shard_fetch so it persists next_attempt_at
        # instead of hot-looping an empty page with an unchanged cursor.
        if entity_type == "post":
            return await _fetch_posts_page(client, cur, organization_urn)
        if entity_type in _STATISTICS_TYPES:
            return await _fetch_statistics(
                client, cur, organization_urn, entity_type,
            )
        log.warning(
            "linkedin_backfill_unknown_entity_type",
            extra={"entity_type": entity_type},
        )
        return FetchResult(
            records=[], next_cursor=_encode_cursor(cur), end_of_data=True,
        )
    finally:
        await close()


async def _fetch_posts_page(
    client: Any, cur: LinkedinCursor, organization_urn: str,
) -> FetchResult:
    rows, next_start = await client.list_posts(
        start=cur.start, count=_page_size(),
    )

    records: list[dict[str, Any]] = []
    crossed_floor = False
    for row in rows:
        modified = _last_modified_ms(row)
        if (
            cur.incremental_floor_ms is not None
            and modified is not None
            and modified <= cur.incremental_floor_ms
        ):
            # DESC ordering: everything from here on is older than the floor.
            crossed_floor = True
            break
        records.append(_record("post", row, organization_urn))
        _bump_high_water(cur, modified)

    cur.rows_seen += len(records)
    is_last = crossed_floor or next_start is None
    if not is_last and next_start is not None:
        cur.start = next_start

    log.info(
        "linkedin_backfill_page",
        extra={"entity_type": "post", "rows": len(records), "is_last": is_last},
    )
    return FetchResult(
        records=records, next_cursor=_encode_cursor(cur), end_of_data=is_last,
    )


async def _fetch_statistics(
    client: Any, cur: LinkedinCursor, organization_urn: str, entity_type: str,
) -> FetchResult:
    now_ms = int(time.time() * 1000)
    if cur.incremental_floor_ms is not None:
        # timeRange.start is inclusive; the floor bucket was already ingested.
        start_ms = cur.incremental_floor_ms + 1
    else:
        start_ms = now_ms - _STATS_BACKFILL_WINDOW_MS

    method = (
        client.share_statistics
        if entity_type == "share_statistics" else client.follower_statistics
    )
    elements = await method(start_ms=start_ms, granularity=_stats_granularity())

    records: list[dict[str, Any]] = []
    for element in elements:
        bucket = _bucket_start_ms(element)
        if bucket is None:
            # Without a timeRange there is no deterministic bucket id — skip
            # rather than minting an unstable external_id (the read path always
            # requests time-bound stats, so this is defensive only).
            log.warning(
                "linkedin_backfill_statistics_element_missing_time_range",
                extra={"entity_type": entity_type},
            )
            continue
        records.append(_record(entity_type, element, organization_urn))
        _bump_high_water(cur, bucket)

    cur.rows_seen += len(records)
    log.info(
        "linkedin_backfill_page",
        extra={"entity_type": entity_type, "rows": len(records), "is_last": True},
    )
    # The statistics finders are NOT paginated — one call is the whole window.
    return FetchResult(
        records=records, next_cursor=_encode_cursor(cur), end_of_data=True,
    )




__all__ = [
    "SHARD_KIND_ENTITY",
    "LinkedinCursor",
    "fetch_page_linkedin",
]
