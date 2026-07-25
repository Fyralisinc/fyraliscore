"""services/ingest/ingestion/fetchers/fireflies.py — Fireflies backfill/poll fetcher.

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `fireflies_transcripts` shard streams one workspace's meeting transcripts.

  - FULL (initial backfill): walk the GraphQL `transcripts` query from offset 0, paginated,
    newest-first.
  - INCREMENTAL (poll): when the shard is warm-started with a
    `transcript_cursor` (the high-water transcript `dateTime`), the fetcher
    passes `start=<iso>` so only recent transcripts come back; the overlap
    re-fetch dedups via the versioned external_id.

============================================================
FAN-OUT: ONE WORKSPACE -> N RECORDS
============================================================
The `fireflies:transcript` handler produces ONE observation per record. Unlike
the Brex archetype there is NO snapshot/balance record — the fetcher emits ONLY
"transcript" records (one per meeting transcript), so the observation count per
workspace equals the number of transcripts.

Each record is tagged with a private `_fyralis_record_type` the handler branches
on. external_id parity (set by the handler) collapses a backfilled record and
its live-webhook twin to one observation. Because a transcript can be
re-processed (a richer summary lands later), its external_id is versioned by a
content `version` (updated_at / processing version).

CONFIRMED (docs.fireflies.ai/graphql-api/query/transcripts): the GraphQL
`transcripts` query is OFFSET-based — `skip` (offset) + `limit` (page size, MAX
50) — and filters by `fromDate`/`toDate` (ISO-8601). The offset/limit cursor
bookkeeping in `FirefliesCursor` maps directly onto `skip`/`limit`; the `start=`
date filter maps onto `fromDate`. Rate-limit signal is HTTP 429 / `too_many_requests`.
TODO(human): the GraphQL query wiring (vs. the cloned REST `_request`) + the exact
digest encoding of the webhook signature still need empirical confirmation. Page size is
overridable via `FIREFLIES_BACKFILL_PAGE_SIZE`.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import FirefliesApiError
from services.ingest.ingestion.fetchers import FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_TRANSCRIPTS = "fireflies_transcripts"
_DEFAULT_PAGE_SIZE = 50


def _page_size() -> int:
    try:
        return min(500, int(os.environ.get("FIREFLIES_BACKFILL_PAGE_SIZE", "50")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class FirefliesCursor(BaseModel):
    """Cursor for one workspace shard. Round-trips through the opaque dict in
    workflow_states.state_data.

    - offset             : the list-transcripts pagination offset within a run.
    - high_water_created : max transcript `dateTime` (ISO) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - incremental_floor  : the `start=` lower bound frozen for this run (None in
                           FULL mode).
    - transcripts_seen   : diagnostic.
    - seeded             : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    offset: int = 0
    high_water_created: str | None = None
    incremental_floor: str | None = None
    transcripts_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> FirefliesCursor:
    if c is None:
        return FirefliesCursor()
    return FirefliesCursor.model_validate(c)


def _encode_cursor(c: FirefliesCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real FirefliesClient against the install's auth;
# the mock harness / tests rebind this symbol to inject a fake.
async def _open_fireflies_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_fireflies_client
    return await open_fireflies_client(install)


def _transcript_created(t: dict[str, Any]) -> Any:
    return t.get("dateTime") or t.get("date") or t.get("createdAt")


def _iso_from_value(value: Any) -> str | None:
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, str) and value:
        return value
    return None


def _bump_high_water(cur: FirefliesCursor, created: Any) -> None:
    created = _iso_from_value(created)
    if isinstance(created, str) and (
        cur.high_water_created is None or created > cur.high_water_created
    ):
        cur.high_water_created = created


async def fetch_page_fireflies(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of transcripts + cursor."""
    workspace_id = shard_identifier.get("workspace_id")
    if not isinstance(workspace_id, str) or not workspace_id:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)
    records: list[dict[str, Any]] = []

    client, close = await _open_fireflies_client(install)
    try:
        # First-call setup: warm-start mode (no snapshot record for Fireflies).
        if not cur.seeded:
            warm = shard_identifier.get("transcript_cursor")
            if isinstance(warm, str) and warm:
                cur.incremental_floor = warm  # warm start -> incremental
                cur.high_water_created = warm
            cur.seeded = True

        try:
            transcripts, next_offset, total = await client.list_transcripts(
                limit=_page_size(),
                offset=cur.offset,
                start=cur.incremental_floor,
            )
        except FirefliesApiError as exc:
            if (exc.context or {}).get("code") == "fireflies_api_rate_limited" or \
               getattr(exc, "_code", None) == "fireflies_api_rate_limited":
                log.info("fireflies_backfill_rate_limited",
                         extra={"workspace_id": workspace_id})
                return FetchResult(
                    records=records, next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        for transcript in transcripts:
            records.append({
                "_fyralis_record_type": "transcript",
                "_fyralis_workspace_id": workspace_id,
                "transcript": transcript,
            })
            _bump_high_water(cur, _transcript_created(transcript))

        cur.transcripts_seen += len(transcripts)
        is_last = next_offset is None
        cur.offset = next_offset if next_offset is not None else cur.offset

        log.info(
            "fireflies_backfill_page",
            extra={"workspace_id": workspace_id, "transcripts": len(transcripts),
                   "records": len(records), "is_last": is_last},
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()




__all__ = [
    "SHARD_KIND_TRANSCRIPTS",
    "FirefliesCursor",
    "fetch_page_fireflies",
]
