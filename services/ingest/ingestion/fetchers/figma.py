"""services/ingest/ingestion/fetchers/figma.py — Figma backfill/poll fetcher (design).

Per the per-source backfill contract (A18): a fetcher takes one
`(install, shard_identifier, cursor)` triple and returns one page of records +
the next cursor. ShardFetch calls it in a loop, persisting the cursor between
calls.

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `figma_file_events` shard streams one file's events (named versions +
comments collapsed into an "event" stream).

  - FULL (initial backfill): walk `GET /v1/files/{key}/events` from offset 0,
    paginated, newest-first.
  - INCREMENTAL (poll): when the shard is warm-started with an `event_cursor`
    (the high-water event `createdAt`), the fetcher passes `start=<date>` so only
    recent events come back; the overlap re-fetch dedups via the versioned
    external_id.

Unlike the Brex archetype (which also emits a per-shard balance snapshot), the
Figma fetcher emits ONLY `event` records — exactly one per event — so a fixture
of N events yields N backfill observations per tenant (the gate's
expected-backfill count keys on this 1:1).

============================================================
FAN-OUT: ONE FILE -> N EVENT RECORDS
============================================================
The `figma:event` handler produces ONE observation per record. Each record is
tagged with a private `_fyralis_record_type="event"` the handler branches on.
external_id parity (set by the handler) collapses a backfilled event and its
live-webhook twin to one observation. Because an event's payload can be
re-published with a new `version`, its external_id is versioned by `version`.

TODO(human): confirm Figma events API pagination (offset vs cursor) +
created/posted filter. This fetcher clones the Brex offset/limit + `start=`
date-filter contract (UNVERIFIED for Figma). Real Figma has no single `/events`
list endpoint — backfill derives events from `/versions` + `/comments`; if so,
replace the single `list_events` call with the two companion walks and merge
them into the event stream. Page size is overridable via
`FIGMA_BACKFILL_PAGE_SIZE`.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.integrations.figma.client import FigmaApiError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_FILE_EVENTS = "figma_file_events"
_DEFAULT_PAGE_SIZE = 100


def _page_size() -> int:
    try:
        return min(500, int(os.environ.get("FIGMA_BACKFILL_PAGE_SIZE", "100")))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


class FigmaCursor(BaseModel):
    """Cursor for one file shard. Round-trips through the opaque dict in
    workflow_states.state_data.

    - offset            : the list-events pagination offset within a run.
    - high_water_created : max event `createdAt` (ISO) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - incremental_floor : the `start=` lower bound frozen for this run (None in
                          FULL mode).
    - events_seen       : diagnostic.
    - seeded            : whether the first-call setup ran.
    """

    model_config = ConfigDict(extra="forbid")

    offset: int = 0
    high_water_created: str | None = None
    incremental_floor: str | None = None
    events_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> FigmaCursor:
    if c is None:
        return FigmaCursor()
    return FigmaCursor.model_validate(c)


def _encode_cursor(c: FigmaCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real FigmaClient against the install's auth;
# the mock harness / tests rebind this symbol to inject a fake.
async def _open_figma_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_figma_client
    return await open_figma_client(install)


def _team_id_of(install: Any, shard_identifier: dict[str, Any]) -> str:
    """Resolve the Figma team id that namespaces every external_id.

    Prefers the install row's `team_id`; falls back to the shard_identifier
    (which the planner/tests may carry it on) so a unit test can drive the
    fetcher without a full install row.
    """
    try:
        if install is not None and "team_id" in install:
            tid = install["team_id"]
            if isinstance(tid, str) and tid:
                return tid
    except (KeyError, TypeError):
        pass
    tid = shard_identifier.get("team_id")
    return tid if isinstance(tid, str) and tid else ""


def _iso_date(iso: str | None) -> str | None:
    """The date portion of an ISO timestamp (Figma `start` is date-granular)."""
    if not isinstance(iso, str) or not iso:
        return None
    return iso[:10]


def _bump_high_water(cur: FigmaCursor, created: Any) -> None:
    if isinstance(created, str) and (
        cur.high_water_created is None or created > cur.high_water_created
    ):
        cur.high_water_created = created


async def fetch_page_figma(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of file events + cursor."""
    file_key = shard_identifier.get("file_key")
    if not isinstance(file_key, str) or not file_key:
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    # The team_id namespaces every external_id (figma:{team_id}:event:…) so two
    # tenants' identical synthetic event ids never collapse on the global
    # observations UNIQUE(source_channel, external_id, occurred_at). It rides on
    # the install row; the shard_identifier carries it as a fallback for tests.
    team_id = _team_id_of(install, shard_identifier)

    cur = _decode_cursor(cursor)
    records: list[dict[str, Any]] = []

    client, close = await _open_figma_client(install)
    try:
        # First-call setup: warm-start mode (no snapshot record — figma is a
        # pure event stream, so the per-tenant observation count equals the
        # event count).
        if not cur.seeded:
            warm = shard_identifier.get("event_cursor")
            if isinstance(warm, str) and warm:
                cur.incremental_floor = warm  # warm start -> incremental
                cur.high_water_created = warm
            cur.seeded = True

        try:
            events, next_offset, total = await client.list_events(
                file_key,
                limit=_page_size(),
                offset=cur.offset,
                start=_iso_date(cur.incremental_floor),
            )
        except FigmaApiError as exc:
            if (exc.context or {}).get("code") == "figma_api_rate_limited" or \
               getattr(exc, "_code", None) == "figma_api_rate_limited":
                log.info("figma_backfill_rate_limited",
                         extra={"file_key": file_key})
                return FetchResult(
                    records=records, next_cursor=_encode_cursor(cur),
                    end_of_data=False,
                )
            raise

        for event in events:
            records.append({
                "_fyralis_record_type": "event",
                "_fyralis_file_key": file_key,
                "_fyralis_team_id": team_id,
                "event": event,
            })
            _bump_high_water(cur, event.get("createdAt") or event.get("created_at"))

        cur.events_seen += len(events)
        is_last = next_offset is None
        cur.offset = next_offset if next_offset is not None else cur.offset

        log.info(
            "figma_backfill_page",
            extra={"file_key": file_key, "events": len(events),
                   "records": len(records), "is_last": is_last},
        )
        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last,
        )
    finally:
        await close()


FETCHER_DISPATCH["figma"] = fetch_page_figma


__all__ = [
    "SHARD_KIND_FILE_EVENTS",
    "FigmaCursor",
    "fetch_page_figma",
]
