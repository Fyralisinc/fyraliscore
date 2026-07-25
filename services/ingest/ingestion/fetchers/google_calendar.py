"""services/ingest/ingestion/fetchers/google_calendar.py — Calendar fetcher (IN-15).

Per A18 (per-source backfill = net-new code) + A16/N1 (cursor advanced by
ShardFetch, opaque to it) + A27.3 (records shaped for the handler).

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
A `google_calendar_events` shard streams one calendar's events. ShardFetch
calls this fetcher in a loop, persisting the returned cursor between calls.
Two modes share the cursor:

  - FULL (initial backfill): `events.list?timeMin=<now-N days>&singleEvents=true
    &orderBy=startTime`, paged via `pageToken`. The final page returns a
    `nextSyncToken`; the fetcher stamps it into the cursor (D2) — the warm
    start for the next incremental run.
  - INCREMENTAL (poll): when the cursor (or the shard, warm-started by the
    planner) carries a `sync_token`, `events.list?syncToken=<token>
    &showDeleted=true` returns ONLY changed/deleted events since the token.
    Google rejects `syncToken` combined with `timeMin`/`orderBy`, so the two
    modes are mutually exclusive.

`end_of_data=True` when a page returns no `nextPageToken` (the walk for this
fetch round is complete; the shard is marked done).

============================================================
SYNC-TOKEN EXPIRY (HTTP 410 -> full reseed; Risk #1)
============================================================
An aged-out sync token yields HTTP 410. The fetcher catches it, clears the
token, switches to FULL mode, and returns an empty cursor-reset page so
ShardFetch re-enters and runs a fresh windowed full sync. Dedup makes the
re-walk idempotent.

============================================================
HANDLER CONFORMANCE (A27.3) + external_id PARITY
============================================================
Each record is the RAW Calendar event object plus two injected private keys:
`_fyralis_calendar_id` (the calendar the event was read from) and
`_fyralis_owner_email` (the impersonated owner — for external-attendee
detection). The `google_calendar:event` handler derives
`external_id = gcal:{calendar_id}:{event_id}` and `occurred_at` from the
event's own start time — identical whether the event arrived via backfill or
the incremental "poll" re-run, so the dedup UNIQUE index collapses twins.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import CompanyOSError
from services.ingest.ingestion.fetchers import FetchResult
from services.ingest.integrations.google_calendar import metrics
from services.ingest.integrations.google_calendar.client import (
    GoogleCalendarClient,
    resolve_scope,
)


log = logging.getLogger(__name__)


SHARD_KIND_EVENTS = "google_calendar_events"


def _backfill_days() -> int:
    """D5 — windowed backfill horizon (env-overridable)."""
    try:
        return int(os.environ.get("GOOGLE_CALENDAR_BACKFILL_DAYS", "180"))
    except ValueError:
        return 180


class GoogleCalendarCursor(BaseModel):
    """Cursor for one calendar shard. Round-trips through the opaque dict in
    workflow_states.state_data per the M6.2a contract.

    - page_token   : Calendar's nextPageToken within the current run.
    - sync_token   : the ACTIVE incremental token (incremental mode when set).
    - next_sync_token : captured from the last page of a full/incremental
                        sync; the warm start for the next run (D2).
    - time_min     : the windowed-backfill lower bound, frozen on first call
                     so paging stays stable across ticks.
    - events_seen  : diagnostic.
    - high_water_updated : max event `updated` seen — the reconciler's gap
                           reference point.
    - seeded       : whether the first-call setup has run.
    """

    model_config = ConfigDict(extra="forbid")

    page_token: str | None = None
    sync_token: str | None = None
    next_sync_token: str | None = None
    time_min: str | None = None
    events_seen: int = 0
    high_water_updated: str | None = None
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> GoogleCalendarCursor:
    if c is None:
        return GoogleCalendarCursor()
    return GoogleCalendarCursor.model_validate(c)


def _encode_cursor(c: GoogleCalendarCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


def _bump_high_water(cur: GoogleCalendarCursor, event: dict[str, Any]) -> None:
    updated = event.get("updated")
    if isinstance(updated, str) and (
        cur.high_water_updated is None or updated > cur.high_water_updated
    ):
        cur.high_water_updated = updated


async def _open_calendar_client(install: asyncpg.Record):  # noqa: ANN202
    """Test seam — monkeypatched by tests. Production builds a real
    GoogleCalendarClient over the shared Gmail DWD minter + GoogleHttpClient."""
    from services.ingest.integrations.gmail.client import build_google_http_client
    from services.ingest.integrations.gmail.dwd import get_minter

    scope = resolve_scope(install["scope"])
    http = build_google_http_client(
        get_minter(),
        source="google_calendar",
        tenant_id=str(install["tenant_id"]),
        installation_id=str(install["id"]),
    )
    await http.__aenter__()
    client = GoogleCalendarClient(http, scope=scope)

    async def close() -> None:
        await http.__aexit__(None, None, None)

    return client, close


async def fetch_page_google_calendar(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of events + next cursor for a calendar shard."""
    calendar_id = shard_identifier.get("calendar_id")
    owner_email = shard_identifier.get("owner_email") or calendar_id
    if not isinstance(calendar_id, str) or not calendar_id:
        # Misconfigured shard — nothing to walk.
        return FetchResult(records=[], next_cursor=cursor, end_of_data=True)

    cur = _decode_cursor(cursor)

    # First-call setup: choose the sync mode + freeze the backfill window.
    if not cur.seeded:
        warm_token = shard_identifier.get("sync_token")
        if isinstance(warm_token, str) and warm_token:
            cur.sync_token = warm_token  # warm start -> incremental
        else:
            cur.time_min = (
                datetime.now(timezone.utc) - timedelta(days=_backfill_days())
            ).isoformat().replace("+00:00", "Z")
        cur.seeded = True

    incremental = cur.sync_token is not None
    client, close = await _open_calendar_client(install)
    try:
        try:
            if incremental:
                body = await client.list_events(
                    calendar_id=calendar_id,
                    user_email=owner_email,
                    sync_token=cur.sync_token,
                    page_token=cur.page_token,
                    show_deleted=True,
                )
            else:
                body = await client.list_events(
                    calendar_id=calendar_id,
                    user_email=owner_email,
                    time_min=cur.time_min,
                    page_token=cur.page_token,
                    order_by="startTime",
                )
        except CompanyOSError as exc:
            status = (exc.context or {}).get("status")
            if status == 410 and incremental:
                # Sync token expired — reseed a windowed full sync.
                metrics.record_fetch_event("sync_token_expired")
                log.info(
                    "google_calendar_sync_token_expired",
                    extra={"calendar_id": calendar_id},
                )
                reseed = GoogleCalendarCursor(
                    seeded=True,
                    time_min=(
                        datetime.now(timezone.utc)
                        - timedelta(days=_backfill_days())
                    ).isoformat().replace("+00:00", "Z"),
                    high_water_updated=cur.high_water_updated,
                )
                return FetchResult(
                    records=[], next_cursor=_encode_cursor(reseed),
                    end_of_data=False,
                )
            raise

        items = body.get("items")
        events = [e for e in items if isinstance(e, dict)] if isinstance(items, list) else []
        records: list[dict[str, Any]] = []
        for event in events:
            event["_fyralis_calendar_id"] = calendar_id
            event["_fyralis_owner_email"] = owner_email
            records.append(event)
            _bump_high_water(cur, event)

        next_page_token = body.get("nextPageToken")
        next_sync_token = body.get("nextSyncToken")
        is_last_page = not next_page_token

        cur.page_token = next_page_token if not is_last_page else None
        if isinstance(next_sync_token, str) and next_sync_token:
            cur.next_sync_token = next_sync_token
        cur.events_seen += len(records)

        if records:
            metrics.record_fetch_event("events", by=len(records))

        return FetchResult(
            records=records,
            next_cursor=_encode_cursor(cur),
            end_of_data=is_last_page,
        )
    finally:
        await close()




__all__ = [
    "SHARD_KIND_EVENTS",
    "GoogleCalendarCursor",
    "fetch_page_google_calendar",
]
