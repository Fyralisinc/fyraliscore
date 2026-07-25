"""services/ingest/ingestion/fetchers/aws.py — AWS CloudTrail events backfill/poll fetcher (IN-AWS).

Per A18 (per-source backfill = net-new code) + A16/N1 (cursor advanced by
ShardFetch, opaque to it) + A27.3 (records shaped for the handler).

============================================================
ONE SHARD KIND, TWO SYNC MODES
============================================================
An `aws_account_events` shard streams one (account, region)'s CloudTrail
management events (alarm-state-change + control-plane management events are
account/region-wide, so there is ONE shard per install, not one-per-resource like
Jira's per-project shards). ShardFetch calls this fetcher in a loop, persisting
the returned cursor between calls. Two modes share it:

  - FULL (initial backfill): walk `CloudTrail:LookupEvents` over a TIME WINDOW
    `[floor_ms, now]`, bounded below by a window floor (AWS_BACKFILL_WINDOW_DAYS,
    default 90 days — CloudTrail only retains 90 days of management events via
    LookupEvents; 0 = no floor). Within the window the walk advances the opaque
    `events_cursor` (CloudTrail `NextToken`) until a page returns no token.
  - INCREMENTAL (poll / reconciler reshare): warm-started with an `updated_cursor`
    (the prior run's high-water event `eventTime` in epoch ms). The floor is set
    to that high-water so only newer events come back; the boundary event
    re-fetches and dedups via the immutable external_id.

`end_of_data=True` when a page returns no continuation token.

============================================================
external_id (set by the handler)
============================================================
`aws:{account_id}:{region}:event:{event_id}` — IMMUTABLE. A CloudTrail
`eventId` is globally unique and stable, so the key is just a namespaced id (no
version suffix). Each record is tagged with private `_fyralis_record_type="event"`
+ `_fyralis_account_id` + `_fyralis_region` for external_id namespacing, matching
what the live poll edge derives from the install.
"""
from __future__ import annotations

import datetime as _dt
import logging
import os
import time
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict

from services.ingest.ingestion.fetchers import FetchResult


log = logging.getLogger(__name__)


SHARD_KIND_ACCOUNT_EVENTS = "aws_account_events"
_DEFAULT_PAGE_SIZE = 50
_MS_PER_DAY = 86_400_000


def _page_size() -> int:
    try:
        return max(1, min(50, int(os.environ.get("AWS_EVENTS_PAGE_SIZE", "50"))))
    except ValueError:
        return _DEFAULT_PAGE_SIZE


def _backfill_window_ms() -> int:
    """Lower-bound window for the FULL walk, in ms. 0 (env) = no floor.

    CloudTrail LookupEvents retains only 90 days of management events, so the
    default floor matches the API's own retention rather than being arbitrary.
    """
    try:
        days = int(os.environ.get("AWS_BACKFILL_WINDOW_DAYS", "90"))
    except ValueError:
        days = 90
    return max(0, days) * _MS_PER_DAY


def _now_ms() -> int:
    return int(time.time() * 1000)


class AwsCursor(BaseModel):
    """Cursor for one account-events shard. Round-trips through the opaque dict
    in workflow_states.state_data per the M6.2a contract.

    - high_water_time_ms : max event `eventTime` (epoch ms) observed — the
                           warm-start / incremental lower bound AND the
                           reconciler's gap reference point.
    - events_cursor      : opaque CloudTrail `NextToken` for the NEXT page within
                           the window. None on the first page and once exhausted.
    - floor_ms           : the lower `from` bound frozen for this run (None in a
                           no-floor walk).
    - to_ms              : the upper `to` bound frozen for this run (== run start;
                           keeps the window stable across pages).
    - events_seen        : diagnostic.
    - seeded             : whether the first-call setup has run.
    """

    model_config = ConfigDict(extra="forbid")

    high_water_time_ms: int | None = None
    events_cursor: str | None = None
    floor_ms: int | None = None
    to_ms: int | None = None
    events_seen: int = 0
    seeded: bool = False


def _decode_cursor(c: dict[str, Any] | None) -> AwsCursor:
    if c is None:
        return AwsCursor()
    return AwsCursor.model_validate(c)


def _encode_cursor(c: AwsCursor) -> dict[str, Any]:
    return c.model_dump(mode="json")


# Test seam — production opens a real AwsClient against the install's auth;
# the mock harness / tests rebind this symbol to inject a fake. Delegates to the
# shared fetchers/_clients.py `open_aws_client` opener (matching every other
# fetcher's seam), which resolves the REAL process-wide secret_store + pool —
# the prior inline build hardcoded secret_store=None, so resolve_credentials
# raised before the first LookupEvents call on any real install.
async def _open_aws_client(install: asyncpg.Record):  # noqa: ANN202
    from services.ingest.ingestion.fetchers._clients import open_aws_client
    return await open_aws_client(install)


def _account_of(install: asyncpg.Record) -> str:
    return str(install["account_id"]) if "account_id" in install else ""


def _region_of(install: asyncpg.Record) -> str:
    return str(install["region"]) if "region" in install else "us-east-1"


def _iso_to_ms(value: str) -> int | None:
    """RFC3339 / ISO-8601 string -> epoch ms (or None)."""
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = _dt.datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return int(dt.timestamp() * 1000)


def _event_time_ms(event: dict[str, Any]) -> int | None:
    """Extract the event time (epoch ms) from a CloudTrail event element.

    Handles BOTH shapes:
      - SYNTHETIC / normalized: camelCase `eventTime` as epoch ms (int/float).
      - REAL botocore LookupEvents: PascalCase `EventTime` as a `datetime`
        (botocore parses CloudTrail's RFC3339 into an aware datetime); some
        captures / fixtures carry it as an ISO-8601 string instead.
    The camelCase epoch-ms path is the fallback and is read first to keep the
    synthetic gate's behavior byte-identical.
    """
    t = event.get("eventTime")
    if t is None:
        t = event.get("EventTime")
    if isinstance(t, bool):
        return None
    if isinstance(t, (int, float)):
        return int(t)
    if isinstance(t, _dt.datetime):
        dt = t if t.tzinfo else t.replace(tzinfo=_dt.timezone.utc)
        return int(dt.timestamp() * 1000)
    if isinstance(t, str):
        return _iso_to_ms(t)
    return None


async def fetch_page_aws(
    install: asyncpg.Record,
    shard_identifier: dict[str, Any],
    cursor: dict[str, Any] | None,
) -> FetchResult:
    """One page of CloudTrail events (tagged as records) + next cursor."""
    cur = _decode_cursor(cursor)

    if not cur.seeded:
        cur.to_ms = _now_ms()
        warm = shard_identifier.get("updated_cursor")
        if isinstance(warm, (int, float)) and warm:
            # Warm start -> incremental: floor at the prior high-water.
            cur.floor_ms = int(warm)
            cur.high_water_time_ms = int(warm)
        else:
            window = _backfill_window_ms()
            cur.floor_ms = (cur.to_ms - window) if window > 0 else None
        cur.seeded = True

    account_id = _account_of(install)
    region = _region_of(install)
    page_size = _page_size()

    client, close = await _open_aws_client(install)
    try:
        page = await client.list_events(
            account_id=account_id,
            region=region,
            from_ms=cur.floor_ms,
            to_ms=cur.to_ms,
            cursor=cur.events_cursor,
            limit=page_size,
        )

        events = page.get("events") or []
        next_cursor = page.get("next_cursor")

        records: list[dict[str, Any]] = []
        for event in events:
            rec = dict(event)
            rec["_fyralis_record_type"] = "event"
            rec["_fyralis_account_id"] = account_id
            rec["_fyralis_region"] = region
            records.append(rec)
            t = _event_time_ms(event)
            if t is not None:
                if cur.high_water_time_ms is None or t > cur.high_water_time_ms:
                    cur.high_water_time_ms = t

        cur.events_seen += len(events)
        # Advance the opaque token. End-of-data when the page returns no token.
        cur.events_cursor = next_cursor if isinstance(next_cursor, str) and next_cursor else None
        is_last = cur.events_cursor is None

        log.info(
            "aws_backfill_page",
            extra={
                "events": len(events),
                "is_last": is_last,
                "high_water_time_ms": cur.high_water_time_ms,
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
    "SHARD_KIND_ACCOUNT_EVENTS",
    "AwsCursor",
    "fetch_page_aws",
]
