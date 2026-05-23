"""services/ingestion/planners/google_calendar.py — Calendar planner (IN-15).

Per ingestion LLD §3 + the Gmail planner precedent (A18.2 loader pattern):
Google Calendar's install record is workspace-scoped, but the planner needs
the 1-to-N active-calendar list to emit one shard per calendar. The
enrichment lives in `google_calendar_calendars`; the SourceOnboarding loader
JSON-aggregates it into `ctx.install["calendars"]` so the planner stays
stateless (no DB I/O), exactly like Gmail's mailbox aggregation.

Each shard is one calendar's event stream (D6 — a user's primary calendar,
addressed by their email). The fetcher walks events.list windowed by timeMin
on first run, then incrementally via syncToken.
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_EVENTS = "google_calendar_events"


def _decode_calendars(install: Any) -> list[dict[str, Any]]:
    """Decode the JSON-aggregated `calendars` column on the install record."""
    raw = install["calendars"] if "calendars" in install else None
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        try:
            decoded = orjson.loads(raw)
        except orjson.JSONDecodeError:
            return []
    elif isinstance(raw, list):
        decoded = raw
    else:
        return []
    return [c for c in decoded if isinstance(c, dict)]


async def plan_shards_google_calendar(ctx: PlannerContext) -> list[Shard]:
    """One `google_calendar_events` shard per active calendar.

    Reads DB state only (calendars pre-aggregated by the loader), so
    `ctx.source_client` is None — same as Gmail.
    """
    install_id = str(ctx.install["id"])
    calendars = _decode_calendars(ctx.install)

    shards: list[Shard] = []
    for cal in calendars:
        calendar_id = cal.get("calendar_id")
        if not isinstance(calendar_id, str) or not calendar_id:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_EVENTS,
            shard_identifier={
                "shard_kind": SHARD_KIND_EVENTS,
                "calendar_id": calendar_id,
                "owner_email": cal.get("owner_email") or calendar_id,
                "installation_id": install_id,
                "sync_token": cal.get("sync_token"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.google_calendar.planned",
        extra={"calendar_shards": len(shards), "installation_id": install_id},
    )
    return shards


PLANNER_DISPATCH["google_calendar"] = plan_shards_google_calendar


__all__ = ["SHARD_KIND_EVENTS", "plan_shards_google_calendar"]
