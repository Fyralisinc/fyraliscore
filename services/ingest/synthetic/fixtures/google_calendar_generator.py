"""Deterministic Google Calendar fixtures for the X2/X3 harness.

`make_google_calendar(...)` builds a fixture the `MockGoogleCalendarClient`
serves and the X3 harness's install-seeding reads. Shape:

    {
      "calendars": ["alice@acme.example", "bob@acme.example"],   # one shard each
      "events":  {calendar_id: [<Calendar v3 event objects>]},  # backfill
      "delta":   {calendar_id: [<Calendar v3 event objects>]},  # incremental
      "page_size": 250,
    }

Events are raw Calendar v3 event objects (the same shape the real API
returns), so the REAL handler/fetcher code is exercised exactly. Same input
params → identical fixture (no randomness).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


_BASE = datetime(2026, 6, 1, 9, 0, 0, tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    # Real Google Calendar `updated` timestamps are RFC3339 WITH milliseconds
    # (e.g. "2026-06-01T09:01:00.000Z"). Emit millis so the reconciler's
    # exclusive-floor probe (`high_water + 1ms`, see reconcilers/google_calendar
    # `_exclusive_updated_floor`) compares correctly as strings. Without millis
    # the floor "...00.001Z" sorts BEFORE the event "...00Z" (because '.' < 'Z'),
    # so `has_updates_since` is spuriously True and the reconciler reshare loops
    # forever — the tenant never reaches completion.
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _event(
    *, calendar_id: str, idx: int, start: datetime, status: str = "confirmed",
) -> dict[str, Any]:
    eid = f"{calendar_id}-evt-{idx}"
    obj: dict[str, Any] = {
        "kind": "calendar#event",
        "id": eid,
        "status": status,
        "summary": f"Meeting {idx} ({calendar_id})",
        "eventType": "default",
        "organizer": {"email": calendar_id},
        "creator": {"email": calendar_id},
        "attendees": [
            {"email": calendar_id, "responseStatus": "accepted"},
            {"email": "guest@partner.example", "responseStatus": "tentative"},
        ],
        "htmlLink": f"https://calendar.google.com/event?eid={eid}",
        # `updated` strictly increases with idx so the high-water mark is
        # deterministic and the reconciler sees no gap on a clean run.
        "updated": _iso(_BASE + timedelta(minutes=idx)),
    }
    if status != "cancelled":
        obj["start"] = {"dateTime": _iso(start)}
        obj["end"] = {"dateTime": _iso(start + timedelta(minutes=30))}
    return obj


def make_google_calendar(
    *,
    calendars: list[str] | None = None,
    events_per_calendar: int = 3,
    page_size: int = 250,
) -> dict[str, Any]:
    """Build a backfill fixture: `events_per_calendar` confirmed events on
    each calendar. `delta` is left empty (clean backfill scenario)."""
    calendars = list(calendars or ["alice@acme.example", "bob@acme.example"])
    events: dict[str, list[dict[str, Any]]] = {}
    for cal in calendars:
        events[cal] = [
            _event(
                calendar_id=cal, idx=i,
                start=_BASE + timedelta(days=i + 1, hours=hash(cal) % 5),
            )
            for i in range(1, events_per_calendar + 1)
        ]
    return {
        "calendars": calendars,
        "events": events,
        "delta": {cal: [] for cal in calendars},
        "page_size": page_size,
    }


__all__ = ["make_google_calendar"]
