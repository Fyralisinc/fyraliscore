"""services/ingest/ingestion/handlers/google_calendar.py — Calendar event handler (IN-15).

ONE channel `google_calendar:event` (decision D3). The handler branches on
the event's `status` to set `kind`: a `cancelled` event is a `state_change`
(a scheduled meeting was dropped/rescheduled); everything else is a `signal`.
`eventType` (default / outOfOffice / focusTime / workingLocation) is preserved
in `content` for downstream capacity reasoning.

Records arrive shaped by the backfill/poll fetcher: the RAW Calendar v3 event
object plus two injected private keys — `_fyralis_calendar_id` and
`_fyralis_owner_email` — so the handler can derive a stable external_id and
attribute the event without a separate lookup. The handler is a pure function
(no DB / network), like every other handler.

Trust posture (D4): a calendar event is the system of record for scheduling,
matching the pre-existing `calendar:sync` channel — `authoritative`. (It
records *intended* attendance, not verified attendance; that nuance is a
downstream Think concern.)

external_id — VERSIONED (Risk #3 + mutation semantics): the observations repo
dedups on `(source_channel, external_id)` IGNORING occurred_at, i.e. one stable
observation per external_id. That fits immutable sources (a sent email, a
merged PR) but calendar events MUTATE — they get cancelled, rescheduled, and
re-RSVP'd. So the external_id encodes the event VERSION:

    gcal:{calendar_id}:{event_id}:{status}:{start_instant}

This means:
  - identical re-fetches (backfill twin == incremental poll twin) collapse to
    one observation (same status + start);
  - a cancellation (status confirmed -> cancelled) lands as a NEW observation
    with kind=state_change — the deprioritization signal is preserved;
  - a reschedule (start changes) lands as a NEW signal so the temporal signal
    stays current;
  - RSVP-only churn (attendee responseStatus flips; status + start unchanged)
    dedups, so attendee ticks don't spam observations.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)


_CHANNEL = "google_calendar:event"
_TRUST = "authoritative"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(dt: Any) -> datetime | None:
    if not isinstance(dt, str) or not dt:
        return None
    s = dt[:-1] + "+00:00" if dt.endswith("Z") else dt
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_event_time(node: Any) -> datetime | None:
    """A Calendar `start`/`end` node is either {"dateTime": ...} (timed) or
    {"date": "YYYY-MM-DD"} (all-day). Returns the UTC datetime, or None."""
    if not isinstance(node, dict):
        return None
    dt = _parse_iso(node.get("dateTime"))
    if dt is not None:
        return dt.astimezone(timezone.utc)
    date = node.get("date")
    if isinstance(date, str) and date:
        parsed = _parse_iso(f"{date}T00:00:00+00:00")
        if parsed is not None:
            return parsed
    return None


def _occurred_at(event: dict[str, Any]) -> datetime:
    """When the event happens. Prefer the start; for a cancelled event in a
    sync delta (no `start`), fall back to originalStartTime, then `updated`,
    then `created`, then now (Risk #5)."""
    for candidate in (
        _parse_event_time(event.get("start")),
        _parse_event_time(event.get("originalStartTime")),
        _parse_iso(event.get("updated")),
        _parse_iso(event.get("created")),
    ):
        if candidate is not None:
            return candidate
    return _utcnow()


def _email(node: Any) -> str | None:
    if isinstance(node, dict):
        e = node.get("email")
        if isinstance(e, str) and e:
            return e.lower()
    return None


def _is_external(email: str, owner_email: str | None) -> bool:
    """An attendee is external if its domain differs from the calendar
    owner's domain."""
    if not owner_email or "@" not in email or "@" not in owner_email:
        return False
    return email.rsplit("@", 1)[-1] != owner_email.rsplit("@", 1)[-1]


def _format_time(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%d %H:%M UTC") if dt else "unknown time"


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_google_calendar_event(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """Calendar v3 event object -> ObservationDraft."""
    if not isinstance(payload, dict):
        raise ValidationError(
            "google calendar payload must be a JSON object", channel=_CHANNEL,
        )
    event_id = payload.get("id")
    if not isinstance(event_id, str) or not event_id:
        raise ValidationError(
            "google calendar event missing id", channel=_CHANNEL,
        )

    calendar_id = payload.get("_fyralis_calendar_id")
    owner_email = payload.get("_fyralis_owner_email")
    if not isinstance(calendar_id, str) or not calendar_id:
        # Fall back to the owner email when the fetcher didn't stamp it
        # (e.g. a direct unit-test payload).
        calendar_id = owner_email if isinstance(owner_email, str) else "primary"

    status = payload.get("status") or "confirmed"
    kind = "state_change" if status == "cancelled" else "signal"

    summary = payload.get("summary") or "(no title)"
    organizer_email = _email(payload.get("organizer")) or _email(payload.get("creator"))
    start_dt = _parse_event_time(payload.get("start"))
    end_dt = _parse_event_time(payload.get("end"))

    # Version discriminator (see module docstring): status + start instant so a
    # cancellation / reschedule is a distinct observation while identical
    # re-fetches and RSVP-only churn dedup.
    start_key = start_dt.isoformat() if start_dt else "none"
    external_id = f"gcal:{calendar_id}:{event_id}:{status}:{start_key}"

    attendees = payload.get("attendees")
    attendee_emails: list[str] = []
    if isinstance(attendees, list):
        for a in attendees:
            em = _email(a)
            if em:
                attendee_emails.append(em)

    # content_text — human-legible synthesis (embedded + shown in UI).
    if status == "cancelled":
        content_text = f"Calendar event '{summary}' was cancelled"
        if start_dt:
            content_text += f" (was {_format_time(start_dt)})"
    else:
        who = organizer_email or "someone"
        content_text = f"{who} scheduled '{summary}' at {_format_time(start_dt)}"
        if attendee_emails:
            shown = ", ".join(attendee_emails[:5])
            more = f" +{len(attendee_emails) - 5} more" if len(attendee_emails) > 5 else ""
            content_text += f" with {len(attendee_emails)} attendee(s): {shown}{more}"

    # entities_hint — emails (for actor/entity resolution) + the topic.
    entities: list[dict[str, Any]] = []
    owner_dom = owner_email if isinstance(owner_email, str) else None
    if organizer_email:
        entities.append({
            "type": "email_address",
            "id": organizer_email,
            "role": "organizer",
            "external": _is_external(organizer_email, owner_dom),
        })
    for em in attendee_emails:
        if em == organizer_email:
            continue  # already emitted as the organizer (avoid a dup hint)
        entities.append({
            "type": "email_address",
            "id": em,
            "role": "attendee",
            "external": _is_external(em, owner_dom),
        })
    if isinstance(summary, str) and summary and summary != "(no title)":
        entities.append({"type": "meeting_topic", "id": summary})

    content: dict[str, Any] = {
        "object_type": "event",
        "event_id": event_id,
        "calendar_id": calendar_id,
        "status": status,
        "event_type": payload.get("eventType", "default"),
        "summary": summary,
        "description": payload.get("description"),
        "location": payload.get("location"),
        "start": payload.get("start"),
        "end": payload.get("end"),
        "start_at": start_dt.isoformat() if start_dt else None,
        "end_at": end_dt.isoformat() if end_dt else None,
        "duration_minutes": (
            int((end_dt - start_dt).total_seconds() // 60)
            if start_dt and end_dt and end_dt > start_dt else None
        ),
        "organizer_email": organizer_email,
        "attendee_emails": attendee_emails,
        "attendee_count": len(attendee_emails),
        "recurring_event_id": payload.get("recurringEventId"),
        "is_recurring_instance": bool(payload.get("recurringEventId")),
        "hangout_link": payload.get("hangoutLink"),
        "html_link": payload.get("htmlLink"),
        "owner_email": owner_email if isinstance(owner_email, str) else None,
    }

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content=content,
        occurred_at=_occurred_at(payload),
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=(f"email:{organizer_email}" if organizer_email else None),
        external_id=external_id,
        entities_hint=entities,
        raw_payload=payload,
    )


# Single channel (D3). Register the trust default so any code that looks up
# source_channel -> trust finds it (the handler also sets it per draft).
CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_google_calendar_event"]
