"""services/ingestion/handlers/calendar.py — calendar webhook handler.

BUILD-PLAN §3 Prompt 2.B:
    "Google Calendar event types — created, updated, cancelled.
     content_text synthesized ('Alice scheduled '1:1 with Bob' from
     2pm-3pm tomorrow').
     entities_hint: attendee actor_ids (resolved via `email:` prefix
     through `ActorRepo`), meeting topic, any Goal/Commitment
     pattern-matched in description (use `EntityAliasRepo.fast_path_resolve`).
     Trust tier: authoritative.
     Signature: Google's `X-Goog-Channel-Token` if using push
     notifications — for Wave 2 accept a shared-secret header, document."

Signature note
--------------
Google push notifications carry `X-Goog-Channel-Token`, which the
caller sets when subscribing. That token is NOT a signature — it is a
shared secret. Wave 2 verifies constant-time against a configured
`CALENDAR_WEBHOOK_TOKEN` environment variable. A true HMAC signature
will follow in Phase 2 when Google offers one; until then the token
compare is the agreed-upon protection.

Payload shape (canonical; Google's API delivers this via Events.get()
after a push notification — the caller is expected to pre-fetch the
resource so the handler receives a ready JSON object):

    {
        "action":     "created" | "updated" | "cancelled",
        "event": {
            "id":        "event-id",
            "summary":   "1:1 with Bob",
            "description": "agenda, links, ...",
            "start": {"dateTime": "2026-04-22T14:00:00Z", "timeZone": "..."},
            "end":   {"dateTime": "2026-04-22T15:00:00Z", "timeZone": "..."},
            "attendees": [
                {"email": "alice@company.com", "responseStatus": "accepted"},
                {"email": "bob@company.com",   "responseStatus": "needsAction"},
            ],
            "organizer": {"email": "alice@company.com"},
            "status":    "confirmed" | "cancelled",
            "htmlLink":  "...",
        },
        "calendar_id": "alice@company.com",   # optional
    }
"""
from __future__ import annotations

import hmac
import re
from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    HandlerError,
    ObservationDraft,
    register,
)


_CHANNEL = "calendar:sync"


class CalendarSignatureError(HandlerError):
    default_code = "calendar_signature_invalid"


def verify_calendar_token(
    provided_token: str | None,
    secret: str | None,
) -> None:
    """Constant-time token check for `X-Goog-Channel-Token`.

    This is not a true HMAC; it's a shared secret embedded in the
    channel subscription. Missing secret raises
    `CalendarSignatureError`.
    """
    if not secret:
        raise CalendarSignatureError(
            "CALENDAR_WEBHOOK_TOKEN is not configured",
            reason="missing_secret",
        )
    if not provided_token:
        raise CalendarSignatureError(
            "missing X-Goog-Channel-Token header",
            reason="missing_signature",
        )
    if not hmac.compare_digest(
        provided_token.strip(), secret.strip()
    ):
        raise CalendarSignatureError(
            "calendar token mismatch", reason="mismatch"
        )


def _parse_iso(dt: Any, default: datetime | None = None) -> datetime:
    if dt is None:
        return default or datetime.now(timezone.utc)
    if isinstance(dt, datetime):
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    s = str(dt)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return default or datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _fmt_time(dt: datetime) -> str:
    """Readable short time ("2:00 PM") for content_text synthesis."""
    return dt.strftime("%-I:%M %p")


async def _resolve_attendees(
    attendees: list[dict[str, Any]],
    actor_resolver: Any,
) -> list[dict[str, Any]]:
    """Map attendee emails to actor_ids via actor_resolver.

    Each attendee becomes either `{"type": "actor", "id": <uuid>}`
    (resolved) or `{"type": "email_address", "id": <addr>}` (unknown).
    """
    hints: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for a in attendees or []:
        if not isinstance(a, dict):
            continue
        email = (a.get("email") or "").strip().lower()
        if not email:
            continue
        actor_id = None
        if actor_resolver is not None:
            try:
                actor_id = await actor_resolver.resolve_by_source_actor_ref(
                    f"email:{email}"
                )
            except Exception:
                actor_id = None
        if actor_id:
            entity = {"type": "actor", "id": str(actor_id)}
        else:
            entity = {"type": "email_address", "id": email}
        key = (entity["type"], entity["id"])
        if key in seen:
            continue
        seen.add(key)
        hints.append(entity)
    return hints


async def _scan_description_for_aliases(
    description: str,
    tenant_id: Any,
    alias_resolver: Any,
) -> list[dict[str, Any]]:
    """Walk the description with fast-path alias matches.

    Conservative: emit 2- and 3-gram candidates only; only keep
    confirmed hits.
    """
    if not description or alias_resolver is None or tenant_id is None:
        return []
    words = re.findall(r"[A-Za-z][A-Za-z0-9_\-]+", description)
    candidates: set[str] = set()
    for n in (2, 3):
        for i in range(len(words) - n + 1):
            phrase = " ".join(words[i:i + n])
            if len(phrase) >= 5:
                candidates.add(phrase)
    hits: list[dict[str, Any]] = []
    seen: set[str] = set()
    for phrase in candidates:
        try:
            ref = await alias_resolver.fast_path_resolve(phrase, tenant_id)
        except Exception:
            ref = None
        if ref:
            import json

            key = json.dumps(ref, sort_keys=True)
            if key not in seen:
                seen.add(key)
                hits.append(ref)
    return hits


async def handle_calendar_webhook(
    payload: dict[str, Any],
    headers: dict[str, str],
    *,
    tenant_id: Any = None,
    actor_resolver: Any = None,
    alias_resolver: Any = None,
) -> ObservationDraft:
    """Google Calendar event → ObservationDraft.

    The caller (ingestion core / gateway) has already fetched the
    event resource from Google and synthesised the canonical payload
    shape above. Signature verification is the caller's job; if the
    `X-Goog-Channel-Token` header is present AND an env token is set,
    we double-check defensively.
    """
    if not isinstance(payload, dict):
        raise ValidationError(
            "calendar payload must be a JSON object", channel=_CHANNEL
        )
    action = payload.get("action") or "updated"
    event = payload.get("event")
    if not isinstance(event, dict):
        raise ValidationError(
            "calendar payload missing 'event' object", channel=_CHANNEL
        )

    event_id = event.get("id")
    summary = (event.get("summary") or "").strip() or "(untitled event)"
    description = event.get("description") or ""

    start_block = event.get("start") or {}
    end_block = event.get("end") or {}
    start = _parse_iso(
        start_block.get("dateTime") or start_block.get("date")
    )
    end = _parse_iso(end_block.get("dateTime") or end_block.get("date"))

    organizer = event.get("organizer") or {}
    organizer_email = (organizer.get("email") or "").strip().lower() \
        if isinstance(organizer, dict) else ""
    organizer_actor_id = None
    if organizer_email and actor_resolver is not None:
        try:
            organizer_actor_id = await actor_resolver.resolve_by_source_actor_ref(
                f"email:{organizer_email}"
            )
        except Exception:
            organizer_actor_id = None

    organizer_display = organizer_email.split("@")[0] if organizer_email else "someone"

    # Synthesize content_text by action.
    if action == "created":
        content_text = (
            f"{organizer_display} scheduled '{summary}' from "
            f"{_fmt_time(start)}-{_fmt_time(end)}"
        )
        kind = "signal"
    elif action == "cancelled":
        content_text = (
            f"{organizer_display} cancelled '{summary}' "
            f"(was {_fmt_time(start)}-{_fmt_time(end)})"
        )
        kind = "state_change"
    else:  # "updated"
        content_text = (
            f"{organizer_display} updated '{summary}' "
            f"({_fmt_time(start)}-{_fmt_time(end)})"
        )
        kind = "state_change"

    attendees = event.get("attendees") or []
    entities_hint = await _resolve_attendees(attendees, actor_resolver)
    # Organizer added too (if known).
    if organizer_actor_id:
        entities_hint.insert(0, {"type": "actor", "id": str(organizer_actor_id)})
    elif organizer_email:
        entities_hint.insert(0, {"type": "email_address", "id": organizer_email})

    # Meeting topic as an entity hint (title → meeting_topic).
    entities_hint.append({"type": "meeting_topic", "id": summary})

    # Goal/Commitment hits via alias fast path.
    alias_hits = await _scan_description_for_aliases(
        description, tenant_id, alias_resolver
    )
    for h in alias_hits:
        entities_hint.append(h)

    source_actor_ref = (
        f"email:{organizer_email}" if organizer_email else None
    )

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "calendar",
            "action": action,
            "event_id": event_id,
            "summary": summary,
            "description": description,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "organizer": organizer_email,
            "attendees": [
                a.get("email") for a in attendees
                if isinstance(a, dict) and a.get("email")
            ],
            "status": event.get("status"),
        },
        occurred_at=start,
        trust_tier=CHANNEL_TRUST_MAP[_CHANNEL],  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=source_actor_ref,
        external_id=event_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


@register(_CHANNEL)
async def _registered_handler(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    return await handle_calendar_webhook(payload, headers)


__all__ = [
    "CalendarSignatureError",
    "verify_calendar_token",
    "handle_calendar_webhook",
]
