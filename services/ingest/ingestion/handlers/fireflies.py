"""services/ingest/ingestion/handlers/fireflies.py — Fireflies transcript handler.

ONE channel `fireflies:transcript` (mirrors github:webhook / jira:issue's
one-channel/many-record-types shape, though Fireflies has a single record type).
The handler is a pure function (no DB / network) and branches on the input shape
to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    == "transcript" (set by the fetcher's per-workspace fan-out).
  - LIVE WEBHOOK: the raw Fireflies webhook body carries a `type`
    (e.g. "transcript.completed" / "transcription_complete"); the handler maps
    it onto the same record builder so a webhook-delivered transcript and its
    backfill twin dedup.

Signal mapping (the reasoning value):
  - transcript -> kind="signal" (a meeting happened; its content is the signal).

external_id — VERSIONED by a content `version` so a re-processed transcript (a
richer summary / corrected transcript lands later) re-observes rather than
silently dedups (the observations repo dedups on (source_channel, external_id)
IGNORING occurred_at):
  - transcript: fireflies:{workspace_id}:transcript:{transcript_id}:{version}

Trust posture: Fireflies is an AI notetaker — an *attesting agent* transcribing
what humans said in a meeting, NOT the system of record -> `attested_agent`
(same tier as slack:message / gmail: / discord:message).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion import idempotency
from services.ingest.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)


_CHANNEL = "fireflies:transcript"
_TRUST = "attested_agent"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    elif len(s) >= 5 and s[-5] in "+-" and s[-3] != ":":
        s = s[:-2] + ":" + s[-2:]
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _transcript_version(t: dict[str, Any]) -> str:
    """The content version that the external_id is keyed on.

    Prefer an explicit version/updated_at so a re-processed transcript (a richer
    summary lands later) re-observes; fall back to a stable token so a transcript
    with no version still produces a deterministic external_id.
    """
    for key in ("version", "updatedAt", "updated_at", "processedAt", "dateTime", "date"):
        val = t.get(key)
        if isinstance(val, (str, int)) and str(val):
            return str(val)
    return "v1"


def _participants(t: dict[str, Any]) -> list[str]:
    raw = (
        t.get("participants")
        or t.get("attendees")
        or t.get("speakers")
        or []
    )
    out: list[str] = []
    if isinstance(raw, list):
        for p in raw:
            if isinstance(p, str) and p:
                out.append(p)
            elif isinstance(p, dict):
                name = p.get("name") or p.get("displayName") or p.get("email")
                if isinstance(name, str) and name:
                    out.append(name)
    return out


def _transcript_extras(t: dict[str, Any]) -> dict[str, Any]:
    """The richer Fireflies transcript fields beyond the core meeting signal.

    Only present (non-None) keys are returned so `content` stays lean.
    """
    summary = t.get("summary")
    summary_text = None
    action_items = None
    if isinstance(summary, dict):
        summary_text = (
            summary.get("overview")
            or summary.get("shorthand_bullet")
            or summary.get("gist")
        )
        action_items = summary.get("action_items") or summary.get("actionItems")
    elif isinstance(summary, str):
        summary_text = summary

    raw: dict[str, Any] = {
        "summary": summary_text,
        "action_items": action_items if isinstance(action_items, (list, str)) else None,
        "duration_minutes": t.get("duration") or t.get("durationMinutes"),
        "meeting_url": t.get("meetingLink") or t.get("transcript_url") or t.get("audio_url"),
        "organizer_email": t.get("organizerEmail") or t.get("host_email"),
        "calendar_id": t.get("calendarId") or t.get("calendar_id"),
        "fireflies_user_id": t.get("userId") or t.get("user_id"),
    }
    return {k: v for k, v in raw.items() if v is not None}


def _transcript_draft(
    transcript: dict[str, Any], workspace_id: str,
) -> ObservationDraft:
    transcript_id = str(
        transcript.get("id")
        or transcript.get("transcript_id")
        or transcript.get("transcriptId")
        or ""
    )
    if not workspace_id or not transcript_id:
        raise ValidationError(
            "fireflies transcript missing workspace_id/id", channel=_CHANNEL,
        )
    version = _transcript_version(transcript)
    external_id = idempotency.fireflies_transcript(
        workspace_id, transcript_id, version,
    )

    title = (
        transcript.get("title")
        or transcript.get("meetingTitle")
        or transcript.get("name")
        or "Untitled meeting"
    )
    occurred = (
        _parse_iso(transcript.get("dateTime"))
        or _parse_iso(transcript.get("date"))
        or _parse_iso(transcript.get("createdAt"))
        or _utcnow()
    )
    participants = _participants(transcript)

    content_text = title
    if participants:
        who = ", ".join(participants[:5])
        content_text = f"{title} · {who}"

    entities: list[dict[str, Any]] = [
        {"type": "fireflies_workspace", "id": workspace_id},
        {"type": "meeting", "id": transcript_id},
    ]
    for name in participants:
        entities.append({"type": "person", "id": name, "role": "participant"})

    content: dict[str, Any] = {
        "object_type": "transcript",
        "workspace_id": workspace_id,
        "transcript_id": transcript_id,
        "title": title,
        "participants": participants,
        "date": transcript.get("dateTime") or transcript.get("date"),
        "version": version,
    }
    # Merge the richer fields (summary, action items, duration, links) — additive,
    # only present keys.
    content.update(_transcript_extras(transcript))

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=transcript,
    )


def _workspace_id_of(payload: dict[str, Any], obj: dict[str, Any] | None) -> str:
    """Resolve the workspace id for a webhook/backfill record."""
    wid = (
        payload.get("_fyralis_workspace_id")
        or payload.get("workspaceId")
        or payload.get("workspace_id")
    )
    if isinstance(wid, str) and wid:
        return wid
    if isinstance(obj, dict):
        cand = obj.get("workspaceId") or obj.get("workspace_id")
        if isinstance(cand, str) and cand:
            return cand
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_fireflies_transcript(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "fireflies payload must be a JSON object", channel=_CHANNEL,
        )

    # --- LIVE WEBHOOK path (raw Fireflies webhook body) ---
    event_type = payload.get("type") or payload.get("eventType")
    if isinstance(event_type, str) and event_type:
        if event_type.startswith("transcript") or "transcription" in event_type:
            transcript = (
                payload.get("transcript")
                or payload.get("data")
                or payload.get("meeting")
                or {}
            )
            if isinstance(transcript, dict) and (
                transcript.get("id")
                or transcript.get("transcript_id")
                or transcript.get("transcriptId")
            ):
                return _transcript_draft(
                    transcript, _workspace_id_of(payload, transcript),
                )
            raise ValidationError(
                f"fireflies {event_type} missing transcript", channel=_CHANNEL,
            )
        raise ValidationError(
            f"unsupported fireflies webhook type {event_type!r}", channel=_CHANNEL,
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if record_type == "transcript" or "transcript" in payload:
        transcript = payload.get("transcript") or {}
        return _transcript_draft(
            transcript, _workspace_id_of(payload, transcript),
        )

    raise ValidationError(
        "fireflies payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_fireflies_transcript"]
