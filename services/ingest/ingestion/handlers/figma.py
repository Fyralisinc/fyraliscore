"""services/ingest/ingestion/handlers/figma.py — Figma file-event handler.

ONE channel `figma:event` (mirrors brex:transaction / jira:issue's
one-channel/many-record-types shape). The handler is a pure function (no DB /
network) and branches on the input shape to produce exactly ONE observation per
call:

  - BACKFILL / POLL: records arrive tagged with a private
    `_fyralis_record_type="event"` (set by the fetcher's per-file fan-out), plus
    `_fyralis_team_id` (the namespacing scope) and `_fyralis_file_key`.
  - LIVE WEBHOOK: the raw Figma webhook body carries an `event_type`
    (e.g. "FILE_VERSION_UPDATE", "FILE_COMMENT", "LIBRARY_PUBLISH",
    "DEV_MODE_STATUS_UPDATE", "FILE_DELETE"); the handler maps it onto the same
    event builder so a webhook-delivered change and its backfill twin dedup.

Signal mapping (the reasoning value):
  - most events (version/comment/publish/file update) -> kind="signal"
  - FILE_DELETE and a dev-mode revert (ready_for_dev -> in-progress) ->
    kind="state_change" (a design lifecycle reversal worth surfacing)

external_id — VERSIONED for the MUTABLE entities (the observations repo dedups
on (source_channel, external_id) IGNORING occurred_at; a re-publish must land as
a NEW observation, not silently dedup):
  - event: figma:{team_id}:event:{event_id}:{version}

The key is namespaced by `team_id` (Figma-global) so the same synthetic event id
seen by two tenants stays distinct — mandatory under the global
UNIQUE(source_channel, external_id, occurred_at) with no tenant_id.

Trust posture: Figma is the first-party system of record for our own design
data -> `authoritative` (mirrors jira/mercury/grafana/brex).
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


_CHANNEL = "figma:event"
_TRUST = "authoritative"


def _figma_event_external_id(team_id: str, event_id: str, version: str) -> str:
    """`figma:{team_id}:event:{event_id}:{version}` — namespaced by the Figma
    team id (Figma-global, so two tenants' identical synthetic event ids stay
    distinct under the global UNIQUE-without-tenant_id) and VERSIONED by version
    so a re-publish lands a NEW observation while identical re-fetches dedup.

    Single source of truth is `idempotency.figma_event` once the shared-file /
    wiring agent adds it (see the source summary `notes`); this inline fallback
    keeps the handler self-contained + testable until then and produces the
    IDENTICAL string, so swapping to the shared constructor is a no-op.
    """
    fn = getattr(idempotency, "figma_event", None)
    if callable(fn):
        return fn(team_id, event_id, version)
    return f"figma:{team_id}:event:{event_id}:{version}"

# Event types (or derived statuses) that represent a design-lifecycle reversal —
# a deletion or a ready-for-dev rollback. Everything else is a forward signal.
_STATE_CHANGE_EVENTS = frozenset({"FILE_DELETE"})
_DEV_REVERT_STATUSES = frozenset({"in_progress", "not_ready", "reverted"})


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


def _event_version(event: dict[str, Any]) -> str:
    """The mutation discriminator that versions the external_id.

    Prefers an explicit `version` (named-version id / library publish version),
    then the comment/event `updated`/`createdAt`, so a re-published or edited
    event lands a NEW observation. `none` when nothing distinguishes it (an
    immutable single-shot event collapses on re-fetch)."""
    for key in ("version", "version_id", "updated_at", "updated", "modified_at"):
        v = event.get(key)
        if v is not None and str(v):
            return str(v)
    created = event.get("createdAt") or event.get("created_at")
    return str(created) if created else "none"


def _is_state_change(event_type: str, event: dict[str, Any]) -> bool:
    if event_type in _STATE_CHANGE_EVENTS:
        return True
    if event_type == "DEV_MODE_STATUS_UPDATE":
        status = str(event.get("status") or event.get("dev_status") or "").lower()
        return status in _DEV_REVERT_STATUSES
    return False


# ---------------------------------------------------------------------
# Draft builder (shared by backfill + webhook paths)
# ---------------------------------------------------------------------

def _event_draft(
    event: dict[str, Any], team_id: str, file_key: str, event_type: str,
) -> ObservationDraft:
    event_id = str(event.get("id") or event.get("event_id") or "")
    if not team_id or not event_id:
        raise ValidationError(
            "figma event missing team_id/id", channel=_CHANNEL,
        )
    version = _event_version(event)
    external_id = _figma_event_external_id(team_id, event_id, version)

    occurred = (
        _parse_iso(event.get("createdAt"))
        or _parse_iso(event.get("created_at"))
        or _parse_iso(event.get("timestamp"))
        or _utcnow()
    )

    label = (
        event.get("label")
        or event.get("description")
        or event.get("message")
        or event.get("file_name")
        or file_key
        or "figma event"
    )
    actor = (
        (event.get("triggered_by") or {}).get("handle")
        if isinstance(event.get("triggered_by"), dict)
        else None
    ) or event.get("user") or event.get("author")

    is_state_change = _is_state_change(event_type, event)
    kind_word = event_type.replace("_", " ").lower() if event_type else "event"
    content_text = f"{kind_word}: {label}"
    if isinstance(actor, str) and actor:
        content_text = f"{content_text} · by {actor}"

    entities: list[dict[str, Any]] = [
        {"type": "figma_file", "id": file_key},
        {"type": "figma_team", "id": team_id},
    ]
    if isinstance(actor, str) and actor:
        entities.append({"type": "person", "id": actor, "role": "actor"})

    content: dict[str, Any] = {
        "object_type": "event",
        "event_type": event_type or event.get("event_type"),
        "event_id": event_id,
        "team_id": team_id,
        "file_key": file_key or event.get("file_key"),
        "version": version,
        "label": label,
        "message": event.get("message"),
        "actor": actor,
        "created_at": event.get("createdAt") or event.get("created_at"),
        "status": event.get("status") or event.get("dev_status"),
    }
    content = {k: v for k, v in content.items() if v is not None}

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="state_change" if is_state_change else "signal",
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=event,
    )


def _team_id_of(payload: dict[str, Any], event: dict[str, Any] | None) -> str:
    """Resolve the team id for a webhook/backfill record."""
    tid = payload.get("_fyralis_team_id") or payload.get("team_id") or payload.get("teamId")
    if isinstance(tid, str) and tid:
        return tid
    if isinstance(event, dict):
        cand = event.get("team_id") or event.get("teamId")
        if isinstance(cand, str) and cand:
            return cand
    return ""


def _file_key_of(payload: dict[str, Any], event: dict[str, Any] | None) -> str:
    fk = payload.get("_fyralis_file_key") or payload.get("file_key") or payload.get("fileKey")
    if isinstance(fk, str) and fk:
        return fk
    if isinstance(event, dict):
        cand = event.get("file_key") or event.get("fileKey")
        if isinstance(cand, str) and cand:
            return cand
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_figma_event(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("figma payload must be a JSON object", channel=_CHANNEL)

    # --- LIVE WEBHOOK path (raw Figma webhook body) ---
    # Figma Webhooks V2 carry the event type in `event_type` and the payload
    # fields inline on the body itself.
    event_type = payload.get("event_type") or payload.get("type")
    if isinstance(event_type, str) and event_type and event_type != "event":
        if event_type == "PING":
            raise ValidationError("figma PING is not an observation", channel=_CHANNEL)
        event = payload.get("event")
        if not isinstance(event, dict):
            # The webhook body IS the event (fields inline) — wrap it minus the
            # routing keys so the builder sees a flat event object.
            event = {
                k: v for k, v in payload.items()
                if k not in ("event_type", "type", "passcode",
                             "_fyralis_team_id", "_fyralis_file_key")
            }
        return _event_draft(
            event,
            _team_id_of(payload, event),
            _file_key_of(payload, event),
            event_type,
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if record_type == "event" or "event" in payload:
        event = payload.get("event") or {}
        if not isinstance(event, dict):
            raise ValidationError("figma event record malformed", channel=_CHANNEL)
        et = str(event.get("event_type") or event.get("type") or "FILE_UPDATE")
        return _event_draft(
            event,
            _team_id_of(payload, event),
            _file_key_of(payload, event),
            et,
        )

    raise ValidationError(
        "figma payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_figma_event"]
