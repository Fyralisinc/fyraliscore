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
_SNAPSHOT_CHANNEL = "figma:file_snapshot"
_TRUST = "authoritative"


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
    event: dict[str, Any], scope_id: str, team_id: str, file_key: str,
    event_type: str,
) -> ObservationDraft:
    """Build the observation draft.

    `scope_id` is the value that namespaces the external_id — the Figma
    `webhook_id` for a REAL live delivery (R2), or the `team_id` for a
    backfill/legacy record. It MUST be non-empty (the global UNIQUE has no
    tenant_id, so an un-namespaced key would let two tenants collide).

    external_id discriminator:
      - event carries an id (backfill / legacy synthetic webhook) ->
        `figma:{scope}:event:{event_id}:{version}` (VERSIONED, unchanged).
      - REAL Figma V2 webhook (no stable event id) -> `(file_key, timestamp)`
        is the only durable discriminator the delivery offers, so
        `figma:{scope}:event:{file_key}:{timestamp}`.
    """
    if not scope_id:
        raise ValidationError(
            "figma event missing scope id (webhook_id/team_id)",
            channel=_CHANNEL,
        )
    event_id = str(event.get("id") or event.get("event_id") or "")
    if event_id:
        version = _event_version(event)
        external_id = idempotency.figma_event(scope_id, event_id, version)
    else:
        # Real Figma Webhooks V2 carry NO event id — the durable discriminator
        # is (file_key, timestamp). `idempotency.figma_event` is the same
        # f-string, so this stays one shape: figma:{scope}:event:{file_key}:{ts}.
        ts = str(
            event.get("timestamp")
            or event.get("createdAt")
            or event.get("created_at")
            or ""
        )
        if not file_key or not ts:
            raise ValidationError(
                "figma webhook missing file_key/timestamp", channel=_CHANNEL,
            )
        version = ts
        external_id = idempotency.figma_event(scope_id, file_key, ts)

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

    webhook_id = str(event.get("webhook_id") or "")
    entities: list[dict[str, Any]] = [
        {"type": "figma_file", "id": file_key},
    ]
    if team_id:
        entities.append({"type": "figma_team", "id": team_id})
    if webhook_id:
        entities.append({"type": "figma_webhook", "id": webhook_id})
    if isinstance(actor, str) and actor:
        entities.append({"type": "person", "id": actor, "role": "actor"})

    content: dict[str, Any] = {
        "object_type": "event",
        "event_type": event_type or event.get("event_type"),
        "event_id": event_id or None,
        "team_id": team_id or None,
        "webhook_id": webhook_id or None,
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


def _scope_id_of(payload: dict[str, Any], event: dict[str, Any] | None) -> str:
    """The value that namespaces the external_id (R2).

    REAL Figma Webhooks V2 carry a Figma-assigned `webhook_id` and NO `team_id`
    in the body, so the webhook_id is the durable install scope. Prefer it
    (live `webhook_id` / backfill `_fyralis_webhook_id`); fall back to the
    team id for legacy synthetic webhooks and backfill records that predate
    the webhook_id model. Mirrors the install key:
    `provider_installations(provider='figma', installation_id=<webhook_id>)`.
    """
    for src in (payload, event if isinstance(event, dict) else {}):
        for key in ("webhook_id", "_fyralis_webhook_id", "webhookId"):
            v = src.get(key)
            if isinstance(v, str) and v:
                return v
    # Legacy fallback: team id (the pre-R2 namespace).
    return _team_id_of(payload, event)


def _snapshot_draft(payload: dict[str, Any]) -> ObservationDraft:
    """Build the durable file-design observation from a snapshot record.

    The fetcher has already written the complete Figma response to S3.  This
    handler deliberately copies only ``StoredArtifact.public_ref()`` into
    content; the bucket/key remain in ``artifact_descriptors`` until core
    writes the private catalog/link rows in the observation transaction.
    """
    from services.ingest.ingestion.artifacts import (
        ArtifactDescriptorError,
        StoredArtifact,
    )

    file_key = _file_key_of(payload, None)
    if not file_key:
        raise ValidationError("figma snapshot missing file_key", channel=_SNAPSHOT_CHANNEL)
    snapshot = payload.get("snapshot")
    if not isinstance(snapshot, dict):
        raise ValidationError("figma snapshot record malformed", channel=_SNAPSHOT_CHANNEL)
    raw_artifact = payload.get("artifact")
    if not isinstance(raw_artifact, dict):
        raise ValidationError("figma snapshot missing artifact", channel=_SNAPSHOT_CHANNEL)
    try:
        artifact = StoredArtifact.from_private_descriptor(raw_artifact)
    except ArtifactDescriptorError as exc:
        raise ValidationError("figma snapshot artifact invalid", channel=_SNAPSHOT_CHANNEL) from exc

    version_value = snapshot.get("version")
    version = str(version_value) if version_value is not None and str(version_value) else artifact.content_hash
    team_id = _team_id_of(payload, None)
    installation_id_raw = payload.get("_fyralis_installation_id") or payload.get("installation_id")
    installation_id = (
        str(installation_id_raw)
        if installation_id_raw is not None and str(installation_id_raw)
        else ""
    )
    scope_id = installation_id or team_id
    if not scope_id:
        raise ValidationError(
            "figma snapshot missing installation/team scope", channel=_SNAPSHOT_CHANNEL,
        )

    file_data = payload.get("file")
    file_data = file_data if isinstance(file_data, dict) else {}
    file_name = file_data.get("name")
    if not isinstance(file_name, str) or not file_name:
        file_name = file_key
    project_name = file_data.get("project_name")
    if not isinstance(project_name, str) or not project_name:
        project_name = None
    projection_raw = snapshot.get("projection")
    projection_raw = projection_raw if isinstance(projection_raw, dict) else {}
    pages_raw = projection_raw.get("page_names")
    page_names = [
        page[:300] for page in pages_raw[:64]
        if isinstance(page, str) and page
    ] if isinstance(pages_raw, list) else []
    node_count_raw = projection_raw.get("node_count")
    node_count = node_count_raw if isinstance(node_count_raw, int) and node_count_raw >= 0 else 0
    text_preview_raw = projection_raw.get("text_preview")
    text_preview = (
        _truncate(text_preview_raw, 4_000)
        if isinstance(text_preview_raw, str)
        else ""
    )
    last_modified = snapshot.get("last_modified")
    captured_at = snapshot.get("captured_at")
    occurred = (
        _parse_iso(last_modified)
        or _parse_iso(captured_at)
        or _utcnow()
    )
    source_locator: dict[str, Any] = {
        "file_key": file_key,
        "version": version,
    }
    if installation_id:
        source_locator["installation_id"] = installation_id

    content: dict[str, Any] = {
        "object_type": "figma_file_snapshot",
        "file_key": file_key,
        "file_name": file_name,
        "team_id": team_id or None,
        "figma_version": version,
        "last_modified": last_modified,
        "source_locator": source_locator,
        "artifacts": [artifact.public_ref()],
        "projection": {
            "page_names": page_names,
            "node_count": node_count,
            "text_preview": text_preview,
        },
    }
    if project_name is not None:
        content["project_name"] = project_name
    content = {k: v for k, v in content.items() if v is not None}

    title = f"Figma design snapshot: {file_name} · version {version}"
    if page_names:
        title = f"{title} · pages: {', '.join(page_names[:8])}"
    if text_preview:
        title = f"{title}\n{text_preview}"
    entities: list[dict[str, Any]] = [{"type": "figma_file", "id": file_key}]
    if team_id:
        entities.append({"type": "figma_team", "id": team_id})
    if installation_id:
        entities.append({"type": "figma_installation", "id": installation_id})

    return ObservationDraft(
        source_channel=_SNAPSHOT_CHANNEL,
        content_text=_truncate(title, 4_500),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="state_change",
        external_id=idempotency.figma_file_snapshot(scope_id, file_key, version),
        entities_hint=entities,
        # Never retain the private descriptor as a raw payload.  The separate
        # descriptor field is transported by NormalizedEnvelope and is not
        # stored in observations.content.
        raw_payload=None,
        artifact_descriptors=[artifact.private_descriptor()],
    )


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_SNAPSHOT_CHANNEL)
@register(_CHANNEL)
async def handle_figma_event(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("figma payload must be a JSON object", channel=_CHANNEL)

    if payload.get("_fyralis_record_type") == "file_snapshot":
        return _snapshot_draft(payload)

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
            _scope_id_of(payload, event),
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
            _scope_id_of(payload, event),
            _team_id_of(payload, event),
            _file_key_of(payload, event),
            et,
        )

    raise ValidationError(
        "figma payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)
CHANNEL_TRUST_MAP.setdefault(_SNAPSHOT_CHANNEL, _TRUST)


__all__ = ["handle_figma_event"]
