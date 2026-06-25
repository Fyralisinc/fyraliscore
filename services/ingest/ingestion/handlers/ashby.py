"""services/ingest/ingestion/handlers/ashby.py — Ashby recruiting-ATS handler.

ONE channel `ashby:object` (mirrors gusto:object's one-channel/many-record-types
shape). The handler is a pure function (no DB / network) and branches on the
input shape to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"candidate","application","job","interview","offer"} (set by the fetcher).
  - LIVE WEBHOOK: an Ashby webhook event — `{"action": "...", "data": {...}}`
    with the entity body under `data` (and `organizationId` carried for tenant
    resolution). The handler maps the entity onto the same record builder so a
    webhook-delivered change and its backfill twin dedup on the same external_id.
    When the webhook carries only an id (no full body) the handler emits a thin
    change observation keyed the same way.

Signal mapping (the reasoning value): recruiting objects move through pipeline
states. An application/offer reaching a terminal lifecycle state (hired / offer
accepted / rejected / withdrawn / archived) is a `state_change` (the
hiring-funnel signal); everything else (a new candidate, an interview scheduled,
an open application) is a `signal`.

external_id — per the CONTRACT, NOT version-suffixed:
  - ashby:{org}:{entity}:{id}
Ashby ids are stable per entity; the entity_kind discriminates so multi-entity
fixtures sharing an id never collide. Re-walks of an unchanged entity dedup on
(source_channel, external_id); a state change re-observes via occurred_at.

Trust posture: Ashby is the recruiting system of record -> `authoritative`.
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


_CHANNEL = "ashby:object"
_TRUST = "authoritative"

# Map an Ashby entity record_type (backfill) / webhook object kind to a canonical
# kind. Keys are lowercased; values are the canonical entity_kind.
_ENTITY_NORMALISE = {
    "candidate": "candidate",
    "application": "application",
    "job": "job",
    "interview": "interview",
    "offer": "offer",
}

# Recruiting-pipeline states that constitute a state_change (vs an open signal).
_STATE_CHANGE_STATUSES = {
    "hired", "accepted", "offeraccepted", "offer_accepted",
    "rejected", "declined", "withdrawn", "archived",
    "closed", "cancelled", "canceled", "filled",
}


ashby_entity = idempotency.ashby_entity


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


def _entity_updated(entity: dict[str, Any]) -> str | None:
    for key in ("updatedAt", "updated_at", "createdAt", "created_at"):
        v = entity.get(key)
        if isinstance(v, str) and v:
            return v
    return None


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _entity_id_of(entity: dict[str, Any]) -> str:
    rid = entity.get("id") or entity.get("Id")
    return str(rid) if rid not in (None, "") else ""


def _status_of(entity: dict[str, Any]) -> str | None:
    """The lifecycle status of a recruiting object across Ashby's entity kinds:
    application/interview `status`, offer `offerStatus`, candidate `stage`."""
    for key in ("status", "offerStatus", "offer_status", "stage", "state"):
        v = entity.get(key)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, dict):
            # Ashby often nests a {"id","title"|"name"} ref for stage/status.
            for nk in ("title", "name", "value"):
                nv = v.get(nk)
                if isinstance(nv, str) and nv:
                    return nv
    return None


def _person(entity: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    """(actor_ref, entity_hint) for the candidate/person on the recruiting object."""
    name = entity.get("name") or entity.get("candidateName")
    cid = entity.get("candidateId") or entity.get("id")
    if isinstance(name, str) and name:
        hint: dict[str, Any] = {"type": "person", "role": "candidate", "id": name}
        actor = f"ashby:candidate:{cid}" if cid else None
        return actor, hint
    # Fall back to a nested candidate ref.
    ref = entity.get("candidate")
    if isinstance(ref, dict):
        nm = ref.get("name")
        rid = ref.get("id")
        if isinstance(nm, str) and nm:
            actor = f"ashby:candidate:{rid}" if rid else None
            return actor, {"type": "person", "role": "candidate", "id": nm}
    return None, None


def _label(entity_kind: str, entity: dict[str, Any]) -> str:
    """Human reference like 'Candidate Ada Lovelace' / 'Job Staff Engineer'."""
    nice = entity_kind.replace("_", " ").title()
    doc = (
        entity.get("name")
        or entity.get("title")
        or entity.get("candidateName")
        or _entity_id_of(entity)
        or "?"
    )
    return f"{nice} {doc}"


def _classify(entity: dict[str, Any]) -> tuple[str, str]:
    """Return (kind, status_word) for a recruiting object.

    Objects whose status indicates a terminal pipeline transition (hired /
    accepted / rejected / withdrawn / archived / …) are `state_change`;
    everything else is an open `signal`."""
    status = _status_of(entity)
    status_word = str(status).strip().lower() if status else "open"
    norm = status_word.replace(" ", "")
    if norm in _STATE_CHANGE_STATUSES:
        return "state_change", status_word
    return "signal", status_word


def _entity_extras(entity: dict[str, Any]) -> dict[str, Any]:
    """The richer Ashby recruiting fields beyond the header. Only present keys are
    returned so `content` stays lean."""
    extras: dict[str, Any] = {}
    for src, dst in (
        ("jobId", "job_id"),
        ("candidateId", "candidate_id"),
        ("applicationId", "application_id"),
        ("interviewStageId", "interview_stage_id"),
        ("currentInterviewStage", "current_stage"),
        ("source", "candidate_source"),
        ("location", "location"),
        ("department", "department"),
        ("scheduledAt", "scheduled_at"),
        ("startDate", "start_date"),
        ("title", "title"),
    ):
        v = entity.get(src)
        if v is not None:
            # Flatten Ashby's {"id","title"|"name"} refs.
            if isinstance(v, dict):
                extras[dst] = v.get("title") or v.get("name") or v.get("value") or v.get("id")
            else:
                extras[dst] = v
    return extras


# ---------------------------------------------------------------------
# Record builder (shared by backfill + webhook paths)
# ---------------------------------------------------------------------

def _entity_draft(
    entity_kind: str, entity: dict[str, Any], org_id: str,
) -> ObservationDraft:
    entity_id = _entity_id_of(entity)
    if not org_id or not entity_id:
        raise ValidationError(
            "ashby entity missing org_id/id", channel=_CHANNEL,
        )
    external_id = ashby_entity(org_id, entity_kind, entity_id)

    updated = _entity_updated(entity)
    occurred = _parse_iso(updated) or _utcnow()
    kind, status_word = _classify(entity)
    actor_ref, person_hint = _person(entity)

    label = _label(entity_kind, entity)
    who = (person_hint or {}).get("id")
    parts = [label]
    if who and who != label.split(" ", 1)[-1]:
        parts.append(f"· {who}")
    parts.append(f"· {status_word}")
    content_text = " ".join(str(p) for p in parts)

    entities: list[dict[str, Any]] = [
        {"type": "ashby_object", "id": f"{entity_kind}:{entity_id}"},
    ]
    if person_hint:
        entities.append(person_hint)

    content: dict[str, Any] = {
        "object_type": entity_kind,
        "org_id": org_id,
        "entity_id": entity_id,
        "status": status_word,
        "name": entity.get("name") or entity.get("title"),
        "last_updated": updated,
    }
    content.update(_entity_extras(entity))

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=actor_ref,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=entity,
    )


def _thin_change_draft(
    entity_kind: str, entity_id: str, org_id: str, *,
    action: str | None, updated: str | None,
) -> ObservationDraft:
    """A webhook notification with no full entity body (id + action only). Emit a
    thin change observation; the next backfill/poll re-fetch fills the full body
    and dedups on the same (unversioned) external_id."""
    if not org_id or not entity_id:
        raise ValidationError(
            "ashby change missing org_id/id", channel=_CHANNEL,
        )
    external_id = ashby_entity(org_id, entity_kind, entity_id)
    occurred = _parse_iso(updated) or _utcnow()
    act = action or "update"
    content_text = (
        f"{entity_kind.replace('_', ' ').title()} {entity_id} "
        f"{act.lower()} (live)"
    )
    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "object_type": entity_kind,
            "org_id": org_id,
            "entity_id": entity_id,
            "action": act,
            "thin_change": True,
            "last_updated": updated,
        },
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=[{"type": "ashby_object",
                        "id": f"{entity_kind}:{entity_id}"}],
        raw_payload=None,
    )


def _org_of(payload: dict[str, Any]) -> str:
    rid = payload.get("_fyralis_org_id") or payload.get("organizationId")
    if isinstance(rid, str) and rid:
        return rid
    return ""


def _kind_from_action(action: Any, data: dict[str, Any]) -> str | None:
    """Resolve the canonical entity_kind from an Ashby webhook `action` (e.g.
    "applicationSubmit", "interviewSchedule", "offerCreate") or the data shape."""
    # Explicit object kind on the body wins.
    for key in ("resourceType", "objectType", "type"):
        v = data.get(key)
        if isinstance(v, str) and v.lower() in _ENTITY_NORMALISE:
            return _ENTITY_NORMALISE[v.lower()]
    if isinstance(action, str):
        low = action.lower()
        for k in _ENTITY_NORMALISE:
            if low.startswith(k):
                return _ENTITY_NORMALISE[k]
    return None


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_ashby_object(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("ashby payload must be a JSON object", channel=_CHANNEL)

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if isinstance(record_type, str) and record_type:
        entity_kind = _ENTITY_NORMALISE.get(record_type.lower())
        if entity_kind is None:
            raise ValidationError(
                f"unsupported ashby record_type {record_type!r}",
                channel=_CHANNEL,
            )
        entity = payload.get("entity") or {}
        return _entity_draft(entity_kind, entity, _org_of(payload))

    # --- LIVE WEBHOOK path (Ashby webhook event) ---
    # Shape: {"action":"applicationUpdate","data":{...entity...},
    #         "organizationId":"..."}. The harness may also flatten the entity
    # body alongside `organizationId`.
    org_id = _org_of(payload)
    action = payload.get("action") or payload.get("event")
    data = payload.get("data")
    if not isinstance(data, dict):
        data = payload  # flattened harness convenience
    entity_kind = _kind_from_action(action, data)
    if entity_kind is not None:
        body = data if data.get("id") or data.get("Id") else None
        if body is not None:
            return _entity_draft(entity_kind, body, org_id)
        return _thin_change_draft(
            entity_kind,
            str(data.get("id") or data.get("entityId") or ""),
            org_id,
            action=action if isinstance(action, str) else None,
            updated=_entity_updated(data),
        )

    raise ValidationError(
        "ashby payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_ashby_object", "ashby_entity"]
