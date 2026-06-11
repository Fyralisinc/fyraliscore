"""services/ingest/ingestion/handlers/hibob.py — HiBob People/HR entity handler.

ONE channel `hibob:object` (mirrors gusto:object's one-channel/many-record-types
shape). The handler is a pure function (no DB / network) and branches on the
input shape to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"employee","lifecycle","timeoff","payroll"} (set by the fetcher), plus a
    `_fyralis_company_id`.
  - LIVE WEBHOOK: a HiBob webhook body carrying a `companyId` + an entity payload
    (and/or a `type`/`eventType` describing the change); the handler maps it onto
    the same record builders so a webhook-delivered change and its backfill twin
    dedup.

Signal mapping (the reasoning value):
  - lifecycle change (hire / termination / role change)     -> kind="state_change"
    (the org-change signal: someone joined / left / moved)
  - time-off request that is approved / declined / cancelled -> kind="state_change"
    (the coverage / capacity signal)
  - everything else (employee profile update, payroll run)  -> kind="signal"

external_id — VERSIONED (the observations repo dedups on
(source_channel, external_id) IGNORING occurred_at; a status change must land as
a NEW observation, not silently dedup). Per the CONTRACT:
  - hibob:{company}:{entity}:{id}:{ver}

where {ver} is the row's modified/version field (or the change timestamp on a
thin webhook). Namespaced by company so the same entity id across HiBob accounts
stays distinct (the global UNIQUE has no tenant_id).

Trust posture: HiBob is the HR system of record -> `authoritative`.
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


_CHANNEL = "hibob:object"
_TRUST = "authoritative"

# Allowed entity kinds (one shard per type). Normalised lowercase so a webhook's
# `type` and the fetcher's record_type map onto the same set.
_ENTITY_KINDS = frozenset({"employee", "lifecycle", "timeoff", "payroll"})

# Lifecycle statuses that represent an org-change state change. Everything else
# (an in-flight / draft change) is a signal.
_LIFECYCLE_STATE_CHANGES = frozenset(
    {"hired", "hire", "terminated", "termination", "offboarded", "left", "rehired"}
)
# Time-off statuses that represent a coverage/capacity state change.
_TIMEOFF_STATE_CHANGES = frozenset(
    {"approved", "declined", "rejected", "cancelled", "canceled"}
)


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


def _external_id(company_id: str, entity_kind: str, entity_id: str, ver: str) -> str:
    """`hibob:{company}:{entity}:{id}:{ver}` — namespaced by company and
    discriminated by entity kind (so multiple entity kinds sharing an id never
    collide), VERSIONED by the row's modified/version so each change re-observes.

    Built inline (NOT via the shared idempotency module) so this handler adds no
    new shared-file surface; the format is the CONTRACT external_id verbatim.
    """
    return f"hibob:{company_id}:{entity_kind}:{entity_id}:{ver}"


# ---------------------------------------------------------------------
# Field extraction across HiBob's varying entity shapes
# ---------------------------------------------------------------------

def _entity_id_of(entity: dict[str, Any]) -> str:
    for key in (
        "id",
        "/root/id",
        "root.id",
        "employeeId",
        "employee_id",
        "requestId",
        "request_id",
        "payrollId",
        "payroll_id",
    ):
        v = entity.get(key)
        if v not in (None, ""):
            return str(v)
    return ""


def _modified_of(entity: dict[str, Any]) -> str | None:
    for key in ("modified", "modifiedAt", "lastModified", "updatedAt", "updated"):
        v = entity.get(key)
        if isinstance(v, str) and v:
            return v
    return None


def _status_of(entity: dict[str, Any]) -> str:
    for key in ("status", "state", "lifecycleStatus", "approvalStatus"):
        v = entity.get(key)
        if isinstance(v, str) and v:
            return v.lower()
    return ""


def _display_name(entity: dict[str, Any]) -> str | None:
    name = (
        entity.get("displayName")
        or entity.get("/root/displayName")
        or entity.get("root.displayName")
        or entity.get("fullName")
        or entity.get("name")
    )
    if isinstance(name, str) and name:
        return name
    first = entity.get("firstName") or ""
    last = entity.get("surname") or entity.get("lastName") or ""
    combined = f"{first} {last}".strip()
    return combined or None


def _classify(entity_kind: str, status_word: str) -> str:
    if entity_kind == "lifecycle" and status_word in _LIFECYCLE_STATE_CHANGES:
        return "state_change"
    if entity_kind == "timeoff" and status_word in _TIMEOFF_STATE_CHANGES:
        return "state_change"
    return "signal"


# ---------------------------------------------------------------------
# Record builder (shared by backfill + webhook paths)
# ---------------------------------------------------------------------

def _entity_draft(
    entity_kind: str, entity: dict[str, Any], company_id: str,
) -> ObservationDraft:
    entity_id = _entity_id_of(entity)
    if not company_id or not entity_id:
        raise ValidationError(
            "hibob entity missing company_id/id", channel=_CHANNEL,
        )
    modified = _modified_of(entity)
    # Version slot for the external_id: prefer the row's modified field; fall
    # back to a stable marker so an unversioned row dedups against itself.
    ver = modified or "0"
    external_id = _external_id(company_id, entity_kind, entity_id, ver)

    occurred = _parse_iso(modified) or _utcnow()
    status_word = _status_of(entity)
    kind = _classify(entity_kind, status_word)

    name = _display_name(entity)
    nice = entity_kind.title()
    parts = [f"{nice} {name}" if name else f"{nice} #{entity_id}"]
    if status_word:
        parts.append(f"· {status_word}")
    content_text = " ".join(parts)

    entities: list[dict[str, Any]] = [
        {"type": "hibob_object", "id": f"{entity_kind}:{entity_id}"},
    ]
    # Employees / time-off requests reference a person; surface as a person hint.
    if name:
        entities.append({"type": "person", "id": name, "role": entity_kind})

    content: dict[str, Any] = {
        "object_type": entity_kind,
        "company_id": company_id,
        "entity_id": entity_id,
        "status": status_word or None,
        "display_name": name,
        "department": entity.get("department") or entity.get("/work/department"),
        "title": (
            entity.get("title")
            or entity.get("jobTitle")
            or entity.get("/work/title")
        ),
        "email": entity.get("email") or entity.get("/root/email"),
        "effective_date": entity.get("effectiveDate") or entity.get("startDate"),
        "modified": modified,
    }
    # Drop None keys so content stays lean.
    content = {k: v for k, v in content.items() if v is not None}

    actor_ref = f"hibob:employee:{entity_id}" if entity_kind == "employee" else None

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
    entity_kind: str, entity_id: str, company_id: str, *,
    event_type: str | None, modified: str | None,
) -> ObservationDraft:
    """A webhook notification with no full entity body. Emit a thin change
    observation; the next backfill/poll re-fetch fills the full body (and dedups
    by the modified version if unchanged)."""
    if not company_id or not entity_id:
        raise ValidationError(
            "hibob change missing company_id/id", channel=_CHANNEL,
        )
    ver = modified or _utcnow().isoformat()
    external_id = _external_id(company_id, entity_kind, entity_id, f"chg:{ver}")
    occurred = _parse_iso(modified) or _utcnow()
    ev = event_type or "update"
    content_text = f"{entity_kind.title()} #{entity_id} {ev.lower()} (live)"
    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "object_type": entity_kind,
            "company_id": company_id,
            "entity_id": entity_id,
            "event_type": ev,
            "thin_change": True,
            "modified": modified,
        },
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=[{"type": "hibob_object",
                        "id": f"{entity_kind}:{entity_id}"}],
        raw_payload=None,
    )


def _normalise_kind(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    k = value.lower()
    # Allow webhook event types like "employee.updated" / "timeOff.approved".
    head = k.split(".", 1)[0].split("_", 1)[0]
    # Map a few aliases onto the canonical entity kinds.
    alias = {
        "person": "employee",
        "people": "employee",
        "timeoffrequest": "timeoff",
        "timeoff": "timeoff",
        "time": "timeoff",
    }
    head = alias.get(head, head)
    return head if head in _ENTITY_KINDS else None


def _company_of(payload: dict[str, Any]) -> str:
    rid = payload.get("_fyralis_company_id") or payload.get("companyId")
    if rid not in (None, ""):
        return str(rid)
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_hibob_object(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("hibob payload must be a JSON object", channel=_CHANNEL)

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if isinstance(record_type, str) and record_type:
        entity_kind = _normalise_kind(record_type)
        if entity_kind is None:
            raise ValidationError(
                f"unsupported hibob record_type {record_type!r}",
                channel=_CHANNEL,
            )
        entity = payload.get("entity") or {}
        return _entity_draft(entity_kind, entity, _company_of(payload))

    # --- LIVE WEBHOOK path ---
    # Shape (gate stand-in): {"companyId":"...", "type":"<kind>.<event>",
    #   "entity": {...full body...}?, "id": "...", "modified": "..."}. When a full
    #   entity body is present the handler builds the full draft (dedups with its
    #   backfill twin); otherwise it emits a thin change observation.
    company_id = _company_of(payload)
    entity_kind = _normalise_kind(payload.get("type") or payload.get("eventType"))
    if entity_kind is None:
        raise ValidationError(
            f"unsupported hibob event type {payload.get('type')!r}",
            channel=_CHANNEL,
        )
    body = payload.get("entity")
    if isinstance(body, dict) and _entity_id_of(body):
        return _entity_draft(entity_kind, body, company_id)
    return _thin_change_draft(
        entity_kind, str(payload.get("id") or ""), company_id,
        event_type=payload.get("type") or payload.get("eventType"),
        modified=payload.get("modified") or payload.get("modifiedAt"),
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_hibob_object"]
