"""services/ingest/ingestion/handlers/carta.py — Carta cap-table handler.

ONE channel `carta:object` (mirrors gusto:object's one-channel/many-record-types
shape). The handler is a pure function (no DB / network) and branches on the
input shape to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"shareholder","shareclass","safenote","optiongrant"} (set by the fetcher
    or the poll dispatcher).
  - LIVE POLL: the poll dispatcher (`integrations/carta/poll.py`) emits the SAME
    fetcher-shaped tagged record, so a polled change and its backfill twin dedup.

Carta is POLL-ONLY (no webhook), so there is no webhook-envelope branch — the
live edge re-uses the backfill record shape exactly.

Signal mapping (the reasoning value): cap-table objects mutate through lifecycle
states. A SAFE that converts, an option grant that is exercised/cancelled, or a
share class change is a `state_change`; everything else (a new shareholder, an
open grant) is a `signal`.

external_id — VERSIONED by `SyncToken` and DISCRIMINATED by entity_kind (the
observations repo dedups on (source_channel, external_id) IGNORING occurred_at):
  - carta:{firm_id}:{entity_kind}:{entity_id}:{sync_token}
The entity_kind discriminator keeps multi-entity fixtures with the same id from
ever colliding (cap-table-shaped, NOT transaction-shaped).

Trust posture: Carta is the cap-table system of record -> `authoritative`.
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


_CHANNEL = "carta:object"
_TRUST = "authoritative"

# Map a CARTA entity `name` / record_type (backfill/poll) to a canonical kind.
_ENTITY_NORMALISE = {
    "shareholder": "shareholder",
    "shareclass": "share_class",
    "safenote": "safe_note",
    "optiongrant": "option_grant",
}

# Cap-table lifecycle states that constitute a state_change (vs an open signal).
_STATE_CHANGE_STATUSES = {
    "converted", "exercised", "cancelled", "canceled", "terminated",
    "repurchased", "expired", "forfeited",
}


def carta_entity(
    firm_id: str, entity_kind: str, entity_id: str, sync_token: str,
) -> str:
    """`carta:{firm}:{kind}:{id}:{sync_token}` — VERSIONED by sync_token so each
    cap-table mutation re-observes, DISCRIMINATED by entity_kind so multi-entity
    fixtures sharing an id never collide.

    TODO(human): during the wiring phase move this constructor to
        `services/ingest/ingestion/idempotency/__init__.py` as
        `carta_entity(firm_id, entity_kind, entity_id, sync_token)` (the
        canonical home, mirroring `gusto_entity`) and import it here. That module
        is a SHARED file this phase must not edit, so the format lives here for
        now — the format string MUST stay byte-identical across the move.
    """
    return f"carta:{firm_id}:{entity_kind}:{entity_id}:{sync_token}"


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


def _last_updated(entity: dict[str, Any]) -> str | None:
    meta = entity.get("MetaData") or entity.get("Metadata") or {}
    if isinstance(meta, dict):
        v = meta.get("LastUpdatedTime")
        return v if isinstance(v, str) else None
    return None


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _holder(entity: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    """(actor_ref, entity_hint) for the stakeholder on the cap-table object."""
    ref = entity.get("StakeholderRef") or entity.get("HolderRef")
    if isinstance(ref, dict):
        name = ref.get("name")
        rid = ref.get("value")
        hint: dict[str, Any] = {"type": "person", "role": "stakeholder"}
        if name:
            hint["id"] = name
        elif rid:
            hint["id"] = str(rid)
        actor = f"carta:stakeholder:{rid}" if rid else None
        return actor, (hint if hint.get("id") else None)
    return None, None


def _label(entity_kind: str, entity: dict[str, Any]) -> str:
    """Human reference like 'Option Grant OG-12' / 'Safe Note SAFE-3'."""
    doc = entity.get("DocNumber") or entity.get("Id") or "?"
    nice = entity_kind.replace("_", " ").title()
    return f"{nice} {doc}"


def _classify(entity: dict[str, Any]) -> tuple[str, str]:
    """Return (kind, status_word) for a cap-table object.

    Objects whose `Status` indicates a lifecycle transition (converted /
    exercised / cancelled / …) are `state_change`; everything else is an open
    `signal`."""
    status = entity.get("Status")
    status_word = str(status).strip().lower() if status else "active"
    if status_word in _STATE_CHANGE_STATUSES:
        return "state_change", status_word
    return "signal", status_word


def _entity_extras(entity: dict[str, Any]) -> dict[str, Any]:
    """The richer Carta cap-table fields beyond the header. Only present keys are
    returned so `content` stays lean."""
    extras: dict[str, Any] = {}
    for src, dst in (
        ("ShareCount", "share_count"),
        ("Quantity", "quantity"),
        ("StrikePrice", "strike_price"),
        ("PricePerShare", "price_per_share"),
        ("InvestmentAmount", "investment_amount"),
        ("ValuationCap", "valuation_cap"),
        ("DiscountRate", "discount_rate"),
        ("VestingSchedule", "vesting_schedule"),
        ("GrantDate", "grant_date"),
        ("IssueDate", "issue_date"),
        ("Ownership", "ownership_pct"),
    ):
        if entity.get(src) is not None:
            extras[dst] = entity.get(src)
    sc = entity.get("ShareClassRef")
    if isinstance(sc, dict) and (sc.get("name") or sc.get("value")):
        extras["share_class"] = sc.get("name") or str(sc.get("value"))
    return extras


def _entity_draft(
    entity_kind: str, entity: dict[str, Any], firm_id: str,
) -> ObservationDraft:
    entity_id = str(entity.get("Id") or "")
    if not firm_id or not entity_id:
        raise ValidationError(
            "carta entity missing firm_id/Id", channel=_CHANNEL,
        )
    sync_token = str(entity.get("SyncToken") or "0")
    external_id = carta_entity(firm_id, entity_kind, entity_id, sync_token)

    updated = _last_updated(entity)
    occurred = _parse_iso(updated) or _utcnow()
    kind, status_word = _classify(entity)
    actor_ref, holder_hint = _holder(entity)

    label = _label(entity_kind, entity)
    who = (holder_hint or {}).get("id")
    parts = [label]
    if who:
        parts.append(f"· {who}")
    amount = (
        entity.get("InvestmentAmount")
        or entity.get("ShareCount")
        or entity.get("Quantity")
    )
    if amount is not None:
        parts.append(f"· {amount}")
    parts.append(f"· {status_word}")
    content_text = " ".join(str(p) for p in parts)

    entities: list[dict[str, Any]] = [
        {"type": "carta_object", "id": f"{entity_kind}:{entity_id}"},
    ]
    if holder_hint:
        entities.append(holder_hint)

    content: dict[str, Any] = {
        "object_type": entity_kind,
        "firm_id": firm_id,
        "entity_id": entity_id,
        "sync_token": sync_token,
        "doc_number": entity.get("DocNumber"),
        "status": status_word,
        "stakeholder": (entity.get("StakeholderRef") or {}).get("name")
        if isinstance(entity.get("StakeholderRef"), dict) else None,
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


def _firm_of(payload: dict[str, Any]) -> str:
    rid = payload.get("_fyralis_firm_id") or payload.get("firmId")
    if isinstance(rid, str) and rid:
        return rid
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_carta_object(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("carta payload must be a JSON object", channel=_CHANNEL)

    # --- BACKFILL / POLL path (fetcher- or poll-tagged records) ---
    # Carta is poll-only; the live edge re-uses the SAME tagged record shape, so
    # there is one branch for both.
    record_type = payload.get("_fyralis_record_type")
    if isinstance(record_type, str) and record_type:
        entity_kind = _ENTITY_NORMALISE.get(record_type.lower())
        if entity_kind is None:
            raise ValidationError(
                f"unsupported carta record_type {record_type!r}",
                channel=_CHANNEL,
            )
        entity = payload.get("entity") or {}
        return _entity_draft(entity_kind, entity, _firm_of(payload))

    raise ValidationError(
        "carta payload is not a tagged cap-table record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_carta_object", "carta_entity"]
