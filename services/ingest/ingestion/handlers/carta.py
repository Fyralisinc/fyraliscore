"""services/ingest/ingestion/handlers/carta.py — Carta cap-table handler.

ONE channel `carta:object` (mirrors gusto:object's one-channel/many-record-types
shape). The handler is a pure function (no DB / network) and branches on the
input shape to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"stakeholder","shareclass","optiongrant","convertiblenote"} (set by the
    fetcher or the poll dispatcher) plus `_fyralis_firm_id` (the Carta issuer id
    — the install scope, stored in `carta_installations.firm_id`).
  - LIVE POLL: the poll dispatcher (`integrations/carta/poll.py`) emits the SAME
    fetcher-shaped tagged record, so a polled change and its backfill twin dedup.

Carta is POLL-ONLY (no webhook), so there is no webhook-envelope branch — the
live edge re-uses the backfill record shape exactly.

Wire shapes (CONFIRMED against the Issuer v1alpha1 OpenAPI embedded in
docs.carta.com/api-platform/reference — see integrations/carta/client.py):
entities use camelCase field names and protobuf wrapper objects —
`{"value": "<decimal string>"}` (v1alpha1Decimal / dates) and
`{"currencyCode": {"value": "USD"}, "amount": {"value": "1.25"}}`
(v1alpha1Money). This handler decodes those wrappers into plain strings /
`{"amount","currency"}` dicts for `content`.

Signal mapping (the reasoning value): cap-table objects mutate through lifecycle
states. An option grant that is canceled/terminated/exercised, or a convertible
note that converts/cancels, is a `state_change`; everything else (a new
stakeholder, an outstanding grant) is a `signal`. A stakeholder whose
`relationship` is `EX_*` (e.g. EX_EMPLOYEE) is mapped to a "former"
state_change — an inferred mapping from the relationship enum, not a documented
lifecycle field.

external_id — VERSIONED by a content digest and DISCRIMINATED by entity_kind
(the observations repo dedups on (source_channel, external_id) IGNORING
occurred_at):
  - carta:{firm_id}:{entity_kind}:{entity_id}:{version}
where `version = carta_version(entity)` — the first 12 hex chars of the SHA-256
of the canonical (key-sorted) entity JSON. Carta's v1alpha1 entities carry no
SyncToken-style revision counter, so the digest replaces it: a re-walked
unchanged entity dedups; ANY field change (e.g. lastModifiedDatetime bump on an
option grant) re-observes as a new observation. Decimals arrive as wrapper
STRINGS so the digest is float-drift-free across JSON round-trips. The
entity_kind discriminator keeps multi-entity rows with the same id from ever
colliding (cap-table-shaped, NOT transaction-shaped).

Trust posture: Carta is the cap-table system of record -> `authoritative`.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

import orjson

from lib.shared.errors import ValidationError

from services.ingest.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)
from services.ingest.ingestion.idempotency import carta_entity


_CHANNEL = "carta:object"
_TRUST = "authoritative"

# Map a record_type tag (entity_type.lower(), set by the fetcher / poll
# dispatcher) to the canonical entity kind baked into the external_id.
_ENTITY_NORMALISE = {
    "stakeholder": "stakeholder",
    "shareclass": "share_class",
    "optiongrant": "option_grant",
    "convertiblenote": "convertible_note",
}


def carta_version(entity: dict[str, Any]) -> str:
    """Deterministic version token for a cap-table entity: the first 12 hex
    chars of SHA-256 over the canonical (key-sorted) entity JSON. Replaces the
    revision counter Carta's v1alpha1 entities do not have — identical wire
    payloads dedup, any mutation re-observes."""
    return hashlib.sha256(
        orjson.dumps(entity, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()[:12]


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


# ---------------------------------------------------------------------
# Wrapper decoding (v1alpha1Decimal / Date / DateTime / Money)
# ---------------------------------------------------------------------

def _wrapped(value: Any) -> str | None:
    """`{"value": "<string>"}` -> the string (decimal / date / datetime /
    currency-code wrappers all share this shape)."""
    if isinstance(value, dict):
        v = value.get("value")
        return v if isinstance(v, str) and v else None
    return None


def _money(value: Any) -> dict[str, Any] | None:
    """v1alpha1Money -> `{"amount": "<decimal str>", "currency": "USD"}`."""
    if not isinstance(value, dict):
        return None
    amount = _wrapped(value.get("amount"))
    currency = _wrapped(value.get("currencyCode"))
    if amount is None and currency is None:
        return None
    out: dict[str, Any] = {}
    if amount is not None:
        out["amount"] = amount
    if currency is not None:
        out["currency"] = currency
    return out


def _decimal_gt_zero(value: Any) -> bool:
    s = _wrapped(value)
    if s is None:
        return False
    try:
        return float(s) > 0
    except (TypeError, ValueError):
        return False


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ---------------------------------------------------------------------
# Per-kind decoding
# ---------------------------------------------------------------------

def _classify(entity_kind: str, entity: dict[str, Any]) -> tuple[str, str]:
    """Return (kind, status_word) for a cap-table object.

    Lifecycle transitions are derived from the documented per-kind fields:
      - option_grant: canceledDate / terminationDate / exercisedQuantity>0
      - convertible_note: conversionDatetime / canceledDatetime
      - stakeholder: relationship EX_* -> "former" (inferred from the enum)
    Everything else is an open `signal`.
    """
    if entity_kind == "option_grant":
        if _wrapped(entity.get("canceledDate")):
            return "state_change", "canceled"
        if _wrapped(entity.get("terminationDate")):
            return "state_change", "terminated"
        if _decimal_gt_zero(entity.get("exercisedQuantity")):
            return "state_change", "exercised"
        return "signal", "outstanding"
    if entity_kind == "convertible_note":
        if _wrapped(entity.get("conversionDatetime")):
            return "state_change", "converted"
        if _wrapped(entity.get("canceledDatetime")):
            return "state_change", "canceled"
        return "signal", "outstanding"
    if entity_kind == "stakeholder":
        rel = entity.get("relationship")
        if isinstance(rel, str) and rel.upper().startswith("EX_"):
            return "state_change", "former"
        return "signal", (rel.lower() if isinstance(rel, str) and rel else "active")
    return "signal", "active"


def _occurred_iso(entity_kind: str, entity: dict[str, Any]) -> str | None:
    """The best per-kind source timestamp (ISO string), or None.

    Only option grants carry `lastModifiedDatetime`; convertible notes carry
    issue/conversion/cancel datetimes; stakeholders and share classes have no
    timestamp (caller falls back to now)."""
    if entity_kind == "option_grant":
        return (
            _wrapped(entity.get("lastModifiedDatetime"))
            or _wrapped(entity.get("issueDate"))
        )
    if entity_kind == "convertible_note":
        return (
            _wrapped(entity.get("conversionDatetime"))
            or _wrapped(entity.get("canceledDatetime"))
            or _wrapped(entity.get("issueDatetime"))
        )
    return None


def _label(entity_kind: str, entity: dict[str, Any]) -> str:
    """Human reference like 'Option Grant OG-12' / 'Stakeholder Jane Doe'."""
    nice = entity_kind.replace("_", " ").title()
    if entity_kind == "stakeholder":
        ref = entity.get("fullName") or entity.get("id") or "?"
    elif entity_kind == "share_class":
        ref = entity.get("name") or entity.get("id") or "?"
    else:
        ref = entity.get("securityLabel") or entity.get("id") or "?"
    return f"{nice} {ref}"


def _holder(
    entity_kind: str, entity: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    """(actor_ref, entity_hint) for the stakeholder on the cap-table object."""
    if entity_kind == "stakeholder":
        sid = entity.get("id")
        name = entity.get("fullName")
        hint: dict[str, Any] = {"type": "person", "role": "stakeholder"}
        hint["id"] = name or (str(sid) if sid else None)
        actor = f"carta:stakeholder:{sid}" if sid else None
        return actor, (hint if hint.get("id") else None)
    sid = entity.get("stakeholderId")
    if sid:
        return (
            f"carta:stakeholder:{sid}",
            {"type": "person", "role": "stakeholder", "id": str(sid)},
        )
    return None, None


def _entity_extras(entity_kind: str, entity: dict[str, Any]) -> dict[str, Any]:
    """The richer per-kind cap-table fields beyond the header. Wrapper objects
    are decoded; only present keys are returned so `content` stays lean."""
    extras: dict[str, Any] = {}

    def put(key: str, value: Any) -> None:
        if value is not None:
            extras[key] = value

    if entity_kind == "stakeholder":
        put("full_name", entity.get("fullName"))
        put("email", entity.get("email"))
        put("employee_id", entity.get("employeeId"))
        put("relationship", entity.get("relationship"))
        put("stakeholder_entity_type", entity.get("entityType"))
        put("group", entity.get("group"))
    elif entity_kind == "share_class":
        put("name", entity.get("name"))
        put("prefix", entity.get("prefix"))
        put("share_class_type", entity.get("type"))
        put("authorized_share_count", _wrapped(entity.get("authorizedShareCount")))
        put("par_value", _money(entity.get("parValue")))
        put("seniority", entity.get("seniority"))
        put("pari_passu", entity.get("pariPassu"))
    elif entity_kind == "option_grant":
        put("security_label", entity.get("securityLabel"))
        put("security_id", entity.get("securityId"))
        put("stakeholder_id", entity.get("stakeholderId"))
        put("share_class_id", entity.get("shareClassId"))
        put("stock_option_type", entity.get("stockOptionType"))
        put("quantity", _wrapped(entity.get("quantity")))
        put("outstanding_quantity", _wrapped(entity.get("outstandingQuantity")))
        put("vested_quantity", _wrapped(entity.get("vestedQuantity")))
        put("exercised_quantity", _wrapped(entity.get("exercisedQuantity")))
        put("exercise_price", _money(entity.get("exercisePrice")))
        put("issue_date", _wrapped(entity.get("issueDate")))
        put("canceled_date", _wrapped(entity.get("canceledDate")))
        put("termination_date", _wrapped(entity.get("terminationDate")))
        put("expiration_date", _wrapped(entity.get("grantExpirationDate")))
        put("last_modified", _wrapped(entity.get("lastModifiedDatetime")))
    elif entity_kind == "convertible_note":
        put("security_label", entity.get("securityLabel"))
        put("security_id", entity.get("securityId"))
        put("stakeholder_id", entity.get("stakeholderId"))
        put("cash_paid", _money(entity.get("cashPaid")))
        put("valuation_cap", _money(entity.get("priceCap")))
        put("discount_percentage", _wrapped(entity.get("discountPercentage")))
        put("interest_rate", _wrapped(entity.get("interestRate")))
        put("issue_datetime", _wrapped(entity.get("issueDatetime")))
        put("maturity_datetime", _wrapped(entity.get("maturityDatetime")))
        put("conversion_datetime", _wrapped(entity.get("conversionDatetime")))
        put("canceled_datetime", _wrapped(entity.get("canceledDatetime")))
    return extras


def _headline_amount(entity_kind: str, extras: dict[str, Any]) -> str | None:
    """The single most informative figure for the content_text line."""
    if entity_kind == "option_grant":
        qty = extras.get("quantity")
        return f"{qty} options" if qty else None
    if entity_kind == "convertible_note":
        cash = extras.get("cash_paid")
        if isinstance(cash, dict) and cash.get("amount"):
            cur = cash.get("currency") or ""
            return f"{cash['amount']} {cur}".strip()
        return None
    if entity_kind == "share_class":
        count = extras.get("authorized_share_count")
        return f"{count} authorized" if count else None
    return None


def _entity_draft(
    entity_kind: str, entity: dict[str, Any], firm_id: str,
) -> ObservationDraft:
    entity_id = str(entity.get("id") or "")
    if not firm_id or not entity_id:
        raise ValidationError(
            "carta entity missing firm_id/id", channel=_CHANNEL,
        )
    version = carta_version(entity)
    external_id = carta_entity(firm_id, entity_kind, entity_id, version)

    occurred_iso = _occurred_iso(entity_kind, entity)
    occurred = _parse_iso(occurred_iso) or _utcnow()
    kind, status_word = _classify(entity_kind, entity)
    actor_ref, holder_hint = _holder(entity_kind, entity)
    extras = _entity_extras(entity_kind, entity)

    label = _label(entity_kind, entity)
    parts = [label]
    who = (holder_hint or {}).get("id")
    if who and entity_kind != "stakeholder":
        parts.append(f"· {who}")
    amount = _headline_amount(entity_kind, extras)
    if amount:
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
        "version": version,
        "status": status_word,
        "issuer_id": entity.get("issuerId"),
    }
    content.update(extras)

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
    rid = payload.get("_fyralis_firm_id") or payload.get("issuerId")
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


__all__ = ["handle_carta_object", "carta_entity", "carta_version"]
