"""services/ingest/ingestion/handlers/ramp.py — Ramp record handler (finance).

ONE channel `ramp:transaction` (mirrors jira:issue's
one-channel/many-record-types shape). The handler is a pure function (no DB /
network) and branches on the input shape to produce exactly ONE observation per
call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"transaction","reimbursement","card","user"} (set by the fetcher — the
    VERIFIED Ramp Developer API taxonomy, docs.ramp.com) and carry the REAL
    Ramp resource body under `entity`.
  - LIVE WEBHOOK: a real Ramp flat event ({"id","type","created_at",
    "business_id","object":{"id":…}}). The webhook carries only the affected
    resource id (no body), so the handler emits a thin change observation; the
    poll re-fetch fills the authoritative body.

Money decode is DEFENSIVE for Ramp's dual representation (verified
docs.ramp.com/developer-api/v1/monetary-values):
  - top-level `amount` is a NUMBER in major units (dollars),
  - nested currency-amount objects ({"amount": <int minor units>,
    "currency_code", "minor_unit_conversion_rate"}) carry integer cents.

Signal mapping (the reasoning value) — blueprint §4 state predicate:
  - transaction DECLINED / ERROR, or carrying disputes      -> kind="state_change"
  - reimbursement terminal (REIMBURSED* / MANUALLY_REIMBURSED
    or REJECTED / CANCELED / failed states)                 -> kind="state_change"
  - card SUSPENDED / TERMINATED / CHIP_LOCKED               -> kind="state_change"
  - user USER_INACTIVE / USER_SUSPENDED / INVITE_EXPIRED    -> kind="state_change"
  - everything else (pending / cleared / active …)          -> kind="signal"

external_id — VERSIONED by state (blueprint §4; the mutable-source dedup lesson;
the observations repo dedups on (source_channel, external_id) IGNORING
occurred_at so a state change must land as a NEW observation):
  - ramp:{business_id}:txn:{id}:{state}
  - ramp:{business_id}:reimb:{id}:{state}
  - ramp:{business_id}:card:{id}:{state}
  - ramp:{business_id}:user:{id}:{status}
  - ramp:{business_id}:txn:{id}:chg:{event_id}   (thin live-webhook change)

These external_ids are built INLINE here (not via the shared idempotency module)
so this isolated handler + its tests form a self-consistent loop; the shared
idempotency owner provisions `ramp_transaction` per blueprint §2.8.

Trust posture: Ramp is the spend/card system of record -> `authoritative`.
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


_CHANNEL = "ramp:transaction"
_TRUST = "authoritative"

# Verified state vocabularies (docs.ramp.com OpenAPI enums), lowercased.
# Transaction states: ALL/CLEARED/COMPLETION/DECLINED/ERROR/PENDING/
# PENDING_INITIATION — declined/error are the §4 state-change states (a
# dispute is carried in the `disputes` list, not `state`).
_TXN_STATE_CHANGES = frozenset({"declined", "error"})
# Reimbursement terminal states — cash moved (good) or definitively not (bad).
_REIMB_STATE_CHANGES = frozenset({
    "reimbursed", "reimbursed_via_push", "manually_reimbursed",
    "rejected", "canceled", "deleted", "failed_reimbursement",
    "push_payment_failed", "export_failed",
})
# Card lifecycle states that flip availability.
_CARD_STATE_CHANGES = frozenset({"suspended", "terminated", "chip_locked"})
# User lifecycle states that revoke access.
_USER_STATE_CHANGES = frozenset({
    "user_inactive", "user_suspended", "invite_expired",
})

_RECORD_TYPES = frozenset({"transaction", "reimbursement", "card", "user"})

def _external_id(
    record_type: str, business_id: str, entity_id: str, state: str,
) -> str:
    """ramp:{business_id}:{seg}:{id}:{state} — versioned by state (§4)."""
    return idempotency.ramp_entity(record_type, business_id, entity_id, state)


def _change_external_id(business_id: str, entity_id: str, ver: str) -> str:
    """Thin live-webhook variant (body-less notification), versioned by the
    stable event id (constant across retries) so each live change event stays
    distinct from its backfill twin until the poll re-fetch carries the
    authoritative body."""
    return idempotency.ramp_change(business_id, entity_id, ver)


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


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------
# Money — defensive dual decode (major-unit number vs minor-unit object)
# ---------------------------------------------------------------------

def _money(value: Any) -> tuple[float | None, str | None]:
    """Decode either Ramp money shape to `(major_units, currency_code)`.

    - a bare number is already major units (dollars),
    - a CurrencyAmount object carries integer minor units (cents) +
      `minor_unit_conversion_rate` (default 100) + `currency_code`.
    """
    if isinstance(value, dict):
        minor = _num(value.get("amount"))
        if minor is None:
            return None, value.get("currency_code")
        rate = _num(value.get("minor_unit_conversion_rate")) or 100.0
        return minor / rate, value.get("currency_code")
    return _num(value), None


def _fmt_money(amount: float | None, currency: str | None) -> str:
    if amount is None:
        return "?"
    cur = (currency or "USD").upper()
    return f"${amount:,.2f}" if cur == "USD" else f"{amount:,.2f} {cur}"


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _person_hint(name: str | None) -> dict[str, Any] | None:
    return {"type": "person", "id": name} if name else None


# ---------------------------------------------------------------------
# Per-record-type builders (verified field names)
# ---------------------------------------------------------------------

def _transaction_draft(entity: dict[str, Any], business_id: str) -> ObservationDraft:
    entity_id = str(entity.get("id") or "")
    if not business_id or not entity_id:
        raise ValidationError(
            "ramp transaction missing business_id/id", channel=_CHANNEL,
        )

    state = str(entity.get("state") or "").lower()
    disputes = entity.get("disputes")
    disputed = isinstance(disputes, list) and len(disputes) > 0
    if disputed:
        kind, status_word = "state_change", "disputed"
    elif state in _TXN_STATE_CHANGES:
        kind, status_word = "state_change", state
    else:
        kind, status_word = "signal", (state or "recorded")

    # Money: top-level `amount` is dollars; original_transaction_amount is the
    # pre-conversion minor-unit object. Decode both defensively.
    amount, _ = _money(entity.get("amount"))
    currency = entity.get("currency_code")
    if amount is None:
        amount, currency2 = _money(entity.get("original_transaction_amount"))
        currency = currency or currency2

    occurred = (
        _parse_iso(entity.get("user_transaction_time"))
        or _parse_iso(entity.get("settlement_date"))
        or _utcnow()
    )

    holder = entity.get("card_holder")
    holder_name = None
    holder_user_id = None
    if isinstance(holder, dict):
        first = holder.get("first_name") or ""
        last = holder.get("last_name") or ""
        holder_name = f"{first} {last}".strip() or None
        uid = holder.get("user_id")
        holder_user_id = str(uid) if uid else None

    merchant = entity.get("merchant_name")
    category = entity.get("sk_category_name")
    line_items = entity.get("line_items")
    n_lines = len(line_items) if isinstance(line_items, list) else 0

    external_id = _external_id("transaction", business_id, entity_id, status_word)

    parts = [f"Transaction {entity_id[:13]}"]
    if merchant:
        parts.append(f"· {merchant}")
    parts.append(f"· {_fmt_money(amount, currency)}")
    parts.append(f"· {status_word}")
    if holder_name:
        parts.append(f"({holder_name})")
    if n_lines > 1:
        parts.append(f"· {n_lines} lines")
    content_text = " ".join(parts)

    entities: list[dict[str, Any]] = [
        {"type": "ramp_transaction", "id": f"transaction:{entity_id}"},
    ]
    hint = _person_hint(holder_name)
    if hint:
        entities.append(hint)

    content = _compact({
        "object_type": "transaction",
        "business_id": business_id,
        "entity_id": entity_id,
        "state": state or None,
        "status": status_word,
        "amount": amount,
        "currency": currency,
        "merchant": merchant,
        "merchant_id": entity.get("merchant_id"),
        "category": category,
        "card_holder": holder_name,
        "card_id": entity.get("card_id"),
        "memo": entity.get("memo"),
        "user_transaction_time": entity.get("user_transaction_time"),
        "settlement_date": entity.get("settlement_date"),
        "sync_status": entity.get("sync_status"),
        "disputed": True if disputed else None,
        "decline_reason": _decline_reason(entity),
        "original_amount": _original_amount(entity),
        "line_item_count": n_lines or None,
    })

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=(f"ramp:user:{holder_user_id}" if holder_user_id else None),
        external_id=external_id,
        entities_hint=entities,
        raw_payload=entity,
    )


def _decline_reason(entity: dict[str, Any]) -> str | None:
    dd = entity.get("decline_details")
    if isinstance(dd, dict):
        reason = dd.get("reason")
        return str(reason) if reason else None
    return None


def _original_amount(entity: dict[str, Any]) -> dict[str, Any] | None:
    """Pre-conversion amount, decoded from the minor-unit object — emitted only
    when it differs in currency from the settled amount."""
    orig = entity.get("original_transaction_amount")
    if not isinstance(orig, dict):
        return None
    amount, currency = _money(orig)
    if amount is None:
        return None
    settled_currency = entity.get("currency_code")
    if currency and settled_currency and currency == settled_currency:
        return None
    return {"amount": amount, "currency": currency}


def _reimbursement_draft(entity: dict[str, Any], business_id: str) -> ObservationDraft:
    entity_id = str(entity.get("id") or "")
    if not business_id or not entity_id:
        raise ValidationError(
            "ramp reimbursement missing business_id/id", channel=_CHANNEL,
        )

    state = str(entity.get("state") or "").lower()
    kind = "state_change" if state in _REIMB_STATE_CHANGES else "signal"
    status_word = state or "recorded"

    # Money: top-level `amount` is the payor's major-unit number + `currency`;
    # payee_amount / original_reimbursement_amount are minor-unit objects.
    amount, _ = _money(entity.get("amount"))
    currency = entity.get("currency")
    if amount is None:
        amount, currency2 = _money(entity.get("payee_amount"))
        currency = currency or currency2

    occurred = (
        _parse_iso(entity.get("updated_at"))
        or _parse_iso(entity.get("created_at"))
        or _utcnow()
    )

    who = entity.get("user_full_name")
    uid = entity.get("user_id")
    external_id = _external_id("reimbursement", business_id, entity_id, status_word)

    parts = [f"Reimbursement {entity_id[:13]}"]
    if who:
        parts.append(f"· {who}")
    if entity.get("merchant"):
        parts.append(f"· {entity['merchant']}")
    parts.append(f"· {_fmt_money(amount, currency)}")
    parts.append(f"· {status_word}")
    content_text = " ".join(parts)

    entities: list[dict[str, Any]] = [
        {"type": "ramp_transaction", "id": f"reimbursement:{entity_id}"},
    ]
    hint = _person_hint(who if isinstance(who, str) else None)
    if hint:
        entities.append(hint)

    content = _compact({
        "object_type": "reimbursement",
        "business_id": business_id,
        "entity_id": entity_id,
        "state": state or None,
        "status": status_word,
        "amount": amount,
        "currency": currency,
        "merchant": entity.get("merchant"),
        "user": who,
        "reimbursement_type": entity.get("type"),
        "direction": entity.get("direction"),
        "memo": entity.get("memo"),
        "transaction_date": entity.get("transaction_date"),
        "created_at": entity.get("created_at"),
        "updated_at": entity.get("updated_at"),
        "sync_status": entity.get("sync_status"),
    })

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=(f"ramp:user:{uid}" if uid else None),
        external_id=external_id,
        entities_hint=entities,
        raw_payload=entity,
    )


def _card_draft(entity: dict[str, Any], business_id: str) -> ObservationDraft:
    entity_id = str(entity.get("id") or "")
    if not business_id or not entity_id:
        raise ValidationError(
            "ramp card missing business_id/id", channel=_CHANNEL,
        )

    state = str(entity.get("state") or "").lower()
    kind = "state_change" if state in _CARD_STATE_CHANGES else "signal"
    status_word = state or "recorded"
    occurred = _parse_iso(entity.get("created_at")) or _utcnow()

    name = entity.get("display_name")
    holder = entity.get("cardholder_name")
    last_four = entity.get("last_four")
    external_id = _external_id("card", business_id, entity_id, status_word)

    parts = [f"Card {name or entity_id[:13]}"]
    if last_four:
        parts.append(f"·{last_four}")
    if holder:
        parts.append(f"· {holder}")
    parts.append(f"· {status_word}")
    content_text = " ".join(parts)

    entities: list[dict[str, Any]] = [
        {"type": "ramp_transaction", "id": f"card:{entity_id}"},
    ]
    hint = _person_hint(holder if isinstance(holder, str) else None)
    if hint:
        entities.append(hint)

    content = _compact({
        "object_type": "card",
        "business_id": business_id,
        "entity_id": entity_id,
        "state": state or None,
        "status": status_word,
        "display_name": name,
        "cardholder": holder,
        "cardholder_id": entity.get("cardholder_id"),
        "last_four": last_four,
        "is_physical": entity.get("is_physical"),
        "expiration": entity.get("expiration"),
        "created_at": entity.get("created_at"),
    })

    cid = entity.get("cardholder_id")
    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=(f"ramp:user:{cid}" if cid else None),
        external_id=external_id,
        entities_hint=entities,
        raw_payload=entity,
    )


def _user_draft(entity: dict[str, Any], business_id: str) -> ObservationDraft:
    entity_id = str(entity.get("id") or "")
    if not business_id or not entity_id:
        raise ValidationError(
            "ramp user missing business_id/id", channel=_CHANNEL,
        )

    status = str(entity.get("status") or "").lower()
    kind = "state_change" if status in _USER_STATE_CHANGES else "signal"
    status_word = status or "recorded"
    occurred = _utcnow()  # users carry no creation/update timestamp (verified)

    first = entity.get("first_name") or ""
    last = entity.get("last_name") or ""
    name = f"{first} {last}".strip() or None
    external_id = _external_id("user", business_id, entity_id, status_word)

    parts = [f"User {name or entity_id[:13]}"]
    if entity.get("role"):
        parts.append(f"· {str(entity['role']).lower()}")
    parts.append(f"· {status_word}")
    content_text = " ".join(parts)

    entities: list[dict[str, Any]] = [
        {"type": "ramp_transaction", "id": f"user:{entity_id}"},
    ]
    hint = _person_hint(name)
    if hint:
        entities.append(hint)

    content = _compact({
        "object_type": "user",
        "business_id": business_id,
        "entity_id": entity_id,
        "status": status_word,
        "name": name,
        "email": entity.get("email"),
        "role": entity.get("role"),
        "department_id": entity.get("department_id"),
        "is_manager": entity.get("is_manager"),
    })

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=f"ramp:user:{entity_id}",
        external_id=external_id,
        entities_hint=entities,
        raw_payload=entity,
    )


_DRAFT_BUILDERS = {
    "transaction": _transaction_draft,
    "reimbursement": _reimbursement_draft,
    "card": _card_draft,
    "user": _user_draft,
}


def _compact(d: dict[str, Any]) -> dict[str, Any]:
    """Drop absent keys so `content` stays lean (None means 'not present')."""
    return {k: v for k, v in d.items() if v is not None}


def _thin_change_draft(
    entity_kind: str, entity_id: str, business_id: str, *,
    operation: str | None, last_updated: str | None,
    version: str | None = None,
) -> ObservationDraft:
    """A webhook notification with no full entity body (real Ramp deliveries
    carry only the affected `object.id`). Emit a thin change observation; the
    next backfill/poll re-fetch fills the full body (and dedups by the
    state-versioned external_id if unchanged).

    `version` is the stable event `id` (constant across retries — the dedup
    discriminator); when absent the change is versioned by the timestamp."""
    if not business_id or not entity_id:
        raise ValidationError(
            "ramp change missing business_id/id", channel=_CHANNEL,
        )
    ver = version or last_updated or _utcnow().isoformat()
    external_id = _change_external_id(business_id, entity_id, ver)
    occurred = _parse_iso(last_updated) or _utcnow()
    op = operation or "update"
    content_text = (
        f"{entity_kind.replace('_', ' ').title()} {entity_id[:13]} "
        f"{op.lower()} (live)"
    )
    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "object_type": entity_kind,
            "business_id": business_id,
            "entity_id": entity_id,
            "operation": op,
            "thin_change": True,
            "last_updated": last_updated,
        },
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=[{"type": "ramp_transaction",
                        "id": f"{entity_kind}:{entity_id}"}],
        raw_payload=None,
    )


def _business_of(payload: dict[str, Any]) -> str:
    rid = payload.get("_fyralis_business_id") or payload.get("business_id")
    if isinstance(rid, str) and rid:
        return rid
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_ramp_transaction(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("ramp payload must be a JSON object", channel=_CHANNEL)

    # --- LIVE WEBHOOK path (REAL Ramp: flat event) ---
    # Real Ramp deliveries are flat: {"id","type","created_at","business_id",
    # "object":{"id":...}}. `business_id` (root) is the tenant; `type` is
    # dot.notation (e.g. "transactions.cleared"); `object.id` is the affected
    # resource; `id` is the STABLE event id (constant across retries -> the
    # dedup discriminator). No entity body (thin notification — the poll
    # re-fetch fills it). Verified against docs.ramp.com webhooks.
    if payload.get("business_id") and isinstance(payload.get("object"), dict):
        business_id = str(payload.get("business_id") or "")
        ev_type = str(payload.get("type") or "")
        resource = ev_type.split(".", 1)[0] if ev_type else "object"
        action = ev_type.rsplit(".", 1)[-1] if "." in ev_type else "update"
        obj = payload.get("object") or {}
        entity_id = str(obj.get("id") or "")
        created = payload.get("created_at")
        return _thin_change_draft(
            (resource.rstrip("s") or "object"),
            entity_id,
            business_id,
            operation=action,
            last_updated=created if isinstance(created, str) else None,
            version=str(payload.get("id") or "") or None,
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if isinstance(record_type, str) and record_type:
        builder = _DRAFT_BUILDERS.get(record_type.lower())
        if builder is None:
            raise ValidationError(
                f"unsupported ramp record_type {record_type!r}",
                channel=_CHANNEL,
            )
        entity = payload.get("entity") or {}
        return builder(entity, _business_of(payload))

    raise ValidationError(
        "ramp payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_ramp_transaction"]
