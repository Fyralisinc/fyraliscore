"""services/ingestion/handlers/quickbooks.py — QuickBooks entity handler (finance).

ONE channel `quickbooks:object` (mirrors jira:issue's one-channel/many-record-
types shape). The handler is a pure function (no DB / network) and branches on
the input shape to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"invoice","bill","billpayment","payment"} (set by the fetcher).
  - LIVE WEBHOOK: an Intuit `eventNotifications` entity entry (or a CloudEvents
    wrapper); the handler maps the entity `name`+`operation` onto the same record
    builders so a webhook-delivered change and its backfill twin dedup. The
    webhook only carries the entity id + operation, so when no full entity body
    is present the handler emits a thin change observation keyed the same way.

Signal mapping (the reasoning value):
  - invoice/bill that is fully paid (Balance == 0)          -> kind="state_change"
    (the AR/AP-health signal: receivable collected / payable cleared)
  - invoice/bill past due with an open balance              -> kind="state_change"
    (overdue — the cash-risk signal)
  - everything else (created / updated, payments)           -> kind="signal"

external_id — VERSIONED by `SyncToken` (the mutable-source dedup lesson; the
observations repo dedups on (source_channel, external_id) IGNORING occurred_at):
  - qbo:{realm}:{entity}:{id}:{SyncToken}

Trust posture: QuickBooks is the accounting system of record -> `authoritative`.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)


_CHANNEL = "quickbooks:object"
_TRUST = "authoritative"

# Map a QBO entity `name` (webhook) / record_type (backfill) to a canonical kind.
_ENTITY_NORMALISE = {
    "invoice": "invoice",
    "bill": "bill",
    "billpayment": "bill_payment",
    "payment": "payment",
}


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


def _fmt_money(amount: Any) -> str:
    val = _num(amount)
    return f"${val:,.2f}" if val is not None else str(amount)


def _doc_label(entity_kind: str, entity: dict[str, Any]) -> str:
    """Human reference like 'Invoice #1037' / 'Bill 9921'."""
    doc = entity.get("DocNumber") or entity.get("Id") or "?"
    nice = entity_kind.replace("_", " ").title()
    return f"{nice} #{doc}"


def _party(entity: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    """(actor_ref, entity_hint) for the customer/vendor on the doc."""
    ref = entity.get("CustomerRef") or entity.get("VendorRef")
    if isinstance(ref, dict):
        name = ref.get("name")
        rid = ref.get("value")
        role = "customer" if "CustomerRef" in entity else "vendor"
        hint: dict[str, Any] = {"type": "organization", "role": role}
        if name:
            hint["id"] = name
        elif rid:
            hint["id"] = str(rid)
        actor = f"qbo:{role}:{rid}" if rid else None
        return actor, (hint if hint.get("id") else None)
    return None, None


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _classify(entity_kind: str, entity: dict[str, Any]) -> tuple[str, str]:
    """Return (kind, status_word) for the entity.

    Invoices/bills with a zero balance are 'paid' (a state_change — AR collected
    / AP cleared); those past their due date with an open balance are 'overdue'
    (a state_change — cash-risk). Everything else is an 'open' signal.
    """
    if entity_kind in ("invoice", "bill"):
        balance = _num(entity.get("Balance"))
        if balance is not None and balance <= 0:
            return "state_change", "paid"
        due = _parse_iso(entity.get("DueDate"))
        if due is not None and due < _utcnow() and (balance or 0) > 0:
            return "state_change", "overdue"
        return "signal", "open"
    # Payments / bill payments are cash events — always a signal.
    return "signal", "recorded"


# ---------------------------------------------------------------------
# Record builder (shared by backfill + webhook paths)
# ---------------------------------------------------------------------

def _entity_draft(
    entity_kind: str, entity: dict[str, Any], realm_id: str,
) -> ObservationDraft:
    entity_id = str(entity.get("Id") or "")
    if not realm_id or not entity_id:
        raise ValidationError(
            "quickbooks entity missing realm_id/Id", channel=_CHANNEL,
        )
    sync_token = str(entity.get("SyncToken") or "0")
    external_id = f"qbo:{realm_id}:{entity_kind}:{entity_id}:{sync_token}"

    updated = _last_updated(entity)
    occurred = _parse_iso(updated) or _utcnow()
    kind, status_word = _classify(entity_kind, entity)
    actor_ref, party_hint = _party(entity)

    total = entity.get("TotalAmt")
    balance = entity.get("Balance")
    label = _doc_label(entity_kind, entity)
    who = (party_hint or {}).get("id")
    parts = [label]
    if who:
        parts.append(f"· {who}")
    parts.append(f"· {_fmt_money(total)}")
    if entity_kind in ("invoice", "bill"):
        parts.append(f"· {status_word} (bal {_fmt_money(balance)})")
    else:
        parts.append(f"· {status_word}")
    content_text = " ".join(parts)

    entities: list[dict[str, Any]] = [
        {"type": "quickbooks_object", "id": f"{entity_kind}:{entity_id}"},
    ]
    if party_hint:
        entities.append(party_hint)

    content: dict[str, Any] = {
        "object_type": entity_kind,
        "realm_id": realm_id,
        "entity_id": entity_id,
        "sync_token": sync_token,
        "doc_number": entity.get("DocNumber"),
        "status": status_word,
        "total_amount": total,
        "balance": balance,
        "currency": (entity.get("CurrencyRef") or {}).get("value")
        if isinstance(entity.get("CurrencyRef"), dict) else None,
        "txn_date": entity.get("TxnDate"),
        "due_date": entity.get("DueDate"),
        "customer": (entity.get("CustomerRef") or {}).get("name")
        if isinstance(entity.get("CustomerRef"), dict) else None,
        "vendor": (entity.get("VendorRef") or {}).get("name")
        if isinstance(entity.get("VendorRef"), dict) else None,
        "last_updated": updated,
    }

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
    entity_kind: str, entity_id: str, realm_id: str, *,
    operation: str | None, last_updated: str | None,
) -> ObservationDraft:
    """A webhook notification with no full entity body (Intuit webhooks carry
    only id + operation). Emit a thin change observation; the next backfill/poll
    re-fetch fills the full body (and dedups by SyncToken if unchanged)."""
    if not realm_id or not entity_id:
        raise ValidationError(
            "quickbooks change missing realm_id/id", channel=_CHANNEL,
        )
    # The webhook lacks a SyncToken; version by LastUpdatedTime to keep each
    # change event distinct (the poll re-fetch carries the authoritative body).
    ver = last_updated or _utcnow().isoformat()
    external_id = f"qbo:{realm_id}:{entity_kind}:{entity_id}:chg:{ver}"
    occurred = _parse_iso(last_updated) or _utcnow()
    op = operation or "Update"
    content_text = f"{entity_kind.replace('_', ' ').title()} #{entity_id} {op.lower()} (live)"
    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "object_type": entity_kind,
            "realm_id": realm_id,
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
        entities_hint=[{"type": "quickbooks_object",
                        "id": f"{entity_kind}:{entity_id}"}],
        raw_payload=None,
    )


def _realm_of(payload: dict[str, Any], entity: dict[str, Any] | None) -> str:
    rid = payload.get("_fyralis_realm_id") or payload.get("realmId")
    if isinstance(rid, str) and rid:
        return rid
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_quickbooks_object(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("quickbooks payload must be a JSON object", channel=_CHANNEL)

    # --- LIVE WEBHOOK path (Intuit eventNotifications) ---
    # Shape: {"eventNotifications":[{"realmId":"...","dataChangeEvent":
    #   {"entities":[{"name":"Invoice","id":"1","operation":"Update",
    #   "lastUpdated":"..."}]}}]}. The finance harness may also send a single
    # flattened {"realmId","name","id","operation","entity"?} for convenience.
    notifications = payload.get("eventNotifications")
    if isinstance(notifications, list) and notifications:
        first = notifications[0]
        realm_id = str(first.get("realmId") or "") if isinstance(first, dict) else ""
        dce = first.get("dataChangeEvent") if isinstance(first, dict) else None
        ents = dce.get("entities") if isinstance(dce, dict) else None
        if isinstance(ents, list) and ents:
            ent = ents[0]
            name = str(ent.get("name") or "").lower()
            entity_kind = _ENTITY_NORMALISE.get(name)
            if entity_kind is None:
                raise ValidationError(
                    f"unsupported quickbooks entity {ent.get('name')!r}",
                    channel=_CHANNEL,
                )
            return _thin_change_draft(
                entity_kind, str(ent.get("id") or ""), realm_id,
                operation=ent.get("operation"),
                last_updated=ent.get("lastUpdated"),
            )
        raise ValidationError("quickbooks webhook missing entities", channel=_CHANNEL)

    # Flattened webhook (harness convenience): a `name` + `id` (+ optional full
    # `entity` body).
    if "name" in payload and "id" in payload and "_fyralis_record_type" not in payload:
        name = str(payload.get("name") or "").lower()
        entity_kind = _ENTITY_NORMALISE.get(name)
        if entity_kind is None:
            raise ValidationError(
                f"unsupported quickbooks entity {payload.get('name')!r}",
                channel=_CHANNEL,
            )
        realm_id = str(payload.get("realmId") or "")
        body = payload.get("entity")
        if isinstance(body, dict) and body.get("Id"):
            return _entity_draft(entity_kind, body, realm_id)
        return _thin_change_draft(
            entity_kind, str(payload.get("id") or ""), realm_id,
            operation=payload.get("operation"),
            last_updated=payload.get("lastUpdated"),
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if isinstance(record_type, str) and record_type:
        entity_kind = _ENTITY_NORMALISE.get(record_type.lower())
        if entity_kind is None:
            raise ValidationError(
                f"unsupported quickbooks record_type {record_type!r}",
                channel=_CHANNEL,
            )
        entity = payload.get("entity") or {}
        return _entity_draft(entity_kind, entity, _realm_of(payload, entity))

    raise ValidationError(
        "quickbooks payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_quickbooks_object"]
