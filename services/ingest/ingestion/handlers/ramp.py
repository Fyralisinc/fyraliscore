"""services/ingest/ingestion/handlers/ramp.py — Ramp transaction handler (finance).

Cloned from the QuickBooks archetype. ONE channel `ramp:transaction` (mirrors
jira:issue's one-channel/many-record-types shape). The handler is a pure function
(no DB / network) and branches on the input shape to produce exactly ONE
observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"invoice","bill","billpayment","payment"} (set by the fetcher; kept as the
    archetype taxonomy until the verified Ramp taxonomy lands — see fetcher TODO).
  - LIVE WEBHOOK: a Ramp `eventNotifications` entity entry (or a CloudEvents
    wrapper); the handler maps the entity `name`+`operation` onto the same record
    builders so a webhook-delivered change and its backfill twin dedup. The
    webhook only carries the entity id + operation, so when no full entity body
    is present the handler emits a thin change observation keyed the same way.

Signal mapping (the reasoning value) — blueprint §4 state predicate:
  - state ∈ {declined, disputed}                            -> kind="state_change"
  - invoice/bill that is fully paid (Balance == 0)          -> kind="state_change"
    (the AR/AP-health signal: receivable collected / payable cleared)
  - invoice/bill past due with an open balance              -> kind="state_change"
    (overdue — the cash-risk signal)
  - everything else (created / updated, payments)           -> kind="signal"

external_id — VERSIONED by state (blueprint §4; the mutable-source dedup lesson;
the observations repo dedups on (source_channel, external_id) IGNORING occurred_at
so a state change must land as a NEW observation):
  - ramp:{business_id}:txn:{txn_id}:{state}

These external_ids are built INLINE here (not via the shared idempotency module)
so this isolated handler + its tests form a self-consistent loop; the shared
idempotency owner provisions `ramp_transaction` per blueprint §2.8.

Trust posture: Ramp is the spend/card system of record -> `authoritative`.
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


_CHANNEL = "ramp:transaction"
_TRUST = "authoritative"

# States that flip an observation from a plain signal to a state_change
# (blueprint §4: declined / disputed). Kept as a module constant so the verified
# Ramp transaction-state vocabulary can be extended in one place.
# TODO(human): confirm Ramp transaction state vocabulary (declined / disputed /
# other terminal states) and map them here.
_STATE_CHANGE_STATES = frozenset({"declined", "disputed"})


def _txn_external_id(business_id: str, txn_id: str, state: str) -> str:
    """ramp:{business_id}:txn:{txn_id}:{state} — versioned by state (§4).

    Built inline (not via the shared idempotency module) so this isolated
    handler + tests stay self-consistent; the shared `ramp_transaction`
    constructor (§2.8) yields the identical string."""
    return f"ramp:{business_id}:txn:{txn_id}:{state}"


def _change_external_id(business_id: str, txn_id: str, ver: str) -> str:
    """Thin live-webhook variant (body-less notification), versioned by the
    change marker so each live change event stays distinct from its backfill
    twin until the poll re-fetch carries the authoritative body."""
    return f"ramp:{business_id}:txn:{txn_id}:chg:{ver}"

# Map a RAMP entity `name` (webhook) / record_type (backfill) to a canonical kind.
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
        actor = f"ramp:{role}:{rid}" if rid else None
        return actor, (hint if hint.get("id") else None)
    return None, None


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ---------------------------------------------------------------------
# Rich-field extraction (the signals beyond the AR/AP-health header)
# ---------------------------------------------------------------------

def _ref_name(ref: Any) -> str | None:
    """Human name of a RAMP ReferenceType ({value, name}), value as fallback."""
    if isinstance(ref, dict):
        name = ref.get("name")
        if isinstance(name, str) and name:
            return name
        val = ref.get("value")
        return str(val) if val not in (None, "") else None
    return None


def _line_summary(line: dict[str, Any]) -> dict[str, Any]:
    """Flatten one RAMP Line into the bits that carry revenue/expense meaning.

    Covers sales (SalesItemLineDetail) and expense
    (Account-/Item-BasedExpenseLineDetail) lines — the *what was sold / bought*
    that header TotalAmt hides."""
    summary: dict[str, Any] = {
        "amount": line.get("Amount"),
        "description": line.get("Description"),
        "detail_type": line.get("DetailType"),
    }
    sid = line.get("SalesItemLineDetail")
    if isinstance(sid, dict):
        summary["item"] = _ref_name(sid.get("ItemRef"))
        summary["quantity"] = sid.get("Qty")
        summary["unit_price"] = sid.get("UnitPrice")
        summary["class"] = _ref_name(sid.get("ClassRef"))
    for key in ("AccountBasedExpenseLineDetail", "ItemBasedExpenseLineDetail"):
        d = line.get(key)
        if isinstance(d, dict):
            summary["account"] = _ref_name(d.get("AccountRef"))
            summary.setdefault("item", _ref_name(d.get("ItemRef")))
            summary["class"] = _ref_name(d.get("ClassRef"))
            billable = _ref_name(d.get("CustomerRef"))
            if billable:
                summary["billable_customer"] = billable
    return {k: v for k, v in summary.items() if v is not None}


def _entity_extras(entity: dict[str, Any]) -> dict[str, Any]:
    """The richer RAMP entity fields beyond the header money signal.

    Already returned by `SELECT *`; the handler is where they were dropped.
    Only present keys are returned so `content` stays lean."""
    extras: dict[str, Any] = {}

    # Line items — product-level revenue / expense-category breakdown.
    lines = entity.get("Line")
    if isinstance(lines, list):
        summaries = [
            s for ln in lines
            if isinstance(ln, dict)
            and ln.get("DetailType") not in ("SubTotalLineDetail",)
            for s in (_line_summary(ln),) if s
        ]
        if summaries:
            extras["line_items"] = summaries

    # LinkedTxn — the AR/AP graph edges (Invoice<->Payment, PO->Bill->Payment).
    linked = entity.get("LinkedTxn")
    if isinstance(linked, list) and linked:
        edges = [
            {"txn_id": lt.get("TxnId"), "txn_type": lt.get("TxnType")}
            for lt in linked if isinstance(lt, dict) and lt.get("TxnId")
        ]
        if edges:
            extras["linked_txns"] = edges

    # Tax — total + (if present) the lightweight rate breakdown.
    tax = entity.get("TxnTaxDetail")
    if isinstance(tax, dict) and tax:
        tax_out: dict[str, Any] = {}
        if tax.get("TotalTax") is not None:
            tax_out["total_tax"] = tax.get("TotalTax")
        if isinstance(tax.get("TaxLine"), list):
            tax_out["lines"] = len(tax["TaxLine"])
        if tax_out:
            extras["tax"] = tax_out

    # Multi-currency normalization.
    for src, dst in (
        ("HomeTotalAmt", "home_total_amount"),
        ("HomeBalance", "home_balance"),
        ("ExchangeRate", "exchange_rate"),
    ):
        if entity.get(src) is not None:
            extras[dst] = entity.get(src)

    # Cash-position nuance on Payments.
    if entity.get("UnappliedAmt") is not None:
        extras["unapplied_amount"] = entity.get("UnappliedAmt")
    dep = _ref_name(entity.get("DepositToAccountRef"))
    if dep:
        extras["deposit_to_account"] = dep
    ar = _ref_name(entity.get("ARAccountRef"))
    if ar:
        extras["ar_account"] = ar
    ap = _ref_name(entity.get("APAccountRef"))
    if ap:
        extras["ap_account"] = ap

    # Payment channel (how cash moved + funding account).
    pm = _ref_name(entity.get("PaymentMethodRef"))
    if pm:
        extras["payment_method"] = pm
    if entity.get("PaymentRefNum"):
        extras["payment_ref_num"] = entity.get("PaymentRefNum")
    if entity.get("PayType"):
        extras["pay_type"] = entity.get("PayType")

    # Segmentation dimensions for P&L attribution.
    for src, dst in (
        ("ClassRef", "class"),
        ("DepartmentRef", "department"),
        ("ProjectRef", "project"),
    ):
        name = _ref_name(entity.get(src))
        if name:
            extras[dst] = name

    return extras


def _explicit_state(entity: dict[str, Any]) -> str | None:
    """A Ramp-supplied transaction state, if present (declined / disputed / ...).
    TODO(human): confirm the Ramp transaction state field name + value casing."""
    for key in ("state", "status", "TxnStatus"):
        v = entity.get(key)
        if isinstance(v, str) and v:
            return v.lower()
    return None


def _classify(entity_kind: str, entity: dict[str, Any]) -> tuple[str, str]:
    """Return (kind, status_word) for the entity.

    Per blueprint §4 the state predicate wins first: a transaction in a
    declined/disputed state is a state_change. Otherwise the archetype AR/AP
    health rules apply: invoices/bills with a zero balance are 'paid' (a
    state_change — AR collected / AP cleared); those past their due date with an
    open balance are 'overdue' (a state_change — cash-risk). Everything else is a
    plain signal.
    """
    state = _explicit_state(entity)
    if state in _STATE_CHANGE_STATES:
        return "state_change", state  # type: ignore[return-value]
    if entity_kind in ("invoice", "bill"):
        balance = _num(entity.get("Balance"))
        if balance is not None and balance <= 0:
            return "state_change", "paid"
        due = _parse_iso(entity.get("DueDate"))
        if due is not None and due < _utcnow() and (balance or 0) > 0:
            return "state_change", "overdue"
        return "signal", "open"
    # Payments / bill payments are cash events — signal unless an explicit state.
    return "signal", state or "recorded"


# ---------------------------------------------------------------------
# Record builder (shared by backfill + webhook paths)
# ---------------------------------------------------------------------

def _entity_draft(
    entity_kind: str, entity: dict[str, Any], business_id: str,
) -> ObservationDraft:
    entity_id = str(entity.get("Id") or "")
    if not business_id or not entity_id:
        raise ValidationError(
            "ramp entity missing business_id/Id", channel=_CHANNEL,
        )
    sync_token = str(entity.get("SyncToken") or "0")

    updated = _last_updated(entity)
    occurred = _parse_iso(updated) or _utcnow()
    kind, status_word = _classify(entity_kind, entity)
    actor_ref, party_hint = _party(entity)

    # external_id versioned by state (§4). SyncToken is folded into the state
    # token so each per-edit mutation stays distinct (the mutable-source dedup
    # lesson — a coarse status alone would collide across in-place edits).
    state_token = f"{status_word}.{sync_token}" if sync_token != "0" else status_word
    external_id = _txn_external_id(business_id, entity_id, state_token)

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
        {"type": "ramp_transaction", "id": f"{entity_kind}:{entity_id}"},
    ]
    if party_hint:
        entities.append(party_hint)

    content: dict[str, Any] = {
        "object_type": entity_kind,
        "business_id": business_id,
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
    # Merge the richer fields (line items, linked txns, tax, multi-currency,
    # payment channel, P&L dimensions) — additive, only present keys.
    content.update(_entity_extras(entity))
    # Surface line-item depth inline so the reasoning layer sees there's detail.
    line_items = content.get("line_items")
    if isinstance(line_items, list) and line_items:
        n = len(line_items)
        content_text = f"{content_text} · {n} line{'s' if n != 1 else ''}"

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
    entity_kind: str, entity_id: str, business_id: str, *,
    operation: str | None, last_updated: str | None,
) -> ObservationDraft:
    """A webhook notification with no full entity body (body-less webhooks carry
    only id + operation). Emit a thin change observation; the next backfill/poll
    re-fetch fills the full body (and dedups by SyncToken if unchanged).
    TODO(human): confirm Ramp webhook payload shape (body-less vs full entity)."""
    if not business_id or not entity_id:
        raise ValidationError(
            "ramp change missing business_id/id", channel=_CHANNEL,
        )
    # The webhook lacks a SyncToken; version by LastUpdatedTime to keep each
    # change event distinct (the poll re-fetch carries the authoritative body).
    ver = last_updated or _utcnow().isoformat()
    external_id = _change_external_id(business_id, entity_id, ver)
    occurred = _parse_iso(last_updated) or _utcnow()
    op = operation or "Update"
    content_text = f"{entity_kind.replace('_', ' ').title()} #{entity_id} {op.lower()} (live)"
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


def _business_of(payload: dict[str, Any], entity: dict[str, Any] | None) -> str:
    rid = payload.get("_fyralis_business_id") or payload.get("businessId")
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

    # --- LIVE WEBHOOK path (cloned eventNotifications envelope shape) ---
    # Shape: {"eventNotifications":[{"businessId":"...","dataChangeEvent":
    #   {"entities":[{"name":"Invoice","id":"1","operation":"Update",
    #   "lastUpdated":"..."}]}}]}. The finance harness may also send a single
    # flattened {"businessId","name","id","operation","entity"?} for convenience.
    notifications = payload.get("eventNotifications")
    if isinstance(notifications, list) and notifications:
        first = notifications[0]
        business_id = str(first.get("businessId") or "") if isinstance(first, dict) else ""
        dce = first.get("dataChangeEvent") if isinstance(first, dict) else None
        ents = dce.get("entities") if isinstance(dce, dict) else None
        if isinstance(ents, list) and ents:
            ent = ents[0]
            name = str(ent.get("name") or "").lower()
            entity_kind = _ENTITY_NORMALISE.get(name)
            if entity_kind is None:
                raise ValidationError(
                    f"unsupported ramp entity {ent.get('name')!r}",
                    channel=_CHANNEL,
                )
            return _thin_change_draft(
                entity_kind, str(ent.get("id") or ""), business_id,
                operation=ent.get("operation"),
                last_updated=ent.get("lastUpdated"),
            )
        raise ValidationError("ramp webhook missing entities", channel=_CHANNEL)

    # Flattened webhook (harness convenience): a `name` + `id` (+ optional full
    # `entity` body).
    if "name" in payload and "id" in payload and "_fyralis_record_type" not in payload:
        name = str(payload.get("name") or "").lower()
        entity_kind = _ENTITY_NORMALISE.get(name)
        if entity_kind is None:
            raise ValidationError(
                f"unsupported ramp entity {payload.get('name')!r}",
                channel=_CHANNEL,
            )
        business_id = str(payload.get("businessId") or "")
        body = payload.get("entity")
        if isinstance(body, dict) and body.get("Id"):
            return _entity_draft(entity_kind, body, business_id)
        return _thin_change_draft(
            entity_kind, str(payload.get("id") or ""), business_id,
            operation=payload.get("operation"),
            last_updated=payload.get("lastUpdated"),
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if isinstance(record_type, str) and record_type:
        entity_kind = _ENTITY_NORMALISE.get(record_type.lower())
        if entity_kind is None:
            raise ValidationError(
                f"unsupported ramp record_type {record_type!r}",
                channel=_CHANNEL,
            )
        entity = payload.get("entity") or {}
        return _entity_draft(entity_kind, entity, _business_of(payload, entity))

    raise ValidationError(
        "ramp payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_ramp_transaction"]
