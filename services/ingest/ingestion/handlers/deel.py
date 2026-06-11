"""services/ingest/ingestion/handlers/deel.py — Deel payment/contract handler.

ONE channel `deel:payment` (mirrors github:webhook / jira:issue's
one-channel/many-record-types shape). The handler is a pure function (no DB /
network) and branches on the input shape to produce exactly ONE observation per
call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"payment","contract_snapshot"} (set by the fetcher's per-contract
    fan-out).
  - LIVE WEBHOOK: the raw Deel webhook body carries a `type`
    (e.g. "payment.created", "payment.updated", "contract.updated"); the
    handler maps it onto the same record builders so a webhook-delivered change
    and its backfill twin dedup.

Signal mapping (the reasoning value):
  - payment (status pending/sent/posted/paid)  -> kind="signal"
  - payment (status failed/rejected)           -> kind="state_change" (the
                                                  cash-risk signal: a payment
                                                  failed/rejected)
  - contract snapshot                          -> kind="signal" (contract state)

external_id — VERSIONED for the MUTABLE entities (the observations repo dedups
on (source_channel, external_id) IGNORING occurred_at; a status change must land
as a NEW observation, not silently dedup):
  - payment:           deel:{contract_id}:payment:{payment_id}:{status}
  - contract_snapshot: deel:{contract_id}:contract:{updated}

Trust posture: Deel is the system of record for contractor payments ->
`authoritative`.
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


_CHANNEL = "deel:payment"
_TRUST = "authoritative"

# Statuses that represent a cash-risk state change (a payment that did not / will
# not complete). Everything else (pending, sent, posted, paid) is a signal.
_STATE_CHANGE_STATUSES = frozenset({"failed", "rejected"})


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


def _fmt_money(amount: Any) -> str:
    try:
        val = float(amount)
    except (TypeError, ValueError):
        return str(amount)
    return f"${abs(val):,.2f}"


def _direction(amount: Any) -> str:
    try:
        val = float(amount)
    except (TypeError, ValueError):
        return "transfer"
    return "outflow" if val < 0 else "inflow"


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ---------------------------------------------------------------------
# Rich-field extraction (the signals beyond the core money movement)
# ---------------------------------------------------------------------

# Bank identifiers that must NOT land verbatim in the reasoning layer — masked
# to the last 4 chars so the rail/counterparty mapping is still useful without
# leaking full account/routing/IBAN numbers into observations + LLM context.
_SENSITIVE_ROUTING_KEYS = frozenset(
    {"accountNumber", "routingNumber", "account_number", "routing_number", "iban"}
)


def _mask_secret(value: Any) -> Any:
    if isinstance(value, str) and len(value) > 4:
        return "••" + value[-4:]
    return value


def _redact_routing(obj: Any) -> Any:
    """Deep copy of a `details` sub-tree with sensitive bank ids masked."""
    if isinstance(obj, dict):
        return {
            k: (_mask_secret(v) if k in _SENSITIVE_ROUTING_KEYS else _redact_routing(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact_routing(x) for x in obj]
    return obj


def _payment_extras(payment: dict[str, Any]) -> dict[str, Any]:
    """The richer Deel payment fields beyond the core money signal.

    Only present (non-None) keys are returned so `content` stays lean. These are
    already fetched over the wire by the client; the handler is where they were
    previously dropped.
    """
    raw: dict[str, Any] = {
        # cash-risk detail — WHY a payment failed (pairs with state_change).
        "reason_for_failure": payment.get("reasonForFailure") or payment.get("reason_for_failure"),
        "failed_at": payment.get("failedAt") or payment.get("failed_at"),
        # forward cash-flow — projected settlement of a pending payment.
        "estimated_delivery_date": (
            payment.get("estimatedDeliveryDate") or payment.get("estimated_delivery_date")
        ),
        # free spend classification + bookkeeping mapping.
        "deel_category": payment.get("deelCategory") or payment.get("deel_category"),
        "general_ledger_code_name": (
            payment.get("generalLedgerCodeName") or payment.get("general_ledger_code_name")
        ),
        # stable counterparty identity + the memo that travels on the payment.
        "counterparty_id": payment.get("counterpartyId") or payment.get("counterparty_id"),
        "counterparty_nickname": (
            payment.get("counterpartyNickname") or payment.get("counterparty_nickname")
        ),
        "external_memo": payment.get("externalMemo") or payment.get("external_memo"),
        # true all-in cost (links to the fee transaction).
        "fee_id": payment.get("feeId") or payment.get("fee_id"),
        "dashboard_link": payment.get("dashboardLink") or payment.get("dashboard_link"),
        # FX exposure (currency from/to, amount, rate, fee).
        "currency_exchange_info": (
            payment.get("currencyExchangeInfo") or payment.get("currency_exchange_info")
        ),
    }
    extras = {k: v for k, v in raw.items() if v is not None}
    # rail/counterparty-bank mapping (ACH / wire / card), PII-redacted.
    details = payment.get("details")
    if isinstance(details, dict) and details:
        extras["details"] = _redact_routing(details)
    return extras


# ---------------------------------------------------------------------
# Per-record-type draft builders (shared by backfill + webhook paths)
# ---------------------------------------------------------------------

def _first_present(obj: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = obj.get(key)
        if value not in (None, ""):
            return value
    return None


def _nested_value(obj: dict[str, Any], key: str) -> Any:
    value = obj.get(key)
    if value not in (None, ""):
        return value
    for container_key in ("contract", "contractor", "worker", "vendor"):
        nested = obj.get(container_key)
        if isinstance(nested, dict):
            value = nested.get(key)
            if value not in (None, ""):
                return value
    return None


def _payment_draft(payment: dict[str, Any], contract_id: str) -> ObservationDraft:
    payment_id = str(payment.get("id") or payment.get("invoice_id") or "")
    if not contract_id or not payment_id:
        raise ValidationError(
            "deel payment missing contract_id/id", channel=_CHANNEL,
        )
    status = str(payment.get("status") or "unknown").lower()
    amount = _first_present(payment, "amount", "total", "total_amount", "gross_amount")
    counterparty = (
        payment.get("counterpartyName")
        or payment.get("counterparty_name")
        or payment.get("counterparty")
        or payment.get("worker_name")
        or payment.get("contractor_name")
        or payment.get("vendor")
        or _nested_value(payment, "name")
        or payment.get("bankDescription")
        or payment.get("bank_description")
        or "unknown counterparty"
    )
    kind_field = payment.get("kind") or payment.get("type") or "payment"
    occurred = (
        _parse_iso(payment.get("postedAt"))
        or _parse_iso(payment.get("posted_at"))
        or _parse_iso(payment.get("createdAt"))
        or _parse_iso(payment.get("created_at"))
        or _parse_iso(payment.get("issued_at"))
        or _parse_iso(payment.get("invoice_date"))
        or _parse_iso(payment.get("updated_at"))
        or _parse_iso(payment.get("updatedAt"))
        or _utcnow()
    )
    external_id = idempotency.deel_payment(contract_id, payment_id, status)

    is_state_change = status in _STATE_CHANGE_STATUSES

    direction = _direction(amount)
    money = _fmt_money(amount)
    prep = "to" if direction == "outflow" else "from"
    content_text = f"{money} {direction} {prep} {counterparty} · {status} · {kind_field}"
    # Surface WHY a payment failed inline — turns a bare failure into an
    # actionable liquidity / counterparty-risk signal.
    reason = payment.get("reasonForFailure") or payment.get("reason_for_failure")
    if is_state_change and isinstance(reason, str) and reason:
        content_text = f"{content_text} — {reason}"

    entities: list[dict[str, Any]] = [
        {"type": "deel_contract", "id": contract_id},
    ]
    if isinstance(counterparty, str) and counterparty and counterparty != "unknown counterparty":
        entities.append({"type": "organization", "id": counterparty, "role": "counterparty"})

    content: dict[str, Any] = {
        "object_type": "payment",
        "contract_id": contract_id,
        "payment_id": payment_id,
        "amount": amount,
        "direction": direction,
        "status": status,
        "kind": kind_field,
        "counterparty": counterparty,
        "note": payment.get("note"),
        "bank_description": payment.get("bankDescription") or payment.get("bank_description"),
        "posted_at": payment.get("postedAt") or payment.get("posted_at"),
        "created_at": payment.get("createdAt") or payment.get("created_at"),
        "issued_at": payment.get("issued_at"),
        "invoice_date": payment.get("invoice_date"),
    }
    # Merge the richer fields (failure reason, category, GL code, FX, rail
    # routing, memos) — additive, only present keys.
    content.update(_payment_extras(payment))
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
        raw_payload=payment,
    )


def _contract_snapshot_draft(
    contract: dict[str, Any], contract_id: str, updated: str | None,
) -> ObservationDraft:
    if not contract_id:
        raise ValidationError(
            "deel contract snapshot missing contract_id", channel=_CHANNEL,
        )
    occurred = _parse_iso(updated) or _utcnow()
    updated_token = updated or occurred.isoformat()
    external_id = idempotency.deel_contract(contract_id, updated_token)

    name = (
        contract.get("name")
        or contract.get("title")
        or contract.get("contract_name")
        or contract_id
    )
    status = contract.get("status")
    contract_type = (
        contract.get("type")
        or contract.get("contractType")
        or contract.get("contract_type")
    )
    rate = _first_present(contract, "rate", "amount", "total_amount")
    content_text = (
        f"{name} contract ({contract_type or 'contract'}): {status or 'unknown'}"
        f" · {_fmt_money(rate)} (as of {updated_token[:10]})"
    )

    entities: list[dict[str, Any]] = [{"type": "deel_contract", "id": contract_id}]
    content: dict[str, Any] = {
        "object_type": "contract_snapshot",
        "contract_id": contract_id,
        "contract_name": name,
        "contract_type": contract_type,
        "status": status,
        "rate": rate,
        "updated": updated_token,
    }
    # Entity-attribution context for the contract (no account/routing PII).
    for src, dst in (
        ("workerName", "worker_name"),
        ("worker_name", "worker_name"),
        ("legalBusinessName", "legal_business_name"),
        ("legal_business_name", "legal_business_name"),
        ("createdAt", "contract_created_at"),
        ("created_at", "contract_created_at"),
    ):
        if contract.get(src) is not None:
            content[dst] = contract.get(src)
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
        raw_payload=contract,
    )


def _contract_id_of(payload: dict[str, Any], obj: dict[str, Any] | None) -> str:
    """Resolve the contract id for a webhook/backfill record."""
    cid = (
        payload.get("_fyralis_contract_id")
        or payload.get("contractId")
        or payload.get("contract_id")
    )
    if cid not in (None, ""):
        return str(cid)
    if isinstance(obj, dict):
        cand = obj.get("contractId") or obj.get("contract_id")
        if cand in (None, ""):
            contract = obj.get("contract")
            if isinstance(contract, dict):
                cand = contract.get("id") or contract.get("contract_id")
        if cand not in (None, ""):
            return str(cand)
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_deel_payment(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("deel payload must be a JSON object", channel=_CHANNEL)

    # --- LIVE WEBHOOK path (raw Deel webhook body) ---
    event_type = payload.get("type")
    if isinstance(event_type, str) and event_type:
        if event_type.startswith("payment."):
            payment = payload.get("payment") or payload.get("data") or {}
            if isinstance(payment, dict) and payment.get("id"):
                return _payment_draft(payment, _contract_id_of(payload, payment))
            raise ValidationError(
                f"deel {event_type} missing payment", channel=_CHANNEL,
            )
        if event_type.startswith("contract."):
            contract = payload.get("contract") or payload.get("data") or {}
            if isinstance(contract, dict):
                cid = _contract_id_of(payload, contract) or str(contract.get("id") or "")
                return _contract_snapshot_draft(
                    contract, cid, payload.get("updated") or payload.get("as_of"),
                )
        raise ValidationError(
            f"unsupported deel webhook type {event_type!r}", channel=_CHANNEL,
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if record_type == "contract_snapshot":
        return _contract_snapshot_draft(
            payload.get("contract") or {},
            _contract_id_of(payload, payload.get("contract")),
            payload.get("updated") or payload.get("as_of"),
        )
    if record_type == "payment" or "payment" in payload:
        payment = payload.get("payment") or {}
        return _payment_draft(payment, _contract_id_of(payload, payment))

    raise ValidationError(
        "deel payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_deel_payment"]
