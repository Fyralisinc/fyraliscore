"""services/ingest/ingestion/handlers/brex.py — Brex transaction/balance handler.

ONE channel `brex:transaction` (mirrors github:webhook / jira:issue's
one-channel/many-record-types shape). The handler is a pure function (no DB /
network) and branches on the input shape to produce exactly ONE observation per
call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"transaction","account_snapshot"} (set by the fetcher's per-account
    fan-out).
  - LIVE WEBHOOK: the raw Brex webhook body carries a `type`
    (e.g. "transaction.created", "transaction.updated", "account.updated"); the
    handler maps it onto the same record builders so a webhook-delivered change
    and its backfill twin dedup.

Signal mapping (the reasoning value):
  - transaction (status pending/sent/posted) -> kind="signal"
  - transaction (status failed/cancelled)    -> kind="state_change" (the
                                                cash-risk signal: a payment
                                                failed/declined)
  - account balance snapshot                 -> kind="signal" (cash position)

external_id — VERSIONED for the MUTABLE entities (the observations repo dedups
on (source_channel, external_id) IGNORING occurred_at; a status change must land
as a NEW observation, not silently dedup):
  - transaction:      brex:{account_id}:txn:{txn_id}:{status}
  - account_snapshot: brex:{account_id}:balance:{as_of_date}

Trust posture: Brex is the bank's system of record for cash -> `authoritative`.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion import idempotency
from services.ingest.ingestion.handlers import (
    ObservationDraft,
)


_CHANNEL = "brex:transaction"
_TRUST = "authoritative"

# Statuses that represent a cash-risk state change (a payment that did not / will
# not complete). Everything else (pending, sent, posted, completed) is a signal.
_STATE_CHANGE_STATUSES = frozenset({"failed", "cancelled", "canceled", "declined"})


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


def _money_value(amount: Any) -> float | None:
    if isinstance(amount, dict):
        raw = amount.get("amount")
        if raw is None:
            raw = amount.get("value")
        try:
            return float(raw) / 100.0
        except (TypeError, ValueError):
            return None
    try:
        return float(amount)
    except (TypeError, ValueError):
        return None


def _money_currency(amount: Any) -> str | None:
    if isinstance(amount, dict):
        cur = amount.get("currency") or amount.get("currency_code")
        return str(cur).upper() if cur not in (None, "") else None
    return None


def _first_present(obj: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = obj.get(key)
        if value not in (None, ""):
            return value
    return None


def _fmt_money(amount: Any) -> str:
    val = _money_value(amount)
    if val is None:
        return str(amount)
    currency = _money_currency(amount)
    prefix = "$" if currency in (None, "USD") else ""
    suffix = "" if currency in (None, "USD") else f" {currency}"
    return f"{prefix}{abs(val):,.2f}{suffix}"


def _direction(amount: Any) -> str:
    val = _money_value(amount)
    if val is None:
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


def _txn_extras(txn: dict[str, Any]) -> dict[str, Any]:
    """The richer Brex transaction fields beyond the core money signal.

    Only present (non-None) keys are returned so `content` stays lean. These are
    already fetched over the wire by the client; the handler is where they were
    previously dropped.
    """
    raw: dict[str, Any] = {
        # cash-risk detail — WHY a payment failed (pairs with state_change).
        "reason_for_failure": txn.get("reasonForFailure") or txn.get("reason_for_failure"),
        "failed_at": txn.get("failedAt") or txn.get("failed_at"),
        # forward cash-flow — projected settlement of a pending payment.
        "estimated_delivery_date": (
            txn.get("estimatedDeliveryDate") or txn.get("estimated_delivery_date")
        ),
        # free spend classification + bookkeeping mapping.
        "brex_category": txn.get("brexCategory") or txn.get("brex_category"),
        "general_ledger_code_name": (
            txn.get("generalLedgerCodeName") or txn.get("general_ledger_code_name")
        ),
        # stable counterparty identity + the memo that travels on the payment.
        "counterparty_id": txn.get("counterpartyId") or txn.get("counterparty_id"),
        "counterparty_nickname": (
            txn.get("counterpartyNickname") or txn.get("counterparty_nickname")
        ),
        "external_memo": txn.get("externalMemo") or txn.get("external_memo"),
        # true all-in cost (links to the fee transaction).
        "fee_id": txn.get("feeId") or txn.get("fee_id"),
        "dashboard_link": txn.get("dashboardLink") or txn.get("dashboard_link"),
        # FX exposure (currency from/to, amount, rate, fee).
        "currency_exchange_info": (
            txn.get("currencyExchangeInfo") or txn.get("currency_exchange_info")
        ),
    }
    extras = {k: v for k, v in raw.items() if v is not None}
    # rail/counterparty-bank mapping (ACH / wire / card), PII-redacted.
    details = txn.get("details")
    if isinstance(details, dict) and details:
        extras["details"] = _redact_routing(details)
    return extras


# ---------------------------------------------------------------------
# Per-record-type draft builders (shared by backfill + webhook paths)
# ---------------------------------------------------------------------

def _transaction_draft(txn: dict[str, Any], account_id: str) -> ObservationDraft:
    txn_id = str(txn.get("id") or "")
    if not account_id or not txn_id:
        raise ValidationError(
            "brex transaction missing account_id/id", channel=_CHANNEL,
        )
    status = str(txn.get("status") or "unknown").lower()
    amount = txn.get("amount")
    amount_value = _money_value(amount)
    counterparty = (
        txn.get("counterpartyName")
        or txn.get("counterparty_name")
        or txn.get("counterparty")
        or txn.get("bankDescription")
        or txn.get("bank_description")
        or txn.get("merchant_name")
        or txn.get("description")
        or "unknown counterparty"
    )
    kind_field = txn.get("kind") or txn.get("type") or "transaction"
    occurred = (
        _parse_iso(txn.get("postedAt"))
        or _parse_iso(txn.get("posted_at"))
        or _parse_iso(txn.get("createdAt"))
        or _parse_iso(txn.get("created_at"))
        or _parse_iso(txn.get("initiated_at"))
        or _utcnow()
    )
    external_id = idempotency.brex_transaction(account_id, txn_id, status)

    is_state_change = status in _STATE_CHANGE_STATUSES

    direction = _direction(amount)
    money = _fmt_money(amount)
    prep = "to" if direction == "outflow" else "from"
    content_text = f"{money} {direction} {prep} {counterparty} · {status} · {kind_field}"
    # Surface WHY a payment failed inline — turns a bare failure into an
    # actionable liquidity / counterparty-risk signal.
    reason = txn.get("reasonForFailure") or txn.get("reason_for_failure")
    if is_state_change and isinstance(reason, str) and reason:
        content_text = f"{content_text} — {reason}"

    entities: list[dict[str, Any]] = [
        {"type": "brex_account", "id": account_id},
    ]
    if isinstance(counterparty, str) and counterparty and counterparty != "unknown counterparty":
        entities.append({"type": "organization", "id": counterparty, "role": "counterparty"})

    content: dict[str, Any] = {
        "object_type": "transaction",
        "account_id": account_id,
        "transaction_id": txn_id,
        "amount": amount_value if amount_value is not None else amount,
        "direction": direction,
        "status": status,
        "kind": kind_field,
        "counterparty": counterparty,
        "note": txn.get("note"),
        "bank_description": txn.get("bankDescription") or txn.get("bank_description"),
        "posted_at": txn.get("postedAt") or txn.get("posted_at"),
        "created_at": txn.get("createdAt") or txn.get("created_at"),
    }
    if isinstance(amount, dict):
        content["amount_raw"] = amount
        currency = _money_currency(amount)
        if currency:
            content["currency"] = currency
    # Merge the richer fields (failure reason, category, GL code, FX, rail
    # routing, memos) — additive, only present keys.
    content.update(_txn_extras(txn))
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
        raw_payload=txn,
    )


def _account_snapshot_draft(
    account: dict[str, Any], account_id: str, as_of: str | None,
) -> ObservationDraft:
    if not account_id:
        raise ValidationError(
            "brex account snapshot missing account_id", channel=_CHANNEL,
        )
    occurred = _parse_iso(as_of) or _utcnow()
    as_of_date = (as_of or occurred.isoformat())[:10]
    external_id = idempotency.brex_balance(account_id, as_of_date)

    name = account.get("name") or account.get("nickname") or account_id
    available = _first_present(
        account, "availableBalance", "available_balance", "cash_available_balance",
    )
    current = _first_present(account, "currentBalance", "current_balance")
    if available is None:
        available = account.get("balance")
    available_value = _money_value(available)
    current_value = _money_value(current)
    content_text = (
        f"{name} balance: {_fmt_money(available)} available"
        f" / {_fmt_money(current)} current (as of {as_of_date})"
    )

    entities: list[dict[str, Any]] = [{"type": "brex_account", "id": account_id}]
    content: dict[str, Any] = {
        "object_type": "account_snapshot",
        "account_id": account_id,
        "account_name": name,
        "account_kind": (
            account.get("_fyralis_account_kind") or account.get("type") or account.get("kind")
        ),
        "available_balance": available_value if available_value is not None else available,
        "current_balance": current_value if current_value is not None else current,
        "as_of": as_of_date,
    }
    if isinstance(available, dict):
        content["available_balance_raw"] = available
    if isinstance(current, dict):
        content["current_balance_raw"] = current
    # Entity-attribution context for the cash position (no account/routing PII).
    for src, dst in (
        ("status", "account_status"),
        ("legalBusinessName", "legal_business_name"),
        ("legal_business_name", "legal_business_name"),
        ("createdAt", "account_created_at"),
        ("created_at", "account_created_at"),
    ):
        if account.get(src) is not None:
            content[dst] = account.get(src)
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
        raw_payload=account,
    )


def _account_id_of(payload: dict[str, Any], obj: dict[str, Any] | None) -> str:
    """Resolve the account id for a webhook/backfill record."""
    aid = (
        payload.get("_fyralis_account_id")
        or payload.get("accountId")
        or payload.get("account_id")
    )
    if isinstance(aid, str) and aid:
        return aid
    if isinstance(obj, dict):
        cand = obj.get("accountId") or obj.get("account_id")
        if isinstance(cand, str) and cand:
            return cand
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

async def handle_brex_transaction(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("brex payload must be a JSON object", channel=_CHANNEL)

    # --- LIVE WEBHOOK path (raw Brex webhook body) ---
    event_type = payload.get("type")
    if isinstance(event_type, str) and event_type:
        if event_type.startswith("transaction."):
            txn = payload.get("transaction") or payload.get("data") or {}
            if isinstance(txn, dict) and txn.get("id"):
                return _transaction_draft(txn, _account_id_of(payload, txn))
            raise ValidationError(
                f"brex {event_type} missing transaction", channel=_CHANNEL,
            )
        if event_type.startswith("account."):
            account = payload.get("account") or payload.get("data") or {}
            if isinstance(account, dict):
                aid = _account_id_of(payload, account) or str(account.get("id") or "")
                return _account_snapshot_draft(
                    account, aid, payload.get("as_of"),
                )
        raise ValidationError(
            f"unsupported brex webhook type {event_type!r}", channel=_CHANNEL,
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if record_type == "account_snapshot":
        return _account_snapshot_draft(
            payload.get("account") or {},
            _account_id_of(payload, payload.get("account")),
            payload.get("as_of"),
        )
    if record_type == "transaction" or "transaction" in payload:
        txn = payload.get("transaction") or {}
        return _transaction_draft(txn, _account_id_of(payload, txn))

    raise ValidationError(
        "brex payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )




__all__ = ["handle_brex_transaction"]
