"""Deel contracts/payments fixture generator (finance).

`make_deel(contracts=N, payments_per_contract=M, ...)` produces a
deterministic Deel install fixture shaped to feed `MockDeelClient`. It
mirrors the github/gmail generators: every field is derived via `hashlib`
(stable across runs), timestamps land in 2026-01, and the shape is exactly what
the mock client paginates over.

Fixture shape (consumed by `MockDeelClient(fixture=...)`):
    {
      "contracts": {
        "<contract_id>": {
          # the `GET /contract/{id}` body (state snapshot probe).
          "id": "<contract_id>",
          "name": "...", "type": "ongoing_time_based", "status": "in_progress",
          "rate": 12345.67, "createdAt": "2026-01-05T00:00:00Z", ...,
          # newest-first payment list paginated by the mock client.
          "payments": [ {<payment>}, ... ],
        },
        ...
      },
      "contract_order": ["<contract_id>", ...],   # planner shard order
      "page_size": 100,
    }

The fetcher (services/ingest/ingestion/fetchers/deel.py) emits, per contract:
ONE `contract_snapshot` (first page only) + ONE `payment` record per payment — so
the observation count per contract is `1 + payments_per_contract`.
"""
from __future__ import annotations

import hashlib
from typing import Any


# Default per-contract kinds (cycled across contracts). Mirrors Deel's
# `type` field on the contract body.
_DEFAULT_CONTRACT_KINDS = ("ongoing_time_based", "milestone")

# Payment statuses cycled across a single contract's stream. None map to the
# handler's cash-risk `_STATE_CHANGE_STATUSES` so the happy path is all signals;
# the handler still versions external_id by status either way.
_PAYMENT_STATUSES = ("sent", "posted", "pending")


def make_deel(
    *,
    contracts: int = 1,
    payments_per_contract: int = 4,
    contract_kinds: list[str] | None = None,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
    seed: str = "",
) -> dict[str, Any]:
    """Build a deterministic Deel install fixture.

    Args:
      contracts: Number of contracts (one shard each in the planner).
      payments_per_contract: Payments on each contract's stream.
      contract_kinds: Optional override for the per-contract `type` cycle.
      base_iso: Anchor timestamp (2026-01); payments are spaced backwards from it
        so the list is newest-first (Deel's ordering).
      page_size: The mock client's per-page cap for `list_payments`.
      seed: Optional namespacing salt mixed into the synthetic `contract_id`
        (which is what the payment `external_id` keys on,
        `deel:{contract_id}:payment:…`). Default "" preserves the original ids;
        a per-tenant value (e.g. the tenant slug) makes the contract_ids — and
        therefore every observation's external_id — tenant-unique, mirroring
        production where each tenant's Deel org has distinct contract ids.
        Without it, a multi-tenant synthetic run collides on the global
        `observations` UNIQUE(source_channel, external_id, occurred_at) index.

    Returns:
      Fixture dict consumable by `MockDeelClient(fixture=...)`.
    """
    kinds = contract_kinds or list(_DEFAULT_CONTRACT_KINDS)
    base_date = base_iso[:10]  # YYYY-MM-DD anchor for spacing.

    contracts_map: dict[str, dict[str, Any]] = {}
    contract_order: list[str] = []
    for c in range(contracts):
        # seed="" reproduces the original ids (back-compat for existing tests);
        # a non-empty seed namespaces the contract_id per tenant.
        _con_parts = ("deel-contract", c) if not seed else ("deel-contract", seed, c)
        contract_id = f"con_{_digest(*_con_parts)[:16]}"
        contract_order.append(contract_id)
        kind = kinds[c % len(kinds)]
        # Deterministic-but-varied rate (cents precision).
        rate = round(10_000.0 + (int(_digest(contract_id)[:6], 16) % 500_000) / 100.0, 2)
        payments = [
            _payment(contract_id, idx, base_date)
            for idx in range(payments_per_contract)
        ]
        contracts_map[contract_id] = {
            "id": contract_id,
            "name": f"Contractor Agreement {c + 1}",
            "title": f"Contractor Agreement {c + 1}",
            "type": kind,
            "contractType": kind,
            "status": "in_progress",
            "workerName": f"Worker {c + 1}",
            "legalBusinessName": "Acme Robotics, Inc.",
            "rate": rate,
            "amount": rate,
            "createdAt": f"{base_date}T00:00:00Z",
            # Newest-first payment stream (the mock paginates this slice).
            "payments": payments,
        }

    return {
        "contracts": contracts_map,
        "contract_order": contract_order,
        "page_size": page_size,
    }


def _payment(contract_id: str, idx: int, base_date: str) -> dict[str, Any]:
    """One deterministic Deel payment, newest-first by `idx`.

    idx=0 is the newest; later indices are older — matching Deel's
    listing order. Both `postedAt` and `createdAt` land in 2026-01 so the
    handler's occurred_at (postedAt|createdAt) is always a 2026 timestamp.
    """
    payment_id = f"pay_{_digest(contract_id, 'pay', idx)[:20]}"
    status = _PAYMENT_STATUSES[idx % len(_PAYMENT_STATUSES)]
    # Space payments one hour apart, newest first: idx 0 -> 23:00, etc.
    hour = 23 - (idx % 24)
    iso = f"{base_date}T{hour:02d}:00:00Z"
    # Alternate inflow/outflow; amount derived from the digest for variety.
    magnitude = round((int(_digest(payment_id)[:5], 16) % 100_000) / 100.0 + 1.0, 2)
    amount = magnitude if idx % 2 == 0 else -magnitude
    direction = "credit" if amount >= 0 else "debit"
    counterparty = f"Counterparty {_digest(payment_id, 'cp')[:6]}"
    return {
        "id": payment_id,
        "contractId": contract_id,
        "status": status,
        "amount": amount,
        "kind": direction,
        "counterpartyName": counterparty,
        "counterpartyId": f"cp_{_digest(counterparty)[:10]}",
        "bankDescription": f"{direction.upper()} {counterparty}",
        "note": f"payment {idx} on {contract_id}",
        "externalMemo": f"memo-{idx}",
        "deelCategory": "Vendors" if amount < 0 else "Revenue",
        "postedAt": iso,
        "createdAt": iso,
        "details": {
            "rail": "ach" if idx % 2 == 0 else "wire",
            "accountNumber": f"{_digest(payment_id, 'acctno')[:12]}",
            "routingNumber": f"{_digest(payment_id, 'routing')[:9]}",
        },
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_deel"]
