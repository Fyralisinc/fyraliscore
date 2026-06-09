"""Mercury accounts/transactions fixture generator (finance).

`make_mercury(accounts=N, transactions_per_account=M, ...)` produces a
deterministic Mercury install fixture shaped to feed `MockMercuryClient`. It
mirrors the github/gmail generators: every field is derived via `hashlib`
(stable across runs), timestamps land in 2026-01, and the shape is exactly what
the mock client paginates over.

Fixture shape (consumed by `MockMercuryClient(fixture=...)`):
    {
      "accounts": {
        "<account_id>": {
          # the `GET /account/{id}` body (balance snapshot probe).
          "id": "<account_id>",
          "name": "...", "type": "checking", "status": "active",
          "availableBalance": 12345.67, "currentBalance": 12345.67,
          "createdAt": "2026-01-05T00:00:00Z", ...,
          # newest-first transaction list paginated by the mock client.
          "transactions": [ {<txn>}, ... ],
        },
        ...
      },
      "account_order": ["<account_id>", ...],   # planner shard order
      "page_size": 100,
    }

The fetcher (services/ingest/ingestion/fetchers/mercury.py) emits, per account:
ONE `account_snapshot` (first page only) + ONE `transaction` record per txn — so
the observation count per account is `1 + transactions_per_account`.
"""
from __future__ import annotations

import hashlib
from typing import Any


# Default per-account kinds (cycled across accounts). Mirrors Mercury's
# `type` field on the account body.
_DEFAULT_ACCOUNT_KINDS = ("checking", "savings")

# Transaction statuses cycled across a single account's stream. None map to the
# handler's cash-risk `_STATE_CHANGE_STATUSES` so the happy path is all signals;
# the handler still versions external_id by status either way.
_TXN_STATUSES = ("sent", "posted", "pending")


def make_mercury(
    *,
    accounts: int = 1,
    transactions_per_account: int = 4,
    account_kinds: list[str] | None = None,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
    seed: str = "",
) -> dict[str, Any]:
    """Build a deterministic Mercury install fixture.

    Args:
      accounts: Number of accounts (one shard each in the planner).
      transactions_per_account: Transactions on each account's stream.
      account_kinds: Optional override for the per-account `type` cycle.
      base_iso: Anchor timestamp (2026-01); txns are spaced backwards from it
        so the list is newest-first (Mercury's ordering).
      page_size: The mock client's per-page cap for `list_transactions`.
      seed: Optional namespacing salt mixed into the synthetic `account_id`
        (which is what the transaction `external_id` keys on,
        `mercury:{account_id}:txn:…`). Default "" preserves the original ids;
        a per-tenant value (e.g. the tenant slug) makes the account_ids — and
        therefore every observation's external_id — tenant-unique, mirroring
        production where each tenant's Mercury org has distinct account ids.
        Without it, a multi-tenant synthetic run collides on the global
        `observations` UNIQUE(source_channel, external_id, occurred_at) index.

    Returns:
      Fixture dict consumable by `MockMercuryClient(fixture=...)`.
    """
    kinds = account_kinds or list(_DEFAULT_ACCOUNT_KINDS)
    base_date = base_iso[:10]  # YYYY-MM-DD anchor for spacing.

    accounts_map: dict[str, dict[str, Any]] = {}
    account_order: list[str] = []
    for a in range(accounts):
        # seed="" reproduces the original ids (back-compat for existing tests);
        # a non-empty seed namespaces the account_id per tenant.
        _acct_parts = ("mercury-account", a) if not seed else ("mercury-account", seed, a)
        account_id = f"acct_{_digest(*_acct_parts)[:16]}"
        account_order.append(account_id)
        kind = kinds[a % len(kinds)]
        # Deterministic-but-varied balance (cents precision).
        available = round(10_000.0 + (int(_digest(account_id)[:6], 16) % 500_000) / 100.0, 2)
        txns = [
            _txn(account_id, idx, base_date)
            for idx in range(transactions_per_account)
        ]
        accounts_map[account_id] = {
            "id": account_id,
            "name": f"Operating {kind.title()} {a + 1}",
            "nickname": f"Acct {a + 1}",
            "type": kind,
            "kind": kind,
            "status": "active",
            "legalBusinessName": "Acme Robotics, Inc.",
            "availableBalance": available,
            "currentBalance": available,
            "createdAt": f"{base_date}T00:00:00Z",
            # Newest-first transaction stream (the mock paginates this slice).
            "transactions": txns,
        }

    return {
        "accounts": accounts_map,
        "account_order": account_order,
        "page_size": page_size,
    }


def _txn(account_id: str, idx: int, base_date: str) -> dict[str, Any]:
    """One deterministic Mercury transaction, newest-first by `idx`.

    idx=0 is the newest; later indices are older — matching Mercury's
    listing order. Both `postedAt` and `createdAt` land in 2026-01 so the
    handler's occurred_at (postedAt|createdAt) is always a 2026 timestamp.
    """
    txn_id = f"txn_{_digest(account_id, 'txn', idx)[:20]}"
    status = _TXN_STATUSES[idx % len(_TXN_STATUSES)]
    # Space transactions one hour apart, newest first: idx 0 -> 23:00, etc.
    hour = 23 - (idx % 24)
    iso = f"{base_date}T{hour:02d}:00:00Z"
    # Alternate inflow/outflow; amount derived from the digest for variety.
    magnitude = round((int(_digest(txn_id)[:5], 16) % 100_000) / 100.0 + 1.0, 2)
    amount = magnitude if idx % 2 == 0 else -magnitude
    direction = "credit" if amount >= 0 else "debit"
    counterparty = f"Counterparty {_digest(txn_id, 'cp')[:6]}"
    return {
        "id": txn_id,
        "accountId": account_id,
        "status": status,
        "amount": amount,
        "kind": direction,
        "counterpartyName": counterparty,
        "counterpartyId": f"cp_{_digest(counterparty)[:10]}",
        "bankDescription": f"{direction.upper()} {counterparty}",
        "note": f"transaction {idx} on {account_id}",
        "externalMemo": f"memo-{idx}",
        "mercuryCategory": "Vendors" if amount < 0 else "Revenue",
        "postedAt": iso,
        "createdAt": iso,
        "details": {
            "rail": "ach" if idx % 2 == 0 else "wire",
            "accountNumber": f"{_digest(txn_id, 'acctno')[:12]}",
            "routingNumber": f"{_digest(txn_id, 'routing')[:9]}",
        },
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_mercury"]
