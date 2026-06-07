"""QuickBooks Online entity fixture generator (finance, IN-FIN).

`make_quickbooks(realm_id=..., entities=[...], rows_per_entity=N)` produces a
deterministic per-entity-type fixture shaped to feed `MockQuickBooksClient`. The
mock paginates each entity list by QBO's offset cursor (`STARTPOSITION` /
`start_position`) and the fetcher drives one `quickbooks_entity` shard per entity
type.

Each generated entity carries exactly the fields the `quickbooks:object` handler
reads (handlers/quickbooks.py):
  - `Id`, `SyncToken`, `MetaData.LastUpdatedTime` (every entity),
  - `TotalAmt`, `Balance`, `DueDate`, `CustomerRef`/`VendorRef` for the AR/AP
    entities (Invoice / Bill),
  - a `Line` item so the handler's rich-field extraction has something to lift.

Determinism: timestamps are spaced one minute apart, oldest first, anchored at
`base_iso`; ids/amounts are derived from a stable SHA-256 digest of
(realm_id, entity_type, idx). Re-running with the same args yields byte-identical
output.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


# The four transactional entities the planner shards on (client.DEFAULT_ENTITIES).
DEFAULT_ENTITIES: tuple[str, ...] = ("Invoice", "Bill", "BillPayment", "Payment")

# Which entities carry the AR/AP balance signal (Invoice -> AR, Bill -> AP).
_AR_AP = {"Invoice", "Bill"}


def make_quickbooks(
    *,
    realm_id: str = "9341452000000001",
    entities: list[str] | None = None,
    rows_per_entity: int = 1,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
) -> dict[str, Any]:
    """Build a QuickBooks realm fixture.

    Args:
      realm_id: QBO company realm id (stamped into refs + returned at top level).
      entities: Entity types to generate; defaults to the four transactional
        entities ("Invoice", "Bill", "BillPayment", "Payment").
      rows_per_entity: Number of rows generated for EACH entity type.
      base_iso: Anchor for the (deterministic, 1-min-spaced) LastUpdatedTime
        timestamps. Accepts "...Z" or an explicit offset.
      page_size: The mock client's per-query MAXRESULTS cap (so callers can drive
        multi-page pagination by setting rows_per_entity > page_size).

    Returns:
      Fixture dict consumed by `MockQuickBooksClient(fixture=...)`:
        {
          "realm_id": "...",
          "page_size": 100,
          "entities": {
            "Invoice": [ {<full QBO entity>}, ... ],   # ordered oldest-first
            "Bill":    [ ... ],
            ...
          },
        }
    """
    ents = list(entities) if entities is not None else list(DEFAULT_ENTITIES)
    base = _parse_iso(base_iso)

    entities_out: dict[str, list[dict[str, Any]]] = {}
    for entity_type in ents:
        rows = [
            _entity(realm_id, entity_type, idx, base)
            for idx in range(rows_per_entity)
        ]
        entities_out[entity_type] = rows

    return {
        "realm_id": realm_id,
        "page_size": page_size,
        "entities": entities_out,
    }


# ---------------------------------------------------------------------
# Per-entity builders
# ---------------------------------------------------------------------

def _entity(
    realm_id: str, entity_type: str, idx: int, base: datetime,
) -> dict[str, Any]:
    # ISO LastUpdatedTime spaced 1 minute apart, oldest first, with offset.
    updated = (base + timedelta(minutes=idx)).isoformat()
    seed = _digest(realm_id, entity_type, idx)
    entity_id = str(1000 + idx)
    # A nonzero SyncToken on some rows exercises external_id versioning.
    sync_token = str(idx % 3)
    total = round(100.0 + (int(seed[:6], 16) % 900_000) / 100.0, 2)

    entity: dict[str, Any] = {
        "Id": entity_id,
        "SyncToken": sync_token,
        "DocNumber": f"{entity_type[:3].upper()}-{entity_id}",
        "TotalAmt": total,
        "MetaData": {
            "CreateTime": (base - timedelta(days=1)).isoformat(),
            "LastUpdatedTime": updated,
        },
    }

    if entity_type in _AR_AP:
        # AR/AP entities carry Balance + DueDate (the handler's paid/overdue
        # signal) plus the customer/vendor party.
        # Even rows are fully paid (Balance 0 -> state_change "paid"); odd rows
        # stay open. A far-future DueDate keeps "open" rows out of "overdue".
        balance = 0.0 if idx % 2 == 0 else total
        entity["Balance"] = balance
        entity["DueDate"] = (base + timedelta(days=30)).date().isoformat()
        entity["TxnDate"] = base.date().isoformat()
        if entity_type == "Invoice":
            entity["CustomerRef"] = {
                "value": str(1 + idx), "name": f"Customer-{seed[:6]}",
            }
            entity["Line"] = [{
                "Id": "1",
                "Amount": total,
                "Description": "Platform License",
                "DetailType": "SalesItemLineDetail",
                "SalesItemLineDetail": {
                    "ItemRef": {"value": "5", "name": "Platform License"},
                    "Qty": 1,
                    "UnitPrice": total,
                },
            }]
        else:  # Bill
            entity["VendorRef"] = {
                "value": str(1 + idx), "name": f"Vendor-{seed[:6]}",
            }
            entity["Line"] = [{
                "Id": "1",
                "Amount": total,
                "Description": "Cloud Infra",
                "DetailType": "AccountBasedExpenseLineDetail",
                "AccountBasedExpenseLineDetail": {
                    "AccountRef": {"value": "33", "name": "Cloud Infra"},
                },
            }]
    else:
        # Payment / BillPayment are pure cash events (signal only). Payment is
        # customer-side AR cash; BillPayment is vendor-side AP cash.
        if entity_type == "Payment":
            entity["CustomerRef"] = {
                "value": str(1 + idx), "name": f"Customer-{seed[:6]}",
            }
        else:  # BillPayment
            entity["VendorRef"] = {
                "value": str(1 + idx), "name": f"Vendor-{seed[:6]}",
            }
        entity["Line"] = [{
            "Amount": total,
            "LinkedTxn": [{"TxnId": entity_id, "TxnType": "Invoice"
                           if entity_type == "Payment" else "Bill"}],
        }]

    return entity


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _parse_iso(value: str) -> datetime:
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_quickbooks", "DEFAULT_ENTITIES"]
