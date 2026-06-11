"""Ramp entity fixture generator (finance, IN-FIN).

`make_ramp(business_id=..., entities=[...], rows_per_entity=N)` produces a
deterministic per-entity-type fixture shaped to feed `MockRampClient`. Rows are
REAL-shaped Ramp Developer API resources (verified docs.ramp.com OpenAPI):
the mock paginates each entity list by the real KEYSET scheme (`start=<last
entity id>` embedded in a `page.next` URL) and the fetcher drives one
`ramp_entity` shard per entity type.

Entity taxonomy: `transaction` / `reimbursement` / `card` / `user`.

Each generated entity carries exactly the fields the `ramp:transaction` handler
reads (handlers/ramp.py):
  - transaction    : `id`, `state` (CLEARED, every 4th DECLINED), `amount`
                     (major-unit number) + `currency_code`,
                     `original_transaction_amount` (minor-unit object),
                     `user_transaction_time`, `merchant_name`,
                     `sk_category_name`, `card_holder`, `line_items`.
  - reimbursement  : `id`, `state` (PENDING, every other REIMBURSED), `amount`
                     + `currency`, `payee_amount` (minor-unit object),
                     `created_at` / `updated_at`, `user_full_name`, `merchant`.
  - card           : `id`, `state` (ACTIVE, every 3rd SUSPENDED),
                     `display_name`, `last_four`, `cardholder_name`,
                     `created_at`.
  - user           : `id`, `status` (USER_ACTIVE, every 3rd USER_INACTIVE),
                     `first_name`/`last_name`, `email`, `role`.

Determinism: timestamps are spaced one minute apart, oldest first, anchored at
`base_iso`; ids/amounts are derived from a stable SHA-256 digest of
(business_id, entity_type, idx). Re-running with the same args yields
byte-identical output.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


# The four streams the planner shards on (client.DEFAULT_ENTITIES).
DEFAULT_ENTITIES: tuple[str, ...] = ("transaction", "reimbursement", "card", "user")


def make_ramp(
    *,
    business_id: str = "bus-9341452000000001",
    entities: list[str] | None = None,
    rows_per_entity: int = 1,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
) -> dict[str, Any]:
    """Build a Ramp business fixture.

    Args:
      business_id: Ramp business id (stamped into records + returned at top
        level; the webhook root `business_id` twin).
      entities: Entity types to generate; defaults to the four streams
        ("transaction", "reimbursement", "card", "user").
      rows_per_entity: Number of rows generated for EACH entity type.
      base_iso: Anchor for the (deterministic, 1-min-spaced) timestamps.
        Accepts "...Z" or an explicit offset.
      page_size: The mock client's per-page cap (so callers can drive
        multi-page keyset pagination by setting rows_per_entity > page_size).

    Returns:
      Fixture dict consumed by `MockRampClient(fixture=...)`:
        {
          "business_id": "...",
          "page_size": 100,
          "entities": {
            "transaction":   [ {<real-shaped Ramp transaction>}, ... ],
            "reimbursement": [ ... ],
            "card":          [ ... ],
            "user":          [ ... ],
          },
        }
      Rows are ordered oldest-first by their stream timestamp.
    """
    ents = list(entities) if entities is not None else list(DEFAULT_ENTITIES)
    base = _parse_iso(base_iso)

    entities_out: dict[str, list[dict[str, Any]]] = {}
    for entity_type in ents:
        rows = [
            _entity(business_id, entity_type, idx, base)
            for idx in range(rows_per_entity)
        ]
        entities_out[entity_type] = rows

    return {
        "business_id": business_id,
        "page_size": page_size,
        "entities": entities_out,
    }


# ---------------------------------------------------------------------
# Per-entity builders (real Ramp Developer API shapes)
# ---------------------------------------------------------------------

def _entity(
    business_id: str, entity_type: str, idx: int, base: datetime,
) -> dict[str, Any]:
    # ISO timestamp spaced 1 minute apart, oldest first, with offset.
    ts = (base + timedelta(minutes=idx)).isoformat()
    seed = _digest(business_id, entity_type, idx)
    entity_id = _uuidish(seed)
    dollars = round(100.0 + (int(seed[:6], 16) % 900_000) / 100.0, 2)
    cents = int(round(dollars * 100))
    person_seed = seed[6:12]

    if entity_type == "transaction":
        # Every 4th transaction is DECLINED (a state_change); the rest cleared.
        state = "DECLINED" if idx % 4 == 3 else "CLEARED"
        return {
            "id": entity_id,
            "state": state,
            "amount": dollars,                       # major units (dollars)
            "currency_code": "USD",
            "original_transaction_amount": {         # minor units (cents)
                "amount": cents,
                "currency_code": "USD",
                "minor_unit_conversion_rate": 100,
            },
            "user_transaction_time": ts,
            "settlement_date": ts,
            "merchant_name": f"Merchant-{person_seed}",
            "merchant_id": _uuidish(seed[2:] + "m"),
            "sk_category_id": 1 + (idx % 5),
            "sk_category_name": "Cloud Computing",
            "memo": None,
            "card_holder": {
                "first_name": "Card",
                "last_name": f"Holder-{person_seed}",
                "department_name": "Engineering",
                "user_id": _uuidish(seed[4:] + "u"),
            },
            "card_id": _uuidish(seed[6:] + "c"),
            "line_items": [{
                "amount": {
                    "amount": cents,
                    "currency_code": "USD",
                    "minor_unit_conversion_rate": 100,
                },
            }],
            "disputes": [],
            "sync_status": "SYNC_READY",
            "synced_at": None,
        }

    if entity_type == "reimbursement":
        # Every other reimbursement is fully paid (REIMBURSED — a
        # state_change); the rest stay pending.
        state = "REIMBURSED" if idx % 2 == 0 else "PENDING"
        return {
            "id": entity_id,
            "state": state,
            "amount": dollars,                       # major units (payor pays)
            "currency": "USD",
            "payee_amount": {                        # minor units (cents)
                "amount": cents,
                "currency_code": "USD",
                "minor_unit_conversion_rate": 100,
            },
            "created_at": (base - timedelta(days=1)).isoformat(),
            "updated_at": ts,
            "transaction_date": base.date().isoformat(),
            "user_full_name": f"Emp Loyee-{person_seed}",
            "user_id": _uuidish(seed[4:] + "u"),
            "merchant": f"Vendor-{person_seed}",
            "type": "OUT_OF_POCKET",
            "direction": "BUSINESS_TO_USER",
            "memo": None,
            "sync_status": "SYNC_READY",
        }

    if entity_type == "card":
        # Every 3rd card is SUSPENDED (a state_change); the rest active.
        state = "SUSPENDED" if idx % 3 == 2 else "ACTIVE"
        return {
            "id": entity_id,
            "state": state,
            "display_name": f"Card-{person_seed}",
            "last_four": str(int(seed[:4], 16) % 10000).zfill(4),
            "cardholder_id": _uuidish(seed[4:] + "u"),
            "cardholder_name": f"Card Holder-{person_seed}",
            "is_physical": idx % 2 == 0,
            "expiration": "2030-01",
            "created_at": ts,
        }

    if entity_type == "user":
        # Every 3rd user is USER_INACTIVE (a state_change); the rest active.
        status = "USER_INACTIVE" if idx % 3 == 2 else "USER_ACTIVE"
        return {
            "id": entity_id,
            "status": status,
            "first_name": "Emp",
            "last_name": f"Loyee-{person_seed}",
            "email": f"emp.{person_seed}@example.com",
            "role": "BUSINESS_ADMIN" if idx == 0 else "BUSINESS_USER",
            "department_id": _uuidish(seed[8:] + "d"),
            "is_manager": idx == 0,
        }

    raise ValueError(f"unknown ramp entity_type {entity_type!r}")


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


def _uuidish(seed: str) -> str:
    """Deterministic uuid-format id from a hex digest (real Ramp ids are
    uuids)."""
    s = hashlib.sha256(seed.encode()).hexdigest()
    return f"{s[0:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:32]}"


__all__ = ["make_ramp", "DEFAULT_ENTITIES"]
