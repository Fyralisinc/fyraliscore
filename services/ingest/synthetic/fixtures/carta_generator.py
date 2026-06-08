"""Carta cap-table fixture generator (IN-CARTA).

`make_carta(firm_id=..., entities=[...], rows_per_entity=N)` produces a
deterministic per-entity-type fixture shaped to feed `MockCartaClient`. The mock
paginates each entity list by CARTA's offset cursor (`STARTPOSITION` /
`start_position`) and the fetcher drives one `carta_entity` shard per entity
type.

Each generated entity carries exactly the fields the `carta:object` handler
reads (handlers/carta.py):
  - `Id`, `SyncToken`, `MetaData.LastUpdatedTime` (every entity),
  - `Status` (the handler's state_change/open signal classifier),
  - `StakeholderRef` + cap-table extras (ShareCount / Quantity / StrikePrice /
    InvestmentAmount / ValuationCap / ...) per entity type.

DEFAULT: 4 entity kinds (Shareholder / ShareClass / SafeNote / OptionGrant) ×
1 row = exactly 4 backfill observations per tenant. Because the entity_kind is
baked into the external_id (`carta:{firm}:{kind}:{id}:{sync_token}`), the four
rows stay distinct even if their `Id`s repeat — so multi-entity fixtures never
collide (cap-table-shaped, NOT transaction-shaped).

Determinism: timestamps are spaced one minute apart, oldest first, anchored at
`base_iso`; ids/amounts are derived from a stable SHA-256 digest of
(firm_id, entity_type, idx). Re-running with the same args yields byte-identical
output. The `seed` kwarg, when set, salts the digest so distinct tenants get
distinct ids/amounts without colliding.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


# The four cap-table entities the planner shards on (client.DEFAULT_ENTITIES).
DEFAULT_ENTITIES: tuple[str, ...] = (
    "Shareholder", "ShareClass", "SafeNote", "OptionGrant",
)


def make_carta(
    *,
    firm_id: str = "firm_9341452000000001",
    entities: list[str] | None = None,
    rows_per_entity: int = 1,
    seed: int | None = None,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
) -> dict[str, Any]:
    """Build a Carta firm fixture.

    Args:
      firm_id: CARTA firm scope id (stamped into refs + returned at top level).
      entities: Entity types to generate; defaults to the four cap-table entities
        ("Shareholder", "ShareClass", "SafeNote", "OptionGrant").
      rows_per_entity: Number of rows generated for EACH entity type. The default
        4 entities × 1 row = exactly 4 backfill observations per tenant.
      seed: Optional salt mixed into the deterministic digest so distinct tenants
        get distinct ids/amounts (the global observations UNIQUE has no tenant_id,
        so per-tenant fixtures must differ — though the entity_kind discriminator
        already keeps same-id rows distinct WITHIN a tenant).
      base_iso: Anchor for the (deterministic, 1-min-spaced) LastUpdatedTime
        timestamps. Accepts "...Z" or an explicit offset.
      page_size: The mock client's per-query MAXRESULTS cap (so callers can drive
        multi-page pagination by setting rows_per_entity > page_size).

    Returns:
      Fixture dict consumed by `MockCartaClient(fixture=...)`:
        {
          "firm_id": "...",
          "page_size": 100,
          "entities": {
            "Shareholder": [ {<full CARTA entity>}, ... ],   # oldest-first
            "ShareClass":  [ ... ],
            "SafeNote":    [ ... ],
            "OptionGrant": [ ... ],
          },
        }
    """
    ents = list(entities) if entities is not None else list(DEFAULT_ENTITIES)
    base = _parse_iso(base_iso)
    salt = "" if seed is None else str(seed)

    entities_out: dict[str, list[dict[str, Any]]] = {}
    for entity_type in ents:
        rows = [
            _entity(firm_id, entity_type, idx, base, salt)
            for idx in range(rows_per_entity)
        ]
        entities_out[entity_type] = rows

    return {
        "firm_id": firm_id,
        "page_size": page_size,
        "entities": entities_out,
    }


# ---------------------------------------------------------------------
# Per-entity builders
# ---------------------------------------------------------------------

def _entity(
    firm_id: str, entity_type: str, idx: int, base: datetime, salt: str,
) -> dict[str, Any]:
    # ISO LastUpdatedTime spaced 1 minute apart, oldest first, with offset.
    updated = (base + timedelta(minutes=idx)).isoformat()
    digest = _digest(salt, firm_id, entity_type, idx)
    entity_id = str(1000 + idx)
    # A nonzero SyncToken on some rows exercises external_id versioning.
    sync_token = str(idx % 3)
    amount = round(100.0 + (int(digest[:6], 16) % 900_000) / 100.0, 2)

    entity: dict[str, Any] = {
        "Id": entity_id,
        "SyncToken": sync_token,
        "DocNumber": f"{entity_type[:3].upper()}-{entity_id}",
        "Status": "active",
        "StakeholderRef": {
            "value": str(1 + idx), "name": f"Holder-{digest[:6]}",
        },
        "MetaData": {
            "CreateTime": (base - timedelta(days=1)).isoformat(),
            "LastUpdatedTime": updated,
        },
    }

    if entity_type == "Shareholder":
        entity["ShareCount"] = 1000 * (idx + 1)
        entity["Ownership"] = round(1.0 + idx, 4)
        entity["ShareClassRef"] = {"value": "1", "name": "Common"}
    elif entity_type == "ShareClass":
        entity["ShareCount"] = 10_000_000
        entity["PricePerShare"] = amount
        entity["DocNumber"] = f"SC-{entity_id}"
    elif entity_type == "SafeNote":
        entity["InvestmentAmount"] = amount
        entity["ValuationCap"] = amount * 100
        entity["DiscountRate"] = 0.2
        entity["IssueDate"] = base.date().isoformat()
    else:  # OptionGrant
        entity["Quantity"] = 500 * (idx + 1)
        entity["StrikePrice"] = round(amount / 100.0, 4)
        entity["GrantDate"] = base.date().isoformat()
        entity["VestingSchedule"] = "4yr-1yr-cliff"
        entity["ShareClassRef"] = {"value": "1", "name": "Common"}

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


__all__ = ["make_carta", "DEFAULT_ENTITIES"]
