"""HiBob People/HR entity fixture generator (IN-PEOPLE, source 23).

`make_hibob(company_id=..., entities=[...], rows_per_entity=N, seed=...)` produces
a deterministic per-entity-type fixture shaped to feed `MockHibobClient`. The mock
paginates each entity list by HiBob's offset/limit cursor and the fetcher drives
one `hibob_entity` shard per entity type.

Each generated entity carries exactly the fields the `hibob:object` handler reads
(handlers/hibob.py):
  - `id` + `modified` (every entity) — `id` is the external_id key, `modified` is
    the high-water + the external_id VERSION (`hibob:{company}:{entity}:{id}:{ver}`),
  - `status` for lifecycle/timeoff (the handler's state_change classifier),
  - `displayName` / `department` / `title` / `email` so the content extraction has
    something to lift.

DEFAULT: 4 entity kinds (employee / lifecycle / timeoff / payroll) × 1 row =
exactly 4 backfill observations per tenant. Because the entity_kind is baked into
the external_id, the rows stay distinct even if their `id`s repeat — so
multi-entity fixtures never collide (HR-entity-shaped, NOT transaction-shaped).

Determinism: `modified` timestamps are spaced one minute apart, oldest first,
anchored at `base_iso`; ids/values are derived from a stable SHA-256 digest of
(seed, company_id, entity_type, idx). Re-running with the same args yields
byte-identical output. The `seed` kwarg, when set, salts the digest so distinct
tenants get distinct ids without colliding (the global observations UNIQUE has no
tenant_id — though the company_id namespace already keeps cross-tenant rows
distinct, and the entity_kind discriminator keeps same-id rows distinct WITHIN a
tenant).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


# The four People/HR entities the planner shards on (client.DEFAULT_ENTITIES).
DEFAULT_ENTITIES: tuple[str, ...] = ("employee", "lifecycle", "timeoff", "payroll")

# Lifecycle / time-off status words that the handler classifies as state_change.
_LIFECYCLE_STATUSES = ("hired", "terminated", "rehired")
_TIMEOFF_STATUSES = ("approved", "declined", "cancelled")


def make_hibob(
    *,
    company_id: str = "hibob-co-0001",
    entities: list[str] | None = None,
    rows_per_entity: int = 1,
    seed: int | str | None = None,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
) -> dict[str, Any]:
    """Build a HiBob company fixture.

    Args:
      company_id: HiBob company/account id (the scope-id; returned at top level
        and stamped into the external_id namespace).
      entities: Entity types to generate; defaults to the four People/HR entities
        ("employee", "lifecycle", "timeoff", "payroll").
      rows_per_entity: Number of rows generated for EACH entity type. The default
        4 entities × 1 row = exactly 4 backfill observations per tenant.
      seed: Optional salt mixed into the deterministic digest so distinct tenants
        get distinct ids (the company_id namespace + entity_kind discriminator
        already keep rows distinct, so this is belt-and-suspenders).
      base_iso: Anchor for the (deterministic, 1-min-spaced) `modified`
        timestamps. Accepts "...Z" or an explicit offset.
      page_size: The mock client's per-list offset/limit cap (so callers can drive
        multi-page pagination by setting rows_per_entity > page_size).

    Returns:
      Fixture dict consumed by `MockHibobClient(fixture=...)`:
        {
          "company_id": "...",
          "page_size": 100,
          "entities": {
            "employee":  [ {<full HiBob entity>}, ... ],   # oldest-first
            "lifecycle": [ ... ],
            "timeoff":   [ ... ],
            "payroll":   [ ... ],
          },
        }
    """
    ents = list(entities) if entities is not None else list(DEFAULT_ENTITIES)
    base = _parse_iso(base_iso)
    salt = "" if seed is None else str(seed)

    entities_out: dict[str, list[dict[str, Any]]] = {}
    for entity_type in ents:
        rows = [
            _entity(company_id, entity_type, idx, base, salt)
            for idx in range(rows_per_entity)
        ]
        entities_out[entity_type] = rows

    return {
        "company_id": company_id,
        "page_size": page_size,
        "entities": entities_out,
    }


# ---------------------------------------------------------------------
# Per-entity builders
# ---------------------------------------------------------------------

def _entity(
    company_id: str, entity_type: str, idx: int, base: datetime, salt: str,
) -> dict[str, Any]:
    # ISO `modified` spaced 1 minute apart, oldest first, with offset.
    modified = (base + timedelta(minutes=idx)).isoformat()
    digest = _digest(salt, company_id, entity_type, idx)
    entity_id = str(1000 + idx)
    name = f"Person {digest[:6]}"

    entity: dict[str, Any] = {
        "id": entity_id,
        # The high-water field AND the external_id version slot.
        "modified": modified,
        "displayName": name,
    }

    if entity_type == "employee":
        entity["department"] = "Engineering"
        entity["title"] = "Software Engineer"
        entity["email"] = f"{digest[:8]}@example.com"
        entity["status"] = "active"
        entity["startDate"] = base.date().isoformat()
    elif entity_type == "lifecycle":
        # Even rows are a hire (state_change); odd rows are an in-flight change.
        status = _LIFECYCLE_STATUSES[idx % len(_LIFECYCLE_STATUSES)]
        entity["status"] = status
        entity["lifecycleStatus"] = status
        entity["effectiveDate"] = base.date().isoformat()
    elif entity_type == "timeoff":
        status = _TIMEOFF_STATUSES[idx % len(_TIMEOFF_STATUSES)]
        entity["requestId"] = entity_id
        entity["status"] = status
        entity["approvalStatus"] = status
        entity["startDate"] = base.date().isoformat()
    else:  # payroll
        entity["payrollId"] = entity_id
        entity["status"] = "processed"
        amount = round(1000.0 + (int(digest[:6], 16) % 900_000) / 100.0, 2)
        entity["grossPay"] = amount
        entity["effectiveDate"] = base.date().isoformat()

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


__all__ = ["make_hibob", "DEFAULT_ENTITIES"]
