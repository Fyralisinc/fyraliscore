"""Ashby recruiting-ATS entity fixture generator (IN-PEOPLE, source 24).

`make_ashby(org_id=..., entities=[...], rows_per_entity=N, seed=...)` produces a
deterministic per-entity-type fixture shaped to feed `MockAshbyClient`. The mock
paginates each entity list by Ashby's CURSOR (request `cursor` / response
`nextCursor` + `moreDataAvailable`) and the fetcher drives one `ashby_entity`
shard per entity type.

Each generated entity carries exactly the fields the `ashby:object` handler reads
(handlers/ashby.py):
  - `id` + `updatedAt` (every entity) — `id` is the external_id key
    (`ashby:{org}:{entity}:{id}`, NOT version-suffixed per the CONTRACT),
    `updatedAt` is the high-water + occurred_at,
  - `status` for offer/application/interview (the handler's state classifier),
  - `name` / `title` so the content extraction has something to lift.

DEFAULT: 5 entity kinds (candidate / application / job / interview / offer) ×
1 row = exactly 5 backfill observations per tenant. Because the entity_kind is
baked into the external_id, the rows stay distinct even if their `id`s repeat — so
multi-entity fixtures never collide (recruiting-entity-shaped, NOT
transaction-shaped).

Determinism: `updatedAt` timestamps are spaced one minute apart, oldest first,
anchored at `base_iso`; ids/values are derived from a stable SHA-256 digest of
(seed, org_id, entity_type, idx). Re-running with the same args yields
byte-identical output. The `seed` kwarg, when set, salts the digest so distinct
tenants get distinct ids without colliding (the org_id namespace + entity_kind
discriminator already keep rows distinct).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


# The five recruiting entities the planner shards on (client.DEFAULT_ENTITIES).
DEFAULT_ENTITIES: tuple[str, ...] = (
    "candidate", "application", "job", "interview", "offer",
)


def make_ashby(
    *,
    org_id: str = "ashby-org-0001",
    entities: list[str] | None = None,
    rows_per_entity: int = 1,
    seed: int | str | None = None,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
) -> dict[str, Any]:
    """Build an Ashby org fixture.

    Args:
      org_id: Ashby organization id (the scope-id; returned at top level and
        stamped into the external_id namespace).
      entities: Entity types to generate; defaults to the five recruiting entities
        ("candidate", "application", "job", "interview", "offer").
      rows_per_entity: Number of rows generated for EACH entity type. The default
        5 entities × 1 row = exactly 5 backfill observations per tenant.
      seed: Optional salt mixed into the deterministic digest so distinct tenants
        get distinct ids (the org_id namespace + entity_kind discriminator already
        keep rows distinct, so this is belt-and-suspenders).
      base_iso: Anchor for the (deterministic, 1-min-spaced) `updatedAt`
        timestamps. Accepts "...Z" or an explicit offset.
      page_size: The mock client's per-list cursor-page cap (so callers can drive
        multi-page pagination by setting rows_per_entity > page_size).

    Returns:
      Fixture dict consumed by `MockAshbyClient(fixture=...)`:
        {
          "org_id": "...",
          "page_size": 100,
          "entities": {
            "candidate":   [ {<full Ashby entity>}, ... ],   # oldest-first
            "application": [ ... ],
            "job":         [ ... ],
            "interview":   [ ... ],
            "offer":       [ ... ],
          },
        }
    """
    ents = list(entities) if entities is not None else list(DEFAULT_ENTITIES)
    base = _parse_iso(base_iso)
    salt = "" if seed is None else str(seed)

    entities_out: dict[str, list[dict[str, Any]]] = {}
    for entity_type in ents:
        rows = [
            _entity(org_id, entity_type, idx, base, salt)
            for idx in range(rows_per_entity)
        ]
        entities_out[entity_type] = rows

    return {
        "org_id": org_id,
        "page_size": page_size,
        "entities": entities_out,
    }


# ---------------------------------------------------------------------
# Per-entity builders
# ---------------------------------------------------------------------

def _entity(
    org_id: str, entity_type: str, idx: int, base: datetime, salt: str,
) -> dict[str, Any]:
    # ISO `updatedAt` spaced 1 minute apart, oldest first, with offset.
    updated = (base + timedelta(minutes=idx)).isoformat()
    created = (base - timedelta(days=1) + timedelta(minutes=idx)).isoformat()
    digest = _digest(salt, org_id, entity_type, idx)
    entity_id = f"{entity_type[:3]}_{digest[:12]}"
    name = f"Candidate {digest[:6]}"

    entity: dict[str, Any] = {
        "id": entity_id,
        "createdAt": created,
        "updatedAt": updated,
    }

    if entity_type == "candidate":
        entity["name"] = name
        entity["candidateId"] = entity_id
        entity["email"] = f"{digest[:8]}@example.com"
    elif entity_type == "application":
        entity["candidateName"] = name
        entity["candidate"] = {"id": f"can_{digest[:12]}", "name": name}
        entity["status"] = "active"
        entity["stage"] = "Phone Screen"
    elif entity_type == "job":
        entity["title"] = "Senior Software Engineer"
        entity["status"] = "open"
        entity["locationName"] = "Remote"
    elif entity_type == "interview":
        entity["candidate"] = {"id": f"can_{digest[:12]}", "name": name}
        entity["status"] = "scheduled"
        entity["interviewStage"] = {"title": "Technical Screen"}
    else:  # offer
        entity["candidate"] = {"id": f"can_{digest[:12]}", "name": name}
        # A terminal offer status is the recruiting state-change signal.
        entity["offerStatus"] = "accepted"
        entity["status"] = "accepted"

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


__all__ = ["make_ashby", "DEFAULT_ENTITIES"]
