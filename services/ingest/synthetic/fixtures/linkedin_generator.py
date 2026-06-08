"""LinkedIn organization fixture generator (IN-PEOPLE, source 25, partner-gated).

`make_linkedin(organization_urn=..., entities=[...], rows_per_entity=N, seed=...)`
produces a deterministic per-entity-type fixture shaped to feed
`MockLinkedinClient`. The mock paginates each entity list by LinkedIn's offset
cursor (`STARTPOSITION` / `start_position`) — cloning the Carta/Gusto query shape
— and the fetcher drives one `linkedin_entity` shard per entity type.

Each generated entity carries exactly the fields the `linkedin:object` handler
reads (handlers/linkedin.py):
  - `Id`, `MetaData.LastUpdatedTime` (every entity) — `Id` is the external_id key
    (`linkedin:{org}:{kind}:{id}`, NOT version-suffixed per the CONTRACT),
    `LastUpdatedTime` is the high-water + occurred_at,
  - `Status` (the handler's state_change/open classifier),
  - `AuthorRef` + per-entity extras (ImpressionCount / LikeCount / FollowerCount).

DEFAULT: 3 entity kinds (share / social_action / follower_stat) × 1 row =
exactly 3 backfill observations per tenant. Because the entity_kind is baked into
the external_id (`linkedin:{org}:{kind}:{id}`), the rows stay distinct even if
their `Id`s repeat — so multi-entity fixtures never collide (organization-data
shaped, NOT transaction-shaped).

Determinism: timestamps are spaced one minute apart, oldest first, anchored at
`base_iso`; ids/values are derived from a stable SHA-256 digest of
(seed, organization_urn, entity_type, idx). Re-running with the same args yields
byte-identical output. The `seed` kwarg, when set, salts the digest so distinct
tenants get distinct ids without colliding (the organization_urn namespaces the
global observations UNIQUE; the entity_kind discriminator keeps same-id rows
distinct WITHIN a tenant).

NOTE: LinkedIn recruitment APIs are PARTNER-GATED (Marketing Developer Platform /
Talent Solutions, invite-only). This fixture mirrors the placeholder read surface
the production client clones from Carta/Gusto; the real per-entity data shapes are
UNVERIFIED pending partner entitlement (see integrations/linkedin/client.py).
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any


# The three organization entities the planner shards on (client.DEFAULT_ENTITIES).
DEFAULT_ENTITIES: tuple[str, ...] = ("share", "social_action", "follower_stat")


def make_linkedin(
    *,
    organization_urn: str = "li-org-0001",
    entities: list[str] | None = None,
    rows_per_entity: int = 1,
    seed: int | str | None = None,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 100,
) -> dict[str, Any]:
    """Build a LinkedIn organization fixture.

    Args:
      organization_urn: LinkedIn organization URN scope-id (stamped into refs +
        returned at top level).
      entities: Entity types to generate; defaults to the three organization
        entities ("share", "social_action", "follower_stat").
      rows_per_entity: Number of rows generated for EACH entity type. The default
        3 entities × 1 row = exactly 3 backfill observations per tenant.
      seed: Optional salt mixed into the deterministic digest so distinct tenants
        get distinct ids (the organization_urn namespaces the global observations
        UNIQUE; the entity_kind discriminator already keeps same-id rows distinct
        WITHIN a tenant).
      base_iso: Anchor for the (deterministic, 1-min-spaced) LastUpdatedTime
        timestamps. Accepts "...Z" or an explicit offset.
      page_size: The mock client's per-query MAXRESULTS cap (so callers can drive
        multi-page pagination by setting rows_per_entity > page_size).

    Returns:
      Fixture dict consumed by `MockLinkedinClient(fixture=...)`:
        {
          "organization_urn": "...",
          "page_size": 100,
          "entities": {
            "share":         [ {<full LinkedIn entity>}, ... ],   # oldest-first
            "social_action": [ ... ],
            "follower_stat": [ ... ],
          },
        }
    """
    ents = list(entities) if entities is not None else list(DEFAULT_ENTITIES)
    base = _parse_iso(base_iso)
    salt = "" if seed is None else str(seed)

    entities_out: dict[str, list[dict[str, Any]]] = {}
    for entity_type in ents:
        rows = [
            _entity(organization_urn, entity_type, idx, base, salt)
            for idx in range(rows_per_entity)
        ]
        entities_out[entity_type] = rows

    return {
        "organization_urn": organization_urn,
        "page_size": page_size,
        "entities": entities_out,
    }


# ---------------------------------------------------------------------
# Per-entity builders
# ---------------------------------------------------------------------

def _entity(
    organization_urn: str, entity_type: str, idx: int, base: datetime, salt: str,
) -> dict[str, Any]:
    # ISO LastUpdatedTime spaced 1 minute apart, oldest first, with offset.
    updated = (base + timedelta(minutes=idx)).isoformat()
    digest = _digest(salt, organization_urn, entity_type, idx)
    entity_id = str(1000 + idx)

    entity: dict[str, Any] = {
        "Id": entity_id,
        "DocNumber": f"{entity_type[:3].upper()}-{entity_id}",
        "Status": "active",
        "AuthorRef": {
            "value": str(1 + idx), "name": f"Member-{digest[:6]}",
        },
        "MetaData": {
            "CreateTime": (base - timedelta(days=1)).isoformat(),
            "LastUpdatedTime": updated,
        },
    }

    if entity_type == "share":
        entity["ImpressionCount"] = 1000 * (idx + 1)
        entity["LikeCount"] = 10 * (idx + 1)
        entity["CommentCount"] = idx + 1
        entity["Text"] = f"Org update {digest[:6]}"
    elif entity_type == "social_action":
        entity["LikeCount"] = 50 * (idx + 1)
        entity["CommentCount"] = 5 * (idx + 1)
        entity["ShareCount"] = idx + 1
    else:  # follower_stat
        entity["FollowerCount"] = 10_000 + (int(digest[:6], 16) % 90_000)
        entity["OrganicFollowerGain"] = 100 * (idx + 1)
        entity["PaidFollowerGain"] = 10 * (idx + 1)

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


__all__ = ["make_linkedin", "DEFAULT_ENTITIES"]
