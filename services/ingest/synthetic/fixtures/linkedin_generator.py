"""LinkedIn organization fixture generator (IN-PEOPLE, source 25, partner-gated).

`make_linkedin(organization_urn=..., entities=[...], rows_per_entity=N, seed=...)`
produces a deterministic per-stream fixture shaped to feed
`MockLinkedinClient`, mirroring the REAL Community-Management API shapes:

  - "post" — `/rest/posts?q=author` finder elements: `id` is a share URN
    (`urn:li:share:{n}`), `author` is the organization URN, and `createdAt` /
    `lastModifiedAt` / `publishedAt` are **epoch-millis integers**. The mock
    serves these DESC by `lastModifiedAt` with `start`/`count` offset paging.
  - "share_statistics" — time-bound `organizationalEntityShareStatistics`
    elements: `timeRange{start,end}` (epoch millis) + `totalShareStatistics`
    counters. `timeRange.start` is the snapshot-bucket id the handler keys the
    external_id on.
  - "follower_statistics" — time-bound `organizationalEntityFollowerStatistics`
    elements: `timeRange{start,end}` + `followerGains{organicFollowerGain,
    paidFollowerGain}`.

DEFAULT: 3 streams (post / share_statistics / follower_statistics) × 1 row =
exactly 3 backfill observations per tenant. The entity_kind is baked into the
external_id (`linkedin:{org}:{kind}:{id}`), so streams stay distinct even if
their ids repeat — multi-stream fixtures never collide.

Determinism: timestamps are spaced one minute apart, oldest first, anchored at
`base_iso` (converted to epoch millis); ids/values are derived from a stable
SHA-256 digest of (seed, organization_urn, entity_type, idx). Re-running with
the same args yields byte-identical output. The `seed` kwarg, when set, salts
the digest so distinct tenants get distinct values (the organization_urn
namespaces the global observations UNIQUE).

NOTE: keep `base_iso` within the API's rolling 12-month statistics window of
the clock the harness runs under — the fetcher requests time-bound statistics
from `now - 12 months`, and the mock honours that floor like the wire does.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


# The three organization streams the planner shards on (client.DEFAULT_ENTITIES).
DEFAULT_ENTITIES: tuple[str, ...] = (
    "post", "share_statistics", "follower_statistics",
)

_DAY_MS = 24 * 3600 * 1000


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
      organization_urn: LinkedIn organization URN scope-id (stamped into post
        authors / organizationalEntity refs + returned at top level). A bare
        id is up-converted to `urn:li:organization:{id}` on wire-shaped fields
        only — the top-level scope-id stays verbatim (it namespaces the
        external_id exactly as the install row does).
      entities: Streams to generate; defaults to the three organization
        streams ("post", "share_statistics", "follower_statistics").
      rows_per_entity: Number of rows generated for EACH stream. The default
        3 streams × 1 row = exactly 3 backfill observations per tenant.
      seed: Optional salt mixed into the deterministic digest so distinct
        tenants get distinct values.
      base_iso: Anchor for the (deterministic, 1-min-spaced) epoch-millis
        timestamps. Accepts "...Z" or an explicit offset.
      page_size: The mock client's per-call `count` cap (so callers can drive
        multi-page posts pagination by setting rows_per_entity > page_size).

    Returns:
      Fixture dict consumed by `MockLinkedinClient(fixture=...)`:
        {
          "organization_urn": "...",
          "page_size": 100,
          "entities": {
            "post":                [ {<post element>}, ... ],     # oldest-first
            "share_statistics":    [ {<stat element>}, ... ],
            "follower_statistics": [ {<stat element>}, ... ],
          },
        }
    """
    ents = list(entities) if entities is not None else list(DEFAULT_ENTITIES)
    base_ms = _epoch_ms(_parse_iso(base_iso))
    salt = "" if seed is None else str(seed)

    entities_out: dict[str, list[dict[str, Any]]] = {}
    for entity_type in ents:
        rows = [
            _entity(organization_urn, entity_type, idx, base_ms, salt)
            for idx in range(rows_per_entity)
        ]
        entities_out[entity_type] = rows

    return {
        "organization_urn": organization_urn,
        "page_size": page_size,
        "entities": entities_out,
    }


# ---------------------------------------------------------------------
# Per-stream builders
# ---------------------------------------------------------------------

def _entity(
    organization_urn: str, entity_type: str, idx: int, base_ms: int, salt: str,
) -> dict[str, Any]:
    # Epoch-millis timestamps spaced 1 minute apart, oldest first.
    stamp_ms = base_ms + idx * 60_000
    digest = _digest(salt, organization_urn, entity_type, idx)
    author_urn = _org_wire_urn(organization_urn)

    if entity_type == "post":
        return {
            "id": f"urn:li:share:{1000 + idx}",
            "author": author_urn,
            "commentary": f"Org update {digest[:6]}",
            "visibility": "PUBLIC",
            "lifecycleState": "PUBLISHED",
            "lifecycleStateInfo": {"isEditedByAuthor": False},
            "isReshareDisabledByAuthor": False,
            "distribution": {
                "feedDistribution": "MAIN_FEED",
                "thirdPartyDistributionChannels": [],
            },
            "createdAt": stamp_ms - _DAY_MS,
            "publishedAt": stamp_ms - _DAY_MS,
            "lastModifiedAt": stamp_ms,
        }

    # Statistics streams: one time-bound bucket per row, window-keyed by
    # timeRange.start (the snapshot version the handler uses as the id).
    time_range = {"start": stamp_ms, "end": stamp_ms + _DAY_MS}
    if entity_type == "share_statistics":
        return {
            "organizationalEntity": author_urn,
            "timeRange": time_range,
            "totalShareStatistics": {
                "clickCount": 7 * (idx + 1),
                "likeCount": 10 * (idx + 1),
                "commentCount": idx + 1,
                "shareCount": idx,
                "impressionCount": 1000 * (idx + 1),
                "uniqueImpressionsCount": 800 * (idx + 1),
                "engagement": round(0.005 * (idx + 1), 6),
            },
        }
    # follower_statistics (and any custom stream name falls through to this
    # follower-shaped default, keeping unknown-entity fixtures harmless).
    return {
        "organizationalEntity": author_urn,
        "timeRange": time_range,
        "followerGains": {
            "organicFollowerGain": 100 * (idx + 1) + (int(digest[:4], 16) % 50),
            "paidFollowerGain": 10 * (idx + 1),
        },
    }


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _org_wire_urn(organization_urn: str) -> str:
    if organization_urn.startswith("urn:"):
        return organization_urn
    return f"urn:li:organization:{organization_urn}"


def _parse_iso(value: str) -> datetime:
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_linkedin", "DEFAULT_ENTITIES"]
