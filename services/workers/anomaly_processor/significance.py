"""services/workers/anomaly_processor/significance.py — scoring.

Spec §18 "Significance scoring" block. Detectors supply a base score;
this module modulates by:
- critical_path modulator (1.5×) — any Commitment in the region with
  `contributes_to.is_critical_path=TRUE`.
- customer modulator (1.3×) — any Commitment in the region with a
  non-NULL `external_counterparty_ref` OR any Resource in the region
  with `kind='relational'`.
- trust_tier modulator (1.15× / 1.10× / 1.0×) — higher when the
  triggering signals include authoritative or authoritative_external
  tiers.

Final score is clipped to [0.0, 1.0]. Threshold per §18 line 3687:

    SIGNIFICANCE_THRESHOLD = 0.4

Above → real anomaly (debounce + T3 enqueue).
Below → Memory Fabric accumulation.
"""
from __future__ import annotations

from typing import Iterable
from uuid import UUID

import asyncpg

from .detectors import AnomalyCandidate


SIGNIFICANCE_THRESHOLD: float = 0.4

# Trust-tier ordering for the trust modulator. `authoritative` and
# `authoritative_external` carry weight; everything else is baseline.
_HIGH_TRUST_TIERS = frozenset({"authoritative", "authoritative_external"})
_MEDIUM_TRUST_TIERS = frozenset({"attested_agent", "reputable"})


async def _region_touches_critical_path(
    candidate: AnomalyCandidate,
    conn: asyncpg.Connection,
) -> bool:
    """
    Any Commitment in the region whose `contributes_to.is_critical_path=TRUE`.
    """
    commitment_ids = [
        UUID(e["entity_id"]) for e in candidate.region_entity_ids
        if e.get("entity_kind") == "commitment"
    ]
    if not commitment_ids:
        return False
    row = await conn.fetchval(
        """
        SELECT 1
        FROM contributes_to
        WHERE commitment_id = ANY($1::uuid[])
          AND is_critical_path = TRUE
        LIMIT 1
        """,
        commitment_ids,
    )
    return row is not None


async def _region_touches_customer(
    candidate: AnomalyCandidate,
    conn: asyncpg.Connection,
) -> bool:
    """
    Customer touches either via:
    - Commitment.external_counterparty_ref IS NOT NULL, OR
    - Resource.kind='relational' in the region, OR
    - The entity_type itself is 'resource' (relational customer).
    """
    # Commitments with external_counterparty_ref
    commitment_ids = [
        UUID(e["entity_id"]) for e in candidate.region_entity_ids
        if e.get("entity_kind") == "commitment"
    ]
    if commitment_ids:
        row = await conn.fetchval(
            """
            SELECT 1
            FROM commitments
            WHERE id = ANY($1::uuid[])
              AND external_counterparty_ref IS NOT NULL
            LIMIT 1
            """,
            commitment_ids,
        )
        if row is not None:
            return True

    # Resources with kind='relational'
    resource_ids = [
        UUID(e["entity_id"]) for e in candidate.region_entity_ids
        if e.get("entity_kind") == "resource"
    ]
    if resource_ids:
        row = await conn.fetchval(
            """
            SELECT 1
            FROM resources
            WHERE id = ANY($1::uuid[])
              AND kind = 'relational'
            LIMIT 1
            """,
            resource_ids,
        )
        if row is not None:
            return True

    return False


def _trust_modulator(trust_tiers: Iterable[str]) -> float:
    """
    Higher weight when triggering signals carry `authoritative` or
    `authoritative_external` trust tiers. Returns a multiplier in
    [1.0, 1.15].
    """
    tiers = list(trust_tiers or [])
    if not tiers:
        return 1.0
    if any(t in _HIGH_TRUST_TIERS for t in tiers):
        return 1.15
    if any(t in _MEDIUM_TRUST_TIERS for t in tiers):
        return 1.05
    return 1.0


async def compute_significance(
    candidate: AnomalyCandidate,
    conn: asyncpg.Connection,
) -> float:
    """
    Return the modulated, clipped significance for this candidate.
    Spec §18 Significance scoring block.

    Formula:
        score = base
        score *= 1.5 if critical path
        score *= 1.3 if customer
        score *= trust_tier_modulator
        return min(1.0, max(0.0, score))
    """
    score = float(candidate.significance)

    if await _region_touches_critical_path(candidate, conn):
        score *= 1.5
    if await _region_touches_customer(candidate, conn):
        score *= 1.3

    score *= _trust_modulator(candidate.trust_tiers)

    if score < 0.0:
        return 0.0
    if score > 1.0:
        return 1.0
    return score


__all__ = [
    "SIGNIFICANCE_THRESHOLD",
    "compute_significance",
]
