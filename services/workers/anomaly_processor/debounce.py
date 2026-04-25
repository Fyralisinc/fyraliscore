"""services/workers/anomaly_processor/debounce.py — region debounce.

Per ARCHITECTURE-REVIEW-1 §I5 and AUDIT-REVIEW-1-FIXES FU3, debounce
is now **drop-or-escalate**, never "update-in-place":

  * If the new candidate's significance is *not materially higher* than
    the most-recent anomaly in the same region within the debounce
    window, **drop silently** (the prior anomaly already covers it).
  * If the new candidate's significance is *materially higher* (>=
    ESCALATION_DELTA above the prior), **publish a FRESH T3** with an
    `escalates` back-link to the prior anomaly_id. The prior row is
    left untouched — editing a trigger that has already been
    dequeued/applied is not a well-defined operation.

This replaces the previous `update_existing_anomaly` mutation, which
could silently drop real updates once the prior trigger had already
been applied (the bug ARCHITECTURE-REVIEW-1 §I5 describes).

Debounce state still lives in `think_anomalies_raw`. We read the
most-recent row for (tenant, kind, region_hash) within the window and
compare its `significance` to the new candidate's.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg


# Per AUDIT-REVIEW-1-FIXES key decision: escalation fires only when the
# new candidate's significance exceeds the prior anomaly's by at least
# this much. Tuned small enough to catch genuine worsening, large enough
# to avoid thrashing the queue on minor score jitter.
ESCALATION_DELTA: float = 0.2


@dataclass(frozen=True)
class DebounceDecision:
    """Outcome of `decide_debounce`.

    Exactly one of these is true:
      * `action == 'publish_new'`  — no prior in window; publish a fresh
        anomaly with escalates=None.
      * `action == 'escalate'`     — prior exists but the new candidate
        is materially more severe; publish a fresh anomaly with
        escalates=prior_anomaly_id.
      * `action == 'suppress'`     — prior covers this; drop silently.
    """

    action: str  # 'publish_new' | 'escalate' | 'suppress'
    prior_anomaly_id: UUID | None
    prior_significance: float | None


def compute_region_hash(
    tenant_id: UUID, kind: str, region_entity_ids: list[dict[str, Any]]
) -> str:
    """
    Stable hash over (tenant, kind, sorted entities). Used as the
    debounce key and as the Memory Fabric grouping key.
    """
    canonical_entities = sorted(
        (
            (str(e.get("entity_kind", "")), str(e.get("entity_id", "")))
            for e in region_entity_ids
        )
    )
    payload = {
        "tenant": str(tenant_id),
        "kind": kind,
        "entities": canonical_entities,
    }
    raw = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.blake2b(raw, digest_size=16).hexdigest()


async def most_recent_anomaly_in_region(
    region_hash: str,
    tenant_id: UUID,
    kind: str,
    within: timedelta,
    conn: asyncpg.Connection,
) -> tuple[UUID, float] | None:
    """
    Return (id, significance) of the most-recent `think_anomalies_raw`
    row whose `region.region_hash == $region_hash` AND `kind == $kind`
    AND tenant_id = $tenant_id AND published_at >= now() - within.
    Else None.
    """
    cutoff = datetime.now(timezone.utc) - within
    row = await conn.fetchrow(
        """
        SELECT id, significance
        FROM think_anomalies_raw
        WHERE tenant_id = $1
          AND kind = $2
          AND region ->> 'region_hash' = $3
          AND published_at >= $4
        ORDER BY published_at DESC
        LIMIT 1
        """,
        tenant_id, kind, region_hash, cutoff,
    )
    if row is None:
        return None
    return row["id"], float(row["significance"] or 0.0)


async def decide_debounce(
    *,
    region_hash: str,
    tenant_id: UUID,
    kind: str,
    new_significance: float,
    within: timedelta,
    conn: asyncpg.Connection,
    escalation_delta: float = ESCALATION_DELTA,
) -> DebounceDecision:
    """
    Apply the drop-or-escalate decision.

    Never mutates `think_anomalies_raw`. The caller acts on the
    returned `DebounceDecision`:

      'publish_new' → enqueue T3 + write anomaly_raw row
      'escalate'    → enqueue T3 (with escalates ref) + write
                       a NEW anomaly_raw row; leave prior untouched.
      'suppress'    → record sub-threshold into Memory Fabric and
                       log 'anomaly.debounced_suppressed'.
    """
    prior = await most_recent_anomaly_in_region(
        region_hash, tenant_id, kind, within, conn
    )
    if prior is None:
        return DebounceDecision(
            action="publish_new",
            prior_anomaly_id=None,
            prior_significance=None,
        )

    prior_id, prior_sig = prior
    if new_significance >= prior_sig + escalation_delta:
        return DebounceDecision(
            action="escalate",
            prior_anomaly_id=prior_id,
            prior_significance=prior_sig,
        )

    return DebounceDecision(
        action="suppress",
        prior_anomaly_id=prior_id,
        prior_significance=prior_sig,
    )


# Back-compat alias for callers still importing the old name. The
# returned UUID is `prior_anomaly_id` when action in ('escalate',
# 'suppress'), else None. Kept until all callers migrate to
# `decide_debounce`.
async def recent_anomaly_in_region(
    region_hash: str,
    tenant_id: UUID,
    kind: str,
    within: timedelta,
    conn: asyncpg.Connection,
) -> UUID | None:
    prior = await most_recent_anomaly_in_region(
        region_hash, tenant_id, kind, within, conn
    )
    return prior[0] if prior is not None else None


__all__ = [
    "ESCALATION_DELTA",
    "DebounceDecision",
    "compute_region_hash",
    "most_recent_anomaly_in_region",
    "decide_debounce",
    "recent_anomaly_in_region",  # deprecated
]
