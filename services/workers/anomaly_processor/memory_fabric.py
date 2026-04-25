"""services/workers/anomaly_processor/memory_fabric.py — accumulator.

Spec §18 Memory Fabric block. Sub-threshold anomaly candidates land
in `signal_memory_fabric` (migration 0009). A periodic sweep promotes
a region to a full anomaly when > 5 unpromoted rows land within a
7-day window for the same region_hash.

Promotion produces an AnomalyCandidate the worker can then enqueue
as a T3 trigger (bypassing debounce — accumulation has already
earned the promotion).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7

from .detectors import AnomalyCandidate


PROMOTE_COUNT_THRESHOLD = 5        # > 5 rows → promote
PROMOTE_WINDOW = timedelta(days=7)  # within the last 7 days


def _jsonb(v: Any) -> str:
    return json.dumps(v, default=str, sort_keys=True)


async def record_subthreshold_signal(
    candidate: AnomalyCandidate,
    region_hash: str,
    final_significance: float,
    conn: asyncpg.Connection,
) -> UUID:
    """
    Persist a sub-threshold signal to `signal_memory_fabric`. Returns
    the new fabric row id.

    The `signal_ref` JSONB holds enough of the candidate to rehydrate
    a synthetic AnomalyCandidate on promotion — triggering obs ids
    are preserved.
    """
    fabric_id = uuid7()
    signal_ref = {
        "kind": candidate.kind,
        "entity_type": candidate.entity_type,
        "entity_id": str(candidate.entity_id),
        "region_entity_ids": candidate.region_entity_ids,
        "triggering_observation_ids": [
            str(o) for o in candidate.triggering_observation_ids
        ],
        "payload": candidate.payload,
        "trust_tiers": candidate.trust_tiers,
    }
    await conn.execute(
        """
        INSERT INTO signal_memory_fabric
          (id, tenant_id, region_hash, signal_ref, significance)
        VALUES ($1, $2, $3, $4::jsonb, $5)
        """,
        fabric_id,
        candidate.tenant_id,
        region_hash,
        _jsonb(signal_ref),
        float(final_significance),
    )
    return fabric_id


async def promote_if_accumulated(
    tenant_id: UUID,
    region_hash: str,
    conn: asyncpg.Connection,
    *,
    count_threshold: int = PROMOTE_COUNT_THRESHOLD,
    window: timedelta = PROMOTE_WINDOW,
    reference_time: datetime | None = None,
) -> AnomalyCandidate | None:
    """
    If > count_threshold unpromoted rows in `window` days for this
    (tenant, region_hash), promote them:
    - Combine triggering obs ids, region, kind.
    - Compute combined significance = min(1.0, mean_significance + 0.2).
    - Stamp promoted_at=now() on all the rows.
    - Return an AnomalyCandidate the caller should T3-enqueue.
    Else return None.
    """
    ref = reference_time or datetime.now(timezone.utc)
    start = ref - window

    rows = await conn.fetch(
        """
        SELECT id, signal_ref, significance, recorded_at
        FROM signal_memory_fabric
        WHERE tenant_id = $1
          AND region_hash = $2
          AND promoted_at IS NULL
          AND recorded_at >= $3
        ORDER BY recorded_at ASC
        FOR UPDATE
        """,
        tenant_id, region_hash, start,
    )
    if len(rows) <= count_threshold:
        return None

    # Mark all rows promoted.
    await conn.execute(
        """
        UPDATE signal_memory_fabric
        SET promoted_at = now()
        WHERE id = ANY($1::uuid[])
        """,
        [r["id"] for r in rows],
    )

    # Build a combined candidate from the accumulated rows. Use the
    # first row's signal_ref as the shape template — detectors produce
    # the same shape across calls, so the first row is representative.
    first_ref = rows[0]["signal_ref"]
    if isinstance(first_ref, (bytes, bytearray)):
        first_ref = first_ref.decode()
    if isinstance(first_ref, str):
        try:
            first_ref = json.loads(first_ref)
        except json.JSONDecodeError:
            first_ref = {}
    if not isinstance(first_ref, dict):
        first_ref = {}

    # Combine triggering_observation_ids across all rows.
    all_trig_ids: list[UUID] = []
    all_trust_tiers: list[str] = []
    for r in rows:
        ref_obj = r["signal_ref"]
        if isinstance(ref_obj, (bytes, bytearray)):
            ref_obj = ref_obj.decode()
        if isinstance(ref_obj, str):
            try:
                ref_obj = json.loads(ref_obj)
            except json.JSONDecodeError:
                continue
        if not isinstance(ref_obj, dict):
            continue
        for oid in ref_obj.get("triggering_observation_ids") or []:
            try:
                all_trig_ids.append(UUID(str(oid)))
            except (ValueError, TypeError):
                continue
        all_trust_tiers.extend(ref_obj.get("trust_tiers") or [])

    avg_sig = sum(float(r["significance"]) for r in rows) / len(rows)
    promoted_sig = min(1.0, avg_sig + 0.2)

    # Fallback values if signal_ref is garbled.
    kind = first_ref.get("kind", "memory_fabric_promoted")
    entity_type = first_ref.get("entity_type", "unknown")
    try:
        entity_id = UUID(str(first_ref.get("entity_id")))
    except (ValueError, TypeError):
        entity_id = uuid7()
    region = first_ref.get("region_entity_ids") or []
    if not isinstance(region, list):
        region = []
    payload = dict(first_ref.get("payload") or {})
    payload["memory_fabric_promoted"] = True
    payload["accumulated_count"] = len(rows)
    payload["fabric_row_ids"] = [str(r["id"]) for r in rows]

    return AnomalyCandidate(
        kind=kind,
        entity_type=entity_type,
        entity_id=entity_id,
        tenant_id=tenant_id,
        region_entity_ids=region,
        significance=promoted_sig,
        triggering_observation_ids=all_trig_ids,
        payload=payload,
        trust_tiers=all_trust_tiers,
    )


async def list_unpromoted_region_hashes(
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> list[str]:
    """
    Distinct region_hash values with at least one unpromoted row.
    Used by the worker's periodic promotion sweep.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT region_hash
        FROM signal_memory_fabric
        WHERE tenant_id = $1 AND promoted_at IS NULL
        """,
        tenant_id,
    )
    return [r["region_hash"] for r in rows]


__all__ = [
    "PROMOTE_COUNT_THRESHOLD",
    "PROMOTE_WINDOW",
    "list_unpromoted_region_hashes",
    "promote_if_accumulated",
    "record_subthreshold_signal",
]
