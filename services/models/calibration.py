"""
services/models/calibration.py — calibration offset lookup.

Wave 4-C implementation. Spec reference: ARCHITECTURE-FINAL.md §9,
"Application at validation time (during Think.validate)".

Historical note (from Wave 1-C)
-------------------------------
Wave 1 shipped this module as an identity stub so the insert pipeline
could be fully wired before the underlying `calibration_offsets` table
existed. Wave 4-C replaces the identity implementation with the real
lookup against the table landed in `db/migrations/0011_calibration_tables.sql`.

Public surface
--------------
`async apply_calibration(confidence, actor_ids, proposition_kind, *, tenant_id, conn=None)`
    Returns the calibrated confidence, clipped to [0.05, 0.95].
    Two-tier resolution:
      1. Empirical — matching row in `calibration_offsets` with
         sample_size >= MIN_SAMPLE_SIZE_FOR_EMPIRICAL (20). Use that
         row's `offset`.
      2. Cold-start — no row OR row has sample_size < threshold. Use
         `PROP_KIND_DEFAULTS[proposition_kind]` (unknown kind → 1.0).

    Identity fallback fires only when inputs are insufficient
    (missing conn/tenant/kind/actors), never for the normal
    "no history yet" case — that path returns the per-kind default,
    which was the whole point of the cold-start fix (review §C5).

`apply_calibration_sync(...)` — sync identity fallback. Retained so
    call sites that cannot await still compile; production path is
    always the async one.

Cold-start policy (ARCHITECTURE-REVIEW-1 §C5)
---------------------------------------------
Two regimes:
  * cold-start:  no `calibration_offsets` row, OR row with
                 sample_size < MIN_SAMPLE_SIZE_FOR_EMPIRICAL (20).
                 Multiplier = `PROP_KIND_DEFAULTS[proposition_kind]`.
  * empirical:   row exists with sample_size >= 20.
                 Multiplier = row.offset.

The hot path is still a single indexed SELECT; the cold-start branch
only uses an in-memory lookup, no extra DB round-trip.
"""
from __future__ import annotations

from typing import Any, Sequence
from uuid import UUID

import asyncpg


_CONFIDENCE_MIN = 0.05
_CONFIDENCE_MAX = 0.95

# Per ARCHITECTURE-REVIEW-1 §C5: threshold at which a bucket switches
# from "cold-start, use default" to "empirical, use row.offset". Mirrors
# services/workers/calibration_updater/compute.py::MIN_SAMPLES_PER_TUPLE.
MIN_SAMPLE_SIZE_FOR_EMPIRICAL: int = 20

# Per-kind defaults (identical to calibration_updater.compute.PROP_KIND_DEFAULTS).
# Imported via re-export rather than local copy so the two modules can
# never drift. If the import fails at module import time (circular),
# fall back to a local copy.
try:
    from services.workers.calibration_updater.compute import (
        PROP_KIND_DEFAULTS,
    )
except Exception:  # pragma: no cover — defensive
    PROP_KIND_DEFAULTS = {
        "state":                 0.95,
        "relation":              0.93,
        "prediction":            0.85,
        "pattern":               0.90,
        "pattern_instance":      0.90,
        "capability_assessment": 0.88,
        "hypothesis":            0.80,
        "concern":               0.92,
        "market_assessment":     0.87,
        "environmental_trend":   0.90,
        "recommendation":        0.85,
    }

_DEFAULT_FALLBACK = 1.0


def _clip(value: float) -> float:
    if value < _CONFIDENCE_MIN:
        return _CONFIDENCE_MIN
    if value > _CONFIDENCE_MAX:
        return _CONFIDENCE_MAX
    return float(value)


def _cold_start_multiplier(proposition_kind: str | None) -> float:
    if proposition_kind is None:
        return _DEFAULT_FALLBACK
    return PROP_KIND_DEFAULTS.get(proposition_kind, _DEFAULT_FALLBACK)


async def apply_calibration(
    confidence: float,
    actor_ids: Sequence[UUID] | None = None,
    proposition_kind: str | None = None,
    *,
    tenant_id: UUID | None = None,
    conn: asyncpg.Connection | None = None,
) -> float:
    """
    Look up the (actor, proposition_kind, bucket) offset and apply it.

    Cold-start regime returns `confidence * PROP_KIND_DEFAULTS[kind]`
    clipped. Empirical regime returns `confidence * row.offset` clipped.
    Identity only when inputs are insufficient.
    """
    raw = float(confidence)

    # Insufficient inputs → identity (no actor, no tenant, no conn).
    if not actor_ids or proposition_kind is None or tenant_id is None or conn is None:
        return _clip(raw)

    primary_actor = actor_ids[0]
    row = await conn.fetchrow(
        """
        SELECT "offset", sample_size
        FROM calibration_offsets
        WHERE tenant_id = $1
          AND actor_id = $2
          AND proposition_kind = $3
          AND bucket_low <= $4
          AND bucket_high > $4
        LIMIT 1
        """,
        tenant_id,
        primary_actor,
        proposition_kind,
        raw,
    )

    if row is None or int(row["sample_size"] or 0) < MIN_SAMPLE_SIZE_FOR_EMPIRICAL:
        # Cold-start regime.
        mult = _cold_start_multiplier(proposition_kind)
        return _clip(raw * mult)

    offset = float(row["offset"])
    return _clip(raw * offset)


def apply_calibration_sync(
    confidence: float,
    actor_ids: Sequence[UUID] | None = None,
    proposition_kind: str | None = None,
    *,
    tenant_id: UUID | None = None,
    conn: Any | None = None,
) -> float:
    """
    Sync identity fallback. Used by callers that cannot await (no
    live callers today — retained so Wave 1-C's fast-path tests that
    imported this symbol keep compiling).
    """
    return _clip(float(confidence))


__all__ = ["apply_calibration", "apply_calibration_sync"]
