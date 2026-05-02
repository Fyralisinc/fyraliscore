"""
services/workers/calibration_updater/compute.py — pure-compute core.

No DB access. Every function here takes data in, returns data out.
This is what the Brier-score + bucket-rate fixtures in
services/workers/calibration_updater/tests/ exercise.

Spec: ARCHITECTURE-FINAL.md §9 "Weekly calibration update".

Key functions
-------------
`brier_score(stats) -> float`
    Mean squared error of predicted_probability vs binary outcome.
    Only stats with outcome ∈ {True, False} count; None (inconclusive)
    is dropped.

`bucketed_offsets(stats, *, min_samples_per_bucket=5) -> list[OffsetRow]`
    For every bucket in `CONFIDENCE_BUCKETS` that has >= 5 resolved
    stats, compute offset = empirical_rate / bucket_midpoint, clipped
    to [0.3, 1.5]. Returns one OffsetRow per bucket.

`cold_start_offsets(proposition_kind) -> list[OffsetRow]`
    Returns one full-range bucket (0.0, 1.0) with the per-kind default
    offset. Used when < 20 samples are available.

Why full-range for cold-start?
------------------------------
Spec §9 "Cold-start handling" says "Use proposition-kind defaults
(predictions: 0.85x offset; states: 0.95x; patterns: 0.90x)". We
materialise those defaults as actual DB rows keyed on a catch-all
bucket (0.0-1.0) so the read path in
`services/models/calibration.py::apply_calibration` can be a single
indexed SELECT without special-casing cold start.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


# Spec §9 lines 2644-2645.
CONFIDENCE_BUCKETS: list[tuple[float, float]] = [
    (0.0, 0.2),
    (0.2, 0.4),
    (0.4, 0.6),
    (0.6, 0.7),
    (0.7, 0.8),
    (0.8, 0.9),
    (0.9, 1.0),
]

# Full-range bucket used by cold-start to provide a single row that
# `apply_calibration`'s bucket_low <= $4 AND bucket_high > $4 query
# can always hit (we set bucket_high=1.01 so 1.0 still matches).
COLD_START_BUCKET_LOW = 0.0
COLD_START_BUCKET_HIGH = 1.01

# Per ARCHITECTURE-FINAL.md §9 "Cold-start handling" (and
# ARCHITECTURE-REVIEW-1 §C5), one default per proposition kind. Must
# cover every `PropositionKind` value — a missing kind is a spec
# defect, not a runtime condition. Calibration cold-start is the
# single path by which a new-tenant 0.85-prediction becomes 0.72 on
# insert, so the defaults are load-bearing for the Model insert
# falsifier-adequacy threshold (Invariant M2, confidence > 0.7).
PROP_KIND_DEFAULTS: dict[str, float] = {
    "state":                 0.95,
    "relation":              0.93,
    "prediction":            0.85,
    "pattern":               0.90,
    "pattern_instance":      0.90,
    "capability_assessment": 0.88,
    "hypothesis":            0.80,   # hypotheses are over-confident on assertion
    "concern":               0.92,
    "market_assessment":     0.87,
    "environmental_trend":   0.90,
    # Recommendations are inferential ("you should do X") and prone to
    # over-confidence in the same way as predictions; mirror that prior.
    "recommendation":        0.85,
}

# Back-compat alias. Older call sites reference `DEFAULT_OFFSETS`.
DEFAULT_OFFSETS = PROP_KIND_DEFAULTS

# If a caller supplies a kind not in the table (a bug or a
# forward-compatible kind we don't know about yet), fall back to 1.0
# (identity) rather than raising — cold-start must not crash the hot
# path. The updater logs a structured warning when this fires.
_DEFAULT_FALLBACK = 1.0

MIN_SAMPLES_PER_TUPLE = 20
MIN_SAMPLES_PER_BUCKET = 5
OFFSET_MIN = 0.3
OFFSET_MAX = 1.5


@dataclass(frozen=True)
class Stat:
    """A single resolved prediction. `outcome` None => inconclusive."""
    asserted_confidence: float
    outcome: bool | None


@dataclass(frozen=True)
class OffsetRow:
    """One row ready for upsert into `calibration_offsets`."""
    bucket_low: float
    bucket_high: float
    offset: float
    sample_size: int


def brier_score(stats: Sequence[Stat]) -> float | None:
    """
    Mean (asserted - outcome)^2 across conclusive stats.

    Returns None when no conclusive stats are present (matches the
    spec's unstated-but-implied "no data → don't compute").
    """
    concluded = [s for s in stats if s.outcome is not None]
    if not concluded:
        return None
    total = 0.0
    for s in concluded:
        y = 1.0 if s.outcome else 0.0
        diff = s.asserted_confidence - y
        total += diff * diff
    return total / len(concluded)


def _clip(value: float, lo: float, hi: float) -> float:
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


def bucketed_offsets(
    stats: Sequence[Stat],
    *,
    min_samples_per_bucket: int = MIN_SAMPLES_PER_BUCKET,
) -> list[OffsetRow]:
    """
    Compute per-bucket offsets from stats.

    Only buckets with >= `min_samples_per_bucket` conclusive stats
    contribute. Offset = empirical_rate / bucket_midpoint, clipped to
    [OFFSET_MIN, OFFSET_MAX].
    """
    out: list[OffsetRow] = []
    for low, high in CONFIDENCE_BUCKETS:
        bucket = [
            s for s in stats
            if s.outcome is not None
            and low <= s.asserted_confidence < high
        ]
        if len(bucket) < min_samples_per_bucket:
            continue
        empirical = sum(1.0 if s.outcome else 0.0 for s in bucket) / len(bucket)
        midpoint = (low + high) / 2.0
        if midpoint <= 0.0:
            # Pathological: the 0.0-0.2 bucket's midpoint is 0.1, never 0;
            # kept for safety.
            raw_offset = 1.0
        else:
            raw_offset = empirical / midpoint
        offset = _clip(raw_offset, OFFSET_MIN, OFFSET_MAX)
        out.append(OffsetRow(
            bucket_low=low,
            bucket_high=high,
            offset=offset,
            sample_size=len(bucket),
        ))
    return out


def cold_start_offsets(proposition_kind: str) -> list[OffsetRow]:
    """
    Single full-range bucket with the per-kind default offset.

    Emitted when a (tenant, actor, kind) tuple has fewer than
    MIN_SAMPLES_PER_TUPLE conclusive stats.
    """
    default = DEFAULT_OFFSETS.get(proposition_kind, _DEFAULT_FALLBACK)
    return [OffsetRow(
        bucket_low=COLD_START_BUCKET_LOW,
        bucket_high=COLD_START_BUCKET_HIGH,
        offset=default,
        sample_size=0,
    )]


def compute_offsets_for_tuple(
    stats: Sequence[Stat],
    proposition_kind: str,
    *,
    min_samples_per_tuple: int = MIN_SAMPLES_PER_TUPLE,
) -> list[OffsetRow]:
    """
    Policy layer over `bucketed_offsets` and `cold_start_offsets`.

    If we have fewer than `min_samples_per_tuple` conclusive stats,
    emit a single cold-start row. Otherwise compute per-bucket
    offsets. If per-bucket computation yields zero rows (all buckets
    below min_samples_per_bucket), fall back to cold-start — this
    avoids the weird case where an actor has 30 stats all in bucket
    0.6-0.7 and every other bucket is empty, but we still want *some*
    offset row for the tuple.
    """
    concluded = [s for s in stats if s.outcome is not None]
    if len(concluded) < min_samples_per_tuple:
        return cold_start_offsets(proposition_kind)
    bucketed = bucketed_offsets(concluded)
    if not bucketed:
        return cold_start_offsets(proposition_kind)
    return bucketed


__all__ = [
    "Stat",
    "OffsetRow",
    "CONFIDENCE_BUCKETS",
    "COLD_START_BUCKET_LOW",
    "COLD_START_BUCKET_HIGH",
    "DEFAULT_OFFSETS",
    "MIN_SAMPLES_PER_TUPLE",
    "MIN_SAMPLES_PER_BUCKET",
    "OFFSET_MIN",
    "OFFSET_MAX",
    "brier_score",
    "bucketed_offsets",
    "cold_start_offsets",
    "compute_offsets_for_tuple",
]
