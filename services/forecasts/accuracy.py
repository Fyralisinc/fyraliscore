"""services/forecasts/accuracy.py — calibration bins + recent
resolutions for the Forecasts > Accuracy tab.

Calibration on the Forecasts page is not the same surface as the
internal Models calibration (which lives in `calibration_stats` and
underwrites the trust score on Today cards). Here it is the
CEO-readable view of: "for predictions I made at ~70% confidence over
the last 6 months, how often did they turn out true?".

The bins follow the spec (§5.3): 50-60, 60-70, 70-80, 80-90, 90-100.
For each bin we report:

  predicted_rate    — the midpoint of the bin, used as the model's
                      claimed probability. Surfaced verbatim by the UI.
  observed_hit_rate — fraction of resolved-in-bin predictions whose
                      outcome was 'true'. 'partial' counts as 0.5.
                      None when n_resolved < MIN_SAMPLES.
  n_resolved        — count of resolved predictions in the bin.

The Accuracy tab also needs:

  recent_resolutions    — last N resolved predictions for the table.
  calibration_summary   — single number + delta vs last week, for the
                          summary strip.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg


# Minimum resolved sample count per bin before we report a hit-rate.
# Mirrors services.calibration.hit_rate.MIN_SAMPLES_FOR_CALIBRATION but
# lower because this surface is user-facing and "n=3" is honest.
MIN_BIN_SAMPLES = 3

# Bins as (lower, upper, label, midpoint). Upper is exclusive except for
# the last bin (which is inclusive at 1.0).
_BINS: tuple[tuple[float, float, str, float], ...] = (
    (0.50, 0.60, "50-60", 0.55),
    (0.60, 0.70, "60-70", 0.65),
    (0.70, 0.80, "70-80", 0.75),
    (0.80, 0.90, "80-90", 0.85),
    (0.90, 1.0001, "90-100", 0.95),
)


@dataclass
class AccuracyBin:
    bin_label: str
    predicted_rate: float
    observed_hit_rate: float | None
    n_resolved: int


@dataclass
class RecentResolution:
    id: UUID
    statement: str
    category: str
    confidence: float
    outcome: str
    resolution_timeliness: str | None
    resolved_at: datetime
    resolution_at: datetime


@dataclass
class CalibrationSummary:
    value: float | None
    delta_vs_last_week: float | None
    n_resolved_total: int


# ---------------------------------------------------------------------
# Accuracy bins
# ---------------------------------------------------------------------


async def accuracy_bins(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    range_days: int = 180,
) -> list[AccuracyBin]:
    """Compute bin-by-bin hit rates over the last `range_days`.

    `'partial'` outcomes count as 0.5. Bins with fewer than
    MIN_BIN_SAMPLES resolved samples report observed_hit_rate=None
    (honest absence).
    """
    range_days = max(7, int(range_days))
    rows = await conn.fetch(
        """
        SELECT confidence, outcome
        FROM predictions
        WHERE tenant_id = $1
          AND status = 'resolved'
          AND outcome IS NOT NULL
          AND resolved_at >= now() - make_interval(days => $2)
        """,
        tenant_id, range_days,
    )

    counters: dict[str, list[float]] = {b[2]: [] for b in _BINS}
    for r in rows:
        conf = float(r["confidence"] or 0.0)
        outcome = r["outcome"]
        for lo, hi, label, _mid in _BINS:
            if lo <= conf < hi:
                if outcome == "true":
                    counters[label].append(1.0)
                elif outcome == "partial":
                    counters[label].append(0.5)
                else:
                    counters[label].append(0.0)
                break

    bins: list[AccuracyBin] = []
    for _lo, _hi, label, mid in _BINS:
        vals = counters[label]
        n = len(vals)
        if n >= MIN_BIN_SAMPLES:
            rate: float | None = sum(vals) / n
        else:
            rate = None
        bins.append(AccuracyBin(
            bin_label=label,
            predicted_rate=mid,
            observed_hit_rate=rate,
            n_resolved=n,
        ))
    return bins


# ---------------------------------------------------------------------
# Recent resolutions list
# ---------------------------------------------------------------------


async def recent_resolutions(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    limit: int = 20,
) -> list[RecentResolution]:
    """Most recently resolved predictions, newest first."""
    limit = max(1, min(int(limit), 200))
    rows = await conn.fetch(
        """
        SELECT id, statement, category, confidence, outcome,
               resolution_timeliness, resolved_at, resolution_at
        FROM predictions
        WHERE tenant_id = $1
          AND status = 'resolved'
          AND outcome IS NOT NULL
          AND resolved_at IS NOT NULL
        ORDER BY resolved_at DESC
        LIMIT $2
        """,
        tenant_id, limit,
    )
    return [
        RecentResolution(
            id=r["id"],
            statement=r["statement"],
            category=r["category"],
            confidence=float(r["confidence"]),
            outcome=r["outcome"],
            resolution_timeliness=r["resolution_timeliness"],
            resolved_at=r["resolved_at"],
            resolution_at=r["resolution_at"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------
# Calibration summary
# ---------------------------------------------------------------------


async def calibration_summary(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> CalibrationSummary:
    """Single calibration score for the summary strip.

    The score is `1 - mean(|predicted - observed|)` over all resolved
    predictions in the last 180 days. 1.0 = perfectly calibrated; 0 =
    worst-case. Returns None when no resolved samples exist.

    The delta is the difference between the current 7-day-trailing
    window and the prior 7-day window. None when either window has no
    resolved samples.
    """
    rows = await conn.fetch(
        """
        SELECT confidence, outcome, resolved_at
        FROM predictions
        WHERE tenant_id = $1
          AND status = 'resolved'
          AND outcome IS NOT NULL
          AND resolved_at IS NOT NULL
          AND resolved_at >= now() - make_interval(days => 180)
        """,
        tenant_id,
    )
    if not rows:
        return CalibrationSummary(value=None, delta_vs_last_week=None,
                                  n_resolved_total=0)

    def _score(records: list[asyncpg.Record]) -> float | None:
        if not records:
            return None
        deltas: list[float] = []
        for r in records:
            obs = 1.0 if r["outcome"] == "true" else (
                0.5 if r["outcome"] == "partial" else 0.0
            )
            deltas.append(abs(float(r["confidence"]) - obs))
        return 1.0 - (sum(deltas) / len(deltas))

    overall = _score(list(rows))

    # Split into two 7-day windows for delta. `resolved_at` already
    # filtered to last 180 days, so we just bucket.
    now = datetime.now(tz=rows[0]["resolved_at"].tzinfo)
    from datetime import timedelta
    cur_lo = now - timedelta(days=7)
    prev_lo = now - timedelta(days=14)
    current_window: list[asyncpg.Record] = [
        r for r in rows if r["resolved_at"] >= cur_lo
    ]
    previous_window: list[asyncpg.Record] = [
        r for r in rows
        if prev_lo <= r["resolved_at"] < cur_lo
    ]
    cur = _score(current_window)
    prev = _score(previous_window)
    delta: float | None
    if cur is None or prev is None:
        delta = None
    else:
        delta = cur - prev

    return CalibrationSummary(
        value=overall,
        delta_vs_last_week=delta,
        n_resolved_total=len(rows),
    )


__all__ = [
    "AccuracyBin",
    "RecentResolution",
    "CalibrationSummary",
    "MIN_BIN_SAMPLES",
    "accuracy_bins",
    "recent_resolutions",
    "calibration_summary",
]
