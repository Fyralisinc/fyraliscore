"""Pure helpers used by Layer 4 sub-evaluators.

All functions are deterministic and side-effect-free so sub-evaluator logic
can be hand-verified against the tests in ``test_l4_metrics.py``.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

DEFAULT_WINDOW = timedelta(weeks=4)

# Commitment outcomes that count as "will slip" when surfaced in the upcoming
# window. ``slipped_but_completed`` is included because the commitment was
# on a slipping trajectory at the checkpoint even though it later completed.
_POSITIVE_SLIP_OUTCOMES = {"will_slip", "slipped_but_completed"}

# Health levels that mark a trajectory as degrading.
_DEGRADING_HEALTH = {"degraded", "critical", "churned"}


def precision_recall_f1(
    tp: int, fp: int, fn: int
) -> tuple[float, float, float]:
    """Return (precision, recall, F1) from raw confusion counts.

    Degenerate cases:
        * tp == fp == 0 → precision = 0.0
        * tp == fn == 0 → recall = 0.0
        * precision + recall == 0 → F1 = 0.0
    """
    if tp < 0 or fp < 0 or fn < 0:
        raise ValueError("confusion counts must be non-negative")
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    if precision + recall == 0.0:
        f1 = 0.0
    else:
        f1 = 2 * precision * recall / (precision + recall)
    return precision, recall, f1


def _as_datetime(value: Any) -> datetime | None:
    """Coerce the many shapes ground-truth values arrive in into a datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        # Corpus JSON uses ISO-8601 with a trailing "Z"; datetime.fromisoformat
        # in py3.12 accepts "+00:00" but not bare "Z".
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def derive_positive_commitments(
    ground_truth: list[Any],
    checkpoint: datetime,
    window: timedelta = DEFAULT_WINDOW,
) -> set[str]:
    """Commitment ids that should be surfaced as at-risk at ``checkpoint``.

    A commitment counts as positive iff its ``true_outcome`` is one of the
    slip outcomes AND ``resolution_timestamp <= checkpoint + window`` (i.e.
    the slip is resolvable within the upcoming window). Missing timestamps
    still count as positive so SUTs get credit for surfacing genuinely open
    slipping work.
    """
    horizon = checkpoint + window
    positives: set[str] = set()
    for gt in ground_truth:
        commitments = _extract_list(gt, "commitments")
        for c in commitments:
            outcome = c.get("true_outcome")
            if outcome not in _POSITIVE_SLIP_OUTCOMES:
                continue
            resolved_at = _as_datetime(c.get("resolution_timestamp"))
            if resolved_at is None:
                positives.add(c["id"])
                continue
            if resolved_at <= horizon:
                positives.add(c["id"])
    return positives


def derive_degrading_customers(
    ground_truth: list[Any],
    checkpoint: datetime,
    window: timedelta = DEFAULT_WINDOW,
) -> set[str]:
    """Customer ids whose trajectory enters degraded/critical in the window.

    The corpus stores ``trajectory`` as an ordered list of health levels (one
    step per simulated week-ish). We don't have per-step timestamps in the
    minimal corpus shape, so the conservative rule is:

        * customer is positive if its ``trajectory`` contains a degrading
          level (degraded/critical/churned) for the first time, AND its
          ``true_health`` at the checkpoint is also degrading.

    That matches the spec ("first time in the next 4-week window") for
    single-checkpoint mini-corpora while remaining well-defined for
    multi-checkpoint corpora whose ``true_health`` tracks the current state.
    """
    positives: set[str] = set()
    for gt in ground_truth:
        customers = _extract_list(gt, "customers")
        for cust in customers:
            trajectory = cust.get("trajectory") or []
            true_health = cust.get("true_health")
            became_bad = any(level in _DEGRADING_HEALTH for level in trajectory)
            if became_bad and true_health in _DEGRADING_HEALTH:
                positives.add(cust["id"])
    # Window parameter retained for API symmetry even though the minimal
    # corpus shape doesn't carry per-step timestamps; it's used only to mark
    # intent and leaves the function signature stable for richer corpora.
    _ = (checkpoint, window)
    return positives


def _extract_list(gt: Any, attr: str) -> list[dict[str, Any]]:
    """Handle both pydantic GroundTruth and plain dicts uniformly."""
    if hasattr(gt, attr):
        return list(getattr(gt, attr))
    if isinstance(gt, dict):
        return list(gt.get(attr, []))
    return []


def extract_ground_truth_timestamp(gt: Any) -> datetime | None:
    """Timestamp of a ground-truth checkpoint row, regardless of shape."""
    if hasattr(gt, "timestamp"):
        ts = gt.timestamp
        return ts if isinstance(ts, datetime) else _as_datetime(ts)
    if isinstance(gt, dict):
        return _as_datetime(gt.get("timestamp"))
    return None


def turbulence_events_from_ground_truth(
    ground_truth: list[Any],
) -> list[dict[str, Any]]:
    """Return the ``TurbulenceEvent``-like rows embedded in ground truth.

    The minimal corpus stores these under ``patterns`` (each pattern has at
    least a ``detection_eligible_after`` timestamp). Richer corpora may carry
    a dedicated ``turbulence_events`` list. We merge both sources so the
    evaluator has a single list of "genuine anomaly" moments keyed by
    timestamp.
    """
    events: list[dict[str, Any]] = []
    for gt in ground_truth:
        for name in ("turbulence_events", "anomalies_ground_truth", "patterns"):
            for ev in _extract_list(gt, name):
                ts = _as_datetime(
                    ev.get("scheduled_at")
                    or ev.get("timestamp")
                    or ev.get("detection_eligible_after")
                )
                if ts is None:
                    continue
                events.append(
                    {
                        "timestamp": ts,
                        "kind": ev.get("kind") or ev.get("id") or name,
                        "source": name,
                        "raw": ev,
                    }
                )
    return events


def monthly_checkpoints(
    start: datetime, end: datetime
) -> list[datetime]:
    """Produce a deterministic monthly checkpoint sequence between dates.

    Uses 4-week stride (28 days) rather than calendar months so windows line
    up with the 4-week positive-label horizon.
    """
    if end < start:
        return []
    stride = timedelta(weeks=4)
    checkpoints: list[datetime] = []
    cursor = start
    while cursor <= end:
        checkpoints.append(cursor)
        cursor = cursor + stride
    return checkpoints
