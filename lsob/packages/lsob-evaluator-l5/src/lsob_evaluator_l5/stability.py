"""Helpers for finding ground-truth facts that remain stable over windows.

A "stable window" is N consecutive checkpoints where a scalar fact holds the
same value. The canonical use case is: for each commitment, find windows where
`true_outcome` stays constant for 6 months.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from lsob_contracts import GroundTruth


@dataclass(frozen=True)
class StableWindow:
    """Gold fact that was stable over a contiguous block of checkpoints."""

    entity_kind: str
    entity_id: str
    field: str  # e.g. "true_outcome" or "true_health"
    value: str  # the stable value
    start_timestamp: datetime
    end_timestamp: datetime
    checkpoint_timestamps: list[datetime]


def _read_field(row: dict, field: str) -> str | None:
    v = row.get(field)
    if v is None:
        return None
    return str(v)


def find_stable_windows(
    ground_truth: Sequence[GroundTruth],
    *,
    window: int = 6,
) -> list[StableWindow]:
    """Find windows of size `window` where an entity keeps a constant value.

    Looks at `commitments[*].true_outcome` and `customers[*].true_health`.
    Walks ground truth in declared (chronological) order. Each maximal run of
    >= `window` consecutive matching values produces exactly one window — the
    first `window` checkpoints in that run, so downstream stability checks
    have a well-defined start/end.
    """
    # Organize values per (entity_kind, entity_id, field) in checkpoint order.
    series: dict[tuple[str, str, str], list[tuple[datetime, str]]] = {}
    for gt in ground_truth:
        for c in gt.commitments:
            cid = c.get("id") or c.get("commitment_id")
            if cid is None:
                continue
            outcome = _read_field(c, "true_outcome")
            if outcome is None:
                continue
            series.setdefault(("commitment", str(cid), "true_outcome"), []).append(
                (gt.timestamp, outcome)
            )
        for cu in gt.customers:
            cuid = cu.get("id") or cu.get("customer_id")
            if cuid is None:
                continue
            health = _read_field(cu, "true_health")
            if health is None:
                continue
            series.setdefault(("customer", str(cuid), "true_health"), []).append(
                (gt.timestamp, health)
            )

    out: list[StableWindow] = []
    for (kind, entity_id, field), pairs in series.items():
        if len(pairs) < window:
            continue
        # Scan for maximal runs of identical values.
        i = 0
        while i <= len(pairs) - window:
            value = pairs[i][1]
            run_end = i
            while run_end + 1 < len(pairs) and pairs[run_end + 1][1] == value:
                run_end += 1
            run_len = run_end - i + 1
            if run_len >= window:
                window_points = pairs[i : i + window]
                out.append(
                    StableWindow(
                        entity_kind=kind,
                        entity_id=entity_id,
                        field=field,
                        value=value,
                        start_timestamp=window_points[0][0],
                        end_timestamp=window_points[-1][0],
                        checkpoint_timestamps=[p[0] for p in window_points],
                    )
                )
                # Skip past this run; one window per run is sufficient.
                i = run_end + 1
            else:
                i += 1
    return out
