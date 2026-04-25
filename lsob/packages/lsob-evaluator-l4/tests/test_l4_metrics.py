"""Hand-computed tests for Layer 4 metric helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from lsob_evaluator_l4.metrics import (
    DEFAULT_WINDOW,
    derive_degrading_customers,
    derive_positive_commitments,
    monthly_checkpoints,
    precision_recall_f1,
    turbulence_events_from_ground_truth,
)

UTC = timezone.utc


def test_precision_recall_f1_exact_match() -> None:
    p, r, f = precision_recall_f1(tp=2, fp=0, fn=0)
    assert p == 1.0
    assert r == 1.0
    assert f == 1.0


def test_precision_recall_f1_hand_values() -> None:
    # tp=2, fp=1, fn=1 → P=2/3, R=2/3, F1=2/3
    p, r, f = precision_recall_f1(tp=2, fp=1, fn=1)
    assert p == pytest.approx(2 / 3)
    assert r == pytest.approx(2 / 3)
    assert f == pytest.approx(2 / 3)


def test_precision_recall_f1_zero_predictions() -> None:
    p, r, f = precision_recall_f1(tp=0, fp=0, fn=3)
    assert p == 0.0
    assert r == 0.0
    assert f == 0.0


def test_precision_recall_f1_asymmetric() -> None:
    # tp=1, fp=3, fn=0 → P=0.25, R=1.0, F1=0.4
    p, r, f = precision_recall_f1(tp=1, fp=3, fn=0)
    assert p == pytest.approx(0.25)
    assert r == pytest.approx(1.0)
    assert f == pytest.approx(0.4)


def test_precision_recall_f1_rejects_negative() -> None:
    with pytest.raises(ValueError):
        precision_recall_f1(tp=-1, fp=0, fn=0)


def test_derive_positive_commitments_includes_slip_within_window() -> None:
    checkpoint = datetime(2026, 1, 1, tzinfo=UTC)
    gt = {
        "timestamp": checkpoint,
        "commitments": [
            {
                "id": "C1",
                "true_outcome": "will_slip",
                "resolution_timestamp": datetime(2026, 1, 20, tzinfo=UTC),
            },
            {
                "id": "C2",
                "true_outcome": "slipped_but_completed",
                "resolution_timestamp": datetime(2026, 1, 15, tzinfo=UTC),
            },
            {
                "id": "C3",
                "true_outcome": "will_succeed",
                "resolution_timestamp": datetime(2026, 1, 20, tzinfo=UTC),
            },
            {
                "id": "C4",
                "true_outcome": "will_slip",
                "resolution_timestamp": datetime(2026, 3, 1, tzinfo=UTC),
            },
        ],
    }
    positives = derive_positive_commitments([gt], checkpoint, DEFAULT_WINDOW)
    assert positives == {"C1", "C2"}


def test_derive_positive_commitments_handles_missing_resolution() -> None:
    checkpoint = datetime(2026, 1, 1, tzinfo=UTC)
    gt = {
        "timestamp": checkpoint,
        "commitments": [
            {"id": "C-open", "true_outcome": "will_slip"},
        ],
    }
    assert derive_positive_commitments([gt], checkpoint) == {"C-open"}


def test_derive_positive_commitments_respects_window_param() -> None:
    checkpoint = datetime(2026, 1, 1, tzinfo=UTC)
    gt = {
        "timestamp": checkpoint,
        "commitments": [
            {
                "id": "C-late",
                "true_outcome": "will_slip",
                "resolution_timestamp": datetime(2026, 1, 10, tzinfo=UTC),
            },
        ],
    }
    # Narrow window excludes it; wide window includes it.
    assert derive_positive_commitments(
        [gt], checkpoint, window=timedelta(days=5)
    ) == set()
    assert derive_positive_commitments(
        [gt], checkpoint, window=timedelta(days=30)
    ) == {"C-late"}


def test_derive_positive_commitments_accepts_iso_strings() -> None:
    checkpoint = datetime(2026, 1, 1, tzinfo=UTC)
    gt = {
        "timestamp": "2026-01-01T00:00:00Z",
        "commitments": [
            {
                "id": "C-iso",
                "true_outcome": "will_slip",
                "resolution_timestamp": "2026-01-15T00:00:00Z",
            },
        ],
    }
    assert derive_positive_commitments([gt], checkpoint) == {"C-iso"}


def test_derive_degrading_customers_matches_degraded_true_health() -> None:
    checkpoint = datetime(2026, 1, 31, tzinfo=UTC)
    gt = {
        "timestamp": checkpoint,
        "customers": [
            {
                "id": "acme",
                "true_health": "degraded",
                "trajectory": ["healthy", "warning", "degraded"],
            },
            {
                "id": "stable",
                "true_health": "healthy",
                "trajectory": ["healthy", "healthy"],
            },
            {
                "id": "warned-only",
                "true_health": "warning",
                "trajectory": ["healthy", "warning"],
            },
            {
                "id": "critical-now",
                "true_health": "critical",
                "trajectory": ["warning", "degraded", "critical"],
            },
        ],
    }
    assert derive_degrading_customers([gt], checkpoint) == {"acme", "critical-now"}


def test_turbulence_events_from_patterns() -> None:
    gt = {
        "timestamp": datetime(2026, 1, 31, tzinfo=UTC),
        "patterns": [
            {
                "id": "P-alice-optimism",
                "description": "x",
                "detection_eligible_after": "2026-01-16T00:00:00Z",
            },
        ],
    }
    events = turbulence_events_from_ground_truth([gt])
    assert len(events) == 1
    assert events[0]["timestamp"] == datetime(2026, 1, 16, tzinfo=UTC)
    assert events[0]["source"] == "patterns"


def test_turbulence_events_merge_sources() -> None:
    gt = {
        "timestamp": datetime(2026, 1, 31, tzinfo=UTC),
        "turbulence_events": [
            {
                "event_id": "T1",
                "kind": "layoff",
                "scheduled_at": "2026-01-10T00:00:00Z",
            },
        ],
        "patterns": [
            {
                "id": "P1",
                "detection_eligible_after": "2026-01-20T00:00:00Z",
            },
        ],
    }
    events = turbulence_events_from_ground_truth([gt])
    timestamps = sorted(ev["timestamp"] for ev in events)
    assert timestamps == [
        datetime(2026, 1, 10, tzinfo=UTC),
        datetime(2026, 1, 20, tzinfo=UTC),
    ]


def test_monthly_checkpoints_inclusive_stride() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 3, 1, tzinfo=UTC)
    checkpoints = monthly_checkpoints(start, end)
    # 4-week stride: Jan 1, Jan 29, Feb 26.
    assert checkpoints == [
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 29, tzinfo=UTC),
        datetime(2026, 2, 26, tzinfo=UTC),
    ]
