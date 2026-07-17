from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from lib.evaluation.epistemic_repair.reconciliation import (
    AttemptCost,
    TimingSpan,
    reconcile_costs,
    reconcile_timing,
)


T0 = datetime(2026, 1, 1, tzinfo=UTC)


def at(milliseconds: int) -> datetime:
    return T0 + timedelta(milliseconds=milliseconds)


def test_exclusive_leaves_reconcile_and_inclusive_parent_is_not_double_counted() -> None:
    report = reconcile_timing(
        wall_started_at=at(0),
        wall_ended_at=at(1000),
        spans=[
            TimingSpan("logical_call", at(0), at(1000), "inclusive"),
            TimingSpan("queue", at(0), at(100)),
            TimingSpan("provider", at(100), at(850), physical_attempt_id="a1"),
            TimingSpan("validation", at(850), at(950)),
            TimingSpan("apply", at(950), at(1000)),
        ],
    )

    assert report.reconciled
    assert report.exclusive_sum_ms == pytest.approx(1000)
    assert report.gap_ms == 0
    assert report.overlap_ms == 0


def test_failed_attempt_time_is_included_in_wall_reconciliation() -> None:
    report = reconcile_timing(
        wall_started_at=at(0),
        wall_ended_at=at(1000),
        spans=[
            TimingSpan(
                "provider", at(0), at(300), physical_attempt_id="failed-1",
                attempt_outcome="timeout",
            ),
            TimingSpan("backoff", at(300), at(400)),
            TimingSpan(
                "provider", at(400), at(900), physical_attempt_id="success-2",
                attempt_outcome="success",
            ),
            TimingSpan("validation", at(900), at(1000)),
        ],
    )

    assert report.reconciled
    assert report.failed_attempt_ids == ("failed-1",)


def test_gap_or_overlap_beyond_one_percent_fails_objectively() -> None:
    gap = reconcile_timing(
        wall_started_at=at(0), wall_ended_at=at(1000),
        spans=[TimingSpan("work", at(0), at(980))],
    )
    overlap = reconcile_timing(
        wall_started_at=at(0), wall_ended_at=at(1000),
        spans=[TimingSpan("one", at(0), at(600)), TimingSpan("two", at(589), at(1000))],
    )

    assert not gap.reconciled
    assert gap.relative_error == pytest.approx(0.02)
    assert not overlap.reconciled
    assert overlap.overlap_ms == pytest.approx(11)


def test_one_percent_boundary_is_accepted() -> None:
    report = reconcile_timing(
        wall_started_at=at(0), wall_ended_at=at(1000),
        spans=[TimingSpan("work", at(0), at(990))],
    )
    assert report.reconciled


def test_costs_count_failed_attempts_and_prefer_actual_over_estimate() -> None:
    report = reconcile_costs([
        AttemptCost("a1", "provider_error", 100, 0, estimated_cost_usd=Decimal("0.01")),
        AttemptCost(
            "a2", "success", 200, 50,
            actual_cost_usd=Decimal("0.025"), estimated_cost_usd=Decimal("0.03"),
        ),
    ])

    assert report.reconciled
    assert report.attempt_count == 2
    assert report.failed_attempt_count == 1
    assert report.input_tokens == 300
    assert report.output_tokens == 50
    assert report.best_available_cost_usd == Decimal("0.035")
    assert report.actual_estimate_delta_usd == Decimal("-0.005")


def test_missing_usage_is_continuous_coverage_not_false_zero() -> None:
    report = reconcile_costs([
        AttemptCost("known", "success", 10, 5, actual_cost_usd=Decimal("0.002")),
        AttemptCost("unknown", "timeout", None, None),
    ])

    assert not report.reconciled
    assert report.token_coverage == 0.5
    assert report.cost_coverage == 0.5
    assert report.failed_attempt_count == 1


def test_duplicate_attempt_id_is_an_integrity_failure() -> None:
    report = reconcile_costs([
        AttemptCost("same", "success", 1, 1, estimated_cost_usd=Decimal("0.1")),
        AttemptCost("same", "success", 1, 1, estimated_cost_usd=Decimal("0.1")),
    ])
    assert not report.reconciled
    assert "duplicate physical_attempt_id: same" in report.errors
