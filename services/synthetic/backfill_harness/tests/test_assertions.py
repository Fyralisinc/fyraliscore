"""Unit tests for properties-based assertions.

Exercises each assertion with deliberately-violating fixtures + with
clean fixtures to confirm both the pass and fail paths.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.synthetic.backfill_harness import (
    BackfillScenario,
    HarnessResult,
    PropertyViolation,
    TenantOutcome,
    assert_all_complete,
    assert_completion_emitted_per_tenant,
    assert_cursor_monotonic_per_shard,
    assert_no_duplicate_observations,
    assert_observation_count_matches_fixture,
    assert_reshare_cycles_completed,
)


def _outcome(
    *,
    source: str = "gmail",
    completion_observed: bool = True,
    completion_signal_count: int = 1,
    observations: list[dict] | None = None,
    cursor_history: dict[str, list[dict]] | None = None,
    pass_count: int = 0,
    expected_reshare: bool = False,
    expected_obs: int = 0,
) -> TenantOutcome:
    return TenantOutcome(
        scenario=BackfillScenario(
            tenant_slug="x", source=source,
            expected_observation_count=expected_obs,
        ),
        tenant_id=uuid4(),
        completion_observed=completion_observed,
        completion_signal_count=completion_signal_count,
        observations=observations or [],
        cursor_history=cursor_history or {},
        reconciliation_pass_count=pass_count,
        expected_reshare=expected_reshare,
    )


def _result(*outcomes: TenantOutcome) -> HarnessResult:
    return HarnessResult(outcomes=list(outcomes))


def test_assert_all_complete_passes_when_all_completed() -> None:
    assert_all_complete(_result(_outcome(), _outcome()))


def test_assert_all_complete_fails_when_incomplete() -> None:
    with pytest.raises(PropertyViolation, match="did NOT reach completion"):
        assert_all_complete(
            _result(_outcome(), _outcome(completion_observed=False)),
        )


def test_assert_no_duplicate_observations_passes_on_distinct() -> None:
    assert_no_duplicate_observations(_result(_outcome(
        observations=[{"external_id": "a"}, {"external_id": "b"}],
    )))


def test_assert_no_duplicate_observations_fails_on_duplicates() -> None:
    with pytest.raises(PropertyViolation, match="duplicate observations"):
        assert_no_duplicate_observations(_result(_outcome(
            observations=[{"external_id": "a"}, {"external_id": "a"}],
        )))


def test_assert_cursor_monotonic_passes_when_increasing() -> None:
    assert_cursor_monotonic_per_shard(_result(_outcome(
        cursor_history={"s1": [
            {"pages_fetched": 1}, {"pages_fetched": 2},
        ]},
    )))


def test_assert_cursor_monotonic_fails_on_regression() -> None:
    with pytest.raises(PropertyViolation, match="cursor pages_fetched regressed"):
        assert_cursor_monotonic_per_shard(_result(_outcome(
            cursor_history={"s1": [
                {"pages_fetched": 5}, {"pages_fetched": 3},
            ]},
        )))


def test_assert_completion_emitted_passes_at_exactly_one() -> None:
    assert_completion_emitted_per_tenant(_result(
        _outcome(completion_signal_count=1),
    ))


def test_assert_completion_emitted_fails_on_zero_or_multiple() -> None:
    with pytest.raises(PropertyViolation, match="expected exactly 1"):
        assert_completion_emitted_per_tenant(_result(
            _outcome(completion_signal_count=0),
        ))
    with pytest.raises(PropertyViolation, match="expected exactly 1"):
        assert_completion_emitted_per_tenant(_result(
            _outcome(completion_signal_count=2),
        ))


def test_assert_observation_count_matches_fixture_passes_on_exact() -> None:
    assert_observation_count_matches_fixture(_result(_outcome(
        expected_obs=10,
        observations=[{"external_id": str(i)} for i in range(10)],
    )))


def test_assert_observation_count_fails_outside_tolerance() -> None:
    with pytest.raises(PropertyViolation, match="expected 10 observations"):
        assert_observation_count_matches_fixture(
            _result(_outcome(
                expected_obs=10,
                observations=[{"external_id": str(i)} for i in range(7)],
            )),
            tolerance=0.0,
        )


def test_assert_observation_count_respects_tolerance() -> None:
    assert_observation_count_matches_fixture(
        _result(_outcome(
            expected_obs=100,
            observations=[{"external_id": str(i)} for i in range(95)],
        )),
        tolerance=0.10,  # 10% tolerance
    )


def test_assert_observation_count_skips_when_expected_is_zero() -> None:
    # Scenario didn't set expected_observation_count; assertion no-ops.
    assert_observation_count_matches_fixture(_result(_outcome(
        expected_obs=0,
        observations=[{"external_id": "a"}],
    )))


def test_assert_reshare_cycles_passes_when_pass_count_positive() -> None:
    assert_reshare_cycles_completed(_result(_outcome(
        expected_reshare=True, pass_count=1,
    )))


def test_assert_reshare_cycles_fails_when_expected_but_zero() -> None:
    with pytest.raises(PropertyViolation, match="reconciliation_pass_count"):
        assert_reshare_cycles_completed(_result(_outcome(
            expected_reshare=True, pass_count=0,
        )))


def test_assert_reshare_cycles_skips_when_not_expected() -> None:
    # Scenarios that don't expect reshare are not checked.
    assert_reshare_cycles_completed(_result(_outcome(
        expected_reshare=False, pass_count=0,
    )))
