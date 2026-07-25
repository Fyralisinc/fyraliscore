from __future__ import annotations

import asyncio

import pytest

from services.ingest.synthetic.provider_lab.calibration import (
    LabCalibrationConfig,
    assess_lab_calibration,
    calibrate_provider_lab,
    require_lab_calibration,
)


def test_assessment_enforces_two_x_capacity_and_ten_percent_p99() -> None:
    config = LabCalibrationConfig(
        target_fyralis_rps=100,
        client_timeout_seconds=1,
        minimum_samples=200,
    )
    passed = assess_lab_calibration(
        config=config,
        elapsed_seconds=1,
        latencies_seconds=[0.05] * 205,
        failed_requests=0,
    )
    assert passed.passed is True
    assert passed.capacity_ratio == 2.05
    assert passed.p99_timeout_ratio == 0.05
    require_lab_calibration(passed)

    too_slow = assess_lab_calibration(
        config=config,
        elapsed_seconds=1,
        latencies_seconds=[0.11] * 205,
        failed_requests=0,
    )
    assert too_slow.passed is False
    with pytest.raises(RuntimeError, match="p99/timeout ratio"):
        require_lab_calibration(too_slow)


async def test_live_calibrator_drives_exact_injected_operation() -> None:
    calls = 0

    async def operation() -> bool:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return True

    result = await calibrate_provider_lab(
        operation,
        config=LabCalibrationConfig(
            target_fyralis_rps=10,
            client_timeout_seconds=1,
            probe_seconds=0.02,
            concurrency=2,
            minimum_samples=1,
        ),
    )

    assert calls == result.attempted_requests
    assert result.successful_requests > 0
    assert result.passed


async def test_calibrator_is_forbidden_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")

    async def operation() -> bool:
        raise AssertionError("must not execute")

    with pytest.raises(RuntimeError, match="cannot run in production"):
        await calibrate_provider_lab(
            operation,
            config=LabCalibrationConfig(
                target_fyralis_rps=1,
                client_timeout_seconds=1,
                probe_seconds=0.01,
                minimum_samples=1,
            ),
        )
