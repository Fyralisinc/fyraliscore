from __future__ import annotations

from dataclasses import replace

import pytest

from services.ingest.source_certification.load_search import (
    LoadMeasurement,
    LoadSearchConfig,
    certification_stable,
    find_maximum_stable_rate,
)
from services.ingest.synthetic.provider_lab.calibration import (
    LabCalibrationConfig,
    assess_lab_calibration,
)


def _measurement(rate: float, seconds: int, *, stable: bool) -> LoadMeasurement:
    return LoadMeasurement(
        offered_rate=rate,
        duration_seconds=seconds,
        stable=stable,
        requests_per_second=min(rate, 100),
        quota_units_per_second=min(rate, 100),
        records_per_second=min(rate, 100),
        bytes_per_second=min(rate, 100) * 1000,
        p50_latency_ms=5,
        p95_latency_ms=10,
        p99_latency_ms=20,
        kafka_lag=0,
        observation_p99_latency_ms=50,
        backlog_growth_per_second=0 if stable else 1,
        missing_records=0,
        unexpected_duplicates=0,
        cross_tenant_leaks=0,
        cooldown_violations=0,
        cursor_consistency_errors=0,
        dlq_entries=0,
        cpu_percent=50,
        memory_bytes=1024,
        limiting_component="provider_quota" if stable else "kafka",
    )


def _lab_calibration(*, capacity_ratio: float = 3):
    config = LabCalibrationConfig(
        target_fyralis_rps=100,
        client_timeout_seconds=1,
        minimum_samples=100,
    )
    return assess_lab_calibration(
        config=config,
        elapsed_seconds=1,
        latencies_seconds=[0.05] * int(100 * capacity_ratio),
        failed_requests=0,
    )


async def test_search_steps_brackets_and_validates_within_five_percent() -> None:
    calls = []

    async def run(rate, seconds, phase, mode):  # noqa: ANN001,ANN202
        calls.append((rate, seconds, phase, mode))
        return _measurement(rate, seconds, stable=rate <= 100)

    envelope = await find_maximum_stable_rate(
        run,
        mode="provider_safe",
        config=LoadSearchConfig(initial_rate=40),
        lab_calibration=_lab_calibration(),
        include_soak=True,
    )

    assert 95 <= envelope.maximum_stable_rate <= 100
    assert envelope.tolerance_fraction <= 0.05
    assert envelope.validation.duration_seconds == 900
    assert envelope.soak is not None
    assert envelope.soak.duration_seconds == 3600
    assert calls[0][2] == "warmup"
    assert any(call[2] == "binary_search" for call in calls)


async def test_unstable_warmup_fails_without_searching() -> None:
    async def run(rate, seconds, phase, mode):  # noqa: ANN001,ANN202
        return _measurement(rate, seconds, stable=False)

    with pytest.raises(RuntimeError, match="warmup"):
        await find_maximum_stable_rate(
            run,
            mode="fyralis_ceiling",
            config=LoadSearchConfig(initial_rate=10),
            lab_calibration=_lab_calibration(),
            include_soak=False,
        )


def test_correctness_failure_is_never_stable() -> None:
    measurement = _measurement(10, 120, stable=True)
    broken = replace(measurement, cross_tenant_leaks=1)
    assert certification_stable(broken) is False


async def test_search_rejects_an_underpowered_provider_lab() -> None:
    calls = 0

    async def run(rate, seconds, phase, mode):  # noqa: ANN001,ANN202
        nonlocal calls
        calls += 1
        return _measurement(rate, seconds, stable=True)

    with pytest.raises(RuntimeError, match="calibration failed"):
        await find_maximum_stable_rate(
            run,
            mode="provider_safe",
            config=LoadSearchConfig(initial_rate=10),
            lab_calibration=_lab_calibration(capacity_ratio=1),
            include_soak=False,
        )
    assert calls == 0
