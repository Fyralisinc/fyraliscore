"""Deterministic maximum-stable-throughput search.

The controller owns the prescribed workload shape; a source-specific driver
owns traffic generation and measurement.  This separation lets CI use a
virtual clock while weekly jobs run the exact two-minute warmup, fifteen-minute
validation, and sixty-minute soak durations.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from services.ingest.synthetic.provider_lab.calibration import (
    LabCalibration,
    require_lab_calibration,
)


LoadMode = Literal["provider_safe", "fyralis_ceiling"]
Phase = Literal["warmup", "step", "binary_search", "validation", "soak"]


@dataclass(frozen=True, slots=True)
class LoadSearchConfig:
    initial_rate: float
    step_fraction: float = 0.25
    tolerance_fraction: float = 0.05
    warmup_seconds: int = 120
    step_seconds: int = 120
    validation_seconds: int = 900
    soak_seconds: int = 3600
    maximum_steps: int = 40

    def __post_init__(self) -> None:
        if self.initial_rate <= 0:
            raise ValueError("initial_rate must be > 0")
        if not 0 < self.step_fraction < 1:
            raise ValueError("step_fraction must be in (0, 1)")
        if not 0 < self.tolerance_fraction < 1:
            raise ValueError("tolerance_fraction must be in (0, 1)")
        for name in (
            "warmup_seconds",
            "step_seconds",
            "validation_seconds",
            "soak_seconds",
            "maximum_steps",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0")


@dataclass(frozen=True, slots=True)
class LoadMeasurement:
    offered_rate: float
    duration_seconds: int
    stable: bool
    requests_per_second: float
    quota_units_per_second: float
    records_per_second: float
    bytes_per_second: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    kafka_lag: float
    observation_p99_latency_ms: float
    backlog_growth_per_second: float
    missing_records: int
    unexpected_duplicates: int
    cross_tenant_leaks: int
    cooldown_violations: int
    cursor_consistency_errors: int
    dlq_entries: int
    cpu_percent: float
    memory_bytes: int
    limiting_component: str


@dataclass(frozen=True, slots=True)
class LoadTrial:
    phase: Phase
    measurement: LoadMeasurement


@dataclass(frozen=True, slots=True)
class LoadEnvelope:
    mode: LoadMode
    maximum_stable_rate: float
    tolerance_fraction: float
    validation: LoadMeasurement
    soak: LoadMeasurement | None
    trials: tuple[LoadTrial, ...]


LoadRunner = Callable[
    [float, int, Phase, LoadMode],
    Awaitable[LoadMeasurement],
]


def certification_stable(measurement: LoadMeasurement) -> bool:
    """Apply non-negotiable correctness/stability conditions."""

    return (
        measurement.stable
        and measurement.backlog_growth_per_second <= 0
        and measurement.missing_records == 0
        and measurement.unexpected_duplicates == 0
        and measurement.cross_tenant_leaks == 0
        and measurement.cooldown_violations == 0
        and measurement.cursor_consistency_errors == 0
        and measurement.dlq_entries == 0
    )


async def find_maximum_stable_rate(
    run: LoadRunner,
    *,
    mode: LoadMode,
    config: LoadSearchConfig,
    lab_calibration: LabCalibration,
    include_soak: bool,
) -> LoadEnvelope:
    """Step to instability, binary search within 5%, then validate."""

    # Certification is invalid when the fake provider is the limiting
    # component. Keep this as a mandatory input rather than a caller
    # convention that a new source pack can accidentally omit.
    require_lab_calibration(lab_calibration)
    trials: list[LoadTrial] = []

    async def measure(rate: float, seconds: int, phase: Phase) -> LoadMeasurement:
        result = await run(rate, seconds, phase, mode)
        if result.offered_rate != rate or result.duration_seconds != seconds:
            raise ValueError(
                "load runner returned measurement for a different rate/duration"
            )
        trials.append(LoadTrial(phase=phase, measurement=result))
        return result

    warmup = await measure(
        config.initial_rate,
        config.warmup_seconds,
        "warmup",
    )
    if not certification_stable(warmup):
        raise RuntimeError("initial warmup rate is not stable")

    low = config.initial_rate
    high: float | None = None
    for _ in range(config.maximum_steps):
        candidate = low * (1 + config.step_fraction)
        stepped = await measure(candidate, config.step_seconds, "step")
        if certification_stable(stepped):
            low = candidate
            continue
        high = candidate
        break
    if high is None:
        raise RuntimeError(
            "load search never found an unstable upper bound; increase lab offer limit"
        )

    for _ in range(config.maximum_steps):
        if (high - low) / low <= config.tolerance_fraction:
            break
        candidate = (low + high) / 2
        searched = await measure(candidate, config.step_seconds, "binary_search")
        if certification_stable(searched):
            low = candidate
        else:
            high = candidate
    else:
        raise RuntimeError("binary search exceeded maximum_steps")

    validation = await measure(low, config.validation_seconds, "validation")
    if not certification_stable(validation):
        raise RuntimeError("maximum candidate failed fifteen-minute validation")

    soak: LoadMeasurement | None = None
    if include_soak:
        soak = await measure(low, config.soak_seconds, "soak")
        if not certification_stable(soak):
            raise RuntimeError("maximum candidate failed weekly soak")

    return LoadEnvelope(
        mode=mode,
        maximum_stable_rate=low,
        tolerance_fraction=(high - low) / low,
        validation=validation,
        soak=soak,
        trials=tuple(trials),
    )


__all__ = [
    "LoadEnvelope",
    "LoadMeasurement",
    "LoadSearchConfig",
    "LoadTrial",
    "certification_stable",
    "find_maximum_stable_rate",
]
