"""Capacity calibration for Provider Lab workload drivers.

Certification load must not accidentally benchmark the simulator.  A source
driver supplies the exact provider-client operation it plans to exercise; this
module runs that operation without an offered-rate cap and proves that the lab
can serve at least twice the intended Fyralis rate while keeping p99 below ten
percent of the configured client timeout.
"""
from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass

from lib.shared.env import is_prod


ProbeCall = Callable[[], Awaitable[bool | None]]


@dataclass(frozen=True, slots=True)
class LabCalibrationConfig:
    target_fyralis_rps: float
    client_timeout_seconds: float
    probe_seconds: float = 30.0
    concurrency: int = 32
    minimum_samples: int = 100
    minimum_capacity_ratio: float = 2.0
    maximum_p99_timeout_ratio: float = 0.1

    def __post_init__(self) -> None:
        for name in (
            "target_fyralis_rps",
            "client_timeout_seconds",
            "probe_seconds",
            "minimum_capacity_ratio",
            "maximum_p99_timeout_ratio",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0
            ):
                raise ValueError(f"{name} must be a finite positive number")
        for name in ("concurrency", "minimum_samples"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.maximum_p99_timeout_ratio > 1:
            raise ValueError("maximum_p99_timeout_ratio cannot exceed 1")


@dataclass(frozen=True, slots=True)
class LabCalibration:
    target_fyralis_rps: float
    elapsed_seconds: float
    attempted_requests: int
    successful_requests: int
    failed_requests: int
    achieved_requests_per_second: float
    capacity_ratio: float
    p99_latency_ms: float
    p99_timeout_ratio: float
    minimum_samples: int
    minimum_capacity_ratio: float
    maximum_p99_timeout_ratio: float

    @property
    def passed(self) -> bool:
        return (
            self.attempted_requests >= self.minimum_samples
            and self.failed_requests == 0
            and self.capacity_ratio >= self.minimum_capacity_ratio
            and self.p99_timeout_ratio <= self.maximum_p99_timeout_ratio
        )

    def certification_metrics(self) -> dict[str, float]:
        """Return the metric names consumed by the release evaluator."""

        return {
            "lab_capacity_ratio": self.capacity_ratio,
            "lab_p99_timeout_ratio": self.p99_timeout_ratio,
            "lab_requests_per_second": self.achieved_requests_per_second,
            "lab_p99_latency_ms": self.p99_latency_ms,
            "lab_failed_requests": float(self.failed_requests),
            "lab_sample_requests": float(self.attempted_requests),
        }


def assess_lab_calibration(
    *,
    config: LabCalibrationConfig,
    elapsed_seconds: float,
    latencies_seconds: Sequence[float],
    failed_requests: int,
) -> LabCalibration:
    """Build a deterministic calibration result from measured samples."""

    if (
        not math.isfinite(elapsed_seconds)
        or elapsed_seconds <= 0
    ):
        raise ValueError("elapsed_seconds must be finite and positive")
    if failed_requests < 0:
        raise ValueError("failed_requests cannot be negative")
    normalized_latencies = sorted(float(value) for value in latencies_seconds)
    if any(
        not math.isfinite(value) or value < 0
        for value in normalized_latencies
    ):
        raise ValueError("latencies_seconds must be finite and non-negative")
    attempted = len(normalized_latencies) + failed_requests
    successful = len(normalized_latencies)
    achieved = successful / elapsed_seconds
    p99_seconds = (
        normalized_latencies[
            min(
                len(normalized_latencies) - 1,
                max(0, math.ceil(len(normalized_latencies) * 0.99) - 1),
            )
        ]
        if normalized_latencies
        else math.inf
    )
    return LabCalibration(
        target_fyralis_rps=float(config.target_fyralis_rps),
        elapsed_seconds=elapsed_seconds,
        attempted_requests=attempted,
        successful_requests=successful,
        failed_requests=failed_requests,
        achieved_requests_per_second=achieved,
        capacity_ratio=achieved / config.target_fyralis_rps,
        p99_latency_ms=p99_seconds * 1000,
        p99_timeout_ratio=p99_seconds / config.client_timeout_seconds,
        minimum_samples=config.minimum_samples,
        minimum_capacity_ratio=config.minimum_capacity_ratio,
        maximum_p99_timeout_ratio=config.maximum_p99_timeout_ratio,
    )


async def calibrate_provider_lab(
    call: ProbeCall,
    *,
    config: LabCalibrationConfig,
) -> LabCalibration:
    """Drive one exact client operation at the lab's unconstrained ceiling."""

    if is_prod():
        raise RuntimeError(
            "Provider Lab calibration is test-only and cannot run in production",
        )

    deadline = time.monotonic() + config.probe_seconds
    latencies: list[float] = []
    failures = 0
    attempts_started = 0

    async def worker() -> None:
        nonlocal attempts_started, failures
        # ``probe_seconds`` is the minimum observation window, while
        # ``minimum_samples`` is an independent evidence requirement.  A busy
        # CI host can exhaust a short diagnostic window before scheduling
        # enough calls to cover the declared operation mix.  Continue only
        # until both requirements have been met; the request timeout still
        # bounds every individual call.
        while (
            time.monotonic() < deadline
            or attempts_started < config.minimum_samples
        ):
            attempts_started += 1
            started = time.perf_counter()
            try:
                accepted = await asyncio.wait_for(
                    call(),
                    timeout=config.client_timeout_seconds,
                )
            except Exception:  # noqa: BLE001
                failures += 1
                continue
            if accepted is False:
                failures += 1
                continue
            latencies.append(time.perf_counter() - started)

    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(config.concurrency)))
    elapsed = time.perf_counter() - started
    return assess_lab_calibration(
        config=config,
        elapsed_seconds=elapsed,
        latencies_seconds=latencies,
        failed_requests=failures,
    )


def require_lab_calibration(calibration: LabCalibration) -> None:
    """Fail the benchmark before Fyralis load when the lab is too slow."""

    if calibration.passed:
        return
    failures: list[str] = []
    if calibration.attempted_requests < calibration.minimum_samples:
        failures.append(
            f"samples {calibration.attempted_requests} < "
            f"{calibration.minimum_samples}"
        )
    if calibration.failed_requests:
        failures.append(f"failed requests {calibration.failed_requests}")
    if calibration.capacity_ratio < calibration.minimum_capacity_ratio:
        failures.append(
            f"capacity ratio {calibration.capacity_ratio:.3f} < "
            f"{calibration.minimum_capacity_ratio:.3f}"
        )
    if calibration.p99_timeout_ratio > calibration.maximum_p99_timeout_ratio:
        failures.append(
            f"p99/timeout ratio {calibration.p99_timeout_ratio:.3f} > "
            f"{calibration.maximum_p99_timeout_ratio:.3f}"
        )
    raise RuntimeError("Provider Lab calibration failed: " + "; ".join(failures))


__all__ = [
    "LabCalibration",
    "LabCalibrationConfig",
    "ProbeCall",
    "assess_lab_calibration",
    "calibrate_provider_lab",
    "require_lab_calibration",
]
