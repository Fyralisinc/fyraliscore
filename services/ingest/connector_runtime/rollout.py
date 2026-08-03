"""Fleet rollout models, threshold evaluation, and revision propagation."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from services.ingest.connector_runtime.policy import RoutingPolicy


class RolloutStage(StrEnum):
    SHADOW = "shadow"
    CANARY = "canary"
    COHORT = "cohort"
    FULL = "full"


@dataclass(frozen=True)
class RolloutThresholds:
    minimum_executions: int = 100
    maximum_error_rate: float = 0.02
    maximum_parity_mismatch_rate: float = 0.001
    maximum_p95_regression_ratio: float = 1.25
    maximum_lifecycle_failures: int = 0
    maximum_dlq_rate_delta: float = 0.001

    def __post_init__(self) -> None:
        if self.minimum_executions < 0:
            raise ValueError("minimum executions cannot be negative")
        for name in (
            "maximum_error_rate",
            "maximum_parity_mismatch_rate",
            "maximum_dlq_rate_delta",
        ):
            if not 0 <= getattr(self, name) <= 1:
                raise ValueError(f"{name} must be between zero and one")
        if self.maximum_p95_regression_ratio < 1:
            raise ValueError("p95 regression threshold cannot be below one")


@dataclass(frozen=True)
class RolloutMetrics:
    executions: int
    failures: int = 0
    parity_samples: int = 0
    parity_mismatches: int = 0
    connector_p95_ms: float = 0
    legacy_p95_ms: float = 0
    lifecycle_failures: int = 0
    connector_dlq_rate: float = 0
    baseline_dlq_rate: float = 0

    @property
    def error_rate(self) -> float:
        return self.failures / self.executions if self.executions else 0

    @property
    def parity_mismatch_rate(self) -> float:
        return (
            self.parity_mismatches / self.parity_samples
            if self.parity_samples
            else 0
        )

    @property
    def p95_regression_ratio(self) -> float:
        if self.legacy_p95_ms <= 0:
            return 1
        return self.connector_p95_ms / self.legacy_p95_ms

    @property
    def dlq_rate_delta(self) -> float:
        return max(0, self.connector_dlq_rate - self.baseline_dlq_rate)

    def snapshot(self) -> dict[str, float | int]:
        return {
            "executions": self.executions,
            "failures": self.failures,
            "error_rate": self.error_rate,
            "parity_samples": self.parity_samples,
            "parity_mismatches": self.parity_mismatches,
            "parity_mismatch_rate": self.parity_mismatch_rate,
            "connector_p95_ms": self.connector_p95_ms,
            "legacy_p95_ms": self.legacy_p95_ms,
            "p95_regression_ratio": self.p95_regression_ratio,
            "lifecycle_failures": self.lifecycle_failures,
            "connector_dlq_rate": self.connector_dlq_rate,
            "baseline_dlq_rate": self.baseline_dlq_rate,
            "dlq_rate_delta": self.dlq_rate_delta,
        }


@dataclass(frozen=True)
class RolloutAssessment:
    ready_to_promote: bool
    rollback_required: bool
    reasons: tuple[str, ...]


def assess_rollout(
    metrics: RolloutMetrics,
    thresholds: RolloutThresholds,
) -> RolloutAssessment:
    breaches: list[str] = []
    if metrics.error_rate > thresholds.maximum_error_rate:
        breaches.append("error_rate")
    if metrics.parity_mismatch_rate > thresholds.maximum_parity_mismatch_rate:
        breaches.append("parity_mismatch_rate")
    if metrics.p95_regression_ratio > thresholds.maximum_p95_regression_ratio:
        breaches.append("p95_regression")
    if metrics.lifecycle_failures > thresholds.maximum_lifecycle_failures:
        breaches.append("lifecycle_failures")
    if metrics.dlq_rate_delta > thresholds.maximum_dlq_rate_delta:
        breaches.append("dlq_rate_delta")
    enough_evidence = metrics.executions >= thresholds.minimum_executions
    return RolloutAssessment(
        ready_to_promote=enough_evidence and not breaches,
        rollback_required=bool(breaches),
        reasons=tuple(breaches or (() if enough_evidence else ("insufficient_evidence",))),
    )


@dataclass(frozen=True)
class RolloutRevision:
    revision: int
    policy: Mapping[str, object]
    stage: RolloutStage
    tenant_cohort: tuple[str, ...] = ()
    thresholds: RolloutThresholds = field(default_factory=RolloutThresholds)

    def __post_init__(self) -> None:
        if self.revision < 1:
            raise ValueError("rollout revision must be positive")
        object.__setattr__(self, "policy", MappingProxyType(dict(self.policy)))


class RolloutRepository:
    async def load_active(self) -> RolloutRevision | None: ...

    async def rollback_to_legacy(
        self,
        failed_revision: int,
        *,
        actor: str,
        reason: str,
        metrics: Mapping[str, object],
    ) -> RolloutRevision: ...

    async def audit(
        self,
        revision: int,
        *,
        action: str,
        actor: str,
        reason: str,
        metrics: Mapping[str, object] | None = None,
    ) -> None: ...


class RoutingConfiguration(Protocol):
    def snapshot(self) -> RoutingPolicy: ...

    def apply(self, value: Mapping[str, object]) -> RoutingPolicy: ...


MetricReader = Callable[[RolloutRevision], Awaitable[RolloutMetrics]]


class FleetRoutingController:
    """Watch the durable active revision and keep one process in sync."""

    def __init__(
        self,
        repository: RolloutRepository,
        configuration: RoutingConfiguration,
        *,
        actor: str,
        metric_reader: MetricReader | None = None,
    ) -> None:
        self._repository = repository
        self._configuration = configuration
        self._actor = actor
        self._metric_reader = metric_reader

    async def refresh_once(self) -> RolloutRevision | None:
        revision = await self._repository.load_active()
        if revision is None:
            return None
        current = self._configuration.snapshot()
        if revision.revision > current.revision:
            self._configuration.apply(
                {**revision.policy, "revision": revision.revision}
            )
            await self._repository.audit(
                revision.revision,
                action="propagated",
                actor=self._actor,
                reason="fleet process applied active revision",
            )
        return revision

    async def evaluate_once(self) -> RolloutAssessment | None:
        revision = await self.refresh_once()
        if revision is None or self._metric_reader is None:
            return None
        metrics = await self._metric_reader(revision)
        assessment = assess_rollout(metrics, revision.thresholds)
        if assessment.rollback_required:
            rollback = await self._repository.rollback_to_legacy(
                revision.revision,
                actor=self._actor,
                reason=",".join(assessment.reasons),
                metrics=metrics.snapshot(),
            )
            self._configuration.apply(
                {**rollback.policy, "revision": rollback.revision}
            )
        return assessment

    async def run(
        self,
        stop_event: asyncio.Event,
        *,
        interval_seconds: float = 5.0,
    ) -> None:
        while not stop_event.is_set():
            await self.evaluate_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue


__all__ = [
    "FleetRoutingController",
    "MetricReader",
    "RolloutAssessment",
    "RolloutMetrics",
    "RolloutRepository",
    "RolloutRevision",
    "RolloutStage",
    "RolloutThresholds",
    "assess_rollout",
]
