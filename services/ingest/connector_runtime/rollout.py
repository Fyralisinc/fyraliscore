"""Connector-artifact rollout evaluation and revision propagation.

Every policy executes through the Source Connector contract. Rollouts select an
artifact revision and evidence cohort; they cannot route back to source-local
implementations because that execution surface no longer exists.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from services.ingest.connector_runtime.policy import RoutingPolicy


class RolloutStage(StrEnum):
    CANARY = "canary"
    COHORT = "cohort"
    FULL = "full"


@dataclass(frozen=True)
class RolloutThresholds:
    minimum_executions: int = 100
    maximum_error_rate: float = 0.02
    maximum_p95_ms: float = 30_000
    maximum_lifecycle_failures: int = 0
    maximum_dlq_rate: float = 0.001

    def __post_init__(self) -> None:
        if self.minimum_executions < 0:
            raise ValueError("minimum executions cannot be negative")
        if not 0 <= self.maximum_error_rate <= 1:
            raise ValueError("maximum_error_rate must be between zero and one")
        if self.maximum_p95_ms <= 0:
            raise ValueError("maximum_p95_ms must be positive")
        if not 0 <= self.maximum_dlq_rate <= 1:
            raise ValueError("maximum_dlq_rate must be between zero and one")


@dataclass(frozen=True)
class RolloutMetrics:
    executions: int
    failures: int = 0
    connector_p95_ms: float = 0
    lifecycle_failures: int = 0
    connector_dlq_rate: float = 0

    @property
    def error_rate(self) -> float:
        return self.failures / self.executions if self.executions else 0

    def snapshot(self) -> dict[str, float | int]:
        return {
            "executions": self.executions,
            "failures": self.failures,
            "error_rate": self.error_rate,
            "connector_p95_ms": self.connector_p95_ms,
            "lifecycle_failures": self.lifecycle_failures,
            "connector_dlq_rate": self.connector_dlq_rate,
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
    if metrics.connector_p95_ms > thresholds.maximum_p95_ms:
        breaches.append("connector_p95")
    if metrics.lifecycle_failures > thresholds.maximum_lifecycle_failures:
        breaches.append("lifecycle_failures")
    if metrics.connector_dlq_rate > thresholds.maximum_dlq_rate:
        breaches.append("connector_dlq_rate")
    enough_evidence = metrics.executions >= thresholds.minimum_executions
    return RolloutAssessment(
        ready_to_promote=enough_evidence and not breaches,
        rollback_required=bool(breaches),
        reasons=tuple(
            breaches or (() if enough_evidence else ("insufficient_evidence",))
        ),
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
        if self.stage is RolloutStage.CANARY and len(self.tenant_cohort) != 1:
            raise ValueError("canary rollout requires exactly one tenant")
        if self.stage is RolloutStage.COHORT and not self.tenant_cohort:
            raise ValueError("cohort rollout requires at least one tenant")
        if self.stage is RolloutStage.FULL and self.tenant_cohort:
            raise ValueError("full rollout cannot carry a tenant cohort")
        object.__setattr__(self, "policy", MappingProxyType(dict(self.policy)))

    def effective_policy(self) -> Mapping[str, object]:
        return MappingProxyType(
            {"revision": self.revision, "global": "connector"}
        )


class RolloutRepository(Protocol):
    async def load_active(self) -> RolloutRevision | None: ...

    async def rollback_to_previous(
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
    """Propagate the active contract revision and roll back artifacts only."""

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
        self._active_revision: int | None = None

    @property
    def active_revision(self) -> int | None:
        return self._active_revision

    async def refresh_once(self) -> RolloutRevision | None:
        revision = await self._repository.load_active()
        if revision is None:
            self._active_revision = None
            return None
        self._active_revision = revision.revision
        current = self._configuration.snapshot()
        if revision.revision > current.revision:
            self._configuration.apply(revision.effective_policy())
            await self._repository.audit(
                revision.revision,
                action="propagated",
                actor=self._actor,
                reason="process applied connector artifact revision",
            )
        return revision

    async def evaluate_once(self) -> RolloutAssessment | None:
        revision = await self.refresh_once()
        if revision is None or self._metric_reader is None:
            return None
        metrics = await self._metric_reader(revision)
        assessment = assess_rollout(metrics, revision.thresholds)
        if assessment.rollback_required:
            rollback = await self._repository.rollback_to_previous(
                revision.revision,
                actor=self._actor,
                reason=",".join(assessment.reasons),
                metrics=metrics.snapshot(),
            )
            self._configuration.apply(rollback.effective_policy())
            self._active_revision = rollback.revision
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
            except TimeoutError:
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
