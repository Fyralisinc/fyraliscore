from __future__ import annotations

from collections.abc import Mapping

import pytest

from services.ingest.connector_platform.routing_config import (
    RoutingConfigurationController,
)
from services.ingest.connector_runtime.policy import AtomicRoutingPolicy, ExecutionMode
from services.ingest.connector_runtime.rollout import (
    FleetRoutingController,
    RolloutMetrics,
    RolloutRevision,
    RolloutStage,
    RolloutThresholds,
    assess_rollout,
)


def test_rollout_thresholds_gate_promotion_and_trigger_rollback() -> None:
    thresholds = RolloutThresholds(
        minimum_executions=100,
        maximum_error_rate=0.01,
        maximum_parity_mismatch_rate=0,
        maximum_p95_regression_ratio=1.1,
        maximum_lifecycle_failures=0,
        maximum_dlq_rate_delta=0,
    )
    healthy = assess_rollout(
        RolloutMetrics(
            executions=100,
            connector_p95_ms=10,
            legacy_p95_ms=10,
        ),
        thresholds,
    )
    unhealthy = assess_rollout(
        RolloutMetrics(
            executions=100,
            failures=2,
            connector_p95_ms=12,
            legacy_p95_ms=10,
        ),
        thresholds,
    )

    assert healthy.ready_to_promote
    assert not healthy.rollback_required
    assert unhealthy.rollback_required
    assert unhealthy.reasons == ("error_rate", "p95_regression")


class _Repository:
    def __init__(self, active: RolloutRevision) -> None:
        self.active = active
        self.audit_events: list[tuple[int, str]] = []
        self.rollback_metrics: Mapping[str, object] | None = None

    async def load_active(self):
        return self.active

    async def audit(
        self, revision, *, action, actor, reason, metrics=None
    ) -> None:
        self.audit_events.append((revision, action))

    async def rollback_to_legacy(
        self, failed_revision, *, actor, reason, metrics
    ):
        self.rollback_metrics = metrics
        self.active = RolloutRevision(
            revision=failed_revision + 1,
            policy={"global": "legacy"},
            stage=RolloutStage.FULL,
            thresholds=RolloutThresholds(minimum_executions=0),
        )
        return self.active


@pytest.mark.asyncio
async def test_fleet_controller_propagates_revision_then_rolls_back_atomically() -> None:
    repository = _Repository(
        RolloutRevision(
            revision=2,
            policy={"global": "connector"},
            stage=RolloutStage.CANARY,
            tenant_cohort=("tenant-a",),
            thresholds=RolloutThresholds(
                minimum_executions=1,
                maximum_error_rate=0,
            ),
        )
    )
    configuration = RoutingConfigurationController(AtomicRoutingPolicy())

    async def metrics(_revision):
        return RolloutMetrics(executions=1, failures=1)

    controller = FleetRoutingController(
        repository,
        configuration,
        actor="worker-1",
        metric_reader=metrics,
    )
    assessment = await controller.evaluate_once()

    assert assessment is not None and assessment.rollback_required
    assert repository.audit_events == [(2, "propagated")]
    assert repository.rollback_metrics is not None
    assert configuration.snapshot().revision == 3
    assert configuration.snapshot().global_mode is ExecutionMode.LEGACY
