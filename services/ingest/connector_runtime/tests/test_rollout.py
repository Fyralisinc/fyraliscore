from collections.abc import Mapping
from uuid import uuid4

import pytest

from services.ingest.connector_platform.routing_config import (
    RoutingConfigurationController,
)
from services.ingest.connector_runtime.policy import AtomicRoutingPolicy
from services.ingest.connector_runtime.rollout import (
    FleetRoutingController,
    RolloutMetrics,
    RolloutRevision,
    RolloutStage,
    RolloutThresholds,
    assess_rollout,
)


def test_rollout_thresholds_use_connector_only_evidence() -> None:
    thresholds = RolloutThresholds(
        minimum_executions=100,
        maximum_error_rate=0.01,
        maximum_p95_ms=100,
        maximum_lifecycle_failures=0,
        maximum_dlq_rate=0,
    )
    assert assess_rollout(
        RolloutMetrics(executions=100, connector_p95_ms=10), thresholds
    ).ready_to_promote
    assessment = assess_rollout(
        RolloutMetrics(executions=100, failures=2, connector_p95_ms=120),
        thresholds,
    )
    assert assessment.rollback_required
    assert assessment.reasons == ("error_rate", "connector_p95")


class _Repository:
    def __init__(self, active: RolloutRevision) -> None:
        self.active = active
        self.audit_events: list[tuple[int, str]] = []
        self.rollback_metrics: Mapping[str, object] | None = None

    async def load_active(self):
        return self.active

    async def audit(self, revision, *, action, actor, reason, metrics=None):
        self.audit_events.append((revision, action))

    async def rollback_to_previous(self, failed_revision, *, actor, reason, metrics):
        self.rollback_metrics = metrics
        self.active = RolloutRevision(
            revision=failed_revision + 1,
            policy={"artifact_revision": failed_revision - 1},
            stage=RolloutStage.FULL,
            thresholds=RolloutThresholds(minimum_executions=0),
        )
        return self.active


@pytest.mark.asyncio
async def test_controller_rolls_back_to_previous_artifact_revision() -> None:
    repository = _Repository(
        RolloutRevision(
            revision=2,
            policy={"artifact_revision": 2},
            stage=RolloutStage.CANARY,
            tenant_cohort=(str(uuid4()),),
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
    assert controller.active_revision == 3
    assert configuration.snapshot().revision == 3


def test_every_stage_materializes_contract_only_policy() -> None:
    for revision in (
        RolloutRevision(2, {}, RolloutStage.CANARY, (str(uuid4()),)),
        RolloutRevision(3, {}, RolloutStage.COHORT, (str(uuid4()), str(uuid4()))),
        RolloutRevision(4, {}, RolloutStage.FULL),
    ):
        assert dict(revision.effective_policy()) == {
            "revision": revision.revision,
            "global": "connector",
        }
