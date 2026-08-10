from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

from services.ingest.connector_runtime.lifecycle import (
    DesiredInstallationState,
    InstallationLifecycle,
    InstallationLifecycleController,
    InstallationPhase,
    LifecycleEvidence,
)


def _lifecycle() -> InstallationLifecycle:
    return InstallationLifecycle(
        installation_id=uuid4(),
        tenant_id=uuid4(),
        connector_id="fyralis/slack",
        desired=DesiredInstallationState.READY,
        observed=InstallationPhase.DRAFT,
        generation=1,
        observed_generation=0,
    )


def test_lifecycle_reaches_ready_one_idempotent_step_at_a_time() -> None:
    controller = InstallationLifecycleController()
    current = _lifecycle()
    evidence = LifecycleEvidence(
        authorized=True,
        configuration_valid=True,
        initialized=True,
    )
    now = datetime.now(timezone.utc)

    for expected in (
        InstallationPhase.AUTHORIZING,
        InstallationPhase.VALIDATING,
        InstallationPhase.INITIALIZING,
        InstallationPhase.READY,
    ):
        current = controller.reconcile(current, evidence, now=now)
        assert current.observed is expected

    assert current.execution_available


def test_pause_maintenance_failure_and_removal_are_observed() -> None:
    controller = InstallationLifecycleController()
    current = _lifecycle()
    now = datetime.now(timezone.utc)

    paused = controller.reconcile(
        replace(current, desired=DesiredInstallationState.PAUSED),
        LifecycleEvidence(),
        now=now,
    )
    assert paused.observed is InstallationPhase.PAUSED

    maintenance = controller.reconcile(
        replace(current, desired=DesiredInstallationState.MAINTENANCE),
        LifecycleEvidence(),
        now=now,
    )
    assert maintenance.observed is InstallationPhase.MAINTENANCE

    failed = controller.reconcile(
        current,
        LifecycleEvidence(failure_reason="credential rejected"),
        now=now,
    )
    assert failed.observed is InstallationPhase.FAILED

    removing = replace(current, desired=DesiredInstallationState.REMOVED)
    removing = controller.reconcile(removing, LifecycleEvidence(), now=now)
    assert removing.observed is InstallationPhase.UNINSTALLING
    removed = controller.reconcile(
        removing,
        LifecycleEvidence(removal_complete=True),
        now=now,
    )
    assert removed.observed is InstallationPhase.REMOVED
