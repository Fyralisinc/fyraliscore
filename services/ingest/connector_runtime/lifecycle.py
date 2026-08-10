"""Desired/observed installation lifecycle independent of persistence."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Protocol
from uuid import UUID


class DesiredInstallationState(StrEnum):
    READY = "Ready"
    PAUSED = "Paused"
    MAINTENANCE = "Maintenance"
    REMOVED = "Removed"


class InstallationPhase(StrEnum):
    DRAFT = "Draft"
    AUTHORIZING = "Authorizing"
    VALIDATING = "Validating"
    INITIALIZING = "Initializing"
    READY = "Ready"
    DEGRADED = "Degraded"
    PAUSED = "Paused"
    MAINTENANCE = "Maintenance"
    FAILED = "Failed"
    UNINSTALLING = "Uninstalling"
    REMOVED = "Removed"


@dataclass(frozen=True)
class InstallationCondition:
    type: str
    status: bool | None
    reason: str
    message: str
    observed_at: datetime


@dataclass(frozen=True)
class InstallationLifecycle:
    installation_id: UUID
    tenant_id: UUID
    connector_id: str
    desired: DesiredInstallationState
    observed: InstallationPhase
    generation: int
    observed_generation: int
    conditions: tuple[InstallationCondition, ...] = ()

    @property
    def execution_available(self) -> bool:
        return self.observed in {
            InstallationPhase.READY,
            InstallationPhase.DEGRADED,
        }


@dataclass(frozen=True)
class LifecycleEvidence:
    authorized: bool = False
    configuration_valid: bool = False
    initialized: bool = False
    healthy: bool = True
    removal_complete: bool = False
    failure_reason: str | None = None


class LifecycleRepository(Protocol):
    async def load(self, installation_id: UUID) -> InstallationLifecycle | None: ...

    async def save(self, lifecycle: InstallationLifecycle) -> None: ...


def _condition(
    lifecycle: InstallationLifecycle,
    *,
    condition_type: str,
    status: bool | None,
    reason: str,
    message: str,
    now: datetime,
) -> tuple[InstallationCondition, ...]:
    retained = tuple(
        item for item in lifecycle.conditions if item.type != condition_type
    )
    return retained + (
        InstallationCondition(
            type=condition_type,
            status=status,
            reason=reason,
            message=message,
            observed_at=now,
        ),
    )


class InstallationLifecycleController:
    """Advance one idempotent lifecycle step from current evidence."""

    def reconcile(
        self,
        lifecycle: InstallationLifecycle,
        evidence: LifecycleEvidence,
        *,
        now: datetime,
    ) -> InstallationLifecycle:
        phase = lifecycle.observed

        if phase is InstallationPhase.REMOVED:
            return lifecycle
        if lifecycle.desired is DesiredInstallationState.REMOVED:
            target = (
                InstallationPhase.REMOVED
                if evidence.removal_complete
                else InstallationPhase.UNINSTALLING
            )
            conditions = _condition(
                lifecycle,
                condition_type="Removed",
                status=evidence.removal_complete,
                reason="CleanupComplete" if evidence.removal_complete else "CleanupPending",
                message=(
                    "installation cleanup completed"
                    if evidence.removal_complete
                    else "installation cleanup is in progress"
                ),
                now=now,
            )
            return replace(
                lifecycle,
                observed=target,
                observed_generation=lifecycle.generation,
                conditions=conditions,
            )
        if lifecycle.desired is DesiredInstallationState.PAUSED:
            return replace(
                lifecycle,
                observed=InstallationPhase.PAUSED,
                observed_generation=lifecycle.generation,
            )
        if lifecycle.desired is DesiredInstallationState.MAINTENANCE:
            return replace(
                lifecycle,
                observed=InstallationPhase.MAINTENANCE,
                observed_generation=lifecycle.generation,
            )
        if evidence.failure_reason is not None:
            conditions = _condition(
                lifecycle,
                condition_type="Ready",
                status=False,
                reason="ReconcileFailed",
                message=evidence.failure_reason,
                now=now,
            )
            return replace(
                lifecycle,
                observed=InstallationPhase.FAILED,
                observed_generation=lifecycle.generation,
                conditions=conditions,
            )

        if phase in {
            InstallationPhase.PAUSED,
            InstallationPhase.MAINTENANCE,
            InstallationPhase.FAILED,
        }:
            phase = InstallationPhase.DRAFT
        if phase is InstallationPhase.DRAFT:
            phase = InstallationPhase.AUTHORIZING
        elif phase is InstallationPhase.AUTHORIZING and evidence.authorized:
            phase = InstallationPhase.VALIDATING
        elif phase is InstallationPhase.VALIDATING and evidence.configuration_valid:
            phase = InstallationPhase.INITIALIZING
        elif phase is InstallationPhase.INITIALIZING and evidence.initialized:
            phase = (
                InstallationPhase.READY
                if evidence.healthy
                else InstallationPhase.DEGRADED
            )
        elif phase in {InstallationPhase.READY, InstallationPhase.DEGRADED}:
            phase = (
                InstallationPhase.READY
                if evidence.healthy
                else InstallationPhase.DEGRADED
            )

        conditions = _condition(
            lifecycle,
            condition_type="Ready",
            status=True if phase is InstallationPhase.READY else False,
            reason=phase.value,
            message=f"installation observed in {phase.value}",
            now=now,
        )
        return replace(
            lifecycle,
            observed=phase,
            observed_generation=lifecycle.generation,
            conditions=conditions,
        )


__all__ = [
    "DesiredInstallationState",
    "InstallationCondition",
    "InstallationLifecycle",
    "InstallationLifecycleController",
    "InstallationPhase",
    "LifecycleEvidence",
    "LifecycleRepository",
]
