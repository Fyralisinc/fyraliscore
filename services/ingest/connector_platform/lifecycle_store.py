"""Persist connector lifecycle in the existing workflow-state substrate."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from services.ingest.connector_runtime.lifecycle import (
    DesiredInstallationState,
    InstallationCondition,
    InstallationLifecycle,
    InstallationPhase,
)
from services.ingest.ingestion.workflows.state import (
    WorkflowState,
    load_state,
    persist_state,
)


LIFECYCLE_WORKFLOW_KIND = "source_connector_installation"


def _condition_to_json(condition: InstallationCondition) -> dict[str, Any]:
    return {
        "type": condition.type,
        "status": condition.status,
        "reason": condition.reason,
        "message": condition.message,
        "observed_at": condition.observed_at.isoformat(),
    }


def _condition_from_json(value: dict[str, Any]) -> InstallationCondition:
    return InstallationCondition(
        type=str(value["type"]),
        status=value.get("status"),
        reason=str(value["reason"]),
        message=str(value.get("message", "")),
        observed_at=datetime.fromisoformat(str(value["observed_at"])),
    )


class WorkflowStateLifecycleRepository:
    """No-schema-change lifecycle adapter over ``workflow_states`` JSONB."""

    def __init__(self, executor: Any) -> None:
        self._executor = executor

    async def load(self, installation_id: UUID) -> InstallationLifecycle | None:
        state = await load_state(
            self._executor,
            LIFECYCLE_WORKFLOW_KIND,
            str(installation_id),
        )
        if state is None:
            return None
        value = state.state_data
        return InstallationLifecycle(
            installation_id=installation_id,
            tenant_id=UUID(str(value["tenant_id"])),
            connector_id=str(value["connector_id"]),
            desired=DesiredInstallationState(str(value["desired"])),
            observed=InstallationPhase(str(value["observed"])),
            generation=int(value["generation"]),
            observed_generation=int(value["observed_generation"]),
            conditions=tuple(
                _condition_from_json(item)
                for item in value.get("conditions", ())
            ),
        )

    async def save(self, lifecycle: InstallationLifecycle) -> None:
        now = datetime.now(timezone.utc)
        await persist_state(
            self._executor,
            WorkflowState(
                workflow_kind=LIFECYCLE_WORKFLOW_KIND,
                workflow_id=str(lifecycle.installation_id),
                tenant_id=lifecycle.tenant_id,
                state_data={
                    "tenant_id": str(lifecycle.tenant_id),
                    "connector_id": lifecycle.connector_id,
                    "desired": lifecycle.desired.value,
                    "observed": lifecycle.observed.value,
                    "generation": lifecycle.generation,
                    "observed_generation": lifecycle.observed_generation,
                    "conditions": [
                        _condition_to_json(condition)
                        for condition in lifecycle.conditions
                    ],
                },
                last_advanced_at=now,
                paused_at=(
                    now
                    if lifecycle.observed is InstallationPhase.PAUSED
                    else None
                ),
            ),
        )


def lifecycle_from_existing_install(
    install: Any,
    *,
    connector_id: str,
) -> InstallationLifecycle:
    """Map today's ``enabled`` flag into a compatible initial lifecycle."""

    enabled = bool(install.get("enabled", True))
    phase = InstallationPhase.READY if enabled else InstallationPhase.PAUSED
    desired = (
        DesiredInstallationState.READY
        if enabled
        else DesiredInstallationState.PAUSED
    )
    return InstallationLifecycle(
        installation_id=UUID(str(install["id"])),
        tenant_id=UUID(str(install["tenant_id"])),
        connector_id=connector_id,
        desired=desired,
        observed=phase,
        generation=1,
        observed_generation=1,
    )


__all__ = [
    "LIFECYCLE_WORKFLOW_KIND",
    "WorkflowStateLifecycleRepository",
    "lifecycle_from_existing_install",
]
