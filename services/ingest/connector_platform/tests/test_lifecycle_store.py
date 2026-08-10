from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.ingest.connector_platform.lifecycle_store import (
    WorkflowStateLifecycleRepository,
)
from services.ingest.connector_runtime.lifecycle import (
    DesiredInstallationState,
    InstallationCondition,
    InstallationLifecycle,
    InstallationPhase,
)


class _StateExecutor:
    def __init__(self) -> None:
        self.row = None

    async def execute(self, _sql, kind, workflow_id, tenant_id, state_data, advanced, paused):
        self.row = {
            "workflow_kind": kind,
            "workflow_id": workflow_id,
            "tenant_id": tenant_id,
            "state_data": json.loads(state_data),
            "last_advanced_at": advanced,
            "paused_at": paused,
        }

    async def fetchrow(self, _sql, kind, workflow_id):
        if self.row is None:
            return None
        if self.row["workflow_kind"] == kind and self.row["workflow_id"] == workflow_id:
            return self.row
        return None


@pytest.mark.asyncio
async def test_lifecycle_round_trips_through_existing_workflow_state_shape() -> None:
    executor = _StateExecutor()
    repository = WorkflowStateLifecycleRepository(executor)
    lifecycle = InstallationLifecycle(
        installation_id=uuid4(),
        tenant_id=uuid4(),
        connector_id="fyralis/slack",
        desired=DesiredInstallationState.PAUSED,
        observed=InstallationPhase.PAUSED,
        generation=3,
        observed_generation=3,
        conditions=(
            InstallationCondition(
                type="Ready",
                status=False,
                reason="Paused",
                message="operator pause",
                observed_at=datetime.now(timezone.utc),
            ),
        ),
    )

    await repository.save(lifecycle)
    loaded = await repository.load(lifecycle.installation_id)

    assert loaded == lifecycle
    assert executor.row is not None
    assert executor.row["paused_at"] is not None
