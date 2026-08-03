"""Continuous installation reconciliation owned by the connector runtime."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from services.ingest.connector_runtime.authority import (
    AuthorityRepository,
    scope_authority,
)
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.connector_runtime.lifecycle import (
    DesiredInstallationState,
    InstallationCondition,
    InstallationLifecycle,
    InstallationLifecycleController,
    InstallationPhase,
    LifecycleEvidence,
)
from services.ingest.connector_runtime.registry import ConnectorRegistry
from services.ingest.source_contract.capabilities import (
    CLEANUP_V1,
    HEALTH_PROBE_V1,
)
from services.ingest.source_contract.capabilities.lifecycle import (
    CleanupRequest,
    HealthProbeRequest,
)
from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.models import InstallationRef


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


class PostgresInstallationLifecycleRepository:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @staticmethod
    def _lifecycle(row: Any) -> InstallationLifecycle:
        return InstallationLifecycle(
            installation_id=row["id"],
            tenant_id=row["tenant_id"],
            connector_id=row["connector_id"],
            desired=DesiredInstallationState(row["desired_state"]),
            observed=InstallationPhase(row["observed_phase"]),
            generation=int(row["generation"]),
            observed_generation=int(row["observed_generation"]),
            conditions=tuple(
                _condition_from_json(item) for item in (row["conditions"] or ())
            ),
        )

    async def load(self, installation_id: UUID) -> InstallationLifecycle | None:
        row = await self._pool.fetchrow(
            """
            SELECT id, tenant_id, connector_id, desired_state, observed_phase,
                   generation, observed_generation, conditions
              FROM source_connector_installations
             WHERE id = $1
            """,
            installation_id,
        )
        return self._lifecycle(row) if row is not None else None

    async def list_due(self, *, limit: int = 100) -> tuple[InstallationLifecycle, ...]:
        rows = await self._pool.fetch(
            """
            SELECT id, tenant_id, connector_id, desired_state, observed_phase,
                   generation, observed_generation, conditions
              FROM source_connector_installations
             WHERE observed_phase <> 'Removed'
               AND next_reconcile_at <= now()
             ORDER BY next_reconcile_at, id
             LIMIT $1
            """,
            limit,
        )
        return tuple(self._lifecycle(row) for row in rows)

    async def save(self, lifecycle: InstallationLifecycle) -> None:
        status = await self._pool.execute(
            """
            UPDATE source_connector_installations
               SET desired_state = $3,
                   observed_phase = $4,
                   observed_generation = $5,
                   conditions = $6::jsonb,
                   next_reconcile_at = CASE
                       WHEN $4 = 'Removed' THEN 'infinity'::timestamptz
                       WHEN $4 IN ('Ready', 'Degraded') THEN now() + interval '60 seconds'
                       ELSE now() + interval '5 seconds'
                   END,
                   removed_at = CASE WHEN $4 = 'Removed' THEN now() ELSE removed_at END,
                   updated_at = now()
             WHERE id = $1 AND generation = $2
            """,
            lifecycle.installation_id,
            lifecycle.generation,
            lifecycle.desired.value,
            lifecycle.observed.value,
            lifecycle.observed_generation,
            json.dumps([_condition_to_json(item) for item in lifecycle.conditions]),
        )
        if not status.endswith(" 1"):
            raise RuntimeError("installation lifecycle generation fence was lost")

    async def retire_credentials(self, installation_id: UUID) -> None:
        await self._pool.execute(
            """
            UPDATE source_connector_credentials
               SET state = 'retired', retired_at = now()
             WHERE installation_id = $1
               AND state IN ('current', 'pending')
            """,
            installation_id,
        )


class ContinuousInstallationController:
    def __init__(
        self,
        registry: ConnectorRegistry,
        authority_repository: AuthorityRepository,
        lifecycle_repository: PostgresInstallationLifecycleRepository,
        host_services: HostServicesFactory,
        *,
        operation_timeout_seconds: float = 30.0,
        admitted_connector_ids: frozenset[str] | None = None,
    ) -> None:
        self._registry = registry
        self._authorities = authority_repository
        self._repository = lifecycle_repository
        self._host_services = host_services
        self._transitions = InstallationLifecycleController()
        self._operation_timeout_seconds = operation_timeout_seconds
        self._admitted_connector_ids = admitted_connector_ids

    async def _observe(
        self, lifecycle: InstallationLifecycle, now: datetime
    ) -> LifecycleEvidence:
        if (
            self._admitted_connector_ids is not None
            and lifecycle.connector_id not in self._admitted_connector_ids
        ):
            return LifecycleEvidence(
                failure_reason=(
                    "ArtifactQuarantined: connector artifact is not admitted"
                )
            )
        installation = InstallationRef(
            id=lifecycle.installation_id,
            tenant_id=lifecycle.tenant_id,
            connector_id=lifecycle.connector_id,
            generation=lifecycle.generation,
        )
        authority = await self._authorities.load(lifecycle.installation_id)
        if authority is None:
            return LifecycleEvidence()
        try:
            grant = scope_authority(
                self._registry.require(installation.connector_id).manifest,
                authority.validate_for(installation),
            )
            services = self._host_services.build(
                installation.id,
                grant,
                connector_id=installation.connector_id,
            )
            binding = self._registry.resolve_for_install(
                BindingContext(
                    installation=installation,
                    authority=grant,
                    services=services,
                )
            )
            operation = OperationContext(
                invocation_id=uuid4(),
                deadline=now + timedelta(seconds=self._operation_timeout_seconds),
                services=services,
            )
            if lifecycle.desired is DesiredInstallationState.REMOVED:
                cleanup = binding.require(CLEANUP_V1)
                result = await cleanup.cleanup(
                    CleanupRequest(
                        operation_id=str(operation.invocation_id),
                        revoke_remote=True,
                    ),
                    operation,
                )
                if result.complete:
                    await self._repository.retire_credentials(installation.id)
                    await self._authorities.revoke(
                        installation.id,
                        revoked_at=now,
                        reason="installation_removed",
                    )
                return LifecycleEvidence(
                    authorized=True,
                    configuration_valid=True,
                    initialized=True,
                    removal_complete=result.complete,
                )

            healthy = True
            if lifecycle.observed in {
                InstallationPhase.INITIALIZING,
                InstallationPhase.READY,
                InstallationPhase.DEGRADED,
            }:
                report = await binding.require(HEALTH_PROBE_V1).probe(
                    HealthProbeRequest(depth="remote"), operation
                )
                healthy = report.healthy
            return LifecycleEvidence(
                authorized=True,
                configuration_valid=True,
                initialized=True,
                healthy=healthy,
            )
        except Exception as exc:
            return LifecycleEvidence(
                failure_reason=f"{type(exc).__name__}: connector lifecycle step failed"
            )

    async def reconcile_one(
        self, lifecycle: InstallationLifecycle
    ) -> InstallationLifecycle:
        now = datetime.now(timezone.utc)
        evidence = await self._observe(lifecycle, now)
        updated = self._transitions.reconcile(lifecycle, evidence, now=now)
        await self._repository.save(updated)
        return updated

    async def run_once(self, *, limit: int = 100) -> int:
        due = await self._repository.list_due(limit=limit)
        for lifecycle in due:
            await self.reconcile_one(lifecycle)
        return len(due)

    async def run(
        self,
        stop_event: asyncio.Event,
        *,
        interval_seconds: float = 5.0,
    ) -> None:
        while not stop_event.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue


__all__ = [
    "ContinuousInstallationController",
    "PostgresInstallationLifecycleRepository",
]
