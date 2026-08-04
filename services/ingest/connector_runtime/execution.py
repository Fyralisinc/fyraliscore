"""Registry-resolved, contract-only connector capability execution."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Generic, TypeVar
from uuid import UUID, uuid4

from services.ingest.connector_runtime.failures import classify_failure, translate_failure
from services.ingest.connector_runtime.lifecycle import InstallationLifecycle
from services.ingest.connector_runtime.policy import AtomicRoutingPolicy, RouteRequest
from services.ingest.connector_runtime.registry import ConnectorRegistry
from services.ingest.connector_runtime.telemetry import (
    CapabilityTelemetry,
    ConnectorTelemetryFields,
)
from services.ingest.source_contract.connector import (
    BindingContext,
    CapabilityKey,
    GrantedAuthority,
    OperationContext,
    StaticBoundConnector,
)
from services.ingest.source_contract.errors import CapabilityUnavailableError
from services.ingest.source_contract.host_services import HostServices
from services.ingest.source_contract.models import InstallationRef


T = TypeVar("T")
CapabilityCall = Callable[[object, OperationContext], Awaitable[T]]


@dataclass(frozen=True)
class CapabilityExecutionRequest(Generic[T]):
    installation: InstallationRef
    source: str
    authority: GrantedAuthority
    services: HostServices
    capability: CapabilityKey[Any]
    connector_call: CapabilityCall[T]
    deadline: datetime
    execution_id: UUID | None = None
    lifecycle: InstallationLifecycle | None = None


class ConnectorCapabilityExecutor:
    """Execute only admitted connector capabilities.

    Artifact quarantine, missing authority, unavailable facets, and connector
    failures are surfaced as typed failures. There is no alternate execution
    implementation and therefore no hidden data-path fallback.
    """

    def __init__(
        self,
        registry: ConnectorRegistry,
        routing: AtomicRoutingPolicy,
    ) -> None:
        self._registry = registry
        self._routing = routing
        self._fingerprint = registry.health().fingerprint

    def _admit(self, request: CapabilityExecutionRequest[Any]) -> None:
        self._routing.resolve(
            RouteRequest(
                tenant_id=request.installation.tenant_id,
                connector_id=request.installation.connector_id,
                source=request.source,
                capability=request.capability.ref.id,
            )
        )

    def _bind(self, request: CapabilityExecutionRequest[Any]) -> StaticBoundConnector:
        if request.lifecycle is not None and not request.lifecycle.execution_available:
            raise CapabilityUnavailableError(
                "installation lifecycle does not permit connector execution",
                details={
                    "installation_id": str(request.installation.id),
                    "observed": request.lifecycle.observed.value,
                },
            )
        return self._registry.resolve_for_install(
            BindingContext(
                installation=request.installation,
                authority=request.authority,
                services=request.services,
            )
        )

    def _telemetry_fields(
        self,
        request: CapabilityExecutionRequest[Any],
        execution_id: UUID,
    ) -> ConnectorTelemetryFields:
        description = self._registry.describe(request.installation.connector_id)
        return ConnectorTelemetryFields(
            connector_id=description.connector_id,
            capability=request.capability.ref.id,
            contract_version=description.negotiated_contract_version,
            connector_version=description.connector_version,
            execution_id=execution_id,
            registry_fingerprint=self._fingerprint,
            installation_id=request.installation.id,
        )

    async def _connector_call(
        self,
        request: CapabilityExecutionRequest[T],
        execution_id: UUID,
    ) -> T:
        binding = self._bind(request)
        capability = binding.require(request.capability)
        operation = OperationContext(
            invocation_id=execution_id,
            deadline=request.deadline,
            services=request.services,
        )
        request.services.cancellation.raise_if_cancelled()
        if datetime.now(timezone.utc) >= request.deadline:
            raise asyncio.TimeoutError("connector operation deadline elapsed")
        return await request.connector_call(capability, operation)

    @staticmethod
    def _rollout_execution(
        request: CapabilityExecutionRequest[Any],
        *,
        outcome: str,
        started_at: float,
    ) -> None:
        attributes = (
            ("connector_id", str(request.installation.connector_id)),
            ("capability", str(request.capability.ref.id)),
            ("implementation", "connector"),
            ("outcome", outcome),
        )
        request.services.metrics.increment(
            "source_connector.rollout.execution", attributes=attributes
        )
        request.services.metrics.observe(
            "source_connector.rollout.duration_ms",
            (time.monotonic() - started_at) * 1000,
            attributes=attributes,
        )

    async def execute(self, request: CapabilityExecutionRequest[T]) -> T:
        self._admit(request)
        execution_id = request.execution_id or uuid4()
        telemetry = CapabilityTelemetry(request.services)
        fields = self._telemetry_fields(request, execution_id)
        started_at = telemetry.started(fields)
        connector_started = time.monotonic()
        try:
            result = await self._connector_call(request, execution_id)
        except (Exception, asyncio.CancelledError) as exc:
            self._rollout_execution(
                request,
                outcome="failed",
                started_at=connector_started,
            )
            failure = classify_failure(exc)
            telemetry.failed(
                fields,
                started_at,
                failure_code=failure.code,
                retryable=failure.retryable,
            )
            raise translate_failure(exc) from exc
        self._rollout_execution(
            request,
            outcome="completed",
            started_at=connector_started,
        )
        telemetry.completed(fields, started_at, mode="connector")
        return result


__all__ = [
    "CapabilityCall",
    "CapabilityExecutionRequest",
    "ConnectorCapabilityExecutor",
]
