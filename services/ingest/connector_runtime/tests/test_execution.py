from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.ingest.connector_conformance.fakes import (
    FakeHostEnvironment,
    make_binding_context,
)
from services.ingest.connector_runtime.execution import (
    CapabilityExecutionRequest,
    ConnectorCapabilityExecutor,
)
from services.ingest.connector_runtime.failures import (
    RuntimeConnectorFailure,
    RuntimeFailureAction,
    classify_failure,
)
from services.ingest.connector_runtime.policy import (
    AtomicRoutingPolicy,
    ExecutionMode,
    RoutingPolicy,
)
from services.ingest.connector_runtime.registry import ConnectorRegistryBuilder
from services.ingest.connector_runtime.shadow import (
    InMemoryShadowReportSink,
    ShadowDimension,
)
from services.ingest.connector_runtime.tests.helpers import make_candidate
from services.ingest.source_contract.capabilities import IDENTITY_V1
from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.models import IdentityInput, SourceRecord


def _identity_input() -> IdentityInput:
    return IdentityInput(
        record=SourceRecord(native_type="event", payload={"id": "1"}),
        external_installation_id="workspace-1",
        ingress_kind="webhook",
    )


def _request(candidate, environment, *, legacy_value="legacy:workspace-1"):
    context = make_binding_context(
        candidate.manifest,
        environment=environment,
    )

    async def legacy_call() -> str:
        return legacy_value

    async def connector_call(capability, _operation) -> str:
        return capability.external_id(_identity_input())

    return CapabilityExecutionRequest(
        installation=context.installation,
        source=candidate.manifest.source,
        authority=context.authority,
        services=context.services,
        capability=IDENTITY_V1,
        connector_call=connector_call,
        legacy_call=legacy_call,
        deadline=datetime.now(timezone.utc) + timedelta(seconds=5),
        shadow_projection=lambda value: {ShadowDimension.IDENTITY: value},
    )


@pytest.mark.asyncio
async def test_default_legacy_mode_never_binds_connector() -> None:
    candidate, connector = make_candidate()
    environment = FakeHostEnvironment()
    registry = ConnectorRegistryBuilder().add(candidate).build()
    executor = ConnectorCapabilityExecutor(registry, AtomicRoutingPolicy())

    result = await executor.execute(_request(candidate, environment))

    assert result == "legacy:workspace-1"
    assert connector.bind_calls == 0
    assert environment.metrics.increments == [
        (
            "source_connector.rollout.execution",
            1,
            (
                ("connector_id", "fyralis/example"),
                ("capability", "semantic.identity"),
                ("implementation", "legacy"),
                ("outcome", "completed"),
            ),
        )
    ]


@pytest.mark.asyncio
async def test_connector_mode_binds_then_emits_complete_telemetry() -> None:
    candidate, connector = make_candidate()
    environment = FakeHostEnvironment()
    registry = ConnectorRegistryBuilder().add(candidate).build()
    executor = ConnectorCapabilityExecutor(
        registry,
        AtomicRoutingPolicy(RoutingPolicy(global_mode=ExecutionMode.CONNECTOR)),
    )

    result = await executor.execute(_request(candidate, environment))

    assert result == "example:workspace-1"
    assert connector.bind_calls == 1
    completed = next(
        item
        for item in environment.metrics.increments
        if item[0] == "source_connector.capability.completed"
    )
    attributes = dict(completed[2])
    assert attributes["connector_id"] == "fyralis/example"
    assert attributes["capability"] == "semantic.identity"
    assert attributes["contract_version"] == "1.0.0"
    assert attributes["connector_version"] == "1.0.0"
    assert len(attributes["registry_fingerprint"]) == 64
    assert attributes["installation_id"]
    assert attributes["execution_id"]


@pytest.mark.asyncio
async def test_insufficient_authority_fails_before_connector_binding() -> None:
    candidate, connector = make_candidate()
    environment = FakeHostEnvironment()
    registry = ConnectorRegistryBuilder().add(candidate).build()
    executor = ConnectorCapabilityExecutor(
        registry,
        AtomicRoutingPolicy(RoutingPolicy(global_mode=ExecutionMode.CONNECTOR)),
    )
    request = _request(candidate, environment)
    request = CapabilityExecutionRequest(
        **{**request.__dict__, "authority": GrantedAuthority()}
    )

    with pytest.raises(RuntimeConnectorFailure) as captured:
        await executor.execute(request)

    assert connector.bind_calls == 0
    assert captured.value.translated.action is RuntimeFailureAction.FAIL_CLOSED


@pytest.mark.asyncio
async def test_shadow_compares_but_always_returns_legacy_output() -> None:
    candidate, connector = make_candidate()
    environment = FakeHostEnvironment()
    registry = ConnectorRegistryBuilder().add(candidate).build()
    sink = InMemoryShadowReportSink()
    executor = ConnectorCapabilityExecutor(
        registry,
        AtomicRoutingPolicy(RoutingPolicy(global_mode=ExecutionMode.SHADOW)),
        shadow_sink=sink,
    )

    result = await executor.execute(_request(candidate, environment))

    assert result == "legacy:workspace-1"
    assert connector.bind_calls == 1
    assert len(sink.reports) == 1
    assert not sink.reports[0].matches
    assert sink.reports[0].differences[0].dimension is ShadowDimension.IDENTITY


@pytest.mark.asyncio
async def test_shadow_can_skip_side_effecting_capabilities() -> None:
    candidate, connector = make_candidate()
    environment = FakeHostEnvironment()
    registry = ConnectorRegistryBuilder().add(candidate).build()
    executor = ConnectorCapabilityExecutor(
        registry,
        AtomicRoutingPolicy(RoutingPolicy(global_mode=ExecutionMode.SHADOW)),
    )
    request = _request(candidate, environment)
    request = CapabilityExecutionRequest(**{**request.__dict__, "shadow_safe": False})

    assert await executor.execute(request) == "legacy:workspace-1"
    assert connector.bind_calls == 0


def test_legacy_recoverable_marker_preserves_worker_retry_semantics() -> None:
    class LegacyRateLimit(Exception):
        recoverable = True
        code = "legacy_rate_limited"

    translated = classify_failure(LegacyRateLimit())
    failure = RuntimeConnectorFailure("bounded", translated)

    assert failure.retryable
    assert failure.recoverable
    assert failure.translated.action is RuntimeFailureAction.RETRY
