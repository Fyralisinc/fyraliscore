"""Deterministic, infrastructure-free host services for connector tests."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid5

from services.ingest.source_contract.connector import (
    BindingContext,
    GrantedAuthority,
)
from services.ingest.source_contract.host_services import (
    CallbackAllocation,
    GovernedGatewayRequest,
    GovernedHttpRequest,
    GovernedHttpResponse,
    HostServices,
    InstallationData,
    InstallationDataPatch,
    SecretCandidate,
    SecretValue,
)
from services.ingest.source_contract.manifest import ConnectorManifest
from services.ingest.source_contract.models import (
    InstallationRef,
    PublicationReceipt,
    SourceRecord,
    VersionedState,
)


_FAKE_NAMESPACE = UUID("2d4fbc69-6a02-475e-848f-c327415a4795")


class FakeSecrets:
    def __init__(self, values: dict[str, SecretValue] | None = None) -> None:
        self.values = dict(values or {})
        self.stored: list[SecretCandidate] = []

    async def resolve(self, slot: str) -> SecretValue:
        return self.values[slot]

    async def store_candidate(self, candidate: SecretCandidate) -> str:
        self.stored.append(candidate)
        return f"fake-secret:{candidate.slot}:{len(self.stored)}"


class FakeHttp:
    """A governed HTTP port that rejects unplanned network behavior."""

    def __init__(
        self,
        responses: list[GovernedHttpResponse] | None = None,
    ) -> None:
        self.responses = list(responses or [])
        self.requests: list[GovernedHttpRequest] = []

    async def send(self, request: GovernedHttpRequest) -> GovernedHttpResponse:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError(
                "unexpected HTTP request in connector conformance test"
            )
        return self.responses.pop(0)


class FakeGateway:
    def __init__(self) -> None:
        self.connections: dict[str, list[dict[str, Any]]] = {}
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.planned_connections: list[list[dict[str, Any]]] = []

    async def connect(self, request: GovernedGatewayRequest) -> str:
        connection_id = f"fake-gateway-{len(self.connections) + 1}"
        self.connections[connection_id] = (
            list(self.planned_connections.pop(0))
            if self.planned_connections
            else []
        )
        return connection_id

    async def send_json(self, connection_id: str, payload: dict[str, Any]) -> None:
        self.sent.append((connection_id, dict(payload)))

    async def receive_json(self, connection_id: str) -> dict[str, Any]:
        if not self.connections[connection_id]:
            raise AssertionError("unexpected gateway receive")
        return self.connections[connection_id].pop(0)

    async def close(self, connection_id: str, *, code: int = 1000) -> None:
        self.connections.pop(connection_id, None)


class FakeStateView:
    def __init__(self, values: dict[str, VersionedState] | None = None) -> None:
        self.values = dict(values or {})

    async def read(self, kind: str) -> VersionedState | None:
        return self.values.get(kind)


class FakeInstallationStore:
    def __init__(
        self,
        values: dict[str, InstallationData] | None = None,
    ) -> None:
        self.values = dict(values or {})

    async def read(self, namespace: str) -> InstallationData | None:
        return self.values.get(namespace)

    async def compare_and_set(self, patch: InstallationDataPatch) -> int:
        current = self.values.get(patch.namespace)
        actual_generation = current.generation if current is not None else 0
        if actual_generation != patch.expected_generation:
            raise ValueError("fake installation generation conflict")
        generation = actual_generation + 1
        self.values[patch.namespace] = InstallationData(
            namespace=patch.namespace,
            generation=generation,
            values=dict(patch.values),
        )
        return generation


class FakeRawEmission:
    def __init__(self) -> None:
        self.records: list[SourceRecord] = []
        self.ingress_kinds: list[str] = []

    async def emit(
        self, record: SourceRecord, *, ingress_kind: str
    ) -> PublicationReceipt:
        self.records.append(record)
        self.ingress_kinds.append(ingress_kind)
        index = len(self.records)
        return PublicationReceipt(
            receipt_id=uuid5(_FAKE_NAMESPACE, f"receipt:{index}"),
            raw_object_key=f"fake/raw/{index}",
            content_hash=f"fake-content-hash-{index}",
            acknowledged_at=datetime(2025, 1, 1, tzinfo=UTC),
        )


class FakeSubscriptionCallbacks:
    def __init__(self) -> None:
        self.purposes: list[str] = []

    async def allocate(self, purpose: str) -> CallbackAllocation:
        self.purposes.append(purpose)
        index = len(self.purposes)
        return CallbackAllocation(
            callback_url=f"https://callbacks.example.test/{index}",
            endpoint_id=f"fake-endpoint-{index}",
            verification_nonce=SecretValue.from_text(f"fake-nonce-{index}"),
        )


class FakeClock:
    def __init__(self, current: datetime | None = None) -> None:
        self.current = current or datetime(2025, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        return self.current


class FakeCancellation:
    def __init__(self) -> None:
        self._cancelled = False

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:
        self._cancelled = True

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise RuntimeError("fake operation cancelled")

    async def wait(self) -> None:
        return None


class FakeMetrics:
    def __init__(self) -> None:
        self.increments: list[
            tuple[str, int, tuple[tuple[str, str], ...]]
        ] = []
        self.observations: list[
            tuple[str, float, tuple[tuple[str, str], ...]]
        ] = []

    def increment(
        self,
        name: str,
        value: int = 1,
        *,
        attributes: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self.increments.append((name, value, attributes))

    def observe(
        self,
        name: str,
        value: float,
        *,
        attributes: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self.observations.append((name, value, attributes))


class FakeLog:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, Any]]] = []

    def debug(self, event: str, **fields: Any) -> None:
        self.events.append(("debug", event, fields))

    def info(self, event: str, **fields: Any) -> None:
        self.events.append(("info", event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        self.events.append(("warning", event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.events.append(("error", event, fields))


class FakeLease:
    def __init__(self) -> None:
        self.heartbeats: list[dict[str, Any] | None] = []

    async def heartbeat(self, details: dict[str, Any] | None = None) -> None:
        self.heartbeats.append(details)


class FakeHostEnvironment:
    """Owns fake port state and exposes one complete ``HostServices`` value."""

    def __init__(self) -> None:
        self.secrets = FakeSecrets()
        self.http = FakeHttp()
        self.gateway = FakeGateway()
        self.state = FakeStateView()
        self.installation_store = FakeInstallationStore()
        self.raw_emission = FakeRawEmission()
        self.subscription_callbacks = FakeSubscriptionCallbacks()
        self.clock = FakeClock()
        self.cancellation = FakeCancellation()
        self.metrics = FakeMetrics()
        self.logger = FakeLog()
        self.lease = FakeLease()
        self.services = HostServices(
            secrets=self.secrets,
            http=self.http,
            gateway=self.gateway,
            state=self.state,
            installation_store=self.installation_store,
            raw_emission=self.raw_emission,
            subscription_callbacks=self.subscription_callbacks,
            clock=self.clock,
            cancellation=self.cancellation,
            metrics=self.metrics,
            logger=self.logger,
            lease=self.lease,
        )


def make_binding_context(
    manifest: ConnectorManifest,
    *,
    environment: FakeHostEnvironment | None = None,
    authority: GrantedAuthority | None = None,
    installation_id: UUID | None = None,
    tenant_id: UUID | None = None,
) -> BindingContext:
    """Create a deterministic installation with exactly requested authority."""

    fake_environment = environment or FakeHostEnvironment()
    permissions = manifest.spec.permissions
    granted = authority or GrantedAuthority(
        secret_slots=frozenset(permissions.secret_slots),
        outbound_hosts=frozenset(permissions.outbound_hosts),
        scopes=frozenset(permissions.requested_scopes),
        maximum_trust_tier=manifest.spec.trust.maximum_tier,
    )
    installation = InstallationRef(
        id=installation_id
        or uuid5(_FAKE_NAMESPACE, f"installation:{manifest.connector_id}"),
        tenant_id=tenant_id
        or uuid5(_FAKE_NAMESPACE, f"tenant:{manifest.connector_id}"),
        connector_id=manifest.connector_id,
        generation=1,
    )
    return BindingContext(
        installation=installation,
        authority=granted,
        services=fake_environment.services,
    )


__all__ = [
    "FakeCancellation",
    "FakeClock",
    "FakeHostEnvironment",
    "FakeGateway",
    "FakeHttp",
    "FakeInstallationStore",
    "FakeLease",
    "FakeLog",
    "FakeMetrics",
    "FakeRawEmission",
    "FakeSecrets",
    "FakeStateView",
    "FakeSubscriptionCallbacks",
    "make_binding_context",
]
