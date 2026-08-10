"""Concrete least-authority host-service adapters."""

from __future__ import annotations

import asyncio
import fnmatch
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any
from urllib.parse import quote, urlsplit
from uuid import UUID

import httpx
from websockets.asyncio.client import connect as websocket_connect

from services.ingest.source_contract.connector import GrantedAuthority
from services.ingest.source_contract.errors import (
    OperationCancelledError,
    PermissionDeniedError,
)
from services.ingest.source_contract.host_services import (
    CallbackAllocation,
    GovernedHttpRequest,
    GovernedHttpResponse,
    GovernedGatewayRequest,
    HostServices,
    InstallationData,
    InstallationDataPatch,
    SecretCandidate,
    SecretValue,
)
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import (
    PublicationReceipt,
    SourceRecord,
    VersionedState,
)


SecretReader = Callable[[UUID, SlotId], Awaitable[SecretValue]]
SecretCandidateWriter = Callable[[UUID, SecretCandidate], Awaitable[str]]
StateReader = Callable[[UUID, str], Awaitable[VersionedState | None]]
InstallationDataReader = Callable[
    [UUID, str], Awaitable[InstallationData | None]
]
InstallationDataWriter = Callable[
    [UUID, InstallationDataPatch], Awaitable[int]
]
RawRecordPublisher = Callable[
    [UUID, SourceRecord, str], Awaitable[PublicationReceipt]
]
CallbackProvider = Callable[[UUID, str], Awaitable[CallbackAllocation]]
MetricIncrement = Callable[[str, int, tuple[tuple[str, str], ...]], None]
MetricObserve = Callable[[str, float, tuple[tuple[str, str], ...]], None]
LeaseHeartbeat = Callable[[UUID, dict[str, Any] | None], Awaitable[None]]


async def _deny_secret_read(_installation: UUID, slot: SlotId) -> SecretValue:
    raise PermissionDeniedError(
        "secret backend is not configured",
        details={"slot": str(slot)},
    )


async def _deny_secret_write(
    _installation: UUID, candidate: SecretCandidate
) -> str:
    raise PermissionDeniedError(
        "secret candidate backend is not configured",
        details={"slot": str(candidate.slot)},
    )


async def _empty_state(_installation: UUID, _kind: str) -> VersionedState | None:
    return None


async def _empty_installation_data(
    _installation: UUID, _namespace: str
) -> InstallationData | None:
    return None


async def _deny_installation_write(
    _installation: UUID, patch: InstallationDataPatch
) -> int:
    raise PermissionDeniedError(
        "installation store backend is not configured",
        details={"namespace": patch.namespace},
    )


async def _deny_raw_emit(
    _installation: UUID, _record: SourceRecord, _ingress_kind: str
) -> PublicationReceipt:
    raise PermissionDeniedError("raw emission backend is not configured")


async def _deny_callback(
    _installation: UUID, purpose: str
) -> CallbackAllocation:
    raise PermissionDeniedError(
        "callback allocator is not configured",
        details={"purpose": purpose},
    )


async def _noop_lease(
    _installation: UUID, _details: dict[str, Any] | None
) -> None:
    return None


class ScopedSecrets:
    def __init__(
        self,
        installation_id: UUID,
        allowed_slots: frozenset[str],
        reader: SecretReader,
        writer: SecretCandidateWriter,
    ) -> None:
        self._installation_id = installation_id
        self._allowed_slots = allowed_slots
        self._reader = reader
        self._writer = writer

    def _authorize(self, slot: SlotId) -> None:
        if str(slot) not in self._allowed_slots:
            raise PermissionDeniedError(
                "connector was not granted this secret slot",
                details={"slot": str(slot)},
            )

    async def resolve(self, slot: SlotId) -> SecretValue:
        self._authorize(slot)
        return await self._reader(self._installation_id, slot)

    async def store_candidate(self, candidate: SecretCandidate) -> str:
        self._authorize(candidate.slot)
        return await self._writer(self._installation_id, candidate)


class GovernedHttp:
    """HTTPS-only client constrained to the binding's exact host grants."""

    def __init__(
        self,
        allowed_hosts: frozenset[str],
        client: httpx.AsyncClient,
        *,
        maximum_timeout_seconds: float = 30.0,
        maximum_response_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self._allowed_hosts = allowed_hosts
        self._client = client
        self._maximum_timeout_seconds = maximum_timeout_seconds
        self._maximum_response_bytes = maximum_response_bytes

    async def send(self, request: GovernedHttpRequest) -> GovernedHttpResponse:
        parsed = urlsplit(request.url)
        host = (parsed.hostname or "").lower()
        granted = any(fnmatch.fnmatchcase(host, pattern) for pattern in self._allowed_hosts)
        if parsed.scheme != "https" or not granted:
            raise PermissionDeniedError(
                "outbound HTTP destination is outside the binding grant",
                details={"scheme": parsed.scheme, "host": host},
            )
        if parsed.username is not None or parsed.password is not None:
            raise PermissionDeniedError("userinfo is forbidden in governed URLs")
        timeout = min(
            request.timeout_seconds or self._maximum_timeout_seconds,
            self._maximum_timeout_seconds,
        )
        url = request.url
        if request.url_secret is not None:
            if request.url_secret_placeholder not in url:
                raise PermissionDeniedError("governed URL secret placeholder is absent")
            url = url.replace(
                request.url_secret_placeholder,
                quote(request.url_secret.reveal_text(), safe=""),
            )
        response = await self._client.request(
            request.method.upper(),
            url,
            headers=request.headers,
            params=request.query,
            content=request.body,
            timeout=timeout,
            follow_redirects=False,
        )
        body = await response.aread()
        if len(body) > self._maximum_response_bytes:
            raise PermissionDeniedError(
                "governed HTTP response exceeded the configured byte limit",
                details={"limit": self._maximum_response_bytes},
            )
        return GovernedHttpResponse(
            status_code=response.status_code,
            headers=tuple(response.headers.multi_items()),
            body=body,
        )


class ReadOnlyStateView:
    def __init__(self, installation_id: UUID, reader: StateReader) -> None:
        self._installation_id = installation_id
        self._reader = reader

    async def read(self, kind: str) -> VersionedState | None:
        return await self._reader(self._installation_id, kind)


class GovernedGateway:
    """Bounded WebSocket sessions constrained to the connector host grant."""

    def __init__(self, allowed_hosts: frozenset[str]) -> None:
        self._allowed_hosts = allowed_hosts
        self._connections: dict[str, Any] = {}

    async def connect(self, request: GovernedGatewayRequest) -> str:
        parsed = urlsplit(request.url)
        host = (parsed.hostname or "").lower()
        granted = any(fnmatch.fnmatchcase(host, pattern) for pattern in self._allowed_hosts)
        if parsed.scheme != "wss" or not granted:
            raise PermissionDeniedError(
                "gateway destination is outside the binding grant",
                details={"scheme": parsed.scheme, "host": host},
            )
        connection = await websocket_connect(
            request.url,
            additional_headers=request.headers,
            max_size=request.maximum_message_bytes,
            open_timeout=15,
        )
        connection_id = str(id(connection))
        self._connections[connection_id] = connection
        return connection_id

    def _require(self, connection_id: str) -> Any:
        try:
            return self._connections[connection_id]
        except KeyError as exc:
            raise PermissionDeniedError("gateway connection is unavailable") from exc

    async def send_json(self, connection_id: str, payload: dict[str, Any]) -> None:
        import json

        await self._require(connection_id).send(json.dumps(payload))

    async def receive_json(self, connection_id: str) -> dict[str, Any]:
        import json

        raw = await self._require(connection_id).recv()
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise PermissionDeniedError("gateway frame is not a JSON object")
        return value

    async def close(self, connection_id: str, *, code: int = 1000) -> None:
        connection = self._connections.pop(connection_id, None)
        if connection is not None:
            await connection.close(code=code)


class ScopedInstallationStore:
    def __init__(
        self,
        installation_id: UUID,
        reader: InstallationDataReader,
        writer: InstallationDataWriter,
    ) -> None:
        self._installation_id = installation_id
        self._reader = reader
        self._writer = writer

    async def read(self, namespace: str) -> InstallationData | None:
        return await self._reader(self._installation_id, namespace)

    async def compare_and_set(self, patch: InstallationDataPatch) -> int:
        return await self._writer(self._installation_id, patch)


class DurableRawEmitter:
    """Delegates only to a host-owned durable raw publication primitive."""

    def __init__(self, installation_id: UUID, publisher: RawRecordPublisher) -> None:
        self._installation_id = installation_id
        self._publisher = publisher

    async def emit(
        self, record: SourceRecord, *, ingress_kind: str
    ) -> PublicationReceipt:
        return await self._publisher(self._installation_id, record, ingress_kind)


class ScopedCallbackAllocator:
    def __init__(self, installation_id: UUID, provider: CallbackProvider) -> None:
        self._installation_id = installation_id
        self._provider = provider

    async def allocate(self, purpose: str) -> CallbackAllocation:
        return await self._provider(self._installation_id, purpose)


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class CancellationToken:
    def __init__(self, event: asyncio.Event | None = None) -> None:
        self._event = event or asyncio.Event()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> None:
        self._event.set()

    def raise_if_cancelled(self) -> None:
        if self.cancelled:
            raise OperationCancelledError("connector operation was cancelled")

    async def wait(self) -> None:
        await self._event.wait()


class BoundedMetrics:
    def __init__(
        self,
        base_attributes: tuple[tuple[str, str], ...],
        incrementer: MetricIncrement | None = None,
        observer: MetricObserve | None = None,
    ) -> None:
        self._base = base_attributes
        self._incrementer = incrementer or (lambda _n, _v, _a: None)
        self._observer = observer or (lambda _n, _v, _a: None)

    def _attributes(
        self, attributes: tuple[tuple[str, str], ...]
    ) -> tuple[tuple[str, str], ...]:
        return tuple(sorted(dict(self._base + attributes).items()))

    def increment(
        self,
        name: str,
        value: int = 1,
        *,
        attributes: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self._incrementer(name, value, self._attributes(attributes))

    def observe(
        self,
        name: str,
        value: float,
        *,
        attributes: tuple[tuple[str, str], ...] = (),
    ) -> None:
        self._observer(name, value, self._attributes(attributes))


def _redact_fields(fields: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in fields.items():
        lowered = key.lower()
        if isinstance(value, SecretValue) or any(
            marker in lowered for marker in ("secret", "token", "password")
        ):
            result[key] = "<redacted>"
        else:
            result[key] = value
    return result


class StructuredLog:
    def __init__(self, logger: logging.Logger, base_fields: dict[str, str]) -> None:
        self._logger = logger
        self._base = MappingProxyType(dict(base_fields))

    def _write(self, level: int, event: str, fields: dict[str, Any]) -> None:
        self._logger.log(
            level,
            event,
            extra={**self._base, **_redact_fields(fields)},
        )

    def debug(self, event: str, **fields: Any) -> None:
        self._write(logging.DEBUG, event, fields)

    def info(self, event: str, **fields: Any) -> None:
        self._write(logging.INFO, event, fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._write(logging.WARNING, event, fields)

    def error(self, event: str, **fields: Any) -> None:
        self._write(logging.ERROR, event, fields)


class ScopedLease:
    def __init__(self, installation_id: UUID, heartbeat: LeaseHeartbeat) -> None:
        self._installation_id = installation_id
        self._heartbeat = heartbeat

    async def heartbeat(self, details: dict[str, Any] | None = None) -> None:
        await self._heartbeat(self._installation_id, details)


class HostServicesFactory:
    """Build installation-scoped concrete ports from platform callbacks."""

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        secret_reader: SecretReader = _deny_secret_read,
        secret_writer: SecretCandidateWriter = _deny_secret_write,
        state_reader: StateReader = _empty_state,
        installation_reader: InstallationDataReader = _empty_installation_data,
        installation_writer: InstallationDataWriter = _deny_installation_write,
        raw_publisher: RawRecordPublisher = _deny_raw_emit,
        callback_provider: CallbackProvider = _deny_callback,
        metric_incrementer: MetricIncrement | None = None,
        metric_observer: MetricObserve | None = None,
        logger: logging.Logger | None = None,
        lease_heartbeat: LeaseHeartbeat = _noop_lease,
    ) -> None:
        self._http_client = http_client
        self._secret_reader = secret_reader
        self._secret_writer = secret_writer
        self._state_reader = state_reader
        self._installation_reader = installation_reader
        self._installation_writer = installation_writer
        self._raw_publisher = raw_publisher
        self._callback_provider = callback_provider
        self._metric_incrementer = metric_incrementer
        self._metric_observer = metric_observer
        self._logger = logger or logging.getLogger("source_connector")
        self._lease_heartbeat = lease_heartbeat

    def build(
        self,
        installation_id: UUID,
        authority: GrantedAuthority,
        *,
        connector_id: str,
        cancellation: CancellationToken | None = None,
    ) -> HostServices:
        attributes = (
            ("connector_id", connector_id),
            ("installation_id", str(installation_id)),
        )
        return HostServices(
            secrets=ScopedSecrets(
                installation_id,
                authority.secret_slots,
                self._secret_reader,
                self._secret_writer,
            ),
            http=GovernedHttp(authority.outbound_hosts, self._http_client),
            gateway=GovernedGateway(authority.outbound_hosts),
            state=ReadOnlyStateView(installation_id, self._state_reader),
            installation_store=ScopedInstallationStore(
                installation_id,
                self._installation_reader,
                self._installation_writer,
            ),
            raw_emission=DurableRawEmitter(installation_id, self._raw_publisher),
            subscription_callbacks=ScopedCallbackAllocator(
                installation_id, self._callback_provider
            ),
            clock=SystemClock(),
            cancellation=cancellation or CancellationToken(),
            metrics=BoundedMetrics(
                attributes,
                self._metric_incrementer,
                self._metric_observer,
            ),
            logger=StructuredLog(self._logger, dict(attributes)),
            lease=ScopedLease(installation_id, self._lease_heartbeat),
        )


__all__ = [
    "BoundedMetrics",
    "CancellationToken",
    "DurableRawEmitter",
    "GovernedHttp",
    "HostServicesFactory",
    "ReadOnlyStateView",
    "ScopedInstallationStore",
    "ScopedSecrets",
    "StructuredLog",
    "SystemClock",
]
