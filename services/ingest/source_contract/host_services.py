"""Least-authority host ports granted to a bound connector."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from services.ingest.source_contract.identity import SlotId

if TYPE_CHECKING:
    from services.ingest.source_contract.models import (
        PublicationReceipt,
        SourceRecord,
        VersionedState,
    )


@dataclass(frozen=True, repr=False)
class SecretValue:
    """Secret bytes with deliberately redacted repr/str output."""

    _value: bytes

    @classmethod
    def from_text(cls, value: str) -> "SecretValue":
        return cls(value.encode("utf-8"))

    def reveal_bytes(self) -> bytes:
        return self._value

    def reveal_text(self) -> str:
        return self._value.decode("utf-8")

    def __repr__(self) -> str:
        return "SecretValue(<redacted>)"

    def __str__(self) -> str:
        return "<redacted>"


@dataclass(frozen=True)
class SecretCandidate:
    slot: SlotId
    value: SecretValue
    expires_at: datetime | None = None


@dataclass(frozen=True)
class GovernedHttpRequest:
    method: str
    url: str
    headers: tuple[tuple[str, str], ...] = ()
    query: tuple[tuple[str, str], ...] = ()
    body: bytes | None = None
    timeout_seconds: float | None = None


@dataclass(frozen=True)
class GovernedHttpResponse:
    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


@dataclass(frozen=True)
class StateProposal:
    kind: str
    expected_revision: int
    schema_version: int
    payload: dict[str, Any]


@dataclass(frozen=True)
class InstallationData:
    namespace: str
    generation: int
    values: dict[str, Any]


@dataclass(frozen=True)
class InstallationDataPatch:
    namespace: str
    expected_generation: int
    values: dict[str, Any]


@runtime_checkable
class SecretsPort(Protocol):
    async def resolve(self, slot: SlotId) -> SecretValue: ...

    async def store_candidate(self, candidate: SecretCandidate) -> str: ...


@runtime_checkable
class HttpPort(Protocol):
    async def send(self, request: GovernedHttpRequest) -> GovernedHttpResponse: ...


@runtime_checkable
class StateViewPort(Protocol):
    """Read-only state view.

    Capability results carry proposed next state back to the runtime. The port
    intentionally provides no direct commit operation, preserving host-owned
    publication/checkpoint ordering.
    """

    async def read(self, kind: str) -> "VersionedState | None": ...


@runtime_checkable
class InstallationStorePort(Protocol):
    async def read(self, namespace: str) -> InstallationData | None: ...

    async def compare_and_set(self, patch: InstallationDataPatch) -> int: ...


@runtime_checkable
class RawEmissionPort(Protocol):
    """Host-owned durable emission used only by active/gateway connectors."""

    async def emit(self, record: "SourceRecord") -> "PublicationReceipt": ...


@runtime_checkable
class ClockPort(Protocol):
    def now(self) -> datetime: ...


@runtime_checkable
class CancellationPort(Protocol):
    @property
    def cancelled(self) -> bool: ...

    def raise_if_cancelled(self) -> None: ...

    async def wait(self) -> None: ...


@runtime_checkable
class MetricsPort(Protocol):
    def increment(
        self,
        name: str,
        value: int = 1,
        *,
        attributes: tuple[tuple[str, str], ...] = (),
    ) -> None: ...

    def observe(
        self,
        name: str,
        value: float,
        *,
        attributes: tuple[tuple[str, str], ...] = (),
    ) -> None: ...


@runtime_checkable
class LogPort(Protocol):
    def debug(self, event: str, **fields: Any) -> None: ...

    def info(self, event: str, **fields: Any) -> None: ...

    def warning(self, event: str, **fields: Any) -> None: ...

    def error(self, event: str, **fields: Any) -> None: ...


@runtime_checkable
class LeasePort(Protocol):
    async def heartbeat(self, details: dict[str, Any] | None = None) -> None: ...


@dataclass(frozen=True)
class HostServices:
    secrets: SecretsPort
    http: HttpPort
    state: StateViewPort
    installation_store: InstallationStorePort
    raw_emission: RawEmissionPort
    clock: ClockPort
    cancellation: CancellationPort
    metrics: MetricsPort
    logger: LogPort
    lease: LeasePort


__all__ = [
    "CancellationPort",
    "ClockPort",
    "GovernedHttpRequest",
    "GovernedHttpResponse",
    "HostServices",
    "HttpPort",
    "InstallationData",
    "InstallationDataPatch",
    "InstallationStorePort",
    "LeasePort",
    "LogPort",
    "MetricsPort",
    "RawEmissionPort",
    "SecretCandidate",
    "SecretValue",
    "SecretsPort",
    "StateProposal",
    "StateViewPort",
]
