"""Contract-only connector routing and fail-closed artifact quarantine."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from types import MappingProxyType
from typing import Mapping
from uuid import UUID

from services.ingest.source_contract.errors import SourceUnavailableError


class ExecutionMode(StrEnum):
    """The sole supported source execution mode."""

    CONNECTOR = "connector"


@dataclass(frozen=True)
class RouteRequest:
    tenant_id: UUID
    connector_id: str
    source: str
    capability: str


@dataclass(frozen=True)
class RouteDecision:
    mode: ExecutionMode
    matched_scope: str
    policy_revision: int


@dataclass(frozen=True)
class RoutingPolicy:
    """Immutable contract-only routing snapshot.

    Source execution can no longer select a parallel implementation. Rollback
    is performed by deploying a previously signed connector artifact.
    """

    revision: int = 1
    global_mode: ExecutionMode = ExecutionMode.CONNECTOR

    def __post_init__(self) -> None:
        if self.revision < 1:
            raise ValueError("policy revision must be positive")
        if self.global_mode is not ExecutionMode.CONNECTOR:
            raise ValueError("only connector execution is supported")

    def resolve(self, request: RouteRequest) -> RouteDecision:
        return RouteDecision(ExecutionMode.CONNECTOR, "global", self.revision)


class AtomicRoutingPolicy:
    """Process-local connector revision and quarantine snapshot."""

    def __init__(self, policy: RoutingPolicy | None = None) -> None:
        self._lock = Lock()
        self._policy = policy or RoutingPolicy()
        self._quarantined: Mapping[str, str] = MappingProxyType({})

    def snapshot(self) -> RoutingPolicy:
        with self._lock:
            return self._policy

    def replace(self, policy: RoutingPolicy) -> None:
        with self._lock:
            if policy.revision <= self._policy.revision:
                raise ValueError("replacement policy revision must increase")
            self._policy = policy

    def replace_quarantine(self, quarantined: Mapping[str, str]) -> None:
        with self._lock:
            self._quarantined = MappingProxyType(dict(quarantined))

    def quarantined(self) -> Mapping[str, str]:
        with self._lock:
            return self._quarantined

    def resolve(self, request: RouteRequest) -> RouteDecision:
        with self._lock:
            reason = self._quarantined.get(request.connector_id)
            policy = self._policy
        if reason is not None:
            raise SourceUnavailableError(
                "connector artifact is quarantined",
                details={
                    "connector_id": request.connector_id,
                    "reason": reason,
                    "policy_revision": policy.revision,
                },
            )
        return policy.resolve(request)


__all__ = [
    "AtomicRoutingPolicy",
    "ExecutionMode",
    "RouteDecision",
    "RouteRequest",
    "RoutingPolicy",
]
