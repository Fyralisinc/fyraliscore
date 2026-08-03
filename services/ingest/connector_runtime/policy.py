"""Configuration-driven routing between legacy and connector execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from threading import Lock
from types import MappingProxyType
from typing import Mapping
from uuid import UUID


class ExecutionMode(StrEnum):
    """Which implementation is authoritative for one capability call."""

    LEGACY = "legacy"
    SHADOW = "shadow"
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
    """Immutable routing snapshot with narrow-to-broad precedence.

    Missing configuration always resolves to ``legacy``. A replacement policy
    can therefore roll all traffic back without a deployment.
    """

    revision: int = 1
    global_mode: ExecutionMode = ExecutionMode.LEGACY
    connector_modes: Mapping[str, ExecutionMode] = field(default_factory=dict)
    capability_modes: Mapping[str, ExecutionMode] = field(default_factory=dict)
    tenant_modes: Mapping[UUID, ExecutionMode] = field(default_factory=dict)
    tenant_connector_modes: Mapping[tuple[UUID, str], ExecutionMode] = field(
        default_factory=dict
    )
    connector_capability_modes: Mapping[tuple[str, str], ExecutionMode] = field(
        default_factory=dict
    )
    tenant_capability_modes: Mapping[tuple[UUID, str, str], ExecutionMode] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        if self.revision < 1:
            raise ValueError("policy revision must be positive")
        for name in (
            "connector_modes",
            "capability_modes",
            "tenant_modes",
            "tenant_connector_modes",
            "connector_capability_modes",
            "tenant_capability_modes",
        ):
            object.__setattr__(
                self,
                name,
                MappingProxyType(dict(getattr(self, name))),
            )

    def resolve(self, request: RouteRequest) -> RouteDecision:
        ordered: tuple[tuple[str, ExecutionMode | None], ...] = (
            (
                "tenant_capability",
                self.tenant_capability_modes.get(
                    (request.tenant_id, request.connector_id, request.capability)
                ),
            ),
            (
                "tenant_connector",
                self.tenant_connector_modes.get(
                    (request.tenant_id, request.connector_id)
                ),
            ),
            (
                "connector_capability",
                self.connector_capability_modes.get(
                    (request.connector_id, request.capability)
                ),
            ),
            ("tenant", self.tenant_modes.get(request.tenant_id)),
            ("connector", self.connector_modes.get(request.connector_id)),
            ("capability", self.capability_modes.get(request.capability)),
            ("global", self.global_mode),
        )
        for scope, mode in ordered:
            if mode is not None:
                return RouteDecision(mode, scope, self.revision)
        raise AssertionError("global routing mode must always resolve")


class AtomicRoutingPolicy:
    """Process-local atomic snapshot holder for immediate config rollback."""

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

    def rollback_to_legacy(self, revision: int) -> None:
        self.replace(RoutingPolicy(revision=revision))

    def replace_quarantine(self, quarantined: Mapping[str, str]) -> None:
        """Atomically install a fail-closed connector admission snapshot."""

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
            return RouteDecision(ExecutionMode.LEGACY, "artifact_quarantine", policy.revision)
        return policy.resolve(request)


__all__ = [
    "AtomicRoutingPolicy",
    "ExecutionMode",
    "RouteDecision",
    "RouteRequest",
    "RoutingPolicy",
]
