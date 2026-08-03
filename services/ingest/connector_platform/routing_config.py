"""Parse and atomically apply connector routing configuration."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

from services.ingest.connector_runtime.policy import (
    AtomicRoutingPolicy,
    ExecutionMode,
    RoutingPolicy,
)


def _mode(value: Any) -> ExecutionMode:
    return ExecutionMode(str(value).lower())


def _mapping_modes(value: Any) -> dict[str, ExecutionMode]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("routing override must be a JSON object")
    return {str(key): _mode(mode) for key, mode in value.items()}


def _rows(value: Any, *fields: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("scoped routing overrides must be arrays of objects")
    for item in value:
        missing = set(fields) - set(item)
        if missing:
            raise ValueError(
                f"scoped routing override is missing {tuple(sorted(missing))}"
            )
    return value


def parse_routing_policy(
    value: str | Mapping[str, Any] | None,
    *,
    fallback_revision: int = 1,
) -> RoutingPolicy:
    if value is None or value == "":
        return RoutingPolicy(revision=fallback_revision)
    data = json.loads(value) if isinstance(value, str) else dict(value)
    revision = int(data.get("revision", fallback_revision))
    tenant_modes = {
        UUID(str(key)): mode
        for key, mode in _mapping_modes(data.get("tenants")).items()
    }
    tenant_connector_modes = {
        (UUID(str(item["tenant_id"])), str(item["connector_id"])): _mode(
            item["mode"]
        )
        for item in _rows(
            data.get("tenant_connectors"), "tenant_id", "connector_id", "mode"
        )
    }
    connector_capability_modes = {
        (str(item["connector_id"]), str(item["capability"])): _mode(item["mode"])
        for item in _rows(
            data.get("connector_capabilities"),
            "connector_id",
            "capability",
            "mode",
        )
    }
    tenant_capability_modes = {
        (
            UUID(str(item["tenant_id"])),
            str(item["connector_id"]),
            str(item["capability"]),
        ): _mode(item["mode"])
        for item in _rows(
            data.get("tenant_capabilities"),
            "tenant_id",
            "connector_id",
            "capability",
            "mode",
        )
    }
    return RoutingPolicy(
        revision=revision,
        global_mode=_mode(data.get("global", "legacy")),
        connector_modes=_mapping_modes(data.get("connectors")),
        capability_modes=_mapping_modes(data.get("capabilities")),
        tenant_modes=tenant_modes,
        tenant_connector_modes=tenant_connector_modes,
        connector_capability_modes=connector_capability_modes,
        tenant_capability_modes=tenant_capability_modes,
    )


class RoutingConfigurationController:
    """Apply watched configuration to the live process snapshot."""

    def __init__(self, routing: AtomicRoutingPolicy) -> None:
        self._routing = routing

    def snapshot(self) -> RoutingPolicy:
        return self._routing.snapshot()

    def apply(self, value: str | Mapping[str, Any]) -> RoutingPolicy:
        next_revision = self._routing.snapshot().revision + 1
        policy = parse_routing_policy(value, fallback_revision=next_revision)
        self._routing.replace(policy)
        return policy

    def rollback(self) -> RoutingPolicy:
        policy = RoutingPolicy(revision=self._routing.snapshot().revision + 1)
        self._routing.replace(policy)
        return policy


__all__ = [
    "RoutingConfigurationController",
    "parse_routing_policy",
]
