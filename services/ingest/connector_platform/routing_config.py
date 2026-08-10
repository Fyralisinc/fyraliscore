"""Parse and atomically apply contract-only connector revisions."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from services.ingest.connector_runtime.policy import AtomicRoutingPolicy, RoutingPolicy


_RETIRED_ROUTING_KEYS = frozenset(
    {
        "connectors",
        "capabilities",
        "tenants",
        "tenant_connectors",
        "connector_capabilities",
        "tenant_capabilities",
    }
)


def parse_routing_policy(
    value: str | Mapping[str, Any] | None,
    *,
    fallback_revision: int = 1,
) -> RoutingPolicy:
    """Parse the revision envelope used to propagate connector artifacts.

    Source-specific execution modes were intentionally removed. Every admitted
    source executes through its connector, so this configuration carries only
    the monotonically increasing control-plane revision.
    """

    if value is None or value == "":
        return RoutingPolicy(revision=fallback_revision)
    data = json.loads(value) if isinstance(value, str) else dict(value)
    retired = sorted(_RETIRED_ROUTING_KEYS.intersection(data))
    if retired:
        raise ValueError(
            "source-specific routing overrides are retired: " + ", ".join(retired)
        )
    mode = str(data.get("global", "connector")).lower()
    if mode != "connector":
        raise ValueError("only contract connector execution is supported")
    return RoutingPolicy(revision=int(data.get("revision", fallback_revision)))


class RoutingConfigurationController:
    """Apply watched connector revisions to the live process snapshot."""

    def __init__(self, routing: AtomicRoutingPolicy) -> None:
        self._routing = routing

    def snapshot(self) -> RoutingPolicy:
        return self._routing.snapshot()

    def apply(self, value: str | Mapping[str, Any]) -> RoutingPolicy:
        next_revision = self._routing.snapshot().revision + 1
        policy = parse_routing_policy(value, fallback_revision=next_revision)
        self._routing.replace(policy)
        return policy


__all__ = ["RoutingConfigurationController", "parse_routing_policy"]
