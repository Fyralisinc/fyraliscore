from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.connector_runtime.policy import (
    AtomicRoutingPolicy,
    ExecutionMode,
    RouteRequest,
    RoutingPolicy,
)


def test_policy_defaults_to_legacy_and_uses_narrowest_override() -> None:
    tenant_id = uuid4()
    request = RouteRequest(
        tenant_id=tenant_id,
        connector_id="fyralis/slack",
        source="slack",
        capability="semantic.identity",
    )
    policy = RoutingPolicy(
        revision=4,
        global_mode=ExecutionMode.SHADOW,
        connector_modes={"fyralis/slack": ExecutionMode.CONNECTOR},
        tenant_capability_modes={
            (tenant_id, "fyralis/slack", "semantic.identity"): ExecutionMode.LEGACY
        },
    )

    decision = policy.resolve(request)

    assert decision.mode is ExecutionMode.LEGACY
    assert decision.matched_scope == "tenant_capability"
    assert RoutingPolicy().resolve(request).mode is ExecutionMode.LEGACY


def test_atomic_policy_replacement_supports_immediate_rollback() -> None:
    tenant_id = uuid4()
    request = RouteRequest(tenant_id, "fyralis/slack", "slack", "pull")
    holder = AtomicRoutingPolicy(
        RoutingPolicy(revision=1, global_mode=ExecutionMode.CONNECTOR)
    )

    holder.rollback_to_legacy(revision=2)

    assert holder.resolve(request).mode is ExecutionMode.LEGACY
    with pytest.raises(ValueError):
        holder.replace(RoutingPolicy(revision=2))
