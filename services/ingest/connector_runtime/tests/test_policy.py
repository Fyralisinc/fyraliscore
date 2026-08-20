from uuid import uuid4

import pytest

from services.ingest.connector_runtime.policy import (
    AtomicRoutingPolicy,
    ExecutionMode,
    RouteRequest,
    RoutingPolicy,
)
from services.ingest.source_contract.errors import SourceUnavailableError


def _request() -> RouteRequest:
    return RouteRequest(uuid4(), "fyralis/slack", "slack", "semantic.identity")


def test_policy_has_one_contract_execution_mode() -> None:
    decision = RoutingPolicy(revision=4).resolve(_request())
    assert decision.mode is ExecutionMode.CONNECTOR
    assert decision.matched_scope == "global"
    assert decision.policy_revision == 4


def test_atomic_policy_replacement_is_monotonic() -> None:
    holder = AtomicRoutingPolicy(RoutingPolicy(revision=1))
    holder.replace(RoutingPolicy(revision=2))
    assert holder.resolve(_request()).policy_revision == 2
    with pytest.raises(ValueError, match="increase"):
        holder.replace(RoutingPolicy(revision=2))


def test_quarantined_connector_fails_closed() -> None:
    holder = AtomicRoutingPolicy()
    holder.replace_quarantine({"fyralis/slack": "signature invalid"})
    with pytest.raises(SourceUnavailableError, match="quarantined"):
        holder.resolve(_request())
