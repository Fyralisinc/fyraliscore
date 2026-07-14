from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import pytest

from services.platform.access_control.authority import AuthorityDecision, Principal
from services.product.model_trace.repo import supports, trace_back


pytestmark = pytest.mark.asyncio


class _FakeConn:
    def __init__(self, *, tenant_id: UUID, nodes: dict[UUID, dict], edges: list[tuple[UUID, UUID, str]]):
        self.tenant_id = tenant_id
        self.nodes = nodes
        self.edges = edges

    async def fetchrow(self, query: str, *args):
        node_id, tenant_id = args
        if tenant_id != self.tenant_id:
            return None
        return self.nodes.get(node_id)

    async def fetch(self, query: str, *args):
        tenant_id, node_id, kinds = args
        if tenant_id != self.tenant_id:
            return []
        if "source_model_id AS neighbor_id" in query:
            return [
                {"neighbor_id": source, "edge_kind": edge_kind}
                for source, target, edge_kind in self.edges
                if target == node_id and edge_kind in kinds
            ]
        if "target_model_id AS neighbor_id" in query:
            return [
                {"neighbor_id": target, "edge_kind": edge_kind}
                for source, target, edge_kind in self.edges
                if source == node_id and edge_kind in kinds
            ]
        if "JOIN models m ON m.id = e.target_model_id" in query:
            return [
                self.nodes[target] | {"via_edge_kind": edge_kind}
                for source, target, edge_kind in self.edges
                if source == node_id and edge_kind in kinds
            ]
        return []


def _node(model_id: UUID, tenant_id: UUID, natural: str) -> dict:
    return {
        "id": model_id,
        "tenant_id": tenant_id,
        "natural": natural,
        "proposition_kind": "belief",
        "claim_role": "fact",
        "abstraction_level": "atomic",
        "confidence": 0.8,
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "last_confirmed_at": None,
        "resolved_at": None,
        "status": "active",
    }


async def test_trace_back_filters_unauthorized_neighbor_before_chain_pick(monkeypatch):
    tenant = UUID(int=100)
    actor = UUID(int=101)
    secret = UUID(int=1)
    allowed = UUID(int=2)
    seed = UUID(int=3)
    conn = _FakeConn(
        tenant_id=tenant,
        nodes={
            secret: _node(secret, tenant, "Secret finance renewal risk."),
            allowed: _node(allowed, tenant, "Allowed delivery dependency."),
            seed: _node(seed, tenant, "Seed customer situation."),
        },
        edges=[
            (secret, seed, "supports"),
            (allowed, seed, "supports"),
        ],
    )

    async def fake_authorize_read(principal, purpose, object_ref, *, conn):
        if object_ref.object_id == secret:
            return AuthorityDecision(False, "model_out_of_scope")
        return AuthorityDecision(True, "authorized")

    monkeypatch.setattr(
        "services.product.model_trace.repo.authorize_read",
        fake_authorize_read,
    )

    chain = await trace_back(
        conn,  # type: ignore[arg-type]
        tenant,
        seed,
        max_depth=1,
        principal=Principal(tenant_id=tenant, actor_id=actor),
    )

    assert [step.id for step in chain] == [seed, allowed]


async def test_supports_filters_unauthorized_targets(monkeypatch):
    tenant = UUID(int=200)
    actor = UUID(int=201)
    seed = UUID(int=202)
    secret = UUID(int=203)
    allowed = UUID(int=204)
    conn = _FakeConn(
        tenant_id=tenant,
        nodes={
            seed: _node(seed, tenant, "Seed operational pattern."),
            secret: _node(secret, tenant, "Secret finance consequence."),
            allowed: _node(allowed, tenant, "Allowed support consequence."),
        },
        edges=[
            (seed, secret, "supports"),
            (seed, allowed, "supports"),
        ],
    )

    async def fake_authorize_read(principal, purpose, object_ref, *, conn):
        if object_ref.object_id == secret:
            return AuthorityDecision(False, "model_out_of_scope")
        return AuthorityDecision(True, "authorized")

    monkeypatch.setattr(
        "services.product.model_trace.repo.authorize_read",
        fake_authorize_read,
    )

    items = await supports(
        conn,  # type: ignore[arg-type]
        tenant,
        seed,
        principal=Principal(tenant_id=tenant, actor_id=actor),
    )

    assert [item.id for item in items] == [allowed]
