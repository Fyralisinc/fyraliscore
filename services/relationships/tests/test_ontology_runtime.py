from __future__ import annotations

from uuid import UUID

import pytest

from lib.shared.edge_registry import EdgeRegistryError
from lib.shared.ids import uuid7
from services.relationships.ontology_runtime import (
    is_edge_kind_writable,
    resolve_edge_kind_spec,
    validate_weight_for_spec,
)


class _Conn:
    def __init__(self, tenant_id: UUID, proposal: dict | None = None) -> None:
        self.tenant_id = tenant_id
        self.proposal = proposal

    async def fetchval(self, query, *args):
        assert "to_regclass" in query
        return "relationship_ontology_proposals"

    async def fetchrow(self, query, *args):
        tenant_id, kind = args
        if tenant_id == self.tenant_id and self.proposal:
            if self.proposal["proposed_edge_kind"] == kind:
                return _Record(self.proposal)
        return None


class _Record(dict):
    def keys(self):
        return super().keys()


@pytest.mark.asyncio
async def test_resolve_edge_kind_spec_accepts_static_registry_kind() -> None:
    spec = await resolve_edge_kind_spec(
        _Conn(uuid7()),
        tenant_id=uuid7(),
        kind="blocks",
    )

    assert spec.name == "blocks"


@pytest.mark.asyncio
async def test_resolve_edge_kind_spec_derives_accepted_dynamic_kind() -> None:
    tenant_id = uuid7()
    spec = await resolve_edge_kind_spec(
        _Conn(
            tenant_id,
            {
                "proposed_edge_kind": "gated_by_decision",
                "parent_kind": "blocks",
                "nearest_existing_kind": "blocks",
                "retrieval_fallback_kind": "blocks",
                "directionality": "directed",
            },
        ),
        tenant_id=tenant_id,
        kind="gated_by_decision",
    )

    assert spec.name == "gated_by_decision"
    assert spec.is_directed is True
    assert spec.weight_allowed is True
    assert "enables" in spec.mutually_exclusive_with
    validate_weight_for_spec(spec, 0.5)


@pytest.mark.asyncio
async def test_unaccepted_dynamic_kind_is_not_writable() -> None:
    tenant_id = uuid7()

    assert await is_edge_kind_writable(
        _Conn(tenant_id),
        tenant_id=tenant_id,
        kind="gated_by_decision",
    ) is False
    with pytest.raises(EdgeRegistryError):
        await resolve_edge_kind_spec(
            _Conn(tenant_id),
            tenant_id=tenant_id,
            kind="gated_by_decision",
        )

