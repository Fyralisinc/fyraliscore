"""Runtime edge-kind semantics for accepted ontology proposals."""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.edge_registry import (
    EdgeKindSpec,
    EdgeRegistryError,
    assert_writable,
    get_spec,
)


async def resolve_edge_kind_spec(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    kind: str,
) -> EdgeKindSpec:
    """Return static or accepted dynamic edge-kind semantics."""

    try:
        return assert_writable(kind)
    except EdgeRegistryError as original_error:
        proposal = await _load_accepted_proposal(
            conn,
            tenant_id=tenant_id,
            kind=kind,
        )
        if proposal is None:
            raise original_error
        return _spec_from_proposal(kind, proposal)


async def is_edge_kind_writable(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    kind: str,
) -> bool:
    try:
        await resolve_edge_kind_spec(conn, tenant_id=tenant_id, kind=kind)
    except EdgeRegistryError:
        return False
    return True


def validate_weight_for_spec(spec: EdgeKindSpec, weight: float | None) -> None:
    if weight is None:
        if spec.weight_required:
            raise EdgeRegistryError(
                f"edge_kind {spec.name!r} requires a weight; got None"
            )
        return
    if not spec.weight_allowed:
        raise EdgeRegistryError(
            f"edge_kind {spec.name!r} forbids weight; got {weight}"
        )
    try:
        w = float(weight)
    except (TypeError, ValueError) as exc:
        raise EdgeRegistryError(
            f"edge_kind {spec.name!r} weight must be numeric; got {weight!r}"
        ) from exc
    if not (0.0 <= w <= 1.0):
        raise EdgeRegistryError(
            f"edge_kind {spec.name!r} weight out of range [0, 1]: {w}"
        )


async def _load_accepted_proposal(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    kind: str,
) -> dict[str, Any] | None:
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.relationship_ontology_proposals')"
    )
    if table_name is None:
        return None
    row = await conn.fetchrow(
        """
        SELECT proposed_edge_kind, parent_kind, nearest_existing_kind,
               retrieval_fallback_kind, directionality
        FROM relationship_ontology_proposals
        WHERE tenant_id = $1
          AND proposed_edge_kind = $2
          AND status = 'accepted'
        """,
        tenant_id,
        kind,
    )
    return {k: row[k] for k in row.keys()} if row is not None else None


def _spec_from_proposal(kind: str, proposal: dict[str, Any]) -> EdgeKindSpec:
    fallback = (
        proposal.get("retrieval_fallback_kind")
        or proposal.get("nearest_existing_kind")
        or proposal.get("parent_kind")
        or "supports"
    )
    try:
        base = get_spec(str(fallback))
    except EdgeRegistryError:
        base = get_spec("supports")
    directionality = str(proposal.get("directionality") or "unknown")
    if directionality == "directed":
        is_directed = True
    elif directionality == "symmetric":
        is_directed = False
    else:
        is_directed = base.is_directed
    cycle_scope = (
        frozenset({kind, *base.cycle_scope})
        if base.cycle_scope is not None
        else None
    )
    return EdgeKindSpec(
        name=kind,
        is_directed=is_directed,
        cycle_scope=cycle_scope,
        weight_required=base.weight_required,
        weight_allowed=base.weight_allowed,
        on_source_archive=None,
        on_target_archive=None,
        mutually_exclusive_with=base.mutually_exclusive_with,
        enabled_for_writes=True,
    )


__all__ = [
    "is_edge_kind_writable",
    "resolve_edge_kind_spec",
    "validate_weight_for_spec",
]

