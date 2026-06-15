from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from services.reasoning.think.diff_schema import EdgeOp
from services.reasoning.think.edge_semantics import canonicalize_edge_semantics


pytestmark = pytest.mark.asyncio


class _NoRowsConn:
    async def fetch(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []


async def test_edge_semantics_does_not_rewrite_explicit_explains_to_blocks():
    op = EdgeOp(
        op="add",
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="explains",
        explanation=(
            "This explains the composite. It does not block the target and is "
            "not clearly a blocking dependency."
        ),
    )

    out = await canonicalize_edge_semantics(
        op,
        _NoRowsConn(),  # type: ignore[arg-type]
        tenant_id=uuid7(),
    )

    assert out.edge_kind == "explains"
    assert out.metadata is None or "canonicalized_by" not in out.metadata


async def test_edge_semantics_preserves_negated_blocking_language():
    op = EdgeOp(
        op="add",
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="supports",
        explanation=(
            "This does not block the target; it helps explain the customer "
            "risk mechanism."
        ),
    )

    out = await canonicalize_edge_semantics(
        op,
        _NoRowsConn(),  # type: ignore[arg-type]
        tenant_id=uuid7(),
    )

    assert out.edge_kind == "explains"
    assert out.edge_kind != "blocks"


async def test_edge_semantics_does_not_rewrite_analogy_from_endpoint_terms():
    op = EdgeOp(
        op="add",
        source_model_id=uuid7(),
        target_model_id=uuid7(),
        edge_kind="analogous_to",
        explanation=(
            "Both memories are analogous enterprise-review patterns; the edge "
            "does not assert a dependency between them."
        ),
    )

    out = await canonicalize_edge_semantics(
        op,
        _NoRowsConn(),  # type: ignore[arg-type]
        tenant_id=uuid7(),
    )

    assert out.edge_kind == "analogous_to"
    assert out.metadata is None or "canonicalized_by" not in out.metadata
