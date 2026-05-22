"""services/model_trace/tests/test_repo.py — integration tests for the
trace walks (back / forward) and adjacency helpers (supports / depends_on).

The tests stage small evidence graphs and assert the walk surfaces the
expected chain. Sparse / missing edges return either a single-seed chain
or empty adjacency without raising.
"""
from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from services.model_trace.repo import (
    depends_on,
    supports,
    trace_back,
    trace_forward,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trace_back_single_seed_when_no_edges(
    tx_conn, tenant, make_model,
):
    """No inbound evidence → chain is just the seed step."""
    node = await make_model("isolated belief", kind="state")
    chain = await trace_back(tx_conn, tenant, node, max_depth=4)
    assert len(chain) == 1
    assert chain[0].id == node
    assert chain[0].via_edge_kind is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trace_back_missing_node_returns_empty(tx_conn, tenant):
    """Seed node not in tenant → empty chain (no raise)."""
    missing = uuid7()
    chain = await trace_back(tx_conn, tenant, missing, max_depth=4)
    assert chain == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trace_back_walks_supports_chain(
    tx_conn, tenant, make_model, link_edge,
):
    """A→supports→B→supports→C: trace_back(C) returns [C, B, A]."""
    a = await make_model("a-leaf", kind="state")
    b = await make_model("b-mid", kind="state")
    c = await make_model("c-top", kind="state")
    await link_edge(a, b, kind="supports")
    await link_edge(b, c, kind="supports")
    chain = await trace_back(tx_conn, tenant, c, max_depth=4)
    ids = [s.id for s in chain]
    assert ids == [c, b, a]
    assert chain[1].via_edge_kind == "supports"
    assert chain[2].via_edge_kind == "supports"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trace_forward_walks_supports_chain(
    tx_conn, tenant, make_model, link_edge,
):
    """A→supports→B→supports→C: trace_forward(A) returns [A, B, C]."""
    a = await make_model("a-leaf", kind="state")
    b = await make_model("b-mid", kind="state")
    c = await make_model("c-top", kind="state")
    await link_edge(a, b, kind="supports")
    await link_edge(b, c, kind="supports")
    chain = await trace_forward(tx_conn, tenant, a, max_depth=4)
    ids = [s.id for s in chain]
    assert ids == [a, b, c]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trace_back_max_depth_caps_chain(
    tx_conn, tenant, make_model, link_edge,
):
    """max_depth=1 → chain length capped at 2 (seed + one hop)."""
    a = await make_model("a", kind="state")
    b = await make_model("b", kind="state")
    c = await make_model("c", kind="state")
    await link_edge(a, b, kind="supports")
    await link_edge(b, c, kind="supports")
    chain = await trace_back(tx_conn, tenant, c, max_depth=1)
    assert len(chain) == 2
    assert chain[0].id == c
    assert chain[1].id == b


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trace_back_follows_instance_of_outgoing(
    tx_conn, tenant, make_model, link_edge,
):
    """instance_of: source(instance) → target(pattern). trace_back on
    the instance should walk OUTGOING instance_of to surface the
    pattern as upstream."""
    pattern = await make_model("the pattern", kind="pattern")
    instance = await make_model("an instance", kind="pattern_instance")
    await link_edge(instance, pattern, kind="instance_of")
    chain = await trace_back(tx_conn, tenant, instance, max_depth=4)
    assert [s.id for s in chain] == [instance, pattern]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trace_skips_inert_edges(
    tx_conn, tenant, make_model, link_edge,
):
    """Inert edges must not appear in trace walks."""
    a = await make_model("a", kind="state")
    b = await make_model("b", kind="state")
    await link_edge(a, b, kind="supports")
    # Flip to inert.
    await tx_conn.execute(
        "UPDATE model_edges SET status='inert' WHERE tenant_id = $1",
        tenant,
    )
    chain = await trace_back(tx_conn, tenant, b, max_depth=4)
    assert [s.id for s in chain] == [b]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_supports_returns_one_hop_downstream(
    tx_conn, tenant, make_model, link_edge,
):
    """`supports` adjacency: node → things it supports."""
    a = await make_model("a", kind="state")
    b = await make_model("b", kind="state")
    c = await make_model("c", kind="state")
    await link_edge(a, b, kind="supports")
    await link_edge(a, c, kind="contributes_to_resolution")
    items = await supports(tx_conn, tenant, a)
    got = {s.id for s in items}
    assert got == {b, c}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_depends_on_returns_one_hop_upstream(
    tx_conn, tenant, make_model, link_edge,
):
    """`depends_on` adjacency: node ← things it depends on."""
    a = await make_model("a", kind="state")
    b = await make_model("b", kind="state")
    pattern = await make_model("p", kind="pattern")
    await link_edge(b, a, kind="supports")          # b supports a
    await link_edge(a, pattern, kind="instance_of")  # a is instance of pattern
    items = await depends_on(tx_conn, tenant, a)
    got = {s.id for s in items}
    assert got == {b, pattern}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_trace_back_tenant_isolated(
    tx_conn, tenant, make_model, link_edge,
):
    """trace_back filters by tenant: other-tenant's edges are invisible."""
    a = await make_model("a", kind="state")
    b = await make_model("b", kind="state")
    await link_edge(a, b, kind="supports")
    other_tenant = uuid7()
    chain = await trace_back(tx_conn, other_tenant, b, max_depth=4)
    assert chain == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_to_dict_serialisation(tx_conn, tenant, make_model):
    """TraceStep.to_dict returns JSON-friendly shape with required keys."""
    node = await make_model("hello", kind="state")
    chain = await trace_back(tx_conn, tenant, node, max_depth=2)
    assert len(chain) == 1
    d = chain[0].to_dict()
    assert d["id"] == str(node)
    assert d["kind"] == "claim"
    assert d["label"]
    assert "summary" in d
    assert "ts" in d
    assert d["via_edge_kind"] is None
