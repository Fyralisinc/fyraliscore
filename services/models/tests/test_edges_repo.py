"""
services/models/tests/test_edges_repo.py — integration tests for the
unified Model-to-Model edge primitive (S1, migration 0031).

Coverage matrix (Stage 0 acceptance gate):
  * link / unlink basic + idempotency
  * traverse_forward / traverse_backward
  * mark_inert on archive
  * cycle check across cycle_scope
  * dual-write convergence (array + edges in lockstep)
  * pattern promotion writes both `instance_of` edge AND
    legacy back-link in supporting_model_ids
  * symmetric edge_kind: skipped in v1 (`contradicts` is reserved
    and rejected at the repo layer)

All tests use the conftest's `tx_conn` fixture (transaction rolled
back at teardown) and tenant UUID isolation.
"""
from __future__ import annotations

import asyncio
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.edge_registry import EdgeRegistryError
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.models.edges_repo import EdgesRepo


# ---------------------------------------------------------------------
# Helpers — minimal Model row builder for edge tests.
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def make_model(tx_conn, tenant, born_from_event, embedding):
    """Insert a minimal Model row directly via SQL (bypasses ModelsRepo
    so we can stage many Models without spinning the full 9-step
    pipeline). Returns a callable that creates one Model and yields
    its id."""
    import json
    async def _make(natural: str = "test model") -> UUID:
        mid = uuid7()
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, falsifier, signal_readings,
                supporting_event_ids, supporting_model_ids,
                contributing_models, status,
                confidence_at_assertion
            ) VALUES (
                $1, $2, $3,
                '{"kind":"state","subject":"x","assertion":"y"}'::jsonb,
                $4, $5,
                '{}'::uuid[], '[]'::jsonb,
                '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
                0.6, NULL, '[]'::jsonb,
                '{}'::uuid[], '{}'::uuid[],
                '{}'::uuid[], 'active',
                0.6
            )
            """,
            mid, tenant, born_from_event,
            natural, embedding,
        )
        return mid
    return _make


# ---------------------------------------------------------------------
# link / unlink basics
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_link_supports_inserts_one_row(tx_conn, tenant, make_model):
    a = await make_model("A")
    b = await make_model("B")
    repo = EdgesRepo()
    ids = await repo.link(
        tx_conn,
        source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    assert len(ids) == 1
    row = await tx_conn.fetchrow(
        "SELECT * FROM model_edges WHERE id = $1", ids[0]
    )
    assert row["source_model_id"] == a
    assert row["target_model_id"] == b
    assert row["edge_kind"] == "supports"
    assert row["status"] == "active"
    assert row["detected_by"] == "manual"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_link_idempotent(tx_conn, tenant, make_model):
    """A second link() with the same (source, target, kind) returns
    the existing edge id, no second row inserted."""
    a = await make_model("A")
    b = await make_model("B")
    repo = EdgesRepo()
    ids1 = await repo.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    ids2 = await repo.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    assert ids1 == ids2
    count = await tx_conn.fetchval(
        """
        SELECT count(*) FROM model_edges
        WHERE source_model_id = $1 AND target_model_id = $2
          AND edge_kind = 'supports'
        """,
        a, b,
    )
    assert count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_link_rejects_self_edge(tx_conn, tenant, make_model):
    a = await make_model("A")
    repo = EdgesRepo()
    with pytest.raises(ValidationError):
        await repo.link(
            tx_conn, source=a, target=a, kind="supports",
            tenant_id=tenant, detected_by="manual",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_link_rejects_reserved_kind(tx_conn, tenant, make_model):
    """contradicts is reserved in v1; repo refuses to write it
    until a producer ships."""
    a = await make_model("A")
    b = await make_model("B")
    repo = EdgesRepo()
    with pytest.raises(EdgeRegistryError) as exc:
        await repo.link(
            tx_conn, source=a, target=b, kind="contradicts",
            tenant_id=tenant, detected_by="manual",
            weight=0.5,  # required for contradicts
        )
    assert "reserved" in str(exc.value)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unlink_removes_edge(tx_conn, tenant, make_model):
    a = await make_model("A")
    b = await make_model("B")
    repo = EdgesRepo()
    await repo.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    n = await repo.unlink(
        tx_conn, source=a, target=b, kind="supports", tenant_id=tenant,
    )
    assert n == 1
    count = await tx_conn.fetchval(
        "SELECT count(*) FROM model_edges WHERE source_model_id = $1",
        a,
    )
    assert count == 0


# ---------------------------------------------------------------------
# Traversal (forward + backward)
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_traverse_forward_returns_targets(tx_conn, tenant, make_model):
    a = await make_model("A")
    b = await make_model("B")
    c = await make_model("C")
    repo = EdgesRepo()
    await repo.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await repo.link(
        tx_conn, source=a, target=c, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    out = await repo.traverse_forward(
        tx_conn, source=a, kinds=["supports"], tenant_id=tenant,
    )
    targets = {e["target_model_id"] for e in out}
    assert targets == {b, c}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_traverse_backward_is_new_capability(tx_conn, tenant, make_model):
    """The whole point of S1: 'what depends on X?' is now O(log n)
    for every kind, not just `supports` inside the archive cascade."""
    a = await make_model("A")
    b = await make_model("B")
    c = await make_model("C")
    repo = EdgesRepo()
    await repo.link(
        tx_conn, source=a, target=c, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await repo.link(
        tx_conn, source=b, target=c, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    out = await repo.traverse_backward(
        tx_conn, target=c, kinds=["supports"], tenant_id=tenant,
    )
    sources = {e["source_model_id"] for e in out}
    assert sources == {a, b}


# ---------------------------------------------------------------------
# Cycle check
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cycle_check_rejects_direct_cycle(tx_conn, tenant, make_model):
    """A → B → A would close a cycle in the supports DAG."""
    a = await make_model("A")
    b = await make_model("B")
    repo = EdgesRepo()
    await repo.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    with pytest.raises(ValidationError) as exc:
        await repo.link(
            tx_conn, source=b, target=a, kind="supports",
            tenant_id=tenant, detected_by="manual",
        )
    assert "cycle" in str(exc.value)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cycle_check_rejects_transitive_cycle(tx_conn, tenant, make_model):
    """A → B → C → A also closes a cycle (transitive)."""
    a = await make_model("A")
    b = await make_model("B")
    c = await make_model("C")
    repo = EdgesRepo()
    await repo.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await repo.link(
        tx_conn, source=b, target=c, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    with pytest.raises(ValidationError):
        await repo.link(
            tx_conn, source=c, target=a, kind="supports",
            tenant_id=tenant, detected_by="manual",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cycle_check_crosses_supports_and_instance_of(
    tx_conn, tenant, make_model,
):
    """instance_of and supports share a cycle scope; A supports B
    via instance_of, then B supports A via supports → cycle."""
    a = await make_model("A")
    b = await make_model("B")
    repo = EdgesRepo()
    await repo.link(
        tx_conn, source=a, target=b, kind="instance_of",
        tenant_id=tenant, detected_by="manual",
    )
    with pytest.raises(ValidationError):
        await repo.link(
            tx_conn, source=b, target=a, kind="supports",
            tenant_id=tenant, detected_by="manual",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_cycle_check_does_not_cross_unrelated_scopes(
    tx_conn, tenant, make_model,
):
    """contributes_to_resolution and supports are SEPARATE cycle
    scopes — a contributes-to-resolution edge in one direction
    must NOT block a supports edge in the other."""
    a = await make_model("A")
    b = await make_model("B")
    repo = EdgesRepo()
    await repo.link(
        tx_conn, source=a, target=b, kind="contributes_to_resolution",
        tenant_id=tenant, detected_by="manual",
    )
    # supports cycle scope is {supports, instance_of}, doesn't see
    # the contributes edge — link must succeed.
    await repo.link(
        tx_conn, source=b, target=a, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )


# ---------------------------------------------------------------------
# mark_inert
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mark_inert_flips_status_both_directions(
    tx_conn, tenant, make_model,
):
    """When a Model is archived, every edge it appears in
    (source or target) flips to status='inert'."""
    a = await make_model("A")
    b = await make_model("B")
    c = await make_model("C")
    repo = EdgesRepo()
    await repo.link(
        tx_conn, source=a, target=b, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    await repo.link(
        tx_conn, source=c, target=a, kind="supports",
        tenant_id=tenant, detected_by="manual",
    )
    flipped = await repo.mark_inert(
        tx_conn, model_id=a, tenant_id=tenant,
        reason="endpoint_archived",
    )
    # Both edges touched A.
    assert len(flipped) == 2
    rows = await tx_conn.fetch(
        "SELECT status, status_reason FROM model_edges "
        "WHERE source_model_id = $1 OR target_model_id = $1",
        a,
    )
    for r in rows:
        assert r["status"] == "inert"
        assert r["status_reason"] == "endpoint_archived"


# ---------------------------------------------------------------------
# Dual-write convergence — _set_model_relations chokepoint
# ---------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dual_write_convergence_supports(
    tx_conn, tenant, make_model,
):
    """After _set_model_relations runs, the supporting_model_ids
    array and the typed `supports` edges agree."""
    from services.models.repo import _set_model_relations
    a = await make_model("supporter1")
    b = await make_model("supporter2")
    m = await make_model("M")
    await _set_model_relations(
        tx_conn,
        model_id=m, tenant_id=tenant,
        detected_by="manual",
        supports=[a, b],
    )
    array_after = await tx_conn.fetchval(
        "SELECT supporting_model_ids FROM models WHERE id = $1", m,
    )
    edge_sources = await tx_conn.fetch(
        """
        SELECT source_model_id FROM model_edges
        WHERE target_model_id = $1 AND edge_kind = 'supports'
          AND status = 'active'
        """,
        m,
    )
    assert set(array_after) == {a, b}
    assert {r["source_model_id"] for r in edge_sources} == {a, b}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dual_write_diff_removes_dropped_supporter(
    tx_conn, tenant, make_model,
):
    """If a supporter is removed from the array on a subsequent
    _set_model_relations call, the corresponding edge must be
    DELETEd (not just left dangling)."""
    from services.models.repo import _set_model_relations
    a = await make_model("supporter1")
    b = await make_model("supporter2")
    m = await make_model("M")
    await _set_model_relations(
        tx_conn,
        model_id=m, tenant_id=tenant,
        detected_by="manual",
        supports=[a, b],
    )
    # Now drop b.
    await _set_model_relations(
        tx_conn,
        model_id=m, tenant_id=tenant,
        detected_by="manual",
        supports=[a],
    )
    edge_sources = await tx_conn.fetch(
        """
        SELECT source_model_id FROM model_edges
        WHERE target_model_id = $1 AND edge_kind = 'supports'
          AND status = 'active'
        """,
        m,
    )
    assert {r["source_model_id"] for r in edge_sources} == {a}
    array_after = await tx_conn.fetchval(
        "SELECT supporting_model_ids FROM models WHERE id = $1", m,
    )
    assert set(array_after) == {a}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dual_write_instance_of_writes_outgoing_edge(
    tx_conn, tenant, make_model,
):
    """instance_of edges go FROM the constituent TO the pattern (the
    semantically correct direction). Pattern back-link in
    supporting_model_ids is preserved (legacy mixed semantics)."""
    from services.models.repo import _set_model_relations
    constituent = await make_model("constituent")
    pattern = await make_model("pattern")
    await _set_model_relations(
        tx_conn,
        model_id=constituent, tenant_id=tenant,
        detected_by="precipitation",
        instance_of=[pattern],
    )
    # Typed edge: source=constituent, target=pattern.
    edge_targets = await tx_conn.fetch(
        """
        SELECT target_model_id FROM model_edges
        WHERE source_model_id = $1 AND edge_kind = 'instance_of'
          AND status = 'active'
        """,
        constituent,
    )
    assert {r["target_model_id"] for r in edge_targets} == {pattern}
    # Legacy back-link: pattern id appears in constituent's
    # supporting_model_ids.
    array_after = await tx_conn.fetchval(
        "SELECT supporting_model_ids FROM models WHERE id = $1",
        constituent,
    )
    assert pattern in array_after


@pytest.mark.integration
@pytest.mark.asyncio
async def test_superseded_by_writes_singleton_edge(
    tx_conn, tenant, make_model,
):
    """superseded_by has no array column — the edge is purely
    additive."""
    from services.models.repo import _set_model_relations
    old = await make_model("old")
    replacement = await make_model("new")
    await _set_model_relations(
        tx_conn,
        model_id=old, tenant_id=tenant,
        detected_by="manual",
        superseded_by=replacement,
    )
    edge = await tx_conn.fetchrow(
        """
        SELECT * FROM model_edges
        WHERE source_model_id = $1 AND target_model_id = $2
          AND edge_kind = 'superseded_by'
        """,
        old, replacement,
    )
    assert edge is not None
    assert edge["status"] == "active"
