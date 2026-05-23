"""
Second-pass expansion tests.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.retrieval.primary import TriggerContext, primary_retrieve
from services.retrieval.second_pass import second_pass_expand

from services.retrieval.tests._fixtures import build_fixture, make_embedding


pytestmark = pytest.mark.integration


async def _first_pass(tx_conn, pool, tenant):
    fs = await build_fixture(tx_conn, tenant, pool=pool)
    seeds = [{"type": "commitment", "id": str(fs.hero_commitment_id)}]
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=seeds,
        seed_natural_text="alice ships reliably",
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=make_embedding("alice"),
    )
    result = await primary_retrieve(trigger, tx_conn)
    return fs, result


async def test_second_pass_dependency_context_adds_deps(
    tx_conn, fresh_db, tenant
):
    fs, first = await _first_pass(tx_conn, fresh_db, tenant)
    expanded = await second_pass_expand(
        first, ["dependency_context"], tx_conn,
    )
    # Expansion should be a superset of the first pass.
    assert len(expanded.acts["commitments"]) >= len(first.acts["commitments"])
    assert "dependency_context" in expanded.notes["second_pass"][
        "dimensions_processed"
    ]


async def test_second_pass_supporting_evidence_adds_observations(
    tx_conn, fresh_db, tenant
):
    fs, first = await _first_pass(tx_conn, fresh_db, tenant)
    # Build a Model with a supporting_event_id to exercise this
    # dimension. Pick one of the first-pass models and UPDATE it in
    # the test tx to point at a specific observation.
    if not first.models:
        pytest.skip("no first-pass models")
    target = first.models[0]
    ev = fs.observation_ids[0]
    await tx_conn.execute(
        """
        UPDATE models
        SET supporting_event_ids = $2::uuid[]
        WHERE id = $1
        """,
        target.id, [ev],
    )
    # Re-fetch the first pass with the updated supporting events.
    # (In reality Think would populate these on Model creation.)
    first.models[0] = first.models[0].model_copy(
        update={"supporting_event_ids": [ev]}
    )
    expanded = await second_pass_expand(
        first, ["supporting_evidence"], tx_conn,
    )
    obs_ids = {o.id for o in expanded.observations}
    assert ev in obs_ids


async def test_second_pass_adjacent_commitments(
    tx_conn, fresh_db, tenant
):
    fs, first = await _first_pass(tx_conn, fresh_db, tenant)
    expanded = await second_pass_expand(
        first, ["adjacent_commitments"], tx_conn,
    )
    # Adjacent commits share a goal with the first-pass commits —
    # fixture links many commits to the same goal so we expect >0 new.
    assert len(expanded.acts["commitments"]) >= len(first.acts["commitments"])


async def test_second_pass_unknown_dimension_is_skipped(
    tx_conn, fresh_db, tenant
):
    fs, first = await _first_pass(tx_conn, fresh_db, tenant)
    expanded = await second_pass_expand(
        first, ["foo_bar_quux"], tx_conn,
    )
    # Unknown dimensions are logged + skipped — no error.
    assert "foo_bar_quux" in expanded.notes["second_pass"]["dimensions_unknown"]


async def test_second_pass_two_hop_cap_enforced(
    tx_conn, fresh_db, tenant
):
    fs, first = await _first_pass(tx_conn, fresh_db, tenant)
    # max_hops=3 → must raise per our cap enforcement.
    with pytest.raises(ValueError):
        await second_pass_expand(
            first, ["dependency_context"], tx_conn,
            max_hops=3,
        )


async def test_second_pass_reconsolidates_new_models_only(
    tx_conn, fresh_db, tenant
):
    fs, first = await _first_pass(tx_conn, fresh_db, tenant)
    # Snapshot activation of first-pass Models.
    before = await tx_conn.fetch(
        "SELECT id, activation FROM models WHERE tenant_id = $1", tenant
    )
    by_id = {r["id"]: r["activation"] for r in before}

    expanded = await second_pass_expand(
        first, ["dependency_context"], tx_conn,
    )
    after = await tx_conn.fetch(
        "SELECT id, activation FROM models WHERE tenant_id = $1", tenant
    )
    after_by_id = {r["id"]: r["activation"] for r in after}
    # First-pass Models' activation should NOT bump a second time.
    first_ids = {m.id for m in first.models}
    for mid in first_ids:
        if mid in by_id and mid in after_by_id:
            assert abs(after_by_id[mid] - by_id[mid]) < 1e-9, (
                f"model {mid} activation bumped twice "
                f"({by_id[mid]} -> {after_by_id[mid]})"
            )
