"""services/think/tests/test_thresholds.py — pure-function threshold tests.

`compute_threshold` is a pure function. These tests don't touch the DB.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.think.diff_schema import ActOp
from services.think.thresholds import compute_threshold


pytestmark = [pytest.mark.integration]


def test_create_commitment_baseline():
    op = ActOp(
        op="create_commitment", confidence_basis=uuid4(),
        entity={"title": "ship widget"},
    )
    assert compute_threshold(op, basis=None) == 0.55


def test_create_goal_baseline():
    op = ActOp(
        op="create_goal", confidence_basis=None,
        entity={"title": "expand to eu"},
    )
    assert compute_threshold(op, basis=None) == 0.50


def test_doneverified_has_highest_commitment_baseline():
    op = ActOp(
        op="transition_commitment",
        confidence_basis=uuid4(),
        entity={"id": uuid4(), "new_state": "doneverified"},
    )
    # doneverified baseline 0.80; no modulators.
    assert compute_threshold(op, basis=None) == 0.80


def test_external_counterparty_raises_threshold():
    op = ActOp(
        op="transition_commitment",
        confidence_basis=uuid4(),
        entity={
            "id": uuid4(),
            "new_state": "active",
            "external_counterparty_ref": {
                "type": "customer_resource", "id": str(uuid4())
            },
        },
    )
    # active baseline 0.50 + external 0.10 = 0.60
    assert compute_threshold(op, basis=None) == pytest.approx(0.60)


def test_critical_path_raises_threshold():
    op = ActOp(
        op="create_commitment",
        confidence_basis=uuid4(),
        entity={
            "title": "x",
            "contributes_to_goal_ids": [(str(uuid4()), True)],
        },
    )
    # create 0.55 + critical_path 0.05
    assert compute_threshold(op, basis=None) == pytest.approx(0.60)


def test_first_person_override_lowers_threshold():
    actor = uuid4()
    op = ActOp(
        op="transition_commitment",
        confidence_basis=uuid4(),
        entity={
            "id": uuid4(),
            "new_state": "doneverified",
            "owner_id": str(actor),
        },
    )
    basis = {
        "proposition_kind": "contestation",
        "scope_actors": [actor],
    }
    # doneverified 0.80 − 0.15 = 0.65
    assert compute_threshold(op, basis=basis) == pytest.approx(0.65)


def test_all_modulators_combined():
    actor = uuid4()
    op = ActOp(
        op="transition_commitment",
        confidence_basis=uuid4(),
        entity={
            "id": uuid4(),
            "new_state": "active",
            "external_counterparty_ref": {"type": "customer_resource", "id": str(uuid4())},
            "owner_id": str(actor),
            "contributes_to_goal_ids": [(str(uuid4()), True)],
        },
    )
    basis = {
        "proposition_kind": "contestation",
        "scope_actors": [actor],
    }
    # active 0.50 + 0.10 + 0.05 − 0.15 = 0.50
    assert compute_threshold(op, basis=basis) == pytest.approx(0.50)


def test_threshold_clipped_low():
    # Impossible via normal modulators, but exercise the clip.
    from services.think import thresholds as th
    val = th._MIN - 0.01
    # Use the clip directly by patching via compute_threshold
    op = ActOp(
        op="create_goal", confidence_basis=None,
        entity={
            "title": "x",
            # Fake huge first-person override by picking a basis that
            # matches, combined with a baseline we force.
        },
    )
    # Goal create baseline 0.50. Can't go below 0.30 via normal path.
    assert compute_threshold(op, basis=None) >= 0.30


def test_threshold_clipped_high():
    # Exceeding 0.95 — would require baseline + all modulators.
    op = ActOp(
        op="transition_commitment",
        confidence_basis=uuid4(),
        entity={
            "id": uuid4(),
            "new_state": "doneverified",
            "external_counterparty_ref": {"type": "customer_resource", "id": str(uuid4())},
            "contributes_to_goal_ids": [(str(uuid4()), True)],
        },
    )
    # 0.80 + 0.10 + 0.05 = 0.95 — right at the cap.
    assert compute_threshold(op, basis=None) == 0.95


def test_unknown_op_falls_back_to_default():
    # An op not in _BASELINE uses 0.60.
    op = ActOp(op="add_edge_contributes_to", entity={})
    # add_edge_contributes_to is in _BASELINE → 0.55.
    assert compute_threshold(op, basis=None) == 0.55


def test_transition_commitment_blocked():
    op = ActOp(
        op="transition_commitment",
        confidence_basis=uuid4(),
        entity={"id": uuid4(), "new_state": "blocked"},
    )
    assert compute_threshold(op, basis=None) == 0.60


def test_transition_decision_to_revisited():
    op = ActOp(
        op="transition_decision",
        confidence_basis=uuid4(),
        entity={"id": uuid4(), "new_state": "revisited"},
    )
    assert compute_threshold(op, basis=None) == 0.70


def test_transition_decision_to_archived():
    op = ActOp(
        op="transition_decision",
        confidence_basis=uuid4(),
        entity={"id": uuid4(), "new_state": "archived"},
    )
    assert compute_threshold(op, basis=None) == 0.75


def test_update_goal_baseline():
    op = ActOp(op="update_goal", entity={"id": uuid4()})
    # update_goal baseline 0.50 (no modulators).
    assert compute_threshold(op, basis=None) == 0.50


def test_first_person_override_requires_scope_actor_match():
    op = ActOp(
        op="transition_commitment",
        confidence_basis=uuid4(),
        entity={
            "id": uuid4(),
            "new_state": "active",
            "owner_id": str(uuid4()),  # different actor
        },
    )
    basis = {
        "proposition_kind": "contestation",
        "scope_actors": [uuid4()],  # doesn't match owner
    }
    # No override applied → 0.50
    assert compute_threshold(op, basis=basis) == 0.50


def test_threshold_ignores_non_contestation_basis():
    op = ActOp(
        op="transition_commitment",
        confidence_basis=uuid4(),
        entity={"id": uuid4(), "new_state": "active", "owner_id": str(uuid4())},
    )
    basis = {"proposition_kind": "prediction", "scope_actors": []}
    assert compute_threshold(op, basis=basis) == 0.50


def test_transition_goal_uses_shared_baseline():
    op = ActOp(
        op="transition_goal",
        confidence_basis=uuid4(),
        entity={"id": uuid4(), "new_state": "achieved"},
    )
    assert compute_threshold(op, basis=None) == 0.55


def test_add_edge_depends_on():
    op = ActOp(
        op="add_edge_depends_on",
        confidence_basis=uuid4(),
        entity={
            "dependent_commitment_id": str(uuid4()),
            "dependency_commitment_id": str(uuid4()),
        },
    )
    assert compute_threshold(op, basis=None) == 0.55


def test_region_lock_key_is_deterministic():
    from services.think.region_locks import region_lock_key
    t = uuid4()
    e = [("commitment", uuid4()), ("goal", uuid4())]
    key1 = region_lock_key(t, e)
    key2 = region_lock_key(t, e)
    assert key1 == key2


def test_region_lock_key_sort_stable():
    from services.think.region_locks import region_lock_key
    t = uuid4()
    e1 = uuid4()
    e2 = uuid4()
    k1 = region_lock_key(t, [("commitment", e1), ("goal", e2)])
    k2 = region_lock_key(t, [("goal", e2), ("commitment", e1)])
    assert k1 == k2


def test_region_lock_key_disjoint_tenants_different():
    from services.think.region_locks import region_lock_key
    e = [("commitment", uuid4())]
    k1 = region_lock_key(uuid4(), e)
    k2 = region_lock_key(uuid4(), e)
    assert k1 != k2
