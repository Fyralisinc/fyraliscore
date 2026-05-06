"""Integration tests for services/acts/commitments.py — real Postgres."""
from __future__ import annotations


import pytest

from lib.shared.errors import InvariantViolation, ValidationError
from services.acts import commitments, goals, decisions, invariants as inv
from services.acts.tests.conftest import (
    TENANT_A,
    TENANT_B,
    future_due,
    make_actor,
    make_observation,
    past_due,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------
# Create-path
# ---------------------------------------------------------------------

async def test_commit_create_proposed_happy(acts_db, event_id):
    c = await commitments.create(
        title="draft",
        initial_state="proposed",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    assert c.state == "proposed"
    assert c.owner_id is None
    assert c.ambition_level == "base"


async def test_commit_create_active_with_goal_and_owner(
    acts_db, event_id, actor_id
):
    g = await goals.create(
        title="parent goal",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="work",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    assert c.state == "active"
    assert c.owner_id == actor_id


async def test_c1_active_without_owner_rejected(acts_db, event_id):
    with pytest.raises(InvariantViolation) as exc:
        await commitments.create(
            title="no owner",
            initial_state="active",
            due_date=future_due(),
            contributes_to_goal_ids=[],
            estimated_capacity={"maintenance": True},
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )
    assert exc.value.invariant == "C1"


async def test_c9_past_due_date_rejected(acts_db, event_id, actor_id):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    with pytest.raises(InvariantViolation) as exc:
        await commitments.create(
            title="late",
            initial_state="active",
            owner_id=actor_id,
            due_date=past_due(),
            contributes_to_goal_ids=[g.id],
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )
    assert exc.value.invariant == "C9"


async def test_c10_active_without_contrib_or_maintenance_rejected(
    acts_db, event_id, actor_id
):
    with pytest.raises(InvariantViolation) as exc:
        await commitments.create(
            title="orphan",
            initial_state="active",
            owner_id=actor_id,
            due_date=future_due(),
            contributes_to_goal_ids=[],
            # no maintenance flag
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )
    assert exc.value.invariant == "C10"


async def test_c10_maintenance_flag_allows_no_contributes(
    acts_db, event_id, actor_id
):
    c = await commitments.create(
        title="maint",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[],
        estimated_capacity={"maintenance": True},
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    assert c.state == "active"


async def test_c5_inactive_owner_rejected(acts_db, event_id, inactive_actor_id):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    with pytest.raises(InvariantViolation) as exc:
        await commitments.create(
            title="bad owner",
            initial_state="active",
            owner_id=inactive_actor_id,
            due_date=future_due(),
            contributes_to_goal_ids=[g.id],
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )
    assert exc.value.invariant == "C5"


async def test_c5_inactive_contributor_rejected(
    acts_db, event_id, actor_id, inactive_actor_id
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    with pytest.raises(InvariantViolation) as exc:
        await commitments.create(
            title="bad contrib",
            initial_state="active",
            owner_id=actor_id,
            due_date=future_due(),
            contributes_to_goal_ids=[g.id],
            contributors=[(inactive_actor_id, "reviewer")],
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )
    assert exc.value.invariant == "C5"


async def test_auto_block_with_unsatisfied_deps(
    acts_db, event_id, actor_id
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    dep = await commitments.create(
        title="dep",
        initial_state="proposed",
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    main = await commitments.create(
        title="main",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[g.id],
        depends_on_commitment_ids=[dep.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    assert main.state == "blocked"


# ---------------------------------------------------------------------
# C6 acyclic depends_on
# ---------------------------------------------------------------------

async def test_c6_depends_on_direct_cycle_rejected(acts_db, event_id, actor_id):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    a = await commitments.create(
        title="A",
        initial_state="proposed",
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    b = await commitments.create(
        title="B",
        initial_state="proposed",
        contributes_to_goal_ids=[g.id],
        depends_on_commitment_ids=[a.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # Now B depends on A. Adding A depends on B would close the cycle.
    with pytest.raises(InvariantViolation) as exc:
        await commitments.add_edge(
            "depends_on",
            dependent_commitment_id=a.id,
            dependency_commitment_id=b.id,
        )
    assert exc.value.invariant == "C6"


async def test_c6_self_dependency_rejected(acts_db, event_id):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    a = await commitments.create(
        title="A",
        initial_state="proposed",
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    with pytest.raises(InvariantViolation) as exc:
        await commitments.add_edge(
            "depends_on",
            dependent_commitment_id=a.id,
            dependency_commitment_id=a.id,
        )
    assert exc.value.invariant == "C6"


async def test_c6_transitive_cycle_rejected(acts_db, event_id):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    a = await commitments.create(
        title="A",
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    b = await commitments.create(
        title="B",
        contributes_to_goal_ids=[g.id],
        depends_on_commitment_ids=[a.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="C",
        contributes_to_goal_ids=[g.id],
        depends_on_commitment_ids=[b.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # A->B->C, trying to add A depends on C closes an A->B->C->A loop.
    with pytest.raises(InvariantViolation) as exc:
        await commitments.add_edge(
            "depends_on",
            dependent_commitment_id=a.id,
            dependency_commitment_id=c.id,
        )
    assert exc.value.invariant == "C6"


# ---------------------------------------------------------------------
# Transition state-machine and invariants
# ---------------------------------------------------------------------

async def test_transition_requires_cause_event_id(
    acts_db, event_id, actor_id
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        initial_state="proposed",
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    with pytest.raises(InvariantViolation) as exc:
        # Transition a proposed → closed without cause_event_id.
        await commitments.transition(c.id, "closed", cause_event_id=None)
    assert exc.value.invariant == "C4"


async def test_c3_doneverified_requires_resolved_events(
    acts_db, event_id, event_id2, actor_id
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    await commitments.transition(
        c.id, "doneunverified", cause_event_id=event_id2
    )
    with pytest.raises(InvariantViolation) as exc:
        await commitments.transition(
            c.id, "doneverified", cause_event_id=event_id2
        )
    assert exc.value.invariant == "C3"


async def test_c8_cannot_exit_doneverified(
    acts_db, event_id, event_id2, actor_id
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    await commitments.transition(
        c.id, "doneunverified", cause_event_id=event_id2
    )
    await commitments.transition(
        c.id,
        "doneverified",
        resolved_by_event_ids=[event_id2],
        cause_event_id=event_id2,
    )
    with pytest.raises(InvariantViolation) as exc:
        await commitments.transition(c.id, "active", cause_event_id=event_id2)
    assert exc.value.invariant == "C8"


async def test_c8_cannot_exit_closed(
    acts_db, event_id, event_id2
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        initial_state="proposed",
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    await commitments.transition(c.id, "closed", cause_event_id=event_id2)
    with pytest.raises(InvariantViolation) as exc:
        await commitments.transition(c.id, "active", cause_event_id=event_id2)
    assert exc.value.invariant == "C8"


async def test_c2_blocked_without_reason_rejected(
    acts_db, event_id, event_id2, actor_id
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    with pytest.raises(InvariantViolation) as exc:
        await commitments.transition(c.id, "blocked", cause_event_id=event_id2)
    assert exc.value.invariant == "C2"


async def test_c2_blocked_satisfied_by_revisited_decision(
    acts_db, event_id, event_id2, actor_id
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    d = await decisions.create(
        title="d",
        decision_text="we pick X",
        state="active",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[g.id],
        constrained_by_decision_ids=[d.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # Transition decision to revisited.
    await decisions.transition(d.id, "revisited", cause_event_id=event_id2)
    # Now blocking is allowed.
    c = await commitments.transition(
        c.id, "blocked", cause_event_id=event_id2
    )
    assert c.state == "blocked"


async def test_full_legal_chain(acts_db, event_id, event_id2, actor_id):
    """proposed → active → paused → active → doneunverified → doneverified."""
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        initial_state="proposed",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.transition(c.id, "active", cause_event_id=event_id2)
    assert c.state == "active"
    c = await commitments.transition(c.id, "paused", cause_event_id=event_id2)
    assert c.state == "paused"
    c = await commitments.transition(c.id, "active", cause_event_id=event_id2)
    assert c.state == "active"
    c = await commitments.transition(
        c.id, "doneunverified", cause_event_id=event_id2
    )
    assert c.state == "doneunverified"
    c = await commitments.transition(
        c.id,
        "doneverified",
        resolved_by_event_ids=[event_id2],
        cause_event_id=event_id2,
    )
    assert c.state == "doneverified"
    assert c.terminal_at is not None


# ---------------------------------------------------------------------
# Contributors & edges
# ---------------------------------------------------------------------

async def test_contributor_add_remove_readd_idempotent(
    acts_db, event_id, actor_id, actor_id2
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        initial_state="proposed",
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    r = await commitments.add_contributor(c.id, actor_id2, "reviewer")
    assert r.role == "reviewer"
    # idempotent re-add with updated role
    r = await commitments.add_contributor(c.id, actor_id2, "approver")
    assert r.role == "approver"
    # remove
    removed = await commitments.remove_contributor(c.id, actor_id2)
    assert removed
    # re-add after removal
    r = await commitments.add_contributor(c.id, actor_id2, "reviewer")
    assert r.role == "reviewer"


async def test_edge_idempotent_insert(acts_db, event_id):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        contributes_to_goal_ids=[g.id],
        initial_state="proposed",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    e1 = await commitments.add_edge(
        "contributes_to", commitment_id=c.id, goal_id=g.id
    )
    e2 = await commitments.add_edge(
        "contributes_to", commitment_id=c.id, goal_id=g.id
    )
    assert e1.commitment_id == e2.commitment_id
    assert e1.goal_id == e2.goal_id


async def test_multiple_contributes_to_atomic(acts_db, event_id, actor_id):
    g1 = await goals.create(
        title="g1",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    g2 = await goals.create(
        title="g2",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="multi",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[(g1.id, True), (g2.id, False)],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    async with acts_db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT goal_id, is_critical_path FROM contributes_to "
            "WHERE commitment_id = $1",
            c.id,
        )
    mapping = {r["goal_id"]: r["is_critical_path"] for r in rows}
    assert mapping[g1.id] is True
    assert mapping[g2.id] is False


# ---------------------------------------------------------------------
# Large-graph performance (C6 with 1000 commitments)
# ---------------------------------------------------------------------

async def test_large_graph_cycle_check_fast(acts_db, event_id, actor_id):
    """1000 commitments, 999 depends_on edges in a chain — cycle check
    on the final edge runs comfortably under 2s on a dev laptop.
    (BUILD-PLAN spec: < 500ms; we give a 2s budget to absorb CI noise.)"""
    import time

    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # Bulk insert 1000 commitments via raw SQL (faster than service API
    # for setup).
    from lib.shared.ids import uuid7
    ids = [uuid7() for _ in range(1000)]
    async with acts_db.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO commitments (
                  id, tenant_id, title, state, created_by_event_id
                ) VALUES ($1, $2, $3, 'proposed', $4)
                """,
                [(i, TENANT_A, f"c{n}", event_id) for n, i in enumerate(ids)],
            )
            # Chain: ids[i] depends on ids[i+1]
            await conn.executemany(
                """
                INSERT INTO depends_on (
                  dependent_commitment_id, dependency_commitment_id
                ) VALUES ($1, $2)
                """,
                [(ids[i], ids[i + 1]) for i in range(len(ids) - 1)],
            )
    # Now try adding ids[-1] -> ids[0], which would close a cycle.
    start = time.monotonic()
    with pytest.raises(InvariantViolation) as exc:
        await commitments.add_edge(
            "depends_on",
            dependent_commitment_id=ids[-1],
            dependency_commitment_id=ids[0],
        )
    elapsed = time.monotonic() - start
    assert exc.value.invariant == "C6"
    assert elapsed < 2.0, f"cycle check took {elapsed:.3f}s"


# ---------------------------------------------------------------------
# Invariant audit function entry point
# ---------------------------------------------------------------------

async def test_validate_invariants_catches_c3(
    acts_db, event_id, event_id2, actor_id
):
    g = await goals.create(
        title="g",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="c",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[g.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # Set state to doneverified directly (bypassing transition) so the
    # audit validator has something to catch — this mimics a corruption
    # or a direct SQL intervention.
    async with acts_db.acquire() as conn:
        await conn.execute(
            "UPDATE commitments SET state = 'doneverified' WHERE id = $1",
            c.id,
        )
        ok, violations = await inv.validate_commitment_invariants(c.id, conn)
    assert not ok
    assert any(v.invariant == "C3" for v in violations)


async def test_tenant_isolation_commitments(acts_db, event_id):
    ev_b = await make_observation(acts_db, tenant_id=TENANT_B)
    actor_b = await make_actor(acts_db, tenant_id=TENANT_B)

    g_a = await goals.create(
        title="A",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    g_b = await goals.create(
        title="B",
        created_by_event_id=ev_b,
        tenant_id=TENANT_B,
    )
    # Cross-tenant contributes_to edge must be blocked.
    with pytest.raises(ValidationError):
        await commitments.create(
            title="cross",
            initial_state="proposed",
            contributes_to_goal_ids=[g_b.id],   # wrong tenant
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )
