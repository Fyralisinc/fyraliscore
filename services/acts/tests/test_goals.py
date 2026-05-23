"""Integration tests for services/acts/goals.py — real Postgres."""
from __future__ import annotations


import pytest

from lib.shared.errors import InvariantViolation, ValidationError
from services.acts import goals, invariants as inv
from services.acts.tests.conftest import TENANT_A, TENANT_B, make_observation, future_due


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_goal_create_happy(acts_db, event_id):
    g = await goals.create(
        title="Ship Q2 launch",
        description="Launch by end of Q2",
        target_date=future_due(24 * 30),
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    assert g.state == "active"
    assert g.cached_health == "healthy"
    assert g.altitude == "operational"
    assert g.created_by_event_id == event_id


async def test_goal_create_requires_title(acts_db, event_id):
    with pytest.raises(ValidationError):
        await goals.create(
            title="",
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )


async def test_goal_parent_must_exist(acts_db, event_id):
    import uuid
    fake = uuid.uuid4()
    with pytest.raises(ValidationError):
        await goals.create(
            title="child",
            parent_goal_id=fake,
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )


async def test_goal_parent_must_be_same_tenant(acts_db, event_id):
    # Parent in tenant B, child in tenant A.
    ev_b = await make_observation(acts_db, tenant_id=TENANT_B)
    parent = await goals.create(
        title="parent",
        created_by_event_id=ev_b,
        tenant_id=TENANT_B,
    )
    with pytest.raises(ValidationError):
        await goals.create(
            title="child",
            parent_goal_id=parent.id,
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )


async def test_goal_parent_must_be_active(acts_db, event_id, event_id2):
    parent = await goals.create(
        title="parent",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # abandon parent
    await goals.transition(parent.id, "abandoned", cause_event_id=event_id2)
    with pytest.raises(ValidationError):
        await goals.create(
            title="child",
            parent_goal_id=parent.id,
            created_by_event_id=event_id,
            tenant_id=TENANT_A,
        )


async def test_goal_transitions_all_legal(acts_db, event_id, event_id2):
    # active -> paused -> active -> achieved? no, need to ensure G4 is
    # satisfied (no critical-path commitments => trivially satisfied).
    g = await goals.create(
        title="transition journey",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    g = await goals.transition(g.id, "paused", cause_event_id=event_id2)
    assert g.state == "paused"
    g = await goals.transition(g.id, "active", cause_event_id=event_id2)
    assert g.state == "active"
    g = await goals.transition(g.id, "achieved", cause_event_id=event_id2)
    assert g.state == "achieved"
    assert g.archived_at is not None


async def test_goal_terminal_blocks_transition(acts_db, event_id):
    g = await goals.create(
        title="terminal",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    await goals.transition(g.id, "abandoned", cause_event_id=event_id)
    with pytest.raises(InvariantViolation) as exc:
        await goals.transition(g.id, "active", cause_event_id=event_id)
    assert exc.value.invariant == "G_STATE"


async def test_goal_cycle_prevention_direct(acts_db, event_id):
    # a → b ; then try to set a's parent = b (would make b parent and
    # also ancestor of a via a→b→a? No — a's parent would be b, and b's
    # parent is a. Cycle.)
    a = await goals.create(
        title="A",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    b = await goals.create(
        title="B",
        parent_goal_id=a.id,  # B's parent is A
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # Now proposing A's parent = B would create a cycle A->B->A.
    with pytest.raises(InvariantViolation) as exc:
        await goals.validate_acyclic(a.id, b.id)
    assert exc.value.invariant == "G2"


async def test_goal_self_parent_rejected(acts_db, event_id):
    g = await goals.create(
        title="self",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    with pytest.raises(InvariantViolation) as exc:
        await goals.validate_acyclic(g.id, g.id)
    assert exc.value.invariant == "G2"


async def test_goal_g4_direct_children(acts_db, event_id, actor_id):
    """Achieved with an open critical-path commitment is rejected."""
    from services.acts import commitments
    g = await goals.create(
        title="G4 check",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # Critical-path commitment, still active → G4 must block achieve.
    c = await commitments.create(
        title="critical work",
        initial_state="active",
        owner_id=actor_id,
        due_date=future_due(),
        contributes_to_goal_ids=[(g.id, True)],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    assert c.state == "active"
    with pytest.raises(InvariantViolation) as exc:
        await goals.transition(g.id, "achieved", cause_event_id=event_id)
    assert exc.value.invariant == "G4"


async def test_goal_cached_health_healthy_when_no_critical_path(
    acts_db, event_id, actor_id
):
    from services.acts import commitments
    g = await goals.create(
        title="no cp",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # Non-critical-path commitment in any state shouldn't touch health.
    await commitments.create(
        title="not critical",
        initial_state="proposed",
        contributes_to_goal_ids=[(g.id, False)],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    async with acts_db.acquire() as conn:
        health = await goals.recompute_cached_health(g.id, conn)
    assert health == "healthy"


async def test_goal_cached_health_degraded_on_blocked(
    acts_db, event_id, event_id2, actor_id
):
    from services.acts import commitments
    g = await goals.create(
        title="health check",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # Two critical-path commitments, one becomes blocked (via
    # unsatisfied dep).
    dep = await commitments.create(
        title="dep",
        initial_state="proposed",
        contributes_to_goal_ids=[(g.id, True)],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    main = await commitments.create(
        title="main",
        initial_state="active",
        owner_id=actor_id,
        contributes_to_goal_ids=[(g.id, True)],
        depends_on_commitment_ids=[dep.id],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # main should have auto-blocked
    assert main.state == "blocked"
    async with acts_db.acquire() as conn:
        health = await goals.recompute_cached_health(g.id, conn)
    assert health == "degraded"


async def test_goal_cached_health_critical_on_closed_and_others_incomplete(
    acts_db, event_id, event_id2, actor_id
):
    from services.acts import commitments
    g = await goals.create(
        title="critical",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    a = await commitments.create(
        title="A",
        initial_state="active",
        owner_id=actor_id,
        contributes_to_goal_ids=[(g.id, True)],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    b = await commitments.create(
        title="B",
        initial_state="active",
        owner_id=actor_id,
        contributes_to_goal_ids=[(g.id, True)],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    # Close A (non-doneverified terminal).
    await commitments.transition(
        a.id, "closed", cause_event_id=event_id2
    )
    # B is still active, not doneverified → critical.
    async with acts_db.acquire() as conn:
        health = await goals.recompute_cached_health(g.id, conn)
    assert health == "critical"


async def test_goal_cached_health_healthy_all_doneverified(
    acts_db, event_id, event_id2, actor_id
):
    from services.acts import commitments
    g = await goals.create(
        title="all done",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    c = await commitments.create(
        title="only",
        initial_state="active",
        owner_id=actor_id,
        contributes_to_goal_ids=[(g.id, True)],
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    await commitments.transition(
        c.id,
        "doneunverified",
        cause_event_id=event_id2,
    )
    await commitments.transition(
        c.id,
        "doneverified",
        resolved_by_event_ids=[event_id2],
        cause_event_id=event_id2,
    )
    async with acts_db.acquire() as conn:
        health = await goals.recompute_cached_health(g.id, conn)
    assert health == "healthy"


async def test_goal_g1_active_with_no_work_flagged(acts_db, event_id):
    """G1 audit: an active goal with no commitments/sub-goals violates G1."""
    g = await goals.create(
        title="empty",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    async with acts_db.acquire() as conn:
        ok, violations = await inv.validate_goal_invariants(g.id, conn)
    assert not ok
    assert any(v.invariant == "G1" for v in violations)


async def test_goal_g1_satisfied_with_subgoal(acts_db, event_id):
    parent = await goals.create(
        title="parent",
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    await goals.create(
        title="child",
        parent_goal_id=parent.id,
        created_by_event_id=event_id,
        tenant_id=TENANT_A,
    )
    async with acts_db.acquire() as conn:
        ok, violations = await inv.validate_goal_invariants(parent.id, conn)
    # G1 is satisfied. G3 freshness depends on equal cached_health,
    # which at creation is 'healthy' and computed is 'healthy' (no
    # critical path), so it should pass.
    g1_violations = [v for v in violations if v.invariant == "G1"]
    assert not g1_violations


async def test_goal_tenant_isolation(acts_db, event_id):
    ev_b = await make_observation(acts_db, tenant_id=TENANT_B)
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
    # Simple sanity: querying by tenant A shouldn't return B's goal.
    async with acts_db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM goals WHERE tenant_id = $1", TENANT_A
        )
    ids = {r["id"] for r in rows}
    assert g_a.id in ids
    assert g_b.id not in ids
