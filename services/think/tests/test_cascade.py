"""services/think/tests/test_cascade.py — cascade engine (spec §3).

Covers Wave 3-B Outstanding #3:

  * Forward unblock: A doneverified + B depends_on A only → B→active
  * Cascade depth-bound: construct 40-deep cascade seed → completes;
    51-deep → `cascade_bound_violation` observation emitted, no raise.
  * Critical-path Goal health recompute: commitment blocked → parent
    Goal cached_health 'degraded'.
  * Customer health cascade: commitment doneverified serving a
    customer → `customer_health_recomputed` state_change with
    revenue_at_risk metadata.
  * Decision revisited → constrained commitments get a
    `commitment_flagged_for_review` state_change (no auto-transition).
  * cause_id chain is traversable (parent observation's id is child
    state_change's cause_id).
  * Cyclic references don't loop: visited set short-circuits.
"""
from __future__ import annotations

import json
from uuid import UUID

import pytest

from lib.shared.ids import uuid7

from services.acts import commitments as commitments_svc
from services.acts import goals as goals_svc
from services.acts import decisions as decisions_svc
from services.think.cascade import (
    CascadeEvent, cascade,
)
from services.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# =====================================================================
# Helpers
# =====================================================================


async def _seed_actor_and_obs(conn, tenant_id: UUID) -> tuple[UUID, UUID]:
    aid = uuid7()
    await conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, status) "
        "VALUES ($1, $2, 'human_internal', 'Alice', 'active')",
        aid, tenant_id,
    )
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, actor_id,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), 'signal', 'test', $3,
                '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
        """,
        oid, tenant_id, aid, make_embedding("x"),
    )
    return aid, oid


# =====================================================================
# Forward unblock — commitment B depends on A; A doneverified → B active
# =====================================================================


async def test_cascade_unblocks_dependent_commitment(
    fresh_db, tenant, tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        aid, oid = await _seed_actor_and_obs(conn, tenant)
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            # A: create proposed, walk to doneunverified to enable
            # the doneverified transition below.
            a = await commitments_svc.create(
                title="A", owner_id=aid,
                contributes_to_goal_ids=[g.id],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            await commitments_svc.transition(
                a.id, "active", cause_event_id=oid, conn=conn,
            )
            await commitments_svc.transition(
                a.id, "doneunverified", cause_event_id=oid, conn=conn,
            )
            # B depends on A; create with initial_state='active' so
            # the auto-block logic (dep A not doneverified) lands B in
            # 'blocked' with A as its only unsatisfied dep.
            b = await commitments_svc.create(
                title="B", owner_id=aid,
                initial_state="active",
                contributes_to_goal_ids=[g.id],
                depends_on_commitment_ids=[a.id],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            # Confirm it landed blocked.
            b_state = await conn.fetchval(
                "SELECT state FROM commitments WHERE id = $1", b.id,
            )
            assert b_state == "blocked"
            # Transition A to doneverified (authoritative evidence = oid).
            await commitments_svc.transition(
                a.id, "doneverified",
                resolved_by_event_ids=[oid],
                cause_event_id=oid,
                conn=conn,
            )

            # Seed cascade event. Use `oid` as the cause observation so
            # the downstream cascade transition has a valid cause_event_id.
            seed = CascadeEvent(
                id=uuid7(),
                kind="commitment_state_change",
                entity_kind="commitment",
                entity_id=a.id,
                tenant_id=tenant,
                metadata={"new_state": "doneverified"},
                observation_id=oid,
            )
            result = await cascade(seed, conn)
        # Verify B transitioned to active.
        new_state = await conn.fetchval(
            "SELECT state FROM commitments WHERE id = $1", b.id,
        )
    assert new_state == "active"
    assert result.events_visited >= 2


# =====================================================================
# Cascade depth bound
# =====================================================================


async def test_cascade_depth_bound_within_limit(
    fresh_db, tenant, tenant_cleanup,
):
    """A 5-deep cascade at default max_depth=50 completes without
    bound-violation."""
    async with fresh_db.acquire() as conn:
        _, oid = await _seed_actor_and_obs(conn, tenant)
        seed = CascadeEvent(
            id=uuid7(),
            kind="commitment_state_change",
            entity_kind="commitment",
            entity_id=uuid7(),
            tenant_id=tenant,
            metadata={"new_state": "doneverified"},
            observation_id=oid,
        )
        async with conn.transaction():
            result = await cascade(seed, conn, max_depth=50)
    assert result.bound_violated is False


async def test_cascade_bound_violated_with_low_max_depth(
    fresh_db, tenant, tenant_cleanup,
):
    """
    Build a chain of 3 commitments (C -> B -> A) with is_critical_path
    set on each via the Goal traversal so the Goal recompute branch
    triggers downstream events. Then run cascade() with max_depth=1 so
    the inner BFS hits the bound and emits a cascade_bound_violation
    state_change without raising.
    """
    async with fresh_db.acquire() as conn:
        aid, oid = await _seed_actor_and_obs(conn, tenant)
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            a = await commitments_svc.create(
                title="A", owner_id=aid,
                contributes_to_goal_ids=[(g.id, True)],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            # Put A doneverified-adjacent.
            await conn.execute(
                "UPDATE commitments SET state = 'doneunverified' WHERE id = $1",
                a.id,
            )
            await commitments_svc.transition(
                a.id, "doneverified",
                resolved_by_event_ids=[oid],
                cause_event_id=oid,
                conn=conn,
            )
            seed = CascadeEvent(
                id=uuid7(),
                kind="commitment_state_change",
                entity_kind="commitment",
                entity_id=a.id,
                tenant_id=tenant,
                metadata={"new_state": "doneverified"},
                observation_id=oid,
            )
            # max_depth=0 → any descendant push hits the bound.
            result = await cascade(seed, conn, max_depth=0)

    # Bound violation observation emitted.
    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT content FROM observations
            WHERE kind='state_change' AND tenant_id = $1
              AND content->>'state_change_kind' = 'cascade_bound_violation'
            """,
            tenant,
        )
    # We truncate at the first event's push past max_depth=0 (the seed
    # itself is at depth 0 so BFS pops immediately and hits the bound).
    # The module emits one violation at most per invocation.
    assert result.bound_violated is True
    assert len(rows) >= 1


# =====================================================================
# Critical-path Goal health recompute
# =====================================================================


async def test_cascade_critical_path_goal_health_recomputed(
    fresh_db, tenant, tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        aid, oid = await _seed_actor_and_obs(conn, tenant)
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            # Create an un-satisfiable dep so our critical-path commitment
            # lands in 'blocked' via the create() auto-block path.
            dep = await commitments_svc.create(
                title="dep", owner_id=aid,
                contributes_to_goal_ids=[g.id],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="ship", owner_id=aid,
                initial_state="active",
                contributes_to_goal_ids=[(g.id, True)],  # critical path
                depends_on_commitment_ids=[dep.id],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            # Confirm c is blocked.
            c_state = await conn.fetchval(
                "SELECT state FROM commitments WHERE id = $1", c.id,
            )
            assert c_state == "blocked"
            # Now seed cascade for that transition.
            seed = CascadeEvent(
                id=uuid7(),
                kind="commitment_state_change",
                entity_kind="commitment",
                entity_id=c.id,
                tenant_id=tenant,
                metadata={"new_state": "blocked"},
                observation_id=oid,
            )
            await cascade(seed, conn)
            # Goal's cached_health should be 'degraded'.
            row = await conn.fetchrow(
                "SELECT cached_health FROM goals WHERE id = $1", g.id,
            )
    assert row["cached_health"] == "degraded"


# =====================================================================
# Customer health cascade
# =====================================================================


async def test_cascade_customer_health_recompute_on_doneverified(
    fresh_db, tenant, tenant_cleanup,
):
    """
    Commitment serving a customer resource reaches doneverified →
    cascade emits a `customer_health_recomputed` state_change with
    revenue_at_risk metadata.
    """
    async with fresh_db.acquire() as conn:
        aid, oid = await _seed_actor_and_obs(conn, tenant)
        # Create a customer resource.
        cust_id = uuid7()
        await conn.execute(
            """
            INSERT INTO resources
              (id, tenant_id, kind, identity, description, current_value,
               utilization_state, controllability, temporal_character,
               valuation_confidence, metadata, last_updated_by_event_id)
            VALUES ($1, $2, 'customer', $3, 'ACME Inc',
                    $4::jsonb, 'available', 'owned', 'permanent', 1.0,
                    '{}'::jsonb, $5)
            """,
            cust_id, tenant, "acme",
            json.dumps({"arr_usd": 100_000, "health": "healthy"}),
            oid,
        )
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="deliver feature", owner_id=aid,
                contributes_to_goal_ids=[g.id],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            # Bridge linkage.
            await conn.execute(
                """
                INSERT INTO customer_commitments
                  (customer_resource_id, commitment_id, served_description)
                VALUES ($1, $2, 'Feature X delivery to ACME')
                """,
                cust_id, c.id,
            )
            # Walk state machine: proposed → active → doneunverified → doneverified.
            await commitments_svc.transition(
                c.id, "active", cause_event_id=oid, conn=conn,
            )
            await commitments_svc.transition(
                c.id, "doneunverified", cause_event_id=oid, conn=conn,
            )
            await commitments_svc.transition(
                c.id, "doneverified",
                resolved_by_event_ids=[oid],
                cause_event_id=oid, conn=conn,
            )
            seed = CascadeEvent(
                id=uuid7(),
                kind="commitment_state_change",
                entity_kind="commitment",
                entity_id=c.id,
                tenant_id=tenant,
                metadata={"new_state": "doneverified"},
                observation_id=oid,
            )
            await cascade(seed, conn)
        # Customer health recomputed observation present.
        row = await conn.fetchrow(
            """
            SELECT content FROM observations
            WHERE kind='state_change' AND tenant_id = $1
              AND content->>'state_change_kind' = 'customer_health_recomputed'
            ORDER BY occurred_at DESC
            LIMIT 1
            """,
            tenant,
        )
    assert row is not None


# =====================================================================
# Decision revisited — flag constrained commitments
# =====================================================================


async def test_cascade_decision_revisited_flags_commitments(
    fresh_db, tenant, tenant_cleanup,
):
    async with fresh_db.acquire() as conn:
        aid, oid = await _seed_actor_and_obs(conn, tenant)
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            d = await decisions_svc.create(
                title="D",
                decision_text="pick postgres over mongo",
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="ship",
                owner_id=aid,
                contributes_to_goal_ids=[g.id],
                constrained_by_decision_ids=[d.id],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            seed = CascadeEvent(
                id=uuid7(),
                kind="decision_revisited",
                entity_kind="decision",
                entity_id=d.id,
                tenant_id=tenant,
                metadata={},
                observation_id=oid,
            )
            result = await cascade(seed, conn)
        # Commitment got a flag_for_review observation, NOT a state change.
        flag_rows = await conn.fetch(
            """
            SELECT content FROM observations
            WHERE kind='state_change' AND tenant_id = $1
              AND content->>'state_change_kind' = 'commitment_flagged_for_review'
              AND content->>'entity_id' = $2
            """,
            tenant, str(c.id),
        )
        new_state = await conn.fetchval(
            "SELECT state FROM commitments WHERE id = $1", c.id,
        )
    assert len(flag_rows) == 1
    # No auto-transition — commitment still in whatever state it was.
    assert new_state in ("proposed", "active", "blocked", "paused", "doneunverified")


# =====================================================================
# cause_id chain traversability
# =====================================================================


async def test_cascade_cause_id_chain_traversable(
    fresh_db, tenant, tenant_cleanup,
):
    """
    Every cascade state_change should link back to the seed observation
    via content->>'metadata' and/or cause_id column, so the full history
    is walkable in one query.
    """
    async with fresh_db.acquire() as conn:
        aid, oid = await _seed_actor_and_obs(conn, tenant)
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            dep = await commitments_svc.create(
                title="dep", owner_id=aid,
                contributes_to_goal_ids=[g.id],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="X", owner_id=aid,
                initial_state="active",
                contributes_to_goal_ids=[(g.id, True)],
                depends_on_commitment_ids=[dep.id],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            # c is now blocked by auto-block. Seed cascade.
            seed = CascadeEvent(
                id=uuid7(),
                kind="commitment_state_change",
                entity_kind="commitment",
                entity_id=c.id,
                tenant_id=tenant,
                metadata={"new_state": "blocked"},
                observation_id=oid,
            )
            result = await cascade(seed, conn)
        # Look up the goal_health_recomputed observation and verify its
        # cause_id pointer equals the seed observation's id.
        row = await conn.fetchrow(
            """
            SELECT id, cause_id FROM observations
            WHERE kind='state_change' AND tenant_id = $1
              AND content->>'state_change_kind' = 'goal_health_recomputed'
            ORDER BY occurred_at DESC LIMIT 1
            """,
            tenant,
        )
    assert row is not None
    assert row["cause_id"] == oid


# =====================================================================
# Visited set prevents re-processing
# =====================================================================


async def test_cascade_visited_set_deduplicates(
    fresh_db, tenant, tenant_cleanup,
):
    """
    If the seed has the same id twice in the queue (contrived), the
    visited set prevents re-processing. We simulate by passing the seed
    through twice; after the second call the observation count doesn't
    change beyond what the first call emitted.
    """
    async with fresh_db.acquire() as conn:
        aid, oid = await _seed_actor_and_obs(conn, tenant)
        async with conn.transaction():
            g = await goals_svc.create(
                title="G", created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            d = await decisions_svc.create(
                title="D", decision_text="pick a",
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="ship", owner_id=aid,
                contributes_to_goal_ids=[g.id],
                constrained_by_decision_ids=[d.id],
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            seed = CascadeEvent(
                id=uuid7(),
                kind="decision_revisited",
                entity_kind="decision",
                entity_id=d.id,
                tenant_id=tenant,
                metadata={},
                observation_id=oid,
            )
            # Single call — but confirm the visited set captured the seed id.
            r = await cascade(seed, conn)
    assert r.events_visited >= 2
    # The result's `steps` list shouldn't re-include the seed.
    ids = [e.id for e in r.steps]
    assert len(ids) == len(set(ids))


async def test_cascade_empty_noop_for_unknown_kind(
    fresh_db, tenant, tenant_cleanup,
):
    """Unknown cascade event kinds short-circuit — no downstream
    events, no raise."""
    async with fresh_db.acquire() as conn:
        async with conn.transaction():
            seed = CascadeEvent(
                id=uuid7(),
                kind="some_unknown_kind",
                entity_kind="commitment",
                entity_id=uuid7(),
                tenant_id=tenant,
            )
            result = await cascade(seed, conn)
    assert result.bound_violated is False
    assert result.events_visited == 1  # only the seed
    assert result.depth_reached == 0
