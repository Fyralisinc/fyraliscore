"""Category 6 — Cascade & propagation pressure.

The cascade is where the substrate's "alive" claim either holds up
or breaks down. Existing harness covers depth=0 forced violation
and orphan-unblock invariants. These adversarial cases exercise:

  * Real depth approaching MAX (saturation, not forced violation)
  * True cycles that exercise BFS visited dedup
  * Cascade against archived/missing Models
  * Goal health recomputation (branch A sub-task, never tested)
  * Resource health (branch C, never tested)
  * Cross-branch interaction (commitment_state_change + decision_revisited)
  * Cascade-noop rate measurement
  * Fan-out from a single seed
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.think.cascade import (
    CascadeEvent,
    MAX_CASCADE_DEPTH,
    cascade,
)
from services.think.observability import METRICS

from .. import _fixtures as F
from .._runner import Case
from . import _helpers as H


# =====================================================================
# CP1 — Wide fan-out: one done commit unblocks 20 dependents
# =====================================================================


async def _setup_fanout(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(conn, tenant, title="Fanout parent")
            done = await F.make_commitment(
                conn, tenant, owner_id=owner, state="doneverified",
            )
            blocked_ids = []
            for i in range(20):
                b = await F.make_commitment(
                    conn, tenant, owner_id=owner, state="blocked",
                    title=f"blocked_{i}",
                )
                await F.add_contributes_to(conn, commitment_id=b, goal_id=goal)
                await F.add_depends_on(conn, dependent=b, dependency=done)
                blocked_ids.append(b)
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant, "done": done, "blocked": blocked_ids,
                "obs": obs,
            }


async def _run_fanout(pool: asyncpg.Pool, ctx: dict) -> dict:
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["done"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=ctx["obs"],
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
            unblocked = await conn.fetchval(
                "SELECT COUNT(*) FROM commitments "
                "WHERE id = ANY($1::uuid[]) AND state = 'active'",
                ctx["blocked"],
            )
    return {
        "events_visited": result.events_visited,
        "depth_reached": result.depth_reached,
        "bound_violated": result.bound_violated,
        "unblocked": unblocked,
    }


def _assert_fanout(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["unblocked"] != 20:
        return False, (
            f"expected all 20 dependents unblocked; got {actual['unblocked']}"
        )
    if actual["bound_violated"]:
        return False, "depth bound violated unexpectedly"
    return True, ""


CASE_FANOUT = Case(
    stage="adversarial.cascade",
    name="wide_fanout_unblocks_20_dependents",
    intent="One doneverified commitment unblocks 20 dependents in "
           "single cascade walk; depth bound is not breached",
    setup=_setup_fanout,
    run=H.safe_pipeline(_run_fanout),
    expected=lambda _ctx: {},
    assertion=_assert_fanout,
    failure_mode_under_test=(
        "fan-out is sequential and slow, OR cascade emits per-dependent "
        "T1 triggers that overwhelm the queue"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# CP2 — Deep chain at MAX_DEPTH-1 saturates without violating
# =====================================================================
# Build a chain of 10 commitments where each blocked one depends on
# the next done one. Cascade walks the chain; at MAX_DEPTH-1=49 it
# should still complete cleanly. (Production MAX is 50.)


async def _setup_deep_chain(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(conn, tenant, title="deep chain")
            # Build a linear depends_on chain:
            #   c0 (done) <- c1 (blocked) <- c2 (blocked) ... <- c9
            # When c0 doneverifies, c1 should unblock; but unblocking
            # c1 doesn't doneverify it, so the chain stops at depth 1.
            chain = []
            for i in range(10):
                state = "doneverified" if i == 0 else "blocked"
                c = await F.make_commitment(
                    conn, tenant, owner_id=owner, state=state,
                    title=f"chain_{i}",
                )
                await F.add_contributes_to(conn, commitment_id=c, goal_id=goal)
                if i > 0:
                    await F.add_depends_on(
                        conn, dependent=c, dependency=chain[i-1],
                    )
                chain.append(c)
            obs = await F.make_observation(conn, tenant)
            return {"tenant": tenant, "chain": chain, "obs": obs}


async def _run_deep_chain(pool: asyncpg.Pool, ctx: dict) -> dict:
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["chain"][0],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=ctx["obs"],
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
            states = await conn.fetch(
                "SELECT id, state FROM commitments WHERE id = ANY($1::uuid[]) "
                "ORDER BY id",
                ctx["chain"],
            )
    return {
        "events_visited": result.events_visited,
        "depth_reached": result.depth_reached,
        "bound_violated": result.bound_violated,
        "states": [r["state"] for r in states],
    }


CASE_DEEP_CHAIN = Case(
    stage="adversarial.cascade",
    name="long_dependency_chain_stops_at_unblock",
    intent="Deep depends-on chain where only the head is done: "
           "cascade unblocks chain[1] but stops there (active doesn't "
           "trigger further cascades)",
    setup=_setup_deep_chain,
    run=H.safe_pipeline(_run_deep_chain),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "cascade walks the full chain incorrectly because the branch "
        "logic confuses 'unblock to active' with 'doneverify'; OR "
        "depth bound triggers before chain stops naturally"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# CP3 — Cycle: A depends on B, B depends on A (visited dedup must catch)
# =====================================================================
# Model a cycle by building two commitments that mutually depend on
# each other (the schema allows it). The cascade BFS visited set
# should prevent re-visit.


async def _setup_cycle(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(conn, tenant, title="cycle parent")
            a = await F.make_commitment(conn, tenant, owner_id=owner,
                                          state="doneverified", title="A")
            b = await F.make_commitment(conn, tenant, owner_id=owner,
                                          state="blocked", title="B")
            await F.add_contributes_to(conn, commitment_id=a, goal_id=goal)
            await F.add_contributes_to(conn, commitment_id=b, goal_id=goal)
            await F.add_depends_on(conn, dependent=b, dependency=a)
            await F.add_depends_on(conn, dependent=a, dependency=b)
            obs = await F.make_observation(conn, tenant)
            return {"tenant": tenant, "a": a, "b": b, "obs": obs}


async def _run_cycle(pool: asyncpg.Pool, ctx: dict) -> dict:
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["a"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=ctx["obs"],
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
    return {
        "events_visited": result.events_visited,
        "depth_reached": result.depth_reached,
        "bound_violated": result.bound_violated,
    }


def _assert_cycle(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["bound_violated"]:
        return False, (
            "cycle hit depth bound — visited dedup should have stopped "
            "the cascade before that"
        )
    if actual["events_visited"] > 5:
        return False, (
            f"cycle visited too many events ({actual['events_visited']}); "
            f"BFS should converge fast"
        )
    return True, ""


CASE_CYCLE = Case(
    stage="adversarial.cascade",
    name="dependency_cycle_terminates_via_visited_dedup",
    intent="A↔B mutual depends_on cycle: cascade terminates via "
           "visited dedup, not via depth bound",
    setup=_setup_cycle,
    run=H.safe_pipeline(_run_cycle),
    expected=lambda _ctx: {},
    assertion=_assert_cycle,
    failure_mode_under_test=(
        "BFS visited set keys on the wrong field (e.g. event id "
        "instead of (entity_kind, entity_id)) and the cycle propagates "
        "until depth bound trips"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# CP4 — Cascade against deleted/missing entity_id
# =====================================================================


async def _setup_missing_entity(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            return {"tenant": tenant}


async def _run_missing_entity(pool: asyncpg.Pool, ctx: dict) -> dict:
    # Reference an entity_id that doesn't exist.
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=uuid7(),  # ghost
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
    return {
        "events_visited": result.events_visited,
        "bound_violated": result.bound_violated,
    }


CASE_MISSING_ENTITY = Case(
    stage="adversarial.cascade",
    name="cascade_against_missing_entity",
    intent="Cascade seeded by a non-existent commitment id completes "
           "cleanly (no downstream, no crash)",
    setup=_setup_missing_entity,
    run=H.safe_pipeline(_run_missing_entity),
    expected=lambda _ctx: {"events_visited": 1, "bound_violated": False},
    assertion=lambda a, e, c: (
        (a.get("events_visited") == 1 and not a.get("bound_violated"),
         "" if (a.get("events_visited") == 1 and not a.get("bound_violated"))
         else f"got {a!r}")
    ),
    failure_mode_under_test=(
        "cascade looks up the entity, finds nothing, but treats the "
        "empty result as a 'has dependents' branch and infinite-loops"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# CP5 — Cross-branch: decision revisited + commitment unblock interleaved
# =====================================================================


async def _setup_cross_branch(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(conn, tenant, title="cross")
            decision = await F.make_decision(
                conn, tenant, title="D", state="revisited",
            )
            done = await F.make_commitment(conn, tenant, owner_id=owner,
                                             state="doneverified", title="done")
            blocked = await F.make_commitment(conn, tenant, owner_id=owner,
                                                state="blocked", title="blocked")
            await F.add_contributes_to(conn, commitment_id=blocked, goal_id=goal)
            await F.add_constrained_by(
                conn, commitment_id=blocked, decision_id=decision,
            )
            await F.add_depends_on(conn, dependent=blocked, dependency=done)
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant,
                "decision": decision,
                "done": done,
                "blocked": blocked,
                "obs": obs,
            }


async def _run_cross_branch(pool: asyncpg.Pool, ctx: dict) -> dict:
    # Two seeds: one decision_revisited, one commitment_state_change.
    # Both touch `blocked`. Run sequentially; verify final state.
    async with pool.acquire() as conn:
        async with conn.transaction():
            seed_dec = CascadeEvent(
                id=uuid7(),
                kind="decision_revisited",
                entity_kind="decision",
                entity_id=ctx["decision"],
                tenant_id=ctx["tenant"],
                metadata={},
                observation_id=ctx["obs"],
            )
            r1 = await cascade(seed_dec, conn, tenant_id=ctx["tenant"])

            seed_done = CascadeEvent(
                id=uuid7(),
                kind="commitment_state_change",
                entity_kind="commitment",
                entity_id=ctx["done"],
                tenant_id=ctx["tenant"],
                metadata={"new_state": "doneverified"},
                observation_id=ctx["obs"],
            )
            r2 = await cascade(seed_done, conn, tenant_id=ctx["tenant"])

            row = await conn.fetchrow(
                "SELECT state FROM commitments WHERE id=$1",
                ctx["blocked"],
            )
    return {
        "decision_walk": r1.events_visited,
        "commit_walk": r2.events_visited,
        "blocked_state": row["state"],
    }


CASE_CROSS_BRANCH = Case(
    stage="adversarial.cascade",
    name="decision_revisit_then_commit_unblock",
    intent="A commitment that is both flagged-for-review (branch B) "
           "and an unblock target (branch A) reaches state='active' "
           "after the second cascade",
    setup=_setup_cross_branch,
    run=H.safe_pipeline(_run_cross_branch),
    expected=lambda _ctx: {"blocked_state": "active"},
    assertion=lambda a, e, c: (
        (a.get("blocked_state") == "active",
         "" if a.get("blocked_state") == "active"
         else f"got {a.get('blocked_state')!r}")
    ),
    failure_mode_under_test=(
        "branch B's flag-for-review state lingers and branch A refuses "
        "to unblock because it sees the flag — branches mutually "
        "exclude when they shouldn't"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# CP6 — Cascade noop: state didn't actually change
# =====================================================================


async def _setup_noop(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(conn, tenant, title="noop parent")
            commit = await F.make_commitment(
                conn, tenant, owner_id=owner, state="doneverified",
            )
            await F.add_contributes_to(conn, commitment_id=commit, goal_id=goal)
            obs = await F.make_observation(conn, tenant)
            return {"tenant": tenant, "commit": commit, "obs": obs}


async def _run_noop(pool: asyncpg.Pool, ctx: dict) -> dict:
    # Re-emit a doneverified state_change for an already-doneverified
    # commit — should produce no downstream changes.
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["commit"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=ctx["obs"],
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
    return {"events_visited": result.events_visited, "steps": len(result.steps)}


CASE_NOOP = Case(
    stage="adversarial.cascade",
    name="cascade_noop_no_dependents",
    intent="A done commit with no dependents/no contributes_to to "
           "non-critical goals produces a 1-step cascade (just the seed)",
    setup=_setup_noop,
    run=H.safe_pipeline(_run_noop),
    expected=lambda _ctx: {},
    assertion=lambda a, e, c: (
        (a.get("events_visited") == 1,
         "" if a.get("events_visited") == 1
         else f"got events_visited={a.get('events_visited')}")
    ),
    failure_mode_under_test=(
        "cascade does work on a no-op (e.g. recomputes goal health "
        "even when the commit's state didn't change), wasting cycles"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should cascade detect 'state didn't actually change' and "
        "short-circuit? Currently it walks regardless. For high-volume "
        "re-emissions this could matter."
    ),
    domain="extraction",
)


# =====================================================================
# CP7 — Goal health recomputation on critical-path commitment doneverify
# =====================================================================


async def _setup_goal_health(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(
                conn, tenant, title="critical goal",
                cached_health="at_risk",
            )
            commit = await F.make_commitment(
                conn, tenant, owner_id=owner, state="doneverified",
            )
            await F.add_contributes_to(
                conn, commitment_id=commit, goal_id=goal,
                is_critical_path=True,
            )
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant,
                "goal": goal,
                "commit": commit,
                "obs": obs,
            }


async def _run_goal_health(pool: asyncpg.Pool, ctx: dict) -> dict:
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["commit"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=ctx["obs"],
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            await cascade(seed, conn, tenant_id=ctx["tenant"])
            row = await conn.fetchrow(
                "SELECT cached_health FROM goals WHERE id=$1", ctx["goal"],
            )
    return {"goal_health": row["cached_health"] if row else None}


CASE_GOAL_HEALTH = Case(
    stage="adversarial.cascade",
    name="critical_path_doneverify_recomputes_goal_health",
    intent="Commit doneverify on a critical-path edge triggers a "
           "goal_health recompute (branch A sub-task)",
    setup=_setup_goal_health,
    run=H.safe_pipeline(_run_goal_health),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "branch A drops the goal-health recompute step; cached_health "
        "stays stale forever despite critical-path commits being done"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Document what cached_health values are valid and the "
        "recompute formula. Today this is implicit; cascade has a "
        "branch but no test asserts the resulting health value."
    ),
    domain="extraction",
)


# =====================================================================
# CP8 — Cascade with no observation_id and decision_revisited (no C4)
# =====================================================================


async def _setup_dec_no_obs(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            decision = await F.make_decision(
                conn, tenant, state="revisited", title="D",
            )
            commit = await F.make_commitment(
                conn, tenant, owner_id=owner, title="C",
            )
            await F.add_constrained_by(
                conn, commitment_id=commit, decision_id=decision,
            )
            return {
                "tenant": tenant, "decision": decision, "commit": commit,
            }


async def _run_dec_no_obs(pool: asyncpg.Pool, ctx: dict) -> dict:
    # decision_revisited path doesn't transition commitment state,
    # only emits a flag — so missing observation_id (C4) shouldn't
    # raise the way it does for unblock.
    seed = CascadeEvent(
        id=uuid7(),
        kind="decision_revisited",
        entity_kind="decision",
        entity_id=ctx["decision"],
        tenant_id=ctx["tenant"],
        metadata={},
        observation_id=None,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
    return {
        "events_visited": result.events_visited,
        "violations": len(result.invariant_violations),
    }


CASE_DEC_NO_OBS = Case(
    stage="adversarial.cascade",
    name="decision_revisited_without_observation_id",
    intent="decision_revisited with observation_id=None completes "
           "cleanly (the branch only flags; no transition is required)",
    setup=_setup_dec_no_obs,
    run=H.safe_pipeline(_run_dec_no_obs),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "branch B requires cause_event_id to write the flag and "
        "raises C4-style invariant when missing — cascade halts on "
        "an avoidable problem"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# CP9 — Cascade against a Model with empty scope_entities
# =====================================================================
# Models can have empty scope. Cascade is keyed off entity_id, not
# scope, so this should be irrelevant — but verify the substrate
# doesn't surface any odd behavior.


async def _setup_empty_scope_model(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            await F.make_model(
                conn, tenant,
                natural="empty scope model",
                scope_actors=[],
                scope_entities=[],
            )
            return {"tenant": tenant}


async def _run_empty_scope_model(pool: asyncpg.Pool, ctx: dict) -> dict:
    # Cascade with unknown kind; the empty-scope Model is not part
    # of the BFS but verifies it doesn't crash queries.
    seed = CascadeEvent(
        id=uuid7(),
        kind="unknown_kind_no_branch",
        entity_kind="commitment",
        entity_id=uuid7(),
        tenant_id=ctx["tenant"],
        metadata={},
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
    return {"events_visited": result.events_visited}


CASE_EMPTY_SCOPE = Case(
    stage="adversarial.cascade",
    name="empty_scope_model_does_not_break_cascade",
    intent="A Model with no scope_actors/scope_entities does not "
           "interfere with cascade on unrelated entities",
    setup=_setup_empty_scope_model,
    run=H.safe_pipeline(_run_empty_scope_model),
    expected=lambda _ctx: {"events_visited": 1},
    assertion=lambda a, e, c: (
        (a.get("events_visited") == 1,
         "" if a.get("events_visited") == 1
         else f"got {a!r}")
    ),
    failure_mode_under_test=(
        "queries that scope by scope_actors[] crash on empty arrays "
        "or treat them as wildcard, accidentally including unrelated "
        "Models"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# CP10 — Cascade depth bound at MAX_CASCADE_DEPTH-1 produces no violation
# =====================================================================
# Run with a max_depth set to MAX_CASCADE_DEPTH-1 against a chain
# that stops naturally at depth 1 — the bound shouldn't trigger.


async def _run_high_bound(pool: asyncpg.Pool, ctx: dict) -> dict:
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["chain"][0],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=ctx["obs"],
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(
                seed, conn, tenant_id=ctx["tenant"],
                max_depth=MAX_CASCADE_DEPTH - 1,
            )
    return {
        "depth_reached": result.depth_reached,
        "bound_violated": result.bound_violated,
    }


CASE_HIGH_BOUND = Case(
    stage="adversarial.cascade",
    name="depth_bound_at_max_minus_one_no_violation",
    intent="With max_depth=MAX-1 on a chain that converges at depth 1, "
           "bound_violated stays False — bound check uses '>=' correctly",
    setup=_setup_deep_chain,
    run=H.safe_pipeline(_run_high_bound),
    expected=lambda _ctx: {"bound_violated": False},
    assertion=lambda a, e, c: (
        (not a.get("bound_violated"),
         "" if not a.get("bound_violated")
         else f"bound_violated unexpectedly")
    ),
    failure_mode_under_test=(
        "off-by-one in depth bound check causes False-positive "
        "violations on shallow cascades"
    ),
    expected_behavior="specified",
    domain="extraction",
)


CASES = [
    CASE_FANOUT,
    CASE_DEEP_CHAIN,
    CASE_CYCLE,
    CASE_MISSING_ENTITY,
    CASE_CROSS_BRANCH,
    CASE_NOOP,
    CASE_GOAL_HEALTH,
    CASE_DEC_NO_OBS,
    CASE_EMPTY_SCOPE,
    CASE_HIGH_BOUND,
]
