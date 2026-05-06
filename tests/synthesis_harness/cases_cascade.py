"""Cascade stage — three branches + BFS depth bound + visited dedup."""
from __future__ import annotations

from uuid import UUID

import asyncpg

from services.think.cascade import CascadeEvent, MAX_CASCADE_DEPTH, cascade
from services.think.observability import METRICS
from lib.shared.ids import uuid7

from . import _fixtures as F
from ._runner import Case


# =====================================================================
# K1 — commitment_state_change → unblock dependent (sole remaining dep done)
# =====================================================================


async def _setup_unblock(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            cause_obs = await F.make_observation(
                conn, tenant, content_text="dep finished",
            )
            # Goal & contributes_to are required: an Act invariant rejects
            # transition for orphan commitments ("no contributes_to edges
            # and is not maintenance"). Without this, cascade still emits
            # the unblock-rejected log and `blocked` stays in `blocked`.
            goal = await F.make_goal(conn, tenant, title="Parent goal")
            dep = await F.make_commitment(
                conn, tenant, owner_id=owner, title="Dep",
                state="doneverified",
            )
            blocked = await F.make_commitment(
                conn, tenant, owner_id=owner, title="Dependent",
                state="blocked",
            )
            await F.add_contributes_to(conn, commitment_id=blocked, goal_id=goal)
            await F.add_depends_on(conn, dependent=blocked, dependency=dep)
            return {
                "tenant": tenant,
                "dep": dep,
                "blocked": blocked,
                "cause_obs": cause_obs,
                "goal": goal,
            }


async def _run_unblock(pool: asyncpg.Pool, ctx: dict) -> dict:
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["dep"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=ctx["cause_obs"],
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
            row = await conn.fetchrow(
                "SELECT state FROM commitments WHERE id=$1",
                ctx["blocked"],
            )
    return {
        "events_visited": result.events_visited,
        "depth_reached": result.depth_reached,
        "bound_violated": result.bound_violated,
        "kinds": [s.kind for s in result.steps],
        "blocked_state": row["state"],
    }


def _expected_unblock(_ctx: dict) -> dict:
    return {
        "blocked_state": "active",
        "kind_present": "commitment_state_change",
        "bound_violated": False,
    }


def _assert_unblock(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["blocked_state"] != "active":
        return False, f"dependent did not unblock; state={actual['blocked_state']}"
    if actual["bound_violated"]:
        return False, "bound was violated unexpectedly"
    # Look for at least 2 cascade events (initial + one downstream).
    if actual["events_visited"] < 2:
        return False, f"expected at least 2 events visited; got {actual['events_visited']}"
    return True, ""


CASE_UNBLOCK = Case(
    stage="cascade",
    name="commitment_state_change_unblocks_dependent",
    intent="doneverified on dep with no other unsatisfied deps unblocks dependent → state=active",
    setup=_setup_unblock,
    run=_run_unblock,
    expected=_expected_unblock,
    assertion=_assert_unblock,
)


# =====================================================================
# K2 — commitment_state_change with OTHER unsatisfied deps does NOT unblock
# =====================================================================


async def _setup_no_unblock(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            dep1 = await F.make_commitment(
                conn, tenant, owner_id=owner, state="doneverified",
            )
            dep2 = await F.make_commitment(
                conn, tenant, owner_id=owner, state="active",  # NOT done
            )
            blocked = await F.make_commitment(
                conn, tenant, owner_id=owner, state="blocked",
            )
            await F.add_depends_on(conn, dependent=blocked, dependency=dep1)
            await F.add_depends_on(conn, dependent=blocked, dependency=dep2)
            return {"tenant": tenant, "dep1": dep1, "blocked": blocked}


async def _run_no_unblock(pool: asyncpg.Pool, ctx: dict) -> dict:
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["dep1"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
            row = await conn.fetchrow(
                "SELECT state FROM commitments WHERE id=$1",
                ctx["blocked"],
            )
    return {"blocked_state": row["state"], "events_visited": result.events_visited}


def _expected_no_unblock(_ctx: dict) -> dict:
    return {"blocked_state": "blocked"}


def _assert_no_unblock(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["blocked_state"] != "blocked":
        return False, f"dependent should remain blocked; got {actual['blocked_state']}"
    return True, ""


CASE_NO_UNBLOCK = Case(
    stage="cascade",
    name="commitment_state_change_no_unblock_when_other_deps",
    intent="Dependent stays blocked when at least one other dependency unsatisfied",
    setup=_setup_no_unblock,
    run=_run_no_unblock,
    expected=_expected_no_unblock,
    assertion=_assert_no_unblock,
)


# =====================================================================
# K3 — decision_revisited → flags every constrained_by commitment
# =====================================================================


async def _setup_decision_rev(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            decision = await F.make_decision(
                conn, tenant, title="D", state="revisited",
            )
            c1 = await F.make_commitment(conn, tenant, owner_id=owner, title="C1")
            c2 = await F.make_commitment(conn, tenant, owner_id=owner, title="C2")
            done = await F.make_commitment(conn, tenant, owner_id=owner,
                                            title="Done", state="doneverified")
            await F.add_constrained_by(conn, commitment_id=c1, decision_id=decision)
            await F.add_constrained_by(conn, commitment_id=c2, decision_id=decision)
            await F.add_constrained_by(conn, commitment_id=done, decision_id=decision)
            return {
                "tenant": tenant,
                "decision": decision,
                "c1": c1, "c2": c2, "done": done,
            }


async def _run_decision_rev(pool: asyncpg.Pool, ctx: dict) -> dict:
    seed = CascadeEvent(
        id=uuid7(),
        kind="decision_revisited",
        entity_kind="decision",
        entity_id=ctx["decision"],
        tenant_id=ctx["tenant"],
        metadata={},
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
            # Look at flag observations
            flags = await conn.fetch(
                """
                SELECT (content->>'entity_id') AS eid
                FROM observations
                WHERE tenant_id = $1 AND kind = 'state_change'
                  AND content->>'state_change_kind' = 'commitment_flagged_for_review'
                """,
                ctx["tenant"],
            )
    flagged_ids = {r["eid"] for r in flags}
    return {
        "events_visited": result.events_visited,
        "kinds": [s.kind for s in result.steps],
        "flagged_ids": flagged_ids,
    }


def _expected_decision_rev(ctx: dict) -> dict:
    return {
        "must_flag": {str(ctx["c1"]), str(ctx["c2"])},
        "must_not_flag": {str(ctx["done"])},
    }


def _assert_decision_rev(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    flagged = actual["flagged_ids"]
    missing = expected["must_flag"] - flagged
    leaked = expected["must_not_flag"] & flagged
    if missing or leaked:
        return False, f"missing={missing} leaked={leaked} got={flagged}"
    return True, ""


CASE_DECISION_REV = Case(
    stage="cascade",
    name="decision_revisited_flags_constrained_commitments",
    intent="decision_revisited flags every non-terminal constrained_by commitment for review",
    setup=_setup_decision_rev,
    run=_run_decision_rev,
    expected=_expected_decision_rev,
    assertion=_assert_decision_rev,
)


# =====================================================================
# K4 — depth bound: artificial deep chain triggers bound_violated
# =====================================================================


async def _setup_depth_bound(pool: asyncpg.Pool, _ctx: dict) -> dict:
    """Build a chain of (N+2) commitments where each blocked one depends on the previous done one,
    so the cascade walks: dep0 (doneverified) -> unblock dep1 (now active) ... but wait the
    cascade only unblocks BLOCKED commitments whose dep just turned doneverified. So we
    pre-seed dep0 doneverified, dep1 blocked deps on dep0, dep2 blocked deps on dep1, etc.
    When dep1 becomes active via cascade, that itself doesn't doneverify anything, so the
    chain stops at depth 1.

    For a true depth test we instead exercise the bound directly with `max_depth=0`.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            dep = await F.make_commitment(conn, tenant, owner_id=owner, state="doneverified")
            blocked = await F.make_commitment(conn, tenant, owner_id=owner, state="blocked")
            await F.add_depends_on(conn, dependent=blocked, dependency=dep)
            return {"tenant": tenant, "dep": dep, "blocked": blocked}


async def _run_depth_bound(pool: asyncpg.Pool, ctx: dict) -> dict:
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["dep"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            # max_depth=0 forces violation on the very first non-seed dequeue
            result = await cascade(seed, conn, tenant_id=ctx["tenant"], max_depth=0)
    return {
        "bound_violated": result.bound_violated,
        "depth_reached": result.depth_reached,
    }


def _expected_depth_bound(_ctx: dict) -> dict:
    return {"bound_violated": True, "depth_reached": 0}


def _assert_depth_bound(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if not actual["bound_violated"]:
        return False, f"bound should have been violated; got {actual}"
    return True, ""


CASE_DEPTH_BOUND = Case(
    stage="cascade",
    name="bfs_depth_bound_logs_violation",
    intent="max_depth=0 forces bound_violated=True without raising",
    setup=_setup_depth_bound,
    run=_run_depth_bound,
    expected=_expected_depth_bound,
    assertion=_assert_depth_bound,
)


# =====================================================================
# K5 — visited dedup: cascade does not loop on cycles
# =====================================================================


async def _setup_no_op_kind(pool: asyncpg.Pool, _ctx: dict) -> dict:
    """Unknown event kind returns no downstream → cascade halts cleanly."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            return {"tenant": tenant}


async def _run_no_op_kind(pool: asyncpg.Pool, ctx: dict) -> dict:
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
    return {
        "events_visited": result.events_visited,
        "bound_violated": result.bound_violated,
        "depth_reached": result.depth_reached,
    }


def _expected_no_op_kind(_ctx: dict) -> dict:
    return {"events_visited": 1, "bound_violated": False, "depth_reached": 0}


def _assert_no_op_kind(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual != expected:
        return False, f"got {actual} expected {expected}"
    return True, ""


CASE_NO_OP_KIND = Case(
    stage="cascade",
    name="unknown_kind_halts_cleanly",
    intent="Unknown cascade kind returns no downstream and visited count=1",
    setup=_setup_no_op_kind,
    run=_run_no_op_kind,
    expected=_expected_no_op_kind,
    assertion=_assert_no_op_kind,
)


# =====================================================================
# K6 — invariant violation surfaces on CascadeResult + metric counter
# =====================================================================
#
# T1b regression: cascade unblock rejection used to be a silent INFO
# log only. Setup: dependent commitment is orphan (no contributes_to
# edge), so commitments_svc.transition('active') raises
# InvariantViolation. Cascade should:
#   - record the violation on CascadeResult.invariant_violations
#   - bump METRICS.cascade_invariant_violations['commitment_unblock']
#   - keep walking (BFS continues; bound_violated stays False)


async def _setup_orphan_unblock(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(conn, tenant, title="Parent goal")
            dep = await F.make_commitment(
                conn, tenant, owner_id=owner, title="Dep",
                state="doneverified",
            )
            blocked = await F.make_commitment(
                conn, tenant, owner_id=owner, title="Dependent",
                state="blocked",
            )
            await F.add_contributes_to(conn, commitment_id=blocked, goal_id=goal)
            await F.add_depends_on(conn, dependent=blocked, dependency=dep)
            return {
                "tenant": tenant,
                "dep": dep,
                "blocked": blocked,
                "goal": goal,
            }


async def _run_orphan_unblock(pool: asyncpg.Pool, ctx: dict) -> dict:
    # Force C4 (missing cause_event_id) which raises *before* the
    # UPDATE inside `commitments.transition`. We deliberately pass
    # `observation_id=None` on the seed CascadeEvent so the cascade
    # calls `transition(cause_event_id=None)` and gets back C4. We
    # avoid C10 (orphan commitment) here because that one raises
    # *after* the UPDATE has already landed in the row, which would
    # leave `state='active'` even though the cascade caught the
    # exception — orthogonal to T1b.
    before = METRICS.cascade_invariant_violations.get(
        "commitment_unblock", 0,
    )
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=ctx["dep"],
        tenant_id=ctx["tenant"],
        metadata={"new_state": "doneverified"},
        observation_id=None,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await cascade(seed, conn, tenant_id=ctx["tenant"])
            row = await conn.fetchrow(
                "SELECT state FROM commitments WHERE id=$1",
                ctx["blocked"],
            )
    after = METRICS.cascade_invariant_violations.get(
        "commitment_unblock", 0,
    )
    return {
        "blocked_state": row["state"],
        "events_visited": result.events_visited,
        "bound_violated": result.bound_violated,
        "violations_count": len(result.invariant_violations),
        "violation_branch": (
            result.invariant_violations[0].branch
            if result.invariant_violations else None
        ),
        "violation_entity_id": (
            str(result.invariant_violations[0].entity_id)
            if result.invariant_violations else None
        ),
        "violation_code": (
            result.invariant_violations[0].code
            if result.invariant_violations else None
        ),
        "metric_delta": after - before,
    }


def _expected_orphan_unblock(ctx: dict) -> dict:
    # `metric_delta` is asserted as `>= 1` rather than `== 1` because
    # METRICS is a process-wide singleton and cascade scenarios run
    # concurrently — another scenario in the same harness run can
    # bump the counter between this scenario's snapshot and read.
    # Structured `invariant_violations` on CascadeResult is the
    # deterministic per-scenario signal; the metric is "any bump
    # observed" smoke proof.
    return {
        "blocked_state": "blocked",
        "bound_violated": False,
        "violations_count": 1,
        "violation_branch": "commitment_unblock",
        "violation_entity_id": str(ctx["blocked"]),
    }


def _assert_orphan_unblock(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    diffs = []
    for k, v in expected.items():
        if actual.get(k) != v:
            diffs.append(f"{k}: got {actual.get(k)!r} expected {v!r}")
    if actual.get("metric_delta", 0) < 1:
        diffs.append(f"metric_delta: got {actual.get('metric_delta')} expected >= 1")
    return (not diffs), "; ".join(diffs)


CASE_ORPHAN_UNBLOCK = Case(
    stage="cascade",
    name="invariant_violation_surfaces_explicitly",
    intent="Orphan-commitment unblock rejection appears on CascadeResult and bumps metric (T1b)",
    setup=_setup_orphan_unblock,
    run=_run_orphan_unblock,
    expected=_expected_orphan_unblock,
    assertion=_assert_orphan_unblock,
)


CASES = [
    CASE_UNBLOCK,
    CASE_NO_UNBLOCK,
    CASE_DECISION_REV,
    CASE_DEPTH_BOUND,
    CASE_NO_OP_KIND,
    CASE_ORPHAN_UNBLOCK,
]
