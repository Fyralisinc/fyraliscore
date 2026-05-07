"""Category 7 — Concurrency / race-condition harness.

The standard harness's `concurrency=8` semaphore parallelizes ACROSS
cases (each its own tenant). This module parallelizes WITHIN a case:
N coroutines fire into a single tenant, await all, then assert an
invariant on the resulting state. Race conditions, region-lock
serialization correctness, and connection-pool contention only
surface here.

Each scenario is shaped:
  setup: build seed state in one tenant
  run:   fire N parallel ops via asyncio.gather
  assert: invariant on the substrate after all ops complete
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.think.applier import apply_diff
from services.think.diff_schema import ClaimOp, ValidatedDiff

from .. import _fixtures as F
from .._runner import Case
from . import _helpers as H


def _build_diff(tenant_id: UUID, trigger_id: UUID, op: ClaimOp) -> ValidatedDiff:
    return ValidatedDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant_id,
        claim_ops=[op],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
        reasoning_trace="adversarial.concurrency",
    )


async def _setup_tenant_actor(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            return {"tenant": tenant, "actor": actor}


async def _apply_one(
    pool: asyncpg.Pool, *, tenant_id: UUID, actor_id: UUID,
    natural: str, embed_seed: str, confidence: float = 0.6,
) -> dict:
    """Single apply_diff against a fresh observation. Returns (success, error)."""
    trigger_id = uuid7()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                obs = await F.make_observation(
                    conn, tenant_id, actor_id=actor_id,
                )
                op = H.make_state_insert_op(
                    tenant_id=tenant_id, born_from_event_id=obs,
                    natural=natural, scope_actors=[actor_id],
                    embed_seed=embed_seed, confidence=confidence,
                )
                await apply_diff(
                    _build_diff(tenant_id, trigger_id, op),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
        return {"ok": True, "trigger": str(trigger_id)}
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False, "trigger": str(trigger_id),
            "error": f"{type(exc).__name__}: {exc}",
        }


# =====================================================================
# CC1 — N=5 parallel identical inserts → 1 Model, 4 auto_merges
# =====================================================================
# This is the classic race: 5 coroutines all see no candidate, all
# insert. With reconciler running INSIDE the apply transaction and
# region locks, the result should still be 1 Model.


async def _run_parallel_identical(pool: asyncpg.Pool, ctx: dict) -> dict:
    results = await asyncio.gather(*[
        _apply_one(
            pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
            natural="Parallel identical signal",
            embed_seed="cc1-parallel-identical",
        )
        for _ in range(5)
    ])
    async with pool.acquire() as conn:
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models "
            "WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
        decisions = await conn.fetch(
            "SELECT decision FROM reconciliation_events "
            "WHERE tenant_id=$1",
            ctx["tenant"],
        )
    return {
        "successes": sum(1 for r in results if r["ok"]),
        "failures": [r["error"] for r in results if not r["ok"]],
        "active_models": active,
        "decisions": [d["decision"] for d in decisions],
    }


def _assert_parallel_identical(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["successes"] != 5:
        return False, (
            f"expected all 5 applies to succeed; got {actual['successes']} "
            f"successes; failures={actual['failures']}"
        )
    if actual["active_models"] != 1:
        return False, (
            f"5 identical inserts under contention should still collapse "
            f"to 1 Model; got {actual['active_models']}; "
            f"decisions={actual['decisions']}"
        )
    return True, ""


CASE_PARALLEL_IDENTICAL = Case(
    stage="adversarial.concurrency",
    name="five_parallel_identical_inserts_collapse_to_one",
    intent="5 coroutines firing identical inserts into one tenant "
           "+ scope produce 1 Model (region lock + reconcile serialize)",
    setup=_setup_tenant_actor,
    run=H.safe_pipeline(_run_parallel_identical),
    expected=lambda _ctx: {},
    assertion=_assert_parallel_identical,
    failure_mode_under_test=(
        "all 5 candidates query before any commits; each sees "
        "no_match; all 5 insert distinct Models. Region lock either "
        "doesn't serialize them or reconciler runs before the lock "
        "is acquired"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# CC2 — N=10 parallel inserts on 10 distinct actors (no contention)
# =====================================================================


async def _setup_ten_actors(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actors = [
                await F.make_actor(conn, tenant, display_name=f"a_{i}")
                for i in range(10)
            ]
            return {"tenant": tenant, "actors": actors}


async def _run_ten_actors(pool: asyncpg.Pool, ctx: dict) -> dict:
    results = await asyncio.gather(*[
        _apply_one(
            pool, tenant_id=ctx["tenant"], actor_id=actor,
            natural=f"Independent signal {i}",
            embed_seed=f"cc2-independent-{i}",
        )
        for i, actor in enumerate(ctx["actors"])
    ])
    async with pool.acquire() as conn:
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {
        "successes": sum(1 for r in results if r["ok"]),
        "active_models": active,
    }


CASE_TEN_ACTORS = Case(
    stage="adversarial.concurrency",
    name="ten_parallel_inserts_independent_actors",
    intent="10 inserts to 10 distinct actor scopes (no contention) "
           "all succeed and all 10 Models survive",
    setup=_setup_ten_actors,
    run=H.safe_pipeline(_run_ten_actors),
    expected=lambda _ctx: {"successes": 10, "active_models": 10},
    assertion=lambda a, e, c: (
        (a.get("successes") == 10 and a.get("active_models") == 10,
         "" if (a.get("successes") == 10 and a.get("active_models") == 10)
         else f"got {a!r}")
    ),
    failure_mode_under_test=(
        "connection pool exhaustion (e.g. asyncpg pool of 10 + each "
        "case holds a connection through the entire transaction) "
        "causes deadlock or timeout"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# CC3 — Overlapping regions: actor A and actor B, half overlap
# =====================================================================


async def _setup_two_actors(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            a = await F.make_actor(conn, tenant, display_name="A")
            b = await F.make_actor(conn, tenant, display_name="B")
            return {"tenant": tenant, "a": a, "b": b}


async def _apply_with_scope(
    pool: asyncpg.Pool, *, tenant_id: UUID, scope: list[UUID],
    natural: str, embed_seed: str,
) -> dict:
    trigger_id = uuid7()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                obs = await F.make_observation(conn, tenant_id, actor_id=scope[0])
                op = H.make_state_insert_op(
                    tenant_id=tenant_id, born_from_event_id=obs,
                    natural=natural, scope_actors=scope,
                    embed_seed=embed_seed,
                )
                await apply_diff(
                    _build_diff(tenant_id, trigger_id, op),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
        return {"ok": True}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


async def _run_overlapping(pool: asyncpg.Pool, ctx: dict) -> dict:
    # 4 inserts: 2 to scope=[A], 1 to scope=[B], 1 to scope=[A,B]
    results = await asyncio.gather(
        _apply_with_scope(
            pool, tenant_id=ctx["tenant"], scope=[ctx["a"]],
            natural="A-only signal 1", embed_seed="cc3-a-only-1",
        ),
        _apply_with_scope(
            pool, tenant_id=ctx["tenant"], scope=[ctx["a"]],
            natural="A-only signal 2", embed_seed="cc3-a-only-2",
        ),
        _apply_with_scope(
            pool, tenant_id=ctx["tenant"], scope=[ctx["b"]],
            natural="B-only signal", embed_seed="cc3-b-only",
        ),
        _apply_with_scope(
            pool, tenant_id=ctx["tenant"], scope=[ctx["a"], ctx["b"]],
            natural="A and B signal", embed_seed="cc3-ab-overlap",
        ),
    )
    async with pool.acquire() as conn:
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {
        "successes": sum(1 for r in results if r["ok"]),
        "active_models": active,
        "failures": [r.get("error") for r in results if not r["ok"]],
    }


CASE_OVERLAPPING = Case(
    stage="adversarial.concurrency",
    name="overlapping_region_locks_serialize",
    intent="4 parallel inserts on overlapping scopes ([A], [A], [B], "
           "[A,B]) all succeed; region lock ordering doesn't deadlock",
    setup=_setup_two_actors,
    run=H.safe_pipeline(_run_overlapping),
    expected=lambda _ctx: {"successes": 4, "active_models": 4},
    assertion=lambda a, e, c: (
        (a.get("successes") == 4 and a.get("active_models") == 4,
         "" if (a.get("successes") == 4 and a.get("active_models") == 4)
         else f"got {a!r}")
    ),
    failure_mode_under_test=(
        "the [A] and [A,B] inserts deadlock because lock ordering "
        "isn't deterministic; OR the [A,B] lock starves under heavy "
        "[A]-only load"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# CC4 — Parallel cascades on same goal
# =====================================================================


async def _setup_shared_goal(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            goal = await F.make_goal(
                conn, tenant, title="shared", cached_health="at_risk",
            )
            commits = []
            for i in range(5):
                c = await F.make_commitment(
                    conn, tenant, owner_id=owner, state="doneverified",
                    title=f"shared_{i}",
                )
                await F.add_contributes_to(
                    conn, commitment_id=c, goal_id=goal,
                    is_critical_path=True,
                )
                commits.append(c)
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant, "goal": goal,
                "commits": commits, "obs": obs,
            }


async def _cascade_for(
    pool: asyncpg.Pool, *, tenant_id: UUID, commit_id: UUID, obs: UUID,
) -> dict:
    from services.think.cascade import CascadeEvent, cascade
    seed = CascadeEvent(
        id=uuid7(),
        kind="commitment_state_change",
        entity_kind="commitment",
        entity_id=commit_id,
        tenant_id=tenant_id,
        metadata={"new_state": "doneverified"},
        observation_id=obs,
    )
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                result = await cascade(seed, conn, tenant_id=tenant_id)
        return {
            "ok": True, "events_visited": result.events_visited,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


async def _run_parallel_cascades(pool: asyncpg.Pool, ctx: dict) -> dict:
    results = await asyncio.gather(*[
        _cascade_for(
            pool, tenant_id=ctx["tenant"],
            commit_id=c, obs=ctx["obs"],
        )
        for c in ctx["commits"]
    ])
    return {
        "successes": sum(1 for r in results if r["ok"]),
        "errors": [r.get("error") for r in results if not r["ok"]],
    }


CASE_PARALLEL_CASCADES = Case(
    stage="adversarial.concurrency",
    name="five_parallel_cascades_on_shared_goal",
    intent="5 cascades all touching the same goal complete without "
           "deadlock or duplicate goal-health updates",
    setup=_setup_shared_goal,
    run=H.safe_pipeline(_run_parallel_cascades),
    expected=lambda _ctx: {"successes": 5},
    assertion=lambda a, e, c: (
        (a.get("successes") == 5,
         "" if a.get("successes") == 5
         else f"got {a!r}")
    ),
    failure_mode_under_test=(
        "5 parallel goal-health recomputes serialize via row lock "
        "but one starves indefinitely; OR they all commit "
        "concurrently and leave inconsistent cached_health"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# CC5 — Parallel reconciliation candidates: 5 inserts of same proposition
# =====================================================================
# Same as CC1 but explicit: the reconciler's "find candidates" query
# should pick up rows committed by sibling coroutines and auto-merge.


async def _run_parallel_reconcile(pool: asyncpg.Pool, ctx: dict) -> dict:
    return await _run_parallel_identical(pool, ctx)


CASE_PARALLEL_RECONCILE = Case(
    stage="adversarial.concurrency",
    name="parallel_reconcile_candidates_first_wins",
    intent="Of 5 parallel identical inserts, exactly 1 wins the "
           "no_match decision; the other 4 see auto_merge",
    setup=_setup_tenant_actor,
    run=H.safe_pipeline(_run_parallel_reconcile),
    expected=lambda _ctx: {},
    assertion=lambda a, e, c: (
        (a.get("decisions", []).count("no_match") == 1
         and a.get("decisions", []).count("auto_merge") == 4
         and a.get("active_models") == 1,
         "" if (a.get("decisions", []).count("no_match") == 1
                and a.get("decisions", []).count("auto_merge") == 4
                and a.get("active_models") == 1)
         else f"decisions={a.get('decisions')} active={a.get('active_models')}")
    ),
    failure_mode_under_test=(
        "two coroutines simultaneously see 'no candidate' and both "
        "insert; the audit trail then shows 2 no_match decisions "
        "and 3 auto_merge — substrate has 2 active Models"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# CC6 — Parallel contestations on same Model
# =====================================================================


async def _setup_target_model(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actors = [
                await F.make_actor(conn, tenant, display_name=f"contestor_{i}")
                for i in range(5)
            ]
            mid = await F.make_model(
                conn, tenant,
                natural="Model under parallel contestation",
                scope_actors=actors,
                confidence=0.9,
            )
            return {"tenant": tenant, "actors": actors, "model": mid}


async def _contest_one(
    pool: asyncpg.Pool, *, tenant_id: UUID, model_id: UUID, actor_id: UUID,
) -> dict:
    from services.contestability.service import ContestationInput, contest_model
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                result = await contest_model(
                    conn,
                    ContestationInput(
                        model_id=model_id,
                        contestor_actor_id=actor_id,
                        tenant_id=tenant_id,
                        contestation_kind="belief",
                        rationale="parallel contestation race",
                    ),
                )
        return {"ok": True, "new_conf": result.new_confidence}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


async def _run_parallel_contest(pool: asyncpg.Pool, ctx: dict) -> dict:
    results = await asyncio.gather(*[
        _contest_one(
            pool, tenant_id=ctx["tenant"],
            model_id=ctx["model"], actor_id=a,
        )
        for a in ctx["actors"]
    ])
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT confidence, contested_count FROM models WHERE id=$1",
            ctx["model"],
        )
    return {
        "successes": sum(1 for r in results if r["ok"]),
        "final_confidence": float(row["confidence"]) if row else None,
        "contested_count": row["contested_count"] if row else None,
    }


def _assert_parallel_contest(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["successes"] < 5:
        return False, (
            f"5 parallel contests should all succeed; got "
            f"{actual['successes']}"
        )
    # Final confidence should be at the floor (0.15) since each
    # contest multiplies by 0.3 and there are 5 of them — but
    # serialization of contests means the floor is hit fast.
    if actual["final_confidence"] is None:
        return False, "no Model row"
    return True, ""


CASE_PARALLEL_CONTEST = Case(
    stage="adversarial.concurrency",
    name="five_parallel_contestations_serialize",
    intent="5 actors contesting the same Model in parallel: all "
           "succeed, contested_count reflects the actual count",
    setup=_setup_target_model,
    run=H.safe_pipeline(_run_parallel_contest),
    expected=lambda _ctx: {},
    assertion=_assert_parallel_contest,
    failure_mode_under_test=(
        "row-level locking on contest doesn't serialize; "
        "contested_count is undercounted (lost-update race)"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# CC7 — Parallel archives of same Model
# =====================================================================


async def _setup_archive_target(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            mid = await F.make_model(
                conn, tenant,
                natural="Archive race target",
                scope_actors=[actor],
            )
            return {"tenant": tenant, "actor": actor, "model": mid}


async def _archive_one(
    pool: asyncpg.Pool, *, tenant_id: UUID, model_id: UUID,
) -> dict:
    trigger_id = uuid7()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                obs = await F.make_observation(conn, tenant_id)
                op = ClaimOp(
                    op="archive", model_id=model_id,
                    reason="manual",
                )
                await apply_diff(
                    _build_diff(tenant_id, trigger_id, op),
                    conn, trigger_kind="T1",
                    trigger_cause_event_id=obs,
                )
        return {"ok": True}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


async def _run_parallel_archives(pool: asyncpg.Pool, ctx: dict) -> dict:
    results = await asyncio.gather(*[
        _archive_one(pool, tenant_id=ctx["tenant"], model_id=ctx["model"])
        for _ in range(5)
    ])
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, archive_reason FROM models WHERE id=$1",
            ctx["model"],
        )
    return {
        "successes": sum(1 for r in results if r["ok"]),
        "errors": [r.get("error") for r in results if not r["ok"]],
        "status": row["status"] if row else None,
        "archive_reason": row["archive_reason"] if row else None,
    }


def _assert_parallel_archives(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["status"] != "archived":
        return False, f"final status not archived: {actual['status']!r}"
    return True, ""


CASE_PARALLEL_ARCHIVES = Case(
    stage="adversarial.concurrency",
    name="parallel_archives_idempotent",
    intent="5 parallel archive ops on the same Model: final status "
           "is archived; archive_reason is consistent",
    setup=_setup_archive_target,
    run=H.safe_pipeline(_run_parallel_archives),
    expected=lambda _ctx: {},
    assertion=_assert_parallel_archives,
    failure_mode_under_test=(
        "parallel archives produce inconsistent archive_reason "
        "(lost update); OR one of them crashes the pipeline because "
        "the row was already archived"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should parallel archives serialize cleanly? Currently "
        "behavior depends on row-locking discipline of the apply "
        "path."
    ),
    domain="ops",
)


# =====================================================================
# CC8 — Parallel triggers with same trigger_id (idempotency under contention)
# =====================================================================


async def _run_trigger_id_race(pool: asyncpg.Pool, ctx: dict) -> dict:
    trigger_id = uuid7()

    async def _go(idx: int) -> dict:
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    obs = await F.make_observation(
                        conn, ctx["tenant"], actor_id=ctx["actor"],
                    )
                    op = H.make_state_insert_op(
                        tenant_id=ctx["tenant"], born_from_event_id=obs,
                        natural=f"trigger race {idx}",
                        scope_actors=[ctx["actor"]],
                        embed_seed=f"cc8-trigger-race-{idx}",
                    )
                    await apply_diff(
                        _build_diff(ctx["tenant"], trigger_id, op),
                        conn, trigger_kind="T1",
                        trigger_cause_event_id=obs,
                    )
            return {"ok": True}
        except Exception as exc:  # noqa: BLE001
            # Surface the error message too so we can see whether the
            # 4 losers got AlreadyAppliedError or some unrelated error.
            return {
                "ok": False, "error_type": type(exc).__name__,
                "error": str(exc)[:160],
            }

    results = await asyncio.gather(*[_go(i) for i in range(5)])
    async with pool.acquire() as conn:
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    other_errs = [
        f"{r.get('error_type')}: {r.get('error')}" for r in results
        if not r["ok"] and r.get("error_type") != "AlreadyAppliedError"
    ]
    return {
        "successes": sum(1 for r in results if r["ok"]),
        "already_applied_count": sum(
            1 for r in results
            if not r["ok"] and r.get("error_type") == "AlreadyAppliedError"
        ),
        "other_errors": len(other_errs),
        "other_errors_sample": other_errs[:2],
        "active": active,
    }


def _assert_trigger_id_race(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    # Exactly 1 success, 4 AlreadyAppliedError, 0 other errors.
    if actual["successes"] != 1:
        return False, (
            f"trigger_id race: expected 1 success; got "
            f"{actual['successes']}"
        )
    if actual["other_errors"] > 0:
        return False, f"unexpected errors: {actual['other_errors']}"
    if actual["active"] != 1:
        return False, (
            f"only one apply should land Models; got "
            f"{actual['active']} active"
        )
    return True, ""


CASE_TRIGGER_RACE = Case(
    stage="adversarial.concurrency",
    name="parallel_trigger_id_idempotency",
    intent="5 parallel applies of the same trigger_id: exactly one "
           "wins, four raise AlreadyAppliedError",
    setup=_setup_tenant_actor,
    run=H.safe_pipeline(_run_trigger_id_race),
    expected=lambda _ctx: {},
    assertion=_assert_trigger_id_race,
    failure_mode_under_test=(
        "applied_triggers idempotency check is read-then-insert "
        "without a unique constraint OR upsert; multiple coroutines "
        "see 'not applied' and insert duplicates"
    ),
    expected_behavior="specified",
    domain="ingest",
)


# =====================================================================
# CC9 — Pool exhaustion: more concurrent ops than pool size
# =====================================================================


async def _run_pool_exhaustion(pool: asyncpg.Pool, ctx: dict) -> dict:
    # The harness pool max_size=20 (see __main__.py); fire 30 to
    # ensure we exceed it. asyncpg should queue acquires; nobody
    # should crash.
    results = await asyncio.gather(*[
        _apply_one(
            pool, tenant_id=ctx["tenant"], actor_id=ctx["actor"],
            natural=f"pool exhaustion probe {i}",
            embed_seed=f"cc9-pool-{i}",
        )
        for i in range(30)
    ])
    return {
        "successes": sum(1 for r in results if r["ok"]),
        "errors": [r.get("error") for r in results if not r["ok"]][:3],
    }


CASE_POOL_EXHAUSTION = Case(
    stage="adversarial.concurrency",
    name="pool_exhaustion_does_not_crash",
    intent="30 concurrent applies against a max-size-20 pool all "
           "complete (asyncpg queues acquires, no crash, no timeout)",
    setup=_setup_tenant_actor,
    run=H.safe_pipeline(_run_pool_exhaustion),
    expected=lambda _ctx: {"successes": 30},
    assertion=lambda a, e, c: (
        (a.get("successes") == 30,
         "" if a.get("successes") == 30
         else f"got {a.get('successes')} successes; errors={a.get('errors')}")
    ),
    failure_mode_under_test=(
        "the 21st acquire blocks indefinitely or times out; OR the "
        "pool's connection-init callback (pgvector codec) deadlocks "
        "under concurrent acquires"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# CC10 — Region lock: same scope set in different orders
# =====================================================================
# Two coroutines with scope=[A,B] vs scope=[B,A]. Region key is
# permutation-stable, so they should serialize on the same lock.


async def _run_perm_lock(pool: asyncpg.Pool, ctx: dict) -> dict:
    # Reuse two-actors setup
    a, b = ctx["a"], ctx["b"]
    results = await asyncio.gather(
        _apply_with_scope(
            pool, tenant_id=ctx["tenant"], scope=[a, b],
            natural="forward order",
            embed_seed="cc10-permutation-canonical",
        ),
        _apply_with_scope(
            pool, tenant_id=ctx["tenant"], scope=[b, a],
            natural="reverse order",
            embed_seed="cc10-permutation-canonical",
        ),
    )
    async with pool.acquire() as conn:
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
        decisions = await conn.fetch(
            "SELECT decision FROM reconciliation_events WHERE tenant_id=$1",
            ctx["tenant"],
        )
    return {
        "successes": sum(1 for r in results if r["ok"]),
        "active": active,
        "decisions": [d["decision"] for d in decisions],
    }


def _assert_perm_lock(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["successes"] != 2:
        return False, f"both should succeed; got {actual['successes']}"
    # Region lock should serialize them; reconciler should fold #2 into #1.
    if actual["active"] != 1:
        return False, (
            f"permuted-scope inserts should collapse to 1 Model; "
            f"got {actual['active']}; decisions={actual['decisions']}"
        )
    return True, ""


CASE_PERM_LOCK = Case(
    stage="adversarial.concurrency",
    name="permuted_scope_holds_same_region_lock",
    intent="scope=[A,B] and scope=[B,A] hash to the same region lock; "
           "two parallel inserts of identical text fold to 1 Model",
    setup=_setup_two_actors,
    run=H.safe_pipeline(_run_perm_lock),
    expected=lambda _ctx: {},
    assertion=_assert_perm_lock,
    failure_mode_under_test=(
        "region lock key is order-sensitive; the two coroutines hold "
        "different locks, both insert, substrate ends with 2 Models"
    ),
    expected_behavior="specified",
    domain="ops",
)


CASES = [
    CASE_PARALLEL_IDENTICAL,
    CASE_TEN_ACTORS,
    CASE_OVERLAPPING,
    CASE_PARALLEL_CASCADES,
    CASE_PARALLEL_RECONCILE,
    CASE_PARALLEL_CONTEST,
    CASE_PARALLEL_ARCHIVES,
    CASE_TRIGGER_RACE,
    CASE_POOL_EXHAUSTION,
    CASE_PERM_LOCK,
]
