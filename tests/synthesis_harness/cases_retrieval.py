"""Retrieval stage test cases — the four pathways, RRF fusion, and second-pass."""
from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg

from services.retrieval.primary import TriggerContext, primary_retrieve
from services.retrieval.second_pass import (
    SECOND_PASS_SPARSE_THRESHOLD,
    second_pass_expand,
    should_run_second_pass,
)

from . import _fixtures as F
from ._runner import Case


# =====================================================================
# R1 — Pathway A (structural): seed an actor → expect that actor's models
# =====================================================================


async def _setup_path_a_actor(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant, display_name="Alpha")
            other_actor = await F.make_actor(conn, tenant, display_name="Bravo")
            # In-scope model
            in_scope = await F.make_model(
                conn, tenant,
                natural="Alpha is on track",
                scope_actors=[actor],
                activation=0.9,
                embed_seed="alpha-track",
            )
            # Out-of-scope model — same tenant, no actor scope match
            out_scope = await F.make_model(
                conn, tenant,
                natural="Bravo is blocked",
                scope_actors=[other_actor],
                activation=0.9,
                embed_seed="bravo-block",
            )
            return {
                "tenant": tenant,
                "actor": actor,
                "in_scope_model": in_scope,
                "out_scope_model": out_scope,
            }


async def _run_path_a_actor(pool: asyncpg.Pool, ctx: dict) -> dict:
    trigger = TriggerContext(
        kind="T1",
        tenant_id=ctx["tenant"],
        scope_actors=[ctx["actor"]],
        seed_natural_text="ping about Alpha",
        precomputed_seed_vector=F.deterministic_vector("unrelated-text"),
        seed_occurred_at=F.isoplus(0),
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await primary_retrieve(trigger, conn)
    model_ids = [str(m.id) for m in result.models]
    scores = {str(k): v for k, v in result.model_scores.items()}
    return {
        "model_ids": model_ids,
        "scores": scores,
        "pathways_run": result.notes.get("pathways_run", []),
    }


def _expected_path_a_actor(ctx: dict) -> dict:
    # In-scope must outrank out-of-scope: pathway A surfaces the in-scope
    # model, pathway B is irrelevant since the seed vector is unrelated to
    # both (so RRF fusion only awards in-scope model an A-rank contribution).
    return {
        "in_scope": str(ctx["in_scope_model"]),
        "out_scope": str(ctx["out_scope_model"]),
    }


def _assert_path_a_actor(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    ids = actual["model_ids"]
    if expected["in_scope"] not in ids:
        return False, f"in-scope model missing; ids={ids}"
    s_in = actual["scores"].get(expected["in_scope"], 0)
    s_out = actual["scores"].get(expected["out_scope"], 0)
    if s_in <= s_out:
        return False, f"in-scope score {s_in} not > out-of-scope {s_out}; ids={ids}"
    return True, ""


CASE_PATH_A_ACTOR = Case(
    stage="retrieval",
    name="pathway_A_actor_scope",
    intent="T1 with scope_actor seeds pathway A and surfaces actor-scoped Models",
    setup=_setup_path_a_actor,
    run=_run_path_a_actor,
    expected=_expected_path_a_actor,
    assertion=_assert_path_a_actor,
)


# =====================================================================
# R2 — Pathway B (semantic): seed vector → cosine match
# =====================================================================


async def _setup_path_b(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            target = await F.make_model(
                conn, tenant,
                natural="Latency spike on prod gateway",
                embed_seed="latency-spike",
                activation=0.5,
            )
            distractor = await F.make_model(
                conn, tenant,
                natural="Marketing budget approval",
                embed_seed="marketing-budget",
                activation=0.5,
            )
            return {
                "tenant": tenant,
                "target": target,
                "distractor": distractor,
            }


async def _run_path_b(pool: asyncpg.Pool, ctx: dict) -> dict:
    trigger = TriggerContext(
        kind="T1",
        tenant_id=ctx["tenant"],
        seed_natural_text="latency",
        precomputed_seed_vector=F.deterministic_vector("latency-spike"),
        seed_occurred_at=F.isoplus(0),
        semantic_k=10,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await primary_retrieve(trigger, conn)
    return {"model_ids": [str(m.id) for m in result.models]}


def _expected_path_b(ctx: dict) -> dict:
    return {
        "first_must_be": str(ctx["target"]),
    }


def _assert_path_b(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    ids = actual["model_ids"]
    if not ids:
        return False, "no models returned"
    if ids[0] != expected["first_must_be"]:
        return False, f"top model {ids[0]} != target {expected['first_must_be']}; full={ids}"
    return True, ""


CASE_PATH_B = Case(
    stage="retrieval",
    name="pathway_B_semantic_top1",
    intent="Pathway B ranks the cosine-nearest Model first",
    setup=_setup_path_b,
    run=_run_path_b,
    expected=_expected_path_b,
    assertion=_assert_path_b,
)


# =====================================================================
# R3 — Pathway C (temporal): seed time → only models with last_retrieved_at in window
# =====================================================================


async def _setup_path_c(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            now = F.isoplus(0)
            # In-window model (recently retrieved → within 1 day window)
            in_win = await F.make_model(
                conn, tenant,
                natural="Recent thing",
                scope_actors=[actor],
                last_retrieved_at=F.isoplus(-3600),  # 1 hour ago
                embed_seed="recent",
            )
            # Out-of-window model (last retrieved 30 days ago, outside default 7-day window)
            out_win = await F.make_model(
                conn, tenant,
                natural="Old thing",
                scope_actors=[actor],
                last_retrieved_at=F.isoplus(-30 * 86400),
                embed_seed="old",
            )
            # Recent observation by this actor
            obs = await F.make_observation(
                conn, tenant,
                content_text="something happened",
                actor_id=actor,
                occurred_at=F.isoplus(-1800),  # 30 min ago
            )
            return {
                "tenant": tenant,
                "actor": actor,
                "in_win": in_win,
                "out_win": out_win,
                "obs_id": obs,
            }


def _expected_path_c(ctx: dict) -> dict:
    return {
        "must_include_obs": [str(ctx["obs_id"])],
        "must_include_model": str(ctx["in_win"]),
        "out_win": str(ctx["out_win"]),
    }


async def _run_path_c(pool: asyncpg.Pool, ctx: dict) -> dict:
    trigger = TriggerContext(
        kind="T1",
        tenant_id=ctx["tenant"],
        scope_actors=[ctx["actor"]],
        seed_natural_text="probe",
        precomputed_seed_vector=F.deterministic_vector("probe-unrelated"),
        seed_occurred_at=F.isoplus(0),
        temporal_window=timedelta(days=1),
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await primary_retrieve(trigger, conn)
    return {
        "model_ids": [str(m.id) for m in result.models],
        "obs_ids": [str(o.id) for o in result.observations],
        "scores": {str(k): v for k, v in result.model_scores.items()},
        "pathway_results": [
            {"name": p.source_pathway, "model_ids": [str(m.id) for m in p.models]}
            for p in result.pathway_results
        ],
    }


def _assert_path_c(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    obs_ids = set(actual["obs_ids"])
    if expected["must_include_obs"][0] not in obs_ids:
        return False, f"observation missing; got obs_ids={obs_ids}"
    # Pathway C must surface the in-window model — confirm via the per-pathway result.
    path_c = next((p for p in actual["pathway_results"] if p["name"] == "C"), None)
    if path_c is None:
        return False, "pathway C did not run"
    if expected["must_include_model"] not in path_c["model_ids"]:
        return False, f"in-window model not in pathway C; got C={path_c['model_ids']}"
    if expected["out_win"] in path_c["model_ids"]:
        return False, f"out-of-window model leaked into pathway C; got C={path_c['model_ids']}"
    return True, ""


CASE_PATH_C = Case(
    stage="retrieval",
    name="pathway_C_temporal_window",
    intent="Pathway C surfaces in-window obs/models and excludes out-of-window",
    setup=_setup_path_c,
    run=_run_path_c,
    expected=_expected_path_c,
    assertion=_assert_path_c,
)


# =====================================================================
# R4 — Pathway D (pattern): T4 with seed_signature → matching pattern + instances
# =====================================================================


async def _setup_path_d(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            sig = {"type": "weekly_dip", "domain": "support"}
            pattern_mid = await F.make_model(
                conn, tenant,
                natural="weekly dip pattern",
                proposition={"kind": "pattern", "signature": sig, "id": "p-001"},
                embed_seed="pattern-weekly-dip",
            )
            # Instance referencing the pattern
            inst_mid = await F.make_model(
                conn, tenant,
                natural="instance of weekly dip",
                proposition={
                    "kind": "pattern_instance",
                    "pattern_id": str(pattern_mid),
                    "instance_id": "i-1",
                },
                embed_seed="instance-1",
            )
            # Decoy pattern (different signature)
            decoy = await F.make_model(
                conn, tenant,
                natural="other pattern",
                proposition={"kind": "pattern", "signature": {"type": "spike"}},
                embed_seed="decoy",
            )
            return {
                "tenant": tenant,
                "pattern": pattern_mid,
                "instance": inst_mid,
                "decoy": decoy,
                "sig": sig,
            }


async def _run_path_d(pool: asyncpg.Pool, ctx: dict) -> dict:
    trigger = TriggerContext(
        kind="T4",
        tenant_id=ctx["tenant"],
        subkind="background_pattern",
        seed_signature=ctx["sig"],
        seed_natural_text="pattern review",
        precomputed_seed_vector=F.deterministic_vector("pattern-review"),
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await primary_retrieve(trigger, conn)
    return {"model_ids": [str(m.id) for m in result.models]}


def _expected_path_d(ctx: dict) -> dict:
    return {
        "must_include": [str(ctx["pattern"]), str(ctx["instance"])],
        "must_exclude": [str(ctx["decoy"])],
    }


def _assert_path_d(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    ids = set(actual["model_ids"])
    missing = [i for i in expected["must_include"] if i not in ids]
    leaked = [i for i in expected["must_exclude"] if i in ids]
    if missing or leaked:
        return False, f"missing={missing} leaked={leaked} ids={ids}"
    return True, ""


CASE_PATH_D = Case(
    stage="retrieval",
    name="pathway_D_pattern_signature",
    intent="T4 with seed_signature returns matching pattern and its instances",
    setup=_setup_path_d,
    run=_run_path_d,
    expected=_expected_path_d,
    assertion=_assert_path_d,
)


# =====================================================================
# R5 — RRF fusion: model present in pathway A AND pathway B should rank
#       above one present only in B at lower rank
# =====================================================================


async def _setup_rrf(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            # Model X — actor-scoped (pathway A) AND semantically near (pathway B)
            x = await F.make_model(
                conn, tenant,
                natural="combined hit",
                scope_actors=[actor],
                embed_seed="combined-hit",
                activation=0.5,
            )
            # Model Y — only semantically further; no actor scope
            y = await F.make_model(
                conn, tenant,
                natural="far semantic only",
                embed_seed="far-semantic",
                activation=0.9,
            )
            return {"tenant": tenant, "actor": actor, "x": x, "y": y}


async def _run_rrf(pool: asyncpg.Pool, ctx: dict) -> dict:
    trigger = TriggerContext(
        kind="T1",
        tenant_id=ctx["tenant"],
        scope_actors=[ctx["actor"]],
        seed_natural_text="combined",
        # Vector matches X's seed exactly (cosine 0)
        precomputed_seed_vector=F.deterministic_vector("combined-hit"),
        seed_occurred_at=F.isoplus(0),
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            result = await primary_retrieve(trigger, conn)
    ids = [str(m.id) for m in result.models]
    scores = {str(k): v for k, v in result.model_scores.items()}
    return {"ids": ids, "scores": scores}


def _expected_rrf(ctx: dict) -> dict:
    return {
        "x_first": str(ctx["x"]),
        "y_present": str(ctx["y"]),
    }


def _assert_rrf(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    ids = actual["ids"]
    if not ids or ids[0] != expected["x_first"]:
        return False, f"X (multi-pathway) not first; ids={ids} scores={actual['scores']}"
    if expected["y_present"] not in ids:
        return False, f"Y missing from result; ids={ids}"
    # Score check
    sx = actual["scores"].get(expected["x_first"], 0)
    sy = actual["scores"].get(expected["y_present"], 0)
    if sx <= sy:
        return False, f"X score {sx} not strictly > Y {sy}"
    return True, ""


CASE_RRF = Case(
    stage="retrieval",
    name="rrf_fusion_multipathway",
    intent="RRF fusion: A∩B model outranks B-only model with higher activation",
    setup=_setup_rrf,
    run=_run_rrf,
    expected=_expected_rrf,
    assertion=_assert_rrf,
)


# =====================================================================
# R6 — Second-pass decision: sparse primary should activate second pass
# =====================================================================


async def _setup_sparse(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            # Only 2 models — under sparse threshold (5)
            for i in range(2):
                await F.make_model(
                    conn, tenant,
                    natural=f"thing {i}",
                    scope_actors=[actor],
                    embed_seed=f"thing-{i}",
                )
            return {"tenant": tenant, "actor": actor}


async def _run_sparse(pool: asyncpg.Pool, ctx: dict) -> dict:
    trigger = TriggerContext(
        kind="T1",
        tenant_id=ctx["tenant"],
        scope_actors=[ctx["actor"]],
        seed_natural_text="probe",
        precomputed_seed_vector=F.deterministic_vector("thing-0"),
        seed_occurred_at=F.isoplus(0),
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            primary = await primary_retrieve(trigger, conn)
    decision = should_run_second_pass(primary, trigger)
    return {
        "n_primary_models": len(primary.models),
        "should_run": decision.run,
        "trigger_condition": decision.trigger_condition,
    }


def _expected_sparse(_ctx: dict) -> dict:
    return {
        "should_run": True,
        "expected_condition_substring": "sparse",
        "max_primary_models": SECOND_PASS_SPARSE_THRESHOLD - 1,
    }


def _assert_sparse(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["n_primary_models"] >= 5:
        return False, f"primary unexpectedly populous: {actual['n_primary_models']}"
    if not actual["should_run"]:
        return False, f"second pass not activated: {actual}"
    if "sparse" not in (actual.get("trigger_condition") or "").lower():
        return False, f"trigger_condition not sparse: {actual.get('trigger_condition')!r}"
    return True, ""


CASE_SECOND_PASS_SPARSE = Case(
    stage="retrieval",
    name="second_pass_sparse_activation",
    intent="Sparse primary result triggers second-pass with reason 'sparse'",
    setup=_setup_sparse,
    run=_run_sparse,
    expected=_expected_sparse,
    assertion=_assert_sparse,
)


CASES = [
    CASE_PATH_A_ACTOR,
    CASE_PATH_B,
    CASE_PATH_C,
    CASE_PATH_D,
    CASE_RRF,
    CASE_SECOND_PASS_SPARSE,
]
