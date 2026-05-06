"""Falsifier validation stage — adequacy + the 5 evaluator outcomes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg

from services.models.falsifier import is_adequate_falsifier
from services.workers.deadline_resolver.evaluators import (
    EvaluationContext,
    evaluate_falsifier,
)
from lib.shared.ids import uuid7

from . import _fixtures as F
from ._runner import Case


# =====================================================================
# F1 — Adequacy: observation_pattern with too-short pattern fails
# =====================================================================


async def _setup_adequacy_short(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    return {}


async def _run_adequacy_short(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    falsifier = {
        "kind": "observation_pattern",
        "pattern": "tooshort",  # < 20 chars
        "within_window": "P7D",
    }
    ok, reason = is_adequate_falsifier(falsifier)
    return {"ok": ok, "reason": reason}


def _expected_adequacy_short(_ctx: dict) -> dict:
    return {"ok": False, "reason_substring": "vague"}


def _assert_adequacy_short(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["ok"] is not False:
        return False, f"expected adequacy=False, got {actual}"
    if "vague" not in (actual["reason"] or "").lower():
        return False, f"reason should mention vague pattern; got {actual['reason']!r}"
    return True, ""


CASE_ADEQ_SHORT = Case(
    stage="falsifier",
    name="adequacy_observation_pattern_too_short",
    intent="observation_pattern with pattern < 20 chars fails adequacy",
    setup=_setup_adequacy_short,
    run=_run_adequacy_short,
    expected=_expected_adequacy_short,
    assertion=_assert_adequacy_short,
)


# =====================================================================
# F2 — Adequacy: well-formed observation_pattern passes
# =====================================================================


async def _run_adequacy_ok(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    falsifier = {
        "kind": "observation_pattern",
        "pattern": "fewer than 3 PRs merged this week per engineer",
        "within_window": "P7D",
    }
    ok, reason = is_adequate_falsifier(falsifier)
    return {"ok": ok, "reason": reason}


def _expected_adequacy_ok(_ctx: dict) -> dict:
    return {"ok": True}


def _assert_adequacy_ok(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["ok"] is not True:
        return False, f"expected adequacy=True, got {actual}"
    return True, ""


CASE_ADEQ_OK = Case(
    stage="falsifier",
    name="adequacy_observation_pattern_ok",
    intent="Well-formed observation_pattern passes adequacy",
    setup=_setup_adequacy_short,
    run=_run_adequacy_ok,
    expected=_expected_adequacy_ok,
    assertion=_assert_adequacy_ok,
)


# =====================================================================
# F3 — Adequacy: prediction_deadline in past fails
# =====================================================================


async def _run_adeq_pred_past(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    falsifier = {
        "kind": "prediction_deadline",
        "evaluate_at": past,
        "check": "Commitment 00000000-0000-0000-0000-000000000001 in state doneverified",
    }
    ok, reason = is_adequate_falsifier(falsifier)
    return {"ok": ok, "reason": reason}


def _expected_pred_past(_ctx: dict) -> dict:
    return {"ok": False, "reason_substring": "past"}


def _assert_pred_past(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["ok"] is not False:
        return False, f"expected False; got {actual}"
    if "past" not in (actual["reason"] or "").lower():
        return False, f"reason should mention past; got {actual['reason']!r}"
    return True, ""


CASE_PRED_PAST = Case(
    stage="falsifier",
    name="adequacy_prediction_deadline_in_past",
    intent="prediction_deadline.evaluate_at in past fails adequacy",
    setup=_setup_adequacy_short,
    run=_run_adeq_pred_past,
    expected=_expected_pred_past,
    assertion=_assert_pred_past,
)


# =====================================================================
# F4 — Evaluator: observation_pattern with matching obs → 'violated'
# =====================================================================


async def _setup_eval_obs_violated(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            # Create a "prediction" model with created_at ~ 1 hour ago (use last_retrieved as proxy).
            # The evaluator only needs prediction_created_at on EvaluationContext, not actual model.
            # Insert an observation that matches the pattern within the window.
            await F.make_observation(
                conn, tenant,
                content_text="severe latency degradation observed across services",
                actor_id=actor,
                occurred_at=F.isoplus(-1800),  # 30 min ago
            )
            return {"tenant": tenant}


async def _run_eval_obs_violated(pool: asyncpg.Pool, ctx: dict) -> dict:
    falsifier = {
        "kind": "observation_pattern",
        "pattern": "severe latency degradation across services exceeding budget",
        "within_window": "1 day",
        "direction": "violates",
    }
    async with pool.acquire() as conn:
        ec = EvaluationContext(
            conn=conn,
            tenant_id=ctx["tenant"],
            prediction_id=uuid7(),
            prediction_created_at=F.isoplus(-3600),  # 1 hour ago
        )
        outcome = await evaluate_falsifier(falsifier, ec)
    return {"outcome": outcome}


def _expected_eval_obs_violated(_ctx: dict) -> dict:
    return {"outcome": "violated"}


def _assert_eval_obs_violated(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual != expected:
        return False, f"got {actual} expected {expected}"
    return True, ""


CASE_EVAL_OBS_VIO = Case(
    stage="falsifier",
    name="eval_observation_pattern_violated",
    intent="observation_pattern: matching obs in window → 'violated'",
    setup=_setup_eval_obs_violated,
    run=_run_eval_obs_violated,
    expected=_expected_eval_obs_violated,
    assertion=_assert_eval_obs_violated,
)


# =====================================================================
# F5 — Evaluator: observation_pattern with no matching obs → 'inconclusive'
# =====================================================================


async def _setup_eval_obs_inconclusive(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            # Insert an UNRELATED observation
            await F.make_observation(
                conn, tenant,
                content_text="lunch ordered for the team",
                actor_id=actor,
                occurred_at=F.isoplus(-1800),
            )
            return {"tenant": tenant}


async def _run_eval_obs_inconclusive(pool: asyncpg.Pool, ctx: dict) -> dict:
    falsifier = {
        "kind": "observation_pattern",
        "pattern": "severe latency degradation across services exceeding budget",
        "within_window": "1 day",
    }
    async with pool.acquire() as conn:
        ec = EvaluationContext(
            conn=conn,
            tenant_id=ctx["tenant"],
            prediction_id=uuid7(),
            prediction_created_at=F.isoplus(-3600),
        )
        outcome = await evaluate_falsifier(falsifier, ec)
    return {"outcome": outcome}


def _expected_eval_obs_inconclusive(_ctx: dict) -> dict:
    return {"outcome": "inconclusive"}


CASE_EVAL_OBS_INC = Case(
    stage="falsifier",
    name="eval_observation_pattern_inconclusive",
    intent="observation_pattern: no matching obs in window → 'inconclusive'",
    setup=_setup_eval_obs_inconclusive,
    run=_run_eval_obs_inconclusive,
    expected=_expected_eval_obs_inconclusive,
    assertion=_assert_eval_obs_violated,  # same shape: actual == expected
)


# =====================================================================
# F6 — Evaluator: commitment_outcome with state in contradicting_state → 'violated'
# =====================================================================


async def _setup_eval_commit_violated(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            commit = await F.make_commitment(
                conn, tenant, owner_id=owner, state="blocked",
            )
            return {"tenant": tenant, "commit": commit}


async def _run_eval_commit_violated(pool: asyncpg.Pool, ctx: dict) -> dict:
    falsifier = {
        "kind": "commitment_outcome",
        "commitment_ref": str(ctx["commit"]),
        "contradicting_state": ["blocked", "paused"],
    }
    async with pool.acquire() as conn:
        ec = EvaluationContext(
            conn=conn,
            tenant_id=ctx["tenant"],
            prediction_id=uuid7(),
            prediction_created_at=F.isoplus(-3600),
        )
        outcome = await evaluate_falsifier(falsifier, ec)
    return {"outcome": outcome}


def _expected_eval_commit_violated(_ctx: dict) -> dict:
    return {"outcome": "violated"}


CASE_EVAL_COMMIT_VIO = Case(
    stage="falsifier",
    name="eval_commitment_outcome_violated",
    intent="commitment_outcome: commit state == contradicting_state → 'violated'",
    setup=_setup_eval_commit_violated,
    run=_run_eval_commit_violated,
    expected=_expected_eval_commit_violated,
    assertion=_assert_eval_obs_violated,
)


# =====================================================================
# F7 — Evaluator: commitment_outcome with terminal state (not contradicting) → 'confirmed'
# =====================================================================


async def _setup_eval_commit_confirmed(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            commit = await F.make_commitment(
                conn, tenant, owner_id=owner, state="doneverified",
            )
            return {"tenant": tenant, "commit": commit}


async def _run_eval_commit_confirmed(pool: asyncpg.Pool, ctx: dict) -> dict:
    falsifier = {
        "kind": "commitment_outcome",
        "commitment_ref": str(ctx["commit"]),
        "contradicting_state": ["blocked"],
    }
    async with pool.acquire() as conn:
        ec = EvaluationContext(
            conn=conn,
            tenant_id=ctx["tenant"],
            prediction_id=uuid7(),
            prediction_created_at=F.isoplus(-3600),
        )
        outcome = await evaluate_falsifier(falsifier, ec)
    return {"outcome": outcome}


def _expected_eval_commit_confirmed(_ctx: dict) -> dict:
    return {"outcome": "confirmed"}


CASE_EVAL_COMMIT_OK = Case(
    stage="falsifier",
    name="eval_commitment_outcome_terminal_confirmed",
    intent="commitment_outcome: commit reached terminal state w/o contradiction → 'confirmed'",
    setup=_setup_eval_commit_confirmed,
    run=_run_eval_commit_confirmed,
    expected=_expected_eval_commit_confirmed,
    assertion=_assert_eval_obs_violated,
)


# =====================================================================
# F8 — Evaluator: explicit_contestation with contestations from required actors → 'violated'
# =====================================================================


async def _setup_eval_explicit(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            a1 = await F.make_actor(conn, tenant, display_name="A1")
            a2 = await F.make_actor(conn, tenant, display_name="A2")
            # The evaluator joins on observations.content->>'contested_model_id'
            # so we fix a prediction_id and reference it.
            prediction_id = uuid7()
            import json as _json
            for actor in (a1, a2):
                obs_id = uuid7()
                await conn.execute(
                    """
                    INSERT INTO observations (
                        id, tenant_id, occurred_at, ingested_at, kind,
                        source_channel, source_actor_ref, actor_id,
                        content, content_text,
                        embedding, embedding_pending,
                        trust_tier, external_id, cause_id, entities_mentioned
                    ) VALUES (
                        $1, $2, $3, $3, 'contestation',
                        'harness', NULL, $4,
                        $5::jsonb, $6, NULL, FALSE,
                        'authoritative', NULL, NULL, '[]'::jsonb
                    )
                    """,
                    obs_id, tenant, F.isoplus(-3600),
                    actor,
                    _json.dumps({"contested_model_id": str(prediction_id),
                                 "contestation_kind": "belief"}),
                    f"contestation from {actor}",
                )
            return {"tenant": tenant, "a1": a1, "a2": a2,
                    "prediction_id": prediction_id}


async def _run_eval_explicit(pool: asyncpg.Pool, ctx: dict) -> dict:
    falsifier = {
        "kind": "explicit_contestation",
        "contesting_actors": [str(ctx["a1"]), str(ctx["a2"])],
        "within_window": "30 days",
    }
    async with pool.acquire() as conn:
        ec = EvaluationContext(
            conn=conn,
            tenant_id=ctx["tenant"],
            prediction_id=ctx["prediction_id"],
            prediction_created_at=F.isoplus(-2 * 86400),  # 2 days ago
        )
        outcome = await evaluate_falsifier(falsifier, ec)
    return {"outcome": outcome}


def _expected_eval_explicit(_ctx: dict) -> dict:
    return {"outcome": "violated"}


CASE_EVAL_EXPL = Case(
    stage="falsifier",
    name="eval_explicit_contestation_violated",
    intent="explicit_contestation: required actors contested in window → 'violated'",
    setup=_setup_eval_explicit,
    run=_run_eval_explicit,
    expected=_expected_eval_explicit,
    assertion=_assert_eval_obs_violated,
)


# =====================================================================
# F9 — Adequacy: explicit_contestation with empty actors list fails
# =====================================================================


async def _run_adeq_expl_empty(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    falsifier = {
        "kind": "explicit_contestation",
        "contesting_actors": [],
    }
    ok, reason = is_adequate_falsifier(falsifier)
    return {"ok": ok, "reason": reason}


def _expected_adeq_expl_empty(_ctx: dict) -> dict:
    return {"ok": False}


def _assert_adeq_simple(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual["ok"] != expected["ok"]:
        return False, f"got {actual} expected {expected}"
    return True, ""


CASE_ADEQ_EXPL_EMPTY = Case(
    stage="falsifier",
    name="adequacy_explicit_contestation_empty",
    intent="explicit_contestation with empty actors list fails adequacy",
    setup=_setup_adequacy_short,
    run=_run_adeq_expl_empty,
    expected=_expected_adeq_expl_empty,
    assertion=_assert_adeq_simple,
)


# =====================================================================
# F10 — Adequacy: unknown kind rejected
# =====================================================================


async def _run_adeq_unknown(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    falsifier = {"kind": "made_up_kind", "anything": True}
    ok, reason = is_adequate_falsifier(falsifier)
    return {"ok": ok, "reason": reason}


def _expected_adeq_unknown(_ctx: dict) -> dict:
    return {"ok": False}


CASE_ADEQ_UNKNOWN = Case(
    stage="falsifier",
    name="adequacy_unknown_kind",
    intent="Unknown falsifier kind fails adequacy",
    setup=_setup_adequacy_short,
    run=_run_adeq_unknown,
    expected=_expected_adeq_unknown,
    assertion=_assert_adeq_simple,
)


CASES = [
    CASE_ADEQ_SHORT,
    CASE_ADEQ_OK,
    CASE_PRED_PAST,
    CASE_EVAL_OBS_VIO,
    CASE_EVAL_OBS_INC,
    CASE_EVAL_COMMIT_VIO,
    CASE_EVAL_COMMIT_OK,
    CASE_EVAL_EXPL,
    CASE_ADEQ_EXPL_EMPTY,
    CASE_ADEQ_UNKNOWN,
]
