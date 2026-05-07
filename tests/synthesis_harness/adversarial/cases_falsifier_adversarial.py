"""Category 5 — Falsifier adversarial cases.

The falsifier system is the substrate's epistemic backbone. The
existing harness covers grammar (8 ISO + 8 human + 8 malformed) and
adequacy/evaluator outcomes for observation_pattern,
commitment_outcome, and explicit_contestation. These adversarial
cases hit the under-covered seams:

  * prediction_deadline EVALUATION (only adequacy tested today)
  * resource_threshold — kind never exercised at all
  * observation_pattern direction='confirms' — only 'violates' tested
  * explicit_contestation partial-match (1 of N actors)
  * Falsifier-never-fires (passes adequacy, never observable)
  * Tautological falsifier
  * High-confidence claim with weak falsifier
  * Self-referential observable_via
  * Multiple competing falsifiers
  * Edge-case window forms
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg

from lib.shared.errors import MalformedFalsifierError
from lib.shared.ids import uuid7
from services.models.falsifier import (
    is_adequate_falsifier,
    parse_within_window,
)
from services.workers.deadline_resolver.evaluators import (
    EvaluationContext,
    evaluate_falsifier,
)

from .. import _fixtures as F
from .._runner import Case
from . import _helpers as H


async def _setup_blank(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    return {}


async def _setup_with_actor(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            return {"tenant": tenant, "actor": actor}


# =====================================================================
# FA1 — prediction_deadline evaluator: future deadline → 'inconclusive'
# =====================================================================


async def _run_pred_future(pool: asyncpg.Pool, ctx: dict) -> dict:
    falsifier = {
        "kind": "prediction_deadline",
        "evaluate_at": (datetime.now(timezone.utc) + timedelta(days=14)).isoformat(),
        "check": f"Model {uuid7()} confidence < 0.5",
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


CASE_PRED_FUTURE = Case(
    stage="adversarial.falsifier",
    name="prediction_deadline_future_inconclusive",
    intent="prediction_deadline whose evaluate_at is in the future "
           "must evaluate to 'inconclusive' (the deadline hasn't fired)",
    setup=_setup_with_actor,
    run=H.safe_pipeline(_run_pred_future),
    expected=lambda _ctx: {"outcome": "inconclusive"},
    assertion=lambda a, e, c: (
        (a.get("outcome") == "inconclusive",
         "" if a.get("outcome") == "inconclusive"
         else f"got {a.get('outcome')!r}")
    ),
    failure_mode_under_test=(
        "evaluator fires the check expression early; produces "
        "'confirmed' or 'violated' before the deadline has actually "
        "arrived"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# FA2 — prediction_deadline: past deadline + check satisfied → 'confirmed'
# =====================================================================


async def _setup_pred_past(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            commit = await F.make_commitment(
                conn, tenant, owner_id=owner, state="doneverified",
            )
            return {"tenant": tenant, "commit": commit}


async def _run_pred_past(pool: asyncpg.Pool, ctx: dict) -> dict:
    falsifier = {
        "kind": "prediction_deadline",
        # The is_adequate_falsifier check rejects past evaluate_at,
        # but the evaluator should still produce a definitive verdict
        # when called after the deadline has passed.
        "evaluate_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
        "check": f"Commitment {ctx['commit']} in state doneverified",
    }
    async with pool.acquire() as conn:
        ec = EvaluationContext(
            conn=conn,
            tenant_id=ctx["tenant"],
            prediction_id=uuid7(),
            prediction_created_at=F.isoplus(-86400),
        )
        outcome = await evaluate_falsifier(falsifier, ec)
    return {"outcome": outcome}


CASE_PRED_PAST_CONFIRMED = Case(
    stage="adversarial.falsifier",
    name="prediction_deadline_past_check_satisfied",
    intent="A prediction_deadline past its evaluate_at, with the "
           "check satisfied, evaluates to 'confirmed'",
    setup=_setup_pred_past,
    run=H.safe_pipeline(_run_pred_past),
    expected=lambda _ctx: {"outcome": "confirmed"},
    assertion=lambda a, e, c: (
        (a.get("outcome") == "confirmed",
         "" if a.get("outcome") == "confirmed"
         else f"got {a.get('outcome')!r}")
    ),
    failure_mode_under_test=(
        "evaluator returns 'inconclusive' when the deadline has "
        "passed but the referenced commitment is in the predicted "
        "state — confirmation never happens"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# FA3 — resource_threshold adequacy
# =====================================================================


async def _run_rt_adequacy(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    good = {
        "kind": "resource_threshold",
        "resource_ref": str(uuid7()),
        "threshold": {"metric": "balance", "below": 0.0},
    }
    bad_no_ref = {
        "kind": "resource_threshold",
        "threshold": {"metric": "balance", "below": 0.0},
    }
    bad_no_threshold = {
        "kind": "resource_threshold",
        "resource_ref": str(uuid7()),
    }
    return {
        "good": is_adequate_falsifier(good)[0],
        "bad_no_ref": is_adequate_falsifier(bad_no_ref)[0],
        "bad_no_threshold": is_adequate_falsifier(bad_no_threshold)[0],
    }


CASE_RT_ADEQUACY = Case(
    stage="adversarial.falsifier",
    name="resource_threshold_adequacy",
    intent="resource_threshold passes adequacy with both ref+threshold; "
           "fails with either missing — kind not exercised by existing harness",
    setup=_setup_blank,
    run=H.safe_pipeline(_run_rt_adequacy),
    expected=lambda _ctx: {
        "good": True, "bad_no_ref": False, "bad_no_threshold": False,
    },
    assertion=lambda a, e, c: (
        (a == e, "" if a == e else f"got {a!r}")
    ),
    failure_mode_under_test=(
        "resource_threshold adequacy regresses (e.g. accepts a "
        "missing threshold), and no test catches it"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# FA4 — observation_pattern direction='confirms' evaluation
# =====================================================================
# When direction is 'confirms' AND a matching observation appears in
# the window, the evaluator should emit 'confirmed', not 'violated'.


async def _setup_confirms(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            await F.make_observation(
                conn, tenant,
                content_text=(
                    "deployment health check confirmed all 200 OK across "
                    "all regions for the past hour"
                ),
                actor_id=actor,
                occurred_at=F.isoplus(-1800),
            )
            return {"tenant": tenant, "actor": actor}


async def _run_confirms(pool: asyncpg.Pool, ctx: dict) -> dict:
    falsifier = {
        "kind": "observation_pattern",
        "pattern": "deployment health check confirmed all 200 OK across all regions",
        "within_window": "P1D",
        "direction": "confirms",
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


CASE_CONFIRMS = Case(
    stage="adversarial.falsifier",
    name="observation_pattern_direction_confirms",
    intent="observation_pattern with direction='confirms' and a "
           "matching observation in window evaluates as 'confirmed' "
           "(not 'violated')",
    setup=_setup_confirms,
    run=H.safe_pipeline(_run_confirms),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "evaluator ignores the direction field and always reports "
        "'violated' when a match is found, regardless of whether the "
        "match confirms or contradicts"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Does the substrate actually support direction='confirms'? "
        "Spec §10 mentions 'specific signal shape would CONTRADICT' — "
        "the confirms direction may be out of scope. Document the "
        "intended semantics."
    ),
    domain="extraction",
)


# =====================================================================
# FA5 — explicit_contestation: partial match (1 of 2 required actors)
# =====================================================================


async def _setup_partial(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            a1 = await F.make_actor(conn, tenant, display_name="Required-1")
            a2 = await F.make_actor(conn, tenant, display_name="Required-2")
            prediction_id = uuid7()
            # Contest from a1 ONLY
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
                a1,
                json.dumps({
                    "contested_model_id": str(prediction_id),
                    "contestation_kind": "belief",
                }),
                "partial contestation by required-1 only",
            )
            return {
                "tenant": tenant, "a1": a1, "a2": a2,
                "prediction_id": prediction_id,
            }


async def _run_partial(pool: asyncpg.Pool, ctx: dict) -> dict:
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
            prediction_created_at=F.isoplus(-86400),
        )
        outcome = await evaluate_falsifier(falsifier, ec)
    return {"outcome": outcome}


CASE_PARTIAL = Case(
    stage="adversarial.falsifier",
    name="explicit_contestation_partial_match",
    intent="explicit_contestation with 2 required actors and only 1 "
           "actually contesting — should be 'inconclusive' (not yet "
           "violated; still possible the second actor will contest)",
    setup=_setup_partial,
    run=H.safe_pipeline(_run_partial),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "evaluator interprets 'contesting_actors' as 'any of' instead "
        "of 'all of' and reports 'violated' from a single actor's "
        "contest — falsifier fires too easily"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Define semantics: contesting_actors = 'all of' (current?) or "
        "'any of'. Spec §10 says 'authoritative contestation from "
        "specified actors' — ambiguous between the two readings."
    ),
    domain="extraction",
)


# =====================================================================
# FA6 — High-confidence claim with weak falsifier slips through validator
# =====================================================================


async def _run_weak_falsifier(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    weak = {
        "kind": "observation_pattern",
        # Just 21 chars — passes the >=20 check but is barely useful.
        "pattern": "the customer says no.",
        "within_window": "P1Y",  # 1-year window: hardly informative
    }
    ok, reason = is_adequate_falsifier(weak)
    return {"adequate": ok, "reason": reason}


CASE_WEAK_FALSIFIER = Case(
    stage="adversarial.falsifier",
    name="weak_falsifier_passes_minimal_adequacy",
    intent="A 21-char pattern + 1-year window passes adequacy but is "
           "weak — adequacy floor doesn't catch low-quality falsifiers",
    setup=_setup_blank,
    run=H.safe_pipeline(_run_weak_falsifier),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "minimum-length adequacy lets weak falsifiers pass; high-"
        "confidence Models with toothless falsifiers accumulate"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should adequacy be tied to confidence (e.g. confidence > 0.8 "
        "requires pattern >= 50 chars, window <= 30 days)? The current "
        "static 20-char floor is independent of the claim's strength."
    ),
    domain="extraction",
)


# =====================================================================
# FA7 — Falsifier with within_window in years (long-horizon predictions)
# =====================================================================


async def _run_long_window(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    cases = {}
    for spec in ("P10Y", "10 years", "P5Y", "PT100H", "P1W2DT3H"):
        try:
            td = parse_within_window(spec)
            cases[spec] = td.total_seconds() if td else None
        except MalformedFalsifierError as e:
            cases[spec] = f"raised:{e.field}"
    return {"cases": cases}


CASE_LONG_WINDOW = Case(
    stage="adversarial.falsifier",
    name="parser_long_and_compound_windows",
    intent="parse_within_window handles long horizons (10Y, P5Y) and "
           "compound forms (P1W2DT3H) — these are valid ISO-8601",
    setup=_setup_blank,
    run=H.safe_pipeline(_run_long_window),
    expected=lambda _ctx: {},
    assertion=lambda a, e, c: (
        (
            isinstance(a.get("cases", {}).get("P10Y"), float)
            and isinstance(a.get("cases", {}).get("P1W2DT3H"), float),
            "" if isinstance(a.get("cases", {}).get("P10Y"), float)
            else f"long-horizon parse failed: {a}"
        )
    ),
    failure_mode_under_test=(
        "parser silently drops compound forms or rejects multi-year "
        "horizons; long-horizon Predictions can't have falsifiers"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# FA8 — Falsifier kind is None (caller error)
# =====================================================================


async def _run_kind_none(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    cases = {}
    cases["none"] = is_adequate_falsifier(None)
    cases["empty_dict"] = is_adequate_falsifier({})
    cases["wrong_type"] = is_adequate_falsifier("hello")  # not a dict
    return {"cases": cases}


CASE_KIND_NONE = Case(
    stage="adversarial.falsifier",
    name="falsifier_kind_none_or_wrong_type",
    intent="None / empty dict / non-dict input return (False, reason) "
           "with no exception",
    setup=_setup_blank,
    run=H.safe_pipeline(_run_kind_none),
    expected=lambda _ctx: {},
    assertion=lambda a, e, c: (
        (
            (a.get("cases", {}).get("none", (None,))[0] is False)
            and (a.get("cases", {}).get("empty_dict", (None,))[0] is False)
            and (a.get("cases", {}).get("wrong_type", (None,))[0] is False),
            "" if (
                (a.get("cases", {}).get("none", (None,))[0] is False)
                and (a.get("cases", {}).get("empty_dict", (None,))[0] is False)
                and (a.get("cases", {}).get("wrong_type", (None,))[0] is False)
            )
            else f"got {a!r}"
        )
    ),
    failure_mode_under_test=(
        "is_adequate_falsifier raises on degenerate input instead of "
        "returning (False, ...) — callers crash on bad LLM output"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# FA9 — Multiple falsifiers in one Model (not currently supported)
# =====================================================================
# What does is_adequate_falsifier do when 'kind' is a list of two
# competing kinds? Current implementation reads one kind only.


async def _run_multiple(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    falsifier = {
        "kind": ["observation_pattern", "prediction_deadline"],
        "pattern": "long enough pattern for adequacy minimum length",
        "within_window": "P7D",
        "evaluate_at": (datetime.now(timezone.utc) + timedelta(days=14)).isoformat(),
        "check": "irrelevant",
    }
    ok, reason = is_adequate_falsifier(falsifier)
    return {"ok": ok, "reason": reason}


def _assert_multi_kind_rejected(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    # Acceptable: ok=False (clean reject) OR TypeError (loud crash).
    # Both prevent the bad falsifier from passing adequacy. The
    # finding is the inconsistency, not which one fires.
    if actual.get("crashed"):
        if "TypeError" in str(actual.get("error_type", "")) or \
           "unhashable" in str(actual.get("error", "")):
            return True, "loud-raise on unhashable kind (acceptable)"
        return False, f"unexpected crash: {actual.get('error')}"
    if actual.get("ok") is False:
        return True, ""
    return False, f"multi-kind passed adequacy: {actual!r}"


CASE_MULTIPLE = Case(
    stage="adversarial.falsifier",
    name="multiple_kinds_in_one_falsifier",
    intent="A falsifier whose kind is a list of two values must NOT "
           "pass adequacy; either clean reject or loud raise is OK",
    setup=_setup_blank,
    run=H.safe_pipeline(_run_multiple),
    expected=lambda _ctx: {},
    assertion=_assert_multi_kind_rejected,
    failure_mode_under_test=(
        "is_adequate_falsifier raises TypeError on unhashable kind "
        "instead of cleanly returning (False, reason). Callers that "
        "wrap in `try: is_adequate(): except: ...` may misclassify."
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should is_adequate_falsifier defensively check `isinstance(kind, "
        "str)` before the set-membership lookup? Today an unhashable "
        "kind raises TypeError; downstream callers handle this "
        "inconsistently."
    ),
    domain="extraction",
)


# =====================================================================
# FA10 — Self-referential evaluator (commitment_outcome to self)
# =====================================================================
# A Model whose commitment_outcome falsifier references its own
# born-from commitment. The evaluator can't tell — it just looks up
# the commitment by id and reads its state.


async def _setup_self_ref(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            owner = await F.make_actor(conn, tenant)
            # Commitment exists but is in 'active' state — not in
            # 'contradicting_state'. Evaluator returns 'inconclusive'.
            commit = await F.make_commitment(
                conn, tenant, owner_id=owner, state="active",
            )
            return {"tenant": tenant, "commit": commit}


async def _run_self_ref(pool: asyncpg.Pool, ctx: dict) -> dict:
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


CASE_SELF_REF = Case(
    stage="adversarial.falsifier",
    name="self_referential_commitment_outcome",
    intent="A Model whose commitment_outcome references its own "
           "commitment, with the commitment NOT in any contradicting "
           "state, evaluates as 'inconclusive' (the falsifier never "
           "actually fires)",
    setup=_setup_self_ref,
    run=H.safe_pipeline(_run_self_ref),
    expected=lambda _ctx: {"outcome": "inconclusive"},
    assertion=lambda a, e, c: (
        (a.get("outcome") == "inconclusive",
         "" if a.get("outcome") == "inconclusive"
         else f"got {a.get('outcome')!r}")
    ),
    failure_mode_under_test=(
        "the substrate has no detection of self-referential or "
        "tautological falsifiers; this Model will live forever, "
        "never confirmed nor violated"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should the validator detect tautological falsifiers (the "
        "commitment is in 'active' which can never be one of "
        "['blocked', 'paused'] without an intermediate transition)? "
        "Currently it can't — they pass adequacy and are uncatchable."
    ),
    domain="extraction",
)


# =====================================================================
# FA11 — Falsifier within_window negative parses cleanly (regression)
# =====================================================================


async def _run_negative_window(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    raised = {}
    for spec in ("P-1D", "-7 days", "P0Y0M0W0D", "-10 minutes"):
        try:
            parse_within_window(spec)
            raised[spec] = "no_raise"
        except MalformedFalsifierError as e:
            raised[spec] = e.field
        except Exception as e:  # noqa: BLE001
            raised[spec] = f"wrong_type:{type(e).__name__}"
    return {"raised": raised}


CASE_NEG_WINDOW = Case(
    stage="adversarial.falsifier",
    name="negative_or_zero_window_rejected",
    intent="P-1D, -7 days, P0Y0M0W0D all raise MalformedFalsifierError",
    setup=_setup_blank,
    run=H.safe_pipeline(_run_negative_window),
    expected=lambda _ctx: {},
    assertion=lambda a, e, c: (
        (
            all(v == "within_window" for v in a.get("raised", {}).values()),
            "" if all(v == "within_window" for v in a.get("raised", {}).values())
            else f"got {a!r}"
        )
    ),
    failure_mode_under_test=(
        "regex permits negative numbers or zero-length durations, "
        "letting falsifiers with degenerate windows pass adequacy"
    ),
    expected_behavior="specified",
    domain="extraction",
)


CASES = [
    CASE_PRED_FUTURE,
    CASE_PRED_PAST_CONFIRMED,
    CASE_RT_ADEQUACY,
    CASE_CONFIRMS,
    CASE_PARTIAL,
    CASE_WEAK_FALSIFIER,
    CASE_LONG_WINDOW,
    CASE_KIND_NONE,
    CASE_MULTIPLE,
    CASE_SELF_REF,
    CASE_NEG_WINDOW,
]
