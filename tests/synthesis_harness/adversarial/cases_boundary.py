"""Category 2 — Boundary and degenerate inputs.

What happens at the edges of the input space? These are mostly
substrate-mechanical (validator, applier, repos) rather than
LLM-driven, so they run cheap and produce sharp findings.

Each case targets one of: empty/single-char content, very long
content, missing entity, multi-entity overload, ambiguous
references, bot/system actors, future/stale timestamps,
missing-model references, threading/quoting.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
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
        reasoning_trace="adversarial.boundary",
    )


# =====================================================================
# B1 — Empty content_text observation can still be inserted
# =====================================================================
# What does the substrate do with an observation whose content_text
# is empty? An ingestion pipeline that strips noisy bot prefixes
# could plausibly produce one.


async def _setup_empty_content(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            return {"tenant": tenant}


async def _run_empty_content(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                obs = await F.make_observation(
                    conn, ctx["tenant"], content_text="",
                )
                return {"obs_id": str(obs), "raised": False}
            except Exception as exc:  # noqa: BLE001
                return {"raised": True, "error": str(exc)}


CASE_EMPTY_CONTENT = Case(
    stage="adversarial.boundary",
    name="empty_content_text_observation",
    intent="Empty content_text observation either inserts cleanly OR "
           "is rejected at write time — both are defensible; silent "
           "corruption (e.g. NULL embedding row) is not",
    setup=_setup_empty_content,
    run=H.safe_pipeline(_run_empty_content),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "empty content_text inserts but produces a zero-vector "
        "embedding which then matches every other zero-vector at "
        "reconcile time"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should observations require non-empty content_text at the DB "
        "level (CHECK constraint) or at the ingestion adapter? Currently "
        "neither layer enforces it; downstream Pathway B is silently "
        "vulnerable to zero-vector cosine collisions."
    ),
    domain="ingest",
)


# =====================================================================
# B2 — Single-character signal ("k", "+1") through the validator
# =====================================================================


async def _run_single_char(pool: asyncpg.Pool, ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs = await F.make_observation(
                conn, ctx["tenant"], content_text="+1",
            )
            return {"obs_id": str(obs), "ok": True}


CASE_SINGLE_CHAR = Case(
    stage="adversarial.boundary",
    name="single_character_signal",
    intent="A '+1' signal must not crash ingestion; what it produces "
           "downstream is open",
    setup=_setup_empty_content,
    run=H.safe_pipeline(_run_single_char),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "embedding service receives single token and returns "
        "degenerate vector that then collides at reconcile time"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Define the minimum-content-length contract. Reactions ('+1', "
        "'k', '👍') are common in real chat data and currently route "
        "through the same pipeline as full sentences."
    ),
    domain="ingest",
)


# =====================================================================
# B3 — Very long content (50 KB) does not get silently truncated
# =====================================================================


async def _run_huge_content(pool: asyncpg.Pool, ctx: dict) -> dict:
    huge = "the team discussed many things. " * 1700  # ≈ 51 KB
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs = await F.make_observation(
                conn, ctx["tenant"], content_text=huge,
            )
            row = await conn.fetchrow(
                "SELECT length(content_text) AS l FROM observations WHERE id=$1",
                obs,
            )
            return {
                "input_len": len(huge),
                "stored_len": int(row["l"]) if row else None,
            }


def _assert_huge_content(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["stored_len"] != actual["input_len"]:
        return False, (
            f"silent truncation: stored {actual['stored_len']} of "
            f"{actual['input_len']} chars"
        )
    return True, ""


CASE_HUGE_CONTENT = Case(
    stage="adversarial.boundary",
    name="huge_content_no_silent_truncation",
    intent="50KB content_text round-trips without truncation, OR is "
           "rejected at write time with a clear error",
    setup=_setup_empty_content,
    run=H.safe_pipeline(_run_huge_content),
    expected=lambda _ctx: {},
    assertion=_assert_huge_content,
    failure_mode_under_test=(
        "content_text is silently truncated by a column-length cap or "
        "by the embedding service token limit, losing context"
    ),
    expected_behavior="specified",
    domain="ingest",
)


# =====================================================================
# B4 — Insert claim_op with ZERO scope and zero entities — no scope
# =====================================================================
# The reconciler's _find_candidates degrades gracefully on empty scope
# (no scope filter, decides on text+kind alone). But the apply path
# may create an unreachable Model. Verify what happens.


async def _setup_no_scope(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            obs = await F.make_observation(conn, tenant)
            return {"tenant": tenant, "obs": obs, "trigger_id": uuid7()}


async def _run_no_scope(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = H.make_state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="A claim with no scope at all",
        scope_actors=[],
        scope_entities=[],
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            summary = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["tenant"],
        )
    return {"summary": summary, "model_count": count}


CASE_NO_SCOPE = Case(
    stage="adversarial.boundary",
    name="zero_scope_zero_entities_insert",
    intent="A Model with no scope_actors and no scope_entities should "
           "either be rejected or be marked unreachable; not silently "
           "join the substrate",
    setup=_setup_no_scope,
    run=H.safe_pipeline(_run_no_scope),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "scope-less Model lands in the substrate but no retrieval "
        "Pathway will surface it (Pathway A needs scope_actors, "
        "Pathway B needs proximity, Pathway C needs temporal "
        "co-occurrence) — orphan Model accumulates"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should the validator reject scope-less inserts? Production "
        "Models without scope are dead-on-arrival for retrieval, but "
        "calibration won't notice."
    ),
    domain="ingest",
)


# =====================================================================
# B5 — Insert with 25 scope_actors (multi-entity overload)
# =====================================================================


async def _setup_many_actors(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actors = []
            for i in range(25):
                aid = await F.make_actor(
                    conn, tenant, display_name=f"actor_{i}",
                )
                actors.append(aid)
            obs = await F.make_observation(conn, tenant, actor_id=actors[0])
            return {
                "tenant": tenant, "obs": obs, "actors": actors,
                "trigger_id": uuid7(),
            }


async def _run_many_actors(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = H.make_state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="status update naming everyone on the team",
        scope_actors=ctx["actors"],
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            summary = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            row = await conn.fetchrow(
                "SELECT array_length(scope_actors, 1) AS n FROM models "
                "WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 1",
                ctx["tenant"],
            )
    return {
        "summary": summary,
        "stored_actors": int(row["n"]) if row and row["n"] else 0,
    }


def _assert_many_actors(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["stored_actors"] != 25:
        return False, (
            f"scope_actors changed: stored {actual['stored_actors']}, "
            f"expected 25"
        )
    return True, ""


CASE_MANY_ACTORS = Case(
    stage="adversarial.boundary",
    name="twentyfive_scope_actors",
    intent="25 scope_actors round-trip without truncation; the region "
           "lock acquires successfully",
    setup=_setup_many_actors,
    run=H.safe_pipeline(_run_many_actors),
    expected=lambda _ctx: {},
    assertion=_assert_many_actors,
    failure_mode_under_test=(
        "scope_actors array silently truncated by some column cap, OR "
        "region lock with 25 entities exceeds the SHA-256 input cap, OR "
        "lock-acquire serializes too long to be practical"
    ),
    expected_behavior="specified",
    domain="leadership",
)


# =====================================================================
# B6 — occurred_at in the future (clock skew)
# =====================================================================


async def _run_future_occurred_at(pool: asyncpg.Pool, ctx: dict) -> dict:
    future = F.isoplus(86400 * 7)  # 7 days from now
    async with pool.acquire() as conn:
        async with conn.transaction():
            obs = await F.make_observation(
                conn, ctx["tenant"], content_text="from the future",
                occurred_at=future,
            )
            row = await conn.fetchrow(
                "SELECT occurred_at, ingested_at FROM observations WHERE id=$1",
                obs,
            )
    return {
        "occurred_at": row["occurred_at"].isoformat(),
        "ingested_at": row["ingested_at"].isoformat(),
        "future_diff_seconds": (
            (row["occurred_at"] - row["ingested_at"]).total_seconds()
        ),
    }


def _assert_future_occurred(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    # Either: rejected (raise), or normalized (occurred_at clamped to
    # ingested_at), or accepted with future timestamp. We just want
    # to record what happens; the design question is the deliverable.
    return True, ""


CASE_FUTURE_OCCURRED = Case(
    stage="adversarial.boundary",
    name="occurred_at_in_the_future",
    intent="An observation with occurred_at 7 days in the future is "
           "either rejected, normalized, or accepted; document which",
    setup=_setup_empty_content,
    run=H.safe_pipeline(_run_future_occurred_at),
    expected=lambda _ctx: {},
    assertion=_assert_future_occurred,
    failure_mode_under_test=(
        "future-dated observation passes through; downstream temporal "
        "windows (Pathway C, falsifier evaluators) treat it as 'in "
        "scope' for an indefinite future window"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should the substrate refuse future-dated observations, clamp "
        "to ingested_at, or warn? Currently no validation; clock skew "
        "from a misconfigured client could quietly poison Pathway C."
    ),
    domain="ingest",
)


# =====================================================================
# B7 — claim_op.update against a non-existent model_id
# =====================================================================


async def _setup_missing_model(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant, "obs": obs, "trigger_id": uuid7(),
                "missing_model": uuid7(),
            }


async def _run_missing_model(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = ClaimOp(
        op="update",
        model_id=ctx["missing_model"],
        changes={"confidence": 0.8},
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await apply_diff(
                    diff, conn, trigger_kind="T1",
                    trigger_cause_event_id=ctx["obs"],
                )
                return {"raised": False}
            except Exception as exc:  # noqa: BLE001
                return {
                    "raised": True,
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:240],
                }


def _assert_missing_model(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"runner crash: {actual.get('error')}"
    # We expect EITHER a clear raise (preferred) OR a documented no-op.
    # A silent success that pretends to update is the failure mode.
    return True, ""


CASE_MISSING_MODEL = Case(
    stage="adversarial.boundary",
    name="update_against_nonexistent_model_id",
    intent="claim_op.update against a model_id that doesn't exist must "
           "raise loudly OR be a documented no-op; silent success is wrong",
    setup=_setup_missing_model,
    run=H.safe_pipeline(_run_missing_model),
    expected=lambda _ctx: {},
    assertion=_assert_missing_model,
    failure_mode_under_test=(
        "applier issues an UPDATE with WHERE id=missing returning 0 "
        "rows and reports success; downstream consumers think the "
        "update landed"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Define the contract for update-against-missing. Loud raise "
        "(symmetric with insert), silent no-op (current?), or auto-archive "
        "the update. Right now this is not asserted anywhere."
    ),
    domain="ingest",
)


# =====================================================================
# B8 — Falsifier with within_window = "" (empty string)
# =====================================================================


async def _run_empty_window(_pool: asyncpg.Pool, _ctx: dict) -> dict:
    from services.models.falsifier import is_adequate_falsifier
    from lib.shared.errors import MalformedFalsifierError
    falsifier = {
        "kind": "observation_pattern",
        "pattern": "valid pattern with enough characters to pass the "
                   "minimum length requirement",
        "within_window": "",
    }
    raised = False
    err = None
    try:
        is_adequate_falsifier(falsifier)
    except MalformedFalsifierError as exc:
        raised = True
        err = exc.field
    except Exception as exc:  # noqa: BLE001
        raised = True
        err = f"wrong_type:{type(exc).__name__}"
    return {"raised": raised, "field": err}


CASE_EMPTY_WINDOW = Case(
    stage="adversarial.boundary",
    name="falsifier_within_window_empty_string",
    intent="within_window='' is treated as 'missing' (returns None), "
           "not 'malformed' (raise) — and the surrounding adequacy "
           "check rejects it for kinds where window is required",
    setup=_setup_empty_content,
    run=H.safe_pipeline(_run_empty_window),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "empty-string window slips past the regex and returns None "
        "silently, leaving the falsifier perpetually inconclusive — "
        "but documented behavior is that adequacy catches missing "
        "windows for required-window kinds"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should empty string be treated as 'missing' or 'malformed'? "
        "Currently 'missing' (returns None, adequacy fails for "
        "kinds where window is required). The distinction matters "
        "for failure_reason classification: malformed_falsifier vs "
        "no_window."
    ),
    domain="extraction",
)


# =====================================================================
# B9 — claim_op.archive on already-archived model
# =====================================================================


async def _setup_already_archived(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            obs = await F.make_observation(conn, tenant)
            mid = await F.make_model(
                conn, tenant, status="archived",
                archive_reason="superseded",
            )
            return {
                "tenant": tenant, "obs": obs, "model_id": mid,
                "trigger_id": uuid7(),
            }


async def _run_archive_archived(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = ClaimOp(
        op="archive",
        model_id=ctx["model_id"],
        reason="superseded",
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await apply_diff(
                    diff, conn, trigger_kind="T1",
                    trigger_cause_event_id=ctx["obs"],
                )
                return {"raised": False}
            except Exception as exc:  # noqa: BLE001
                return {
                    "raised": True,
                    "error_type": type(exc).__name__,
                }


CASE_ARCHIVE_ARCHIVED = Case(
    stage="adversarial.boundary",
    name="archive_already_archived_model",
    intent="claim_op.archive against an archived Model must be "
           "idempotent (no-op) or rejected; either is OK if documented",
    setup=_setup_already_archived,
    run=H.safe_pipeline(_run_archive_archived),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "archiving an archived Model overwrites archive_reason silently, "
        "losing the original archival reason"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should re-archive be idempotent (no-op) or surface as an error? "
        "Audit trail concern: original archive_reason can be lost."
    ),
    domain="ingest",
)


# =====================================================================
# B10 — Insert with confidence at the boundary clip values
# =====================================================================


async def _run_clip_low(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = H.make_state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="boundary low confidence",
        confidence=0.0,  # below the [0.05, 0.95] clip floor
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            row = await conn.fetchrow(
                "SELECT confidence FROM models WHERE tenant_id=$1 "
                "ORDER BY created_at DESC LIMIT 1",
                ctx["tenant"],
            )
    return {"confidence": float(row["confidence"]) if row else None}


def _assert_clip_low(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    # Acceptable: loud raise (current behavior — Pydantic Field
    # ge=0.05) OR clipped insert. Silent <0.05 is the failure.
    if actual.get("crashed"):
        # ValidationError on confidence is the loud-raise path.
        if "0.05" in str(actual.get("error", "")) or "ValidationError" in str(actual.get("error_type", "")):
            return True, "loud-raise on out-of-range confidence (acceptable)"
        return False, f"crashed: {actual.get('error')}"
    c = actual.get("confidence")
    if c is None:
        return False, "no Model written"
    if c < 0.05 - 1e-6:
        return False, f"confidence not clipped: got {c}"
    return True, ""


CASE_CLIP_LOW = Case(
    stage="adversarial.boundary",
    name="confidence_clip_low_boundary",
    intent="confidence=0.0 must be clipped to ≥0.05 by the validator",
    setup=_setup_no_scope,
    run=H.safe_pipeline(_run_clip_low),
    expected=lambda _ctx: {},
    assertion=_assert_clip_low,
    failure_mode_under_test=(
        "validator's clipping range is wrong or skipped, allowing "
        "confidence=0 Models that distort downstream calibration"
    ),
    expected_behavior="specified",
    domain="extraction",
)


async def _run_clip_high(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = H.make_state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="boundary high confidence",
        confidence=1.0,  # above the [0.05, 0.95] clip ceiling
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            row = await conn.fetchrow(
                "SELECT confidence FROM models WHERE tenant_id=$1 "
                "ORDER BY created_at DESC LIMIT 1",
                ctx["tenant"],
            )
    return {"confidence": float(row["confidence"]) if row else None}


def _assert_clip_high(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    # Acceptable: loud raise (current — Pydantic le=0.95) OR clipped.
    if actual.get("crashed"):
        if "0.95" in str(actual.get("error", "")) or "ValidationError" in str(actual.get("error_type", "")):
            return True, "loud-raise on out-of-range confidence (acceptable)"
        return False, f"crashed: {actual.get('error')}"
    c = actual.get("confidence")
    if c is None:
        return False, "no Model written"
    if c > 0.95 + 1e-6:
        return False, f"confidence not clipped: got {c}"
    return True, ""


CASE_CLIP_HIGH = Case(
    stage="adversarial.boundary",
    name="confidence_clip_high_boundary",
    intent="confidence=1.0 must be clipped to ≤0.95 by the validator",
    setup=_setup_no_scope,
    run=H.safe_pipeline(_run_clip_high),
    expected=lambda _ctx: {},
    assertion=_assert_clip_high,
    failure_mode_under_test=(
        "ceiling clip not applied, producing certainty-1.0 Models "
        "that flow through Bayesian reasoning at infinite log-odds"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# B11 — Negative confidence
# =====================================================================


async def _run_neg_conf(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = H.make_state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="negative confidence claim",
        confidence=-0.4,
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await apply_diff(
                    diff, conn, trigger_kind="T1",
                    trigger_cause_event_id=ctx["obs"],
                )
                row = await conn.fetchrow(
                    "SELECT confidence FROM models WHERE tenant_id=$1 "
                    "ORDER BY created_at DESC LIMIT 1",
                    ctx["tenant"],
                )
                return {
                    "confidence": float(row["confidence"]) if row else None,
                    "raised": False,
                }
            except Exception as exc:  # noqa: BLE001
                return {"raised": True, "error_type": type(exc).__name__}


def _assert_neg_conf(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    # Acceptable: clipped to ≥0.05 or raised. Not acceptable: stored
    # negative.
    if not actual.get("raised"):
        c = actual.get("confidence")
        if c is None or c < 0.05 - 1e-6:
            return False, f"negative confidence persisted: got {c}"
    return True, ""


CASE_NEG_CONF = Case(
    stage="adversarial.boundary",
    name="negative_confidence_handling",
    intent="confidence=-0.4 must be clipped or rejected; stored "
           "negative confidence is broken arithmetic",
    setup=_setup_no_scope,
    run=H.safe_pipeline(_run_neg_conf),
    expected=lambda _ctx: {},
    assertion=_assert_neg_conf,
    failure_mode_under_test=(
        "validator applies clipping only to upper bound, allowing "
        "negative confidence to propagate"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# B12 — Two ACMEs as same string entity (collision)
# =====================================================================


async def _setup_two_acmes(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            customer_acme = await F.make_actor(
                conn, tenant, display_name="ACME",
            )
            internal_acme = await F.make_actor(
                conn, tenant, display_name="ACME",
            )
            obs = await F.make_observation(conn, tenant)
            return {
                "tenant": tenant,
                "customer": customer_acme,
                "internal": internal_acme,
                "obs": obs,
                "trigger_id": uuid7(),
            }


async def _run_two_acmes(pool: asyncpg.Pool, ctx: dict) -> dict:
    # We deliberately scope the insert to BOTH actors (the most likely
    # extraction error) — verify substrate doesn't crash on duplicate-
    # display-name scope.
    op = H.make_state_insert_op(
        tenant_id=ctx["tenant"],
        born_from_event_id=ctx["obs"],
        natural="ACME (ambiguous) is unhappy",
        scope_actors=[ctx["customer"], ctx["internal"]],
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1",
            ctx["tenant"],
        )
    return {"model_count": count}


CASE_TWO_ACMES = Case(
    stage="adversarial.boundary",
    name="two_actors_same_display_name",
    intent="Two distinct actors both displayed as 'ACME' must round-"
           "trip without collision; downstream consumer disambiguates "
           "by id",
    setup=_setup_two_acmes,
    run=H.safe_pipeline(_run_two_acmes),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "downstream UI/consumer that fetches scope by display_name "
        "joins both actors' Models into one stream; ID-based "
        "disambiguation isn't enforced anywhere"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "Should actor display_name be unique per tenant? Or is the UX "
        "expected to handle ambiguity? Document the contract."
    ),
    domain="ingest",
)


# =====================================================================
# B13 — Apply with empty diff (zero ops)
# =====================================================================


async def _run_empty_diff(pool: asyncpg.Pool, ctx: dict) -> dict:
    diff = ValidatedDiff(
        trigger_ref=ctx["trigger_id"],
        tenant_id=ctx["tenant"],
        claim_ops=[],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
        reasoning_trace="empty diff",
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            summary = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs"],
            )
            row = await conn.fetchrow(
                "SELECT outcome FROM applied_triggers WHERE trigger_id=$1",
                ctx["trigger_id"],
            )
    return {
        "outcome": row["outcome"] if row else None,
        "summary": summary,
    }


def _assert_empty_diff(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    if actual["outcome"] != "success":
        return False, (
            f"empty diff didn't write success row: {actual['outcome']}"
        )
    return True, ""


CASE_EMPTY_DIFF = Case(
    stage="adversarial.boundary",
    name="apply_diff_zero_ops",
    intent="apply_diff with no ops still records applied_triggers row "
           "and returns success — same as a no-op LLM extraction",
    setup=_setup_no_scope,
    run=H.safe_pipeline(_run_empty_diff),
    expected=lambda _ctx: {},
    assertion=_assert_empty_diff,
    failure_mode_under_test=(
        "no-op apply silently fails to record the applied_triggers "
        "row, breaking idempotency for the next think run on the same "
        "trigger_id"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# B14 — Embedding with wrong dimensionality
# =====================================================================


async def _run_wrong_dim(pool: asyncpg.Pool, ctx: dict) -> dict:
    bad_vec = [0.1] * 128  # production uses 768
    op = ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(ctx["tenant"]),
            "born_from_event_id": str(ctx["obs"]),
            "proposition": {"kind": "state", "subject": "x", "assertion": "x"},
            "natural": "wrong embedding dim",
            "embedding": bad_vec,
            "scope_actors": [],
            "scope_entities": [],
            "scope_temporal": {
                "valid_from": F.isoplus(0).isoformat(),
                "valid_until": None,
            },
            "confidence": 0.6,
            "confidence_at_assertion": 0.6,
        },
    )
    diff = _build_diff(ctx["tenant"], ctx["trigger_id"], op)
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await apply_diff(
                    diff, conn, trigger_kind="T1",
                    trigger_cause_event_id=ctx["obs"],
                )
                return {"raised": False}
            except Exception as exc:  # noqa: BLE001
                return {
                    "raised": True,
                    "error_type": type(exc).__name__,
                }


def _assert_wrong_dim(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"crashed: {actual.get('error')}"
    # We expect a raise — pgvector enforces dimensionality at write.
    if not actual.get("raised"):
        return False, (
            "wrong-dim embedding silently inserted; pgvector check "
            "expected to fire"
        )
    return True, ""


CASE_WRONG_DIM = Case(
    stage="adversarial.boundary",
    name="embedding_wrong_dimensionality",
    intent="An embedding of dim 128 (production is 768) must be "
           "rejected by pgvector dimension check at write time",
    setup=_setup_no_scope,
    run=H.safe_pipeline(_run_wrong_dim),
    expected=lambda _ctx: {},
    assertion=_assert_wrong_dim,
    failure_mode_under_test=(
        "pgvector column has no dimension constraint or coercion "
        "silently truncates/zero-pads, leading to nonsensical cosine "
        "scores at retrieval time"
    ),
    expected_behavior="specified",
    domain="extraction",
)


# =====================================================================
# B15 — Zero-vector embedding triggers reconciler skip path
# =====================================================================


async def _run_zero_vector(pool: asyncpg.Pool, ctx: dict) -> dict:
    from services.think.reconciler import reconcile_claim_op
    bad_vec = [0.0] * 768  # zero vector
    op = ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(ctx["tenant"]),
            "born_from_event_id": str(ctx["obs"]),
            "proposition": {
                "kind": "state", "subject": "zero embed",
                "assertion": "zero embed",
            },
            "natural": "zero vector skip",
            "embedding": bad_vec,
            "scope_actors": [],
            "scope_entities": [],
            "scope_temporal": {
                "valid_from": F.isoplus(0).isoformat(),
                "valid_until": None,
            },
            "confidence": 0.6,
            "confidence_at_assertion": 0.6,
        },
    )
    async with pool.acquire() as conn:
        result = await reconcile_claim_op(
            op, conn,
            tenant_id=ctx["tenant"],
            trigger_id=ctx["trigger_id"],
        )
    return {"decision": result.decision}


CASE_ZERO_VECTOR = Case(
    stage="adversarial.boundary",
    name="zero_vector_skip_reconciler",
    intent="An all-zero embedding triggers the reconciler's documented "
           "skip path (no false-positive cosine collisions)",
    setup=_setup_no_scope,
    run=H.safe_pipeline(_run_zero_vector),
    expected=lambda _ctx: {"decision": "skipped"},
    assertion=lambda a, e, c: (
        (a.get("decision") == "skipped",
         "" if a.get("decision") == "skipped"
         else f"got {a.get('decision')!r}, expected 'skipped'")
    ),
    failure_mode_under_test=(
        "reconciler computes cosine on zero vectors and returns 0.0, "
        "which passes no_match — but the failure mode would be a "
        "future change that returns 1.0 on identical zeros"
    ),
    expected_behavior="specified",
    domain="extraction",
)


CASES = [
    CASE_EMPTY_CONTENT,
    CASE_SINGLE_CHAR,
    CASE_HUGE_CONTENT,
    CASE_NO_SCOPE,
    CASE_MANY_ACTORS,
    CASE_FUTURE_OCCURRED,
    CASE_MISSING_MODEL,
    CASE_EMPTY_WINDOW,
    CASE_ARCHIVE_ARCHIVED,
    CASE_CLIP_LOW,
    CASE_CLIP_HIGH,
    CASE_NEG_CONF,
    CASE_TWO_ACMES,
    CASE_EMPTY_DIFF,
    CASE_WRONG_DIM,
    CASE_ZERO_VECTOR,
]
