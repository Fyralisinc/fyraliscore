"""Reconciliation stage — Think pipeline end-to-end with the real LLM.

Reconciliation in this codebase is implicit: there is no automatic
"detect duplicate Model" step inside Think. Reconciliation behavior
is the LLM's job (it can emit `claim_op.update` or
`claim_op.archive` against existing Models) and is *gated* by:
  - `applied_triggers` idempotency keyed on trigger_id
  - region-locked apply transaction
  - validator's confidence/threshold/falsifier checks

These cases test that gating, not the LLM's semantic choices —
asserting "Think correctly produces a Model" via an LLM is too
flaky for a regression harness. We do exercise the LLM in one
"smoke" case (LLM completes, structured output validates,
think returns a non-error outcome).
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from services.retrieval.primary import TriggerContext
from services.think.applier import apply_diff
from services.think.diff_schema import ClaimOp, RawDiff, ValidatedDiff
from services.think.reason import think
from lib.llm.provider import LLMConfig, build_provider
from lib.shared.ids import uuid7

from . import _fixtures as F
from ._runner import Case


# =====================================================================
# Helpers
# =====================================================================


def _trigger_with_id(
    *,
    kind: str,
    tenant_id: UUID,
    trigger_id: UUID,
    observation_id: UUID | None = None,
    seed_text: str = "synthetic seed",
    scope_actors: list[UUID] | None = None,
    seed_entity_ids: list[dict] | None = None,
    seed_occurred_at: datetime | None = None,
    subkind: str | None = None,
    model_id: UUID | None = None,
) -> TriggerContext:
    return TriggerContext(
        kind=kind,
        tenant_id=tenant_id,
        observation_id=observation_id,
        scope_actors=scope_actors or [],
        seed_entity_ids=seed_entity_ids or [],
        seed_natural_text=seed_text,
        seed_occurred_at=seed_occurred_at or F.isoplus(0),
        precomputed_seed_vector=F.deterministic_vector(seed_text),
        seed_signature={"trigger_id": str(trigger_id)},
        subkind=subkind,
        model_id=model_id,
    )


# =====================================================================
# RC1 — Idempotency: applier raises AlreadyAppliedError on duplicate trigger_id
# =====================================================================


async def _setup_idempotency(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            obs = await F.make_observation(
                conn, tenant,
                content_text="idempotency probe",
                actor_id=actor,
            )
            return {
                "tenant": tenant,
                "actor": actor,
                "obs": obs,
                "trigger_id": uuid7(),
            }


def _empty_validated_diff(tenant: UUID, trigger_id: UUID) -> ValidatedDiff:
    return ValidatedDiff(
        trigger_ref=trigger_id,
        tenant_id=tenant,
        claim_ops=[],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
        reasoning_trace="harness idempotency",
    )


async def _run_idempotency(pool: asyncpg.Pool, ctx: dict) -> dict:
    diff = _empty_validated_diff(ctx["tenant"], ctx["trigger_id"])
    first_ok = False
    second_skipped = False
    err_class = None
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(diff, conn, trigger_kind="T1", trigger_cause_event_id=ctx["obs"])
            first_ok = True
        # Re-apply same trigger
        async with conn.transaction():
            try:
                await apply_diff(diff, conn, trigger_kind="T1", trigger_cause_event_id=ctx["obs"])
            except Exception as exc:
                err_class = type(exc).__name__
                if err_class == "AlreadyAppliedError":
                    second_skipped = True
    return {"first_ok": first_ok, "second_skipped": second_skipped, "err_class": err_class}


def _expected_idempotency(_ctx: dict) -> dict:
    return {"first_ok": True, "second_skipped": True, "err_class": "AlreadyAppliedError"}


def _assert_idempotency(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual != expected:
        return False, f"got {actual}"
    return True, ""


CASE_IDEMPOTENCY = Case(
    stage="reconciliation",
    name="applier_idempotency_per_trigger_id",
    intent="Re-applying a diff with same trigger_id raises AlreadyAppliedError",
    setup=_setup_idempotency,
    run=_run_idempotency,
    expected=_expected_idempotency,
    assertion=_assert_idempotency,
)


# =====================================================================
# RC2 — applied_triggers row is inserted with outcome='success'
# =====================================================================


async def _setup_applied_marker(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            obs = await F.make_observation(conn, tenant, content_text="marker probe")
            return {"tenant": tenant, "obs": obs, "trigger_id": uuid7()}


async def _run_applied_marker(pool: asyncpg.Pool, ctx: dict) -> dict:
    diff = _empty_validated_diff(ctx["tenant"], ctx["trigger_id"])
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(diff, conn, trigger_kind="T1", trigger_cause_event_id=ctx["obs"])
        row = await conn.fetchrow(
            "SELECT outcome FROM applied_triggers WHERE trigger_id=$1",
            ctx["trigger_id"],
        )
    return {"outcome": row["outcome"] if row else None}


def _expected_applied_marker(_ctx: dict) -> dict:
    return {"outcome": "success"}


def _assert_applied_marker(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual != expected:
        return False, f"got {actual} expected {expected}"
    return True, ""


CASE_APPLIED_MARKER = Case(
    stage="reconciliation",
    name="applied_triggers_outcome_success",
    intent="Applier writes applied_triggers row with outcome='success'",
    setup=_setup_applied_marker,
    run=_run_applied_marker,
    expected=_expected_applied_marker,
    assertion=_assert_applied_marker,
)


# =====================================================================
# RC3 — DeepSeek smoke: think() runs end-to-end with real LLM and either
#   succeeds or fails cleanly with structured-output reasoning.
# =====================================================================


def _llm_available() -> bool:
    if os.environ.get("HARNESS_SKIP_LLM") in ("1", "true", "yes"):
        return False
    if os.environ.get("LLM_PROVIDER", "").lower() != "deepseek":
        return False
    return bool(os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("LLM_API_KEY"))


async def _setup_llm_smoke(pool: asyncpg.Pool, _ctx: dict) -> dict:
    if not _llm_available():
        return {"skip": True}
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant, display_name="Engineer")
            # Insert a vague user signal observation
            obs = await F.make_observation(
                conn, tenant,
                content_text=(
                    "User reports the rate limiter on the gateway is throwing "
                    "false positives on 5% of legitimate traffic during peak hours."
                ),
                actor_id=actor,
                trust_tier="authoritative",
            )
            # Provide an in-scope Model so retrieval has something to surface
            await F.make_model(
                conn, tenant,
                natural="Rate limiter has elevated false positive rate",
                scope_actors=[actor],
                confidence=0.6,
                embed_seed="rate-limiter-fp",
            )
            return {
                "tenant": tenant,
                "actor": actor,
                "obs": obs,
                "trigger_id": uuid7(),
                "skip": False,
            }


async def _run_llm_smoke(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    trigger = _trigger_with_id(
        kind="T1",
        tenant_id=ctx["tenant"],
        trigger_id=ctx["trigger_id"],
        observation_id=ctx["obs"],
        scope_actors=[ctx["actor"]],
        seed_text="Rate limiter false positives on legitimate traffic",
    )
    config = LLMConfig.from_env()
    provider = build_provider(config)
    outcome = await think(
        trigger,
        pool,
        llm_provider=provider,
        triggering_content="Rate limiter false positives during peak traffic",
        reason_for_trigger="user signal",
        trigger_kind_subkind="T1.event_arrival",
    )
    return {
        "skipped": False,
        "status": outcome.status,
        "ops_applied_count": outcome.ops_applied_count,
        "llm_calls_count": outcome.llm_calls_count,
        "llm_input_tokens": outcome.llm_input_tokens,
        "model": outcome.llm_model_name,
        "elapsed_ms": outcome.elapsed_ms,
        "error": outcome.error,
    }


def _expected_llm_smoke(_ctx: dict) -> dict:
    return {"acceptable_status": {"success", "skipped_idempotent"}}


def _assert_llm_smoke(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual.get("skipped"):
        return True, "skipped (no LLM provider configured)"
    if actual["status"] not in expected["acceptable_status"]:
        return False, f"think status {actual['status']!r}; error={actual.get('error')!r}"
    if actual.get("status") == "success":
        if actual["llm_calls_count"] < 1:
            return False, f"expected at least 1 LLM call; got {actual['llm_calls_count']}"
    return True, ""


CASE_LLM_SMOKE = Case(
    stage="reconciliation",
    name="think_end_to_end_deepseek_smoke",
    intent="Real DeepSeek call: think() completes end-to-end, returns success or skipped",
    setup=_setup_llm_smoke,
    run=_run_llm_smoke,
    expected=_expected_llm_smoke,
    assertion=_assert_llm_smoke,
)


# =====================================================================
# RC4 — Same trigger via think() twice → second run is skipped_idempotent
# =====================================================================


async def _setup_think_idempotency(pool: asyncpg.Pool, _ctx: dict) -> dict:
    if not _llm_available():
        return {"skip": True}
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            obs = await F.make_observation(
                conn, tenant,
                content_text="Engineer reported a regression in the export job.",
                actor_id=actor,
            )
            return {
                "tenant": tenant,
                "actor": actor,
                "obs": obs,
                "trigger_id": uuid7(),
                "skip": False,
            }


async def _run_think_idempotency(pool: asyncpg.Pool, ctx: dict) -> dict:
    if ctx.get("skip"):
        return {"skipped": True}
    trigger = _trigger_with_id(
        kind="T1",
        tenant_id=ctx["tenant"],
        trigger_id=ctx["trigger_id"],
        observation_id=ctx["obs"],
        scope_actors=[ctx["actor"]],
        seed_text="export job regression report",
    )
    config = LLMConfig.from_env()
    provider = build_provider(config)
    first = await think(
        trigger, pool, llm_provider=provider,
        triggering_content="export job regression",
        reason_for_trigger="user signal",
        trigger_kind_subkind="T1.event_arrival",
    )
    second = await think(
        trigger, pool, llm_provider=provider,
        triggering_content="export job regression",
        reason_for_trigger="user signal",
        trigger_kind_subkind="T1.event_arrival",
    )
    return {
        "skipped": False,
        "first_status": first.status,
        "second_status": second.status,
    }


def _expected_think_idempotency(_ctx: dict) -> dict:
    return {"second_status": "skipped_idempotent"}


def _assert_think_idempotency(actual: dict, expected: dict, _ctx: dict) -> tuple[bool, str]:
    if actual.get("skipped"):
        return True, "skipped (no LLM provider)"
    if actual["second_status"] != "skipped_idempotent":
        return False, f"second run status {actual['second_status']!r}; first={actual['first_status']!r}"
    return True, ""


CASE_THINK_IDEMPOTENCY = Case(
    stage="reconciliation",
    name="think_repeat_trigger_id_skipped_idempotent",
    intent="Calling think() twice with the same trigger_id → second run skipped_idempotent",
    setup=_setup_think_idempotency,
    run=_run_think_idempotency,
    expected=_expected_think_idempotency,
    assertion=_assert_think_idempotency,
)


CASES = [
    CASE_IDEMPOTENCY,
    CASE_APPLIED_MARKER,
    CASE_LLM_SMOKE,
    CASE_THINK_IDEMPOTENCY,
]
