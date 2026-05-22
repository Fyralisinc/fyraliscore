"""Category 8 — Failure-injection harness.

Real systems fail. Test what happens when components fail mid-pipeline.
This module installs explicit fault wrappers (a FailingLLM, a
half-applied diff, a closed connection) and verifies the substrate
either degrades gracefully or fails loud-and-fast.

The principle: silent failures are the enemy. A loud raise with a
clean error class is acceptable; a quiet success that masks corruption
is the failure mode.
"""
from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import asyncpg

from lib.llm.provider import LLMConfig, LLMError, LLMProvider
from lib.shared.ids import uuid7
from services.retrieval.primary import TriggerContext
from services.think.applier import apply_diff
from services.think.diff_schema import ClaimOp, ValidatedDiff
from services.think.reason import think

from .. import _fixtures as F
from .._runner import Case
from . import _helpers as H


# =====================================================================
# Mock provider scaffolding
# =====================================================================
# We construct providers without going through LLMConfig.from_env so
# the env doesn't need a real key for these cases. Each mock returns
# a chosen failure or response shape.


def _stub_config() -> LLMConfig:
    return LLMConfig(
        provider="deepseek",
        model="deepseek-chat",
        api_key="stub-key",
        timeout_s=5.0,
        max_retries=0,
    )


class _TimeoutLLM(LLMProvider):
    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        raise asyncio.TimeoutError("synthetic LLM timeout")


class _MalformedJSONLLM(LLMProvider):
    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        return "}}}{{ this is not json {{{}}}"


class _EmptyLLM(LLMProvider):
    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        return ""


class _NonsenseSchemaLLM(LLMProvider):
    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        # Valid JSON but wrong schema
        return '{"foo": "bar", "baz": 42}'


class _RaisingLLM(LLMProvider):
    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        raise LLMError("synthetic LLM error")


# =====================================================================
# FI1 — LLM timeout: think() must fail with a recognizable error class
# =====================================================================


async def _setup_with_obs(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            tenant = await F.make_tenant(conn)
            actor = await F.make_actor(conn, tenant)
            obs = await F.make_observation(
                conn, tenant, actor_id=actor,
                content_text="fault injection probe",
            )
            return {
                "tenant": tenant, "actor": actor, "obs": obs,
                "trigger_id": uuid7(),
            }


async def _drive_with_provider(
    pool: asyncpg.Pool, ctx: dict, provider: LLMProvider,
) -> dict:
    trigger = TriggerContext(
        kind="T1",
        tenant_id=ctx["tenant"],
        observation_id=ctx["obs"],
        scope_actors=[ctx["actor"]],
        seed_entity_ids=[],
        seed_natural_text="failure injection",
        seed_occurred_at=F.isoplus(0),
        precomputed_seed_vector=F.deterministic_vector("failure injection"),
        seed_signature={"trigger_id": str(ctx["trigger_id"])},
    )
    try:
        outcome = await think(
            trigger, pool, llm_provider=provider,
            triggering_content="failure injection probe",
            reason_for_trigger="harness",
            trigger_kind_subkind="T1.event_arrival",
        )
        return {
            "raised": False,
            "status": outcome.status,
            "ops_applied": outcome.ops_applied_count,
            "error": outcome.error,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "raised": True,
            "error_type": type(exc).__name__,
            "error_msg": str(exc)[:240],
        }


async def _run_timeout(pool: asyncpg.Pool, ctx: dict) -> dict:
    return await _drive_with_provider(pool, ctx, _TimeoutLLM(_stub_config()))


def _assert_loud_failure(actual: dict, _e: dict, _c: dict) -> tuple[bool, str]:
    if actual.get("crashed"):
        return False, f"runner crash: {actual.get('error')}"
    # Acceptable: think raises (loud) OR returns a non-success status.
    if actual.get("raised"):
        return True, ""
    if actual.get("status") == "success":
        return False, (
            "synthetic LLM failure produced status='success' — silent "
            "swallowing of upstream failure"
        )
    return True, ""


CASE_TIMEOUT = Case(
    stage="adversarial.failure_injection",
    name="llm_timeout_fails_loudly",
    intent="A timeout raised by the LLM provider must NOT result in "
           "think() returning status='success' — fail loud or surface "
           "as failed status",
    setup=_setup_with_obs,
    run=H.safe_pipeline(_run_timeout),
    expected=lambda _ctx: {},
    assertion=_assert_loud_failure,
    failure_mode_under_test=(
        "think swallows the asyncio.TimeoutError and reports success "
        "with zero ops applied; downstream observers never learn the "
        "LLM hung"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# FI2 — LLM returns malformed JSON: validator must reject
# =====================================================================


async def _run_malformed(pool: asyncpg.Pool, ctx: dict) -> dict:
    return await _drive_with_provider(pool, ctx, _MalformedJSONLLM(_stub_config()))


CASE_MALFORMED = Case(
    stage="adversarial.failure_injection",
    name="llm_malformed_json_rejected",
    intent="LLM returns garbage JSON; think() does not crash and "
           "either retries or surfaces a validation failure status",
    setup=_setup_with_obs,
    run=H.safe_pipeline(_run_malformed),
    expected=lambda _ctx: {},
    assertion=_assert_loud_failure,
    failure_mode_under_test=(
        "JSON parser exception bubbles up unwrapped; the trigger is "
        "marked failed without enough context to debug what the LLM "
        "actually returned"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# FI3 — LLM returns empty string
# =====================================================================


async def _run_empty(pool: asyncpg.Pool, ctx: dict) -> dict:
    return await _drive_with_provider(pool, ctx, _EmptyLLM(_stub_config()))


CASE_EMPTY = Case(
    stage="adversarial.failure_injection",
    name="llm_empty_response",
    intent="LLM returns '' — think() handles cleanly (retry, fail, or "
           "skip), does not produce phantom Models",
    setup=_setup_with_obs,
    run=H.safe_pipeline(_run_empty),
    expected=lambda _ctx: {},
    assertion=_assert_loud_failure,
    failure_mode_under_test=(
        "empty string falls through JSON parser as 'no ops to apply' "
        "and think reports success; observability hides the silent LLM"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# FI4 — LLM returns valid JSON, wrong schema
# =====================================================================


async def _run_wrong_schema(pool: asyncpg.Pool, ctx: dict) -> dict:
    return await _drive_with_provider(pool, ctx, _NonsenseSchemaLLM(_stub_config()))


CASE_WRONG_SCHEMA = Case(
    stage="adversarial.failure_injection",
    name="llm_valid_json_wrong_schema",
    intent="LLM returns parseable JSON that doesn't match RawDiff "
           "schema; think() rejects loudly",
    setup=_setup_with_obs,
    run=H.safe_pipeline(_run_wrong_schema),
    expected=lambda _ctx: {},
    assertion=_assert_loud_failure,
    failure_mode_under_test=(
        "schema validation is too permissive (extra='allow' on the "
        "wrong layer) and a nonsensical RawDiff lands as a no-op think"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# FI5 — LLM raises LLMError
# =====================================================================


async def _run_llm_raises(pool: asyncpg.Pool, ctx: dict) -> dict:
    return await _drive_with_provider(pool, ctx, _RaisingLLM(_stub_config()))


CASE_LLM_RAISES = Case(
    stage="adversarial.failure_injection",
    name="llm_raises_handled_via_outcome",
    intent="LLM raises LLMError; think() returns failed-status outcome",
    setup=_setup_with_obs,
    run=H.safe_pipeline(_run_llm_raises),
    expected=lambda _ctx: {},
    assertion=_assert_loud_failure,
    failure_mode_under_test=(
        "LLMError leaks past think's outer handler and crashes the "
        "worker; circuit breaker doesn't trip"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# FI6 — apply_diff on closed connection raises cleanly
# =====================================================================


async def _run_closed_conn(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = H.make_state_insert_op(
        tenant_id=ctx["tenant"], born_from_event_id=ctx["obs"],
        natural="closed conn probe",
        scope_actors=[ctx["actor"]],
    )
    diff = ValidatedDiff(
        trigger_ref=ctx["trigger_id"], tenant_id=ctx["tenant"],
        claim_ops=[op], act_ops=[], resource_ops=[],
        new_predictions=[], reasoning_trace="closed conn",
    )
    conn = await pool.acquire()
    await conn.close()
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


CASE_CLOSED_CONN = Case(
    stage="adversarial.failure_injection",
    name="apply_on_closed_connection_raises",
    intent="apply_diff on a closed connection raises (no silent "
           "no-op or hang)",
    setup=_setup_with_obs,
    run=H.safe_pipeline(_run_closed_conn),
    expected=lambda _ctx: {"raised": True},
    assertion=lambda a, e, c: (
        (a.get("raised") is True,
         "" if a.get("raised") is True
         else f"got {a!r}; closed-connection apply should raise")
    ),
    failure_mode_under_test=(
        "asyncpg silently retries on a closed conn and the apply "
        "appears to succeed against a stale/no-op connection"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# FI7 — Validator rejects every op (all malformed)
# =====================================================================
# Build a diff with three ops, each missing required fields. Validator
# should drop them and return a Validation failure.


async def _run_all_malformed(pool: asyncpg.Pool, ctx: dict) -> dict:
    bad_ops = [
        ClaimOp(op="insert", entry={}),  # missing everything
        ClaimOp(op="update"),  # missing model_id and changes
        ClaimOp(op="archive"),  # missing model_id
    ]
    diff = ValidatedDiff(
        trigger_ref=ctx["trigger_id"], tenant_id=ctx["tenant"],
        claim_ops=bad_ops, act_ops=[], resource_ops=[],
        new_predictions=[], reasoning_trace="all malformed",
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                summary = await apply_diff(
                    diff, conn, trigger_kind="T1",
                    trigger_cause_event_id=ctx["obs"],
                )
                return {
                    "raised": False,
                    "summary": summary,
                }
            except Exception as exc:  # noqa: BLE001
                return {"raised": True, "error_type": type(exc).__name__}


CASE_ALL_MALFORMED = Case(
    stage="adversarial.failure_injection",
    name="apply_diff_all_ops_malformed",
    intent="A diff with three malformed ops either raises OR returns "
           "a summary indicating zero successful applies — never "
           "phantom-applies a malformed op",
    setup=_setup_with_obs,
    run=H.safe_pipeline(_run_all_malformed),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "applier blindly forwards malformed ops to the SQL layer and "
        "they crash mid-transaction with an opaque integrity error"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "What's the right behavior for an all-malformed diff? Drop "
        "all and report success-with-warnings, raise ValidationError, "
        "or fail the trigger? Currently behavior depends on which "
        "validation layer fires first."
    ),
    domain="ops",
)


# =====================================================================
# FI8 — Reconciler with degraded DB (raises during candidate query)
# =====================================================================
# Test that reconciler's exception-handler demotes to skipped.


async def _run_reconcile_degraded(pool: asyncpg.Pool, ctx: dict) -> dict:
    from services.think.reconciler import reconcile_claim_op
    op = H.make_state_insert_op(
        tenant_id=ctx["tenant"], born_from_event_id=ctx["obs"],
        natural="degraded probe",
        scope_actors=[ctx["actor"]],
    )

    # Use a connection that will fail on the next query (we close it
    # after acquire)
    conn = await pool.acquire()
    await conn.close()
    result = await reconcile_claim_op(
        op, conn,
        tenant_id=ctx["tenant"],
        trigger_id=ctx["trigger_id"],
    )
    return {"decision": result.decision}


CASE_RECONCILE_DEGRADED = Case(
    stage="adversarial.failure_injection",
    name="reconciler_degrades_to_skipped_on_failure",
    intent="Reconciler internal exception (closed conn) demotes to "
           "decision='skipped' — never aborts the apply",
    setup=_setup_with_obs,
    run=H.safe_pipeline(_run_reconcile_degraded),
    expected=lambda _ctx: {"decision": "skipped"},
    assertion=lambda a, e, c: (
        (a.get("decision") == "skipped",
         "" if a.get("decision") == "skipped"
         else f"got {a.get('decision')!r}")
    ),
    failure_mode_under_test=(
        "reconciler's BLE001 catch regresses; closed-conn or query "
        "error propagates and aborts the apply transaction"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# FI9 — Wrong-tenant connection (tenant isolation invariant)
# =====================================================================
# Apply a diff with tenant_id=A but query results filtered to
# tenant_id=B should produce zero results — no cross-tenant leak.


async def _setup_two_tenants(pool: asyncpg.Pool, _ctx: dict) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            t1 = await F.make_tenant(conn)
            t2 = await F.make_tenant(conn)
            actor1 = await F.make_actor(conn, t1)
            actor2 = await F.make_actor(conn, t2)
            await F.make_model(
                conn, t1, natural="tenant 1 secret",
                scope_actors=[actor1], embed_seed="leak-probe",
            )
            obs2 = await F.make_observation(conn, t2, actor_id=actor2)
            return {
                "t1": t1, "t2": t2,
                "actor1": actor1, "actor2": actor2,
                "obs2": obs2,
            }


async def _run_no_leak(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = H.make_state_insert_op(
        tenant_id=ctx["t2"], born_from_event_id=ctx["obs2"],
        natural="tenant 2 lookup probe",
        scope_actors=[ctx["actor2"]],
        embed_seed="leak-probe",  # same seed as t1's secret
    )
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=ctx["t2"],
        claim_ops=[op], act_ops=[], resource_ops=[],
        new_predictions=[], reasoning_trace="leak probe",
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=ctx["obs2"],
            )
        # Reconciler in t2 must NOT auto-merge into t1's secret.
        t2_count = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["t2"],
        )
        t1_count = await conn.fetchval(
            "SELECT COUNT(*) FROM models WHERE tenant_id=$1 AND status='active'",
            ctx["t1"],
        )
    return {"t1_count": t1_count, "t2_count": t2_count}


CASE_NO_LEAK = Case(
    stage="adversarial.failure_injection",
    name="cross_tenant_no_reconcile_leak",
    intent="Tenant 2's insert must NOT merge into Tenant 1's existing "
           "Model even with identical embed_seed — tenant_id is a hard "
           "isolation boundary",
    setup=_setup_two_tenants,
    run=H.safe_pipeline(_run_no_leak),
    expected=lambda _ctx: {"t1_count": 1, "t2_count": 1},
    assertion=lambda a, e, c: (
        (a.get("t1_count") == 1 and a.get("t2_count") == 1,
         "" if (a.get("t1_count") == 1 and a.get("t2_count") == 1)
         else f"isolation breach: {a!r}")
    ),
    failure_mode_under_test=(
        "reconciler's tenant_id WHERE clause regresses (e.g. accepts "
        "$1 IS NULL OR tenant_id=$1) and cross-tenant matches happen"
    ),
    expected_behavior="specified",
    domain="ops",
)


# =====================================================================
# FI10 — Embedding service unavailable: insert without embedding
# =====================================================================
# We can't easily mock the embedding service from this layer, but we
# can verify the substrate's behavior when an insert lacks an
# embedding (production code uses zero-vector placeholder).


async def _run_no_embedding_insert(pool: asyncpg.Pool, ctx: dict) -> dict:
    op = ClaimOp(
        op="insert",
        entry={
            "tenant_id": str(ctx["tenant"]),
            "born_from_event_id": str(ctx["obs"]),
            "proposition": {
                "kind": "state", "subject": "no-embed",
                "assertion": "no-embed",
            },
            "natural": "no-embed insert",
            # embedding deliberately absent
            "scope_actors": [str(ctx["actor"])],
            "scope_entities": [],
            "scope_temporal": {
                "valid_from": F.isoplus(0).isoformat(),
                "valid_until": None,
            },
            "confidence": 0.6,
            "confidence_at_assertion": 0.6,
        },
    )
    diff = ValidatedDiff(
        trigger_ref=uuid7(), tenant_id=ctx["tenant"],
        claim_ops=[op], act_ops=[], resource_ops=[],
        new_predictions=[], reasoning_trace="no embedding",
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                await apply_diff(
                    diff, conn, trigger_kind="T1",
                    trigger_cause_event_id=ctx["obs"],
                )
                return {"raised": False}
            except Exception as exc:  # noqa: BLE001
                return {"raised": True, "error_type": type(exc).__name__}


CASE_NO_EMBED_INSERT = Case(
    stage="adversarial.failure_injection",
    name="insert_without_embedding",
    intent="Insert with no embedding key in entry: applier either "
           "rejects OR uses a placeholder; reconciler skips cleanly",
    setup=_setup_with_obs,
    run=H.safe_pipeline(_run_no_embedding_insert),
    expected=lambda _ctx: {},
    assertion=H.assert_no_crash,
    failure_mode_under_test=(
        "applier crashes on missing embedding key OR inserts a NULL "
        "embedding that subsequent retrieval can't index"
    ),
    expected_behavior="underspecified",
    underspec_question=(
        "What's the contract for missing embedding in a claim_op? "
        "Reconciler skips, but applier behavior is undocumented."
    ),
    domain="ops",
)


CASES = [
    CASE_TIMEOUT,
    CASE_MALFORMED,
    CASE_EMPTY,
    CASE_WRONG_SCHEMA,
    CASE_LLM_RAISES,
    CASE_CLOSED_CONN,
    CASE_ALL_MALFORMED,
    CASE_RECONCILE_DEGRADED,
    CASE_NO_LEAK,
    CASE_NO_EMBED_INSERT,
]
