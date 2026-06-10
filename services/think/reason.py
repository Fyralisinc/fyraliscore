"""services/think/reason.py — the cognitive pipeline entry point.

Spec §7 "The think() function" + BUILD-PLAN §4 Prompt 3.B item 2.

Orchestrates:
  1. Retrieval
  2. Authoritative-vs-inferential dispatch
  3. Validation
  4. Region lock + apply + anomalies + cascade (all in one tx)
  5. Post-commit region_lock_log write + metrics

Returns a ThinkRunOutcome the caller uses to complete the trigger
queue row and log. On failure, raises (the worker's handle_failure
path categorizes).
"""
from __future__ import annotations

import asyncio
import os
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.llm.provider import (
    LLMProvider,
    LLMUsageAggregator,
    using_usage_aggregator,
)
from lib.shared.errors import (
    CompanyOSError,
    ValidationError,
)
from lib.shared.ids import uuid7

from services.retrieval.assembler import (
    AccessContext,
)
from services.retrieval.primary import (
    TriggerContext,
)
from services.sage.inquiry_traces.emitter import (
    TraceContext as _SageTraceContext,
    emission_enabled as _sage_emission_enabled,
    set_trace_context as _sage_set_trace_context,
)

from .anomaly_integration import (
    check_anomalies,
    publish_anomalies,
)
from .applier import AlreadyAppliedError, apply_diff
from .cascade import CascadeEvent, CascadeResult, cascade
from .context_planner import assemble_reasoning_context, plan_context
from .debug_capture import capture as debug_capture
from .deterministic import deterministic_handler, is_authoritative
from .llm_reason import llm_reason
from .observability import (
    METRICS,
    ThinkRunRecord,
    emit,
    insert_think_run,
    record_think_run_cost,
    update_think_run,
    write_region_lock_log,
)
from .post_commit import enqueue_post_commit_actions
from .region_locks import (
    RegionLockAcquisition,
    acquire_region_lock,
    region_lock_key,
)
from .validator import (
    OutOfRegionError,
    validate,
)


_log = structlog.get_logger(__name__)


def _raise_if_postgres_error(exc: Exception) -> None:
    """A swallowed SQL error leaves the active transaction unusable."""
    if isinstance(exc, asyncpg.PostgresError):
        raise exc


def _tx_health_check_enabled() -> bool:
    return os.environ.get("THINK_TX_HEALTH_CHECK", "0") == "1"


def _narrow_inferential_transaction_enabled() -> bool:
    return os.environ.get("THINK_NARROW_INFERENTIAL_TX", "1") != "0"


@asynccontextmanager
async def _mutation_transaction(conn: asyncpg.Connection):
    if conn.is_in_transaction():
        yield
    else:
        async with conn.transaction():
            yield


async def _assert_tx_usable(conn: asyncpg.Connection, phase: str) -> None:
    if not _tx_health_check_enabled():
        return
    try:
        await conn.execute("SELECT 1")
    except asyncpg.PostgresError as exc:
        raise RuntimeError(
            "think transaction aborted after "
            f"{phase}: {type(exc).__name__}: {exc}"
        ) from exc


# ---------------------------------------------------------------------
# Public return shape
# ---------------------------------------------------------------------


@dataclass
class ThinkRunOutcome:
    run_id: UUID
    trigger_id: UUID
    trigger_kind: str
    status: str
    error: str | None = None
    ops_applied_count: int = 0
    cascade_depth: int = 0
    anomalies_flagged: int = 0
    llm_latency_ms: int | None = None
    elapsed_ms: float = 0.0
    region_tenant_hash: int | None = None
    region_entity_hash: int | None = None
    region_acquisition: RegionLockAcquisition | None = None
    # OP-2 cost attribution.
    llm_calls_count: int = 0
    llm_input_tokens: int = 0
    llm_output_tokens: int = 0
    llm_cost_usd: float = 0.0
    llm_model_name: str | None = None
    # Raised exception for caller's failure classification.
    exception: BaseException | None = None

    @property
    def succeeded(self) -> bool:
        return self.status == "success"

    @property
    def skipped_idempotent(self) -> bool:
        return self.status == "skipped_idempotent"


# ---------------------------------------------------------------------
# think() — single-shot entry point
# ---------------------------------------------------------------------


async def think(
    trigger: TriggerContext,
    pool: asyncpg.Pool,
    *,
    llm_provider: LLMProvider | None = None,
    embedder: Any | None = None,
    access_context: AccessContext | None = None,
    triggering_content: str | None = None,
    reason_for_trigger: str | None = None,
    trigger_kind_subkind: str | None = None,
    max_retrieval_reruns: int = 2,
) -> ThinkRunOutcome:
    """
    Single-shot Think invocation.

    Authoritative/deterministic triggers keep the legacy wide
    transaction because a few deterministic handlers intentionally do
    side-effectful reasoning. Inferential triggers run retrieval,
    context assembly, and LLM reasoning outside the apply transaction,
    then open a short mutation transaction for the advisory lock,
    validation, apply, anomaly publication, queueing, and cascade.

    For tests that want to drive everything inside one pre-opened
    transaction (ROLLBACK at teardown), use `think_in_conn` instead —
    see worker.py for the LISTEN/poll-driven caller that uses this.
    """
    from .deterministic import _trigger_ref  # type: ignore

    started_at = time.monotonic()
    trigger_id = _trigger_ref(trigger)
    trigger_kind_full = trigger_kind_subkind or trigger.kind
    run_id = uuid7()

    record = ThinkRunRecord(
        id=run_id,
        tenant_id=trigger.tenant_id,
        trigger_id=trigger_id,
        trigger_kind=trigger_kind_full,
    )

    METRICS.inc_run(trigger_kind_full)
    emit("think.started",
         run_id=str(run_id),
         trigger_id=str(trigger_id),
         trigger_kind=trigger_kind_full,
         tenant_id=str(trigger.tenant_id))

    rerun_count = 0
    transaction_retry_count = 0
    max_transaction_retries = int(
        os.environ.get("THINK_TRANSACTION_RETRY_ATTEMPTS", "8")
    )
    expanded_region: set[tuple[str, str]] | None = None

    # OP-2: install a usage aggregator on the provider for this run.
    # Aggregator is cleared after the run (finally block). Any LLM call
    # made via `provider.structured` records tokens + cost into it.
    usage_agg: LLMUsageAggregator | None = None
    usage_ctx: Any | None = None
    if llm_provider is not None:
        usage_agg = LLMUsageAggregator()
        usage_ctx = using_usage_aggregator(usage_agg)
        usage_ctx.__enter__()
        llm_provider.set_usage_aggregator(usage_agg)

    try:
        while True:
            try:
                use_wide_transaction = (
                    is_authoritative(trigger)
                    or not _narrow_inferential_transaction_enabled()
                )
                async with pool.acquire() as conn:
                    if use_wide_transaction:
                        async with conn.transaction():
                            outcome = await _run_once(
                                conn=conn,
                                trigger=trigger,
                                llm_provider=llm_provider,
                                embedder=embedder,
                                access_context=access_context,
                                triggering_content=triggering_content,
                                reason_for_trigger=reason_for_trigger,
                                record=record,
                                expanded_region=expanded_region,
                            )
                    else:
                        outcome = await _run_once(
                            conn=conn,
                            trigger=trigger,
                            llm_provider=llm_provider,
                            embedder=embedder,
                            access_context=access_context,
                            triggering_content=triggering_content,
                            reason_for_trigger=reason_for_trigger,
                            record=record,
                            expanded_region=expanded_region,
                        )
            except OutOfRegionError as e:
                # Re-run retrieval with the missing entities explicitly
                # allowed. The failed transaction rolls back its
                # think_runs row, so the same run_id can be reused for
                # the successful retry.
                rerun_count += 1
                if rerun_count > max_retrieval_reruns:
                    METRICS.inc_failed(trigger_kind_full)
                    emit(
                        "think.failed",
                        run_id=str(run_id),
                        error="out_of_region_exhausted",
                        rerun_count=rerun_count,
                    )
                    out = ThinkRunOutcome(
                        run_id=run_id,
                        trigger_id=trigger_id,
                        trigger_kind=trigger_kind_full,
                        status="failed",
                        error=(
                            f"out_of_region_after_{rerun_count}_reruns: "
                            f"{e.message}"
                        ),
                        exception=e,
                        elapsed_ms=(time.monotonic() - started_at) * 1000.0,
                    )
                    _snapshot_usage(out, usage_agg, llm_provider)
                    await _record_failed_run(pool, record, out.error)
                    await _record_cost_for_outcome(
                        pool, out, trigger.tenant_id,
                    )
                    return out
                emit(
                    "think.out_of_region",
                    run_id=str(run_id),
                    attempt=rerun_count,
                    missing=e.context.get("missing"),
                )
                prev = expanded_region or set()
                missing = e.context.get("missing") or []
                prev.update((t, i) for (t, i) in missing)
                expanded_region = prev
                continue
            except (
                asyncpg.exceptions.DeadlockDetectedError,
                asyncpg.exceptions.SerializationError,
            ) as e:
                transaction_retry_count += 1
                if transaction_retry_count > max_transaction_retries:
                    raise
                backoff_s = min(
                    5.0,
                    0.1 * (2 ** max(0, transaction_retry_count - 1)),
                ) + random.uniform(0.0, 0.25)
                emit(
                    "think.transaction_retry",
                    run_id=str(run_id),
                    attempt=transaction_retry_count,
                    max_attempts=max_transaction_retries,
                    error_type=type(e).__name__,
                    backoff_s=round(backoff_s, 3),
                )
                await asyncio.sleep(backoff_s)
                continue

            outcome.elapsed_ms = (time.monotonic() - started_at) * 1000.0
            METRICS.observe_latency(trigger_kind_full, outcome.elapsed_ms)
            # OP-2: snapshot usage into the outcome + emit the cost record.
            if usage_agg is not None:
                outcome.llm_calls_count = usage_agg.call_count
                outcome.llm_input_tokens = usage_agg.total_input_tokens
                outcome.llm_output_tokens = usage_agg.total_output_tokens
                outcome.llm_cost_usd = usage_agg.total_cost_usd
                if llm_provider is not None:
                    outcome.llm_model_name = llm_provider.config.model
            await _record_cost_for_outcome(
                pool, outcome, trigger.tenant_id,
            )
            # Post-commit region_lock_log write (best-effort).
            if outcome.region_acquisition is not None:
                rla = outcome.region_acquisition
                released_at = time.monotonic()
                hold_ms = int((released_at - rla.acquired_at) * 1000)
                await write_region_lock_log(
                    pool,
                    tenant_id=trigger.tenant_id,
                    think_run_id=run_id,
                    tenant_hash=rla.tenant_hash,
                    entity_hash=rla.entity_hash,
                    entity_ids=rla.entity_ids,
                    acquired_at=rla.acquired_at,
                    released_at=released_at,
                    wait_duration_ms=rla.wait_duration_ms,
                    hold_duration_ms=hold_ms,
                )
                METRICS.observe_region_lock_wait(rla.wait_duration_ms)
            emit("think.completed",
                 run_id=str(run_id),
                 status=outcome.status,
                 elapsed_ms=outcome.elapsed_ms)
            return outcome
    except CompanyOSError as e:
        out = _fail_outcome(
            run_id, trigger_id, trigger_kind_full, e, started_at
        )
        _snapshot_usage(out, usage_agg, llm_provider)
        await _record_failed_run(pool, record, out.error)
        await _record_cost_for_outcome(pool, out, trigger.tenant_id)
        return out
    except Exception as e:
        out = _fail_outcome(
            run_id, trigger_id, trigger_kind_full, e, started_at
        )
        _snapshot_usage(out, usage_agg, llm_provider)
        await _record_failed_run(pool, record, out.error)
        await _record_cost_for_outcome(pool, out, trigger.tenant_id)
        return out
    finally:
        # Always detach the aggregator so it doesn't leak across runs.
        if usage_ctx is not None:
            usage_ctx.__exit__(None, None, None)
        if llm_provider is not None:
            llm_provider.set_usage_aggregator(None)
        # Phase 1 trace context — clear the per-task ContextVar so a
        # subsequent Think run on the same asyncio task starts clean.
        # `set_trace_context(None)` is cheap and safe even when no
        # context was ever installed.
        try:
            _sage_set_trace_context(None)
        except Exception:  # noqa: BLE001 — defensive; never raise here
            pass


def _snapshot_usage(
    outcome: ThinkRunOutcome,
    agg: LLMUsageAggregator | None,
    provider: LLMProvider | None,
) -> None:
    if agg is None:
        return
    outcome.llm_calls_count = agg.call_count
    outcome.llm_input_tokens = agg.total_input_tokens
    outcome.llm_output_tokens = agg.total_output_tokens
    outcome.llm_cost_usd = agg.total_cost_usd
    if provider is not None:
        outcome.llm_model_name = provider.config.model


async def _record_cost_for_outcome(
    pool: asyncpg.Pool,
    outcome: ThinkRunOutcome,
    tenant_id: UUID,
) -> None:
    """Map the outcome's status to the `think_run_costs.outcome` check
    constraint value, then emit the row. Best-effort — failures inside
    `record_think_run_cost` are already logged + swallowed."""
    status_map = {
        "success": "success",
        "skipped_idempotent": "skipped_idempotent",
        "failed": "failed",
    }
    outcome_kind = status_map.get(outcome.status, "failed")
    # Inspect exception type for richer classification.
    if outcome.exception is not None:
        exc_name = type(outcome.exception).__name__
        if "Validation" in exc_name or "ValidationFailure" in exc_name:
            outcome_kind = "validation_failure"
        elif "Reasoning" in exc_name or "ReasoningFailure" in exc_name:
            outcome_kind = "reasoning_exhausted"
    await record_think_run_cost(
        pool,
        trigger_id=outcome.trigger_id,
        tenant_id=tenant_id,
        trigger_kind=outcome.trigger_kind,
        outcome=outcome_kind,
        llm_calls_count=outcome.llm_calls_count,
        llm_input_tokens_total=outcome.llm_input_tokens,
        llm_output_tokens_total=outcome.llm_output_tokens,
        llm_cost_usd=outcome.llm_cost_usd,
        latency_total_ms=int(outcome.elapsed_ms),
        retry_count=0,
        model_name=outcome.llm_model_name,
    )


async def _record_failed_run(
    pool: asyncpg.Pool,
    record: ThinkRunRecord,
    error: str | None,
) -> None:
    """Persist a failed run after the apply transaction rolls back.

    The normal progressive `think_runs` row is written inside the
    mutation transaction so failed validation/apply attempts roll it
    back. Production operators still need a durable row explaining the
    failed invocation, so write a compact failed record out-of-band.
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO think_runs (
                  id, tenant_id, trigger_id, trigger_kind,
                  started_at, ended_at, status, error
                )
                VALUES ($1, $2, $3, $4, now(), now(), 'failed', $5)
                ON CONFLICT (id) DO UPDATE
                SET ended_at = now(),
                    status = 'failed',
                    error = EXCLUDED.error
                """,
                record.id,
                record.tenant_id,
                record.trigger_id,
                record.trigger_kind,
                (error or "failed")[:4000],
            )
    except Exception as exc:  # noqa: BLE001
        _log.warning(
            "think.failed_run_record_write_failed",
            run_id=str(record.id),
            error=str(exc),
        )


def _fail_outcome(
    run_id: UUID,
    trigger_id: UUID,
    trigger_kind: str,
    exc: BaseException,
    started_at: float,
) -> ThinkRunOutcome:
    METRICS.inc_failed(trigger_kind)
    emit("think.failed",
         run_id=str(run_id),
         trigger_id=str(trigger_id),
         trigger_kind=trigger_kind,
         error=str(exc),
         error_type=type(exc).__name__)
    return ThinkRunOutcome(
        run_id=run_id,
        trigger_id=trigger_id,
        trigger_kind=trigger_kind,
        status="failed",
        error=f"{type(exc).__name__}: {exc}",
        exception=exc,
        elapsed_ms=(time.monotonic() - started_at) * 1000.0,
    )


# ---------------------------------------------------------------------
# The in-tx body — runs inside `conn.transaction()`
# ---------------------------------------------------------------------


async def _run_once(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    llm_provider: LLMProvider | None,
    access_context: AccessContext | None,
    triggering_content: str | None,
    reason_for_trigger: str | None,
    record: ThinkRunRecord,
    expanded_region: set[tuple[str, str]] | None,
    embedder: Any | None = None,
) -> ThinkRunOutcome:
    """
    Run one Think attempt. Called by `think()`.

    If the caller has already opened a transaction, the whole attempt
    participates in it. Otherwise, pre-mutation work runs without an
    open transaction and this function opens a short mutation
    transaction only for think_runs, the advisory lock, validation,
    apply, anomalies, queueing, and cascade.
    """
    trigger_kind_full = record.trigger_kind

    from services.relationships.adjudication import (
        adjudicate_candidates_for_trigger,
        load_candidate_for_trigger,
    )
    loaded_relationship_candidate = await load_candidate_for_trigger(
        conn,
        trigger,
    )

    # --- 1. Context planning --------------------------------------
    context_plan = await plan_context(
        trigger,
        conn,
        embedder=embedder,
        llm_provider=llm_provider,
    )
    inquiry_result = context_plan.inquiry_result
    first = context_plan.retrieval_result
    reasoning_frame = context_plan.reasoning_frame
    await _assert_tx_usable(conn, "context_planning")
    emit("think.retrieval_done",
         run_id=str(record.id),
         models=len(first.models),
         observations=len(first.observations),
         pathways_run=first.notes.get("pathways_run"))
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="trigger",
        payload={
            "trigger_id": str(record.trigger_id),
            "trigger_kind": trigger_kind_full,
            "observation_id": str(trigger.observation_id)
                if getattr(trigger, "observation_id", None) else None,
            "triggering_content": triggering_content,
            "reason_for_trigger": reason_for_trigger,
            "reasoning_frame": reasoning_frame.to_dict(),
            "relationship_candidate": loaded_relationship_candidate,
        },
    )
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="retrieval",
        payload={
            "model_count": len(first.models),
            "observation_count": len(first.observations),
            "notes": first.notes,
            "models": [
                {
                    "id": str(getattr(m, "id", None)),
                    "proposition_kind": getattr(m, "proposition_kind", None),
                    "confidence": getattr(m, "confidence", None),
                    "proposition": getattr(m, "proposition", None),
                    "status": getattr(m, "status", None),
                }
                for m in first.models
            ],
            "observations": [
                {
                    "id": str(getattr(o, "id", None)),
                    "kind": getattr(o, "kind", None),
                    "source_channel": getattr(o, "source_channel", None),
                    "occurred_at": str(getattr(o, "occurred_at", None)),
                    "content_text": getattr(o, "content_text", None),
                }
                for o in first.observations
            ],
        },
    )
    if inquiry_result is not None:
        await debug_capture(
            conn,
            run_id=record.id,
            tenant_id=trigger.tenant_id,
            stage="inquiry",
            payload={
                "session_id": str(inquiry_result.session_id),
                "route": inquiry_result.route,
                "hypotheses": [
                    {
                        "id": h.id,
                        "claim": h.claim,
                        "confidence": h.confidence,
                        "impact_if_true": h.impact_if_true,
                    }
                    for h in inquiry_result.hypotheses
                ],
                "questions": [
                    {
                        "question_id": q.question_id,
                        "question": q.question,
                        "primitive": q.primitive,
                        "score": q.score,
                        "round_index": q.round_index,
                    }
                    for q in inquiry_result.questions
                ],
                "retrieval_actions": [
                    {
                        "question_id": a.question_id,
                        "path": a.path,
                        "target": a.target,
                        "budget": a.budget,
                    }
                    for a in inquiry_result.retrieval_actions
                ],
                "evidence_count": len(inquiry_result.evidence_cards),
            },
        )
        await debug_capture(
            conn,
            run_id=record.id,
            tenant_id=trigger.tenant_id,
            stage="sufficiency",
            payload={
                "status": inquiry_result.sufficiency.status,
                "reason": inquiry_result.sufficiency.reason,
                "evidence_count": inquiry_result.sufficiency.evidence_count,
                "answered_questions": inquiry_result.sufficiency.answered_questions,
                "remaining_unknowns": list(
                    inquiry_result.sufficiency.remaining_unknowns
                ),
            },
        )
        await debug_capture(
            conn,
            run_id=record.id,
            tenant_id=trigger.tenant_id,
            stage="context_packet",
            payload=inquiry_result.context_packet,
        )

    # Phase 1 trace wiring — install a TraceContext for the rest of the
    # pipeline so validator drops and applier successes feed the
    # `inquiry_outcome_events` table tied to this inquiry session. The
    # emitter is a no-op when no session is in flight (deterministic
    # T2 paths) or when SAGE_TRACE_EMIT=0.
    #
    # The matching reset lives in the outer `think()` `finally` block
    # so the context is always cleared once the run completes (success,
    # CompanyOSError, or unexpected exception). Subsequent Think runs on
    # the same asyncio task install a fresh context here on entry.
    if inquiry_result is not None and _sage_emission_enabled():
        sage_reader_notes = (inquiry_result.notes or {}).get("sage_reader")
        sage_signatures = (
            sage_reader_notes.get("signatures", [])
            if isinstance(sage_reader_notes, dict) else []
        )
        sage_question_primitives = []
        if isinstance(sage_reader_notes, dict):
            for qnote in (sage_reader_notes.get("questions") or {}).values():
                if isinstance(qnote, dict) and qnote.get("question_primitive"):
                    primitive = str(qnote["question_primitive"])
                    if primitive not in sage_question_primitives:
                        sage_question_primitives.append(primitive)
        _sage_set_trace_context(
            _SageTraceContext(
                tenant_id=trigger.tenant_id,
                inquiry_session_id=inquiry_result.session_id,
                conn=conn,
                metadata={
                    "trigger_kind": trigger_kind_full,
                    "signal_type": trigger.kind,
                    "entities": [
                        str(e.get("id") or e.get("name") or e.get("type"))
                        for e in trigger.seed_entity_ids
                        if isinstance(e, dict)
                    ][:12],
                    "question_primitives": sage_question_primitives[:8],
                    "sage_signatures": sage_signatures[:8],
                    "run_id": str(record.id),
                },
            )
        )

    # --- 2. Assemble model-facing context -------------------------
    reasoning_context = await assemble_reasoning_context(
        context_plan,
        trigger,
        conn,
        access_context=access_context,
        expanded_region=expanded_region,
        run_id=record.id,
    )
    bundle = reasoning_context.bundle
    allowed_region = reasoning_context.allowed_region
    actor_operating_summary = reasoning_context.actor_operating_summary
    await _assert_tx_usable(conn, "reasoning_context")

    th: int | None = None
    eh: int | None = None
    acquisition: RegionLockAcquisition | None = None
    mutation_row_inserted = False
    if conn.is_in_transaction():
        th, eh = region_lock_key(trigger.tenant_id, [
            (t, i) for (t, i) in allowed_region
        ])
        await insert_think_run(
            conn, record,
            region_tenant_hash=th,
            region_entity_hash=eh,
        )
        await update_think_run(
            conn, record.id,
            retrieval_model_count=len(bundle.models),
            retrieval_observation_count=len(bundle.observations),
        )
        acquisition = await acquire_region_lock(
            conn, trigger.tenant_id, [(t, i) for (t, i) in allowed_region]
        )
        mutation_row_inserted = True

    # --- 3. Reason ------------------------------------------------
    llm_latency_ms: int | None = None
    if is_authoritative(trigger):
        raw_diff = await deterministic_handler(trigger, bundle, conn)
    else:
        if llm_provider is None:
            raise ValidationError(
                "inferential trigger requires llm_provider",
                trigger_kind=trigger.kind,
            )
        raw_diff, llm_latency_ms = await llm_reason(
            trigger, bundle, llm_provider,
            triggering_content=triggering_content,
            triggering_actor_summary=actor_operating_summary,
            reason_for_trigger=reason_for_trigger,
            reasoning_frame=reasoning_frame,
        )
    # Ensure trigger_ref / tenant_id match what the caller expects —
    # even if the LLM hallucinated the fields, we overwrite for safety.
    from .deterministic import _trigger_ref  # type: ignore
    raw_diff.trigger_ref = _trigger_ref(trigger)
    raw_diff.tenant_id = trigger.tenant_id

    # Deterministic fallbacks for cases where the LLM consistently
    # refuses to emit the right diff:
    #   1. self-reported new work ("I've started X") → create_commitment
    #      recommendation when no matching commitment exists.
    #   2. blocked/on-hold/awaiting-approval signals → transition the
    #      best-matching commitment to 'blocked'.
    # Both injectors are idempotent — no-op if the LLM already produced
    # an equivalent op.
    from .auto_create_commitment import (
        maybe_inject_block_transition,
        maybe_inject_create_commitment,
        maybe_inject_customer_risk,
        maybe_inject_decision_revisit,
        maybe_inject_future_prediction,
    )

    raw_diff = maybe_inject_create_commitment(raw_diff, trigger, bundle)
    raw_diff = maybe_inject_block_transition(raw_diff, trigger, bundle)
    raw_diff = maybe_inject_decision_revisit(raw_diff, trigger, bundle)
    raw_diff = maybe_inject_future_prediction(raw_diff, trigger, bundle)
    raw_diff = maybe_inject_customer_risk(raw_diff, trigger, bundle)
    from .context_use import summarize_context_use
    raw_context_use = summarize_context_use(bundle, raw_diff)
    # Extend allowed_region for any transition target the deterministic
    # block injector picked, so the validator doesn't reject it.
    for op in raw_diff.act_ops:
        if op.op == "transition_commitment":
            ent = op.entity or {}
            tid = ent.get("id")
            if tid:
                allowed_region = sorted(
                    set(allowed_region) | {("commitment", str(tid))}
                )
        elif op.op == "transition_decision":
            ent = op.entity or {}
            tid = ent.get("id")
            if tid:
                allowed_region = sorted(
                    set(allowed_region) | {("decision", str(tid))}
                )

    async with _mutation_transaction(conn):
        if not mutation_row_inserted:
            th, eh = region_lock_key(trigger.tenant_id, [
                (t, i) for (t, i) in allowed_region
            ])
            await insert_think_run(
                conn, record,
                region_tenant_hash=th,
                region_entity_hash=eh,
            )
            await update_think_run(
                conn, record.id,
                retrieval_model_count=len(bundle.models),
                retrieval_observation_count=len(bundle.observations),
            )
            acquisition = await acquire_region_lock(
                conn,
                trigger.tenant_id,
                [(t, i) for (t, i) in allowed_region],
            )
        assert th is not None
        assert eh is not None
        assert acquisition is not None

        if llm_latency_ms is not None:
            await update_think_run(
                conn, record.id, llm_latency_ms=llm_latency_ms
            )
        await debug_capture(
            conn,
            run_id=record.id,
            tenant_id=trigger.tenant_id,
            stage="response",
            payload={
                "llm_latency_ms": llm_latency_ms,
                "is_authoritative": is_authoritative(trigger),
                "raw_diff": raw_diff,
                "reasoning_frame": reasoning_frame.to_dict(),
                "actor_operating_context": actor_operating_summary,
                "context_use": raw_context_use,
            },
        )

        # --- 4. Validate ---------------------------------------------
        validated = await validate(
            raw_diff, first, conn,
            allowed_region=allowed_region,
            strict_region=True,
        )
        validated_context_use = summarize_context_use(bundle, validated)
        METRICS.observe_context_use(trigger_kind_full, validated_context_use)
        emit(
            "think.context_use",
            run_id=str(record.id),
            grade=validated_context_use.get("context_use_grade"),
            selected_context_reference_ratio=validated_context_use.get(
                "selected_context_reference_ratio"
            ),
            selected_model_reference_ratio=validated_context_use.get(
                "selected_model_reference_ratio"
            ),
            graph_selected_reference_ratio=validated_context_use.get(
                "graph_selected_reference_ratio"
            ),
            selected_context_used=validated_context_use.get(
                "selected_context_used"
            ),
        )
        emit("think.validation_done",
             run_id=str(record.id),
             claim_ops=len(validated.claim_ops),
             edge_ops=len(validated.edge_ops),
             ontology_gap_ops=len(validated.ontology_gap_ops),
             act_ops=len(validated.act_ops),
             resource_ops=len(validated.resource_ops),
             dropped_ops=validated.dropped_op_count)
        if validated.dropped_op_count:
            emit("think.validation_partial",
                 run_id=str(record.id),
                 dropped=validated.dropped_op_count,
                 errors=validated.dropped_op_errors[:5])
        await update_think_run(
            conn, record.id,
            validation_error_count=validated.dropped_op_count,
        )
        await debug_capture(
            conn,
            run_id=record.id,
            tenant_id=trigger.tenant_id,
            stage="validation",
            payload={
                "claim_ops": validated.claim_ops,
                "edge_ops": validated.edge_ops,
                "ontology_gap_ops": validated.ontology_gap_ops,
                "act_ops": validated.act_ops,
                "resource_ops": validated.resource_ops,
                "dropped_op_count": validated.dropped_op_count,
                "dropped_op_errors": list(validated.dropped_op_errors[:20]),
                "context_use": validated_context_use,
            },
        )

        # --- 5. Apply ------------------------------------------------
        try:
            applied = await apply_diff(
                validated, conn,
                trigger_kind=trigger_kind_full,
                trigger_cause_event_id=trigger.observation_id,
                think_run_id=record.id,
            )
        except AlreadyAppliedError as e:
            await update_think_run(
                conn, record.id,
                status="skipped_idempotent",
                error=f"already applied: prior={e.context.get('prior_outcome')}",
            )
            emit("think.skipped_idempotent", run_id=str(record.id))
            return ThinkRunOutcome(
                run_id=record.id,
                trigger_id=record.trigger_id,
                trigger_kind=trigger_kind_full,
                status="skipped_idempotent",
                region_tenant_hash=th,
                region_entity_hash=eh,
                region_acquisition=acquisition,
                llm_latency_ms=llm_latency_ms,
            )
        await _assert_tx_usable(conn, "apply_diff")

        candidate_adjudications = await adjudicate_candidates_for_trigger(
            conn,
            trigger=trigger,
            diff=validated,
            applied=applied,
        )
        await _assert_tx_usable(conn, "relationship_adjudication")
        if candidate_adjudications:
            def _adjudication_payload(candidate_adjudication):
                return {
                    "candidate_id": str(candidate_adjudication.candidate_id),
                    "review_status": candidate_adjudication.review_status,
                    "reason": candidate_adjudication.reason,
                    "decision_reason": candidate_adjudication.decision_reason,
                    "accepted_model_id": (
                        str(candidate_adjudication.accepted_model_id)
                        if candidate_adjudication.accepted_model_id else None
                    ),
                    "accepted_edge_ids": [
                        str(edge_id)
                        for edge_id in candidate_adjudication.accepted_edge_ids
                    ],
                    "metadata": candidate_adjudication.metadata,
                }
            applied["relationship_candidate_adjudication"] = {
                **_adjudication_payload(candidate_adjudications[0]),
            }
            if len(candidate_adjudications) > 1:
                applied["relationship_candidate_adjudications"] = [
                    _adjudication_payload(adjudication)
                    for adjudication in candidate_adjudications
                ]

        emit("think.apply_done",
             run_id=str(record.id),
             ops_applied=(
                 len(applied["claim_ops"])
                 + len(applied["edge_ops"])
                 + len(applied.get("ontology_gap_ops", []))
                 + len(applied["act_ops"])
                 + len(applied["resource_ops"])
             ),
             state_changes=applied.get("state_changes_emitted", 0))
        applied["context_use"] = validated_context_use
        applied["reasoning_frame"] = reasoning_frame.to_dict()
        await debug_capture(
            conn,
            run_id=record.id,
            tenant_id=trigger.tenant_id,
            stage="apply",
            payload=applied,
        )

        # Track ops metrics per kind.
        for summary in applied.get("claim_ops", []):
            METRICS.inc_op(f"claim_{summary.get('op')}")
        for summary in applied.get("edge_ops", []):
            METRICS.inc_op(f"edge_{summary.get('op')}_{summary.get('edge_kind')}")
        for summary in applied.get("ontology_gap_ops", []):
            METRICS.inc_op(
                f"ontology_gap_{summary.get('op')}_{summary.get('proposed_edge_kind')}"
            )
        for summary in applied.get("act_ops", []):
            METRICS.inc_op(summary.get("op", "act_unknown"))
        for summary in applied.get("resource_ops", []):
            METRICS.inc_op(summary.get("op", "resource_unknown"))

        # --- 6. Anomalies ---------------------------------------------
        anomalies = await check_anomalies(validated, conn)
        await publish_anomalies(anomalies, record.id, trigger.tenant_id, conn)
        await _assert_tx_usable(conn, "anomaly_publish")
        emit("think.anomalies_published",
             run_id=str(record.id), count=len(anomalies))

        # --- 7. Post-commit durability queue (OP-1) -------------------
        # THINK-DESIGN-AUDIT §8.1, §10 arg 1. Post-commit side effects
        # (publish anomalies downstream, schedule predictions, broadcast
        # realtime, invalidate metrics) used to run inline after apply
        # committed — a crash between commit and post-commit swallowed
        # them and the idempotency ledger prevented re-running. Enqueuing
        # INSIDE this transaction makes the queue rows atomic with the
        # apply; a separate worker (services/think/post_commit.py::
        # post_commit_worker) drains the queue with at-least-once delivery
        # and dead-letters after MAX_ATTEMPTS=5 failures.
        anomaly_dicts = [
            {
                "kind": a.kind,
                "region": a.region,
                "significance": float(a.significance),
                "triggering_op": a.triggering_op,
            }
            for a in anomalies
        ]
        await enqueue_post_commit_actions(
            trigger, validated, conn, anomalies=anomaly_dicts,
        )
        await _assert_tx_usable(conn, "post_commit_enqueue")

        # --- 8. Cascade ----------------------------------------------
        casc_result: CascadeResult | None = None
        if validated.act_ops:
            # Pick the first applied act_op as the cascade seed.
            seed_op = validated.act_ops[0]
            if seed_op.op == "transition_commitment":
                cid = seed_op.entity.get("id")
                new_state = seed_op.entity.get("new_state")
                if cid:
                    # Grab the most recent state_change observation for this
                    # commitment to chain cause_id.
                    seed_obs = await conn.fetchval(
                        """
                        SELECT id FROM observations
                        WHERE kind = 'state_change'
                          AND tenant_id = $2
                          AND entities_mentioned @> $1::jsonb
                        ORDER BY occurred_at DESC
                        LIMIT 1
                        """,
                        _entities_filter("commitment", cid),
                        trigger.tenant_id,
                    )
                    seed_event = CascadeEvent(
                        id=uuid7(),
                        kind="commitment_state_change",
                        entity_kind="commitment",
                        entity_id=UUID(str(cid)),
                        tenant_id=trigger.tenant_id,
                        metadata={"new_state": new_state},
                        observation_id=seed_obs,
                    )
                    casc_result = await cascade(seed_event, conn)
            elif seed_op.op == "transition_decision" and seed_op.entity.get("new_state") == "revisited":
                did = seed_op.entity.get("id")
                if did:
                    seed_obs = await conn.fetchval(
                        """
                        SELECT id FROM observations
                        WHERE kind = 'state_change'
                          AND tenant_id = $2
                          AND entities_mentioned @> $1::jsonb
                        ORDER BY occurred_at DESC
                        LIMIT 1
                        """,
                        _entities_filter("decision", did),
                        trigger.tenant_id,
                    )
                    seed_event = CascadeEvent(
                        id=uuid7(),
                        kind="decision_revisited",
                        entity_kind="decision",
                        entity_id=UUID(str(did)),
                        tenant_id=trigger.tenant_id,
                        metadata={},
                        observation_id=seed_obs,
                    )
                    casc_result = await cascade(seed_event, conn)
        await _assert_tx_usable(conn, "cascade")
        cascade_depth = casc_result.depth_reached if casc_result else 0
        if casc_result is not None:
            METRICS.observe_cascade_depth(trigger_kind_full, cascade_depth)
        await update_think_run(
            conn, record.id,
            status="success",
            ops_applied=applied,
            cascade_depth=cascade_depth,
        )
        emit("think.committed",
             run_id=str(record.id),
             cascade_depth=cascade_depth)

        return ThinkRunOutcome(
            run_id=record.id,
            trigger_id=record.trigger_id,
            trigger_kind=trigger_kind_full,
            status="success",
            ops_applied_count=(
                len(applied["claim_ops"])
                + len(applied["edge_ops"])
                + len(applied.get("ontology_gap_ops", []))
                + len(applied["act_ops"])
                + len(applied["resource_ops"])
            ),
            cascade_depth=cascade_depth,
            anomalies_flagged=len(anomalies),
            llm_latency_ms=llm_latency_ms,
            region_tenant_hash=th,
            region_entity_hash=eh,
            region_acquisition=acquisition,
        )


def _entities_filter(kind: str, id_: Any) -> str:
    import json as _json
    return _json.dumps([{"type": kind, "id": str(id_)}])


__all__ = [
    "think",
    "ThinkRunOutcome",
]
