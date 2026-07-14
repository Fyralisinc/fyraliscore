"""services/reasoning/think/reason.py — the cognitive pipeline entry point.

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
)
from lib.shared.ids import uuid7
from services.domain.triggers import enqueue_trigger

from services.reasoning.retrieval.assembler import (
    AccessContext,
    ContextBundle,
)
from services.reasoning.retrieval.primary import (
    RetrievalResult,
    TriggerContext,
)
from services.reasoning.sage.inquiry_traces.emitter import (
    set_trace_context as _sage_set_trace_context,
)
from services.reasoning.sage.latent_gaps import create_latent_gap_hypotheses_for_sources

from .anomaly_integration import (
    check_anomalies,
    publish_anomalies,
)
from .applier import AlreadyAppliedError, apply_diff, check_already_applied
from .debug_capture import capture as debug_capture
from .debug_capture import defer_transactional_captures
from .debug_capture import capture_with_pool as debug_capture_with_pool
from .debug_capture import flush_captures as flush_debug_captures
from .deterministic import is_authoritative
from .lanes import classify_trigger_lane
from .llm_reason import build_noise_only_raw_diff
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
from .representation_audit import (
    build_representation_audit,
    persist_representation_audit,
)
from .region_locks import (
    RegionLockAcquisition,
    acquire_region_lock,
    region_lock_key,
)
from .coherence_repair import (
    ResidualRepairResolution,
    enqueue_residual_repair_triggers_for_sources,
    resolve_residual_repair_outcome,
)
from .reasoning_frame import ReasoningFrame
from .residuals import (
    ThinkResidualContext,
    absorb_think_residuals,
    persist_think_residuals,
)
from .run_pipeline import (
    _diff_reuse_on_tx_retry_enabled,  # noqa: F401
    _hash_context_bundle,  # noqa: F401
    RawReasoningOutput,
    ReasoningRunState,
    assert_tx_usable,
    build_raw_reasoning_output,
    prepare_reasoning_run_state,
    record_stage_timing,
    run_cascade_for_validated_act_ops,
    validate_raw_reasoning_output,
)
from .validator import (
    OutOfRegionError,
)


_log = structlog.get_logger(__name__)


def _raise_if_postgres_error(exc: Exception) -> None:
    """A swallowed SQL error leaves the active transaction unusable."""
    if isinstance(exc, asyncpg.PostgresError):
        raise exc


def _early_idempotency_skip_enabled() -> bool:
    """Cost-plan §2.2: when set, check `applied_triggers` at the top of `think()`
    and skip retrieval + reasoning entirely for an already-applied trigger.
    Default off — the in-apply AlreadyAppliedError guard is the correctness
    backstop either way; this only saves the wasted reasoning spend."""
    return os.environ.get("THINK_EARLY_IDEMPOTENCY_SKIP", "0").strip().lower() in {
        "1",
        "on",
        "true",
        "yes",
    }


def _narrow_inferential_transaction_enabled() -> bool:
    return os.environ.get("THINK_NARROW_INFERENTIAL_TX", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _representation_repair_triggers_enabled() -> bool:
    return os.environ.get(
        "THINK_REPRESENTATION_REPAIR_TRIGGERS", "1"
    ).strip().lower() in {
        "1",
        "on",
        "true",
        "yes",
    }


def _representation_repair_max_triggers(default: int = 3) -> int:
    raw = os.environ.get("THINK_REPRESENTATION_REPAIR_MAX_TRIGGERS")
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


@asynccontextmanager
async def _mutation_transaction(conn: asyncpg.Connection):
    if conn.is_in_transaction():
        yield
    else:
        async with conn.transaction():
            yield


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
    # Cost-plan §0.1: cached-input subset of llm_input_tokens.
    llm_cache_read_tokens: int = 0
    llm_cache_creation_tokens: int = 0
    # Raised exception for caller's failure classification.
    exception: BaseException | None = None
    residual_context: ThinkResidualContext | None = None

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
    side-effectful reasoning. Inferential triggers default to the narrow
    path: retrieval/planning and LLM reasoning run outside an explicit DB
    transaction, then validation/apply/cascade run inside the short mutation
    transaction. Set THINK_NARROW_INFERENTIAL_TX=0 only as an emergency
    rollback to the legacy wide transaction.

    For tests that want to drive everything inside one pre-opened
    transaction (ROLLBACK at teardown), use `think_in_conn` instead —
    see worker.py for the LISTEN/poll-driven caller that uses this.
    """
    started_at = time.monotonic()
    trigger_id, trigger_kind_full, record = _start_think_record(
        trigger,
        trigger_kind_subkind=trigger_kind_subkind,
    )
    skipped = await _try_early_idempotency_skip(
        pool,
        trigger=trigger,
        run_id=record.id,
        trigger_id=trigger_id,
        trigger_kind_full=trigger_kind_full,
        started_at=started_at,
    )
    if skipped is not None:
        return skipped

    usage_agg, usage_ctx = _install_usage_aggregator(llm_provider)
    rerun_count = 0
    transaction_retry_count = 0
    max_transaction_retries = int(
        os.environ.get("THINK_TRANSACTION_RETRY_ATTEMPTS", "8")
    )
    expanded_region: set[tuple[str, str]] | None = None
    reason_cache: dict[str, Any] = {}

    try:
        while True:
            try:
                outcome = await _run_think_attempt(
                    pool,
                    trigger=trigger,
                    llm_provider=llm_provider,
                    embedder=embedder,
                    access_context=access_context,
                    triggering_content=triggering_content,
                    reason_for_trigger=reason_for_trigger,
                    record=record,
                    expanded_region=expanded_region,
                    reason_cache=reason_cache,
                )
            except OutOfRegionError as e:
                rerun_count += 1
                if rerun_count > max_retrieval_reruns:
                    return await _handle_out_of_region_exhausted(
                        pool,
                        trigger=trigger,
                        record=record,
                        trigger_kind_full=trigger_kind_full,
                        trigger_id=trigger_id,
                        exc=e,
                        started_at=started_at,
                        rerun_count=rerun_count,
                        retry_count=transaction_retry_count + rerun_count,
                        usage_agg=usage_agg,
                        llm_provider=llm_provider,
                    )
                expanded_region = _expand_region_after_out_of_region(expanded_region, e)
                emit(
                    "think.out_of_region",
                    run_id=str(record.id),
                    attempt=rerun_count,
                    missing=e.context.get("missing"),
                )
                reason_cache.clear()
                continue
            except (
                asyncpg.exceptions.DeadlockDetectedError,
                asyncpg.exceptions.SerializationError,
            ) as e:
                transaction_retry_count += 1
                if transaction_retry_count > max_transaction_retries:
                    raise
                await _sleep_before_transaction_retry(
                    run_id=record.id,
                    attempt=transaction_retry_count,
                    max_attempts=max_transaction_retries,
                    exc=e,
                )
                continue

            await _finalize_successful_outcome(
                pool,
                trigger=trigger,
                run_id=record.id,
                outcome=outcome,
                started_at=started_at,
                trigger_kind_full=trigger_kind_full,
                retry_count=transaction_retry_count + rerun_count,
                usage_agg=usage_agg,
                llm_provider=llm_provider,
            )
            return outcome
    except CompanyOSError as e:
        return await _record_failed_outcome(
            pool,
            trigger=trigger,
            record=record,
            trigger_id=trigger_id,
            trigger_kind_full=trigger_kind_full,
            exc=e,
            started_at=started_at,
            retry_count=transaction_retry_count + rerun_count,
            usage_agg=usage_agg,
            llm_provider=llm_provider,
        )
    except Exception as e:
        return await _record_failed_outcome(
            pool,
            trigger=trigger,
            record=record,
            trigger_id=trigger_id,
            trigger_kind_full=trigger_kind_full,
            exc=e,
            started_at=started_at,
            retry_count=transaction_retry_count + rerun_count,
            usage_agg=usage_agg,
            llm_provider=llm_provider,
        )
    finally:
        _detach_usage_and_trace(llm_provider, usage_ctx)


def _start_think_record(
    trigger: TriggerContext,
    *,
    trigger_kind_subkind: str | None,
) -> tuple[UUID, str, ThinkRunRecord]:
    from .deterministic import _trigger_ref  # type: ignore

    trigger_id = _trigger_ref(trigger)
    trigger_kind_full = trigger_kind_subkind or trigger.kind
    run_id = uuid7()
    seed_payload = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    lane_decision = classify_trigger_lane(
        trigger.kind,
        trigger.subkind,
        seed_payload,
    )
    record = ThinkRunRecord(
        id=run_id,
        tenant_id=trigger.tenant_id,
        trigger_id=trigger_id,
        trigger_kind=trigger_kind_full,
        lane=lane_decision.lane.value,
    )

    METRICS.inc_run(trigger_kind_full)
    emit(
        "think.started",
        run_id=str(run_id),
        trigger_id=str(trigger_id),
        trigger_kind=trigger_kind_full,
        lane=record.lane,
        lane_reason=lane_decision.reason,
        tenant_id=str(trigger.tenant_id),
    )
    return trigger_id, trigger_kind_full, record


async def _try_early_idempotency_skip(
    pool: asyncpg.Pool,
    *,
    trigger: TriggerContext,
    run_id: UUID,
    trigger_id: UUID,
    trigger_kind_full: str,
    started_at: float,
) -> ThinkRunOutcome | None:
    if not _early_idempotency_skip_enabled():
        return None
    prior_outcome: str | None = None
    try:
        async with pool.acquire() as conn:
            prior_outcome = await check_already_applied(conn, trigger_id)
    except Exception as exc:  # noqa: BLE001 — pre-check never fails the run
        _log.warning("think.early_idempotency_check_failed", error=str(exc))
    if prior_outcome is None:
        return None
    emit(
        "think.skipped_idempotent",
        run_id=str(run_id),
        reason="early_precheck",
        prior_outcome=prior_outcome,
    )
    out = ThinkRunOutcome(
        run_id=run_id,
        trigger_id=trigger_id,
        trigger_kind=trigger_kind_full,
        status="skipped_idempotent",
        elapsed_ms=(time.monotonic() - started_at) * 1000.0,
    )
    await _record_cost_for_outcome(pool, out, trigger.tenant_id)
    return out


def _install_usage_aggregator(
    llm_provider: LLMProvider | None,
) -> tuple[LLMUsageAggregator | None, Any | None]:
    if llm_provider is None:
        return None, None
    usage_agg = LLMUsageAggregator()
    usage_ctx = using_usage_aggregator(usage_agg)
    usage_ctx.__enter__()
    llm_provider.set_usage_aggregator(usage_agg)
    return usage_agg, usage_ctx


async def _run_think_attempt(
    pool: asyncpg.Pool,
    *,
    trigger: TriggerContext,
    llm_provider: LLMProvider | None,
    embedder: Any | None,
    access_context: AccessContext | None,
    triggering_content: str | None,
    reason_for_trigger: str | None,
    record: ThinkRunRecord,
    expanded_region: set[tuple[str, str]] | None,
    reason_cache: dict[str, Any],
) -> ThinkRunOutcome:
    use_wide_transaction = (
        is_authoritative(trigger) or not _narrow_inferential_transaction_enabled()
    )
    with defer_transactional_captures() as debug_scope:
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
                        reason_cache=reason_cache,
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
                    reason_cache=reason_cache,
                )
    await flush_debug_captures(pool, debug_scope.artifacts)
    return outcome


def _expand_region_after_out_of_region(
    expanded_region: set[tuple[str, str]] | None,
    exc: OutOfRegionError,
) -> set[tuple[str, str]]:
    region = expanded_region or set()
    missing = exc.context.get("missing") or []
    region.update((t, i) for (t, i) in missing)
    return region


async def _sleep_before_transaction_retry(
    *,
    run_id: UUID,
    attempt: int,
    max_attempts: int,
    exc: Exception,
) -> None:
    backoff_s = min(5.0, 0.1 * (2 ** max(0, attempt - 1))) + random.uniform(0.0, 0.25)
    emit(
        "think.transaction_retry",
        run_id=str(run_id),
        attempt=attempt,
        max_attempts=max_attempts,
        error_type=type(exc).__name__,
        backoff_s=round(backoff_s, 3),
    )
    await asyncio.sleep(backoff_s)


async def _handle_out_of_region_exhausted(
    pool: asyncpg.Pool,
    *,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    trigger_id: UUID,
    exc: OutOfRegionError,
    started_at: float,
    rerun_count: int,
    retry_count: int,
    usage_agg: LLMUsageAggregator | None,
    llm_provider: LLMProvider | None,
) -> ThinkRunOutcome:
    METRICS.inc_failed(trigger_kind_full)
    emit(
        "think.failed",
        run_id=str(record.id),
        error="out_of_region_exhausted",
        rerun_count=rerun_count,
    )
    out = ThinkRunOutcome(
        run_id=record.id,
        trigger_id=trigger_id,
        trigger_kind=trigger_kind_full,
        status="failed",
        error=f"out_of_region_after_{rerun_count}_reruns: {exc.message}",
        exception=exc,
        elapsed_ms=(time.monotonic() - started_at) * 1000.0,
    )
    _snapshot_usage(out, usage_agg, llm_provider)
    await _record_failed_run(pool, record, out.error)
    await _record_cost_for_outcome(
        pool,
        out,
        trigger.tenant_id,
        retry_count=retry_count,
        usage_agg=usage_agg,
    )
    return out


async def _finalize_successful_outcome(
    pool: asyncpg.Pool,
    *,
    trigger: TriggerContext,
    run_id: UUID,
    outcome: ThinkRunOutcome,
    started_at: float,
    trigger_kind_full: str,
    retry_count: int,
    usage_agg: LLMUsageAggregator | None,
    llm_provider: LLMProvider | None,
) -> None:
    outcome.elapsed_ms = (time.monotonic() - started_at) * 1000.0
    METRICS.observe_latency(trigger_kind_full, outcome.elapsed_ms)
    _snapshot_usage(outcome, usage_agg, llm_provider)
    await _record_cost_for_outcome(
        pool,
        outcome,
        trigger.tenant_id,
        retry_count=retry_count,
        usage_agg=usage_agg,
    )
    await _record_region_lock_release(pool, trigger, run_id, outcome)
    await _record_success_residuals(pool, outcome)
    emit(
        "think.completed",
        run_id=str(run_id),
        status=outcome.status,
        elapsed_ms=outcome.elapsed_ms,
    )


async def _record_success_residuals(
    pool: asyncpg.Pool,
    outcome: ThinkRunOutcome,
) -> None:
    context = outcome.residual_context
    if context is None:
        return
    stage_timings = _stage_timings_from_ops_summary(context.ops_applied_summary)
    try:
        started = time.perf_counter()
        repair_resolution = await resolve_residual_repair_outcome(pool, context)
    except Exception as exc:  # noqa: BLE001 - repair resolution must not fail Think
        record_stage_timing(
            stage_timings,
            "residual_repair_resolution",
            started,
            error_type=type(exc).__name__,
        )
        _log.warning(
            "think.residual_repair_resolution_failed",
            run_id=str(outcome.run_id),
            trigger_id=str(outcome.trigger_id),
            error=str(exc),
        )
        repair_resolution = None
    else:
        record_stage_timing(
            stage_timings,
            "residual_repair_resolution",
            started,
            resolved=repair_resolution.resolved if repair_resolution else None,
        )
    if repair_resolution is not None and repair_resolution.terminal:
        emit(
            "think.residual_repair_resolved",
            run_id=str(outcome.run_id),
            trigger_id=str(outcome.trigger_id),
            residual_id=str(repair_resolution.residual_id),
            status=repair_resolution.status,
            reason=repair_resolution.reason,
            resolved=repair_resolution.resolved,
        )
        await _record_residual_repair_resolution_observability(
            pool,
            outcome,
            repair_resolution=repair_resolution,
            stage_timings=stage_timings,
        )
        return
    absorbed_count = 0
    residual_creation_count = 0
    repair_trigger_count = 0
    latent_gap_count = 0
    try:
        started = time.perf_counter()
        absorbed_count = await absorb_think_residuals(pool, context)
    except Exception as exc:  # noqa: BLE001 - residuals must not fail Think
        record_stage_timing(
            stage_timings,
            "residual_absorption",
            started,
            error_type=type(exc).__name__,
        )
        _log.warning(
            "think.residual_absorption_failed",
            run_id=str(outcome.run_id),
            trigger_id=str(outcome.trigger_id),
            error=str(exc),
        )
    else:
        record_stage_timing(
            stage_timings,
            "residual_absorption",
            started,
            absorbed_count=absorbed_count,
        )
        if absorbed_count:
            emit(
                "think.residuals_absorbed",
                run_id=str(outcome.run_id),
                trigger_id=str(outcome.trigger_id),
                residual_count=absorbed_count,
            )
    try:
        started = time.perf_counter()
        residual_count = await persist_think_residuals(pool, context)
    except Exception as exc:  # noqa: BLE001 - residuals must not fail Think
        record_stage_timing(
            stage_timings,
            "residual_creation",
            started,
            error_type=type(exc).__name__,
        )
        _log.warning(
            "think.residual_write_failed",
            run_id=str(outcome.run_id),
            trigger_id=str(outcome.trigger_id),
            error=str(exc),
        )
        if stage_timings:
            await _record_residual_post_success_observability(
                pool,
                outcome,
                absorbed_count=absorbed_count,
                residual_creation_count=0,
                repair_trigger_count=repair_trigger_count,
                latent_gap_count=latent_gap_count,
                stage_timings=stage_timings,
            )
        return
    else:
        record_stage_timing(
            stage_timings,
            "residual_creation",
            started,
            residual_count=residual_count,
        )
    residual_creation_count = residual_count
    if residual_count:
        emit(
            "think.residuals_recorded",
            run_id=str(outcome.run_id),
            trigger_id=str(outcome.trigger_id),
            residual_count=residual_count,
        )
    try:
        started = time.perf_counter()
        repair_triggers = await enqueue_residual_repair_triggers_for_sources(
            pool,
            tenant_id=context.tenant_id,
            source_observation_ids=context.source_observation_ids,
            cascade_depth=_residual_repair_cascade_depth(context),
        )
    except Exception as exc:  # noqa: BLE001 - repair scheduling must not fail Think
        record_stage_timing(
            stage_timings,
            "residual_repair_enqueue",
            started,
            error_type=type(exc).__name__,
        )
        _log.warning(
            "think.residual_repair_enqueue_failed",
            run_id=str(outcome.run_id),
            trigger_id=str(outcome.trigger_id),
            error=str(exc),
        )
    else:
        repair_trigger_count = len(repair_triggers)
        record_stage_timing(
            stage_timings,
            "residual_repair_enqueue",
            started,
            repair_trigger_count=repair_trigger_count,
        )
        if repair_trigger_count:
            emit(
                "think.residual_repairs_scheduled",
                run_id=str(outcome.run_id),
                trigger_id=str(outcome.trigger_id),
                repair_trigger_count=repair_trigger_count,
            )
    try:
        started = time.perf_counter()
        latent_gaps = await create_latent_gap_hypotheses_for_sources(
            pool,
            tenant_id=context.tenant_id,
            source_observation_ids=context.source_observation_ids,
        )
    except Exception as exc:  # noqa: BLE001 - latent gaps must not fail Think
        record_stage_timing(
            stage_timings,
            "latent_gap_hypothesis_create",
            started,
            error_type=type(exc).__name__,
        )
        _log.warning(
            "think.latent_gap_hypothesis_create_failed",
            run_id=str(outcome.run_id),
            trigger_id=str(outcome.trigger_id),
            error=str(exc),
        )
    else:
        latent_gap_count = len(latent_gaps)
        record_stage_timing(
            stage_timings,
            "latent_gap_hypothesis_create",
            started,
            latent_gap_count=latent_gap_count,
        )
        if latent_gap_count:
            emit(
                "think.latent_gap_hypotheses_created",
                run_id=str(outcome.run_id),
                trigger_id=str(outcome.trigger_id),
                latent_gap_count=latent_gap_count,
            )
    if (
        absorbed_count
        or residual_creation_count
        or repair_trigger_count
        or latent_gap_count
        or stage_timings
    ):
        await _record_residual_post_success_observability(
            pool,
            outcome,
            absorbed_count=absorbed_count,
            residual_creation_count=residual_creation_count,
            repair_trigger_count=repair_trigger_count,
            latent_gap_count=latent_gap_count,
            stage_timings=stage_timings,
        )


async def _record_residual_post_success_observability(
    pool: asyncpg.Pool,
    outcome: ThinkRunOutcome,
    *,
    absorbed_count: int,
    residual_creation_count: int,
    repair_trigger_count: int,
    latent_gap_count: int,
    stage_timings: list[dict[str, Any]] | None = None,
) -> None:
    context = outcome.residual_context
    if context is None:
        return
    ops_summary = dict(context.ops_applied_summary or {})
    if absorbed_count:
        ops_summary["residual_absorptions"] = {
            "count": absorbed_count,
            "source": "think_success_residual_absorber",
        }
    if residual_creation_count:
        ops_summary["residual_creations"] = {
            "count": residual_creation_count,
            "source": "think_success_residual_writer",
        }
    if repair_trigger_count:
        ops_summary["residual_repair_triggers"] = {
            "count": repair_trigger_count,
            "source": "think_success_residual_repair_scheduler",
        }
    if latent_gap_count:
        ops_summary["latent_gap_hypotheses"] = {
            "count": latent_gap_count,
            "source": "think_success_latent_gap_creator",
        }
    _attach_think_stage_timing_summary(ops_summary, stage_timings)
    try:
        async with pool.acquire() as conn:
            await update_think_run(conn, outcome.run_id, ops_applied=ops_summary)
    except Exception as exc:  # noqa: BLE001 - post-success observability only
        _log.warning(
            "think.residual_post_success_observability_failed",
            run_id=str(outcome.run_id),
            trigger_id=str(outcome.trigger_id),
            error=str(exc),
        )


async def _record_residual_repair_resolution_observability(
    pool: asyncpg.Pool,
    outcome: ThinkRunOutcome,
    *,
    repair_resolution: ResidualRepairResolution,
    stage_timings: list[dict[str, Any]] | None = None,
) -> None:
    context = outcome.residual_context
    if context is None:
        return
    ops_summary = dict(context.ops_applied_summary or {})
    ops_summary["residual_repair_resolution"] = {
        "residual_id": str(repair_resolution.residual_id),
        "status": repair_resolution.status,
        "reason": repair_resolution.reason,
        "resolved": repair_resolution.resolved,
        "source": "residual_repair_resolution",
    }
    _attach_think_stage_timing_summary(ops_summary, stage_timings)
    try:
        async with pool.acquire() as conn:
            await update_think_run(conn, outcome.run_id, ops_applied=ops_summary)
    except Exception as exc:  # noqa: BLE001 - post-success observability only
        _log.warning(
            "think.residual_repair_resolution_observability_failed",
            run_id=str(outcome.run_id),
            trigger_id=str(outcome.trigger_id),
            error=str(exc),
        )


def _residual_repair_cascade_depth(context: ThinkResidualContext) -> int:
    if context.repair_cascade_depth is not None:
        return max(0, int(context.repair_cascade_depth)) + 1
    summary = context.ops_applied_summary
    reasoning_frame = (
        summary.get("reasoning_frame") if isinstance(summary, dict) else None
    )
    if not isinstance(reasoning_frame, dict):
        return 0
    raw = reasoning_frame.get("cascade_depth", 0)
    try:
        return max(0, int(raw)) + 1
    except (TypeError, ValueError):
        return 1


async def _record_region_lock_release(
    pool: asyncpg.Pool,
    trigger: TriggerContext,
    run_id: UUID,
    outcome: ThinkRunOutcome,
) -> None:
    if outcome.region_acquisition is None:
        return
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


async def _record_failed_outcome(
    pool: asyncpg.Pool,
    *,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    trigger_id: UUID,
    trigger_kind_full: str,
    exc: BaseException,
    started_at: float,
    retry_count: int,
    usage_agg: LLMUsageAggregator | None,
    llm_provider: LLMProvider | None,
) -> ThinkRunOutcome:
    out = _fail_outcome(record.id, trigger_id, trigger_kind_full, exc, started_at)
    _snapshot_usage(out, usage_agg, llm_provider)
    await debug_capture_with_pool(
        pool,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="error",
        payload={
            "trigger_id": str(trigger_id),
            "trigger_kind": trigger_kind_full,
            "error": out.error,
            "error_type": type(exc).__name__,
            "exception_repr": repr(exc),
        },
    )
    await _record_failed_run(pool, record, out.error)
    await _record_cost_for_outcome(
        pool,
        out,
        trigger.tenant_id,
        retry_count=retry_count,
        usage_agg=usage_agg,
    )
    return out


def _detach_usage_and_trace(
    llm_provider: LLMProvider | None,
    usage_ctx: Any | None,
) -> None:
    if usage_ctx is not None:
        usage_ctx.__exit__(None, None, None)
    if llm_provider is not None:
        llm_provider.set_usage_aggregator(None)
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
    outcome.llm_cache_read_tokens = agg.total_cache_read_tokens
    outcome.llm_cache_creation_tokens = agg.total_cache_creation_tokens
    if provider is not None:
        outcome.llm_model_name = provider.config.model


async def _record_cost_for_outcome(
    pool: asyncpg.Pool,
    outcome: ThinkRunOutcome,
    tenant_id: UUID,
    *,
    retry_count: int = 0,
    usage_agg: LLMUsageAggregator | None = None,
) -> None:
    """Map the outcome's status to the `think_run_costs.outcome` check
    constraint value, then emit the cost row(s). Best-effort — failures inside
    `record_think_run_cost` are already logged + swallowed.

    Cost-plan §0.1: when a usage aggregator is available, emit one row per
    `purpose` (main_reasoning / question_planning / parse_repair) so planning
    and repair spend is no longer hidden inside the main call. `retry_count` is
    the real `transaction_retry_count + rerun_count` (was hardcoded 0) and is
    attributed to the main_reasoning row; run-level latency likewise."""
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

    by_purpose = usage_agg.by_purpose() if usage_agg is not None else {}
    if not by_purpose:
        # No per-call breakdown (e.g. deterministic run with no LLM calls) —
        # a single aggregate row from the outcome fields, as before.
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
            retry_count=retry_count,
            model_name=outcome.llm_model_name,
            purpose="main_reasoning",
            cache_read_input_tokens=outcome.llm_cache_read_tokens,
            cache_creation_input_tokens=outcome.llm_cache_creation_tokens,
        )
        return

    assert usage_agg is not None
    for purpose, usage in by_purpose.items():
        is_main = purpose == "main_reasoning"
        await record_think_run_cost(
            pool,
            trigger_id=outcome.trigger_id,
            tenant_id=tenant_id,
            trigger_kind=outcome.trigger_kind,
            outcome=outcome_kind,
            llm_calls_count=usage_agg.call_count_for(purpose),
            llm_input_tokens_total=usage.input_tokens,
            llm_output_tokens_total=usage.output_tokens,
            llm_cost_usd=usage.cost_usd,
            # Latency + retries are run-level; attribute to the main row only so
            # SUM across purposes doesn't double-count them.
            latency_total_ms=int(outcome.elapsed_ms) if is_main else 0,
            retry_count=retry_count if is_main else 0,
            model_name=usage.model_name or outcome.llm_model_name,
            purpose=purpose,
            cache_read_input_tokens=usage.cache_read_tokens,
            cache_creation_input_tokens=usage.cache_creation_tokens,
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
                  id, tenant_id, trigger_id, trigger_kind, lane,
                  started_at, ended_at, status, error
                )
                VALUES ($1, $2, $3, $4, $5, now(), now(), 'failed', $6)
                ON CONFLICT (id) DO UPDATE
                SET ended_at = now(),
                    status = 'failed',
                    lane = COALESCE(EXCLUDED.lane, think_runs.lane),
                    error = EXCLUDED.error
                """,
                record.id,
                record.tenant_id,
                record.trigger_id,
                record.trigger_kind,
                record.lane,
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
    emit(
        "think.failed",
        run_id=str(run_id),
        trigger_id=str(trigger_id),
        trigger_kind=trigger_kind,
        error=str(exc),
        error_type=type(exc).__name__,
    )
    return ThinkRunOutcome(
        run_id=run_id,
        trigger_id=trigger_id,
        trigger_kind=trigger_kind,
        status="failed",
        error=f"{type(exc).__name__}: {exc}",
        exception=exc,
        elapsed_ms=(time.monotonic() - started_at) * 1000.0,
    )


def _stage_timing_elapsed_ms(note: dict[str, Any]) -> float | None:
    try:
        elapsed_ms = float(note.get("elapsed_ms"))
    except (TypeError, ValueError):
        return None
    return elapsed_ms if elapsed_ms >= 0 else None


def _stage_timings_from_ops_summary(
    ops_summary: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not isinstance(ops_summary, dict):
        return []
    raw_timings = ops_summary.get("think_stage_timings")
    if not isinstance(raw_timings, list):
        return []
    return [dict(note) for note in raw_timings if isinstance(note, dict)]


def _attach_think_stage_timing_summary(
    ops_summary: dict[str, Any],
    stage_timings: list[dict[str, Any]] | None,
) -> None:
    if not stage_timings:
        return

    normalized: list[dict[str, Any]] = []
    total_ms = 0.0
    llm_ms = 0.0
    non_llm_ms = 0.0
    for raw_note in stage_timings:
        if not isinstance(raw_note, dict):
            continue
        elapsed_ms = _stage_timing_elapsed_ms(raw_note)
        if elapsed_ms is None:
            continue
        note = dict(raw_note)
        note["elapsed_ms"] = round(elapsed_ms, 3)
        normalized.append(note)
        total_ms += elapsed_ms
        if note.get("is_llm"):
            llm_ms += elapsed_ms
        else:
            non_llm_ms += elapsed_ms

    if not normalized:
        return
    ops_summary["think_stage_timings"] = normalized
    ops_summary["think_stage_timings_ms_total"] = round(total_ms, 3)
    ops_summary["think_llm_stage_timings_ms_total"] = round(llm_ms, 3)
    ops_summary["think_non_llm_stage_timings_ms_total"] = round(non_llm_ms, 3)


async def _apply_validated_diff(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    validated: Any,
    region_tenant_hash: int,
    region_entity_hash: int,
    acquisition: RegionLockAcquisition,
    llm_latency_ms: int | None,
) -> tuple[dict[str, Any] | None, ThinkRunOutcome | None]:
    try:
        applied = await apply_diff(
            validated,
            conn,
            trigger_kind=trigger_kind_full,
            trigger_cause_event_id=trigger.observation_id,
            trigger_supporting_event_ids=list(trigger.observation_ids or []),
            think_run_id=record.id,
            parent_cascade_payload=(
                trigger.seed_signature
                if isinstance(trigger.seed_signature, dict)
                else None
            ),
        )
    except AlreadyAppliedError as e:
        await update_think_run(
            conn,
            record.id,
            status="skipped_idempotent",
            error=f"already applied: prior={e.context.get('prior_outcome')}",
        )
        emit("think.skipped_idempotent", run_id=str(record.id))
        return None, ThinkRunOutcome(
            run_id=record.id,
            trigger_id=record.trigger_id,
            trigger_kind=trigger_kind_full,
            status="skipped_idempotent",
            region_tenant_hash=region_tenant_hash,
            region_entity_hash=region_entity_hash,
            region_acquisition=acquisition,
            llm_latency_ms=llm_latency_ms,
        )
    await assert_tx_usable(conn, "apply_diff")

    from services.reasoning.relationships.adjudication import (
        adjudicate_candidates_for_trigger,
    )

    candidate_adjudications = await adjudicate_candidates_for_trigger(
        conn,
        trigger=trigger,
        diff=validated,
        applied=applied,
    )
    await assert_tx_usable(conn, "relationship_adjudication")
    if candidate_adjudications:
        applied["relationship_candidate_adjudication"] = {
            **_adjudication_payload(candidate_adjudications[0]),
        }
        if len(candidate_adjudications) > 1:
            applied["relationship_candidate_adjudications"] = [
                _adjudication_payload(adjudication)
                for adjudication in candidate_adjudications
            ]
    return applied, None


def _adjudication_payload(candidate_adjudication: Any) -> dict[str, Any]:
    return {
        "candidate_id": str(candidate_adjudication.candidate_id),
        "review_status": candidate_adjudication.review_status,
        "reason": candidate_adjudication.reason,
        "decision_reason": candidate_adjudication.decision_reason,
        "accepted_model_id": (
            str(candidate_adjudication.accepted_model_id)
            if candidate_adjudication.accepted_model_id
            else None
        ),
        "accepted_edge_ids": [
            str(edge_id) for edge_id in candidate_adjudication.accepted_edge_ids
        ],
        "metadata": candidate_adjudication.metadata,
    }


async def _record_apply_observability(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    reasoning_frame: Any,
    applied: dict[str, Any],
    validated_context_use: dict[str, Any],
) -> None:
    emit(
        "think.apply_done",
        run_id=str(record.id),
        ops_applied=(
            len(applied["claim_ops"])
            + len(applied.get("memory_lifecycle_ops", []))
            + len(applied.get("relation_claim_ops", []))
            + len(applied.get("relation_frame_ops", []))
            + len(applied["edge_ops"])
            + len(applied.get("ontology_gap_ops", []))
            + len(applied.get("open_question_ops", []))
            + len(applied["act_ops"])
            + len(applied["resource_ops"])
        ),
        state_changes=applied.get("state_changes_emitted", 0),
    )
    applied["context_use"] = validated_context_use
    applied["reasoning_frame"] = reasoning_frame.to_dict()
    try:
        from services.reasoning.edge_intelligence.context_feedback import (
            record_context_use_pair_feedback,
        )

        await record_context_use_pair_feedback(
            conn,
            tenant_id=trigger.tenant_id,
            trigger_ref=record.id,
            context_use=validated_context_use,
            primitive=_primitive_from_trigger(trigger),
        )
    except Exception as exc:  # noqa: BLE001
        _raise_if_postgres_error(exc)
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="apply",
        payload=applied,
    )
    for summary in applied.get("claim_ops", []):
        METRICS.inc_op(f"claim_{summary.get('op')}")
    for summary in applied.get("memory_lifecycle_ops", []):
        METRICS.inc_op(f"memory_lifecycle_{summary.get('action')}")
    for summary in applied.get("edge_ops", []):
        METRICS.inc_op(f"edge_{summary.get('op')}_{summary.get('edge_kind')}")
    for summary in applied.get("relation_claim_ops", []):
        METRICS.inc_op(f"relation_claim_{summary.get('op')}_{summary.get('edge_kind')}")
    for summary in applied.get("relation_frame_ops", []):
        METRICS.inc_op(
            f"relation_frame_{summary.get('op')}_{summary.get('relation_kind')}"
        )
    for summary in applied.get("ontology_gap_ops", []):
        METRICS.inc_op(
            f"ontology_gap_{summary.get('op')}_{summary.get('proposed_edge_kind')}"
        )
    for summary in applied.get("open_question_ops", []):
        METRICS.inc_op(
            f"open_question_{summary.get('op')}_{summary.get('question_type')}"
        )
    for summary in applied.get("act_ops", []):
        METRICS.inc_op(summary.get("op", "act_unknown"))
    for summary in applied.get("resource_ops", []):
        METRICS.inc_op(summary.get("op", "resource_unknown"))


def _primitive_from_trigger(trigger: TriggerContext) -> str | None:
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    for key in ("question_primitive", "primitive"):
        value = signature.get(key)
        if value:
            return str(value)
    primitives = signature.get("question_primitives")
    if isinstance(primitives, list) and primitives:
        return str(primitives[0])
    nested = signature.get("seed_signature")
    if isinstance(nested, dict):
        value = nested.get("question_primitive") or nested.get("primitive")
        if value:
            return str(value)
    return trigger.subkind or trigger.kind


_REPRESENTATION_REPAIR_WARNING_PRIORITY: dict[str, int] = {
    "prediction_lifecycle_not_exercised": 10,
    "truth_pressure_absent_for_contestable_memory": 20,
    "missing_curiosity_coverage": 30,
    "company_question_coverage_too_thin": 40,
    "missing_source_coverage": 50,
    "missing_discovered_pattern_coverage": 60,
    "selected_raw_evidence_too_low": 70,
    "selected_model_support_runaway": 80,
}

_REPRESENTATION_REPAIR_INTENTS: dict[str, str] = {
    "prediction_lifecycle_not_exercised": "exercise_prediction_lifecycle",
    "truth_pressure_absent_for_contestable_memory": "seek_counterevidence",
    "missing_curiosity_coverage": "cover_unresolved_unknowns",
    "company_question_coverage_too_thin": "expand_company_question_coverage",
    "missing_source_coverage": "attach_missing_source_evidence",
    "missing_discovered_pattern_coverage": "represent_discovered_patterns",
    "selected_raw_evidence_too_low": "revisit_raw_evidence_selection",
    "selected_model_support_runaway": "split_or_absorb_overloaded_memory",
}

_JUSTIFIED_NOOP_REPAIR_GRADES = {
    "justified_noop_context_used",
    "noop_trace_accounted",
}

_NOISE_NOOP_REPAIR_TRACE_MARKERS = (
    "discard_as_noise",
    "noise-only",
    "noise only",
    "noisy path",
)


def _representation_repair_payloads_from_audit(
    trigger: TriggerContext,
    audit: Any,
    *,
    max_payloads: int | None = None,
) -> list[dict[str, Any]]:
    if trigger.kind == "T4" and trigger.subkind == "representation_repair":
        return []
    if _audit_repairs_suppressed_for_justified_noop(trigger, audit):
        return []
    warnings = [
        warning
        for warning in (getattr(audit, "warnings", None) or [])
        if isinstance(warning, dict)
        and str(warning.get("code") or "") in _REPRESENTATION_REPAIR_WARNING_PRIORITY
    ]
    if not warnings:
        return []
    warnings.sort(
        key=lambda warning: _REPRESENTATION_REPAIR_WARNING_PRIORITY[
            str(warning.get("code") or "")
        ]
    )
    limit = (
        _representation_repair_max_triggers() if max_payloads is None else max_payloads
    )
    if limit <= 0:
        return []

    observation_ids = _trigger_observation_seed_ids(trigger)
    model_ids = _trigger_model_seed_ids(trigger)
    source_channels = list(getattr(audit, "source_channels", None) or [])
    payloads: list[dict[str, Any]] = []
    for warning in warnings[:limit]:
        code = str(warning.get("code") or "")
        intent = _REPRESENTATION_REPAIR_INTENTS[code]
        repair_key = f"{getattr(audit, 'trigger_id')}:{code}"
        payload: dict[str, Any] = {
            "repair_key": repair_key,
            "repair_intent": intent,
            "audit_warning_code": code,
            "audit_warning": warning,
            "source_trigger_id": str(getattr(audit, "trigger_id")),
            "source_run_id": str(getattr(audit, "run_id")),
            "source_trigger_kind": str(getattr(audit, "trigger_kind")),
            "seed_natural_text": _representation_repair_seed_text(
                intent=intent,
                warning=warning,
                source_channels=source_channels,
            ),
        }
        if trigger.seed_entity_ids:
            payload["seed_entity_ids"] = list(trigger.seed_entity_ids)
        if trigger.scope_actors:
            payload["scope_actors"] = [
                str(actor_id) for actor_id in trigger.scope_actors
            ]
        if observation_ids:
            payload["observation_ids"] = [
                str(observation_id) for observation_id in observation_ids
            ]
        if model_ids:
            payload["model_ids"] = [str(model_id) for model_id in model_ids]
        if trigger.region_spec:
            payload["region_spec"] = trigger.region_spec
        cascade_depth = _next_repair_cascade_depth(trigger)
        if cascade_depth > 0:
            payload["cascade_depth"] = cascade_depth
        payloads.append(payload)
    return payloads


def _audit_repairs_suppressed_for_justified_noop(
    trigger: TriggerContext,
    audit: Any,
) -> bool:
    """Do not spend T4 repair on a correct empty/noise outcome.

    The audit should still record representation pressure, but a successful
    no-op is not itself evidence that semantic maintenance should run.
    """
    if trigger.kind != "T1":
        return False
    model_adaptiveness = _safe_int(getattr(audit, "model_adaptiveness", 0))
    edge_adaptiveness = _safe_int(getattr(audit, "edge_adaptiveness", 0))
    if model_adaptiveness + edge_adaptiveness > 0:
        return False
    metrics = getattr(audit, "metrics", None)
    metrics = metrics if isinstance(metrics, dict) else {}
    if _safe_int(metrics.get("state_changes_emitted")) > 0:
        return False
    grade = str(metrics.get("context_use_grade") or "")
    if grade in _JUSTIFIED_NOOP_REPAIR_GRADES:
        return True
    trace = str(metrics.get("reasoning_trace") or "").lower()
    return any(marker in trace for marker in _NOISE_NOOP_REPAIR_TRACE_MARKERS)


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _representation_repair_seed_text(
    *,
    intent: str,
    warning: dict[str, Any],
    source_channels: list[str],
) -> str:
    message = str(warning.get("message") or warning.get("code") or "representation gap")
    channel_hint = ", ".join(source_channels[:4])
    parts = [
        f"Representation repair needed: {intent}.",
        message,
    ]
    if channel_hint:
        parts.append(f"Source channels: {channel_hint}.")
    return " ".join(parts)


def _trigger_observation_seed_ids(trigger: TriggerContext) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in [trigger.observation_id, *list(trigger.observation_ids or [])]:
        if value is None or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _trigger_model_seed_ids(trigger: TriggerContext) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in [trigger.model_id, *list(trigger.member_model_ids or [])]:
        if value is None or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _next_repair_cascade_depth(trigger: TriggerContext) -> int:
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    raw = signature.get("cascade_depth", 0)
    try:
        return max(0, int(raw)) + 1
    except (TypeError, ValueError):
        return 1


async def _enqueue_representation_repair_triggers(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    audit: Any,
) -> list[dict[str, Any]]:
    if not _representation_repair_triggers_enabled():
        return []
    payloads = _representation_repair_payloads_from_audit(trigger, audit)
    queued: list[dict[str, Any]] = []
    for payload in payloads:
        repair_key = str(payload["repair_key"])
        existing_id = await conn.fetchval(
            """
            SELECT id
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND trigger_kind = 'T4'
              AND trigger_subkind = 'representation_repair'
              AND completed_at IS NULL
              AND payload->>'repair_key' = $2
            LIMIT 1
            """,
            audit.tenant_id,
            repair_key,
        )
        if existing_id is not None:
            queued.append(
                {
                    "id": str(existing_id),
                    "repair_key": repair_key,
                    "audit_warning_code": payload["audit_warning_code"],
                    "deduped": True,
                }
            )
            continue
        trigger_id = await enqueue_trigger(
            conn,
            tenant_id=audit.tenant_id,
            trigger_kind="T4",
            trigger_subkind="representation_repair",
            observation_id=trigger.observation_id,
            model_id=trigger.model_id,
            payload=payload,
        )
        queued.append(
            {
                "id": str(trigger_id),
                "repair_key": repair_key,
                "audit_warning_code": payload["audit_warning_code"],
                "deduped": False,
            }
        )
    return queued


async def _publish_anomalies_and_enqueue_post_commit(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    validated: Any,
    applied: dict[str, Any],
) -> list[Any]:
    anomalies = await check_anomalies(validated, conn)
    await publish_anomalies(anomalies, record.id, trigger.tenant_id, conn)
    await assert_tx_usable(conn, "anomaly_publish")
    emit("think.anomalies_published", run_id=str(record.id), count=len(anomalies))
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
        trigger,
        validated,
        conn,
        anomalies=anomaly_dicts,
        applied_model_ids=applied.get("applied_model_ids") or [],
        applied_open_question_ids=[
            summary["open_question_id"]
            for summary in applied.get("open_question_ops", [])
            if isinstance(summary, dict)
            and summary.get("op") == "insert"
            and summary.get("open_question_id")
        ],
        applied_ops_summary=applied,
    )
    await assert_tx_usable(conn, "post_commit_enqueue")
    return anomalies


async def _record_representation_audit(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    validated: Any,
    bundle: Any,
    applied: dict[str, Any],
    validated_context_use: dict[str, Any] | None = None,
) -> None:
    if validated_context_use is not None and "context_use" not in applied:
        applied["context_use"] = validated_context_use
    audit = build_representation_audit(
        trigger=trigger,
        run_id=record.id,
        trigger_id=record.trigger_id,
        trigger_kind_full=trigger_kind_full,
        validated=validated,
        bundle=bundle,
        applied=applied,
    )
    applied["representation_audit"] = audit.to_dict()
    await persist_representation_audit(conn, audit)
    await assert_tx_usable(conn, "representation_audit")
    repair_triggers = await _enqueue_representation_repair_triggers(
        conn=conn,
        trigger=trigger,
        audit=audit,
    )
    if repair_triggers:
        applied["representation_repair_triggers"] = repair_triggers
        await assert_tx_usable(conn, "representation_repair_enqueue")
    emit(
        "think.representation_audit",
        run_id=str(record.id),
        status=audit.budget_status,
        warnings=len(audit.warnings),
        repair_triggers=len(repair_triggers),
        model_adaptiveness=audit.model_adaptiveness,
        edge_adaptiveness=audit.edge_adaptiveness,
        coverage_roles=audit.coverage_roles,
    )


async def _finalize_successful_run(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    validated: Any,
    applied: dict[str, Any],
    anomalies: list[Any],
    llm_latency_ms: int | None,
    region_tenant_hash: int,
    region_entity_hash: int,
    acquisition: RegionLockAcquisition,
    stage_timings: list[dict[str, Any]] | None = None,
) -> ThinkRunOutcome:
    started = time.perf_counter()
    casc_result = await run_cascade_for_validated_act_ops(
        conn=conn,
        trigger=trigger,
        validated=validated,
    )
    record_stage_timing(stage_timings, "cascade", started)
    await assert_tx_usable(conn, "cascade")
    cascade_depth = casc_result.depth_reached if casc_result else 0
    if casc_result is not None:
        METRICS.observe_cascade_depth(trigger_kind_full, cascade_depth)
    _attach_think_stage_timing_summary(applied, stage_timings)
    await update_think_run(
        conn,
        record.id,
        status="success",
        ops_applied=applied,
        cascade_depth=cascade_depth,
    )
    emit("think.committed", run_id=str(record.id), cascade_depth=cascade_depth)

    return ThinkRunOutcome(
        run_id=record.id,
        trigger_id=record.trigger_id,
        trigger_kind=trigger_kind_full,
        status="success",
        ops_applied_count=(
            len(applied["claim_ops"])
            + len(applied.get("memory_lifecycle_ops", []))
            + len(applied.get("relation_claim_ops", []))
            + len(applied.get("relation_frame_ops", []))
            + len(applied["edge_ops"])
            + len(applied.get("ontology_gap_ops", []))
            + len(applied.get("open_question_ops", []))
            + len(applied["act_ops"])
            + len(applied["resource_ops"])
        ),
        cascade_depth=cascade_depth,
        anomalies_flagged=len(anomalies),
        llm_latency_ms=llm_latency_ms,
        region_tenant_hash=region_tenant_hash,
        region_entity_hash=region_entity_hash,
        region_acquisition=acquisition,
        residual_context=_build_residual_context(
            trigger=trigger,
            record=record,
            trigger_kind_full=trigger_kind_full,
            validated=validated,
            applied=applied,
        ),
    )


def _build_residual_context(
    *,
    trigger: TriggerContext,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    validated: Any,
    applied: dict[str, Any],
) -> ThinkResidualContext:
    validation_errors = tuple(
        str(error)
        for error in (getattr(validated, "dropped_op_errors", None) or [])
        if str(error).strip()
    )
    apply_errors = tuple(
        str(error)
        for error in (applied.get("apply_dropped_op_errors") or [])
        if str(error).strip()
    )
    repair_payload = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    return ThinkResidualContext(
        tenant_id=trigger.tenant_id,
        think_run_id=record.id,
        trigger_id=record.trigger_id,
        trigger_kind=trigger_kind_full,
        trigger_subkind=trigger.subkind,
        source_observation_ids=tuple(_trigger_observation_seed_ids(trigger)),
        validation_dropped_op_count=int(getattr(validated, "dropped_op_count", 0) or 0),
        validation_dropped_op_errors=validation_errors,
        apply_dropped_op_count=int(applied.get("apply_dropped_op_count") or 0),
        apply_dropped_op_errors=apply_errors,
        reasoning_trace=str(
            applied.get("reasoning_trace")
            or getattr(validated, "reasoning_trace", None)
            or ""
        ),
        ops_applied_summary=applied,
        repair_source=_payload_text(repair_payload, "repair_source"),
        repair_key=_payload_text(repair_payload, "repair_key"),
        repair_residual_id=_payload_uuid(repair_payload, "residual_id"),
        repair_residual_kind=_payload_text(repair_payload, "residual_kind"),
        repair_intent=_payload_text(repair_payload, "repair_intent"),
        repair_cascade_depth=_payload_int(repair_payload, "cascade_depth"),
    )


def _payload_text(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _payload_uuid(payload: dict[str, Any], key: str) -> UUID | None:
    value = _payload_text(payload, key)
    if value is None:
        return None
    try:
        return UUID(value)
    except (TypeError, ValueError):
        return None


def _payload_int(payload: dict[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _trigger_fast_noop_region(trigger: TriggerContext) -> list[tuple[str, str]]:
    entities: set[tuple[str, str]] = set()
    for ent in trigger.seed_entity_ids or []:
        if not isinstance(ent, dict):
            continue
        ent_type = ent.get("type") or ent.get("kind")
        ent_id = ent.get("id")
        if ent_type is None or ent_id is None:
            continue
        entities.add((str(ent_type), str(ent_id)))
    for actor_id in trigger.scope_actors or []:
        entities.add(("actor", str(actor_id)))
    return sorted(entities)


def _build_noise_noop_fast_path(
    trigger: TriggerContext,
) -> tuple[ReasoningRunState, RawReasoningOutput] | None:
    if is_authoritative(trigger):
        return None
    raw_diff = build_noise_only_raw_diff(trigger)
    if raw_diff is None:
        return None

    retrieval_result = RetrievalResult(
        trigger=trigger,
        notes={
            "fast_path": "noise_noop",
            "retrieval_skipped": True,
        },
    )
    reasoning_frame = ReasoningFrame.from_trigger(
        trigger,
        retrieval_result=retrieval_result,
    )
    bundle = ContextBundle(
        notes={
            "fast_path": "noise_noop",
            "retrieval_skipped": True,
            "reason": "noise_only_t1",
        }
    )
    allowed_region = _trigger_fast_noop_region(trigger)

    from .context_use import summarize_context_use

    state = ReasoningRunState(
        context_plan=None,
        retrieval_result=retrieval_result,
        reasoning_frame=reasoning_frame,
        bundle=bundle,
        allowed_region=allowed_region,
        actor_operating_summary=None,
        region_tenant_hash=None,
        region_entity_hash=None,
        acquisition=None,
        mutation_row_inserted=False,
    )
    raw = RawReasoningOutput(
        raw_diff=raw_diff,
        raw_context_use=summarize_context_use(bundle, raw_diff),
        allowed_region=allowed_region,
        llm_latency_ms=0,
    )
    return state, raw


async def _prepare_attempt_reasoning_output(
    *,
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    llm_provider: LLMProvider | None,
    access_context: AccessContext | None,
    triggering_content: str | None,
    reason_for_trigger: str | None,
    record: ThinkRunRecord,
    trigger_kind_full: str,
    expanded_region: set[tuple[str, str]] | None,
    embedder: Any | None,
    read_pool: asyncpg.Pool | None,
    reason_cache: dict[str, Any] | None,
    stage_timings: list[dict[str, Any]] | None,
) -> tuple[ReasoningRunState, RawReasoningOutput]:
    started = time.perf_counter()
    fast_path = _build_noise_noop_fast_path(trigger)
    if fast_path is not None:
        record_stage_timing(stage_timings, "noise_noop_fast_path", started)
        state, raw = fast_path
        emit(
            "think.noise_noop_fast_path",
            run_id=str(record.id),
            trigger_ref=str(raw.raw_diff.trigger_ref),
            observation_count=len(trigger.observation_ids or [])
            + int(trigger.observation_id is not None),
        )
        return state, raw

    state = await prepare_reasoning_run_state(
        conn=conn,
        trigger=trigger,
        llm_provider=llm_provider,
        access_context=access_context,
        triggering_content=triggering_content,
        reason_for_trigger=reason_for_trigger,
        record=record,
        trigger_kind_full=trigger_kind_full,
        expanded_region=expanded_region,
        embedder=embedder,
        read_pool=read_pool,
        stage_timings=stage_timings,
    )
    raw = await build_raw_reasoning_output(
        conn=conn,
        trigger=trigger,
        llm_provider=llm_provider,
        triggering_content=triggering_content,
        reason_for_trigger=reason_for_trigger,
        record=record,
        state=state,
        reason_cache=reason_cache,
        stage_timings=stage_timings,
    )
    return state, raw


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
    read_pool: asyncpg.Pool | None = None,
    reason_cache: dict[str, Any] | None = None,
) -> ThinkRunOutcome:
    """Run one Think attempt, opening only the short mutation transaction here."""
    trigger_kind_full = record.trigger_kind
    stage_timings: list[dict[str, Any]] = []
    state, raw = await _prepare_attempt_reasoning_output(
        conn=conn,
        trigger=trigger,
        llm_provider=llm_provider,
        access_context=access_context,
        triggering_content=triggering_content,
        reason_for_trigger=reason_for_trigger,
        record=record,
        trigger_kind_full=trigger_kind_full,
        expanded_region=expanded_region,
        embedder=embedder,
        read_pool=read_pool,
        reason_cache=reason_cache,
        stage_timings=stage_timings,
    )

    async with _mutation_transaction(conn):
        th = state.region_tenant_hash
        eh = state.region_entity_hash
        acquisition = state.acquisition
        if not state.mutation_row_inserted:
            started = time.perf_counter()
            th, eh = region_lock_key(
                trigger.tenant_id, [(t, i) for (t, i) in raw.allowed_region]
            )
            await insert_think_run(
                conn,
                record,
                region_tenant_hash=th,
                region_entity_hash=eh,
            )
            await update_think_run(
                conn,
                record.id,
                retrieval_model_count=len(state.bundle.models),
                retrieval_observation_count=len(state.bundle.observations),
            )
            acquisition = await acquire_region_lock(
                conn,
                trigger.tenant_id,
                [(t, i) for (t, i) in raw.allowed_region],
            )
            record_stage_timing(
                stage_timings,
                "region_lock_and_run_record",
                started,
                allowed_region_size=len(raw.allowed_region),
                late_mutation_row=True,
            )
        assert th is not None
        assert eh is not None
        assert acquisition is not None

        started = time.perf_counter()
        if raw.llm_latency_ms is not None:
            await update_think_run(conn, record.id, llm_latency_ms=raw.llm_latency_ms)
        await debug_capture(
            conn,
            run_id=record.id,
            tenant_id=trigger.tenant_id,
            stage="response",
            payload={
                "llm_latency_ms": raw.llm_latency_ms,
                "is_authoritative": is_authoritative(trigger),
                "raw_diff": raw.raw_diff,
                "reasoning_frame": state.reasoning_frame.to_dict(),
                "actor_operating_context": state.actor_operating_summary,
                "context_use": raw.raw_context_use,
                "mutation_compile_summary": raw.mutation_compile_summary,
            },
        )
        record_stage_timing(stage_timings, "response_observability", started)

        validated, validated_context_use = await validate_raw_reasoning_output(
            conn=conn,
            trigger=trigger,
            record=record,
            trigger_kind_full=trigger_kind_full,
            retrieval_result=state.retrieval_result,
            bundle=state.bundle,
            raw=raw,
            reason_cache=reason_cache,
            stage_timings=stage_timings,
        )
        started = time.perf_counter()
        applied, skipped = await _apply_validated_diff(
            conn=conn,
            trigger=trigger,
            record=record,
            trigger_kind_full=trigger_kind_full,
            validated=validated,
            region_tenant_hash=th,
            region_entity_hash=eh,
            acquisition=acquisition,
            llm_latency_ms=raw.llm_latency_ms,
        )
        record_stage_timing(stage_timings, "apply_and_adjudication", started)
        if skipped is not None:
            return skipped
        assert applied is not None
        started = time.perf_counter()
        await _record_representation_audit(
            conn=conn,
            trigger=trigger,
            record=record,
            trigger_kind_full=trigger_kind_full,
            validated=validated,
            bundle=state.bundle,
            applied=applied,
            validated_context_use=validated_context_use,
        )
        record_stage_timing(stage_timings, "representation_audit", started)
        started = time.perf_counter()
        await _record_apply_observability(
            conn=conn,
            trigger=trigger,
            record=record,
            reasoning_frame=state.reasoning_frame,
            applied=applied,
            validated_context_use=validated_context_use,
        )
        record_stage_timing(stage_timings, "apply_observability", started)
        started = time.perf_counter()
        anomalies = await _publish_anomalies_and_enqueue_post_commit(
            conn=conn,
            trigger=trigger,
            record=record,
            validated=validated,
            applied=applied,
        )
        record_stage_timing(
            stage_timings,
            "post_commit_enqueue",
            started,
            anomaly_count=len(anomalies),
        )
        return await _finalize_successful_run(
            conn=conn,
            trigger=trigger,
            record=record,
            trigger_kind_full=trigger_kind_full,
            validated=validated,
            applied=applied,
            anomalies=anomalies,
            llm_latency_ms=raw.llm_latency_ms,
            region_tenant_hash=th,
            region_entity_hash=eh,
            acquisition=acquisition,
            stage_timings=stage_timings,
        )


__all__ = [
    "think",
    "ThinkRunOutcome",
]
