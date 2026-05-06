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

import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.llm.provider import LLMProvider, LLMUsageAggregator
from lib.shared.errors import (
    CompanyOSError,
    ValidationError,
)
from lib.shared.ids import uuid7

from services.retrieval.assembler import (
    AccessContext,
    assemble_context,
)
from services.retrieval.primary import (
    TriggerContext,
    primary_retrieve,
)

from .anomaly_integration import (
    check_anomalies,
    publish_anomalies,
)
from .applier import AlreadyAppliedError, apply_diff
from .cascade import CascadeEvent, CascadeResult, cascade
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
    touched_entity_ids,
)
from .validator import (
    OutOfRegionError,
    validate,
)


_log = structlog.get_logger(__name__)


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
    Single-shot Think invocation. Opens its own transaction on `pool`,
    acquires the region lock, runs the full pipeline, commits.

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
    expanded_region: set[tuple[str, str]] | None = None

    # OP-2: install a usage aggregator on the provider for this run.
    # Aggregator is cleared after the run (finally block). Any LLM call
    # made via `provider.structured` records tokens + cost into it.
    usage_agg: LLMUsageAggregator | None = None
    if llm_provider is not None:
        usage_agg = LLMUsageAggregator()
        llm_provider.set_usage_aggregator(usage_agg)

    try:
        while True:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    outcome = await _run_once(
                        conn=conn,
                        trigger=trigger,
                        pool=pool,
                        llm_provider=llm_provider,
                        embedder=embedder,
                        access_context=access_context,
                        triggering_content=triggering_content,
                        reason_for_trigger=reason_for_trigger,
                        record=record,
                        expanded_region=expanded_region,
                    )
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
    except OutOfRegionError as e:
        # Re-run retrieval with an expanded region, up to max_retrieval_reruns.
        rerun_count += 1
        if rerun_count > max_retrieval_reruns:
            METRICS.inc_failed(trigger_kind_full)
            emit("think.failed",
                 run_id=str(run_id),
                 error="out_of_region_exhausted",
                 rerun_count=rerun_count)
            out = ThinkRunOutcome(
                run_id=run_id,
                trigger_id=trigger_id,
                trigger_kind=trigger_kind_full,
                status="failed",
                error=f"out_of_region_after_{rerun_count}_reruns: {e.message}",
                exception=e,
                elapsed_ms=(time.monotonic() - started_at) * 1000.0,
            )
            _snapshot_usage(out, usage_agg, llm_provider)
            await _record_cost_for_outcome(pool, out, trigger.tenant_id)
            return out
        emit("think.out_of_region",
             run_id=str(run_id),
             attempt=rerun_count,
             missing=e.context.get("missing"))
        # Expand the allowed region by adding the missing entities.
        prev = expanded_region or set()
        missing = e.context.get("missing") or []
        prev.update((t, i) for (t, i) in missing)
        expanded_region = prev
        # Fall through: the outer while loop will retry.
        try:
            return await think(
                trigger, pool,
                llm_provider=llm_provider,
                access_context=access_context,
                triggering_content=triggering_content,
                reason_for_trigger=reason_for_trigger,
                trigger_kind_subkind=trigger_kind_subkind,
                max_retrieval_reruns=max_retrieval_reruns - 1,
            )
        except Exception as inner:
            out = _fail_outcome(
                run_id, trigger_id, trigger_kind_full, inner, started_at
            )
            _snapshot_usage(out, usage_agg, llm_provider)
            await _record_cost_for_outcome(pool, out, trigger.tenant_id)
            return out
    except CompanyOSError as e:
        out = _fail_outcome(
            run_id, trigger_id, trigger_kind_full, e, started_at
        )
        _snapshot_usage(out, usage_agg, llm_provider)
        await _record_cost_for_outcome(pool, out, trigger.tenant_id)
        return out
    except Exception as e:
        out = _fail_outcome(
            run_id, trigger_id, trigger_kind_full, e, started_at
        )
        _snapshot_usage(out, usage_agg, llm_provider)
        await _record_cost_for_outcome(pool, out, trigger.tenant_id)
        return out
    finally:
        # Always detach the aggregator so it doesn't leak across runs.
        if llm_provider is not None:
            llm_provider.set_usage_aggregator(None)


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
    pool: asyncpg.Pool,
    llm_provider: LLMProvider | None,
    access_context: AccessContext | None,
    triggering_content: str | None,
    reason_for_trigger: str | None,
    record: ThinkRunRecord,
    expanded_region: set[tuple[str, str]] | None,
    embedder: Any | None = None,
) -> ThinkRunOutcome:
    """
    Full pipeline inside one open transaction. Called by `think()`.

    Does NOT write to think_region_lock_log (that's post-commit in the
    outer caller). DOES write think_runs inside the tx.
    """
    trigger_kind_full = record.trigger_kind

    # --- 1. Retrieval ---------------------------------------------
    t0 = time.monotonic()
    first = await primary_retrieve(trigger, conn, embedder=embedder)
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

    # --- 2. Compute region BEFORE the LLM -------------------------
    allowed_region = touched_entity_ids(first)
    if expanded_region:
        merged = set(allowed_region) | set(expanded_region)
        allowed_region = sorted(merged)

    th, eh = region_lock_key(trigger.tenant_id, [
        (t, i) for (t, i) in allowed_region
    ])

    # --- 3. Insert the think_runs row ----------------------------
    await insert_think_run(
        conn, record,
        region_tenant_hash=th,
        region_entity_hash=eh,
    )
    await update_think_run(
        conn, record.id,
        retrieval_model_count=len(first.models),
        retrieval_observation_count=len(first.observations),
    )

    # --- 4. Acquire region lock -----------------------------------
    acquisition = await acquire_region_lock(
        conn, trigger.tenant_id, [(t, i) for (t, i) in allowed_region]
    )

    # --- 5. Assemble context --------------------------------------
    access = access_context or AccessContext(tenant_id=trigger.tenant_id)
    bundle = await assemble_context(first, access, conn)

    import structlog as _diag_log
    _diag_log.get_logger("think.diag").warning(
        "augmentation.entry",
        run_id=str(record.id),
        bundle_commitments=len(bundle.acts_summary.get("commitments", [])),
    )

    # Demo augmentation: the retrieval pathways only surface commitments
    # connected to retrieved Models — and Pathway A frequently fails
    # entirely due to strict CommitmentRow state validation when the
    # snapshot includes states outside the canonical literal (e.g.
    # 'at_risk'). For the demo we want the LLM to see the full active
    # ledger regardless, so we pull active commitments directly and
    # attach lightweight stubs that expose only the fields the prompt
    # renderer reads (id, state, owner_id, due_date, title). Bypassing
    # CommitmentRow validation keeps the augmentation tolerant of
    # snapshot drift.
    try:
        from types import SimpleNamespace

        existing_ids = {
            getattr(c, "id", None)
            for c in bundle.acts_summary.get("commitments", [])
        }
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, title, state, owner_id, due_date,
                   last_state_change_at, created_at
            FROM commitments
            WHERE tenant_id = $1
              AND terminal_at IS NULL
              AND state != 'closed'
            ORDER BY last_state_change_at DESC NULLS LAST,
                     created_at DESC
            LIMIT 25
            """,
            trigger.tenant_id,
        )
        for r in rows:
            if r["id"] in existing_ids:
                continue
            stub = SimpleNamespace(
                id=r["id"],
                tenant_id=r["tenant_id"],
                title=r["title"],
                state=r["state"],
                owner_id=r["owner_id"],
                due_date=r["due_date"],
                last_state_change_at=r["last_state_change_at"],
                created_at=r["created_at"],
            )
            bundle.acts_summary.setdefault(
                "commitments", []
            ).append(stub)
            existing_ids.add(r["id"])
            # Extend the region allow-list so the validator does not
            # reject act_ops the LLM emits against the augmented
            # commitments. Without this every transition_commitment on
            # a freshly-augmented entity raises out_of_region and the
            # worker exhausts its retry budget.
            allowed_region = sorted(
                set(allowed_region) | {("commitment", str(r["id"]))}
            )
    except Exception as _aug_err:  # noqa: BLE001
        await debug_capture(
            conn,
            run_id=record.id,
            tenant_id=trigger.tenant_id,
            stage="error",
            payload={"phase": "acts_augmentation", "error": repr(_aug_err)},
        )

    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="retrieval",
        payload={
            "phase": "post_augmentation",
            "commitment_count": len(bundle.acts_summary.get("commitments", [])),
            "commitment_titles": [
                getattr(c, "title", None)
                for c in bundle.acts_summary.get("commitments", [])
            ][:80],
        },
    )

    # --- 6. Reason ------------------------------------------------
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
            reason_for_trigger=reason_for_trigger,
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
    )

    raw_diff = maybe_inject_create_commitment(raw_diff, trigger, bundle)
    raw_diff = maybe_inject_block_transition(raw_diff, trigger, bundle)
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

    if llm_latency_ms is not None:
        await update_think_run(conn, record.id, llm_latency_ms=llm_latency_ms)
    await debug_capture(
        conn,
        run_id=record.id,
        tenant_id=trigger.tenant_id,
        stage="response",
        payload={
            "llm_latency_ms": llm_latency_ms,
            "is_authoritative": is_authoritative(trigger),
            "raw_diff": raw_diff,
        },
    )

    # --- 7. Validate ---------------------------------------------
    validated = await validate(
        raw_diff, first, conn,
        allowed_region=allowed_region,
        strict_region=True,
    )
    emit("think.validation_done",
         run_id=str(record.id),
         claim_ops=len(validated.claim_ops),
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
            "act_ops": validated.act_ops,
            "resource_ops": validated.resource_ops,
            "dropped_op_count": validated.dropped_op_count,
            "dropped_op_errors": list(validated.dropped_op_errors[:20]),
        },
    )

    # --- 8. Apply ------------------------------------------------
    try:
        applied = await apply_diff(
            validated, conn,
            trigger_kind=trigger_kind_full,
            trigger_cause_event_id=trigger.observation_id,
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

    emit("think.apply_done",
         run_id=str(record.id),
         ops_applied=len(applied["claim_ops"]) + len(applied["act_ops"]) + len(applied["resource_ops"]),
         state_changes=applied.get("state_changes_emitted", 0))
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
    for summary in applied.get("act_ops", []):
        METRICS.inc_op(summary.get("op", "act_unknown"))
    for summary in applied.get("resource_ops", []):
        METRICS.inc_op(summary.get("op", "resource_unknown"))

    # --- 9. Anomalies ---------------------------------------------
    anomalies = await check_anomalies(validated, conn)
    await publish_anomalies(anomalies, record.id, trigger.tenant_id, conn)
    emit("think.anomalies_published",
         run_id=str(record.id), count=len(anomalies))

    # --- 9b. Post-commit durability queue (OP-1) ------------------
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

    # --- 10. Cascade ---------------------------------------------
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
                      AND entities_mentioned @> $1::jsonb
                    ORDER BY occurred_at DESC
                    LIMIT 1
                    """,
                    _entities_filter("commitment", cid),
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
                      AND entities_mentioned @> $1::jsonb
                    ORDER BY occurred_at DESC
                    LIMIT 1
                    """,
                    _entities_filter("decision", did),
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
            len(applied["claim_ops"]) + len(applied["act_ops"]) + len(applied["resource_ops"])
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
