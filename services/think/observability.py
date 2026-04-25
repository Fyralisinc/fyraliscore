"""services/think/observability.py — structlog emitters + think_runs
writes + region_lock_log writes.

BUILD-PLAN §4 Prompt 3.B item 11.

Every Think run emits:
  * think.started
  * think.retrieval_done
  * think.validation_done
  * think.apply_done
  * think.committed
  * think.anomalies_published
  * think.completed     (happy path)
  * think.failed        (any raise)

Two DB writes besides the apply-owned `applied_triggers`:

  1. `think_runs` — ONE row per invocation, updated progressively
     inside the apply transaction so a rollback drops the row.
  2. `think_region_lock_log` — POST-COMMIT, best-effort, fire-and-forget.
     Failures are swallowed with a warning; the lock already did its job.

Prometheus-style metrics are collected in a module-level Metrics
singleton that tests can snapshot.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
import structlog


_log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------
# Metrics — simple in-memory prometheus-compatible counters/histograms
# ---------------------------------------------------------------------


@dataclass
class Metrics:
    runs_total: dict[str, int] = field(default_factory=dict)
    runs_failed: dict[str, int] = field(default_factory=dict)
    run_latency_ms: dict[str, list[float]] = field(default_factory=dict)
    ops_by_kind: dict[str, int] = field(default_factory=dict)
    queue_depth: dict[str, int] = field(default_factory=dict)   # per tenant
    cascade_depth_reached: dict[str, list[int]] = field(default_factory=dict)
    region_lock_waits_ms: list[float] = field(default_factory=list)
    # OP-4: dropped-op counters keyed by (reason, op_type).
    validation_dropped_ops: dict[tuple[str, str], int] = field(
        default_factory=dict
    )
    # OP-2: per-trigger-kind cost summaries (rolling in-memory).
    cost_usd_by_kind: dict[str, float] = field(default_factory=dict)
    llm_calls_by_kind: dict[str, int] = field(default_factory=dict)
    input_tokens_by_kind: dict[str, int] = field(default_factory=dict)
    output_tokens_by_kind: dict[str, int] = field(default_factory=dict)

    def inc_run(self, trigger_kind: str) -> None:
        self.runs_total[trigger_kind] = self.runs_total.get(trigger_kind, 0) + 1

    def inc_failed(self, trigger_kind: str) -> None:
        self.runs_failed[trigger_kind] = self.runs_failed.get(trigger_kind, 0) + 1

    def observe_latency(self, trigger_kind: str, ms: float) -> None:
        self.run_latency_ms.setdefault(trigger_kind, []).append(ms)

    def inc_op(self, op_kind: str, n: int = 1) -> None:
        self.ops_by_kind[op_kind] = self.ops_by_kind.get(op_kind, 0) + n

    def observe_cascade_depth(self, trigger_kind: str, depth: int) -> None:
        self.cascade_depth_reached.setdefault(trigger_kind, []).append(depth)

    def observe_region_lock_wait(self, ms: float) -> None:
        self.region_lock_waits_ms.append(ms)

    def set_queue_depth(self, tenant_id: UUID | str, depth: int) -> None:
        self.queue_depth[str(tenant_id)] = depth

    # --- OP-4 --------------------------------------------------------
    def inc_dropped_op(self, reason: str, op_type: str, n: int = 1) -> None:
        """`think.validation.dropped_ops{reason, op_type}` counter."""
        key = (reason, op_type)
        self.validation_dropped_ops[key] = (
            self.validation_dropped_ops.get(key, 0) + n
        )

    # --- OP-2 --------------------------------------------------------
    def record_cost(
        self,
        trigger_kind: str,
        *,
        cost_usd: float,
        input_tokens: int,
        output_tokens: int,
        llm_calls: int,
    ) -> None:
        self.cost_usd_by_kind[trigger_kind] = (
            self.cost_usd_by_kind.get(trigger_kind, 0.0) + cost_usd
        )
        self.llm_calls_by_kind[trigger_kind] = (
            self.llm_calls_by_kind.get(trigger_kind, 0) + llm_calls
        )
        self.input_tokens_by_kind[trigger_kind] = (
            self.input_tokens_by_kind.get(trigger_kind, 0) + input_tokens
        )
        self.output_tokens_by_kind[trigger_kind] = (
            self.output_tokens_by_kind.get(trigger_kind, 0) + output_tokens
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "runs_total": dict(self.runs_total),
            "runs_failed": dict(self.runs_failed),
            "run_latency_ms": {
                k: list(v) for k, v in self.run_latency_ms.items()
            },
            "ops_by_kind": dict(self.ops_by_kind),
            "queue_depth": dict(self.queue_depth),
            "cascade_depth_reached": {
                k: list(v) for k, v in self.cascade_depth_reached.items()
            },
            "region_lock_waits_ms": list(self.region_lock_waits_ms),
            "validation_dropped_ops": {
                f"{r}|{t}": n
                for (r, t), n in self.validation_dropped_ops.items()
            },
            "cost_usd_by_kind": dict(self.cost_usd_by_kind),
            "llm_calls_by_kind": dict(self.llm_calls_by_kind),
            "input_tokens_by_kind": dict(self.input_tokens_by_kind),
            "output_tokens_by_kind": dict(self.output_tokens_by_kind),
        }

    def reset(self) -> None:
        self.runs_total.clear()
        self.runs_failed.clear()
        self.run_latency_ms.clear()
        self.ops_by_kind.clear()
        self.queue_depth.clear()
        self.cascade_depth_reached.clear()
        self.region_lock_waits_ms.clear()
        self.validation_dropped_ops.clear()
        self.cost_usd_by_kind.clear()
        self.llm_calls_by_kind.clear()
        self.input_tokens_by_kind.clear()
        self.output_tokens_by_kind.clear()


METRICS = Metrics()


# ---------------------------------------------------------------------
# Run record — helpers to INSERT / UPDATE think_runs inside the tx
# ---------------------------------------------------------------------


@dataclass
class ThinkRunRecord:
    id: UUID
    tenant_id: UUID
    trigger_id: UUID
    trigger_kind: str
    started_at: float = field(default_factory=time.monotonic)

    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.started_at) * 1000.0


async def insert_think_run(
    conn: asyncpg.Connection,
    record: ThinkRunRecord,
    *,
    region_tenant_hash: int | None = None,
    region_entity_hash: int | None = None,
) -> None:
    """
    INSERT a think_runs row at the start of a Think run. Runs in the
    caller's apply transaction so a rollback drops it. Status starts
    as 'running'.
    """
    await conn.execute(
        """
        INSERT INTO think_runs
          (id, tenant_id, trigger_id, trigger_kind,
           started_at, status,
           region_tenant_hash, region_entity_hash)
        VALUES ($1, $2, $3, $4, now(), 'running', $5, $6)
        """,
        record.id,
        record.tenant_id,
        record.trigger_id,
        record.trigger_kind,
        region_tenant_hash,
        region_entity_hash,
    )


async def update_think_run(
    conn: asyncpg.Connection,
    run_id: UUID,
    *,
    status: str | None = None,
    error: str | None = None,
    retrieval_model_count: int | None = None,
    retrieval_observation_count: int | None = None,
    llm_latency_ms: int | None = None,
    validation_error_count: int | None = None,
    ops_applied: dict | None = None,
    cascade_depth: int | None = None,
) -> None:
    """
    Progressive UPDATE on a think_runs row. Every call patches a subset
    of columns. Runs in the caller's apply transaction.
    """
    set_clauses: list[str] = []
    params: list[Any] = []
    i = 1
    if status is not None:
        set_clauses.append(f"status = ${i}")
        params.append(status)
        i += 1
        if status in ("success", "failed", "skipped_idempotent"):
            set_clauses.append("ended_at = now()")
    if error is not None:
        set_clauses.append(f"error = ${i}")
        params.append(error)
        i += 1
    if retrieval_model_count is not None:
        set_clauses.append(f"retrieval_model_count = ${i}")
        params.append(retrieval_model_count)
        i += 1
    if retrieval_observation_count is not None:
        set_clauses.append(f"retrieval_observation_count = ${i}")
        params.append(retrieval_observation_count)
        i += 1
    if llm_latency_ms is not None:
        set_clauses.append(f"llm_latency_ms = ${i}")
        params.append(llm_latency_ms)
        i += 1
    if validation_error_count is not None:
        set_clauses.append(f"validation_error_count = ${i}")
        params.append(validation_error_count)
        i += 1
    if ops_applied is not None:
        set_clauses.append(f"ops_applied = ${i}::jsonb")
        params.append(json.dumps(ops_applied, default=str))
        i += 1
    if cascade_depth is not None:
        set_clauses.append(f"cascade_depth = ${i}")
        params.append(cascade_depth)
        i += 1

    if not set_clauses:
        return
    params.append(run_id)
    await conn.execute(
        f"UPDATE think_runs SET {', '.join(set_clauses)} WHERE id = ${i}",
        *params,
    )


# ---------------------------------------------------------------------
# Region lock log — post-commit, best-effort.
# ---------------------------------------------------------------------


async def write_region_lock_log(
    pool: asyncpg.Pool | None,
    *,
    tenant_id: UUID,
    think_run_id: UUID,
    tenant_hash: int,
    entity_hash: int,
    entity_ids: list[tuple[str, str]],
    acquired_at: float,
    released_at: float,
    wait_duration_ms: int,
    hold_duration_ms: int,
) -> None:
    """
    Fire-and-forget write to think_region_lock_log. Errors are
    swallowed with a warning — the lock already did its job.

    The `acquired_at` / `released_at` arguments are time.monotonic()
    values. We convert to a timestamp by computing `now() - elapsed`.
    For simplicity we use `now()` as a proxy and store the monotonic
    elapsed values in the duration columns.
    """
    if pool is None:
        return
    from datetime import datetime, timezone, timedelta
    from lib.shared.ids import uuid7

    now = datetime.now(timezone.utc)
    hold = timedelta(milliseconds=hold_duration_ms)
    acquired_ts = now - hold
    released_ts = now

    try:
        async with pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO think_region_lock_log
                  (id, tenant_id, think_run_id, tenant_hash, entity_hash,
                   entity_ids, acquired_at, released_at,
                   wait_duration_ms, hold_duration_ms)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)
                """,
                uuid7(),
                tenant_id,
                think_run_id,
                tenant_hash,
                entity_hash,
                json.dumps(entity_ids),
                acquired_ts,
                released_ts,
                int(wait_duration_ms),
                int(hold_duration_ms),
            )
    except Exception as e:
        _log.warning("think.region_lock_log_write_failed", error=str(e))


# ---------------------------------------------------------------------
# Structured emitters
# ---------------------------------------------------------------------


def emit(event: str, **fields: Any) -> None:
    """Helper for all `think.*` log events."""
    _log.info(event, **fields)


# ---------------------------------------------------------------------
# OP-2 — Per-trigger cost recording (think_run_costs table).
# ---------------------------------------------------------------------


async def record_think_run_cost(
    pool: asyncpg.Pool | None,
    *,
    trigger_id: UUID,
    tenant_id: UUID,
    trigger_kind: str,
    outcome: str,
    llm_calls_count: int,
    llm_input_tokens_total: int,
    llm_output_tokens_total: int,
    llm_cost_usd: float,
    latency_total_ms: int,
    retry_count: int = 0,
    model_name: str | None = None,
) -> None:
    """Insert a row into `think_run_costs`. Runs post-commit, best-effort.

    THINK-DESIGN-AUDIT §9.3 — persistent cost attribution per Think run.
    Row includes outcome so a failed-but-expensive trigger is visible
    in per-outcome cost queries.

    `outcome` MUST be one of `{success, validation_failure,
    reasoning_exhausted, dead_letter, skipped_idempotent, failed}` per
    the migration's CHECK constraint. The caller passes the outcome
    mapped from its pipeline state; anything not in the set is coerced
    to `'failed'` with a structured warning so an unknown bucket never
    kills the cost record.
    """
    if pool is None:
        return

    allowed = {
        "success", "validation_failure", "reasoning_exhausted",
        "dead_letter", "skipped_idempotent", "failed",
    }
    if outcome not in allowed:
        _log.warning(
            "think.cost_record.unknown_outcome",
            outcome=outcome,
            trigger_id=str(trigger_id),
        )
        outcome = "failed"

    # Also update the in-process counter. Callers that want synchronous
    # cost totals (tests, dashboards) can read METRICS.
    METRICS.record_cost(
        trigger_kind,
        cost_usd=float(llm_cost_usd),
        input_tokens=int(llm_input_tokens_total),
        output_tokens=int(llm_output_tokens_total),
        llm_calls=int(llm_calls_count),
    )

    try:
        async with pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO think_run_costs
                  (trigger_id, tenant_id, trigger_kind,
                   llm_calls_count, llm_input_tokens_total,
                   llm_output_tokens_total, llm_cost_usd,
                   latency_total_ms, retry_count, outcome, model_name)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (trigger_id, computed_at) DO NOTHING
                """,
                trigger_id,
                tenant_id,
                trigger_kind,
                int(llm_calls_count),
                int(llm_input_tokens_total),
                int(llm_output_tokens_total),
                float(llm_cost_usd),
                int(latency_total_ms),
                int(retry_count),
                outcome,
                model_name,
            )
    except Exception as e:
        # Cost recording is advisory — never crash the run for it.
        _log.warning(
            "think.cost_record.insert_failed",
            trigger_id=str(trigger_id),
            error=str(e),
        )


# ---------------------------------------------------------------------
# OP-4 — Dropped-op classification logging + metrics helper.
# ---------------------------------------------------------------------


def log_dropped_op(
    *,
    trigger_id: UUID | str | None,
    tenant_id: UUID | str | None,
    op_kind: str,
    op_type: str,
    failure_reason: str,
    original_op: Any,
) -> None:
    """Emit a structured `validation_op_dropped` log + bump the
    `think.validation.dropped_ops{reason, op_type}` counter.

    `op_type` is the surface the op came from ('claim' / 'act' /
    'resource'). `failure_reason` is a short classification tag (e.g.
    'inadequate_falsifier', 'confidence_below_threshold',
    'invalid_entity_reference', 'illegal_transition').
    """
    from datetime import datetime, timezone

    serialized: Any = original_op
    # Best-effort serialisation — Pydantic models expose
    # `.model_dump(mode='json')`; fall back to str() for anything else.
    if hasattr(original_op, "model_dump"):
        try:
            serialized = original_op.model_dump(mode="json")
        except Exception:
            serialized = str(original_op)
    elif hasattr(original_op, "__dict__"):
        try:
            serialized = {
                k: v for k, v in vars(original_op).items()
                if not k.startswith("_")
            }
        except Exception:
            serialized = str(original_op)

    _log.info(
        "validation_op_dropped",
        trigger_id=str(trigger_id) if trigger_id is not None else None,
        tenant_id=str(tenant_id) if tenant_id is not None else None,
        op_kind=op_kind,
        op_type=op_type,
        failure_reason=failure_reason,
        original_op=serialized,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
    METRICS.inc_dropped_op(failure_reason, op_type)


# ---------------------------------------------------------------------
# OP-5 — simple aggregation helper for the dashboard.
# ---------------------------------------------------------------------


async def aggregate_costs_for_tenant(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    window_hours: int = 24,
) -> dict[str, Any]:
    """Sum costs per trigger_kind over a window. Used by the OP-5
    dashboard."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT trigger_kind,
                   count(*) AS runs,
                   sum(llm_calls_count) AS calls,
                   sum(llm_input_tokens_total) AS input_tokens,
                   sum(llm_output_tokens_total) AS output_tokens,
                   sum(llm_cost_usd) AS total_cost_usd,
                   avg(latency_total_ms) AS avg_latency_ms
            FROM think_run_costs
            WHERE tenant_id = $1
              AND computed_at > now() - ($2 || ' hours')::interval
            GROUP BY trigger_kind
            ORDER BY trigger_kind
            """,
            tenant_id, str(window_hours),
        )
    return {
        "tenant_id": str(tenant_id),
        "window_hours": window_hours,
        "rows": [
            {
                "trigger_kind": r["trigger_kind"],
                "runs": int(r["runs"]),
                "calls": int(r["calls"] or 0),
                "input_tokens": int(r["input_tokens"] or 0),
                "output_tokens": int(r["output_tokens"] or 0),
                "total_cost_usd": float(r["total_cost_usd"] or 0.0),
                "avg_latency_ms": float(r["avg_latency_ms"] or 0.0),
            }
            for r in rows
        ],
    }


__all__ = [
    "METRICS",
    "Metrics",
    "ThinkRunRecord",
    "insert_think_run",
    "update_think_run",
    "write_region_lock_log",
    "emit",
    # OP-2
    "record_think_run_cost",
    "aggregate_costs_for_tenant",
    # OP-4
    "log_dropped_op",
]
