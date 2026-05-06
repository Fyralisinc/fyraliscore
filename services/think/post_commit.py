"""services/think/post_commit.py — durable post-commit action queue.

OP-1 (THINK-DESIGN-AUDIT §8.1, §10 arg 1). Post-commit side effects
(publish_anomalies / schedule_predictions / broadcast_realtime /
invalidate_metrics) used to run INLINE after the apply transaction
committed. If the worker crashed between commit and post-commit, the
side effects were silently lost — subsequent retries of the trigger
short-circuited on the `applied_triggers` idempotency ledger without
re-running the side effects.

Fix:

  1. `enqueue_post_commit_actions(trigger, validated_diff, conn)` runs
     INSIDE the apply transaction. It writes one row per action-kind
     into `pending_post_commit_actions`. A crash before commit rolls
     the rows back with the apply; a crash after commit leaves the
     rows durable for the worker to pick up.

  2. `post_commit_worker()` polls the queue with FOR UPDATE SKIP LOCKED
     (matching the existing think_trigger_queue dispatcher), dispatches
     each action to its handler, and on failure bumps `attempts` and
     `scheduled_at` with exponential backoff. After 5 failed attempts
     the row is moved to dead-letter state (`dead_lettered_at` set).

Dedup: the `post_commit_dedup UNIQUE NULLS NOT DISTINCT` constraint in
migration 0015 collapses two enqueues for the same (tenant, trigger,
action_kind) where both still have processed_at=NULL. That means the
same trigger re-processed after idempotency short-circuit doesn't
double-fire post-commit. A new pending row after the previous one was
processed is allowed (NULL vs non-NULL don't collide).
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg
import structlog

from services.retrieval.primary import TriggerContext

from .diff_schema import ValidatedDiff


_log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

MAX_ATTEMPTS = 5
# Exponential backoff base (seconds). Actual backoff = BASE * 2^(attempts-1),
# capped at 300s (5 min) — mirrors the audit pseudocode.
BACKOFF_BASE_SECONDS = 2
BACKOFF_CAP_SECONDS = 300

POLL_INTERVAL_SECONDS = 2.0
BATCH_SIZE = 10

ACTION_KINDS = (
    "publish_anomalies",
    "schedule_predictions",
    "broadcast_realtime",
    "invalidate_metrics",
)


# ---------------------------------------------------------------------
# Dispatch registry — post-commit worker looks up the handler per kind.
# ---------------------------------------------------------------------

ActionHandler = Callable[[dict[str, Any], UUID, UUID], Awaitable[None]]
"""Signature: (payload, tenant_id, trigger_id) -> awaitable None.

Handlers MUST be idempotent — the queue guarantees at-least-once
dispatch, not exactly-once. A crash mid-dispatch leaves the action
available for retry; if the handler partially completed, its second
run should be a no-op for the already-done side effects.
"""


# Default handlers are no-ops (publish_anomalies is already committed
# to `think_anomalies_raw` in `anomaly_integration.py`; Wave 4-B's
# anomaly_processor consumes from there). Real dispatch wiring is left
# for the Wave 4-B integration PR. We keep the registry so the worker
# can be driven end-to-end in tests.

async def _default_publish_anomalies(
    payload: dict[str, Any], tenant_id: UUID, trigger_id: UUID,
) -> None:
    _log.info(
        "post_commit.publish_anomalies.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        anomaly_count=len(payload.get("anomalies", [])),
    )


async def _default_schedule_predictions(
    payload: dict[str, Any], tenant_id: UUID, trigger_id: UUID,
) -> None:
    _log.info(
        "post_commit.schedule_predictions.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        prediction_count=len(payload.get("predictions", [])),
    )


async def _default_broadcast_realtime(
    payload: dict[str, Any], tenant_id: UUID, trigger_id: UUID,
) -> None:
    _log.info(
        "post_commit.broadcast_realtime.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
    )


async def _default_invalidate_metrics(
    payload: dict[str, Any], tenant_id: UUID, trigger_id: UUID,
) -> None:
    _log.info(
        "post_commit.invalidate_metrics.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        affected_count=len(payload.get("affected_entities", [])),
    )


_DISPATCHERS: dict[str, ActionHandler] = {
    "publish_anomalies": _default_publish_anomalies,
    "schedule_predictions": _default_schedule_predictions,
    "broadcast_realtime": _default_broadcast_realtime,
    "invalidate_metrics": _default_invalidate_metrics,
}


def register_handler(action_kind: str, handler: ActionHandler) -> None:
    """Install a custom handler for an action kind. Primarily used in
    tests to inject a deterministic / counted / failing handler."""
    if action_kind not in ACTION_KINDS:
        raise ValueError(f"unknown action_kind: {action_kind!r}")
    _DISPATCHERS[action_kind] = handler


def get_handler(action_kind: str) -> ActionHandler:
    return _DISPATCHERS[action_kind]


def reset_handlers() -> None:
    """Restore the module-default handlers (used by tests for teardown)."""
    _DISPATCHERS["publish_anomalies"] = _default_publish_anomalies
    _DISPATCHERS["schedule_predictions"] = _default_schedule_predictions
    _DISPATCHERS["broadcast_realtime"] = _default_broadcast_realtime
    _DISPATCHERS["invalidate_metrics"] = _default_invalidate_metrics


# ---------------------------------------------------------------------
# Payload builders — one per action kind.
# ---------------------------------------------------------------------


def _summarize_op_count(diff: ValidatedDiff) -> dict[str, int]:
    return {
        "claim_ops": len(diff.claim_ops),
        "act_ops": len(diff.act_ops),
        "resource_ops": len(diff.resource_ops),
    }


def _affected_entities(diff: ValidatedDiff) -> list[dict[str, str]]:
    """List of entities whose cached metrics may now be stale.

    Walks every validated op to find the (type, id) of every entity
    touched. Dedup by tuple.
    """
    seen: set[tuple[str, str]] = set()

    def _add(t: str, i: Any) -> None:
        if i is None:
            return
        seen.add((str(t), str(i)))

    for op in diff.claim_ops:
        if op.model_id is not None:
            _add("model", op.model_id)
        if op.op == "insert" and op.entry:
            for e in op.entry.get("scope_entities", []) or []:
                if isinstance(e, dict):
                    _add(e.get("type"), e.get("id"))
    for op in diff.act_ops:
        ent = op.entity or {}
        eid = ent.get("id")
        if op.op.startswith("create_commitment") or op.op == "transition_commitment":
            _add("commitment", eid)
        elif op.op.startswith("create_goal") or op.op in ("update_goal", "transition_goal"):
            _add("goal", eid)
        elif op.op.startswith("create_decision") or op.op == "transition_decision":
            _add("decision", eid)
    for op in diff.resource_ops:
        if op.resource_id is not None:
            _add("resource", op.resource_id)

    return [{"type": t, "id": i} for (t, i) in sorted(seen)]


def _summarize_diff(diff: ValidatedDiff) -> dict[str, Any]:
    return {
        "tenant_id": str(diff.tenant_id),
        "trigger_ref": str(diff.trigger_ref),
        "op_counts": _summarize_op_count(diff),
        "affected_entities": _affected_entities(diff),
        "dropped_op_count": diff.dropped_op_count,
    }


def _anomalies_payload(anomalies: list[dict[str, Any]] | None) -> dict[str, Any]:
    return {"anomalies": list(anomalies or [])}


def _predictions_payload(diff: ValidatedDiff) -> dict[str, Any]:
    preds: list[dict[str, Any]] = []
    for op in diff.new_predictions:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        entry = op.entry
        preds.append(
            {
                "tenant_id": str(diff.tenant_id),
                "trigger_ref": str(diff.trigger_ref),
                "entry": entry,
                "evaluate_at": entry.get("evaluate_at"),
            }
        )
    return {"predictions": preds}


def _payload_has_content(kind: str, payload: dict[str, Any]) -> bool:
    """Don't enqueue empty actions — keeps the queue tight and avoids
    burning handler cycles on empty broadcasts."""
    if kind == "publish_anomalies":
        return bool(payload.get("anomalies"))
    if kind == "schedule_predictions":
        return bool(payload.get("predictions"))
    if kind == "broadcast_realtime":
        # Always broadcast (even if diff is small, UI listeners want the
        # heartbeat). Callers pass at least op_counts.
        return True
    if kind == "invalidate_metrics":
        return bool(payload.get("affected_entities"))
    return False


# ---------------------------------------------------------------------
# Public: enqueue post-commit actions (called inside the apply tx)
# ---------------------------------------------------------------------


async def enqueue_post_commit_actions(
    trigger: TriggerContext | Any,
    validated_diff: ValidatedDiff,
    conn: asyncpg.Connection,
    *,
    anomalies: list[dict[str, Any]] | None = None,
) -> list[UUID]:
    """Enqueue post-commit actions derived from `validated_diff`.

    MUST be called inside the same transaction as `apply_diff`. The
    rows are atomically committed with the apply.

    Returns the list of newly-inserted row ids (excludes duplicates
    that were deduped by the unique constraint). Callers rarely need
    them; returned for test introspection.
    """
    tenant_id = validated_diff.tenant_id
    trigger_id = validated_diff.trigger_ref

    actions: list[tuple[str, dict[str, Any]]] = [
        ("publish_anomalies", _anomalies_payload(anomalies)),
        ("schedule_predictions", _predictions_payload(validated_diff)),
        ("broadcast_realtime", {"diff_summary": _summarize_diff(validated_diff)}),
        ("invalidate_metrics", {"affected_entities": _affected_entities(validated_diff)}),
    ]

    inserted: list[UUID] = []
    for kind, payload in actions:
        if not _payload_has_content(kind, payload):
            continue
        row = await conn.fetchrow(
            """
            INSERT INTO pending_post_commit_actions
              (tenant_id, trigger_id, action_kind, action_payload)
            VALUES ($1, $2, $3, $4::jsonb)
            ON CONFLICT ON CONSTRAINT post_commit_dedup DO NOTHING
            RETURNING id
            """,
            tenant_id,
            trigger_id,
            kind,
            json.dumps(payload, default=str),
        )
        if row is not None:
            inserted.append(row["id"])

    _log.info(
        "post_commit.enqueued",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        enqueued=len(inserted),
        attempted=len(actions),
    )
    return inserted


# ---------------------------------------------------------------------
# Worker — drains the queue with FOR UPDATE SKIP LOCKED.
# ---------------------------------------------------------------------


@dataclass
class PendingAction:
    id: UUID
    tenant_id: UUID
    trigger_id: UUID
    action_kind: str
    action_payload: dict[str, Any]
    attempts: int
    scheduled_at: Any
    created_at: Any


def _compute_backoff(next_attempts: int) -> int:
    """Exponential backoff seconds for `next_attempts` (1-indexed).

    Attempt 1 retry → 2s, attempt 2 → 4s, attempt 3 → 8s, capped at 300s.
    """
    if next_attempts <= 0:
        return 0
    # 2^(next_attempts - 1) so first retry is base seconds.
    seconds = BACKOFF_BASE_SECONDS * (2 ** (next_attempts - 1))
    return min(seconds, BACKOFF_CAP_SECONDS)


async def fetch_pending_actions(
    conn: asyncpg.Connection,
    *,
    limit: int = BATCH_SIZE,
    tenant_id: UUID | None = None,
) -> list[PendingAction]:
    """Fetch up to `limit` pending actions whose scheduled_at <= now().
    Caller owns a transaction and uses FOR UPDATE SKIP LOCKED so
    multiple workers can run in parallel without stepping on each
    other. `tenant_id` optionally restricts to a single tenant (used by
    per-tenant workers and tenant-scoped tests)."""
    if tenant_id is None:
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, trigger_id, action_kind, action_payload,
                   attempts, scheduled_at, created_at
            FROM pending_post_commit_actions
            WHERE processed_at IS NULL
              AND dead_lettered_at IS NULL
              AND scheduled_at <= now()
            ORDER BY scheduled_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            """,
            limit,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, trigger_id, action_kind, action_payload,
                   attempts, scheduled_at, created_at
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND processed_at IS NULL
              AND dead_lettered_at IS NULL
              AND scheduled_at <= now()
            ORDER BY scheduled_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            """,
            tenant_id, limit,
        )
    return [
        PendingAction(
            id=r["id"],
            tenant_id=r["tenant_id"],
            trigger_id=r["trigger_id"],
            action_kind=r["action_kind"],
            action_payload=_json_load(r["action_payload"]),
            attempts=r["attempts"],
            scheduled_at=r["scheduled_at"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


def _json_load(value: Any) -> dict[str, Any]:
    if isinstance(value, (dict, list)):
        return value if isinstance(value, dict) else {"items": value}
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value)
    return {}


async def mark_action_processed(
    conn: asyncpg.Connection, action_id: UUID,
) -> None:
    await conn.execute(
        """
        UPDATE pending_post_commit_actions
        SET processed_at = now(),
            last_error = NULL
        WHERE id = $1
        """,
        action_id,
    )


async def increment_attempts(
    conn: asyncpg.Connection,
    action_id: UUID,
    *,
    error: str,
) -> int:
    """Bump `attempts` by 1, reschedule with exponential backoff, and
    store the error. Returns the new (post-increment) attempts count.
    """
    current = await conn.fetchval(
        "SELECT attempts FROM pending_post_commit_actions WHERE id = $1",
        action_id,
    )
    if current is None:
        return 0
    next_attempts = int(current) + 1
    backoff = _compute_backoff(next_attempts)
    await conn.execute(
        """
        UPDATE pending_post_commit_actions
        SET attempts = $2,
            scheduled_at = now() + ($3 || ' seconds')::interval,
            last_error = $4
        WHERE id = $1
        """,
        action_id,
        next_attempts,
        str(backoff),
        error[:2000],
    )
    return next_attempts


async def move_to_dead_letter(
    conn: asyncpg.Connection, action_id: UUID, *, error: str,
) -> None:
    """Mark the row as dead-lettered. It is no longer eligible for the
    worker's poll query (partial index excludes `dead_lettered_at IS
    NOT NULL`). Operators drain with a plain SELECT."""
    await conn.execute(
        """
        UPDATE pending_post_commit_actions
        SET dead_lettered_at = now(),
            last_error = $2
        WHERE id = $1
        """,
        action_id,
        error[:2000],
    )
    _log.warning(
        "post_commit.dead_lettered",
        action_id=str(action_id),
        error=error[:200],
    )


async def dispatch_action(action: PendingAction) -> None:
    """Look up the registered handler and invoke it. Handlers MUST be
    idempotent; see the docstring at the top of this file."""
    handler = _DISPATCHERS.get(action.action_kind)
    if handler is None:
        raise RuntimeError(
            f"no handler registered for action_kind={action.action_kind!r}"
        )
    await handler(action.action_payload, action.tenant_id, action.trigger_id)


@dataclass
class WorkerStats:
    processed: int = 0
    failed: int = 0
    dead_lettered: int = 0
    iterations: int = 0


async def process_batch(
    pool: asyncpg.Pool,
    *,
    limit: int = BATCH_SIZE,
    stats: WorkerStats | None = None,
    tenant_id: UUID | None = None,
) -> WorkerStats:
    """Process one batch of pending actions. One DB connection per
    batch; each action is dispatched under its own savepoint so a
    handler crash doesn't roll back the bookkeeping.

    Returns the (updated) WorkerStats. Callers that want to drive the
    worker in-process one batch at a time (tests) use this directly.
    `tenant_id` restricts processing to a single tenant (per-tenant
    workers, test isolation).
    """
    stats = stats or WorkerStats()
    stats.iterations += 1

    async with pool.acquire() as conn:
        async with conn.transaction():
            actions = await fetch_pending_actions(
                conn, limit=limit, tenant_id=tenant_id,
            )
            for action in actions:
                try:
                    await dispatch_action(action)
                except Exception as exc:
                    err = f"{type(exc).__name__}: {exc}"
                    new_attempts = await increment_attempts(
                        conn, action.id, error=err,
                    )
                    if new_attempts >= MAX_ATTEMPTS:
                        await move_to_dead_letter(
                            conn, action.id,
                            error=(
                                f"exceeded max attempts ({MAX_ATTEMPTS}): {err}"
                            ),
                        )
                        stats.dead_lettered += 1
                    else:
                        stats.failed += 1
                    _log.warning(
                        "post_commit.dispatch_failed",
                        action_id=str(action.id),
                        action_kind=action.action_kind,
                        attempts=new_attempts,
                        error=err[:200],
                    )
                else:
                    await mark_action_processed(conn, action.id)
                    stats.processed += 1
    return stats


async def post_commit_worker(
    pool: asyncpg.Pool,
    *,
    poll_interval: float = POLL_INTERVAL_SECONDS,
    batch_size: int = BATCH_SIZE,
    stop_event: asyncio.Event | None = None,
    tenant_id: UUID | None = None,
) -> None:
    """Long-running worker loop. Polls the queue, dispatches actions,
    sleeps, repeats. `stop_event` lets callers (tests, supervisor
    shutdown) stop the loop cleanly. `tenant_id` scopes to a single
    tenant (per-tenant worker deployment or per-test isolation).
    """
    stats = WorkerStats()
    _log.info("post_commit.worker.started", poll_interval=poll_interval)
    try:
        while True:
            if stop_event is not None and stop_event.is_set():
                break
            try:
                await process_batch(
                    pool, limit=batch_size, stats=stats, tenant_id=tenant_id,
                )
            except Exception as e:
                _log.exception("post_commit.worker.iteration_error", error=str(e))
            if stop_event is not None:
                try:
                    await asyncio.wait_for(
                        stop_event.wait(), timeout=poll_interval,
                    )
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(poll_interval)
    finally:
        _log.info(
            "post_commit.worker.stopped",
            processed=stats.processed,
            failed=stats.failed,
            dead_lettered=stats.dead_lettered,
            iterations=stats.iterations,
        )


__all__ = [
    "MAX_ATTEMPTS",
    "BACKOFF_BASE_SECONDS",
    "BACKOFF_CAP_SECONDS",
    "POLL_INTERVAL_SECONDS",
    "BATCH_SIZE",
    "ACTION_KINDS",
    "ActionHandler",
    "PendingAction",
    "WorkerStats",
    "enqueue_post_commit_actions",
    "fetch_pending_actions",
    "mark_action_processed",
    "increment_attempts",
    "move_to_dead_letter",
    "dispatch_action",
    "process_batch",
    "post_commit_worker",
    "register_handler",
    "get_handler",
    "reset_handlers",
]
