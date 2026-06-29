"""services/reasoning/think/post_commit.py — durable post-commit action queue.

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

Dedup: enqueue treats `(tenant, trigger, action_kind)` as an immutable
idempotency key. Once an action has ever been recorded, later enqueue
attempts for the same key are skipped, even if the prior row has already
processed. The `post_commit_dedup UNIQUE NULLS NOT DISTINCT` constraint
still protects concurrent first writers while the application-level
existence check closes the race between enqueue and `processed_at`
updates.
"""

from __future__ import annotations

import asyncio
import json
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg
import structlog

from lib.shared.backoff import (
    QUEUE_RETRY_BACKOFF_BASE_SECONDS,
    QUEUE_RETRY_BACKOFF_CAP_SECONDS,
    queue_retry_backoff_seconds,
)
from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import ValidatedDiff


_log = structlog.get_logger(__name__)

_DISPATCH_CONN: ContextVar[asyncpg.Connection | None] = ContextVar(
    "post_commit_dispatch_conn",
    default=None,
)


# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------

MAX_ATTEMPTS = 5
BACKOFF_BASE_SECONDS = int(QUEUE_RETRY_BACKOFF_BASE_SECONDS)
BACKOFF_CAP_SECONDS = int(QUEUE_RETRY_BACKOFF_CAP_SECONDS)

POLL_INTERVAL_SECONDS = 2.0
BATCH_SIZE = 10
VIEW_CEO_REFRESH_CHANNEL = "view_ceo_refresh"

ACTION_KINDS = (
    "publish_anomalies",
    "schedule_predictions",
    "broadcast_realtime",
    "invalidate_metrics",
    "discover_model_edges",
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


# Product-facing default handlers emit the durable CEO-view refresh NOTIFY
# consumed by services.product.greeting.scheduler. `publish_anomalies` is
# already committed to `think_anomalies_raw` in `anomaly_integration.py`;
# Wave 4-B's anomaly_processor consumes from there. `discover_model_edges` is
# the durable post-commit path for topology candidate generation. We keep the
# registry so the worker can be driven end-to-end in tests.


async def _default_publish_anomalies(
    payload: dict[str, Any],
    tenant_id: UUID,
    trigger_id: UUID,
) -> None:
    _log.info(
        "post_commit.publish_anomalies.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        anomaly_count=len(payload.get("anomalies", [])),
    )
    await _notify_view_ceo_refresh(
        tenant_id,
        trigger_id,
        reason="anomaly_flagged",
    )


async def _default_schedule_predictions(
    payload: dict[str, Any],
    tenant_id: UUID,
    trigger_id: UUID,
) -> None:
    predictions = payload.get("predictions", [])
    missing_schedule = [
        index for index, prediction in enumerate(predictions)
        if not isinstance(prediction, dict) or not prediction.get("evaluate_at")
    ]
    if missing_schedule:
        raise RuntimeError(
            "schedule_predictions payload missing evaluate_at for "
            f"prediction indexes {missing_schedule}"
        )
    _log.info(
        "post_commit.schedule_predictions.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        prediction_count=len(predictions),
    )
    await _notify_view_ceo_refresh(
        tenant_id,
        trigger_id,
        reason="prediction_scheduled",
    )


async def _default_broadcast_realtime(
    payload: dict[str, Any],
    tenant_id: UUID,
    trigger_id: UUID,
) -> None:
    _log.info(
        "post_commit.broadcast_realtime.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
    )
    await _notify_view_ceo_refresh(
        tenant_id,
        trigger_id,
        reason="substrate_changed",
    )


async def _default_invalidate_metrics(
    payload: dict[str, Any],
    tenant_id: UUID,
    trigger_id: UUID,
) -> None:
    _log.info(
        "post_commit.invalidate_metrics.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        affected_count=len(payload.get("affected_entities", [])),
    )
    await _notify_view_ceo_refresh(
        tenant_id,
        trigger_id,
        reason="metrics_invalidated",
    )


async def _default_discover_model_edges(
    payload: dict[str, Any],
    tenant_id: UUID,
    trigger_id: UUID,
) -> None:
    model_ids = _model_ids_from_payload(payload)
    if not model_ids:
        return

    from lib.shared.db import get_pool
    from services.reasoning.edge_intelligence import promote_pair_evidence_candidates
    from services.reasoning.topology import LatentTopologyService

    service = LatentTopologyService()
    candidates_inserted = 0
    think_triggers_enqueued = 0
    duplicates_suppressed = 0
    pair_evidence_scanned = 0
    pair_evidence_candidates_inserted = 0
    pair_evidence_candidates_skipped = 0
    pair_evidence_failed = 0
    skipped: dict[str, int] = {}
    errors: list[str] = []

    async def _run(conn: asyncpg.Connection) -> None:
        nonlocal candidates_inserted
        nonlocal think_triggers_enqueued
        nonlocal duplicates_suppressed
        nonlocal pair_evidence_scanned
        nonlocal pair_evidence_candidates_inserted
        nonlocal pair_evidence_candidates_skipped
        nonlocal pair_evidence_failed
        for model_id in model_ids:
            try:
                async with conn.transaction():
                    result = await service.generate_for_model_id(
                        conn,
                        tenant_id=tenant_id,
                        model_id=model_id,
                        enqueue_think=True,
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{model_id}: {type(exc).__name__}: {exc}")
                continue

            candidates_inserted += len(result.inserted_candidates)
            think_triggers_enqueued += result.enqueued_think_triggers
            duplicates_suppressed += result.duplicates_suppressed
            if result.skipped_reason:
                skipped[result.skipped_reason] = (
                    skipped.get(result.skipped_reason, 0) + 1
                )

        promotion_limit = max(20, min(200, len(model_ids) * 12))
        promotion = await promote_pair_evidence_candidates(
            conn,
            tenant_id=tenant_id,
            limit=promotion_limit,
            model_ids=model_ids,
        )
        pair_evidence_scanned += promotion.scanned_pair_evidence
        pair_evidence_candidates_inserted += promotion.candidates_inserted
        pair_evidence_candidates_skipped += promotion.candidates_skipped
        pair_evidence_failed += promotion.failed_pair_evidence
        errors.extend(promotion.errors)

    conn = _DISPATCH_CONN.get()
    if conn is not None:
        await _run(conn)
    else:
        pool = get_pool()
        async with pool.acquire() as acquired:
            await _run(acquired)

    _log.info(
        "post_commit.discover_model_edges.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        model_count=len(model_ids),
        candidates_inserted=candidates_inserted,
        think_triggers_enqueued=think_triggers_enqueued,
        duplicates_suppressed=duplicates_suppressed,
        pair_evidence_scanned=pair_evidence_scanned,
        pair_evidence_candidates_inserted=pair_evidence_candidates_inserted,
        pair_evidence_candidates_skipped=pair_evidence_candidates_skipped,
        pair_evidence_failed=pair_evidence_failed,
        skipped=skipped,
        error_count=len(errors),
    )
    if errors:
        raise RuntimeError("; ".join(errors[:3]))


_DISPATCHERS: dict[str, ActionHandler] = {
    "publish_anomalies": _default_publish_anomalies,
    "schedule_predictions": _default_schedule_predictions,
    "broadcast_realtime": _default_broadcast_realtime,
    "invalidate_metrics": _default_invalidate_metrics,
    "discover_model_edges": _default_discover_model_edges,
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
    _DISPATCHERS["discover_model_edges"] = _default_discover_model_edges


async def _notify_view_ceo_refresh(
    tenant_id: UUID,
    trigger_id: UUID,
    *,
    reason: str,
) -> None:
    """Ask gateway CEO-view schedulers to refresh cached product state.

    When called by ``process_batch`` this runs on the same transaction that
    marks the post-commit action processed, so Postgres delivers the NOTIFY
    only if the queue bookkeeping commits.
    """
    payload = json.dumps(
        {
            "tenant_id": str(tenant_id),
            "trigger_id": str(trigger_id),
            "reason": reason,
        },
        default=str,
    )
    conn = _DISPATCH_CONN.get()
    if conn is not None:
        await conn.execute(
            "SELECT pg_notify($1, $2)",
            VIEW_CEO_REFRESH_CHANNEL,
            payload,
        )
        return

    from lib.shared.db import get_pool

    pool = get_pool()
    async with pool.acquire() as acquired:
        await acquired.execute(
            "SELECT pg_notify($1, $2)",
            VIEW_CEO_REFRESH_CHANNEL,
            payload,
        )


# ---------------------------------------------------------------------
# Payload builders — one per action kind.
# ---------------------------------------------------------------------


def _summarize_op_count(diff: ValidatedDiff) -> dict[str, int]:
    return {
        "claim_ops": len(diff.claim_ops),
        "memory_lifecycle_ops": len(diff.memory_lifecycle_ops),
        "relation_claim_ops": len(diff.relation_claim_ops),
        "relation_frame_ops": len(diff.relation_frame_ops),
        "edge_ops": len(diff.edge_ops),
        "ontology_gap_ops": len(diff.ontology_gap_ops),
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
    for op in diff.memory_lifecycle_ops:
        _add("model", op.model_id)
        _add("model", op.superseded_by_model_id)
        for model_id in op.evidence_model_ids:
            _add("model", model_id)
    for op in diff.edge_ops:
        _add("model", op.source_model_id)
        _add("model", op.target_model_id)
    for op in diff.relation_claim_ops:
        _add("model", op.source_model_id)
        _add("model", op.target_model_id)
        for model_id in op.evidence_model_ids:
            _add("model", model_id)
    for op in diff.relation_frame_ops:
        for participant in op.participants:
            _add("model", participant.model_id)
        for model_id in op.evidence_model_ids:
            _add("model", model_id)
    for op in diff.ontology_gap_ops:
        _add("model", op.source_model_id)
        _add("model", op.target_model_id)
        for model_id in op.evidence_model_ids:
            _add("model", model_id)
    for op in diff.act_ops:
        ent = op.entity or {}
        eid = ent.get("id")
        if op.op.startswith("create_commitment") or op.op == "transition_commitment":
            _add("commitment", eid)
        elif op.op.startswith("create_goal") or op.op in (
            "update_goal",
            "transition_goal",
        ):
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


def _edge_discovery_payload(
    applied_model_ids: list[UUID] | tuple[UUID, ...] | None,
) -> dict[str, Any]:
    seen: set[UUID] = set()
    model_ids: list[str] = []
    for value in applied_model_ids or ():
        try:
            model_id = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError):
            continue
        if model_id in seen:
            continue
        seen.add(model_id)
        model_ids.append(str(model_id))
    return {"model_ids": model_ids}


def _model_ids_from_payload(payload: dict[str, Any]) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in payload.get("model_ids") or ():
        try:
            model_id = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError):
            continue
        if model_id in seen:
            continue
        seen.add(model_id)
        out.append(model_id)
    return out


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
    if kind == "discover_model_edges":
        return bool(payload.get("model_ids"))
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
    applied_model_ids: list[UUID] | tuple[UUID, ...] | None = None,
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
        (
            "invalidate_metrics",
            {"affected_entities": _affected_entities(validated_diff)},
        ),
        ("discover_model_edges", _edge_discovery_payload(applied_model_ids)),
    ]

    inserted: list[UUID] = []
    for kind, payload in actions:
        if not _payload_has_content(kind, payload):
            continue
        existing = await conn.fetchval(
            """
            SELECT 1
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND trigger_id = $2
              AND action_kind = $3
            LIMIT 1
            """,
            tenant_id,
            trigger_id,
            kind,
        )
        if existing is not None:
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

    Attempt 1 retry -> 10s, attempt 2 -> 20s, capped at 300s.
    """
    return int(queue_retry_backoff_seconds(next_attempts))


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
            tenant_id,
            limit,
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
    conn: asyncpg.Connection,
    action_id: UUID,
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
    conn: asyncpg.Connection,
    action_id: UUID,
    *,
    error: str,
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
                conn,
                limit=limit,
                tenant_id=tenant_id,
            )
            for action in actions:
                try:
                    token = _DISPATCH_CONN.set(conn)
                    try:
                        await dispatch_action(action)
                    finally:
                        _DISPATCH_CONN.reset(token)
                except Exception as exc:
                    err = f"{type(exc).__name__}: {exc}"
                    new_attempts = await increment_attempts(
                        conn,
                        action.id,
                        error=err,
                    )
                    if new_attempts >= MAX_ATTEMPTS:
                        await move_to_dead_letter(
                            conn,
                            action.id,
                            error=(f"exceeded max attempts ({MAX_ATTEMPTS}): {err}"),
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
                    pool,
                    limit=batch_size,
                    stats=stats,
                    tenant_id=tenant_id,
                )
            except Exception as e:
                _log.exception("post_commit.worker.iteration_error", error=str(e))
            if stop_event is not None:
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=poll_interval,
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
    "VIEW_CEO_REFRESH_CHANNEL",
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
