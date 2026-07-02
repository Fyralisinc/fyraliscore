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
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg
import structlog

from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import ClaimOp, ValidatedDiff


_log = structlog.get_logger(__name__)

_DISPATCH_CONN: ContextVar[asyncpg.Connection | None] = ContextVar(
    "post_commit_dispatch_conn",
    default=None,
)


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
PROJECTION_EVENTS_PER_MODEL = 8
PROJECTION_MIN_EVENT_LIMIT = 24
PROJECTION_MAX_EVENT_LIMIT = 1000

ACTION_KINDS = (
    "publish_anomalies",
    "schedule_predictions",
    "broadcast_realtime",
    "invalidate_metrics",
    "materialize_projections",
    "discover_model_edges",
    "search_open_questions",
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


@dataclass(frozen=True)
class _ProjectionMaterializationDispatch:
    mode: str
    processed_events: int = 0
    failed_events: int = 0
    routed_events: int = 0
    enqueued_jobs: int = 0
    route_errors: int = 0
    processed_jobs: int = 0
    failed_jobs: int = 0
    projection_errors: tuple[dict[str, Any], ...] = ()


# Most default handlers are no-ops (publish_anomalies is already
# committed to `think_anomalies_raw` in `anomaly_integration.py`; Wave
# 4-B's anomaly_processor consumes from there). `discover_model_edges`
# is the durable post-commit path for topology candidate generation.
# We keep the registry so the worker can be driven end-to-end in tests.


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


async def _default_schedule_predictions(
    payload: dict[str, Any],
    tenant_id: UUID,
    trigger_id: UUID,
) -> None:
    _log.info(
        "post_commit.schedule_predictions.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        prediction_count=len(payload.get("predictions", [])),
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


async def _projection_tables_ready(conn: asyncpg.Connection) -> bool:
    rows = await conn.fetch(
        """
        SELECT to_regclass(name) IS NOT NULL AS exists
        FROM unnest($1::text[]) AS name
        """,
        [
            "public.model_events",
            "public.projection_checkpoints",
            "public.projection_snapshots",
        ],
    )
    return bool(rows) and all(row["exists"] for row in rows)


async def _projection_delta_tables_ready(conn: asyncpg.Connection) -> bool:
    rows = await conn.fetch(
        """
        SELECT to_regclass(name) IS NOT NULL AS exists
        FROM unnest($1::text[]) AS name
        """,
        [
            "public.projection_refresh_jobs",
            "public.projection_dependencies",
            "public.projection_watch_keys",
            "public.projection_inquiry_state",
        ],
    )
    return bool(rows) and all(row["exists"] for row in rows)


async def _default_materialize_projections(
    payload: dict[str, Any],
    tenant_id: UUID,
    trigger_id: UUID,
) -> None:
    model_ids = _model_ids_from_payload(payload)
    if not model_ids:
        return

    from lib.shared.db import get_pool
    from services.domain.projections import (
        ProjectionRunner,
        enqueue_refreshes_for_event,
    )
    from services.domain.projections.catalog import projectors_for
    from services.domain.projections.store import fetch_events_for_models

    raw_limit = payload.get("limit")
    limit = _projection_event_limit(len(model_ids), raw_limit=raw_limit)
    projection_names = payload.get("projection_names")
    if not isinstance(projection_names, list) or not projection_names:
        projection_names = ["all"]
    projectors = projectors_for(projection_names)
    runner = ProjectionRunner(projectors)

    async def _run(conn: asyncpg.Connection) -> _ProjectionMaterializationDispatch:
        if not await _projection_tables_ready(conn):
            _log.info(
                "post_commit.materialize_projections.skipped",
                tenant_id=str(tenant_id),
                trigger_id=str(trigger_id),
                reason="projection_tables_missing",
            )
            return _ProjectionMaterializationDispatch(mode="skipped")
        if await _projection_delta_tables_ready(conn):
            events = await fetch_events_for_models(
                conn,
                tenant_id=tenant_id,
                model_ids=model_ids,
                limit=limit,
            )
            route_reports = [
                await enqueue_refreshes_for_event(conn, event, projectors)
                for event in events
            ]
            enqueued_jobs = sum(len(report.enqueued_jobs) for report in route_reports)
            refresh_report = await runner.run_queued_refresh_jobs_once_detailed(
                conn,
                tenant_id=tenant_id,
                limit=max(1, limit, enqueued_jobs),
            )
            route_errors = [
                {
                    "projection_name": error.projection_name,
                    "projection_version": error.projection_version,
                    "event_id": str(route_report.event_id),
                    "stage": error.stage,
                    "message": error.message,
                }
                for route_report in route_reports
                for error in route_report.errors
            ]
            refresh_errors = [
                {
                    "projection_name": error.projection_name,
                    "projection_version": error.projection_version,
                    "subject_key": error.subject_key,
                    "job_id": str(error.job_id),
                    "stage": error.stage,
                    "message": error.message,
                }
                for error in refresh_report.errors
            ]
            return _ProjectionMaterializationDispatch(
                mode="delta_queue",
                routed_events=len(events),
                enqueued_jobs=enqueued_jobs,
                route_errors=len(route_errors),
                processed_jobs=refresh_report.processed_jobs,
                failed_jobs=refresh_report.failed_jobs,
                projection_errors=tuple([*route_errors, *refresh_errors][:5]),
            )

        legacy_report = await runner.run_once_detailed(
            conn,
            tenant_id=tenant_id,
            limit=limit,
        )
        return _ProjectionMaterializationDispatch(
            mode="legacy_checkpoint",
            processed_events=legacy_report.processed_events,
            failed_events=legacy_report.failed_events,
            projection_errors=tuple(
                {
                    "projection_name": error.projection_name,
                    "projection_version": error.projection_version,
                    "event_id": str(error.event_id),
                    "model_id": str(error.model_id),
                    "stage": error.stage,
                    "message": error.message,
                }
                for error in legacy_report.errors[:5]
            ),
        )

    conn = _DISPATCH_CONN.get()
    if conn is not None:
        report = await _run(conn)
    else:
        pool = get_pool()
        async with pool.acquire() as acquired:
            report = await _run(acquired)

    _log.info(
        "post_commit.materialize_projections.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        mode=report.mode,
        model_count=len(model_ids),
        limit=limit,
        processed_events=report.processed_events,
        failed_events=report.failed_events,
        routed_events=report.routed_events,
        enqueued_jobs=report.enqueued_jobs,
        route_errors=report.route_errors,
        processed_jobs=report.processed_jobs,
        failed_jobs=report.failed_jobs,
        projection_errors=list(report.projection_errors),
    )


async def _default_discover_model_edges(
    payload: dict[str, Any],
    tenant_id: UUID,
    trigger_id: UUID,
) -> None:
    model_ids = _model_ids_from_payload(payload)
    if not model_ids:
        return
    enqueue_think = bool(payload.get("enqueue_think", True))
    think_enqueue_budget = _think_enqueue_budget_from_payload(
        payload,
        default=len(model_ids) if enqueue_think else 0,
    )

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
        remaining_think_budget = think_enqueue_budget
        for model_id in model_ids:
            model_enqueue_think = enqueue_think and remaining_think_budget > 0
            try:
                async with conn.transaction():
                    result = await service.generate_for_model_id(
                        conn,
                        tenant_id=tenant_id,
                        model_id=model_id,
                        enqueue_think=model_enqueue_think,
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{model_id}: {type(exc).__name__}: {exc}")
                continue

            candidates_inserted += len(result.inserted_candidates)
            think_triggers_enqueued += result.enqueued_think_triggers
            if model_enqueue_think:
                remaining_think_budget = max(
                    0,
                    remaining_think_budget - int(result.enqueued_think_triggers or 0),
                )
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
        enqueue_think=enqueue_think,
        think_enqueue_budget=think_enqueue_budget,
        source_trigger_kind=payload.get("source_trigger_kind"),
        source_trigger_subkind=payload.get("source_trigger_subkind"),
        selector=payload.get("selector"),
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


async def _default_search_open_questions(
    payload: dict[str, Any],
    tenant_id: UUID,
    trigger_id: UUID,
) -> None:
    model_ids = _model_ids_from_payload(payload)
    question_ids = _uuid_list_from_payload(payload, "open_question_ids")
    if not model_ids and not question_ids:
        return

    from lib.shared.db import get_pool
    from services.domain.models.open_questions import ModelOpenQuestionsRepo
    from services.domain.triggers import enqueue_trigger

    repo = ModelOpenQuestionsRepo()
    raw_limit = payload.get("limit")
    try:
        limit = int(raw_limit) if raw_limit is not None else 50
    except (TypeError, ValueError):
        limit = 50
    limit = max(1, min(limit, 200))
    enqueued = 0
    deduped = 0
    searched_ids: list[UUID] = []

    async def _run(conn: asyncpg.Connection) -> None:
        nonlocal enqueued
        nonlocal deduped
        questions = await repo.list_due_for_search(
            conn,
            tenant_id=tenant_id,
            question_ids=question_ids,
            model_ids=model_ids,
            limit=limit,
        )
        for question in questions:
            key = f"{question.model_id}:{question.id}"
            existing = await conn.fetchval(
                """
                SELECT id
                FROM think_trigger_queue
                WHERE tenant_id = $1
                  AND trigger_kind = 'T4'
                  AND trigger_subkind = 'open_question_search'
                  AND completed_at IS NULL
                  AND payload->>'open_question_key' = $2
                LIMIT 1
                """,
                tenant_id,
                key,
            )
            if existing is not None:
                deduped += 1
                searched_ids.append(question.id)
                continue
            await enqueue_trigger(
                conn,
                tenant_id=tenant_id,
                trigger_kind="T4",
                trigger_subkind="open_question_search",
                model_id=question.model_id,
                payload=_open_question_trigger_payload(question, key),
            )
            enqueued += 1
            searched_ids.append(question.id)
        await repo.mark_searched(conn, question_ids=searched_ids)

    conn = _DISPATCH_CONN.get()
    if conn is not None:
        await _run(conn)
    else:
        pool = get_pool()
        async with pool.acquire() as acquired:
            async with acquired.transaction():
                await _run(acquired)

    _log.info(
        "post_commit.search_open_questions.dispatched",
        tenant_id=str(tenant_id),
        trigger_id=str(trigger_id),
        model_count=len(model_ids),
        question_count=len(question_ids),
        enqueued=enqueued,
        deduped=deduped,
    )


_DISPATCHERS: dict[str, ActionHandler] = {
    "publish_anomalies": _default_publish_anomalies,
    "schedule_predictions": _default_schedule_predictions,
    "broadcast_realtime": _default_broadcast_realtime,
    "invalidate_metrics": _default_invalidate_metrics,
    "materialize_projections": _default_materialize_projections,
    "discover_model_edges": _default_discover_model_edges,
    "search_open_questions": _default_search_open_questions,
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
    _DISPATCHERS["materialize_projections"] = _default_materialize_projections
    _DISPATCHERS["discover_model_edges"] = _default_discover_model_edges
    _DISPATCHERS["search_open_questions"] = _default_search_open_questions


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
        "open_question_ops": len(diff.open_question_ops),
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
    for op in diff.open_question_ops:
        _add("model", op.model_id)
        _add("model", op.resolution_model_id)
        for model_id in op.source_model_ids:
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
    seen: set[str] = set()
    for source, op in _iter_prediction_claim_inserts(diff):
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        entry = op.entry
        dedupe_key = _prediction_payload_key(entry)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        preds.append(
            {
                "tenant_id": str(diff.tenant_id),
                "trigger_ref": str(diff.trigger_ref),
                "source": source,
                "entry": entry,
                "evaluate_at": entry.get("evaluate_at"),
            }
        )
    return {"predictions": preds}


def _iter_prediction_claim_inserts(
    diff: ValidatedDiff,
) -> list[tuple[str, ClaimOp]]:
    out: list[tuple[str, ClaimOp]] = []
    out.extend(("new_predictions", op) for op in diff.new_predictions)
    out.extend(
        ("claim_ops", op)
        for op in diff.claim_ops
        if op.op == "insert"
        and isinstance(op.entry, dict)
        and _entry_is_prediction_like(op.entry)
    )
    return out


def _entry_is_prediction_like(entry: dict[str, Any]) -> bool:
    proposition = entry.get("proposition")
    prop = proposition if isinstance(proposition, dict) else {}
    falsifier = entry.get("falsifier")
    falsifier_kind = falsifier.get("kind") if isinstance(falsifier, dict) else None
    return (
        entry.get("claim_role") == "prediction"
        or prop.get("claim_role") == "prediction"
        or prop.get("kind") == "prediction"
        or falsifier_kind == "prediction_deadline"
    )


def _prediction_payload_key(entry: dict[str, Any]) -> str:
    return json.dumps(
        {
            "natural": entry.get("natural"),
            "proposition": entry.get("proposition"),
            "evaluate_at": entry.get("evaluate_at"),
            "resolution_criteria": entry.get("resolution_criteria"),
        },
        sort_keys=True,
        default=str,
    )


def _edge_discovery_payload(
    applied_model_ids: list[UUID] | tuple[UUID, ...] | None,
    *,
    trigger: TriggerContext | Any | None = None,
    applied_ops_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_model_ids, selector = _edge_discovery_model_ids(
        applied_model_ids,
        applied_ops_summary=applied_ops_summary,
    )
    source_kind = str(getattr(trigger, "kind", "") or "")
    source_subkind = str(getattr(trigger, "subkind", "") or "")
    enqueue_think = source_kind == "T1"
    return {
        "model_ids": [str(model_id) for model_id in selected_model_ids],
        "source_trigger_kind": source_kind or None,
        "source_trigger_subkind": source_subkind or None,
        "selector": selector,
        # Only primary business-signal batches should promote fresh topology
        # hints into immediate T4 Think. Downstream maintenance runs may still
        # persist cheap candidate memory, but should not recursively spend LLM.
        "enqueue_think": enqueue_think,
        "think_enqueue_budget": _edge_discovery_think_enqueue_budget(
            source_kind=source_kind,
            model_count=len(selected_model_ids),
        ),
    }


def _edge_discovery_think_enqueue_budget(
    *,
    source_kind: str,
    model_count: int,
) -> int:
    if source_kind != "T1" or model_count <= 0:
        return 0
    return max(1, min(2, int(model_count)))


def _edge_discovery_model_ids(
    applied_model_ids: list[UUID] | tuple[UUID, ...] | None,
    *,
    applied_ops_summary: dict[str, Any] | None = None,
) -> tuple[list[UUID], str]:
    inserted = _inserted_model_ids_from_apply_summary(applied_ops_summary)
    if inserted:
        return inserted, "claim_insert_models"
    if applied_ops_summary is not None:
        return [], "no_claim_insert_models"

    seen: set[UUID] = set()
    model_ids: list[UUID] = []
    for value in applied_model_ids or ():
        try:
            model_id = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError):
            continue
        if model_id in seen:
            continue
        seen.add(model_id)
        model_ids.append(model_id)
    return model_ids, "legacy_all_applied_models"


def _inserted_model_ids_from_apply_summary(
    applied_ops_summary: dict[str, Any] | None,
) -> list[UUID]:
    if not isinstance(applied_ops_summary, dict):
        return []
    out: list[UUID] = []
    seen: set[UUID] = set()
    for summary in applied_ops_summary.get("claim_ops") or ():
        if not isinstance(summary, dict) or summary.get("op") != "insert":
            continue
        raw_model_id = summary.get("model_id")
        try:
            model_id = (
                raw_model_id
                if isinstance(raw_model_id, UUID)
                else UUID(str(raw_model_id))
            )
        except (TypeError, ValueError, AttributeError):
            continue
        if model_id not in seen:
            seen.add(model_id)
            out.append(model_id)
    return out


def _projection_materialization_payload(
    applied_model_ids: list[UUID] | tuple[UUID, ...] | None,
    *,
    trigger: TriggerContext | Any | None = None,
    applied_ops_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_model_ids = _dedupe_model_ids(applied_model_ids)
    source_kind = str(getattr(trigger, "kind", "") or "")
    source_subkind = str(getattr(trigger, "subkind", "") or "")
    model_count = len(selected_model_ids)
    return {
        "model_ids": [str(model_id) for model_id in selected_model_ids],
        "source_trigger_kind": source_kind or None,
        "source_trigger_subkind": source_subkind or None,
        "selector": "all_applied_models",
        "projection_names": _projection_names_for_apply_summary(applied_ops_summary),
        "limit": _projection_event_limit(model_count),
    }


def _projection_names_for_apply_summary(
    applied_ops_summary: dict[str, Any] | None,
) -> list[str]:
    if not isinstance(applied_ops_summary, dict):
        return ["all"]

    names: set[str] = set()
    if _summary_has_entity_projection_signal(
        applied_ops_summary,
        entity_types={
            "candidate_commitment",
            "commitment",
            "jira",
            "pr",
            "pull_request",
            "ticket",
            "work_item",
        },
        domain_tags={
            "commitment",
            "commitments",
            "deadline",
            "deliverable",
            "handoff",
            "obligation",
            "promise",
        },
        act_ops=(),
    ):
        names.add("commitments")
    if _summary_has_entity_projection_signal(
        applied_ops_summary,
        entity_types={
            "account",
            "candidate_customer",
            "customer",
            "customer_resource",
            "org",
            "organization",
        },
        domain_tags={
            "account",
            "churn",
            "customer",
            "customers",
            "implementation",
            "onboarding",
            "relationship",
            "renewal",
            "retention",
            "revenue",
            "trust",
        },
        act_ops=(),
    ):
        names.add("customers")
    if _summary_has_entity_projection_signal(
        applied_ops_summary,
        entity_types={
            "candidate_goal",
            "goal",
            "initiative",
            "objective",
            "project",
            "workstream",
        },
        domain_tags={
            "goal",
            "goals",
            "initiative",
            "milestone",
            "northstar",
            "objective",
            "outcome",
            "roadmap",
        },
        act_ops=(),
    ):
        names.add("goals")
    if _summary_has_entity_projection_signal(
        applied_ops_summary,
        entity_types={"candidate_decision", "choice", "decision"},
        domain_tags={
            "approval",
            "decision",
            "decision_pressure",
            "decisions",
            "escalation",
            "go/no-go",
            "option",
            "prioritize",
            "tradeoff",
        },
        act_ops=("create_decision", "transition_decision"),
    ):
        names.add("decisions")
    if _summary_has_items(applied_ops_summary, "resource_ops"):
        names.add("resources")
    if _summary_has_actor_profile_signal(applied_ops_summary):
        names.add("employee_profiles")
    if _summary_has_decision_surface_signal(applied_ops_summary):
        names.add("decision_surfaces")
    if any(
        _summary_has_items(applied_ops_summary, key)
        for key in (
            "claim_ops",
            "memory_lifecycle_ops",
            "relation_claim_ops",
            "relation_frame_ops",
            "edge_ops",
            "ontology_gap_ops",
            "open_question_ops",
            "act_ops",
        )
    ):
        names.add("constraints")
    if not names:
        names.add("constraints")
    return sorted(names)


def _summary_has_items(summary: dict[str, Any], key: str) -> bool:
    value = summary.get(key)
    return isinstance(value, list) and bool(value)


def _summary_has_entity_projection_signal(
    summary: dict[str, Any],
    *,
    entity_types: set[str],
    domain_tags: set[str],
    act_ops: tuple[str, ...],
) -> bool:
    for item in summary.get("claim_ops") or ():
        if not isinstance(item, dict):
            continue
        tags = _summary_item_domain_tags(item)
        if tags.intersection(domain_tags):
            return True
        for entity in _summary_item_scope_entities(item):
            entity_type = str(entity.get("type") or "").strip().casefold()
            if entity_type in entity_types:
                return True

    if act_ops:
        expected = {op.casefold() for op in act_ops}
        for item in summary.get("act_ops") or ():
            if not isinstance(item, dict):
                continue
            op = str(item.get("op") or item.get("act_op") or "").casefold()
            if op in expected:
                return True
    return False


def _summary_item_domain_tags(item: dict[str, Any]) -> set[str]:
    sources = [item]
    for key in ("entry", "payload", "proposition"):
        value = item.get(key)
        if isinstance(value, dict):
            sources.append(value)
    tags: set[str] = set()
    for source in sources:
        tags.update(
            str(tag).casefold()
            for tag in source.get("domain_tags") or ()
            if str(tag).strip()
        )
    return tags


def _summary_item_scope_entities(item: dict[str, Any]) -> list[dict[str, Any]]:
    sources = [item]
    for key in ("entry", "payload", "proposition"):
        value = item.get(key)
        if isinstance(value, dict):
            sources.append(value)
    out: list[dict[str, Any]] = []
    for source in sources:
        for entity in source.get("scope_entities") or ():
            if isinstance(entity, dict):
                out.append(entity)
    return out


def _summary_has_actor_profile_signal(summary: dict[str, Any]) -> bool:
    employee_tags = {
        "actor",
        "capacity",
        "employee",
        "employees",
        "mentorship",
        "people",
        "preference",
        "support_need",
        "team",
        "work_pattern",
        "work_style",
        "workload",
    }
    profile_roles = {
        "capability",
        "concern",
        "pattern",
        "relation",
        "recommendation",
    }
    for item in summary.get("claim_ops") or ():
        if not isinstance(item, dict):
            continue
        tags = _summary_item_domain_tags(item)
        if tags.intersection(employee_tags):
            return True
        scope_actors = item.get("scope_actors") or {}
        entry = item.get("entry") if isinstance(item.get("entry"), dict) else {}
        if not scope_actors and isinstance(entry, dict):
            scope_actors = entry.get("scope_actors") or ()
        if (
            str(item.get("claim_role") or entry.get("claim_role") or "").casefold()
            in profile_roles
            and bool(scope_actors)
        ):
            return True
    return False


def _summary_has_decision_surface_signal(summary: dict[str, Any]) -> bool:
    decision_tags = {
        "approval",
        "blocker",
        "blocked",
        "bottleneck",
        "capacity",
        "constraint",
        "decision",
        "decision_pressure",
        "dependency",
        "escalation",
        "execution",
        "owner",
        "pressure",
        "priority",
        "resource",
        "risk",
        "tradeoff",
    }
    decision_roles = {"concern", "recommendation", "situation"}
    for item in summary.get("claim_ops") or ():
        if not isinstance(item, dict):
            continue
        tags = _summary_item_domain_tags(item)
        entry = item.get("entry") if isinstance(item.get("entry"), dict) else {}
        role = str(item.get("claim_role") or entry.get("claim_role") or "").casefold()
        if tags.intersection(decision_tags):
            return True
        if role in decision_roles and tags.intersection(decision_tags):
            return True

    for item in summary.get("act_ops") or ():
        if not isinstance(item, dict):
            continue
        op = str(item.get("op") or item.get("act_op") or "").casefold()
        if op in {"create_decision", "transition_decision"}:
            return True
    return False


def _projection_event_limit(model_count: int, *, raw_limit: Any = None) -> int:
    if raw_limit is not None:
        try:
            explicit = int(raw_limit)
        except (TypeError, ValueError):
            explicit = None
        if explicit is not None:
            return max(1, min(explicit, PROJECTION_MAX_EVENT_LIMIT))

    scaled = max(
        PROJECTION_MIN_EVENT_LIMIT,
        max(0, int(model_count)) * PROJECTION_EVENTS_PER_MODEL,
    )
    return max(1, min(scaled, PROJECTION_MAX_EVENT_LIMIT))


def _open_questions_payload(
    validated_diff: ValidatedDiff,
    *,
    applied_model_ids: list[UUID] | tuple[UUID, ...] | None,
    applied_open_question_ids: list[UUID] | tuple[UUID, ...] | None,
) -> dict[str, Any]:
    model_ids: list[UUID] = []
    seen_models: set[UUID] = set()
    for value in applied_model_ids or ():
        try:
            model_id = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError):
            continue
        if model_id not in seen_models:
            seen_models.add(model_id)
            model_ids.append(model_id)
    has_insert = False
    for op in validated_diff.open_question_ops:
        if op.op == "insert":
            has_insert = True
        if op.model_id is not None and op.model_id not in seen_models:
            seen_models.add(op.model_id)
            model_ids.append(op.model_id)
    question_ids: list[UUID] = []
    seen_questions: set[UUID] = set()
    for value in applied_open_question_ids or ():
        try:
            question_id = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError):
            continue
        if question_id not in seen_questions:
            seen_questions.add(question_id)
            question_ids.append(question_id)
    if not has_insert and not question_ids:
        return {"model_ids": [], "open_question_ids": []}
    return {
        "model_ids": [str(model_id) for model_id in model_ids],
        "open_question_ids": [str(question_id) for question_id in question_ids],
        "limit": max(20, min(200, max(1, len(question_ids) + len(model_ids)) * 12)),
    }


def _model_ids_from_payload(payload: dict[str, Any]) -> list[UUID]:
    return _dedupe_model_ids(payload.get("model_ids") or ())


def _dedupe_model_ids(
    values: list[UUID] | tuple[UUID, ...] | list[Any] | tuple[Any, ...] | Any,
) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values or ():
        try:
            model_id = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError):
            continue
        if model_id in seen:
            continue
        seen.add(model_id)
        out.append(model_id)
    return out


def _think_enqueue_budget_from_payload(
    payload: dict[str, Any],
    *,
    default: int,
) -> int:
    try:
        raw_budget = payload.get("think_enqueue_budget", default)
        budget = int(raw_budget)
    except (TypeError, ValueError):
        budget = int(default)
    return max(0, budget)


def _uuid_list_from_payload(payload: dict[str, Any], key: str) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    values = payload.get(key)
    if not isinstance(values, list):
        return []
    for value in values:
        try:
            uid = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError, AttributeError):
            continue
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


def _open_question_trigger_payload(question: Any, key: str) -> dict[str, Any]:
    search_signature = (
        question.search_signature if isinstance(question.search_signature, dict) else {}
    )
    expected_signal = (
        question.expected_resolution_signal
        if isinstance(question.expected_resolution_signal, dict)
        else {}
    )
    seed_parts = [f"Open question search: {question.question}"]
    if question.rationale:
        seed_parts.append(str(question.rationale))
    signal_shape = expected_signal.get("signal_shape") or expected_signal.get("answer")
    if signal_shape:
        seed_parts.append(str(signal_shape))
    source_model_ids = [str(model_id) for model_id in question.source_model_ids]
    return {
        "open_question_key": key,
        "open_question_id": str(question.id),
        "source_model_id": str(question.model_id),
        "source_model_ids": source_model_ids,
        "model_ids": [str(question.model_id)],
        "question": question.question,
        "question_type": question.question_type,
        "rationale": question.rationale,
        "priority": question.priority,
        "expected_resolution_signal": expected_signal,
        "search_signature": search_signature,
        "seed_natural_text": " ".join(part for part in seed_parts if part)[:2000],
        "seed_occurred_at": question.created_at.isoformat(),
        "question_primitive": "OPEN_QUESTION",
    }


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
    if kind == "materialize_projections":
        return bool(payload.get("model_ids"))
    if kind == "discover_model_edges":
        return bool(payload.get("model_ids"))
    if kind == "search_open_questions":
        return bool(payload.get("model_ids") or payload.get("open_question_ids"))
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
    applied_open_question_ids: list[UUID] | tuple[UUID, ...] | None = None,
    applied_ops_summary: dict[str, Any] | None = None,
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
        (
            "materialize_projections",
            _projection_materialization_payload(
                applied_model_ids,
                trigger=trigger,
                applied_ops_summary=applied_ops_summary,
            ),
        ),
        (
            "discover_model_edges",
            _edge_discovery_payload(
                applied_model_ids,
                trigger=trigger,
                applied_ops_summary=applied_ops_summary,
            ),
        ),
        (
            "search_open_questions",
            _open_questions_payload(
                validated_diff,
                applied_model_ids=applied_model_ids,
                applied_open_question_ids=applied_open_question_ids,
            ),
        ),
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
    action_timeout_seconds: float | None = None,
) -> WorkerStats:
    """Process one batch of pending actions. Each action is fetched and
    dispatched in its own transaction so one slow or failing side effect
    cannot roll back bookkeeping for earlier successful actions.

    Returns the (updated) WorkerStats. Callers that want to drive the
    worker in-process one batch at a time (tests) use this directly.
    `tenant_id` restricts processing to a single tenant (per-tenant
    workers, test isolation). `action_timeout_seconds` bounds each
    individual side effect; the outer drain may still impose a total
    batch timeout.
    """
    stats = stats or WorkerStats()
    stats.iterations += 1

    for _ in range(max(0, int(limit))):
        dispatched = await _process_one_pending_action(
            pool,
            stats=stats,
            tenant_id=tenant_id,
            action_timeout_seconds=action_timeout_seconds,
        )
        if not dispatched:
            break
    return stats


async def _process_one_pending_action(
    pool: asyncpg.Pool,
    *,
    stats: WorkerStats,
    tenant_id: UUID | None,
    action_timeout_seconds: float | None,
) -> bool:
    async with pool.acquire() as conn:
        async with conn.transaction():
            actions = await fetch_pending_actions(
                conn,
                limit=1,
                tenant_id=tenant_id,
            )
            if not actions:
                return False
            action = actions[0]
            try:
                await _dispatch_action_in_transaction(
                    action,
                    conn,
                    action_timeout_seconds=action_timeout_seconds,
                )
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
    return True


async def _dispatch_action_in_transaction(
    action: PendingAction,
    conn: asyncpg.Connection,
    *,
    action_timeout_seconds: float | None,
) -> None:
    token = _DISPATCH_CONN.set(conn)
    try:
        dispatch = dispatch_action(action)
        if action_timeout_seconds is not None and action_timeout_seconds > 0:
            await asyncio.wait_for(dispatch, timeout=float(action_timeout_seconds))
        else:
            await dispatch
    finally:
        _DISPATCH_CONN.reset(token)


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
