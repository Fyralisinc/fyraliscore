"""Repository for persisted Resolution Threads.

Resolution Threads are monitored state-change contracts. They may be
created directly, or instantiated from a Decision Delta's
impact.resolution_thread payload when the user accepts the delta.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError


THREAD_STATUSES: frozenset[str] = frozenset({
    "draft",
    "active",
    "waiting_on_owner",
    "blocked",
    "monitoring",
    "confirmed",
    "resolved",
    "failed",
})
STEP_STATUSES: frozenset[str] = frozenset({
    "not_started",
    "in_progress",
    "waiting",
    "blocked",
    "done",
    "failed",
})
SIGNAL_STATUSES: frozenset[str] = frozenset({
    "watching",
    "seen",
    "missing",
    "contradicted",
})


class ResolutionThreadRepoError(CompanyOSError):
    default_code = "resolution_thread_repo_error"


class ResolutionThreadNotFoundError(ResolutionThreadRepoError):
    default_code = "resolution_thread_not_found"


@dataclass
class ResolutionStep:
    id: UUID
    thread_id: UUID
    label: str
    owner_label: str
    status: str
    due_at: datetime | None
    proof_needed: str | None
    blocked_by: str | None
    ordinal: int
    created_at: datetime
    updated_at: datetime


@dataclass
class ResolutionWatchedSignal:
    id: UUID
    thread_id: UUID
    label: str
    source_type: str
    expected: str
    status: str
    last_observed_at: datetime | None
    matched_evidence: dict[str, Any] | None
    ordinal: int
    created_at: datetime
    updated_at: datetime


@dataclass
class ResolutionEvent:
    id: UUID
    thread_id: UUID
    event_type: str
    actor_id: UUID | None
    payload: dict[str, Any]
    created_at: datetime


@dataclass
class ResolutionThread:
    id: UUID
    tenant_id: UUID
    source_decision_delta_id: UUID | None
    target_node_kind: str | None
    target_node_id: UUID | None
    title: str
    status: str
    current_state: str
    target_state: str
    owner_label: str
    next_review_at: datetime | None
    success_criteria: list[str]
    escalation_triggers: list[str]
    created_by: UUID | None
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None
    failed_at: datetime | None
    steps: list[ResolutionStep] = field(default_factory=list)
    watched_signals: list[ResolutionWatchedSignal] = field(default_factory=list)
    events: list[ResolutionEvent] = field(default_factory=list)


async def create_thread(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    payload: dict[str, Any],
    source_decision_delta_id: UUID | None = None,
    target_node_kind: str | None = None,
    target_node_id: UUID | None = None,
    created_by: UUID | None = None,
) -> ResolutionThread:
    normalized = _normalize_thread_payload(payload)

    existing: ResolutionThread | None = None
    if source_decision_delta_id is not None:
        existing = await get_thread_by_source_delta(
            conn,
            tenant_id=tenant_id,
            source_decision_delta_id=source_decision_delta_id,
        )
    if existing is not None:
        return existing

    row = await conn.fetchrow(
        """
        INSERT INTO resolution_threads (
          tenant_id, source_decision_delta_id,
          target_node_kind, target_node_id,
          title, status, current_state, target_state, owner_label,
          next_review_at, success_criteria, escalation_triggers,
          created_by, resolved_at, failed_at
        ) VALUES (
          $1, $2, $3, $4,
          $5, $6, $7, $8, $9,
          $10, $11::jsonb, $12::jsonb,
          $13, $14, $15
        )
        RETURNING *
        """,
        tenant_id,
        source_decision_delta_id,
        target_node_kind,
        target_node_id,
        normalized["title"],
        normalized["status"],
        normalized["currentState"],
        normalized["targetState"],
        normalized["owner"],
        _parse_dt(normalized.get("nextReviewAt")),
        _dump_jsonb(normalized["successCriteria"]),
        _dump_jsonb(normalized["escalationTriggers"]),
        created_by,
        datetime.now(timezone.utc) if normalized["status"] == "resolved" else None,
        datetime.now(timezone.utc) if normalized["status"] == "failed" else None,
    )
    thread_id: UUID = row["id"]

    await _replace_steps(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
        steps=normalized["steps"],
    )
    await _replace_signals(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
        signals=normalized["watchedSignals"],
    )
    await append_event(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
        event_type="created",
        actor_id=created_by,
        payload={
            "source_decision_delta_id": (
                str(source_decision_delta_id) if source_decision_delta_id else None
            )
        },
    )
    loaded = await get_thread(conn, tenant_id=tenant_id, thread_id=thread_id)
    assert loaded is not None
    return loaded


async def ensure_thread_for_delta(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    delta_view: Any,
    actor_id: UUID | None = None,
) -> tuple[ResolutionThread | None, bool]:
    """Create a thread from delta.impact.resolution_thread once.

    Returns (thread, created). If the delta has no resolution payload,
    returns (None, False).
    """
    existing = await get_thread_by_source_delta(
        conn,
        tenant_id=tenant_id,
        source_decision_delta_id=delta_view.id,
    )
    if existing is not None:
        return existing, False

    impact = delta_view.impact or {}
    raw = _first(impact, "resolutionThread", "resolution_thread")
    if not isinstance(raw, dict):
        return None, False

    thread = await create_thread(
        conn,
        tenant_id=tenant_id,
        payload=raw,
        source_decision_delta_id=delta_view.id,
        target_node_kind=delta_view.target_node_kind,
        target_node_id=delta_view.target_node_id,
        created_by=actor_id,
    )
    return thread, True


async def list_threads(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    status: str | Iterable[str] | None = None,
    target_node_kind: str | None = None,
    target_node_id: UUID | None = None,
    source_decision_delta_id: UUID | None = None,
    limit: int = 50,
) -> list[ResolutionThread]:
    clauses: list[str] = []
    args: list[Any] = [tenant_id]

    if status is not None:
        statuses = [status] if isinstance(status, str) else list(status)
        for item in statuses:
            if item not in THREAD_STATUSES:
                raise ValidationError(f"invalid status {item!r}", field="status")
        args.append(statuses)
        clauses.append(f"status = ANY(${len(args)}::text[])")
    if target_node_kind is not None:
        args.append(target_node_kind)
        clauses.append(f"target_node_kind = ${len(args)}")
    if target_node_id is not None:
        args.append(target_node_id)
        clauses.append(f"target_node_id = ${len(args)}")
    if source_decision_delta_id is not None:
        args.append(source_decision_delta_id)
        clauses.append(f"source_decision_delta_id = ${len(args)}")

    args.append(max(1, min(200, int(limit))))
    extra = " AND " + " AND ".join(clauses) if clauses else ""
    rows = await conn.fetch(
        """
        SELECT * FROM resolution_threads
        WHERE tenant_id = $1
        """
        + extra
        + f" ORDER BY updated_at DESC LIMIT ${len(args)}",
        *args,
    )
    threads = [_thread_from_row(row) for row in rows]
    await _load_children(conn, tenant_id=tenant_id, threads=threads)
    return threads


async def get_thread(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
    include_events: bool = False,
) -> ResolutionThread | None:
    row = await conn.fetchrow(
        "SELECT * FROM resolution_threads WHERE id = $1 AND tenant_id = $2",
        thread_id,
        tenant_id,
    )
    if row is None:
        return None
    thread = _thread_from_row(row)
    await _load_children(
        conn,
        tenant_id=tenant_id,
        threads=[thread],
        include_events=include_events,
    )
    return thread


async def get_thread_by_source_delta(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    source_decision_delta_id: UUID,
) -> ResolutionThread | None:
    row = await conn.fetchrow(
        """
        SELECT * FROM resolution_threads
        WHERE tenant_id = $1 AND source_decision_delta_id = $2
        """,
        tenant_id,
        source_decision_delta_id,
    )
    if row is None:
        return None
    thread = _thread_from_row(row)
    await _load_children(conn, tenant_id=tenant_id, threads=[thread])
    return thread


async def get_threads_by_source_delta_ids(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    source_decision_delta_ids: list[UUID],
) -> dict[UUID, ResolutionThread]:
    if not source_decision_delta_ids:
        return {}
    rows = await conn.fetch(
        """
        SELECT * FROM resolution_threads
        WHERE tenant_id = $1 AND source_decision_delta_id = ANY($2::uuid[])
        """,
        tenant_id,
        source_decision_delta_ids,
    )
    threads = [_thread_from_row(row) for row in rows]
    await _load_children(conn, tenant_id=tenant_id, threads=threads)
    return {
        t.source_decision_delta_id: t
        for t in threads
        if t.source_decision_delta_id is not None
    }


async def update_thread_status(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
    status: str,
    actor_id: UUID | None = None,
    reason: str | None = None,
) -> ResolutionThread:
    if status not in THREAD_STATUSES:
        raise ValidationError(f"invalid status {status!r}", field="status")
    existing = await get_thread(conn, tenant_id=tenant_id, thread_id=thread_id)
    if existing is None:
        raise ResolutionThreadNotFoundError(
            f"resolution thread {thread_id} not found",
            thread_id=str(thread_id),
        )
    now = datetime.now(timezone.utc)
    await conn.execute(
        """
        UPDATE resolution_threads
        SET status = $3,
            resolved_at = CASE WHEN $3 = 'resolved' THEN $4 ELSE resolved_at END,
            failed_at = CASE WHEN $3 = 'failed' THEN $4 ELSE failed_at END
        WHERE id = $1 AND tenant_id = $2
        """,
        thread_id,
        tenant_id,
        status,
        now,
    )
    await append_event(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
        event_type="thread_status_changed",
        actor_id=actor_id,
        payload={"from": existing.status, "to": status, "reason": reason},
    )
    updated = await get_thread(conn, tenant_id=tenant_id, thread_id=thread_id)
    assert updated is not None
    return updated


async def update_step_status(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
    step_id: UUID,
    status: str,
    actor_id: UUID | None = None,
    proof: str | None = None,
    blocked_by: str | None = None,
) -> ResolutionThread:
    if status not in STEP_STATUSES:
        raise ValidationError(f"invalid step status {status!r}", field="status")
    row = await conn.fetchrow(
        """
        SELECT status FROM resolution_thread_steps
        WHERE id = $1 AND thread_id = $2 AND tenant_id = $3
        """,
        step_id,
        thread_id,
        tenant_id,
    )
    if row is None:
        raise ResolutionThreadNotFoundError(
            f"resolution step {step_id} not found",
            step_id=str(step_id),
        )
    await conn.execute(
        """
        UPDATE resolution_thread_steps
        SET status = $4,
            proof_needed = COALESCE($5, proof_needed),
            blocked_by = COALESCE($6, blocked_by)
        WHERE id = $1 AND thread_id = $2 AND tenant_id = $3
        """,
        step_id,
        thread_id,
        tenant_id,
        status,
        proof,
        blocked_by,
    )
    await append_event(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
        event_type="step_status_changed",
        actor_id=actor_id,
        payload={
            "step_id": str(step_id),
            "from": row["status"],
            "to": status,
        },
    )
    return await _refresh_parent_status(conn, tenant_id=tenant_id, thread_id=thread_id)


async def update_signal_status(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
    signal_id: UUID,
    status: str,
    actor_id: UUID | None = None,
    matched_evidence: dict[str, Any] | None = None,
    observed_at: datetime | None = None,
) -> ResolutionThread:
    if status not in SIGNAL_STATUSES:
        raise ValidationError(f"invalid signal status {status!r}", field="status")
    row = await conn.fetchrow(
        """
        SELECT status FROM resolution_thread_watched_signals
        WHERE id = $1 AND thread_id = $2 AND tenant_id = $3
        """,
        signal_id,
        thread_id,
        tenant_id,
    )
    if row is None:
        raise ResolutionThreadNotFoundError(
            f"resolution signal {signal_id} not found",
            signal_id=str(signal_id),
        )
    await conn.execute(
        """
        UPDATE resolution_thread_watched_signals
        SET status = $4,
            last_observed_at = CASE
              WHEN $4 IN ('seen', 'contradicted') THEN COALESCE($5, now())
              ELSE last_observed_at
            END,
            matched_evidence = COALESCE($6::jsonb, matched_evidence)
        WHERE id = $1 AND thread_id = $2 AND tenant_id = $3
        """,
        signal_id,
        thread_id,
        tenant_id,
        status,
        observed_at,
        _dump_jsonb(matched_evidence),
    )
    await append_event(
        conn,
        tenant_id=tenant_id,
        thread_id=thread_id,
        event_type="signal_status_changed",
        actor_id=actor_id,
        payload={
            "signal_id": str(signal_id),
            "from": row["status"],
            "to": status,
            "matched_evidence": matched_evidence,
        },
    )
    return await _refresh_parent_status(conn, tenant_id=tenant_id, thread_id=thread_id)


async def append_event(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
    event_type: str,
    actor_id: UUID | None = None,
    payload: dict[str, Any] | None = None,
) -> UUID:
    row = await conn.fetchrow(
        """
        INSERT INTO resolution_thread_events (
          tenant_id, thread_id, event_type, actor_id, payload
        ) VALUES ($1, $2, $3, $4, $5::jsonb)
        RETURNING id
        """,
        tenant_id,
        thread_id,
        event_type,
        actor_id,
        _dump_jsonb(payload or {}),
    )
    return row["id"]


def thread_to_wire(thread: ResolutionThread, *, include_events: bool = False) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(thread.id),
        "sourceDecisionDeltaId": (
            str(thread.source_decision_delta_id)
            if thread.source_decision_delta_id else None
        ),
        "targetNodeKind": thread.target_node_kind,
        "targetNodeId": str(thread.target_node_id) if thread.target_node_id else None,
        "title": thread.title,
        "status": thread.status,
        "currentState": thread.current_state,
        "targetState": thread.target_state,
        "owner": thread.owner_label,
        "nextReviewAt": _isofmt(thread.next_review_at),
        "successCriteria": thread.success_criteria,
        "steps": [
            {
                "id": str(s.id),
                "label": s.label,
                "owner": s.owner_label,
                "status": s.status,
                "dueAt": _isofmt(s.due_at),
                "proofNeeded": s.proof_needed,
                "blockedBy": s.blocked_by,
            }
            for s in thread.steps
        ],
        "watchedSignals": [
            {
                "id": str(s.id),
                "label": s.label,
                "sourceType": s.source_type,
                "expected": s.expected,
                "status": s.status,
                "lastObservedAt": _isofmt(s.last_observed_at),
                "matchedEvidence": s.matched_evidence,
            }
            for s in thread.watched_signals
        ],
        "escalationTriggers": thread.escalation_triggers,
        "createdAt": _isofmt(thread.created_at),
        "updatedAt": _isofmt(thread.updated_at),
        "resolvedAt": _isofmt(thread.resolved_at),
        "failedAt": _isofmt(thread.failed_at),
    }
    if include_events:
        out["events"] = [
            {
                "id": str(e.id),
                "eventType": e.event_type,
                "actorId": str(e.actor_id) if e.actor_id else None,
                "payload": e.payload,
                "createdAt": _isofmt(e.created_at),
            }
            for e in thread.events
        ]
    return out


async def _replace_steps(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
    steps: list[dict[str, Any]],
) -> None:
    for idx, step in enumerate(steps):
        await conn.execute(
            """
            INSERT INTO resolution_thread_steps (
              tenant_id, thread_id, label, owner_label, status,
              due_at, proof_needed, blocked_by, ordinal
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            tenant_id,
            thread_id,
            step["label"],
            step["owner"],
            step["status"],
            _parse_dt(step.get("dueAt")),
            step.get("proofNeeded"),
            step.get("blockedBy"),
            idx,
        )


async def _replace_signals(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
    signals: list[dict[str, Any]],
) -> None:
    for idx, signal in enumerate(signals):
        await conn.execute(
            """
            INSERT INTO resolution_thread_watched_signals (
              tenant_id, thread_id, label, source_type,
              expected, status, ordinal
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            tenant_id,
            thread_id,
            signal["label"],
            signal["sourceType"],
            signal["expected"],
            signal["status"],
            idx,
        )


async def _load_children(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    threads: list[ResolutionThread],
    include_events: bool = False,
) -> None:
    if not threads:
        return
    ids = [t.id for t in threads]
    by_id = {t.id: t for t in threads}
    step_rows = await conn.fetch(
        """
        SELECT * FROM resolution_thread_steps
        WHERE tenant_id = $1 AND thread_id = ANY($2::uuid[])
        ORDER BY ordinal ASC, created_at ASC
        """,
        tenant_id,
        ids,
    )
    for row in step_rows:
        by_id[row["thread_id"]].steps.append(_step_from_row(row))

    signal_rows = await conn.fetch(
        """
        SELECT * FROM resolution_thread_watched_signals
        WHERE tenant_id = $1 AND thread_id = ANY($2::uuid[])
        ORDER BY ordinal ASC, created_at ASC
        """,
        tenant_id,
        ids,
    )
    for row in signal_rows:
        by_id[row["thread_id"]].watched_signals.append(_signal_from_row(row))

    if include_events:
        event_rows = await conn.fetch(
            """
            SELECT * FROM resolution_thread_events
            WHERE tenant_id = $1 AND thread_id = ANY($2::uuid[])
            ORDER BY created_at DESC
            """,
            tenant_id,
            ids,
        )
        for row in event_rows:
            by_id[row["thread_id"]].events.append(_event_from_row(row))


async def _refresh_parent_status(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    thread_id: UUID,
) -> ResolutionThread:
    thread = await get_thread(conn, tenant_id=tenant_id, thread_id=thread_id)
    if thread is None:
        raise ResolutionThreadNotFoundError(
            f"resolution thread {thread_id} not found",
            thread_id=str(thread_id),
        )
    if thread.status in {"resolved", "failed"}:
        return thread
    step_statuses = {s.status for s in thread.steps}
    signal_statuses = {s.status for s in thread.watched_signals}
    next_status: str | None = None
    if "failed" in step_statuses or "contradicted" in signal_statuses:
        next_status = "blocked"
    elif "blocked" in step_statuses:
        next_status = "blocked"
    elif (
        thread.steps
        and all(s.status == "done" for s in thread.steps)
        and (
            not thread.watched_signals
            or all(s.status == "seen" for s in thread.watched_signals)
        )
    ):
        next_status = "confirmed"
    elif signal_statuses and signal_statuses <= {"seen"}:
        next_status = "monitoring"
    if next_status and next_status != thread.status:
        await conn.execute(
            """
            UPDATE resolution_threads
            SET status = $3
            WHERE id = $1 AND tenant_id = $2
            """,
            thread_id,
            tenant_id,
            next_status,
        )
        await append_event(
            conn,
            tenant_id=tenant_id,
            thread_id=thread_id,
            event_type="thread_status_auto_changed",
            payload={"from": thread.status, "to": next_status},
        )
        refreshed = await get_thread(conn, tenant_id=tenant_id, thread_id=thread_id)
        assert refreshed is not None
        return refreshed
    return thread


def _normalize_thread_payload(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValidationError("resolution thread payload must be an object", field="payload")
    title = _required_str(_first(raw, "title", "name", "label"), "title")
    current = _required_str(_first(raw, "currentState", "current_state"), "currentState")
    target = _required_str(_first(raw, "targetState", "target_state"), "targetState")
    owner = _required_str(_first(raw, "owner", "ownerLabel", "owner_label"), "owner")
    status = str(_first(raw, "status") or "active")
    if status not in THREAD_STATUSES:
        raise ValidationError(f"invalid thread status {status!r}", field="status")
    return {
        "title": title,
        "status": status,
        "currentState": current,
        "targetState": target,
        "owner": owner,
        "nextReviewAt": _first(raw, "nextReviewAt", "next_review_at"),
        "successCriteria": _str_list(_first(raw, "successCriteria", "success_criteria")),
        "steps": _normalize_steps(_first(raw, "steps")),
        "watchedSignals": _normalize_signals(_first(raw, "watchedSignals", "watched_signals")),
        "escalationTriggers": _str_list(_first(raw, "escalationTriggers", "escalation_triggers")),
    }


def _normalize_steps(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValidationError("steps must be a list", field="steps")
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValidationError("step must be an object", field=f"steps[{idx}]")
        label = _required_str(_first(item, "label", "title", "name"), f"steps[{idx}].label")
        owner = _required_str(_first(item, "owner", "ownerLabel", "owner_label"), f"steps[{idx}].owner")
        status = str(_first(item, "status") or "not_started")
        if status not in STEP_STATUSES:
            raise ValidationError(f"invalid step status {status!r}", field=f"steps[{idx}].status")
        out.append({
            "label": label,
            "owner": owner,
            "status": status,
            "dueAt": _first(item, "dueAt", "due_at", "deadline"),
            "proofNeeded": _optional_str(_first(item, "proofNeeded", "proof_needed", "proof")),
            "blockedBy": _optional_str(_first(item, "blockedBy", "blocked_by")),
        })
    return out


def _normalize_signals(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValidationError("watchedSignals must be a list", field="watchedSignals")
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValidationError("watched signal must be an object", field=f"watchedSignals[{idx}]")
        label = _required_str(_first(item, "label", "title", "name"), f"watchedSignals[{idx}].label")
        source = _required_str(_first(item, "sourceType", "source_type", "source"), f"watchedSignals[{idx}].sourceType")
        expected = _required_str(_first(item, "expected", "expectation"), f"watchedSignals[{idx}].expected")
        status = str(_first(item, "status") or "watching")
        if status not in SIGNAL_STATUSES:
            raise ValidationError(f"invalid signal status {status!r}", field=f"watchedSignals[{idx}].status")
        out.append({
            "label": label,
            "sourceType": source,
            "expected": expected,
            "status": status,
        })
    return out


def _thread_from_row(row: asyncpg.Record) -> ResolutionThread:
    return ResolutionThread(
        id=row["id"],
        tenant_id=row["tenant_id"],
        source_decision_delta_id=row["source_decision_delta_id"],
        target_node_kind=row["target_node_kind"],
        target_node_id=row["target_node_id"],
        title=row["title"],
        status=row["status"],
        current_state=row["current_state"],
        target_state=row["target_state"],
        owner_label=row["owner_label"],
        next_review_at=row["next_review_at"],
        success_criteria=_str_list(row["success_criteria"]),
        escalation_triggers=_str_list(row["escalation_triggers"]),
        created_by=row["created_by"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        resolved_at=row["resolved_at"],
        failed_at=row["failed_at"],
    )


def _step_from_row(row: asyncpg.Record) -> ResolutionStep:
    return ResolutionStep(
        id=row["id"],
        thread_id=row["thread_id"],
        label=row["label"],
        owner_label=row["owner_label"],
        status=row["status"],
        due_at=row["due_at"],
        proof_needed=row["proof_needed"],
        blocked_by=row["blocked_by"],
        ordinal=int(row["ordinal"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _signal_from_row(row: asyncpg.Record) -> ResolutionWatchedSignal:
    return ResolutionWatchedSignal(
        id=row["id"],
        thread_id=row["thread_id"],
        label=row["label"],
        source_type=row["source_type"],
        expected=row["expected"],
        status=row["status"],
        last_observed_at=row["last_observed_at"],
        matched_evidence=row["matched_evidence"],
        ordinal=int(row["ordinal"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _event_from_row(row: asyncpg.Record) -> ResolutionEvent:
    return ResolutionEvent(
        id=row["id"],
        thread_id=row["thread_id"],
        event_type=row["event_type"],
        actor_id=row["actor_id"],
        payload=row["payload"] or {},
        created_at=row["created_at"],
    )


def _first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload:
            return payload[key]
    return None


def _required_str(value: Any, field: str) -> str:
    s = _optional_str(value)
    if not s:
        raise ValidationError(f"{field} is required", field=field)
    return s


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _str_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(v) for v in value if str(v).strip()]


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as e:
            raise ValidationError("invalid timestamp", field="timestamp") from e
    return None


def _dump_jsonb(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, default=str)


def _isofmt(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None
