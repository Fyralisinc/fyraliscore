"""Persistence adapter for Ask Fyralis."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Protocol
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7

from .schemas import (
    AskAnswerPayload,
    AskEvidenceItem,
    AskMode,
    AskProposedStateChange,
    AskScope,
    AskSession,
)


class AskStore(Protocol):
    async def create_session(
        self,
        *,
        tenant_id: UUID,
        viewer_id: UUID,
        scope: AskScope,
        source_route: str | None,
        source_object_id: UUID | None,
        source_object_type: str | None,
        mode: AskMode,
    ) -> AskSession: ...

    async def get_session(self, session_id: UUID, *, tenant_id: UUID) -> AskSession | None: ...
    async def update_scope(self, session_id: UUID, *, scope: AskScope) -> None: ...
    async def add_message(
        self,
        *,
        session_id: UUID,
        role: str,
        content: str,
        structured_payload: dict[str, Any] | None = None,
    ) -> UUID: ...
    async def add_retrieval_run(
        self,
        *,
        session_id: UUID,
        message_id: UUID,
        intent: str,
        retrieval_plan: dict[str, Any],
        mode: AskMode,
        status: str,
        latency_ms: int | None,
    ) -> UUID: ...
    async def update_retrieval_run_status(
        self,
        retrieval_run_id: UUID,
        *,
        status: str,
        latency_ms: int | None = None,
        error: str | None = None,
    ) -> None: ...
    async def add_evidence_items(
        self,
        retrieval_run_id: UUID,
        items: list[AskEvidenceItem],
    ) -> None: ...
    async def add_answer(
        self,
        *,
        session_id: UUID,
        message_id: UUID,
        retrieval_run_id: UUID,
        payload: AskAnswerPayload,
        mode: AskMode,
        scope: AskScope,
        latency_ms: int,
    ) -> UUID: ...
    async def update_answer_payload(
        self,
        answer_id: UUID,
        payload: AskAnswerPayload,
    ) -> None: ...
    async def add_proposed_state_change(
        self,
        *,
        tenant_id: UUID,
        answer_id: UUID,
        proposed_op: dict[str, Any],
    ) -> AskProposedStateChange: ...
    async def list_evidence(
        self,
        retrieval_run_id: UUID,
        *,
        tenant_id: UUID | None = None,
    ) -> tuple[list[AskEvidenceItem], list[AskEvidenceItem]]: ...
    async def act_on_proposed_change(
        self,
        *,
        tenant_id: UUID,
        change_id: UUID,
        action: str,
        note: str | None,
        delegate_to: str | None,
    ) -> AskProposedStateChange: ...
    async def add_feedback(
        self,
        *,
        session_id: UUID,
        answer_id: UUID | None,
        viewer_id: UUID,
        feedback_type: str,
        payload: dict[str, Any],
    ) -> UUID: ...


class PostgresAskStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create_session(
        self,
        *,
        tenant_id: UUID,
        viewer_id: UUID,
        scope: AskScope,
        source_route: str | None,
        source_object_id: UUID | None,
        source_object_type: str | None,
        mode: AskMode,
    ) -> AskSession:
        sid = uuid7()
        scope_json = scope.model_dump(mode="json")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO ask_sessions (
                  id, tenant_id, viewer_id, initial_scope, current_scope,
                  source_route, source_object_id, source_object_type,
                  mode, status
                )
                VALUES ($1, $2, $3, $4::jsonb, $4::jsonb, $5, $6, $7, $8, 'open')
                RETURNING *
                """,
                sid,
                tenant_id,
                viewer_id,
                _jsonb(scope_json),
                source_route,
                source_object_id,
                source_object_type,
                mode,
            )
            await conn.execute(
                """
                INSERT INTO ask_scopes (
                  id, session_id, scope_type, label, root_nodes,
                  related_entities, filters, access_snapshot
                )
                VALUES ($1, $2, $3, $4, $5::uuid[], $6::uuid[], $7::jsonb, $8::jsonb)
                """,
                uuid7(),
                sid,
                scope.type,
                scope.label,
                scope.root_node_ids,
                scope.related_entity_ids,
                _jsonb(scope.filters),
                _jsonb({"access_mode": scope.access_mode}),
            )
        return _session_from_row(row)

    async def get_session(self, session_id: UUID, *, tenant_id: UUID) -> AskSession | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM ask_sessions WHERE id = $1 AND tenant_id = $2",
                session_id,
                tenant_id,
            )
        return _session_from_row(row) if row else None

    async def update_scope(self, session_id: UUID, *, scope: AskScope) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE ask_sessions SET current_scope = $2::jsonb WHERE id = $1",
                session_id,
                _jsonb(scope.model_dump(mode="json")),
            )
            await conn.execute(
                """
                INSERT INTO ask_scopes (
                  id, session_id, scope_type, label, root_nodes,
                  related_entities, filters, access_snapshot
                )
                VALUES ($1, $2, $3, $4, $5::uuid[], $6::uuid[], $7::jsonb, $8::jsonb)
                """,
                uuid7(),
                session_id,
                scope.type,
                scope.label,
                scope.root_node_ids,
                scope.related_entity_ids,
                _jsonb(scope.filters),
                _jsonb({"access_mode": scope.access_mode}),
            )

    async def add_message(
        self,
        *,
        session_id: UUID,
        role: str,
        content: str,
        structured_payload: dict[str, Any] | None = None,
    ) -> UUID:
        mid = uuid7()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ask_messages (
                  id, session_id, role, content, structured_payload
                )
                VALUES ($1, $2, $3, $4, $5::jsonb)
                """,
                mid,
                session_id,
                role,
                content,
                _jsonb(structured_payload) if structured_payload is not None else None,
            )
        return mid

    async def add_retrieval_run(
        self,
        *,
        session_id: UUID,
        message_id: UUID,
        intent: str,
        retrieval_plan: dict[str, Any],
        mode: AskMode,
        status: str,
        latency_ms: int | None,
    ) -> UUID:
        rid = uuid7()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ask_retrieval_runs (
                  id, session_id, message_id, intent, retrieval_plan,
                  mode, status, latency_ms
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8)
                """,
                rid,
                session_id,
                message_id,
                intent,
                _jsonb(retrieval_plan),
                mode,
                status,
                latency_ms,
            )
        return rid

    async def update_retrieval_run_status(
        self,
        retrieval_run_id: UUID,
        *,
        status: str,
        latency_ms: int | None = None,
        error: str | None = None,
    ) -> None:
        plan_patch = {"error": error} if error else {}
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE ask_retrieval_runs
                   SET status = $2,
                       latency_ms = COALESCE($3, latency_ms),
                       retrieval_plan = CASE
                         WHEN $4::jsonb = '{}'::jsonb THEN retrieval_plan
                         ELSE retrieval_plan || $4::jsonb
                       END
                 WHERE id = $1
                """,
                retrieval_run_id,
                status,
                latency_ms,
                _jsonb(plan_patch),
            )

    async def add_evidence_items(
        self,
        retrieval_run_id: UUID,
        items: list[AskEvidenceItem],
    ) -> None:
        if not items:
            return
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO ask_evidence_items (
                  id, retrieval_run_id, source_ref, source_kind, summary,
                  strength, supports_answer, is_counterevidence,
                  token_estimate, access_scope, omitted_reason, raw_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '{}'::jsonb, $10, $11::jsonb)
                """,
                [
                    (
                        item.id,
                        retrieval_run_id,
                        item.source_ref,
                        item.source_kind,
                        item.summary,
                        item.strength,
                        item.supports_answer,
                        item.is_counterevidence,
                        item.token_estimate,
                        item.omitted_reason,
                        _jsonb(item.raw_payload),
                    )
                    for item in items
                ],
            )

    async def add_answer(
        self,
        *,
        session_id: UUID,
        message_id: UUID,
        retrieval_run_id: UUID,
        payload: AskAnswerPayload,
        mode: AskMode,
        scope: AskScope,
        latency_ms: int,
    ) -> UUID:
        aid = uuid7()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ask_answers (
                  id, session_id, message_id, retrieval_run_id,
                  answer_payload, confidence, mode, scope, token_estimate, latency_ms
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8::jsonb, $9, $10)
                """,
                aid,
                session_id,
                message_id,
                retrieval_run_id,
                _jsonb(payload.model_dump(mode="json")),
                payload.confidence,
                mode,
                _jsonb(scope.model_dump(mode="json")),
                _estimate_tokens(payload.answer),
                latency_ms,
            )
        return aid

    async def update_answer_payload(
        self,
        answer_id: UUID,
        payload: AskAnswerPayload,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE ask_answers
                SET answer_payload = $2::jsonb, confidence = $3
                WHERE id = $1
                """,
                answer_id,
                _jsonb(payload.model_dump(mode="json")),
                payload.confidence,
            )

    async def add_proposed_state_change(
        self,
        *,
        tenant_id: UUID,
        answer_id: UUID,
        proposed_op: dict[str, Any],
    ) -> AskProposedStateChange:
        change_id = uuid7()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO ask_proposed_state_changes (
                  id, answer_id, tenant_id, proposed_op, status
                )
                VALUES ($1, $2, $3, $4::jsonb, 'proposed')
                RETURNING *
                """,
                change_id,
                answer_id,
                tenant_id,
                _jsonb(proposed_op),
            )
        return _change_from_row(row)

    async def list_evidence(
        self,
        retrieval_run_id: UUID,
        *,
        tenant_id: UUID | None = None,
    ) -> tuple[list[AskEvidenceItem], list[AskEvidenceItem]]:
        async with self._pool.acquire() as conn:
            if tenant_id is None:
                rows = await conn.fetch(
                    """
                    SELECT *
                    FROM ask_evidence_items
                    WHERE retrieval_run_id = $1
                    ORDER BY created_at, id
                    """,
                    retrieval_run_id,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT e.*
                    FROM ask_evidence_items e
                    JOIN ask_retrieval_runs r ON r.id = e.retrieval_run_id
                    JOIN ask_sessions s ON s.id = r.session_id
                    WHERE e.retrieval_run_id = $1
                      AND s.tenant_id = $2
                    ORDER BY e.created_at, e.id
                    """,
                    retrieval_run_id,
                    tenant_id,
                )
        items = [_evidence_from_row(row) for row in rows]
        return (
            [item for item in items if not item.omitted_reason],
            [item for item in items if item.omitted_reason],
        )

    async def act_on_proposed_change(
        self,
        *,
        tenant_id: UUID,
        change_id: UUID,
        action: str,
        note: str | None,
        delegate_to: str | None,
    ) -> AskProposedStateChange:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT *
                    FROM ask_proposed_state_changes
                    WHERE id = $1 AND tenant_id = $2
                    FOR UPDATE
                    """,
                    change_id,
                    tenant_id,
                )
                if row is None:
                    raise LookupError("proposed state change not found")
                status = {
                    "accept": "accepted",
                    "reject": "rejected",
                    "delegate": "delegated",
                    "deep_review": "delegated",
                }[action]
                trigger_id: UUID | None = row["linked_trigger_id"]
                if action in {"accept", "deep_review"} and trigger_id is None:
                    trigger_id = uuid7()
                    await conn.execute(
                        """
                        INSERT INTO think_trigger_queue (
                          id, tenant_id, trigger_kind, trigger_subkind, payload
                        )
                        VALUES ($1, $2, 'T4', 'ask_proposed_state_change', $3::jsonb)
                        """,
                        trigger_id,
                        tenant_id,
                        _jsonb({
                            "source": "ask_fyralis",
                            "proposed_state_change_id": str(change_id),
                            "action": action,
                            "note": note,
                            "delegate_to": delegate_to,
                            "proposed_op": _coerce_json(row["proposed_op"]),
                        }),
                    )
                updated = await conn.fetchrow(
                    """
                    UPDATE ask_proposed_state_changes
                    SET status = $3, linked_trigger_id = $4
                    WHERE id = $1 AND tenant_id = $2
                    RETURNING *
                    """,
                    change_id,
                    tenant_id,
                    status,
                    trigger_id,
                )
        return _change_from_row(updated)

    async def add_feedback(
        self,
        *,
        session_id: UUID,
        answer_id: UUID | None,
        viewer_id: UUID,
        feedback_type: str,
        payload: dict[str, Any],
    ) -> UUID:
        fid = uuid7()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO ask_feedback (
                      id, session_id, answer_id, viewer_id, feedback_type, payload
                    )
                    VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                    """,
                    fid,
                    session_id,
                    answer_id,
                    viewer_id,
                    feedback_type,
                    _jsonb(payload),
                )
                if feedback_type == "helpful" and answer_id is not None:
                    await _enqueue_accepted_answer_writeback(
                        conn,
                        feedback_id=fid,
                        session_id=session_id,
                        answer_id=answer_id,
                        viewer_id=viewer_id,
                        payload=payload,
                    )
        return fid


def _session_from_row(row: asyncpg.Record) -> AskSession:
    return AskSession(
        id=row["id"],
        tenant_id=row["tenant_id"],
        viewer_id=row["viewer_id"],
        initial_scope=AskScope.model_validate(_coerce_json(row["initial_scope"])),
        current_scope=AskScope.model_validate(_coerce_json(row["current_scope"])),
        source_route=row["source_route"],
        source_object_id=row["source_object_id"],
        source_object_type=row["source_object_type"],
        mode=row["mode"],
        status=row["status"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _change_from_row(row: asyncpg.Record) -> AskProposedStateChange:
    return AskProposedStateChange(
        id=row["id"],
        answer_id=row["answer_id"],
        proposed_op=_coerce_json(row["proposed_op"]),
        status=row["status"],
        linked_trigger_id=row["linked_trigger_id"],
    )


def _evidence_from_row(row: asyncpg.Record) -> AskEvidenceItem:
    return AskEvidenceItem(
        id=row["id"],
        source_ref=row["source_ref"],
        source_kind=row["source_kind"],
        summary=row["summary"],
        strength=row["strength"] or "contextual",
        supports_answer=row["supports_answer"],
        is_counterevidence=row["is_counterevidence"],
        token_estimate=row["token_estimate"],
        omitted_reason=row["omitted_reason"],
        raw_payload=_coerce_json(row["raw_payload"]),
    )


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _coerce_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _estimate_tokens(text: str) -> int:
    return max(1, len(text or "") // 4)


class InMemoryAskStore:
    """Small test adapter with the same semantics as PostgresAskStore."""

    def __init__(self) -> None:
        self.sessions: dict[UUID, AskSession] = {}
        self.evidence: dict[UUID, list[AskEvidenceItem]] = {}
        self.retrieval_runs: dict[UUID, dict[str, Any]] = {}
        self.changes: dict[UUID, AskProposedStateChange] = {}
        self.feedback: list[dict[str, Any]] = []
        self.accepted_answer_writebacks: list[dict[str, Any]] = []

    async def create_session(
        self,
        *,
        tenant_id: UUID,
        viewer_id: UUID,
        scope: AskScope,
        source_route: str | None,
        source_object_id: UUID | None,
        source_object_type: str | None,
        mode: AskMode,
    ) -> AskSession:
        now = datetime.now(timezone.utc)
        session = AskSession(
            id=uuid7(),
            tenant_id=tenant_id,
            viewer_id=viewer_id,
            initial_scope=scope,
            current_scope=scope,
            source_route=source_route,
            source_object_id=source_object_id,
            source_object_type=source_object_type,
            mode=mode,
            status="open",
            created_at=now,
            updated_at=now,
        )
        self.sessions[session.id] = session
        return session

    async def get_session(self, session_id: UUID, *, tenant_id: UUID) -> AskSession | None:
        session = self.sessions.get(session_id)
        if session and session.tenant_id == tenant_id:
            return session
        return None

    async def update_scope(self, session_id: UUID, *, scope: AskScope) -> None:
        session = self.sessions[session_id]
        self.sessions[session_id] = session.model_copy(
            update={"current_scope": scope, "updated_at": datetime.now(timezone.utc)}
        )

    async def add_message(
        self,
        *,
        session_id: UUID,
        role: str,
        content: str,
        structured_payload: dict[str, Any] | None = None,
    ) -> UUID:
        return uuid7()

    async def add_retrieval_run(
        self,
        *,
        session_id: UUID,
        message_id: UUID,
        intent: str,
        retrieval_plan: dict[str, Any],
        mode: AskMode,
        status: str,
        latency_ms: int | None,
    ) -> UUID:
        rid = uuid7()
        self.evidence[rid] = []
        self.retrieval_runs[rid] = {
            "session_id": session_id,
            "message_id": message_id,
            "intent": intent,
            "retrieval_plan": retrieval_plan,
            "mode": mode,
            "status": status,
            "latency_ms": latency_ms,
        }
        return rid

    async def update_retrieval_run_status(
        self,
        retrieval_run_id: UUID,
        *,
        status: str,
        latency_ms: int | None = None,
        error: str | None = None,
    ) -> None:
        run = self.retrieval_runs.setdefault(retrieval_run_id, {})
        run["status"] = status
        if latency_ms is not None:
            run["latency_ms"] = latency_ms
        if error:
            plan = dict(run.get("retrieval_plan") or {})
            plan["error"] = error
            run["retrieval_plan"] = plan

    async def add_evidence_items(
        self,
        retrieval_run_id: UUID,
        items: list[AskEvidenceItem],
    ) -> None:
        self.evidence.setdefault(retrieval_run_id, []).extend(items)

    async def add_answer(
        self,
        *,
        session_id: UUID,
        message_id: UUID,
        retrieval_run_id: UUID,
        payload: AskAnswerPayload,
        mode: AskMode,
        scope: AskScope,
        latency_ms: int,
    ) -> UUID:
        return uuid7()

    async def update_answer_payload(
        self,
        answer_id: UUID,
        payload: AskAnswerPayload,
    ) -> None:
        return None

    async def add_proposed_state_change(
        self,
        *,
        tenant_id: UUID,
        answer_id: UUID,
        proposed_op: dict[str, Any],
    ) -> AskProposedStateChange:
        change = AskProposedStateChange(
            id=uuid7(),
            answer_id=answer_id,
            proposed_op=proposed_op,
            status="proposed",
        )
        self.changes[change.id] = change
        return change

    async def list_evidence(
        self,
        retrieval_run_id: UUID,
        *,
        tenant_id: UUID | None = None,
    ) -> tuple[list[AskEvidenceItem], list[AskEvidenceItem]]:
        del tenant_id
        items = self.evidence.get(retrieval_run_id, [])
        return (
            [item for item in items if not item.omitted_reason],
            [item for item in items if item.omitted_reason],
        )

    async def act_on_proposed_change(
        self,
        *,
        tenant_id: UUID,
        change_id: UUID,
        action: str,
        note: str | None,
        delegate_to: str | None,
    ) -> AskProposedStateChange:
        change = self.changes[change_id]
        status = {
            "accept": "accepted",
            "reject": "rejected",
            "delegate": "delegated",
            "deep_review": "delegated",
        }[action]
        linked = uuid7() if action in {"accept", "deep_review"} else None
        updated = change.model_copy(update={"status": status, "linked_trigger_id": linked})
        self.changes[change_id] = updated
        return updated

    async def add_feedback(
        self,
        *,
        session_id: UUID,
        answer_id: UUID | None,
        viewer_id: UUID,
        feedback_type: str,
        payload: dict[str, Any],
    ) -> UUID:
        fid = uuid7()
        self.feedback.append({
            "id": fid,
            "session_id": session_id,
            "answer_id": answer_id,
            "viewer_id": viewer_id,
            "feedback_type": feedback_type,
            "payload": payload,
        })
        if feedback_type == "helpful" and answer_id is not None:
            self.accepted_answer_writebacks.append({
                "feedback_id": fid,
                "session_id": session_id,
                "answer_id": answer_id,
                "viewer_id": viewer_id,
                "confidence_cap": 0.72,
                "falsifier_required": True,
            })
        return fid


async def _enqueue_accepted_answer_writeback(
    conn: asyncpg.Connection,
    *,
    feedback_id: UUID,
    session_id: UUID,
    answer_id: UUID,
    viewer_id: UUID,
    payload: dict[str, Any],
) -> None:
    row = await conn.fetchrow(
        """
        SELECT
          s.tenant_id,
          s.current_scope,
          a.answer_payload,
          a.confidence,
          a.retrieval_run_id
        FROM ask_answers a
        JOIN ask_sessions s ON s.id = a.session_id
        WHERE a.id = $1
          AND a.session_id = $2
        """,
        answer_id,
        session_id,
    )
    if row is None:
        return
    evidence_rows = await conn.fetch(
        """
        SELECT source_ref, source_kind, summary, strength, supports_answer,
               is_counterevidence
        FROM ask_evidence_items
        WHERE retrieval_run_id = $1
          AND omitted_reason IS NULL
        ORDER BY supports_answer DESC, is_counterevidence ASC, created_at ASC
        LIMIT 16
        """,
        row["retrieval_run_id"],
    )
    confidence = row["confidence"]
    try:
        capped_confidence = min(float(confidence), 0.72)
    except (TypeError, ValueError):
        capped_confidence = 0.6
    trigger_id = uuid7()
    await conn.execute(
        """
        INSERT INTO think_trigger_queue (
          id, tenant_id, trigger_kind, trigger_subkind, payload
        )
        VALUES ($1, $2, 'T4', 'ask_answer_accepted', $3::jsonb)
        """,
        trigger_id,
        row["tenant_id"],
        _jsonb({
            "source": "ask_fyralis",
            "feedback_id": str(feedback_id),
            "answer_id": str(answer_id),
            "session_id": str(session_id),
            "viewer_id": str(viewer_id),
            "feedback_payload": payload,
            "answer_payload": _coerce_json(row["answer_payload"]),
            "scope": _coerce_json(row["current_scope"]),
            "confidence_cap": capped_confidence,
            "claim_role": "hypothesis",
            "falsifier_required": True,
            "writeback_instruction": (
                "If the accepted Ask answer contains a durable company-memory "
                "claim supported by the evidence, emit it as a capped-confidence "
                "hypothesis/fact with a concrete falsifier and provenance. "
                "Do not write back conversational phrasing or unsupported advice."
            ),
            "provenance": [
                {
                    "source_ref": str(r["source_ref"]) if r["source_ref"] else None,
                    "source_kind": r["source_kind"],
                    "summary": r["summary"],
                    "strength": r["strength"],
                    "supports_answer": bool(r["supports_answer"]),
                    "is_counterevidence": bool(r["is_counterevidence"]),
                }
                for r in evidence_rows
            ],
        }),
    )
