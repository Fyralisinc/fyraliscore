"""User-facing clarification requests for ambiguous system decisions."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7


_VALID_PRIORITIES = frozenset({"low", "normal", "high", "critical"})
_VALID_STATUSES = frozenset({"open", "answered", "dismissed", "expired", "superseded"})


def _jsonb(value: Any, *, default: Any) -> str:
    return json.dumps(value if value is not None else default, default=str)


def _coerce_json(value: Any, *, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return default


@dataclass(frozen=True, slots=True)
class ClarificationRequest:
    id: UUID
    tenant_id: UUID
    kind: str
    status: str
    priority: str
    question: str
    explanation: str
    object_kind: str
    object_id: UUID | None
    object_key: str | None
    source_observation_id: UUID | None
    model_id: UUID | None
    options: list[dict[str, Any]]
    payload: dict[str, Any]
    answer: dict[str, Any] | None
    answered_by: UUID | None
    answered_at: datetime | None
    dismissed_reason: str | None
    expires_at: datetime | None
    created_at: datetime
    updated_at: datetime

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "tenant_id": str(self.tenant_id),
            "kind": self.kind,
            "status": self.status,
            "priority": self.priority,
            "question": self.question,
            "explanation": self.explanation,
            "object_kind": self.object_kind,
            "object_id": str(self.object_id) if self.object_id else None,
            "object_key": self.object_key,
            "source_observation_id": (
                str(self.source_observation_id) if self.source_observation_id else None
            ),
            "model_id": str(self.model_id) if self.model_id else None,
            "options": self.options,
            "payload": self.payload,
            "answer": self.answer,
            "answered_by": str(self.answered_by) if self.answered_by else None,
            "answered_at": self.answered_at.isoformat() if self.answered_at else None,
            "dismissed_reason": self.dismissed_reason,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


async def open_clarification_request(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    kind: str,
    question: str,
    object_kind: str,
    object_id: UUID | None = None,
    object_key: str | None = None,
    priority: str = "normal",
    explanation: str = "",
    source_observation_id: UUID | None = None,
    model_id: UUID | None = None,
    options: list[dict[str, Any]] | None = None,
    payload: dict[str, Any] | None = None,
    expires_at: datetime | None = None,
    request_id: UUID | None = None,
) -> UUID:
    """Open or return one active clarification for the same object.

    Clarifications are deliberately bounded questions. They are for cases where
    the system should neither silently guess nor throw away the ambiguity.
    """

    kind = str(kind or "").strip()
    question = str(question or "").strip()
    object_kind = str(object_kind or "").strip()
    object_key = str(object_key).strip() if object_key is not None else None
    if object_key == "":
        object_key = None
    priority = str(priority or "normal").strip().lower()
    if not kind:
        raise ValidationError("clarification kind is required", field="kind")
    if not question:
        raise ValidationError("clarification question is required", field="question")
    if not object_kind:
        raise ValidationError("clarification object_kind is required", field="object_kind")
    if priority not in _VALID_PRIORITIES:
        raise ValidationError(
            "clarification priority is invalid",
            field="priority",
            value=priority,
        )

    new_id = request_id or uuid7()
    inserted = await conn.fetchval(
        """
        INSERT INTO clarification_requests (
          id, tenant_id, kind, priority, question, explanation,
          object_kind, object_id, object_key, source_observation_id, model_id,
          options, payload, expires_at
        )
        VALUES (
          $1, $2, $3, $4, $5, $6,
          $7, $8, $9, $10, $11,
          $12::jsonb, $13::jsonb, $14
        )
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        new_id,
        tenant_id,
        kind,
        priority,
        question,
        explanation,
        object_kind,
        object_id,
        object_key,
        source_observation_id,
        model_id,
        _jsonb(options, default=[]),
        _jsonb(payload, default={}),
        expires_at,
    )
    if inserted is not None:
        return inserted

    existing = await conn.fetchval(
        """
        SELECT id
        FROM clarification_requests
        WHERE tenant_id = $1
          AND kind = $2
          AND object_kind = $3
          AND status = 'open'
          AND (object_key IS NOT NULL OR object_id IS NOT NULL)
          AND COALESCE(object_key, object_id::text) =
              COALESCE($5::text, $4::uuid::text)
        ORDER BY created_at DESC
        LIMIT 1
        """,
        tenant_id,
        kind,
        object_kind,
        object_id,
        object_key,
    )
    return existing or new_id


async def list_clarification_requests(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    status: str = "open",
    limit: int = 50,
) -> list[ClarificationRequest]:
    status = str(status or "open").strip().lower()
    if status not in _VALID_STATUSES and status != "all":
        raise ValidationError(
            "clarification status is invalid",
            field="status",
            value=status,
        )
    limit = max(1, min(200, int(limit)))
    if status == "all":
        rows = await conn.fetch(
            """
            SELECT *
            FROM clarification_requests
            WHERE tenant_id = $1
            ORDER BY
              CASE priority
                WHEN 'critical' THEN 0
                WHEN 'high' THEN 1
                WHEN 'normal' THEN 2
                ELSE 3
              END,
              created_at DESC
            LIMIT $2
            """,
            tenant_id,
            limit,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT *
            FROM clarification_requests
            WHERE tenant_id = $1 AND status = $2
            ORDER BY
              CASE priority
                WHEN 'critical' THEN 0
                WHEN 'high' THEN 1
                WHEN 'normal' THEN 2
                ELSE 3
              END,
              created_at DESC
            LIMIT $3
            """,
            tenant_id,
            status,
            limit,
        )
    return [_hydrate(row) for row in rows]


async def get_clarification_request(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    request_id: UUID,
) -> ClarificationRequest | None:
    row = await conn.fetchrow(
        """
        SELECT *
        FROM clarification_requests
        WHERE id = $1 AND tenant_id = $2
        """,
        request_id,
        tenant_id,
    )
    return _hydrate(row) if row is not None else None


async def answer_clarification_request(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    request_id: UUID,
    answer: dict[str, Any],
    answered_by: UUID | None = None,
) -> ClarificationRequest | None:
    if not isinstance(answer, dict) or not answer:
        raise ValidationError("clarification answer must be a non-empty object")
    row = await conn.fetchrow(
        """
        UPDATE clarification_requests
        SET status = 'answered',
            answer = $3::jsonb,
            answered_by = $4,
            answered_at = now(),
            updated_at = now()
        WHERE id = $1
          AND tenant_id = $2
          AND status = 'open'
        RETURNING *
        """,
        request_id,
        tenant_id,
        _jsonb(answer, default={}),
        answered_by,
    )
    return _hydrate(row) if row is not None else None


async def dismiss_clarification_request(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    request_id: UUID,
    reason: str,
    answered_by: UUID | None = None,
) -> ClarificationRequest | None:
    row = await conn.fetchrow(
        """
        UPDATE clarification_requests
        SET status = 'dismissed',
            dismissed_reason = $3,
            answered_by = $4,
            answered_at = now(),
            updated_at = now()
        WHERE id = $1
          AND tenant_id = $2
          AND status = 'open'
        RETURNING *
        """,
        request_id,
        tenant_id,
        str(reason or "dismissed"),
        answered_by,
    )
    return _hydrate(row) if row is not None else None


def _hydrate(row: asyncpg.Record) -> ClarificationRequest:
    return ClarificationRequest(
        id=row["id"],
        tenant_id=row["tenant_id"],
        kind=row["kind"],
        status=row["status"],
        priority=row["priority"],
        question=row["question"],
        explanation=row["explanation"],
        object_kind=row["object_kind"],
        object_id=row["object_id"],
        object_key=row["object_key"],
        source_observation_id=row["source_observation_id"],
        model_id=row["model_id"],
        options=list(_coerce_json(row["options"], default=[])),
        payload=dict(_coerce_json(row["payload"], default={})),
        answer=(
            dict(_coerce_json(row["answer"], default={}))
            if row["answer"] is not None
            else None
        ),
        answered_by=row["answered_by"],
        answered_at=row["answered_at"],
        dismissed_reason=row["dismissed_reason"],
        expires_at=row["expires_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


__all__ = [
    "ClarificationRequest",
    "answer_clarification_request",
    "dismiss_clarification_request",
    "get_clarification_request",
    "list_clarification_requests",
    "open_clarification_request",
]
