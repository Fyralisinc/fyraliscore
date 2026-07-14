"""Open-question facets for Model-layer unresolved uncertainty."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any, Literal, Sequence
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from lib.shared.ids import uuid7


OpenQuestionStatus = Literal[
    "open",
    "resolved",
    "stale",
    "superseded",
    "duplicate",
    "archived",
]

OPEN_QUESTION_STATUSES: frozenset[str] = frozenset(
    {
        "open",
        "resolved",
        "stale",
        "superseded",
        "duplicate",
        "archived",
    }
)

OPEN_QUESTION_TYPES: frozenset[str] = frozenset(
    {
        "evidence_gap",
        "temporal_status",
        "causal_mechanism",
        "constraint_boundary",
        "owner_or_decision",
        "impact_scope",
        "contradiction_check",
        "projection_gap",
        "other",
    }
)


class ModelOpenQuestionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID | None = None
    tenant_id: UUID
    model_id: UUID
    question: str
    question_type: str = "evidence_gap"
    rationale: str | None = None
    priority: float = Field(default=0.5, ge=0.0, le=1.0)
    expected_resolution_signal: dict[str, Any] = Field(default_factory=dict)
    search_signature: dict[str, Any] = Field(default_factory=dict)
    source_event_id: UUID | None = None
    source_model_ids: list[UUID] = Field(default_factory=list)
    next_search_at: datetime | None = None


class ModelOpenQuestionRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    tenant_id: UUID
    model_id: UUID
    question: str
    question_type: str
    rationale: str | None = None
    priority: float
    status: OpenQuestionStatus
    expected_resolution_signal: dict[str, Any] = Field(default_factory=dict)
    search_signature: dict[str, Any] = Field(default_factory=dict)
    source_event_id: UUID | None = None
    source_model_ids: list[UUID] = Field(default_factory=list)
    dedupe_key: str
    created_at: datetime
    updated_at: datetime
    last_searched_at: datetime | None = None
    next_search_at: datetime | None = None
    resolved_at: datetime | None = None
    resolution_model_id: UUID | None = None
    resolution_note: str | None = None


_SPACE_RE = re.compile(r"\s+")
_NON_WORD_RE = re.compile(r"[^a-z0-9?]+")


def _jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, default=str)


def _json_load(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return fallback


def normalize_question_type(value: str | None) -> str:
    text = str(value or "evidence_gap").strip().lower()
    text = re.sub(r"[^a-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        return "evidence_gap"
    if text not in OPEN_QUESTION_TYPES:
        return "other"
    return text


def normalize_question_text(value: str | None) -> str:
    text = _SPACE_RE.sub(" ", str(value or "").strip())
    return text[:1000]


def dedupe_key_for_question(question: str) -> str:
    text = normalize_question_text(question).lower()
    text = _NON_WORD_RE.sub(" ", text)
    text = _SPACE_RE.sub(" ", text).strip()
    return text[:220]


def _coerce_uuid_list(values: Any) -> list[UUID]:
    if not isinstance(values, (list, tuple)):
        return []
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        try:
            uid = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError):
            continue
        if uid not in seen:
            seen.add(uid)
            out.append(uid)
    return out


def _hydrate(row: asyncpg.Record | dict[str, Any]) -> ModelOpenQuestionRow:
    raw = dict(row)
    raw["expected_resolution_signal"] = _json_load(
        raw.get("expected_resolution_signal"),
        {},
    )
    raw["search_signature"] = _json_load(raw.get("search_signature"), {})
    raw["source_model_ids"] = _coerce_uuid_list(raw.get("source_model_ids"))
    return ModelOpenQuestionRow.model_validate(raw)


class ModelOpenQuestionsRepo:
    """Repository for unresolved question facets attached to Models."""

    async def insert(
        self,
        conn: asyncpg.Connection,
        proposed: ModelOpenQuestionCreate,
    ) -> ModelOpenQuestionRow:
        question = normalize_question_text(proposed.question)
        if not question:
            raise ValueError("open question requires question text")
        question_type = normalize_question_type(proposed.question_type)
        dedupe_key = dedupe_key_for_question(question)
        source_model_ids = _coerce_uuid_list([proposed.model_id, *proposed.source_model_ids])
        row_id = proposed.id or uuid7()
        row = await conn.fetchrow(
            """
            INSERT INTO model_open_questions (
              id, tenant_id, model_id, question, question_type, rationale,
              priority, expected_resolution_signal, search_signature,
              source_event_id, source_model_ids, dedupe_key, next_search_at
            ) VALUES (
              $1, $2, $3, $4, $5, $6,
              $7, $8::jsonb, $9::jsonb,
              $10, $11::uuid[], $12, $13
            )
            ON CONFLICT DO NOTHING
            RETURNING *
            """,
            row_id,
            proposed.tenant_id,
            proposed.model_id,
            question,
            question_type,
            proposed.rationale,
            float(proposed.priority),
            _jsonb(proposed.expected_resolution_signal),
            _jsonb(proposed.search_signature),
            proposed.source_event_id,
            source_model_ids,
            dedupe_key,
            proposed.next_search_at,
        )
        if row is not None:
            return _hydrate(row)
        existing = await conn.fetchrow(
            """
            SELECT *
            FROM model_open_questions
            WHERE tenant_id = $1
              AND model_id = $2
              AND question_type = $3
              AND dedupe_key = $4
              AND status = 'open'
            ORDER BY created_at ASC
            LIMIT 1
            """,
            proposed.tenant_id,
            proposed.model_id,
            question_type,
            dedupe_key,
        )
        if existing is None:
            raise ValueError("open question insert conflicted but no existing row found")
        return _hydrate(existing)

    async def insert_many(
        self,
        conn: asyncpg.Connection,
        questions: Sequence[ModelOpenQuestionCreate],
    ) -> list[ModelOpenQuestionRow]:
        rows: list[ModelOpenQuestionRow] = []
        for question in questions:
            rows.append(await self.insert(conn, question))
        return rows

    async def list_for_model(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        model_id: UUID,
        statuses: Sequence[str] = ("open",),
        limit: int = 100,
    ) -> list[ModelOpenQuestionRow]:
        rows = await conn.fetch(
            """
            SELECT *
            FROM model_open_questions
            WHERE tenant_id = $1
              AND model_id = $2
              AND status = ANY($3::text[])
            ORDER BY priority DESC, created_at ASC
            LIMIT $4
            """,
            tenant_id,
            model_id,
            [str(status) for status in statuses],
            max(1, int(limit)),
        )
        return [_hydrate(row) for row in rows]

    async def list_due_for_search(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID | None = None,
        question_ids: Sequence[UUID] | None = None,
        model_ids: Sequence[UUID] | None = None,
        limit: int = 50,
    ) -> list[ModelOpenQuestionRow]:
        params: list[Any] = [max(1, int(limit))]
        tenant_sql = ""
        question_sql = ""
        model_sql = ""
        if tenant_id is not None:
            params.append(tenant_id)
            tenant_sql = f"AND tenant_id = ${len(params)}\n"
        question_list = list(question_ids or ())
        if question_list:
            params.append(question_list)
            question_sql = f"AND id = ANY(${len(params)}::uuid[])\n"
        model_list = list(model_ids or ())
        if model_list:
            params.append(model_list)
            model_sql = f"AND model_id = ANY(${len(params)}::uuid[])\n"
        rows = await conn.fetch(
            f"""
            SELECT *
            FROM model_open_questions
            WHERE status = 'open'
              AND (next_search_at IS NULL OR next_search_at <= now())
              {tenant_sql}
              {question_sql}
              {model_sql}
            ORDER BY priority DESC, created_at ASC
            LIMIT $1
            """,
            *params,
        )
        return [_hydrate(row) for row in rows]

    async def mark_searched(
        self,
        conn: asyncpg.Connection,
        *,
        question_ids: Sequence[UUID],
        backoff: timedelta | None = None,
    ) -> int:
        ids = list(dict.fromkeys(question_ids))
        if not ids:
            return 0
        interval_seconds = int((backoff or timedelta(hours=6)).total_seconds())
        tag = await conn.execute(
            """
            UPDATE model_open_questions
            SET last_searched_at = now(),
                next_search_at = now() + ($2 || ' seconds')::interval,
                updated_at = now()
            WHERE id = ANY($1::uuid[])
              AND status = 'open'
            """,
            ids,
            str(max(60, interval_seconds)),
        )
        return _rows_affected(tag)

    async def resolve(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        question_id: UUID,
        resolution_model_id: UUID | None = None,
        resolution_note: str | None = None,
        status: OpenQuestionStatus = "resolved",
    ) -> ModelOpenQuestionRow | None:
        if status not in OPEN_QUESTION_STATUSES or status == "open":
            raise ValueError(f"invalid terminal open question status: {status}")
        row = await conn.fetchrow(
            """
            UPDATE model_open_questions
            SET status = $3,
                resolved_at = COALESCE(resolved_at, now()),
                resolution_model_id = $4,
                resolution_note = $5,
                updated_at = now()
            WHERE tenant_id = $1
              AND id = $2
            RETURNING *
            """,
            tenant_id,
            question_id,
            status,
            resolution_model_id,
            resolution_note,
        )
        return _hydrate(row) if row is not None else None

    async def archive_for_model(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        model_id: UUID,
        status: OpenQuestionStatus = "archived",
        resolution_note: str | None = None,
    ) -> int:
        if status == "open" or status not in OPEN_QUESTION_STATUSES:
            raise ValueError(f"invalid archive status: {status}")
        tag = await conn.execute(
            """
            UPDATE model_open_questions
            SET status = $3,
                resolved_at = COALESCE(resolved_at, now()),
                resolution_note = COALESCE($4, resolution_note),
                updated_at = now()
            WHERE tenant_id = $1
              AND model_id = $2
              AND status = 'open'
            """,
            tenant_id,
            model_id,
            status,
            resolution_note,
        )
        return _rows_affected(tag)


def _rows_affected(command_tag: str) -> int:
    try:
        return int(str(command_tag).split()[-1])
    except (ValueError, IndexError):
        return 0


__all__ = [
    "ModelOpenQuestionCreate",
    "ModelOpenQuestionRow",
    "ModelOpenQuestionsRepo",
    "OPEN_QUESTION_STATUSES",
    "OPEN_QUESTION_TYPES",
    "OpenQuestionStatus",
    "dedupe_key_for_question",
    "normalize_question_text",
    "normalize_question_type",
]
