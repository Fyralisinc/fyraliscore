"""services/sage/inquiry_traces/repo.py — Phase 1 trace repos.

Three repos, one per gap-filler table from migration 0049:

  * RetrievalPlansRepo   — `retrieval_plans`
  * OmittedEvidenceRepo  — `omitted_evidence`
  * OutcomeEventsRepo    — `inquiry_outcome_events`

Each is tenant-bound at construction (`__init__(pool, *, tenant_id=…)`)
so call sites don't have to thread tenant_id through every method.
Methods accept an optional `conn=` so callers that own a transaction
(e.g. the inquiry executor batching plan + retrieval + omissions in
one shot) can keep everything atomic. When `conn` is None we acquire
from the pool for the duration of the call.

Style cribbed from services/models/repo.py + services/forecasts/repo.py:
raw asyncpg, structlog, uuid7() ids, JSONB columns serialized via
`json.dumps` and cast `::jsonb` in the SQL.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7
from services.sage.inquiry_traces.types import (
    OMISSION_REASONS,
    OUTCOME_EVENT_TYPES,
    OmittedEvidenceRow,
    OutcomeEventRow,
    RetrievalPlanRow,
)


_log = structlog.get_logger(__name__)


class SageInquiryTraceRepoError(CompanyOSError):
    default_code = "sage_inquiry_trace_repo_error"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _jsonb(value: Any) -> str:
    """asyncpg needs a JSON string when the param is cast ::jsonb."""
    return json.dumps(value, sort_keys=True, default=str)


def _coerce_jsonb_obj(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _coerce_jsonb_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [v for v in parsed if isinstance(v, dict)]
    return []


class _PoolConnMixin:
    """Internal mixin: acquire a connection if the caller didn't pass one."""

    _pool: asyncpg.Pool | None
    tenant_id: UUID

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise SageInquiryTraceRepoError(
                "repo constructed without a pool; pass conn= on every call"
            )
        return self._pool


# ---------------------------------------------------------------------
# RetrievalPlansRepo
# ---------------------------------------------------------------------


class RetrievalPlansRepo(_PoolConnMixin):
    """CRUD over `retrieval_plans`."""

    def __init__(
        self,
        pool: asyncpg.Pool | None,
        *,
        tenant_id: UUID,
    ) -> None:
        self._pool = pool
        self.tenant_id = tenant_id

    async def insert(
        self,
        plan: RetrievalPlanRow,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> RetrievalPlanRow:
        """Insert one plan row. Returns the hydrated row with id + created_at."""
        if not plan.question_id or not plan.question_id.strip():
            raise ValidationError(
                "question_id is required", field="question_id",
            )
        if plan.plan_revision < 0:
            raise ValidationError(
                "plan_revision must be >= 0", field="plan_revision",
            )

        row_id = plan.id or uuid7()

        async def _do(c: asyncpg.Connection) -> RetrievalPlanRow:
            row = await c.fetchrow(
                """
                INSERT INTO retrieval_plans (
                    id, tenant_id, inquiry_session_id, question_id,
                    plan_revision, intents, paths, budgets,
                    success_conditions, notes
                ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6::jsonb, $7::jsonb, $8::jsonb,
                    $9::jsonb, $10::jsonb
                )
                RETURNING id, tenant_id, inquiry_session_id, question_id,
                          plan_revision, intents, paths, budgets,
                          success_conditions, notes, created_at
                """,
                row_id,
                self.tenant_id,
                plan.inquiry_session_id,
                plan.question_id.strip(),
                int(plan.plan_revision),
                _jsonb(plan.intents),
                _jsonb(plan.paths),
                _jsonb(plan.budgets),
                _jsonb(plan.success_conditions),
                _jsonb(plan.notes),
            )
            if row is None:  # pragma: no cover — INSERT RETURNING always rows
                raise SageInquiryTraceRepoError(
                    "retrieval_plans INSERT returned no row",
                )
            return _row_to_plan(row)

        if conn is not None:
            return await _do(conn)
        async with self._require_pool().acquire() as owned:
            return await _do(owned)

    async def list_for_session(
        self,
        session_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> list[RetrievalPlanRow]:
        """List every plan row for a session, oldest first.

        Ordered by (question_id, plan_revision, created_at) so callers
        replaying the session see the planning sequence per question.
        """
        async def _do(c: asyncpg.Connection) -> list[RetrievalPlanRow]:
            rows = await c.fetch(
                """
                SELECT id, tenant_id, inquiry_session_id, question_id,
                       plan_revision, intents, paths, budgets,
                       success_conditions, notes, created_at
                FROM retrieval_plans
                WHERE tenant_id = $1
                  AND inquiry_session_id = $2
                ORDER BY question_id ASC, plan_revision ASC, created_at ASC
                """,
                self.tenant_id,
                session_id,
            )
            return [_row_to_plan(r) for r in rows]

        if conn is not None:
            return await _do(conn)
        async with self._require_pool().acquire() as owned:
            return await _do(owned)


def _row_to_plan(r: asyncpg.Record) -> RetrievalPlanRow:
    return RetrievalPlanRow(
        id=r["id"],
        tenant_id=r["tenant_id"],
        inquiry_session_id=r["inquiry_session_id"],
        question_id=r["question_id"],
        plan_revision=int(r["plan_revision"]),
        intents=_coerce_jsonb_list(r["intents"]),
        paths=_coerce_jsonb_list(r["paths"]),
        budgets=_coerce_jsonb_obj(r["budgets"]),
        success_conditions=_coerce_jsonb_list(r["success_conditions"]),
        notes=_coerce_jsonb_obj(r["notes"]),
        created_at=r["created_at"],
    )


# ---------------------------------------------------------------------
# OmittedEvidenceRepo
# ---------------------------------------------------------------------


class OmittedEvidenceRepo(_PoolConnMixin):
    """CRUD over `omitted_evidence`."""

    def __init__(
        self,
        pool: asyncpg.Pool | None,
        *,
        tenant_id: UUID,
    ) -> None:
        self._pool = pool
        self.tenant_id = tenant_id

    async def insert(
        self,
        item: OmittedEvidenceRow,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> OmittedEvidenceRow:
        """Insert one omission record. Returns the hydrated row."""
        if item.omission_reason not in OMISSION_REASONS:
            raise ValidationError(
                f"invalid omission_reason {item.omission_reason!r}",
                field="omission_reason",
            )
        if not item.source_type or not item.source_type.strip():
            raise ValidationError(
                "source_type is required", field="source_type",
            )
        if not item.source_ref or not item.source_ref.strip():
            raise ValidationError(
                "source_ref is required", field="source_ref",
            )

        row_id = item.id or uuid7()

        async def _do(c: asyncpg.Connection) -> OmittedEvidenceRow:
            row = await c.fetchrow(
                """
                INSERT INTO omitted_evidence (
                    id, tenant_id, inquiry_session_id, question_id,
                    source_type, source_ref, source_ref_id,
                    retrieval_paths, omission_reason, reason_detail,
                    score, metadata
                ) VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7,
                    $8::jsonb, $9, $10,
                    $11, $12::jsonb
                )
                RETURNING id, tenant_id, inquiry_session_id, question_id,
                          source_type, source_ref, source_ref_id,
                          retrieval_paths, omission_reason, reason_detail,
                          score, metadata, created_at
                """,
                row_id,
                self.tenant_id,
                item.inquiry_session_id,
                item.question_id,
                item.source_type.strip(),
                item.source_ref.strip(),
                item.source_ref_id,
                _jsonb(item.retrieval_paths),
                item.omission_reason,
                item.reason_detail,
                float(item.score),
                _jsonb(item.metadata),
            )
            if row is None:  # pragma: no cover
                raise SageInquiryTraceRepoError(
                    "omitted_evidence INSERT returned no row",
                )
            return _row_to_omission(row)

        if conn is not None:
            return await _do(conn)
        async with self._require_pool().acquire() as owned:
            return await _do(owned)

    async def list_for_session(
        self,
        session_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> list[OmittedEvidenceRow]:
        """List every omission record for a session, oldest first."""

        async def _do(c: asyncpg.Connection) -> list[OmittedEvidenceRow]:
            rows = await c.fetch(
                """
                SELECT id, tenant_id, inquiry_session_id, question_id,
                       source_type, source_ref, source_ref_id,
                       retrieval_paths, omission_reason, reason_detail,
                       score, metadata, created_at
                FROM omitted_evidence
                WHERE tenant_id = $1
                  AND inquiry_session_id = $2
                ORDER BY created_at ASC, id ASC
                """,
                self.tenant_id,
                session_id,
            )
            return [_row_to_omission(r) for r in rows]

        if conn is not None:
            return await _do(conn)
        async with self._require_pool().acquire() as owned:
            return await _do(owned)


def _row_to_omission(r: asyncpg.Record) -> OmittedEvidenceRow:
    return OmittedEvidenceRow(
        id=r["id"],
        tenant_id=r["tenant_id"],
        inquiry_session_id=r["inquiry_session_id"],
        question_id=r["question_id"],
        source_type=r["source_type"],
        source_ref=r["source_ref"],
        source_ref_id=r["source_ref_id"],
        retrieval_paths=_coerce_jsonb_list(r["retrieval_paths"]),
        omission_reason=r["omission_reason"],
        reason_detail=r["reason_detail"],
        score=float(r["score"]) if r["score"] is not None else 0.0,
        metadata=_coerce_jsonb_obj(r["metadata"]),
        created_at=r["created_at"],
    )


# ---------------------------------------------------------------------
# OutcomeEventsRepo
# ---------------------------------------------------------------------


class OutcomeEventsRepo(_PoolConnMixin):
    """Append-only event log over `inquiry_outcome_events`."""

    def __init__(
        self,
        pool: asyncpg.Pool | None,
        *,
        tenant_id: UUID,
    ) -> None:
        self._pool = pool
        self.tenant_id = tenant_id

    async def append(
        self,
        session_id: UUID,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> OutcomeEventRow:
        """Append one outcome event. Returns the hydrated row.

        Raises `ValidationError` if `event_type` is not in
        `OUTCOME_EVENT_TYPES`. The CHECK on the SQL side is the second
        line of defense.
        """
        if event_type not in OUTCOME_EVENT_TYPES:
            raise ValidationError(
                f"invalid event_type {event_type!r}", field="event_type",
            )
        payload_dict = payload if isinstance(payload, dict) else {}
        row_id = uuid7()

        async def _do(c: asyncpg.Connection) -> OutcomeEventRow:
            row = await c.fetchrow(
                """
                INSERT INTO inquiry_outcome_events (
                    id, tenant_id, inquiry_session_id,
                    event_type, payload
                ) VALUES (
                    $1, $2, $3, $4, $5::jsonb
                )
                RETURNING id, tenant_id, inquiry_session_id,
                          event_type, payload, created_at
                """,
                row_id,
                self.tenant_id,
                session_id,
                event_type,
                _jsonb(payload_dict),
            )
            if row is None:  # pragma: no cover
                raise SageInquiryTraceRepoError(
                    "inquiry_outcome_events INSERT returned no row",
                )
            return _row_to_event(row)

        if conn is not None:
            return await _do(conn)
        async with self._require_pool().acquire() as owned:
            return await _do(owned)

    async def list_for_session(
        self,
        session_id: UUID,
        event_type: str | None = None,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> list[OutcomeEventRow]:
        """List events for a session, oldest first.

        When `event_type` is provided we filter to just that type so the
        topology optimizer can stream e.g. `node_used_in_valid_diff`
        events without a full table scan.
        """
        if event_type is not None and event_type not in OUTCOME_EVENT_TYPES:
            raise ValidationError(
                f"invalid event_type filter {event_type!r}",
                field="event_type",
            )

        async def _do(c: asyncpg.Connection) -> list[OutcomeEventRow]:
            if event_type is None:
                rows = await c.fetch(
                    """
                    SELECT id, tenant_id, inquiry_session_id,
                           event_type, payload, created_at
                    FROM inquiry_outcome_events
                    WHERE tenant_id = $1
                      AND inquiry_session_id = $2
                    ORDER BY created_at ASC, id ASC
                    """,
                    self.tenant_id,
                    session_id,
                )
            else:
                rows = await c.fetch(
                    """
                    SELECT id, tenant_id, inquiry_session_id,
                           event_type, payload, created_at
                    FROM inquiry_outcome_events
                    WHERE tenant_id = $1
                      AND inquiry_session_id = $2
                      AND event_type = $3
                    ORDER BY created_at ASC, id ASC
                    """,
                    self.tenant_id,
                    session_id,
                    event_type,
                )
            return [_row_to_event(r) for r in rows]

        if conn is not None:
            return await _do(conn)
        async with self._require_pool().acquire() as owned:
            return await _do(owned)

    async def aggregate_by_type(
        self,
        session_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> dict[str, int]:
        """Return `{event_type: count}` for a session.

        Event types with zero rows are absent from the dict (callers can
        `.get(name, 0)` if they need a dense map).
        """

        async def _do(c: asyncpg.Connection) -> dict[str, int]:
            rows = await c.fetch(
                """
                SELECT event_type, COUNT(*) AS n
                FROM inquiry_outcome_events
                WHERE tenant_id = $1
                  AND inquiry_session_id = $2
                GROUP BY event_type
                """,
                self.tenant_id,
                session_id,
            )
            return {r["event_type"]: int(r["n"]) for r in rows}

        if conn is not None:
            return await _do(conn)
        async with self._require_pool().acquire() as owned:
            return await _do(owned)


def _row_to_event(r: asyncpg.Record) -> OutcomeEventRow:
    return OutcomeEventRow(
        id=r["id"],
        tenant_id=r["tenant_id"],
        inquiry_session_id=r["inquiry_session_id"],
        event_type=r["event_type"],
        payload=_coerce_jsonb_obj(r["payload"]),
        created_at=r["created_at"],
    )


__all__ = [
    "OmittedEvidenceRepo",
    "OutcomeEventsRepo",
    "RetrievalPlansRepo",
    "SageInquiryTraceRepoError",
]
