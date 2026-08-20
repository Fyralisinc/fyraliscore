"""Persistent residual evidence for model-metabolism compression debt.

This surface is deliberately not canonical truth. It records small, source-backed
residual obligations when valuable signal content has not yet been absorbed into
models, readings, edges, relation frames, projections, or a justified ignore.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7

RESIDUAL_KINDS = {
    "valuable_unmodeled",
    "counterevidence_unattached",
    "relation_unanchored",
    "open_question_needed",
    "validation_dropped_value",
    "authority_blocked",
    "compression_uncertain",
}
RESIDUAL_STATUSES = {"open", "absorbed", "rejected", "expired"}
ABSORPTION_OBJECT_KINDS = {
    "model",
    "model_signal_reading",
    "model_edge",
    "relation_claim",
    "relation_instance",
    "model_open_question",
    "projection_snapshot",
    "inquiry_outcome_event",
    "clarification_request",
}

_COLS = (
    "id",
    "tenant_id",
    "source_observation_id",
    "think_run_id",
    "trigger_id",
    "model_id",
    "residual_kind",
    "compact_summary",
    "reason",
    "status",
    "absorption_object_kind",
    "absorption_object_id",
    "metadata",
    "created_at",
    "updated_at",
    "resolved_at",
)
_COLS_SQL = ", ".join(_COLS)


@dataclass(frozen=True)
class ModelResidualEvidence:
    tenant_id: UUID
    residual_kind: str
    compact_summary: str
    reason: str
    id: UUID | None = None
    source_observation_id: UUID | None = None
    think_run_id: UUID | None = None
    trigger_id: UUID | None = None
    model_id: UUID | None = None
    status: str = "open"
    absorption_object_kind: str | None = None
    absorption_object_id: UUID | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    resolved_at: datetime | None = None


class ModelResidualEvidenceRepo:
    """Tenant-bound repository for `model_residual_evidence`."""

    def __init__(
        self,
        pool: asyncpg.Pool | None = None,
        *,
        tenant_id: UUID,
    ) -> None:
        self._pool = pool
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID:
        return self._tenant_id

    async def insert_open(
        self,
        residual: ModelResidualEvidence,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelResidualEvidence:
        """Insert or return the existing open residual for this source/kind/reason."""

        self._validate_residual(residual, require_open=True)
        return await self._with_conn(conn, lambda c: self._insert_open(c, residual))

    async def list_open(
        self,
        *,
        limit: int = 100,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelResidualEvidence]:
        if limit < 1:
            raise ValueError("limit must be positive")
        return await self._with_conn(conn, lambda c: self._list_open(c, limit))

    async def list_for_observations(
        self,
        observation_ids: list[UUID],
        *,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelResidualEvidence]:
        if not observation_ids:
            return []
        unique_ids = sorted(set(observation_ids), key=str)
        return await self._with_conn(
            conn,
            lambda c: self._list_for_observations(c, unique_ids),
        )

    async def absorb(
        self,
        residual_id: UUID,
        *,
        object_kind: str,
        object_id: UUID,
        metadata: dict[str, Any] | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> ModelResidualEvidence | None:
        if object_kind not in ABSORPTION_OBJECT_KINDS:
            raise ValueError(f"invalid absorption object kind: {object_kind}")
        return await self._resolve(
            residual_id,
            status="absorbed",
            object_kind=object_kind,
            object_id=object_id,
            metadata=metadata,
            conn=conn,
        )

    async def reject(
        self,
        residual_id: UUID,
        *,
        reason: str,
        conn: asyncpg.Connection | None = None,
    ) -> ModelResidualEvidence | None:
        return await self._resolve(
            residual_id,
            status="rejected",
            metadata={"resolution_reason": reason},
            conn=conn,
        )

    async def expire(
        self,
        residual_id: UUID,
        *,
        reason: str,
        conn: asyncpg.Connection | None = None,
    ) -> ModelResidualEvidence | None:
        return await self._resolve(
            residual_id,
            status="expired",
            metadata={"resolution_reason": reason},
            conn=conn,
        )

    async def _insert_open(
        self,
        conn: asyncpg.Connection,
        residual: ModelResidualEvidence,
    ) -> ModelResidualEvidence:
        row_id = residual.id or uuid7()
        row = await conn.fetchrow(
            f"""
            INSERT INTO model_residual_evidence (
                id, tenant_id, source_observation_id, think_run_id, trigger_id,
                model_id, residual_kind, compact_summary, reason, status,
                metadata
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8, $9, 'open',
                $10::jsonb
            )
            ON CONFLICT DO NOTHING
            RETURNING {_COLS_SQL}
            """,
            row_id,
            self._tenant_id,
            residual.source_observation_id,
            residual.think_run_id,
            residual.trigger_id,
            residual.model_id,
            residual.residual_kind,
            residual.compact_summary,
            residual.reason,
            _jsonb(residual.metadata),
        )
        if row is not None:
            return _hydrate(row)
        existing = await conn.fetchrow(
            f"""
            SELECT {_COLS_SQL}
            FROM model_residual_evidence
            WHERE tenant_id = $1
              AND source_observation_id IS NOT DISTINCT FROM $2
              AND residual_kind = $3
              AND md5(reason) = md5($4)
              AND status = 'open'
            ORDER BY created_at ASC
            LIMIT 1
            """,
            self._tenant_id,
            residual.source_observation_id,
            residual.residual_kind,
            residual.reason,
        )
        if existing is None:
            raise RuntimeError("open residual insert conflicted but no row was found")
        return _hydrate(existing)

    async def _list_open(
        self,
        conn: asyncpg.Connection,
        limit: int,
    ) -> list[ModelResidualEvidence]:
        rows = await conn.fetch(
            f"""
            SELECT {_COLS_SQL}
            FROM model_residual_evidence
            WHERE tenant_id = $1 AND status = 'open'
            ORDER BY created_at ASC
            LIMIT $2
            """,
            self._tenant_id,
            limit,
        )
        return [_hydrate(row) for row in rows]

    async def _list_for_observations(
        self,
        conn: asyncpg.Connection,
        observation_ids: list[UUID],
    ) -> list[ModelResidualEvidence]:
        rows = await conn.fetch(
            f"""
            SELECT {_COLS_SQL}
            FROM model_residual_evidence
            WHERE tenant_id = $1
              AND source_observation_id = ANY($2::uuid[])
            ORDER BY created_at ASC
            """,
            self._tenant_id,
            observation_ids,
        )
        return [_hydrate(row) for row in rows]

    async def _resolve(
        self,
        residual_id: UUID,
        *,
        status: str,
        object_kind: str | None = None,
        object_id: UUID | None = None,
        metadata: dict[str, Any] | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> ModelResidualEvidence | None:
        if status not in {"absorbed", "rejected", "expired"}:
            raise ValueError(f"invalid residual resolution status: {status}")
        return await self._with_conn(
            conn,
            lambda c: self._resolve_with_conn(
                c,
                residual_id,
                status=status,
                object_kind=object_kind,
                object_id=object_id,
                metadata=metadata or {},
            ),
        )

    async def _resolve_with_conn(
        self,
        conn: asyncpg.Connection,
        residual_id: UUID,
        *,
        status: str,
        object_kind: str | None,
        object_id: UUID | None,
        metadata: dict[str, Any],
    ) -> ModelResidualEvidence | None:
        row = await conn.fetchrow(
            f"""
            UPDATE model_residual_evidence
            SET status = $3,
                absorption_object_kind = $4,
                absorption_object_id = $5,
                metadata = metadata || $6::jsonb,
                updated_at = now(),
                resolved_at = now()
            WHERE tenant_id = $1
              AND id = $2
              AND status = 'open'
            RETURNING {_COLS_SQL}
            """,
            self._tenant_id,
            residual_id,
            status,
            object_kind,
            object_id,
            _jsonb(metadata),
        )
        return _hydrate(row) if row is not None else None

    async def _with_conn(self, conn: Any, operation: Any) -> Any:
        if conn is not None:
            return await operation(conn)
        if self._pool is None:
            raise RuntimeError(
                "ModelResidualEvidenceRepo was constructed without a pool; "
                "pass conn= or construct it with a pool"
            )
        async with self._pool.acquire() as acquired:
            return await operation(acquired)

    def _validate_residual(
        self,
        residual: ModelResidualEvidence,
        *,
        require_open: bool = False,
    ) -> None:
        if residual.tenant_id != self._tenant_id:
            raise ValueError("ModelResidualEvidence.tenant_id does not match repo")
        if residual.residual_kind not in RESIDUAL_KINDS:
            raise ValueError(f"invalid residual kind: {residual.residual_kind}")
        if residual.status not in RESIDUAL_STATUSES:
            raise ValueError(f"invalid residual status: {residual.status}")
        if require_open and residual.status != "open":
            raise ValueError("insert_open requires residual.status='open'")
        if not residual.compact_summary.strip():
            raise ValueError("compact_summary is required")
        if not residual.reason.strip():
            raise ValueError("reason is required")


def _hydrate(row: Any) -> ModelResidualEvidence:
    data = dict(row)
    metadata = data.get("metadata") or {}
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    return ModelResidualEvidence(
        id=data.get("id"),
        tenant_id=data["tenant_id"],
        source_observation_id=data.get("source_observation_id"),
        think_run_id=data.get("think_run_id"),
        trigger_id=data.get("trigger_id"),
        model_id=data.get("model_id"),
        residual_kind=data["residual_kind"],
        compact_summary=data["compact_summary"],
        reason=data["reason"],
        status=data["status"],
        absorption_object_kind=data.get("absorption_object_kind"),
        absorption_object_id=data.get("absorption_object_id"),
        metadata=dict(metadata) if isinstance(metadata, dict) else {},
        created_at=data.get("created_at"),
        updated_at=data.get("updated_at"),
        resolved_at=data.get("resolved_at"),
    )


def _jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, default=str)


__all__ = [
    "ABSORPTION_OBJECT_KINDS",
    "ModelResidualEvidence",
    "ModelResidualEvidenceRepo",
    "RESIDUAL_KINDS",
    "RESIDUAL_STATUSES",
]
