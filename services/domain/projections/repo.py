"""Read API for materialized projection snapshots.

Projection snapshots are rebuildable operating views over canonical Models.
This repo gives retrieval, workers, APIs, and future extensions one typed way
to load those views without reaching into retrieval-private helpers.
"""
from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.types import ModelRow
from services.domain.models.read_shapes import (
    MODEL_ROW_SELECT_COLS,
    MODEL_ROW_SELECT_SQL,
    hydrate_model_row,
)


_MODEL_SELECT_COLS = MODEL_ROW_SELECT_COLS
_MODEL_SELECT_SQL = MODEL_ROW_SELECT_SQL


@dataclass(frozen=True)
class ProjectionRecord:
    tenant_id: UUID
    projection_name: str
    projection_version: str
    subject_key: str
    payload: dict[str, Any]
    confidence: float
    severity: str | None
    source_model_ids: tuple[UUID, ...]
    source_event_ids: tuple[UUID, ...]
    updated_at: datetime


@dataclass(frozen=True)
class ProjectionContext:
    projection_name: str
    projection_version: str
    subject_key: str
    payload: dict[str, Any]
    confidence: float
    severity: str | None
    source_model_ids: tuple[UUID, ...]
    source_models: tuple[ModelRow, ...]


@dataclass(frozen=True)
class ProjectionStaleness:
    projection_name: str
    projection_version: str
    is_stale: bool
    reason: str
    latest_model_event_id: UUID | None = None
    latest_model_event_created_at: datetime | None = None
    checkpoint_event_id: UUID | None = None
    checkpoint_event_created_at: datetime | None = None
    checkpoint_updated_at: datetime | None = None


class ProjectionRepo:
    """Typed reads for projection snapshots and their backing Models."""

    async def get_snapshot(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        projection_name: str,
        subject_key: str,
        projection_version: str = "v1",
    ) -> ProjectionRecord | None:
        row = await conn.fetchrow(
            """
            SELECT
              tenant_id, projection_name, projection_version, subject_key,
              payload, confidence, severity, source_model_ids,
              source_event_ids, updated_at
            FROM projection_snapshots
            WHERE tenant_id = $1
              AND projection_name = $2
              AND projection_version = $3
              AND subject_key = $4
            """,
            tenant_id,
            projection_name,
            projection_version,
            subject_key,
        )
        return _hydrate_projection(row) if row is not None else None

    async def list_subjects(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        projection_name: str,
        projection_version: str = "v1",
        limit: int = 100,
    ) -> list[str]:
        rows = await conn.fetch(
            """
            SELECT subject_key
            FROM projection_snapshots
            WHERE tenant_id = $1
              AND projection_name = $2
              AND projection_version = $3
            ORDER BY updated_at DESC, subject_key ASC
            LIMIT $4
            """,
            tenant_id,
            projection_name,
            projection_version,
            max(0, limit),
        )
        return [row["subject_key"] for row in rows]

    async def list_snapshots(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        projection_name: str,
        projection_version: str = "v1",
        limit: int = 100,
    ) -> list[ProjectionRecord]:
        rows = await conn.fetch(
            """
            SELECT
              tenant_id, projection_name, projection_version, subject_key,
              payload, confidence, severity, source_model_ids,
              source_event_ids, updated_at
            FROM projection_snapshots
            WHERE tenant_id = $1
              AND projection_name = $2
              AND projection_version = $3
            ORDER BY updated_at DESC, confidence DESC, subject_key ASC
            LIMIT $4
            """,
            tenant_id,
            projection_name,
            projection_version,
            max(0, limit),
        )
        return [_hydrate_projection(row) for row in rows]

    async def list_snapshots_for_subjects(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        subjects: Sequence[tuple[str, str]],
        projection_version: str = "v1",
        limit: int = 100,
        require_source_models: bool = False,
    ) -> list[ProjectionRecord]:
        """Load snapshots for ordered ``(projection_name, subject_key)`` pairs."""
        pairs = _dedupe_subject_pairs(subjects)
        if not pairs or limit <= 0:
            return []

        projection_names = [projection_name for projection_name, _ in pairs]
        subject_keys = [subject_key for _, subject_key in pairs]
        source_filter = (
            "AND cardinality(ps.source_model_ids) > 0"
            if require_source_models
            else ""
        )
        rows = await conn.fetch(
            f"""
            WITH wanted AS (
              SELECT *
              FROM unnest($2::text[], $3::text[])
                WITH ORDINALITY AS t(projection_name, subject_key, ord)
            )
            SELECT
              ps.tenant_id, ps.projection_name, ps.projection_version,
              ps.subject_key, ps.payload, ps.confidence, ps.severity,
              ps.source_model_ids, ps.source_event_ids, ps.updated_at
            FROM wanted w
            JOIN projection_snapshots ps
              ON ps.projection_name = w.projection_name
             AND ps.subject_key = w.subject_key
            WHERE ps.tenant_id = $1
              AND ps.projection_version = $4
              {source_filter}
            ORDER BY w.ord ASC, ps.confidence DESC, ps.updated_at DESC
            LIMIT $5
            """,
            tenant_id,
            projection_names,
            subject_keys,
            projection_version,
            limit,
        )
        return [_hydrate_projection(row) for row in rows]

    async def get_context(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        projection_name: str,
        subject_key: str,
        projection_version: str = "v1",
        include_models: bool = True,
    ) -> ProjectionContext | None:
        snapshot = await self.get_snapshot(
            conn,
            tenant_id=tenant_id,
            projection_name=projection_name,
            projection_version=projection_version,
            subject_key=subject_key,
        )
        if snapshot is None:
            return None

        source_models: tuple[ModelRow, ...] = ()
        if include_models:
            source_models = await self.load_models_by_id(
                conn,
                tenant_id=tenant_id,
                model_ids=snapshot.source_model_ids,
            )
        return ProjectionContext(
            projection_name=snapshot.projection_name,
            projection_version=snapshot.projection_version,
            subject_key=snapshot.subject_key,
            payload=snapshot.payload,
            confidence=snapshot.confidence,
            severity=snapshot.severity,
            source_model_ids=snapshot.source_model_ids,
            source_models=source_models,
        )

    async def load_models_by_id(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        model_ids: Sequence[UUID],
    ) -> tuple[ModelRow, ...]:
        ids = _dedupe_uuids(model_ids)
        if not ids:
            return ()
        rows = await conn.fetch(
            f"""
            SELECT {_MODEL_SELECT_SQL}
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND id = ANY($2::uuid[])
            """,
            tenant_id,
            ids,
        )
        models = [_hydrate_model(row) for row in rows]
        by_id = {model.id: model for model in models}
        return tuple(model for model_id in ids if (model := by_id.get(model_id)))

    async def is_stale(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        projection_name: str,
        projection_version: str = "v1",
    ) -> ProjectionStaleness:
        results = await self.list_staleness(
            conn,
            tenant_id=tenant_id,
            projection_names=[projection_name],
            projection_version=projection_version,
        )
        return results[0]

    async def list_staleness(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        projection_names: Sequence[str],
        projection_version: str = "v1",
    ) -> list[ProjectionStaleness]:
        names = _dedupe_names(projection_names)
        if not names:
            return []

        latest = await conn.fetchrow(
            """
            SELECT id, created_at
            FROM model_events
            WHERE tenant_id = $1
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            tenant_id,
        )
        if latest is None:
            return [
                ProjectionStaleness(
                    projection_name=name,
                    projection_version=projection_version,
                    is_stale=False,
                    reason="no_model_events",
                )
                for name in names
            ]

        rows = await conn.fetch(
            """
            WITH requested AS (
              SELECT *
              FROM unnest($2::text[]) WITH ORDINALITY AS t(projection_name, ord)
            )
            SELECT
              r.projection_name,
              c.last_processed_event_id,
              c.last_processed_event_created_at,
              c.updated_at
            FROM requested r
            LEFT JOIN projection_checkpoints c
              ON c.tenant_id = $1
             AND c.projection_name = r.projection_name
             AND c.projection_version = $3
            ORDER BY r.ord ASC
            """,
            tenant_id,
            names,
            projection_version,
        )
        out: list[ProjectionStaleness] = []
        for row in rows:
            checkpoint_missing = (
                row["last_processed_event_created_at"] is None
                or row["last_processed_event_id"] is None
            )
            pending_exists = (
                _event_after(
                    latest["id"],
                    latest["created_at"],
                    row["last_processed_event_id"],
                    row["last_processed_event_created_at"],
                )
                if not checkpoint_missing
                else False
            )
            out.append(
                ProjectionStaleness(
                    projection_name=row["projection_name"],
                    projection_version=projection_version,
                    is_stale=checkpoint_missing or pending_exists,
                    reason=(
                        "no_checkpoint"
                        if checkpoint_missing
                        else "pending_model_events"
                        if pending_exists
                        else "current"
                    ),
                    latest_model_event_id=latest["id"],
                    latest_model_event_created_at=latest["created_at"],
                    checkpoint_event_id=row["last_processed_event_id"],
                    checkpoint_event_created_at=row["last_processed_event_created_at"],
                    checkpoint_updated_at=row["updated_at"],
                )
            )
        return out


def _event_after(
    event_id: UUID,
    event_created_at: datetime,
    checkpoint_event_id: UUID,
    checkpoint_event_created_at: datetime,
) -> bool:
    if event_created_at > checkpoint_event_created_at:
        return True
    if event_created_at < checkpoint_event_created_at:
        return False
    return event_id.int > checkpoint_event_id.int


def _hydrate_projection(row: asyncpg.Record) -> ProjectionRecord:
    return ProjectionRecord(
        tenant_id=row["tenant_id"],
        projection_name=row["projection_name"],
        projection_version=row["projection_version"],
        subject_key=row["subject_key"],
        payload=_loads_dict(row["payload"]),
        confidence=float(row["confidence"]),
        severity=row["severity"],
        source_model_ids=_uuid_tuple(row["source_model_ids"]),
        source_event_ids=_uuid_tuple(row["source_event_ids"]),
        updated_at=row["updated_at"],
    )


def _hydrate_model(record: asyncpg.Record) -> ModelRow:
    return hydrate_model_row(
        record,
        drop_internal_fields=True,
        null_invalid_embedding=True,
        use_vector_to_list=True,
    )


def _loads_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    return value if isinstance(value, dict) else {}


def _uuid_tuple(value: Any) -> tuple[UUID, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return ()
    out: list[UUID] = []
    for item in value or ():
        try:
            out.append(item if isinstance(item, UUID) else UUID(str(item)))
        except (TypeError, ValueError):
            continue
    return tuple(out)


def _dedupe_uuids(model_ids: Sequence[UUID]) -> list[UUID]:
    seen: set[UUID] = set()
    out: list[UUID] = []
    for model_id in model_ids:
        if model_id in seen:
            continue
        seen.add(model_id)
        out.append(model_id)
    return out


def _dedupe_names(names: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for name in names:
        normalized = str(name or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _dedupe_subject_pairs(
    subjects: Sequence[tuple[str, str]],
) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for projection_name, subject_key in subjects:
        key = (
            str(projection_name or "").strip(),
            str(subject_key or "").strip(),
        )
        if not key[0] or not key[1]:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out
