"""Persistence helpers for projection workers."""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from services.domain.projections.types import ModelEvent, ProjectionSnapshot


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _dict_list(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _hydrate_event(row: asyncpg.Record) -> ModelEvent:
    semantic_snapshot = row["semantic_snapshot"]
    previous_snapshot = row["previous_snapshot"]
    scope_entities = row["scope_entities"]
    if isinstance(semantic_snapshot, str):
        semantic_snapshot = json.loads(semantic_snapshot)
    if isinstance(previous_snapshot, str):
        previous_snapshot = json.loads(previous_snapshot)
    if isinstance(scope_entities, str):
        scope_entities = json.loads(scope_entities)
    return ModelEvent(
        id=row["id"],
        tenant_id=row["tenant_id"],
        model_id=row["model_id"],
        event_type=row["event_type"],
        changed_fields=tuple(row["changed_fields"] or ()),
        proposition_kind=row["proposition_kind"],
        claim_role=row["claim_role"],
        domain_tags=tuple(row["domain_tags"] or ()),
        scope_entities=_dict_list(scope_entities),
        semantic_snapshot=semantic_snapshot or {},
        previous_snapshot=previous_snapshot,
        source_event_id=row["source_event_id"],
        created_at=row["created_at"],
    )


async def fetch_pending_events(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    projection_name: str,
    projection_version: str,
    limit: int,
) -> list[ModelEvent]:
    checkpoint = await conn.fetchrow(
        """
        SELECT last_processed_event_id, last_processed_event_created_at
        FROM projection_checkpoints
        WHERE tenant_id = $1
          AND projection_name = $2
          AND projection_version = $3
        """,
        tenant_id,
        projection_name,
        projection_version,
    )
    params: list[Any] = [tenant_id, limit]
    cursor = ""
    if checkpoint and checkpoint["last_processed_event_created_at"] is not None:
        params.extend(
            [
                checkpoint["last_processed_event_created_at"],
                checkpoint["last_processed_event_id"],
            ]
        )
        cursor = """
          AND (
            created_at > $3
            OR (created_at = $3 AND id > $4)
          )
        """
    rows = await conn.fetch(
        f"""
        SELECT
          id, tenant_id, model_id, event_type, changed_fields,
          proposition_kind, claim_role, domain_tags, scope_entities,
          semantic_snapshot, previous_snapshot, source_event_id, created_at
        FROM model_events
        WHERE tenant_id = $1
        {cursor}
        ORDER BY created_at ASC, id ASC
        LIMIT $2
        """,
        *params,
    )
    return [_hydrate_event(row) for row in rows]


async def upsert_checkpoint(
    conn: asyncpg.Connection,
    *,
    event: ModelEvent,
    projection_name: str,
    projection_version: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO projection_checkpoints (
          tenant_id, projection_name, projection_version,
          last_processed_event_id, last_processed_event_created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, now())
        ON CONFLICT (tenant_id, projection_name, projection_version)
        DO UPDATE SET
          last_processed_event_id = EXCLUDED.last_processed_event_id,
          last_processed_event_created_at = EXCLUDED.last_processed_event_created_at,
          updated_at = now()
        """,
        event.tenant_id,
        projection_name,
        projection_version,
        event.id,
        event.created_at,
    )


async def upsert_projection_snapshot(
    conn: asyncpg.Connection,
    snapshot: ProjectionSnapshot,
) -> None:
    await conn.execute(
        """
        INSERT INTO projection_snapshots (
          tenant_id, projection_name, projection_version, subject_key,
          payload, confidence, severity, source_model_ids, source_event_ids,
          updated_at
        ) VALUES (
          $1, $2, $3, $4,
          $5::jsonb, $6, $7, $8::uuid[], $9::uuid[],
          now()
        )
        ON CONFLICT (tenant_id, projection_name, projection_version, subject_key)
        DO UPDATE SET
          payload = EXCLUDED.payload,
          confidence = EXCLUDED.confidence,
          severity = EXCLUDED.severity,
          source_model_ids = EXCLUDED.source_model_ids,
          source_event_ids = EXCLUDED.source_event_ids,
          updated_at = now()
        """,
        snapshot.tenant_id,
        snapshot.projection_name,
        snapshot.projection_version,
        snapshot.subject_key,
        _jsonb(snapshot.payload),
        float(snapshot.confidence),
        snapshot.severity,
        list(snapshot.source_model_ids),
        list(snapshot.source_event_ids),
    )
