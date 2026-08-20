"""Persistence helpers for projection workers."""
from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.domain.projections.types import (
    ModelEvent,
    ProjectionDependencyRef,
    ProjectionRefreshJob,
    ProjectionRefreshReason,
    ProjectionSnapshot,
    ProjectionSubjectRef,
    ProjectionWatchKey,
)


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _dict_list(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _loads_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _uuid_tuple(value: Any) -> tuple[UUID, ...]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for raw in value or ():
        try:
            uid = raw if isinstance(raw, UUID) else UUID(str(raw))
        except (TypeError, ValueError):
            continue
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return tuple(out)


def _dependency_refs_from_json(value: Any) -> tuple[ProjectionDependencyRef, ...]:
    raw = _loads_json(value)
    if not isinstance(raw, list):
        return ()
    refs: list[ProjectionDependencyRef] = []
    seen: set[tuple[str, str]] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        ref_kind = str(item.get("ref_kind") or item.get("kind") or "").strip()
        ref_value = str(item.get("ref_value") or item.get("value") or "").strip()
        if not ref_kind or not ref_value:
            continue
        key = (ref_kind, ref_value)
        if key in seen:
            continue
        seen.add(key)
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        refs.append(
            ProjectionDependencyRef(
                ref_kind=ref_kind,
                ref_value=ref_value,
                reason=str(item["reason"]) if item.get("reason") else None,
                metadata=metadata,
            )
        )
    return tuple(refs)


def _dependency_refs_json(refs: Sequence[ProjectionDependencyRef]) -> str:
    return _jsonb(
        [
            {
                "ref_kind": ref.ref_kind,
                "ref_value": ref.ref_value,
                "reason": ref.reason,
                "metadata": ref.metadata,
            }
            for ref in _dedupe_dependency_refs(refs)
        ]
    )


def _dedupe_dependency_refs(
    refs: Sequence[ProjectionDependencyRef],
) -> tuple[ProjectionDependencyRef, ...]:
    out: list[ProjectionDependencyRef] = []
    seen: set[tuple[str, str]] = set()
    for ref in refs:
        ref_kind = str(ref.ref_kind or "").strip()
        ref_value = str(ref.ref_value or "").strip()
        if not ref_kind or not ref_value:
            continue
        key = (ref_kind, ref_value)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            ProjectionDependencyRef(
                ref_kind=ref_kind,
                ref_value=ref_value,
                reason=ref.reason,
                metadata=dict(ref.metadata or {}),
            )
        )
    return tuple(out)


def _dedupe_watch_keys(
    keys: Sequence[ProjectionWatchKey],
) -> tuple[ProjectionWatchKey, ...]:
    out: list[ProjectionWatchKey] = []
    seen: set[tuple[str, str]] = set()
    for key in keys:
        watch_kind = str(key.watch_kind or "").strip()
        watch_value = str(key.watch_value or "").strip()
        if not watch_kind or not watch_value:
            continue
        dedupe_key = (watch_kind, watch_value)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(
            ProjectionWatchKey(
                watch_kind=watch_kind,
                watch_value=watch_value,
                reason=key.reason,
                metadata=dict(key.metadata or {}),
            )
        )
    return tuple(out)


def _hydrate_refresh_job(row: asyncpg.Record) -> ProjectionRefreshJob:
    return ProjectionRefreshJob(
        id=row["id"],
        tenant_id=row["tenant_id"],
        projection_name=row["projection_name"],
        projection_version=row["projection_version"],
        subject_key=row["subject_key"],
        reason=row["reason"],
        event_ids=_uuid_tuple(row["event_ids"]),
        dependency_refs=_dependency_refs_from_json(row["dependency_refs"]),
        payload=_loads_json(row["payload"]) or {},
        status=row["status"],
        attempts=int(row["attempts"] or 0),
        max_attempts=int(row["max_attempts"] or 5),
        scheduled_at=row["scheduled_at"],
        leased_at=row["leased_at"],
        processed_at=row["processed_at"],
        last_error=row["last_error"],
    )


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


async def fetch_events_for_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: Sequence[UUID],
    limit: int,
) -> list[ModelEvent]:
    if limit <= 0:
        return []
    unique_model_ids = list(dict.fromkeys(_uuid_tuple(model_ids)))
    if not unique_model_ids:
        return []
    rows = await conn.fetch(
        """
        SELECT
          id, tenant_id, model_id, event_type, changed_fields,
          proposition_kind, claim_role, domain_tags, scope_entities,
          semantic_snapshot, previous_snapshot, source_event_id, created_at
        FROM model_events
        WHERE tenant_id = $1
          AND model_id = ANY($2::uuid[])
        ORDER BY created_at ASC, id ASC
        LIMIT $3
        """,
        tenant_id,
        unique_model_ids,
        limit,
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


def dependency_refs_for_snapshot(
    snapshot: ProjectionSnapshot,
    *,
    extra_refs: Sequence[ProjectionDependencyRef] = (),
) -> tuple[ProjectionDependencyRef, ...]:
    refs: list[ProjectionDependencyRef] = [
        *[
            ProjectionDependencyRef("model", str(model_id), reason="source_model")
            for model_id in snapshot.source_model_ids
        ],
        *[
            ProjectionDependencyRef("model_event", str(event_id), reason="source_event")
            for event_id in snapshot.source_event_ids
        ],
        *extra_refs,
    ]
    return _dedupe_dependency_refs(refs)


async def replace_projection_dependencies(
    conn: asyncpg.Connection,
    snapshot: ProjectionSnapshot,
    *,
    extra_refs: Sequence[ProjectionDependencyRef] = (),
) -> None:
    refs = dependency_refs_for_snapshot(snapshot, extra_refs=extra_refs)
    await conn.execute(
        """
        DELETE FROM projection_dependencies
        WHERE tenant_id = $1
          AND projection_name = $2
          AND projection_version = $3
          AND subject_key = $4
        """,
        snapshot.tenant_id,
        snapshot.projection_name,
        snapshot.projection_version,
        snapshot.subject_key,
    )
    if not refs:
        return
    await conn.executemany(
        """
        INSERT INTO projection_dependencies (
          tenant_id, projection_name, projection_version, subject_key,
          ref_kind, ref_value, reason, metadata, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now())
        ON CONFLICT (
          tenant_id, projection_name, projection_version, subject_key,
          ref_kind, ref_value
        ) DO UPDATE SET
          reason = EXCLUDED.reason,
          metadata = EXCLUDED.metadata
        """,
        [
            (
                snapshot.tenant_id,
                snapshot.projection_name,
                snapshot.projection_version,
                snapshot.subject_key,
                ref.ref_kind,
                ref.ref_value,
                ref.reason,
                _jsonb(ref.metadata),
            )
            for ref in refs
        ],
    )


async def replace_projection_watch_keys(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    projection_name: str,
    projection_version: str,
    subject_key: str,
    watch_keys: Sequence[ProjectionWatchKey],
) -> None:
    keys = _dedupe_watch_keys(watch_keys)
    await conn.execute(
        """
        DELETE FROM projection_watch_keys
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
    if not keys:
        return
    await conn.executemany(
        """
        INSERT INTO projection_watch_keys (
          tenant_id, projection_name, projection_version, subject_key,
          watch_kind, watch_value, reason, metadata, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, now())
        ON CONFLICT (
          tenant_id, projection_name, projection_version, subject_key,
          watch_kind, watch_value
        ) DO UPDATE SET
          reason = EXCLUDED.reason,
          metadata = EXCLUDED.metadata
        """,
        [
            (
                tenant_id,
                projection_name,
                projection_version,
                subject_key,
                key.watch_kind,
                key.watch_value,
                key.reason,
                _jsonb(key.metadata),
            )
            for key in keys
        ],
    )


async def enqueue_projection_refresh_job(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    projection_name: str,
    subject_key: str,
    projection_version: str = "v1",
    reason: ProjectionRefreshReason | str,
    event_ids: Sequence[UUID] = (),
    dependency_refs: Sequence[ProjectionDependencyRef] = (),
    payload: dict[str, Any] | None = None,
    max_attempts: int = 5,
    scheduled_at: datetime | None = None,
) -> UUID:
    job_id = uuid7()
    row = await conn.fetchrow(
        """
        INSERT INTO projection_refresh_jobs (
          id, tenant_id, projection_name, projection_version, subject_key,
          reason, event_ids, dependency_refs, payload, max_attempts,
          scheduled_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5,
          $6, $7::uuid[], $8::jsonb, $9::jsonb, $10,
          COALESCE($11::timestamptz, now()), now()
        )
        ON CONFLICT (
          tenant_id, projection_name, projection_version, subject_key
        ) WHERE status = 'pending'
        DO UPDATE SET
          reason = EXCLUDED.reason,
          event_ids = ARRAY(
            SELECT DISTINCT x
            FROM unnest(projection_refresh_jobs.event_ids || EXCLUDED.event_ids) AS t(x)
          ),
          dependency_refs = projection_refresh_jobs.dependency_refs
            || EXCLUDED.dependency_refs,
          payload = projection_refresh_jobs.payload || EXCLUDED.payload,
          max_attempts = GREATEST(
            projection_refresh_jobs.max_attempts,
            EXCLUDED.max_attempts
          ),
          scheduled_at = LEAST(
            projection_refresh_jobs.scheduled_at,
            EXCLUDED.scheduled_at
          ),
          updated_at = now()
        RETURNING id
        """,
        job_id,
        tenant_id,
        projection_name.strip(),
        projection_version.strip() or "v1",
        subject_key.strip(),
        str(reason),
        list(_uuid_tuple(event_ids)),
        _dependency_refs_json(dependency_refs),
        _jsonb(payload or {}),
        max(1, int(max_attempts)),
        scheduled_at,
    )
    if row is None:
        raise RuntimeError("projection refresh enqueue returned no row")
    return row["id"]


async def lease_projection_refresh_jobs(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    limit: int,
    lease_token: UUID | None = None,
) -> list[ProjectionRefreshJob]:
    if limit <= 0:
        return []
    lease_token = lease_token or uuid7()
    rows = await conn.fetch(
        """
        WITH picked AS (
          SELECT id
          FROM projection_refresh_jobs
          WHERE tenant_id = $1
            AND status = 'pending'
            AND scheduled_at <= now()
            AND attempts < max_attempts
          ORDER BY scheduled_at ASC, created_at ASC, id ASC
          FOR UPDATE SKIP LOCKED
          LIMIT $2
        )
        UPDATE projection_refresh_jobs AS job
        SET status = 'leased',
            attempts = attempts + 1,
            lease_token = $3,
            leased_at = now(),
            updated_at = now()
        FROM picked
        WHERE job.id = picked.id
        RETURNING
          job.id, job.tenant_id, job.projection_name, job.projection_version,
          job.subject_key, job.reason, job.event_ids, job.dependency_refs,
          job.payload, job.status, job.attempts, job.max_attempts,
          job.scheduled_at, job.leased_at, job.processed_at, job.last_error
        """,
        tenant_id,
        limit,
        lease_token,
    )
    return [_hydrate_refresh_job(row) for row in rows]


async def complete_projection_refresh_job(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    job_id: UUID,
) -> None:
    await conn.execute(
        """
        UPDATE projection_refresh_jobs
        SET status = 'processed',
            processed_at = now(),
            lease_token = NULL,
            updated_at = now(),
            last_error = NULL
        WHERE tenant_id = $1
          AND id = $2
        """,
        tenant_id,
        job_id,
    )


async def fail_projection_refresh_job(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    job_id: UUID,
    error: str,
) -> None:
    await conn.execute(
        """
        UPDATE projection_refresh_jobs
        SET status = CASE
              WHEN attempts >= max_attempts THEN 'dead_letter'
              ELSE 'pending'
            END,
            lease_token = NULL,
            leased_at = NULL,
            scheduled_at = CASE
              WHEN attempts >= max_attempts THEN scheduled_at
              ELSE now() + make_interval(secs => LEAST(300, attempts * 10))
            END,
            last_error = $3,
            updated_at = now()
        WHERE tenant_id = $1
          AND id = $2
        """,
        tenant_id,
        job_id,
        error[:2000],
    )


async def list_projection_subjects_for_dependency(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    ref_kind: str,
    ref_value: str,
    limit: int = 100,
) -> list[ProjectionSubjectRef]:
    rows = await conn.fetch(
        """
        SELECT projection_name, projection_version, subject_key
        FROM projection_dependencies
        WHERE tenant_id = $1
          AND ref_kind = $2
          AND ref_value = $3
        ORDER BY projection_name ASC, subject_key ASC
        LIMIT $4
        """,
        tenant_id,
        ref_kind,
        ref_value,
        max(0, limit),
    )
    return [
        ProjectionSubjectRef(
            projection_name=row["projection_name"],
            projection_version=row["projection_version"],
            subject_key=row["subject_key"],
        )
        for row in rows
    ]


async def list_projection_subjects_for_watch_key(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    watch_kind: str,
    watch_value: str,
    limit: int = 100,
) -> list[ProjectionSubjectRef]:
    rows = await conn.fetch(
        """
        SELECT projection_name, projection_version, subject_key
        FROM projection_watch_keys
        WHERE tenant_id = $1
          AND watch_kind = $2
          AND watch_value = $3
        ORDER BY projection_name ASC, subject_key ASC
        LIMIT $4
        """,
        tenant_id,
        watch_kind,
        watch_value,
        max(0, limit),
    )
    return [
        ProjectionSubjectRef(
            projection_name=row["projection_name"],
            projection_version=row["projection_version"],
            subject_key=row["subject_key"],
        )
        for row in rows
    ]
