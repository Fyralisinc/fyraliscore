"""services/resources/repo.py — CRUD for `resources`.

BUILD-PLAN.md §3 Prompt 2.C:
    create / update_attributes / archive / search_by_kind /
    search_by_name_fuzzy.

Per SCHEMA-LOCK S4.1:
  resources (id, tenant_id, kind, identity, description, current_value,
             valuation_confidence, utilization_state, controllability,
             temporal_character, metadata, created_at, last_updated_at,
             last_updated_by_event_id, archived_at)

Every write emits a state_change observation inside the same
transaction via `services/observations/state_change.emit_state_change`.
The observation's `cause_id` is the caller-supplied Observation that
authorized the mutation (`created_by_event_id` or `last_updated_by_event_id`).

Pydantic Literal types on `kind`/`utilization_state`/etc. at the service
boundary catch invalid values before they hit Postgres.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, get_args
from uuid import UUID

import asyncpg

from lib.shared.db import transaction
from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import (
    ResourceControllability,
    ResourceKind,
    ResourceRow,
    ResourceTemporalCharacter,
    ResourceUtilizationState,
)
from services.observations.state_change import emit_state_change


_VALID_KINDS: tuple[str, ...] = get_args(ResourceKind)
_VALID_UTILIZATION: tuple[str, ...] = get_args(ResourceUtilizationState)
_VALID_CONTROLLABILITY: tuple[str, ...] = get_args(ResourceControllability)
_VALID_TEMPORAL: tuple[str, ...] = get_args(ResourceTemporalCharacter)


# =====================================================================
# Create
# =====================================================================

async def create(
    *,
    kind: ResourceKind,
    identity: str,
    description: str | None = None,
    current_value: dict[str, Any],
    utilization_state: ResourceUtilizationState = "available",
    controllability: ResourceControllability = "owned",
    temporal_character: ResourceTemporalCharacter = "permanent",
    valuation_confidence: float = 1.0,
    metadata: dict[str, Any] | None = None,
    created_by_event_id: UUID,
    tenant_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> ResourceRow:
    """
    INSERT a `resources` row + state_change observation atomically.
    The caller's `created_by_event_id` becomes both `last_updated_by_event_id`
    and the `cause_id` of the emitted state_change (chain reconstruction).
    """
    if kind not in _VALID_KINDS:
        raise ValidationError(
            f"invalid resource kind {kind!r}",
            kind=kind,
            valid=list(_VALID_KINDS),
        )
    if utilization_state not in _VALID_UTILIZATION:
        raise ValidationError(
            f"invalid utilization_state {utilization_state!r}",
            utilization_state=utilization_state,
            valid=list(_VALID_UTILIZATION),
        )
    if controllability not in _VALID_CONTROLLABILITY:
        raise ValidationError(
            f"invalid controllability {controllability!r}",
            controllability=controllability,
            valid=list(_VALID_CONTROLLABILITY),
        )
    if temporal_character not in _VALID_TEMPORAL:
        raise ValidationError(
            f"invalid temporal_character {temporal_character!r}",
            temporal_character=temporal_character,
            valid=list(_VALID_TEMPORAL),
        )
    if not identity or not identity.strip():
        raise ValidationError("identity is required", field="identity")
    if not isinstance(current_value, dict):
        raise ValidationError(
            "current_value must be a dict", field="current_value"
        )

    async def _do(tx: asyncpg.Connection) -> ResourceRow:
        resource_id = uuid7()
        cv_json = json.dumps(current_value, default=str)
        md_json = json.dumps(metadata, default=str) if metadata is not None else None
        row = await tx.fetchrow(
            """
            INSERT INTO resources (
              id, tenant_id, kind, identity, description, current_value,
              valuation_confidence, utilization_state, controllability,
              temporal_character, metadata, last_updated_by_event_id
            ) VALUES (
              $1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10,
              $11::jsonb, $12
            )
            RETURNING *
            """,
            resource_id,
            tenant_id,
            kind,
            identity,
            description,
            cv_json,
            valuation_confidence,
            utilization_state,
            controllability,
            temporal_character,
            md_json,
            created_by_event_id,
        )
        await emit_state_change(
            tx,
            kind="resource_created",
            entity_id=resource_id,
            tenant_id=tenant_id,
            cause_event_id=created_by_event_id,
            entity_kind="resource",
            metadata={
                "resource_kind": kind,
                "identity": identity,
                "utilization_state": utilization_state,
            },
        )
        return ResourceRow.model_validate(dict(row))

    if conn is None:
        async with transaction() as tx:
            return await _do(tx)
    return await _do(conn)


# =====================================================================
# Update
# =====================================================================

async def update_attributes(
    resource_id: UUID,
    *,
    patch: dict[str, Any] | None = None,
    metadata_patch: dict[str, Any] | None = None,
    description: str | None = None,
    last_updated_by_event_id: UUID,
    conn: asyncpg.Connection | None = None,
) -> ResourceRow:
    """
    Merge `patch` into `resources.current_value` (JSONB shallow merge),
    optionally `metadata_patch` into `metadata`, bump `last_updated_at`,
    and emit a `resource_updated` state_change.

    `patch=None` is allowed (e.g. a metadata-only edit). At least one
    of patch / metadata_patch / description must be non-None.
    """
    if patch is None and metadata_patch is None and description is None:
        raise ValidationError(
            "update_attributes requires at least one of patch, "
            "metadata_patch, or description"
        )

    async def _do(tx: asyncpg.Connection) -> ResourceRow:
        row = await tx.fetchrow(
            "SELECT * FROM resources WHERE id = $1 FOR UPDATE",
            resource_id,
        )
        if row is None:
            raise ValidationError(
                "resource not found", resource_id=str(resource_id)
            )
        if row["archived_at"] is not None:
            raise InvariantViolation(
                "R4",
                "cannot update archived resource",
                resource_id=str(resource_id),
            )
        new_cv = dict(row["current_value"] or {})
        if patch:
            new_cv.update(patch)
        new_meta = dict(row["metadata"] or {}) if row["metadata"] else {}
        if metadata_patch:
            new_meta.update(metadata_patch)
        new_description = description if description is not None else row["description"]
        updated = await tx.fetchrow(
            """
            UPDATE resources
            SET current_value = $2::jsonb,
                metadata = $3::jsonb,
                description = $4,
                last_updated_at = now(),
                last_updated_by_event_id = $5
            WHERE id = $1
            RETURNING *
            """,
            resource_id,
            json.dumps(new_cv, default=str),
            json.dumps(new_meta, default=str) if new_meta else None,
            new_description,
            last_updated_by_event_id,
        )
        await emit_state_change(
            tx,
            kind="resource_updated",
            entity_id=resource_id,
            tenant_id=row["tenant_id"],
            cause_event_id=last_updated_by_event_id,
            entity_kind="resource",
            metadata={
                "resource_kind": row["kind"],
                "patch_keys": sorted(list(patch.keys())) if patch else [],
            },
        )
        return ResourceRow.model_validate(dict(updated))

    if conn is None:
        async with transaction() as tx:
            return await _do(tx)
    return await _do(conn)


# =====================================================================
# Archive
# =====================================================================

async def archive(
    resource_id: UUID,
    reason: str,
    *,
    cause_event_id: UUID | None = None,
    conn: asyncpg.Connection | None = None,
) -> ResourceRow:
    async def _do(tx: asyncpg.Connection) -> ResourceRow:
        row = await tx.fetchrow(
            "SELECT * FROM resources WHERE id = $1 FOR UPDATE",
            resource_id,
        )
        if row is None:
            raise ValidationError(
                "resource not found", resource_id=str(resource_id)
            )
        if row["archived_at"] is not None:
            # Idempotent: already archived — return as-is, no new event.
            return ResourceRow.model_validate(dict(row))
        updated = await tx.fetchrow(
            """
            UPDATE resources
            SET archived_at = now(), last_updated_at = now()
            WHERE id = $1
            RETURNING *
            """,
            resource_id,
        )
        await emit_state_change(
            tx,
            kind="resource_archived",
            entity_id=resource_id,
            tenant_id=row["tenant_id"],
            cause_event_id=cause_event_id,
            entity_kind="resource",
            metadata={"reason": reason, "resource_kind": row["kind"]},
        )
        return ResourceRow.model_validate(dict(updated))

    if conn is None:
        async with transaction() as tx:
            return await _do(tx)
    return await _do(conn)


# =====================================================================
# Get
# =====================================================================

async def get(
    resource_id: UUID,
    *,
    include_archived: bool = True,
    conn: asyncpg.Connection | None = None,
) -> ResourceRow | None:
    """Fetch a resource by id regardless of archival state by default."""
    q = "SELECT * FROM resources WHERE id = $1"
    if not include_archived:
        q += " AND archived_at IS NULL"
    if conn is not None:
        row = await conn.fetchrow(q, resource_id)
    else:
        from lib.shared.db import get_pool
        pool = get_pool()
        async with pool.acquire() as c:
            row = await c.fetchrow(q, resource_id)
    return ResourceRow.model_validate(dict(row)) if row else None


# =====================================================================
# Search
# =====================================================================

async def search_by_kind(
    kind: ResourceKind,
    tenant_id: UUID,
    *,
    include_archived: bool = False,
    limit: int | None = None,
    conn: asyncpg.Connection | None = None,
) -> list[ResourceRow]:
    """Returns non-archived resources of the given kind by default."""
    if kind not in _VALID_KINDS:
        raise ValidationError(
            f"invalid resource kind {kind!r}",
            kind=kind,
            valid=list(_VALID_KINDS),
        )
    q_parts = [
        "SELECT * FROM resources WHERE tenant_id = $1 AND kind = $2"
    ]
    args: list[Any] = [tenant_id, kind]
    if not include_archived:
        q_parts.append("AND archived_at IS NULL")
    q_parts.append("ORDER BY created_at DESC")
    if limit is not None:
        q_parts.append(f"LIMIT {int(limit)}")
    q = " ".join(q_parts)

    if conn is not None:
        rows = await conn.fetch(q, *args)
    else:
        from lib.shared.db import get_pool
        pool = get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(q, *args)
    return [ResourceRow.model_validate(dict(r)) for r in rows]


async def search_by_name_fuzzy(
    query: str,
    tenant_id: UUID,
    *,
    limit: int = 20,
    conn: asyncpg.Connection | None = None,
) -> list[ResourceRow]:
    """
    Trigram similarity (`pg_trgm`, installed per SCHEMA-LOCK S22.1) over
    `identity` and `description`. Uses `similarity()` and a threshold
    filter via the `%` operator; ordered by greatest similarity
    descending. Archived resources are excluded.
    """
    if not query or not query.strip():
        return []
    # We use `similarity(...)` directly and filter by a modest threshold
    # (0.1) rather than relying on the `%` operator whose threshold is
    # session-scoped and defaults to 0.3. This keeps behavior deterministic
    # across callers who may share a pool.
    q = """
        SELECT *,
               GREATEST(
                 similarity(identity, $2),
                 COALESCE(similarity(description, $2), 0.0)
               ) AS sim
        FROM resources
        WHERE tenant_id = $1
          AND archived_at IS NULL
          AND (
            similarity(identity, $2) >= 0.1
            OR COALESCE(similarity(description, $2), 0.0) >= 0.1
          )
        ORDER BY sim DESC
        LIMIT $3
    """
    if conn is not None:
        rows = await conn.fetch(q, tenant_id, query, int(limit))
    else:
        from lib.shared.db import get_pool
        pool = get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(q, tenant_id, query, int(limit))
    out: list[ResourceRow] = []
    for r in rows:
        d = dict(r)
        d.pop("sim", None)  # not in ResourceRow
        out.append(ResourceRow.model_validate(d))
    return out


__all__ = [
    "create",
    "update_attributes",
    "archive",
    "get",
    "search_by_kind",
    "search_by_name_fuzzy",
]
