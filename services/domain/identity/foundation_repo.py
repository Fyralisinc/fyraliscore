"""PostgreSQL repositories for the identity-resolution foundation."""

from __future__ import annotations

import json
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7

from .foundation import (
    EntityMentionCreate,
    EntityMentionRow,
    ResolutionRunCreate,
    ResolutionRunRow,
    SourceReferenceCreate,
    SourceReferenceRow,
)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes, bytearray)) else value


def _source_reference(row: asyncpg.Record) -> SourceReferenceRow:
    value = dict(row)
    value["attributes"] = _json(value["attributes"])
    value["evidence_id"] = value["latest_evidence_id"]
    return SourceReferenceRow.model_validate(value)


def _mention(row: asyncpg.Record) -> EntityMentionRow:
    value = dict(row)
    value["expected_types"] = tuple(_json(value["expected_types"]))
    value["context"] = _json(value["context"])
    return EntityMentionRow.model_validate(value)


def _run(row: asyncpg.Record) -> ResolutionRunRow:
    value = dict(row)
    value["capability_snapshot"] = _json(value["capability_snapshot"])
    return ResolutionRunRow.model_validate(value)


_SOURCE_COLUMNS = (
    "id", "tenant_id", "connector_installation_id", "installation_scope", "source",
    "native_type", "native_id", "stable_key", "reference_kind", "attributes",
    "first_evidence_id", "latest_evidence_id", "valid_from", "valid_to", "status",
    "version", "first_seen_at", "last_seen_at",
)
_MENTION_COLUMNS = (
    "id", "tenant_id", "observation_id", "observation_occurred_at", "evidence_id",
    "source_reference_id", "mention_key", "mention_kind", "text", "span_start",
    "span_end", "expected_types", "context", "status", "created_at",
)
_RUN_COLUMNS = (
    "id", "tenant_id", "input_kind", "observation_id", "observation_occurred_at",
    "requester_actor_id", "input_hash", "resolver_name", "resolver_version",
    "policy_version", "capability_snapshot", "status", "result_hash", "failure",
    "started_at", "completed_at",
)


class SourceReferenceRepository:
    async def register(
        self, value: SourceReferenceCreate, *, conn: asyncpg.Connection
    ) -> SourceReferenceRow:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"{value.tenant_id}:{value.computed_stable_key}",
        )
        row = await conn.fetchrow(
            f"""
            INSERT INTO identity_source_references (
              id, tenant_id, connector_installation_id, installation_scope,
              source, native_type, native_id, stable_key, reference_kind,
              attributes, first_evidence_id, latest_evidence_id, valid_from,
              valid_to, status
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb,
              $11, $11, $12, $13, $14
            )
            ON CONFLICT (tenant_id, stable_key) DO UPDATE SET
              connector_installation_id = COALESCE(
                identity_source_references.connector_installation_id,
                EXCLUDED.connector_installation_id
              ),
              reference_kind = EXCLUDED.reference_kind,
              attributes = identity_source_references.attributes || EXCLUDED.attributes,
              latest_evidence_id = EXCLUDED.latest_evidence_id,
              valid_from = COALESCE(identity_source_references.valid_from, EXCLUDED.valid_from),
              valid_to = EXCLUDED.valid_to,
              status = EXCLUDED.status,
              version = CASE
                WHEN identity_source_references.latest_evidence_id = EXCLUDED.latest_evidence_id
                THEN identity_source_references.version
                ELSE identity_source_references.version + 1
              END,
              last_seen_at = now()
            RETURNING {', '.join(_SOURCE_COLUMNS)}
            """,
            uuid7(), value.tenant_id, value.connector_installation_id,
            value.installation_scope, value.source, value.native_type, value.native_id,
            value.computed_stable_key, value.reference_kind,
            json.dumps(value.attributes, sort_keys=True), value.evidence_id,
            value.valid_from, value.valid_to, value.status,
        )
        assert row is not None
        result = _source_reference(row)
        if (
            result.source != value.source
            or result.installation_scope != value.installation_scope
            or result.native_type != value.native_type
            or result.native_id != value.native_id
        ):
            raise ValidationError("source reference key collision")
        return result

    async def get(
        self, reference_id: UUID, *, tenant_id: UUID, conn: asyncpg.Connection
    ) -> SourceReferenceRow | None:
        row = await conn.fetchrow(
            f"SELECT {', '.join(_SOURCE_COLUMNS)} FROM identity_source_references "
            "WHERE id = $1 AND tenant_id = $2",
            reference_id, tenant_id,
        )
        return _source_reference(row) if row is not None else None


class EntityMentionRepository:
    async def register(
        self, value: EntityMentionCreate, *, conn: asyncpg.Connection
    ) -> EntityMentionRow:
        row = await conn.fetchrow(
            f"""
            INSERT INTO entity_mentions (
              id, tenant_id, observation_id, observation_occurred_at, evidence_id,
              source_reference_id, mention_key, mention_kind, text, span_start,
              span_end, expected_types, context
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
              $12::jsonb, $13::jsonb
            )
            ON CONFLICT (tenant_id, mention_key) DO UPDATE
              SET mention_key = entity_mentions.mention_key
            RETURNING {', '.join(_MENTION_COLUMNS)}
            """,
            uuid7(), value.tenant_id, value.observation_id,
            value.observation_occurred_at, value.evidence_id,
            value.source_reference_id, value.computed_mention_key, value.mention_kind,
            value.text, value.span_start, value.span_end,
            json.dumps(value.expected_types), json.dumps(value.context, sort_keys=True),
        )
        assert row is not None
        return _mention(row)

    async def for_observation(
        self, observation_id: UUID, *, tenant_id: UUID, conn: asyncpg.Connection
    ) -> list[EntityMentionRow]:
        rows = await conn.fetch(
            f"SELECT {', '.join(_MENTION_COLUMNS)} FROM entity_mentions "
            "WHERE tenant_id = $1 AND observation_id = $2 AND status = 'registered' "
            "ORDER BY created_at, id",
            tenant_id, observation_id,
        )
        return [_mention(row) for row in rows]


class ResolutionRunRepository:
    async def start(
        self, value: ResolutionRunCreate, *, conn: asyncpg.Connection
    ) -> ResolutionRunRow:
        row = await conn.fetchrow(
            f"""
            INSERT INTO identity_resolution_runs (
              id, tenant_id, input_kind, observation_id, observation_occurred_at,
              requester_actor_id, input_hash, resolver_name, resolver_version,
              policy_version, capability_snapshot
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
            ON CONFLICT (
              tenant_id, input_kind, input_hash, resolver_name, resolver_version
            ) DO UPDATE SET input_hash = identity_resolution_runs.input_hash
            RETURNING {', '.join(_RUN_COLUMNS)}
            """,
            uuid7(), value.tenant_id, value.input_kind, value.observation_id,
            value.observation_occurred_at, value.requester_actor_id,
            value.input_hash, value.resolver_name, value.resolver_version,
            value.policy_version,
            json.dumps(value.capability_snapshot, sort_keys=True),
        )
        assert row is not None
        return _run(row)

    async def finish(
        self,
        run_id: UUID,
        *,
        tenant_id: UUID,
        status: Literal["completed", "failed"],
        result_hash: str | None = None,
        failure: str | None = None,
        conn: asyncpg.Connection,
    ) -> ResolutionRunRow:
        if status == "completed" and result_hash is None:
            raise ValidationError("completed resolver runs require a result hash")
        if status == "failed" and not failure:
            raise ValidationError("failed resolver runs require a failure reason")
        row = await conn.fetchrow(
            f"""
            UPDATE identity_resolution_runs
               SET status = $3, result_hash = $4, failure = $5,
                   completed_at = now()
             WHERE id = $1 AND tenant_id = $2 AND status = 'running'
            RETURNING {', '.join(_RUN_COLUMNS)}
            """,
            run_id, tenant_id, status, result_hash, failure,
        )
        if row is None:
            raise ValidationError("resolver run is not active")
        return _run(row)


__all__ = [
    "EntityMentionRepository",
    "ResolutionRunRepository",
    "SourceReferenceRepository",
]
