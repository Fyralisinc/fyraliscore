"""Persistence for immutable source revisions and their raw lineage."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from lib.shared.types import SourceEvidenceCreate, SourceEvidenceRow


_COLUMNS = (
    "id", "tenant_id", "source", "connector_installation_id",
    "installation_scope", "source_channel", "source_object_type",
    "source_object_id", "source_revision_id", "operation",
    "source_recorded_at", "valid_from", "valid_to",
    "supersedes_evidence_id", "parent_ref", "container_ref", "thread_id",
    "raw_object_key", "content_hash", "raw_ingested_at", "normalized_at",
    "ingress_kind", "ingress_metadata", "idem_hints", "contract_version",
    "connector_version", "parser_version", "normalizer_version",
    "raw_retention_state", "raw_expired_at", "first_seen_at", "last_seen_at",
)
_SELECT = ", ".join(_COLUMNS)


def _json_object(value: Any) -> Any:
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return value


def _hydrate(row: asyncpg.Record) -> SourceEvidenceRow:
    value = dict(row)
    for key in ("parent_ref", "container_ref", "ingress_metadata", "idem_hints"):
        value[key] = _json_object(value.get(key))
    return SourceEvidenceRow.model_validate(value)


@dataclass(frozen=True)
class EvidencePersistResult:
    evidence: SourceEvidenceRow
    deduped: bool


class SourceEvidenceRepository:
    async def insert(
        self,
        value: SourceEvidenceCreate,
        *,
        conn: asyncpg.Connection,
    ) -> EvidencePersistResult:
        existing = await conn.fetchrow(
            f"""
            SELECT {_SELECT}
              FROM source_evidence
             WHERE tenant_id = $1
               AND source = $2
               AND installation_scope = $3
               AND source_object_type = $4
               AND source_object_id = $5
               AND source_revision_id = $6
               AND operation = $7
            """,
            value.tenant_id,
            value.source,
            value.installation_scope,
            value.source_object_type,
            value.source_object_id,
            value.source_revision_id,
            value.operation,
        )
        if existing is not None:
            row = await conn.fetchrow(
                f"""
                UPDATE source_evidence
                   SET last_seen_at = now()
                 WHERE id = $1
                RETURNING {_SELECT}
                """,
                existing["id"],
            )
            assert row is not None
            return EvidencePersistResult(_hydrate(row), True)

        supersedes_evidence_id: UUID | None = None
        if value.supersedes_revision_id:
            supersedes_evidence_id = await conn.fetchval(
                """
                SELECT id
                  FROM source_evidence
                 WHERE tenant_id = $1
                   AND source = $2
                   AND installation_scope = $3
                   AND source_object_type = $4
                   AND source_object_id = $5
                   AND source_revision_id = $6
                 ORDER BY source_recorded_at DESC
                 LIMIT 1
                """,
                value.tenant_id,
                value.source,
                value.installation_scope,
                value.source_object_type,
                value.source_object_id,
                value.supersedes_revision_id,
            )
        evidence_id = value.id or uuid7()
        row = await conn.fetchrow(
            f"""
            INSERT INTO source_evidence (
                id, tenant_id, source, connector_installation_id,
                installation_scope, source_channel, source_object_type,
                source_object_id, source_revision_id, operation,
                source_recorded_at, valid_from, valid_to,
                supersedes_evidence_id, parent_ref, container_ref, thread_id,
                raw_object_key, content_hash, raw_ingested_at, normalized_at,
                ingress_kind, ingress_metadata, idem_hints, contract_version,
                connector_version, parser_version, normalizer_version,
                raw_retention_state
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15::jsonb, $16::jsonb, $17,
                $18, $19, $20, $21, $22, $23::jsonb, $24::jsonb, $25,
                $26, $27, $28, $29
            )
            ON CONFLICT (
                tenant_id, source, installation_scope, source_object_type,
                source_object_id, source_revision_id, operation
            ) DO UPDATE SET last_seen_at = now()
            RETURNING {_SELECT}
            """,
            evidence_id,
            value.tenant_id,
            value.source,
            value.connector_installation_id,
            value.installation_scope,
            value.source_channel,
            value.source_object_type,
            value.source_object_id,
            value.source_revision_id,
            value.operation,
            value.source_recorded_at,
            value.valid_from,
            value.valid_to,
            supersedes_evidence_id,
            json.dumps(value.parent_ref) if value.parent_ref is not None else None,
            json.dumps(value.container_ref) if value.container_ref is not None else None,
            value.thread_id,
            value.raw_object_key,
            value.content_hash,
            value.raw_ingested_at,
            value.normalized_at,
            value.ingress_kind,
            json.dumps(value.ingress_metadata),
            json.dumps(value.idem_hints),
            value.contract_version,
            value.connector_version,
            value.parser_version,
            value.normalizer_version,
            value.raw_retention_state,
        )
        assert row is not None
        persisted = _hydrate(row)
        return EvidencePersistResult(persisted, persisted.id != evidence_id)

    async def mark_raw_expired(
        self,
        evidence_id: UUID,
        *,
        tenant_id: UUID,
        conn: asyncpg.Connection,
    ) -> SourceEvidenceRow | None:
        row = await conn.fetchrow(
            f"""
            UPDATE source_evidence
               SET raw_retention_state = 'expired',
                   raw_expired_at = now(),
                   last_seen_at = now()
             WHERE id = $1 AND tenant_id = $2
            RETURNING {_SELECT}
            """,
            evidence_id,
            tenant_id,
        )
        return _hydrate(row) if row is not None else None
