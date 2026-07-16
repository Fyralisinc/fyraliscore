"""Shared transaction records for consequential semantic appliers.

This module owns no business transition.  It only gives every named writer the
same command-result-event-outbox commit shape and exact idempotency behavior.
The caller must keep these writes in the same transaction as its semantic row.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import BaseModel

from lib.contracts.agency import AgencyWriteContext
from lib.contracts.kernel import WriterCutoverState, canonical_sha256
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7


@dataclass(frozen=True)
class AgencyProtocolIds:
    command_result_id: UUID
    event_id: UUID
    outbox_id: UUID

    @classmethod
    def new(cls) -> AgencyProtocolIds:
        return cls(command_result_id=uuid7(), event_id=uuid7(), outbox_id=uuid7())


@dataclass(frozen=True)
class AgencyCommitResult:
    command_result_id: UUID
    event_id: UUID
    object_id: UUID
    object_version: int
    result: dict[str, Any]
    duplicate: bool = False


async def prior_protocol_result(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    writer_id: str,
    idempotency_key: str,
    request_digest: str,
) -> AgencyCommitResult | None:
    row = await conn.fetchrow(
        """
        SELECT id, request_digest, object_id, object_version, result,
               (SELECT e.id FROM agency_canonical_events e
                 WHERE e.command_result_id = agency_command_results.id) AS event_id
        FROM agency_command_results
        WHERE tenant_id = $1 AND writer_id = $2
          AND semantic_idempotency_key = $3
        """,
        tenant_id,
        writer_id,
        idempotency_key,
    )
    if row is None:
        return None
    if row["request_digest"] != request_digest:
        raise InvariantViolation(
            "AGENCY_IDEMPOTENCY_CONFLICT",
            "one consequential idempotency key was reused for different content",
            writer_id=writer_id,
            idempotency_key=idempotency_key,
        )
    return AgencyCommitResult(
        command_result_id=row["id"],
        event_id=row["event_id"],
        object_id=row["object_id"],
        object_version=int(row["object_version"]),
        result=_json(row["result"]),
        duplicate=True,
    )


async def insert_protocol_result(
    *,
    conn: asyncpg.Connection,
    ids: AgencyProtocolIds,
    context: AgencyWriteContext,
    writer_id: str,
    command_kind: str,
    command: BaseModel,
    request_digest: str,
    object_type: str,
    object_id: UUID,
    object_version: int,
    result: dict[str, Any],
    consumption_authority_fingerprint: str | None = None,
) -> None:
    await validate_registered_protocol_writer_scope(
        conn=conn,
        context=context,
        writer_id=writer_id,
    )
    await conn.execute(
        """
        INSERT INTO agency_command_results (
            id, tenant_id, command_id, writer_id,
            semantic_idempotency_key, request_digest, command_kind, status,
            command, processing_authority_fingerprint,
            consumption_authority_fingerprint, writer_scope_id, writer_epoch,
            object_type, object_id, object_version, result
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, 'applied', $8::jsonb, $9,
            $10, $11, $12, $13, $14, $15, $16::jsonb
        )
        """,
        ids.command_result_id,
        context.tenant_id,
        context.command_id,
        writer_id,
        context.idempotency_key,
        request_digest,
        command_kind,
        json.dumps(command.model_dump(mode="json")),
        context.processing_authority.fingerprint,
        consumption_authority_fingerprint,
        context.writer_scope_epoch.scope_id,
        context.writer_scope_epoch.epoch,
        object_type,
        object_id,
        object_version,
        json.dumps(result),
    )


async def validate_registered_protocol_writer_scope(
    *,
    conn: asyncpg.Connection,
    context: AgencyWriteContext,
    writer_id: str,
) -> bool:
    """Enforce a canonical claim when this responsibility/partition is cut over.

    Unregistered legacy scopes remain available during semantic strangulation.
    Once either the presented scope ID has entered the registry or its exact
    partition has an active claim, an embedded command cannot bypass the
    authoritative owner, epoch, state, or scope ID.
    """

    embedded = context.writer_scope_epoch
    registered = None
    try:
        registered_scope_id = UUID(embedded.scope_id)
    except ValueError:
        registered_scope_id = None
    if registered_scope_id is not None:
        registered = await conn.fetchrow(
            """
            SELECT h.*, c.scope_id AS claimed_scope_id
            FROM writer_scope_heads h
            LEFT JOIN writer_scope_partition_claims c
              ON c.tenant_id=h.tenant_id AND c.scope_id=h.scope_id
             AND c.semantic_responsibility=h.semantic_responsibility
             AND c.source_partition=$3
            WHERE h.tenant_id=$1 AND h.scope_id=$2
            FOR KEY SHARE OF h
            """,
            context.tenant_id,
            registered_scope_id,
            embedded.source_partition,
        )
    if registered is None:
        competing = await conn.fetchrow(
            """
            SELECT h.*, c.scope_id AS claimed_scope_id
            FROM writer_scope_partition_claims c
            JOIN writer_scope_heads h
              ON h.tenant_id=c.tenant_id AND h.scope_id=c.scope_id
            WHERE c.tenant_id=$1 AND c.semantic_responsibility=$2
              AND c.source_partition=$3
            FOR KEY SHARE OF h
            """,
            context.tenant_id,
            embedded.semantic_responsibility,
            embedded.source_partition,
        )
        if competing is None:
            return False
        registered = competing
    current_state = WriterCutoverState(str(registered["current_state"]))
    exact = (
        str(registered["scope_id"]) == embedded.scope_id
        and registered["semantic_responsibility"]
        == embedded.semantic_responsibility
        and embedded.source_partition in tuple(registered["source_partitions"])
        and registered["writer_owner"] == writer_id == embedded.writer_owner
        and int(registered["current_epoch"]) == embedded.epoch
        and current_state is embedded.state
        and registered["claimed_scope_id"] is not None
        and embedded.permits(
            writer_owner=writer_id,
            epoch=embedded.epoch,
            tenant_id=context.tenant_id,
            semantic_responsibility=embedded.semantic_responsibility,
            source_partition=embedded.source_partition,
        )
    )
    if not exact:
        raise InvariantViolation(
            "AGENCY_WRITER_SCOPE_FENCED",
            "canonical writer-scope registry rejected this semantic commit",
            presented_scope_id=embedded.scope_id,
            current_scope_id=str(registered["scope_id"]),
            presented_epoch=embedded.epoch,
            current_epoch=int(registered["current_epoch"]),
            presented_owner=embedded.writer_owner,
            current_owner=registered["writer_owner"],
            presented_state=embedded.state.value,
            current_state=current_state.value,
            source_partition=embedded.source_partition,
        )
    return True


async def insert_protocol_event_and_outbox(
    *,
    conn: asyncpg.Connection,
    ids: AgencyProtocolIds,
    context: AgencyWriteContext,
    writer_id: str,
    object_type: str,
    object_id: UUID,
    object_version: int,
    semantic_transition: str,
    event_payload: dict[str, Any],
    intervention_spec_digest: str | None,
    destination_operation: str,
) -> AgencyCommitResult:
    payload = {
        "command_result_id": str(ids.command_result_id),
        "writer_id": writer_id,
        "object_type": object_type,
        "object_id": str(object_id),
        "object_version": object_version,
        "semantic_transition": semantic_transition,
        **event_payload,
    }
    await conn.execute(
        """
        INSERT INTO agency_canonical_events (
            id, tenant_id, command_result_id, writer_id, object_type,
            object_id, object_version, semantic_transition,
            intervention_spec_digest, event_payload
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb
        )
        """,
        ids.event_id,
        context.tenant_id,
        ids.command_result_id,
        writer_id,
        object_type,
        object_id,
        object_version,
        semantic_transition,
        intervention_spec_digest,
        json.dumps(payload),
    )
    await conn.execute(
        """
        INSERT INTO agency_outbox_records (
            id, tenant_id, event_id, destination_operation,
            payload_hash, payload, deadline, attempt_budget
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, 8)
        """,
        ids.outbox_id,
        context.tenant_id,
        ids.event_id,
        destination_operation,
        canonical_sha256(payload),
        json.dumps(payload),
        context.expires_at,
    )
    return AgencyCommitResult(
        command_result_id=ids.command_result_id,
        event_id=ids.event_id,
        object_id=object_id,
        object_version=object_version,
        result=event_payload,
    )


def ensure_live_context(context: AgencyWriteContext, *, now: datetime) -> None:
    if now < context.issued_at:
        raise InvariantViolation(
            "AGENCY_COMMAND_TIME",
            "consequential command cannot execute before issuance",
        )
    if now >= context.expires_at:
        raise InvariantViolation(
            "AGENCY_COMMAND_EXPIRED",
            "consequential command expired before commit",
        )
    if not context.processing_authority.is_live(now):
        raise InvariantViolation(
            "AGENCY_PROCESSING_AUTHORITY",
            "processing authority expired before consequential commit",
        )


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


__all__ = [
    "AgencyCommitResult",
    "AgencyProtocolIds",
    "ensure_live_context",
    "insert_protocol_event_and_outbox",
    "insert_protocol_result",
    "prior_protocol_result",
    "validate_registered_protocol_writer_scope",
]
