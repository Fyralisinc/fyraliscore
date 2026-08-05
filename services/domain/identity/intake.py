"""Transactional outbox from persisted observations to identity resolution."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ObservationRow


class IdentityOutboxRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID
    tenant_id: UUID
    event_kind: Literal[
        "observation.ready_for_identity", "identity.reresolution_requested"
    ]
    observation_id: UUID
    observation_occurred_at: datetime
    evidence_id: UUID
    contract_version: int
    dedupe_key: str
    reason: str
    payload: dict[str, Any]
    status: Literal["pending", "leased", "completed", "dead_letter"]
    available_at: datetime
    attempt_count: int
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    last_error: str | None = None
    completed_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


_COLUMNS = (
    "id", "tenant_id", "event_kind", "observation_id",
    "observation_occurred_at", "evidence_id", "contract_version", "dedupe_key",
    "reason", "payload", "status", "available_at", "attempt_count",
    "lease_owner", "lease_expires_at", "last_error", "completed_at",
    "created_at", "updated_at",
)
_SELECT = ", ".join(_COLUMNS)


def _hydrate(row: asyncpg.Record) -> IdentityOutboxRow:
    value = dict(row)
    if isinstance(value["payload"], str):
        value["payload"] = json.loads(value["payload"])
    return IdentityOutboxRow.model_validate(value)


class IdentityIntakeRepository:
    contract_version = 1

    async def enqueue_observation_ready(
        self, observation: ObservationRow, *, conn: asyncpg.Connection
    ) -> IdentityOutboxRow:
        if observation.evidence_id is None:
            raise ValidationError("identity intake requires immutable evidence")
        return await self.enqueue(
            tenant_id=observation.tenant_id,
            observation_id=observation.id,
            observation_occurred_at=observation.occurred_at,
            evidence_id=observation.evidence_id,
            event_kind="observation.ready_for_identity",
            reason="initial_observation",
            cause_assertion_ids=(),
            conn=conn,
        )

    async def enqueue_reprocess(
        self,
        observation: ObservationRow,
        *,
        reason: str,
        cause_assertion_ids: tuple[UUID, ...],
        conn: asyncpg.Connection,
    ) -> IdentityOutboxRow:
        if observation.evidence_id is None:
            raise ValidationError("identity reprocessing requires immutable evidence")
        if not cause_assertion_ids:
            raise ValidationError("identity reprocessing requires a causal assertion")
        return await self.enqueue(
            tenant_id=observation.tenant_id,
            observation_id=observation.id,
            observation_occurred_at=observation.occurred_at,
            evidence_id=observation.evidence_id,
            event_kind="identity.reresolution_requested",
            reason=reason,
            cause_assertion_ids=cause_assertion_ids,
            conn=conn,
        )

    async def enqueue(
        self,
        *,
        tenant_id: UUID,
        observation_id: UUID,
        observation_occurred_at: datetime,
        evidence_id: UUID,
        event_kind: Literal[
            "observation.ready_for_identity", "identity.reresolution_requested"
        ],
        reason: str,
        cause_assertion_ids: tuple[UUID, ...],
        conn: asyncpg.Connection,
    ) -> IdentityOutboxRow:
        if not reason.strip():
            raise ValidationError("identity intake reason must be non-empty")
        causal = sorted(str(value) for value in set(cause_assertion_ids))
        cause_hash = hashlib.sha256(
            json.dumps(causal, separators=(",", ":")).encode()
        ).hexdigest()[:16]
        dedupe_key = (
            f"{tenant_id}:identity:{event_kind}:{observation_id}:"
            f"{reason}:{cause_hash}:v{self.contract_version}"
        )
        payload = {
            "observation_id": str(observation_id),
            "evidence_id": str(evidence_id),
            "reason": reason,
            "cause_assertion_ids": causal,
        }
        row = await conn.fetchrow(
            f"""
            INSERT INTO identity_resolution_outbox (
              id, tenant_id, event_kind, observation_id,
              observation_occurred_at, evidence_id, contract_version,
              dedupe_key, reason, payload
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
            ON CONFLICT (tenant_id, dedupe_key) DO UPDATE
              SET updated_at = identity_resolution_outbox.updated_at
            RETURNING {_SELECT}
            """,
            uuid7(), tenant_id, event_kind, observation_id,
            observation_occurred_at, evidence_id, self.contract_version,
            dedupe_key, reason, json.dumps(payload, sort_keys=True),
        )
        assert row is not None
        persisted = _hydrate(row)
        if persisted.evidence_id != evidence_id:
            raise ValidationError("identity intake key maps to different evidence")
        return persisted

    async def claim(
        self,
        *,
        worker_id: str,
        batch_size: int,
        lease_seconds: int,
        conn: asyncpg.Connection,
    ) -> list[IdentityOutboxRow]:
        if not worker_id.strip() or batch_size < 1 or lease_seconds < 1:
            raise ValidationError("identity claim parameters are invalid")
        rows = await conn.fetch(
            f"""
            WITH ranked AS (
              SELECT id,tenant_id,available_at,created_at,
                     row_number() OVER (
                       PARTITION BY tenant_id ORDER BY available_at,created_at,id
                     ) AS tenant_rank
                FROM identity_resolution_outbox
               WHERE (
                 (status = 'pending' AND available_at <= now())
                 OR (status = 'leased' AND lease_expires_at <= now())
               )
            ), candidates AS (
              SELECT item.id FROM identity_resolution_outbox item
              JOIN ranked ON ranked.id=item.id
              ORDER BY ranked.tenant_rank,ranked.available_at,
                       ranked.created_at,item.id
              LIMIT $1 FOR UPDATE OF item SKIP LOCKED
            )
            UPDATE identity_resolution_outbox AS item
               SET status = 'leased', lease_owner = $2,
                   lease_expires_at = now() + make_interval(secs => $3),
                   attempt_count = item.attempt_count + 1, updated_at = now()
              FROM candidates WHERE item.id = candidates.id
            RETURNING {', '.join('item.' + value for value in _COLUMNS)}
            """,
            batch_size, worker_id, lease_seconds,
        )
        return [_hydrate(row) for row in rows]

    async def complete(
        self,
        item_id: UUID,
        *,
        tenant_id: UUID,
        worker_id: str,
        conn: asyncpg.Connection,
    ) -> IdentityOutboxRow:
        row = await conn.fetchrow(
            f"""
            UPDATE identity_resolution_outbox
               SET status = 'completed', completed_at = now(),
                   lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
             WHERE id = $1 AND tenant_id = $2
               AND status = 'leased' AND lease_owner = $3
            RETURNING {_SELECT}
            """,
            item_id, tenant_id, worker_id,
        )
        if row is None:
            raise ValidationError("identity outbox item is not leased by this worker")
        return _hydrate(row)

    async def retry(
        self,
        item_id: UUID,
        *,
        tenant_id: UUID,
        worker_id: str,
        error: str,
        delay_seconds: int,
        max_attempts: int,
        conn: asyncpg.Connection,
    ) -> IdentityOutboxRow:
        if delay_seconds < 0 or max_attempts < 1:
            raise ValidationError("identity retry parameters are invalid")
        row = await conn.fetchrow(
            f"""
            UPDATE identity_resolution_outbox
               SET status = CASE WHEN attempt_count >= $6
                                 THEN 'dead_letter' ELSE 'pending' END,
                   available_at = now() + make_interval(secs => $5),
                   lease_owner = NULL, lease_expires_at = NULL,
                   last_error = $4, updated_at = now()
             WHERE id = $1 AND tenant_id = $2
               AND status = 'leased' AND lease_owner = $3
            RETURNING {_SELECT}
            """,
            item_id, tenant_id, worker_id, error[:2000], delay_seconds, max_attempts,
        )
        if row is None:
            raise ValidationError("identity outbox item is not leased by this worker")
        return _hydrate(row)


__all__ = ["IdentityIntakeRepository", "IdentityOutboxRow"]
