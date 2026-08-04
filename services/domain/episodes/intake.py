"""Transactional outbox consumed by the future episode constructor."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ObservationRow
from services.domain.identity.resolution import IdentityResolutionSnapshot


class PerceptionOutboxRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    tenant_id: UUID
    event_kind: Literal["observation.ready_for_episode"]
    aggregate_type: Literal["observation"]
    aggregate_id: UUID
    observation_id: UUID
    observation_occurred_at: datetime
    evidence_id: UUID
    identity_snapshot_id: UUID | None = None
    identity_snapshot_hash: str | None = None
    identity_resolution_status: Literal["complete", "partial"] | None = None
    contract_version: int
    dedupe_key: str
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
    "id", "tenant_id", "event_kind", "aggregate_type", "aggregate_id",
    "observation_id", "observation_occurred_at", "evidence_id",
    "identity_snapshot_id", "identity_snapshot_hash", "identity_resolution_status",
    "contract_version", "dedupe_key", "payload", "status", "available_at",
    "attempt_count", "lease_owner", "lease_expires_at", "last_error",
    "completed_at", "created_at", "updated_at",
)
_SELECT = ", ".join(_COLUMNS)


def _hydrate(row: asyncpg.Record) -> PerceptionOutboxRow:
    value = dict(row)
    if isinstance(value["payload"], str):
        value["payload"] = json.loads(value["payload"])
    return PerceptionOutboxRow.model_validate(value)


class EpisodeIntakeRepository:
    contract_version = 2

    async def enqueue_identity_resolved(
        self,
        observation: ObservationRow,
        identity_snapshot: IdentityResolutionSnapshot,
        *,
        conn: asyncpg.Connection,
    ) -> PerceptionOutboxRow:
        if observation.evidence_id is None:
            raise ValidationError("episode intake requires immutable evidence")
        if (
            identity_snapshot.tenant_id != observation.tenant_id
            or identity_snapshot.observation_id != observation.id
        ):
            raise ValidationError("identity snapshot does not belong to observation")
        return await self.enqueue_ready(
            tenant_id=observation.tenant_id,
            observation_id=observation.id,
            observation_occurred_at=observation.occurred_at,
            evidence_id=observation.evidence_id,
            source_channel=observation.source_channel,
            kind=observation.kind,
            trust_tier=observation.trust_tier,
            actor_id=observation.actor_id,
            identity_snapshot_id=identity_snapshot.id,
            identity_snapshot_hash=identity_snapshot.snapshot_hash,
            identity_resolution_status=identity_snapshot.resolution_status,
            conn=conn,
        )

    async def enqueue_ready(
        self,
        *,
        tenant_id: UUID,
        observation_id: UUID,
        observation_occurred_at: datetime,
        evidence_id: UUID,
        source_channel: str,
        kind: str,
        trust_tier: str,
        actor_id: UUID | None,
        identity_snapshot_id: UUID,
        identity_snapshot_hash: str,
        identity_resolution_status: Literal["complete", "partial"],
        conn: asyncpg.Connection,
    ) -> PerceptionOutboxRow:
        dedupe_key = (
            f"{tenant_id}:observation:{observation_id}:"
            f"identity:{identity_snapshot_hash}:v{self.contract_version}"
        )
        payload = {
            "observation_id": str(observation_id),
            "evidence_id": str(evidence_id),
            "source_channel": source_channel,
            "kind": kind,
            "trust_tier": trust_tier,
            "occurred_at": observation_occurred_at.isoformat(),
            "actor_id": str(actor_id) if actor_id else None,
            "identity_snapshot_id": str(identity_snapshot_id),
            "identity_snapshot_hash": identity_snapshot_hash,
            "identity_resolution_status": identity_resolution_status,
        }
        row = await conn.fetchrow(
            f"""
            INSERT INTO perception_outbox (
              id, tenant_id, event_kind, aggregate_type, aggregate_id,
              observation_id, observation_occurred_at, evidence_id,
              identity_snapshot_id, identity_snapshot_hash,
              identity_resolution_status, contract_version, dedupe_key, payload
            ) VALUES (
              $1, $2, 'observation.ready_for_episode', 'observation', $3,
              $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb
            )
            ON CONFLICT (tenant_id, dedupe_key)
            DO UPDATE SET updated_at = perception_outbox.updated_at
            RETURNING {_SELECT}
            """,
            uuid7(),
            tenant_id,
            observation_id,
            observation_occurred_at,
            evidence_id,
            identity_snapshot_id,
            identity_snapshot_hash,
            identity_resolution_status,
            self.contract_version,
            dedupe_key,
            json.dumps(payload, sort_keys=True),
        )
        assert row is not None
        persisted = _hydrate(row)
        if (
            persisted.evidence_id != evidence_id
            or persisted.observation_occurred_at != observation_occurred_at
            or persisted.identity_snapshot_id != identity_snapshot_id
        ):
            raise ValidationError("episode intake dedupe key maps to different evidence")
        return persisted

    async def claim(
        self,
        *,
        worker_id: str,
        batch_size: int,
        lease_seconds: int,
        conn: asyncpg.Connection,
    ) -> list[PerceptionOutboxRow]:
        if not worker_id.strip():
            raise ValidationError("worker_id must be non-empty")
        if batch_size < 1 or lease_seconds < 1:
            raise ValidationError("batch_size and lease_seconds must be positive")
        rows = await conn.fetch(
            f"""
            WITH candidates AS (
              SELECT id FROM perception_outbox
               WHERE (
                 (status = 'pending' AND available_at <= now())
                 OR (status = 'leased' AND lease_expires_at <= now())
               )
               ORDER BY available_at, created_at, id
               LIMIT $1
               FOR UPDATE SKIP LOCKED
            )
            UPDATE perception_outbox AS item
               SET status = 'leased', lease_owner = $2,
                   lease_expires_at = now() + make_interval(secs => $3),
                   attempt_count = item.attempt_count + 1,
                   updated_at = now()
              FROM candidates
             WHERE item.id = candidates.id
            RETURNING {', '.join('item.' + column for column in _COLUMNS)}
            """,
            batch_size,
            worker_id,
            lease_seconds,
        )
        return [_hydrate(row) for row in rows]

    async def complete(
        self,
        item_id: UUID,
        *,
        tenant_id: UUID,
        worker_id: str,
        conn: asyncpg.Connection,
    ) -> PerceptionOutboxRow:
        row = await conn.fetchrow(
            f"""
            UPDATE perception_outbox
               SET status = 'completed', completed_at = now(),
                   lease_owner = NULL, lease_expires_at = NULL,
                   updated_at = now()
             WHERE id = $1 AND tenant_id = $2
               AND status = 'leased' AND lease_owner = $3
            RETURNING {_SELECT}
            """,
            item_id,
            tenant_id,
            worker_id,
        )
        if row is None:
            raise ValidationError("outbox item is not leased by this worker")
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
    ) -> PerceptionOutboxRow:
        if delay_seconds < 0 or max_attempts < 1:
            raise ValidationError("retry delay and max attempts are invalid")
        row = await conn.fetchrow(
            f"""
            UPDATE perception_outbox
               SET status = CASE
                     WHEN attempt_count >= $6 THEN 'dead_letter'
                     ELSE 'pending'
                   END,
                   available_at = now() + make_interval(secs => $5),
                   lease_owner = NULL, lease_expires_at = NULL,
                   last_error = $4, updated_at = now()
             WHERE id = $1 AND tenant_id = $2
               AND status = 'leased' AND lease_owner = $3
            RETURNING {_SELECT}
            """,
            item_id,
            tenant_id,
            worker_id,
            error[:2000],
            delay_seconds,
            max_attempts,
        )
        if row is None:
            raise ValidationError("outbox item is not leased by this worker")
        return _hydrate(row)


__all__ = ["EpisodeIntakeRepository", "PerceptionOutboxRow"]
