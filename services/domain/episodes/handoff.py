"""Transactional outbox for settled episode snapshots."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7

from .contracts import EpisodeSnapshot


class EpisodeSnapshotOutboxRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: UUID
    tenant_id: UUID
    event_kind: Literal["episode.snapshot_settled"]
    topic_id: UUID
    episode_id: UUID
    episode_snapshot_id: UUID
    episode_snapshot_hash: str
    mode: Literal["automatic_update", "query_answer"]
    requester_actor_id: UUID | None
    query_text: str | None
    contract_version: int
    dedupe_key: str
    payload: dict[str, Any]
    status: Literal["pending", "leased", "completed", "dead_letter"]
    available_at: datetime
    attempt_count: int
    lease_owner: str | None
    lease_expires_at: datetime | None
    last_error: str | None
    completed_at: datetime | None
    created_at: datetime
    updated_at: datetime


_COLUMNS = (
    "id","tenant_id","event_kind","topic_id","episode_id",
    "episode_snapshot_id","episode_snapshot_hash","mode","requester_actor_id",
    "query_text","contract_version","dedupe_key","payload","status",
    "available_at","attempt_count","lease_owner","lease_expires_at",
    "last_error","completed_at","created_at","updated_at",
)


def _row(row: asyncpg.Record) -> EpisodeSnapshotOutboxRow:
    value = dict(row)
    if isinstance(value["payload"], str):
        value["payload"] = json.loads(value["payload"])
    return EpisodeSnapshotOutboxRow.model_validate(value)


class EpisodeSnapshotOutboxRepository:
    contract_version = 1

    async def enqueue(
        self, snapshot: EpisodeSnapshot, *, conn: asyncpg.Connection
    ) -> EpisodeSnapshotOutboxRow:
        if snapshot.lifecycle_state != "settled" or snapshot.settlement is None:
            raise ValidationError("reasoning handoff requires a settled snapshot")
        topic = await conn.fetchrow(
            "SELECT origin,requester_actor_id,query_text FROM episode_topics "
            "WHERE tenant_id=$1 AND id=$2",
            snapshot.tenant_id, snapshot.topic_id,
        )
        if topic is None:
            raise ValidationError("snapshot topic is missing")
        mode = "query_answer" if topic["origin"] == "query_seeded" else "automatic_update"
        dedupe = (
            f"{snapshot.tenant_id}:episode-snapshot:{snapshot.snapshot_hash}:"
            f"reasoning-v{self.contract_version}"
        )
        payload = {
            "episode_snapshot_id": str(snapshot.id),
            "episode_snapshot_hash": snapshot.snapshot_hash,
            "topic_id": str(snapshot.topic_id), "episode_id": str(snapshot.episode_id),
            "mode": mode,
            "requester_actor_id": str(topic["requester_actor_id"]) if topic["requester_actor_id"] else None,
            "query_text": topic["query_text"],
            "access_policy_hash": snapshot.access.policy_hash,
        }
        row = await conn.fetchrow(
            f"""
            INSERT INTO episode_snapshot_outbox (
              id,tenant_id,topic_id,episode_id,episode_snapshot_id,
              episode_snapshot_hash,mode,requester_actor_id,query_text,
              contract_version,dedupe_key,payload
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
            ON CONFLICT (tenant_id,dedupe_key)
              DO UPDATE SET updated_at=episode_snapshot_outbox.updated_at
            RETURNING {','.join(_COLUMNS)}
            """,
            uuid7(), snapshot.tenant_id, snapshot.topic_id, snapshot.episode_id,
            snapshot.id, snapshot.snapshot_hash, mode, topic["requester_actor_id"],
            topic["query_text"], self.contract_version, dedupe,
            json.dumps(payload, sort_keys=True),
        )
        assert row is not None
        return _row(row)

    async def claim(
        self,
        *,
        worker_id: str,
        batch_size: int,
        lease_seconds: int,
        conn: asyncpg.Connection,
    ) -> list[EpisodeSnapshotOutboxRow]:
        if not worker_id.strip() or batch_size < 1 or lease_seconds < 1:
            raise ValidationError("snapshot outbox claim parameters are invalid")
        rows = await conn.fetch(
            f"""
            WITH candidates AS (
              SELECT id FROM episode_snapshot_outbox
               WHERE (status='pending' AND available_at<=now())
                  OR (status='leased' AND lease_expires_at<=now())
               ORDER BY available_at,created_at,id LIMIT $1
               FOR UPDATE SKIP LOCKED
            )
            UPDATE episode_snapshot_outbox item
               SET status='leased',lease_owner=$2,
                   lease_expires_at=now()+make_interval(secs=>$3),
                   attempt_count=item.attempt_count+1,updated_at=now()
              FROM candidates WHERE item.id=candidates.id
            RETURNING {','.join('item.' + name for name in _COLUMNS)}
            """,
            batch_size,worker_id,lease_seconds,
        )
        return [_row(row) for row in rows]

    async def complete(
        self,item_id: UUID,*,tenant_id: UUID,worker_id: str,conn: asyncpg.Connection
    ) -> EpisodeSnapshotOutboxRow:
        row = await conn.fetchrow(
            f"""
            UPDATE episode_snapshot_outbox
               SET status='completed',completed_at=now(),lease_owner=NULL,
                   lease_expires_at=NULL,updated_at=now()
             WHERE id=$1 AND tenant_id=$2 AND status='leased' AND lease_owner=$3
            RETURNING {','.join(_COLUMNS)}
            """,
            item_id,tenant_id,worker_id,
        )
        if row is None:
            raise ValidationError("snapshot outbox item is not leased by this worker")
        return _row(row)

    async def retry(
        self,item_id: UUID,*,tenant_id: UUID,worker_id: str,error: str,
        delay_seconds: int,max_attempts: int,conn: asyncpg.Connection,
    ) -> EpisodeSnapshotOutboxRow:
        row = await conn.fetchrow(
            f"""
            UPDATE episode_snapshot_outbox
               SET status=CASE WHEN attempt_count >= $6 THEN 'dead_letter' ELSE 'pending' END,
                   available_at=now()+make_interval(secs=>$5),lease_owner=NULL,
                   lease_expires_at=NULL,last_error=$4,updated_at=now()
             WHERE id=$1 AND tenant_id=$2 AND status='leased' AND lease_owner=$3
            RETURNING {','.join(_COLUMNS)}
            """,
            item_id,tenant_id,worker_id,error[:2000],delay_seconds,max_attempts,
        )
        if row is None:
            raise ValidationError("snapshot outbox item is not leased by this worker")
        return _row(row)


__all__ = ["EpisodeSnapshotOutboxRepository", "EpisodeSnapshotOutboxRow"]
