"""Immutable claim-set barrier between identity resolution and episode routing."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ObservationRow
from services.domain.episodes.intake import EpisodeIntakeRepository
from services.domain.identity.resolution import IdentityResolutionSnapshot

from .claims import PerceptionClaimCreate, PerceptionClaimRepository, span_for_text


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PerceptionKnowledgeOutboxRow(_Frozen):
    id: UUID
    tenant_id: UUID
    event_kind: Literal["identity.ready_for_knowledge", "claim.changed"]
    observation_id: UUID
    observation_occurred_at: datetime
    evidence_id: UUID
    identity_snapshot_id: UUID
    identity_snapshot_hash: str
    identity_resolution_status: Literal["complete", "partial"]
    trigger_claim_id: UUID | None
    reason: str
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


class PerceptionKnowledgeSnapshot(_Frozen):
    id: UUID
    tenant_id: UUID
    observation_id: UUID
    observation_occurred_at: datetime
    evidence_id: UUID
    identity_snapshot_id: UUID
    identity_snapshot_hash: str
    identity_resolution_status: Literal["complete", "partial"]
    claim_ids: tuple[UUID, ...]
    claim_set_hash: str
    extractor_name: str
    extractor_version: str
    manifest: dict[str, Any]
    snapshot_hash: str
    created_at: datetime


_OUTBOX_COLUMNS = (
    "id", "tenant_id", "event_kind", "observation_id",
    "observation_occurred_at", "evidence_id", "identity_snapshot_id",
    "identity_snapshot_hash", "identity_resolution_status", "trigger_claim_id",
    "reason", "contract_version", "dedupe_key", "payload", "status",
    "available_at", "attempt_count", "lease_owner", "lease_expires_at",
    "last_error", "completed_at", "created_at", "updated_at",
)
_SNAPSHOT_COLUMNS = (
    "id", "tenant_id", "observation_id", "observation_occurred_at",
    "evidence_id", "identity_snapshot_id", "identity_snapshot_hash",
    "identity_resolution_status", "claim_ids", "claim_set_hash",
    "extractor_name", "extractor_version", "manifest", "snapshot_hash",
    "created_at",
)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes, bytearray)) else value


def _outbox(row: asyncpg.Record) -> PerceptionKnowledgeOutboxRow:
    value = dict(row)
    value["payload"] = _json(value["payload"])
    return PerceptionKnowledgeOutboxRow.model_validate(value)


def _snapshot(row: asyncpg.Record) -> PerceptionKnowledgeSnapshot:
    value = dict(row)
    value["claim_ids"] = tuple(value["claim_ids"])
    value["manifest"] = _json(value["manifest"])
    return PerceptionKnowledgeSnapshot.model_validate(value)


class PerceptionKnowledgeIntakeRepository:
    contract_version = 1

    async def enqueue_identity_resolved(
        self,
        observation: ObservationRow,
        identity_snapshot: IdentityResolutionSnapshot,
        *,
        conn: asyncpg.Connection,
        reason: str = "identity_snapshot_created",
    ) -> PerceptionKnowledgeOutboxRow:
        if observation.evidence_id is None:
            raise ValidationError("knowledge intake requires immutable evidence")
        if (
            identity_snapshot.tenant_id != observation.tenant_id
            or identity_snapshot.observation_id != observation.id
        ):
            raise ValidationError("identity snapshot does not belong to observation")
        dedupe = (
            f"{observation.tenant_id}:observation:{observation.id}:identity:"
            f"{identity_snapshot.snapshot_hash}:knowledge-v{self.contract_version}"
        )
        payload = {
            "observation_id": str(observation.id),
            "identity_snapshot_id": str(identity_snapshot.id),
            "identity_snapshot_hash": identity_snapshot.snapshot_hash,
        }
        row = await conn.fetchrow(
            f"""
            INSERT INTO perception_knowledge_outbox (
              id, tenant_id, event_kind, observation_id,
              observation_occurred_at, evidence_id, identity_snapshot_id,
              identity_snapshot_hash, identity_resolution_status, reason,
              contract_version, dedupe_key, payload
            ) VALUES (
              $1,$2,'identity.ready_for_knowledge',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb
            ) ON CONFLICT (tenant_id,dedupe_key)
              DO UPDATE SET updated_at=perception_knowledge_outbox.updated_at
            RETURNING {','.join(_OUTBOX_COLUMNS)}
            """,
            uuid7(), observation.tenant_id, observation.id, observation.occurred_at,
            observation.evidence_id, identity_snapshot.id,
            identity_snapshot.snapshot_hash, identity_snapshot.resolution_status,
            reason, self.contract_version, dedupe,
            json.dumps(payload, sort_keys=True),
        )
        assert row is not None
        return _outbox(row)

    async def claim(
        self, *, worker_id: str, batch_size: int, lease_seconds: int,
        conn: asyncpg.Connection,
    ) -> list[PerceptionKnowledgeOutboxRow]:
        if not worker_id.strip() or batch_size < 1 or lease_seconds < 1:
            raise ValidationError("knowledge claim parameters are invalid")
        rows = await conn.fetch(
            f"""
            WITH ranked AS (
              SELECT id,tenant_id,available_at,created_at,
                     row_number() OVER (
                       PARTITION BY tenant_id ORDER BY available_at,created_at,id
                     ) AS tenant_rank
                FROM perception_knowledge_outbox
               WHERE (status='pending' AND available_at<=now())
                  OR (status='leased' AND lease_expires_at<=now())
            ), candidates AS (
              SELECT item.id FROM perception_knowledge_outbox item
              JOIN ranked ON ranked.id=item.id
              ORDER BY ranked.tenant_rank,ranked.available_at,ranked.created_at,item.id
              LIMIT $1 FOR UPDATE OF item SKIP LOCKED
            )
            UPDATE perception_knowledge_outbox item
               SET status='leased',lease_owner=$2,
                   lease_expires_at=now()+make_interval(secs=>$3),
                   attempt_count=item.attempt_count+1,updated_at=now()
              FROM candidates WHERE item.id=candidates.id
            RETURNING {','.join('item.' + name for name in _OUTBOX_COLUMNS)}
            """,
            batch_size, worker_id, lease_seconds,
        )
        return [_outbox(row) for row in rows]

    async def complete(
        self, item_id: UUID, *, tenant_id: UUID, worker_id: str,
        conn: asyncpg.Connection,
    ) -> PerceptionKnowledgeOutboxRow:
        row = await conn.fetchrow(
            f"""
            UPDATE perception_knowledge_outbox
               SET status='completed',completed_at=now(),lease_owner=NULL,
                   lease_expires_at=NULL,updated_at=now()
             WHERE id=$1 AND tenant_id=$2 AND status='leased' AND lease_owner=$3
            RETURNING {','.join(_OUTBOX_COLUMNS)}
            """,
            item_id, tenant_id, worker_id,
        )
        if row is None:
            raise ValidationError("knowledge item is not leased by this worker")
        return _outbox(row)

    async def retry(
        self, item_id: UUID, *, tenant_id: UUID, worker_id: str, error: str,
        delay_seconds: int, max_attempts: int, conn: asyncpg.Connection,
    ) -> PerceptionKnowledgeOutboxRow:
        row = await conn.fetchrow(
            f"""
            UPDATE perception_knowledge_outbox
               SET status=CASE WHEN attempt_count >= $6 THEN 'dead_letter' ELSE 'pending' END,
                   available_at=now()+make_interval(secs=>$5),lease_owner=NULL,
                   lease_expires_at=NULL,last_error=$4,updated_at=now()
             WHERE id=$1 AND tenant_id=$2 AND status='leased' AND lease_owner=$3
            RETURNING {','.join(_OUTBOX_COLUMNS)}
            """,
            item_id, tenant_id, worker_id, error[:2000], delay_seconds, max_attempts,
        )
        if row is None:
            raise ValidationError("knowledge item is not leased by this worker")
        return _outbox(row)


_STATUS_SENTENCE = re.compile(
    r"(?P<subject>[A-Z][A-Za-z0-9 _./-]{1,80}?)\s+"
    r"(?:is|are|was|were)\s+(?P<negative>not\s+)?"
    r"(?P<state>complete|completed|done|blocked|in progress|pending|ready|failed|open|closed)\b"
)


class DeterministicClaimExtractor:
    """Conservative, replayable extractor; model extraction can follow later."""

    name = "fyralis-deterministic-claim-extractor"
    version = "1.0.0"

    def extract(
        self, observation: ObservationRow, *, extraction_run_id: UUID,
    ) -> tuple[PerceptionClaimCreate, ...]:
        if observation.evidence_id is None:
            return ()
        values: list[PerceptionClaimCreate] = []
        content = observation.content if isinstance(observation.content, dict) else {}
        structured = content.get("_perception_claims", [])
        for item in structured if isinstance(structured, list) else ():
            if not isinstance(item, dict):
                continue
            subject = item.get("subject_ref")
            predicate = item.get("predicate")
            if not isinstance(subject, dict) or not subject or not isinstance(predicate, str):
                continue
            quote = item.get("quote")
            if not isinstance(quote, str) or not quote:
                quote = observation.content_text
                start = 0
            else:
                start = observation.content_text.find(quote)
                if start < 0:
                    continue
            values.append(
                PerceptionClaimCreate(
                    tenant_id=observation.tenant_id,
                    evidence_id=observation.evidence_id,
                    observation_id=observation.id,
                    claimant_ref=(
                        item.get("claimant_ref")
                        if isinstance(item.get("claimant_ref"), dict)
                        else ({"type": "actor", "id": str(observation.actor_id)}
                              if observation.actor_id else None)
                    ),
                    subject_ref=subject,
                    predicate=predicate.strip().lower().replace(" ", "_"),
                    object_value=item.get("object_value"),
                    modality=item.get("modality", "asserted"),
                    polarity=item.get("polarity", "positive"),
                    confidence=float(item.get("confidence", 1.0)),
                    evidence_span=span_for_text(
                        observation.content_text, start, start + len(quote)
                    ),
                    extractor_kind="deterministic",
                    extractor_name=self.name,
                    extractor_version=self.version,
                    extraction_run_id=extraction_run_id,
                )
            )
        if values:
            return tuple(values)
        for match in _STATUS_SENTENCE.finditer(observation.content_text):
            subject = "-".join(re.findall(r"[a-z0-9]+", match.group("subject").lower()))
            values.append(
                PerceptionClaimCreate(
                    tenant_id=observation.tenant_id,
                    evidence_id=observation.evidence_id,
                    observation_id=observation.id,
                    claimant_ref=(
                        {"type": "actor", "id": str(observation.actor_id)}
                        if observation.actor_id else None
                    ),
                    subject_ref={"type": "topic_phrase", "id": subject},
                    predicate="status",
                    object_value=match.group("state").lower(),
                    polarity="negative" if match.group("negative") else "positive",
                    confidence=0.85,
                    evidence_span=span_for_text(
                        observation.content_text, match.start(), match.end()
                    ),
                    extractor_kind="deterministic",
                    extractor_name=self.name,
                    extractor_version=self.version,
                    extraction_run_id=extraction_run_id,
                )
            )
        return tuple(values)


class PerceptionKnowledgeSnapshotService:
    def __init__(self, extractor: DeterministicClaimExtractor | None = None) -> None:
        self.extractor = extractor or DeterministicClaimExtractor()
        self._claims = PerceptionClaimRepository()

    async def settle(
        self,
        item: PerceptionKnowledgeOutboxRow,
        observation: ObservationRow,
        *,
        conn: asyncpg.Connection,
    ) -> PerceptionKnowledgeSnapshot:
        extraction_run_id = uuid7()
        existing_claim_count = await conn.fetchval(
            "SELECT count(*) FROM perception_claims "
            "WHERE tenant_id=$1 AND observation_id=$2 AND status='active'",
            item.tenant_id, item.observation_id,
        )
        if item.event_kind == "identity.ready_for_knowledge" and not existing_claim_count:
            for claim in self.extractor.extract(
                observation, extraction_run_id=extraction_run_id
            ):
                await self._claims._insert_in_transaction(claim, conn=conn)
        rows = await conn.fetch(
            """
            SELECT id,claim_key FROM perception_claims
             WHERE tenant_id=$1 AND observation_id=$2 AND status='active'
             ORDER BY id
            """,
            item.tenant_id, item.observation_id,
        )
        claim_ids = tuple(row["id"] for row in rows)
        claim_set_hash = hashlib.sha256(
            json.dumps(
                [{"id": str(row["id"]), "claim_key": row["claim_key"]} for row in rows],
                sort_keys=True, separators=(",", ":"),
            ).encode()
        ).hexdigest()
        manifest = {
            "tenant_id": str(item.tenant_id),
            "observation_id": str(item.observation_id),
            "evidence_id": str(item.evidence_id),
            "identity_snapshot_id": str(item.identity_snapshot_id),
            "identity_snapshot_hash": item.identity_snapshot_hash,
            "identity_resolution_status": item.identity_resolution_status,
            "claim_ids": [str(value) for value in claim_ids],
            "claim_set_hash": claim_set_hash,
            "extractor": {"name": self.extractor.name, "version": self.extractor.version},
        }
        snapshot_hash = hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        row = await conn.fetchrow(
            f"""
            INSERT INTO perception_knowledge_snapshots (
              id,tenant_id,observation_id,observation_occurred_at,evidence_id,
              identity_snapshot_id,identity_snapshot_hash,identity_resolution_status,
              claim_ids,claim_set_hash,extractor_name,extractor_version,manifest,snapshot_hash
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14)
            ON CONFLICT (tenant_id,snapshot_hash) DO NOTHING
            RETURNING {','.join(_SNAPSHOT_COLUMNS)}
            """,
            uuid7(), item.tenant_id, item.observation_id,
            item.observation_occurred_at, item.evidence_id,
            item.identity_snapshot_id, item.identity_snapshot_hash,
            item.identity_resolution_status, list(claim_ids), claim_set_hash,
            self.extractor.name, self.extractor.version,
            json.dumps(manifest, sort_keys=True), snapshot_hash,
        )
        if row is None:
            row = await conn.fetchrow(
                f"SELECT {','.join(_SNAPSHOT_COLUMNS)} "
                "FROM perception_knowledge_snapshots "
                "WHERE tenant_id=$1 AND snapshot_hash=$2",
                item.tenant_id, snapshot_hash,
            )
        assert row is not None
        snapshot = _snapshot(row)
        await EpisodeIntakeRepository().enqueue_knowledge_settled(
            observation, snapshot, conn=conn
        )
        return snapshot


class PerceptionKnowledgeWorker:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._intake = PerceptionKnowledgeIntakeRepository()
        self._service = PerceptionKnowledgeSnapshotService()

    async def process_claimed(
        self, item: PerceptionKnowledgeOutboxRow, *, worker_id: str
    ) -> None:
        from services.domain.observations.repo import ObservationRepository

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT set_config('app.current_tenant',$1::text,true)",
                    str(item.tenant_id),
                )
                observation = await ObservationRepository(self._pool).get_by_id(
                    item.observation_id, item.tenant_id, conn=conn
                )
                if observation is None or observation.evidence_id != item.evidence_id:
                    raise ValidationError("knowledge intake observation/evidence is stale")
                current_hash = await conn.fetchval(
                    "SELECT snapshot_hash FROM identity_resolution_snapshots "
                    "WHERE tenant_id=$1 AND id=$2 AND observation_id=$3 "
                    "AND observation_occurred_at=$4",
                    item.tenant_id, item.identity_snapshot_id,
                    item.observation_id, item.observation_occurred_at,
                )
                if current_hash != item.identity_snapshot_hash:
                    raise ValidationError("knowledge intake identity snapshot is stale")
                await self._service.settle(item, observation, conn=conn)
                await self._intake.complete(
                    item.id, tenant_id=item.tenant_id, worker_id=worker_id, conn=conn
                )

    async def run_once(
        self, *, worker_id: str, batch_size: int = 50, lease_seconds: int = 60,
        retry_delay_seconds: int = 5, max_attempts: int = 5,
    ) -> int:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                claimed = await self._intake.claim(
                    worker_id=worker_id, batch_size=batch_size,
                    lease_seconds=lease_seconds, conn=conn,
                )
        for item in claimed:
            try:
                await self.process_claimed(item, worker_id=worker_id)
            except Exception as exc:  # noqa: BLE001 - durable retry owns failures
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await self._intake.retry(
                            item.id, tenant_id=item.tenant_id, worker_id=worker_id,
                            error=f"{type(exc).__name__}: {exc}",
                            delay_seconds=retry_delay_seconds,
                            max_attempts=max_attempts, conn=conn,
                        )
        return len(claimed)


__all__ = [
    "DeterministicClaimExtractor", "PerceptionKnowledgeIntakeRepository",
    "PerceptionKnowledgeOutboxRow", "PerceptionKnowledgeSnapshot",
    "PerceptionKnowledgeSnapshotService", "PerceptionKnowledgeWorker",
]
