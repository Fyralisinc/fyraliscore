"""Durable identity worker that gates episode intake on a sealed snapshot."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import asyncpg

from lib.shared.errors import ValidationError
from services.domain.episodes.intake import EpisodeIntakeRepository
from services.domain.evidence.repo import SourceEvidenceRepository
from services.domain.observations.repo import ObservationRepository

from .capabilities import capability_snapshot
from .foundation import ResolutionRunCreate
from .foundation_repo import ResolutionRunRepository
from .intake import IdentityIntakeRepository, IdentityOutboxRow
from .registrar import ObservationMentionRegistrar
from .service import IdentityResolutionService


def _input_hash(item: IdentityOutboxRow) -> str:
    value = {
        "outbox_id": str(item.id),
        "observation_id": str(item.observation_id),
        "evidence_id": str(item.evidence_id),
        "event_kind": item.event_kind,
        "reason": item.reason,
        "payload": item.payload,
        "resolver_version": IdentityResolutionService.resolver_version,
        "policy_version": IdentityResolutionService.policy_version,
    }
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class IdentityResolutionWorker:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        intake: IdentityIntakeRepository | None = None,
        registrar: ObservationMentionRegistrar | None = None,
        service: IdentityResolutionService | None = None,
    ) -> None:
        self._pool = pool
        self._intake = intake or IdentityIntakeRepository()
        self._registrar = registrar or ObservationMentionRegistrar()
        self._service = service or IdentityResolutionService()
        self._runs = ResolutionRunRepository()

    async def process_claimed(
        self, item: IdentityOutboxRow, *, worker_id: str
    ) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT set_config('app.current_tenant', $1::text, true)",
                    str(item.tenant_id),
                )
                observation = await ObservationRepository(self._pool).get_by_id(
                    item.observation_id, item.tenant_id, conn=conn
                )
                if observation is None or observation.evidence_id != item.evidence_id:
                    raise ValidationError("identity intake observation/evidence is stale")
                evidence = await SourceEvidenceRepository().get(
                    item.evidence_id, tenant_id=item.tenant_id, conn=conn
                )
                if evidence is None:
                    raise ValidationError("identity intake evidence is missing")
                mentions = await self._registrar.register(
                    observation, evidence, conn=conn
                )
                run = await self._runs.start(
                    ResolutionRunCreate(
                        tenant_id=item.tenant_id,
                        input_kind=(
                            "reprocess"
                            if item.event_kind == "identity.reresolution_requested"
                            else "observation"
                        ),
                        observation_id=item.observation_id,
                        observation_occurred_at=item.observation_occurred_at,
                        input_hash=_input_hash(item),
                        resolver_name=self._service.resolver_name,
                        resolver_version=self._service.resolver_version,
                        policy_version=self._service.policy_version,
                        capability_snapshot=capability_snapshot(),
                    ),
                    conn=conn,
                )
                snapshot = await self._service.resolve(
                    run=run,
                    mentions=mentions,
                    access_policy_hash=evidence.access_policy_hash,
                    conn=conn,
                    evaluated_at=evidence.source_recorded_at,
                )
                if run.status == "running":
                    await self._runs.finish(
                        run.id,
                        tenant_id=item.tenant_id,
                        status="completed",
                        result_hash=snapshot.snapshot_hash,
                        conn=conn,
                    )
                await EpisodeIntakeRepository().enqueue_identity_resolved(
                    observation, snapshot, conn=conn
                )
                await conn.execute(
                    """
                    INSERT INTO identity_change_events (
                      id, tenant_id, event_kind, aggregate_ref, evidence_id,
                      payload, dedupe_key
                    ) VALUES (
                      gen_random_uuid(), $1, 'identity.snapshot_created',
                      $2::jsonb, $3, $4::jsonb, $5
                    ) ON CONFLICT (tenant_id, dedupe_key) DO NOTHING
                    """,
                    item.tenant_id,
                    json.dumps(
                        {"kind": "identity_snapshot", "id": str(snapshot.id)},
                        sort_keys=True,
                    ),
                    item.evidence_id,
                    json.dumps(
                        {
                            "observation_id": str(item.observation_id),
                            "snapshot_hash": snapshot.snapshot_hash,
                            "resolution_status": snapshot.resolution_status,
                        },
                        sort_keys=True,
                    ),
                    f"snapshot:{snapshot.snapshot_hash}",
                )
                await self._intake.complete(
                    item.id,
                    tenant_id=item.tenant_id,
                    worker_id=worker_id,
                    conn=conn,
                )

    async def run_once(
        self,
        *,
        worker_id: str,
        batch_size: int = 50,
        lease_seconds: int = 60,
        retry_delay_seconds: int = 5,
        max_attempts: int = 5,
    ) -> int:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                claimed = await self._intake.claim(
                    worker_id=worker_id,
                    batch_size=batch_size,
                    lease_seconds=lease_seconds,
                    conn=conn,
                )
        for item in claimed:
            try:
                await self.process_claimed(item, worker_id=worker_id)
            except Exception as exc:  # noqa: BLE001 - durable retry owns failures
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await self._intake.retry(
                            item.id,
                            tenant_id=item.tenant_id,
                            worker_id=worker_id,
                            error=f"{type(exc).__name__}: {exc}",
                            delay_seconds=retry_delay_seconds,
                            max_attempts=max_attempts,
                            conn=conn,
                        )
        return len(claimed)


__all__ = ["IdentityResolutionWorker"]
