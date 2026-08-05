"""Exactly-once settled-episode handoff into the Think trigger queue."""

from __future__ import annotations

import json

import asyncpg

from services.domain.reasoning_ingress import reasoning_ingress_mode
from services.domain.triggers import enqueue_trigger

from .handoff import EpisodeSnapshotOutboxRepository, EpisodeSnapshotOutboxRow
from .reasoning import EpisodeReasoningInputService


class EpisodeReasoningHandoffWorker:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._outbox = EpisodeSnapshotOutboxRepository()
        self._inputs = EpisodeReasoningInputService()

    async def process_claimed(
        self, item: EpisodeSnapshotOutboxRow, *, worker_id: str
    ) -> None:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "SELECT set_config('app.current_tenant',$1::text,true)",
                    str(item.tenant_id),
                )
                batch = await self._inputs.load(item, conn=conn)
                mode = await reasoning_ingress_mode(conn, tenant_id=item.tenant_id)
                if mode == "episode" or item.mode == "query_answer":
                    observations = batch.observations
                    primary_observation_id = (
                        observations[0]["observation_id"] if observations else None
                    )
                    seed_text = "\n\n".join(
                        str(value.get("content_text") or "") for value in observations
                    )[:8000]
                    assert batch.snapshot.settlement is not None
                    payload = {
                        "episode_snapshot_id": str(item.episode_snapshot_id),
                        "episode_snapshot_hash": item.episode_snapshot_hash,
                        "episode_id": str(item.episode_id),
                        "topic_id": str(item.topic_id),
                        "episode_mode": item.mode,
                        "observation_ids": [
                            str(value) for value in batch.snapshot.observation_ids
                        ],
                        "evidence_ids": [
                            str(value) for value in batch.snapshot.evidence_ids
                        ],
                        "claim_ids": [str(value) for value in batch.snapshot.claim_ids],
                        "contradiction_ids": [
                            str(value.id) for value in batch.snapshot.contradictions
                        ],
                        "requester_actor_id": (
                            str(item.requester_actor_id)
                            if item.requester_actor_id else None
                        ),
                        "query_text": item.query_text,
                        "seed_natural_text": seed_text,
                        "seed_occurred_at": (
                            batch.snapshot.settlement.event_time_watermark.isoformat()
                        ),
                        "input_contract": "episode-reasoning-v1",
                    }
                    existing = await conn.fetchrow(
                        "SELECT tenant_id,trigger_kind,trigger_subkind,payload "
                        "FROM think_trigger_queue WHERE id=$1",
                        item.id,
                    )
                    if existing is None:
                        await enqueue_trigger(
                            conn,
                            trigger_id=item.id,
                            tenant_id=item.tenant_id,
                            trigger_kind="T1",
                            trigger_subkind="episode_snapshot",
                            observation_id=primary_observation_id,
                            payload=payload,
                        )
                    else:
                        existing_payload = existing["payload"]
                        if isinstance(existing_payload, str):
                            existing_payload = json.loads(existing_payload)
                        if (
                            existing["tenant_id"] != item.tenant_id
                            or existing["trigger_kind"] != "T1"
                            or existing["trigger_subkind"] != "episode_snapshot"
                            or existing_payload.get("episode_snapshot_hash")
                            != item.episode_snapshot_hash
                        ):
                            raise ValueError(
                                "snapshot handoff trigger id maps to different work"
                            )
                await self._outbox.complete(
                    item.id, tenant_id=item.tenant_id,
                    worker_id=worker_id, conn=conn,
                )

    async def run_once(
        self, *, worker_id: str, batch_size: int = 50, lease_seconds: int = 60,
        retry_delay_seconds: int = 5, max_attempts: int = 5,
    ) -> int:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                claimed = await self._outbox.claim(
                    worker_id=worker_id, batch_size=batch_size,
                    lease_seconds=lease_seconds, conn=conn,
                )
        for item in claimed:
            try:
                await self.process_claimed(item, worker_id=worker_id)
            except Exception as exc:  # noqa: BLE001 - durable retry owns failures
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await self._outbox.retry(
                            item.id, tenant_id=item.tenant_id,
                            worker_id=worker_id,
                            error=f"{type(exc).__name__}: {exc}",
                            delay_seconds=retry_delay_seconds,
                            max_attempts=max_attempts, conn=conn,
                        )
        return len(claimed)


__all__ = ["EpisodeReasoningHandoffWorker"]
