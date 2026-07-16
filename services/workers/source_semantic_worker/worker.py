"""Leased poller for durable grounding-to-source-semantics work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import structlog

from services.domain.source_semantics.processor import GroundedBeliefProcessor
from services.domain.source_semantics.repo import (
    SourceSemanticRepo,
    SourceSemanticWorkItem,
)


@dataclass(slots=True)
class SourceSemanticWorkerStats:
    batches: int = 0
    claimed: int = 0
    belief_applied: int = 0
    no_admission: int = 0
    retries_scheduled: int = 0
    terminal_failures: int = 0
    stale_claims: int = 0


class SourceSemanticWorker:
    """Own the asynchronous semantic fate for each queued grounding trace."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        worker_id: str,
        repo: SourceSemanticRepo | None = None,
        processor: GroundedBeliefProcessor | None = None,
        lease_duration: timedelta = timedelta(minutes=2),
        retry_delay: timedelta = timedelta(seconds=30),
        max_attempts: int = 5,
        logger: Any | None = None,
    ) -> None:
        if not worker_id.strip():
            raise ValueError("worker_id must be non-empty")
        if lease_duration <= timedelta(0) or retry_delay <= timedelta(0):
            raise ValueError("lease_duration and retry_delay must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._pool = pool
        self._worker_id = worker_id
        self._repo = repo or SourceSemanticRepo()
        self._processor = processor or GroundedBeliefProcessor(
            source_semantic_repo=self._repo
        )
        self._lease_duration = lease_duration
        self._retry_delay = retry_delay
        self._max_attempts = max_attempts
        self._log = logger or structlog.get_logger(__name__)

    async def process_batch(
        self,
        *,
        limit: int = 25,
        stats: SourceSemanticWorkerStats | None = None,
    ) -> int:
        stats = stats or SourceSemanticWorkerStats()
        claim_time = datetime.now(timezone.utc)
        async with self._pool.acquire() as conn, conn.transaction():
            work_items = await self._repo.claim_ready_work(
                conn,
                worker_id=self._worker_id,
                now=claim_time,
                lease_duration=self._lease_duration,
                limit=limit,
            )
        stats.batches += 1
        stats.claimed += len(work_items)
        for item in work_items:
            await self._process_item(item, stats=stats)
        return len(work_items)

    async def _process_item(
        self,
        item: SourceSemanticWorkItem,
        *,
        stats: SourceSemanticWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                embedding = await self._repo.load_claimed_embedding(
                    conn,
                    tenant_id=item.tenant_id,
                    work_item_id=item.id,
                    worker_id=self._worker_id,
                    claim_token=item.claim_token,
                    now=datetime.now(timezone.utc),
                )
                result = await self._processor.process_trace(
                    conn,
                    tenant_id=item.tenant_id,
                    grounding_trace_id=item.grounding_trace_id,
                    embedding=embedding,
                )
                await self._repo.terminalize_work(
                    conn,
                    tenant_id=item.tenant_id,
                    work_item_id=item.id,
                    worker_id=self._worker_id,
                    claim_token=item.claim_token,
                    disposition=result.disposition,
                    interpretation_id=result.interpretation_id,
                    admission_decision_id=result.admission_decision_id,
                    admitted_model_id=result.model_id,
                    now=datetime.now(timezone.utc),
                )
            if result.model_id is None:
                stats.no_admission += 1
            else:
                stats.belief_applied += 1
        except Exception as exc:  # noqa: BLE001
            await self._record_failure(item, exc=exc, stats=stats)

    async def _record_failure(
        self,
        item: SourceSemanticWorkItem,
        *,
        exc: Exception,
        stats: SourceSemanticWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        now = datetime.now(timezone.utc)
        failure_class = type(exc).__name__
        failure_reason = str(exc)[:1000] or failure_class
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                if item.attempt_count >= self._max_attempts:
                    await self._repo.fail_work_terminally(
                        conn,
                        tenant_id=item.tenant_id,
                        work_item_id=item.id,
                        worker_id=self._worker_id,
                        claim_token=item.claim_token,
                        now=now,
                        failure_class=failure_class,
                        failure_reason=failure_reason,
                    )
                    stats.terminal_failures += 1
                else:
                    await self._repo.schedule_retry(
                        conn,
                        tenant_id=item.tenant_id,
                        work_item_id=item.id,
                        worker_id=self._worker_id,
                        claim_token=item.claim_token,
                        now=now,
                        next_attempt_at=now + self._retry_delay,
                        failure_class=failure_class,
                        failure_reason=failure_reason,
                    )
                    stats.retries_scheduled += 1
        except Exception as transition_exc:  # noqa: BLE001
            stats.stale_claims += 1
            self._log.warning(
                "source_semantic_worker.failure_transition_lost_claim",
                work_item_id=str(item.id),
                grounding_trace_id=str(item.grounding_trace_id),
                processing_error=failure_reason,
                transition_error=str(transition_exc),
            )
            return
        self._log.warning(
            "source_semantic_worker.item_failed",
            work_item_id=str(item.id),
            grounding_trace_id=str(item.grounding_trace_id),
            attempt_count=item.attempt_count,
            failure_class=failure_class,
        )


__all__ = ["SourceSemanticWorker", "SourceSemanticWorkerStats"]
