"""Leased projection of canonical agency events into episode stage manifests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import structlog

from lib.contracts.agency import (
    AgencyWriteContext,
    EpisodeStageFate,
    EpisodeStageLink,
    EpisodeUpdateCommand,
)
from lib.contracts.kernel import (
    ProcessingAuthorityContext,
    RestrictionSet,
    WriterCutoverState,
    WriterScopeEpoch,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.intervention_runtime import (
    InterventionManifestWorkContext,
    InterventionManifestWorkItem,
    InterventionManifestWorkRepo,
)
from services.domain.outcomes import EpisodeCoordinator


@dataclass(slots=True)
class InterventionEpisodeCoordinatorWorkerStats:
    batches: int = 0
    discovered: int = 0
    claimed: int = 0
    applied: int = 0
    already_applied: int = 0
    retries_scheduled: int = 0
    terminal_failures: int = 0
    stale_claims: int = 0


class InterventionEpisodeCoordinatorWorker:
    """Link revalidated canonical objects without owning their semantic state."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        worker_id: str,
        repo: InterventionManifestWorkRepo | None = None,
        coordinator: EpisodeCoordinator | None = None,
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
        self._repo = repo or InterventionManifestWorkRepo()
        self._coordinator = coordinator or EpisodeCoordinator()
        self._lease_duration = lease_duration
        self._retry_delay = retry_delay
        self._max_attempts = max_attempts
        self._log = logger or structlog.get_logger(__name__)

    async def process_batch(
        self,
        *,
        limit: int = 25,
        stats: InterventionEpisodeCoordinatorWorkerStats | None = None,
    ) -> int:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        stats = stats or InterventionEpisodeCoordinatorWorkerStats()
        claim_time = datetime.now(timezone.utc)
        async with self._pool.acquire() as conn, conn.transaction():
            discovered = await self._repo.discover_ready_work(
                conn,
                now=claim_time,
                limit=limit,
            )
            work_items = await self._repo.claim_ready_work(
                conn,
                worker_id=self._worker_id,
                now=claim_time,
                lease_duration=self._lease_duration,
                limit=limit,
            )
        stats.batches += 1
        stats.discovered += discovered
        stats.claimed += len(work_items)
        for item in work_items:
            await self._process_item(item, stats=stats)
        return len(work_items)

    async def _process_item(
        self,
        item: InterventionManifestWorkItem,
        *,
        stats: InterventionEpisodeCoordinatorWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                now = datetime.now(timezone.utc)
                context = await self._repo.load_claimed_context(
                    conn,
                    tenant_id=item.tenant_id,
                    work_item_id=item.id,
                    worker_id=self._worker_id,
                    claim_token=item.claim_token,
                    now=now,
                )
                applied_version, duplicate = await self._project_stage(
                    conn=conn,
                    context=context,
                    now=now,
                )
                await self._repo.mark_applied(
                    conn,
                    tenant_id=item.tenant_id,
                    work_item_id=item.id,
                    worker_id=self._worker_id,
                    claim_token=item.claim_token,
                    applied_episode_version=applied_version,
                    now=now,
                )
            if duplicate:
                stats.already_applied += 1
            else:
                stats.applied += 1
        except Exception as exc:  # noqa: BLE001
            await self._record_failure(item, exc=exc, stats=stats)

    async def _project_stage(
        self,
        *,
        conn: asyncpg.Connection,
        context: InterventionManifestWorkContext,
        now: datetime,
    ) -> tuple[int, bool]:
        item = context.work_item
        episode = context.episode
        desired = EpisodeStageLink(
            stage=item.stage,
            fate=EpisodeStageFate.PRESENT,
            object_ref=item.object_ref,
            writer_id=item.writer_id,
        )
        links = list(episode.stage_links)
        by_stage = {link.stage: index for index, link in enumerate(links)}
        existing_index = by_stage.get(item.stage)
        existing = links[existing_index] if existing_index is not None else None

        if existing is not None and existing.fate is EpisodeStageFate.PRESENT:
            if existing != desired:
                raise InvariantViolation(
                    "INTERVENTION_EPISODE_STAGE_CONFLICT",
                    "a present episode stage cannot be replaced by another object",
                    episode_id=str(item.episode_id),
                    stage=item.stage,
                    existing_object_ref=existing.object_ref,
                    attempted_object_ref=item.object_ref,
                )
        elif existing_index is None:
            links.append(desired)
        else:
            links[existing_index] = desired

        source_digest = item.intervention_spec_digest
        episode_digest = episode.intervention_spec_digest
        if (
            source_digest is not None
            and episode_digest is not None
            and source_digest != episode_digest
        ):
            raise InvariantViolation(
                "INTERVENTION_EPISODE_SPEC_CONFLICT",
                "canonical stage source disagrees with the episode InterventionSpec",
                episode_id=str(item.episode_id),
                stage=item.stage,
            )
        target_digest = episode_digest or source_digest
        if existing == desired and target_digest == episode_digest:
            return context.current_episode_version, True

        logical_now = max(now, episode.updated_at)
        successor = episode.model_copy(
            update={
                "intervention_spec_digest": target_digest,
                "stage_links": tuple(links),
                "updated_at": logical_now,
            }
        )
        command = EpisodeUpdateCommand(
            context=self._write_context(item=item, issued_at=logical_now),
            expected_version=context.current_episode_version,
            episode=successor,
        )
        result = await self._coordinator.apply(
            conn=conn,
            command=command,
            now=logical_now,
        )
        return result.object_version, result.duplicate

    @staticmethod
    def _write_context(
        *,
        item: InterventionManifestWorkItem,
        issued_at: datetime,
    ) -> AgencyWriteContext:
        return AgencyWriteContext(
            command_id=uuid7(),
            tenant_id=item.tenant_id,
            processing_authority=ProcessingAuthorityContext(
                tenant_id=item.tenant_id,
                principal_or_service_id=(
                    "service:intervention-episode-coordinator"
                ),
                purpose="intervention_episode_manifest_projection",
                operation="link_revalidated_stage",
                object_types=RestrictionSet.only("intervention_episode"),
                object_ids=RestrictionSet.only(str(item.episode_id)),
                fields=RestrictionSet.only(
                    "intervention_spec_digest",
                    "stage_links",
                    "updated_at",
                ),
                source_labels=RestrictionSet.only("agency-canonical-event"),
                authority_basis_refs=frozenset(
                    {f"canonical-event:{item.source_event_id}"}
                ),
                policy_version="episode-manifest-projection-v1",
                authority_epoch=1,
                decision_time=issued_at,
                expires_at=issued_at + timedelta(minutes=5),
            ),
            writer_scope_epoch=WriterScopeEpoch(
                scope_id=f"episode-manifest:{item.tenant_id}",
                tenant_id=item.tenant_id,
                semantic_responsibility="intervention_episode",
                source_partition=str(item.tenant_id),
                writer_owner="EpisodeCoordinator",
                epoch=1,
                state=WriterCutoverState.NEW_CANONICAL,
            ),
            idempotency_key=f"episode-manifest:{item.source_event_id}",
            issued_at=issued_at,
            expires_at=issued_at + timedelta(minutes=5),
        )

    async def _record_failure(
        self,
        item: InterventionManifestWorkItem,
        *,
        exc: Exception,
        stats: InterventionEpisodeCoordinatorWorkerStats,
    ) -> None:
        assert item.claim_token is not None
        now = datetime.now(timezone.utc)
        failure_class = type(exc).__name__
        failure_reason = str(exc)[:1000] or failure_class
        terminal = isinstance(exc, InvariantViolation) or (
            item.attempt_count >= self._max_attempts
        )
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                if terminal:
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
                "intervention_episode_coordinator.failure_transition_lost_claim",
                work_item_id=str(item.id),
                source_event_id=str(item.source_event_id),
                processing_error=failure_reason,
                transition_error=str(transition_exc),
            )
            return
        self._log.warning(
            "intervention_episode_coordinator.item_failed",
            work_item_id=str(item.id),
            source_event_id=str(item.source_event_id),
            attempt_count=item.attempt_count,
            failure_class=failure_class,
            terminal=terminal,
        )


__all__ = [
    "InterventionEpisodeCoordinatorWorker",
    "InterventionEpisodeCoordinatorWorkerStats",
]
