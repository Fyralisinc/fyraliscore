from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.contracts.agency import (
    EpisodeStageFate,
    EpisodeStageLink,
    InterventionEpisode,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.intervention_runtime import (
    InterventionManifestWorkRepo,
    InterventionManifestWorkStatus,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _command_result(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    writer_id: str,
    object_type: str,
    object_id: UUID,
    object_version: int,
    key: str,
) -> UUID:
    result_id = uuid7()
    await conn.execute(
        """
        INSERT INTO agency_command_results (
          id, tenant_id, command_id, writer_id, semantic_idempotency_key,
          request_digest, command_kind, status, command,
          processing_authority_fingerprint, writer_scope_id, writer_epoch,
          object_type, object_id, object_version, result
        ) VALUES (
          $1,$2,$3,$4,$5,$6,'repo_test','applied','{}'::jsonb,
          $7,'repo-test-scope',1,$8,$9,$10,'{}'::jsonb
        )
        """,
        result_id,
        tenant_id,
        uuid7(),
        writer_id,
        key,
        f"{result_id.int:064x}"[-64:],
        "a" * 64,
        object_type,
        object_id,
        object_version,
    )
    return result_id


async def _episode_fixture(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    now: datetime,
) -> tuple[UUID, str, InterventionEpisode]:
    episode_id = uuid7()
    spec_digest = "b" * 64
    initial = InterventionEpisode(
        episode_id=episode_id,
        tenant_id=tenant_id,
        intervention_spec_digest=spec_digest,
        stage_links=(
            EpisodeStageLink(
                stage="belief",
                fate=EpisodeStageFate.PRESENT,
                object_ref=f"belief:{uuid7()}",
                writer_id="EpistemicApplier",
            ),
        ),
        created_at=now,
        updated_at=now,
    )
    result_id = await _command_result(
        conn,
        tenant_id=tenant_id,
        writer_id="EpisodeCoordinator",
        object_type="intervention_episode",
        object_id=episode_id,
        object_version=1,
        key=f"episode:{episode_id}:1",
    )
    await conn.execute(
        """
        INSERT INTO intervention_episode_heads (
          tenant_id, episode_id, episode_kind, current_version,
          current_episode_digest, intervention_spec_digest, created_at, updated_at
        ) VALUES ($1,$2,'intervention',1,$3,$4,$5,$5)
        """,
        tenant_id,
        episode_id,
        initial.episode_digest,
        spec_digest,
        now,
    )
    await conn.execute(
        """
        INSERT INTO intervention_episode_versions (
          id, tenant_id, episode_id, aggregate_version, episode_digest,
          episode, command_result_id
        ) VALUES ($1,$2,$3,1,$4,$5::jsonb,$6)
        """,
        uuid7(),
        tenant_id,
        episode_id,
        initial.episode_digest,
        json.dumps(initial.model_dump(mode="json")),
        result_id,
    )
    return episode_id, spec_digest, initial


async def _workflow_event(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    episode_id: UUID,
    spec_digest: str,
    now: datetime,
    workflow_id: UUID | None = None,
) -> tuple[UUID, UUID]:
    workflow_id = workflow_id or uuid7()
    result_id = await _command_result(
        conn,
        tenant_id=tenant_id,
        writer_id="AgencyStateApplier",
        object_type="workflow_run",
        object_id=workflow_id,
        object_version=1,
        key=f"workflow:{workflow_id}:1",
    )
    await conn.execute(
        """
        INSERT INTO agency_workflow_run_heads (
          tenant_id, workflow_run_id, episode_id, intervention_spec_digest,
          current_version, current_state, current_snapshot_digest, updated_at
        ) VALUES ($1,$2,$3,$4,1,'planned',$5,$6)
        """,
        tenant_id,
        workflow_id,
        episode_id,
        spec_digest,
        "c" * 64,
        now,
    )
    await conn.execute(
        """
        INSERT INTO agency_workflow_run_versions (
          id, tenant_id, workflow_run_id, aggregate_version, state,
          snapshot_digest, snapshot, command_result_id
        ) VALUES ($1,$2,$3,1,'planned',$4,'{}'::jsonb,$5)
        """,
        uuid7(),
        tenant_id,
        workflow_id,
        "c" * 64,
        result_id,
    )
    event_id = uuid7()
    await conn.execute(
        """
        INSERT INTO agency_canonical_events (
          id, tenant_id, command_result_id, writer_id, object_type,
          object_id, object_version, semantic_transition,
          intervention_spec_digest, event_payload, created_at
        ) VALUES (
          $1,$2,$3,'AgencyStateApplier','workflow_run',$4,1,'planned',
          $5,'{}'::jsonb,$6
        )
        """,
        event_id,
        tenant_id,
        result_id,
        workflow_id,
        spec_digest,
        now,
    )
    return event_id, workflow_id


async def _install_applied_episode_version(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    prior: InterventionEpisode,
    workflow_id: UUID,
    now: datetime,
) -> InterventionEpisode:
    applied = prior.model_copy(
        update={
            "stage_links": (
                *prior.stage_links,
                EpisodeStageLink(
                    stage="workflow",
                    fate=EpisodeStageFate.PRESENT,
                    object_ref=f"workflow:{workflow_id}",
                    writer_id="AgencyStateApplier",
                ),
            ),
            "updated_at": now,
        }
    )
    result_id = await _command_result(
        conn,
        tenant_id=tenant_id,
        writer_id="EpisodeCoordinator",
        object_type="intervention_episode",
        object_id=prior.episode_id,
        object_version=2,
        key=f"episode:{prior.episode_id}:2",
    )
    await conn.execute(
        """
        UPDATE intervention_episode_heads
        SET current_version=2, current_episode_digest=$3, updated_at=$4
        WHERE tenant_id=$1 AND episode_id=$2
        """,
        tenant_id,
        prior.episode_id,
        applied.episode_digest,
        now,
    )
    await conn.execute(
        """
        INSERT INTO intervention_episode_versions (
          id, tenant_id, episode_id, aggregate_version, episode_digest,
          episode, command_result_id
        ) VALUES ($1,$2,$3,2,$4,$5::jsonb,$6)
        """,
        uuid7(),
        tenant_id,
        prior.episode_id,
        applied.episode_digest,
        json.dumps(applied.model_dump(mode="json")),
        result_id,
    )
    return applied


async def test_discovery_is_idempotent_and_revalidates_exact_source(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = InterventionManifestWorkRepo()
    tenant_id = uuid7()
    now = datetime.now(timezone.utc) + timedelta(minutes=1)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name, is_demo) VALUES ($1,'manifest repo',FALSE)",
            tenant_id,
        )
        episode_id, spec_digest, _ = await _episode_fixture(
            conn,
            tenant_id=tenant_id,
            now=now,
        )
        event_id, workflow_id = await _workflow_event(
            conn,
            tenant_id=tenant_id,
            episode_id=episode_id,
            spec_digest=spec_digest,
            now=now + timedelta(seconds=1),
        )

        assert await repo.discover_ready_work(
            conn,
            now=now + timedelta(seconds=2),
            limit=100,
            tenant_id=tenant_id,
        ) == 1
        first = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=now + timedelta(seconds=3),
        )
        duplicate = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=now + timedelta(seconds=4),
        )
        assert first is not None and duplicate is not None
        assert duplicate.id == first.id
        assert first.episode_id == episode_id
        assert first.stage == "workflow"
        assert first.object_ref == f"workflow:{workflow_id}"
        assert first.source_object_version == 1
        assert await repo.discover_ready_work(
            conn,
            now=now + timedelta(seconds=5),
            limit=100,
            tenant_id=tenant_id,
        ) == 0

        (claim,) = await repo.claim_ready_work(
            conn,
            worker_id="manifest:test-a",
            now=now + timedelta(seconds=6),
            lease_duration=timedelta(seconds=10),
            limit=1,
        )
        assert claim.claim_token is not None
        context = await repo.load_claimed_context(
            conn,
            tenant_id=tenant_id,
            work_item_id=claim.id,
            worker_id="manifest:test-a",
            claim_token=claim.claim_token,
            now=now + timedelta(seconds=7),
        )
        assert context.current_episode_version == 1
        assert context.episode.episode_id == episode_id
        assert context.work_item.object_ref == f"workflow:{workflow_id}"

        await conn.execute(
            """
            UPDATE agency_canonical_events
            SET object_version=2
            WHERE id=$1
            """,
            event_id,
        )
        with pytest.raises(
            InvariantViolation,
            match="exact command result",
        ):
            await repo.load_claimed_context(
                conn,
                tenant_id=tenant_id,
                work_item_id=claim.id,
                worker_id="manifest:test-a",
                claim_token=claim.claim_token,
                now=now + timedelta(seconds=8),
            )


async def test_claim_reclaim_retry_stale_fence_and_terminal_transitions(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = InterventionManifestWorkRepo()
    tenant_id = uuid7()
    now = datetime.now(timezone.utc) + timedelta(minutes=1)
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name, is_demo) VALUES ($1,'manifest leases',FALSE)",
            tenant_id,
        )
        episode_id, spec_digest, initial = await _episode_fixture(
            conn,
            tenant_id=tenant_id,
            now=now,
        )
        event_id, workflow_id = await _workflow_event(
            conn,
            tenant_id=tenant_id,
            episode_id=episode_id,
            spec_digest=spec_digest,
            now=now + timedelta(seconds=1),
        )
        item = await repo.discover_from_event(
            conn,
            source_event_id=event_id,
            now=now + timedelta(seconds=2),
        )
        assert item is not None

        (first,) = await repo.claim_ready_work(
            conn,
            worker_id="manifest:test-a",
            now=now + timedelta(seconds=3),
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
        assert first.attempt_count == 1
        assert first.claim_token is not None
        assert await repo.claim_ready_work(
            conn,
            worker_id="manifest:test-b",
            now=now + timedelta(seconds=7),
            lease_duration=timedelta(seconds=5),
            limit=1,
        ) == ()

        (recovered,) = await repo.claim_ready_work(
            conn,
            worker_id="manifest:test-b",
            now=now + timedelta(seconds=9),
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
        assert recovered.attempt_count == 2
        assert recovered.claim_token is not None
        assert recovered.claim_token != first.claim_token
        with pytest.raises(InvariantViolation, match="current live fence token"):
            await repo.schedule_retry(
                conn,
                tenant_id=tenant_id,
                work_item_id=first.id,
                worker_id="manifest:test-a",
                claim_token=first.claim_token,
                now=now + timedelta(seconds=10),
                next_attempt_at=now + timedelta(seconds=20),
                failure_class="stale_worker",
                failure_reason="must not overwrite the recovered claim",
            )

        retry = await repo.schedule_retry(
            conn,
            tenant_id=tenant_id,
            work_item_id=recovered.id,
            worker_id="manifest:test-b",
            claim_token=recovered.claim_token,
            now=now + timedelta(seconds=10),
            next_attempt_at=now + timedelta(seconds=20),
            failure_class="episode_cas",
            failure_reason="another stage advanced first",
        )
        assert retry.status is InterventionManifestWorkStatus.RETRY_SCHEDULED
        assert retry.claim_token is None
        assert retry.last_failure_class == "episode_cas"
        assert await repo.claim_ready_work(
            conn,
            worker_id="manifest:test-c",
            now=now + timedelta(seconds=19),
            lease_duration=timedelta(seconds=5),
            limit=1,
        ) == ()

        (third,) = await repo.claim_ready_work(
            conn,
            worker_id="manifest:test-c",
            now=now + timedelta(seconds=20),
            lease_duration=timedelta(seconds=10),
            limit=1,
        )
        assert third.attempt_count == 3
        assert third.claim_token is not None
        await _install_applied_episode_version(
            conn,
            tenant_id=tenant_id,
            prior=initial,
            workflow_id=workflow_id,
            now=now + timedelta(seconds=21),
        )
        applied = await repo.mark_applied(
            conn,
            tenant_id=tenant_id,
            work_item_id=third.id,
            worker_id="manifest:test-c",
            claim_token=third.claim_token,
            applied_episode_version=2,
            now=now + timedelta(seconds=22),
        )
        assert applied.status is InterventionManifestWorkStatus.APPLIED
        assert applied.applied_episode_version == 2
        assert applied.claim_token is None
        assert await repo.claim_ready_work(
            conn,
            worker_id="manifest:test-d",
            now=now + timedelta(seconds=40),
            lease_duration=timedelta(seconds=5),
            limit=1,
        ) == ()

        # A second episode supplies a distinct terminal-failure path.
        second_episode_id, second_digest, _ = await _episode_fixture(
            conn,
            tenant_id=tenant_id,
            now=now + timedelta(minutes=1),
        )
        second_event_id, _ = await _workflow_event(
            conn,
            tenant_id=tenant_id,
            episode_id=second_episode_id,
            spec_digest=second_digest,
            now=now + timedelta(minutes=1, seconds=1),
        )
        second_item = await repo.discover_from_event(
            conn,
            source_event_id=second_event_id,
            now=now + timedelta(minutes=1, seconds=2),
        )
        assert second_item is not None
        (terminal_claim,) = await repo.claim_ready_work(
            conn,
            worker_id="manifest:test-terminal",
            now=now + timedelta(minutes=1, seconds=3),
            lease_duration=timedelta(seconds=10),
            limit=1,
        )
        failed = await repo.fail_work_terminally(
            conn,
            tenant_id=tenant_id,
            work_item_id=terminal_claim.id,
            worker_id="manifest:test-terminal",
            claim_token=terminal_claim.claim_token,
            now=now + timedelta(minutes=1, seconds=4),
            failure_class="source_contract_violation",
            failure_reason="source can never be linked safely",
        )
        assert failed.status is InterventionManifestWorkStatus.FAILED_TERMINAL
        assert failed.last_failure_class == "source_contract_violation"
