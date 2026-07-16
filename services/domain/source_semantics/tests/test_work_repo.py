from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.contracts.source_semantics import SourceSemanticAdmissionDisposition
from lib.embeddings.ollama import EMBEDDING_DIM
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.source_semantics.repo import (
    SourceSemanticRepo,
    SourceSemanticWorkStatus,
)
from services.domain.source_semantics.tests.test_grounded_belief_vertical import (
    _commit_grounding,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_work_head_waits_for_embedding_retries_and_fences_expired_claim(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    await fresh_db.execute(
        """
        INSERT INTO tenants (id, name, is_demo)
        VALUES ($1, 'source semantic work boundary', FALSE)
        """,
        tenant_id,
    )
    repo = SourceSemanticRepo()
    started_at = datetime.now(timezone.utc) + timedelta(minutes=2)

    async with fresh_db.acquire() as conn:
        episode, trace_id = await _commit_grounding(
            conn,
            tenant_id=tenant_id,
            text="NBI is blocked",
            confidence=0.91,
        )
        source_observation_id = (
            episode.mention_detection_command.detection.source_observation_id
        )

        first = await repo.enqueue_work(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=trace_id,
            now=started_at,
        )
        duplicate = await repo.enqueue_work(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=trace_id,
            now=started_at + timedelta(seconds=1),
        )
        assert duplicate.id == first.id
        assert first.status is SourceSemanticWorkStatus.AWAITING_EMBEDDING
        assert first.attempt_count == 0
        assert await repo.claim_ready_work(
            conn,
            worker_id="source-semantics:test-a",
            now=started_at + timedelta(seconds=2),
            lease_duration=timedelta(seconds=5),
            limit=1,
        ) == ()

        embedding = [0.01] * EMBEDDING_DIM
        await conn.execute(
            """
            UPDATE observations
            SET embedding=$1::vector, embedding_pending=FALSE
            WHERE tenant_id=$2 AND id=$3
            """,
            json.dumps(embedding),
            tenant_id,
            source_observation_id,
        )
        (claimed,) = await repo.claim_ready_work(
            conn,
            worker_id="source-semantics:test-a",
            now=started_at + timedelta(seconds=3),
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
        assert claimed.id == first.id
        assert claimed.status is SourceSemanticWorkStatus.PROCESSING
        assert claimed.attempt_count == 1
        assert claimed.claim_token is not None
        loaded_embedding = await repo.load_claimed_embedding(
            conn,
            tenant_id=tenant_id,
            work_item_id=claimed.id,
            worker_id="source-semantics:test-a",
            claim_token=claimed.claim_token,
            now=started_at + timedelta(seconds=4),
        )
        assert loaded_embedding == embedding

        retry = await repo.schedule_retry(
            conn,
            tenant_id=tenant_id,
            work_item_id=claimed.id,
            worker_id="source-semantics:test-a",
            claim_token=claimed.claim_token,
            now=started_at + timedelta(seconds=4),
            next_attempt_at=started_at + timedelta(seconds=10),
            failure_class="embedding_consumer_unavailable",
            failure_reason="temporary test outage",
        )
        assert retry.status is SourceSemanticWorkStatus.RETRY_SCHEDULED
        assert retry.claim_token is None
        assert await repo.claim_ready_work(
            conn,
            worker_id="source-semantics:test-a",
            now=started_at + timedelta(seconds=9),
            lease_duration=timedelta(seconds=5),
            limit=1,
        ) == ()

        (second_claim,) = await repo.claim_ready_work(
            conn,
            worker_id="source-semantics:test-a",
            now=started_at + timedelta(seconds=10),
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
        assert second_claim.attempt_count == 2
        assert second_claim.claim_token is not None
        assert await repo.claim_ready_work(
            conn,
            worker_id="source-semantics:test-b",
            now=started_at + timedelta(seconds=14),
            lease_duration=timedelta(seconds=5),
            limit=1,
        ) == ()

        (recovered,) = await repo.claim_ready_work(
            conn,
            worker_id="source-semantics:test-b",
            now=started_at + timedelta(seconds=16),
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
        assert recovered.attempt_count == 3
        assert recovered.claim_token is not None
        assert recovered.claim_token != second_claim.claim_token

        with pytest.raises(InvariantViolation, match="current live fence token"):
            await repo.fail_work_terminally(
                conn,
                tenant_id=tenant_id,
                work_item_id=second_claim.id,
                worker_id="source-semantics:test-a",
                claim_token=second_claim.claim_token,
                now=started_at + timedelta(seconds=17),
                failure_class="stale_worker",
                failure_reason="must not win after lease recovery",
            )

        assert await conn.fetchval(
            "SELECT count(*) FROM source_semantic_interpretations WHERE tenant_id=$1",
            tenant_id,
        ) == 0
        assert await conn.fetchval(
            "SELECT count(*) FROM source_semantic_admission_decisions WHERE tenant_id=$1",
            tenant_id,
        ) == 0
        assert await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id=$1",
            tenant_id,
        ) == 0

        trace = await conn.fetchrow(
            """
            SELECT gt.source_observation_id, gt.context_snapshot_id,
                   gt.resolution_assessment_id, gt.grounding_admission_id,
                   emd.mention
            FROM grounding_traces gt
            JOIN entity_mention_detections emd
              ON emd.tenant_id=gt.tenant_id
             AND emd.id=gt.entity_mention_detection_id
            WHERE gt.tenant_id=$1 AND gt.id=$2
            """,
            tenant_id,
            trace_id,
        )
        assert trace is not None
        mention = (
            json.loads(trace["mention"])
            if isinstance(trace["mention"], str)
            else trace["mention"]
        )
        interpretation_id = uuid7()
        admission_decision_id = uuid7()
        await conn.execute(
            """
            INSERT INTO source_semantic_interpretations (
                id, tenant_id, grounding_trace_id, source_observation_id,
                context_snapshot_id, entity_mention_id,
                resolution_assessment_id, grounding_admission_id,
                source_content_hash, source_assertion, semantic_frame,
                speech_act, grounding_continuity, bundle_digest,
                extractor_version, recorded_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
                $10, 'repo-boundary-test-v1', $11
            )
            """,
            interpretation_id,
            tenant_id,
            trace_id,
            trace["source_observation_id"],
            trace["context_snapshot_id"],
            uuid7() if mention is None else UUID(mention["mention_id"]),
            trace["resolution_assessment_id"],
            trace["grounding_admission_id"],
            "a" * 64,
            "b" * 64,
            started_at + timedelta(seconds=17),
        )
        await conn.execute(
            """
            INSERT INTO source_semantic_admission_decisions (
                id, tenant_id, interpretation_id, disposition, reason_codes,
                proposed_belief_assertion, admitted_model_id,
                decision_digest, decided_at
            ) VALUES (
                $1, $2, $3, 'no_admission', ARRAY['repo_boundary_test'],
                NULL, NULL, $4, $5
            )
            """,
            admission_decision_id,
            tenant_id,
            interpretation_id,
            "c" * 64,
            started_at + timedelta(seconds=17),
        )
        terminal = await repo.terminalize_work(
            conn,
            tenant_id=tenant_id,
            work_item_id=recovered.id,
            worker_id="source-semantics:test-b",
            claim_token=recovered.claim_token,
            disposition=SourceSemanticAdmissionDisposition.NO_ADMISSION,
            interpretation_id=interpretation_id,
            admission_decision_id=admission_decision_id,
            admitted_model_id=None,
            now=started_at + timedelta(seconds=18),
        )

        assert terminal.status is SourceSemanticWorkStatus.NO_ADMISSION
        assert terminal.interpretation_id == interpretation_id
        assert terminal.admission_decision_id == admission_decision_id
        assert terminal.admitted_model_id is None
        assert terminal.claim_token is None
        assert await repo.claim_ready_work(
            conn,
            worker_id="source-semantics:test-c",
            now=started_at + timedelta(seconds=30),
            lease_duration=timedelta(seconds=5),
            limit=1,
        ) == ()
