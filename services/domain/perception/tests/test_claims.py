from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import asyncpg
import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import SourceEvidenceCreate
from services.domain.evidence.repo import SourceEvidenceRepository
from services.domain.perception.claims import (
    EvidenceSpan,
    PerceptionClaimCreate,
    PerceptionClaimRepository,
    span_for_text,
)


pytestmark = pytest.mark.integration


async def _seed_evidence_observation(
    conn: asyncpg.Connection,
    tenant_id,
    text: str,
    revision: str,
):
    now = datetime.now(tz=timezone.utc)
    evidence = await SourceEvidenceRepository().insert(
        SourceEvidenceCreate(
            tenant_id=tenant_id,
            source="notion",
            installation_scope="stateless:notion",
            source_channel="notion:page",
            source_object_type="page",
            source_object_id="audit-status",
            source_revision_id=revision,
            operation="update",
            source_recorded_at=now,
            raw_object_key=f"raw/{revision}.json",
            content_hash=hashlib.sha256(text.encode()).hexdigest(),
            raw_ingested_at=now,
            normalized_at=now,
            ingress_kind="poll",
            access_policy={
                "visibility": "tenant",
                "audience": [],
                "source_acl_version": "test-v1",
            },
        ),
        conn=conn,
    )
    observation_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel,
          content, content_text, embedding_pending, trust_tier,
          external_id, entities_mentioned, evidence_id
        ) VALUES (
          $1, $2, $3, 'signal', 'notion:page', '{}'::jsonb, $4,
          TRUE, 'authoritative', $5, '[]'::jsonb, $6
        )
        """,
        observation_id,
        tenant_id,
        now,
        text,
        f"audit-{revision}",
        evidence.evidence.id,
    )
    return evidence.evidence.id, observation_id


def _claim(tenant_id, evidence_id, observation_id, text, *, polarity):
    return PerceptionClaimCreate(
        tenant_id=tenant_id,
        evidence_id=evidence_id,
        observation_id=observation_id,
        claimant_ref={"type": "actor", "id": "simanta"},
        subject_ref={"type": "workstream", "id": "security-audit"},
        predicate="coverage_complete",
        object_value=True,
        modality="asserted",
        polarity=polarity,
        confidence=0.9,
        evidence_span=span_for_text(text),
        extractor_kind="model",
        extractor_name="perception-claim-extractor",
        extractor_version="1.0.0",
    )


async def test_claims_keep_exact_evidence_and_surface_contradictions(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    repo = PerceptionClaimRepository()
    positive_text = "The audit coverage is complete."
    negative_text = "The audit coverage is not complete."
    async with fresh_db.acquire() as conn:
        first_evidence, first_observation = await _seed_evidence_observation(
            conn, tenant_id, positive_text, "r1"
        )
        second_evidence, second_observation = await _seed_evidence_observation(
            conn, tenant_id, negative_text, "r2"
        )
        positive = await repo.insert(
            _claim(
                tenant_id,
                first_evidence,
                first_observation,
                positive_text,
                polarity="positive",
            ),
            conn=conn,
        )
        replay = await repo.insert(
            _claim(
                tenant_id,
                first_evidence,
                first_observation,
                positive_text,
                polarity="positive",
            ),
            conn=conn,
        )
        negative = await repo.insert(
            _claim(
                tenant_id,
                second_evidence,
                second_observation,
                negative_text,
                polarity="negative",
            ),
            conn=conn,
        )
        contradictions = await repo.find_contradictions(
            tenant_id=tenant_id,
            subject_ref={"type": "workstream", "id": "security-audit"},
            predicate="coverage_complete",
            conn=conn,
        )

    assert replay.id == positive.id
    assert negative.id != positive.id
    assert len(contradictions) == 1
    assert {claim.polarity for claim in contradictions[0]} == {
        "positive",
        "negative",
    }


async def test_claim_rejects_a_span_not_found_in_linked_observation(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    text = "Audit status is in progress."
    async with fresh_db.acquire() as conn:
        evidence_id, observation_id = await _seed_evidence_observation(
            conn, tenant_id, text, "bad-span"
        )
        bad = _claim(
            tenant_id,
            evidence_id,
            observation_id,
            text,
            polarity="positive",
        ).model_copy(
            update={
                "evidence_span": EvidenceSpan(
                    start=0,
                    end=len(text),
                    text_hash=hashlib.sha256(b"different text").hexdigest(),
                )
            }
        )
        with pytest.raises(ValidationError, match="span hash"):
            await PerceptionClaimRepository().insert(bad, conn=conn)
