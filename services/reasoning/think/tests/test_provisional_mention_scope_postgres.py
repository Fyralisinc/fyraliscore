"""PostgreSQL proof for final mention-scope authority reopening."""

from __future__ import annotations

import json
import os
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.errors import ValidationError

from services.reasoning.think.validator import _validate_provisional_mention_scope


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@dataclass(frozen=True)
class MentionProof:
    conn: asyncpg.Connection
    tenant_id: UUID
    observation_id: UUID
    detection_id: UUID
    content_text: str

    def entry(self, **changes: object) -> dict[str, object]:
        ref = f"mention:{self.detection_id}"
        entry: dict[str, object] = {
            "natural": self.content_text,
            "scope_entities": [{"type": "mention", "id": ref}],
            "supporting_event_ids": [self.observation_id],
            "supporting_model_ids": [],
            "contributing_models": [],
            "proposition": {
                "abstraction_level": "atomic",
                "claim_role": "fact",
                "evidence_event_ids": [self.observation_id],
                "mention_scope_contract": {
                    "detection_ref": ref,
                    "canonical_identity_authority": False,
                    "cross_observation_grouping_authority": False,
                },
                "closed_atomic_contract": {
                    "compiler_entails_exact_text": True,
                    "evidence_cardinality": "singleton",
                },
            },
        }
        entry.update(changes)
        return entry


@pytest_asyncio.fixture
async def mention_proof() -> AsyncGenerator[MentionProof, None]:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the PostgreSQL mention proof")
    conn = await asyncpg.connect(dsn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        await conn.execute(
            """
            CREATE TEMP TABLE observations (
              id uuid NOT NULL,
              tenant_id uuid NOT NULL,
              content_text text NOT NULL
            ) ON COMMIT DROP;
            CREATE TEMP TABLE entity_mention_detections (
              id uuid NOT NULL,
              tenant_id uuid NOT NULL,
              source_observation_id uuid NOT NULL,
              fate text NOT NULL,
              candidate_surface text NOT NULL,
              mention jsonb
            ) ON COMMIT DROP;
            CREATE TEMP TABLE entity_mention_detection_heads (
              tenant_id uuid NOT NULL,
              source_observation_id uuid NOT NULL,
              current_detection_id uuid NOT NULL
            ) ON COMMIT DROP;
            """
        )
        tenant_id, observation_id, detection_id = uuid4(), uuid4(), uuid4()
        content_text = "Atlas release certificate is blocked."
        mention = {
            "surface": "Atlas",
            "primary_anchor": {
                "coordinate": {
                    "field_path": "content_text",
                    "span_start": 0,
                    "span_end": 5,
                }
            },
        }
        await conn.execute(
            "INSERT INTO observations VALUES ($1,$2,$3)",
            observation_id,
            tenant_id,
            content_text,
        )
        await conn.execute(
            "INSERT INTO entity_mention_detections VALUES ($1,$2,$3,$4,$5,$6::jsonb)",
            detection_id,
            tenant_id,
            observation_id,
            "detected",
            "Atlas",
            json.dumps(mention),
        )
        await conn.execute(
            "INSERT INTO entity_mention_detection_heads VALUES ($1,$2,$3)",
            tenant_id,
            observation_id,
            detection_id,
        )
        yield MentionProof(
            conn, tenant_id, observation_id, detection_id, content_text,
        )
    finally:
        await transaction.rollback()
        await conn.close()


async def test_accepts_current_exact_atomic_singleton_mention(
    mention_proof: MentionProof,
) -> None:
    await _validate_provisional_mention_scope(
        mention_proof.entry(),
        mention_proof.conn,
        tenant_id=mention_proof.tenant_id,
    )


async def test_rejects_arbitrary_detection_uuid(mention_proof: MentionProof) -> None:
    entry = mention_proof.entry()
    arbitrary_ref = f"mention:{uuid4()}"
    entry["scope_entities"] = [{"type": "mention", "id": arbitrary_ref}]
    proposition = dict(entry["proposition"])
    contract = dict(proposition["mention_scope_contract"])
    contract["detection_ref"] = arbitrary_ref
    proposition["mention_scope_contract"] = contract
    entry["proposition"] = proposition

    with pytest.raises(ValidationError, match="current detected head"):
        await _validate_provisional_mention_scope(
            entry, mention_proof.conn, tenant_id=mention_proof.tenant_id,
        )


async def test_rejects_detection_for_wrong_observation(
    mention_proof: MentionProof,
) -> None:
    wrong_observation = uuid4()
    entry = mention_proof.entry(supporting_event_ids=[wrong_observation])
    proposition = dict(entry["proposition"])
    proposition["evidence_event_ids"] = [wrong_observation]
    entry["proposition"] = proposition

    with pytest.raises(ValidationError, match="current detected head"):
        await _validate_provisional_mention_scope(
            entry, mention_proof.conn, tenant_id=mention_proof.tenant_id,
        )


async def test_rejects_mixed_mention_and_canonical_scope(
    mention_proof: MentionProof,
) -> None:
    entry = mention_proof.entry()
    entry["scope_entities"] = [
        *entry["scope_entities"],
        {"type": "entity", "id": str(uuid4())},
    ]

    with pytest.raises(ValidationError, match="cannot mix"):
        await _validate_provisional_mention_scope(
            entry, mention_proof.conn, tenant_id=mention_proof.tenant_id,
        )


async def test_rejects_supporting_model(mention_proof: MentionProof) -> None:
    entry = mention_proof.entry(supporting_model_ids=[uuid4()])

    with pytest.raises(ValidationError, match="supporting Models"):
        await _validate_provisional_mention_scope(
            entry, mention_proof.conn, tenant_id=mention_proof.tenant_id,
        )


async def test_rejects_non_exact_natural_assertion(
    mention_proof: MentionProof,
) -> None:
    entry = mention_proof.entry(natural="Atlas is blocked.")

    with pytest.raises(ValidationError, match="exact observation assertion"):
        await _validate_provisional_mention_scope(
            entry, mention_proof.conn, tenant_id=mention_proof.tenant_id,
        )
