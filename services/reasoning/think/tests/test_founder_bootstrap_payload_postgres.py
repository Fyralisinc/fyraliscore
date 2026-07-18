"""PostgreSQL proof that founder identity reaches Think's semantic episodes."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from services.domain.company_identity_bootstrap import (
    FounderIdentityBootstrapEntry,
    apply_founder_identity_bootstrap,
)
from services.domain.entity_grounding import (
    ensure_persisted_observation_mention_fates,
)
from services.reasoning.think.worker import ThinkWorker

from .conftest import make_embedding


pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def _insert_observation(
    conn,
    *,
    tenant_id: UUID,
    occurred_at: datetime,
    content_text: str,
    mention_surface: str,
) -> UUID:
    observation_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel, content,
           content_text, embedding, embedding_pending, trust_tier,
           entities_mentioned)
        VALUES
          ($1, $2, $3, 'signal', 'email', $4::jsonb, $5, $6, FALSE,
           'authoritative', '[]'::jsonb)
        """,
        observation_id,
        tenant_id,
        occurred_at,
        json.dumps({
            "text": content_text,
            "_unresolved_phrases": [mention_surface],
        }),
        content_text,
        make_embedding(content_text),
    )
    return observation_id


async def test_founder_bootstrap_groups_known_entity_and_isolates_unknown(
    fresh_db,
    tenant,
) -> None:
    """Exercise the real bootstrap -> fate closure -> Think payload dataflow."""

    base = datetime(2026, 7, 18, 9, 0, tzinfo=timezone.utc)
    canonical_ref = {
        "type": "workstream",
        "id": "workstream:atlas-release",
        "version": 1,
    }
    async with fresh_db.acquire() as conn, conn.transaction():
        await apply_founder_identity_bootstrap(
            conn,
            tenant_id=tenant,
            manifest_ref="founder-map:test-atlas:v1",
            authority_ref="company-founder-assertion:test",
            asserted_by_ref="founder:test",
            provenance_refs=("founder-workshop:test",),
            entries=(
                FounderIdentityBootstrapEntry(
                    canonical_ref=canonical_ref,
                    canonical_name="Atlas Release",
                ),
            ),
            effective_at=base - timedelta(seconds=1),
        )
        observation_ids = [
            await _insert_observation(
                conn,
                tenant_id=tenant,
                occurred_at=base,
                content_text="Atlas Release is blocked by security review.",
                mention_surface="Atlas Release",
            ),
            await _insert_observation(
                conn,
                tenant_id=tenant,
                occurred_at=base + timedelta(minutes=1),
                content_text="Atlas Release received the security approval.",
                mention_surface="Atlas Release",
            ),
            await _insert_observation(
                conn,
                tenant_id=tenant,
                occurred_at=base + timedelta(minutes=2),
                content_text="Zephyr Initiative is waiting on an owner.",
                mention_surface="Zephyr Initiative",
            ),
        ]

        coverage = await ensure_persisted_observation_mention_fates(
            conn=conn,
            tenant_id=tenant,
            observation_ids=observation_ids,
            now=base + timedelta(minutes=3),
        )
        worker = ThinkWorker(fresh_db, embedder=False)
        payload = await worker._build_t1_batch_payload(
            conn,
            tenant_id=tenant,
            batch_id=uuid7(),
            members=[{"id": uuid7()} for _ in observation_ids],
            observation_ids=observation_ids,
        )

        episodes = payload["governed_learning_episodes"]
        canonical = [
            episode
            for episode in episodes
            if episode["canonical_ref"] == "workstream:atlas-release"
        ]
        unresolved = [
            episode for episode in episodes if episode["canonical_ref"] is None
        ]

        assert coverage.coverage == 1.0
        assert len(canonical) == 1
        assert {
            assertion["observation_id"]
            for assertion in canonical[0]["assertions"]
        } == {str(observation_ids[0]), str(observation_ids[1])}
        assert all(
            assertion["coordinate_authority"] == "resolved"
            for assertion in canonical[0]["assertions"]
        )
        assert len(unresolved) == 1
        assert [
            assertion["observation_id"]
            for assertion in unresolved[0]["assertions"]
        ] == [str(observation_ids[2])]
        assert unresolved[0]["uncertainty"] == [
            "missing_governed_entity_coordinate"
        ]
