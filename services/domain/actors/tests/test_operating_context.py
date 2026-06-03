from __future__ import annotations

from datetime import datetime, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.actors.operating_context import (
    load_actor_operating_context,
    summarize_actor_operating_context,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _embedding() -> str:
    return "[" + ",".join("0" for _ in range(768)) + "]"


async def test_actor_operating_context_uses_existing_actor_scoped_models(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_capability = uuid7()
    model_concern = uuid7()
    commitment_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, 'human_internal', 'Alice', 'active')
            """,
            actor_id,
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel, actor_id,
              content, content_text, embedding, embedding_pending, trust_tier,
              external_id, entities_mentioned
            ) VALUES (
              $1, $2, $3, 'signal', 'test', $4,
              '{}'::jsonb, 'Alice is blocked on security approval.',
              NULL, TRUE, 'authoritative', $5, '[]'::jsonb
            )
            """,
            obs_id,
            tenant_id,
            now,
            actor_id,
            f"actor-context-{obs_id}",
        )
        await conn.executemany(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id, proposition, "natural",
              embedding, scope_actors, scope_entities, scope_temporal,
              confidence, confidence_at_assertion, falsifier, signal_readings,
              supporting_event_ids, supporting_model_ids, contributing_models,
              status
            ) VALUES (
              $1, $2, $3, $4::jsonb, $5, $6,
              $7::uuid[], '[]'::jsonb,
              '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
              0.7, 0.7, NULL, '[]'::jsonb, ARRAY[$3]::uuid[],
              '{}'::uuid[], '{}'::uuid[], 'active'
            )
            """,
            [
                (
                    model_capability,
                    tenant_id,
                    obs_id,
                    '{"kind":"capability_assessment","capability_id":"incident-review","assessment":"Alice is strong at incident review"}',
                    "Alice is strong at incident review.",
                    _embedding(),
                    [actor_id],
                ),
                (
                    model_concern,
                    tenant_id,
                    obs_id,
                    '{"kind":"concern","about":"Alice workload","nature":"Alice is blocked waiting on security approval","raised_by":"system"}',
                    "Alice is blocked waiting on security approval.",
                    _embedding(),
                    [actor_id],
                ),
            ],
        )
        await conn.execute(
            """
            INSERT INTO commitments (
              id, tenant_id, title, state, owner_id, created_by_event_id
            )
            VALUES (
              $1, $2, 'Ship security review', 'blocked', $3, $4
            )
            """,
            commitment_id,
            tenant_id,
            actor_id,
            obs_id,
        )

        contexts = await load_actor_operating_context(
            conn,
            tenant_id=tenant_id,
            actor_ids=[actor_id],
            reference_time=now,
        )

    assert len(contexts) == 1
    ctx = contexts[0]
    assert ctx.display_name == "Alice"
    assert ctx.active_model_count == 2
    assert ctx.blocked_commitment_count == 1
    assert any("incident review" in item for item in ctx.capabilities)
    assert any("security approval" in item for item in ctx.support_needs)
    summary = summarize_actor_operating_context(contexts)
    assert summary is not None
    assert "Alice" in summary
    assert str(model_capability) in summary
