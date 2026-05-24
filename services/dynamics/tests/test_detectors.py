from __future__ import annotations

from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.dynamics import detect_dynamic_signals


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _embedding() -> str:
    return "[" + ",".join("0" for _ in range(768)) + "]"


async def test_detect_dynamic_signals_finds_audit_oscillation(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
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
              '{}'::jsonb, 'model signal', NULL, TRUE, 'authoritative',
              $5, '[]'::jsonb
            )
            """,
            obs_id,
            tenant_id,
            now,
            actor_id,
            f"dynamic-{obs_id}",
        )
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id, proposition, "natural",
              embedding, scope_actors, scope_entities, scope_temporal,
              confidence, confidence_at_assertion, falsifier, signal_readings,
              supporting_event_ids, supporting_model_ids, contributing_models,
              status, activation, last_retrieved_at
            ) VALUES (
              $1, $2, $3,
              '{"kind":"state","subject":"x","assertion":"y"}'::jsonb,
              'oscillating model', $4, ARRAY[$5]::uuid[], '[]'::jsonb,
              '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
              0.6, 0.6, NULL, '[]'::jsonb, ARRAY[$3]::uuid[],
              '{}'::uuid[], '{}'::uuid[], 'active', 0.9, now()
            )
            """,
            model_id,
            tenant_id,
            obs_id,
            _embedding(),
            actor_id,
        )
        first_event_id = await conn.fetchval(
            """
            INSERT INTO audit_events (
              model_id, tenant_id, occurred_at, cause_id, cause_type,
              previous_state, new_state, changed_fields
            )
            VALUES (
              $1, $2, $3, $4, 'field_update',
              NULL, '{"status":"active"}'::jsonb, ARRAY['status']::text[]
            )
            RETURNING event_id
            """,
            model_id,
            tenant_id,
            now - timedelta(days=2),
            obs_id,
        )
        await conn.execute(
            """
            INSERT INTO audit_events (
              model_id, tenant_id, occurred_at, cause_id, cause_type,
              previous_state, new_state, changed_fields, re_asserts_event_id
            )
            VALUES (
              $1, $2, $3, $4, 'field_update',
              '{"status":"contested_false"}'::jsonb,
              '{"status":"active"}'::jsonb,
              ARRAY['status']::text[], $5
            )
            """,
            model_id,
            tenant_id,
            now - timedelta(days=1),
            obs_id,
            first_event_id,
        )

        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[model_id],
            actor_ids=[actor_id],
            reference_time=now,
        )

    assert any(s.dynamic_kind == "oscillating" for s in signals)
    assert any(str(model_id) in s.summary for s in signals)
