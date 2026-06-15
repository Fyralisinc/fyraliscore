from __future__ import annotations

import pytest

from services.product.recommendations.feedback import record_recommendation_feedback
from services.product.recommendations.tests.conftest import make_recommendation_proposition


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_recommendation_feedback_updates_generic_loop_stats(
    gateway_pool,
    tenant_id,
    seeded_actor,
):
    proposition = make_recommendation_proposition(
        target_actor_id=seeded_actor,
        target_type="commitment",
        target_id=seeded_actor,
        operation="transition",
        payload={"new_state": "paused"},
    )
    async with gateway_pool.acquire() as conn:
        pattern_key = await record_recommendation_feedback(
            conn,
            tenant_id=tenant_id,
            target_actor_id=seeded_actor,
            proposition=proposition,
            action="acted",
            reason="accepted in UI",
        )
        stat = await conn.fetchrow(
            """
            SELECT success_count, last_payload
            FROM think_feedback_stats
            WHERE tenant_id = $1
              AND surface = 'recommendation_feedback'
              AND op_type = 'recommendation'
              AND op_kind = 'acted'
              AND reason = 'accepted in UI'
            """,
            tenant_id,
        )

    assert stat["success_count"] == 1
    assert stat["last_payload"]["pattern_key"] == pattern_key
