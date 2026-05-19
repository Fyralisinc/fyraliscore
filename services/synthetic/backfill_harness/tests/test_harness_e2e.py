"""Full 5-subprocess harness E2E test.

Default-skipped: requires KAFKA_BOOTSTRAP_SERVERS pointing at a real
broker. Run with:

    X3_HARNESS_E2E=1 \\
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \\
    pytest services/synthetic/backfill_harness/tests/test_harness_e2e.py

Same opt-in shape as M-Load's `tests/load/test_cutover_dryrun.py`.
"""
from __future__ import annotations

import os

import asyncpg
import pytest

from services.synthetic.backfill_harness import (
    BackfillHarness,
    BackfillScenario,
    assert_all_complete,
    assert_completion_emitted_per_tenant,
    assert_no_duplicate_observations,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("X3_HARNESS_E2E") != "1",
    reason=(
        "X3 harness E2E requires X3_HARNESS_E2E=1 + real Kafka. "
        "See docs/ingestion/synthetic-testing-guide.md."
    ),
)


@pytest.mark.asyncio
async def test_harness_single_tenant_gmail_completes(
    fresh_db: asyncpg.Pool,
) -> None:
    """Single Gmail tenant; small fixture; properties hold."""
    scenarios = [
        BackfillScenario(
            tenant_slug="e2e-gmail",
            source="gmail",
            fixture_params={"email": "alice@e2e.com", "messages": 5},
            expected_observation_count=5,
        ),
    ]
    harness = BackfillHarness(
        pool=fresh_db,
        scenarios=scenarios,
        completion_deadline_s=90.0,
    )
    result = await harness.run()
    assert_all_complete(result)
    assert_completion_emitted_per_tenant(result)
    assert_no_duplicate_observations(result)


@pytest.mark.asyncio
async def test_harness_parallel_4_tenants_mixed_sources(
    fresh_db: asyncpg.Pool,
) -> None:
    """Four tenants across all four sources concurrently. Verifies
    per-tenant isolation in the shared-subprocess model."""
    scenarios = [
        BackfillScenario(
            tenant_slug="e2e-multi-gmail",
            source="gmail",
            fixture_params={"email": "a@e2e.com", "messages": 3},
        ),
        BackfillScenario(
            tenant_slug="e2e-multi-github",
            source="github",
            fixture_params={
                "org_or_user": "octo", "repos": 1,
                "events_per_repo": 3,
            },
        ),
        BackfillScenario(
            tenant_slug="e2e-multi-slack",
            source="slack",
            fixture_params={
                "team_id": "T_MULTI", "channels": 1,
                "messages_per_channel": 5,
            },
        ),
        BackfillScenario(
            tenant_slug="e2e-multi-discord",
            source="discord",
            fixture_params={
                "guild_id": "G_MULTI", "channels": 1,
                "messages_per_channel": 5,
            },
        ),
    ]
    harness = BackfillHarness(
        pool=fresh_db,
        scenarios=scenarios,
        concurrency=4,
        completion_deadline_s=120.0,
    )
    result = await harness.run()
    assert_all_complete(result)
    assert_completion_emitted_per_tenant(result)
    assert_no_duplicate_observations(result)
