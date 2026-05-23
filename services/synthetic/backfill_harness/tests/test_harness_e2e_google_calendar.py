"""Full 7-subprocess harness E2E test for Google Calendar (IN-15).

Default-skipped (same opt-in shape as test_harness_e2e.py): requires a real
Kafka broker + moto S3, because this drives the FULL worker topology — not the
in-process ingest() shortcut:

    onboarding_triggers -> oauth_poller -> tenant_onboarding -> source_onboarding
      -> shard_fetch -> (S3 raw + Kafka ingestion.raw)
      -> normalizer -> (Kafka ingestion.normalized)
      -> observation_writer -> observations

The google_calendar source is mocked in-process at the `_open_calendar_client`
seam (via the X3 helper, injected into each subprocess), so no real Google
credentials are needed; everything else is the real worker chain.

Run with `scripts/sandbox_google_calendar_full.py` (stands up Kafka + moto +
a throwaway DB and invokes the harness), or manually:

    X3_HARNESS_E2E=1 \\
    KAFKA_BOOTSTRAP_SERVERS=localhost:29092 \\
    S3_ENDPOINT_URL=http://localhost:5000 S3_RAW_BUCKET=fyralis-raw \\
    DATABASE_URL=postgresql://.../gcal_sandbox \\
    pytest services/synthetic/backfill_harness/tests/test_harness_e2e_google_calendar.py
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
    assert_observation_count_matches_fixture,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("X3_HARNESS_E2E") != "1",
    reason="X3 harness E2E requires X3_HARNESS_E2E=1 + real Kafka + moto S3.",
)


def _bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.mark.asyncio
async def test_harness_single_tenant_google_calendar_completes(
    fresh_db: asyncpg.Pool,
) -> None:
    """One Google Calendar tenant, 2 calendars x 3 events = 6 observations,
    materialized end-to-end through the full 7-worker chain."""
    scenarios = [
        BackfillScenario(
            tenant_slug="e2e-gcal",
            source="google_calendar",
            fixture_params={
                "calendars": ["alice@e2e.com", "bob@e2e.com"],
                "events_per_calendar": 3,
            },
            expected_observation_count=6,
        ),
    ]
    harness = BackfillHarness(
        pool=fresh_db,
        scenarios=scenarios,
        kafka_bootstrap_servers=_bootstrap(),
        completion_deadline_s=90.0,
    )
    result = await harness.run()
    assert_all_complete(result)
    assert_completion_emitted_per_tenant(result)
    assert_no_duplicate_observations(result)
    # The full chain (normalizer + observation_writer) materializes the
    # calendar events as observations — the proof the in-process sandbox
    # could not give.
    assert_observation_count_matches_fixture(result)
