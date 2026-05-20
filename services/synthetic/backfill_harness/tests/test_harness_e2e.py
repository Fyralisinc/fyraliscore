"""Full 7-subprocess harness E2E test.

Default-skipped: requires KAFKA_BOOTSTRAP_SERVERS pointing at a real
broker AND S3_ENDPOINT_URL pointing at a moto S3 server (the M6.7
backfill producer writes raw bodies to S3 before publishing). Run with:

    X3_HARNESS_E2E=1 \\
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \\
    S3_ENDPOINT_URL=http://localhost:5000 \\
    S3_RAW_BUCKET=fyralis-raw \\
    pytest services/synthetic/backfill_harness/tests/test_harness_e2e.py

Same opt-in shape as M-Load's `tests/load/test_cutover_dryrun.py`.

M6.7 RESOLVED THE PRODUCER GAP (A27).
    Before M6.7, `assert_observation_count_matches_fixture` failed: M6
    backfill never wired fetched records through to `observations`
    (shard_fetch published an inline envelope the normalizer couldn't
    consume; channel_mapping had no `backfill` entry; per-source records
    didn't match the handler input shape; the observation_writer was
    flag-gated off). M6.7 closed all four layers — the harness now
    spawns the normalizer + observation_writer (5→7), sets
    `kafka_path_enabled=TRUE` per tenant, and the backfill chain
    produces observations end-to-end. The assertion now PASSES; it
    stays as the regression-prevention surface. See A27 +
    docs/decisions/ticket-43-m6-backfill-producer-completion.md.
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
    reason=(
        "X3 harness E2E requires X3_HARNESS_E2E=1 + real Kafka + moto S3 "
        "(S3_ENDPOINT_URL). See docs/ingestion/synthetic-testing-guide.md."
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
    # PASSES post-M6.7 (A27): the backfill chain now produces
    # observations. This is the regression-prevention surface.
    assert_observation_count_matches_fixture(result)


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


# =====================================================================
# M6.7 cross-cutting (A27) — backfill produces observations per source.
# =====================================================================

# The source_channel each source's backfill observations land on — the
# same handler channel its live webhook/gateway twin uses (A27.2/A27.3).
_EXPECTED_CHANNEL = {
    "gmail": "gmail:",
    "github": "github:webhook",
    "slack": "slack:message",
    "discord": "discord:message",
}


@pytest.mark.asyncio
async def test_harness_single_tenant_per_source_produces_observations(
    fresh_db: asyncpg.Pool,
) -> None:
    """One tenant per source; each backfill chain produces observations
    (A27). Proves the producer → normalizer → writer chain is wired for
    every source, and that each source's observations route through the
    correct handler channel."""
    scenarios = [
        BackfillScenario(
            tenant_slug="e2e-src-gmail", source="gmail",
            fixture_params={"email": "g@e2e.com", "messages": 4},
            expected_observation_count=4,
        ),
        BackfillScenario(
            tenant_slug="e2e-src-github", source="github",
            fixture_params={"org_or_user": "octo", "repos": 1,
                            "events_per_repo": 3},
        ),
        BackfillScenario(
            tenant_slug="e2e-src-slack", source="slack",
            fixture_params={"team_id": "T_SRC", "channels": 1,
                            "messages_per_channel": 4},
        ),
        BackfillScenario(
            tenant_slug="e2e-src-discord", source="discord",
            fixture_params={"guild_id": "G_SRC", "channels": 1,
                            "messages_per_channel": 4},
        ),
    ]
    harness = BackfillHarness(
        pool=fresh_db, scenarios=scenarios, concurrency=4,
        completion_deadline_s=120.0,
    )
    result = await harness.run()
    assert_all_complete(result)
    assert_no_duplicate_observations(result)

    by_source = {o.scenario.source: o for o in result.outcomes}
    for source, outcome in by_source.items():
        assert len(outcome.observations) >= 1, (
            f"{source}: backfill produced no observations — the "
            f"producer/normalizer/writer chain is broken for this source"
        )
        for obs in outcome.observations:
            assert obs["source_channel"] == _EXPECTED_CHANNEL[source], (
                f"{source}: observation landed on channel "
                f"{obs['source_channel']!r}; expected "
                f"{_EXPECTED_CHANNEL[source]!r}"
            )
            assert obs["external_id"], (
                f"{source}: observation has no external_id — dedup key "
                f"missing"
            )

    # Gmail's fixture→count mapping is established (1 obs per message).
    gmail_outcome = by_source["gmail"]
    assert_observation_count_matches_fixture(
        type(result)(outcomes=[gmail_outcome])
    )


@pytest.mark.asyncio
async def test_harness_e2e_backfill_to_observation_chain(
    fresh_db: asyncpg.Pool,
) -> None:
    """Integration-level external_id parity surface (A27.5). Each source's
    backfilled observations carry an external_id in the SAME shape its
    webhook/gateway handler derives — gmail `gmail:{install}:{msgid}`,
    slack `{channel}:{ts}`, discord `discord:{snowflake}`. (GitHub uses
    the opaque node_id; we assert non-empty + correct channel.) The
    unit-level handler-equality parity lives in
    services/ingestion/normalizer/tests/test_backfill_external_id_parity.py;
    this asserts the property holds through the live subprocess chain."""
    scenarios = [
        BackfillScenario(
            tenant_slug="chain-gmail", source="gmail",
            fixture_params={"email": "c@e2e.com", "messages": 2},
        ),
        BackfillScenario(
            tenant_slug="chain-slack", source="slack",
            fixture_params={"team_id": "T_CH", "channels": 1,
                            "messages_per_channel": 2},
        ),
        BackfillScenario(
            tenant_slug="chain-discord", source="discord",
            fixture_params={"guild_id": "G_CH", "channels": 1,
                            "messages_per_channel": 2},
        ),
    ]
    harness = BackfillHarness(
        pool=fresh_db, scenarios=scenarios, concurrency=3,
        completion_deadline_s=120.0,
    )
    result = await harness.run()
    assert_all_complete(result)

    by_source = {o.scenario.source: o for o in result.outcomes}
    for obs in by_source["gmail"].observations:
        assert obs["external_id"].startswith("gmail:")
    for obs in by_source["slack"].observations:
        # "{channel}:{ts}" — channel ids are non-empty, ts has a dot.
        assert ":" in obs["external_id"]
        assert not obs["external_id"].startswith("discord:")
    for obs in by_source["discord"].observations:
        assert obs["external_id"].startswith("discord:")


@pytest.mark.asyncio
async def test_harness_sigterm_cleanly_stops_all_seven(
    fresh_db: asyncpg.Pool,
) -> None:
    """Teardown SIGTERMs all 7 subprocesses (5 framework + normalizer +
    observation_writer) and each exits rc==0 within the grace window
    (A27.4)."""
    scenarios = [
        BackfillScenario(
            tenant_slug="teardown7", source="slack",
            fixture_params={"team_id": "T_TD", "channels": 1,
                            "messages_per_channel": 1},
        ),
    ]
    harness = BackfillHarness(
        pool=fresh_db, scenarios=scenarios, completion_deadline_s=90.0,
    )
    result = await harness.run()
    assert set(result.subprocess_returncodes) == {
        "oauth_poller", "tenant_onboarding", "source_onboarding",
        "shard_fetch", "reconciler", "normalizer", "observation_writer",
    }
    for name, rc in result.subprocess_returncodes.items():
        assert rc == 0, (
            f"{name} exited rc={rc} (stderr tail: "
            f"{result.subprocess_stderr_tails.get(name, '')[-500:]})"
        )
