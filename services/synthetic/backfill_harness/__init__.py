"""X3 Backfill Synthetic Harness.

Per A22: orchestrates multi-tenant synthetic backfill exercising the
full M6 chain. Drives OAuth callbacks in-process (via ASGI transport),
spawns 5 shared subprocesses (oauth_poller, tenant_onboarding,
source_onboarding, shard_fetch, reconciler), and waits for per-tenant
`tenant_onboarding_completed` signals in the Bridge inbox.

Properties-based correctness verification rather than exact-data
match: see `assertions.py` for the load-bearing invariants.

Usage:

    from services.synthetic.backfill_harness import (
        BackfillHarness, BackfillScenario, assertions,
    )
    from services.synthetic.fault_profiles import HAPPY_PATH

    scenarios = [
        BackfillScenario(
            tenant_slug="alice",
            source="gmail",
            fixture_params={"email": "alice@x.com", "messages": 10},
            fault_profile=HAPPY_PATH,
        ),
        # ...
    ]
    harness = BackfillHarness(scenarios=scenarios, concurrency=4)
    result = await harness.run()
    assertions.assert_all_complete(result)
    assertions.assert_no_duplicate_observations(result)
"""
from services.synthetic.backfill_harness.assertions import (
    PropertyViolation,
    assert_all_complete,
    assert_completion_emitted_per_tenant,
    assert_cursor_monotonic_per_shard,
    assert_no_duplicate_observations,
    assert_observation_count_matches_fixture,
    assert_reshare_cycles_completed,
)
from services.synthetic.backfill_harness.harness import (
    BackfillHarness,
    HarnessResult,
    TenantOutcome,
)
from services.synthetic.backfill_harness.scenarios import BackfillScenario


__all__ = [
    "BackfillHarness",
    "BackfillScenario",
    "HarnessResult",
    "PropertyViolation",
    "TenantOutcome",
    "assert_all_complete",
    "assert_completion_emitted_per_tenant",
    "assert_cursor_monotonic_per_shard",
    "assert_no_duplicate_observations",
    "assert_observation_count_matches_fixture",
    "assert_reshare_cycles_completed",
]
