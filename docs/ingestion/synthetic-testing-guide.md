# Synthetic Testing Guide — X2 mocks + X3 harness

This guide is the operator's reference for exercising the M6 backfill chain end-to-end with synthetic traffic. The substrate is:

- **X2 mock clients + fixture generators + fault profiles** ([A21](./05-lld-amendments.md#a21--mock-api-server-architecture-stateful-in-process-libraries-with-fixture-generators-and-fault-injection)) — in-process Python libraries that replace production per-source clients at their `_open_<source>_client` factory seams.
- **X3 BackfillHarness** ([A22](./05-lld-amendments.md#a22--backfill-synthetic-harness-oauth-callback-driven-install-simulation-with-parallel-concurrency-and-properties-based-assertions)) — multi-tenant orchestrator that drives the X2 mocks through the M6 chain end-to-end.

---

## 1. When to use this

- Cutting a release candidate and want to confirm the M6 chain still works for all four sources under synthetic load.
- Investigating a bug report from a customer pilot — reproduce a scenario locally with a fixture that matches the customer's mailbox / workspace / installation shape.
- Adding a new per-source feature (e.g., M6.7) — write a `BackfillScenario` exercising it before opening a PR.
- Soak-testing under fault profiles before a customer-facing change ships.

This is NOT a replacement for:
- The unit tests in `services/ingestion/{planners,fetchers,reconcilers}/tests/` (those verify per-source-module logic).
- The 5-subprocess E2E tests in `services/ingestion/workflows/tests/test_oauth_to_*_completion_*.py` (those verify the M6 chain end-to-end for a single tenant per source).
- The M-Load cutover dry run in `tests/load/test_cutover_dryrun.py` (that exercises webhook → ingestion.raw under production-volume QPS).

---

## 2. Defining a `BackfillScenario`

A scenario describes one tenant's synthetic install. The harness runs one scenario per tenant; pass a list of scenarios to exercise concurrent installs.

```python
from services.synthetic.backfill_harness import BackfillScenario
from services.synthetic.fault_profiles import HAPPY_PATH, FLAKY

scenario = BackfillScenario(
    tenant_slug="alice",           # human-readable; appears in tenants.name
    source="gmail",                # gmail / slack / github / discord
    fixture_params={               # kwargs for make_<source>_<entity>
        "email": "alice@example.com",
        "messages": 10,
        "history_events": 0,       # 0 = clean path; >0 triggers reshare
    },
    fault_profile=HAPPY_PATH,      # or FLAKY / RATE_LIMITED / AUTH_EXPIRED
    expected_observation_count=10, # for assert_observation_count_matches_fixture
)
```

Source-specific `fixture_params`:

| Source  | Generator                  | Key params                                                  |
|---------|----------------------------|-------------------------------------------------------------|
| gmail   | `make_gmail_mailbox`       | `email`, `messages`, `history_events`, `message_size_kb`, `page_size` |
| github  | `make_github_repos`        | `org_or_user`, `repos`, `events_per_repo`, `installation_id`, `per_page` |
| slack   | `make_slack_workspace`     | `team_id`, `channels`, `messages_per_channel`, `page_size`  |
| discord | `make_discord_guild`       | `guild_id`, `channels`, `messages_per_channel`, `channel_type`, `page_size` |

See `services/synthetic/fixtures/*_generator.py` for the full kwargs list.

---

## 3. Choosing fixture parameters

**Throughput sizing:**

- `messages=10` (Gmail) / `events_per_repo=20` (GitHub) / `messages_per_channel=50` (Slack/Discord) are reasonable defaults for single-tenant smoke tests.
- For load testing, scale these up to `messages=10000` etc. Each test record flows through the full M6 chain → real Kafka → real Postgres → observations write.

**Reshare trigger (clean vs gap-fill):**

- Gmail: `history_events > 0` configures the mock's `get_profile` to return a higher `historyId` than the cursor's `final_history_id`, triggering the reconciler's gap-fill path.
- GitHub: not yet configurable via fixture (reconciler uses etag-based detection; the mock manages etag state internally).
- Slack/Discord: not yet configurable; planned for future fixture-generator extensions.

For now, use Gmail with `history_events>0` to exercise reshare; the other sources test clean paths only.

**Determinism:**

Fixture generators are deterministic — same parameters always produce identical output. Add a tenant-distinguishing field (e.g., `email`, `team_id`, `guild_id`) to vary per-tenant content.

---

## 4. Configuring fault profiles

```python
from services.synthetic.fault_profiles import FaultProfile

# Custom profile: rate-limit after 100 requests, with 5% random 5xx.
profile = FaultProfile(
    rate_limit_after_n_requests=100,
    random_5xx_probability=0.05,
    rng_seed=42,  # deterministic across runs
)
```

Presets:

- `HAPPY_PATH` — no faults. Use for happy-path E2E tests.
- `RATE_LIMITED` — rate-limit after 50 requests. Tests the framework's retry / cursor-resume behavior.
- `FLAKY` — 10% random 5xx. Tests A19's broad-exception handling (per-shard failure marking).
- `AUTH_EXPIRED` — auth dies after 30 seconds. Tests auth-failure handling.

When a fault fires, the mock raises the source's real error type:

| Source  | Rate limit          | 5xx              | Auth                          | Transient        |
|---------|---------------------|------------------|-------------------------------|------------------|
| Gmail   | `GoogleRateLimited` | `GoogleApiError` | `GoogleApiError` (401)         | `GoogleApiError` |
| GitHub  | `GithubApiError`    | `GithubApiError` | `GithubApiError` (401)         | `GithubApiError` |
| Slack   | `SlackApiError`     | `SlackApiError`  | `SlackApiError` (invalid_auth) | `SlackApiError`  |
| Discord | `DiscordApiError`   | `DiscordApiError`| `DiscordApiError` (401)        | `DiscordApiError`|

Per A19, the framework absorbs these via the broad `except Exception` catches in source_onboarding / shard_fetch / reconciler dispatch sites. The relevant entity is marked failed with the exception repr; the service keeps serving subsequent work.

---

## 5. Running the harness

The harness is **not** runnable in CI by default — it requires a real Kafka broker. The unit tests in `services/synthetic/backfill_harness/tests/test_harness_unit.py` exercise the setup path without subprocess spawning; they DO run in CI.

For the full E2E run (Phase A + B + C):

```sh
# Ensure Postgres + Kafka are running locally (docker compose up postgres kafka).

X3_HARNESS_E2E=1 \
DATABASE_URL=postgresql://company_os:company_os@localhost:5433/company_os \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
pytest services/synthetic/backfill_harness/tests/test_harness_e2e.py -v
```

Or programmatically:

```python
import asyncio, asyncpg
from services.synthetic.backfill_harness import (
    BackfillHarness, BackfillScenario,
    assert_all_complete, assert_no_duplicate_observations,
)
from services.synthetic.fault_profiles import HAPPY_PATH

async def main():
    pool = await asyncpg.create_pool(DATABASE_URL)
    scenarios = [
        BackfillScenario(
            tenant_slug="alice", source="gmail",
            fixture_params={"email": "alice@x.com", "messages": 10},
            expected_observation_count=10,
        ),
        # ... add more scenarios
    ]
    harness = BackfillHarness(
        pool=pool, scenarios=scenarios,
        concurrency=4, completion_deadline_s=60.0,
    )
    result = await harness.run()
    assert_all_complete(result)
    assert_no_duplicate_observations(result)
    print(f"All {len(result.outcomes)} tenants completed in "
          f"{result.wall_time_seconds:.1f}s")

asyncio.run(main())
```

---

## 6. Interpreting `HarnessResult`

```python
result.outcomes              # list[TenantOutcome] — one per scenario
result.subprocess_returncodes # dict[str, int] — per-service rc (0 on clean exit)
result.subprocess_stderr_tails # dict[str, str] — last 2000 chars of stderr
result.wall_time_seconds     # total wall time
```

Per `TenantOutcome`:

- `completion_observed` — `True` iff `tenant_onboarding_completed` fired in the Bridge inbox.
- `completion_signal_count` — should be exactly 1; >1 means idempotency-key dedup broke.
- `observations` — list of `observations` rows for this tenant.
- `cursor_history` — per-shard cursor state snapshot from `workflow_states`.
- `reconciliation_pass_count` — 0 for clean path, >0 if reshare ran.
- `install_error` — non-None if the install phase failed (rare; usually a substrate bug).

---

## 7. Example scenarios

### 7.1. Single-tenant happy path

```python
scenarios = [
    BackfillScenario(
        tenant_slug="alice", source="gmail",
        fixture_params={"email": "alice@x.com", "messages": 5},
        expected_observation_count=5,
    ),
]
harness = BackfillHarness(pool=pool, scenarios=scenarios)
result = await harness.run()
assert_all_complete(result)
assert_no_duplicate_observations(result)
```

### 7.2. Parallel-tenant stress test

```python
scenarios = [
    BackfillScenario(
        tenant_slug=f"stress-{i}", source="slack",
        fixture_params={
            "team_id": f"T_{i:03d}", "channels": 3,
            "messages_per_channel": 100,
        },
        expected_observation_count=300,
    )
    for i in range(50)
]
harness = BackfillHarness(
    pool=pool, scenarios=scenarios,
    concurrency=10, completion_deadline_s=300.0,
)
result = await harness.run()
assert_all_complete(result)
assert_no_duplicate_observations(result)
```

### 7.3. Reshare path with fault injection

```python
from services.synthetic.fault_profiles import FLAKY

scenarios = [
    BackfillScenario(
        tenant_slug="reshare-flaky", source="gmail",
        fixture_params={
            "email": "alice@x.com",
            "messages": 20,
            "history_events": 5,  # triggers reshare
        },
        fault_profile=FLAKY,      # 10% 5xx during fetch
        expected_observation_count=25,
    ),
]
harness = BackfillHarness(
    pool=pool, scenarios=scenarios,
    completion_deadline_s=120.0,
)
result = await harness.run()
assert_all_complete(result)
assert_reshare_cycles_completed(result)
```

---

## 8. Adding a new fixture-generator parameter

Extending `make_gmail_mailbox` (or any generator):

1. Add the parameter with a default in the function signature.
2. Document it in the function's docstring.
3. Ensure the existing `test_fixture_generators_are_deterministic` still passes (same params → identical output).
4. Optionally extend `BackfillScenario.fixture_params` callers in this guide.

The X2 mock client may need an update if the new parameter changes the fixture's shape (e.g., a new field the mock should serve). Add a corresponding test in `services/synthetic/mock_clients/tests/test_mock_clients.py`.

---

## 9. Known limitations

- **Real Kafka required for E2E.** No in-memory broker today; mock-Kafka is mega-prompt-3+ territory.
- **Gmail-only reshare configuration.** GitHub/Slack/Discord reshare scenarios require fixture-generator extensions (see §3).
- **Single fault profile per tenant.** A tenant can't switch profiles mid-run (e.g., happy for the first 100 requests, then flaky). Per-call profile dispatch is a future extension.
- **The harness doesn't drive HTTP OAuth callbacks.** It writes install + trigger rows directly. The X1 retrofit tests verify the OAuth-layer atomicity independently.
