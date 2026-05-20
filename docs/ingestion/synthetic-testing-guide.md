# Synthetic Testing Guide — X2 mocks + X3 harness + live generators

This guide is the operator's reference for exercising the M6 ingestion pipeline end-to-end with synthetic traffic, across **all four sources for both backfill and live ingestion**. The substrate is:

- **X2 mock clients + fixture generators + fault profiles** ([A21](./05-lld-amendments.md#a21--mock-api-server-architecture-stateful-in-process-libraries-with-fixture-generators-and-fault-injection)) — in-process Python libraries that replace production per-source clients at their `_open_<source>_client` factory seams.
- **X3 BackfillHarness** ([A22](./05-lld-amendments.md#a22--backfill-synthetic-harness-oauth-callback-driven-install-simulation-with-parallel-concurrency-and-properties-based-assertions)) — multi-tenant orchestrator that drives the X2 mocks through the M6 chain end-to-end (backfill, all four sources).
- **Live-ingestion generators** — drive the ongoing/live paths in-process (see §9): Gmail Pub/Sub (Y1, [A23](./05-lld-amendments.md)), Discord Gateway (Y2, [A24](./05-lld-amendments.md)), and Slack + GitHub webhooks (Z1, [A25](./05-lld-amendments.md)).

Synthetic coverage is now complete: **webhook (Slack + GitHub via Z1) + backfill (all four via X3) + Pub/Sub (Gmail via Y1) + Gateway (Discord via Y2)** — every source × (backfill + live), targeting specific seeded tenants.

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

## 9. Live-ingestion synthetic generators

The X3 backfill harness covers the M6 backfill chain (OAuth → trigger → run → shards → fetch → reconcile). The **live-ingestion** code paths (continuous traffic after backfill completes) are covered by separate per-source generators in `services/synthetic/live_generators/`.

### 9.1. Gmail Pub/Sub — `GmailPubSubGenerator`

Per [A23](./05-lld-amendments.md#a23--gmail-pubsub-synthetic-generator-fastapi-in-process-invocation-with-mock-gmail-coordination). Drives the Gmail Pub/Sub push-notification path end-to-end in-process via FastAPI ASGI transport.

**Coverage:** FastAPI routing + OIDC envelope validation surface (test-mode no-op'd) + Pub/Sub envelope decoding + `gmail_pubsub_topics` tenant resolution + `handle_push` + `drain_mailbox_history` (real) + per-message thread canonicalization + observation write. **Bypasses:** DWD token minting, real Google httpx client, real OIDC cert fetch.

**Usage:**

```python
import os
from fastapi import FastAPI
from services.synthetic.fixtures import make_gmail_mailbox
from services.synthetic.live_generators import GmailPubSubGenerator
from services.synthetic.mock_clients import MockGmailClient
from services.webhooks.gmail_pubsub import router as gmail_router

# Required env (validated at handler import even though no-op'd in tests).
os.environ["GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE"] = "https://x.example/webhook"
os.environ["GMAIL_PUBSUB_PUSH_OIDC_SA"] = "pusher@y1.iam.gserviceaccount.com"

# Build a FastAPI app with the Gmail Pub/Sub router mounted.
app = FastAPI()
app.include_router(gmail_router)
class _Deps: pass
deps = _Deps(); deps.pool = pool
app.state.deps = deps

# Construct a mailbox + mock client.
mock = MockGmailClient(fixture=make_gmail_mailbox(
    email="alice@x.com", messages=0, starting_history_id=1000,
))

async with GmailPubSubGenerator(
    app=app, pool=pool, mailboxes={"alice@x.com": mock},
) as gen:
    # Dispatch one notification with 3 new messages.
    result = await gen.simulate_push(
        mailbox_email="alice@x.com", new_messages=3,
    )
    assert result.http_status == 200
    assert result.response_body["ingested"] == 3
```

### 9.2. Burst patterns

Each tenant's pattern is a list of `(delay_ms, message_count)` tuples — the generator sleeps then dispatches:

```python
from services.synthetic.scenarios import (
    LivePubSubScenario, PerTenantBurst,
    STEADY_STATE_PUBSUB, BURSTY_PUBSUB, MIXED_PUBSUB,
)

# Custom pattern: 2 messages every 500ms, five times.
custom = LivePubSubScenario(tenants=[
    PerTenantBurst(
        tenant_slug="custom",
        mailbox_email="custom@x.com",
        burst_pattern=[(500, 2)] * 5,
    ),
])

async with GmailPubSubGenerator(app=app, pool=pool,
                                mailboxes={...}) as gen:
    result = await gen.run_scenario(custom)
```

### 9.3. Replay simulation (at-least-once-delivery idempotency)

`LivePubSubScenario.replay_probability` ∈ [0.0, 1.0] — the generator duplicates a fraction of pushes (same historyId, same payload). Tests verify the writer's `external_id` UNIQUE constraint dedupes them:

```python
scenario = LivePubSubScenario(
    tenants=[...],
    replay_probability=1.0,  # every push is followed by a duplicate
)
# After scenario runs: observation count == unique message count
# (duplicates were sent but deduped at the writer).
```

### 9.4. Discord Gateway — `DiscordGatewayGenerator`

Per [A24](./05-lld-amendments.md#a24--discord-gateway-synthetic-generator-in-process-event-injection-without-websocket-simulation). Invokes `handle_message_create` directly with synthesized Discord MESSAGE_CREATE payloads. **No WebSocket simulation** — A24 documents this as explicit non-coverage; lifecycle scenarios remain M4-tested-only.

**Coverage:** MESSAGE_CREATE dispatch (bot/webhook filters) + tenant resolution + ingest core + observation write + dedup. **Non-coverage:** WebSocket framing, HELLO/IDENTIFY/READY handshake, heartbeat protocol, session resume.

**Usage:**

```python
import time
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.integrations.discord.gateway.dispatch import DispatchDeps
from services.synthetic.fixtures import make_discord_guild
from services.synthetic.live_generators import (
    DiscordGatewayGenerator, GuildBinding,
)
from services.synthetic.mock_clients import MockDiscordClient
from services.webhooks.tenant_resolver import (
    InstallationCache, TenantResolverDeps, build_tenant_resolver,
    noop_metrics,
)

# Seed a tenant + provider_installations row for the test guild.
tenant_id = ...  # from test fixture
guild_id = "1504477009927999569"
await pool.execute(
    "INSERT INTO provider_installations "
    "(id, tenant_id, provider, installation_id, enabled) "
    "VALUES ($1, $2, 'discord', $3, TRUE)",
    uuid7(), tenant_id, guild_id,
)

deps = DispatchDeps(
    pool=pool,
    tenant_resolver=build_tenant_resolver(TenantResolverDeps(
        pool=pool, cache=InstallationCache(),
        clock=time.monotonic, metrics=noop_metrics(),
    )),
    actor_repo=ActorRepo(pool),
    alias_repo=EntityAliasRepo(pool),
    embedder=None,
    application_id="1504474857914499194",
)

mock = MockDiscordClient(fixture=make_discord_guild(
    guild_id=guild_id, channels=1, messages_per_channel=0,
))
mock._fixture["channels"][0]["id"] = "channel_test_001"

async with DiscordGatewayGenerator(
    dispatch_deps=deps,
    guild_bindings={guild_id: GuildBinding(
        guild_id=guild_id, mock_client=mock,
    )},
) as gen:
    result = await gen.simulate_message_create(
        guild_id=guild_id, channel_id="channel_test_001",
        content="hello from synthetic Gateway",
    )
    assert result.handler_succeeded
```

**Scenario presets:** `SINGLE_ACTIVE_CHANNEL`, `MULTI_CHANNEL_PER_GUILD`, `HIGH_VOLUME_BURST` (all in `services.synthetic.scenarios`).

**MESSAGE_UPDATE / MESSAGE_DELETE:** the generator records the event but does NOT invoke a handler — v1 dispatch has no handler for these (see A24). The methods return `SimulatedEventResult(handler_invoked=False, notes="...v1 dispatch scope...")` as runnable documentation.

### 9.5. Composing X3 backfill with Y1 live ingestion

Worked example: a tenant completes backfill via X3, then ongoing Gmail Pub/Sub notifications arrive:

```python
from services.synthetic.backfill_harness import BackfillHarness, BackfillScenario

# Phase A: install + backfill via X3.
scenarios = [BackfillScenario(
    tenant_slug="compose",
    source="gmail",
    fixture_params={"email": "compose@x.com", "messages": 5},
    expected_observation_count=5,
)]
backfill = BackfillHarness(pool=pool, scenarios=scenarios)
backfill_result = await backfill.run()
assert_all_complete(backfill_result)

# Phase B: drive ongoing live notifications.
async with GmailPubSubGenerator(
    app=app, pool=pool,
    mailboxes={"compose@x.com": shared_mock},
) as gen:
    await gen.simulate_push(mailbox_email="compose@x.com", new_messages=3)

# Phase C: assert backfill observations + live observations coexist.
total = await pool.fetchval(
    "SELECT count(*) FROM observations WHERE tenant_id = $1",
    backfill_result.outcomes[0].tenant_id,
)
assert total == 5 + 3  # 5 from backfill, 3 from live notification
```

The harness's properties-based assertions (`assert_no_duplicate_observations`, etc.) compose naturally with live-generator output — backfill + live writes go to the same `observations` table with the same `external_id` UNIQUE constraint guarding against duplicates.

### 9.6. Slack + GitHub webhooks — `SlackWebhookGenerator` / `GithubWebhookGenerator` (Z1)

These two drivers (A25) close the synthetic-coverage gap for Slack + GitHub *live* ingestion. Both dispatch real, signed webhooks in-process via `httpx.AsyncClient(transport=ASGITransport(app))` to the gateway app's webhook router, exercising the full path: signature verify → tenant resolution → (GitHub) replay-cache + repo filter → inline `ingest()` → observation write.

Both drivers **target a seeded `provider_installations` row** — they do not create installs. Seed the install first (mirroring what an OAuth callback writes), then drive live traffic:

```python
from services.gateway.main import build_app
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.rate_limit import RateLimiter
from services.synthetic.fixtures import make_slack_workspace, make_github_repos
from services.synthetic.live_generators.slack_webhook import SlackWebhookGenerator
from services.synthetic.live_generators.github_webhook import GithubWebhookGenerator
from services.synthetic.mock_clients import MockSlackClient, MockGithubClient

# Test env: Slack needs WEBHOOK_SECRET_SLACK + WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1;
# GitHub needs the App-level WEBHOOK_SECRET_GITHUB. MASTER_KEK keeps the secret
# store off its dev-warning branch under `filterwarnings = error`.

app = build_app(pool=pool, actor_repo=ActorRepo(pool),
                alias_repo=EntityAliasRepo(pool), embedder=None,
                rate_limiter=RateLimiter(), configure_logging=False)

# Slack: seed provider_installations(provider='slack', installation_id=team_id).
async with SlackWebhookGenerator(
    app=app, mock_client=MockSlackClient(fixture=make_slack_workspace(team_id="T1")),
    signing_secret=slack_secret,
) as gen:
    r = await gen.simulate_message(team_id="T1", channel_id="C1", content="hi")
    assert r.http_status in (200, 201)

# GitHub: seed provider_installations(provider='github', installation_id="999001").
async with GithubWebhookGenerator(
    app=app, mock_client=MockGithubClient(fixture=make_github_repos(
        org_or_user="octo", installation_id="999001")),
    signing_secret=github_secret,
) as gen:
    r = await gen.simulate_issue_event(
        installation_id="999001", repo_full_name="octo/repo", action="opened",
        issue_title="bug")
    assert r.http_status in (200, 201)
    # GitHub also has simulate_pull_request_event(...).
```

**Burst patterns + scenarios.** `LiveSlackScenario(tenants=[SlackTenantTraffic(...)] )` and `LiveGithubScenario(tenants=[GithubTenantTraffic(...)], event_type="issues"|"pull_request")` carry per-tenant `[(delay_ms, count), ...]` patterns, run via `gen.run_scenario(scenario)` (sequential within a tenant, concurrent across tenants). Presets: `STEADY_STATE_SLACK` / `BURSTY_SLACK` / `MIXED_SLACK` and `STEADY_STATE_GITHUB` / `BURSTY_GITHUB` / `MIXED_GITHUB`.

**Replay simulation.** Set `replay_probability` (or call `simulate_*(..., replay=True)`). Slack reuses the message `ts` → observation-layer `external_id = "{channel}:{ts}"` dedup. GitHub reuses the original `X-GitHub-Delivery` + `node_id` → router replay-cache drop (HTTP 200 `handled:replay`), with the `external_id = node_id` dedup as backstop. Either way: no double-counted observation.

**Signature handling.** Drivers sign with the same secret the app is configured with. Pass `signing_secret=` (or let it read the env var). `simulate_*(..., tamper_signature=True)` produces a wrong signature for negative tests (expect 401, no observation). An unknown `team_id` / `installation_id` (no seeded install) returns 401 (`UnknownInstallation`) after signature verification — also no observation.

**Composition with X3.** Same pattern as §9.5: install via X3 → backfill → drive live webhooks via Z1 targeting the same tenant. Backfill + live observations coexist in the shared `observations` table under the `external_id` UNIQUE constraint.

---

## 10. Known limitations

- **Real Kafka required for X3 E2E.** No in-memory broker today; live-ingestion generators (Y1/Y2) work without one because they don't go through the M6 chain.
- **Gmail-only reshare configuration.** GitHub/Slack/Discord reshare scenarios require fixture-generator extensions (see §3).
- **Single fault profile per tenant.** A tenant can't switch profiles mid-run (e.g., happy for the first 100 requests, then flaky). Per-call profile dispatch is a future extension.
- **The X3 harness doesn't drive HTTP OAuth callbacks.** It writes install + trigger rows directly. The X1 retrofit tests verify the OAuth-layer atomicity independently.
- **Y1 bypasses real OIDC + DWD token minting** by design. Those layers have their own tests (`services/integrations/gmail/tests/test_oidc_verify.py`); Y1's scope is the M6 + observation-write surface.
- **Z1 fault profiles are inert for the webhook ingest path.** The Slack/GitHub webhook *ingest* path never calls the source Web API, so a `RATE_LIMITED` / `FLAKY` mock profile has no effect on Z1 dispatch (the scenario accepts `fault_profile` only for parity). Source-API fault behaviour is exercised on the *backfill* path via the X3 harness.
- **Z1 targets seeded installs only.** The drivers resolve to a pre-seeded `provider_installations` row (Slack by `team_id`, GitHub by `installation.id`); they do not run OAuth. Seed the install (or use X3) before driving live traffic.
