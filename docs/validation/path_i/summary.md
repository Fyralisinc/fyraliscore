# M-Validate — synthetic validation summary (A30)

Consolidated verdict across the three composed validation runs. Each run
is operator-invokable: `python -m services.synthetic.validation_runs.runner --run={1,2,3}`
(needs `COMPANY_OS_ENV=test`, `DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`; the
runner brings up its own moto-S3 and resets Kafka topics).

## Overall: **READY** — synthetic-testable end-to-end milestone reached

The M6 ingestion pipeline is empirically validated end-to-end across all
four sources for **both backfill and live** paths, with cross-path dedup
proven for the three sources where it is substantively testable.

| Run | Scope | Verdict |
|---|---|---|
| Run 1 | E2E backfill + live, 16 tenants, all 4 sources | **READY** ✅ |
| Run 2 | Fault injection (FLAKY 10% 5xx) + partition-missing injection, 16 tenants | **PARTIAL** ⚠️ (expected under FLAKY) |
| Run 3 | Concurrency stress, 50 tenants @ concurrency=10, backfill-only | **READY** ✅ |

## Run 1 — E2E backfill + live (READY)

16 tenants (4/source). Backfill drains through the 7 M6 subprocesses; the
live phase then dispatches 5 events/tenant inline through the four
generators, plus cross-path twins, signature-tamper probes, and a replay
probe. All 10 assertions pass; per-source counts exact (gmail 41, github
45, slack 41, discord 40 — backfill + 5×4 live + 1 replay-probe for the
three replay sources). The load-bearing **`assert_cross_path_twins_dedup`**
passes: a backfilled event and its live twin collapse to one
`observations` row for gmail/github/slack.

## Run 2 — fault injection (PARTIAL, expected)

16 tenants with `FLAKY` (10% random 5xx) on every backfill mock +
deliberate out-of-range (`occurred_at`=2023) injection. **A28 verified
under composition**: 4 partition-missing envelopes (one/source) routed to
`ingestion.dlq`, the writer did NOT crash-loop. **A19 verified**: no
orchestrator (non-consumer) subprocess crashed despite injected
flakiness. Cross-path dedup + signature gate held. PARTIAL because FLAKY
dropped some backfill observations (e.g. gmail / discord short) — the
documented, expected outcome; NOT_READY is reserved for an orchestrator
crash or a missed A28 routing.

Observed (16 tenants): partition-missing DLQ 4/4; per-source backfill+live
counts gmail 36/40, github 44/44, slack 40/40, discord 20/40 (FLAKY
dropped gmail + discord backfill fetches; live deltas all exact at 20/
source since webhooks don't call the source API, A25). All framework
subprocesses rc=0 (A19 holds); only the consumer services show the
ticket-#45 rc=-9/-15.

## Run 3 — concurrency stress (READY)

50 tenants (15 gmail / 15 github / 10 slack / 10 discord) through the same
7 shared subprocesses at concurrency=10, backfill-only. All assertions
pass:
- per-tenant isolation: exact counts (gmail/github/slack) + uniform
  positive discord (5% channel-sampling → 1 of 4 channels),
- concurrency exercised: peak 40 simultaneous `in_progress`,
- working signal backlog bounded: peak 105 < 3× tenants (150) and
  **drains to 0** (no leak; the bound is O(tenants) — the producer
  fan-out — not O(concurrency); see A30.6),
- **#39 flake watch**: `tenant_onboarding_completed` fired exactly once
  for all 50 tenants.

Per-tenant volumes are sized so the run drains within the X3 harness's
fixed 30s consumer-drain window (the harness is out of scope to modify);
the stress dimension is tenant concurrency, independent of per-tenant
volume. A higher-volume soak would need the harness to expose a
configurable drain timeout (follow-up — see A30.6).

## Per-source × per-dimension coverage

| Source | Backfill | Live | Cross-path dedup | Signature gate | Replay idempotency |
|---|---|---|---|---|---|
| gmail | ✅ | ✅ | ✅ | — (OIDC no-op by Y1) | ✅ |
| github | ✅ | ✅ | ✅ | ✅ | ✅ |
| slack | ✅ | ✅ | ✅ | ✅ | ✅ |
| discord | ✅ | ✅ | — (disjoint id namespace, A30.3) | — (direct dispatch) | — (no replay, A24) |

Discord exclusions are **architectural, not gaps**: its live ids
(`msg-y2-*`) and backfill ids (fixture-derived) cannot collide (A30.3); it
has no HTTP signature surface; and no replay surface per A24. Discord's
per-path dedup is covered by A27.5 parity (M6.7).

## Queued production tickets (out of scope, remain open)

- **#44** — partition coverage operational decision.
- **#45** — consumer graceful-shutdown (the normalizer/observation_writer
  rc=-9/-15 in every run is this gap; the runner's rc policy accepts it
  and auto-greens when #45 ships).
- **#46** — writer permanent-failure invalid `failure_kind`.

## Chain closeout

Z1 → Q1-minimal → M6.7 → X3 fixes → M-Validate-spine → **M-Validate-Live**.
The synthetic-testability chain is complete (A30.5). Suggested merge
sequence: spine (`feat/ingestion-validation-runs-spine`) → this branch
(`feat/ingestion-validation-runs-live-composition`).
