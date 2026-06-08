# Validation Run 6 — All-12-source concurrent backfill + live overlap

**Status:** READY ✅
**Started:** 2026-06-08T05:28:35.438029+00:00
**Wall time:** 14.1s
**Tenants:** 32

## Pre-flight (fixture realism — Decision 12)

- gmail: external_id='gmail:1b196f1a-226e-4d0d-96e9-40' ✅
- github: external_id='I_kwDO8x2NYDDUMdgx' ✅
- slack: external_id='C_9C1302B2C2:1767225600.000000' ✅
- discord: external_id='discord:402097' ✅
- brex: external_id='brex:acct_2b27365a9a8cca66:txn:t' ✅
- ramp: external_id='ramp:r-pre:txn:1001:overdue.1' ✅
- gusto: external_id='gusto:c-pre:invoice:1001:1' ✅
- deel: external_id='deel:con_63349d041ea22e18:paymen' ✅

## State reset (Decision 10)

- recreated 66 topics; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| gmail | 2 | 16 | 16 | ✅ |
| github | 2 | 18 | 18 | ✅ |
| slack | 2 | 16 | 16 | ✅ |
| discord | 2 | 16 | 16 | ✅ |
| google_calendar | 2 | 12 | 12 | ✅ |
| google_drive | 2 | 12 | 12 | ✅ |
| jira | 2 | 12 | 12 | ✅ |
| mercury | 2 | 16 | 16 | ✅ |
| notion | 2 | 12 | 12 | ✅ |
| quickbooks | 2 | 14 | 14 | ✅ |
| grafana | 2 | 12 | 12 | ✅ |
| telegram | 2 | 16 | 16 | ✅ |
| brex | 2 | 16 | 16 | ✅ |
| ramp | 2 | 14 | 14 | ✅ |
| gusto | 2 | 14 | 14 | ✅ |
| deel | 2 | 16 | 16 | ✅ |

## Live phase (A30)

- tenants_per_source=2; live=3 events/tenant per source
- peak simultaneous backfill source_onboarding_runs in_progress: 29
- live ingress: 202=webhook Kafka cutover (github/slack/jira/mercury/quickbooks/grafana); 200=gmail pubsub / google push (inline drain) / notion shadow-write; discord=direct dispatch

## Per-source × per-dimension coverage

| Source | Backfill | Live | Cross-path dedup | Signature gate | Replay idempotency |
|---|---|---|---|---|---|
| gmail | ✅ | ✅ [200] | ✅ | — | overlap×1 |
| github | ✅ | ✅ [202] | ✅ | — | overlap×2 |
| slack | ✅ | ✅ [202] | ✅ | — | overlap×3 |
| discord | ✅ | ✅ ['direct'] | ✅ | — | overlap×3 |
| google_calendar | ✅ | ✅ [200] | ✅ | — | overlap×3 |
| google_drive | ✅ | ✅ [200] | ✅ | — | overlap×3 |
| jira | ✅ | ✅ [202] | ✅ | ✅ | overlap×3 |
| mercury | ✅ | ✅ [202] | ✅ | ✅ | overlap×3 |
| notion | ✅ | ✅ [200] | ✅ | ✅ | overlap×3 |
| quickbooks | ✅ | ✅ [202] | ✅ | ✅ | overlap×3 |
| grafana | ✅ | ✅ [202] | ✅ | ✅ | overlap×3 |
| telegram | ✅ | ✅ ['direct'] | ✅ | — | overlap×3 |
| brex | ✅ | ✅ [202] | ✅ | ✅ | overlap×3 |
| ramp | ✅ | ✅ [202] | ✅ | ✅ | overlap×3 |
| gusto | ✅ | ✅ [202] | ✅ | ✅ | overlap×3 |
| deel | ✅ | ✅ [202] | ✅ | ✅ | overlap×3 |

## Assertions

- ✅ `assert_live_during_backfill_overlap(all 16 sources)` — every source received ≥1 live burst while its backfill was in_progress: {'gmail': 1, 'github': 2, 'slack': 3, 'discord': 3, 'google_calendar': 3, 'google_drive': 3, 'jira': 3, 'mercury': 3, 'notion': 3, 'quickbooks': 3, 'grafana': 3, 'telegram': 3, 'brex': 3, 'ramp': 3, 'gusto': 3, 'deel': 3}
- ✅ `assert_all_sources_backfilled_concurrently` — peak simultaneous in_progress source runs = 29 (expected ≥ 16)
- ✅ `assert_live_routed_through_expected_ingress` — all sources hit their expected live ingress status
- ✅ `assert_signature_validation_gate_holds` — 9/9 tampered events rejected (no 2xx)
- ✅ `assert_no_duplicate_observations_under_concurrency` — 232 observations, zero duplicate (source_channel, external_id, occurred_at) groups

## Subprocess exit codes (Decision 11)

- `oauth_poller`: rc=0
- `tenant_onboarding`: rc=0
- `source_onboarding`: rc=0
- `shard_fetch`: rc=0
- `reconciler`: rc=0
- `normalizer`: rc=0 — clean (ticket #45 resolved)
- `observation_writer`: rc=0 — clean (ticket #45 resolved)

## Notes

- Consumer rc=-9/-15 expected per ticket #45; greened by the rc annotation.

