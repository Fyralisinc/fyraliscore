# Validation Run 6 — All-12-source concurrent backfill + live overlap

**Status:** READY ✅
**Started:** 2026-06-07T17:31:47.695267+00:00
**Wall time:** 11.7s
**Tenants:** 24

## Pre-flight (fixture realism — Decision 12)

- gmail: external_id='gmail:cdd11c4e-9ce1-4c41-b573-68' ✅
- github: external_id='I_kwDO8x2NYDDUMdgx' ✅
- slack: external_id='C_9C1302B2C2:1767225600.000000' ✅
- discord: external_id='discord:402097' ✅

## State reset (Decision 10)

- recreated 50 topics; cleared 0 stale S3 objects

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

## Live phase (A30)

- tenants_per_source=2; live=3 events/tenant per source
- peak simultaneous backfill source_onboarding_runs in_progress: 24
- live ingress: 202=webhook Kafka cutover (github/slack/jira/mercury/quickbooks/grafana); 200=gmail pubsub / google push (inline drain) / notion shadow-write; discord=direct dispatch

## Per-source × per-dimension coverage

| Source | Backfill | Live | Cross-path dedup | Signature gate | Replay idempotency |
|---|---|---|---|---|---|
| gmail | ✅ | ✅ [200] | ✅ | — | overlap×2 |
| github | ✅ | ✅ [202] | ✅ | — | overlap×3 |
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

## Assertions

- ✅ `assert_live_during_backfill_overlap(all 12 sources)` — every source received ≥1 live burst while its backfill was in_progress: {'gmail': 2, 'github': 3, 'slack': 3, 'discord': 3, 'google_calendar': 3, 'google_drive': 3, 'jira': 3, 'mercury': 3, 'notion': 3, 'quickbooks': 3, 'grafana': 3, 'telegram': 3}
- ✅ `assert_all_sources_backfilled_concurrently` — peak simultaneous in_progress source runs = 24 (expected ≥ 12)
- ✅ `assert_live_routed_through_expected_ingress` — all sources hit their expected live ingress status
- ✅ `assert_signature_validation_gate_holds` — 5/5 tampered events rejected (no 2xx)
- ✅ `assert_no_duplicate_observations_under_concurrency` — 172 observations, zero duplicate (source_channel, external_id, occurred_at) groups

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

