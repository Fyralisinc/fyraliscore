# Validation Run 6 — All-11-source concurrent backfill + live overlap

**Status:** READY ✅
**Started:** 2026-06-07T15:29:07.062495+00:00
**Wall time:** 12.4s
**Tenants:** 22

## Pre-flight (fixture realism — Decision 12)

- gmail: external_id='gmail:6c3e604c-af1a-4aa2-abe6-54' ✅
- github: external_id='I_kwDO8x2NYDDUMdgx' ✅
- slack: external_id='C_9C1302B2C2:1767225600.000000' ✅
- discord: external_id='discord:402097' ✅

## State reset (Decision 10)

- recreated 46 topics; cleared 0 stale S3 objects

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

## Live phase (A30)

- tenants_per_source=2; live=3 events/tenant per source
- peak simultaneous backfill source_onboarding_runs in_progress: 19
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

## Assertions

- ✅ `assert_live_during_backfill_overlap(all 11 sources)` — every source received ≥1 live burst while its backfill was in_progress: {'gmail': 1, 'github': 2, 'slack': 3, 'discord': 3, 'google_calendar': 3, 'google_drive': 3, 'jira': 3, 'mercury': 3, 'notion': 3, 'quickbooks': 3, 'grafana': 3}
- ✅ `assert_all_sources_backfilled_concurrently` — peak simultaneous in_progress source runs = 19 (expected ≥ 11)
- ✅ `assert_live_routed_through_expected_ingress` — all sources hit their expected live ingress status
- ✅ `assert_signature_validation_gate_holds` — 5/5 tampered events rejected (no 2xx)
- ✅ `assert_no_duplicate_observations_under_concurrency` — 156 observations, zero duplicate (source_channel, external_id, occurred_at) groups

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

