# Validation Run 6 — All-25-source concurrent backfill + live overlap

**Status:** READY ✅
**Started:** 2026-06-10T18:19:28.574974+00:00
**Wall time:** 40.2s
**Tenants:** 100

## Pre-flight (fixture realism — Decision 12)

- gmail: external_id='gmail:cb5a7e23-a4da-4a8b-8433-92' ✅
- github: external_id='I_kwDO8x2NYDDUMdgx:closed' ✅
- slack: external_id='C_9C1302B2C2:1767225600.000000' ✅
- discord: external_id='discord:402097' ✅
- brex: external_id='brex:acct_2b27365a9a8cca66:txn:t' ✅
- ramp: external_id='ramp:r-pre:txn:1001:overdue.1' ✅
- gusto: external_id='gusto:c-pre:invoice:1001:1' ✅
- deel: external_id='deel:con_63349d041ea22e18:paymen' ✅
- fireflies: external_id='fireflies:ws-pre:transcript:ts_d' ✅
- signal: external_id='signal:8014b14e-13af-45c3-8ba4-5' ✅
- aws: external_id='aws:900000000001:us-east-1:event' ✅
- miro: external_id='miro:org-pre:item:item_60af6af84' ✅
- figma: external_id='figma:team-pre:event:evt_83d65a9' ✅
- carta: external_id='carta:firm-pre:shareholder:1000:' ✅

## State reset (Decision 10)

- recreated 102 topics; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| gmail | 4 | 44 | 44 | ✅ |
| github | 4 | 48 | 48 | ✅ |
| slack | 4 | 44 | 44 | ✅ |
| discord | 4 | 44 | 44 | ✅ |
| google_calendar | 4 | 36 | 36 | ✅ |
| google_drive | 4 | 36 | 36 | ✅ |
| jira | 4 | 36 | 36 | ✅ |
| mercury | 4 | 44 | 44 | ✅ |
| notion | 4 | 36 | 36 | ✅ |
| quickbooks | 4 | 40 | 40 | ✅ |
| grafana | 4 | 36 | 36 | ✅ |
| telegram | 4 | 44 | 44 | ✅ |
| brex | 4 | 44 | 44 | ✅ |
| ramp | 4 | 40 | 40 | ✅ |
| gusto | 4 | 40 | 40 | ✅ |
| deel | 4 | 44 | 44 | ✅ |
| fireflies | 4 | 40 | 40 | ✅ |
| signal | 4 | 44 | 44 | ✅ |
| aws | 4 | 36 | 36 | ✅ |
| miro | 4 | 40 | 40 | ✅ |
| figma | 4 | 40 | 40 | ✅ |
| carta | 4 | 40 | 40 | ✅ |
| hibob | 4 | 40 | 40 | ✅ |
| ashby | 4 | 44 | 44 | ✅ |
| linkedin | 4 | 36 | 36 | ✅ |

## Live phase (A30)

- tenants_per_source=4; live=6 events/tenant per source
- peak simultaneous backfill source_onboarding_runs in_progress: 96
- live ingress: 202=webhook Kafka cutover (github/slack/jira/mercury/quickbooks/grafana); 200=gmail pubsub / google push (inline drain) / notion shadow-write; discord=direct dispatch

## Per-source × per-dimension coverage

| Source | Backfill | Live | Cross-path dedup | Signature gate | Replay idempotency |
|---|---|---|---|---|---|
| gmail | ✅ | ✅ [200] | ✅ | — | overlap×2 |
| github | ✅ | ✅ [202] | ✅ | — | overlap×6 |
| slack | ✅ | ✅ [202] | ✅ | — | overlap×6 |
| discord | ✅ | ✅ ['direct'] | ✅ | — | overlap×6 |
| google_calendar | ✅ | ✅ [200] | ✅ | — | overlap×6 |
| google_drive | ✅ | ✅ [200] | ✅ | — | overlap×6 |
| jira | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| mercury | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| notion | ✅ | ✅ [200] | ✅ | ✅ | overlap×6 |
| quickbooks | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| grafana | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| telegram | ✅ | ✅ ['direct'] | ✅ | — | overlap×6 |
| brex | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| ramp | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| gusto | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| deel | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| fireflies | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| signal | ✅ | ✅ ['direct'] | ✅ | — | overlap×6 |
| aws | ✅ | ✅ ['direct'] | ✅ | — | overlap×6 |
| miro | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| figma | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| carta | ✅ | ✅ ['direct'] | ✅ | — | overlap×6 |
| hibob | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| ashby | ✅ | ✅ [202] | ✅ | ✅ | overlap×6 |
| linkedin | ✅ | ✅ ['direct'] | ✅ | — | overlap×6 |

## Assertions

- ✅ `assert_live_during_backfill_overlap(all 25 sources)` — every source received ≥1 live burst while its backfill was in_progress: {'gmail': 2, 'github': 6, 'slack': 6, 'discord': 6, 'google_calendar': 6, 'google_drive': 6, 'jira': 6, 'mercury': 6, 'notion': 6, 'quickbooks': 6, 'grafana': 6, 'telegram': 6, 'brex': 6, 'ramp': 6, 'gusto': 6, 'deel': 6, 'fireflies': 6, 'signal': 6, 'aws': 6, 'miro': 6, 'figma': 6, 'carta': 6, 'hibob': 6, 'ashby': 6, 'linkedin': 6}
- ✅ `assert_all_sources_backfilled_concurrently` — peak simultaneous in_progress source runs = 96 (expected ≥ 25)
- ✅ `assert_live_routed_through_expected_ingress` — all sources hit their expected live ingress status
- ✅ `assert_signature_validation_gate_holds` — 14/14 tampered events rejected (no 2xx)
- ✅ `assert_no_duplicate_observations_under_concurrency` — 1016 observations, zero duplicate (source_channel, external_id, occurred_at) groups

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

