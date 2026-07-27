# Validation Run 2 — Fault injection across all canonical sources (FLAKY + partition recovery/bounds)

**Status:** READY ✅
**Started:** 2026-07-27T05:24:41.089617+00:00
**Wall time:** 69.7s
**Tenants:** 54

## Pre-flight (fixture realism — Decision 12)

- slack: 3 records, external_id='C_9C1302B2C2:1767225600.000000' ✅
- github: 2 records, external_id='I_kwDO8x2NYDDUMdgx:closed' ✅
- discord: 3 records, external_id='discord:402097' ✅
- gmail: 3 records, external_id='gmail:f2094896-827a-402e-9f64-6e' ✅
- brex: 4 records, external_id='brex:acct_2b27365a9a8cca66:txn:t' ✅
- ramp: 2 records, external_id='ramp:r-pre:txn:d3a50fe2-d8dc-0ae' ✅
- gusto: 2 records, external_id='gusto:c-pre:employee:8564f649-77' ✅
- deel: 4 records, external_id='deel:con_63349d041ea22e18:paymen' ✅
- fireflies: 3 records, external_id='fireflies:ws-pre:transcript:ts_d' ✅
- signal: 3 records, external_id='signal:54ccb0b1-140c-477b-a7bd-3' ✅
- aws: 3 records, external_id='aws:900000000001:us-east-1:event' ✅
- miro: 3 records, external_id='miro:org-pre:item:item_60af6af84' ✅
- figma: 3 records, external_id='figma:team-pre:event:evt_83d65a9' ✅
- carta: 1 records, external_id='carta:firm-pre:stakeholder:1000:' ✅
- hibob: 1 records, external_id='hibob:hibob-co-pre:employee:1000' ✅

## State reset (Decision 10)

- recreated ['ingestion.raw.slack', 'ingestion.raw.github', 'ingestion.raw.discord', 'ingestion.raw.gmail', 'ingestion.raw.notion', 'ingestion.raw.google_calendar', 'ingestion.raw.google_drive', 'ingestion.raw.jira', 'ingestion.raw.mercury', 'ingestion.raw.quickbooks', 'ingestion.raw.grafana', 'ingestion.raw.telegram', 'ingestion.raw.brex', 'ingestion.raw.ramp', 'ingestion.raw.gusto', 'ingestion.raw.deel', 'ingestion.raw.fireflies', 'ingestion.raw.signal', 'ingestion.raw.aws', 'ingestion.raw.miro', 'ingestion.raw.figma', 'ingestion.raw.carta', 'ingestion.raw.hibob', 'ingestion.raw.ashby', 'ingestion.raw.linkedin', 'ingestion.raw.whatsapp', 'ingestion.raw.facebook_pages', 'ingestion.normalized.slack', 'ingestion.normalized.github', 'ingestion.normalized.discord', 'ingestion.normalized.gmail', 'ingestion.normalized.notion', 'ingestion.normalized.google_calendar', 'ingestion.normalized.google_drive', 'ingestion.normalized.jira', 'ingestion.normalized.mercury', 'ingestion.normalized.quickbooks', 'ingestion.normalized.grafana', 'ingestion.normalized.telegram', 'ingestion.normalized.brex', 'ingestion.normalized.ramp', 'ingestion.normalized.gusto', 'ingestion.normalized.deel', 'ingestion.normalized.fireflies', 'ingestion.normalized.signal', 'ingestion.normalized.aws', 'ingestion.normalized.miro', 'ingestion.normalized.figma', 'ingestion.normalized.carta', 'ingestion.normalized.hibob', 'ingestion.normalized.ashby', 'ingestion.normalized.linkedin', 'ingestion.normalized.whatsapp', 'ingestion.normalized.facebook_pages', 'ingestion.embedding.slack', 'ingestion.embedding.github', 'ingestion.embedding.discord', 'ingestion.embedding.gmail', 'ingestion.embedding.notion', 'ingestion.embedding.google_calendar', 'ingestion.embedding.google_drive', 'ingestion.embedding.jira', 'ingestion.embedding.mercury', 'ingestion.embedding.quickbooks', 'ingestion.embedding.grafana', 'ingestion.embedding.telegram', 'ingestion.embedding.brex', 'ingestion.embedding.ramp', 'ingestion.embedding.gusto', 'ingestion.embedding.deel', 'ingestion.embedding.fireflies', 'ingestion.embedding.signal', 'ingestion.embedding.aws', 'ingestion.embedding.miro', 'ingestion.embedding.figma', 'ingestion.embedding.carta', 'ingestion.embedding.hibob', 'ingestion.embedding.ashby', 'ingestion.embedding.linkedin', 'ingestion.embedding.whatsapp', 'ingestion.embedding.facebook_pages', 'ingestion.summarization.slack', 'ingestion.summarization.github', 'ingestion.summarization.discord', 'ingestion.summarization.gmail', 'ingestion.summarization.notion', 'ingestion.summarization.google_calendar', 'ingestion.summarization.google_drive', 'ingestion.summarization.jira', 'ingestion.summarization.mercury', 'ingestion.summarization.quickbooks', 'ingestion.summarization.grafana', 'ingestion.summarization.telegram', 'ingestion.summarization.brex', 'ingestion.summarization.ramp', 'ingestion.summarization.gusto', 'ingestion.summarization.deel', 'ingestion.summarization.fireflies', 'ingestion.summarization.signal', 'ingestion.summarization.aws', 'ingestion.summarization.miro', 'ingestion.summarization.figma', 'ingestion.summarization.carta', 'ingestion.summarization.hibob', 'ingestion.summarization.ashby', 'ingestion.summarization.linkedin', 'ingestion.summarization.whatsapp', 'ingestion.summarization.facebook_pages', 'ingestion.dlq.slack', 'ingestion.dlq.github', 'ingestion.dlq.discord', 'ingestion.dlq.gmail', 'ingestion.dlq.notion', 'ingestion.dlq.google_calendar', 'ingestion.dlq.google_drive', 'ingestion.dlq.jira', 'ingestion.dlq.mercury', 'ingestion.dlq.quickbooks', 'ingestion.dlq.grafana', 'ingestion.dlq.telegram', 'ingestion.dlq.brex', 'ingestion.dlq.ramp', 'ingestion.dlq.gusto', 'ingestion.dlq.deel', 'ingestion.dlq.fireflies', 'ingestion.dlq.signal', 'ingestion.dlq.aws', 'ingestion.dlq.miro', 'ingestion.dlq.figma', 'ingestion.dlq.carta', 'ingestion.dlq.hibob', 'ingestion.dlq.ashby', 'ingestion.dlq.linkedin', 'ingestion.dlq.whatsapp', 'ingestion.dlq.facebook_pages', 'ingestion.tenant_traffic_signal', 'onboarding.progress']; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| slack | 2 | 311 | 311 | ✅ |
| github | 2 | 411 | 411 | ✅ |
| discord | 2 | 251 | 251 | ✅ |
| gmail | 2 | 31 | 31 | ✅ |
| notion | 2 | 17 | 17 | ✅ |
| google_calendar | 2 | 23 | 23 | ✅ |
| google_drive | 2 | 17 | 17 | ✅ |
| jira | 2 | 17 | 17 | ✅ |
| mercury | 2 | 21 | 21 | ✅ |
| quickbooks | 2 | 19 | 19 | ✅ |
| grafana | 2 | 21 | 21 | ✅ |
| telegram | 2 | 21 | 21 | ✅ |
| brex | 2 | 21 | 21 | ✅ |
| ramp | 2 | 19 | 19 | ✅ |
| gusto | 2 | 15 | 15 | ✅ |
| deel | 2 | 21 | 21 | ✅ |
| fireflies | 2 | 19 | 19 | ✅ |
| signal | 2 | 21 | 21 | ✅ |
| aws | 2 | 17 | 17 | ✅ |
| miro | 2 | 19 | 19 | ✅ |
| figma | 2 | 21 | 21 | ✅ |
| carta | 2 | 19 | 19 | ✅ |
| hibob | 2 | 19 | 19 | ✅ |
| ashby | 2 | 59 | 59 | ✅ |
| linkedin | 2 | 17 | 17 | ✅ |
| whatsapp | 2 | 11 | 11 | ✅ |
| facebook_pages | 2 | 23 | 23 | ✅ |

## Live phase (A30)

- FLAKY (one-in-ten 503) applied to all Provider Lab sources
- partition self-heals (distinct month/source): 27
- out-of-bounds rejections (one/source): 27
- live per-source deltas: {'slack': 10, 'github': 10, 'discord': 10, 'gmail': 10, 'notion': 10, 'google_calendar': 10, 'google_drive': 10, 'jira': 10, 'mercury': 10, 'quickbooks': 10, 'grafana': 10, 'telegram': 10, 'brex': 10, 'ramp': 10, 'gusto': 10, 'deel': 10, 'fireflies': 10, 'signal': 10, 'aws': 10, 'miro': 10, 'figma': 10, 'carta': 10, 'hibob': 10, 'ashby': 10, 'linkedin': 10, 'facebook_pages': 10, 'whatsapp': 10}
- live drain stable: True

## Assertions

- ✅ `assert_partition_boundary_contract`
- ✅ `assert_cross_path_twins_dedup`
- ✅ `assert_signature_validation_gate_holds_for_hmac_sources`
- ✅ `assert_live_observations_attributed_correctly`
- ✅ `assert_all_backfills_complete_after_transient_faults`
- ✅ `assert_backfill_counts_recovered_after_transient_faults`
- ✅ `assert_live_drain_stable`
- ✅ `assert_no_duplicate_observations`

## Subprocess exit codes (Decision 11)

- `oauth_poller`: rc=0
- `tenant_onboarding`: rc=0
- `source_onboarding`: rc=0
- `shard_fetch`: rc=0
- `reconciler`: rc=0
- `normalizer`: rc=0 — clean (ticket #45 resolved)
- `observation_writer`: rc=0 — clean (ticket #45 resolved)

## Notes

- FLAKY faults are transient and must recover without missing records; partial results fail certification. A19: orchestrator subprocesses must not crash; A28: a distinct missing month per source must self-heal, while far-out data must be DLQ'd as out_of_bounds_occurred_at with zero residual partition_missing. Historical sources=26; live-only sources=['whatsapp']. Consumer rc=-9/-15 expected per ticket #45.
