# Validation Run 1 — E2E backfill + live (all canonical sources)

**Status:** PASS ✅
**Started:** 2026-07-27T05:22:57.003440+00:00
**Wall time:** 77.6s
**Tenants:** 54

## Pre-flight (fixture realism — Decision 12)

- slack: 3 records, external_id='C_9C1302B2C2:1767225600.000000', occurred_at=2026-01-01T00:00:00+00:00 ✅
- github: 2 records, external_id='I_kwDO8x2NYDDUMdgx:closed', occurred_at=2026-01-01T00:21:00+00:00 ✅
- discord: 3 records, external_id='discord:402097', occurred_at=2026-01-01T00:00:00+00:00 ✅
- gmail: 3 records, external_id='gmail:31a847b4-d994-4af3-8278-d2', occurred_at=2026-01-01T00:02:00+00:00 ✅
- brex: 4 records, external_id='brex:acct_2b27365a9a8cca66:txn:t', occurred_at=2026-01-05T21:00:00+00:00 ✅
- ramp: 2 records, external_id='ramp:r-pre:txn:d3a50fe2-d8dc-0ae', occurred_at=2026-01-05T00:01:00+00:00 ✅
- gusto: 2 records, external_id='gusto:c-pre:employee:8564f649-77', occurred_at=2025-12-05T00:00:00+00:00 ✅
- deel: 4 records, external_id='deel:con_63349d041ea22e18:paymen', occurred_at=2026-01-05T21:00:00+00:00 ✅
- fireflies: 3 records, external_id='fireflies:ws-pre:transcript:ts_d', occurred_at=2026-01-05T21:00:00+00:00 ✅
- signal: 3 records, external_id='signal:780b10dd-f37c-436b-9103-3', occurred_at=2026-01-05T00:00:00+00:00 ✅
- aws: 3 records, external_id='aws:900000000001:us-east-1:event', occurred_at=2026-05-14T23:58:00+00:00 ✅
- miro: 3 records, external_id='miro:org-pre:item:item_60af6af84', occurred_at=2026-01-05T21:00:00+00:00 ✅
- figma: 3 records, external_id='figma:team-pre:event:evt_83d65a9', occurred_at=2026-01-05T21:00:00+00:00 ✅
- carta: 1 records, external_id='carta:firm-pre:stakeholder:1000:', occurred_at=2026-07-27T05:23:08.532964+00:00 ✅
- hibob: 1 records, external_id='hibob:hibob-co-pre:employee:1000', occurred_at=2026-01-05T00:00:00+00:00 ✅

## State reset (Decision 10)

- recreated ['ingestion.raw.slack', 'ingestion.raw.github', 'ingestion.raw.discord', 'ingestion.raw.gmail', 'ingestion.raw.notion', 'ingestion.raw.google_calendar', 'ingestion.raw.google_drive', 'ingestion.raw.jira', 'ingestion.raw.mercury', 'ingestion.raw.quickbooks', 'ingestion.raw.grafana', 'ingestion.raw.telegram', 'ingestion.raw.brex', 'ingestion.raw.ramp', 'ingestion.raw.gusto', 'ingestion.raw.deel', 'ingestion.raw.fireflies', 'ingestion.raw.signal', 'ingestion.raw.aws', 'ingestion.raw.miro', 'ingestion.raw.figma', 'ingestion.raw.carta', 'ingestion.raw.hibob', 'ingestion.raw.ashby', 'ingestion.raw.linkedin', 'ingestion.raw.whatsapp', 'ingestion.raw.facebook_pages', 'ingestion.normalized.slack', 'ingestion.normalized.github', 'ingestion.normalized.discord', 'ingestion.normalized.gmail', 'ingestion.normalized.notion', 'ingestion.normalized.google_calendar', 'ingestion.normalized.google_drive', 'ingestion.normalized.jira', 'ingestion.normalized.mercury', 'ingestion.normalized.quickbooks', 'ingestion.normalized.grafana', 'ingestion.normalized.telegram', 'ingestion.normalized.brex', 'ingestion.normalized.ramp', 'ingestion.normalized.gusto', 'ingestion.normalized.deel', 'ingestion.normalized.fireflies', 'ingestion.normalized.signal', 'ingestion.normalized.aws', 'ingestion.normalized.miro', 'ingestion.normalized.figma', 'ingestion.normalized.carta', 'ingestion.normalized.hibob', 'ingestion.normalized.ashby', 'ingestion.normalized.linkedin', 'ingestion.normalized.whatsapp', 'ingestion.normalized.facebook_pages', 'ingestion.embedding.slack', 'ingestion.embedding.github', 'ingestion.embedding.discord', 'ingestion.embedding.gmail', 'ingestion.embedding.notion', 'ingestion.embedding.google_calendar', 'ingestion.embedding.google_drive', 'ingestion.embedding.jira', 'ingestion.embedding.mercury', 'ingestion.embedding.quickbooks', 'ingestion.embedding.grafana', 'ingestion.embedding.telegram', 'ingestion.embedding.brex', 'ingestion.embedding.ramp', 'ingestion.embedding.gusto', 'ingestion.embedding.deel', 'ingestion.embedding.fireflies', 'ingestion.embedding.signal', 'ingestion.embedding.aws', 'ingestion.embedding.miro', 'ingestion.embedding.figma', 'ingestion.embedding.carta', 'ingestion.embedding.hibob', 'ingestion.embedding.ashby', 'ingestion.embedding.linkedin', 'ingestion.embedding.whatsapp', 'ingestion.embedding.facebook_pages', 'ingestion.summarization.slack', 'ingestion.summarization.github', 'ingestion.summarization.discord', 'ingestion.summarization.gmail', 'ingestion.summarization.notion', 'ingestion.summarization.google_calendar', 'ingestion.summarization.google_drive', 'ingestion.summarization.jira', 'ingestion.summarization.mercury', 'ingestion.summarization.quickbooks', 'ingestion.summarization.grafana', 'ingestion.summarization.telegram', 'ingestion.summarization.brex', 'ingestion.summarization.ramp', 'ingestion.summarization.gusto', 'ingestion.summarization.deel', 'ingestion.summarization.fireflies', 'ingestion.summarization.signal', 'ingestion.summarization.aws', 'ingestion.summarization.miro', 'ingestion.summarization.figma', 'ingestion.summarization.carta', 'ingestion.summarization.hibob', 'ingestion.summarization.ashby', 'ingestion.summarization.linkedin', 'ingestion.summarization.whatsapp', 'ingestion.summarization.facebook_pages', 'ingestion.dlq.slack', 'ingestion.dlq.github', 'ingestion.dlq.discord', 'ingestion.dlq.gmail', 'ingestion.dlq.notion', 'ingestion.dlq.google_calendar', 'ingestion.dlq.google_drive', 'ingestion.dlq.jira', 'ingestion.dlq.mercury', 'ingestion.dlq.quickbooks', 'ingestion.dlq.grafana', 'ingestion.dlq.telegram', 'ingestion.dlq.brex', 'ingestion.dlq.ramp', 'ingestion.dlq.gusto', 'ingestion.dlq.deel', 'ingestion.dlq.fireflies', 'ingestion.dlq.signal', 'ingestion.dlq.aws', 'ingestion.dlq.miro', 'ingestion.dlq.figma', 'ingestion.dlq.carta', 'ingestion.dlq.hibob', 'ingestion.dlq.ashby', 'ingestion.dlq.linkedin', 'ingestion.dlq.whatsapp', 'ingestion.dlq.facebook_pages', 'ingestion.tenant_traffic_signal', 'onboarding.progress']; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| slack | 2 | 311 | 311 | ✅ |
| github | 2 | 411 | 411 | ✅ |
| discord | 2 | 250 | 250 | ✅ |
| gmail | 2 | 31 | 31 | ✅ |
| notion | 2 | 16 | 16 | ✅ |
| google_calendar | 2 | 22 | 22 | ✅ |
| google_drive | 2 | 16 | 16 | ✅ |
| jira | 2 | 16 | 16 | ✅ |
| mercury | 2 | 20 | 20 | ✅ |
| quickbooks | 2 | 18 | 18 | ✅ |
| grafana | 2 | 20 | 20 | ✅ |
| telegram | 2 | 20 | 20 | ✅ |
| brex | 2 | 20 | 20 | ✅ |
| ramp | 2 | 18 | 18 | ✅ |
| gusto | 2 | 14 | 14 | ✅ |
| deel | 2 | 20 | 20 | ✅ |
| fireflies | 2 | 18 | 18 | ✅ |
| signal | 2 | 20 | 20 | ✅ |
| aws | 2 | 16 | 16 | ✅ |
| miro | 2 | 18 | 18 | ✅ |
| figma | 2 | 20 | 20 | ✅ |
| carta | 2 | 18 | 18 | ✅ |
| hibob | 2 | 18 | 18 | ✅ |
| ashby | 2 | 58 | 58 | ✅ |
| linkedin | 2 | 16 | 16 | ✅ |
| facebook_pages | 2 | 22 | 22 | ✅ |
| whatsapp | 2 | 10 | 10 | ✅ |

## Live phase (A30)

- live events/tenant: 5; per-source live deltas: {'slack': 10, 'github': 10, 'discord': 10, 'gmail': 10, 'notion': 10, 'google_calendar': 10, 'google_drive': 10, 'jira': 10, 'mercury': 10, 'quickbooks': 10, 'grafana': 10, 'telegram': 10, 'brex': 10, 'ramp': 10, 'gusto': 10, 'deel': 10, 'fireflies': 10, 'signal': 10, 'aws': 10, 'miro': 10, 'figma': 10, 'carta': 10, 'hibob': 10, 'ashby': 10, 'linkedin': 10, 'facebook_pages': 10, 'whatsapp': 10}
- cross-path twins (declared=['slack', 'github', 'gmail']; dispatched): ['github', 'gmail', 'slack']
- signature-gate probes (declared=['slack', 'github', 'notion', 'jira', 'mercury', 'quickbooks', 'grafana', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'figma', 'hibob', 'ashby', 'facebook_pages']): [('slack', 401), ('github', 401), ('notion', 401), ('jira', 401), ('mercury', 401), ('quickbooks', 401), ('grafana', 401), ('brex', 401), ('ramp', 401), ('gusto', 401), ('deel', 401), ('fireflies', 401), ('figma', 401), ('hibob', 401), ('ashby', 401), ('facebook_pages', 401), ('whatsapp', 401)]
- replay probe (declared=['slack', 'github', 'gmail']; dispatched_unique→observed): {'slack': 1, 'github': 1, 'gmail': 1}
- live drain stable: True
- live-only sources: ['whatsapp']

## Per-source × per-dimension coverage

| Source | Backfill | Live | Cross-path dedup | Signature gate | Replay idempotency |
|---|---|---|---|---|---|
| slack | ✅ | ✅ | ✅ | ✅ | ✅ |
| github | ✅ | ✅ | ✅ | ✅ | ✅ |
| discord | ✅ | ✅ | — (not in TWIN_SOURCES) | — (not in HMAC_SOURCES) | — (not in REPLAY_SOURCES) |
| gmail | ✅ | ✅ | ✅ | — (not in HMAC_SOURCES) | ✅ |
| notion | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| google_calendar | ✅ | ✅ | — (not in TWIN_SOURCES) | — (not in HMAC_SOURCES) | — (not in REPLAY_SOURCES) |
| google_drive | ✅ | ✅ | — (not in TWIN_SOURCES) | — (not in HMAC_SOURCES) | — (not in REPLAY_SOURCES) |
| jira | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| mercury | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| quickbooks | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| grafana | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| telegram | ✅ | ✅ | — (not in TWIN_SOURCES) | — (not in HMAC_SOURCES) | — (not in REPLAY_SOURCES) |
| brex | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| ramp | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| gusto | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| deel | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| fireflies | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| signal | ✅ | ✅ | — (not in TWIN_SOURCES) | — (not in HMAC_SOURCES) | — (not in REPLAY_SOURCES) |
| aws | ✅ | ✅ | — (not in TWIN_SOURCES) | — (not in HMAC_SOURCES) | — (not in REPLAY_SOURCES) |
| miro | ✅ | ✅ | — (not in TWIN_SOURCES) | — (not in HMAC_SOURCES) | — (not in REPLAY_SOURCES) |
| figma | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| carta | ✅ | ✅ | — (not in TWIN_SOURCES) | — (not in HMAC_SOURCES) | — (not in REPLAY_SOURCES) |
| hibob | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| ashby | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| linkedin | ✅ | ✅ | — (not in TWIN_SOURCES) | — (not in HMAC_SOURCES) | — (not in REPLAY_SOURCES) |
| whatsapp | — (history=None) | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |
| facebook_pages | ✅ | ✅ | — (not in TWIN_SOURCES) | ✅ | — (not in REPLAY_SOURCES) |

## Assertions

- ✅ `assert_all_complete`
- ✅ `assert_observation_count_matches_fixture`
- ✅ `assert_no_duplicate_observations`
- ✅ `assert_external_id_unique_across_paths`
- ✅ `assert_observations_have_exactly_one_t1_trigger`
- ✅ `assert_cross_path_twins_dedup`
- ✅ `assert_live_observations_attributed_correctly`
- ✅ `assert_signature_validation_gate_holds_for_hmac_sources`
- ✅ `assert_live_replay_idempotency_holds`
- ✅ `assert_per_tenant_timeline_monotonic`
- ✅ `assert_zero_partition_missing`
- ✅ `assert_all_contract_sources_have_live_targets` — resolved live targets for 27/27 canonical sources

## Subprocess exit codes (Decision 11)

- `oauth_poller`: rc=0
- `tenant_onboarding`: rc=0
- `source_onboarding`: rc=0
- `shard_fetch`: rc=0
- `reconciler`: rc=0
- `normalizer`: rc=0 — clean (ticket #45 resolved)
- `observation_writer`: rc=0 — clean (ticket #45 resolved)

## Notes

- Live ingestion is inline (no Kafka consumer needed); cross-path twins=['slack', 'github', 'gmail'], signature probes=['slack', 'github', 'notion', 'jira', 'mercury', 'quickbooks', 'grafana', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'figma', 'hibob', 'ashby', 'facebook_pages'], replay probes=['slack', 'github', 'gmail']. Consumer rc=-9/-15 expected per ticket #45.
- Contract live-only bootstrap covered ['whatsapp'] without fabricating a historical planner/fetcher result.

