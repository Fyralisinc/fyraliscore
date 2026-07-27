# Validation Run 4 — Concurrent backfill (production clients → Provider Lab) + live-via-Kafka (54 tenants, 27 canonical sources)

**Status:** READY ✅
**Started:** 2026-07-27T05:30:32.319112+00:00
**Wall time:** 68.7s
**Tenants:** 54

## Pre-flight (fixture realism — Decision 12)

- slack: external_id='C_9C1302B2C2:1767225600.000000' ✅
- github: external_id='I_kwDO8x2NYDDUMdgx:closed' ✅
- discord: external_id='discord:402097' ✅
- gmail: external_id='gmail:c53a3824-156d-4329-8b23-59' ✅
- brex: external_id='brex:acct_2b27365a9a8cca66:txn:t' ✅
- ramp: external_id='ramp:r-pre:txn:d3a50fe2-d8dc-0ae' ✅
- gusto: external_id='gusto:c-pre:employee:8564f649-77' ✅
- deel: external_id='deel:con_63349d041ea22e18:paymen' ✅
- fireflies: external_id='fireflies:ws-pre:transcript:ts_d' ✅
- signal: external_id='signal:48cfd22a-a0af-415b-b06f-c' ✅
- aws: external_id='aws:900000000001:us-east-1:event' ✅
- miro: external_id='miro:org-pre:item:item_60af6af84' ✅
- figma: external_id='figma:team-pre:event:evt_83d65a9' ✅
- carta: external_id='carta:firm-pre:stakeholder:1000:' ✅
- hibob: external_id='hibob:hibob-co-pre:employee:1000' ✅

## State reset (Decision 10)

- recreated ['ingestion.raw.slack', 'ingestion.raw.github', 'ingestion.raw.discord', 'ingestion.raw.gmail', 'ingestion.raw.notion', 'ingestion.raw.google_calendar', 'ingestion.raw.google_drive', 'ingestion.raw.jira', 'ingestion.raw.mercury', 'ingestion.raw.quickbooks', 'ingestion.raw.grafana', 'ingestion.raw.telegram', 'ingestion.raw.brex', 'ingestion.raw.ramp', 'ingestion.raw.gusto', 'ingestion.raw.deel', 'ingestion.raw.fireflies', 'ingestion.raw.signal', 'ingestion.raw.aws', 'ingestion.raw.miro', 'ingestion.raw.figma', 'ingestion.raw.carta', 'ingestion.raw.hibob', 'ingestion.raw.ashby', 'ingestion.raw.linkedin', 'ingestion.raw.whatsapp', 'ingestion.raw.facebook_pages', 'ingestion.normalized.slack', 'ingestion.normalized.github', 'ingestion.normalized.discord', 'ingestion.normalized.gmail', 'ingestion.normalized.notion', 'ingestion.normalized.google_calendar', 'ingestion.normalized.google_drive', 'ingestion.normalized.jira', 'ingestion.normalized.mercury', 'ingestion.normalized.quickbooks', 'ingestion.normalized.grafana', 'ingestion.normalized.telegram', 'ingestion.normalized.brex', 'ingestion.normalized.ramp', 'ingestion.normalized.gusto', 'ingestion.normalized.deel', 'ingestion.normalized.fireflies', 'ingestion.normalized.signal', 'ingestion.normalized.aws', 'ingestion.normalized.miro', 'ingestion.normalized.figma', 'ingestion.normalized.carta', 'ingestion.normalized.hibob', 'ingestion.normalized.ashby', 'ingestion.normalized.linkedin', 'ingestion.normalized.whatsapp', 'ingestion.normalized.facebook_pages', 'ingestion.embedding.slack', 'ingestion.embedding.github', 'ingestion.embedding.discord', 'ingestion.embedding.gmail', 'ingestion.embedding.notion', 'ingestion.embedding.google_calendar', 'ingestion.embedding.google_drive', 'ingestion.embedding.jira', 'ingestion.embedding.mercury', 'ingestion.embedding.quickbooks', 'ingestion.embedding.grafana', 'ingestion.embedding.telegram', 'ingestion.embedding.brex', 'ingestion.embedding.ramp', 'ingestion.embedding.gusto', 'ingestion.embedding.deel', 'ingestion.embedding.fireflies', 'ingestion.embedding.signal', 'ingestion.embedding.aws', 'ingestion.embedding.miro', 'ingestion.embedding.figma', 'ingestion.embedding.carta', 'ingestion.embedding.hibob', 'ingestion.embedding.ashby', 'ingestion.embedding.linkedin', 'ingestion.embedding.whatsapp', 'ingestion.embedding.facebook_pages', 'ingestion.summarization.slack', 'ingestion.summarization.github', 'ingestion.summarization.discord', 'ingestion.summarization.gmail', 'ingestion.summarization.notion', 'ingestion.summarization.google_calendar', 'ingestion.summarization.google_drive', 'ingestion.summarization.jira', 'ingestion.summarization.mercury', 'ingestion.summarization.quickbooks', 'ingestion.summarization.grafana', 'ingestion.summarization.telegram', 'ingestion.summarization.brex', 'ingestion.summarization.ramp', 'ingestion.summarization.gusto', 'ingestion.summarization.deel', 'ingestion.summarization.fireflies', 'ingestion.summarization.signal', 'ingestion.summarization.aws', 'ingestion.summarization.miro', 'ingestion.summarization.figma', 'ingestion.summarization.carta', 'ingestion.summarization.hibob', 'ingestion.summarization.ashby', 'ingestion.summarization.linkedin', 'ingestion.summarization.whatsapp', 'ingestion.summarization.facebook_pages', 'ingestion.dlq.slack', 'ingestion.dlq.github', 'ingestion.dlq.discord', 'ingestion.dlq.gmail', 'ingestion.dlq.notion', 'ingestion.dlq.google_calendar', 'ingestion.dlq.google_drive', 'ingestion.dlq.jira', 'ingestion.dlq.mercury', 'ingestion.dlq.quickbooks', 'ingestion.dlq.grafana', 'ingestion.dlq.telegram', 'ingestion.dlq.brex', 'ingestion.dlq.ramp', 'ingestion.dlq.gusto', 'ingestion.dlq.deel', 'ingestion.dlq.fireflies', 'ingestion.dlq.signal', 'ingestion.dlq.aws', 'ingestion.dlq.miro', 'ingestion.dlq.figma', 'ingestion.dlq.carta', 'ingestion.dlq.hibob', 'ingestion.dlq.ashby', 'ingestion.dlq.linkedin', 'ingestion.dlq.whatsapp', 'ingestion.dlq.facebook_pages', 'ingestion.tenant_traffic_signal', 'onboarding.progress']; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| slack | 2 | 310 | 310 | ✅ |
| github | 2 | 410 | 410 | ✅ |
| discord | 2 | 250 | 250 | ✅ |
| gmail | 2 | 30 | 30 | ✅ |
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
| whatsapp | 2 | 10 | 10 | ✅ |
| facebook_pages | 2 | 22 | 22 | ✅ |

## Live phase (A30)

- concurrency=10; live=5 events/tenant via Kafka cutover
- peak simultaneous backfill in_progress: 52
- peak working signal backlog: 191
- per-source dispatched live events: {'discord': 10, 'telegram': 10, 'carta': 10, 'miro': 10, 'linkedin': 10, 'notion': 10, 'signal': 10, 'google_drive': 10, 'google_calendar': 10, 'slack': 10, 'jira': 10, 'github': 10, 'grafana': 10, 'aws': 10, 'fireflies': 10, 'deel': 10, 'mercury': 10, 'quickbooks': 10, 'gusto': 10, 'ramp': 10, 'brex': 10, 'whatsapp': 10, 'figma': 10, 'ashby': 10, 'hibob': 10, 'facebook_pages': 10, 'gmail': 10}
- live dispatch wall: 7.3s; per-source HTTP statuses: {'notion': [200], 'slack': [202], 'github': [202], 'jira': [202], 'google_calendar': [200], 'google_drive': [200], 'mercury': [202], 'deel': [202], 'grafana': [202], 'fireflies': [202], 'quickbooks': [202], 'brex': [202], 'gusto': [202], 'ramp': [202], 'whatsapp': [202], 'gmail': [200], 'figma': [202], 'ashby': [202], 'hibob': [202], 'facebook_pages': [202]}

## Assertions

- ✅ `assert_contract_scenario_outcome_coverage` — planned=52, outcomes=52, missing=[], unexpected=[]
- ✅ `assert_per_tenant_isolation(backfill+live)` — all historical and live-only tenants match exact totals
- ✅ `assert_concurrency_overlap(live during backfill in_progress)` — peak in_progress=52, live_start<=backfill_done (Δ=25.9s)
- ✅ `assert_completion_fires_exactly_once_per_historical_tenant(#39)` — all historical tenants fired once; live-only targets correctly excluded
- ✅ `assert_all_contract_sources_dispatched_live` — targets=27/27, dispatched=27/27
- ✅ `assert_http_ack_statuses_follow_ingress_contract` — missing=[]; mismatches={}; Kafka-cutover routes require HTTP 202
- ✅ `assert_no_duplicate_observations_under_concurrency` — 1454 observations, zero duplicate (source_channel, external_id, occurred_at) groups
- ✅ `assert_observation_persistence_and_t1_trigger` — 1454 observations each own exactly one same-tenant T1/event_arrival trigger
- ✅ `assert_no_signal_leak(working drains to 0)` — residual working signals=0 (terminal tenant_onboarding_completed excluded)
- ✅ `assert_dlq_empty(no partition_missing)` — 0 partition_missing DLQ envelopes

## Subprocess exit codes (Decision 11)

- `oauth_poller`: rc=0
- `tenant_onboarding`: rc=0
- `source_onboarding`: rc=0
- `shard_fetch`: rc=0
- `reconciler`: rc=0
- `normalizer`: rc=0 — clean (ticket #45 resolved)
- `observation_writer`: rc=0 — clean (ticket #45 resolved)

## Notes

- Live dispatch covered the complete contract catalog. Routes whose provider-ingress contract declares the flagged Kafka cutover must acknowledge with HTTP 202; provider-managed push acknowledgements and direct gateway/poll transports retain their declared boundary. Consumer rc=-9/-15 remains accepted per ticket #45.
- Backfill drove production clients for 26 history-capable contract sources against Provider Lab. Live-only sources ['whatsapp'] contributed live observations without fabricated onboarding-completion or historical assertions.

