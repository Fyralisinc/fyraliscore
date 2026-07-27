# Validation Run 3 — Contract-wide multi-install/multi-replica stress (52 tenants, 104 installations, backfill-only)

**Status:** READY ✅
**Started:** 2026-07-27T06:13:38.269432+00:00
**Wall time:** 79.9s
**Tenants:** 52

## Pre-flight (fixture realism — Decision 12)

- slack: external_id='C_9C1302B2C2:1767225600.000000' ✅
- github: external_id='I_kwDO8x2NYDDUMdgx:closed' ✅
- discord: external_id='discord:402097' ✅
- gmail: external_id='gmail:f0855072-02b3-42a2-a2d2-89' ✅
- brex: external_id='brex:acct_2b27365a9a8cca66:txn:t' ✅
- ramp: external_id='ramp:r-pre:txn:d3a50fe2-d8dc-0ae' ✅
- gusto: external_id='gusto:c-pre:employee:8564f649-77' ✅
- deel: external_id='deel:con_63349d041ea22e18:paymen' ✅
- fireflies: external_id='fireflies:ws-pre:transcript:ts_d' ✅
- signal: external_id='signal:81766b38-6d5a-444d-836f-f' ✅
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
| slack | 2 | 600 | 600 | ✅ |
| github | 2 | 800 | 800 | ✅ |
| discord | 2 | 480 | 480 | ✅ |
| gmail | 2 | 40 | 40 | ✅ |
| notion | 2 | 12 | 12 | ✅ |
| google_calendar | 2 | 24 | 24 | ✅ |
| google_drive | 2 | 12 | 12 | ✅ |
| jira | 2 | 12 | 12 | ✅ |
| mercury | 2 | 20 | 20 | ✅ |
| quickbooks | 2 | 16 | 16 | ✅ |
| grafana | 2 | 20 | 20 | ✅ |
| telegram | 2 | 20 | 20 | ✅ |
| brex | 2 | 20 | 20 | ✅ |
| ramp | 2 | 16 | 16 | ✅ |
| gusto | 2 | 8 | 8 | ✅ |
| deel | 2 | 20 | 20 | ✅ |
| fireflies | 2 | 16 | 16 | ✅ |
| signal | 2 | 20 | 20 | ✅ |
| aws | 2 | 12 | 12 | ✅ |
| miro | 2 | 16 | 16 | ✅ |
| figma | 2 | 20 | 20 | ✅ |
| carta | 2 | 16 | 16 | ✅ |
| hibob | 2 | 16 | 16 | ✅ |
| ashby | 2 | 96 | 96 | ✅ |
| linkedin | 2 | 12 | 12 | ✅ |
| facebook_pages | 2 | 24 | 24 | ✅ |

## Live phase (A30)

- backfill-only; concurrency=10; replicas=2
- replica OAuth claims: {'x3-poll-c7d7a9d0-replica-1': 51, 'x3-poll-c7d7a9d0-replica-2': 53}
- peak simultaneous in_progress: 87
- peak working signal backlog (terminal excluded): 337
- completion-signal distribution: {1: 104}

## Assertions

- ✅ `assert_contract_scenario_outcome_coverage` — planned=104, outcomes=104, missing=[], unexpected=[]
- ✅ `assert_per_tenant_isolation`
- ✅ `assert_same_tenant_sibling_installation_identity` — 52 tenants each retained 2 exact install identities
- ✅ `assert_two_replicas_share_onboarding_claims` — configured=2, observed=2, participating=2, oauth_claims={'x3-poll-c7d7a9d0-replica-1': 51, 'x3-poll-c7d7a9d0-replica-2': 53}
- ✅ `assert_concurrency_exercised(>=5 in_progress)` — peak in_progress=87
- ✅ `assert_signal_backlog_bounded(<=planned_shards+installations=488)` — peak working backlog=337; planned_shards=384, installations=104
- ✅ `assert_no_signal_leak(working drains to 0)` — residual working signals=0 (terminal tenant_onboarding_completed excluded)
- ✅ `assert_completion_fires_exactly_once_per_installation(#39)` — all 104 fired once

## Subprocess exit codes (Decision 11)

- `oauth_poller@1`: rc=0
- `tenant_onboarding@1`: rc=0
- `source_onboarding@1`: rc=0
- `shard_fetch@1`: rc=0
- `reconciler@1`: rc=0
- `normalizer@1`: rc=0 — clean (ticket #45 resolved)
- `observation_writer@1`: rc=0 — clean (ticket #45 resolved)
- `oauth_poller@2`: rc=0
- `tenant_onboarding@2`: rc=0
- `source_onboarding@2`: rc=0
- `shard_fetch@2`: rc=0
- `reconciler@2`: rc=0
- `normalizer@2`: rc=0 — clean (ticket #45 resolved)
- `observation_writer@2`: rc=0 — clean (ticket #45 resolved)

## Notes

- 52 tenants / 104 installations across 26 contract-declared historical sources through 2 shared seven-service replicas (not one process set per tenant). Live phase skipped (Decision: Run 3 = backfill concurrency focus). Consumer rc=-9/-15 expected per ticket #45.
