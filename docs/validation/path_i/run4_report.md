# Validation Run 4 — Concurrent backfill + live-via-Kafka (50 tenants, 4 sources)

**Status:** NOT_READY ❌
**Started:** 2026-06-07T11:15:21.068359+00:00
**Wall time:** 607.3s
**Tenants:** 4

## Pre-flight (fixture realism — Decision 12)

- gmail: external_id='gmail:89d99f3e-57b8-4538-93bb-e4' ✅
- github: external_id='I_kwDO8x2NYDDUMdgx' ✅
- slack: external_id='C_9C1302B2C2:1767225600.000000' ✅
- discord: external_id='discord:402097' ✅

## State reset (Decision 10)

- recreated ['ingestion.raw.slack', 'ingestion.raw.github', 'ingestion.raw.discord', 'ingestion.raw.gmail', 'ingestion.raw.notion', 'ingestion.raw.google_calendar', 'ingestion.raw.google_drive', 'ingestion.raw.jira', 'ingestion.raw.mercury', 'ingestion.raw.quickbooks', 'ingestion.raw.grafana', 'ingestion.normalized.slack', 'ingestion.normalized.github', 'ingestion.normalized.discord', 'ingestion.normalized.gmail', 'ingestion.normalized.notion', 'ingestion.normalized.google_calendar', 'ingestion.normalized.google_drive', 'ingestion.normalized.jira', 'ingestion.normalized.mercury', 'ingestion.normalized.quickbooks', 'ingestion.normalized.grafana', 'ingestion.embedding.slack', 'ingestion.embedding.github', 'ingestion.embedding.discord', 'ingestion.embedding.gmail', 'ingestion.embedding.notion', 'ingestion.embedding.google_calendar', 'ingestion.embedding.google_drive', 'ingestion.embedding.jira', 'ingestion.embedding.mercury', 'ingestion.embedding.quickbooks', 'ingestion.embedding.grafana', 'ingestion.dlq.slack', 'ingestion.dlq.github', 'ingestion.dlq.discord', 'ingestion.dlq.gmail', 'ingestion.dlq.notion', 'ingestion.dlq.google_calendar', 'ingestion.dlq.google_drive', 'ingestion.dlq.jira', 'ingestion.dlq.mercury', 'ingestion.dlq.quickbooks', 'ingestion.dlq.grafana']; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| gmail | 1 | 10 | 10 | ✅ |
| github | 1 | 11 | 11 | ✅ |
| slack | 1 | 10 | 10 | ✅ |
| discord | 1 | 10 | 10 | ✅ |

## Live phase (A30)

- concurrency=10; live=5 events/tenant via Kafka cutover
- peak simultaneous backfill in_progress: 4
- peak working signal backlog: 6
- live dispatch wall: 0.6s; per-source HTTP statuses: {'slack': [202], 'github': [202], 'gmail': [200]}

## Assertions

- ✅ `assert_per_tenant_isolation(backfill+live)` — all tenants match backfill+live expected
- ❌ `assert_concurrency_overlap(live during backfill in_progress)` — peak in_progress=4, live_start<=backfill_done (Δ=600.1s)
- ✅ `assert_live_routed_through_kafka(slack/github → 202)` — statuses={'slack': [202], 'github': [202], 'gmail': [200]}
- ❌ `assert_completion_fires_exactly_once_per_tenant(#39)` — anomalies: ['r4-gmail-0', 'r4-github-0', 'r4-slack-0', 'r4-discord-0']
- ✅ `assert_no_duplicate_observations_under_concurrency` — 41 observations, zero duplicate (source_channel, external_id, occurred_at) groups
- ✅ `assert_no_signal_leak(working drains to 0)` — residual working signals=0 (terminal tenant_onboarding_completed excluded)
- ✅ `assert_dlq_empty(no partition_missing)` — 0 partition_missing DLQ envelopes

## Subprocess exit codes (Decision 11)

- `oauth_poller`: rc=0
- `tenant_onboarding`: rc=0
- `source_onboarding`: rc=0
- `shard_fetch`: rc=1 — **UNEXPECTED (real failure)**
- `reconciler`: rc=0
- `normalizer`: rc=0 — clean (ticket #45 resolved)
- `observation_writer`: rc=0 — clean (ticket #45 resolved)

## Notes

- Live routed through Kafka (slack/github via webhook-router cutover → HTTP 202; discord via gateway cutover; gmail via push-handler cutover). Consumer rc=-9/-15 expected per ticket #45.

