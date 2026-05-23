# Validation Run 4 — Concurrent backfill + live-via-Kafka (50 tenants, 4 sources)

**Status:** READY ✅
**Started:** 2026-05-21T09:51:43.359421+00:00
**Wall time:** 41.6s
**Tenants:** 50

## Pre-flight (fixture realism — Decision 12)

- gmail: external_id='gmail:fbe0c08b-2db7-46a6-9e38-43' ✅
- github: external_id='I_kwDO8x2NYDDUMdgx' ✅
- slack: external_id='C_9C1302B2C2:1767225600.000000' ✅
- discord: external_id='discord:402097' ✅

## State reset (Decision 10)

- recreated ['ingestion.raw', 'ingestion.normalized', 'ingestion.embedding', 'ingestion.dlq']; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| gmail | 15 | 150 | 150 | ✅ |
| github | 15 | 165 | 165 | ✅ |
| slack | 10 | 100 | 100 | ✅ |
| discord | 10 | 100 | 100 | ✅ |

## Live phase (A30)

- concurrency=10; live=5 events/tenant via Kafka cutover
- peak simultaneous backfill in_progress: 50
- peak working signal backlog: 64
- live dispatch wall: 4.2s; per-source HTTP statuses: {'github': [202], 'slack': [202], 'gmail': [200]}

## Assertions

- ✅ `assert_per_tenant_isolation(backfill+live)` — all tenants match backfill+live expected
- ✅ `assert_concurrency_overlap(live during backfill in_progress)` — peak in_progress=50, live_start<=backfill_done (Δ=7.3s)
- ✅ `assert_live_routed_through_kafka(slack/github → 202)` — statuses={'github': [202], 'slack': [202], 'gmail': [200]}
- ✅ `assert_completion_fires_exactly_once_per_tenant(#39)` — all fired once
- ✅ `assert_no_duplicate_observations_under_concurrency` — 515 observations, zero duplicate (source_channel, external_id, occurred_at) groups
- ✅ `assert_no_signal_leak(working drains to 0)` — residual working signals=0 (terminal tenant_onboarding_completed excluded)
- ✅ `assert_dlq_empty(no partition_missing)` — 0 partition_missing DLQ envelopes

## Subprocess exit codes (Decision 11)

- `oauth_poller`: rc=0
- `tenant_onboarding`: rc=0
- `source_onboarding`: rc=0
- `shard_fetch`: rc=0
- `reconciler`: rc=0
- `normalizer`: rc=-9 — expected per ticket #45 (consumer graceful-shutdown)
- `observation_writer`: rc=-15 — expected per ticket #45 (consumer graceful-shutdown)

## Notes

- Live routed through Kafka (slack/github via webhook-router cutover → HTTP 202; discord via gateway cutover; gmail via push-handler cutover). Consumer rc=-9/-15 expected per ticket #45.

