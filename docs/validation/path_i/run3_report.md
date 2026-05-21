# Validation Run 3 — Concurrency stress (50 tenants, backfill-only)

**Status:** READY ✅
**Started:** 2026-05-21T07:48:07.555869+00:00
**Wall time:** 40.5s
**Tenants:** 50

## Pre-flight (fixture realism — Decision 12)

- gmail: external_id='gmail:6cec7f03-ee62-4889-8dec-bb' ✅
- github: external_id='I_kwDO8x2NYDDUMdgx' ✅
- slack: external_id='C_9C1302B2C2:1767225600.000000' ✅
- discord: external_id='discord:402097' ✅

## State reset (Decision 10)

- recreated ['ingestion.raw', 'ingestion.normalized', 'ingestion.embedding', 'ingestion.dlq']; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| gmail | 15 | 150 | 150 | ✅ |
| github | 15 | 300 | 300 | ✅ |
| slack | 10 | 240 | 240 | ✅ |
| discord | 10 | 100 | 100 | ✅ |

## Live phase (A30)

- backfill-only; concurrency=10
- peak simultaneous in_progress: 40
- peak working signal backlog (terminal excluded): 105
- completion-signal distribution: {1: 50}

## Assertions

- ✅ `assert_per_tenant_isolation`
- ✅ `assert_concurrency_exercised(>=5 in_progress)` — peak in_progress=40
- ✅ `assert_signal_backlog_bounded(<3×tenants=150)` — peak working backlog=105 (O(tenants), not O(concurrency) — see A30.6)
- ✅ `assert_no_signal_leak(working drains to 0)` — residual working signals=0 (terminal tenant_onboarding_completed excluded)
- ✅ `assert_completion_fires_exactly_once_per_tenant(#39)` — all 50 fired once

## Subprocess exit codes (Decision 11)

- `oauth_poller`: rc=0
- `tenant_onboarding`: rc=0
- `source_onboarding`: rc=0
- `shard_fetch`: rc=0
- `reconciler`: rc=0
- `normalizer`: rc=-9 — expected per ticket #45 (consumer graceful-shutdown)
- `observation_writer`: rc=-15 — expected per ticket #45 (consumer graceful-shutdown)

## Notes

- 50 tenants through 7 shared subprocesses (not 50× processes). Live phase skipped (Decision: Run 3 = backfill concurrency focus). Consumer rc=-9/-15 expected per ticket #45.

