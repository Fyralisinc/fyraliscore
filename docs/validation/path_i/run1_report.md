# Validation Run 1 — E2E backfill (all sources)

**Status:** PASS ✅
**Started:** 2026-05-20T16:33:24.964270+00:00
**Wall time:** 28.3s
**Tenants:** 16

## Pre-flight (fixture realism — Decision 12)

- gmail: 3 records, external_id='gmail:9df258ba-47e8-40ab-a74a-e0', occurred_at=2026-01-01T00:02:00+00:00 ✅
- github: 2 records, external_id='I_kwDO8x2NYDDUMdgx', occurred_at=2026-01-01T00:21:00+00:00 ✅
- slack: 3 records, external_id='C_9C1302B2C2:1767225600.000000', occurred_at=2026-01-01T00:00:00+00:00 ✅
- discord: 3 records, external_id='discord:402097', occurred_at=2026-01-01T00:00:00+00:00 ✅

## State reset (Decision 10)

- recreated ['ingestion.raw', 'ingestion.normalized', 'ingestion.embedding', 'ingestion.dlq']; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| gmail | 4 | 20 | 20 | ✅ |
| github | 4 | 24 | 24 | ✅ |
| slack | 4 | 20 | 20 | ✅ |
| discord | 4 | 20 | 20 | ✅ |

## Assertions

- ✅ `assert_all_complete`
- ✅ `assert_observation_count_matches_fixture`
- ✅ `assert_no_duplicate_observations`
- ✅ `assert_external_id_unique_across_paths`
- ✅ `assert_zero_partition_missing`

## Subprocess exit codes (Decision 11)

- `oauth_poller`: rc=0
- `tenant_onboarding`: rc=0
- `source_onboarding`: rc=0
- `shard_fetch`: rc=0
- `reconciler`: rc=0
- `normalizer`: rc=-9 — expected per ticket #45 (consumer graceful-shutdown)
- `observation_writer`: rc=-15 — expected per ticket #45 (consumer graceful-shutdown)

## Notes

- Live phase + Runs 2/3 deferred to M-Validate-Live (ticket #47). Consumer rc=-9/-15 expected per ticket #45.

