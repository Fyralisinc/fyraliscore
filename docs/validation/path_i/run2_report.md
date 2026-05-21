# Validation Run 2 — Fault injection (FLAKY + partition-missing)

**Status:** PARTIAL ⚠️
**Started:** 2026-05-21T07:56:53.134942+00:00
**Wall time:** 234.3s
**Tenants:** 16

## Pre-flight (fixture realism — Decision 12)

- gmail: 3 records, external_id='gmail:cffd504a-8a40-4010-bcd2-1a' ✅
- github: 2 records, external_id='I_kwDO8x2NYDDUMdgx' ✅
- slack: 3 records, external_id='C_9C1302B2C2:1767225600.000000' ✅
- discord: 3 records, external_id='discord:402097' ✅

## State reset (Decision 10)

- recreated ['ingestion.raw', 'ingestion.normalized', 'ingestion.embedding', 'ingestion.dlq']; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| gmail | 4 | 40 | 36 | ❌ |
| github | 4 | 44 | 44 | ✅ |
| slack | 4 | 40 | 40 | ✅ |
| discord | 4 | 40 | 20 | ❌ |

## Live phase (A30)

- FLAKY (10% 5xx) applied to all backfill mocks
- partition-missing injections (one/source): 4
- live per-source deltas: {'gmail': 20, 'slack': 20, 'discord': 20, 'github': 20}

## Assertions

- ✅ `assert_partition_missing_routes_to_dlq`
- ✅ `assert_cross_path_twins_dedup`
- ✅ `assert_signature_validation_gate_holds_for_hmac_sources`
- ✅ `assert_no_duplicate_observations`

## Subprocess exit codes (Decision 11)

- `oauth_poller`: rc=0
- `tenant_onboarding`: rc=0
- `source_onboarding`: rc=0
- `shard_fetch`: rc=0
- `reconciler`: rc=0
- `normalizer`: rc=-9 — expected per ticket #45 (consumer graceful-shutdown)
- `observation_writer`: rc=-15 — expected per ticket #45 (consumer graceful-shutdown)

## Notes

- FLAKY fault profile; partial backfill counts are expected (verdict PARTIAL). A19: orchestrator subprocesses must not crash; A28: partition-missing must route to DLQ. Consumer rc=-9/-15 expected per ticket #45.

