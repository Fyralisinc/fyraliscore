# Validation Run 1 — E2E backfill + live (all sources)

**Status:** PASS ✅
**Started:** 2026-05-21T10:08:45.079269+00:00
**Wall time:** 27.4s
**Tenants:** 16

## Pre-flight (fixture realism — Decision 12)

- gmail: 3 records, external_id='gmail:6885ec50-e382-4cf0-9b64-a8', occurred_at=2026-01-01T00:02:00+00:00 ✅
- github: 2 records, external_id='I_kwDO8x2NYDDUMdgx', occurred_at=2026-01-01T00:21:00+00:00 ✅
- slack: 3 records, external_id='C_9C1302B2C2:1767225600.000000', occurred_at=2026-01-01T00:00:00+00:00 ✅
- discord: 3 records, external_id='discord:402097', occurred_at=2026-01-01T00:00:00+00:00 ✅

## State reset (Decision 10)

- recreated ['ingestion.raw', 'ingestion.normalized', 'ingestion.embedding', 'ingestion.dlq']; cleared 0 stale S3 objects

## Per-source observation counts

| Source | Tenants | Expected | Actual | Result |
|---|---|---|---|---|
| gmail | 4 | 41 | 41 | ✅ |
| github | 4 | 45 | 45 | ✅ |
| slack | 4 | 41 | 41 | ✅ |
| discord | 4 | 40 | 40 | ✅ |

## Live phase (A30)

- live events/tenant: 5; per-source live deltas: {'gmail': 20, 'slack': 20, 'discord': 20, 'github': 20}
- cross-path twins dispatched (gmail/github/slack): ['github', 'gmail', 'slack']
- signature-gate probes (HMAC): [('slack', 401), ('github', 401)]
- replay probe (dispatched_unique→observed): {'gmail': 1, 'slack': 1, 'github': 1}
- live drain stable: True

## Per-source × per-dimension coverage

| Source | Backfill | Live | Cross-path dedup | Signature gate | Replay idempotency |
|---|---|---|---|---|---|
| gmail | ✅ | ✅ | ✅ | — (OIDC no-op) | ✅ |
| github | ✅ | ✅ | ✅ | ✅ | ✅ |
| slack | ✅ | ✅ | ✅ | ✅ | ✅ |
| discord | ✅ | ✅ | — (namespace, A30.3) | — (direct dispatch) | — (no replay, A24) |

## Assertions

- ✅ `assert_all_complete`
- ✅ `assert_observation_count_matches_fixture`
- ✅ `assert_no_duplicate_observations`
- ✅ `assert_external_id_unique_across_paths`
- ✅ `assert_cross_path_twins_dedup`
- ✅ `assert_live_observations_attributed_correctly`
- ✅ `assert_signature_validation_gate_holds_for_hmac_sources`
- ✅ `assert_live_replay_idempotency_holds`
- ✅ `assert_per_tenant_timeline_monotonic`
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

- Live ingestion is inline (no Kafka consumer needed); cross-path twins exercised for gmail/github/slack; Discord excluded by namespace topology (A30.3). Consumer rc=-9/-15 expected per ticket #45.

