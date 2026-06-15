# M-Load Cutover Dry Run

This runbook covers the staging-only synthetic webhook soak used before moving
Slack and GitHub webhook ingestion from inline processing to the Kafka-backed
cutover path.

The test lives at `tests/load/test_cutover_dryrun.py`. It is skipped by default
and only runs when a staging target, provider secrets, and the explicit pytest
flag are present.

## Preconditions

- Staging gateway is deployed with the candidate build.
- Staging Kafka, Postgres, object storage, and normalizer/writer workers are
  healthy.
- The test DSN and dashboard access point at staging, not production.
- The operator has Slack and GitHub webhook signing secrets for staging.
- Alerts for gateway errors, ingest latency, queue depth, dead letters, and
  worker liveness are visible.

## Command

Use the default one-hour run for release promotion:

```bash
CUTOVER_DRYRUN_TARGET_URL=https://staging-gateway.example.com \
CUTOVER_DRYRUN_SLACK_SECRET=... \
CUTOVER_DRYRUN_GITHUB_SECRET=... \
uv run pytest -q tests/load/test_cutover_dryrun.py --run-cutover-dryrun
```

Optional knobs:

```bash
CUTOVER_DRYRUN_QPS=100
CUTOVER_DRYRUN_DURATION_S=3600
CUTOVER_DRYRUN_TENANTS=500
```

For an emergency smoke after rollback, keep duration explicit in the release
record:

```bash
CUTOVER_DRYRUN_DURATION_S=300 \
uv run pytest -q tests/load/test_cutover_dryrun.py --run-cutover-dryrun
```

## Success Criteria

| Property | Gate |
| --- | --- |
| Sender throughput | `sent_total >= qps * duration_s * 0.90` |
| Sender error rate | `< 5 percent` |
| End-to-end p95 | `< 30 seconds` from webhook arrival to writer commit |
| Deduplication | zero duplicate observations after writer drain |
| Circuit breaker | lag breach is visible during injected pressure window |
| Dead letters | zero new dead-letter rows |

The pytest assertion covers sender throughput and sender error rate. The
operator must also verify the downstream database and dashboard checks below.

## During The Run

Watch:

- gateway request rate and 4xx/5xx rate
- Kafka producer delivery failures and flush latency
- writer consumer lag
- `pending_post_commit_actions` queue depth
- `model_reeval_queue` and `think_trigger_queue` pending depth
- dead-letter rows
- DB pool saturation

Stop the run if privacy alerts, cross-tenant read alerts, or sustained gateway
5xx rates appear.

## Downstream Checks

Run these against the staging database after workers drain.

Pending queues:

```sql
SELECT COUNT(*) AS pending_think_triggers
FROM think_trigger_queue
WHERE completed_at IS NULL;

SELECT COUNT(*) AS pending_post_commit
FROM pending_post_commit_actions
WHERE processed_at IS NULL AND dead_lettered_at IS NULL;

SELECT COUNT(*) AS dead_lettered_post_commit
FROM pending_post_commit_actions
WHERE dead_lettered_at IS NOT NULL;
```

Duplicate observations by source id:

```sql
SELECT tenant_id, source_channel, external_id, COUNT(*) AS duplicates
FROM observations
WHERE external_id IS NOT NULL
  AND ingested_at >= now() - interval '2 hours'
GROUP BY tenant_id, source_channel, external_id
HAVING COUNT(*) > 1
ORDER BY duplicates DESC
LIMIT 50;
```

Recent latency proxy:

```sql
SELECT
  percentile_cont(0.95) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (ingested_at - occurred_at))
  ) AS p95_seconds
FROM observations
WHERE ingested_at >= now() - interval '2 hours';
```

## Rollback

If the dry run fails:

1. Disable the cutover flag for the affected tenant cohort.
2. Keep inline ingestion enabled.
3. Stop the synthetic sender.
4. Snapshot queue depths and dead-letter rows.
5. Drain safe pending work or quarantine unsafe work after owner review.
6. Re-run the shadow/cutover unit suite and the synthetic load generator smoke.
7. Repeat the staging dry run before enabling cutover again.

Do not reuse a failed soak as rollout evidence. Attach the failed metrics and
the follow-up passing run to the release record.
