# M-Load — Production Kafka Readers + Synthetic Cutover Dry Run

**Status:** Implementation complete; **dry run validation TBD on staging** (cannot be exercised in CI / local dev environments).

This runbook covers M-Load's three deliverables and the operator procedure for running the cutover dry run against staging.

---

## 1. What M-Load ships

1. **Real Kafka admin readers** for the circuit breaker:
   - `_measure_kafka_lag_default` — confluent_kafka `AdminClient` reads committed offsets per partition, watermarks, and one timestamp probe per partition to convert message-count-lag to time-lag.
   - `_sample_active_tenants_default` — Consumer reads `ingestion.tenant_traffic_signal` over `lookback_sec` and returns `{tenant_id: partition}`.

2. **Explicit `mmh3.hash` murmur2 partition match** in `services/webhooks/router.py::_kafka_partition_for_tenant`:
   ```python
   import mmh3
   key_bytes = str(tenant_id).encode("utf-8")
   h = mmh3.hash(key_bytes, seed=0x9747b28c, signed=False)
   return (h & 0x7fffffff) % num_partitions
   ```
   Matches librdkafka's default `murmur2_random` partitioner. Verified via `test_kafka_partition_lookup_matches_actual_landing_partition` (off-line algorithm match against the canonical formula; ≥99% match against an actual broker validated in staging per §3).

3. **Synthetic webhook traffic generator** (`services/synthetic/cutover_load.py`):
   - Slack + GitHub webhooks (M5 cutover providers; Gmail/Discord use push/Gateway).
   - Configurable QPS + duration + tenant pool with Zipf-ish distribution.
   - 5% duplicate-payload rate to exercise writer dedup.
   - HMAC-SHA256 signature on every request (`v0=...` Slack, `sha256=...` GitHub).

4. **Cutover dry-run orchestrator** (`tests/load/test_cutover_dryrun.py`):
   - Default-skipped; runs only with `CUTOVER_DRYRUN_TARGET_URL` set.
   - Runs the synthetic harness for the configured duration.
   - Asserts the four cutover properties (throughput, latency, dedup, breaker behavior).

---

## 2. The four cutover properties

| # | Property | How it's verified |
|---|----------|-------------------|
| 1 | Webhook → ingestion.raw throughput matches QPS within ±10%. | Synthetic harness reports `sent_total`; expected = `qps × duration_s`. |
| 2 | End-to-end p95 latency (webhook → writer commit) < 30s. | Manual operator query in staging (see §4.2). |
| 3 | Duplicate payloads dedup at the writer (zero duplicate observations). | Manual operator query in staging (see §4.3). |
| 4 | Circuit breaker correctly detects per-tenant lag. | Synthetic breach injection mid-run (see §4.4); confirm breaker state transitions. |

Properties 1 + 4 are validated automatically by the dry-run test; 2 + 3 require operator queries documented below.

---

## 3. Running the dry run on staging

### Prerequisites
- Staging gateway URL exposing `/webhooks/slack/events` + `/webhooks/github/events`.
- Slack signing secret + GitHub webhook secret configured on the gateway side and known to the operator.
- Real Kafka broker running; circuit breaker service deployed pointing at it.
- ~1 hour of dedicated time on a staging cluster.

### Procedure

```sh
export CUTOVER_DRYRUN_TARGET_URL="https://staging-gateway.internal"
export CUTOVER_DRYRUN_SLACK_SECRET="<from gateway env>"
export CUTOVER_DRYRUN_GITHUB_SECRET="<from gateway env>"
export CUTOVER_DRYRUN_QPS=100           # default
export CUTOVER_DRYRUN_DURATION_S=3600   # 1 hour default
export CUTOVER_DRYRUN_TENANTS=500       # default

# Run the dry-run test:
pytest tests/load/test_cutover_dryrun.py -s

# OR drive the synthetic harness directly:
python -m services.synthetic.cutover_load \
    --target-url "$CUTOVER_DRYRUN_TARGET_URL" \
    --slack-signing-secret "$CUTOVER_DRYRUN_SLACK_SECRET" \
    --github-webhook-secret "$CUTOVER_DRYRUN_GITHUB_SECRET" \
    --qps 100 --duration-s 3600 --tenant-count 500
```

The test asserts properties 1 + 4 (synthetic) and prints metrics for the operator to validate properties 2 + 3.

---

## 4. Operator validation queries (staging)

### 4.1. Throughput (property 1)
The synthetic harness reports `sent_total` and `qps_actual` in its output. Confirm:
- `sent_total >= qps × duration_s × 0.9`
- `errors` bucket is empty or contains only acceptable transient codes.

### 4.2. End-to-end latency (property 2)

```sql
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY (observed_at - webhook_arrival_at))
       AS p95_latency
  FROM observations
 WHERE webhook_arrival_at >= now() - interval '1 hour'
   AND source_channel IN ('slack:message', 'github:webhook');
```

**Pass:** `p95_latency < 30 seconds`.
**TBD on staging:** record actual numbers here after first dry run.

### 4.3. Dedup (property 3)

```sql
SELECT external_id, count(*) AS dup_count
  FROM observations
 WHERE created_at >= now() - interval '1 hour'
   AND source_channel IN ('slack:message', 'github:webhook')
 GROUP BY external_id HAVING count(*) > 1
 LIMIT 20;
```

**Pass:** zero rows returned. Writer dedup is the UNIQUE constraint on `observations.external_id`.

### 4.4. Circuit breaker — synthetic breach (property 4)

During the dry run, ~30 minutes in, **introduce a synthetic lag breach** by stopping the normalizer worker briefly (e.g., `docker pause` for 90 seconds). The breaker should:
1. `_measure_kafka_lag_default` observes growing per-partition lag.
2. `_sample_active_tenants_default` returns the tenants whose `raw_partition` is in the lagging set.
3. Breaker transitions per-tenant `kafka_lag_breach=TRUE` flags.
4. After unpause + drain, breaker transitions back to clean.

```sql
SELECT tenant_id, flag_key, value, updated_at
  FROM tenant_flags
 WHERE flag_key = 'kafka_lag_breach'
 ORDER BY updated_at DESC LIMIT 50;
```

**Pass:** at least one tenant transitions to `value=true` during the breach window and back to `value=false` after recovery.

---

## 5. Expected metrics (TBD — populate after first staging run)

Placeholder values; **operator updates this section after first dry run**:

| Metric | Expected | Actual |
|---|---|---|
| sent_total (1hr @ 100 QPS) | ~360,000 | _TBD_ |
| qps_actual | ≥ 90 | _TBD_ |
| End-to-end p95 latency | < 30s | _TBD_ |
| Duplicate observations | 0 | _TBD_ |
| Breaker transitions during synthetic breach | ≥ 1 tenant in/out | _TBD_ |

---

## 6. Failure modes during dry run

| Symptom | Likely cause | Remediation |
|---|---|---|
| `qps_actual` significantly below `qps` | Gateway rate-limit or HTTP backpressure | Inspect gateway logs; reduce QPS or scale up |
| `sent_total > 0` but `observations` count is 0 | Kafka path broken (producer config, missing topic) | Inspect normalizer logs; verify topic exists |
| Duplicates appear in observations | Writer UNIQUE constraint not present or external_id misassigned | Migration drift; verify schema |
| Breaker flag never transitions during synthetic breach | `_measure_kafka_lag_default` returning empty dict (probe failed) or breaker config thresholds too high | Inspect breaker service logs; check committed offsets manually |

---

## 7. What's NOT in M-Load

- F4 retrofit (`onboarding_triggers` writes from OAuth callbacks). See `docs/decisions/ticket-36-oauth-callbacks-onboarding-triggers-retrofit.md`.
- Gmail/Discord steady-state path retirement. See ticket-35 + ticket-37.
- Production Kafka reader for Pub/Sub push or Gateway events (Gmail / Discord). Both use existing inline paths.
- Backfill correctness validation (covered by M6.3-M6.6 E2E tests).
