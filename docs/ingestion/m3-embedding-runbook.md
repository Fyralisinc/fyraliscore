# M3 Embedding Pipeline — Production Operator Runbook

**Scope.** This runbook covers the M3 deliverables: the DLQ writer
(`services.ingestion.writers.dlq_writer`), the live embedding worker
(`services.ingestion.writers.embedding_worker`), and the backlog
embedding service
(`services.ingestion.recovery.embedding_backlog`). For the M2 shadow
path (raw tier, normalizer, no-op writer) see
[m2-shadow-path-runbook.md](m2-shadow-path-runbook.md).

**Audience.** On-call operator with `psql` + `kubectl` access. Assumes
familiarity with the M2 runbook.

**As of:** 2026-05-17 (`feat/ingestion-m3-embedding-worker` branch,
pre-merge).

---

## 0. Quick-reference

| Service | Source topic | Sink | Pause control |
|---|---|---|---|
| DLQ writer | `ingestion.dlq` | `ingestion_failures` (UPSERT) | scale to 0 replicas |
| Live embedding worker | `ingestion.embedding` | `observations.embedding` (UPDATE) | scale to 0 replicas |
| Backlog drainer | `observations WHERE embedding_pending=TRUE` (cursor) | `observations.embedding` (UPDATE) | `BACKFILL_OLLAMA_QPS=0` (no restart needed) |

**Failure surfaces (read order in an incident):**

1. `ingestion_failures` (queryable DLQ mirror — `SELECT * FROM ingestion_failures WHERE resolved_at IS NULL ORDER BY last_seen_at DESC`)
2. In-process metrics endpoints (named under each service below)
3. Application logs (structured JSON; key fields named per-service)

---

## 1. DLQ writer (`services.ingestion.writers.dlq_writer`)

### 1.1 What it does

Consumes `ingestion.dlq`. Each envelope becomes a row in
`ingestion_failures` via `INSERT ... ON CONFLICT (tenant_id, source,
raw_s3_key, failure_kind) DO UPDATE SET attempt_count = attempt_count +
1, last_seen_at = EXCLUDED.last_seen_at, error_summary = EXCLUDED.
error_summary` against the UNIQUE index added in migration 0051.
Concurrent producers of the same logical failure serialise at the DB.

Path A. First production user of `asyncpg.create_pool(...,
statement_cache_size=0)` — the pgbouncer-compatibility flag from the
M1.3 ADR.

### 1.2 Env vars

```
KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
DATABASE_URL              required
POSTGRES_POOL_SIZE        default: 5
DLQ_WRITER_LOG_LEVEL      default: INFO
```

### 1.3 Metrics

```
dlq_writer.messages_consumed
dlq_writer.upserts
dlq_writer.parse_failure
dlq_writer.db_error
dlq_writer.consumer_lag_seconds
```

### 1.4 Common incidents

**(a) `dlq_writer.parse_failure` rising.** A producer is publishing
malformed envelopes to `ingestion.dlq`. Investigate via the topic dump:

```bash
kafka-console-consumer --bootstrap-server $BROKER \
  --topic ingestion.dlq --from-beginning --max-messages 20
```

The DLQ writer skips + commits offset on parse failure — the bad
messages don't loop. Drain them and fix the producer.

**(b) `dlq_writer.db_error` rising.** Postgres or pgbouncer
unhealthy. The writer continues consuming (per the M3.1 PRIME
DIRECTIVE) but doesn't UPSERT until the connection recovers. Check
pgbouncer + Postgres health; the offset has NOT been committed for
the failing messages, so they redeliver on the next poll.

**(c) `dlq_writer.consumer_lag_seconds > 300` (5 minutes).** Either
the producer is bursting faster than the writer can drain (rare —
DLQ is low-volume by design), or the writer is stuck on a single
slow row. Check the most recent `ingestion_failures.last_seen_at`;
if it's recent, the writer is keeping up. If stale, scale to 2
replicas (group-balanced).

### 1.5 Replay surface

Each failure-kind has a specific replay anchor (LLD §8, "Replay
anchor" column). The replay tool reads the anchor field per kind:

- `normalizer_parse_error`, `observation_insert_error`: `raw_s3_key`
  → re-publishes the raw envelope to `ingestion.raw`.
- `embedding_ollama_failure`: `error_context.observation_id` →
  re-publishes to `ingestion.embedding` (M3.2 worker picks it up).
- Others: see catalog.

Operator command to mark a row resolved without replay (e.g. discard):

```sql
UPDATE ingestion_failures
   SET resolved_at = now(),
       resolution_kind = 'discarded',
       resolved_by = '<operator-id>'
 WHERE id = $1;
```

---

## 2. Live embedding worker (`services.ingestion.writers.embedding_worker`)

### 2.1 What it does

Consumes `ingestion.embedding`. For each envelope:

1. SELECTs the observation by `observation_id` (sets
   `app.current_tenant` for RLS).
2. If `embedding_pending` is FALSE → skip (inline path or backlog
   drainer beat the worker; LLD §5.4 guard).
3. Calls `OllamaClient.embed(content_text)` (the client retries
   internally on transients — 3x default with exponential backoff).
4. `UPDATE observations SET embedding=$1, embedding_pending=FALSE
   WHERE id=$2 AND embedding_pending=TRUE`. The flag-only guard is
   load-bearing for race-safety AND operator-driven re-embed.
5. On terminal `OllamaError`: publish DLQ
   (`failure_kind="embedding.ollama_failure"`) + commit offset.

### 2.2 Env vars

```
KAFKA_BOOTSTRAP_SERVERS         default: localhost:9092
DATABASE_URL                    required
POSTGRES_POOL_SIZE              default: 5
OLLAMA_URL                      default: http://localhost:11434
OLLAMA_EMBED_MODEL              default: nomic-embed-text
EMBEDDING_WORKER_LOG_LEVEL      default: INFO
```

### 2.3 Metrics

```
embedding_worker.messages_consumed
embedding_worker.envelope_parse_failure
embedding_worker.observation_missing
embedding_worker.guard_no_op
embedding_worker.embeds_succeeded
embedding_worker.embeds_failed
embedding_worker.dlq_publish.{success,failure,skipped}
```

### 2.4 Common incidents

**(a) `embedding_worker.embeds_failed` rising sharply.** Ollama is
unhealthy. The OllamaClient already retried 3x per call; the worker
DLQs the failure and moves on. Inspect:

```bash
curl -s $OLLAMA_URL/api/tags  # Is Ollama up?
curl -s $OLLAMA_URL/api/embeddings -d '{"model":"nomic-embed-text","prompt":"x"}' | head
```

Failed observations stay at `embedding_pending=TRUE`. After Ollama
recovers, run the backlog drainer (§3) to pick them up.

**(b) `embedding_worker.guard_no_op` >> `embeds_succeeded`.** The
inline path is succeeding most of the time and the worker is
short-circuiting on the pending=FALSE guard. **Expected during the
cutover** — the worker exists to catch inline failures, not steal
work from inline. If the ratio is high in steady-state, the topic
publish is happening too eagerly (e.g. the inline path is publishing
for already-embedded rows). Audit the
`services.ingestion.embedding.publish_embedding_request` call site
at `services/ingestion/core.py`.

**(c) `embedding_worker.observation_missing` rising.** Observations
are being deleted between the publish and consume. Check:

- Tenant-data-deletion jobs running mid-pipeline.
- Bad `observation_id` values being published (envelope schema
  drift). Inspect a few raw envelopes from `ingestion.embedding`.

**(d) Slow embedding throughput.** Ollama is CPU/GPU-bound. The
worker is single-threaded per process (each call blocks on the HTTP
response from Ollama). Scale horizontally: more worker pods → more
parallel calls to Ollama. Watch Ollama saturation — at some point
adding workers stops helping.

### 2.5 Re-embed pattern

Operator wants to re-compute embeddings (model changed, content
hash changed, debugging):

```sql
UPDATE observations
   SET embedding_pending = TRUE
 WHERE <selection>;
```

Then publish to `ingestion.embedding` for each `observation_id`
(or run the backlog drainer — §3). The LLD §5.4 guard means the
worker's UPDATE succeeds even though `embedding != NULL`. This is
the load-bearing reason the guard is `WHERE embedding_pending =
TRUE` and NOT `WHERE embedding IS NULL` — see amendment A3.

---

## 3. Backlog embedding service (`services.ingestion.recovery.embedding_backlog`)

### 3.1 What it does

Long-running rate-limited drainer that scans
`observations.embedding_pending=TRUE` directly from Postgres (NOT
via Kafka, to avoid starving steady-state). Reshaped from
"one-shot script" to "service" per amendment A4.

Persists scan cursor in `embedding_backlog_state` (migration 0052).
SIGTERM resumes from the same point on restart.

Rate-limited via the M1.3 Lua bucket
`rate:*system:ollama:embed`. No parallel rate-limit surface in the
service code.

### 3.2 Env vars

```
DATABASE_URL                   required
REDIS_URL                      default: redis://localhost:6379/0
OLLAMA_URL                     default: http://localhost:11434
OLLAMA_EMBED_MODEL             default: nomic-embed-text
KAFKA_BOOTSTRAP_SERVERS        default: localhost:9092  (DLQ publishes)
BACKFILL_OLLAMA_QPS            default: 10              (operator throttle)
BACKFILL_INSTANCE_NAME         default: "default"
BACKFILL_BATCH_SIZE            default: 50
EMBEDDING_BACKLOG_LOG_LEVEL    default: INFO
```

### 3.3 Metrics

```
backlog.iterations
backlog.rows_selected
backlog.rows_embedded
backlog.rows_skipped_no_text
backlog.rows_failed
backlog.rate_limit_denials
backlog.rate_limit_sentinels
backlog.cursor_resets
backlog.dlq_publish.{success,failure,skipped}
```

### 3.4 Operator controls

#### (a) Pause — `BACKFILL_OLLAMA_QPS=0`

The operator pause switch. Updates the env var and the Lua bucket
config in Redis takes effect on the next acquire call (≤1s). The
service stalls indefinitely without thrashing — `acquire.lua`
returns the `-1` sentinel, the service polls at
`SENTINEL_RECHECK_SEC=1s`, no Ollama calls.

**No restart required.** Verify via metrics:
`backlog.rate_limit_sentinels` increments while
`backlog.rows_embedded` stays flat.

To resume: set `BACKFILL_OLLAMA_QPS` back to the desired value.

#### (b) Throttle — `BACKFILL_OLLAMA_QPS=<N>`

Sets `refill_per_sec=N` on the Lua bucket; capacity = `max(1, N)`
(~1s of burst). The service paces itself to ~N embeddings/sec.

#### (c) Hard stop — SIGTERM

```bash
kill -TERM <pid>
# OR
kubectl delete pod backlog-drainer-<id>
```

The service completes its current iteration (at most one row) and
exits cleanly with code 0. Cursor was persisted after the previous
row, so restart picks up where it left off without re-processing.

Verified by `test_backlog_service_resumes_from_cursor` — real
subprocess + real SIGTERM, not a flag flip.

#### (d) Reset cursor (force re-scan from beginning)

Rare. Useful if the cursor row is corrupted or you want to
re-validate the entire pending set:

```sql
UPDATE embedding_backlog_state
   SET cursor_ingested_at = NULL,
       cursor_id          = NULL,
       updated_at         = now()
 WHERE instance_name = 'default';
```

### 3.5 Common incidents

**(a) Cursor is non-NULL but `rows_selected` is 0.** All rows past
the cursor are non-pending. The service should reset the cursor on
the next iteration (`backlog.cursor_resets` will increment) and then
either find late arrivals or pause at the drained state.

**(b) `backlog.rate_limit_sentinels` rising without operator
intent.** Someone set `BACKFILL_OLLAMA_QPS=0` without
documentation. Check deployment manifests + env-var override
sources.

**(c) `backlog.rows_failed` rising.** Ollama is failing for many
rows. The service publishes DLQ for each (same envelope shape as
M3.2 worker — `failure_kind="embedding.ollama_failure"`,
`error_context.observation_id` for replay) and advances the cursor
past the row. After Ollama recovers, the cursor reset (when the
service hits end-of-scan) will re-attempt those rows in the next
pass.

**(d) Service is processing the same rows in a loop.** This SHOULD
NOT happen — the cursor advances past every row regardless of
outcome. If observed: cursor write is failing. Check
`embedding_backlog_state.updated_at` is updating; check
`postgres_pool_size` and pgbouncer health.

### 3.6 Sizing guidance

For a one-pass drain over N pending observations at Q QPS:
`duration ≈ N / Q` seconds. Real-world Ollama on a single instance
caps around 20–50 QPS for `nomic-embed-text` (CPU). For a 1M-row
backlog at 30 QPS: ~9.3 hours wall-clock.

For larger backlogs (10M+): run multiple instances with distinct
`BACKFILL_INSTANCE_NAME` values. Each instance has its own cursor
row; the SHARED `rate:*system:ollama:embed` Lua bucket caps total
throughput regardless of replica count.

---

## 4. Cross-service: replay an embedding failure

Walkthrough for `embedding_ollama_failure` rows in `ingestion_failures`:

```sql
SELECT id, tenant_id, error_summary,
       error_context->>'observation_id' AS obs_id
  FROM ingestion_failures
 WHERE failure_kind = 'embedding_ollama_failure'
   AND resolved_at IS NULL
 LIMIT 10;
```

For each row, the replay anchor is `obs_id`. To re-attempt:

```sql
UPDATE observations
   SET embedding_pending = TRUE
 WHERE id = '<obs_id>';
```

Then either:
- Publish manually to `ingestion.embedding` (M3.2 worker picks up),
  or
- Wait for the backlog drainer to scan past this row.

After successful re-embed, mark resolved:

```sql
UPDATE ingestion_failures
   SET resolved_at = now(),
       resolution_kind = 'manual_recovered',
       resolved_by = '<operator-id>'
 WHERE id = '<failure_row_id>';
```

---

## 5. Deployment checklist

Before flipping `ingestion.kafka_path_enabled=true` for a tenant
(M5 cutover):

- [ ] Migration `0051_ingestion_failures_upsert_key.sql` applied
      against the target DB. **Requires Postgres 15+** (NULLS
      DISTINCT). Confirm: `SELECT pg_indexes_size('ingestion_failures');`
      and `\d ingestion_failures` shows `ingestion_failures_upsert_key_idx`.
- [ ] Migration `0052_embedding_backlog_state.sql` applied.
- [ ] Kafka topic `ingestion.embedding` exists with 4 partitions
      (dev) / 64 (prod), 7-day retention, zstd compression. The
      script `scripts/dev/create-kafka-topics.sh` is idempotent —
      run on the prod broker if absent.
- [ ] Redis reachable on `REDIS_URL` from the backlog service
      deployment (the rate limiter is the only Redis user in M3).
- [ ] Ollama reachable on `OLLAMA_URL` from both worker
      deployments. Confirm the model is loaded:
      `curl $OLLAMA_URL/api/tags | jq '.models[] | .name'` includes
      `nomic-embed-text`.
- [ ] DLQ writer deployment scaled ≥1. (It's idempotent; safe to
      start before producers.)
- [ ] Embedding worker deployment scaled ≥1.
- [ ] Backlog drainer deployment with `BACKFILL_OLLAMA_QPS` set
      to a conservative initial value (10 QPS for first pass; tune
      up after observing Ollama saturation).
- [ ] Alerts on:
      - `ingestion_failures` with `resolved_at IS NULL AND
        last_seen_at > now() - interval '1 hour'` count rising.
      - `embedding_worker.embeds_failed` rate > 0.1/s sustained.
      - `backlog.rate_limit_sentinels` rising while
        `BACKFILL_OLLAMA_QPS != 0` (unexpected pause).

---

## 6. Known-limits / future work

- **DLQ writer is single-threaded per process** (one Kafka consumer
  per group member). Scale horizontally by adding replicas —
  consumer group rebalancing handles partition assignment.
- **Backlog drainer's `rate:*system:ollama:embed` bucket is global.**
  Multi-instance deployments share the throughput cap; this is
  intentional (one global Ollama saturation budget). If per-tenant
  fairness becomes a concern, the M1.3 bucket key supports
  per-tenant variants — that's an M5+ design discussion.
- **No automatic DLQ replay** in M3. Replays are operator-driven
  via the SQL walkthroughs in §4 (and the future replay tool in
  M5/M6). Auto-replay was deliberately scoped out — recovery
  decisions are policy-shaped and require human judgement.
