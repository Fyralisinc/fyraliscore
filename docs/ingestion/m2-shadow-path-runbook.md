# M2 Shadow-Path Operator Runbook

**Audience:** an operator who has never seen this code before and needs to
deploy + monitor the M2 shadow path in production within an hour.

**Scope:** M2 only (raw tier + normalizer + no-op writer). M3+ (real DB
writes, embedding worker, DLQ) is out of scope.

**Goal of M2 in production:** run the shadow path alongside the existing
inline ingest path for 48 hours, observe zero divergence between the
two paths' record sets, then hand off to M3 which will let the shadow
path start writing observations.

---

## 0. What you're running

```
                                  ┌──────────────────────┐
        webhooks / gateway / pubsub  →  inline ingest()  →  observations table
                                  └──────────────────────┘   (Path A — source of truth in M2)

                                  ┌──────────────────────────────────────────┐
                                  │  shadow_write_raw()                       │
                                  │    ↓                                      │
                                  │  S3 (raw bytes)  +  ingestion.raw (Kafka) │
                                  │    ↓                                      │
                                  │  normalizer worker  (Path B — no DB)      │
                                  │    ↓                                      │
                                  │  ingestion.normalized (Kafka)             │
                                  │    ↓                                      │
                                  │  observation_writer (Path B — logs only)  │
                                  └──────────────────────────────────────────┘
```

Two processes you will start: **normalizer** and **observation_writer**.
Everything else (the inline path, the shadow-write call sites in the
webhook router / Discord gateway / Gmail Pub/Sub endpoint) is already
running as part of the gateway service.

---

## 1. Prerequisites

Check each of these before starting M2. If anything is missing, stop —
don't proceed.

### 1a. Docker daemon

```bash
docker ps
```

Must succeed. If not: install Docker, then `sudo systemctl start docker`.

### 1b. Postgres (5433/dev)

```bash
psql "$DATABASE_URL" -c "SELECT 1"
```

Must return `1`. The gateway / inline path is already using this DB.

### 1c. Kafka + S3 (M2 dev stack)

```bash
./scripts/dev/m2-up.sh
```

Wait for both containers to report `healthy`:

```bash
docker ps --filter "name=fyralis_dev_" --format '{{.Names}}: {{.Status}}'
# expect:
#   fyralis_dev_kafka:    Up X seconds (healthy)
#   fyralis_dev_moto_s3:  Up X seconds (healthy)
```

### 1d. Kafka topics

```bash
./scripts/dev/create-kafka-topics.sh
```

Idempotent. Verify:

```bash
docker exec fyralis_dev_kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
# expect: ingestion.normalized, ingestion.raw
```

### 1e. Tenant feature flag default

```sql
-- Verify default-on behavior. ingestion.shadow_write_enabled returns
-- True when no row exists; this query shows existing per-tenant overrides.
SELECT tenant_id, flag_name, flag_value, updated_at
FROM tenant_flags
WHERE flag_name = 'ingestion.shadow_write_enabled';
```

Empty result = every tenant has the shadow path enabled (default-on).
That's what M2 wants. Per-tenant disables come later, §6.

---

## 2. Environment variables

```bash
# Required for both processes.
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Required for the normalizer (S3 reads). For dev moto:
export S3_ENDPOINT_URL=http://localhost:5001
export S3_RAW_BUCKET=fyralis-raw
export S3_REGION_NAME=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

# Optional: log level.
export NORMALIZER_LOG_LEVEL=INFO
export WRITER_LOG_LEVEL=INFO

# Optional: normalizer pool size.
export NORMALIZER_NUM_WORKERS=2
```

Production: replace `S3_ENDPOINT_URL` with the real S3 / R2 endpoint
and the keys with production credentials sourced from the secret store
(per SC-008). Do NOT inline production secrets into `.env`.

---

## 3. Starting the processes

Run each in its own terminal / systemd unit. Both processes are
restart-safe; they reconnect to Kafka cleanly.

### 3a. Normalizer pool

```bash
.venv/bin/python -m services.ingestion.normalizer
```

This spawns `NORMALIZER_NUM_WORKERS` worker processes (default 2). Each
worker joins consumer group `normalizer` and reads from `ingestion.raw`.

For dev debugging, you can run a single worker in the foreground:

```bash
.venv/bin/python -m services.ingestion.normalizer --single-worker
```

You should see:

```
INFO services.ingestion.normalizer.supervisor normalizer.worker_started [pid=...] label=normalizer-0
INFO services.ingestion.normalizer.supervisor normalizer.worker_started [pid=...] label=normalizer-1
```

### 3b. Observation writer

```bash
.venv/bin/python -m services.ingestion.writers
```

This is a single process. It joins consumer group `observation-writer`
and reads from `ingestion.normalized`.

You should see:

```
INFO services.ingestion.writers.observation_writer writer.shadow_write_event ...
```

…for every normalized envelope. **In M2 this writer does NOT INSERT
into `observations`.** It logs and bumps in-process metrics. The
inline path is still the only writer to `observations`.

---

## 4. Monitoring: Kafka consumer-group lag

The single most useful health signal in M2 is consumer-group lag on
the two topics. Lag > 0 means there's a backlog the worker is still
draining; lag growing unbounded means the worker is slower than
ingress (a problem).

### 4a. Normalizer lag (on `ingestion.raw`)

```bash
docker exec fyralis_dev_kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group normalizer \
  --describe
```

Read the `LAG` column. Healthy: every partition < 1000. Unhealthy: any
partition's lag growing monotonically over a 5-minute window.

### 4b. Writer lag (on `ingestion.normalized`)

```bash
docker exec fyralis_dev_kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group observation-writer \
  --describe
```

Same expectations. The writer's logic is trivial (parse + append +
log), so lag here usually means Kafka is the bottleneck, not the
writer.

### 4c. Live tail of inflight messages

```bash
docker exec fyralis_dev_kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ingestion.raw \
  --max-messages 5 \
  --timeout-ms 5000
```

Sample bytes from `ingestion.raw`. JSON-decode them to confirm the
envelope shape matches `RawEnvelope` (Pydantic model in
[services/ingestion/raw_tier/envelope.py](services/ingestion/raw_tier/envelope.py)).

---

## 5. Zero-divergence check — the 48-hour gate

This is the load-bearing M2 production task. Compare the inline path's
observations to the shadow path's writer log. They MUST agree.

### 5a. SQL: count inline observations in the test window

```sql
-- Replace WINDOW_START / WINDOW_END with the 48-hour window's bounds.
SELECT
  source_channel,
  COUNT(*) AS inline_count
FROM observations
WHERE created_at >= TIMESTAMP 'WINDOW_START'
  AND created_at <  TIMESTAMP 'WINDOW_END'
GROUP BY source_channel
ORDER BY source_channel;
```

### 5b. Shadow path's event count

The writer's shadow log is in-process (M2 design: no DB writes from
Path B). Read it via the process's structured logs — every successful
envelope emits a `writer.shadow_write_event` log line carrying
`source_channel` and `external_id`. Pipe writer logs to a file:

```bash
.venv/bin/python -m services.ingestion.writers \
  2>&1 | tee /var/log/fyralis/observation-writer.log
```

Then aggregate:

```bash
# Count shadow-write events by source_channel.
grep -F 'writer.shadow_write_event' /var/log/fyralis/observation-writer.log \
  | jq -r 'select(.created_at >= "WINDOW_START" and .created_at <  "WINDOW_END") | .source_channel' \
  | sort | uniq -c
```

(Adjust `jq` if your logger emits text instead of structured JSON — see
[services/ingestion/writers/observation_writer.py](services/ingestion/writers/observation_writer.py)
for the field names.)

### 5c. Set-equality on `external_id` (the load-bearing check)

Counts can match while specific records diverge — a webhook delivered
twice within the test window dedups on the inline path's UNIQUE
constraint but not on the shadow path's append-only log. Set equality
catches this:

```sql
-- Inline path's external_ids in the window.
\copy (
  SELECT external_id
  FROM observations
  WHERE created_at >= TIMESTAMP 'WINDOW_START'
    AND created_at <  TIMESTAMP 'WINDOW_END'
    AND external_id IS NOT NULL
) TO '/tmp/inline_external_ids.txt'
```

```bash
# Shadow path's external_ids from the writer's logs.
grep -F 'writer.shadow_write_event' /var/log/fyralis/observation-writer.log \
  | jq -r '.external_id // empty' \
  | sort -u > /tmp/shadow_external_ids.txt

sort -u /tmp/inline_external_ids.txt > /tmp/inline_sorted.txt

# Diff.
diff /tmp/inline_sorted.txt /tmp/shadow_external_ids.txt
# Empty output = ZERO DIVERGENCE. M2 gate passed.
```

The E2E unit test asserts the same property at small scale — see
[services/ingestion/tests/test_e2e_shadow.py](services/ingestion/tests/test_e2e_shadow.py).

---

## 6. Per-tenant disable

If a specific tenant's shadow path needs to be disabled in emergency
(e.g. their data has a producer-side bug that triggers
`EnvelopeInvariantError` storms), flip their flag:

```sql
INSERT INTO tenant_flags (tenant_id, flag_name, flag_value, updated_at)
VALUES ('<tenant-uuid>', 'ingestion.shadow_write_enabled', false, now())
ON CONFLICT (tenant_id, flag_name)
DO UPDATE SET flag_value = false, updated_at = now();
```

Effect: that tenant's inline path keeps running; their shadow path
no-ops. The shadow path's 30-second TTL cache picks up the change
within 30 seconds — see
[services/ingestion/feature_flags/client.py](services/ingestion/feature_flags/client.py).

**To re-enable:**

```sql
UPDATE tenant_flags
   SET flag_value = true, updated_at = now()
 WHERE tenant_id = '<tenant-uuid>'
   AND flag_name = 'ingestion.shadow_write_enabled';
```

**Global disable** (kill switch for an incident):

```sql
INSERT INTO tenant_flags (tenant_id, flag_name, flag_value, updated_at)
SELECT id, 'ingestion.shadow_write_enabled', false, now()
  FROM tenants
ON CONFLICT (tenant_id, flag_name) DO UPDATE SET flag_value = false;
```

Reverses with `UPDATE … SET flag_value = true`. The inline path is
unaffected by these flags.

---

## 7. Triage: divergence appears

**Divergence in M2 is a BUG TICKET, not an emergency.** The inline
path is the source of truth; the shadow path is observable only via
the writer's logs. Filing a bug:

1. **Capture the divergent records.** Save the two `/tmp/*.txt` files
   from §5c and the writer's log for the test window.

2. **Identify the asymmetry direction:**
   - `inline_only` (records the inline path has but the shadow path
     doesn't) → likely shadow-write failure. Check:
     - `shadow_path.failure` log lines from the gateway service.
     - The shadow-write metric counters
       (`shadow_write.failure.s3`, `shadow_write.failure.kafka`).
   - `shadow_only` (records the shadow path has but the inline path
     doesn't) → very unusual; the inline path should write before
     the shadow does (the M2.1 ordering decision — see
     [services/webhooks/router.py](services/webhooks/router.py) line 741+).

3. **Identify the affected source / ingress_kind / tenant.** The
   writer's shadow-write event log has all three fields per record.

4. **File the ticket with:**
   - Source / ingress_kind / tenant_id.
   - Count of divergent records.
   - Direction (`inline_only` vs `shadow_only`).
   - Time window.
   - Sample 5–10 divergent `external_id`s.

5. **Do NOT roll back the shadow path** unless the divergence is in
   the `shadow_only` direction AND has security implications (e.g.
   the shadow records contain raw guild_ids that the inline path
   correctly hashed). In the normal `inline_only` direction the
   shadow path is just leaky observability — operationally safe to
   leave running while the bug is investigated.

---

## 8. Common errors and what they mean

### 8a. Normalizer logs `normalizer.invariant_failure`

A raw envelope failed
[services/ingestion/normalizer/invariants.py](services/ingestion/normalizer/invariants.py).
The Kafka message was COMMITTED (the worker does not loop on garbage —
this is the M2.4 prime directive) and the next message proceeds.

Likely causes:
- A producer was deployed with a buggy content_hash or raw_s3_key
  shape — check recent gateway deploys.
- An old envelope being replayed (`ingested_at` past the 30-day
  window) — usually safe to ignore.
- Forbidden raw identifier in `ingress_metadata` (SC-006) — actually
  an incident; file with security label.

### 8b. Normalizer logs `normalizer.unsupported_combination`

`(source, ingress_kind)` not yet mapped to a handler. In M2 this fires
exactly for `(gmail, pubsub)` envelopes — see
[services/ingestion/normalizer/channel_mapping.py](services/ingestion/normalizer/channel_mapping.py).
Expected behavior; no action required. M6 adds the mapping.

### 8c. Writer logs `writer.parse_failed`

Bytes on `ingestion.normalized` failed `NormalizedEnvelope`
validation. This means the NORMALIZER produced invalid output — a
bug. Capture the offending message offset + partition (visible in the
log line) and file a normalizer ticket.

### 8d. Lag growing unbounded on `ingestion.raw`

The normalizer pool is slower than ingress.
1. Check `top` / `htop` — are the workers CPU-bound?
2. Increase `NORMALIZER_NUM_WORKERS` and restart the supervisor.
3. If a single worker is stuck (zero CPU but non-zero lag on its
   partitions), it's likely blocked on S3 — check the S3 endpoint /
   credentials.

---

## 9. Stopping cleanly

```bash
# Find the processes.
ps aux | grep services.ingestion.normalizer
ps aux | grep services.ingestion.writers

# Send SIGTERM to each. Both processes handle SIGTERM with graceful
# Kafka close + commit of in-flight offsets.
kill -TERM <pid>
```

For the normalizer supervisor: TERMing the supervisor cascades a TERM
to each child worker, then joins with a 10s timeout. Logs the exit:

```
INFO services.ingestion.normalizer.supervisor normalizer.worker_died ...
```

Verify final lag is acceptable (§4a, §4b). If lag was non-zero at
shutdown, those messages will be re-consumed when the workers
restart — at-least-once delivery is preserved by the post-process
commit pattern.

To reset the entire dev stack (Kafka + moto):

```bash
./scripts/dev/m2-down.sh   # stop containers
./scripts/dev/m2-reset.sh  # stop + delete volumes (fresh start)
```

---

## 10. Reference

- LLD: [docs/ingestion/03-low-level-design.md](03-low-level-design.md)
- HLD: [docs/ingestion/02-high-level-design.md](02-high-level-design.md)
- Pending LLD amendments (M1 + M2 findings): [docs/decisions/lld-amendments-pending.md](../decisions/lld-amendments-pending.md)
- E2E shadow test (the same property this runbook's §5c proves at
  scale, asserted on every CI run):
  [services/ingestion/tests/test_e2e_shadow.py](../../services/ingestion/tests/test_e2e_shadow.py)
- "Don't get stuck on garbage" test:
  [services/ingestion/normalizer/tests/test_worker_garbage_envelope.py](../../services/ingestion/normalizer/tests/test_worker_garbage_envelope.py)
