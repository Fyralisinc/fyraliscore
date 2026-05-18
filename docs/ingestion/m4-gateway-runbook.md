# M4 Discord Gateway Worker — Production Operator Runbook

**Scope.** This runbook covers the M4 + A6 deliverables: the Discord
Gateway worker (`services.integrations.discord.gateway.worker`), the
Redis leader lease (`services.integrations.discord.gateway.leader_lock`),
the persisted session state
(`services.integrations.discord.gateway.session_state`), the
lifecycle orchestrator
(`services.integrations.discord.gateway.lifecycle`), and the A6
broker-ack durability barrier
(`services.integrations.discord.gateway._durability`).

For the M2 shadow path (raw tier the gateway worker writes into),
see [m2-shadow-path-runbook.md](m2-shadow-path-runbook.md). For the
M3 embedding pipeline, see [m3-embedding-runbook.md](m3-embedding-runbook.md).

**Audience.** On-call operator with `kubectl` + `redis-cli` +
`psql` access. Assumes familiarity with the M2 runbook (the gateway
worker writes into the same raw tier the M2 webhook router writes
to).

**As of:** 2026-05-18 (`fix/a6-broker-ack-ordering` branch,
pre-merge, A6 closeout).

---

## 0. Quick-reference

| Component | Backing store | Failure mode |
|---|---|---|
| Leader lease | Redis key `gateway:discord:leader_lock` (30s TTL, 10s refresh) | Lease loss → worker requests graceful shutdown; do NOT fight for re-acquire |
| Session state | Postgres `gateway_session_state` (UPSERT per frame) | Staleness > 4 min → next worker IDENTIFYs fresh instead of RESUME |
| Broker-ack barrier | Per-frame `pre_save_flush` (2s timeout) | Flush failure → save skipped, metric increments, next worker re-processes frame under M2 dedup |

**Failure surfaces (read order in an incident):**

1. Metric `discord_gateway_pre_save_flush_failures_total` (broker durability uncertain)
2. Metric `discord_gateway_connect_failure_total` + log `discord_gateway_connect_failed`
3. Postgres: `SELECT * FROM gateway_session_state WHERE application_id = $1` (verify session_id + last_seq freshness)
4. Redis: `redis-cli GET gateway:discord:leader_lock` + `TTL gateway:discord:leader_lock`
5. Application logs (structured JSON, structlog, `integrations.discord.gateway.*` namespace)

---

## 1. Gateway worker lifecycle

### 1.1 What it does

Long-running process that maintains one Discord Gateway WSS
connection per Discord application. Lifecycle on startup:

```
state-load  →  lease-acquire  →  RESUME-or-IDENTIFY  →  WS loop
```

1. Connect to Redis + Postgres.
2. Load `gateway_session_state` (returns None if absent or older
   than 4 min — Discord has torn down the buffered session by then).
3. Acquire the Redis lease (backoff-bounded; exits non-zero on
   timeout — orchestrator restarts the pod and retries).
4. Construct `DiscordGatewayClient` with the loaded state injected.
   RESUME if state is present + session_id non-NULL; IDENTIFY fresh
   otherwise. Choice is logged at INFO.
5. Run the WS dispatch loop. Each op-0 DISPATCH frame:
   - `dispatch_handler` runs (calls `shadow_write_raw` to S3 +
     Kafka).
   - **A6 broker-ack barrier**: `pre_save_flush(producer, 2s)`
     awaits broker ack for the in-flight shadow-write.
   - On success: `save_session_state(last_seq=N)` (fire-and-forget
     task).
   - On flush failure: metric increments, save skipped, next frame
     proceeds.

### 1.2 Env vars

```
DATABASE_URL              required (Postgres for session_state)
REDIS_URL                 required (Redis for lease)
KAFKA_BOOTSTRAP_SERVERS   required (shadow-path target)
DISCORD_BOT_TOKEN         required (per-application)
DISCORD_APPLICATION_ID    required
S3_RAW_BUCKET             default: fyralis-raw
INGESTION_ENV             default: dev
GATEWAY_LOG_LEVEL         default: INFO
```

### 1.3 Exit codes

| Code | Cause | Supervisor action |
|---|---|---|
| 0 | SIGTERM/SIGINT clean shutdown OR lease lost cleanly | Restart per schedule |
| 1 | Fatal Discord close (4004, 4010–4014) | **Do NOT auto-restart** — investigate Bot/Application config |
| 2 | Configuration error at startup (missing env) | Investigate env wiring; restart after fix |

---

## 2. A6 broker-ack durability barrier

### 2.1 Latency expectation

The gateway worker adds **~3 ms p95** per frame for broker-ack
durability. Measured on the dev single-broker cluster (n=100):

| Percentile | Latency |
|---|---|
| p50 | 1.43 ms |
| mean | 1.65 ms |
| p95 | 2.71 ms |
| p99 | 6.51 ms |
| max | 6.52 ms |

Production 3-broker clusters with `acks=all` run in the same order
of magnitude (single-digit milliseconds).

**Sustained throughput well below ~300 frames/sec/shard.** At the
measured p95 ceiling (2.71 ms/frame), one shard can drain ~370
frames/sec sequentially. Discord's actual MESSAGE_CREATE rate per
typical tenant is **<5 msg/sec sustained** — three orders of
magnitude under this ceiling. The latency cost of A6's barrier is
not a practical constraint.

If a future high-volume tenant (e.g. a busy community server)
approaches the per-shard ceiling, the durability mechanism is
swappable for Option 3 (delivery-report-callback save) without
breaking the worker's external contract — see
[docs/decisions/a6-resolution.md](../decisions/a6-resolution.md)
"Reversibility."

### 2.2 The failure metric

`discord_gateway_pre_save_flush_failures_total` — broad-scope
counter. Increments whenever `pre_save_flush(producer, 2s)` raises
ANY exception:

- `TimeoutError` (broker didn't ack within 2s, e.g. ISR shrink,
  broker unavailable, network partition).
- `ConnectionError` / broker-disconnect exceptions.
- Other producer-side exceptions.

**What it means.** The flush after the frame's shadow-write did
not complete. The frame is in the producer's local queue but the
broker may or may not have committed it. The save was **skipped**
deliberately — we will NOT advance `last_seq` past a frame whose
Kafka durability is uncertain.

**What happens to the frame.** The gateway worker continues
operating. The frame's seq is NOT persisted. On the next session
restart (planned SIGTERM or RESUME after disconnect), the worker
RESUMEs from the previously-saved seq and Discord re-delivers the
frame. M2 content_hash dedup absorbs the re-processing. **No data
loss, but elevated work for the next worker** (re-process a small
number of frames).

**What to do when it fires.**

1. Check broker connectivity from the gateway pod:
   ```
   kubectl exec -it <gateway-pod> -- kafka-broker-api-versions --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS
   ```
   Should return within a couple of seconds. If it hangs or errors,
   it's a broker-side problem — escalate to Kafka oncall.

2. Check ISR health:
   ```
   kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
     --describe --topic ingestion.raw
   ```
   Look for partitions with `Isr` smaller than `Replicas`. Under-
   replicated partitions can cause `acks=all` to time out.

3. Read the structured log lines:
   ```
   kubectl logs <gateway-pod> | grep discord_gateway_pre_save_flush_failed
   ```
   Each line carries `seq`, `error_type`, `error_message[:200]`.
   Repeated `TimeoutError` indicates broker latency; repeated
   `ConnectionError` indicates broker reachability.

4. Decide: continue (transient broker issue, A6 contract holds —
   frames re-process on next RESUME) OR rotate the gateway pod
   (forces a RESUME, which re-processes the small window of
   skipped-save frames cleanly).

**When NOT to be alarmed.** A few isolated failures per hour
during broker restarts or rolling deploys are expected and
benign — A6 is doing its job. Page only on sustained rate
(e.g. >10 failures/min sustained for >5 min).

---

## 3. Leader lease

### 3.1 What it does

Single-holder lock in Redis. Key `gateway:discord:leader_lock`, 30s
TTL, 10s refresh. The holder runs the gateway WSS connection;
other pods loop on backoff trying to acquire.

**Prime directive:** on refresh failure (lease expired or another
pod took over), the worker MUST request graceful shutdown — NEVER
re-acquire while the WS is still open. Two pods sharing the bot
token cause duplicate consumption + Discord "Already authenticated"
4004 close.

### 3.2 Common queries

```
# Who holds the lease right now?
redis-cli GET gateway:discord:leader_lock
# How much time is left on it?
redis-cli TTL gateway:discord:leader_lock
```

If `GET` returns nil but pods are running and not connecting,
something has cleared the key — check Redis ACLs and any cleanup
scripts.

### 3.3 Manually releasing the lease

```
# Only if you're sure the holding pod is dead:
redis-cli DEL gateway:discord:leader_lock
```

Next pod acquires within its next backoff cycle (≤ 30s).

---

## 4. Session state

### 4.1 What it does

Postgres-persisted `(application_id, shard_id) → session_id, last_seq,
resume_gateway_url, heartbeat_interval_ms, ...`. UPSERT per dispatched
frame, AFTER the A6 flush completes. Used by the next worker on
restart to decide RESUME-vs-IDENTIFY.

### 4.2 Staleness contract

Rows older than 4 minutes (`STALENESS_THRESHOLD` in
`session_state.py`) are treated as None by `load_session_state` —
Discord retains buffered sessions for ~4-5 min, beyond that a RESUME
attempt will be rejected with Invalid Session and force IDENTIFY
anyway. Short-circuiting at load time saves the roundtrip.

### 4.3 Common queries

```sql
-- Current session for an application:
SELECT application_id, shard_id, session_id, last_seq,
       updated_at, NOW() - updated_at AS age
FROM gateway_session_state
WHERE application_id = $1;

-- Find sessions about to go stale (within 30s of the 4-min cutoff):
SELECT application_id, last_seq, updated_at,
       NOW() - updated_at AS age
FROM gateway_session_state
WHERE updated_at < NOW() - INTERVAL '3 minutes 30 seconds'
ORDER BY updated_at;
```

A sustained increase in age across applications indicates the
gateway workers are not dispatching frames (could be: Discord
heartbeat issues, lease churn, broker problems gating the save via
A6 — check the `pre_save_flush_failures` metric first).

---

## 5. A6 resolution status

**LLD amendments tracker A6 — Discord Gateway shadow-write Kafka
flush window. RESOLVED as of `fix/a6-broker-ack-ordering` (commits
`269ce65` + `08c3b1f` + `4ddaf7f`).**

**M5 pre-cutover gate condition (8): resolved.** The gateway
worker's save-state is durable against broker-not-yet-acked frames;
verified by
[`test_no_frames_lost_across_sigkill`](../../services/integrations/discord/gateway/tests/test_gateway_lifecycle.py)
running against the extracted production function (no test-level
workaround — the subprocess simulation imports the same
`pre_save_flush` from
[`services/integrations/discord/gateway/_durability.py`](../../services/integrations/discord/gateway/_durability.py)
that production uses).

Operators may proceed with M5 cutover planning subject to the
remaining seven pre-cutover gate conditions in
[04-implementation-plan.md §M5](04-implementation-plan.md).

---

## 6. References

- [03-low-level-design.md §1.5](03-low-level-design.md) — gateway
  session state schema.
- [docs/decisions/a6-resolution.md](../decisions/a6-resolution.md) —
  A6 design decision (Option 1 chosen) and Phase 3 import-graph
  trade-off note.
- [05-lld-amendments.md §A6](05-lld-amendments.md) — the amendment
  itself, now in resolved state with citations.
- [04-implementation-plan.md §M5](04-implementation-plan.md) — M5
  pre-cutover gate conditions, with condition (8) marked resolved.
