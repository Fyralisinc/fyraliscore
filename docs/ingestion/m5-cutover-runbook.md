# M5 Cutover Mechanism — Production Operator Runbook

**Scope.** This runbook covers the M5 cutover code path: the ingestion
circuit breaker
([services/ingestion/feature_flags/circuit_breaker.py](../../services/ingestion/feature_flags/circuit_breaker.py)),
the observation writer's full-mode branch
([services/ingestion/writers/observation_writer.py](../../services/ingestion/writers/observation_writer.py)),
the webhook router's flag-branched cutover
([services/webhooks/router.py](../../services/webhooks/router.py)),
and the `tenant_flags.ingestion.kafka_path_enabled` per-tenant
toggle that drives all three.

**Status: code complete, execution deferred.** The mechanism is
merged and tested at unit + integration levels. Real-traffic
cutover happens when:
  (a) customers exist (zero today),
  (b) the M-Load work-unit completes a synthetic-traffic dry run,
  (c) the M-Temporal work-unit replaces the deferred Kafka admin
      readers (currently `NotImplementedError`).

For the upstream M2 shadow path (the raw tier this cutover writes
into), see [m2-shadow-path-runbook.md](m2-shadow-path-runbook.md).
For the M3 embedding pipeline (downstream of the writer), see
[m3-embedding-runbook.md](m3-embedding-runbook.md). For the M4
Discord Gateway worker (a sibling ingress path; not affected by
cutover today), see [m4-gateway-runbook.md](m4-gateway-runbook.md).

**Audience.** On-call operator with `kubectl` + `psql` access.
Assumes familiarity with the M2 runbook (the cutover writes to the
same raw tier; the writer consumes from the same `ingestion.normalized`
topic).

**As of:** 2026-05-18 (`feat/ingestion-m5-cutover-mechanism` branch,
pre-merge, M5.4 closeout).

---

## 0. Quick-reference

| Component | Backing store | Failure mode |
|---|---|---|
| Cutover flag | Postgres `tenant_flags` row `(tenant_id, "ingestion.kafka_path_enabled")` | Missing row → default FALSE (inline path) |
| Flag cache | In-process 30s TTL per `(tenant_id, flag_name)` | Stale cached value during the 30s after a flip — bounded by the TTL |
| Circuit breaker state | Postgres `circuit_breaker_state` row `(instance_name, tenant_id)` | Tripped tenant frozen until operator flips flag back; bookkeeping auto-resets on flag→TRUE |
| Writer full mode | Postgres `observations` via `ingest_from_draft` (per-envelope txn) | Permanent error → DLQ + offset commit; transient → re-raise (supervisor restart) |
| Router cutover fallback | Inline `ingest()` + bumped fallback metric | Customer sees 200/201; operator sees `webhook_router_kafka_path_total{outcome="fallback"}` ticking |

**Operator failure surfaces (read order in an incident):**

1. Metric `webhook_router_kafka_path_total{outcome="fallback"}` —
   sustained increment = cutover path losing connectivity (customers
   still served via inline).
2. Event `circuit_breaker.tripped` (log + ops-alert channel) —
   per-tenant flag flipped to FALSE due to sustained lag breach.
3. `SELECT * FROM circuit_breaker_state WHERE tripped = TRUE` —
   audit list of tripped tenants.
4. Consumer-group lag on `ingestion.normalized` (the writer's input;
   the breaker's lag-measurement target).
5. Application logs (structlog, `webhooks.router.*`,
   `services.ingestion.feature_flags.circuit_breaker.*`,
   `services.ingestion.writers.observation_writer.*`).

---

## 1. Pre-cutover gates

These are the M5 readiness gates from
[04-implementation-plan.md §M5](04-implementation-plan.md#m5--steady-state-cutover-the-riskiest-milestone).
All six must be green before flipping the flag for any production
tenant.

1. **M1–M4 stable for ≥1 week in production.** Stable = zero
   un-acked incidents; backlogs drained.
2. **Shadow-path observation counts match inline within 0.01% for
   ≥48 h of sustained traffic.** Run the
   `test_e2e_shadow.py`-equivalent measurement at production volume;
   the M2 shadow-soak is the gate, not a unit test.
3. **Circuit breaker tested in staging:** inject synthetic 60 s+
   lag for 5+ min; verify the flag flips to FALSE for the affected
   tenant within `breach_window_ticks × tick_interval_sec` (default
   5 × 60 s = 5 min); verify traffic reverts to inline.
4. **This runbook reviewed + signed off** by the on-call team.
5. **Diagnostic queries Block 2 results in hand** (or explicit
   acknowledgment that proceeding without them is acceptable).
6. **Q4 WS-latency decision answered.** If "YES, 1–5 s is fine":
   single mode ships (Mode B collapse documented in
   [05-lld-amendments.md](05-lld-amendments.md)). If "NO":
   re-introduce Mode B before the first real cutover.

Gate conditions 7 and 8 (test_ingest_core.py CI + A6 broker-ack
ordering) were resolved during M3+M4 closeout — see the existing
notes in §M5 of the plan.

---

## 2. Per-source cutover semantics

The cutover does NOT apply uniformly across providers. This section
is the authoritative table.

| Provider | Cutover supported? | Path when flag=TRUE | Path when flag=FALSE |
|---|---|---|---|
| **Slack** (`slack:message` via `/webhooks/slack/*`) | YES | Kafka publish → writer full mode → observation. Router returns 202. | Inline `ingest()` → observation. Router returns 200/201. M2 shadow path also fires. |
| **GitHub** (`github:webhook` via `/webhooks/github/*`) | YES | Kafka publish → writer full mode → observation. Router returns 202. | Inline `ingest()` → observation. Router returns 200/201. M2 shadow path also fires. |
| **Discord** (`discord:interaction` via `/webhooks/discord/*`) | **NO — deferred** | _Cutover suppressed at the router._ Falls through to inline regardless of the flag. | Inline `ingest()` (existing M2 behaviour). |
| **Discord Gateway** (`discord:message` via `services/integrations/discord/gateway/`) | **NO — out of cutover scope** | The gateway worker has its own raw-tier write path (M4); it does NOT consult `kafka_path_enabled`. | Same as flag=TRUE — the gateway always writes to the raw tier. |
| **Gmail** (Pub/Sub via `services/webhooks/gmail_pubsub.py`) | **NO — M6 territory** | Gmail does not enter via this webhook router. The M6 backfill cutover is a separate work-unit. | Same as flag=TRUE — Gmail's ingress is not yet integrated with the cutover flag. |
| **Linear** | N/A — not in the four-source family (LLD §1) | N/A | Inline `ingest()`. No cutover path. |
| **Stripe** | N/A — not in the four-source family | N/A | Inline `ingest()`. No cutover path. |

**Why Discord webhook cutover is deferred:** Discord interactions
(slash commands) require a synchronous response with a specific
shape (`{"type": 4, "data": {"content": "..."}}` —
`CHANNEL_MESSAGE_WITH_SOURCE`) within ~3 seconds, or the Discord
client UI displays "The application didn't respond in time." The
M5.3 202-with-empty-body contract doesn't fit that shape. Resolving
this needs the Discord-response-shape question answered: either
synthesize the required response shape inside the 202 path, or keep
Discord interactions on the inline path indefinitely. See
[05-lld-amendments.md](05-lld-amendments.md) entry A7 for the open
amendment. The router enforces this by membership in
`_CUTOVER_ENABLED_PROVIDERS` = {slack, github} at
[services/webhooks/router.py:79](../../services/webhooks/router.py#L79).

**Why Gmail is deferred to M6:** Gmail enters the data plane via
Google Pub/Sub (the watch + history-poller pattern), not the webhook
router. The Pub/Sub endpoint
[services/webhooks/gmail_pubsub.py](../../services/webhooks/gmail_pubsub.py)
calls `shadow_write_raw` directly but does not consult
`kafka_path_enabled`. M6's backfill rollout brings Gmail under the
cutover flag at the same time the rest of the Gmail Temporal pipeline
lands.

**Operational implication.** Flipping the flag for a tenant whose
traffic is mostly Discord-Gateway-derived produces no behavioural
change at the router level. The traffic_signal sampling
([traffic_signal.py](../../services/ingestion/feature_flags/traffic_signal.py))
also only fires on the slack/github cutover path today — the
breaker's "active tenants" set is therefore biased toward those
providers until M6 lands.

---

## 3. Operator procedures

### 3a. Enable cutover for one tenant

```sql
INSERT INTO tenant_flags
    (tenant_id, flag_name, flag_value, set_by, note, set_at)
VALUES
    ('<tenant_uuid>', 'ingestion.kafka_path_enabled', TRUE,
     'operator:<your-id>',
     'enabling cutover per ramp tier <N>',
     now())
ON CONFLICT (tenant_id, flag_name) DO UPDATE SET
    flag_value = EXCLUDED.flag_value,
    set_by     = EXCLUDED.set_by,
    note       = EXCLUDED.note,
    set_at     = now();
```

**Propagation latency: ≤ 30 s** (the per-process flag cache TTL).
Webhooks arriving in the window between the UPDATE and the next
cache refresh continue on inline; the dedup mechanisms (S3
PutIfAbsent on content_hash + observations UNIQUE on
`(source_channel, external_id, occurred_at)`) catch any double
delivery. The N1-during-cutover safety property is verified by
`test_double_ingestion_safe_during_cutover` at
[services/webhooks/tests/test_router_m5_cutover.py](../../services/webhooks/tests/test_router_m5_cutover.py).

**Verify the flip propagated:**

```sql
SELECT flag_value, set_by, set_at
  FROM tenant_flags
 WHERE tenant_id = '<tenant_uuid>'
   AND flag_name = 'ingestion.kafka_path_enabled';
```

```bash
# Tail the router metric counter — should start incrementing
# webhook_router_kafka_path_total{provider="slack", outcome="success"}
# within ~30 s of the flip.
kubectl logs -l app=fyralis-gateway -f | grep -i 'router.kafka_path'
```

### 3b. Disable cutover for one tenant (operator-driven rollback)

```sql
UPDATE tenant_flags
   SET flag_value = FALSE,
       set_by     = 'operator:<your-id>',
       note       = '<reason>',
       set_at     = now()
 WHERE tenant_id = '<tenant_uuid>'
   AND flag_name = 'ingestion.kafka_path_enabled';
```

Within 30 s, the webhook router resumes the inline path. In-flight
envelopes already in the Kafka pipeline drain via the writer's full
mode (which dedups against any inline-side double-write per N1).

### 3c. Operator-driven re-enable after a circuit-breaker trip

The breaker flips the flag to FALSE when a tenant's partition lag
breaches threshold for 5 consecutive ticks. Recovery requires
operator intervention — **the breaker NEVER auto-re-enables a
tenant.** Rationale: auto-recovery during an incident produces
flapping (broker briefly recovers → breaker re-enables → broker
re-fails).

**Step 1 — Investigate the underlying lag.** The breaker is the
symptom; find the root cause first.

```sql
-- Tripped tenants and when they tripped.
SELECT tenant_id, consecutive_breach_ticks, tripped, tripped_at, last_tick_at
  FROM circuit_breaker_state
 WHERE tripped = TRUE
 ORDER BY tripped_at DESC;
```

Check the normalizer + writer consumer-group lag on
`ingestion.raw` / `ingestion.normalized` (see m2-shadow-path-runbook
§4). Common root causes: broker outage, normalizer/writer pod
crash-loop, DB connection exhaustion at the writer.

**Step 2 — Manual re-enable (single UPDATE).** Once the underlying
lag is back below threshold:

```sql
UPDATE tenant_flags
   SET flag_value = TRUE,
       set_by     = 'operator:<your-id>',
       note       = 'manual re-enable after breaker trip on <date>',
       set_at     = now()
 WHERE tenant_id = '<tenant_uuid>'
   AND flag_name = 'ingestion.kafka_path_enabled';
```

**That is the entire operator action.** The breaker auto-resets its
bookkeeping on the next tick: it observes `kafka_path_enabled=TRUE`
on a tenant whose `circuit_breaker_state.tripped=TRUE` row exists,
and resets the row (`tripped=FALSE`, `consecutive_breach_ticks=0`,
`tripped_at=NULL`). This is auto-reset of **breaker bookkeeping**,
NOT auto-recovery of the **flag**. See
[circuit_breaker.py:367-393](../../services/ingestion/feature_flags/circuit_breaker.py#L367-L393)
and the test
`test_breaker_resets_bookkeeping_on_operator_reenable` at
[test_circuit_breaker.py](../../services/ingestion/feature_flags/tests/test_circuit_breaker.py).

**Important — do NOT manually clear `circuit_breaker_state`.** The
auto-reset behaviour was introduced in M5.1's gap-closure
specifically to remove this footgun: a manual flag flip without a
state-row cleanup used to leave the breaker permanently blind to
the tenant. The current behaviour treats the flag flip as the
single operator action.

**Step 3 — Verify re-enable propagated** (same as §3a's verify step).

### 3d. Global rollback (all tenants → inline)

Operator-driven rollback of the entire cutover. Use only if a
systemic issue is observed across multiple tenants (rare; per-tenant
rollback is the primary recovery path).

```sql
UPDATE tenant_flags
   SET flag_value = FALSE,
       set_by     = 'operator:<your-id>',
       note       = 'global rollback: <incident-id>',
       set_at     = now()
 WHERE flag_name = 'ingestion.kafka_path_enabled'
   AND flag_value = TRUE;
```

**Propagation latency: ≤ 30 s.** In-flight Kafka envelopes still
drain via the writer's full mode (the writer reads the flag per
envelope and may produce double-writes that the UNIQUE constraint
catches). The dedup invariants hold during the transition window.

---

## 4. Monitoring + alerts

### 4a. `webhook_router_kafka_path_total{provider, outcome}` — cutover smoke detector

Defined at
[services/webhooks/metrics.py](../../services/webhooks/metrics.py).

**Labels:**
- `provider` ∈ {slack, github} (the cutover-enabled set).
- `outcome` ∈ {success, fallback}.

**Semantics:**
- `success` — flag was TRUE, Kafka publish succeeded, router
  returned 202. **Expected normal-state increment** for any tenant
  in the cutover ramp.
- `fallback` — flag was TRUE, Kafka publish failed (S3 timeout,
  Kafka leader unavailable, missing deps); router fell back to
  inline `ingest()` and returned 200/201. **Graceful degradation:
  customers see no error.** Sustained increment is the smoke
  signal.

**Alert thresholds (operator-side):**

| Threshold | Severity | Action |
|---|---|---|
| Any `fallback` increment in last 5 min | **Info** | Log inspection: which provider, which tenant. One-off failures are normal (broker rebalance, transient S3). |
| `>10 fallback / min sustained for >5 min` | **Warn** | Page on-call. Investigate raw-tier health (S3, Kafka leader, broker disk). The cutover path is degraded but customers are unaffected. |
| `>50 fallback / min sustained for >5 min` | **Page** | Same investigation; consider §3d global rollback if root cause not findable within 15 min. |

**Why these thresholds.** Cutover-attempt volume per cutover-enabled
tenant scales with webhook traffic. The thresholds assume ≤ a few
hundred cutover-enabled tenants in the production ramp; recalibrate
during the M-Load dry run.

The `fallback` increment is NEVER a customer-facing error — the
inline path returns 200/201. The metric exists so an operator can
detect cutover-path degradation BEFORE customers experience a
secondary effect (e.g., the writer-tier observation latency
becoming worse than the inline path's).

### 4b. `circuit_breaker.tripped` event — per-tenant rollback primary signal

Emitted by
[circuit_breaker.py::_default_alert](../../services/ingestion/feature_flags/circuit_breaker.py)
when the breaker trips a tenant. Default sink: structlog WARNING
at logger `services.ingestion.feature_flags.circuit_breaker`. In
production, replace `_default_alert` with the real ops channel
(PagerDuty / Slack webhook) at service-startup time.

**Event payload:**

```json
{
  "tenant_id": "<uuid>",
  "partition": <int>,
  "lag_seconds": <float>,
  "threshold_seconds": 60,
  "window_ticks": 5,
  "tripped_at": "<iso-8601>"
}
```

**Response procedure: §3c above.** This is the primary operator
signal for per-tenant rollback; treat it as a page even if the
underlying broker lag is brief. The breaker fires only after 5
consecutive minutes of breach, so the trip is intentionally lagging
indicators — once it fires, the customer's Kafka path has been
degraded for ≥ 5 minutes already.

**Verifying the trip flipped the flag:**

```sql
SELECT cbs.tenant_id,
       cbs.tripped, cbs.tripped_at, cbs.consecutive_breach_ticks,
       tf.flag_value AS kafka_path_enabled, tf.set_by, tf.set_at
  FROM circuit_breaker_state cbs
  LEFT JOIN tenant_flags tf
    ON tf.tenant_id = cbs.tenant_id
   AND tf.flag_name = 'ingestion.kafka_path_enabled'
 WHERE cbs.tripped = TRUE;
```

`flag_value` should be FALSE and `set_by` should be
`auto:circuit_breaker`. If either is otherwise, the trip didn't
land — investigate the flag-flip side of the trip path
([circuit_breaker.py::_process_tick](../../services/ingestion/feature_flags/circuit_breaker.py))
for an exception during `tenant_flags.set_bool`.

### 4c. Consumer-group lag on `ingestion.normalized`

The breaker's input. Measure with the same procedure as the M2
shadow-path runbook §4 (`kafka-consumer-groups.sh --describe`).

Per-partition lag > 60 s for any partition carrying cutover-tenant
traffic is the breaker's trigger condition. If lag is high but the
breaker hasn't fired, the breaker may be deferred: in M5.4 the
production Kafka readers raise `NotImplementedError` until
M-Temporal wires the real implementations. See §6 below.

---

## 5. Failure modes (catalog)

| Symptom | Likely cause | Diagnosis | Recovery |
|---|---|---|---|
| Customer sees 5xx on `/webhooks/slack/*` | Cutover bug: 5xx should NEVER surface — fallback is graceful | Application logs at `webhooks.router.kafka_path_failed` | Operator §3b: flip flag to FALSE for the affected tenant. File bug. |
| `webhook_router_kafka_path_total{outcome="fallback"}` sustained increment | Raw-tier degradation (S3 timeout, Kafka leader unavailable) | m2-shadow-path-runbook §4–5 | If broker is recoverable: wait. If not: §3d global rollback. |
| `circuit_breaker.tripped` event for tenant X | Sustained lag on tenant X's partition | §3c step 1 — investigate the underlying lag | §3c step 2 — flip flag back to TRUE after lag clears. |
| Observation count divergence between inline and full-mode tenants | `ingest_from_draft` divergence (N1 cutover-safety violation) | Run `test_writer_observations_match_inline_for_same_input` at [test_observation_writer_m5.py](../../services/ingestion/writers/tests/test_observation_writer_m5.py) | Page on-call; file P0; consider §3d global rollback. |
| Breaker doesn't fire despite sustained lag | Production Kafka readers default to `NotImplementedError` (M5.4 deferral); breaker is shipping the state-machine logic only until M-Temporal injects real readers | §6 below | M-Temporal is the dependency; until it lands, monitor lag manually via `kafka-consumer-groups.sh`. |
| Tripped tenant doesn't recover after flag flip back to TRUE | Cache TTL not yet elapsed (≤ 30 s) — wait | `SELECT flag_value FROM tenant_flags WHERE tenant_id=$1` | If still FALSE after 30 s, check `circuit_breaker_state` for the bookkeeping reset; check breaker logs for the `bookkeeping_reset_on_operator_reenable` event. |

---

## 6. Deferrals (what M5.4 does NOT close)

Tracked here so the next operator (or the M-Load / M-Temporal /
M6 owner) inherits the open work.

1. **Production execution.** Code complete, gates documented, tests
   green. Actual cutover happens when customers exist + M-Load dry
   run completes + M-Temporal wires the breaker's Kafka readers.

2. **M-Temporal.** New planned work-unit. Scope: stand up Temporal
   infrastructure (per LLD §11.2 + plan §3.5); port the circuit
   breaker from the asyncio service it ships as today to a Temporal
   Schedule; inject real `_measure_kafka_lag_default` +
   `_sample_active_tenants_default` implementations (today both
   raise `NotImplementedError` — fail-loud is intentional). Must
   land before M6 because TenantOnboardingWorkflow + ShardFetchWorkflow
   require Temporal.

3. **M-Load.** New planned work-unit. Scope: synthetic-traffic dry
   run against staging at production-equivalent volume to validate
   the cutover's behaviour under realistic conditions before the
   first real tenant is enabled.

4. **Discord webhook cutover.** Suppressed at the router level via
   `_CUTOVER_ENABLED_PROVIDERS` = {slack, github}. See §2 +
   [05-lld-amendments.md](05-lld-amendments.md) entry A7.

5. **Partition stand-in (`_kafka_partition_for_tenant`).** The
   traffic-signal hook uses a blake2b-based deterministic hash
   instead of librdkafka's murmur2_random. Operationally inert
   until the breaker is wired against real Kafka; M-Temporal must
   refine via on_delivery correlation. See
   [05-lld-amendments.md](05-lld-amendments.md) entry A8.

6. **Mode B collapse.** Per Finding 4 (M5 Phase 0), the writer
   ships single-mode under per-envelope `ingest_from_draft` calls.
   Mode B (max_poll_records=1 split) collapses to a no-op until a
   Q4 product call restores meaningful differentiation. See
   [05-lld-amendments.md](05-lld-amendments.md) entry A10 + plan
   §6 Q4.

---

## 7. References

- **Code:**
  - [circuit_breaker.py](../../services/ingestion/feature_flags/circuit_breaker.py)
    — the breaker (state machine, persistence, alerts).
  - [client.py](../../services/ingestion/feature_flags/client.py) —
    `TenantFlags.get_bool` / `set_bool` (cache + DB).
  - [traffic_signal.py](../../services/ingestion/feature_flags/traffic_signal.py)
    — 1% deterministic-hash sampler.
  - [observation_writer.py](../../services/ingestion/writers/observation_writer.py)
    — writer full mode + `make_writer_pool`.
  - [router.py](../../services/webhooks/router.py) — webhook router
    flag-branch.
  - [metrics.py](../../services/webhooks/metrics.py) — the
    cutover-path metric module.

- **Tests:**
  - [test_circuit_breaker.py](../../services/ingestion/feature_flags/tests/test_circuit_breaker.py)
    — 9 tests including the subprocess SIGTERM-survival test.
  - [test_observation_writer_m5.py](../../services/ingestion/writers/tests/test_observation_writer_m5.py)
    — 7 tests including the load-bearing parity test.
  - [test_router_m5_cutover.py](../../services/webhooks/tests/test_router_m5_cutover.py)
    — 6 tests including the load-bearing double-ingestion-safe
    test.

- **Schemas:**
  - `tenant_flags` — created in migration 0050 (M1).
  - `circuit_breaker_state` — created in migration 0053 (M5.1).

- **Plan + amendments:**
  - [04-implementation-plan.md §M5](04-implementation-plan.md#m5--steady-state-cutover-the-riskiest-milestone).
  - [05-lld-amendments.md](05-lld-amendments.md) — A7 / A8 / A9 /
    A10 capture the four M5-surfaced deferrals.

- **Adjacent runbooks:**
  - [m2-shadow-path-runbook.md](m2-shadow-path-runbook.md) — the
    raw tier this cutover writes into.
  - [m3-embedding-runbook.md](m3-embedding-runbook.md) — the
    embedding pipeline downstream of the writer.
  - [m4-gateway-runbook.md](m4-gateway-runbook.md) — the Discord
    Gateway worker (sibling ingress; not affected by cutover today).
