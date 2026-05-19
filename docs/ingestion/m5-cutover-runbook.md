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

## 6.A. M6.1 services (OAuth poller + TenantOnboarding orchestrator) — operator section

**Scope.** M6.1 is the OAuth-callback-to-tenant-onboarding handoff. Two
long-running asyncio services compose the chain. Added to this runbook
because the on-call operator for the M5 cutover is the same person who
will operate M6.1 services in the post-cutover ramp, and the diagnostic
muscle memory (Postgres-state-as-checkpoint, structlog grep targets,
`workflow_states` introspection) carries over from M5.

The end-to-end-shaping test is
[test_oauth_to_tenant_completion_end_to_end.py](../../services/ingestion/workflows/tests/test_oauth_to_tenant_completion_end_to_end.py)
— it runs both services as real subprocesses and verifies the full
trigger-to-Bridge-signal chain. If this test ever fails in CI, the
operator-side procedures below cannot be trusted; treat M6.1 as not
shippable.

### 6.A.1. Architecture summary

```
+----------------------+    onboarding_run_   +-------------------------+
|  oauth_poller        |    created signal    | tenant_onboarding       |
|  (asyncio service)   |--------------------->|  orchestrator           |
|                      |  inbox=(tenant_      |  (asyncio service)      |
|  reads:              |    onboarding,       |                         |
|    onboarding_       |    tenant_           |  reads inbox:           |
|    triggers          |    onboarding)       |    (tenant_onboarding,  |
|  writes:             |                      |     tenant_onboarding)  |
|    onboarding_runs   |                      |  writes:                |
|    workflow_signals  |                      |    source_onboarding_   |
+----------------------+                      |    runs                 |
                                              |  emits:                 |
                                              |    source_onboarding_   |
                                              |    requested → M6.2     |
                                              |    tenant_onboarding_   |
                                              |    completed → Bridge   |
                                              +-------------------------+
```

Both services are LongRunningService subclasses (per
[runtime.py](../../services/ingestion/workflows/runtime.py)). Each tick
runs one Postgres transaction per work-item (one trigger for the
poller; one signal for the orchestrator). The transactional invariant
(claim + writes + signal-emit commit-or-rollback as a unit) is the
load-bearing M6.1 property.

### 6.A.2. Start procedures

The two services run as **independent processes**. Either can be the
first one started; until the orchestrator runs, signals accumulate in
its inbox harmlessly.

```bash
# Service 1: OAuth poller.
DATABASE_URL="postgres://..." \
  OAUTH_POLLER_TICK_SEC=5.0 \
  OAUTH_POLLER_BATCH=50 \
  OAUTH_POLLER_INSTANCE=prod-01 \
  WORKFLOWS_LOG_LEVEL=INFO \
  python -m services.ingestion.workflows.oauth_poller

# Service 2: TenantOnboarding orchestrator.
DATABASE_URL="postgres://..." \
  ORCHESTRATOR_TICK_SEC=10.0 \
  ORCHESTRATOR_BATCH=50 \
  ORCHESTRATOR_INSTANCE=prod-01 \
  WORKFLOWS_LOG_LEVEL=INFO \
  python -m services.ingestion.workflows.tenant_onboarding
```

The same env-var-driven dispatcher
[services/ingestion/workflows/__main__.py](../../services/ingestion/workflows/__main__.py)
also works: set `WORKFLOW_SERVICE=oauth_poller` or
`WORKFLOW_SERVICE=tenant_onboarding` and invoke
`python -m services.ingestion.workflows`. Either form is supported;
prefer the per-module CLI for production (one container image, one
entrypoint per service) and the dispatcher for tests / local
development.

**Env vars (poller):**

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres DSN. |
| `OAUTH_POLLER_TICK_SEC` | `5.0` | Tick interval. Lower = faster install-to-onboarding handoff (operator UX). Each tick processes up to `BATCH` triggers. |
| `OAUTH_POLLER_BATCH` | `50` | Max triggers per tick. Each trigger gets its own transaction; soft cap. |
| `OAUTH_POLLER_INSTANCE` | `default` | Instance name. Diagnostic only — written to `workflow_states.workflow_id`. Per-replica unique recommended. |
| `WORKFLOWS_LOG_LEVEL` | `INFO` | Standard. |

**Env vars (orchestrator):**

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres DSN. |
| `ORCHESTRATOR_TICK_SEC` | `10.0` | Tick interval. The orchestrator drains a batch of signals per tick; lower = faster fan-out + completion latency. |
| `ORCHESTRATOR_BATCH` | `50` | Max signals drained per tick. Each signal gets its own transaction. |
| `ORCHESTRATOR_INSTANCE` | `default` | Instance name. The same `(workflow_kind, workflow_id)` inbox is consumed by every replica regardless of `INSTANCE` — instance is for `workflow_states` audit only, NOT inbox sharding (per [05-lld-amendments.md A13](05-lld-amendments.md)). |
| `WORKFLOWS_LOG_LEVEL` | `INFO` | Standard. |

**Replication model.** Both services support multiple replicas safely
without coordination — the poller uses `FOR UPDATE SKIP LOCKED` on
`onboarding_triggers`; the orchestrator uses `FOR UPDATE SKIP LOCKED`
inside `claim_signals`. Replica count is a horizontal-scale knob; no
leader election is needed. The M6.1 stress test
`test_poller_and_orchestrator_run_concurrently_without_deadlock` in
[test_tenant_onboarding.py](../../services/ingestion/workflows/tests/test_tenant_onboarding.py)
verifies a 2-poller + 1-orchestrator topology drains 20 triggers
without deadlock.

### 6.A.3. Diagnostic queries (read-only)

Per-tick heartbeat:

```sql
-- Last tick from each service replica. Stale = service down or stuck.
SELECT workflow_kind, workflow_id AS instance,
       last_advanced_at,
       state_data ->> 'last_tick_at' AS last_tick_iso,
       state_data ->> 'lifetime_triggers_claimed' AS lifetime_triggers,
       state_data ->> 'lifetime_signals_processed' AS lifetime_signals
  FROM workflow_states
 WHERE workflow_kind IN ('oauth_poller', 'tenant_onboarding')
 ORDER BY workflow_kind, workflow_id;
```

Unconsumed triggers (poller backlog):

```sql
SELECT count(*) FILTER (WHERE consumed_at IS NULL)        AS pending,
       count(*) FILTER (WHERE consumed_at IS NOT NULL)    AS done,
       max(now() - created_at) FILTER (WHERE consumed_at IS NULL)
                                                          AS oldest_pending_age
  FROM onboarding_triggers;
```

Unconsumed signals in each inbox (orchestrator backlog + Bridge
backlog):

```sql
SELECT workflow_kind, workflow_id, signal_kind,
       count(*) FILTER (WHERE consumed_at IS NULL)  AS pending,
       max(now() - created_at) FILTER (WHERE consumed_at IS NULL)
                                                    AS oldest_pending_age
  FROM workflow_signals
 WHERE workflow_kind IN ('tenant_onboarding', 'source_onboarding', 'bridge')
 GROUP BY workflow_kind, workflow_id, signal_kind
 ORDER BY workflow_kind, signal_kind;
```

Per-tenant fan-out status (audit one tenant's full chain):

```sql
SELECT r.id AS run_id, r.status AS run_status, r.started_at, r.completed_at,
       sor.source, sor.status AS source_status, sor.completed_at AS source_completed_at,
       sor.failure_reason
  FROM onboarding_runs r
  LEFT JOIN source_onboarding_runs sor ON sor.onboarding_run_id = r.id
 WHERE r.tenant_id = '<tenant_uuid>'
 ORDER BY r.started_at DESC, sor.source;
```

### 6.A.4. Failure-mode catalog

| Symptom | Likely cause | Diagnosis | Recovery |
|---|---|---|---|
| **A. Trigger row stuck with `consumed_at IS NULL`** | Poller down OR poller crashed mid-transaction (txn rolled back, row remains claimable) | `workflow_states` heartbeat for `oauth_poller` is stale; check service logs at logger `services.ingestion.workflows.oauth_poller`. | Restart the poller service. Per the load-bearing rollback property (`test_oauth_poller_idempotent_across_restart`), the row will be re-claimed cleanly with no duplicate downstream effect. |
| **B. `onboarding_runs` row in `'failed'` status with `error_summary = 'No active installs for tenant at orchestrator tick-time.'`** | **Phase 2 Decision 3.** The trigger fired (OAuth callback completed), but by the time the orchestrator picked up the resulting `onboarding_run_created` signal, the tenant had zero active rows in either `provider_installations` (enabled=TRUE) or `gmail_installations` (disabled_at IS NULL). Race conditions: (a) install enabled→disabled flip between trigger-fire and tick; (b) the trigger row references an install that was deleted; (c) test fixture bug. | `SELECT * FROM provider_installations WHERE tenant_id=$1; SELECT * FROM gmail_installations WHERE tenant_id=$1;` — if both empty, the cause is real (no installs at tick-time). If one is non-empty but `enabled=FALSE`/`disabled_at IS NOT NULL`, the install was disabled between trigger and tick. | Investigate WHY the install is inactive. If legitimate (user uninstalled before onboarding completed), the failure is correct behaviour — no action needed. If accidental (test fixture bug, manual SQL flip), re-enable the install row and re-emit an `onboarding_run_created` signal manually (rare; document the manual UPDATE in an incident note). |
| **C. `onboarding_run_created` signal with `consumed_at IS NOT NULL` but NO `source_onboarding_runs` row exists** | Should be impossible per the orchestrator's per-signal atomic transaction. If observed, the transaction rollback contract was violated — orchestrator crashed AFTER the signal-mark-consumed but BEFORE the source-row insert was committed (which the substrate guarantees is impossible if `claim_signals(conn)` is in the same `conn.transaction()` block). | Diagnostic query: `SELECT consumed_at, consumed_by FROM workflow_signals WHERE signal_kind='onboarding_run_created' AND idempotency_key=$1;` cross-checked against `SELECT count(*) FROM source_onboarding_runs WHERE onboarding_run_id=$1;`. | Page on-call P0. The atomic transaction is broken — file a bug against the substrate. Manual recovery: re-emit the signal with a new idempotency_key OR insert the source rows by hand from the run row's `tenant_id`. |
| **D. `source_onboarding_requested` signal in inbox `(source_onboarding, source_onboarding)` with `consumed_at IS NULL` for > 5 min** | M6.2's SourceOnboarding service is down or has never been deployed (M6.2 is the next milestone after M6.1; until it ships, this signal will accumulate by design). | `SELECT workflow_kind, workflow_id, signal_kind, count(*) FROM workflow_signals WHERE consumed_at IS NULL GROUP BY 1,2,3;` — if `source_onboarding` is the only un-drained inbox, M6.2 is not running. | Pre-M6.2: this is **expected**. The `source_onboarding_requested` signals accumulate harmlessly until M6.2 ships. The orchestrator's `source_onboarding_runs` row stays in `pending`; the parent run stays in `running`; no observable to the user. Post-M6.2: investigate the M6.2 service health. |
| **E. Parent `onboarding_runs` row stuck in `'running'` status forever** | One or more `source_onboarding_runs` rows are still `pending`/`in_progress` AND no `source_onboarding_completed` signal has arrived for them. Two causes: (a) M6.2 hasn't shipped yet (see D above); (b) M6.2 shipped but a specific per-source backfill (M6.3-M6.6) is failing silently. | The fan-out audit query above (`6.A.3` per-tenant). If `source_status='pending'` for source X, check M6.3-M6.6's service-specific runbook (M6.4 GitHub fetcher, etc.). | Post-M6.2: investigate the specific source's service. If the source backfill is truly stuck, an operator can inject a manual `source_onboarding_completed` signal (with `failure_reason` set) to advance the parent run — but ONLY after confirming the source state is reconciled. Don't do this without an incident-review-grade reason. |
| **F. `onboarding_runs` row with `status='failed'` and `error_summary IS NULL`** | The orchestrator marked the run failed but the `_MARK_RUN_FAILED_SQL` path that supplies `error_summary` was NOT followed — should be impossible per current code paths, both of which (`_handle_run_created` zero-installs branch, `_handle_source_completed` failure branch) populate `error_summary`. If observed: orchestrator crashed AFTER a partial run-mark-failed UPDATE (rare; the txn rollback should prevent this). | `SELECT status, error_summary, completed_at FROM onboarding_runs WHERE id=$1;` | Page on-call P0. File a bug against the orchestrator's transaction discipline. The empty `error_summary` makes the failure mode invisible — investigate by cross-referencing the orchestrator's structured logs around `completed_at` for the run. |
| **G. `tenant_onboarding_completed` signal in Bridge inbox NOT consumed for > 30 min** | Bridge consumer not yet deployed (Bridge is out of M6.1 scope; the signal is the producer-side handoff). Until Bridge ships, these signals accumulate by design. | Same drain-query as D, filtered to `workflow_kind='bridge'`. | Pre-Bridge: this is **expected**. The signals are durable; Bridge will drain them on startup via the substrate's standard claim semantics. Post-Bridge: investigate Bridge's health. |

**Operational note on inbox-sentinel addressing (A13).** Every signal
in `workflow_signals` whose `workflow_kind='tenant_onboarding'` has
`workflow_id='tenant_onboarding'`. Per-run identity lives in
`idempotency_key` (carries the run_id) and `signal_data` (carries the
full payload). An operator filtering by `workflow_id='<some_run_id>'`
will find zero rows — that's not a missing row, that's correct
addressing. See
[05-lld-amendments.md A13](05-lld-amendments.md#a13--signal-addressing-is-a-routing-partition-key-not-a-workflow-instance-identifier)
for the rationale.

### 6.A.5. When-to-investigate (alert thresholds)

| Threshold | Severity | Action |
|---|---|---|
| `workflow_states.last_advanced_at` for either service older than `2 × tick_interval` | **Info** | Likely transient (pod restart, DB blip). Check next tick. |
| `workflow_states.last_advanced_at` older than `10 × tick_interval` | **Warn** | Page on-call. Service is stuck (deadlock, infinite loop, or dead). Investigate logs at the service's structlog logger. |
| `onboarding_triggers` pending count growing for > 10 min OR oldest pending > 5 min | **Warn** | Poller capacity or correctness issue. Could be a sudden trigger spike or a poller that's crashing on a poison-pill trigger row. Inspect logs. |
| Single `onboarding_runs` row in `'running'` for > 1 h | **Info** | Normal for slow per-source backfills (gmail history can take hours). If the source row count is also growing → fine. If `source_onboarding_runs` are stuck at `pending` → see failure-mode E. |
| `onboarding_runs.status='failed'` rate increases beyond baseline | **Warn** | Check `error_summary` clustering. Zero-installs failures (failure-mode B) might point to install-side flakiness; per-source failures point to M6.2-M6.6 service issues. |

### 6.A.6. Pre-M6.2 expected state

Until M6.2's SourceOnboarding service ships, the M6.1 chain
terminates at the `source_onboarding_requested` signal emit. The
expected steady-state observation:

- `onboarding_triggers` drains to zero (poller works).
- `onboarding_runs` rows exist with `status='running'` (orchestrator
  fanned out).
- `source_onboarding_runs` rows exist with `status='pending'`
  (orchestrator created them; M6.2 hasn't picked them up).
- `workflow_signals` with `workflow_kind='source_onboarding'` and
  `consumed_at IS NULL` accumulates by design.
- `workflow_signals` with `signal_kind='tenant_onboarding_completed'`
  is empty (no run can reach completion until M6.2 + completion
  signals exist).

This is NOT a degraded state — it's the M6.1 deliverable. The end-to-
end integration test
[test_oauth_to_tenant_completion_end_to_end.py](../../services/ingestion/workflows/tests/test_oauth_to_tenant_completion_end_to_end.py)
injects synthetic `source_onboarding_completed` signals to validate
the completion-roll-up path; in production, this path activates only
once M6.2 ships.

---

## 6.B. M6.2a services (SourceOnboarding + ShardFetch) — operator section

**Scope.** M6.2a is the per-source planner → fetcher chain. Two
long-running asyncio services compose it: SourceOnboarding (consumes
`source_onboarding_requested` from M6.1's TenantOnboarding; calls the
per-source planner; INSERTs `onboarding_shards` rows; emits
`shard_fetch_requested`) and ShardFetch (consumes
`shard_fetch_requested`; runs the per-page fetch loop under the N1
invariant; publishes records to `ingestion.raw`; emits
`shard_fetch_completed`). Same operator persona as §6.A; this section
extends the M6.1 runbook with M6.2a's two services.

The end-to-end-shaping test is
[test_oauth_to_source_completion_end_to_end.py](../../services/ingestion/workflows/tests/test_oauth_to_source_completion_end_to_end.py)
— four real subprocesses (oauth_poller + tenant_onboarding from
M6.1 + source_onboarding + shard_fetch from M6.2a) running the full
chain. If this test fails in CI, M6.2a is not shippable.

### 6.B.1. Architecture summary (the full M6 chain)

```
[1] OAuth callback        →  writes onboarding_triggers row.
[2] oauth_poller          →  emits onboarding_run_created.        (M6.1)
[3] tenant_onboarding     →  emits source_onboarding_requested.   (M6.1)
[4] source_onboarding     →  calls PLANNER_DISPATCH[source];      (M6.2a)
                             INSERTs onboarding_shards rows;
                             emits shard_fetch_requested per shard.
[5] shard_fetch           →  calls FETCHER_DISPATCH[source];      (M6.2a)
                             N1-advances cursor per page;
                             publishes records to ingestion.raw;
                             emits shard_fetch_completed.
[6] source_onboarding     →  consumes shard_fetch_completed;      (M6.2a)
                             rolls up to source_onboarding_runs;
                             emits source_onboarding_completed.
[7] tenant_onboarding     →  consumes source_onboarding_completed;(M6.1)
                             rolls up to onboarding_runs;
                             emits tenant_onboarding_completed.
[8] (Bridge consumer)     →  consumes tenant_onboarding_completed.(out of M6 scope)
```

### 6.B.2. Start procedures (the two new services)

```bash
# Service 4: SourceOnboarding.
DATABASE_URL="postgres://..." \
  SOURCE_ONBOARDING_TICK_SEC=5.0 \
  SOURCE_ONBOARDING_BATCH=50 \
  SOURCE_ONBOARDING_INSTANCE=prod-01 \
  WORKFLOWS_LOG_LEVEL=INFO \
  python -m services.ingestion.workflows.source_onboarding

# Service 5: ShardFetch.
DATABASE_URL="postgres://..." \
  KAFKA_BOOTSTRAP_SERVERS="broker-1:9092,broker-2:9092" \
  SHARD_FETCH_TICK_SEC=5.0 \
  SHARD_FETCH_BATCH=10 \
  SHARD_FETCH_LEASE_SEC=30.0 \
  SHARD_FETCH_FLUSH_SEC=5.0 \
  SHARD_FETCH_INSTANCE=prod-01 \
  WORKFLOWS_LOG_LEVEL=INFO \
  python -m services.ingestion.workflows.shard_fetch
```

Same env-var-driven dispatcher (`python -m services.ingestion.workflows`
with `WORKFLOW_SERVICE=...`) also recognizes both new services.

**Env vars (SourceOnboarding):**

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres DSN. |
| `SOURCE_ONBOARDING_TICK_SEC` | `5.0` | Tick interval. |
| `SOURCE_ONBOARDING_BATCH` | `50` | Max signals drained per tick. |
| `SOURCE_ONBOARDING_INSTANCE` | `default` | Diagnostic instance name (per-replica unique recommended). |
| `WORKFLOWS_LOG_LEVEL` | `INFO` | Standard. |

**Env vars (ShardFetch):**

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres DSN. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap for the `ingestion.raw` publisher. |
| `SHARD_FETCH_TICK_SEC` | `5.0` | Tick interval. Each tick drains signals AND scans for orphan in-progress shards. |
| `SHARD_FETCH_BATCH` | `10` | Max signals drained per tick. Each signal handler runs the FULL fetch loop for its shard — tick batch is small because per-shard fetch can take minutes. |
| `SHARD_FETCH_LEASE_SEC` | `30.0` | Orphan-scan lease timeout. A shard in `state='in_progress'` with no N1 advance for this many seconds is treated as orphan (previous owner crashed) and another replica picks it up. **Production tuning: must be longer than the slowest expected per-source fetcher call's natural latency.** |
| `SHARD_FETCH_FLUSH_SEC` | `5.0` | Kafka flush timeout per N1 advance. If exceeded, the advance raises `CursorAdvanceFlushFailure`; the shard stays `in_progress` and orphan-scan retries. |
| `SHARD_FETCH_INSTANCE` | `default` | Diagnostic instance name. |
| `WORKFLOWS_LOG_LEVEL` | `INFO` | Standard. |

**Replication model.** Both services support multiple replicas safely:
- SourceOnboarding uses `claim_signals` SKIP LOCKED on its inbox.
- ShardFetch uses TWO CLAIM-VIA-UPDATE mechanisms (signal-driven and orphan-scan; documented in `shard_fetch.py` module docstring). Concurrent replicas drain signals via SKIP LOCKED and refresh leases via UPDATE-with-state-guard; disjoint work guaranteed.

### 6.B.3. Diagnostic queries

Per-service heartbeat:

```sql
SELECT workflow_kind, workflow_id AS instance,
       last_advanced_at,
       state_data ->> 'last_tick_at'              AS last_tick_iso,
       state_data ->> 'lifetime_signals_processed' AS lifetime_signals,
       state_data ->> 'lifetime_orphans_resumed'   AS lifetime_orphans
  FROM workflow_states
 WHERE workflow_kind IN ('source_onboarding', 'shard_fetch')
   AND workflow_id NOT LIKE '0%'   -- exclude per-shard cursor rows
 ORDER BY workflow_kind, workflow_id;
```

Per-shard cursor state (the N1 home, keyed by `shard_id`):

```sql
SELECT s.id, s.source, s.shard_kind, s.state,
       ws.last_advanced_at,
       ws.state_data ->> 'pages_fetched'  AS pages_fetched,
       ws.state_data ->> 'end_of_data'    AS end_of_data,
       s.last_error
  FROM onboarding_shards s
  LEFT JOIN workflow_states ws
    ON ws.workflow_kind = 'shard_fetch'
   AND ws.workflow_id   = s.id::text
 WHERE s.onboarding_run_id = '<run_uuid>'
 ORDER BY s.created_at;
```

Backlog at each inbox:

```sql
SELECT workflow_kind, workflow_id, signal_kind,
       count(*) FILTER (WHERE consumed_at IS NULL) AS pending,
       max(now() - created_at) FILTER (WHERE consumed_at IS NULL)
                                                   AS oldest_pending_age
  FROM workflow_signals
 WHERE workflow_kind IN ('source_onboarding', 'shard_fetch',
                         'tenant_onboarding', 'bridge')
 GROUP BY 1, 2, 3
 ORDER BY workflow_kind, signal_kind;
```

### 6.B.4. Pre-M6.3 expected steady state (CRITICAL for operators)

**Until M6.3-M6.6 ship, EVERY shard fails with `NotImplementedError`.**
The per-source planner and fetcher dispatch tables ship with stub
entries for every source — `slack`, `github`, `discord`, `gmail` —
that raise `NotImplementedError` naming the responsible M6.x sub-block
(M6.3 = gmail, M6.4 = github, M6.5 = slack, M6.6 = discord).

**This is by design**, not a regression. The two failure modes
operators will see in production until M6.3-M6.6 ship:

1. **Planner stub fires** (M6.2a SourceOnboarding receives a
   `source_onboarding_requested` and immediately fails the run):
   - `source_onboarding_runs.status = 'failed'`.
   - `source_onboarding_runs.failure_reason` contains the M6.x reference.
   - `source_onboarding_completed` emitted to TenantOnboarding with
     failure status; parent `onboarding_runs.status = 'failed'`.
   - No shard rows created (planner raised before the INSERTs).

2. **Fetcher stub fires** (only happens if planners are real but fetchers
   aren't — won't occur in the pre-M6.3 state since both are stubbed
   for every source; this matters once partial implementations land):
   - `onboarding_shards.state = 'failed'` with `last_error` naming M6.x.
   - `shard_fetch_completed` emitted with status='failed' and
     `failure_reason`.
   - Source-onboarding-runs rollup marks the run failed with the
     rolled-up shard failures.

**Diagnostic query** to confirm a failure is the expected stub path
(not a real issue):

```sql
SELECT failure_reason
  FROM source_onboarding_runs
 WHERE status = 'failed'
   AND failure_reason ILIKE '%M6.%'
   AND created_at > now() - interval '1 hour';
```

If the failure_reason contains an M6.x reference (`M6.3` / `M6.4` /
`M6.5` / `M6.6`), it's the expected pre-implementation steady state.
If it's a different message (e.g., "No active install...", "shard
Y: timeout"), investigate per §6.B.5.

### 6.B.5. Failure-mode catalog

| Symptom | Likely cause | Diagnosis | Recovery |
|---|---|---|---|
| **A. `source_onboarding_runs.status='failed'` with `failure_reason` containing `M6.x`** | **Pre-M6.3 expected state** — planner stub. | Confirm M6.x reference in failure_reason per §6.B.4. | None. This is by design; landing M6.x's per-source planner replaces the stub. |
| **B. `source_onboarding_runs.status='failed'` with `failure_reason="No active install..."`** | A14 race: install was disabled between M6.1 TenantOnboarding's `source_onboarding_requested` emit and M6.2a SourceOnboarding's pickup. | `SELECT * FROM provider_installations WHERE tenant_id=$1 AND provider=$2;` / `SELECT * FROM gmail_installations WHERE tenant_id=$1;`. | If install legitimately disabled (user uninstalled), the failure is correct. If accidental, re-enable the install row and re-trigger the onboarding flow with a fresh `onboarding_triggers` row (the old `source_onboarding_runs` row stays failed — that's audit history). |
| **C. `onboarding_shards.state='in_progress'` with `workflow_states.last_advanced_at` very old** | ShardFetch crashed mid-fetch; orphan-scan should pick up. | `SELECT now() - last_advanced_at FROM workflow_states WHERE workflow_kind='shard_fetch' AND workflow_id=<shard_id>::text;` — compare to `SHARD_FETCH_LEASE_SEC`. | Wait for next tick of ANY ShardFetch replica. If shard still stuck after 2× lease timeout, check ShardFetch service health (logs, replica count). |
| **D. `onboarding_shards.state='failed'` with `last_error` containing M6.x** | Pre-M6.x fetcher stub. | Same as A; confirm M6.x reference. | None; M6.x's per-source fetcher fills in the stub. |
| **E. `onboarding_shards.state='failed'` with `last_error` NOT containing M6.x** | Real fetcher failure (rate limit, source API down, permission denied, etc.). | Inspect `last_error` for source-specific failure mode. Cross-reference with the M6.x service's runbook (M6.3 gmail, M6.4 github, etc.). | Per-source recovery procedure (often: wait for rate limit window, retry by re-triggering onboarding). Reconciler (M6.2b) will re-shard if gaps detected. |
| **F. `shard_fetch_requested` accumulating in shard_fetch inbox** | ShardFetch service down or backlogged. | `SELECT workflow_kind, workflow_id, count(*) FROM workflow_signals WHERE consumed_at IS NULL GROUP BY 1,2;` — confirm spike on `shard_fetch`. | Check ShardFetch service health + replicas. The N1 invariant means even if signals queue, no data is lost; service comes back online and drains. |
| **G. `shard_fetch_completed` accumulating in source_onboarding inbox** | SourceOnboarding service down or backlogged. | Same query, filtered to `source_onboarding`. | Check SourceOnboarding service health. Same no-data-loss property. |
| **H. CursorAdvanceFlushFailure exceptions in ShardFetch logs** | Kafka broker timeout. N1 invariant working as designed: cursor NOT advanced; shard stays `in_progress`; orphan-scan re-attempts. | Check Kafka broker health (per the M2 shadow-path runbook §4). | Wait for Kafka recovery. ShardFetch's orphan-scan auto-retries; no operator action needed unless broker is permanently down. |
| **I. Parent `onboarding_runs.status='running'` for hours** | One or more `source_onboarding_runs` not yet terminal. | Per-tenant audit query (§6.A.3). If a `source_onboarding_runs` row is stuck `in_progress`, drill into its shards. | Per cause: fetcher failure → wait/retry; service down → restart. |

### 6.B.6. When-to-investigate (alert thresholds)

| Threshold | Severity | Action |
|---|---|---|
| `workflow_states.last_advanced_at` for `source_onboarding` or `shard_fetch` (diagnostic) older than `2 × tick_interval` | **Info** | Likely transient (pod restart). Check next tick. |
| Per-shard `workflow_states.last_advanced_at` older than `2 × SHARD_FETCH_LEASE_SEC` | **Warn** | Orphan-scan should have picked up. If still stuck, the orphan-scan path is broken or no replica is running. |
| `shard_fetch_requested` backlog > 100 OR oldest pending > 10 min | **Warn** | ShardFetch capacity. Check replica count + per-fetcher latency. |
| Spike in `source_onboarding_runs.status='failed'` with non-M6.x reasons | **Warn** | Real failures (vs. expected stub path). Investigate `failure_reason` clustering. |
| Spike in `onboarding_shards.state='failed'` with non-M6.x `last_error` | **Warn** | Per-source fetcher issues. Cross-reference with source health (e.g., GitHub status page). |

### 6.B.7. Pre-Reconciler (pre-M6.2b) expected state

Until M6.2b's Reconciler ships, there is NO automatic re-share of
shards that completed with coverage gaps. M6.2a's chain marks a
parent run 'complete' as soon as all shards reach a terminal state
(done or failed). If a shard completed `done` but only fetched 80%
of the expected records (e.g., a Slack channel with deleted
messages that the test fetcher missed), M6.2a does NOT detect that.

M6.2b's Reconciler — the next M6.2 sub-block — will trigger on
`source_onboarding_completed`, query the source's
authoritative-count APIs (per-source; M6.3-M6.6 implement them),
detect gaps >0.1%, and INSERT new `onboarding_shards` rows with
`state='reconciliation_resharded'` + `parent_shard_id` set. The
new shards re-enter the fetch loop via `shard_fetch_requested`
emits (Reconciler is also a producer).

Operators reading M6.2a logs/metrics: "no gap detection" is the
expected pre-M6.2b state, not a regression. The M6.2a chain is
correct for the "complete-as-soon-as-shards-terminal" semantic;
reconciliation is a post-completion check.

---

## 6.C. M6.2b Reconciler — operator section

**Scope.** M6.2b is the at-completion reconciliation check. One
long-running asyncio service (Reconciler) intercepts the M6.2a
chain between SourceOnboarding's "all shards complete" rollup and
TenantOnboarding's source-completion handling. On CLEAN: stamps
`reconciled_at` + emits `source_onboarding_completed` to
TenantOnboarding. On RE-SHARE: increments
`reconciliation_pass_count`, transitions
`source_onboarding_runs.status` back to `'in_progress'`, marks
originals `'reconciliation_resharded'`, INSERTs new shards with
`parent_shard_id` linkage + boosted `recency_score`, emits
`shard_fetch_requested` per new shard. The cycle restarts.

The two end-to-end-shaping tests:
- [test_oauth_to_source_completion_end_to_end.py](../../services/ingestion/workflows/tests/test_oauth_to_source_completion_end_to_end.py)
  — clean-path 5-subprocess E2E (extended in M6.2b Phase 1 with
  the Reconciler subprocess; default-clean stub).
- [test_oauth_to_tenant_completion_with_reconciler_reshare.py](../../services/ingestion/workflows/tests/test_oauth_to_tenant_completion_with_reconciler_reshare.py)
  — re-share-path 5-subprocess E2E; monkeypatched test
  reconciler that returns `has_gaps=True` on pass_0 then `has_gaps=False`
  on pass_1. Verifies the cycle + cross-service idempotency.

### 6.C.1. Start procedure

```bash
# Service 6: Reconciler.
DATABASE_URL="postgres://..." \
  RECONCILER_TICK_SEC=5.0 \
  RECONCILER_BATCH=50 \
  RECONCILER_INSTANCE=prod-01 \
  WORKFLOWS_LOG_LEVEL=INFO \
  python -m services.ingestion.workflows.reconciler
```

Also reachable via the dispatcher (`WORKFLOW_SERVICE=reconciler`).

**Env vars:**

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | Postgres DSN. |
| `RECONCILER_TICK_SEC` | `5.0` | Tick interval. |
| `RECONCILER_BATCH` | `50` | Max signals drained per tick. |
| `RECONCILER_INSTANCE` | `default` | Diagnostic instance name. |
| `WORKFLOWS_LOG_LEVEL` | `INFO` | Standard. |

**Replication model.** Multi-replica safe via `claim_signals`
SKIP LOCKED on the `(reconciler, reconciler)` inbox. The per-run
transaction owns idempotency.

### 6.C.2. The state machine (operator mental model)

Per [05-lld-amendments.md A17](05-lld-amendments.md#a17--reconciler-state-machine-idempotency-key-discipline-and-re-share-recency-boost):

```
source_onboarding_runs.status:

  'pending' → 'in_progress' → 'completed'
                                  ↓
                              (Reconciler)
                                  ↓
            ┌─ 'completed' + reconciled_at = now()    ← TERMINAL (clean)
            │
            └─ 'in_progress' + pass_count++           ← RE-SHARE CYCLE
                    ↓
              new shards fetch
                    ↓
              'completed' (new emit)
                    ↓
              (Reconciler again — clean or another reshare)
```

The TRANSIENT state worth monitoring:
`status='completed' AND reconciled_at IS NULL`. A row in this state
is between SourceOnboarding's rollup and the Reconciler's pickup.
Normal: <1 minute. Investigate per §6.C.5 if longer.

### 6.C.3. Diagnostic queries

Reconciler heartbeat:

```sql
SELECT workflow_kind, workflow_id AS instance,
       last_advanced_at,
       state_data ->> 'last_tick_at'              AS last_tick_iso,
       state_data ->> 'lifetime_signals_processed' AS lifetime
  FROM workflow_states
 WHERE workflow_kind = 'reconciler';
```

Runs awaiting reconciliation (uses migration 0056's index):

```sql
SELECT onboarding_run_id, source, tenant_id, completed_at,
       now() - completed_at AS waiting_for
  FROM source_onboarding_runs
 WHERE status = 'completed' AND reconciled_at IS NULL
 ORDER BY completed_at;
```

Re-share cycle history (per-run audit):

```sql
SELECT onboarding_run_id, source, status, reconciliation_pass_count,
       reconciled_at, completed_at
  FROM source_onboarding_runs
 WHERE tenant_id = '<tenant_uuid>'
 ORDER BY created_at DESC;
```

Re-shared shards for a run:

```sql
SELECT id, state, parent_shard_id, recency_score, shard_identifier
  FROM onboarding_shards
 WHERE onboarding_run_id = '<run_uuid>'
   AND parent_shard_id IS NOT NULL
 ORDER BY created_at;
```

Reconciler inbox backlog:

```sql
SELECT signal_kind, idempotency_key,
       now() - created_at AS pending_age
  FROM workflow_signals
 WHERE workflow_kind = 'reconciler'
   AND consumed_at IS NULL
 ORDER BY created_at;
```

### 6.C.4. Pre-M6.3-M6.6 expected steady state (CRITICAL)

**Until M6.3-M6.6 ship, every `RECONCILER_DISPATCH[source]` returns
`has_gaps=False`** — the default-clean stub. The re-share path
exists in code but is exercised ONLY by tests.

**Why the stubs return clean (not raise NotImplementedError):**
unlike M6.2a's planner/fetcher stubs (which raise so the system
fails loudly), reconcilers MUST return a valid decision because the
system needs to function pre-M6.3-M6.6. If reconcilers raised, no
tenant onboarding would ever reach `tenant_onboarding_completed` and
the entire M6 chain would be unusable. Default-clean keeps the
chain moving while the per-source M6.x sub-blocks land.

**Diagnostic: distinguishing stub-default from real-clean.**

Once M6.3-M6.6 ship per-source reconcilers, operators will need to
tell apart:
- *"clean because the stub defaults to clean"* (pre-M6.x state)
- *"clean because the real algorithm found no gaps"* (post-M6.x;
  the desired production state)

The simplest signal: the stub messages name the responsible
M6.x sub-block. Pre-M6.x runs have `RECONCILER_DISPATCH[source]`'s
stub `message` containing `"M6.3"` / `"M6.4"` / `"M6.5"` / `"M6.6"`.
That message currently isn't persisted to a DB column (the
Reconciler stores it transiently via the decision object). For ops
visibility, scan service logs for the stub message — OR (future
work) add an optional `last_reconciler_message TEXT` column on
`source_onboarding_runs` to persist the stub-vs-real distinction.

```bash
# Pre-M6.x stub matched on log:
kubectl logs reconciler-prod-01 | grep -E "M6\.[3-6].*defaulting to clean"
```

If the M6.x sub-blocks have shipped and stub messages still appear,
the dispatch table override didn't land — check the per-source
service's import-time registration into `RECONCILER_DISPATCH`.

### 6.C.5. Failure-mode catalog

| Symptom | Likely cause | Diagnosis | Recovery |
|---|---|---|---|
| **A. `source_shards_completed` accumulating in reconciler inbox** | Reconciler service down or backlogged. | `SELECT count(*) FROM workflow_signals WHERE workflow_kind='reconciler' AND consumed_at IS NULL;`. | Restart Reconciler. The N1-style at-most-once + at-least-once semantics mean no data loss; signals drain on service recovery. |
| **B. `source_onboarding_runs.status='completed' AND reconciled_at IS NULL` for > 5 min** | Reconciler hasn't picked up. Either down (see A) OR the `source_shards_completed` signal never landed (a deeper M6.2a chain bug). | Compare reconciler-inbox backlog with `awaiting_reconcile_idx` count. If backlog has the row, A applies. If not, investigate SourceOnboarding's rollup-emit. | Per cause: restart reconciler, or escalate the SourceOnboarding-emit bug. |
| **C. `onboarding_shards.state='reconciliation_resharded'` for an original WITHOUT any new shards referencing it** | Reconciler crashed mid-transaction between marking the original and inserting the new shards. **Should be impossible per the A12 atomicity contract** — if observed, file a P0 against the Reconciler's transaction discipline. | `SELECT s.id FROM onboarding_shards s WHERE s.state='reconciliation_resharded' AND NOT EXISTS (SELECT 1 FROM onboarding_shards c WHERE c.parent_shard_id=s.id);` | Manual recovery: insert a new shard for the abandoned-original's gap region, OR revert the original to `state='done'`. Cause investigation: the atomic-rollback test should catch this; if the test passes but production fails, suspect connection-level transaction handling. |
| **D. Re-share cycle that does NOT terminate (more than 3 passes for one run)** | Per-source reconciler bug — algorithm declares `has_gaps=True` repeatedly without converging. | `SELECT reconciliation_pass_count, source, onboarding_run_id FROM source_onboarding_runs ORDER BY reconciliation_pass_count DESC LIMIT 10;`. | Investigate the per-source algorithm (M6.3-M6.6 territory). M6.2b ships no per-source convergence guard; the per-source impl owns this. **Alert threshold: pass_count >= 3 is the investigation trigger.** |
| **E. `source_onboarding_completed` count > 1 for the same (run, source)** | Cross-service idempotency broken — the Reconciler's emit key should be `{run_id}:{source}` (no pass_count). If duplicates exist, the key shape changed. | `SELECT idempotency_key, count(*) FROM workflow_signals WHERE workflow_kind='tenant_onboarding' AND signal_kind='source_onboarding_completed' GROUP BY 1 HAVING count(*)>1;`. | P0 against the Reconciler's emit. Cross-reference [A17](05-lld-amendments.md#a17--reconciler-state-machine-idempotency-key-discipline-and-re-share-recency-boost) section (2) for the load-bearing key-shape rationale. |
| **F. Reconciler tick stuck (lifetime_signals_processed not incrementing)** | Possible deadlock — concurrent replicas spinning on lock contention; OR the dispatch function hung. | Inspect `pg_stat_activity` for blocked queries; check Reconciler logs for "RECONCILER_DISPATCH" execution time. | If dispatch hung, the per-source algorithm has a bug. Pre-M6.3-M6.6 the dispatch is the stub (returns immediately), so this is unlikely. |

### 6.C.6. When-to-investigate (alert thresholds)

| Threshold | Severity | Action |
|---|---|---|
| `awaiting_reconcile_idx` count > 0 with `completed_at < now() - interval '1 min'` | **Info** | Likely transient (Reconciler tick). Check next tick. |
| Same as above with `completed_at < now() - interval '5 min'` | **Warn** | Reconciler likely down or backlogged. Check service health + reconciler inbox. |
| Any row with `reconciliation_pass_count >= 3` | **Warn** | Per-source algorithm not converging. M6.3-M6.6 investigation. |
| Reconciler `workflow_states.last_advanced_at` older than `2 × tick_interval` | **Info** | Likely transient. Check next tick. |
| Same as above older than `10 × tick_interval` | **Warn** | Service stuck or dead. |
| ANY duplicate `source_onboarding_completed` for the same (run, source) | **Page** | Cross-service idempotency broken — load-bearing invariant. See [A17 section 2](05-lld-amendments.md#a17--reconciler-state-machine-idempotency-key-discipline-and-re-share-recency-boost). |

---

## 6.D. M6.3 Gmail backfill — operator section

The M6.3 sub-block ships Gmail-specific entries in the three M6 dispatch tables (planner, fetcher, reconciler) as **net-new code** using `users.messages.list` for backfill. The existing steady-state path (`services/integrations/gmail/{fetcher,history_poller,watch_scheduler,push_handler}.py` writing to `observations` via inline handler dispatch) is **unmodified**. Both paths coexist; the operator must understand the two-path framing.

### 6.D.1. Two-path coexistence (CRITICAL)

After M6.3 merges, two Gmail ingestion paths run in parallel:

| Path | Trigger | Code | Output |
|---|---|---|---|
| **Backfill (M6.3, new)** | `onboarding_triggers` row → M6 framework chain | `services/ingestion/planners/gmail.py`, `fetchers/gmail.py`, `reconcilers/gmail.py` | Kafka `ingestion.raw` → normalizer → observation writer |
| **Steady-state (existing)** | Pub/Sub push to `/webhooks/gmail/pubsub` or 10-min poller | `services/integrations/gmail/push_handler.py` → `fetcher.py::drain_mailbox_history` | Inline `dispatch_gmail_message_resource` → `observations` directly |

The watermark handoff at install-time (backfill's final `historyId` → steady-state's starting point) is **not yet implemented**; it's covered by the deferred ticket "Gmail inline-ingestion retirement" (M7 territory). Until that ticket lands, the two paths share `gmail_mailbox_watches.history_id` as the steady-state watermark; backfill stamps its own `final_history_id` in `workflow_states.state_data["cursor"]` and does NOT advance `gmail_mailbox_watches.history_id`.

### 6.D.2. Pre-F4-ticket expected steady state (CRITICAL)

**The M6.3 plumbing is INERT in production today.** No production code writes `onboarding_triggers` rows for Gmail. The OAuth callback at [oauth.py:_provision_install](../../services/integrations/gmail/oauth.py) creates `gmail_installations` + `gmail_mailbox_watches` but does NOT write an `onboarding_triggers` row. Without that row, `oauth_poller` has nothing to claim, and the M6 chain never starts. This is by design — the F4 retrofit (deferred ticket "OAuth callbacks → onboarding_triggers") closes the gap before first real-customer cutover.

In the pre-F4 steady state, **all Gmail ingestion goes through the existing push/poll path**. Operator queries on the backfill surfaces will show:
- `SELECT count(*) FROM onboarding_triggers WHERE source = 'gmail'` → 0 (or test-only rows).
- `SELECT count(*) FROM source_onboarding_runs WHERE source = 'gmail'` → 0.
- The M6 services run idle (their workflow_states rows update each tick but `last_signals_processed = 0`).

This is NOT a regression. It is the expected pre-F4-retrofit state.

### 6.D.3. Start procedure (post-F4)

Once F4 ticket lands, Gmail backfill runs alongside the other four services. The five M6 services boot via:

```sh
WORKFLOW_SERVICE=oauth_poller         python -m services.ingestion.workflows
WORKFLOW_SERVICE=tenant_onboarding    python -m services.ingestion.workflows
WORKFLOW_SERVICE=source_onboarding    python -m services.ingestion.workflows
WORKFLOW_SERVICE=shard_fetch          python -m services.ingestion.workflows
WORKFLOW_SERVICE=reconciler           python -m services.ingestion.workflows
```

No Gmail-specific service. The dispatch-table wire-in via `services/ingestion/{planners,fetchers,reconcilers}/__init__.py` registers Gmail at process start. The Reconciler's `__main__.py` boot path calls `services.ingestion.reconcilers.gmail.set_pool_provider(pool)` so the Gmail reconciler can read `workflow_states` for each shard's `final_history_id`. (Same wire-up exists in `services/ingestion/workflows/reconciler.py::_run_service` for the per-service CLI path used by subprocess tests.)

### 6.D.4. Diagnostic queries

**Per-mailbox cursor (where backfill landed):**
```sql
SELECT s.id AS shard_id,
       s.shard_identifier->>'mailbox_email' AS mailbox,
       ws.state_data->'cursor'->>'final_history_id' AS final_history_id,
       ws.state_data->'cursor'->>'page_token' AS page_token,
       ws.state_data->>'pages_fetched' AS pages_fetched,
       s.state
  FROM onboarding_shards s
  LEFT JOIN workflow_states ws
    ON ws.workflow_kind = 'shard_fetch' AND ws.workflow_id = s.id::text
 WHERE s.source = 'gmail'
   AND s.shard_kind = 'gmail_mailbox_window'
 ORDER BY s.created_at DESC LIMIT 50;
```

**Gap-fill shards (reshare-cycle audit):**
```sql
SELECT id, parent_shard_id, state,
       shard_identifier->>'start_history_id' AS start_hid,
       shard_identifier->>'end_history_id' AS end_hid,
       recency_score, last_error
  FROM onboarding_shards
 WHERE shard_kind = 'gmail_history_gap'
 ORDER BY created_at DESC LIMIT 50;
```

**Tenants where Gmail has cycled multiple times (potential algorithm issue):**
```sql
SELECT tenant_id, onboarding_run_id, reconciliation_pass_count,
       status, reconciled_at, failure_reason
  FROM source_onboarding_runs
 WHERE source = 'gmail' AND reconciliation_pass_count >= 2
 ORDER BY reconciliation_pass_count DESC, completed_at DESC LIMIT 30;
```

### 6.D.5. Gmail-specific failure modes

| Symptom | Likely cause | Diagnostic step | Remediation |
|---|---|---|---|
| Shards stuck `in_progress`; `pages_fetched` not advancing | Gmail API quota exhaustion (429s). Retry backoff is in effect; eventual progress expected. | Check `gmail.fetcher.get_message_failed` log volume; check service logs for `retry_with_backoff_on_429`. | Wait for quota reset; consider per-tenant rate-limit tuning. |
| Shard `failed` with `GoogleApiError: status=401` | DWD bearer token expired or revoked. | `kubectl logs` on the shard_fetch pod; check `services.integrations.gmail.dwd` minter status. | Re-run DWD provisioning per [oauth.py](../../services/integrations/gmail/oauth.py). |
| Shard `failed` with `historyId out of range` | History-API horizon (~7 days) exceeded. Steady-state path's `gmail_mailbox_watches.history_id` is older than Gmail's retention window. | Compare `final_history_id` in cursor vs `getProfile`'s response horizon. | Treat as full re-backfill via `users.messages.list` (initial-backfill shard); skip the gap-fill path for that mailbox. Algorithm refinement is future work. |
| `gmail_history_gap` shard endlessly re-spawning (`reconciliation_pass_count` keeps growing) | Mailbox receiving live mail at higher rate than reconciler can catch up. Backfill never converges. | `SELECT reconciliation_pass_count FROM source_onboarding_runs WHERE source='gmail'` shows monotonic increase. | Expected for very-active mailboxes; per-source algorithm should add a convergence cap. M6.3 algorithm runs as-is; refinement is per-source-policy. |
| `_pool_provider not registered` RuntimeError in reconciler logs | Service started via a non-blessed entry point that didn't call `set_pool_provider`. | Check the reconciler subprocess invocation in deployment manifest. | Use `python -m services.ingestion.workflows` (with `WORKFLOW_SERVICE=reconciler`) OR `python -m services.ingestion.workflows.reconciler`. Both register the provider. |

### 6.D.6. When to investigate (alert thresholds)

| Threshold | Severity | Notes |
|---|---|---|
| Any Gmail shard `failed` with `historyId out of range` | **Warn** | History-horizon issue. Per-source algorithm correction. |
| Any duplicate `source_onboarding_completed` for the same Gmail (run, source) | **Page** | Cross-service idempotency broken (same A17 invariant). |
| `gmail_mailbox_watches` row with `state='errored'` and `consecutive_poll_failures >= 5` | **Info** | Existing steady-state path issue; M6.3 does not address. |
| `onboarding_triggers` for Gmail with `consume_attempts >= 3` and no `consumed_at` | **Warn** | F4 retrofit landing is exposing oauth_poller issues. |

### 6.D.7. Two-path coexistence resolution path

The deferred ticket "Gmail inline-ingestion retirement" (M7 territory) is the resolution. It converts `services/integrations/gmail/fetcher.py::drain_mailbox_history` to publish to Kafka `ingestion.raw` instead of dispatching inline through `services/ingestion/handlers/gmail.py`. After that change ships:
- Steady-state Gmail messages flow through the same Kafka topic as backfill.
- A single normalizer code path handles both backfill and steady-state.
- `gmail_mailbox_watches.history_id` becomes redundant with `workflow_states.state_data["cursor"]["final_history_id"]`; the steady-state path is rewritten to read/write from the M6 cursor home.

Operators do NOT need to plan for this transition in M6.3-era operations. The runbook will be re-released when the M7 ticket lands.

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
