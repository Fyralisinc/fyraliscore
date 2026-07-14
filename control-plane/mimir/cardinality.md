# WS-MIMIR-CARD — Per-Tenant Cardinality: Measure, Fit, Enforce

Mimir is a **shared, multi-tenant** metrics store. The single biggest operational
risk in a shared TSDB is **cardinality**: one tenant's runaway label set (a UUID
in a label, a per-request `path`, an unbounded `instance`) consumes ingester
memory and degrades **every** tenant. WS-MIMIR-CARD is the method we use to give
each tenant a **budget**, **measure** their fit against it, and **enforce** the
budget with backpressure when they exceed it.

Two orthogonal axes (do not conflate them):

- **Telemetry tier (C3 — T1/T2/T3)** gates *what signal classes* leave the
  customer VPC. It is enforced at the **boundary OTel Collector** inside the VPC.
- **Cardinality budget (this doc)** gates *how much metric cardinality* a tenant
  may store in Mimir. It is enforced **at the distributor/ingester** in the
  control plane. A T1 (metrics-only) tenant still has a series budget.

---

## 1. The budget (where it lives)

| Knob | Meaning | Default (global) | Per-tenant |
|------|---------|------------------|------------|
| `max_global_series_per_user` | **THE budget**: active series per tenant, cluster-wide | `150000` | `runtime_overrides.yaml` |
| `max_global_series_per_metric` | active series for any single metric name | `20000` | yes |
| `ingestion_rate` | sustained samples/sec a tenant may push | `25000` | yes |
| `ingestion_burst_size` | token-bucket burst allowance | `50000` | yes |
| `max_label_names_per_series` | labels per series (bounds per-series fan-out) | `30` | yes |

- **Global defaults** live in `mimir.yaml > limits`. They are the floor every
  tenant starts from.
- **Per-tenant overrides** live in `runtime_overrides.yaml > overrides > <tenant_id>`
  and are **hot-reloaded every 15s** — no restart, no data loss. The tenant key
  is the `X-Scope-OrgID` value, i.e. the verified `tenant_id` the auth-proxy
  injects from the client-cert SAN.

---

## 2. MEASURE — does the tenant fit its budget?

All measurement APIs require `X-Scope-OrgID: <tenant_id>` (multitenancy is ON).
Through the auth-proxy the header is injected from the cert; for local operator
debugging you can hit Mimir directly and set the header by hand.

### 2a. Current active series for a tenant (the headline number)

Mimir exports the per-tenant in-memory series gauge. Query it (as the `__fleet__`
operator tenant, since the metric is labeled by `user`):

```bash
# active series per tenant, right now
curl -s -H 'X-Scope-OrgID: __fleet__' \
  'http://mimir:9009/prometheus/api/v1/query' \
  --data-urlencode 'query=sum by (user) (cortex_ingester_memory_series)'
```

The recording rule `fleet:active_series:by_tenant` (in `fleet-sli/`) precomputes
exactly this so dashboards don't recompute it every refresh.

### 2b. Cardinality breakdown — WHICH label/metric is the culprit

Mimir ships a first-class **cardinality analysis API**. Run it *as the tenant*:

```bash
# the metric names with the most series for tenant `acme`
curl -s -H 'X-Scope-OrgID: acme' \
  'http://mimir:9009/prometheus/api/v1/cardinality/label_names'

# label VALUES with the most series (find the high-cardinality label)
curl -s -H 'X-Scope-OrgID: acme' \
  'http://mimir:9009/prometheus/api/v1/cardinality/label_values?label_names[]=instance'

# per-metric active series for the tenant
curl -s -H 'X-Scope-OrgID: acme' \
  'http://mimir:9009/prometheus/api/v1/cardinality/active_series'
```

These tell you **which metric** and **which label** to fix at the source (drop
the label in the data-plane scrape relabel config, or aggregate it away).

### 2c. Fit ratio (used by the watchdog alert)

```
fit_ratio(tenant) = fleet:active_series:by_tenant{user="<tenant>"} / <its budget>
```

- `< 0.8`  → healthy.
- `0.8–1.0` → **near budget** — the `FleetTenantNearSeriesBudget` alert fires;
  decide: raise the override (tenant is legitimately bigger) or shed cardinality.
- `>= 1.0` → at/over budget — **new series are rejected** (see §3).

---

## 3. ENFORCE / BACKPRESSURE — what happens at the budget edge

Enforcement is automatic at the distributor. It is **graceful**: existing series
keep flowing; only the *excess* is shed.

### 3a. Series budget exceeded (`max_global_series_per_user`)

When a tenant's active series would exceed its budget, Mimir **rejects the new
series** on `POST /api/v1/push` with **HTTP 4xx** and a body like
`per-user series limit ... exceeded`. Prometheus/agent remote-write treats this
as a **non-retriable** write error for those samples and surfaces it as
`prometheus_remote_storage_failed_samples_total` (or the agent's equivalent).
**Already-admitted series keep ingesting** — you lose only the new high-cardinality
series, not the tenant's whole feed.

Operator signal: `cortex_discarded_samples_total{reason="per_user_series_limit"}`
climbs for that tenant.

### 3b. Ingestion rate exceeded (`ingestion_rate` / `ingestion_burst_size`)

A token bucket. When a tenant pushes faster than its sustained rate (after
draining the burst), `POST /api/v1/push` returns **HTTP 429 Too Many Requests**.
Remote-write **retries with backoff** (429 is retriable), so this is true
*backpressure*: the sender slows down, the tenant's local agent buffers (I3 —
data plane survives), and no data is silently dropped as long as the overshoot
is transient. Sustained 429s mean the budget is genuinely too small → raise the
runtime override.

Operator signal: `cortex_discarded_samples_total{reason="rate_limited"}` and
HTTP 429 rate on the distributor.

### 3c. Per-series label fan-out exceeded (`max_label_names_per_series`)

A sample whose series carries more than the allowed label names is **rejected at
ingest** (`cortex_discarded_samples_total{reason="max_label_names_per_series"}`).
This catches the classic "someone added a `request_id` label" blowup at the door
rather than after it has multiplied series.

---

## 4. The operating loop (the WS-MIMIR-CARD method, end to end)

1. **Default budget** applies to a new tenant automatically (`mimir.yaml > limits`).
2. **Watchdog**: `FleetTenantNearSeriesBudget` (in `fleet-sli/`) fires at 80% of
   budget. Recording rule `fleet:active_series:by_tenant` feeds it.
3. **Diagnose** with the cardinality API (§2b): is the growth legitimate
   (bigger data plane) or a label blowup (bug)?
4. **Decide**:
   - *Legit growth* → raise that tenant's `max_global_series_per_user` (and
     usually `ingestion_rate`) in `runtime_overrides.yaml`. Save; it applies in
     ≤15s, **no restart**.
   - *Label blowup* → fix at the **source** (data-plane scrape relabeling / metric
     aggregation). Optionally tighten `max_label_names_per_series` for that tenant
     so the same mistake is rejected at the door next time.
5. **Enforcement is always on** underneath (§3) — even before an operator reacts,
   the budget protects the shared cluster: excess series are rejected, excess
   rate is 429-backpressured, never the whole tenant.

---

## 5. Quick reference — raise a tenant's budget

Edit `runtime_overrides.yaml`:

```yaml
overrides:
  acme:
    max_global_series_per_user: 500000   # was 150000 default
    ingestion_rate: 75000
    ingestion_burst_size: 150000
```

Save. Within 15s Mimir reloads it (no restart). Confirm:

```bash
curl -s -H 'X-Scope-OrgID: acme' \
  'http://mimir:9009/runtime_config?mode=diff'   # shows the applied override
```
