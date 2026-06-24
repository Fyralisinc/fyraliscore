# grafana — central operator Grafana (fleet + per-customer)  (P3)

The **operator-facing** Grafana for the Fyralis BYOC control plane. It queries the
central **Mimir** (metrics) and **Loki** (logs) and renders two surfaces:

| Dashboard | uid | What it shows |
|-----------|-----|---------------|
| **Fyralis Fleet — Overview** | `fyralis-fleet-overview` | Cross-fleet rollup: green/yellow/red deployment counts, worst heartbeat age, a **deployments table** (one row per deployment, health-colored), and the **golden-12 fleet panels**. Reads the `Mimir (fleet)` datasource (cross-tenant reader). |
| **Fyralis Per-Customer — Drill-down** | `fyralis-tenant-drilldown` | Single-customer view, **templated by the `tenant_scope` variable**. The selected tenant is ALSO the `X-Scope-OrgID` value, so every panel is hard-scoped to one customer. Golden-12 per-tenant panels + a Loki logs panel (T2+ tenants only). |

---

## The contract you must understand: TWO X-Scope-OrgID paths

Mimir/Loki are multi-tenant; the tenant key is the **`X-Scope-OrgID`** header (C5).
There are **two completely separate paths** that set it, and they must not be
confused:

```
 AGENT INGEST PATH  (untrusted edge — PUSH)
   data-plane agent ──mTLS client cert──► auth-proxy:8443 ──► Mimir/Loki
       auth-proxy terminates mTLS, reads tenant_id from the VERIFIED
       client-cert SPIFFE SAN (C1), STRIPS any caller X-Scope-OrgID, and
       INJECTS X-Scope-OrgID:<tenant_id>. Identity is server-side from the
       cert — NEVER from a header. (Invariant I4.)

 OPERATOR QUERY PATH  (trusted, internal — this directory)
   Grafana ──HTTP + X-Scope-OrgID header──► mimir:9009 / loki:3100  (DIRECT)
       Grafana sets X-Scope-OrgID ITSELF, per datasource. The operator side
       has NO per-tenant client cert, so these datasources DO NOT go through
       the mTLS auth-proxy. They reach Mimir/Loki directly over cp-net.
       Mimir/Loki trust this header ONLY because it arrives from inside cp-net
       behind the network boundary — exactly the same reason the proxy's
       injected header is trusted (C5).
```

**Why Grafana doesn't go through the auth-proxy:** the proxy's whole job is to
derive a tenant from a *client certificate*. The operator console is a single
trusted service, not a per-tenant agent, and holds no per-tenant cert. Routing
operator queries through the mTLS proxy would force one cert per tenant on the
operator — wrong model. Instead Grafana is trusted *by virtue of being inside
cp-net* and sets the scope header itself. The trust boundary for the query path
is the **network** (`cp-net`); the trust boundary for the ingest path is the
**mTLS cert**.

### Per-customer scoping (templated)

The per-customer datasources set:

```yaml
jsonData:    { httpHeaderName1: X-Scope-OrgID }
secureJsonData: { httpHeaderValue1: ${tenant_scope} }
```

`${tenant_scope}` is the dashboard template variable on the per-customer
dashboard (a query variable populated from `label_values(..., tenant_id)`).
Selecting a customer in the dropdown changes the header value, so the same panels
re-scope to that single tenant's data. One dashboard, every customer.

### Fleet / admin scoping (cross-tenant)

The `Mimir (fleet)` / `Loki (fleet)` datasources set `X-Scope-OrgID` to the
**fleet/admin org id `__fleet__`** (override via `FYRALIS_FLEET_ORG_ID`). This is
the cross-tenant reader the Fleet Overview uses. It relies on **Mimir
tenant-federation** (`tenant_federation.enabled: true` in `mimir/mimir.yaml`):
a query for the `__fleet__` org reads across all tenants. If your Mimir does not
enable federation, point `__fleet__` at a real admin tenant, or set
`FYRALIS_FLEET_ORG_ID` to a bar-joined tenant list (e.g. `acme|globex`).

---

## Health rollup (green / yellow / red)

Per the C4 deployment record, `health ∈ green | yellow | red`. The Fleet
Overview derives it **at query time** from the telemetry actually landing in
Mimir (no separate health metric is required), per deployment:

| Health | Condition |
|--------|-----------|
| 🟢 **green**  | freshest `worker_heartbeat_age_seconds` ≤ **90s** AND no SLI flag tripped (`fyralis_dlq_unresolved` ≤ 100, `fyralis_llm_breaker_state` < 1) |
| 🟡 **yellow** | freshest heartbeat in **(90s, 300s]** — stale but reporting / degraded |
| 🔴 **red**    | freshest heartbeat **> 300s** (going dark) OR a hard SLI flag: LLM breaker open, or DLQ flooded (> 100) |

The deployments table emits a numeric `Health` (0/1/2) mapped to
GREEN/YELLOW/RED color-background cells, sortable so the worst deployments float
to the top. The thresholds (90s / 300s, DLQ 100) are encoded in the panel
queries and table field thresholds — tune them there or migrate them to
`fleet-sli/` recording rules.

---

## Golden-12 panels & the metric catalog

Panels query the **exact metric names that survive the boundary redaction
allowlist** (`boundary/redaction_allowlist.md`, the I1 artifact) — these are the
only families that ever reach Mimir, each stamped with the C4 identity labels
`tenant_id`, `deployment_id`, `region`, `telemetry_tier`:

| SLI | Metric(s) | Panel |
|-----|-----------|-------|
| 1  | `up`, `worker_heartbeat_age_seconds`, `worker_uptime_seconds` | Worker liveness / heartbeat |
| 2  | `kafka_consumergroup_lag`, `normalizer_consumer_lag_seconds`, `breaker_trips_total` | Kafka lag |
| 3  | `fyralis_dlq_unresolved`, `fyralis_dead_letter_rows` | DLQ depth |
| 5  | `fyralis_onboarding_shards` (by `status`) | Backfill shards |
| 7/8| `fyralis_think_queue_pending`, `think_runs_total\|failed\|latency` | Reasoning |
| 9  | `fyralis_embedding_backlog_pending` | Embedding backlog |
| 10 (G3) | `fyralis_llm_breaker_state` (by `provider`) | LLM breaker |
| 11 (G1) | `fyralis_schema_version` | Schema integrity |
| 12 (G2) | `fyralis_oauth_token_refresh_failures_total`, `webhook_verification_failures_total`, `webhook_resolver_outcomes_total` | Auth / ingress |

---

## How to run

### Validate (no Docker needed)

```bash
cd control-plane
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python grafana/validate.py
```

It `yaml.safe_load`s the datasource + dashboard-provider provisioning, `json.load`s
every dashboard, and **asserts `X-Scope-OrgID` is configured on every datasource**
(templated for per-customer, fleet org id for fleet), that the per-customer
dashboard declares `tenant_scope`, that dashboards only reference provisioned
datasource uids, and that the compose fragment exposes `:3000` on `cp-net`,
`depends_on` mimir+loki, and mounts provisioning + dashboards. Exit 0 = all held.

### Bring up (merged with the master compose)

This service ships as a **standalone fragment** (`service.compose.yml`); the
integrate step merges it — do not edit `docker-compose.control-plane.yml`:

```bash
cd control-plane
docker compose \
  -f docker-compose.control-plane.yml \
  -f mimir/service.compose.yml \
  -f loki/service.compose.yml \
  -f grafana/service.compose.yml \
  up -d grafana
# open http://localhost:3000  (admin / fyralis-operator — CHANGE in prod)
```

Grafana on boot provisions the 4 datasources + 2 dashboards; default home is the
Fleet Overview.

---

## Caveats

- **Templated datasource header has a scope.** `${tenant_scope}` resolves only on
  dashboards that define the `tenant_scope` variable (the per-customer
  dashboard). In a raw **Explore** session against the per-customer `Mimir`
  datasource the variable is unset and Grafana sends the literal
  `${tenant_scope}` — Mimir rejects it. **Use the `Mimir (fleet)` / `Loki
  (fleet)` datasource for ad-hoc Explore**, or open the per-customer dashboard.
- **`access: proxy`, not browser.** Datasources use server-side (`proxy`) access
  so the `X-Scope-OrgID` header is attached by the Grafana backend inside cp-net
  and never exposed to the browser. Do **not** switch to `direct`/browser access —
  that would leak the scope header to the client and break the trust model.
- **Fleet view assumes Mimir tenant-federation.** The `__fleet__` cross-tenant
  read needs `tenant_federation.enabled: true` in `mimir/mimir.yaml`. Owned by
  the `mimir/` agent; if absent, override `FYRALIS_FLEET_ORG_ID` (see above).
- **Loki panels are empty for T1 tenants.** Tier T1 is metrics-only (C3); logs
  exist only for tenants opted up to T2+. Empty log results are expected.
- **Health thresholds live in the panels.** 90s / 300s / DLQ-100 are encoded in
  the dashboard queries, not in recording rules. When `fleet-sli/` lands burn-rate
  recording rules, prefer reading a precomputed health series and keep these as a
  fallback.
- **`worker_heartbeat_age_seconds` is per-worker, not per-deployment.** The
  rollup takes `max by (deployment_id)` as the deployment's "freshest" age, i.e.
  a deployment is only as healthy as its *most stale* worker for the green gate.
  There is no single per-deployment heartbeat metric in the allowlist; if one is
  added later (e.g. `deployment_heartbeat_age_seconds`), swap it into the health
  expressions for a cleaner signal.
- **No alerting rules here.** `provisioning/alerting/` is created but empty —
  fleet burn-rate alerts are the `fleet-sli/` agent's deliverable (P3). Grafana
  has `manageAlerts: false` on the Mimir datasource so it won't fight Mimir's
  ruler.
- **Default admin creds are demo-only.** `admin / fyralis-operator` — set
  `GF_ADMIN_USER` / `GF_ADMIN_PASSWORD` (or wire SSO) before any real deployment.
- **`depends_on` is start-order only.** Grafana may boot before Mimir/Loki are
  query-ready; datasource health flaps until they are up. Grafana retries, so
  this is cosmetic on first boot.

---

## Files

| File | Role |
|------|------|
| `provisioning/datasources/datasources.yaml` | Mimir + Loki datasources (per-customer templated + fleet), each with the `X-Scope-OrgID` header. |
| `provisioning/dashboards/dashboards.yaml` | Two file-providers (Fleet folder, Per-Customer folder). |
| `dashboards/fleet/fleet-overview.json` | Fleet Overview: health rollup, deployments table, golden-12 fleet panels. |
| `dashboards/tenant/tenant-drilldown.json` | Per-customer drill-down, templated by `tenant_scope`. |
| `service.compose.yml` | Standalone grafana service fragment (`:3000` on cp-net, depends_on mimir/loki). |
| `validate.py` | Offline validator (yaml + json + header assertions). |
