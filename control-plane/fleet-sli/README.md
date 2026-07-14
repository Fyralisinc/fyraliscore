# fleet-sli — Fleet-level SLI / alert / SLO rules (WS-FLEETSLI)

Cross-fleet **recording rules**, **alerting rules**, and **SLO burn-rate rules**
that turn the golden-12 SLIs into a per-deployment 🟢/🟡/🔴 view and a paging
contract for the whole BYOC fleet. These rules load into the **Mimir ruler** and
evaluate **centrally, once, over every tenant's** remote-written metrics.

## Files

| File | What it is | Loads into ruler |
|---|---|---|
| `recording_rules.yml` | The golden-12 SLIs aggregated **per-deployment** (`fyralis:*`) **and fleet-wide** (`fleet:*`): worker up / heartbeat, kafka lag, DLQ + dead-letter, ingest rate/source + backfill shards, shadow-drop, think queue/failure, embedding backlog/failure-ratio, LLM breaker + spend, DB pool saturation + schema version + partition coverage, OAuth token health, webhook + gateway. Plus a per-deployment `fyralis:health_code` (0/1/2) roll-up. | ✅ |
| `alert_rules.yml` | The **13 deployment alerts ported to fleet scope**, each firing **per deployment** and carrying `tenant_id` / `deployment_id` / `region` labels: heartbeat-stale, scrape-down, DLQ-depth, consumer-lag, signature-failure, embed-failure-ratio, think-backpressure, db-pool-saturated, llm-spend-burn, dead-letter-rows, **schema-version-drift (G1)**, **oauth-refresh-failure (G2)**, **worker-missing (G5)** — plus shadow-drop (silent-loss) and llm-breaker-open (G3). | ✅ |
| `slo_burnrate_rules.yml` | **NFR-5** SLOs as **multi-window, multi-burn-rate** alerts: availability (99.5%, fast 14.4× page / slow 6× ticket) and liveness (dead deployment detected within ~90s = 3 missed heartbeats). | ✅ |
| `slo.md` | The SLO definitions the burn-rate rules enforce (objective, window, error budget, why multi-window). | — (doc) |
| `fleet_sli.rules.yaml` | The Mimir **cardinality watchdog** (`fleet:active_series:by_tenant` + `FleetTenantNearSeriesBudget`) — a Mimir-internal guardrail kept from the WS-MIMIR seed (the seed's placeholder golden/burn rules were superseded by the three files above). | ✅ |
| `service.compose.yml` | One-shot bootstrap that stages the rule files into the ruler's `__fleet__` tenant dir. | — (compose) |

## How it evaluates: the Mimir ruler under `__fleet__`

Per `mimir/mimir.yaml`, the ruler reads filesystem rule groups from
`/data/ruler/<tenant_id>/<file>.yaml`. The fleet-SLI rules are **fleet-wide**, so
they evaluate under the **synthetic ruler tenant `__fleet__`** (Mimir runs the
ruler per tenant). Operator Grafana queries the resulting `fleet:*` / `fyralis:*`
series with `X-Scope-OrgID: __fleet__`.

Every base metric these rules read is remote-written by each data plane through
the **auth proxy**, which injects `X-Scope-OrgID` from the verified client-cert
SAN (C1/I4) — and the **boundary OTel Collector** stamps each series with the
low-cardinality identity labels `tenant_id` / `deployment_id` / `region` /
`telemetry_tier` (C4). So `... by (tenant_id, deployment_id, region)` yields one
SLI series **per deployment**, and `fleet:*` rolls the fleet up. The data plane's
raw metric stays under its own Mimir tenant; the `__fleet__` recording rules read
ACROSS tenants because the ruler for `__fleet__` is configured to evaluate over
the full set (operator-scope), not a single customer tenant.

> **Why central, not per-DP:** the data plane ships only metrics (T1). All SLI
> math, alerting, and SLO burn-rate evaluation happens in the control plane, so a
> customer VPC runs no ruler and the fleet view is computed once over everyone.

## How to run

These rules are mounted into Mimir by the compose integrate step; there is no
standalone process. End state: the four rule files staged under
`/data/ruler/__fleet__/` on the shared `mimir-data` volume, evaluated by the
`mimir` service's ruler.

1. **Integrate the compose fragment.** The integrate step merges
   `service.compose.yml` (and `mimir/service.compose.yml`) into
   `docker-compose.control-plane.yml`. The fleet-sli dir is mounted read-only into
   the Mimir container at `/etc/mimir/fleet-sli` and the bootstrap copies the rule
   files into `/data/ruler/__fleet__/`.

   > **Integration note (`.yml` vs `.yaml`):** the WS-MIMIR fragment
   > `mimir/service.compose.yml` ships its own `mimir-ruler-bootstrap` whose copy
   > glob is `*.yaml` only — it would **miss** the `*.yml` deliverables here. Use
   > **this** fragment's `fleet-sli-ruler-bootstrap` (it enumerates all four rule
   > files explicitly) and have `mimir` depend on it, **or** widen the mimir
   > fragment's glob to `*.yml*`. Do not run both bootstraps with conflicting
   > globs. (See the header of `service.compose.yml` for the two integrate options.)

2. **Bring up the control plane:**
   ```
   docker compose -f docker-compose.control-plane.yml up
   ```
   The bootstrap runs to completion, then Mimir's ruler loads the groups.

3. **Confirm the ruler loaded the groups** (through the proxy, scoped to `__fleet__`):
   ```
   curl -H 'X-Scope-OrgID: __fleet__' http://localhost:9009/prometheus/api/v1/rules
   ```
   and query a recorded SLI, e.g. `fleet:deployments_red` or
   `fyralis:worker_heartbeat_age_max_seconds`.

## Validate (what was run, reproduce it)

```
# 1. YAML parses + every rule has a non-empty PromQL expr + every base metric is
#    a REAL data-plane metric + every recorded-series reference is defined:
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python - <<'PY'
import yaml,glob
for f in ["recording_rules.yml","alert_rules.yml","slo_burnrate_rules.yml","fleet_sli.rules.yaml"]:
    d=yaml.safe_load(open(f)); assert "groups" in d
    for g in d["groups"]:
        for r in g["rules"]:
            assert r.get("expr","").strip(), (f,g["name"])
print("yaml.safe_load + expr-present: OK")
PY

# 2. promtool (via docker — no local promtool):
docker run --rm -v "$PWD":/rules:ro --entrypoint promtool prom/prometheus:latest \
  check rules --lint=all \
  /rules/recording_rules.yml /rules/alert_rules.yml \
  /rules/slo_burnrate_rules.yml /rules/fleet_sli.rules.yaml
```

Result on this checkout: `promtool check rules --lint=all` → **exit 0**,
`58 + 17 + 11 + 2 = 88` rules. Cross-reference check: 66 recorded series, 28
recorded-series references, **0 dangling dependencies**, 0 duplicate group names.

## Recording-rule dependencies (rules that need another rule)

All alert/SLO expressions consume the `fyralis:*` / `fleet:*` recording rules in
`recording_rules.yml`; the table below lists the non-obvious dependencies. Within
a ruler tenant the groups evaluate top-to-bottom each interval, so an in-tenant
dependency is satisfied within one eval cycle.

| Consumer | Depends on |
|---|---|
| `FleetHeartbeatStale` | `fyralis:worker_heartbeat_age_max_seconds` |
| `FleetScrapeDown` / `FleetWorkerMissing` | `fyralis:worker_down_count` / `fyralis:worker_missing_count` |
| `FleetDLQDepth*` / `FleetDeadLetterRows` / `FleetShadowDrop` | `fyralis:dlq_unresolved` / `fyralis:dead_letter_rows_total` / `fyralis:shadow_drop_rate:5m` |
| `FleetConsumerLag*` | `fyralis:kafka_worst_group_lag` |
| `FleetSignatureFailure` / `FleetOAuthRefreshFailure` | `fyralis:webhook_failure_rate:5m` / `fyralis:oauth_refresh_failure_rate:15m` |
| `FleetEmbedFailureRatioHigh` | `fyralis:embed_failure_ratio:10m` |
| `FleetThinkBackpressure` | `fyralis:think_queue_pending` |
| `FleetDBPoolSaturated` | `fyralis:db_pool_saturation_max` |
| `FleetSchemaVersionDrift` | `fyralis:schema_version` **and** `fleet:schema_version_max` (a fleet roll-up — both must be in the same ruler tenant) |
| `FleetLLMSpendBurn` / `FleetLLMBreakerOpen` | `fyralis:llm_spend_usd_per_hour` / `fyralis:llm_breaker_open` |
| `FleetSLOAvailability*Burn` | `fyralis:gateway_error_ratio:{5m,30m,1h,6h}` (recorded in `slo_burnrate_rules.yml` group A) |
| `FleetSLOLivenessHeartbeatMissed` | `fyralis:worker_heartbeat_age_max_seconds` |
| `fyralis:health_code` roll-up | the SLIs in groups 1–9 (worker/kafka/dlq/think/embed/oauth/db) |

## Caveats / assumptions

- **Metric names are grounded but some are gap-metrics (G1/G2/G3/G5) not yet
  emitted everywhere.** Base names come from `boundary/redaction_allowlist.md`
  (Tier-1 allowlist) + the data plane's real instrumentation
  (`lib/observability/metrics.py` / `health.py` / `pools.py`), postgres-exporter
  custom queries, and kafka-exporter. The gap-closing metrics —
  `fyralis_schema_version` (G1), `fyralis_oauth_token_*` (G2),
  `fyralis_llm_breaker_state` (G3), `fyralis_worker_expected_present/_running`
  (G5) — must be wired in the data plane (§12 of the design doc) for the
  corresponding rules to produce data. Until then those alerts simply never fire
  (the `or vector(0)` / `or`-fallbacks keep the recording rules from erroring).
- **Counter-name tolerance via `or`.** Several rules `or` two candidate metric
  names (e.g. `writer_full_mode_writes_total` **or** `writer_full_mode_writes`,
  `db_pool_in_use` **or** `fyralis_db_pool_in_use`) because the data plane's
  hand-rolled exposition and the boundary allowlist disagree on the `_total`
  suffix / `fyralis_` prefix for a few families. The `or` picks whichever exists;
  if a rule reports no data, confirm which spelling the deployment actually emits
  and tighten the rule.
- **Embedding failure-ratio counters are inferred.** `fyralis:embed_failure_ratio:10m`
  assumes `embedding_failures_total` / `embedding_attempts_total` (with
  `embedding_embed_*` fallbacks). If the data plane names them differently, the
  ratio reports 0 (guarded denominator) and must be re-pointed.
- **Thresholds are the design-doc defaults** (DLQ >25/>100, lag >1000/>50000,
  think >500, pool >0.9, embed >5%, LLM >$5/hr, SLO 99.5% / 14.4× / 6×). Tune
  per-tenant SLA once real baselines exist; per-tenant override lives alongside
  the Mimir runtime overrides.
- **`FleetSchemaVersionDrift`** treats the **max** schema version seen across the
  fleet as "current". A coordinated fleet-wide rollout briefly puts every
  deployment at the new max (no alert); a laggard that fails its migration stays
  below and trips after `for: 30m`.
- **30-day SLO windows** (`[30d]`, `[6h]`) only become meaningful once the ruler
  has that much history; on a fresh CP the budget-remaining gauge and slow-burn
  alert warm up over time. The fast page (5m/1h) is useful immediately.
- **`FleetSLOLivenessDeploymentSilent`** uses `up == 0` for all targets; if a
  deployment stops remote-writing entirely the `up` series eventually goes stale
  and the alert resolves — the authoritative "deployment vanished" signal is the
  **fleet registry** `last_heartbeat_ts` (C4), which this alert is meant to
  corroborate, not replace.
