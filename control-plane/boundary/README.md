# WS-BOUNDARY — Boundary OTel Collector (Tier enforcement + metrics egress)

The **boundary** is the egress chokepoint that runs **inside the customer VPC**,
alongside the Fyralis data plane. It scrapes the data-plane metrics, enforces
**Invariant I1 (no PII/payload at Tier 1)**, stamps deployment identity, and
**remote-writes filtered metrics to central Mimir THROUGH the auth proxy**. It is
the customer-VPC half of Phase 2 (`SPRINT_PLAN.md` §P2; contract **C3**).

```
   customer VPC (data plane)                         vendor control plane
  ┌────────────────────────────┐                    ┌───────────────────────┐
  │ workers :9300  ┐           │   mTLS, OUTBOUND   │  auth proxy           │
  │ gateway :8000  ├─scrape─▶ boundary ─remote_write────▶ (verifies cert,   │
  │ exporters …    ┘  OTel Collector │  /api/v1/push  │  SAN→tenant→         │
  │                  (filter+redact+ │                │  X-Scope-OrgID) ─▶ Mimir
  │                   identity stamp)│                │                       │
  └────────────────────────────┘                    └───────────────────────┘
```

## Files

| File | Purpose |
|---|---|
| [`otel-collector-config.yaml`](./otel-collector-config.yaml) | The real, validated T1 collector config: prometheus receiver → `filter/allowlist` → `transform/redact-labels` → `resource` identity → `prometheusremotewrite` (to the proxy). |
| [`tier_policy.yaml`](./tier_policy.yaml) | T1 / T2 (+redacted logs, loki) / T3 (+sampled traces) as parameterized, commented increment blocks. A tier change is config-only. |
| [`redaction_allowlist.md`](./redaction_allowlist.md) | The auditable **I1 artifact**: the explicit metric-family keep-list + the label drop-list / enum allowlist. |
| [`dataplane_remote_write.md`](./dataplane_remote_write.md) | The **WS-REMOTEWRITE** alternative direct path (data-plane Prometheus → proxy, no collector). |
| [`prometheus_remote_write_overlay.yml`](./prometheus_remote_write_overlay.yml) | The `remote_write:` block + `write_relabel_configs` that reproduce I1 on the direct path. |
| [`selftest.py`](./selftest.py) | Validates everything (YAML, structure, redaction behavior, and a real `otelcol-contrib validate`). |

## How it enforces I1 (two gates)

1. **Gate 1 — family allowlist** (`filter/allowlist`): default-deny. Only the
   golden-12 + G1–G7 fleet families survive (`up`, `worker_*`,
   `fyralis_schema_version`, `fyralis_oauth_token*`, `fyralis_llm_breaker*`,
   `fyralis_dlq_unresolved`, `fyralis_think_queue_pending`,
   `fyralis_embedding_backlog_pending`, kafka lag, writer/DLQ counters, …).
   Everything else is dropped at the boundary.
2. **Gate 2 — label drop** (`transform/redact-labels`): on surviving families,
   every id/email/url/free-text label is deleted; only bounded enums
   (`worker`/`job`/`source`/`provider`/`state`/`status`/`table`/`trigger_kind`/…)
   remain. This is a defense-in-depth backstop over the data plane's own
   cardinality discipline (`lib/observability/metrics.py` already forbids
   unbounded label values).

Identity (`tenant_id`, `deployment_id`, `region`, `telemetry_tier`) is added by
the `resource` processor from env (C4 keys). The **authoritative** tenant scoping
is `X-Scope-OrgID`, which the **auth proxy** injects from the verified client-cert
SAN (C1/I4) — **the collector never sets `X-Scope-OrgID`**.

## Run it

```bash
docker run --rm \
  -e FYRALIS_TENANT_ID=acme \
  -e FYRALIS_DEPLOYMENT_ID=acme-use1-7f3a \
  -e FYRALIS_REGION=us-east-1 \
  -e FYRALIS_TELEMETRY_TIER=T1 \
  -e FYRALIS_AUTH_PROXY_URL=https://auth-proxy.fyralis.example:8443 \
  -v $PWD/otel-collector-config.yaml:/etc/otelcol/config.yaml:ro \
  -v /etc/fyralis/ca:/etc/fyralis/ca:ro \
  -v /etc/fyralis/agent:/etc/fyralis/agent:ro \
  otel/opentelemetry-collector-contrib:0.103.1 \
  --config=/etc/otelcol/config.yaml
```

Required env:

| Var | Meaning |
|---|---|
| `FYRALIS_TENANT_ID` / `FYRALIS_DEPLOYMENT_ID` / `FYRALIS_REGION` | C4 identity stamped on every series |
| `FYRALIS_TELEMETRY_TIER` | `T1` (default) / `T2` / `T3` — must match the active pipeline set |
| `FYRALIS_AUTH_PROXY_URL` | HTTPS push base, e.g. `https://auth-proxy…:8443` (proxy fronts Mimir `/api/v1/push`, Loki `/loki/api/v1/push`) |
| `FYRALIS_AUTH_PROXY_GRPC` | (T3 only) OTLP/gRPC `host:port` for traces, e.g. `auth-proxy…:4317` |

mTLS cert material is mounted at `/etc/fyralis/ca/ca.crt` and
`/etc/fyralis/agent/client.{crt,key}` (the per-tenant client cert whose SAN is
`spiffe://fyralis/tenant/<id>`). The compose stanza in
`docker-compose.control-plane.yml` documents the single-host dev wiring (note:
it mounts `./boundary/config.yaml`; for that topology symlink/copy
`otel-collector-config.yaml` → `config.yaml`, or update the mount to the real
filename).

## Switch tiers (config-only, C3)

Tiers are **cumulative** and enforced **by what is wired** — a higher signal
class has no receiver/exporter unless its pipeline block is present, so it
**physically cannot egress**.

- **T1 → T2** (add redacted logs): set `FYRALIS_TELEMETRY_TIER=T2`; merge
  `t2_increment.{receivers,processors,exporters}` and the
  `t2_increment.service.pipelines.logs` block from `tier_policy.yaml` into the
  collector config. Logs are PII-masked (`redaction/logs`) and body-stripped
  (`transform/logs-strip-body`) before going to `loki` via the proxy.
- **T2 → T3** (add sampled traces): set `FYRALIS_TELEMETRY_TIER=T3`; also merge
  `t3_increment.*` and the `traces` pipeline (probabilistic sampling +
  `redaction/traces` + raw SQL/URL/body stripping → OTLP to the proxy).
- **Down-tier:** remove the higher pipeline block(s).

A merged T3 config (T1+T2+T3) validates clean against the real collector — see
the self-test, which assembles and validates it.

## Self-test

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python selftest.py
```

Checks: all three YAMLs parse; the allowlist/redact/identity processors exist
**and are wired into the metrics pipeline**; remote-write targets the proxy and
does **not** set `X-Scope-OrgID`; no logs/traces pipeline exists at T1; a sample
PII label (`email`, `installation_id`, …) is dropped and an allowlisted family
(`fyralis_oauth_token*`, …) is kept (simulated against the rules *parsed from the
config*, not a hand-copy); and — when docker or a native `otelcol-contrib` is
available — a real `otelcol-contrib validate` of the T1 config. **55/55 pass**;
the real-collector validate is green.

## Caveats

- **Scrape targets are data-plane service names.** `otel-collector-config.yaml`
  uses docker-network names (`normalizer:9300`, `gateway:8000`, …) mirroring
  `observability/prometheus/prometheus.yml`. A real customer deploy swaps these
  for its own service discovery / static targets — the receiver block is the only
  thing that changes; the redaction/identity/egress stays fixed.
- **Default-deny is intentional (fail-closed for I1).** A newly added Fyralis
  metric family is dropped until added to the allowlist in **both**
  `otel-collector-config.yaml` and `prometheus_remote_write_overlay.yml` (and
  documented in `redaction_allowlist.md`). Keep the three in sync.
- **Two egress paths, one I1 contract.** Path A = this collector (supports
  T1/T2/T3). Path B = direct data-plane Prometheus `remote_write` (T1 metrics
  only). Both go through the proxy; both reproduce the same allowlist + drop-list.
  See `dataplane_remote_write.md`.
- **`X-Scope-OrgID` is never set here.** If it were, the proxy ignores it (I4).
  Tenant scoping is the cert SAN, server-side at the proxy.
- **G5 gap surfaces as `up==0`.** `anomaly-processor` / `deadline-resolver` are
  scraped as targets; if they're not in the deployment's compose, their `up`
  series is `0`, which is exactly the "coded-but-not-running" signal the fleet
  view needs (design doc §12 G5).
- **`resource_to_telemetry_conversion` is off** on the remote-write exporter so
  identity comes only from the explicit `resource` labels (no `target_info`
  noise). The `tls.insecure: false` paths assume the agent mounts real cert
  material; the dev/demo single-host topology can point at a local proxy.
