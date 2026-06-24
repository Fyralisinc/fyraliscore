# WS-MIMIR — Grafana Mimir, the central multi-tenant metrics store

Mimir is the control plane's **multi-tenant** metrics store. Every tenant's
metrics live in one shared cluster, isolated by the **`X-Scope-OrgID`** header.
That header is injected **server-side by the auth-proxy** from the verified
client-cert SAN (`spiffe://fyralis/tenant/<id>`, SPRINT_PLAN.md C1/C5) — it is
**never** trusted from a caller. Multi-tenancy is hard-ON: a request without
`X-Scope-OrgID` is rejected `401` by Mimir itself.

## Files

| File | What it is |
|------|------------|
| `mimir.yaml` | Mimir config: `multitenancy_enabled: true`, all-in-one `target: all`, filesystem blocks+ruler storage under `/data`, remote-write receive path, and the **per-tenant cardinality budget defaults** (`limits:`). |
| `runtime_overrides.yaml` | **Per-tenant** cardinality budget overrides (hot-reloaded every 15s, no restart). Worked examples: a large tenant `acme`, a small tenant `globex`, the `__fleet__` ruler tenant. |
| `service.compose.yml` | Standalone compose fragment (service `mimir` on `cp-net`, port 9009, data volume, fleet-sli mount) + a `mimir-ruler-loader` that pushes the fleet-sli rules into the ruler. The integrate step merges this into `docker-compose.control-plane.yml`. |
| `cardinality.md` | **WS-MIMIR-CARD**: how to MEASURE per-tenant series fit and ENFORCE/backpressure when a tenant exceeds budget. |
| `validate.py` | `yaml.safe_load` validator: asserts multitenancy on, the budget keys exist, the overrides parse + override the budget. |

## How it wires into the control plane (C5)

```
agent / boundary collector  --mTLS-->  auth-proxy  --http + X-Scope-OrgID-->  mimir:9009
                                       (injects header from cert SAN)        (this service)
operator Grafana  --query + X-Scope-OrgID per tenant-->  auth-proxy / mimir:9009
```

- Service name **must** be `mimir` — the auth-proxy reverse-proxies to
  `http://mimir:9009` (`auth-proxy/config.py DEFAULT_UPSTREAM_URL`).
- Mimir trusts `X-Scope-OrgID` **only** because it arrives from inside `cp-net`
  behind the proxy. The host port `9009:9009` is published for operator
  convenience only; in production do **not** publish it — go through the proxy.

## Run it

### Validate the config (no Docker needed)

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
  /home/prajwal-adhikari/Desktop/v2/fyralis-control-plane/control-plane/mimir/validate.py
```

Asserts: `multitenancy_enabled` true, `target: all`, HTTP 9009, filesystem
storage under `/data`, the three budget knobs present with sane defaults, the
runtime-override file referenced + a per-tenant budget override present.

### Bring it up (standalone fragment)

The bind-mount paths are written relative to the **control-plane root**, so pin
the project dir there:

```bash
cd /home/prajwal-adhikari/Desktop/v2/fyralis-control-plane/control-plane
docker compose --project-directory . -f mimir/service.compose.yml up -d
```

This starts `mimir` (all-in-one) and a one-shot `mimir-ruler-loader` that waits
for `/ready`, then loads the fleet-sli rules into the `__fleet__` ruler tenant.

### Verify (the checks that pass against the real binary)

```bash
MIP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  $(docker compose --project-directory . -f mimir/service.compose.yml ps -q mimir))

# multitenancy enforced: no header => 401, with header => 200
curl -s -o /dev/null -w '%{http_code}\n'                       "http://$MIP:9009/prometheus/api/v1/query?query=up"   # 401
curl -s -o /dev/null -w '%{http_code}\n' -H 'X-Scope-OrgID: acme' "http://$MIP:9009/prometheus/api/v1/query?query=up" # 200

# remote-write receive path (no header 401; with header reachable)
curl -s -o /dev/null -w '%{http_code}\n' -X POST -H 'X-Scope-OrgID: acme' "http://$MIP:9009/api/v1/push"             # 400 (empty body) = endpoint live

# per-tenant override applied (acme bumped to 500k series)
curl -s "http://$MIP:9009/runtime_config" | grep -A2 'acme:'

# ruler evaluating the fleet-sli groups (22 with the golden-12 catalog loaded)
curl -s -H 'X-Scope-OrgID: __fleet__' "http://$MIP:9009/prometheus/api/v1/rules" | head -c 300
```

Tear down: `docker compose --project-directory . -f mimir/service.compose.yml down -v`.

## Per-tenant cardinality budgets

The defaults live in `mimir.yaml > limits` (the floor every tenant gets):

| Knob | Default | Meaning |
|------|---------|---------|
| `max_global_series_per_user` | `150000` | **the** budget — active series per tenant |
| `ingestion_rate` / `ingestion_burst_size` | `25000` / `50000` | samples/sec (token bucket) |
| `max_label_names_per_series` | `30` | labels/series (bounds fan-out) |

Override per tenant in `runtime_overrides.yaml` (hot-reloaded, no restart):

```yaml
overrides:
  acme:                                  # a big data plane
    max_global_series_per_user: 500000
    ingestion_rate: 75000
```

How to **measure fit** and **enforce/backpressure** at the budget edge is in
[`cardinality.md`](./cardinality.md) (the WS-MIMIR-CARD method).

## Schema (what integrate / downstream can rely on)

```yaml
# service / network
service_name:        mimir            # auth-proxy upstream host
network:             cp-net
http_port:           9009             # /api/v1/push, /prometheus/api/v1/query, ruler API
grpc_port:           9095

# auth
multitenancy_enabled: true            # every request needs X-Scope-OrgID; no anon
auth_header:          X-Scope-OrgID    # injected by auth-proxy from cert SAN (C1/C5)

# storage (local dev)
target:              all              # monolithic / all-in-one
blocks_storage:      filesystem  dir=/data/blocks   tsdb=/data/tsdb
ruler_storage:       filesystem  dir=/data/ruler
data_volume:         mimir-data -> /data

# remote-write receive path
remote_write_url:    POST http://mimir:9009/api/v1/push      (X-Scope-OrgID required)
otlp_url:            POST http://mimir:9009/otlp/v1/metrics   (X-Scope-OrgID required)

# per-tenant cardinality budget (limits + runtime_overrides)
limits.max_global_series_per_user:   150000   # default; per-tenant override allowed
limits.ingestion_rate:               25000
limits.max_label_names_per_series:   30
runtime_overrides_file:  /etc/mimir/runtime_overrides.yaml   (reload period 15s)

# ruler / fleet-sli
ruler_tenant:        __fleet__                 # X-Scope-OrgID for recorded fleet:* series
fleet_sli_source:    ./fleet-sli  (read-only)  # rule files
fleet_sli_load_via:  mimirtool rules load -> /prometheus/config/v1/rules
```

## Caveats

- **Local filesystem storage, NOT for production.** Mimir itself logs a warning:
  *"filesystem is for development and testing only; switch to an external object
  store for production."* In production, change `blocks_storage`, `ruler_storage`,
  and `alertmanager_storage` to `backend: s3` (or `gcs`/`azure`) pointing at a
  bucket. The single-host all-in-one `target: all` with replication factor 1 also
  has no HA — production runs the microservices targets across nodes.
- **The `grafana/mimir` image is distroless** (no shell/wget/curl), so the
  service has **no in-container healthcheck**. Readiness is `GET /ready`
  (200 once live; ~15s post-start grace returns 503 *"waiting 15s after being
  ready"* — that is normal, not an error). Probe it from another service.
- **`auth_enabled` is NOT a valid Mimir key.** Mimir renamed the old
  Cortex/Loki flag to `multitenancy_enabled`; the binary **rejects** a literal
  `auth_enabled`. `multitenancy_enabled: true` IS the auth flag — it satisfies
  the contract requirement that auth is on (every request needs `X-Scope-OrgID`).
- **Rules are loaded via the ruler API, not by copying files.** Mimir's
  filesystem ruler backend stores each group as its own object and does **not**
  auto-discover a directory of multi-group YAML files. The `mimir-ruler-loader`
  uses `mimirtool rules load`; a plain `cp` into `/data/ruler/__fleet__/` does
  nothing (the ruler reports *"no rule groups found"*).
- **Compose relative paths resolve from the control-plane root.** Run the
  standalone fragment with `--project-directory .` from the control-plane root,
  or it will look for `mimir/fleet-sli` instead of `fleet-sli`.
- **Exemplars are off and `usage_stats` is disabled** to keep with I1 (no
  surprise PII/egress). Enable exemplars per tenant via `max_global_exemplars_per_user`
  if/when traces (T3) are wired.
```
