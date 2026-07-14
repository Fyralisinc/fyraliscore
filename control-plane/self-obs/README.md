# control-plane / self-obs — Control-Plane Self-Observability (WS-SELFOBS)

> **NFR-10 — "the control plane monitors itself; silence != health."**

This directory is the inside-out half of control-plane (CP) observability. The
rest of the platform watches the *customer fleet* (central Mimir/Loki, scraped
via the auth-proxy, multi-tenant). **Self-obs watches the vendor CONTROL PLANE
itself** — auth-proxy, console, config-dist, release-registry, Mimir, Loki,
Grafana — with a *separate, independent* Prometheus so it can still page when the
thing that broke is the fleet pipeline. You cannot use the system you are
monitoring to tell you that system is down.

The defining requirement is the second clause: **silence != health**. A green
dashboard with no data must NEVER be mistaken for "all good." Every "down" alert
here has a **silence twin** (`absent(...)` / staleness) so the *absence* of the
signal pages just as loudly as an explicit failure.

---

## Deliverables

| File | What it is |
|------|------------|
| `cp_exporter.py` | A small Prometheus exporter that **actively probes** each CP service's health/readiness endpoint every scrape and exposes `up` / probe-latency / last-success metrics, the **ingest-path-alive synthetic**, and the **silence heartbeat**. |
| `cp-prometheus.yml` | A **dedicated** CP Prometheus scrape config: scrapes the exporter + Mimir/Loki/Grafana `/metrics` directly, loads `cp_rules.yml`. |
| `cp_rules.yml` | Recording + **ALERT** rules: auth-proxy down, Mimir/Loki unreachable, console down, config-dist down, ingest-path down, **and the critical control-plane SILENCE page**. |
| `dashboards/cp_self.json` | A Grafana dashboard for CP health (silence watchdog, per-service status, probe latency, ingest path). |
| `service.compose.yml` | `cp-self-obs-exporter` + `cp-prometheus` services on `cp-net` (compose fragment, merged at Assemble). |
| `Dockerfile` / `requirements.txt` | Self-contained image for the exporter (pure stdlib + `prometheus_client`). |
| `selftest.py` | Offline self-test (yaml-load configs, optional promtool, exporter-against-stubs, json-load dashboard). |

---

## How the exporter probes each service (the bespoke bit)

The CP services do **not** share one health contract, and one of them cannot be
probed with a plain HTTP GET at all. The exporter normalises all of this:

| Service | Probe kind | Why |
|---------|-----------|-----|
| **auth-proxy** | **TLS handshake** to `:8443` | mTLS-ONLY (`CERT_REQUIRED`, trusts only the Fyralis CA). A plain GET — and therefore Prometheus' own scrape or a vanilla blackbox http probe — is **rejected at the TLS handshake**. There is no unauthenticated `/healthz`. Liveness = "does it accept TLS and present its server cert / demand a client cert?" |
| **mimir** | HTTP `GET /ready` → `ready` | Image is **distroless** (no shell/curl); readiness is the HTTP endpoint. |
| **loki** | HTTP `GET /ready` → `ready` | Same readiness contract. |
| **grafana** | HTTP `GET /api/health` (2xx) | Standard Grafana health. |
| **console** | HTTP `GET /healthz` (2xx) | FastAPI `{"status":"ok"}`. |
| **config-dist** | HTTP `GET /healthz` (2xx) | FastAPI `{"ok":true,...}`. |
| **release-registry** | HTTP `GET /healthz` (2xx) | FastAPI `{"status":"ok",...}` (container port 8090 on cp-net). |

Every probe yields:

```
cp_service_up{service,component,probe}                       1 healthy / 0 down
cp_probe_latency_seconds{service,component,probe}            probe duration
cp_service_last_success_timestamp_seconds{service,component} unix, 0 if never
cp_probe_http_status_code{service,component}                 last HTTP code (0 for TLS)
```

### The ingest-path-alive synthetic

The crown-jewel signal: **can a tenant agent push a sample through the
auth-proxy into Mimir right now?** Two auto-selected modes:

- **`structural`** (default, no secret): confirm BOTH ingest endpoints are alive
  — the auth-proxy TLS listener **and** Mimir `/ready` — without pushing a byte.
- **`fullpush`** (when a tenant client cert is mounted): make an authenticated
  request **through** the proxy (`https://auth-proxy:8443/prometheus/...`). A 2xx
  proves the *complete* control path: mTLS termination → SAN→tenant resolution →
  `X-Scope-OrgID` injection → Mimir answered.

```
cp_ingest_path_alive{mode}                              1 usable / 0 not
cp_ingest_path_last_success_timestamp_seconds          unix, 0 if never
cp_ingest_path_probe_latency_seconds                   probe duration
```

To enable `fullpush`, issue a tenant cert (`ca/issue_cert.py`), mount it
read-only into the exporter, and set `CP_SELFOBS_CLIENT_CERT` /
`CP_SELFOBS_CLIENT_KEY` (+ optional `CP_SELFOBS_CA_CHAIN`). See the commented
block in `service.compose.yml`.

### The silence heartbeat (NFR-10)

```
cp_self_scrape_heartbeat_timestamp_seconds   set to now() on EVERY scrape
cp_self_scrape_duration_seconds              full sweep duration
cp_services_up / cp_services_total           rollups
```

`cp_rules.yml` pages on `absent(cp_self_scrape_heartbeat_timestamp_seconds)` and
on its staleness: if the exporter dies or cp-prometheus stops scraping it, the
**silence itself pages.**

---

## Alerts (cp_rules.yml)

**Critical silence (page):**
- `ControlPlaneSelfObsSilent` — `absent()` of the heartbeat: we are blind.
- `ControlPlaneSelfObsStale` — heartbeat age > 90s.
- `ControlPlaneProberTargetDown` — cp-prometheus cannot scrape the exporter.

**Per-service down (each with an `absent()` silence twin):**
`AuthProxyDown`, `MimirUnreachable`, `LokiUnreachable`, `ConsoleDown`,
`ConfigDistDown`, `ReleaseRegistryDown` (ticket), `GrafanaDown` (ticket).

**Ingest path:** `IngestPathDown`, `IngestPathStale`, `IngestPathProbeMissing`.

**Degradation (ticket):** `CPProbeLatencyHigh`, `CPServicesDegraded`.

Recording rules: `cp:services_healthy_ratio`,
`cp:self_scrape_heartbeat_age_seconds`,
`cp:ingest_path_last_success_age_seconds`,
`cp:service_last_success_age_seconds`, `cp:probe_latency_seconds:avg5m`.

---

## Run it

### Self-test (offline, no CP running)

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
    control-plane/self-obs/selftest.py
```

Validates: yaml-load of both configs + structure (required alerts present, the
silence alert really uses `absent()`); `promtool check rules`/`check config` if
`promtool` is on PATH (else SKIP); imports the exporter and asserts it produces
the right `up`/latency/synthetic/heartbeat metrics against stub health + TLS
servers; json-loads the dashboard.

### Bring it up (merged into the master compose)

```bash
docker compose \
  -f control-plane/docker-compose.control-plane.yml \
  -f control-plane/self-obs/service.compose.yml \
  up -d cp-self-obs-exporter cp-prometheus
```

- Exporter: `http://localhost:9110/metrics` (cp-net: `cp-self-obs-exporter:9110`)
- CP Prometheus: `http://localhost:9091` (cp-net: `cp-prometheus:9090`)

### Standalone smoke test of just this fragment

Uncomment the `networks:`/`volumes:` block at the bottom of
`service.compose.yml`, then:

```bash
docker network create cp-net
docker compose -f control-plane/self-obs/service.compose.yml up
```

---

## Assemble / integration notes (for the wiring step)

1. **Volume.** Add `cp-self-obs-data:` to the master `volumes:` block (the
   fragment also declares it for standalone use; compose merges them).
2. **Dashboard datasource.** The dashboard reads a **dedicated CP Prometheus**,
   not Mimir. Provision a Grafana datasource (suggested
   `uid: fyralis-cp-prometheus`, name containing "CP") pointing at
   `http://cp-prometheus:9090`, and drop `dashboards/cp_self.json` under
   `grafana/dashboards/` (a new `control-plane` provider, or reuse the Fleet
   folder). The dashboard's `${DS_CP}` input/variable binds to it.
3. **Alertmanager.** `cp-prometheus.yml` points `alerting` at the conventional
   cp-net name `alertmanager:9093`; it is a no-op until one is wired. Route
   `severity=page, scope=control-plane` (and especially `silence="true"`) to the
   on-call pager.
4. **auth-proxy is intentionally NOT a direct Prometheus target** (mTLS-only).
   Its liveness comes solely from the exporter probe — do not add an
   `auth-proxy:8443` scrape job (it would be permanently "down").

---

## Caveats

- **promtool is not on the dev host.** The self-test SKIPs the `promtool` step
  there. Out-of-band validation against `prom/prometheus:v2.53.0` passes:
  `promtool check rules cp_rules.yml` → *25 rules found*; `promtool check config`
  → valid (when `cp_rules.yml` is mounted at the absolute `/etc/prometheus/`
  path the config references, which is exactly how the compose service mounts it).
- **Default ingest synthetic is `structural`, not a real push.** Without a
  mounted tenant client cert it confirms the two ingest *endpoints* are alive but
  does not push a byte. The metric carries `mode="structural"` so the
  dashboard/alert can tell an *inferred* path from a *proven* one. `fullpush`
  needs a tenant cert (see above). Even `fullpush` does a GET through the proxy
  (not a `remote_write` protobuf push), because vendoring snappy+protobuf into a
  tiny exporter is not worth it; the GET exercises the same auth-proxy→Mimir
  control path end-to-end.
- **The TLS handshake probe does not verify the auth-proxy's cert chain.** It is
  a *liveness* probe (is the listener up / does it present a cert / demand a
  client cert?), not an identity check. A handshake that fails *because the
  proxy demanded a client cert* is correctly read as **up** — that is the mTLS
  listener working as designed.
- **Probe-on-scrape.** The exporter runs every probe on each Prometheus scrape
  (fresh data, no stale background snapshot). With the default 15s scrape
  interval and a 5s per-probe timeout, a fully-wedged set of upstreams could make
  one scrape take up to ~`7 services x 5s`; in practice probes run quickly and
  the scrape timeout (10s) bounds Prometheus' wait. Tune `scrape_timeout` /
  `CP_SELFOBS_PROBE_TIMEOUT_S` if you add many slow targets. (Probes are not
  parallelised — deliberately simple; revisit if the target count grows.)
- **Mimir/Loki/Grafana `/metrics` direct scrape** assumes those operational
  endpoints are served without a tenant header (they are). Mimir's *data* APIs
  still require `X-Scope-OrgID`; we never touch those from cp-prometheus.
- **No service-dir imports.** The exporter is deliberately dir-disjoint (pure
  stdlib + `prometheus_client`), so it builds from this dir alone and cannot be
  broken by a sibling service refactor. The trade-off is that the health-endpoint
  paths are encoded here as config/defaults rather than imported from each
  service; if a service changes its health path, update the corresponding
  `CP_*_URL` default (one line).
- **Retention is short (15d).** This is an operational watchdog, not a metrics
  warehouse. Long-term CP trends, if ever wanted, belong in Mimir under a CP
  tenant — out of scope here on purpose (it must survive a Mimir outage).
