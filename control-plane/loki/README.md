# Loki — Tier 2 central log store (WS-LOKI-T2)

Grafana Loki, the **vendor-owned** central store for **Tier 2 (T2) redacted logs**
(`SPRINT_PLAN.md` → Phase 3, Contract C3). It is the logs analogue of Mimir
(metrics): a multi-tenant store that lives in the control plane, behind the
auth-proxy, scoped per tenant by the `X-Scope-OrgID` header.

```
boundary OTel Collector (customer VPC)        VENDOR control plane (cp-net)
  T2 logs, ALREADY REDACTED  ──mTLS──▶  auth-proxy  ──X-Scope-OrgID──▶  Loki
                                        (injects header from              │
                                         verified cert SAN, C1/I4)        ▼
                                                                  /data (filesystem)
  operator Grafana ──query, sets X-Scope-OrgID per tenant──▶ auth-proxy ──▶ Loki
```

---

## Trust boundary — logs arrive ALREADY REDACTED (I1)

**Loki is the sink, not the redactor.** By the time a log line reaches this
store it has already had PII and payload bytes stripped, **inside the customer
VPC**, by the boundary OTel Collector — before any byte left the VPC. This is
**Invariant I1** and it is enforced upstream, not here:

- At **T1** (default) no logs leave the VPC at all — metrics only.
- At **T2** the boundary collector runs a logs pipeline that
  (`boundary/tier_policy.yaml`, `t2_increment`):
  1. **drops** any log attribute not on a small operational allowlist
     (`redaction/logs`, `allow_all_keys: false` — worker/job/source/state/
     status/level/severity/tenant_id/deployment_id/region/…),
  2. **masks** values matching PII/secret regexes anywhere they appear
     (email, IPv4, bearer tokens, `api_key=…`/`secret=…`, card-like numbers),
  3. **replaces the raw body** with the constant marker `[redacted-T2]` so the
     *fact* of a log at a given level/logger egresses, **not** its free text,
  4. only then exports to Loki **through the auth-proxy**.

So Loki stores redacted lines and bounded enum labels. It does **not** re-scan
or re-redact — that would be redundant and would imply the boundary is not
trusted. The redaction contract and its auditable allowlist live in
`boundary/redaction_allowlist.md` and `boundary/tier_policy.yaml`.

> If you ever need defense-in-depth scrubbing *at* Loki, do it in the boundary
> collector or a write-path transform — never relax the boundary on the
> assumption Loki will catch it.

---

## Multi-tenancy (C5) — `auth_enabled: true`

`loki.yaml` sets **`auth_enabled: true`**. With this on, Loki **requires an
`X-Scope-OrgID` header on every request** and isolates each tenant's streams,
ingest, retention, and limits by that value.

The header is **never** accepted from outside `cp-net`:

- **Ingest:** the data plane presents an mTLS client cert; the **auth-proxy**
  verifies it, extracts `tenant_id` from the SPIFFE URI SAN
  (`spiffe://fyralis/tenant/<id>`) **server-side** (C1 / Invariant I4), and
  **injects** `X-Scope-OrgID=<tenant_id>` before forwarding to Loki at
  `/loki/api/v1/push`. The boundary collector deliberately does **not** set the
  header itself.
- **Query:** the operator Grafana datasource sets `X-Scope-OrgID` per tenant on
  the read path (also through the proxy).
- Loki trusts the header **only because** it is reachable solely from behind the
  proxy on `cp-net`. Publishing `3100` to the host (below) is a dev/demo +
  healthcheck convenience; in production Loki is not directly reachable.

> **One tenant = one `X-Scope-OrgID`** (the `tenant_id` from C1). The same value
> keys Mimir, so a tenant's metrics and logs line up.

---

## Storage & retention

- **Storage:** `filesystem`, everything local under **`/data`** (the
  `loki-data` docker volume). TSDB **schema v13** index + chunks + compactor
  working dir + ruler WAL all live there. No object store is needed for the
  single-host control plane.
- **Retention:** owned by the **compactor** (`compactor.retention_enabled:
  true`). The fleet-wide default is **`retention_period: 744h` (31 days)** in
  `limits_config`. Chunks older than that are marked for deletion and removed
  after a `retention_delete_delay` of 2h.
- **Per-tenant retention overrides:** layer them in
  `overrides/loki-overrides.yaml` (hot-reloaded via `runtime_config`, no
  restart):

  ```yaml
  overrides:
    acme:
      retention_period: 2160h     # 90 days for a premium tenant
      ingestion_rate_mb: 16
  ```

## Per-tenant ingestion limits (the noisy-neighbour guard)

Defaults in `limits_config` bound **every** tenant so one noisy data plane can't
exhaust the shared store:

| Limit | Default | Purpose |
|-------|---------|---------|
| `ingestion_rate_mb` | 8 MB/s | sustained push rate per tenant |
| `ingestion_burst_size_mb` | 16 MB | burst allowance per tenant |
| `per_stream_rate_limit` / `_burst` | 3 MB / 8 MB | per-stream throttle |
| `max_streams_per_user` | 10000 | active stream cap per tenant |
| `max_label_names_per_series` | 30 | label-cardinality bound (T2 labels are enums) |
| `max_line_size` | 256 KiB | max single line (`max_line_size_truncate: true`) |
| `reject_old_samples` | true / 168h | drop logs older than 7 days at ingest |
| `max_query_length` | 745h | never query past retention |
| `max_entries_limit_per_query` | 5000 | bound query result size |

Tighten or loosen any of these per tenant in `overrides/loki-overrides.yaml`.

---

## How to run

### As part of the control plane (recommended)
The integrate step merges `service.compose.yml` into
`docker-compose.control-plane.yml`. Then from `control-plane/`:

```bash
docker compose -f docker-compose.control-plane.yml up -d loki
# health:
curl -s localhost:3100/ready          # "ready"
```

### Standalone (dev/demo)
The fragment's mount paths (`./loki/loki.yaml`) are written for the **merged**
master compose, which lives at `control-plane/`. To run the fragment by itself
with those paths resolving correctly, point `--project-directory` at
`control-plane/`:

```bash
# from control-plane/
docker compose --project-directory . -f loki/service.compose.yml config   # validate
docker compose --project-directory . -f loki/service.compose.yml up -d
```

(Plain `docker compose -f loki/service.compose.yml config` from `control-plane/`
also parses cleanly, but resolves the bind sources relative to the fragment's
own dir — use `--project-directory .` so `./loki/loki.yaml` lands on the real
file. After the integrate step merges this into the master compose, the paths
are correct with no flag.)

### Smoke-test the multi-tenant push/query path
Because `auth_enabled: true`, you MUST send `X-Scope-OrgID` (in production the
proxy does this — for a raw local test simulate it):

```bash
# push one (already-redacted) line as tenant "acme"
curl -s -H "Content-Type: application/json" -H "X-Scope-OrgID: acme" \
  -X POST localhost:3100/loki/api/v1/push --data '{
    "streams": [{ "stream": {"job":"fyralis","level":"info"},
      "values": [["'"$(date +%s)000000000"'", "[redacted-T2]"]] }]}'

# query it back as the SAME tenant
curl -s -G localhost:3100/loki/api/v1/query_range \
  -H "X-Scope-OrgID: acme" --data-urlencode 'query={job="fyralis"}'

# a request WITHOUT X-Scope-OrgID is rejected (no tenant) — proves isolation
curl -s localhost:3100/loki/api/v1/query_range --data-urlencode 'query={job="fyralis"}'
```

### Validate the config
```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python - <<'PY'
import yaml
cfg = yaml.safe_load(open("loki/loki.yaml"))
assert cfg["auth_enabled"] is True
assert "retention_period" in cfg["limits_config"]
assert cfg["compactor"]["retention_enabled"] is True
assert cfg["limits_config"]["ingestion_rate_mb"] > 0          # per-tenant limit
print("OK", cfg["limits_config"]["retention_period"])
PY
```

---

## Caveats

- **Single-binary, single-instance, `replication_factor: 1`.** This is the
  control-plane store for a moderate fleet, not an HA log lake. For HA, split
  into read/write/backend targets, move chunks/index to object storage (S3/GCS),
  and raise the replication factor — `schema_config` v13 + the limits carry over.
- **Filesystem retention deletes are compactor-driven.** Deletion is not
  instant: a chunk past `retention_period` is removed on the next compaction
  cycle (`compaction_interval: 10m`) plus the `retention_delete_delay` (2h).
  Disk usage lags the policy by up to that window.
- **`auth_enabled: true` means EVERY request needs `X-Scope-OrgID`.** A request
  without it gets a 4xx ("no org id"). This is intentional — it's what makes the
  store fail-closed for tenancy. Local raw curls must supply the header; through
  the proxy it is always present.
- **Loki trusts `X-Scope-OrgID` blindly.** Tenant *isolation* depends entirely
  on the auth-proxy being the only reachable ingress on `cp-net` and on the
  proxy stripping any client-supplied value (C1/I4). Do **not** publish `3100`
  to an untrusted network in production. (Note: the auth-proxy carries a known
  HIGH-severity SSRF defect tracked in `auth-proxy/security/` — fix it before
  exposing this path; tenant *scoping* is intact, network *containment* is not
  until then.)
- **Redaction is upstream (I1).** Loki stores whatever the boundary sent. If the
  boundary is misconfigured to a wider allowlist or to ship raw bodies, raw text
  could land here. Audit `boundary/redaction_allowlist.md`, not Loki, to prove
  no PII at rest. Loki performs **no** redaction.
- **Runtime overrides file is optional but the mount path must exist.** The
  service mounts `./loki/overrides`; keep that dir present (even empty) so the
  `runtime_config.file` path resolves. An absent file just means "defaults apply
  to all tenants".
- **Structured metadata enabled.** `allow_structured_metadata: true` (Loki 3.x)
  lets T2 identity attrs (tenant_id/deployment_id/region) ride as structured
  metadata. It requires schema v13 (set) — do not downgrade the schema without
  disabling this.
- **Pinned image `grafana/loki:3.4.2`.** The config uses 3.x-only keys
  (`tsdb`, `allow_structured_metadata`, `delete_request_store`). Bumping the
  image major may require config migration — re-validate before changing the tag.

## Files

| File | Purpose |
|------|---------|
| `loki.yaml` | Loki config: `auth_enabled: true`, filesystem `/data`, TSDB v13, compactor retention, per-tenant limits |
| `service.compose.yml` | Standalone service fragment (image/mounts/`cp-net`/`3100`) merged into the master compose |
| `overrides/loki-overrides.yaml` | Per-tenant limit/retention overrides (hot-reloaded; optional) |
| `README.md` | This file |
