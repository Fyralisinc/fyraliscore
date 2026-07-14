# Data-plane direct remote_write — the WS-REMOTEWRITE alternative path

There are **two** ways the customer VPC ships Tier-1 metrics to central Mimir.
Both produce the **identical** filtered, identity-labeled series and both go
**through the auth proxy** (never directly to Mimir); they differ only in *who*
does the scraping and filtering.

```
            ┌───────────────── customer VPC (data plane) ─────────────────┐
            │                                                             │
  PATH A    │  data-plane /metrics ──▶ boundary OTel Collector ──┐        │
 (default)  │   (workers :9300, ...)   (filter + redact + stamp) │        │
            │                                                    │        │   mTLS, OUTBOUND
            │  PATH B  data-plane Prometheus ──(remote_write)────┤        │ ─────────────────▶  auth proxy ──▶ Mimir
            │   (already scraping :9300)  write_relabel_configs   │        │   (proxy injects        (X-Scope-OrgID
            │                            (filter + redact + stamp)┘        │    X-Scope-OrgID         = verified tenant)
            └─────────────────────────────────────────────────────────────┘
```

- **Path A (default)** — the **boundary OTel Collector**
  ([`otel-collector-config.yaml`](./otel-collector-config.yaml)). Use this when
  you want tier enforcement (T1/T2/T3 logs+traces) and a single egress chokepoint.
- **Path B (this doc)** — the customer's **existing data-plane Prometheus**
  remote-writes directly, via the overlay
  ([`prometheus_remote_write_overlay.yml`](./prometheus_remote_write_overlay.yml)).
  Use this when the customer already runs Prometheus and does not want a second
  collector process, and only needs **T1 metrics** (Path B cannot ship logs or
  traces — that requires the collector).

## Why Path B is still I1-safe

The overlay reproduces both boundary gates in pure Prometheus `relabel_configs`,
applied as `write_relabel_configs` (they run immediately before the bytes leave
the process, so a dropped series never hits the network):

| Boundary processor | Path B equivalent |
|---|---|
| `filter/allowlist` (Gate 1, family keep-list) | `write_relabel_configs` `action: keep` on `__name__` |
| `transform/redact-labels` (Gate 2, label drop) | `write_relabel_configs` `action: labeldrop` on label names |
| `resource` (C4 identity stamp) | `write_relabel_configs` `target_label`/`replacement` (or `global.external_labels`) |
| `prometheusremotewrite` exporter → proxy | `remote_write.url` → proxy `/api/v1/push` + `tls_config` mTLS |

The family allowlist and the label drop-list are **byte-for-byte the same sets**
as in [`redaction_allowlist.md`](./redaction_allowlist.md) — keep them in sync
when either path changes.

## How to apply Path B

1. Take the customer's existing `observability/prometheus/prometheus.yml` (29
   scrape targets, unchanged — it keeps scraping `:9300` etc. locally).
2. Merge the `remote_write:` block from
   [`prometheus_remote_write_overlay.yml`](./prometheus_remote_write_overlay.yml)
   into it (top-level key).
3. The installer renders the three identity lines with the deployment's literal
   `tenant_id` / `deployment_id` / `region` (from the C4 record), and mounts the
   per-tenant mTLS client cert at `/etc/fyralis/agent/client.{crt,key}` plus the
   Fyralis CA at `/etc/fyralis/ca/ca.crt`.
4. Point `remote_write.url` at the auth proxy (e.g.
   `https://auth-proxy.fyralis.example:8443/api/v1/push`).
5. Reload Prometheus (`SIGHUP` or `/-/reload`).

## What this path does NOT set (contract reminders)

- **No `X-Scope-OrgID`.** The proxy injects it from the verified cert SAN (C1).
  If the data plane set it, the proxy would ignore it (I4); we don't set it.
- **No raw labels.** The `labeldrop` runs on every series; a leaked id/email
  never reaches the wire.
- **No higher tiers.** Prometheus remote_write is metrics-only. T2 (logs) and T3
  (traces) require the boundary collector (Path A). A deployment on Path B is
  pinned to T1 by construction.

## Caveats specific to Path B

- `replacement:` is a **literal** — Prometheus does not expand `${ENV}` inside
  relabel rules. The installer must substitute the real identity values when it
  renders the file (or use the `global.external_labels` variant, which the
  config loader *does* env-expand on newer Prometheus with `--enable-feature=...`).
- The `keep` regex is **default-deny**: a newly added Fyralis metric family is
  silently dropped until added to the allowlist. That is the intended failure
  mode (fail closed for I1). When the data plane adds a family, update **both**
  this overlay and `otel-collector-config.yaml`.
- `action: keep` on `__name__` runs before `labeldrop`; ordering in the YAML
  list is preserved by Prometheus, so Gate 1 then Gate 2 then identity, as written.
