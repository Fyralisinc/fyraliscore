# demo-dataplane — golden-12 SLI metrics stub

A tiny, stdlib-only HTTP server that exposes the **golden-12 SLI** `fyralis_*`
metric families on `:9300` in Prometheus text format. It exists so the
**testable** control-plane bring-up has a realistic scrape target for the
boundary OTel Collector — **without** standing up the real Fyralis data plane.

## What it proves

It closes the loop on the BYOC telemetry path so a CTO can watch real series
land in the central fleet view:

```
demo-dataplane(:9300)
  └─▶ boundary OTel Collector   (scrape :9300, allowlist-filter, drop PII labels,
                                 stamp tenant_id/deployment_id/region — Invariant I1/C4)
        └─▶ auth-proxy           (mTLS termination; X-Scope-OrgID from the verified
                                 client-cert SAN — never from a header, Invariant I4)
              └─▶ Mimir          (multi-tenant store, keyed by X-Scope-OrgID)
                    └─▶ Grafana   (fleet + per-customer dashboards, fleet-sli rules)
```

The metric **names** match exactly what `boundary/otel-collector-config.yaml`
keeps on its Tier-1 allowlist and what `fleet-sli/recording_rules.yml`
aggregates, so the recording/alert rules light up against this stub.

## Run

```bash
# standalone
python demo-dataplane/metrics_stub.py          # serves on :9300
curl -s localhost:9300/metrics | head
curl -s localhost:9300/healthz

# as part of the bring-up (merged into the master compose)
docker compose -f docker-compose.control-plane.yml up -d demo-dataplane
```

### Scenarios

`DEMO_DP_SCENARIO=healthy` (default) emits green-band values.
`DEMO_DP_SCENARIO=degraded` pushes a few SLIs into the yellow/red band (think
backlog up, a worker class missing, OAuth refresh failures, gateway 503s) so the
fleet roll-up colour and the fleet-sli alerts can be demoed.

## Endpoints

| path        | purpose                                              |
|-------------|------------------------------------------------------|
| `/metrics`  | Prometheus exposition of the golden-12 families      |
| `/healthz`  | liveness `{"status":"ok"}` (also the agent SLI probe)|
| `/`         | human note                                           |

## NOT the real data plane

This is a **demo fixture**. It emits plausible-but-synthetic numbers and opens
no database/Kafka. In **production** the installer (`control-plane/installer/`)
stands up the real data plane in the customer VPC and points the boundary
collector at the **real** targets (workers `:9300`, gateway `:8000`,
postgres-exporter `:9187`, kafka-exporter `:9308`, …). Do **not** ship this
service to a customer.
