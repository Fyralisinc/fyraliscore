# Observability And Alert Guide

Fyralis observability is provisioned as code under `observability/`:

- Prometheus scrape config: `observability/prometheus/prometheus.yml`
- Recording rules: `observability/prometheus/rules/recording.yml`
- Grafana dashboards: `observability/grafana/dashboards/`
- Grafana alerts: `observability/grafana/provisioning/alerting/`

Prometheus labels must stay bounded and privacy-safe. Do not add tenant ids,
actor ids, emails, prompts, object keys, raw paths, query strings, payloads, or
secrets as labels.

## Dashboards

| Dashboard | First question |
| --- | --- |
| System Health | Are gateway/workers scrapeable, heartbeating, and free of dead-letter buildup? |
| Ingestion Funnel | Are sources moving raw events through normalization, writing, embedding, and DLQ cleanly? |
| Webhook Ingress | Are provider signatures passing and webhook fallback paths healthy? |
| Embeddings / Ollama | Is embedding latency/failure rate within budget and backlog draining? |
| Reasoning / LLM Cost | Are Think queues, failures, spend, and token rates under control? |
| Data Plane Infrastructure | Are Postgres, Kafka, Redis, MinIO, Grafana, and pool saturation healthy? |
| Product Workflow Health | Are user-facing product routes within request, 5xx, p95 latency, and SLO burn budgets? |

## Core Alerts

| Alert | Meaning | First action |
| --- | --- | --- |
| `WorkerHeartbeatStale` | A worker event loop stopped advancing. | Check service logs; restart the smallest affected worker. |
| `WorkerScrapeDown` | Prometheus cannot scrape a Fyralis app target. | Check `docker compose ps`, health port, and service logs. |
| `InfraScrapeDown` | Prometheus cannot scrape infrastructure/exporters. | Check exporter/container health and compose network. |
| `DLQDepthHigh` | Ingestion DLQ unresolved depth is accumulating. | Use the ingestion DLQ replay/quarantine runbook. |
| `ConsumerLagHigh` | Kafka consumer group lag is sustained. | Identify lagging group; scale/restart the relevant consumer. |
| `SignatureFailureSpike` | Webhook signature failures exceed baseline. | Check provider secret rotation, timestamp skew, and probing traffic. |
| `EmbedFailureRatioHigh` | Embedding attempts are failing above budget. | Check Ollama/OpenAI embedding backend health and backlog. |
| `ThinkQueueBackpressure` | Think queue depth is high. | Check Think worker health, DB pool saturation, and LLM provider errors. |
| `ThinkStaleLocks` | Think trigger locks are stale. | Inspect stuck workers; replay or clear only through approved tools. |
| `ThinkRetryExhausted` | Think/model re-eval retries were exhausted. | Inspect dead-letter rows; replay after root cause is fixed. |
| `DBPoolSaturated` | A DB pool is above 90% utilization. | Check slow queries, leaks, pool sizing, and DB activity dashboard. |
| `BackupRecoveryUnhealthy` | Backup/restore/inventory evidence is stale, missing, or failed. | Do not promote; fix backup evidence first. |
| `SchemaRLSDriftDetected` | Live schema/RLS no longer matches the schema lock or monitor errored. | Block release promotion; run `scripts/check_schema_drift.py`. |
| `ProductSLOBurnHigh` | Product routes are burning error or latency budget. | Check Product Workflow Health, route-level 5xx/p95, and recent deploys. |
| `LLMSpendBurnRateHigh` | LLM spend rate exceeds the configured launch threshold. | Check Reasoning / LLM Cost and pause high-volume tenants if needed. |
| `DeadLetterRowsPresent` | Product-side dead-letter rows are present. | Use the durable dead-letter admin runbook. |

## Useful Commands

```bash
docker compose ps
curl -fsS http://localhost:8000/healthz
curl -fsS http://localhost:8000/readyz
docker compose logs --tail=200 gateway
```

Run provisioning checks locally:

```bash
.venv/bin/pytest services/platform/runtime/tests/test_observability_provisioning.py -q
```

Run schema/RLS drift check:

```bash
DATABASE_URL=postgresql://... .venv/bin/python scripts/check_schema_drift.py
```

## Privacy Guardrails

- Raw application logs, DB payloads, prompts, source payloads, and PII must not
  be exported through Prometheus labels.
- Per-tenant views belong in access-controlled Postgres/Grafana panels, not
  Prometheus series.
- If a metric needs a new label, keep it to a bounded enum and add tests that
  reject unsafe label names/values.

## Alert Fire Drill

At least once per release cycle:

1. Confirm Grafana contact points are configured.
2. Temporarily route a test alert to the operator channel.
3. Confirm notification receipt and escalation owner.
4. Restore normal thresholds and record the fire-drill evidence in release
   notes.
