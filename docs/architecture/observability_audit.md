# Observability Audit — 2026-06-11

Point-in-time audit of every metrics implementation, exposition endpoint, and
health surface in the repository, performed as Phase 1 of the Prometheus +
Grafana observability initiative. Companion design doc:
[observability_architecture.md](observability_architecture.md).

## 1. Current coverage

### 1.1 Prometheus-format `/metrics` endpoints (live today)

| Process | Endpoint | Source | What it exposes |
|---|---|---|---|
| gateway | `GET :8000/metrics` (public, allowlisted in `_PUBLIC_PATHS`) | `services/app/gateway/core_router.py:95` → `services/app/webhooks/metrics.py:237` | `webhook_verification_failures_total{provider,reason}`, `webhook_resolver_outcomes_total{provider,outcome}`, `webhook_resolver_cache_total{provider,result}`, `webhook_router_kafka_path_total{provider,outcome}`, `webhook_resolver_duration_p95_seconds{provider}` |
| normalizer | `:9300/metrics` | `services/ingest/ingestion/observability.py:66` + `normalizer/worker.py` | `ingestion_normalizer_*` (consumed/produced, parse/invariant failures, transform duration sum/count, last consumer lag, DLQ publish outcomes) |
| observation_writer | `:9300/metrics` | `writers/observation_writer.py` | `ingestion_writer_*` (consumed, full-mode writes, dedup hits, partition missing/autocreated/out-of-bounds, parse failures, DLQ publish) |
| dlq_writer | `:9300/metrics` | `writers/dlq_writer/dlq_writer.py` | `ingestion_dlq_writer_*` incl. the `unresolved_depth` gauge and `depth_alerts_sent` |
| embedding_worker | `:9300/metrics` | `writers/embedding_worker/embedding_worker.py:104` | `ingestion_embedding_worker_*` (consumed, embeds succeeded/failed, guard no-ops, observation missing, DLQ publish) |
| embedding_backlog | `:9300/metrics` | `recovery/embedding_backlog/embedding_backlog.py:87` | `ingestion_backlog_*` (iterations, rows embedded/failed/skipped, rate-limit denials/sentinels, cursor resets) |
| periodic_reconciler | `:9300/metrics` | `workflows/periodic_reconciler.py:100` | `ingestion_periodic_reconciler_*` (runs checked, gaps found, shards resharded, check errors) |
| circuit_breaker | `:9300/metrics` | `feature_flags/circuit_breaker.py:120` | `ingestion_breaker_*` (active/breached/tripped tenants, lag measurements, flag flips) |

All `:9300` servers come from one shared implementation —
`services/ingest/ingestion/observability.py` (`Heartbeat`,
`run_heartbeat_ticker`, `start_health_server`, `render_prometheus`) — gated by
`INGESTION_HEALTH_PORT` (already set to `9300` for **every** container via the
`x-app-env` anchor in `docker-compose.yml:45`). Each also serves
`GET /healthz` (503 when the heartbeat is older than
`INGESTION_HEALTH_STALE_SEC`, default 120s) and emits
`ingestion_heartbeat_age_seconds` / `ingestion_uptime_seconds`.

### 1.2 Health endpoints

| Surface | Where | Notes |
|---|---|---|
| `GET /healthz` (gateway) | `core_router.py:86` | static `{"status": "ok"}` |
| `GET /readyz` (gateway) | `core_router.py:90` | component-by-component startup probe (db, secret store, resolver, flags, integration runtime, realtime, github gateway state, ingestion data plane) |
| `GET /healthz` (8 workers) | `ingestion/observability.py:112` | heartbeat-staleness liveness; wired into compose healthchecks (`x-consumer-healthcheck`) |
| compose infra healthchecks | `docker-compose.yml` | postgres `pg_isready`, kafka topic-list, minio, redis ping |

### 1.3 Instrumented but NOT exposed anywhere

* **Think worker** — `services/reasoning/think/observability.py:46-224`
  (`Metrics` singleton `METRICS`): runs/failures per trigger kind, run latency
  samples, ops by kind, per-tenant queue depth, cascade depth, region-lock
  waits, validation dropped-ops `{reason, op_type}`, **LLM cost USD / token /
  call counters per trigger kind**, cascade invariant violations, reconcile
  decisions, context-use grades. In-memory only; `snapshot()` is used by
  tests. No HTTP server, no compose healthcheck (`think_worker`,
  `post_commit_worker` in `docker-compose.yml:376-384` have none).
* **19 per-source integration metrics modules** —
  `services/ingest/integrations/<source>/metrics.py` for ashby, brex, carta,
  deel, discord, figma, fireflies, github, google_calendar, google_drive,
  gusto, hibob, linkedin, mercury, miro, notion, quickbooks, ramp, slack
  (plus `discord/gateway/metrics.py`). Shapes vary: flat
  `snapshot() -> dict[str,int]` counters (`mercury.request.ok` …) for the
  finance/HR sources; bespoke labeled counters for github/slack/discord/
  notion/google_*. Read by tests and log emission only — never rendered to
  any `/metrics` endpoint.
* **Postgres observability tables** — `think_runs`, `think_run_costs`,
  `think_region_lock_log`, `ingestion_failures`: a persisted metrics
  substrate (cost attribution per Think run, DLQ contents). Queried ad-hoc
  (`aggregate_costs_for_tenant`, validation runs); no dashboard.

### 1.4 Alerting that exists today

* DLQ depth: `dlq_writer.py` polls unresolved `ingestion_failures` count and
  POSTs `dlq.depth_threshold_exceeded` to `INGESTION_ALERT_WEBHOOK_URL`
  (cooldown-debounced; disabled unless `DLQ_DEPTH_ALERT_THRESHOLD` > 0).
* Circuit breaker trips POST to the same webhook.
* Nothing else; no alert manager, no alert rules-as-code.

### 1.5 Dependencies

`pyproject.toml` ships `opentelemetry-api/sdk` +
`opentelemetry-instrumentation-asyncpg` / `-aiokafka` — **installed, never
configured** (no exporter, no env). `prometheus_client` is deliberately
absent; `services/app/webhooks/metrics.py:8-12` cites the constitution's
simplicity principle ("don't add one until there's a second caller").

## 2. Missing coverage (the gaps this initiative closes)

| Gap | Where | Impact |
|---|---|---|
| No Prometheus server / Grafana / exporters | compose files | nothing scrapes the 9 existing endpoints; metrics evaporate on restart |
| Think worker has no health/metrics surface | `think/worker.py`, `scripts/run_think_worker.py` | LLM cost, queue depth, run failures invisible; orchestrator can't detect a hang (**P2-13**, `docs/hardening-backlog.md:664`) |
| `post_commit_worker` likewise | `scripts/run_post_commit_worker.py` | same (**P2-13**) |
| Ollama client unmetered | `lib/embeddings/ollama.py:139` (`_post_with_retry`) | no latency/retry/failure/dim-mismatch visibility on the embedding hot path |
| Gateway request durations log-only | `gateway/middleware.py:121-139` (`duration_ms` structlog field) | no per-route latency histograms / error rates / throughput |
| asyncpg pool unmetered | `lib/shared/db.py:126` | pool saturation (max_size=10) invisible |
| Kafka producer delivery/flush unmetered | `ingestion/kafka/producer.py:116-158` | produce failures and flush latency only visible as `CursorAdvanceFlushFailure` after the fact |
| OAuth refresh unmetered | `services/ingest/integrations/oauth_refresh.py` | silent token-rotation failure is a slow-burn outage |
| Per-source integration counters trapped in-process | 19 `metrics.py` modules | per-provider API outcomes / 429s / install outcomes invisible |
| No percentiles anywhere | all hand-rolled counters | only percentile in the codebase is one resolver p95 gauge |
| `dead_letter_depth` per (tenant, table) missing | `think_trigger_queue` dead letter, post-commit dead letter | **P2-2** (`hardening-backlog.md:556`) |
| `_record_cost` task swallows exceptions | `services/product/rendering/core.py:705-714` | **P2-11** (`hardening-backlog.md:642`) — cost rows silently lost |
| No LLM spend ceilings or burn-rate visibility | `lib/llm/provider.py` | **P2-14** (`hardening-backlog.md:674`) |

## 3. Implementation recommendations (adopted by the design doc)

1. **Keep the hand-rolled exposition convention; centralize it.** Add a small
   dependency-free `lib/observability/` package (registry + Counter / Gauge /
   Histogram with labels and Prometheus text rendering, plus a generic
   health-server reusable outside `services/ingest`). Existing endpoints keep
   their exact output and append the shared registry's families.
2. **Reuse `INGESTION_HEALTH_PORT`** for the think / post-commit workers — the
   compose anchor already sets it in those containers; wiring the server makes
   them scrapeable with zero compose env changes (healthchecks added).
3. **Monitoring stack in `docker-compose.yml`** (prometheus, grafana,
   postgres-exporter with custom queries for the DB-resident metrics,
   kafka-exporter, redis-exporter), config + dashboards as code under
   `observability/`.
4. **Grafana gets two datasources**: Prometheus, and Postgres for the
   `think_run_costs` / `ingestion_failures` substrate (per-tenant spend needs
   the DB — tenant_id must never become a Prometheus label, see §4 of the
   design doc).
5. **kafka-exporter is the consumer-lag source of truth** (per
   group/topic/partition); the in-app `consumer_lag_seconds_last` gauges stay
   as corroboration.

## 4. Backlog items in scope

| Item | Location | Disposition |
|---|---|---|
| P2-2 dead-letter depth not metered/alerted | `docs/hardening-backlog.md:556` | metric via postgres-exporter custom query + Grafana alert (admin endpoint remains open) |
| P2-11 `_record_cost` silent task failure | `docs/hardening-backlog.md:642` | direct code fix (`add_done_callback`) |
| P2-13 worker liveness | `docs/hardening-backlog.md:664` | health server on think + post-commit workers, compose healthchecks |
| P2-14 no cost ceilings | `docs/hardening-backlog.md:674` | partially mitigated: burn-rate alert + spend dashboards (enforcement ceiling remains open) |

Related audit findings: `production-readiness-audit-2026-06-10.md` VULN-013
(silent SAGE degradation — out of scope here, noted in the readiness review)
and its P2 "Scale, Evals, and Observability" exit criterion.
