# Observability Platform — Validation Report (2026-06-11)

End-to-end validation of the Prometheus + Grafana observability initiative
(design: [observability_architecture.md](../architecture/observability_architecture.md),
audit: [observability_audit.md](../architecture/observability_audit.md)).
Validated live on the dev box against the running `fyraliscore` compose
project (the dogfood stack that was already up, image built 2026-06-01).

## 1. Test suites

| Suite | Result |
|---|---|
| New: `lib/observability/tests/` (registry, histogram buckets, health server, pool gauges) | 26 passed |
| New: `lib/embeddings/tests/test_ollama_metrics.py` | 3 passed |
| New: `services/reasoning/think/tests/test_observability_prometheus.py` | 6 passed |
| New: `services/ingest/integrations/tests/test_metrics_export.py` | 3 passed |
| New: `services/app/gateway/tests/test_http_metrics.py` (route-template labels, `unmatched` fallback) | 2 passed |
| Pre-existing contracts: ingestion observability, gateway `/metrics`, webhook metrics text, `lib/embeddings` suite | all green |
| Combined run of the above in one process (shared-registry interference check) | **95 passed** |
| Adjacent regression checks: `lib/shared/tests/test_db*.py` (real Postgres), think tests `-k "metrics or observability or cost"` | 24 passed + 1 pre-existing skip; 32 passed |
| Ruff on every edited file | clean |

## 2. Static validation

* `docker compose config` — exit 0 (full file, all new services).
* `promtool check config` on `observability/prometheus/prometheus.yml` — SUCCESS.
* `promtool check rules` on `recording.yml` — SUCCESS, 7 rules.
* All six dashboard JSONs parse (`json.load`) with expected uids/panel counts
  (system-health 10, ingestion-funnel 9, webhook-ingress 8, embeddings-ollama 8,
  reasoning-llm-cost 12, data-plane-infra 14).
* Post-review coverage expansion added Grafana self-scrape, MinIO native
  cluster scrape, the `InfraScrapeDown` alert, and matching Data Plane
  Infrastructure panels. These are config-validated; live provisioning reload
  is pending the next deliberate observability stack restart.

## 3. Live stack bring-up

`GRAFANA_HOST_PORT=3001 docker compose up -d prometheus grafana
postgres-exporter kafka-exporter redis-exporter` (3001 because the
grafana-*ingestion-source* sandbox container `fyralis_grafana` holds :3000 on
this box — host ports are now parameterized).

| Service | Status |
|---|---|
| company_os_prometheus | Up (healthy), `/-/ready` → "Prometheus Server is Ready." |
| company_os_grafana | Up (healthy), `/api/health` → database ok, v11.1.0 |
| company_os_postgres_exporter | Up, custom queries served |
| company_os_kafka_exporter | Up, lag series flowing |
| company_os_redis_exporter | Up |

## 4. Scrape targets (live)

| Target | Health | Note |
|---|---|---|
| normalizer, observation_writer, dlq_writer, embedding_worker, embedding_backlog, periodic_reconciler `:9300` | **up** | scraped real data immediately (pre-existing endpoints) |
| circuit_breaker `:9300` | **up** | started from the rebuilt image during validation |
| post_commit_worker `:9300` | **up** | NEW endpoint (P2-13); live `post_commit_*` counters + heartbeat verified in-container |
| think_worker `:9300` | validated out-of-band | container needs production secrets (`AUTH_BOOTSTRAP_SECRET` guard) not present on this box; worker run on the host against the live DB instead — `/healthz` 200, `/metrics` served `think_queue_depth` (real DB poll), `db_pool_*{pool="think_worker"}`, heartbeat/uptime, clean SIGTERM-equivalent shutdown |
| gateway `:8000`, shard_fetch, oauth_poller | down (expected) | the running containers are from the **2026-06-01 image**, which predates the `/metrics` public allowlist and the new health-server wiring; current code passes the in-process contract tests. Goes green on the next normal image deploy. |
| postgres / kafka / redis exporters, prometheus self-scrape | **up** | |
| MinIO native scrape, Grafana self-scrape | config-validated | added after the live validation pass; MinIO needs the updated compose env and Grafana needs a provisioning reload/restart before these targets appear live |

Discovered + fixed during live validation: the `scripts/run_think_worker.py` /
`run_post_commit_worker.py` launchers were missing the repo-root `sys.path`
bootstrap every other script launcher has — a latent pre-existing bug
(`ModuleNotFoundError: No module named 'lib'`) that never surfaced because
those containers had never been started. Both launchers now bootstrap like
`run_discord_gateway_worker.py`.

## 5. Metrics flowing (spot queries against live Prometheus)

| Query | Result |
|---|---|
| `fyralis:worker_heartbeat_age_seconds` (recording rule) | 8 series, e.g. `{worker="embedding_worker"} = 1.699` |
| `fyralis_think_queue_pending` (pg-exporter custom) | `0` |
| `fyralis_embedding_backlog_pending` | `0` |
| `fyralis_dead_letter_rows{table_name="model_reeval_dead_letter"}` (**P2-2 metric**) | `0` |
| `fyralis_think_cost_recent_usd_1h` | `0` (this box's `think_run_costs` is empty; e2e runs used a throwaway DB) |
| `kafka_consumergroup_lag_sum` | 3 series (e.g. `ingestion-embedder` lag 0) |
| `ingestion_dlq_writer_unresolved_depth` | `0` |
| `pg_up` / `redis_up` | `1` / `1` |
| `fyralis_dlq_unresolved` | no series — correct: `GROUP BY` returns no rows while `ingestion_failures` has no unresolved rows |

## 6. Grafana provisioning (via API)

* **Dashboards**: all six present in folder *Fyralis* with expected uids.
* **Datasources**: `fyralis-prom` (default) and `fyralis-pg`; a live
  `/api/ds/query` against `think_run_costs` returned a well-formed frame
  (0 rows on this box, as expected).
* **Alert rules**: initial 10 provisioned and evaluating; post-review config now
  contains 11 total after adding `InfraScrapeDown`:
  * `WorkerScrapeDown` — **firing**, a true positive: it correctly caught the
    stale-image targets in §4. The alert pipeline works end to end.
  * `WorkerHeartbeatStale`, `DLQDepthHigh`, `ConsumerLagHigh`,
    `EmbedFailureRatioHigh`, `ThinkQueueBackpressure`, `DBPoolSaturated`,
    `DeadLetterRowsPresent` — inactive, health=ok (evaluating real data).
  * `SignatureFailureSpike`, `LLMSpendBurnRateHigh` — inactive, health=nodata
    (their input series appear once the rebuilt gateway/think worker deploy;
    `nodata` maps to inactive by design for these).
  * `InfraScrapeDown` — config-validated; evaluates after the next Grafana
    provisioning reload.
* **Contact point** `fyralis-ops` (webhook → `INGESTION_ALERT_WEBHOOK_URL`,
  placeholder default) + notification policy grouping by alertname.

## 7. Caveats / operator notes

1. **Stale-image targets**: gateway/shard_fetch/oauth_poller/think_worker show
   `down` until the next image build+deploy (`docker compose build && up -d`).
   `WorkerScrapeDown` will keep firing until then — that is the alert doing
   its job.
2. `.env.production` is required by compose; on this box it was absent (an
   empty placeholder was created for validation). The think worker
   deliberately refuses to boot without `AUTH_BOOTSTRAP_SECRET` in prod env.
3. Grafana host port: use `GRAFANA_HOST_PORT` / `PROMETHEUS_HOST_PORT` env to
   avoid collisions (the grafana-source sandbox holds :3000 on dev boxes).
4. Grafana carries a read-capable Postgres datasource — keep it
   loopback-bound (default) or behind the TLS edge + auth proxy.
5. No screenshots were captured (headless validation); dashboard/alert state
   was verified through the Grafana HTTP API instead.
6. Post-review MinIO/Grafana self-scrape coverage requires restarting the
   observability stack and MinIO so the new compose env and provisioning files
   are active.
