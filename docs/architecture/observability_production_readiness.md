# Observability Platform — Production Readiness Review (2026-06-11)

Phase-12 re-audit after the Prometheus + Grafana initiative
([design](observability_architecture.md) ·
[phase-1 audit](observability_audit.md) ·
[validation report](../validation/observability_validation_report.md)).

## 1. Hardening-backlog items

| Item | Status after this initiative | Evidence |
|---|---|---|
| **P2-13** — worker liveness has no heartbeat or `/health` | **Closed.** Every long-running compose app worker now exposes `/healthz` (503 on wedged loop) + `/metrics`, reusing the compose-anchored `INGESTION_HEALTH_PORT`; compose healthchecks are attached to every scraped worker target. | post_commit verified live in-container (healthy, counters scraped); think worker verified against the live DB (validation report §4); post-review expansion adds the same launcher helper to live source, GitHub Intel, and SAGE/retrieval-memory workers |
| **P2-2** — dead-letter tables not metered or alerted | **Metric + alert halves closed.** `fyralis_dead_letter_rows{table_name}` (postgres-exporter custom query over `model_reeval_dead_letter` + dead-lettered `pending_post_commit_actions`) + the `DeadLetterRowsPresent` Grafana alert. The proposed `/api/admin/dead_letter` endpoint and operator runbook remain **open**. | metric returning live data (validation §5); alert evaluating (validation §6) |
| **P2-11** — `_record_cost` task swallows exceptions | **Closed.** `loop.create_task(_do_insert())` now has an `add_done_callback` logging `rendering.cost_record_task_failed` (covers cancellation-time errors the inner try/except can't see). | `services/product/rendering/core.py` |
| **P2-14** — no LLM cost ceilings | **Partially mitigated.** Spend is now observable (`think_llm_cost_usd_total`, `fyralis:llm_spend_usd_per_hour`, per-tenant SQL panels) and alertable (`LLMSpendBurnRateHigh`, default $5/h — tune to budget). Enforcement (per-call `LLM_MAX_TOKENS_PER_CALL`, per-tenant 429 circuit breaker in `lib/llm/provider.py`) remains **open**. | dashboards + alert provisioned (validation §6) |

## 2. Mandate coverage

| Area | Status |
|---|---|
| Prometheus / Grafana / postgres+kafka+redis exporters, MinIO native scrape, and Grafana self-metrics in compose | ✅ config-validated; core exporter stack live-validated |
| Gateway: verification failures, resolver, cache, Kafka cutover, request latency histograms, throughput, error rates | ✅ (latency/throughput/error-rate are new `http_requests_total` + `http_request_duration_seconds` with route-template labels) |
| Ingestion workers: consumed/produced, parse failures, transform duration, lag, writes, dedup, partition autocreate, DLQ depth, embeds, backlog, reconciliation gaps, breaker events | ✅ exposed (pre-existing counters) + dashboarded; lag source of truth = kafka-exporter |
| Think worker first-class target (runs, failures, trigger kinds, latency, queue depth, cascade depth, validation drops, LLM cost/tokens/calls, reconcile decisions) + health endpoint | ✅ |
| Ollama instrumentation (latency histogram, retry/success/failure/dim-mismatch counters, `{model, operation, status}`) | ✅ |
| DB pool utilization, acquire-wait histogram, saturation | ✅ (`db_pool_*`, `fyralis:db_pool_saturation`, `DBPoolSaturated` alert) |
| Kafka producer flush latency + delivery success/failure | ✅ |
| OAuth refresh attempts/successes/failures | ✅ (`oauth_refresh_*`; exposed via the oauth_poller health server) |
| **Retrieval layer (retrieval latency, pgvector timings, query/failure counts)** | ✅ Closed after the adversarial-review pass flagged it: `retrieval_stage_duration_seconds{stage}` + `retrieval_stage_total{stage,status=ok\|skipped}` at the `_append_pathway_timing` chokepoint in `primary.py` (per-pathway latency + failure counts), `retrieval_pathway_inner_seconds{stage}` for intra-pathway stages, and explicit `retrieval_pgvector_query_seconds{query=ann\|exact_fallback}` + `retrieval_pgvector_queries_total` around pathway B's two `conn.fetch` calls. Debug `notes["timings"]` behavior unchanged. |
| Per-source integration metrics un-trapped from logs | ✅ (`metrics_export.py` aggregator; `integration_*` normalized namespace + verbatim GitHub/Discord-gateway families) |
| Live source + auxiliary app workers (Telegram, Signal, Gmail, Google Calendar/Drive, GitHub Intel, SAGE structural/topology, relationship ontology) | ✅ generic `/healthz` + `/metrics`, DB pool gauges where they own pools, scrape targets, and compose healthchecks |
| Six dashboards | ✅ provisioned + API-verified |
| Alerting (8 required) | ✅ 11 provisioned; app + infra scrape-down coverage separated; `WorkerScrapeDown` produced a live true positive |

## 3. Adversarial-review findings and their resolution

A three-lens review (correctness / runtime-safety / mandate-completeness) ran
over the full diff. Completeness findings, all addressed in-session:

* **Retrieval metrics missing (major)** → implemented (see §2 row).
* **Gateway + workflow pools unregistered (minor)** → `create_gateway_pool`
  now registers `db_pool_*{pool="gateway"}`; `make_workflow_pool` registers
  `{pool="workflow"}` for every workflow process.
* **discord_gateway_worker counters unreachable (minor)** → the launcher now
  starts the health server; `discord_gateway_*` families render via the
  integration collector.
* **tenant_onboarding / source_onboarding / reconciler /
  feels_onboarded_monitor unscraped (minor)** → all four wired via the new
  `start_workflow_health()` helper in `workflows/runtime.py`; scrape targets
  added.
* **Live source + auxiliary app workers unscraped (follow-up)** → Telegram,
  Signal, Gmail, Google Calendar/Drive, GitHub Intel, SAGE structural/topology,
  and relationship ontology launchers now share `scripts/worker_observability.py`
  for heartbeat, metrics, signal handling, and DB pool gauges; scrape targets
  and compose healthchecks added (now 28 worker scrape targets; 35 static
  Prometheus targets including Prometheus/exporters/gateway/MinIO/Grafana).
* **MinIO + Grafana self-scrape missing (follow-up)** → MinIO's native cluster
  metrics endpoint and Grafana's `/metrics` are now scraped; infra scrape-down
  alert and Data Plane Infrastructure panels added.
* **Think latency as rolling-window quantile gauges, not histograms (nit)** —
  accepted and documented (the gateway has true histograms; the think worker's
  gauges sit on a bounded sample window).

## 4. Remaining gaps / recommended future work

1. **P2-2 second half** — `/api/admin/dead_letter` endpoint + operator runbook.
2. **P2-14 enforcement** — budget ceilings in `lib/llm/provider.py`; the
   burn-rate alert is monitoring, not control.
3. **Redeploy to light up remaining targets** — the dev box has stale images
   for several app workers; `WorkerScrapeDown` fires until the stack is rebuilt
   and relaunched with the production env (think worker additionally needs the
   real `.env.production`).
4. **VULN-013 (production-readiness audit)** — SAGE reader degraded-source
   telemetry is still debug-only counters; out of scope here, unchanged.
5. **Alert thresholds are first-pass defaults** (DLQ 25, lag 1000, spend $5/h)
   — tune after a week of real data; recording rules make this a YAML-only
   change.
6. **Multiprocess caveat** — each process owns its registry; if a service is
   ever scaled to N replicas behind one DNS name, per-target scraping (not the
   single static target) must be configured.

## 5. Observability score

Phase-1 audit baseline: metrics existed but nothing collected them — call it
**3/10** (export-only, no percentiles, no dashboards, no alerting, blind LLM
spend). After this initiative: collection, dashboards, alerting, histograms,
DB-substrate lifting, and worker liveness are in place and live-validated —
**8/10**. The remaining two points are budget *enforcement*, threshold tuning
under real load, the P2-2 operator endpoint/runbook, and the stale-image
redeploy on the dogfood box.
