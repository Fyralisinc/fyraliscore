# Services — Coordination Map

The single table of every service/process in Fyralis Core: what it does, what it
depends on, and where to find it. Derived from the source tree and
`docker-compose.yml`. For how the layers fit together, see the
[architecture overview](architecture/index.md).

**Type legend:** `process` = standalone deployable (compose service or
`scripts/run_*`); `in-gateway` = runs inside the gateway process; `consumer` =
Kafka consumer worker; `worker` = `services/workers/*` package (mostly
**undeployed** — see [Workers](architecture/workers.md)); `library` = imported,
not run; `infra` = data store / container.

## Services

| Service | Type | Layer | Responsibility (one line) | Inbound → Outbound | Entry point / location |
|---------|------|-------|---------------------------|--------------------|------------------------|
| Gateway | process | [app](architecture/app.md) | HTTP/WS edge: auth, rate-limit, route to ingest/product/reasoning. | UI + webhooks + OAuth → ingest, domain, product, Postgres | `uvicorn services.app.gateway:app` |
| Realtime dispatcher | in-gateway | [app](architecture/app.md) | `LISTEN observations_new` → fan events to WS subscribers. | `WS /stream` ← Postgres NOTIFY → WS clients | `services/app/realtime/dispatcher.py` |
| Webhook router | in-gateway | [app](architecture/app.md) | Verify provider signature, resolve tenant, ingest inline or via Kafka. | provider webhooks → `ingest()` / Kafka raw | `services/app/webhooks/router.py` |
| Ingestion (uniform path) | in-gateway / lib | [ingest](architecture/ingest.md) | Normalize signal → deduped observation → `T1` trigger. | gateway/discord/synthetic/Kafka → observations, `think_trigger_queue` | `services/ingest/ingestion/core.py` |
| Normalizer | consumer | [ingest](architecture/ingest.md) | Kafka Path B: raw envelope + S3 body → `NormalizedEnvelope` (no DB). | `ingestion.raw.{source}` → `ingestion.normalized.{source}` | `python -m services.ingest.ingestion.normalizer` |
| Observation writer | consumer | [ingest](architecture/ingest.md) | Kafka Path A: normalized → `ingest_from_draft` (full-mode tenants). | `ingestion.normalized.*` → observations | `python -m services.ingest.ingestion.writers` |
| Embedding worker | consumer | [ingest](architecture/ingest.md) | Consume `ingestion.embedding.*` → Ollama → write vectors. | Kafka → Ollama, Postgres | compose `embedding_worker` |
| Embedding backlog | consumer | [ingest](architecture/ingest.md) | Re-embed `embedding_pending` observations (Redis rate-limited). | Postgres scan → Ollama, Redis | compose `embedding_backlog` |
| DLQ writer | consumer | [ingest](architecture/ingest.md) | Persist failed payloads from `ingestion.dlq.*`. | Kafka → `ingestion_failures` | compose `dlq_writer` |
| Shard fetch | process | [ingest](architecture/ingest.md) | Backfill: paginated source-API fetch → raw lane + S3. | source APIs → Kafka raw, S3 | compose `shard_fetch` / `python -m …workflows` |
| OAuth poller | process | [ingest](architecture/ingest.md) | Refresh OAuth tokens; enqueue onboarding. | `provider_installations` → provider APIs | compose `oauth_poller` |
| Tenant / Source onboarding | process | [ingest](architecture/ingest.md) | Idempotent tenant + source bootstrap. | `onboarding_triggers` → tenants/installations | compose `tenant_onboarding`, `source_onboarding` |
| Reconciler / Periodic reconciler | process | [ingest](architecture/ingest.md) | Dedup/merge model inserts; scheduled per-source gap detection. | onboarding runs / timers → `reconciliation_events` | compose `reconciler`, `periodic_reconciler` |
| Discord gateway worker | process | [ingest](architecture/ingest.md) | Durable WSS to Discord → observations. | Discord WSS → `ingest()` | `scripts/run_discord_gateway_worker.py` |
| Telegram gateway worker | process | [ingest](architecture/ingest.md) | Durable MTProto updates connection → observations (gateway-style; single-instance lease). | Telegram MTProto → `ingest()` / Kafka raw | `scripts/run_telegram_gateway_worker.py` |
| Gmail watch / history | process | [ingest](architecture/ingest.md) | Register Pub/Sub watches; full + delta history sync. | Gmail API → Kafka raw / Postgres | `scripts/run_gmail_{watch_scheduler,history_poller}.py` |
| GitHub intel worker | process | [ingest](architecture/ingest.md) | Per-repo FSM + causal enrichment; code-intel reindex. | `github_intel_queue` → `github_signal_enrichment`, Kafka | `scripts/run_github_intel_worker.py` |
| Think worker | process | [reasoning](architecture/reasoning.md) | Drain `think_trigger_queue`/`model_reeval_queue`; run `think()`. | queues → domain mutations, LLM | `scripts/run_think_worker.py` |
| Post-commit worker | process | [reasoning](architecture/reasoning.md) | Drain `pending_post_commit_actions` (cascades, reevals, alerts). | post-commit queue → Postgres | `scripts/run_post_commit_worker.py` |
| Topology sweeper | process | [reasoning](architecture/reasoning.md) / [workers](architecture/workers.md) | Re-run latent topology over high-activation Models → candidates + `T4`. | high-activation Models → `relationship_candidates` | `scripts/run_topology_sweeper.py` *(not in compose)* |
| Contestability | in-gateway | [reasoning](architecture/reasoning.md) | First-person Model contestation → confidence override + `T3`. | `POST /contest/{model_id}` → Models, queue | `services/reasoning/contestability/service.py` |
| Domain repositories | library | [domain](architecture/domain.md) | System-of-record: observations/models/acts/resources/actors/aliases. | gateway, reasoning, ingest, workers → Postgres | `services/domain/*/repo.py` |
| Access control | library | [platform](architecture/platform.md) | Five-layer `can_read` authz; `@requires_access`; materialized views. | gateway, realtime, retrieval → domain reads | `services/platform/access_control/` |
| Execution routing & inquiry | library | [platform](architecture/platform.md) | Deterministic route gate (shadow) + adaptive inquiry retrieval loop. | Think (deep) / Query (fast) → retrieval, LLM | `services/platform/execution/` |
| Sage synthesis loop | library | [reasoning](architecture/reasoning.md) | Query-conditioned reader, structural features, discovery memory, and topology optimization for adaptive synthesis. | inquiry + Think → model graph traces, discovery tables | `services/reasoning/sage/` |
| Greeting / CEO view | in-gateway | [product](architecture/product.md) | Pre-compute + serve cached CEO home view; WS stream. | scheduler + UI → `view_ceo_cache`, rendering | `services/product/greeting/` |
| Rendering | in-gateway | [product](architecture/product.md) | LLM prose with voice-rule retry + cost tracking. | greeting + query → `lib.llm`, `view_render_costs` | `services/product/rendering/` |
| Query (Ask) | in-gateway | [product](architecture/product.md) | Classify → strategy → retrieve → render a conversation turn. | `POST /view/ceo/ask` → retrieval, rendering | `services/product/query/` |
| Today / Recommendations / Decision-deltas | in-gateway | [product](architecture/product.md) | Today briefing, action list (act/dismiss/ratify), proposed-change object. | gateway → domain acts/resources, SSE | `services/product/{today,recommendations,decision_deltas}/` |
| Forecasts / History / Model-trace | in-gateway | [product](architecture/product.md) | Predictions/calibration page, ledger, Model-graph trace. | gateway → predictions, observations, `model_edges` | `services/product/{forecasts,history,model_trace}/` |
| Workers (anomaly, entity, calibration, deadline, precipitation, edge-drift, maintenance) | worker | [workers](architecture/workers.md) | Substrate maintenance + trigger enqueue — **mostly undeployed**. | Postgres polls → `think_trigger_queue`, substrate | `services/workers/*` *(no compose service)* |
| LLM provider / Embeddings / shared | library | [lib](architecture/lib.md) | Structured-output LLM, embeddings, DB/IDs/errors/types. | all services → SDKs, Ollama, Postgres | `lib/llm`, `lib/embeddings`, `lib/shared` |

## Data stores & infra

| Store | Type | Role | Location |
|-------|------|------|----------|
| PostgreSQL 16 + pgvector | infra | Substrate, durable queues, cache, audit, 768-d vector search. | compose `postgres` |
| Ollama | infra | Local embeddings (`nomic-embed-text`, 768-d). | compose `ollama` |
| Redis | infra | Rate-limiter token buckets; optional cache. | compose `redis` |
| Kafka (KRaft) | infra | Per-source ingestion lanes (`ingestion.{raw,normalized,embedding,dlq}.{source}`). | compose `kafka` |
| S3 / MinIO | infra | Raw-tier object storage (`fyralis-raw`). | compose `minio` |
| Prometheus | infra | Scrapes every worker/gateway `/metrics` + exporters; 15d retention. | compose `prometheus`, config `observability/prometheus/` |
| Grafana | infra | Provisioned dashboards + alert rules (folder *Fyralis*). | compose `grafana`, config `observability/grafana/` |
| postgres-exporter | infra | Postgres stats + custom gauges (DLQ depth, think queue, dead-letter rows, embedding backlog). | compose `postgres-exporter`, queries `observability/postgres-exporter/queries.yaml` |
| kafka-exporter | infra | Consumer-group lag / topic metrics (the lag source of truth). | compose `kafka-exporter` |
| redis-exporter | infra | Redis memory/clients/commands metrics. | compose `redis-exporter` |

!!! note "Demo and UI moved to the overlay"
    The **demo** subsystem (company picker, per-session tenants, simulator, SSE)
    and the **UI** (React/Vite frontend) — plus the nginx-proxy / acme-companion
    TLS edge — are no longer in core. They live in the separate **fyraliscore-demo**
    overlay repo. The demo mounts back into the gateway at runtime via the gateway
    extension seam (`services/app/gateway/extensions.py`, entry-point group
    `company_os.gateway_extensions`); core imports nothing from the overlay.

> See [Runtime & Data Plane](architecture/data-plane.md) for how these processes
> communicate (HTTP/WS, asyncpg, durable DB queues, Kafka, `LISTEN/NOTIFY`).
