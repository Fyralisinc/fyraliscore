# End-to-End System Architecture

Fyralis Core is a backend-first organizational intelligence runtime. It ingests
signals from company systems, normalizes them into tenant-scoped observations,
stores them in PostgreSQL with vector indexes, reasons over them with retrieval
and LLM-backed Think workers, and exposes the resulting operating model through
FastAPI product APIs.

The production system is composed of a FastAPI gateway, integration ingress,
Kafka/S3 ingestion data plane, PostgreSQL + pgvector substrate, embedding
services, reasoning workers, product services, and observability services. The
UI/demo overlay is separate from core and talks to the gateway over HTTP and
WebSocket/SSE-style realtime surfaces.

## 1. High-Level Runtime

At a high level, data moves through the system like this:

```text
External systems / UI
  -> FastAPI gateway and live source workers
  -> inline ingest or S3 + Kafka raw ingestion
  -> normalization and observation writing
  -> PostgreSQL domain substrate
  -> durable Think queues
  -> retrieval + LLM reasoning
  -> model/act/resource updates
  -> post-commit side effects
  -> product API responses and realtime updates
```

The most important runtime boundary is PostgreSQL. It is both the durable system
of record and the queue substrate for reasoning and post-commit work.

## 2. External Entry Points

### 2.1 UI / Demo Overlay

The user-facing UI is not owned by the core runtime. Core exposes APIs consumed
by the demo/UI overlay.

Responsibilities:

- Calls product APIs such as Today, model, forecasts, recommendations, Ask, and
  CEO home.
- Opens realtime/streaming surfaces where configured.
- Provides demo/session behavior through overlay-contributed gateway extensions.

Important boundary:

- Core must not import the overlay. The overlay plugs into core through gateway
  extension entry points.

### 2.2 Third-Party Systems

External sources include Slack, GitHub, Gmail, Google Calendar, Google Drive,
Jira, Discord, Telegram, Signal, Notion, finance systems, HR systems, recruiting
systems, and other webhook or polling integrations.

They enter through two broad modes:

- Webhooks and OAuth callbacks handled by the FastAPI gateway.
- Live/polling workers that run as separate long-lived processes.

## 3. Gateway Layer

Code location:

- `services/app/gateway`
- `services/app/webhooks`
- `services/app/realtime`

The gateway is the main FastAPI application. It is responsible for request
transport, middleware, dependency wiring, route mounting, auth, rate limiting,
integration state, realtime dispatch, and startup/shutdown lifecycle.

### 3.1 Gateway App Factory

The gateway app factory creates and wires the main runtime dependencies:

- asyncpg pool for PostgreSQL access.
- `ActorRepo` for actor/session/source identity lookup.
- `EntityAliasRepo` for entity resolution.
- Ollama embedding client when configured.
- Rate limiter.
- Integration runtime state.
- GitHub gateway state.
- Realtime dispatcher.
- CEO-view scheduler and cache wiring.
- Gateway extension hooks.

### 3.2 Middleware

Gateway middleware provides:

- Request context and structured logging.
- Bearer-token authentication.
- Tenant and actor context binding.
- Rate limiting by tenant/actor.
- Public path bypasses for health checks, webhooks, and selected auth/bootstrap
  surfaces.

### 3.3 Route Families

Mounted route families include:

- Health, metrics, auth session, and basic ingest routes.
- Webhook routes under `/webhooks/*`.
- Integration/OAuth install routes.
- Substrate routes for observations, models, commitments, goals, decisions, and
  resources.
- Product routes for Today, recommendations, forecasts, history, model trace,
  map/model pages, decision deltas, conversations, Ask, and rendering.
- Optional feature routes such as finance and Slack panels.
- Overlay-contributed demo/simulation routes when the overlay package is
  installed.

## 4. Integration and Ingestion Layer

Code location:

- `services/ingest/integrations`
- `services/ingest/ingestion`
- `services/app/webhooks`

This layer converts raw provider data into normalized observations.

### 4.1 Webhook Router

The webhook router handles provider callbacks. Its flow is:

1. Capture the raw request body.
2. Enforce payload size limits.
3. Identify the provider verifier.
4. Parse JSON best-effort.
5. Resolve provider installation to a tenant.
6. Load provider secrets.
7. Verify request signatures.
8. Route to inline ingest or the Kafka/S3 ingestion path.

For cutover-enabled tenants and providers, the gateway writes the raw payload to
S3/MinIO, publishes a Kafka raw envelope, flushes the producer, and returns a
durable acknowledgement. If Kafka/S3 publication fails, the gateway falls back
to inline ingest so the external provider is not impacted.

### 4.2 Live Source Workers

Long-running workers ingest sources that do not fit a simple webhook-only
model. Examples include:

- Discord gateway worker.
- Telegram MTProto worker.
- Signal gateway worker.
- Gmail watch scheduler and history poller.
- Google Calendar live poller and watch scheduler.
- Google Drive live poller and watch scheduler.
- GitHub intelligence worker.

These workers generally read provider APIs, maintain source-specific cursors or
state, write raw payloads to S3/MinIO, and publish raw envelopes to Kafka.

### 4.3 Raw Tier

The raw tier is the first durable ingestion stage.

Subcomponents:

- S3/MinIO raw object storage.
- Kafka `ingestion.raw.<source>` topics.
- Raw envelopes containing tenant, source, ingress kind, raw object key,
  timestamps, content hash, and metadata.

Purpose:

- Preserve the original provider payload.
- Decouple external acknowledgements from downstream normalization.
- Support replay, reconciliation, and source isolation.

### 4.4 Normalizer Worker

The normalizer consumes raw Kafka envelopes and fetches raw payloads from S3.
It then dispatches the provider payload through the existing handler registry to
produce a normalized envelope.

Important invariant:

- The normalizer intentionally does not access PostgreSQL. It is a pure
  raw-to-normalized transform.

Output:

- Kafka `ingestion.normalized.<source>` messages carrying normalized draft
  fields.
- Kafka DLQ messages for parse, invariant, or unsupported-shape failures.

### 4.5 Observation Writer

The observation writer consumes normalized envelopes and persists observations.
It reconstructs an `ObservationDraft` and calls the shared ingest-from-draft
path.

Responsibilities:

- Read tenant ingestion flags.
- Preserve shadow/no-op behavior for explicitly disabled Kafka-path tenants.
- Resolve actors and entities.
- Compute or mark embeddings pending.
- Insert observations into PostgreSQL.
- Enqueue Think triggers.
- Publish embedding retry requests when embedding is pending.
- DLQ permanent failures.
- Retry transient failures by allowing Kafka redelivery.

### 4.6 Inline Ingest Path

Inline ingest is the fallback and local/simple path. It skips Kafka/S3 and calls
the ingest core directly from the gateway.

Inline ingest performs the same core domain operations:

1. Handler extracts an `ObservationDraft`.
2. Observation UUID is preassigned.
3. Actor is resolved.
4. Entity aliases are resolved.
5. Embedding is computed or marked pending.
6. Observation is inserted.
7. A T1 Think trigger is enqueued.

## 5. Domain Substrate

Code location:

- `services/domain`
- `db/migrations`

The domain layer owns persisted business state and repository logic. It is the
stable substrate on which reasoning and product surfaces operate.

### 5.1 Actors

Actors represent people, agents, and system participants.

Subcomponents:

- `actors`
- `actor_identity_mappings`
- `actor_sessions`
- `ActorRepo`

Responsibilities:

- Resolve source identities to internal actors.
- Store login/session auth state.
- Provide actor context for product and reasoning flows.

### 5.2 Observations

Observations are append-oriented source signals.

Subcomponents:

- `observations`
- monthly partitions by `occurred_at`
- pgvector embedding index
- source/external-id deduplication
- post-commit observation notifications

Responsibilities:

- Store raw-normalized source facts.
- Preserve content text and raw content JSON.
- Track trust tier, channel, actor, cause chain, and mentioned entities.
- Support semantic, temporal, source, actor, and entity retrieval.

### 5.3 Entity Aliases

Entity aliases map text phrases and source references to canonical entity
references.

Subcomponents:

- `entity_aliases`
- `EntityAliasRepo`
- deferred entity resolver worker

Responsibilities:

- Fast-path exact alias resolution during ingest.
- Store canonical and non-canonical aliases.
- Support later LLM-assisted entity resolution for unresolved phrases.

### 5.4 Models

Models are the system's evolving beliefs, propositions, situations, and memory
nodes.

Subcomponents:

- `models`
- model status notes
- model signal readings
- model scope sidecars
- model composition members
- model edges
- relationship candidates

Responsibilities:

- Store propositions with confidence, activation, lifecycle state, scope, and
  embedding.
- Represent relationships among models.
- Support graph retrieval, semantic retrieval, status transitions, and
  recommendations.

### 5.5 Acts

Acts represent executable organizational state.

Subcomponents:

- `goals`
- `commitments`
- `decisions`
- commitment contributors
- act graph tables such as contributes-to, depends-on, and constrained-by

Responsibilities:

- Track decisions, goals, commitments, owners, due dates, contributors, and
  state machines.
- Provide structured context to reasoning and product surfaces.

### 5.6 Resources

Resources represent organizational assets and resource movement.

Subcomponents:

- `resources`
- `resource_transactions`
- `resource_deployments`
- `customer_commitments`

Responsibilities:

- Track assets, deployment, utilization, transactions, customer/revenue
  commitments, and resource overlays.

### 5.7 Durable Queues and Audit Tables

Important queue and observability tables include:

- `think_trigger_queue`
- `model_reeval_queue`
- `model_reeval_dead_letter`
- `pending_post_commit_actions`
- `applied_triggers`
- `dedup_keys_seen`
- `think_runs`
- `think_run_costs`
- `think_run_artifacts`
- `audit_events`
- `reconciliation_events`

These tables make asynchronous reasoning durable, retryable, auditable, and
inspectable.

## 6. Embedding Layer

Code location:

- `lib/embeddings`
- `services/ingest/ingestion/writers/embedding_worker`
- `services/ingest/ingestion/recovery/embedding_backlog`

Embeddings are used for semantic retrieval over observations, models, code
intelligence records, and entity aliases.

Subcomponents:

- Ollama embedder, defaulting to `nomic-embed-text`.
- OpenAI embedder option.
- Kafka embedding worker.
- Database backlog embedding drainer.
- pgvector columns and HNSW indexes.

Flow:

1. Ingest attempts to embed content text.
2. If embedding fails, the row is stored with `embedding_pending = TRUE`.
3. Kafka embedding worker or backlog scanner retries embedding.
4. Successful embeddings are written back to PostgreSQL.
5. Retrieval uses vector similarity where available.

## 7. Reasoning Layer

Code location:

- `services/reasoning`
- `services/platform/execution`

The reasoning layer converts observations and existing memory into updates to
models, acts, resources, relationships, predictions, and product-relevant state.

### 7.1 Think Worker

The Think worker polls durable queues and processes triggers.

Input queues:

- `think_trigger_queue`
- `model_reeval_queue`

Trigger types:

- T1: new event/observation.
- T2: prediction or belief update.
- T3: anomaly-triggered reasoning.
- T4: maintenance, reevaluation, pattern, topology, or background work.
- T6: legacy topology event compatibility.

Responsibilities:

- Lease queue rows with `FOR UPDATE SKIP LOCKED`.
- Enforce per-tenant concurrency.
- Apply retry and dead-letter policies.
- Build trigger context from queue payloads.
- Call the Think pipeline.
- Mark queue rows complete or failed.

### 7.2 Retrieval and Inquiry

Retrieval gathers relevant context before an LLM reasoning call.

Subcomponents:

- Execution inquiry runtime.
- Legacy retrieval pathway resolver.
- Retrieval assembler.
- Context packet compiler.
- Access-control filtering.
- MMR and graph-anchor selection.

Retrieval pathways include:

- Structural overlap by entities, actors, goals, commitments, resources, and
  scope.
- Semantic vector similarity.
- Temporal recency.
- Pattern/background retrieval.
- Memory graph traversal over model edges.

Output:

- A bounded context bundle containing observations, models, acts, resources,
  bridge data, selected graph anchors, and omission/debug notes.

### 7.3 LLM Provider Layer

Code location:

- `lib/llm/provider.py`

Responsibilities:

- Provide structured-output LLM calls.
- Support configured providers such as Codex/OpenAI/Anthropic/DeepSeek.
- Track usage, tokens, model names, and cost attribution.
- Return Pydantic-shaped reasoning outputs for validation.

### 7.4 Think Pipeline

The Think pipeline performs:

1. Start a Think run record.
2. Optionally skip already-applied triggers.
3. Retrieve relevant context.
4. Build reasoning prompt and structured schema.
5. Call the LLM provider.
6. Validate raw reasoning output.
7. Acquire region locks.
8. Apply validated diffs to models, acts, resources, predictions, and edges.
9. Record metrics, costs, debug artifacts, and context-use telemetry.
10. Enqueue post-commit actions.

### 7.5 Validation and Application

Validation protects the domain substrate from invalid reasoning output.

Responsibilities:

- Check schema correctness.
- Ensure operations are in the allowed reasoning region.
- Prevent duplicate trigger application.
- Enforce domain invariants.
- Retry with expanded retrieval if the LLM references out-of-region state.

The applier writes accepted diffs to PostgreSQL and records idempotency in
`applied_triggers`.

### 7.6 Post-Commit Worker

Post-commit actions are written inside the same transaction that applies the
Think diff. A separate worker processes them after commit.

Action kinds include:

- Publish anomalies.
- Schedule predictions.
- Broadcast realtime updates.
- Invalidate metrics.
- Discover model edges.

Why this exists:

- If the process crashes after applying a Think diff but before side effects,
  the side effects remain durable in `pending_post_commit_actions` and can be
  retried.

## 8. Product Layer

Code location:

- `services/product`

The product layer composes domain and reasoning state into API payloads.

### 8.1 CEO Home / Greeting

Subcomponents:

- greeting snapshot builder
- cache repository
- scheduler
- stream manager
- viewer state repository

Responsibilities:

- Build cached CEO home payloads.
- Store greeting, query grid, cards, status, and close-line content.
- Track viewer last-seen timestamps.
- Support manual refresh and realtime streams.

### 8.2 Today and Decision Deltas

Responsibilities:

- Show changes that need review.
- Present evidence for proposed deltas.
- Accept, delegate, contest, snooze, or add context.
- Promote accepted deltas into domain state.

### 8.3 Recommendations

Responsibilities:

- Present model-backed recommendations.
- Track actions, dismissals, triage, and watches.
- Emit recommendation events for downstream UI/demo surfaces.

### 8.4 Ask / Query

Responsibilities:

- Answer user questions using retrieval and synthesis.
- Support card-scoped context.
- Provide conversation turn actions such as follow-up, save, and done.
- Reuse platform execution and SAGE reader paths.

### 8.5 Forecasts

Responsibilities:

- Create and display predictions.
- Track prediction signals.
- Show patterns, accuracy, details, and scenario answers.
- Feed calibration/hit-rate surfaces.

### 8.6 Model, Map, Trace, and History

Responsibilities:

- Show model overview and item detail.
- Explain supports, dependencies, traces, and evidence.
- Expose topology/map events and relationship context.
- Provide historical ledger-style views of system changes.

### 8.7 Rendering

Responsibilities:

- Generate user-facing prose from structured product data.
- Enforce voice/style checks.
- Record render cost and latency.

## 9. Background Worker Families

Code location:

- `services/workers`
- selected `scripts/run_*.py`

Worker families include:

- Entity resolver: resolves unresolved phrases into canonical entities.
- Anomaly processor: consumes anomaly staging and emits reasoning triggers.
- Calibration updater: updates prediction and model calibration stats.
- Deadline resolver: resolves time-based commitments or overdue state.
- Topology sweeper: refreshes latent relationship candidates.
- SAGE structural feature worker: computes structural graph features.
- SAGE topology optimizer: optimizes topology/readability structures.
- Housekeeper: scheduled lifecycle and maintenance tasks.
- Relationship ontology proposal worker: proposes relationship ontology changes.
- GitHub intel worker: tracks ordered GitHub state and code-intel enrichment.

These workers mostly communicate through PostgreSQL tables and durable queues,
with Kafka used primarily for ingestion and embedding pipelines.

## 10. Infrastructure Components

### 10.1 PostgreSQL + pgvector

PostgreSQL stores:

- Tenant data.
- Observations.
- Models and graph relationships.
- Acts and resources.
- Integration state.
- Durable queues.
- Product caches.
- Audit and cost telemetry.
- Forecasts and calibration.
- Ingestion flags and failures.

pgvector enables semantic search over vector columns.

### 10.2 Kafka

Kafka carries ingestion pipeline events:

- Raw envelopes.
- Normalized envelopes.
- DLQ envelopes.
- Embedding retry requests.
- Traffic/cutover signals.

Kafka allows source isolation, replay, backpressure management, and decoupling
of external provider acknowledgements from database writes.

### 10.3 S3 / MinIO

Object storage keeps raw provider payloads.

Purpose:

- Preserve original input bytes.
- Avoid putting large raw bodies directly in Kafka.
- Support replay and normalization reprocessing.

### 10.4 Redis

Redis is used for selected operational coordination, including:

- Rate limiting in ingestion-related paths.
- Singleton/leader leases for certain live gateway workers.
- Embedding backlog rate limiting.

### 10.5 Ollama

Ollama provides local embeddings for development and deployable single-box
environments. The default embedding model is pulled during compose startup.

### 10.6 Observability

Observability services include:

- Prometheus.
- Grafana.
- Postgres exporter.
- Kafka exporter.
- Redis exporter.
- In-process `/healthz` and `/metrics` endpoints for workers.

The system also persists rich internal telemetry in PostgreSQL, including Think
runs, costs, artifacts, audit events, reconciliation events, and render costs.

## 11. End-to-End Data Flows

### 11.1 Product Read Flow

```text
UI request
  -> FastAPI gateway
  -> middleware resolves request, auth, tenant, and rate limit
  -> product router
  -> domain/reasoning repositories
  -> PostgreSQL
  -> response payload
  -> UI
```

Example:

1. The UI calls `GET /view/ceo/home`.
2. Gateway authenticates or applies the configured dogfood default tenant.
3. CEO home router reads cached rows from `view_ceo_cache`.
4. Viewer state is updated.
5. The response returns greeting, query grid, cards, close line, status, and
   viewer-state metadata.

### 11.2 Inline Ingestion Flow

```text
Webhook request
  -> gateway webhook router
  -> signature verification
  -> tenant resolution
  -> ingest()
  -> handler creates ObservationDraft
  -> actor/entity resolution
  -> embedding attempt
  -> observations insert
  -> think_trigger_queue insert
```

This path is used as a fallback when Kafka/S3 cutover is unavailable or disabled
for a tenant/provider.

### 11.3 Kafka Ingestion Flow

```text
Webhook/live worker
  -> S3 raw payload write
  -> Kafka raw envelope
  -> normalizer worker
  -> S3 raw payload read
  -> provider handler
  -> Kafka normalized envelope
  -> observation writer
  -> PostgreSQL observation
  -> Think trigger
```

This is the production data-plane shape for cutover-enabled sources.

### 11.4 Embedding Recovery Flow

```text
Observation inserted with embedding_pending=true
  -> embedding Kafka request or DB backlog scan
  -> embedding worker calls Ollama/OpenAI
  -> vector written to PostgreSQL
  -> semantic retrieval can use the row
```

Embedding failure does not block ingest. The system preserves the observation
and fills the vector later.

### 11.5 Think Reasoning Flow

```text
think_trigger_queue row
  -> Think worker leases row
  -> trigger context hydrated
  -> retrieval/inquiry gathers evidence
  -> LLM returns structured diff
  -> validator checks diff and region
  -> applier updates domain tables
  -> think_runs / costs / artifacts recorded
  -> post-commit actions enqueued
```

Think is the main transformation from raw organizational signal to updated
organizational memory.

### 11.6 Post-Commit Flow

```text
pending_post_commit_actions
  -> post-commit worker
  -> dispatch action handlers
  -> realtime broadcast / anomaly publish / prediction scheduling /
     model-edge discovery / metrics invalidation
  -> processed or retried with backoff
```

Post-commit work is at-least-once. Handlers must be idempotent.

### 11.7 Realtime Update Flow

```text
Think or post-commit side effect
  -> realtime dispatcher
  -> stream manager / websocket surface
  -> UI receives update
  -> UI refetches or updates affected surfaces
```

Realtime is a notification path, while PostgreSQL remains the source of truth.

## 12. Example End-to-End Flow

This example follows a GitHub pull-request webhook from source event to product
surface.

1. GitHub sends a webhook to `/webhooks/github`.
2. The gateway captures the raw body, parses headers, and verifies the GitHub
   signature.
3. The tenant resolver maps the GitHub installation ID to a Fyralis tenant.
4. If the tenant has Kafka ingestion enabled, the gateway writes the raw webhook
   payload to S3/MinIO and publishes a raw Kafka envelope. If this fails, it
   falls back to inline ingest.
5. The normalizer consumes the raw envelope, fetches the raw payload from S3,
   resolves the GitHub channel, and runs the GitHub handler.
6. The handler emits an `ObservationDraft` containing content text, raw JSON,
   source actor reference, external ID, occurred time, trust tier, kind, and
   entity hints.
7. The normalizer publishes a normalized envelope to Kafka.
8. The observation writer consumes the normalized envelope and reconstructs the
   `ObservationDraft`.
9. The writer resolves the GitHub actor to an internal actor when possible.
10. The writer resolves known entity aliases from the content text and stores
    unresolved phrases for deferred entity resolution.
11. The writer tries to compute a 768-dimensional embedding. If Ollama is down,
    the observation is still inserted with `embedding_pending = TRUE`.
12. The writer inserts the row into `observations`, deduping by source channel
    and external ID.
13. The writer enqueues a T1 `event_arrival` row in `think_trigger_queue`.
14. The Think worker leases the trigger.
15. Retrieval gathers relevant observations, models, acts, resources, graph
    edges, recent context, semantic neighbors, and structural matches.
16. The Think pipeline builds an LLM prompt from the selected context and asks
    the configured LLM provider for a structured diff.
17. The validator checks that the diff is valid and in-region.
18. The applier updates models, model edges, acts, resources, recommendations,
    forecasts, or other domain tables as needed.
19. The Think run writes cost, artifact, and context-use telemetry.
20. The same transaction enqueues post-commit actions such as realtime
    broadcast and model-edge discovery.
21. The post-commit worker processes those actions.
22. Product surfaces such as Today, Recommendations, Model Trace, CEO Home, or
    Forecasts now read the updated PostgreSQL state.
23. The UI receives realtime notification or refetches the relevant endpoint and
    displays the updated organizational intelligence.

## 13. Architectural Properties

The system has several important design properties:

- Backend core is overlay-free; demo/UI code plugs in through extension seams.
- PostgreSQL is the source of truth for both state and durable queues.
- Kafka/S3 ingestion decouples provider acknowledgements from downstream
  normalization and database writes.
- Inline ingest remains as a graceful fallback.
- Embedding failure is non-blocking because pending rows are retried later.
- Think work is tenant-scoped and queue-backed.
- LLM output is structured and validated before mutating domain state.
- Post-commit side effects are durable and retryable.
- Product APIs are composition layers over the persisted substrate, not the
  source of truth.
- Observability is both metrics-based and database-persisted for replay and
  audit.
