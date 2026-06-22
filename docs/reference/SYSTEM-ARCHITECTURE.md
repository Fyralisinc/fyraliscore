# Fyralis Core System Architecture

Last reviewed from code: 2026-06-22

This document explains the Fyralis Core backend at several levels of abstraction: what the system is, how its processes interact at runtime, how data moves through the system, which modules own which responsibilities, and how the codebase is laid out on disk.

The goal is that a new engineer can read this file first, then enter the codebase with a working mental model instead of a folder list and a prayer.

## 1. What This Repository Is

Fyralis Core is an organizational intelligence runtime. It ingests operational signals from SaaS tools and communication systems, normalizes them into observations, reasons over those observations into a persistent model graph, and serves product surfaces such as CEO home, question answering, model traces, recommendations, forecasts, and decision deltas.

The repository is backend-first:

- Python 3.11 packages under `lib/` and `services/`.
- FastAPI gateway for HTTP, WebSocket, integrations, and product APIs.
- Postgres plus pgvector as the primary system of record and vector store.
- Kafka and S3-compatible storage for the newer durable ingestion data plane.
- Redis, Temporal client dependencies, LLM providers, embedding providers, and the extension host API as runtime integration points.
- Docker Compose manifests for local and production-like backend stacks.
- Evaluation, replay, integration, and contract tests.

The active backend source is in the layered package structure:

```text
lib/
services/app/
services/domain/
services/ingest/
services/platform/
services/reasoning/
services/product/
services/workers/
```

Older docs and status files may still mention former top-level service names such as `services/gateway`, `services/models`, `services/think`, or `services/query`. In this checkout the active service tree is the layered package structure above; do not assume an older name is active unless current imports prove it.

The local `ui/` directory contains generated or residual frontend artifacts such as `dist`, `.vite`, `node_modules`, and test output. Its source directories are empty in this checkout, and the README states that the demo/UI overlay lives outside core. Treat this repository as the backend source of truth.

## 2. System In One Page

At the highest level Fyralis is a loop:

1. External systems send events or are polled/backfilled.
2. Ingestion converts raw source payloads into canonical `Observation` rows.
3. Observation inserts enqueue Think triggers.
4. The Think worker retrieves relevant context, including projection-first context when available, asks deterministic and LLM-backed reasoning code to propose changes, validates those changes, and applies them to domain tables.
5. Domain tables store evolving organizational memory: models, relationships, acts, resources, predictions, decisions, commitments, goals, state changes, model events, and rebuildable projection snapshots.
6. Product APIs read and render that memory into user-facing views, answers, cards, traces, deltas, recommendations, and resolution workflows.
7. Post-commit, projection, extension, and realtime workers fan out updates, materialize read views, invalidate cache, and refresh product surfaces.

The compact architecture looks like this:

```text
External sources
  Slack, GitHub, Discord, Gmail, Calendar, Drive, Jira, Mercury,
  QuickBooks, Grafana, Telegram, Brex, Ramp, Gusto, Deel, Fireflies,
  Signal, WhatsApp, AWS, Miro, Figma, Carta, HiBob, Ashby, LinkedIn,
  synthetic
        |
        v
FastAPI gateway / source workers / onboarding workflows
        |
        +------------------------------+
        |                              |
        v                              v
Inline ingestion path             Kafka + S3 ingestion path
handler -> ObservationDraft       RawEnvelope -> NormalizedEnvelope
        |                         normalizer -> observation writer
        +--------------+---------------+
                       |
                       v
                Domain repositories
        observations, actors, aliases, models,
        edges, goals, commitments, decisions, resources,
        model_events, projection_snapshots
                       |
                       v
              think_trigger_queue
                       |
                       v
                 Think worker
     retrieval + adaptive inquiry + LLM/deterministic diff
                       |
                       v
             validated diff applier
                       |
                       v
       updated memory graph + model event outbox
                       |
                       v
       post-commit projection/realtime actions
                       |
                       v
Product surfaces and realtime delivery
CEO home, Ask, Query, Today, recommendations,
forecasts, history, model trace, decision deltas,
resolution threads, WebSocket streams
```

The core abstraction is not "chat over documents." It is an event-sourced memory and reasoning system. Observations are raw inputs; Models and edges are durable beliefs; acts and resources are operational commitments; model events are the neutral belief-change outbox; projection snapshots and product views are rebuildable operating views over that evolving state.

## 3. Layered Architecture

The repository is organized around dependency direction. Lower layers should know less about higher layers.

```text
app/product/workers
        |
        v
reasoning/platform/ingest
        |
        v
domain
        |
        v
lib
```

The intended layering is:

| Layer | Main directories | Responsibility |
| --- | --- | --- |
| Shared library | `lib/shared`, `lib/llm`, `lib/embeddings`, `lib/extensions`, `lib/integrations`, `lib/nexus`, `lib/observability` | IDs, DB helpers, typed schemas, LLM providers, embeddings, extension host APIs, observability helpers, secrets, registries, cross-cutting primitives. |
| Domain | `services/domain/*` | Canonical repositories and invariants for observations, models, model events, projections, actors, aliases, acts, resources, bridge queries. |
| Ingest | `services/ingest/*` | Source handlers, raw/normalized Kafka pipeline, integrations, onboarding, backfills, synthetic data, draft-enricher seams for external source intelligence. |
| Platform | `services/platform/*` | Access control, extension governance/egress, execution routing, adaptive inquiry runtime, process manifest utilities. |
| Reasoning | `services/reasoning/*` | Think loop, retrieval, projection-first context, diff validation/application, topology, relationship discovery, Sage, oracle/outcome facts, calibration, dynamics, contestability. |
| Product | `services/product/*` | User-facing application behavior: CEO view, Ask, query answering, rendering, today, recommendations, forecasts, history, model trace. |
| App | `services/app/*` | FastAPI gateway composition, middleware, route mounting, webhooks, realtime WebSocket transport. |
| Workers | `services/workers/*`, scripts | Long-running background jobs and schedulers around reasoning, topology, calibration, anomalies, maintenance, and post-commit processing. |
| Tests/eval | `tests/`, `benchmarks/` | Unit, integration, contract, quality, benchmark, and real-LLM evaluation harnesses. |

`pyproject.toml` enforces some of this with import-linter contracts:

- Core code must not import demo or simulation overlays.
- `lib` must remain independent of `services`, with a small whitelist for lazy LLM integration points.
- Reasoning core must not directly import app, product, or ingest.
- Domain and ingest have ratchet contracts that prevent new upward imports into reasoning/product or app code beyond explicit allowlists.
- `lib.extensions` must stay independent of `services` so extension manifests, host APIs, and background-worker discovery remain stable external contracts.

There are still known architectural edges. For example, `services/domain/models/repo.py` reaches upward into reasoning modules for topology and affordance policy behavior. That is current code, but it is a dependency worth treating carefully when refactoring.

## 4. Runtime Architecture

### 4.1 Infrastructure Services

The Compose stack defines these infrastructure services:

- `postgres`: primary database, pgvector-backed memory store, queue tables, product caches, integration state.
- `ollama`: local embedding and LLM-compatible service used by default development flows.
- `kafka`: durable event bus for the raw/normalized/embedding ingestion path and onboarding progress.
- `minio`: S3-compatible object storage for raw payload blobs.
- `redis`: shared cache/rate-limit/workflow coordination dependency used by gateway and integration workflows.

Migration services run before app services:

- `migrate`: applies SQL migrations.
- `kafka-init`: creates ingestion topics.
- `minio-init`: creates/configures object buckets.

### 4.2 Long-Running Application Processes

The main backend processes are:

- `gateway`: FastAPI app at `services.app.gateway.main:app`.
- `think_worker`: drains `think_trigger_queue` and applies reasoning diffs.
- `post_commit_worker`: handles pending post-commit actions, including realtime broadcasts, metric invalidation, edge discovery, open-question search, and projection materialization.
- `extension_workers`: discovers `company_os.workers` contributions and supervises installed extension background workers behind one health/metrics surface.
- Ingestion workflow workers: OAuth poller, tenant onboarding, source onboarding, shard fetch, reconciler, onboarding monitor, periodic reconciler.
- Kafka ingestion consumers: normalizer, observation writer, DLQ writer, summarization worker, summarization batch worker, embedding worker, embedding backlog, circuit breaker.
- Live source workers: Discord gateway, Telegram gateway, Signal gateway, Gmail/Google Calendar/Google Drive watch and poll workers.
- Application/reasoning workers: anomaly processor, entity resolver, Sage structural features, Sage topology optimizer, housekeeper, relationship ontology proposals.
- Optional observability profile: Prometheus, Grafana, Postgres exporter, Kafka exporter, and Redis exporter.

Local dogfood scripts may also start `topology_sweeper`, which is implemented under `services/workers/topology_sweeper` and `scripts/run_topology_sweeper.py`. The current main Compose file does not run that sweeper as a service.

### 4.3 App Startup

`services/app/gateway/main.py` builds the FastAPI app.

At startup the gateway:

1. Creates or receives an asyncpg pool.
2. Constructs `GatewayDeps`.
3. Instantiates shared repositories such as `ActorRepo` and `EntityAliasRepo`.
4. Optionally creates an `OllamaClient` embedder.
5. Configures rate limiting.
6. Starts the Ask overlay/router.
7. Runs gateway extension startup hooks.
8. Configures integration runtime dependencies such as secret store, tenant resolver, and tenant flags.
9. Starts GitHub gateway state and OAuth state sweeping.
10. Starts realtime dispatch and CEO view scheduling.
11. Initializes ingestion data-plane helpers.
12. Mounts routers through `mount_gateway_routes(app)`.

The gateway is a composition root. Most dependencies are built there and passed down so lower layers do not need global app state.

### 4.4 Route Mounting

`services/app/gateway/route_mounts.py` is the central route registry. It mounts:

- Core health and utility routes: `/healthz`, `/readyz`, `/metrics`, `/auth/session`, `/ingest/{channel:path}`.
- Domain substrate routes: `/observations`, `/models`, `/commitments`, `/goals`, `/decisions`, `/resources`.
- Contestability and dashboard routes.
- Document ingest, WhatsApp, Sage internal, clarification, and extension management routes.
- Recommendations, structure, today, and map routes.
- Gateway extensions discovered through `services.app.gateway.extensions`.
- Model page and spec routes.
- Product routers for decision deltas, forecasts, model trace, history, and resolution threads.
- Webhooks.
- Integration install/callback routers.
- Optional finance and Slack DM panels.

This makes route mounting explicit, but it also means new product APIs should be added here or through the extension seam.

### 4.5 Middleware And Request Context

Gateway middleware covers:

- Request-scoped context and tenant binding.
- Bearer authentication and public path handling.
- Rate limiting.
- Ingest size error handling.

Public webhook routes intentionally bypass user-session auth but still perform provider-specific verification before accepting data.

## 5. Data Stores And Message Buses

### 5.1 Postgres

Postgres is the system of record. It stores:

- Tenancy and actor identity.
- Observations and embeddings.
- Models, model edges, predictions, and reasoning metadata.
- Model events, projection checkpoints, projection snapshots, semantic terms, and open questions.
- Goals, commitments, decisions, resources, transactions, deployments, and customer commitments.
- Integration installation state and encrypted secrets.
- Ingestion failures, backlog state, workflow state, onboarding state, and source-specific cursors.
- Think queues, run records, costs, artifacts, applied triggers, dead letters, and post-commit actions.
- Product view caches, rendering costs, card conversations, recommendation state, forecast state, model traces, and resolution threads.
- Extension manifests/grants, egress outboxes, audit records, and capability state.
- Sage and retrieval artifacts such as structural features, motifs, inquiry evidence, omitted evidence, discovery shortcuts, negative memory, and topology optimizer runs.

The `db/migrations/` directory is authoritative for schema history. Migrations currently run from the foundation schema through dynamic tenant RLS, model events/projection snapshots, post-commit projection materialization, semantic terms, and model open questions.

### 5.2 pgvector

The system uses pgvector for semantic search. Observation and model embeddings are stored as `VECTOR(768)` in the main database. Embedding providers live under `lib/embeddings`; the default local path uses Ollama.

### 5.3 Kafka

Kafka is the durable ingestion bus. Topics are generated per source:

```text
ingestion.raw.<source>
ingestion.normalized.<source>
ingestion.embedding.<source>
ingestion.dlq.<source>
```

Source names come from the raw-tier `SourceLiteral`, which includes Slack, GitHub, Discord, Gmail, Notion, Google Calendar, Google Drive, Jira, Mercury, QuickBooks, Grafana, Telegram, Brex, Ramp, Gusto, Deel, Fireflies, Signal, WhatsApp, AWS, Miro, Figma, Carta, HiBob, Ashby, and LinkedIn.

Kafka is also used for onboarding progress and some workflow signals.

### 5.4 S3-Compatible Raw Object Store

The Kafka ingestion path stores large or important raw payloads in S3-compatible object storage and places pointers in raw envelopes. The default local implementation is MinIO.

The raw object store is part of the durability boundary: raw payloads can be replayed into normalized envelopes and then into observations.

### 5.5 Redis

Redis is available for rate limiting, integration workflow coordination, and other cross-process runtime coordination where configured.

## 6. Canonical Data Flow: Inline Ingestion

Inline ingestion is the classic direct path used by `/ingest/{channel:path}` and fallback webhook handling.

```text
HTTP request / internal caller
        |
        v
services.ingest.ingestion.core.ingest(...)
        |
        v
handler registry resolves channel
        |
        v
source handler returns ObservationDraft
        |
        v
ingest_from_draft(...)
        |
        +--> registered draft enrichers, gated by extension grants
        +--> preassign uuid7 observation id
        +--> resolve actor by source identity
        +--> resolve entity aliases
        +--> mark large documents for summarization when needed
        +--> compute embedding or mark pending
        +--> build ObservationCreate
        +--> insert observation with dedupe
        +--> enqueue think trigger
        +--> publish post-commit notifications
        +--> optionally publish embedding retry request
```

Key files:

- `services/ingest/ingestion/core.py`
- `services/ingest/ingestion/handlers/__init__.py`
- `services/domain/observations/repo.py`
- `services/domain/actors/repo.py`
- `services/domain/entity_aliases/repo.py`

Important behavior:

- Payloads are validated as dictionaries and limited to 1 MB.
- NUL bytes are rejected.
- Missing observation partitions can be self-healed.
- `(source_channel, external_id)` deduplication prevents duplicate source events.
- Channel-keyed draft enrichers can augment the draft before persistence; extension enrichers are raw-on-failure and capability-gated.
- Actor and alias resolution happen before persistence.
- Large document observations can be converted to pending-summary observations and resumed by summarization workers.
- Failed embedding generation does not fail ingestion; the observation is marked pending.
- New observations enqueue `think_trigger_queue` entries.
- Observation notifications are flushed after commit.

The output of ingestion is not a product response. It is durable memory plus a queued reasoning trigger.

## 7. Canonical Data Flow: Kafka And S3 Ingestion

The newer ingestion data plane separates raw capture, normalization, and observation writing.

```text
source webhook / poller / backfill / gateway
        |
        v
RawEnvelope + raw object in S3
        |
        v
Kafka topic ingestion.raw.<source>
        |
        v
normalizer worker
        |
        v
NormalizedEnvelope
        |
        v
Kafka topic ingestion.normalized.<source>
        |
        v
observation writer
        |
        +--> optional summarization queue for large documents
        +--> optional embedding request
        |
        v
ingest_from_draft(...)
        |
        v
Postgres observations + think trigger
```

### 7.1 Raw Tier

`services/ingest/ingestion/raw_tier/envelope.py` defines the raw envelope contract. It captures:

- Tenant identity.
- Source.
- Ingress kind such as webhook, gateway, pubsub, backfill, or poll.
- Source object identity.
- Raw object pointer.
- Headers or metadata needed for verification and handler behavior.
- Idempotency and trace fields.

### 7.2 Normalizer

`services/ingest/ingestion/normalizer/worker.py` is deliberately database-free. It:

- Consumes `ingestion.raw.<source>` topics.
- Fetches raw payload bytes from S3.
- Validates raw envelope invariants.
- Maps source and ingress kind to the canonical handler channel.
- Reconstructs request headers where needed, especially for GitHub/backfill semantics.
- Calls the same handler registry used by inline ingestion.
- Publishes a `NormalizedEnvelope` to `ingestion.normalized.<source>`.
- Publishes permanent failures to DLQ and commits the offset.

The normalizer can process serially or with per-tenant grouping. Its design goal is that raw-to-normalized replay does not need the database.

### 7.3 Observation Writer

`services/ingest/ingestion/writers/observation_writer.py` consumes normalized envelopes and calls `ingest_from_draft`.

It respects tenant-level rollout flags:

- Missing tenant flag row means full Kafka path.
- Explicit `kafka_path_enabled = FALSE` means shadow/no-op for normalized writes.
- A circuit breaker can flip tenants back to shadow on sustained lag.

The writer owns:

- Normalized envelope parsing.
- Observation partition self-healing.
- Actor and alias repo dependencies.
- Summarization producer integration for large document bodies.
- Embedding producer integration.
- Retry vs DLQ classification.

### 7.4 Webhook Shadow And Cutover

`services/app/webhooks/router.py` can route provider webhooks through the Kafka path. For supported providers it:

1. Captures the raw body.
2. Resolves provider installation and tenant.
3. Loads provider secrets from the encrypted secret store.
4. Verifies signatures.
5. Attempts a Kafka raw write and bounded flush.
6. Returns `202` on successful cutover ingestion.
7. Falls back to inline ingestion when cutover is unavailable.
8. Optionally writes shadow raw envelopes after inline success.

Supported generic webhook provider families include Slack, GitHub, Discord, Jira, Mercury, QuickBooks, Grafana, Brex, Ramp, Gusto, Deel, Fireflies, Miro, Figma, HiBob, and Ashby. Discord has provider-specific response semantics, so it is not cut over in the same way as simple `202` providers. WhatsApp uses its own gateway router and maps live Cloud API items onto the same Kafka/handler path; backfill reconciliation is currently deferred.

### 7.5 Onboarding And Backfill

The ingestion workflow layer handles source onboarding:

- OAuth polling.
- Tenant onboarding.
- Source onboarding.
- Shard fetching.
- Reconciliation.
- Periodic reconciliation.
- "Feels onboarded" monitoring.
- Progress events.

These workers produce raw envelopes and reconciliation state so that initial backfills and continuous syncs converge on the same observation-writing path.

## 8. Canonical Data Flow: Think And Memory Mutation

Think is the reasoning engine. It transforms observations and other triggers into validated memory mutations.

```text
think_trigger_queue
        |
        v
ThinkWorker leases work
        |
        v
think(trigger, ...)
        |
        +--> load candidate relationship trigger
        +--> plan context
        +--> run adaptive inquiry and retrieval
        +--> assemble reasoning context
        +--> compute allowed region
        +--> insert think_runs row
        +--> acquire advisory region lock
        +--> deterministic handler or LLM reasoning
        +--> inject safety-net ops
        +--> validate diff
        +--> apply diff in transaction
        +--> emit neutral model_events for changed models
        +--> adjudicate relationship candidates
        +--> enqueue post-commit actions
        +--> record cost, artifacts, status
```

Key files:

- `services/reasoning/think/worker.py`
- `services/reasoning/think/reason.py`
- `services/reasoning/think/diff_schema.py`
- `services/reasoning/think/applier.py`
- `services/reasoning/think/context_planner.py`
- `services/reasoning/retrieval/primary.py`
- `services/platform/execution/inquiry.py`

### 8.1 Triggers

Think triggers are stored in `think_trigger_queue`. Common trigger kinds include:

- `T1`: event arrival.
- `T2`/`T3`: model or state changes.
- `T4`: model reevaluation and topology-related work.
- `T6`: specialized or periodic reasoning triggers.

The worker also promotes `model_reeval_queue` rows into Think triggers.

### 8.2 Worker Leasing And Concurrency

`ThinkWorker` uses database leasing:

- `FOR UPDATE SKIP LOCKED` avoids duplicate work across workers.
- Per-tenant concurrency is bounded, defaulting to one active Think per tenant.
- Locks have timeout and heartbeat behavior.
- Failed jobs retry until a configured limit, then dead-letter.
- Batched trigger behavior is supported by later migrations.

### 8.3 Context Planning

The context planner gathers evidence before reasoning:

- Direct trigger context.
- Structural retrieval through graph and edge relationships.
- Semantic retrieval through embeddings.
- Temporal retrieval around relevant events.
- Pattern and motif retrieval.
- Active adaptive inquiry through `services.platform.execution.inquiry`.
- Optional second-pass retrieval.
- Dynamic signals.
- Projection-first context from `projection_snapshots`, followed back to source Models.
- Active goals, commitments, decisions, and actor operating context.

The output is a reasoning frame and context packet that downstream reasoning can consume.

### 8.4 Reasoning Diff

Reasoning returns a diff, not arbitrary writes. The diff schema can include:

- `claim_ops`: create/update/archive model claims.
- `edge_ops`: create/update/archive relationships between models.
- `ontology_gap_ops`: propose missing relationship concepts.
- `act_ops`: mutate goals, commitments, and decisions.
- `resource_ops`: mutate resources, transactions, deployments, releases, and customer commitments.
- open-question operations that create, update, or resolve model follow-up questions.
- `new_predictions`: create forecastable predictions.
- `reasoning_trace`: explain evidence and reasoning shape.

Diff validation constrains operations to the allowed tenant, allowed region, and valid schema.

### 8.5 Applying Diffs

`services/reasoning/think/applier.py` applies validated diffs inside a transaction.

Important behavior:

- `applied_triggers` provides idempotency.
- Model write advisory locks prevent conflicting claim mutation.
- Claim operations are applied before edge, act, and resource operations.
- Compound claims can be split into atomic/situation claims.
- Model writes emit projection-neutral `model_events` with semantic snapshots.
- Semantic terms and open questions are sidecar state on Models, not separate sources of truth.
- State-change observations are emitted for meaningful state mutations.
- Reconciler and quality gates run as part of application.
- Outcomes are recorded back to `applied_triggers` and `think_runs`.

Think is therefore a controlled mutation pipeline. LLMs propose structured changes; repositories and validators decide what actually reaches durable memory.

### 8.6 Post-Commit Actions And Projections

Think writes post-commit actions into `pending_post_commit_actions` inside the same transaction as the applied diff. The post-commit worker then handles the durable side effects after commit:

- `publish_anomalies`: publish anomaly observations or follow-up signals.
- `schedule_predictions`: schedule prediction evaluation work.
- `broadcast_realtime`: notify realtime/product listeners.
- `invalidate_metrics`: invalidate affected metric/product views.
- `materialize_projections`: consume `model_events` through `ProjectionRunner` and update `projection_snapshots`.
- `discover_model_edges`: run edge discovery on changed models.
- `search_open_questions`: turn model open questions into retrieval/search follow-up.

Projection materialization is deliberately rebuildable. `ModelsRepo` emits neutral `model_events`; projectors such as `constraints`, `resources`, and `employee_profiles` consume those events and write typed `projection_snapshots` keyed by `projection_name`, `projection_version`, and `subject_key`. Retrieval uses snapshots as a compact first pass, then loads the canonical source Models through `ProjectionRepo`.

## 9. Canonical Data Flow: Product Surfaces

Product code reads the memory graph and turns it into usable application experiences.

### 9.1 CEO Home / Greeting

`services/product/greeting/scheduler.py` runs inside the gateway process. It:

- Builds CEO home snapshots.
- Renders greeting, cards, query grid, status, and close line.
- Stores projections in `view_ceo_cache`.
- Refreshes on a schedule, time-of-day boundary, database notification, and pending post-commit actions.
- Publishes updates to realtime streams.

Important collaborators:

- Snapshot composer.
- Rendering adapter.
- `ViewCeoCacheRepo`.
- Stream publisher.
- Product rendering service.

### 9.2 Rendering

`services/product/rendering/core.py` wraps LLM rendering for product copy and HTML fragments.

It handles:

- Prompt composition.
- Voice rules.
- Retry-on-reject.
- Cost recording in `view_render_costs`.
- Internal rendering endpoints.

Rendering is separated from reasoning. Reasoning mutates memory; rendering makes memory legible to users.

### 9.3 Query Answering

`services/product/query/core.py` serves direct user questions.

The query path:

1. Validate the request.
2. Resolve card or view context.
3. Classify the question using heuristic and optional LLM classification.
4. Choose a strategy.
5. Retrieve context using the fast path of the adaptive inquiry runtime.
6. Assemble answer context.
7. Render a response.
8. Return answer, retrieval trace, and cost metadata.

Query is a read path. It may share retrieval machinery with Think, but it should not perform uncontrolled memory mutation.

### 9.4 Ask Fyralis

`services/product/ask` is a newer overlay for Ask-style interaction. The gateway starts and mounts it during lifespan setup. It has its own router, orchestration, and store abstractions while still reading core memory.

### 9.5 Other Product Surfaces

The product layer also includes:

- `today`: daily triage and aggregation.
- `conversations`: card-scoped probe and discussion threads.
- `recommendations`: recommendation handlers, repos, and watcher state.
- `decision_deltas`: decision change tracking, evidence, apply/promote behavior.
- `forecasts`: prediction accuracy, pages, and APIs.
- `history`: historical aggregation and summaries.
- `model_trace`: trace and inspect model evolution.
- `resolution_threads`: thread evaluation and resolution workflows.

These surfaces should be understood as projections or workflows around the domain memory graph.

## 10. Realtime And Notifications

Realtime transport lives under `services/app/realtime`.

The dispatcher:

- Listens to Postgres notifications such as `observations_new`.
- Fans out updates over WebSockets.
- Tracks replay cursors in `realtime_replay_cursors`.
- Supports CEO home stream updates and product refresh delivery.

The core pattern is:

```text
domain write
  -> schedule notification
  -> transaction commits
  -> notification flush
  -> realtime dispatcher
  -> WebSocket clients
```

This keeps realtime delivery after commit rather than publishing speculative updates from inside uncommitted transactions.

## 11. Component And Subcomponent Reference

### 11.1 `lib/shared`

Shared primitives used across the system:

- `ids`: uuid7 and stable ID helpers.
- `db`: asyncpg helpers and transaction patterns.
- `types`: typed domain and DTO models.
- `errors`: cross-layer error classes.
- `tenant_context`: request/runtime tenant binding.
- `secrets`: envelope encryption and secret-store helpers.
- `memory_grammar`: claim and memory parsing primitives.
- `edge_registry`: canonical relationship type definitions.
- `claim_kind_registry`: claim kind validation.
- `trust`: trust and confidence helper types.
- `testing`: shared test utilities.
- `env` and migration helpers.

This layer should not depend on `services`.

### 11.2 `lib/llm`

`lib/llm/provider.py` abstracts structured LLM calls. The Fyralis app path is Codex-only, with compatibility providers retained for tests and older harnesses.

Responsibilities:

- Provider selection.
- Structured schema calls.
- JSON/parse repair retries.
- Usage and cost aggregation.
- Timeouts and error classification.
- Optional cache behavior.

The rest of the system should depend on `LLMProvider` behavior rather than provider-specific SDKs.

### 11.3 `lib/embeddings`

Embedding abstractions:

- `Embedder` protocol.
- Factory selection.
- Ollama backend.
- OpenAI backend.
- Dimension compatibility with `VECTOR(768)`.

Embeddings are used in observation ingestion, model retrieval, and semantic search.

### 11.4 `lib/integrations`

Shared integration helpers, especially outbound endpoint/base URL resolution.

### 11.5 `services/domain/observations`

Observations are canonical ingested events.

The repository owns:

- Partition creation and self-healing.
- Observation insertion.
- Duplicate detection by source channel and external ID.
- Embedding storage.
- State-change observations.
- Vector and metadata search.
- Post-commit notification scheduling.

Observations are append-oriented. Higher layers reason over them rather than editing history casually.

### 11.6 `services/domain/models`

Models are persistent beliefs, claims, predictions, and structured memory items.

`ModelsRepo` owns:

- Model creation.
- Claim canonicalization.
- Model formation/read-shape hydration.
- Facet extraction for operational, semantic, and retrieval-facing attributes.
- Proposition validation.
- Confidence calibration and clipping.
- Falsifier adequacy checks for high-confidence claims.
- Scope actor validation.
- Semantic term and open-question sidecars.
- Embedding.
- Neutral `model_events` emission.
- State-change emission.
- Audit events.
- Search and retrieval counters.
- Archive behavior and reevaluation enqueueing.

`models/edges_repo.py` owns `model_edges`, the typed relationship graph between models.

Edges support:

- Link/unlink.
- Traversal.
- Cycle checks.
- Inert edge marking.
- Drift samples.
- Dynamic ontology validation.

The older accepted-memory topology queue is retired; some compatibility tables remain.

### 11.7 `services/domain/projections`

Projections are rebuildable operating views over canonical Models.

Core projection families currently include:

- `constraints`: runway, financial capacity, operating capacity, and entity-scoped constraints.
- `resources`: financial, capacity, relational, infrastructure, regulatory, IP, and entity-scoped resources.
- `employee_profiles`: longitudinal employee/person operating views.

The projection layer owns:

- `ProjectionRunner`, which consumes `model_events` per projector and maintains checkpoints.
- `ProjectionRepo`, the typed read API for `projection_snapshots` and their backing Models.
- Projector catalog and `company_os.projections` entry-point discovery.
- Subject resolver catalog and `company_os.projection_subject_resolvers` entry-point discovery.

Projection snapshots are disposable. Canonical belief state remains in Models and edges; snapshots are compact read/retrieval indexes that can be rebuilt from `model_events`.

### 11.8 `services/domain/actors`

Actors represent users, people, bots, systems, and other source identities.

`ActorRepo` owns:

- Actor creation.
- Source identity mappings.
- Resolution by `<channel>:<source_ref>`.
- Listing active actors.
- Deactivation safeguards.

Actor resolution is central to multi-source memory because source systems use incompatible identity models.

### 11.9 `services/domain/entity_aliases`

Entity aliases map phrases and source-specific names to canonical entities.

`EntityAliasRepo` owns:

- Normalization with casefolding and whitespace collapse.
- Fast-path phrase resolution.
- Ambiguity detection.
- Advisory-lock-protected insert.
- Reverse lookup.

Ingestion stores unresolved phrases when alias resolution cannot decide safely.

### 11.10 `services/domain/acts`

Acts are operational objects:

- Goals.
- Commitments.
- Decisions.

The acts layer owns:

- State transition tables.
- Valid transition enforcement.
- Contributor and dependency relationships.
- Goal/commitment/decision invariants.

Think can create or update acts through validated `act_ops`.

### 11.11 `services/domain/resources`

Resources represent operational and business objects:

- Generic resources.
- Resource transactions.
- Deployments.
- Releases.
- Customer commitments.

These tables connect memory to business operations such as revenue risk, feasibility, releases, and customer health.

### 11.12 `services/domain/bridge`

Bridge queries provide dashboard-grade joins over domain state.

Examples:

- Customer health.
- Revenue at risk.
- Feasibility.
- Critical path.
- Cross-domain state summaries.

This layer is useful when product surfaces need coherent business projections rather than raw table access.

### 11.13 `services/ingest/ingestion`

The core ingestion package owns:

- Inline ingestion entrypoint.
- Shared `ingest_from_draft`.
- Handler registry.
- Source-specific handlers.
- Draft-enricher registry and `company_os.draft_enrichers` discovery.
- Raw-tier envelopes.
- Kafka topic definitions.
- Normalizer worker.
- Observation writer.
- Large-document summarization workers.
- Embedding writer/backlog.
- DLQ writer.
- Feature flags and circuit breaker.
- Recovery utilities.

Handlers are pure-ish adapters from source payloads to `ObservationDraft`. They should avoid durable writes. Durable writes happen in `ingest_from_draft` and domain repositories.

### 11.14 `services/ingest/integrations`

Integration modules own source install/callback flows, OAuth state, source-specific clients, and background source workers.

Integration routers include:

- Slack.
- Discord.
- GitHub.
- Notion.
- Jira.
- Mercury.
- QuickBooks.
- Brex, Carta, Deel, Figma, Fireflies, Google Calendar, Google Drive, Grafana, Gusto, Miro, and related source-specific OAuth/client modules.

Additional live source workers and services cover Gmail, Google Calendar, Google Drive, Telegram, Signal, Discord, and WhatsApp paths.

### 11.15 `services/ingest/synthetic`

Synthetic ingestion supports generated organizations, signals, scenarios, and test/demo data. It is useful for evaluation and repeatable local behavior but should remain separate from production source handling.

### 11.16 Extracted `github_intel` / `code_intel`

GitHub/code intelligence is no longer an active in-core service package. The old `services/ingest/code_intel` and `services/ingest/github_intel` directories are residual/cache-only in this checkout. That capability is represented as an external interface that can reattach through extension seams:

- `company_os.draft_enrichers` for inline draft enrichment.
- `company_os.gateway_extensions` for API/read routes.
- `company_os.interfaces` for extension manifest discovery.
- `company_os.workers` for extension background jobs.
- `services/platform/extensions` for grants, access, audit, egress, lifecycle, and marketplace governance.

### 11.17 `services/platform/access_control`

Access control centralizes read permission behavior.

The effective read model includes:

- Tenant isolation.
- Observation scope.
- Act ownership and participation.
- Resource-kind roles.
- Model visibility.
- Shared channels.
- Overrides and audit logs.
- Materialized actor-visible views.

Product and app code should use this layer rather than recreating access checks.

### 11.18 `services/platform/execution`

Execution platform includes:

- Deterministic routing.
- Adaptive inquiry.
- Retrieval for execution.
- Question planning.
- Evidence reservoir behavior.
- Sufficiency checks.
- Context packet compilation.

`retrieve_for_execution()` is shared by Think and Query. Think uses deeper context; Query uses a faster read-oriented path.

Routing currently records and can shadow deterministic decisions such as fast path, deep inquiry path, background path, deterministic update, human validation, or ignore/archive. It is not the primary inline ingest control path in current code.

### 11.19 `services/platform/extensions`

The extension platform is the in-core host for external interfaces.

It owns:

- Capability/grant checks for draft enrichers and read APIs.
- Extension identity, consent, provenance, redaction, and killswitch behavior.
- Edge ingest for extension-authored signals.
- Egress planning, projection, delivery, and append-only stores.
- Marketplace registry/review/signing primitives.

The stable host API lives under `lib/extensions/host_api/v1`; platform code provides the services-backed enforcement.

### 11.20 `services/platform/runtime`

Runtime utilities include process manifest rendering and process-level metadata.

### 11.21 `services/reasoning/think`

Think is the main reasoning loop:

- Worker leasing.
- Trigger lifecycle.
- Context planning.
- Reasoning invocation.
- Diff schema.
- Diff validation.
- Diff application.
- Cost/run/artifact recording.
- Dead-letter and retry handling.

This is the most important package for understanding how observations become durable beliefs and operations.

### 11.22 `services/reasoning/retrieval`

Retrieval provides low-level pathways:

- Structural graph retrieval.
- Semantic embedding retrieval.
- Projection-first retrieval through `ProjectionRepo`.
- Temporal retrieval.
- Pattern retrieval.
- Model-edge retrieval.

Trigger kinds weight these pathways differently. Retrieval can reconsolidate models, check projection freshness, fall back when snapshots are missing, and produce traces for debugging.

### 11.23 `services/reasoning/topology`

Topology is currently centered on `LatentTopologyService`.

It handles:

- Impact signatures.
- Relationship candidates.
- T4 candidate generation.
- Dirty topology implications.

It is called from model writes and workers. Older accepted-memory topology tables remain as compatibility/history, but the active topology behavior is candidate-oriented.

### 11.24 `services/reasoning/relationships`

Relationships code handles:

- Candidate generation.
- Candidate adjudication.
- Relationship ontology behavior.
- Runtime relationship support.
- Ontology proposals.

Relationship candidate triggers allow Think to reason over likely edges without immediately accepting every inferred relation.

### 11.25 `services/reasoning/sage`

Sage is the deeper reading, retrieval, and graph-intelligence layer.

Subareas include:

- Affordance profiles.
- Discovery shortcuts.
- Negative memory.
- Inquiry traces.
- Reader activations.
- Decision attributions.
- Model predictions.
- Outcome evaluation.
- Region summaries.
- Structural features.
- Topology optimization.

Sage workers compute structural features, topology optimizer runs, and ontology proposals. Product and Think can use Sage-derived artifacts for better retrieval and reasoning quality.

### 11.26 `services/reasoning/synthesis`

Synthesis owns query understanding and state contracts for higher-level summaries. It helps transform raw memory into operational facets and structured answer material.

### 11.27 `services/reasoning/contestability`

Contestability supports reviewing, challenging, and evaluating beliefs. This matters because the model graph contains confidence-weighted claims, not absolute truth.

### 11.28 `services/reasoning/calibration`

Calibration manages prediction confidence and outcome-driven confidence adjustment.

### 11.29 `services/reasoning/dynamics`

Dynamics models changing state and drift over time. It supports memory behavior where confidence and relevance evolve.

### 11.30 `services/reasoning/judgment`

Judgment contains decision/judgment support primitives used by reasoning and product code.

### 11.31 `services/reasoning/oracle`

Oracle contains outcome-fact extraction and outcome-oriented helpers used by reasoning quality loops.

### 11.32 `services/product/greeting`

Greeting owns the CEO home projection, scheduler, cache repo, API, stream publishing, and card composition.

### 11.33 `services/product/rendering`

Rendering owns user-facing language generation and rendered fragments. It is product presentation, not durable reasoning.

### 11.34 `services/product/query`

Query owns read-time question answering over memory and retrieval context.

### 11.35 `services/product/ask`

Ask owns the newer conversational product surface and its router/orchestrator/store.

### 11.36 `services/product/today`

Today owns daily summaries, triage, and near-term operational surfaces.

### 11.37 `services/product/recommendations`

Recommendations owns recommendation generation, persistence, and watcher behavior.

### 11.38 `services/product/decision_deltas`

Decision deltas track changes, evidence, and promotion/application flows around decisions.

### 11.39 `services/product/forecasts`

Forecasts expose predictions, accuracy, and forecast pages/APIs.

### 11.40 `services/product/history`

History aggregates and summarizes past state.

### 11.41 `services/product/model_trace`

Model trace lets users inspect why a model exists, where it came from, and how it changed.

### 11.42 `services/product/conversations`

Conversations supports card-scoped product discussions and probes.

### 11.43 `services/product/resolution_threads`

Resolution threads manage threads around resolving open issues, uncertainties, or contested state.

### 11.44 `services/app/gateway`

Gateway is the process composition and HTTP API layer:

- App construction.
- Middleware.
- Dependency injection.
- Route mounting.
- Extension loading.
- Health/metrics/auth.
- Product and domain route composition.
- Ingest endpoint.

Gateway should orchestrate, not own core business logic.

### 11.45 `services/app/webhooks`

Webhooks own public provider event capture:

- Raw body handling.
- Size limit.
- Provider installation lookup.
- Secret loading.
- Signature verification.
- Lifecycle event handling.
- Cutover/shadow Kafka behavior.
- Inline fallback.
- Provider-channel mapping.

### 11.46 `services/app/realtime`

Realtime owns WebSocket transport, Postgres notification listening, replay cursors, and fan-out.

### 11.47 `services/workers`

Worker packages provide background jobs around:

- Anomaly processing.
- Calibration updating.
- Deadline resolution.
- Edge drift.
- Entity resolution.
- Housekeeper lifecycle jobs.
- Maintenance.
- Precipitation.
- Relationship ontology proposals.
- Sage structural features.
- Sage topology optimization.
- Topology sweeping.

Not every implemented worker is mounted in the main Compose stack. Current Compose directly runs anomaly processing, entity resolution, housekeeper, Sage structural features, Sage topology optimizer, and relationship ontology proposals from this tree. Other workers may be manual, dormant, script-launched, or future deployment targets.

## 12. Database Model At A Glance

This section groups the main schema families by purpose.

### 12.1 Tenancy And Identity

- `tenants`
- `actors`
- `actor_identity_mappings`
- `actor_sessions`
- role and access-control tables

These tables establish who the memory belongs to and who can read it.

### 12.2 Observations And Memory

- `observations`
- observation partitions
- `models`
- `model_edges`
- `model_events`
- `model_semantic_terms`
- `model_open_questions`
- model scope tables
- model search and sparse term tables
- model belief address tables

Observations are ingested evidence. Models and edges are durable interpreted memory. Model events, semantic terms, and open questions are sidecars/outboxes attached to the belief kernel.

### 12.3 Acts And Operations

- `goals`
- `commitments`
- `commitment_contributors`
- `decisions`
- `contributes_to`
- `depends_on`
- `constrained_by`
- `resources`
- `resource_transactions`
- `resource_deployments`
- `customer_commitments`

These tables capture work, obligations, decisions, dependencies, and business objects.

### 12.4 Reasoning Queues And Runs

- `think_trigger_queue`
- `model_reeval_queue`
- `model_reeval_dead_letter`
- `applied_triggers`
- `think_runs`
- `think_run_costs`
- `think_run_artifacts`
- `pending_post_commit_actions`
- `think_region_lock_log`

These tables provide durable reasoning orchestration, idempotency, cost tracking, and debugging.

### 12.5 Model And Product Projections

- `projection_checkpoints`
- `projection_snapshots`
- `view_ceo_cache`
- `view_render_costs`
- `viewer_state`
- `card_conversations`
- `model_watchers`
- recommendation tables/columns
- `decision_deltas`
- decision delta evidence
- `predictions`
- prediction signals
- calibration tables

`projection_snapshots` are rebuildable operating views over Models. The `view_*` and product-specific tables make the memory graph usable in product surfaces.

### 12.6 Integration And Ingestion State

- `provider_installations`
- `encrypted_secrets`
- `oauth_install_states`
- `tenant_flags`
- `installation_audit_log`
- `ingestion_failures`
- `embedding_backlog_state`
- summarization batch/job state
- onboarding run/shard/trigger tables
- source onboarding run tables
- workflow state/signal tables
- gateway session state
- source-specific install/resource/cursor tables

These tables let the system install integrations, remember cursors, replay data, protect secrets, and operate cutovers.

### 12.7 Extension Platform

- `extension_grants`
- `extension_oauth_clients`
- `extension_egress`
- `extension_webhook_delivery`
- `extension_egress_progress`
- `extension_audit_log`
- `extension_killswitch`
- `extension_listings`

These tables let installed interfaces authenticate, receive capability-scoped grants, read/write through governed surfaces, receive egress, and be reviewed or disabled.

### 12.8 Sage, Retrieval, And Topology

- `relationship_candidates`
- `relationship_ontology_proposals`
- `model_composition_members`
- `retrieval_affordance_profiles`
- `retrieval_motifs`
- `inquiry_sessions`
- `inquiry_evidence_items`
- `inquiry_question_runs`
- `inquiry_outcome_events`
- `region_sufficient_state`
- `sage_reader_activations`
- `sage_reader_decision_attributions`
- `sage_question_policy_stats`
- `model_structural_features`
- `model_edge_structural_features`
- `sage_topology_optimizer_runs`
- `discovery_shortcuts`
- `negative_memory`
- `retrieval_plans`
- `omitted_evidence`

These tables store the artifacts that make retrieval and reasoning more adaptive than simple vector search.

### 12.9 GitHub And Code Intelligence

GitHub/code intelligence was extracted from core and should return through the extension platform. Migration history still contains older GitHub and code-intel schema families:

- GitHub repo/branch/PR/issue/check state.
- GitHub signal enrichment.
- GitHub intelligence queue.
- Code snapshots.
- Code files.
- Code symbols.
- Code edges.
- Code embeddings.
- Code intelligence index triggers.

Treat these as historical/external-interface support rather than active core package ownership in this checkout.

### 12.10 Legacy And Retired Tables

The migrations include historical demo scaffolding and legacy topology tables. Some demo scaffolding is dropped by later migrations. Some topology tables remain for compatibility/history even though accepted-memory topology has shifted toward latent/candidate behavior.

## 13. Security, Tenancy, And Trust Boundaries

### 13.1 Tenant Isolation

Most durable rows carry `tenant_id`. Request/runtime code binds tenant context, and repositories generally require tenant-scoped operations.

Access-control logic lives in `services/platform/access_control`, while lower repositories enforce key invariants and tenant filters.

### 13.2 Authentication

Gateway authentication uses bearer sessions around `actor_sessions`. Public routes such as webhooks and health checks have different handling.

Development and integration-test paths may include shortcuts; production behavior should be reviewed through gateway middleware and deployment configuration.

### 13.3 Provider Verification

Webhook ingestion is a trust boundary. The webhook router:

- Reads raw body before parsing.
- Applies size limits.
- Resolves provider installations.
- Loads encrypted provider secrets.
- Verifies provider signatures.
- Handles lifecycle events carefully.
- Only then calls ingestion or Kafka cutover.

### 13.4 Secret Storage

Provider secrets are stored through the shared encrypted secret-store abstraction. Integration runtime wiring provides the concrete secret store to routers and workers.

### 13.5 Claim Trust

Not all memory has the same trust level. The system stores confidence, claim kinds, trust metadata, falsifiers, and evidence. High-confidence model creation has additional checks such as falsifier adequacy.

## 14. Extension Seams

### 14.1 Interface Manifests

`lib/extensions/manifest.py` discovers `company_os.interfaces` entry points. A manifest declares the extension id, version, trust tier, compatible host API range, contribution points, activation events, feature flag, and requested capabilities.

Core discovery is cached and failure-isolated: a broken manifest should not break host startup.

### 14.2 Gateway Extensions

`services/app/gateway/extensions.py` discovers `company_os.gateway_extensions` entry points. Extensions can contribute:

- Routers.
- Startup hooks.
- Public path prefixes.

The README emphasizes that demo/UI overlays should depend on core through extension seams rather than core importing overlay code.

### 14.3 Draft Enrichers

`services/ingest/ingestion/enrichers.py` discovers `company_os.draft_enrichers` entry points. Enrichers mutate an `ObservationDraft` before persistence, are channel-keyed, and are raw-on-failure. Extension enrichers must carry a manifest id and pass capability/grant checks through `services/platform/extensions/access.py`.

This is the replacement for the old hardcoded GitHub-intel inline hook.

### 14.4 Projection Extensions

The projection layer exposes two extension points:

- `company_os.projections`: projector factories that consume `model_events` and write new projection families.
- `company_os.projection_subject_resolvers`: retrieval subject resolvers that teach projection-first retrieval how to find extension-owned subjects.

Core projectors and resolvers are registered in `services/domain/projections`; extension discovery is cached and failure-isolated.

### 14.5 Background Workers

`lib/extensions/run_workers.py` discovers `company_os.workers` contributions and supervises every active worker in the `extension_workers` process. A worker whose manifest is missing or host-API-incompatible is skipped. One worker failure is logged and retried without killing sibling workers.

### 14.6 Reasoning Augmentors

The context planner can load reasoning augmentors through entry points such as `company_os.reasoning_augmentors`. This allows additional context enrichment without hard-wiring product or overlay behavior into reasoning core.

### 14.7 Event Subscribers And Egress

Domain writes schedule post-commit notifications, and app/realtime code listens to database notifications. Product schedulers and post-commit workers use this to refresh projections after memory mutation.

Extension egress lives under `services/platform/extensions/egress`. It materializes capability-filtered/redacted observation payloads into `extension_egress` for cursor pull and optionally schedules webhook push delivery.

### 14.8 Source Handlers

Adding a source usually means adding:

- A raw source literal if it participates in Kafka/S3 ingestion.
- Topic creation support.
- A handler channel in the registry.
- A source-specific handler that returns `ObservationDraft`.
- Webhook verification or polling/backfill workers.
- Provider installation and secret handling.
- Contract fixtures and tests.

## 15. Observability And Operations

The system exposes and stores operational state in several places:

- `/healthz`, `/readyz`, and `/metrics` from the gateway.
- Worker `/healthz` and `/metrics` endpoints on `INGESTION_HEALTH_PORT` for long-running consumers/workers.
- Optional Prometheus/Grafana/exporter stack behind the Compose `observability` profile.
- `startup_status` and route-level startup failures.
- `think_runs`, `think_run_costs`, and `think_run_artifacts`.
- `applied_triggers` for idempotency and reasoning outcomes.
- `model_events`, `projection_checkpoints`, and `projection_snapshots` for projection freshness and rebuild debugging.
- `model_reeval_dead_letter` and Think worker dead-letter behavior.
- `ingestion_failures` and DLQ topics.
- Embedding backlog state.
- Summarization worker/batch state for large documents.
- Onboarding progress events and onboarding run tables.
- `audit_events`, `installation_audit_log`, `extension_audit_log`, and reconciliation events.
- Rendering cost records.
- Realtime replay cursors.
- Circuit breaker tenant flags for Kafka path rollback.
- Extension grants, killswitch rows, egress rows, and webhook delivery attempts.

The practical debugging path is:

1. Check gateway health and route availability.
2. Check source installation and tenant flags.
3. Check raw/normalized Kafka topics or inline ingest response.
4. Check observation row and dedupe fields.
5. Check `think_trigger_queue`.
6. Check `think_runs` and artifacts.
7. Check `pending_post_commit_actions`, `model_events`, and projection checkpoints/snapshots.
8. Check applied domain rows and product cache refresh.
9. For extension-owned behavior, check manifest discovery, grants, killswitch, egress, and extension worker metrics.

## 16. Testing And Evaluation

The repository has broad test and evaluation coverage rather than a single test style.

Major areas:

- Unit tests around repositories, handlers, routing, retrieval, reasoning, product code, and workers.
- Integration tests requiring Postgres, Kafka, Ollama, or Docker depending on markers.
- Contract tests for provider payloads and webhook behavior.
- End-to-end and load tests under `tests/integration`, `tests/e2e`, and related directories.
- Real-LLM scenario tests and quality replay.
- Retrieval and synthesis harnesses.
- Benchmark adapters and runners for memory/retrieval datasets.

Pytest markers include:

- `integration`
- `ollama`
- `slow`
- `subprocess_e2e`
- `real_llm`
- `requires_infra`
- `requires_docker`
- `contract`

Benchmark code lives under `benchmarks/`. Raw datasets and cache directories are local artifacts and should not be treated as core source.

## 17. Codebase Structure

This is the working source map.

```text
.
+-- README.md
+-- pyproject.toml
+-- docker-compose.yml
+-- docker-compose.codex-auth.yml
+-- docker-compose.pgadmin.yml
+-- docker-compose.per-source.yml
+-- docker-compose.sandbox.yml
+-- contracts/
+-- db/
|   +-- migrations/
+-- docs/
|   +-- architecture/
|   +-- ingestion/
|   +-- product/
|   +-- reference/
|   +-- ...
+-- lib/
|   +-- shared/
|   +-- llm/
|   +-- embeddings/
|   +-- extensions/
|   +-- integrations/
|   +-- nexus/
|   +-- observability/
+-- services/
|   +-- app/
|   |   +-- gateway/
|   |   +-- realtime/
|   |   +-- webhooks/
|   +-- domain/
|   |   +-- actors/
|   |   +-- acts/
|   |   +-- bridge/
|   |   +-- entity_aliases/
|   |   +-- models/
|   |   +-- observations/
|   |   +-- projections/
|   |   +-- resources/
|   +-- ingest/
|   |   +-- ingestion/
|   |   +-- integrations/
|   |   +-- synthetic/
|   +-- platform/
|   |   +-- access_control/
|   |   +-- execution/
|   |   +-- extensions/
|   |   +-- runtime/
|   +-- reasoning/
|   |   +-- calibration/
|   |   +-- contestability/
|   |   +-- dynamics/
|   |   +-- edge_intelligence/
|   |   +-- judgment/
|   |   +-- oracle/
|   |   +-- relationships/
|   |   +-- retrieval/
|   |   +-- sage/
|   |   +-- synthesis/
|   |   +-- think/
|   |   +-- topology/
|   +-- product/
|   |   +-- ask/
|   |   +-- conversations/
|   |   +-- decision_deltas/
|   |   +-- forecasts/
|   |   +-- greeting/
|   |   +-- history/
|   |   +-- model_trace/
|   |   +-- query/
|   |   +-- recommendations/
|   |   +-- rendering/
|   |   +-- resolution_threads/
|   |   +-- today/
|   +-- workers/
|       +-- anomaly_processor/
|       +-- calibration_updater/
|       +-- deadline_resolver/
|       +-- edge_drift/
|       +-- entity_resolver/
|       +-- housekeeper/
|       +-- maintenance/
|       +-- precipitation/
|       +-- relationship_ontology_proposals/
|       +-- sage_structural_features/
|       +-- sage_topology_optimizer/
|       +-- topology_sweeper/
+-- scripts/
+-- tests/
+-- benchmarks/
+-- ui/
```

### 17.1 Root Files

- `README.md`: local setup and high-level backend overview.
- `pyproject.toml`: package metadata, dependencies, pytest settings, optional dependency groups, and import-linter contracts.
- `docker-compose*.yml`: local and production-like process graphs.
- `contracts/http-routes.json`: route contract data.

### 17.2 `db/migrations`

SQL migrations define the database contract. Use these to understand actual schema reality. Repository code and docs can drift; migrations are the durable schema history.

### 17.3 `docs`

Documentation is split by area:

- `docs/architecture`: layer-specific notes.
- `docs/ingestion`: ingestion design and source isolation.
- `docs/product`: product-specific notes.
- `docs/reference`: broad reference docs, including this file.

Some older docs may predate the current layered source layout. Prefer checking code for final confirmation.

### 17.4 `scripts`

Scripts start local workers, run migrations, manage data plane setup, operate ingestion recovery, re-enable Kafka tenants, run dogfood environments, and support tests/evaluations.

Important script families:

- Gateway and local dogfood startup.
- Think worker runners.
- Post-commit worker runners.
- Topology sweeper runner.
- Ingestion worker runners.
- Migration and infra helpers.
- Benchmark/evaluation helpers.

### 17.5 `tests`

Tests are organized by layer and behavior rather than one strict hierarchy. Expect:

- Unit tests near packages or under top-level test directories.
- Integration tests requiring services.
- Contract fixtures for third-party sources.
- E2E and replay harnesses.
- Real-LLM tests gated by markers.

### 17.6 `benchmarks`

Benchmark code provides adapters, answerers, evaluators, packet compilers, metrics, judges, and reporting around external memory/retrieval tasks. Local dataset caches are not core source.

### 17.7 `ui`

The local `ui/` directory is not the active frontend source in this checkout. Treat it as generated/residual unless a future change restores source files and package metadata.

## 18. How To Extend The System

### 18.1 Add A New Ingestion Source

Typical steps:

1. Add the source to raw-tier source literals if it should use Kafka/S3.
2. Add Kafka topic support through existing topic derivation.
3. Create or extend source install/callback flow under `services/ingest/integrations`.
4. Add provider verification in `services/app/webhooks` if webhooks are public.
5. Add a handler under `services/ingest/ingestion/handlers`.
6. Register the handler channel.
7. Return `ObservationDraft`, not direct database writes.
8. Add source-specific provider installation and secret storage.
9. Add contract fixtures and handler tests.
10. Add backfill/reconciliation workers if the source has history.

The goal is to converge all source paths on `ingest_from_draft`.

### 18.2 Add A New Domain Concept

Typical steps:

1. Add migrations first.
2. Add typed shared models if the concept crosses layers.
3. Add a domain repository that owns invariants.
4. Add state-change observation behavior if product/reasoning needs a timeline.
5. Add Think diff schema operations if reasoning should mutate it.
6. Add access-control behavior.
7. Add product read APIs or bridge queries.
8. Add tests at repository, reasoning, and product layers as needed.

### 18.3 Add A New Reasoning Operation

Typical steps:

1. Extend `diff_schema.py` with a structured operation.
2. Extend validation rules.
3. Extend `applier.py`.
4. Ensure idempotency and advisory locking where needed.
5. Add repository methods instead of writing SQL inline.
6. Add run artifact/debug information.
7. Add focused tests around invalid diffs, allowed-region behavior, and application idempotency.

### 18.4 Add A Product Surface

Typical steps:

1. Start under `services/product/<surface>`.
2. Read domain state through repositories, bridge queries, or platform retrieval.
3. Keep rendering separate from memory mutation.
4. Add a router.
5. Mount it in `services/app/gateway/route_mounts.py` or expose it through a gateway extension.
6. Add access-control checks.
7. Add product-level tests and route contracts if externally visible.

### 18.5 Add A Worker

Typical steps:

1. Put reusable worker logic under `services/workers/<name>` or the owning package.
2. Add a script runner under `scripts/` if needed.
3. Add Compose service wiring only when it should run in the standard stack.
4. Use database leasing or idempotency tables for durable work.
5. Record operational state for debugging.
6. Add health/metrics/logging behavior where appropriate.

## 19. Known Architectural Caveats

These are current realities a maintainer should know:

- The active source is layered. Older docs may still name legacy top-level `services/<name>` packages, but this checkout's active tree is under `services/app`, `services/domain`, `services/ingest`, `services/platform`, `services/product`, `services/reasoning`, and `services/workers`.
- `github_intel` and `code_intel` are extracted interface capabilities in this repo state; residual cache directories or historical migrations are not active core package ownership.
- Domain currently has a few upward imports into reasoning. Respect them when changing import boundaries.
- The deterministic execution router exists and records route decisions, but current inline ingestion primarily uses the direct ingest path rather than routing every event through that router.
- Some worker packages are implemented but not wired into main Compose.
- Local `ui/` is not active source.
- Some docs refer to older process wiring. Confirm current runtime with `docker-compose.yml`, `scripts/`, and code.
- Dev/local auth and public-path behavior should be reviewed before assuming production security posture.
- Compose is a convenient single-machine process graph, not a complete high-availability deployment architecture.
- Older topology and demo schema artifacts remain in migration history for compatibility.

## 20. Source Of Truth Order

When code, docs, and mental models disagree, prefer this order:

1. Database migrations for schema reality.
2. Current package code under `lib/` and layered `services/`.
3. Compose files and scripts for process reality.
4. Tests and contract fixtures for expected behavior.
5. Current docs in `docs/architecture`, `docs/ingestion`, and `docs/reference`.
6. Historical docs and migration residue only after checking current imports.

## 21. Minimal Mental Model For New Engineers

If you only remember one thing, remember this:

```text
Sources become Observations.
Observations enqueue Think.
Think mutates Models, Edges, Acts, Resources, and Predictions through validated diffs.
Model writes emit model_events.
Post-commit workers materialize projections, trigger follow-up work, and broadcast realtime updates.
Product surfaces render durable states and rebuildable projections.
Kafka/S3 makes ingestion replayable.
Postgres is the memory and coordination backbone.
```

Once that loop is clear, the rest of the codebase becomes much easier to navigate.
