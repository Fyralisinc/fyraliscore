# Fyralis Core System Architecture

Last reviewed from code in this checkout: 2026-06-24

This document explains the backend system architecture of Fyralis Core. It is
intentionally not a frontend architecture document and intentionally not an
application-surface walkthrough. Downstream API clients are mentioned only
where they form a runtime boundary.

Fyralis Core is a backend-only organizational intelligence runtime. Its job is
to capture company signals, normalize them into tenant-scoped observations,
maintain a durable memory graph, reason over that graph through controlled
mutation pipelines, and publish post-commit updates for downstream readers.

## 1. System In One Page

The shortest useful description is:

```text
external source event
  -> ingress transport
  -> raw capture or inline handler
  -> normalized ObservationDraft
  -> observation row
  -> think trigger
  -> retrieval and context planning
  -> deterministic or LLM-backed diff
  -> validation
  -> transactional apply
  -> model events and post-commit actions
  -> projections, relationship discovery, realtime notification, workers
```

The most important architectural boundary is PostgreSQL. It is the durable
system of record, the queue substrate, the vector-search substrate, the
idempotency ledger, the audit trail, and the handoff point between synchronous
request handling and asynchronous reasoning.

The second most important boundary is the structured reasoning diff. LLMs and
deterministic handlers do not directly mutate the database. They produce a
schema-shaped proposal; validators narrow it; repositories and appliers perform
the actual writes.

## 2. Scope

Included:

- FastAPI gateway composition and webhook ingress.
- Kafka/S3 raw ingestion and inline ingestion.
- Domain substrate: observations, actors, aliases, models, edges, acts,
  resources, model events, and projections.
- Reasoning: Think worker, retrieval, adaptive inquiry, validation, applier,
  post-commit actions, topology, relationships, Sage, and background workers.
- Platform infrastructure: access control, extension governance, execution
  runtime, process manifest.
- Cross-cutting operational contracts: tenancy, idempotency, queues, RLS,
  observability, extension seams, and validation boundaries.

Excluded:

- Frontend architecture.
- Demo overlay architecture.
- Application-page or user-experience behavior beyond the backend boundary where
  system updates are made available to readers.

## 3. Layer Model

The codebase is a layered Python monolith. The layers are physical directories
under `services/`, with shared primitives under `lib/`.

```text
app / workers
     |
     v
reasoning / ingest / platform
     |
     v
domain
     |
     v
lib
```

Higher layers may call lower layers. Lower layers should not call upward.
`pyproject.toml` enforces this with import-linter contracts:

- Core cannot import demo or simulation overlay packages.
- `lib` cannot import `services`.
- `services.reasoning` cannot directly import `services.app`,
  `services.product`, or `services.ingest`.
- `services.domain` and `services.ingest` have ratchet contracts preventing new
  upward imports beyond explicitly allowlisted debt.
- `lib.extensions` cannot import `services`, preserving the public extension
  host API boundary.

Layer responsibilities:

| Layer | Paths | Responsibility |
| --- | --- | --- |
| Shared primitives | `lib/shared`, `lib/llm`, `lib/embeddings`, `lib/extensions`, `lib/integrations`, `lib/observability` | IDs, DB helpers, errors, typed contracts, LLM providers, embedding providers, extension host APIs, event bus, secrets, telemetry helpers. |
| Domain | `services/domain/*` | Canonical persisted substrate and repository invariants. Owns observations, models, model edges, acts, resources, projections, actors, aliases, bridge queries, and trigger helpers. |
| Ingest | `services/ingest/*` | Source clients, webhook/poller/backfill adapters, raw-tier envelopes, normalizers, observation writers, summarization and embedding workers, feature flags, synthetic scenarios. |
| Platform | `services/platform/*` | Access control, adaptive inquiry/execution runtime, extension governance, runtime process manifest. |
| Reasoning | `services/reasoning/*` | Think pipeline, retrieval, context assembly, validation, applier, topology, relationship intelligence, Sage, calibration, oracle/outcome facts, dynamics. |
| App | `services/app/*` | FastAPI gateway, route mounting, middleware, webhooks, realtime WebSocket transport. |
| Workers | `services/workers/*`, `scripts/run_*.py` | Long-running background processes around reasoning, ingestion support, topology, entity resolution, anomaly detection, maintenance, and lifecycle sweeps. |

## 4. Runtime Topology

The production-style stack is defined primarily by `docker-compose.yml` and
`services/platform/runtime/process_manifest.py`.

### 4.1 Infrastructure

| Service | Role |
| --- | --- |
| Postgres with pgvector | Primary system of record, vector index, durable queues, audit log, projection store, integration state, idempotency ledger. |
| Kafka | Durable ingestion bus for raw, normalized, embedding, DLQ, onboarding, and workflow events. |
| S3-compatible storage | Raw payload object store. Local development uses MinIO. |
| Redis | Rate-limit and coordination dependency for selected ingestion/recovery paths. |
| Ollama | Local embedding service, defaulting to `nomic-embed-text` with 768-dimensional vectors. |
| External LLM providers | Structured reasoning and planning calls through `lib.llm.provider`. |

### 4.2 Long-Running Processes

Process families:

| Family | Representative processes | Role |
| --- | --- | --- |
| Ingress | `gateway` | HTTP, webhook, OAuth callback, route composition, dependency wiring, realtime startup, extension startup. |
| Onboarding/backfill | `oauth_poller`, `tenant_onboarding`, `source_onboarding`, `shard_fetch`, `reconciler`, `feels_onboarded_monitor`, `periodic_reconciler` | Source installation, initial backfill, shard fetching, gap detection, progress events, steady-state reconciliation. |
| Kafka consumers | `normalizer`, `observation_writer`, `dlq_writer`, `summarization_worker`, `summarization_batch_worker`, `embedding_worker`, `embedding_backlog`, `circuit_breaker` | Raw-to-normalized transforms, observation persistence, DLQ persistence, large-document summarization, embedding retry and backlog drain, Kafka cutover safety. |
| Live source workers | Discord, Telegram, Signal, Gmail, Google Calendar, Google Drive workers | Long-lived source ingestion for systems that require sessions, watches, polling, or cursor maintenance. |
| Reasoning workers | `think_worker`, `post_commit_worker`, `anomaly_processor_worker`, `entity_resolver_worker`, Sage workers, relationship ontology worker, housekeeper | Think queue drain, post-commit queue drain, anomaly triggers, deferred entity resolution, structural feature refresh, topology optimization, lifecycle maintenance. |
| Extension workers | `extension_workers` | Discovers and supervises installed extension background workers via `company_os.workers`. |

Local dogfood uses a smaller profile: gateway, Think worker, post-commit worker,
and often a topology sweeper. Production mode includes the ingestion data plane
and live source workers.

## 5. Core Persistent Substrate

### 5.1 Tenant Boundary

Most durable rows carry `tenant_id`. Gateway authentication resolves bearer
sessions to actor and tenant context. Repositories and queries scope reads and
writes by tenant, and later migrations add RLS policies across tenant tables.

Tenant isolation is enforced in layers:

- Auth context at the gateway.
- Repository query scoping.
- Access-control checks under `services/platform/access_control`.
- RLS policies on many tenant tables.
- Worker filters and per-tenant queue/concurrency controls.

### 5.2 Actors And Identity

Actors are canonical people, agents, and system participants.

Substrate:

- `actors`
- `actor_identity_mappings`
- `actor_sessions`
- `services/domain/actors/repo.py`

External systems use incompatible identity schemes, so ingestion resolves
source-specific actor refs into canonical actors. When an actor cannot be
resolved, ingestion preserves the unresolved reference in observation content
and may open a clarification request.

### 5.3 Entity Aliases

Entity aliases map source text and phrases to canonical entity references.

Substrate:

- `entity_aliases`
- `entity_review_queue`
- `services/domain/entity_aliases/repo.py`
- `services/workers/entity_resolver/worker.py`

Inline ingestion performs fast alias lookup from candidate phrases. Unresolved
phrases are stored on the observation. The entity resolver worker later inspects
those phrases with more context, can add aliases, can update
`observations.entities_mentioned`, and can re-enqueue Think when a material
entity is resolved.

### 5.4 Observations

Observations are append-oriented source signals.

Substrate:

- `observations`
- monthly partitions by `occurred_at`
- vector embeddings as `VECTOR(768)`
- `services/domain/observations/repo.py`
- `services/domain/observations/events.py`

Observation insert behavior:

- Dedupes source events by source channel and external ID.
- Stores canonical tenant, actor, content, raw JSON content, trust tier,
  entities, cause, and embedding state.
- Self-heals missing partitions in guarded paths.
- Schedules post-commit `observations_new` notifications.
- Enqueues T1 Think triggers when appropriate.

Observations are the raw evidence layer. Reasoning can read them and cite them;
it should not casually rewrite them.

### 5.5 Models And Model Edges

Models are durable beliefs, claims, predictions, concerns, situations,
recommendations, and other structured memory items.

Substrate:

- `models`
- `model_edges`
- `model_scope_entities`
- `model_scope_actors`
- `model_composition_members`
- `model_signal_readings`
- `model_status_notes`
- `model_semantic_terms`
- `model_open_questions`
- `services/domain/models/repo.py`
- `services/domain/models/edges_repo.py`

`ModelsRepo` owns formation, validation, confidence, activation, embeddings,
falsifier checks, scope sidecars, state-change emission, audit emission, search,
retrieval counters, semantic sidecars, open questions, and neutral model events.

`EdgesRepo` owns typed relationships between Models. Accepted edges represent
meaning in the memory graph: support, tension, causal, blocking, analogy,
co-occurrence, warning, and dynamically governed relationship kinds.

### 5.6 Acts And Resources

Acts are operational objects:

- `goals`
- `commitments`
- `decisions`
- act relationship tables such as `contributes_to`, `depends_on`, and
  `constrained_by`

Resources are business and operational assets:

- `resources`
- `resource_transactions`
- `resource_deployments`
- releases and deployment records
- `customer_commitments`

Think can mutate acts and resources only through validated `act_ops` and
`resource_ops`. These tables connect memory to operational state without making
the LLM a direct database writer.

### 5.7 Model Events And Projections

Model events are neutral belief-change events emitted by model writes.

Substrate:

- `model_events`
- `projection_checkpoints`
- `projection_snapshots`
- `services/domain/models/events.py`
- `services/domain/projections/runtime.py`
- `services/domain/projections/repo.py`

The projection runner consumes `model_events` and materializes rebuildable
snapshots. A projection snapshot is a compact read/retrieval index over source
Models, not the source of truth. If snapshots are deleted or stale, they can be
rebuilt from `model_events` and canonical Models.

This is one of the cleanest internal seams:

```text
model mutation
  -> model_events
  -> projection runner
  -> projection_snapshots
  -> retrieval/read contexts
```

### 5.8 Queues And Ledgers

The system uses Postgres tables as durable work queues and idempotency ledgers.

| Table | Role |
| --- | --- |
| `think_trigger_queue` | Durable queue for Think triggers. Leased with `FOR UPDATE SKIP LOCKED`. |
| `model_reeval_queue` | Background reevaluation source promoted into T4 Think triggers. |
| `pending_post_commit_actions` | Durable post-commit side-effect queue, written in the same transaction as the Think apply. |
| `applied_triggers` | Idempotency ledger for applied Think triggers. |
| `think_runs` | Run status, counts, errors, and applied summaries. |
| `think_run_costs` | LLM/cost/latency records, split by purpose when available. |
| `think_run_artifacts` | Debug artifacts for retrieval, response, validation, apply, and errors. |
| `reconciliation_events` | Duplicate or merge decisions during model insertion. |
| `audit_events` | Model and state-change audit chain. |
| `relationship_candidates` | Pre-acceptance lifecycle for edge, situation, or edge-type proposals. |
| `inquiry_sessions`, `inquiry_question_runs`, `inquiry_evidence_items` | Adaptive inquiry traces and evidence reservoirs. |

## 6. App And Transport Layer

### 6.1 Gateway Composition

`services/app/gateway/main.py` is the FastAPI composition root. It builds or
receives runtime dependencies and owns process startup/shutdown.

Startup wires:

- asyncpg pool.
- actor and alias repositories.
- embedder client.
- rate limiter.
- gateway dependencies.
- integration runtime state.
- GitHub and OAuth cleanup state.
- realtime dispatcher.
- optional scheduled view refreshers for downstream readers.
- ingestion data-plane producers.
- gateway extension startup hooks.

`services/app/gateway/route_mounts.py` is the route registry. It mounts core
routes, webhook routes, integration routes, debug/internal routes when enabled,
extension routers, and downstream API routers. This file is the place to inspect
when a runtime endpoint exists but the owning module is unclear.

### 6.2 Middleware And Request Context

Gateway middleware handles:

- request IDs and structured logging context.
- bearer-session authentication.
- tenant and actor binding.
- public path bypasses for health, auth bootstrap, and provider webhooks.
- rate limiting.

Webhook routes bypass normal user-session auth but must perform provider-specific
verification before data is accepted.

### 6.3 Webhooks

`services/app/webhooks/router.py` handles provider callbacks. Its system role is
to verify and route payloads, not to own business memory.

Typical webhook flow:

```text
provider request
  -> capture raw bytes
  -> enforce size limits
  -> resolve provider installation and tenant
  -> load provider secrets
  -> verify signature or token
  -> publish raw envelope to Kafka/S3 when cutover is enabled
  -> fallback to inline ingest when needed
```

### 6.4 Realtime Dispatcher

`services/app/realtime/dispatcher.py` holds a dedicated asyncpg connection that
listens on `observations_new`.

The event path is:

```text
domain write
  -> notification scheduled in transaction scope
  -> transaction commits
  -> notification emitted on a fresh connection
  -> dispatcher receives LISTEN payload
  -> per-client queue
  -> WebSocket frame
```

The dispatch queue has bounded backpressure. When a client queue is full, the
oldest items are dropped and a lag control frame is emitted so the newest state
can still reach the client.

### 6.5 Extension Gateway Seam

`services/app/gateway/extensions.py` discovers
`company_os.gateway_extensions` entry points. An extension can contribute:

- routers.
- startup hooks.
- public path prefixes.

Core never imports overlay or extension code directly. One bad extension load is
logged and isolated rather than blocking the bare core gateway.

## 7. Ingestion Architecture

The ingestion layer has two write paths that converge on one domain operation.

```text
inline path:
  gateway/internal caller
    -> handler
    -> ObservationDraft
    -> ingest_from_draft

Kafka/S3 path:
  gateway/worker/backfill
    -> RawEnvelope + raw object
    -> normalizer
    -> NormalizedEnvelope
    -> observation writer
    -> ingest_from_draft
```

The convergence point is `services/ingest/ingestion/core.py::ingest_from_draft`.
This prevents inline and Kafka ingestion from evolving into two different
systems.

### 7.1 Source Handlers

Handlers live under `services/ingest/ingestion/handlers`. They translate a
source payload into an `ObservationDraft`.

The draft carries:

- source channel.
- content text.
- raw structured content.
- source actor reference.
- external source ID.
- occurred time.
- trust tier.
- kind.
- entity hints.
- unresolved phrase hints.

Handlers should be adapter code. Durable writes belong in the shared ingest path
and domain repositories.

### 7.2 Inline Ingest

`ingest()` validates payload shape, dispatches the handler, and calls
`ingest_from_draft`.

`ingest_from_draft` performs:

1. Extension draft enrichment through `company_os.draft_enrichers`.
2. Large-document summarization preparation when configured.
3. uuid7 observation ID preassignment.
4. actor resolution.
5. entity alias resolution.
6. embedding generation or `embedding_pending`.
7. `ObservationCreate` construction.
8. transactionally insert observation and optionally enqueue T1.
9. post-commit notification flush.
10. best-effort embedding or summarization retry publish.

### 7.3 Raw Tier

`services/ingest/ingestion/raw_tier/envelope.py` defines `RawEnvelope`.

The raw tier stores the payload body in object storage and sends a pointer
through Kafka:

```text
RawEnvelope {
  source,
  tenant_id,
  raw_s3_key,
  content_hash,
  ingested_at,
  ingress_kind,
  ingress_metadata,
  idem_hints
}
```

Raw topics are source-specific: `ingestion.raw.<source>`.

The raw tier gives the system replay and reconciliation ability. External
providers can be acknowledged after durable raw capture rather than after full
normalization and reasoning.

### 7.4 Normalizer

`services/ingest/ingestion/normalizer/worker.py` consumes raw envelopes and
publishes normalized envelopes.

Normalizer contract:

- consume `ingestion.raw.<source>`.
- fetch raw payload from S3/MinIO.
- validate envelope invariants.
- resolve source and ingress kind to the handler channel.
- call the handler registry.
- publish `ingestion.normalized.<source>`.
- publish unsupported or invalid input to DLQ.
- never access Postgres.

The no-Postgres rule is load-bearing. Normalization must be replayable without
database state.

### 7.5 Observation Writer

`services/ingest/ingestion/writers/observation_writer.py` consumes normalized
envelopes and persists observations.

Writer responsibilities:

- parse and validate `NormalizedEnvelope`.
- respect tenant cutover flags.
- preserve shadow/no-op behavior only when explicitly disabled.
- always persist backfill envelopes because backfill has no inline fallback.
- reconstruct `ObservationDraft`.
- call `ingest_from_draft`.
- retry transient failures.
- DLQ permanent failures.
- cap deterministic poison messages with a durable attempt counter.
- commit Kafka offsets only after definitive outcome.

### 7.6 Summarization And Embeddings

Large documents can be stored as pending-summary observations. Summarization
workers later summarize the source text and resume the normal observation/Think
flow. Embedding workers and the embedding backlog drainer handle
`embedding_pending` rows without making ingestion fail on transient embedder
outages.

### 7.7 Feature Flags And Circuit Breakers

Tenant flags govern Kafka-path cutover. A missing flag defaults to Kafka-first.
An explicit false value keeps live writes on inline ingest while the writer
shadow-logs normalized events. Circuit-breaker code can flip tenants back to
shadow behavior when lag or failure patterns cross safety thresholds.

### 7.8 Onboarding And Reconciliation

Workflow services manage initial and continuous source coverage:

- OAuth polling.
- tenant onboarding.
- source onboarding.
- shard fetch.
- reconciliation.
- progress events.
- "feels onboarded" checks.
- periodic gap reconciliation.

The important design point: backfill and live ingestion produce raw envelopes
that converge on the same normalizer and observation writer path.

## 8. Reasoning Architecture

Reasoning transforms triggers into validated memory mutations.

```text
think_trigger_queue
  -> ThinkWorker leases trigger
  -> plan context
  -> retrieve evidence
  -> assemble bounded context
  -> build reasoning frame
  -> deterministic handler or LLM
  -> raw diff
  -> validation
  -> applier
  -> audit, model events, post-commit actions
```

### 8.1 Think Triggers

Common trigger families:

| Trigger | Source | Meaning |
| --- | --- | --- |
| T1 | ingestion | New event arrived. |
| T2/T3 | belief, prediction, anomaly, contestability | Existing memory needs evaluation. |
| T4 | background/topology/reevaluation | Maintenance, model reeval, latent relationship, representation repair. |
| T6 | legacy topology-specialized work | Compatibility and specialized topology paths. |

`ThinkWorker` drains `think_trigger_queue`, promotes `model_reeval_queue` rows
into T4 triggers, enforces per-tenant concurrency, leases rows with
`FOR UPDATE SKIP LOCKED`, retries failures, and marks exhausted rows.

### 8.2 Context Planning And Retrieval

`services/reasoning/think/context_planner.py`,
`services/platform/execution/inquiry.py`, and
`services/reasoning/retrieval/*` build evidence for a trigger.

Context planning gathers:

- direct trigger context.
- structural retrieval over entity scopes, acts, resources, and model edges.
- semantic retrieval through embeddings.
- temporal retrieval around the trigger.
- pattern and motif retrieval.
- projection-first context from `projection_snapshots`.
- adaptive inquiry evidence cards and sufficiency verdicts.
- actor operating context.
- optional second-pass retrieval.

Retrieval output is compressed by `services/reasoning/retrieval/assembler.py`
into a bounded context bundle: observations, Models, acts, resources, bridge
context, selection notes, and prompt-survival telemetry.

### 8.3 Reasoning Frame

The reasoning frame turns a trigger into the concrete question the run should
answer. It records seed Models, candidate Models, allowed operation surfaces,
budgets, entity scope, source profile, relationship-candidate context, dynamic
signals, and region boundaries.

This frame is stored in debug artifacts and apply observability, so an operator
can inspect not only the LLM response but also the task the system thought it
was asking.

### 8.4 Raw Diff

Reasoning produces a `RawDiff`, not arbitrary SQL.

Diff buckets include:

- `claim_ops` for Models.
- `memory_lifecycle_ops`.
- `relation_claim_ops` and `relation_frame_ops`.
- `edge_ops` for model graph edges.
- `ontology_gap_ops`.
- `open_question_ops`.
- `formation_resolutions`.
- `act_ops` for goals, commitments, and decisions.
- `resource_ops` for resources, transactions, deployments, releases, and
  customer commitments.
- `new_predictions`.
- `reasoning_trace`.

Authoritative/deterministic triggers use deterministic handlers. Inferential
triggers call the configured LLM provider. Narrow deterministic safety nets can
inject missing operational ops for high-value cases, such as commitment
creation, blocked work transitions, decision revisits, future predictions, and
customer risk.

### 8.5 Validation

`services/reasoning/think/validator.py` validates the raw diff against:

- tenant identity.
- allowed region.
- valid model and edge references.
- operation schemas.
- same-diff references from new claims to edges/actions.
- proposition shapes.
- operation budget and surface rules.
- evidence and context-use expectations.

Validation can partially accept a diff by dropping invalid late-discovered ops.
Unexpected database or integrity failures still fail the run.

### 8.6 Mutation Transaction

`services/reasoning/think/reason.py::_run_once` separates expensive
pre-mutation work from mutation when narrow transactions are enabled.

The mutation transaction does:

1. insert or update `think_runs`.
2. acquire advisory region locks.
3. store response artifact.
4. validate raw output.
5. call `apply_diff`.
6. adjudicate relationship candidates.
7. record representation audit.
8. record apply observability and context-use telemetry.
9. write post-commit actions.
10. finalize run status.

`apply_diff` in `services/reasoning/think/applier.py` is the write boundary.
It inserts a pending `applied_triggers` row before mutation, applies claim ops
before dependent edge/act/resource ops, emits state-change observations,
emits model events, records audit/reconciliation, and marks the trigger outcome
inside the same transaction.

### 8.7 Idempotency And Locks

Reasoning has several concurrency guards:

- early `applied_triggers` check can skip already-applied triggers before
  paying retrieval/LLM cost.
- `apply_diff` inserts `applied_triggers` inside the transaction as the final
  correctness guard.
- Think worker leases queue rows with `FOR UPDATE SKIP LOCKED`.
- per-tenant concurrency defaults to one active Think mutation.
- region locks serialize overlapping entity/model write regions.
- a short tenant model-write lock protects apply-time graph mutation.
- deadlock and serialization errors are retried with bounded backoff.

### 8.8 Context-Use Telemetry

`services/reasoning/think/context_use.py` grades whether a successful diff used
the selected context. It records references to selected observations, selected
Models, graph-selected Models, and justified no-op trace references.

The result is stored in `think_runs.ops_applied["context_use"]`, emitted to
metrics, and captured in debug artifacts. This prevents a false sense of
quality where retrieval found good evidence but the diff ignored it.

## 9. Post-Commit Architecture

Post-commit actions are durable side effects written inside the same transaction
as the memory mutation.

`services/reasoning/think/post_commit.py` enqueues:

- `publish_anomalies`
- `schedule_predictions`
- `broadcast_realtime`
- `invalidate_metrics`
- `materialize_projections`
- `discover_model_edges`
- `search_open_questions`

The post-commit worker drains `pending_post_commit_actions` with
`FOR UPDATE SKIP LOCKED`. Each handler must be idempotent because dispatch is
at-least-once. Failures increment attempts and reschedule with exponential
backoff. Exhausted rows are dead-lettered.

The durable queue fixes a critical crash window:

```text
bad old shape:
  apply commits
  process crashes before side effects
  retry sees applied_triggers and skips
  side effects are lost

current shape:
  apply and pending_post_commit_actions commit together
  process crashes
  post_commit_worker later drains durable actions
```

## 10. Relationship And Topology Architecture

Relationship intelligence has two levels:

- accepted memory: `model_edges`.
- candidate memory: `relationship_candidates`.

Topology and edge-intelligence code searches for useful latent relationships,
but it writes proposals into the relationship-candidate lifecycle rather than
silently mutating accepted edges. T4 Think adjudicates candidates and can accept,
reject, retire, or mark them for review.

Subcomponents:

| Component | Paths | Role |
| --- | --- | --- |
| Relationship candidates | `services/reasoning/relationships/*` | Candidate repository, adjudication, promotion, ontology proposals. |
| Latent topology | `services/reasoning/topology/*`, `services/workers/topology_sweeper` | Pattern discovery over high-activation memory frontier. |
| Edge intelligence | `services/reasoning/edge_intelligence/*` | Relation frame extraction, endpoint quality, pair-evidence promotion, context feedback. |
| Judgment scoring | `services/reasoning/judgment/scoring.py` | Shared leverage and usefulness scoring. |

The invariant is that discovery proposes; Think validates and applies.

## 11. Sage And Execution Architecture

Sage is a reasoning-support layer rather than a separate source of truth. It
adds structural features, region summaries, inquiry traces, discovery shortcuts,
negative memory, affordance profiles, topology optimization, outcome evaluation,
and reader behavior that can improve retrieval and reasoning.

Important paths:

- `services/reasoning/sage/*`
- `services/platform/execution/*`
- `services/workers/sage_structural_features`
- `services/workers/sage_topology_optimizer`

The platform execution runtime provides adaptive inquiry:

```text
trigger
  -> route semantics
  -> baseline retrieval
  -> hypotheses
  -> discriminating questions
  -> retrieval actions
  -> evidence reservoir
  -> sufficiency verdict
  -> synthesis context packet
```

Think uses deep inquiry before mutation. Read/query-style callers can use a
faster retrieval mode without entering the mutation pipeline.

## 12. Background Workers

Workers are mostly queue or poll loops around the same substrate.

| Worker | Role |
| --- | --- |
| `think_worker` | Drains `think_trigger_queue` and runs Think. |
| `post_commit_worker` | Drains `pending_post_commit_actions`. |
| `entity_resolver_worker` | Resolves unresolved aliases and re-enqueues Think when material. |
| `anomaly_processor_worker` | Detects anomalies and enqueues T3 triggers. |
| `deadline_resolver` | Converts overdue predictions/deadlines into Think triggers. |
| `precipitation` | Clusters candidate patterns and proposes background reasoning. |
| `relationship_ontology_proposals_worker` | Maintains relationship ontology proposal lifecycle. |
| `sage_structural_features_worker` | Refreshes structural feature rows. |
| `sage_topology_optimizer_worker` | Optimizes topology/reader behavior. |
| `housekeeper_worker` and `maintenance` | Lifecycle, cleanup, decay, and scheduled maintenance. |
| `extension_workers` | Runs extension-provided background workers through host API. |

Workers should not invent alternate write paths. They should enqueue triggers,
call domain repositories, or consume durable queues.

## 13. Extension Architecture

Core exposes extension seams but does not import overlay code.

Entry-point groups:

| Entry point | Owned by | Purpose |
| --- | --- | --- |
| `company_os.gateway_extensions` | `services/app/gateway/extensions.py` | Contribute routers, startup hooks, public prefixes. |
| `company_os.draft_enrichers` | `services/ingest/ingestion/enrichers.py` | Enrich observation drafts before persistence. |
| `company_os.workers` | `lib/extensions/run_workers.py` | Contribute background workers. |
| `company_os.interfaces` | `lib/extensions/manifest.py` | Discover extension manifests/interfaces. |
| `company_os.event_subscribers` | `lib/shared/events.py` | Subscribe to process-local events. |
| `company_os.reasoning_augmentors` | `services/reasoning/think/hooks.py` | Add reasoning context augmentors. |
| `company_os.projections` | `services/domain/projections/catalog.py` | Contribute projectors. |
| `company_os.projection_subject_resolvers` | `services/domain/projections/subjects.py` | Contribute projection subject resolvers. |

Extension governance lives under `services/platform/extensions`: grants,
identity, provenance, egress planning/delivery, marketplace metadata,
redaction, audit, and kill switches.

This preserves a stable host API while keeping core independent from demo,
overlay, and customer-specific code.

## 14. Cross-Cutting Invariants

### 14.1 Transaction Boundaries

Important writes happen transactionally:

- observation insert and T1 enqueue.
- Think validation, apply, audit, model events, and post-commit enqueue.
- projection snapshot and checkpoint updates per projector event.
- queue lease/update bookkeeping.

Post-commit notifications and durable side effects run after data is committed.

### 14.2 Idempotency

The system expects retries:

- source external IDs dedupe observations.
- Kafka consumers commit offsets after definitive handling.
- writer poison attempts are durable.
- `applied_triggers` dedupes Think application.
- post-commit action dedupe prevents duplicate pending actions for the same
  trigger/action.
- handlers are required to be idempotent under at-least-once dispatch.

### 14.3 Evidence And Auditability

Reasoning should be inspectable after the fact. The system records:

- `think_runs`.
- `think_run_artifacts`.
- `think_run_costs`.
- `context_use`.
- `reconciliation_events`.
- `audit_events`.
- `model_events`.
- relationship candidate adjudication metadata.
- projection checkpoints and staleness reasons.

The goal is that an operator can answer: what triggered this, what evidence was
available, what did the model propose, what did validation drop, what was
applied, and what side effects ran?

### 14.4 Rebuildable Views

Canonical state lives in observations, Models, edges, acts, resources, and audit
records. Projection snapshots and downstream caches are rebuildable read views.
This distinction keeps the memory graph from becoming hostage to any one view.

### 14.5 Import Direction

Architectural dependencies are real runtime constraints. A lower layer reaching
upward usually means the wrong object owns the behavior. Existing allowlisted
upward imports are tracked debt, not precedent.

## 15. Example Flows

### 15.1 Inline Event To Memory Mutation

Example: an internal caller posts a Slack-like event to inline ingest.

```text
POST /ingest/slack
  -> gateway validates request and tenant context
  -> services.ingest.ingestion.core.ingest("slack", payload)
  -> Slack handler returns ObservationDraft
  -> actor and aliases resolve
  -> observation embedding is computed
  -> observations row is inserted
  -> think_trigger_queue T1 row is inserted
  -> observations_new notification is emitted after commit
  -> ThinkWorker leases T1
  -> context planner retrieves related memory
  -> LLM or deterministic handler proposes RawDiff
  -> validator drops invalid ops and returns ValidatedDiff
  -> applier writes Models/edges/acts/resources
  -> model_events and pending_post_commit_actions are written
  -> post_commit_worker materializes projections and discovers edge candidates
```

### 15.2 Webhook Through Kafka/S3

Example: a provider webhook lands on a tenant with Kafka path enabled.

```text
provider webhook
  -> gateway captures raw bytes
  -> provider installation resolves tenant
  -> signature verification succeeds
  -> raw body stored in S3/MinIO
  -> RawEnvelope published to ingestion.raw.<source>
  -> normalizer consumes envelope
  -> normalizer fetches raw body and runs handler
  -> NormalizedEnvelope published to ingestion.normalized.<source>
  -> observation_writer consumes normalized envelope
  -> writer calls ingest_from_draft
  -> observation and T1 trigger commit
```

If raw publish fails, webhook handling can fall back to inline ingest for live
providers so the provider does not experience an ingestion-data-plane outage as
a failed callback.

### 15.3 Unknown Entity Alias Resolution

Example: an observation mentions "NBI" before the tenant has an alias for it.

```text
ingest_from_draft
  -> candidate phrase extraction sees "NBI"
  -> EntityAliasRepo has no safe match
  -> observation stores content._unresolved_phrases = ["NBI"]
  -> entity_resolver_worker wakes from observations_new or polling
  -> worker loads observation, same-channel context, known aliases, active Models
  -> LLM/context resolver chooses existing canonical entity or null
  -> high-confidence resolution inserts alias and updates observation entities
  -> material entity resolution re-enqueues T1
```

This keeps inline ingestion fast while allowing ambiguous human language to be
resolved with more context.

### 15.4 Large Document Backfill

Example: a backfilled Google Drive document is too large for direct reasoning.

```text
shard_fetch
  -> raw object stored in S3
  -> RawEnvelope ingress_kind=backfill
  -> normalizer produces NormalizedEnvelope
  -> observation_writer calls ingest_from_draft
  -> large-document logic converts draft to pending-summary observation
  -> observation commits without T1
  -> summarization worker summarizes source text
  -> completed summary resumes observation/embedding/Think flow
```

Backfill envelopes always persist even if live Kafka cutover is disabled,
because backfill has no inline fallback once its cursor has advanced.

### 15.5 Model Update To Projection Snapshot

Example: Think updates a model about an operating constraint.

```text
apply_diff
  -> ModelsRepo writes model update
  -> model_events row is emitted
  -> pending_post_commit_actions gets materialize_projections
  -> post_commit_worker dispatches materialize_projections
  -> ProjectionRunner fetches pending model_events
  -> matching projector recomputes affected subject snapshots
  -> projection_snapshots upserted
  -> projection checkpoint advances
```

Retrieval can later use the compact projection snapshot first and load source
Models from `ProjectionRepo` when it needs evidence.

### 15.6 Relationship Candidate Lifecycle

Example: topology sees two Models that may have a causal relationship.

```text
topology or edge-intelligence worker
  -> computes candidate relationship with evidence
  -> writes relationship_candidates row
  -> enqueues T4 latent relationship trigger
  -> Think loads candidate into reasoning frame
  -> LLM/deterministic diff proposes edge/situation/type action
  -> validator checks mechanism, evidence, endpoints, region
  -> applier writes accepted model_edges or Models
  -> adjudication marks candidate accepted/rejected/needs-review
```

Discovery does not silently promote itself to truth.

## 16. How Components Relate

The main dependency relationships are:

- Gateway owns process wiring and request transport, then delegates down.
- Webhooks verify providers, then choose inline ingest or raw-tier publish.
- Ingest handlers produce drafts; domain repositories write durable rows.
- Observations enqueue Think; Think reads observations and memory.
- Retrieval reads observations, Models, edges, acts, resources, projections, and
  inquiry traces.
- Think produces diffs; validators and appliers own mutation authority.
- ModelsRepo emits model events; ProjectionRunner consumes them.
- Post-commit actions own side effects after the mutation commit.
- Realtime listens to committed observation/state-change notifications.
- Workers either drain queues, poll source APIs, refresh derived state, or
  propose new triggers.
- Extensions plug in through entry points and platform governance, never by core
  importing overlay code.

## 17. Practical Reading Order

For a code-level deep dive, read in this order:

1. `pyproject.toml` import-linter contracts.
2. `docker-compose.yml` and `services/platform/runtime/process_manifest.py`.
3. `services/app/gateway/main.py` and `services/app/gateway/route_mounts.py`.
4. `services/ingest/ingestion/core.py`.
5. `services/ingest/ingestion/raw_tier/envelope.py`.
6. `services/ingest/ingestion/normalizer/worker.py`.
7. `services/ingest/ingestion/writers/observation_writer.py`.
8. `services/domain/observations/repo.py` and `events.py`.
9. `services/reasoning/think/worker.py`.
10. `services/reasoning/think/reason.py` and `run_pipeline.py`.
11. `services/reasoning/think/validator.py` and `applier.py`.
12. `services/reasoning/think/post_commit.py`.
13. `services/domain/models/repo.py`, `edges_repo.py`, and `events.py`.
14. `services/domain/projections/runtime.py` and `repo.py`.
15. `services/platform/execution/inquiry.py` and `services/reasoning/retrieval/*`.
16. `services/workers/*` for specialized background loops.

## 18. Architecture Summary

Fyralis Core is best understood as a durable memory machine:

- ingress captures evidence.
- ingestion normalizes evidence into observations.
- observations trigger reasoning.
- retrieval builds context from memory.
- reasoning proposes structured diffs.
- validation constrains those diffs.
- appliers mutate canonical substrate.
- model events and post-commit actions fan out rebuildable consequences.
- workers keep the substrate fresh, connected, and inspectable.

The elegance of the system is in its hard boundaries: raw capture is separate
from normalization, normalization is separate from persistence, persistence is
separate from reasoning, reasoning is separate from validation, validation is
separate from mutation, and mutation is separate from post-commit side effects.
When those boundaries hold, the system can be retried, replayed, audited,
extended, and scaled without turning downstream behavior or source integrations
into hidden database writers.
