# Fyralis Core Implementation Plan

## Scope

This plan implements the end-to-end Fyralis Core architecture described in `END_TO_END_SYSTEM_ARCHITECTURE.md`, excluding the deeper dynamic relationship-type / intelligent edge discovery problem, which is intentionally deferred.

Included:

- FastAPI gateway and runtime wiring
- Integration ingress and webhook handling
- Kafka/S3 raw ingestion data plane
- Normalizer worker
- Observation writer
- PostgreSQL domain substrate
- pgvector embedding substrate
- Embedding retry workers
- Durable Think queues
- Retrieval and inquiry runtime
- LLM provider abstraction
- Think pipeline
- Validation and application layer
- Post-commit worker
- Product APIs
- Realtime dispatch
- Background workers
- Infrastructure, observability, testing, deployment, and operational hardening

Deferred:

- Advanced intelligent model-edge discovery
- Dynamic relationship ontology proposal and promotion
- Edge compiler / edge evidence adjudication system
- SAGE topology optimizer beyond minimal structural hooks

Basic model edge tables, relationship candidate storage, and graph traversal interfaces should still exist so the rest of the architecture can compile and run, but sophisticated edge intelligence should be feature-flagged off or implemented as a stub until the dedicated edge workstream begins.

---

## Implementation Strategy

Build Fyralis Core as a vertical runtime, not as a collection of beautiful disconnected modules. The right order is:

1. Database substrate and runtime wiring
2. Inline ingestion
3. Observation storage and embedding fallback
4. Durable Think queue
5. Minimal retrieval and Think pipeline
6. Post-commit side effects
7. Product reads
8. Kafka/S3 cutover path
9. Live workers and background workers
10. Observability, evaluation, and hardening

The first production-quality milestone should be:

```text
GitHub webhook
  -> gateway
  -> inline ingest
  -> observation insert
  -> Think trigger
  -> retrieval
  -> structured LLM diff
  -> model update
  -> post-commit realtime notification
  -> product API reads updated state
```

Then add Kafka/S3 ingestion as a durability/cutover layer around the same core ingest path.

---

## Guiding Principles

### 1. PostgreSQL is the system boundary

PostgreSQL is the durable source of truth for domain state, queues, product caches, audit events, costs, and replay records. Kafka transports ingestion events, but PostgreSQL owns semantic state.

### 2. Inline path first, Kafka path second

The inline ingest path must stay simple, reliable, and usable for local development, tests, fallback, and early dogfood. Kafka/S3 should wrap the same normalized domain insert path rather than becoming a second implementation of ingestion.

### 3. Every async operation must be replayable

All workers should be safe under crash, restart, duplicate delivery, and partial failure.

### 4. LLM outputs never mutate state directly

All LLM output must go through schema validation, region validation, idempotency checks, and domain invariant checks before application.

### 5. Product APIs compose persisted state

Product APIs are not the source of truth. They should read from domain state, caches, trace tables, and reasoning artifacts.

### 6. Feature flags protect unstable intelligence

Anything involving advanced reasoning, topology, forecasts, recommendations, and relationship discovery should be tenant-flagged and kill-switchable.

---

# Phase 0: Repository, Local Runtime, and Engineering Baseline

## Goal

Create the development foundation so every later subsystem has predictable conventions, testing hooks, migrations, configuration, and observability scaffolding.

## Deliverables

### 0.1 Repository structure

Expected top-level layout:

```text
services/
  app/
    gateway/
    webhooks/
    realtime/
  ingest/
    integrations/
    ingestion/
  reasoning/
  platform/
    execution/
  product/
  workers/
lib/
  embeddings/
  llm/
  config/
  observability/
  db/
db/
  migrations/
  seeds/
  fixtures/
scripts/
  run_gateway.py
  run_worker.py
  run_normalizer.py
  run_observation_writer.py
  run_embedding_worker.py
  run_post_commit_worker.py
  run_housekeeper.py
tests/
  unit/
  integration/
  e2e/
  fixtures/
```

### 0.2 Configuration system

Implement a typed settings layer.

Required config groups:

```text
DATABASE_URL
REDIS_URL
KAFKA_BOOTSTRAP_SERVERS
S3_ENDPOINT_URL
S3_ACCESS_KEY_ID
S3_SECRET_ACCESS_KEY
S3_BUCKET_RAW
EMBEDDING_PROVIDER
OLLAMA_BASE_URL
OPENAI_API_KEY
LLM_PROVIDER
LLM_MODEL
AUTH_MODE
DEFAULT_DOGFOOD_TENANT_ID
FEATURE_FLAGS
LOG_LEVEL
ENVIRONMENT
```

Implementation notes:

- Use a single settings object loaded by gateway and workers.
- Support `.env`, environment variables, and test overrides.
- Fail fast on missing required production config.
- Allow relaxed local defaults for single-box development.

### 0.3 Database migration framework

Deliver:

- Migration runner
- Migration checksum validation
- Local reset script
- Seed tenant script
- Fixture loader for e2e tests

Acceptance criteria:

- Fresh local database can be created from zero.
- Migrations are idempotent under CI.
- Test fixtures can create tenant, actors, source mappings, and example observations.

### 0.4 Observability baseline

Deliver:

- Structured JSON logging
- Request ID propagation
- Tenant ID and actor ID in logs where available
- Worker heartbeat logging
- `/healthz` and `/metrics` route scaffolding
- Prometheus metrics helpers

Core metrics:

```text
http_requests_total
http_request_duration_seconds
worker_loop_iterations_total
worker_job_attempts_total
worker_job_failures_total
db_query_duration_seconds
llm_requests_total
llm_request_duration_seconds
llm_tokens_total
embedding_requests_total
embedding_failures_total
```

---

# Phase 1: PostgreSQL Domain Substrate

## Goal

Implement the durable substrate for tenants, actors, observations, aliases, models, acts, resources, queues, telemetry, and product caches.

## 1.1 Tenant and source tables

Tables:

```sql
tenants
source_installations
ingestion_source_state
ingestion_flags
integration_oauth_tokens
```

Responsibilities:

- Represent tenants.
- Map third-party installations to tenants.
- Store per-source cursors and sync state.
- Control Kafka cutover, shadow mode, disabled sources, and provider-specific flags.

Acceptance criteria:

- Gateway can resolve a webhook installation to a tenant.
- Workers can read and update source cursors transactionally.
- Tenant-level feature flags are available to gateway, ingest, and reasoning.

## 1.2 Actors

Tables:

```sql
actors
actor_identity_mappings
actor_sessions
```

Repository:

```text
services/domain/actors.py
```

Required methods:

```python
resolve_source_actor(tenant_id, source, external_actor_ref) -> Actor
get_actor(actor_id) -> Actor
create_or_update_mapping(...)
create_session(...)
get_session(...)
```

Acceptance criteria:

- Same GitHub/Gmail/Slack identity consistently maps to the same actor.
- Unknown source identity can be represented without blocking ingest.
- Actor resolution is safe under concurrent ingestion.

## 1.3 Entity aliases

Tables:

```sql
entity_aliases
unresolved_entity_mentions
entity_resolution_jobs
```

Repository:

```text
services/domain/entity_aliases/repo.py
```

Responsibilities:

- Fast exact alias lookup during ingest.
- Store canonical and non-canonical aliases only through an authorized
  adjudication or promotion operation with durable lineage.
- Queue unresolved phrases for background resolution.
- Keep detection, candidate generation and canonical-registry mutation as
  separate authority stages. A resolver decision is not alias-write authority.

Acceptance criteria:

- Ingest can attach known entity refs to observations.
- Unknown phrases do not block ingest.
- Deferred resolution can match an unresolved phrase against already governed
  identity, abstain, or create a review obligation. It cannot invent a
  canonical entity or persist an accepted canonical alias.
- Every canonical alias mutation reconstructs to its adjudication/promotion
  trace; resolver-only writes fail closed.
- An authenticated source-identity binding may ground its exact mention, but
  that mention-scoped authority is non-transferable and cannot authorize a
  canonical alias-registry write.

## 1.4 Observations

Tables:

```sql
observations
observation_mentions
observation_raw_refs
observation_notifications
```

Partitioning:

- Monthly partition by `occurred_at`.
- Index by tenant, source, external ID, actor, occurred time, trust tier, kind.
- pgvector index for observation embeddings.

Suggested columns:

```sql
id uuid primary key
tenant_id uuid not null
source text not null
source_channel text
external_id text
kind text not null
occurred_at timestamptz not null
ingested_at timestamptz not null default now()
actor_id uuid
content_text text not null
raw_content_json jsonb
raw_object_key text
trust_tier text
cause_chain jsonb
mentioned_entity_refs jsonb
embedding vector(768)
embedding_pending boolean not null default false
content_hash text not null
dedup_key text not null
```

Acceptance criteria:

- Source/external ID deduplication works.
- Same observation cannot be inserted twice under retry.
- Observation insert can enqueue a Think trigger in the same transaction.
- Observation can exist without embedding.

## 1.5 Models

Tables:

```sql
models
model_status_notes
model_signal_readings
model_scope_sidecars
model_composition_members
model_edges
relationship_candidates
```

Keep model-edge intelligence minimal for now.

Required model columns:

```sql
id uuid primary key
tenant_id uuid not null
kind text not null
proposition text not null
summary text
confidence numeric
activation numeric
lifecycle_state text
scope_json jsonb
source_observation_ids uuid[]
embedding vector(768)
embedding_pending boolean not null default false
created_at timestamptz
updated_at timestamptz
valid_from timestamptz
valid_to timestamptz
```

Acceptance criteria:

- Think can create, update, deactivate, and supersede models.
- Product APIs can list and inspect models.
- Retrieval can search models by semantic vector and filters.
- Edge tables exist but advanced discovery is disabled behind a flag.

## 1.6 Acts

Tables:

```sql
goals
commitments
decisions
commitment_contributors
act_contributes_to
act_depends_on
act_constrained_by
```

Responsibilities:

- Track executable organizational state.
- Support owners, due dates, state transitions, contributors, and structured dependencies.

Acceptance criteria:

- Think can propose and update goals, commitments, and decisions.
- Product APIs can display acts and evidence.
- State transitions are validated.

## 1.7 Resources

Tables:

```sql
resources
resource_transactions
resource_deployments
customer_commitments
```

Responsibilities:

- Track assets, commitments, utilization, deployments, transactions, and customer/revenue overlays.

Acceptance criteria:

- Think can update resources from observations.
- Product surfaces can show resource context in recommendations and forecasts.
- Resource changes are auditable.

## 1.8 Durable queues and audit tables

Tables:

```sql
think_trigger_queue
model_reeval_queue
model_reeval_dead_letter
pending_post_commit_actions
applied_triggers
dedup_keys_seen
think_runs
think_run_costs
think_run_artifacts
audit_events
reconciliation_events
```

Queue requirements:

- Lease with `FOR UPDATE SKIP LOCKED`.
- Store attempt count, lease owner, lease expiry, status, priority, payload.
- Support backoff and dead-letter.
- Support tenant-scoped concurrency.

Acceptance criteria:

- Worker crash leaves leased jobs recoverable.
- Duplicate trigger application is prevented.
- Audit trail exists for every applied reasoning mutation.

---

# Phase 2: Gateway Layer

## Goal

Implement the FastAPI runtime boundary: request transport, auth, tenancy, route mounting, middleware, realtime dispatch, and extension seams.

## 2.1 Gateway app factory

File:

```text
services/app/gateway/app.py
```

Responsibilities:

- Create asyncpg pool.
- Wire repositories.
- Create embedding client when configured.
- Create rate limiter.
- Load integration runtime state.
- Initialize realtime dispatcher.
- Initialize CEO-view scheduler/cache wiring.
- Register gateway extension hooks.
- Mount route families.

Acceptance criteria:

- Gateway boots locally with only PostgreSQL.
- Optional dependencies degrade gracefully in dev.
- Startup validates required production dependencies.

## 2.2 Middleware

Files:

```text
services/app/gateway/middleware/request_context.py
services/app/gateway/middleware/auth.py
services/app/gateway/middleware/rate_limit.py
services/app/gateway/middleware/tenant_context.py
```

Required behavior:

- Generate/request request ID.
- Bind tenant and actor context.
- Authenticate bearer token.
- Bypass public health and webhook paths.
- Apply tenant/actor rate limits.
- Log request duration, status, route, tenant, actor.

Acceptance criteria:

- Authenticated routes reject missing/invalid token.
- Webhook and health paths work without bearer auth.
- Request context is available to downstream repositories and logs.

## 2.3 Route families

Mount routes:

```text
/healthz
/metrics
/auth/session
/ingest/*
/webhooks/*
/integrations/*
/substrate/observations
/substrate/models
/substrate/goals
/substrate/commitments
/substrate/decisions
/substrate/resources
/view/today
/view/recommendations
/view/forecasts
/view/history
/view/model
/view/model-trace
/view/map
/view/decision-deltas
/conversations
/ask
/render
```

Acceptance criteria:

- Route mounting is modular.
- Product routers can be tested independently.
- Overlay extensions can mount routes without core importing overlay code.

## 2.4 Realtime dispatcher

Files:

```text
services/app/realtime/dispatcher.py
services/app/realtime/streams.py
```

Responsibilities:

- Allow post-commit handlers to publish tenant-scoped events.
- Support WebSocket or SSE style stream surfaces.
- Store enough event metadata for clients to refetch affected surfaces.

Event envelope:

```json
{
  "tenant_id": "...",
  "event_type": "model.updated",
  "affected_resources": ["model:...", "view:today"],
  "occurred_at": "...",
  "payload": {}
}
```

Acceptance criteria:

- UI can subscribe to tenant-scoped updates.
- Realtime event is notification-only; PostgreSQL remains source of truth.
- Failed realtime dispatch does not roll back domain writes.

---

# Phase 3: Integration and Ingestion Layer

## Goal

Convert third-party provider events into normalized observations through both inline and Kafka/S3 paths.

## 3.1 Provider handler interface

File:

```text
services/ingest/integrations/base.py
```

Interface:

```python
class ProviderHandler(Protocol):
    source: str

    async def parse_webhook(self, payload: bytes, headers: Mapping[str, str]) -> ProviderEvent: ...
    async def to_observation_draft(self, event: ProviderEvent) -> ObservationDraft: ...
```

ObservationDraft shape:

```python
@dataclass
class ObservationDraft:
    tenant_id: UUID
    source: str
    source_channel: str | None
    external_id: str | None
    kind: str
    occurred_at: datetime
    actor_source_ref: dict | None
    content_text: str
    raw_content_json: dict
    raw_object_key: str | None
    trust_tier: str
    cause_chain: dict | None
    entity_hints: list[str]
    dedup_key: str
```

Acceptance criteria:

- GitHub handler implemented first.
- Handlers are pure enough to be tested from fixtures.
- Handler output is provider-neutral.

## 3.2 Webhook router

File:

```text
services/app/webhooks/router.py
```

Flow:

1. Capture raw body.
2. Enforce payload size limit.
3. Identify provider verifier.
4. Parse JSON best effort.
5. Resolve provider installation to tenant.
6. Load provider secrets.
7. Verify signature.
8. Route to Kafka/S3 or inline ingest.

Acceptance criteria:

- Invalid signatures are rejected.
- Unknown installations are logged and rejected safely.
- Kafka/S3 failure falls back to inline ingest when configured.
- Webhook returns durable acknowledgement only after raw event is safely accepted or inline ingest succeeds.

## 3.3 Inline ingest core

File:

```text
services/ingest/ingestion/core.py
```

Function:

```python
async def ingest_from_draft(draft: ObservationDraft, *, tx: Connection | None = None) -> IngestResult:
    ...
```

Responsibilities:

- Preassign observation UUID.
- Resolve actor.
- Resolve entity aliases.
- Compute embedding or mark pending.
- Insert observation idempotently.
- Enqueue T1 Think trigger.
- Emit post-commit observation notification record.

Acceptance criteria:

- Same draft under retry produces one observation and one effective Think trigger.
- Embedding failure does not fail ingest.
- Ingest can run inside an external transaction.

## 3.4 Raw tier

Components:

```text
S3/MinIO bucket: raw provider payloads
Kafka topics: ingestion.raw.<source>
```

Raw envelope:

```json
{
  "tenant_id": "...",
  "source": "github",
  "ingress_kind": "webhook",
  "raw_object_key": "tenant/source/date/id.json",
  "headers": {},
  "content_hash": "sha256:...",
  "received_at": "...",
  "provider_metadata": {}
}
```

Acceptance criteria:

- Raw object is written before Kafka message is published.
- Raw object key is deterministic enough for replay and debugging.
- Raw envelope contains no oversized payload body.

## 3.5 Normalizer worker

File:

```text
services/ingest/ingestion/normalizer_worker.py
```

Responsibilities:

- Consume `ingestion.raw.<source>` topics.
- Fetch raw payload from S3/MinIO.
- Dispatch to provider handler.
- Produce normalized envelope to `ingestion.normalized.<source>`.
- Send invalid/unparseable events to DLQ.

Invariant:

- Normalizer does not access PostgreSQL.

Acceptance criteria:

- Normalizer is pure raw-to-normalized.
- Failed parse sends DLQ record with reason and raw object key.
- Normalized envelope can be replayed independently.

## 3.6 Observation writer

File:

```text
services/ingest/ingestion/observation_writer.py
```

Responsibilities:

- Consume normalized envelopes.
- Read tenant ingestion flags.
- Preserve shadow/no-op behavior for disabled Kafka-path tenants.
- Reconstruct ObservationDraft.
- Call shared `ingest_from_draft`.
- Publish embedding retry requests when embedding pending.
- DLQ permanent failures.

Acceptance criteria:

- Kafka path and inline path converge on the same insert behavior.
- Transient PostgreSQL failures are retried through Kafka redelivery.
- Permanent data errors go to DLQ.

## 3.7 Live source workers

Implement after webhook path works.

Workers:

```text
GitHub intelligence worker
Gmail watch scheduler and history poller
Google Calendar live poller and watch scheduler
Google Drive live poller and watch scheduler
Discord gateway worker
Telegram worker
Signal gateway worker
```

Common worker contract:

```python
class LiveSourceWorker:
    async def load_cursor(...): ...
    async def poll_or_receive(...): ...
    async def write_raw_payload(...): ...
    async def publish_raw_envelope(...): ...
    async def advance_cursor(...): ...
```

Acceptance criteria:

- Cursor advances only after raw payload publication.
- Worker can restart without missing events.
- Source-specific rate limits are respected.

---

# Phase 4: Embedding Layer

## Goal

Provide reliable semantic embeddings over observations, models, code intelligence records, and entity aliases.

## 4.1 Embedding client abstraction

Files:

```text
lib/embeddings/base.py
lib/embeddings/ollama.py
lib/embeddings/openai.py
```

Interface:

```python
class Embedder(Protocol):
    dimensions: int
    model_name: str
    async def embed_text(self, text: str) -> list[float]: ...
    async def embed_batch(self, texts: list[str]) -> list[list[float]]: ...
```

Acceptance criteria:

- Ollama local embeddings work in dev.
- OpenAI provider can be enabled by config.
- Provider failures return typed errors.

## 4.2 Embedding pending behavior

Any insert/update that needs an embedding should:

- Try embedding inline when cheap and configured.
- Mark `embedding_pending = TRUE` on failure.
- Enqueue embedding retry request where possible.
- Never block observation/model persistence solely because embedding failed.

Acceptance criteria:

- Turning off Ollama still allows ingest and model writes.
- Backlog worker later fills missing embeddings.

## 4.3 Kafka embedding worker

Topic:

```text
embedding.retry
```

Message:

```json
{
  "tenant_id": "...",
  "table": "observations",
  "row_id": "...",
  "text_field": "content_text",
  "attempt": 1
}
```

Acceptance criteria:

- Worker fills embeddings for observations and models.
- Worker rate limits per provider.
- Poison rows are marked with failure metadata after max attempts.

## 4.4 Database backlog drainer

File:

```text
services/ingest/ingestion/recovery/embedding_backlog.py
```

Responsibilities:

- Periodically scan rows with `embedding_pending = TRUE`.
- Lock batches with `FOR UPDATE SKIP LOCKED`.
- Fill embeddings.
- Respect Redis/provider rate limits.

Acceptance criteria:

- Can recover from lost Kafka retry messages.
- Runs safely in multiple worker replicas.

---

# Phase 5: Reasoning Layer

## Goal

Implement durable queue-backed reasoning that converts observations and existing state into validated domain updates.

## 5.1 Think trigger queue

Queue row fields:

```sql
id uuid primary key
tenant_id uuid not null
trigger_type text not null
trigger_key text not null
payload jsonb not null
priority int not null default 100
status text not null
available_at timestamptz not null default now()
leased_by text
leased_until timestamptz
attempt_count int not null default 0
last_error text
created_at timestamptz
updated_at timestamptz
```

Trigger types:

```text
T1 event_arrival
T2 prediction_or_belief_update
T3 anomaly_trigger
T4 maintenance_or_background
T6 legacy_topology_compatibility
```

Acceptance criteria:

- Queue supports concurrent workers.
- Retry and dead-letter policies are deterministic.
- Duplicate trigger keys are idempotent.

## 5.2 Think worker

File:

```text
services/reasoning/think_worker.py
```

Responsibilities:

- Poll queue.
- Lease rows with `FOR UPDATE SKIP LOCKED`.
- Enforce per-tenant concurrency.
- Hydrate trigger context.
- Call Think pipeline.
- Mark complete or failed.
- Move exhausted jobs to dead-letter.

Acceptance criteria:

- Worker can run many replicas safely.
- Tenant hot spots do not starve other tenants.
- Failed jobs retry with backoff.

## 5.3 Retrieval and inquiry runtime

Files:

```text
services/platform/execution/inquiry.py
services/reasoning/retrieval/assembler.py
services/reasoning/retrieval/pathways.py
services/reasoning/retrieval/context_packet.py
```

Retrieval pathways:

- Structural overlap by entities, actors, goals, commitments, resources, and scope.
- Semantic vector similarity.
- Temporal recency.
- Pattern/background retrieval.
- Minimal memory graph traversal over existing edges.

Context packet output:

```json
{
  "trigger": {},
  "observations": [],
  "models": [],
  "acts": [],
  "resources": [],
  "bridge_data": [],
  "selected_graph_anchors": [],
  "omissions": [],
  "debug": {}
}
```

Acceptance criteria:

- Retrieval is bounded by token budget.
- Access-control filters are always applied before prompt construction.
- Omission/debug notes explain what was left out.
- Context packet is persisted as a Think artifact.

## 5.4 LLM provider layer

Files:

```text
lib/llm/provider.py
lib/llm/openai_provider.py
lib/llm/anthropic_provider.py
lib/llm/deepseek_provider.py
lib/llm/codex_provider.py
```

Responsibilities:

- Structured-output calls.
- Provider-specific retry policy.
- Cost and token tracking.
- Model-name attribution.
- Pydantic output validation.

Interface:

```python
class LLMProvider(Protocol):
    async def structured_call(
        self,
        *,
        messages: list[dict],
        output_schema: type[BaseModel],
        model: str | None = None,
        temperature: float = 0,
        metadata: dict | None = None,
    ) -> StructuredCallResult: ...
```

Acceptance criteria:

- Provider errors are typed.
- Raw response and parsed response can be saved as artifacts.
- Token/cost records are written for every call.

## 5.5 Think output schema

Initial schema should cover:

```text
model_creates
model_updates
model_deactivations
act_creates
act_updates
resource_creates
resource_updates
prediction_creates
recommendation_candidates
edge_hints only, not advanced edge activation
post_commit_actions
```

Do not let Think directly invent durable complex edge semantics for now. Store edge hints or simple relationships only when they pass minimal validation.

Acceptance criteria:

- Invalid LLM shape fails validation.
- Out-of-region references trigger expanded retrieval retry or safe failure.
- Think output can be replayed from artifacts.

## 5.6 Validation and application

Files:

```text
services/reasoning/validation.py
services/reasoning/applier.py
services/reasoning/locks.py
```

Validation responsibilities:

- Schema correctness.
- Allowed reasoning region.
- Domain invariants.
- Idempotency.
- Entity/actor/model existence.
- State transition validity.

Application responsibilities:

- Acquire region locks.
- Apply domain diffs in one transaction.
- Record `applied_triggers`.
- Write audit events.
- Write Think run artifacts and costs.
- Enqueue post-commit actions.

Acceptance criteria:

- LLM cannot mutate state outside region.
- Same trigger cannot apply twice.
- Crash before commit leaves no partial mutation.
- Crash after commit leaves post-commit action durable.

## 5.7 Think run telemetry

Tables:

```sql
think_runs
think_run_costs
think_run_artifacts
```

Artifacts to save:

- Trigger context.
- Retrieval query plan.
- Context packet.
- Prompt messages.
- Raw LLM output.
- Parsed diff.
- Validation result.
- Applied diff summary.

Acceptance criteria:

- Any Think run can be debugged from persisted artifacts.
- Cost attribution is queryable by tenant, provider, model, and time range.

---

# Phase 6: Post-Commit Worker

## Goal

Process side effects durably after domain transactions commit.

## 6.1 Post-commit action table

Fields:

```sql
id uuid primary key
tenant_id uuid not null
action_kind text not null
payload jsonb not null
status text not null
available_at timestamptz not null
leased_by text
leased_until timestamptz
attempt_count int not null default 0
idempotency_key text not null
last_error text
created_at timestamptz
processed_at timestamptz
```

Action kinds:

```text
publish_anomaly
schedule_prediction
broadcast_realtime_update
invalidate_metrics
discover_model_edges_stub
refresh_product_cache
```

The `discover_model_edges_stub` action should exist but do minimal/no advanced work until the edge workstream resumes.

Acceptance criteria:

- Actions are at-least-once and idempotent.
- Handler failure retries with backoff.
- Duplicate idempotency key does not duplicate side effects.

## 6.2 Handler registry

File:

```text
services/workers/post_commit/registry.py
```

Interface:

```python
class PostCommitHandler(Protocol):
    action_kind: str
    async def handle(self, action: PostCommitAction) -> None: ...
```

Acceptance criteria:

- Adding a new action kind does not modify worker loop logic.
- Unknown action kinds are safely failed or dead-lettered.

## 6.3 Realtime broadcast handler

Responsibilities:

- Publish tenant-scoped event through realtime dispatcher.
- Include affected resource references.
- Avoid embedding full domain state in event payload.

Acceptance criteria:

- Product UI can refetch affected surfaces after notification.
- Failed realtime dispatch does not corrupt action queue.

## 6.4 Metrics invalidation handler

Responsibilities:

- Invalidate product cache rows.
- Mark derived metrics stale.
- Optionally enqueue cache refresh.

Acceptance criteria:

- Product read never serves known-stale critical cache unless explicitly allowed.
- Cache invalidation is idempotent.

---

# Phase 7: Product Layer

## Goal

Expose domain and reasoning state through product APIs without making product views the source of truth.

## 7.1 Shared product service conventions

Files:

```text
services/product/common.py
services/product/evidence.py
services/product/cards.py
```

Conventions:

- Product services read domain repositories.
- Responses include evidence references where useful.
- Expensive views may read from cache but must expose cache status.
- Product actions emit events or write explicit domain action records.

Acceptance criteria:

- Each product route has service-layer tests.
- Responses are stable Pydantic models.
- Product writes are auditable.

## 7.2 CEO Home / Greeting

Components:

```text
greeting snapshot builder
view_ceo_cache repository
scheduler
stream manager
viewer state repository
```

Tables:

```sql
view_ceo_cache
viewer_state
view_refresh_jobs
```

Responsibilities:

- Build cached CEO home payloads.
- Store greeting, query grid, cards, status, close-line content.
- Track viewer last-seen timestamps.
- Support manual refresh and realtime updates.

Acceptance criteria:

- `GET /view/ceo/home` returns cached payload quickly.
- Manual refresh can enqueue or perform snapshot rebuild.
- Viewer state updates do not block response path unnecessarily.

## 7.3 Today and Decision Deltas

Routes:

```text
GET /view/today
GET /view/decision-deltas
POST /view/decision-deltas/{id}/accept
POST /view/decision-deltas/{id}/delegate
POST /view/decision-deltas/{id}/contest
POST /view/decision-deltas/{id}/snooze
POST /view/decision-deltas/{id}/context
```

Responsibilities:

- Show changes requiring review.
- Present evidence.
- Promote accepted deltas into domain state.
- Record contest/delegate/snooze actions.

Acceptance criteria:

- User actions are idempotent.
- Accepted deltas write domain changes through validated application path.
- Contested deltas feed audit/reconciliation records.

## 7.4 Recommendations

Routes:

```text
GET /view/recommendations
POST /view/recommendations/{id}/accept
POST /view/recommendations/{id}/dismiss
POST /view/recommendations/{id}/watch
POST /view/recommendations/{id}/triage
```

Tables:

```sql
recommendations
recommendation_events
recommendation_watches
```

Responsibilities:

- Present model-backed recommendations.
- Track actions, dismissals, triage, and watches.
- Emit recommendation events for downstream surfaces.

Acceptance criteria:

- Recommendation lifecycle is auditable.
- Dismissed recommendations stop appearing unless relevant state changes.
- Watched recommendations can trigger later notifications.

## 7.5 Ask / Query

Routes:

```text
POST /ask
GET /conversations/{id}
POST /conversations/{id}/turns/{turn_id}/followup
POST /conversations/{id}/turns/{turn_id}/save
POST /conversations/{id}/turns/{turn_id}/done
```

Responsibilities:

- Answer user questions using retrieval and synthesis.
- Support card-scoped context.
- Store conversation turns.
- Reuse platform execution and SAGE reader paths where available.

Acceptance criteria:

- Ask uses same access-control filters as Think retrieval.
- Answer includes evidence references.
- Saved answers are traceable to context packet and model state.

## 7.6 Forecasts

Routes:

```text
GET /view/forecasts
GET /view/forecasts/{id}
POST /view/forecasts/{id}/scenario
```

Tables:

```sql
forecasts
forecast_signals
forecast_outcomes
forecast_calibration_stats
```

Responsibilities:

- Display predictions.
- Track prediction signals.
- Show patterns, accuracy, details, and scenario answers.
- Feed calibration/hit-rate surfaces.

Acceptance criteria:

- Forecasts can be evaluated against outcomes.
- Calibration updater can update stats.
- Scenario answers cite the model/observation context used.

## 7.7 Model, Map, Trace, and History

Routes:

```text
GET /view/model
GET /view/model/{id}
GET /view/model/{id}/trace
GET /view/map
GET /view/history
```

Responsibilities:

- Show model overview and item detail.
- Explain evidence, lifecycle, supports/dependencies where available.
- Expose topology/map events with current limitations.
- Provide ledger-style system history.

Acceptance criteria:

- Model detail page can show proposition, confidence, evidence, status, lifecycle.
- Trace page can show observation-to-model-to-product flow.
- History view is backed by audit events and Think artifacts.

## 7.8 Rendering

Routes:

```text
POST /render
```

Responsibilities:

- Generate user-facing prose from structured product data.
- Enforce voice/style checks.
- Record render cost and latency.

Acceptance criteria:

- Rendering never becomes the source of truth.
- Raw structured payload remains available.
- Render cost is persisted.

---

# Phase 8: Background Worker Families

## Goal

Add maintenance, enrichment, calibration, anomaly, deadline, and source intelligence workers without destabilizing the core path.

## 8.1 Worker framework

Files:

```text
services/workers/base.py
services/workers/runner.py
```

Common capabilities:

- Heartbeat.
- Graceful shutdown.
- Lease acquisition.
- Backoff.
- Metrics.
- Structured logs.
- Tenant-scoped controls.

Acceptance criteria:

- All workers share lifecycle behavior.
- Workers expose health and metrics.
- Workers can run as independent processes.

## 8.2 Entity resolver

Responsibilities:

- Consume committed mention detections and resolve them against existing
  governed canonical identity.
- Use deterministic matching first.
- Use LLM-assisted resolution behind a flag.
- Emit a resolution, abstention or review/proposal outcome with exact evidence
  lineage. Accepted canonical aliases are written only by the separate
  adjudication/promotion authority.
- Support an optional tenant-bound poll for simulations and scoped worker runs;
  the omitted-tenant mode remains the production global poll.

Acceptance criteria:

- Resolver never blocks ingest.
- Human/tenant review can be required for uncertain aliases.
- Alias changes are auditable.
- A tenant-bound poll cannot select another tenant's unresolved observations.
- Resolver execution alone cannot create canonical identity or mutate the
  accepted canonical-alias registry.

### Learned discovery evidence discipline

Learned mention discovery operates once per persisted signal batch. Its prompt
requires a complete left-to-right pass per focal signal, exact source slices,
the smallest complete written designation, and company-object types chosen from
the referent's stated role. Context from other signals may disambiguate a
literal surface but may never introduce absent text. Transport coordinates,
generic roles and schema/code syntax are explicit negatives. These are
extraction contracts, not canonical-link authority.

Implementation and evaluation keep four learned-provider evidence populations
distinct:

1. The historical v1 `gpt-5.4` run produced fresh exact-span P/R/F1
   `0.8163/0.6452/0.7207`. The later `0.8361` F1 is a post-hoc rescore of those
   same saved outputs, not a second independent run.
2. Sealed v2 contains 80 signals in eight ten-signal batches, 114 gold spans
   and 40 hard negatives. Exact recovery of the already completed structured
   turns produced P/R/F1 `0.8020/0.7105/0.7535`, type accuracy `0.8163` and
   negative cleanliness `0.95`. One schema-invalid item rejected its whole
   batch. This is recovered sealed-run evidence, not a provider rerun, and all
   canonical referents are null.
3. The mutable development corpus and checkpointed one-call-per-ten-signal
   runner are prompt-development feedback only. The recorded pre-prompt-revision
   run reached post-verification exact-span P/R/F1
   `0.7727/0.7969/0.7846`, type accuracy `0.8906` and negative cleanliness
   `1.0`. Because its examples and results were inspected before the current
   boundary/type prompt was frozen, it is not generalization evidence for that
   prompt.
4. Sealed v3 is the untouched organization/entity/time/text-disjoint holdout:
   40 signals in four ten-signal batches, 70 gold mentions and 20 negatives,
   corpus digest
   `e6d5821399403feeac727253f791a8bb0d98d1c42232376c3b30305f00a43bc4`.
   Its single authorized execution completed at `0d9d8e65`: exactly four
   `gpt-5.4` calls and no provider errors. Raw model coordinates scored exact
   P/R/F1 `0.768116/0.757143/0.762590`; production verification uniquely
   source-repaired 13 coordinate errors. The complete extraction path then
   produced 70/70 overlap matches, 66/70 exact, post-verification exact-span
   P/R/F1 `0.942857/0.942857/0.942857`, type accuracy `0.985714` and negative
   cleanliness `1.0`. Source F1 was Slack
   `0.9545`, email `0.9286` and Jira `0.95`. Workstream boundary F1 remained
   weak at `0.5` despite type accuracy `1.0`; four boundary errors remain in
   total. One exact, high-confidence (`0.92`) mention was typed `resource`
   instead of gold `goal`. The immutable report SHA-256 is
   `4427b73f90b2baafb52efe1f44e615cac586c2956e61423fad2c8936d2263eca`.
   This is strong one-shot **complete-pipeline extraction** generalization
   evidence for the frozen prompt/policy on this population, not direct model
   offset quality. All canonical referents are null, so it proves neither
   canonical linking nor company-scale learning behavior. Boundary and type
   uncertainty work continues on development data without changing or
   selectively rerunning v3.
5. Audited broad v4 is the current disjoint broad-extraction population: 40
   signals in four genuine ten-signal batches, 69 gold spans, 67 predictions
   and 66 exact matches. Exact P/R/F1 is
   `0.985075/0.956522/0.970588`, type accuracy and 20/20 negative cleanliness
   are `1.0`, workstream is `6/6`, and Slack/email/Jira F1 is
   `0.976744/0.962963/0.974359`. The immutable pre-fix report SHA-256 is
   `f67eb4f39ae72ca699d675299186bc2c5050fb81ece91077776724a74b64a245`.
   Its receipt lacks a pre-call runtime-source digest, which remains an explicit
   protocol gap. Fixes derived from its misses have unit proof only; the saved
   output must not be replayed or rescored as fresh post-fix generalization.
   This population governs current bounded extraction quality, while v3 remains
historical evidence and the separate DB vertical governs canonical linking.

Objective entity v5 (`ad47a1c0`) now SHA-binds broad v4 with the historical
and downstream populations. `/tmp/objective_entity_evidence_v5.json` has file
SHA-256 `83d6a51f9d61d3dd3c219b8882174c89e7bc85e2fb5ef622365cc40ef24b2128`
and composition SHA-256
`46bd421e273519c311a5deb44dd81b2fc40ddcd800b1396c76ad1bea372593af`.
Its broad component scores `0.990196` and readiness is clear, while the missing
pre-call runtime-source digest and lack of post-holdout current-runtime
generalization remain explicit proof gaps.

Wrong-type consequential admission is a noncompensatory failure class: an exact
span with high confidence must not silently create or update a Model, relation,
authority decision or learned outcome under the wrong company-object type.
Type uncertainty must be retained through candidate generation and resolve to
abstention/review when the consequence-specific admission threshold is not met.

The checkpointed development runner validates the gold/evaluator contract
before provider construction, pins one `gpt-5.4` structured call per genuine
ten-signal batch, and atomically records raw output, usage, exact errors and
pre/post-verification metrics after every batch. Its artifacts must carry
`development_only=true` and `generalization_claim_permitted=false`.

The stage-level entity evaluator also accepts evaluator-owned relation
expectations (`a8487036`). It joins active downstream relations through exact
mention lineage and separately measures admission, endpoint identity, edge
type, direction, lineage coverage/integrity, unexpected edges, harmful topology
propagation, unknown endpoints and active relations without mention lineage.
No-edge expectations test whether rejected, unresolved or unsafe grounding
silently contaminates the company graph. Shared endpoint adjacency is never
treated as causal origin; durable source-mention metadata is required. These
metrics expose how entity errors propagate into topology, but become company
quality evidence only when a populated gold relation suite is executed.

That populated proof now exists for one bounded, production-shaped positive
vertical and one separate adversarial extension.
The sealed company-physics vertical (`eaa02f3f`) starts from seven normalized,
persisted signals processed as one genuine batch. Five of five eligible cases
place the correct canonical target in the candidate set and resolve correctly;
candidate recall at 1/3/5, canonical-link coverage and accuracy, and safe
decision rate are `1.0`, while harmful false links and resolver-owned canonical
alias writes are zero. The same trace materializes two belief Models, preserves
two explicit no-admission outcomes, and admits one directed `blocks` relation
with exact source-mention lineage while preserving the declared no-edge case.
Semantic-disposition, Model-materialization, no-admission-safety,
relation-admission, direction, type and lineage rates are all `1.0` for their
exact labeled denominators; cross-tenant, untraceable and known-wrong-type
consequential incidents are zero.

This vertical is deliberately not merged into v3. V3 remains immutable
extraction evidence with null referents; the DB-backed vertical is separate
linking and downstream-physics evidence over a small authored population. Its
perfect rates prove the production bridge and evaluator semantics for those
cases, not open-world accuracy, scale, drift resistance or company-wide graph
quality.

The adversarial v2 vertical (`8234ee74`, repaired and made reproducibly
composable through `8e21e444` and `be021303`) reuses that exact positive
substrate without weakening it. One DB execution safely rejects all four
attempted harmful graph mutations without a write: two critical
wrong-direction/wrong-type attempts and two high-consequence self-link/cycle
attempts. It also creates a two-hop support chain with exact mention lineage,
retires the first hop after correction, enqueues the immediate dependent for
reevaluation and explicitly leaves the second hop active pending that work.
The artifact therefore proves bounded correction propagation, not completed
transitive repair. The v2 objective entity composer SHA-binds this artifact to
the exact positive vertical and exposes adversarial topology, correction,
consequence-tier and open-world scores plus noncompensatory unsafe-write and
repair-failure blockers. The bounded adversarial components have a combined
top-level weight no larger than one ordinary entity component.

Type confidence is independent of mention-detection confidence (`200d7e48`).
An ambiguous code-like identifier may remain a valid detected mention while
its type confidence is capped below resolver narrowing when no local role cue
supports the type. Batch fate closure is independently proven (`6967e605`):
schema failure, timeout and malformed-sibling cases leave all ten signals with
terminal fates, zero learned-quality credit and idempotent replay.

Historical objective composition v2 (`9bcfde1e`) digest-bound immutable v3,
the positive vertical and adversarial v2. Its `0.9901315789` readiness and
workstream exact-F1 `0.5` tail remain useful falsifying history; they are not
the governing entity-readiness claim. The current entity composition also
requires the audited, disjoint broad-v4 extraction population described below.
The downstream DB vertical remains a separate proof of linking and graph
admission: extraction quality must not be relabelled as canonical-link quality,
and neither population establishes open-world or company-scale graph quality.

### Company-model learning and feedback evidence

Mature retrieval now has a bounded, preregistered nine-batch proof. Early
retrieval is observation-heavy (`1.0` observation share); late retrieval
selects Models for `8/11` eligible items (`0.7272727`), actually references
Models at `0.8`, and records a reason for every late raw-evidence reopening.
This meets the bounded policy but does not rewrite the immutable 45-batch
flat/mixed result.

The first real matched company-model ablation then falsified the broader
learning claim. V2 (`/tmp/bounded_company_model_ablation_v2.json`, SHA-256
`abe417b302f8193ead3d9c10dcbefc45050c766b66e5e447a27244a8950e038e`)
ran three genuine six-signal batches in learned and frozen arms. Both recovered
`0/3` hidden theses, lift was `0`, learned ECE was `0.5725`, continuous score
was `0.7`, and no prior Models were selected. The SAGE/Think seam fix
`a2467b0f` preserves material SAGE Models in composed Think context. Postfix v3
(`/tmp/bounded_company_model_ablation_postfix_v3.json`, SHA-256
`322f7c2a4bf00414aa80e586e64cfbfea1914aef7074934cea83f8b82d3b60e7`)
proves the seam changed selection—three prior Models in learned batch two and
six in batch three, versus zero frozen—but referenced Models remained zero and
the same `0/3`, zero-lift, `0.5725` ECE, `0.7 below_policy` result remained.
This localized the remaining seam to actual context use.

The explicit v4 development experiment (`ce6ea870`,
`/tmp/bounded_company_model_ablation_v4_development.json`, SHA-256
`b76ed8cac461c6fdd8c5f8a30635f0bddec3acba20e1e2ba6a4d005f6e43fe99`)
uses the same signals, hidden truth and policy with a generic Model-summary
consumer that never receives hidden truth. It meets bounded policy at score
`1.0`: learned recovers `3/3` versus frozen `0/3`, lift `1.0`, ECE
`0.1925` versus `0.5725`, and Brier `0.037056` versus `0.327756`.
Learned selected/referenced Models are exactly `0/0`, `3/3`, `6/6` across the
three batches; frozen remains zero. V2/v3 remain falsifying discovery evidence;
v4 proves the corrected mechanism on development data, not untouched
generalization or customer value.

Two additional bounded proofs close narrower portability and repair questions.
Normalized Slack, email, Jira and document/meeting inputs produce equivalent
entity/Model/relation outcomes across two semantic cases and eight genuine
source batches with continuous score `1.0`, while retaining source authority,
coordinates and conversational boundaries. The real-Postgres correction
homeostasis vertical executes two corrections, fences eight Models, records
eight reevaluation pairs, rejects two cycle writes, survives restart exactly,
adds no replay work and scores `1.0` on every registered check. Neither result
establishes open-world source drift or unbounded recovery.

The governing company-learning composition is v8, not the historical
five-component v3 portfolio. It fail-closes eight independent SHA-bound
components: entity/company physics, mature retrieval, adaptive/frozen learning,
normalized source equivalence, correction homeostasis, the joined runtime,
matched feedback quality and strict single-Model synthesis. The current bound
artifact is
`/private/tmp/objective_company_learning_evidence_v8_boundaries.json` (file
SHA-256 `9816c876…25706`, composition SHA-256 `a6b9c9a5…67e125`). All eight
components are observed at score and coverage `1.0`, with no below-policy
component or noncompensatory blocker. Sixteen successful scope limitations are
reported as `proof_boundaries`, not as failures.

This is a bounded simulated-system pass, not release or customer proof. The
joined runtime proves one exact persisted-signal-to-correction vertical; matched
feedback quality proves adaptive later quality `1.0` versus frozen `0.0`; and
the frozen strict-synthesis holdout proves learned `3/3` versus frozen `0/3`
with exactly one complete prior-Model-lineaged Model per thesis. These results
do not erase the immutable 45-batch verdict, establish open-world behavior or
prove customer value.

## 8.3 Anomaly processor

Responsibilities:

- Consume anomaly staging records.
- Deduplicate anomalies.
- Emit T3 anomaly-triggered Think jobs.
- Publish anomaly post-commit events.

Acceptance criteria:

- Repeated anomaly signals do not spam Think queue.
- Anomaly-to-Think trace is visible in audit history.

## 8.4 Calibration updater

Responsibilities:

- Update prediction and model calibration stats.
- Compare forecasts to outcomes.
- Track confidence accuracy over time.

Acceptance criteria:

- Forecast calibration can be computed by tenant/time/model kind.
- Calibration stats are queryable by product APIs.

## 8.5 Deadline resolver

Responsibilities:

- Resolve overdue commitments.
- Enqueue reevaluation for time-sensitive acts.
- Update stale/expired state where deterministic.

Acceptance criteria:

- Time-based state changes occur without new external events.
- Deadline actions are idempotent.

## 8.6 Topology sweeper stub

Responsibilities for now:

- Refresh simple relationship candidates.
- Compute basic graph statistics.
- Avoid activating advanced model edges.

Acceptance criteria:

- Worker can run without changing core graph semantics.
- Output is inspectable and safe to ignore.

## 8.7 SAGE structural feature worker stub

Responsibilities for now:

- Compute structural graph features needed later.
- Store feature snapshots.
- Do not depend on dynamic edge ontology work.

Acceptance criteria:

- Feature tables are populated for future topology/retrieval work.
- No product correctness depends on these features yet.

## 8.8 Housekeeper

Responsibilities:

- Expire stale leases.
- Requeue timed-out jobs.
- Clean old ephemeral artifacts under retention policy.
- Maintain monthly observation partitions.
- Reconcile ingestion flags and stuck states.

Acceptance criteria:

- Queue recovery works after worker crash.
- Retention policy is configurable.
- Partition maintenance is automated.

## 8.9 GitHub intel worker

Responsibilities:

- Track ordered GitHub state.
- Enrich PR/issue/commit observations.
- Maintain source cursor.
- Emit raw envelopes to Kafka/S3 path.

Acceptance criteria:

- PR lifecycle is reconstructed in order.
- Cursor restart does not miss or duplicate events semantically.
- GitHub-derived observations are useful to Think retrieval.

---

# Phase 9: Infrastructure

## Goal

Run the system locally and in production with clear service boundaries.

## 9.1 PostgreSQL + pgvector

Deliver:

- Docker Compose service.
- Production migration path.
- pgvector extension setup.
- HNSW indexes.
- Partition maintenance scripts.

Acceptance criteria:

- Fresh local compose boot creates all required extensions.
- Vector search works for observations and models.
- Partition creation is automated.

## 9.2 Kafka

Topics:

```text
ingestion.raw.github
ingestion.raw.gmail
ingestion.raw.google_calendar
ingestion.raw.google_drive
ingestion.raw.slack
ingestion.normalized.github
ingestion.normalized.gmail
ingestion.normalized.google_calendar
ingestion.normalized.google_drive
ingestion.normalized.slack
ingestion.dlq
embedding.retry
```

Acceptance criteria:

- Topic creation is automated.
- DLQ records contain enough context for replay.
- Consumers use stable consumer groups.

## 9.3 S3 / MinIO

Deliver:

- Raw payload bucket.
- Key naming conventions.
- Local MinIO setup.
- Object retention policy.

Key format:

```text
raw/{tenant_id}/{source}/{yyyy}/{mm}/{dd}/{content_hash}.json
```

Acceptance criteria:

- Raw payload can be fetched from envelope alone.
- Replay tools can scan by tenant/source/date.

## 9.4 Redis

Uses:

- Rate limiting.
- Singleton leases for selected live workers.
- Embedding backlog rate limiting.

Acceptance criteria:

- Redis outage degrades non-critical features safely where possible.
- Critical lease behavior has timeouts.

## 9.5 Ollama

Deliver:

- Local compose setup.
- Startup model pull for `nomic-embed-text`.
- Health check.
- Embedding provider config.

Acceptance criteria:

- Dev environment can compute embeddings without external API keys.
- Ollama downtime does not block ingest.

---

# Phase 10: Security, Tenancy, and Access Control

## Goal

Make tenant isolation and source trust explicit everywhere.

## 10.1 Tenant scoping

Rules:

- Every domain table has `tenant_id` unless truly global.
- Every repository query filters by tenant.
- Every worker job carries tenant ID.
- Every product route resolves tenant from auth/session context.

Acceptance criteria:

- Cross-tenant data leak tests exist for repositories and product APIs.
- Missing tenant context fails closed.

## 10.2 Auth and sessions

Deliver:

- Bearer-token auth.
- Actor session repository.
- Dogfood default tenant mode only in configured environments.
- Public path allowlist.

Acceptance criteria:

- Production cannot accidentally run in dogfood default mode.
- Session actor is bound to request context.

## 10.3 Webhook security

Deliver:

- Provider-specific signature verifiers.
- Secret storage and rotation path.
- Payload size limits.
- Replay protection where provider supports it.

Acceptance criteria:

- Invalid signatures are rejected.
- Oversized payloads are rejected.
- Verification failures are logged without leaking secrets.

## 10.4 Data retention and audit

Deliver:

- Retention settings for raw payloads, artifacts, logs, and product caches.
- Audit events for writes and user actions.
- Reconciliation events for repair operations.

Acceptance criteria:

- Important mutations can be reconstructed.
- Retention jobs do not delete source-of-truth state accidentally.

---

# Phase 11: Testing Strategy

## Goal

Prevent the usual distributed-system circus: duplicate events, partial writes, invisible failures, and tests that only pass because they test nothing.

## 11.1 Unit tests

Coverage areas:

- Provider parsers.
- ObservationDraft generation.
- Actor resolution.
- Entity alias resolution.
- Dedup key generation.
- Embedding fallback behavior.
- Queue leasing logic.
- Validation rules.
- Product serializers.

Acceptance criteria:

- Every provider handler has fixture-based tests.
- Every domain repository has transaction tests.

## 11.2 Integration tests

Coverage areas:

- Gateway startup.
- Webhook route with valid/invalid signature.
- Inline ingest to observation insert.
- Observation insert to Think trigger.
- Think worker leasing.
- Post-commit worker retry.
- Product route reading updated state.

Acceptance criteria:

- Tests run against real PostgreSQL with migrations.
- Kafka/S3 tests run under compose or testcontainers.

## 11.3 E2E tests

Primary e2e scenario:

```text
GitHub PR webhook fixture
  -> gateway webhook route
  -> inline ingest
  -> observation stored
  -> Think trigger enqueued
  -> Think worker runs with mocked LLM
  -> model updated
  -> post-commit realtime event queued/published
  -> product model page reflects update
```

Kafka e2e scenario:

```text
GitHub PR webhook fixture
  -> raw object in MinIO
  -> raw Kafka envelope
  -> normalizer
  -> normalized Kafka envelope
  -> observation writer
  -> observation stored
  -> Think trigger enqueued
```

Acceptance criteria:

- E2E tests assert durable state at every boundary.
- LLM calls are mocked deterministically.
- Replay test proves duplicate event does not duplicate semantic state.

## 11.4 Failure injection tests

Test failures:

- Kafka publish fails, inline fallback succeeds.
- Ollama is down, observation still inserts.
- Worker crashes after leasing job.
- Worker crashes after domain commit before post-commit side effect.
- LLM returns invalid schema.
- LLM references out-of-region state.
- Product cache is stale or missing.

Acceptance criteria:

- System either recovers or fails explicitly.
- No silent data corruption.

---

# Phase 12: Observability and Operations

## Goal

Make the system inspectable enough that debugging does not require ritual sacrifice.

## 12.1 Metrics

Gateway:

```text
http_requests_total
http_request_duration_seconds
http_5xx_total
rate_limit_blocks_total
webhook_signature_failures_total
webhook_payload_too_large_total
```

Ingestion:

```text
ingest_observations_inserted_total
ingest_duplicates_total
ingest_failures_total
kafka_raw_published_total
kafka_normalized_published_total
dlq_messages_total
```

Reasoning:

```text
think_jobs_started_total
think_jobs_completed_total
think_jobs_failed_total
think_jobs_deadlettered_total
think_duration_seconds
retrieval_context_items_total
llm_calls_total
llm_tokens_total
llm_cost_total
validation_failures_total
```

Workers:

```text
worker_lease_acquired_total
worker_lease_expired_total
worker_retries_total
worker_deadletters_total
```

Product:

```text
product_requests_total
product_cache_hits_total
product_cache_misses_total
render_calls_total
render_cost_total
```

## 12.2 Dashboards

Dashboards:

- Gateway health.
- Ingestion throughput and DLQs.
- Think queue depth and latency.
- LLM cost and errors.
- Embedding backlog.
- Post-commit backlog.
- Product latency and cache hit rate.
- Tenant-level health.

## 12.3 Runbooks

Runbooks:

- Replaying raw ingestion from S3.
- Reprocessing normalized events.
- Draining embedding backlog.
- Requeuing stuck Think jobs.
- Inspecting Think artifacts.
- Recovering post-commit actions.
- Rotating provider secrets.
- Pausing tenant ingestion.
- Killing a faulty feature flag.

Acceptance criteria:

- On-call can diagnose stuck pipeline from dashboards and SQL queries.
- Every DLQ has replay instructions.

---

# Phase 13: Deployment Plan

## Goal

Move from local development to dogfood to production safely.

## 13.1 Local compose

Services:

```text
gateway
postgres + pgvector
redis
kafka
minio
ollama
normalizer
observation-writer
embedding-worker
think-worker
post-commit-worker
housekeeper
prometheus
grafana
```

Acceptance criteria:

- One command boots the local stack.
- Seed script creates dogfood tenant and test source installation.
- E2E GitHub fixture can run locally.

## 13.2 Dogfood environment

Deploy:

- Gateway.
- PostgreSQL.
- Redis.
- Ollama or remote embedding provider.
- Think worker.
- Post-commit worker.
- Inline ingest first.

Then add:

- Kafka/S3 raw path.
- Normalizer.
- Observation writer.
- Live workers.

Acceptance criteria:

- Inline ingestion is stable before Kafka cutover.
- Kafka path can run in shadow mode.
- Tenant flags can route selected sources to Kafka path.

## 13.3 Production rollout

Rollout stages:

1. Read-only product APIs over seeded/demo data.
2. Inline ingest for one provider.
3. Think worker with mocked or restricted LLM output.
4. Full Think application for low-risk model updates.
5. Post-commit realtime updates.
6. Kafka/S3 ingestion for selected tenants/providers.
7. Live workers.
8. Forecasts/recommendations.
9. Broader provider expansion.

Acceptance criteria:

- Each stage has rollback flags.
- No stage requires data migration rollback to disable.
- Every source can be paused per tenant.

---

# Phase 14: Milestone Breakdown

## Milestone 1: Database and Gateway Skeleton

Deliver:

- Settings.
- Migrations.
- PostgreSQL pool.
- Gateway app.
- Health/metrics.
- Auth middleware.
- Tenant context.
- Basic repositories.

Exit criteria:

- Gateway boots.
- Tests can create tenant, actor, and source installation.

## Milestone 2: Inline GitHub Ingest

Deliver:

- GitHub webhook verifier.
- GitHub handler to ObservationDraft.
- Inline ingest core.
- Observation insert.
- Actor/entity resolution.
- Embedding fallback.
- Think trigger enqueue.

Exit criteria:

- GitHub webhook fixture creates one observation and one Think trigger.
- Duplicate webhook does not duplicate observation.

## Milestone 3: Minimal Think Loop

Deliver:

- Think queue worker.
- Retrieval v1.
- LLM provider mock + real provider abstraction.
- Structured Think output schema.
- Validator.
- Applier.
- Think telemetry.

Exit criteria:

- Observation trigger can create/update a model through mocked LLM.
- Invalid LLM output is rejected.
- Applied trigger is idempotent.

## Milestone 4: Product Read Path

Deliver:

- Model page.
- Model trace.
- Today v1.
- CEO home cache v1.
- History v1.

Exit criteria:

- Product APIs show state created by Think.
- Trace links observation to Think run to model update.

## Milestone 5: Post-Commit and Realtime

Deliver:

- Post-commit worker.
- Realtime dispatcher.
- Cache invalidation.
- Broadcast event handler.

Exit criteria:

- Think commit enqueues post-commit action.
- Worker processes action idempotently.
- Client can receive/refetch update.

## Milestone 6: Kafka/S3 Cutover Path

Deliver:

- Raw S3 writer.
- Kafka raw producer.
- Normalizer worker.
- Normalized topic.
- Observation writer.
- DLQ.
- Replay script.

Exit criteria:

- GitHub webhook can travel through Kafka/S3 path into same observation writer behavior.
- Kafka failure falls back to inline ingest when configured.

## Milestone 7: Embedding Recovery

Deliver:

- Kafka embedding retry topic.
- Embedding worker.
- Backlog drainer.
- Provider metrics.

Exit criteria:

- Observations inserted during embedding outage are later backfilled.
- Semantic retrieval includes backfilled rows.

## Milestone 8: Product Actions

Deliver:

- Decision deltas.
- Recommendation actions.
- Forecast views.
- Ask/query v1.
- Rendering service.

Exit criteria:

- User actions are persisted and auditable.
- Ask uses retrieval context and cites evidence internally.

## Milestone 9: Background Workers

Deliver:

- Entity resolver.
- Anomaly processor.
- Calibration updater.
- Deadline resolver.
- Housekeeper.
- GitHub intel worker.
- Topology/SAGE stubs.

Exit criteria:

- Workers run independently.
- Stuck jobs recover.
- Calibration and deadline state update without manual intervention.

## Milestone 10: Production Hardening

Deliver:

- Dashboards.
- Runbooks.
- Failure injection tests.
- Retention policies.
- Tenant isolation tests.
- Replay tooling.
- Feature flag control plane.

Exit criteria:

- System survives duplicate events, worker crashes, embedding outage, invalid LLM output, and Kafka/S3 fallback scenarios.
- On-call can inspect every major pipeline boundary.

---

# Cross-Cutting Implementation Details

## Idempotency keys

Use deterministic keys for repeated external and internal events.

Examples:

```text
observation dedup: tenant_id + source + source_channel + external_id
think trigger: tenant_id + trigger_type + observation_id
post-commit action: tenant_id + action_kind + source_transaction_id + semantic_target
```

## Transaction boundaries

Use explicit boundaries:

```text
Webhook ack boundary:
  after raw payload is stored + raw envelope published
  OR after inline ingest transaction commits

Observation insert boundary:
  observation insert + trigger enqueue in same transaction

Think boundary:
  domain mutations + applied_triggers + audit + post_commit_actions in same transaction

Post-commit boundary:
  side effect processed + action marked processed in one idempotent handler transaction where possible
```

## Feature flags

Required flags:

```text
kafka_ingestion_enabled
kafka_ingestion_shadow_mode
inline_ingest_enabled
think_enabled
think_apply_enabled
llm_provider_enabled
post_commit_enabled
realtime_enabled
recommendations_enabled
forecasts_enabled
ask_enabled
advanced_edge_discovery_enabled=false
relationship_ontology_proposals_enabled=false
```

## Minimal viable provider order

Recommended order:

1. GitHub
2. Google Calendar
3. Gmail
4. Google Drive
5. Slack
6. Jira
7. Discord / Telegram / Signal
8. Finance / HR / recruiting systems

Reason:

- GitHub is highly structured and good for e2e validation.
- Calendar and Gmail establish people/time/commitment context.
- Drive adds document context.
- Slack/Jira increase noisy organizational signal after the core can handle it.

## Data replay tools

Scripts:

```text
scripts/replay_raw.py
scripts/replay_normalized.py
scripts/requeue_think.py
scripts/requeue_post_commit.py
scripts/backfill_embeddings.py
scripts/rebuild_product_cache.py
scripts/reconcile_observations.py
```

Acceptance criteria:

- Replays are tenant/source/date scoped.
- Replays are safe under idempotency.
- Replays produce audit/reconciliation events.

---

# Definition of Done for Fyralis Core v1

Fyralis Core v1 is complete when:

1. A GitHub webhook can produce a durable observation through inline ingest.
2. The same GitHub event can travel through Kafka/S3 cutover path.
3. Observation insert enqueues a Think trigger.
4. Think worker retrieves bounded context and calls structured LLM provider.
5. Validator rejects malformed or out-of-region outputs.
6. Applier writes model/act/resource updates transactionally.
7. Post-commit actions are durable and idempotent.
8. Product APIs show updated organizational state.
9. Realtime updates notify clients to refetch.
10. Embedding failures do not block ingestion and are later recovered.
11. Background workers run safely with leases, metrics, and retries.
12. Audit, cost, artifact, and reconciliation tables can explain what happened.
13. Dashboards expose queue health, ingestion health, LLM cost, embedding backlog, product latency, and worker failures.
14. Failure injection tests pass for duplicate events, worker crashes, embedding outage, invalid LLM output, and Kafka/S3 fallback.
15. Advanced edge discovery remains feature-flagged off until the dedicated edge-intelligence workstream resumes.

---

# Recommended Build Order Summary

```text
0. Repo/config/migrations/observability baseline
1. Domain substrate: tenants, actors, observations, queues
2. Gateway: app factory, middleware, route mounting
3. Inline GitHub ingest
4. Embedding abstraction and fallback
5. Think queue and minimal Think worker
6. Retrieval v1 and context packet compiler
7. LLM provider and structured output schema
8. Validator/applier/audit/artifacts
9. Product model/trace/today/history APIs
10. Post-commit worker and realtime dispatcher
11. Kafka/S3 raw ingestion path
12. Normalizer and observation writer
13. Embedding retry worker and backlog drainer
14. Product actions: recommendations, decision deltas, forecasts, Ask, rendering
15. Background workers: entity resolver, anomaly, calibration, deadline, housekeeper, GitHub intel
16. Observability dashboards, runbooks, replay tools
17. Dogfood rollout
18. Production hardening
```
