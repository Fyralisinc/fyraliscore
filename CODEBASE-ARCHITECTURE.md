# **Company OS — Comprehensive Architectural Analysis**

## Executive Summary

**Company OS** is an organizational intelligence runtime designed to surface real-time insights to a founder/CEO by combining continuous signal ingestion, probabilistic reasoning (Models), executable commitments (Acts), and resource tracking (Resources). The system is single-tenant in the dogfood phase but architecturally multi-tenant ready. It consists of:

- A **Python FastAPI gateway** (`services/gateway/main.py:8000`) that coordinates all backend services
- A **React/Vite UI** (`ui/src/App.tsx`) running on port 5173
- A **PostgreSQL 16** database with pgvector (vector search)
- **Ollama** for local embeddings (`nomic-embed-text:v1.5`)
- **DeepSeek LLM** for reasoning (Think) and rendering (Greeting/Query)
- A **simulation harness** for authoring test scenarios
- An **LSOB benchmark suite** (Longitudinal Synthetic-Organization Benchmark) for evaluating reasoning quality

The core workflow is: **Ingest signal → Create Observation → Trigger Think → Generate Models/Acts → Cache & Render → Display to CEO**

---

## 1. Project Identity

### Name & Purpose
- **Project**: Company OS
- **Version**: 0.0.0 (dogfood/MVP phase)
- **Description** (from `pyproject.toml:8`): "organizational intelligence runtime"
- **Author**: Rachin Kalakheti (founder/CEO of the originating company)

### Core Domain Concepts
Drawn from `ARCHITECTURE-FINAL.md` and `SCHEMA-LOCK.md`:

1. **Four Foundations** — the atomic, epistemologically distinct stores:
   - **Observations** (`lib/shared/types.py:ObservationRow`) — append-only empirical signals (Slack messages, GitHub events, emails, calendar updates, financial transactions)
   - **Models** (`lib/shared/types.py:ModelRow`) — epistemic beliefs (hypotheses, patterns, predictions, assessments) with confidence scores, falsifiers, and lifecycle
   - **Acts** (`lib/shared/types.py:GoalRow`, `CommitmentRow`, `DecisionRow`) — executable declarations: Goals (strategic intentions), Commitments (owner-specific delivery promises), Decisions (company choices)
   - **Resources** (`lib/shared/types.py:ResourceRow`) — organizational assets (financial, IP, relational, capacity, infrastructure, regulatory)

2. **Universal Flow Rule** (`ARCHITECTURE-FINAL.md:18`): *input → Observation → Think → always Models, sometimes Acts, sometimes Resources*

3. **Personas** — the dogfood substrate has 12 personas (`simulation/personas.yaml:50-95`): Alice (Staff Engineer), Marcus (Head of Engineering), Monica (Head of Sales), David (CFO), Rachin (Founder/CEO), etc. Each seeds signals as if from a real person.

### Main Users/Personas
- **Rachin** (CEO, dogfood operator)
- **Future**: multi-tenant, role-based views (Operations, Sales, Finance, Product)

---

## 2. High-Level Architecture

### Conceptual Model

```
┌─────────────────────────────────────────────────────────────────────┐
│                       UI (React/Vite, :5173)                        │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │ CEO View: Greeting | Query Grid | Cards | Close Line | Ask   │  │
│  │ Subscribes to: /view/ceo/stream (WebSocket heartbeat)        │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                            ↕ HTTP/WS
┌─────────────────────────────────────────────────────────────────────┐
│                 Gateway (FastAPI, :8000)                            │
│  ┌─────────────┬──────────────┬──────────────┬──────────────┐      │
│  │ /ingest/*   │ /view/ceo/*  │ /rendering/* │ /simulation/ │      │
│  │ (Ingestion  │ (CEO view    │ (Internal    │ (Dev UI)     │      │
│  │  handlers)  │  endpoints)  │  RND calls)  │              │      │
│  └─────────────┴──────────────┴──────────────┴──────────────┘      │
│                            ↓                                        │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │ Service Layer (in-process)                                  │  │
│  │  • Ingestion (handlers, core.ingest)                        │  │
│  │  • Greeting (cache, scheduler, snapshot, rendering_adapter) │  │
│  │  • Query (ask/answer, prefetch, strategies)                 │  │
│  │  • Rendering (voice rules, prompts)                         │  │
│  │  • Think (reason, applier, validator)                       │  │
│  │  • Retrieval (primary, second-pass, maintenance)            │  │
│  │  • Models, Observations, Acts, Resources repos              │  │
│  │  • Entity aliases, Actors                                   │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                            ↓ asyncpg                                │
└─────────────────────────────────────────────────────────────────────┘
                            ↕
┌──────────────────────────────────────────────────────────────────────┐
│           PostgreSQL 16 (Postgres.local:5432)                        │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐      │
│  │ Observations │ Models       │ Acts         │ Resources    │      │
│  │ (partitioned │ (indexed)    │ (Goals,      │ (tracked)    │      │
│  │  by time)    │              │  Commits,    │              │      │
│  │              │              │  Decisions)  │              │      │
│  │ + Actors     │ + Proposals  │ + Triggers   │ + Xacts      │      │
│  │ + Cache      │ + Readings   │ + Queue      │              │      │
│  └──────────────┴──────────────┴──────────────┴──────────────┘      │
│  + pgvector (HNSW) for semantic search                              │
│  + PARTITION BY occurred_at (quarterly)                             │
│  + GIN indexes on JSON arrays                                       │
└──────────────────────────────────────────────────────────────────────┘
                            ↕
        ┌─────────────────────────────────────┐
        │ Ollama (:11434)                     │
        │ nomic-embed-text:v1.5 (768 dims)    │
        └─────────────────────────────────────┘
                            ↕
        ┌─────────────────────────────────────┐
        │ DeepSeek LLM API                    │
        │ deepseek-reasoner (Think)           │
        │ deepseek-chat (Rendering)           │
        └─────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────┐
│ Background Workers (separate processes, polling Postgres)           │
│  • Think Worker: drains think_trigger_queue (T1/T2/T3/T4 triggers)│
│  • Post-Commit Worker: persists model state changes                 │
│  • Greeting Scheduler: refreshes CEO view cache every 15 min       │
│  • (Future) Entity Resolver: resolves actor/entity refs             │
│  • (Future) Anomaly Processor: detects outliers                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow: A Request End-to-End

**Scenario**: Alice (Engineer) posts "shipped rate limiter fix in #2311" in `#eng` Slack channel.

1. **Ingestion** (`services/ingestion/core.py:1-35`):
   - Gateway receives `POST /ingest/slack:message` with signed payload
   - Handler extracts: `content_text="shipped rate limiter fix in #2311"`, `source_actor_ref="alice"`, `occurred_at=now()`, `trust_tier="attested_agent"`
   - Pre-assign `observation_id = uuid7()`
   - Resolve actor: `ActorRepo.resolve_by_source_actor_ref("slack", "alice")` → `actor_id = <alice's UUID>`
   - Fast-path entity extraction: tokenize content, lookup known aliases for "rate limiter", "2311" → `entities_mentioned=[{...}]`
   - Embed via Ollama: `OllamaClient.embed(content_text)` → 768-dim vector
   - **Inside transaction**: Insert `ObservationRow` into `observations` table (partitioned by occurred_at)
   - Enqueue T1 trigger in `think_trigger_queue` with `trigger_kind='T1'`, `observation_id`, `seed_entity_ids=[...]`
   - Emit NOTIFY on channel `observations_new` (for async entity resolver when deployed)

2. **Think** (`services/think/worker.py:1-28`, `reason.py`):
   - Think Worker polls `think_trigger_queue` every 2s (env `THINK_WORKER_POLL_INTERVAL_S`)
   - Dequeues T1 trigger: `TriggerContext(kind='T1', observation_id=..., seed_entity_ids=[...], seed_natural_text='shipped rate...')`
   - **Primary Retrieval** (`services/retrieval/primary.py:1-30`):
     - Call 4 pathways (weights for T1: A=0.4, B=0.4, C=0.2):
       - **Pathway A** (structural): find Models with `scope_entities` overlapping seed entities (rate limiter, issue #2311)
       - **Pathway B** (semantic): vector-search similar Models using observation embedding
       - **Pathway C** (temporal): find Models created in last 7 days with relevant scope
       - Merge results, rank by (pathway_weight × position_decay), return top 80 Models
   - **Reasoning** (`services/think/reason.py`):
     - Pass observation + retrieved Models to DeepSeek-reasoner
     - Model produces schema-validated JSON:
       ```python
       {
         "model_triggers": [
           {"kind": "state", "subject": "rate limiter issue", "confidence": 0.92, "falsifier": {...}},
           {"kind": "relation", "subject": "Alice → feature delivery velocity", "confidence": 0.65}
         ],
         "decision_triggers": [],
         "act_triggers": [...]
       }
       ```
   - **Application** (`services/think/applier.py`):
     - Insert new Models if not duplicates (check by natural + scope)
     - Update existing Models' `signal_readings`, `last_retrieved_at`
     - Enqueue any Acts for activation
   - Commit transaction; emit NOTIFY on `models_changed`

3. **Greeting Scheduler** (`services/greeting/scheduler.py`, cache-on-interval):
   - Every 15 min (env `GREETING_REFRESH_INTERVAL_SECONDS=900`), scheduler refreshes CEO cache
   - **Snapshot** (`services/greeting/snapshot.py`):
     - Assemble `SubstrateSnapshot`: top active Models, active Commitments, resource health, recent state changes, top 3 anomalies, CEO conversation context
   - **Rendering** (RND via HTTP adapter):
     - Send `RenderGreetingRequest` to `POST /rendering/greeting`
     - RND calls DeepSeek-chat with prose prompt: "Write a concise greeting for the CEO mentioning these situation Models..."
     - Returns `RenderGreetingResponse` with `body_html`, cost attribution
   - **Cache** (`services/greeting/cache.py`):
     - Store in Postgres `view_ceo_cache` table with `cache_key='greeting'`, `cached_content={body_html, meta}`, `cached_at=now()`
   - **WebSocket broadcast** (`services/greeting/stream.py`):
     - Push to all connected UI clients: `{type: 'greeting_updated', greeting: {...}}`

4. **CEO View Render** (HTTP `GET /view/ceo/home`):
   - Authenticate via header `X-Tenant-Id` or fallback dev token
   - Fetch all cache keys from `view_ceo_cache`
   - Assemble into CONTRACTS §1.1 payload:
     ```json
     {
       "greeting": { body_html: "...", meta: {...}, cached_at: "...", staleness_seconds: 42 },
       "query_grid": { queries: [...], cached_at: "..." },
       "cards": [ { id, kind, body_html, expanded: { reasoning_html, evidence, verbs }, ... } ],
       "close_line": { body: "...", metadata: {...} },
       "status": { substrate_alive, calibration_pct, needs_you_count }
     }
     ```
   - Return as JSON; UI renders with CSS/React

5. **CEO Asks a Question** (HTTP `POST /view/ceo/ask`):
   - Request: `{query: "what's the status of the rate limiter?", context_card_id: "obs-48201"}`
   - Query handler classifies: is this a prefetch-cache hit? (no)
   - Call `AnswerQueryRequest` pipeline:
     - **Retrieval**: second-pass context via `services/retrieval/second_pass.py` — re-rank cached Models with fresh context
     - **Rendering**: call `POST /rendering/conversation-turn` with Models + query
     - RND generates prose response HTML
   - Return `AnswerQueryResponse` with `turn_id, query_echo, response_html, verbs=[{label: 'save'}, {label: 'followup'}], latency_ms=847`
   - UI renders turn; CEO can mark as 'save' or 'done'

---

## 3. Per-Directory Deep Dive

### **`lib/` — Shared Libraries**

**Purpose**: Reusable, infrastructure-level abstractions.

| Subdirectory | Purpose | Key Files |
|---|---|---|
| `lib/llm` | LLM provider abstraction | `provider.py:77-150` — pluggable backend (Anthropic/OpenAI/DeepSeek); `tests/test_provider.py` |
| `lib/embeddings` | Vector embedding service | `ollama.py:1-80` — wraps Ollama HTTP; `tests/test_ollama.py` |
| `lib/nexus` | Agent attestation stub | `client.py` — Phase 4 integration point (currently mock) |
| `lib/shared` | Shared types, DB, errors | `types.py:1-150` — Pydantic models for all rows; `db.py` — asyncpg helpers; `errors.py` — domain exceptions; `ids.py` — UUID7 generation; `trust.py` — trust tier logic |

**Public API**:
- `lib.llm.provider.build_provider(provider_name, api_key, model) → LLMProvider`
- `lib.embeddings.ollama.OllamaClient(url).embed(text) → list[float]`
- `lib.shared.types.ObservationRow, ModelRow, GoalRow, CommitmentRow, DecisionRow, ResourceRow` (Pydantic)
- `lib.shared.db.fetch_all(pool, query, params) → list[Row]`
- `lib.shared.ids.uuid7() → UUID` (time-ordered)

---

### **`services/` — Business Logic Microservices**

The system is organized as a single-process app with logical service boundaries. Each `services/<domain>/` is a module with a repo layer, event handlers, and tests.

#### **services/observations** (`/Users/rachinkalakheti/fyraliscore/services/observations/`)
- **Purpose**: Immutable observation store (append-only signals)
- **Key Files**:
  - `repo.py` — `ObservationRepository` class; methods: `insert(ObservationCreate)`, `get_by_id(id)`, `list_by_tenant(tenant_id)`, `list_by_embedding_similarity(...)`
  - `events.py` — post-insert hooks (NOTIFY, cost recording)
  - `state_change.py` — specialized handler for state-change observations
  - `partitions.py` — manages quarterly partitions for observations table
- **Data Model** (`lib/shared/types.py:135-150`):
  ```python
  class ObservationRow:
    id: UUID
    tenant_id: UUID
    occurred_at: datetime
    ingested_at: datetime
    kind: ObservationKind  # 'signal' | 'state_change' | 'anomaly_flagged' | ...
    source_channel: str    # 'slack:eng' | 'github:pr' | 'linear' | ...
    actor_id: UUID | None
    content: dict[str, Any]
    content_text: str
    embedding: list[float] | None
    trust_tier: TrustTierValue  # 'authoritative' | 'attested_agent' | ...
  ```
- **DB Table**: `observations` (partitioned by `occurred_at`, HNSW index on embedding)

#### **services/models** (`/Users/rachinkalakheti/fyraliscore/services/models/`)
- **Purpose**: Belief store — epistemic models about the organization
- **Key Files**:
  - `repo.py` — `ModelsRepo` class; methods: `insert(ModelCreate)`, `retrieve(ids, conn)` (activation bump), `get_by_id(id)`, `list_active_by_tenant(tenant_id)`, `update_status(..., reason)`
  - `propositions.py` — helpers for Model proposition schema validation
  - `calibration.py` — confidence calibration machinery
  - `decay.py` — time-based confidence decay (older Models lose confidence)
  - `falsifier.py` — conditions that would invalidate a Model
  - `status_notes.py` — human-override notes on Model status
- **Data Model** (`lib/shared/types.py:152-200`):
  ```python
  class ModelRow:
    id: UUID
    tenant_id: UUID
    born_from_event_id: UUID  # observation_id that triggered this Model
    proposition: dict  # {kind: 'state'|'relation'|..., subject, predicate, ...}
    natural: str  # human prose: "Alice is a high-velocity engineer"
    embedding: list[float]  # of natural text
    scope_actors: list[UUID]
    scope_entities: list[dict]
    scope_temporal: dict  # {start: datetime, end: datetime, window: ...}
    confidence: float  # 0.05–0.95
    activation: float  # 0.0–1.0 (how recently retrieved)
    falsifier: dict | None  # condition that would flip status to 'contested_false'
    signal_readings: list[dict]  # supporting evidence
    status: ModelStatus  # 'active' | 'archived' | 'superseded' | 'contested_false'
    evaluate_at: datetime | None  # for predictions
  ```
- **DB Table**: `models` (indexed on tenant_id, status, confidence, activation)

#### **services/acts** (`/Users/rachinkalakheti/fyraliscore/services/acts/`)
- **Purpose**: Executable declarations — Goals, Commitments, Decisions
- **Key Files**:
  - `repo.py` — repos for Goals, Commitments, Decisions
  - Lifecycle state machines per act type
- **Data Models**:
  ```python
  class GoalRow:
    id: UUID
    tenant_id: UUID
    title: str
    owner_id: UUID
    state: GoalState  # 'active' | 'paused' | 'achieved' | 'abandoned'
    altitude: GoalAltitude  # 'strategic' | 'operational' | 'tactical'
    metrics: list[dict]
  
  class CommitmentRow:
    id: UUID
    tenant_id: UUID
    owner_id: UUID
    goal_id: UUID  # optional parent
    description: str
    state: CommitmentState  # 'proposed' | 'active' | 'blocked' | 'doneunverified' | 'doneverified' | 'closed'
    due_at: datetime
    ambition_level: AmbitionLevel  # 'base' | 'stretch' | 'aspirational'
  
  class DecisionRow:
    id: UUID
    tenant_id: UUID
    title: str
    made_at: datetime
    maker_id: UUID
    options_considered: list[dict]
    chosen_option: str
    state: DecisionState  # 'drafted' | 'active' | 'revisited' | 'archived'
  ```

#### **services/resources** (`/Users/rachinkalakheti/fyraliscore/services/resources/`)
- **Purpose**: Track organizational assets (financial, IP, relational, etc.)
- **Data Models**:
  ```python
  class ResourceRow:
    id: UUID
    kind: ResourceKind  # 'financial' | 'ip' | 'relational' | 'capacity' | 'infrastructure' | 'regulatory'
    owner_id: UUID
    description: str
    utilization_state: ResourceUtilizationState
  
  class ResourceTransactionRow:
    id: UUID
    resource_id: UUID
    type: ResourceTransactionType  # 'acquire' | 'deploy' | 'spend' | ...
    amount: Decimal | None
    recorded_at: datetime
  ```

#### **services/think** (`/Users/rachinkalakheti/fyraliscore/services/think/`)
- **Purpose**: Cognitive engine — reasons about signals to generate Models/Acts
- **Key Files**:
  - `worker.py:1-28` — main loop; polls `think_trigger_queue` with FOR UPDATE SKIP LOCKED; spawns `think()` calls
  - `reason.py` — core reasoning logic; calls LLM with prompt template; parses response into schema
  - `applier.py` — applies LLM output (inserts/updates Models, enqueues Acts)
  - `validator.py` — schema validation post-LLM
  - `llm_reason.py` — LLM call wrapper with retries, cost attribution
  - `prompt.py` — prompt engineering; templates for T1/T2/T3/T4
  - `deterministic.py` — unit tests with frozen LLM responses
  - `circuit_breaker.py` — fallback behavior if LLM is unavailable
  - `observability.py` — metrics (latency, cost, errors)
  - `post_commit.py` — post-commit side effects (cache invalidation, notification)
- **Trigger Types** (`ARCHITECTURE-FINAL.md §7`):
  - **T1** — New signal (observation): pathway mix A+B+C
  - **T2** — Prediction resolution due: pathways A+D
  - **T3** — Anomaly detected: pathways A+B+C
  - **T4** — Background pattern/model-reeval: pathways D+A
- **Prompt Example** (simplified):
  ```
  You are reasoning about organizational signals for a company.
  
  Signal: "shipped rate limiter fix in #2311"
  Source: Alice (Staff Engineer), Slack #eng, 2026-04-23T09:12Z
  Trust tier: attested_agent
  Related models: [list of similar recent models]
  
  Produce a JSON output with:
  {
    "model_triggers": [
      {"kind": "state", "subject": "Rate limiter deployment status", 
       "confidence": 0.88, "falsifier": {...}},
      ...
    ],
    "decision_triggers": [],
    "act_triggers": []
  }
  ```

#### **services/retrieval** (`/Users/rachinkalakheti/fyraliscore/services/retrieval/`)
- **Purpose**: Multi-pathway context retrieval for Think and Query
- **Key Files**:
  - `primary.py:1-30` — main orchestrator; per-trigger pathway weighting; merge + RRF ranking
  - `pathways.py` — implementations:
    - **Pathway A** (structural): actor/entity-scope overlap
    - **Pathway B** (semantic): vector similarity search
    - **Pathway C** (temporal): recent Models in time window
    - **Pathway D** (pattern): precipitation patterns + background relationships
  - `second_pass.py` — re-rank Models with fresh conversational context
  - `maintenance.py` — background relationship updates (links Models to related Models)
  - `scoring.py` — RRF (Reciprocal Rank Fusion) + position decay
  - `assembler.py` — rebuild complex objects from DB rows
  - `config.py` — tuning knobs (top_n=80, decay_base=0.9, etc.)
- **TriggerContext** (`primary.py:86-100`):
  ```python
  @dataclass
  class TriggerContext:
    kind: TriggerKind  # T1, T2, T3, T4
    observation_id: UUID | None
    model_id: UUID | None
    seed_entity_ids: list[UUID]
    seed_natural_text: str | None
    seed_occurred_at: datetime | None
    scope_actors: list[UUID]
  ```

#### **services/greeting** (`/Users/rachinkalakheti/fyraliscore/services/greeting/`)
- **Purpose**: Assemble and cache the CEO view (greeting, query grid, cards, close line)
- **Key Files**:
  - `scheduler.py` — `GreetingScheduler` class; periodic refresh on interval; triggers for manual refresh
  - `cache.py` — `ViewCeoCacheRepo` class; methods: `get_all(tenant_id)`, `set(tenant_id, cache_key, content)`, `invalidate(...)`
  - `snapshot.py` — `SubstrateSnapshot` builder; gathers top Models, active Commitments, anomalies, conversation context
  - `rendering_adapter.py` — abstraction over rendering backends (HTTP adapter for real RND, mock for fallback)
  - `api.py` — FastAPI router for `GET /view/ceo/home` and `POST /view/ceo/force-refresh`
  - `stream.py` — WebSocket manager for `/view/ceo/stream`; broadcasts cache updates
- **Cache Schema** (`services/greeting/cache.py`, mirrors CONTRACTS §3):
  ```sql
  CREATE TABLE view_ceo_cache (
    tenant_id UUID NOT NULL,
    cache_key TEXT NOT NULL,  -- 'greeting' | 'cards' | 'query_grid' | 'status' | 'close_line'
    cached_content JSONB NOT NULL,
    cached_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    recomputed_reason TEXT,   -- 'scheduled' | 'trigger_fired' | 'manual'
    PRIMARY KEY (tenant_id, cache_key)
  );
  ```

#### **services/query** (`/Users/rachinkalakheti/fyraliscore/services/query/`)
- **Purpose**: Handle CEO Ask → Answer pipeline
- **Key Files**:
  - `api.py:1-80` — FastAPI routes: `POST /view/ceo/ask`, `POST /view/ceo/turn-action`
  - `core.py` — `QueryHandler`, `AnswerQueryRequest/Response`
  - `classifier.py` — classify query type (factual, predictive, strategic, etc.)
  - `adapters.py` — rendering adapter (same pattern as greeting)
  - `prefetch.py` — pre-cache query responses for known chips
  - `strategies/` — different answering strategies per query type
- **Conversation Turn** (internal model):
  ```python
  @dataclass
  class Turn:
    turn_id: UUID
    query: str
    response_html: str
    verbs: list[{id: str, label: str}]
    computed_at: datetime
    latency_ms: int
  ```

#### **services/rendering** (`/Users/rachinkalakheti/fyraliscore/services/rendering/`)
- **Purpose**: LLM-backed prose generation with voice enforcement
- **Key Files**:
  - `api.py` — FastAPI routes: `POST /rendering/greeting`, `/card`, `/card-reasoning`, `/query-grid`, `/conversation-turn`, `/close-line`
  - `core.py` — orchestrator; calls LLM, applies voice rules, records cost
  - `prompts/` — Jinja2 templates for each render type
  - `voice_rules.py:1-80` — rule engine; checks:
    - No exclamation marks
    - No marketing language ("leverage", "synergy", etc.)
    - No emoji
    - Sentences ≤35 words
    - Cards must reference concrete data (names, numbers, dates)
    - No hedge preamble ("I'd like to", "FYI")
  - `contracts.py` — Pydantic models for request/response
- **Cost Attribution**:
  Every rendering call logs to `view_render_costs` table with `render_kind, tenant_id, latency_ms, tokens_used, cost_usd, model_used`

#### **services/ingestion** (`/Users/rachinkalakheti/fyraliscore/services/ingestion/`)
- **Purpose**: Parse incoming signals (Slack, GitHub, email, calendar, etc.) into Observations
- **Key Files**:
  - `core.py:1-35` — `ingest(payload, channel, conn)` function; orchestrates the UniformIngestPath
  - `handlers/` — per-channel handlers:
    - `slack.py` — signature verification, message extraction
    - `github.py` — PR/issue/push event parsing
    - `email.py` — email parsing
    - `linear.py` — Linear issue state extraction
    - `calendar.py` — calendar event extraction
  - `handlers/__init__.py` — `CHANNEL_TRUST_MAP` (default trust tier per channel)
- **Handler Contract** (`core.py:1-7`):
  ```python
  @dataclass
  class ObservationDraft:
    content_text: str
    content: dict[str, Any]
    source_actor_ref: str  # "alice", "alice@slack.com", etc.
    external_id: str | None
    occurred_at: datetime
    entities_hint: list[str]  # ["rate limiter", "issue 2311"]
  ```

#### **services/gateway** (`/Users/rachinkalakheti/fyraliscore/services/gateway/`)
- **Purpose**: HTTP entry point; auth, rate limiting, request context
- **Key Files**:
  - `main.py:1-27` — FastAPI app builder; `build_app(pool, actor_repo, alias_repo, embedder, rate_limiter)` factory
  - `auth.py` — `validate_token(token)`, `create_session(body)` helpers
  - `db_bootstrap.py` — pool creation, codec registration, schema validation
  - `logging_config.py` — structlog setup
  - `rate_limit.py` — token-bucket rate limiter; per-(tenant, actor) buckets
- **Middlewares**:
  - `BearerAuthMiddleware` — extracts `Authorization: Bearer <token>` or `X-Tenant-Id` header
  - `RateLimitMiddleware` — enforces per-tenant/actor quotas
  - `RequestContextMiddleware` — binds request_id, tenant_id to structlog context

#### **services/entity_aliases** (`/Users/rachinkalakheti/fyraliscore/services/entity_aliases/`)
- **Purpose**: Map textual entity references to canonical entity IDs
- **Key Files**:
  - `repo.py` — `EntityAliasRepo.fast_path_resolve(phrases)` → list[{phrase, entity_id}]
  - Backing table: `entity_aliases` with columns: `entity_id, entity_kind, phrase, confidence, source_channel`

#### **services/actors** (`/Users/rachinkalakheti/fyraliscore/services/actors/`)
- **Purpose**: Map actor references (Slack usernames, GitHub logins, email addresses) to actor UUIDs
- **Key Files**:
  - `repo.py` — `ActorRepo.resolve_by_source_actor_ref(channel, ref)` → UUID | None
  - Backing tables: `actors`, `actor_identity_mappings`

#### **Other Services** (supporting)
- **services/bridge** — external system integration stubs
- **services/realtime** — WebSocket subscriptions
- **services/access_control** — policy enforcement
- **services/contestability** — Model review/override workflows
- **services/falsifiers** — condition evaluation for Model invalidation
- **services/synthetic** — dogfood data injection (guarded by `COMPANY_OS_ENV`)
- **services/workers** — background job coordination

---

### **`db/` — Schema & Migrations**

**Purpose**: PostgreSQL schema definition; append-only migration files.

- **`db/migrations/0001_foundation.sql`** (150+ lines):
  - Extensions: `vector`, `pg_trgm`, `btree_gin`
  - Foundation tables: `actors`, `observations` (partitioned), `models`, `goals`, `commitments`, `decisions`, `resources`
  - Indexes: HNSW on embeddings, GIN on JSON arrays, B-tree on common predicates
  - Partitions: observations by occurred_at (quarterly)

- **`db/migrations/0002_*.sql` through `0019_*.sql`**:
  - Actor sessions, Think trigger queue, entity review queue, relationship maintenance, calibration, access control, cost attribution, etc.
  - Example: `0004_think_trigger_queue.sql` creates:
    ```sql
    CREATE TABLE think_trigger_queue (
      id UUID PRIMARY KEY,
      trigger_kind CHAR(2) NOT NULL,  -- T1, T2, T3, T4
      payload JSONB NOT NULL,  -- serialized TriggerContext
      enqueued_at TIMESTAMPTZ DEFAULT now(),
      processing_started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      attempt_count INT DEFAULT 0,
      last_error TEXT
    );
    ```

- **`db/seed/`**:
  - SQL and Python scripts to bootstrap test/demo data
  - `personas_seed.sql` — default personas for dogfood

---

### **`ui/` — React/Vite Frontend**

**Tech Stack** (from `ui/package.json`):
- **React** 18.3.1
- **Vite** 5.4.8 (dev server on :5173)
- **TypeScript** 5.5.4
- **TailwindCSS** 3.4.13 (styling)
- **Playwright** 1.47.2 (e2e tests)
- **Vitest** 2.1.2 (unit tests)

**Key Components** (`ui/src/components/`):
- **App.tsx:19-60** — root layout; lifts all state (active card, turns, input focus)
- **TopBar.tsx** — navigation, logo, status indicator
- **Greeting.tsx** — prose rendering of the greeting block
- **QueryGrid.tsx** — query chip grid; each chip is a pre-loaded question
- **Card.tsx** — card shell (observation, decision, or question kind)
- **CardExpanded.tsx** — expanded card with reasoning, evidence, action verbs
- **GroundInput.tsx** — text input for CEO Ask; `/` shortcut to focus
- **ConversationTurn.tsx** — rendered turn history from Ask/Answer pipeline
- **CloseLine.tsx** — summary metrics (signal count, external moves, calibration)
- **Icon.tsx** — Lucide icon wrapper with fallbacks

**Hooks** (`ui/src/hooks/`):
- **useHome()** — fetches `GET /view/ceo/home`, subscribes to `WS /view/ceo/stream`
- **useAsk()** — manages conversation turns; `ask(query)` → `POST /view/ceo/ask`

**API Layer** (`ui/src/api/`):
- **types.ts** — TypeScript contracts (mirrors CONTRACTS.md)
- **client.ts** — HTTP helpers with auth token injection
- **websocket.ts** — WS subscription manager

**Styling**: Tailwind utility classes; custom variables for Company OS colors (serif font, highlight tint, citations).

---

### **`simulation/` — Dogfood Harness**

**Purpose**: Author synthetic signals as different personas; drive Think engine for local testing.

- **personas.yaml** — 12 persona definitions (UUID, role, voice hints)
- **personas.py** — Python loader; `load_personas()`, `switch_active_persona()`, `voice_hints_for(handle)`
- **server.py** — FastAPI app with `/simulation/inject` endpoint; serves Slack simulator UI
- **slack_ui/** — React SPA (no build); posts to `/simulation/inject`
- **scenarios/** — YAML scenario files:
  - **acme_tuesday.yaml** — 38 events across 7 days (Acme renewal risk narrative)
  - **quiet_week.yaml** — 9 low-stakes events
  - **two_fires.yaml** — 13 events, two concurrent situations
- **scenarios/replay.py** — loads YAML, injects events into DB with configurable speed
- **workers/** — CLI wrappers around inject for GitHub, email, calendar, Linear
- **reset.py** — purge synthetic observations (guarded by `COMPANY_OS_ENV`)
- **inspect.py** — print/JSON dump of tenant state (observations, Models, Acts, Resources counts)

---

### **`lsob/` — Benchmark Suite**

**Purpose**: Longitudinal Synthetic-Organization Benchmark; evaluate reasoning quality over time.

**Structure** (Rust-like workspace):
- **lsob-contracts** — shared types/interfaces
- **lsob-simulation** — scenario generation (longer, more complex than manual dogfood scenarios)
- **lsob-evaluator-l{1,6}** — evaluation harnesses (L1=raw output, L6=business outcome)
- **lsob-baselines** — control algorithms for comparison
- **lsob-harness** — runner; orchestrates scenario replay + evaluation
- **lsob-reporting** — result aggregation + visualization

Not fully populated in MVP; core infrastructure in place for Wave 6+ evaluation campaigns.

---

### **`scripts/` — Operational & Development Tools**

| Script | Purpose |
|--------|---------|
| `dogfood_up.sh` | Bring up: gateway (:8000), workers, UI (:5173) |
| `dogfood_down.sh` | Graceful SIGTERM → SIGKILL shutdown |
| `dogfood_logs.sh` | Interleaved tail of all service logs |
| `dogfood_inspect.sh` | One-shot tenant state summary |
| `reset_dogfood_tenant_data.sh` | Purge synthetic data (keep personas) |
| `seed_dogfood_tenant.py` | Idempotent persona + CEO actor bootstrap |
| `run_think_worker.py` | Long-running Think worker process |
| `run_post_commit_worker.py` | Long-running post-commit worker |
| `check_schema_drift.py` | Validate DB matches migrations |
| `think_dashboard.py` | Debug: show in-flight Think runs |
| `capture_scenario_home.py` | Save snapshot of CEO view (for test fixtures) |
| `diagnose_retrieval_determinism.py` | Debugging: retrieval path variance |
| `voice_spot_check.py` | QA: render a card and check voice rules |

---

## 4. Function-Level Catalog (Key Modules)

### **services.ingestion.core**
- **`ingest(payload: dict, channel: str, conn: asyncpg.Connection) → IngestResult`** (line 35+)
  - Parse payload via handler, extract ObservationDraft
  - Resolve actor and fast-path entities
  - Embed via Ollama
  - Insert Observation row
  - Enqueue T1 trigger
  - **Called from**: `services/gateway/main.py` route handler

- **`_extract_phrases(text: str) → list[str]`** (line 79+)
  - Tokenize content_text into 1-3 word runs
  - Used by fast-path entity lookup
  - **Called from**: `ingest()`

### **services.think.reason**
- **`think(trigger: TriggerContext, pool: asyncpg.Pool, provider: LLMProvider) → ThinkRunOutcome`** (main entry)
  - Call primary_retrieve(trigger, pool)
  - Format prompt from prompt.py template
  - Call provider.complete_json(prompt) → response JSON
  - Validate response schema via validator.py
  - Apply result via applier.py
  - **Called from**: `services/think/worker.py` main loop

### **services.think.applier**
- **`apply_think_run(outcome: ThinkRunOutcome, pool: asyncpg.Pool, tenant_id: UUID) → None`** (main entry)
  - Insert new Models from outcome.model_triggers
  - Update existing Models' signal_readings
  - Enqueue Act triggers
  - Commit transaction
  - Emit post-commit events
  - **Called from**: `think()` in reason.py

### **services.retrieval.primary**
- **`primary_retrieve(trigger: TriggerContext, pool: asyncpg.Pool, config: RetrievalConfig) → RetrievalResult`** (line 60+)
  - For each enabled pathway (A, B, C, D based on trigger_kind):
    - Call pathway_a_structural / pathway_b_semantic / pathway_c_temporal / pathway_d_pattern
    - Each returns PathwayResult with Models + scores
  - Merge results (union by Model.id)
  - Score via merge_and_rank_rrf with per-pathway weights
  - Bump activation on returned Models via ModelsRepo.retrieve(ids, conn)
  - **Called from**: `think()` in reason.py

### **services.retrieval.pathways**
- **`pathway_a_structural(trigger, pool) → PathwayResult`**
  - Query `models` WHERE `scope_actors` && trigger.scope_actors OR `scope_entities` overlaps seed_entity_ids
  - Rank by recency and activation
  - Return top matches

- **`pathway_b_semantic(trigger, pool, embedder: OllamaClient) → PathwayResult`**
  - Embed trigger.seed_natural_text via Ollama
  - Vector similarity search: `SELECT * FROM models ORDER BY embedding <-> query_vec LIMIT top_n`
  - Return Models ranked by cosine distance

- **`pathway_c_temporal(trigger, pool) → PathwayResult`**
  - Query `models` WHERE created_at > now() - 7 days AND scope_temporal overlaps trigger's time window
  - Return recently active Models

- **`pathway_d_pattern(trigger, pool) → PathwayResult`**
  - For T4 triggers: retrieve from precipitation worker's pattern candidates
  - Match via seed_signature (pattern hash)
  - Return background relationship Models

### **services.greeting.scheduler**
- **`GreetingScheduler.refresh_tenant(tenant_id: UUID, reason: str) → None`** (async)
  - Build SubstrateSnapshot via snapshot.py
  - Call rendering_adapter.render_greeting(req) → RenderGreetingResponse
  - Store response in view_ceo_cache
  - Emit WebSocket message to all connected clients
  - **Called from**: periodic scheduler + manual POST /view/ceo/force-refresh

### **services.greeting.snapshot**
- **`build_substrate_snapshot(tenant_id: UUID, pool: asyncpg.Pool) → SubstrateSnapshot`** (main entry)
  - Query top 10 active Models (by activation * confidence)
  - Query active Commitments (state != 'closed')
  - Query customer resources with health != 'healthy'
  - Query recent state changes (last 24h)
  - Query top 3 anomalies via anomaly table
  - Query recent CEO asks from conversation context cache
  - Determine time_of_day_bucket (early_morning, morning, afternoon, evening, late)
  - **Called from**: `GreetingScheduler.refresh_tenant()`

### **services.query.api**
- **`ask(req: AskRequestBody, pool: asyncpg.Pool, ...) → AskResponse`** (async route handler, line 66+)
  - Check if query_id → prefetch cache hit
  - Else: call `answer_query(req.query, req.context_card_id, pool, ...)`
  - Return AskResponse with turn_id, response_html, verbs
  - **Called from**: `POST /view/ceo/ask` route

- **`turn_action(req: TurnActionRequest, ...) → TurnActionResponse`** (async route handler)
  - If action == 'save': add turn_id to in-memory saved set
  - If action == 'done': mark complete
  - If action == 'followup': recursively call ask()
  - **Called from**: `POST /view/ceo/turn-action` route

### **services.query.core**
- **`AnswerQueryRequest.execute(pool: asyncpg.Pool, ...) → AnswerQueryResponse`** (async method)
  - Classify query type via classifier.py
  - Retrieve context Models via second_pass.py
  - Call rendering_adapter.render_conversation_turn(req) → HTML response
  - Record cost to view_render_costs
  - **Called from**: `ask()` in api.py

### **services.rendering.core**
- **`render_greeting(req: RenderGreetingRequest, provider: LLMProvider) → RenderGreetingResponse`** (async)
  - Format Jinja2 prompt template with req fields
  - Call provider.complete(prompt) → text response
  - Strip HTML; apply voice rules via VoiceRuleEngine
  - On rejection: retry with explicit correction prompt
  - On success: return response_html (auto-wrapped as markdown HTML)
  - Record cost_usd via cost_record()
  - **Called from**: HTTP client in greeting_adapter.py

- **Similar functions for `render_card`, `render_card_reasoning`, `render_query_grid`, `render_conversation_turn`, `render_close_line`**

### **services.rendering.voice_rules**
- **`VoiceRuleEngine.check(text: str, context: RuleContext) → list[Violation]`** (main entry)
  - Instantiate all VoiceRule subclasses
  - For each rule: rule.check(text, context)
  - Collect violations with severity (REJECT / FLAG)
  - Return list sorted by severity
  - **Called from**: `render_greeting()` and other render functions

- **`class RequiresSpecificity(VoiceRule)`** — check that card body contains at least one concrete reference (name, number, date, dollar amount)

- **`class NoExclamationMarks(VoiceRule)`** — trivial; regex for `!`

- **`class NoMarketingLanguage(VoiceRule)`** — list of forbidden phrases ("synergy", "leverage", "unpack", "exciting")

- **`class SentenceLengthLimit(VoiceRule)`** — split on period; max 35 words per sentence

---

## 5. Data Model (Schemas)

### **Foundation: Four Atomic Stores**

#### **S1 — Observations**
Immutable, append-only. Every signal becomes an Observation.

```sql
CREATE TABLE observations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,  -- when the signal happened (user-provided)
  ingested_at TIMESTAMPTZ DEFAULT now(),  -- when we received it
  kind TEXT NOT NULL,  -- 'signal' | 'state_change' | 'anomaly_flagged' | ...
  source_channel TEXT NOT NULL,  -- 'slack:eng' | 'github:pr' | 'linear' | ...
  source_actor_ref TEXT,  -- raw ref from channel (e.g., "alice", "PR#847")
  actor_id UUID REFERENCES actors(id),  -- resolved actor or NULL
  content JSONB NOT NULL,  -- channel-specific fields; always includes '_synthetic' key
  content_text TEXT NOT NULL,  -- prose summary for embedding + search
  embedding VECTOR(768),  -- Ollama-generated; pgvector HNSW index
  embedding_pending BOOLEAN,  -- true if embed failed; retry later
  trust_tier TEXT NOT NULL,  -- 'authoritative' | 'attested_agent' | 'inferential' | ...
  external_id TEXT,  -- dedup key (e.g., Slack message ts, GitHub issue #)
  cause_id UUID,  -- optional parent observation (for annotations)
  entities_mentioned JSONB,  -- [{entity_id, phrase}] from entity resolver
  PARTITION BY RANGE (occurred_at)  -- quarterly partitions
);
```

#### **S2 — Models**
Epistemic beliefs with confidence, activation, and lifecycle.

```sql
CREATE TABLE models (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  born_from_event_id UUID NOT NULL,  -- observation.id that triggered this Model
  
  -- Content
  proposition JSONB NOT NULL,  -- {kind: 'state'|'relation'|'prediction'|..., subject, ...}
  "natural" TEXT NOT NULL,  -- human-readable prose: "Alice ships fast"
  embedding VECTOR(768) NOT NULL,  -- of "natural" text
  
  -- Scope
  scope_actors UUID[] DEFAULT '{}',  -- set of involved actors
  scope_entities JSONB DEFAULT '[]'::jsonb,  -- [{entity_id, entity_kind}]
  scope_temporal JSONB NOT NULL,  -- {start: datetime, end: datetime, bucket: ...}
  
  -- Epistemic
  confidence FLOAT NOT NULL CHECK (confidence >= 0.05 AND confidence <= 0.95),
  activation FLOAT DEFAULT 1.0,  -- how recently retrieved (0–1)
  falsifier JSONB,  -- condition that would invalidate this Model
  
  -- Signal readings
  signal_readings JSONB DEFAULT '[]'::jsonb,  -- list of supporting observations
  reading_contestable BOOLEAN DEFAULT TRUE,
  
  -- Provenance
  supporting_event_ids UUID[] DEFAULT '{}',  -- observations referenced
  supporting_model_ids UUID[] DEFAULT '{}',  -- Models referenced
  evidential_weight FLOAT DEFAULT 0.5,
  
  -- Lifecycle
  status TEXT NOT NULL DEFAULT 'active',  -- 'active' | 'archived' | 'superseded' | 'contested_false'
  archived_at TIMESTAMPTZ,
  archive_reason TEXT,  -- 'decay' | 'falsifier_triggered' | 'manual' | ...
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_retrieved_at TIMESTAMPTZ,  -- bumped by retrieval
  retrieval_count INTEGER DEFAULT 0,
  
  -- Prediction-specific
  evaluate_at TIMESTAMPTZ,  -- T2 trigger scheduled at this time
  resolution_criteria JSONB,  -- how to evaluate if prediction came true
  contributing_models UUID[],  -- Models that influenced this prediction
  
  -- Access
  visible_to_subjects BOOLEAN DEFAULT TRUE
);
```

#### **S3 — Acts**
Three types: Goals, Commitments, Decisions.

```sql
CREATE TABLE goals (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  title TEXT NOT NULL,
  owner_id UUID NOT NULL REFERENCES actors(id),
  state TEXT NOT NULL,  -- 'active' | 'paused' | 'achieved' | 'abandoned'
  altitude TEXT NOT NULL,  -- 'strategic' | 'operational' | 'tactical'
  description TEXT,
  metrics JSONB,  -- [{name, target, current, trend}]
  supporting_model_ids UUID[],  -- Models that motivated this Goal
  created_at TIMESTAMPTZ DEFAULT now(),
  cached_health TEXT DEFAULT NULL  -- 'healthy' | 'warning' | 'degraded' | 'critical'
);

CREATE TABLE commitments (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  owner_id UUID NOT NULL REFERENCES actors(id),
  goal_id UUID REFERENCES goals(id),  -- optional parent
  description TEXT NOT NULL,
  state TEXT NOT NULL,  -- 'proposed' | 'active' | 'blocked' | 'doneunverified' | 'doneverified' | 'closed'
  due_at TIMESTAMPTZ,
  ambition_level TEXT NOT NULL,  -- 'base' | 'stretch' | 'aspirational'
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE decisions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  title TEXT NOT NULL,
  made_at TIMESTAMPTZ NOT NULL,
  maker_id UUID NOT NULL REFERENCES actors(id),
  options_considered JSONB,  -- [{label, rationale, confidence}]
  chosen_option TEXT NOT NULL,
  state TEXT NOT NULL,  -- 'drafted' | 'active' | 'revisited' | 'archived'
  created_at TIMESTAMPTZ DEFAULT now()
);
```

#### **S4 — Resources**
Organizational assets with utilization and transaction history.

```sql
CREATE TABLE resources (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  kind TEXT NOT NULL,  -- 'financial' | 'ip' | 'relational' | 'capacity' | 'infrastructure' | 'regulatory'
  owner_id UUID REFERENCES actors(id),
  description TEXT NOT NULL,
  utilization_state TEXT NOT NULL,  -- 'available' | 'deployed' | 'committed' | 'depleted' | 'expired'
  controllability TEXT NOT NULL,  -- 'owned' | 'joint' | 'borrowed' | 'leased' | 'limited'
  temporal_character TEXT NOT NULL,  -- 'permanent' | 'time_limited' | 'renewable' | 'consumable'
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE resource_transactions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  resource_id UUID NOT NULL REFERENCES resources(id),
  type TEXT NOT NULL,  -- 'acquire' | 'deploy' | 'spend' | 'release' | ...
  amount DECIMAL,  -- e.g., dollars spent
  recorded_at TIMESTAMPTZ DEFAULT now(),
  recorded_by_actor_id UUID REFERENCES actors(id)
);
```

#### **S5 — Actors & Identity**
People and agents; cross-channel mapping.

```sql
CREATE TABLE actors (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  type TEXT NOT NULL,  -- 'human_internal' | 'human_external' | 'ai_agent'
  display_name TEXT NOT NULL,
  email TEXT,
  status TEXT DEFAULT 'active',  -- 'active' | 'inactive' | 'departed'
  metadata JSONB,
  specification_id UUID,  -- link to agent spec (Nexus)
  created_at TIMESTAMPTZ DEFAULT now(),
  last_seen_at TIMESTAMPTZ
);

CREATE TABLE actor_identity_mappings (
  actor_id UUID NOT NULL REFERENCES actors(id),
  source_channel TEXT NOT NULL,  -- 'slack' | 'github' | 'email' | ...
  source_actor_ref TEXT NOT NULL,  -- channel-specific ref (username, email, etc.)
  confidence FLOAT DEFAULT 1.0,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (source_channel, source_actor_ref)
);
```

#### **Supporting Tables**

- **`think_trigger_queue`** (0004_think_trigger_queue.sql)
  - `id, trigger_kind, payload (JSON), enqueued_at, processing_started_at, completed_at, attempt_count, last_error`
  - Polled by Think Worker via FOR UPDATE SKIP LOCKED

- **`view_ceo_cache`** (CONTRACTS §3)
  - `tenant_id, cache_key, cached_content (JSONB), cached_at, recomputed_reason`
  - Cache keys: 'greeting', 'cards', 'query_grid', 'status', 'close_line', 'query_prefetch:<query_id>'

- **`view_render_costs`** (cost attribution)
  - `id, tenant_id, render_kind, model_used, tokens_used, cost_usd, latency_ms, recorded_at`

- **`signal_memory_fabric`** (Wave 4-C)
  - Links Models to contextually-related Models (relationship maintenance)

- **`anomalies`** (Wave 4-B)
  - Flagged outliers; used by anomaly processor to enqueue T3 triggers

- **`entity_aliases`** and **`entity_review_queue`**
  - Entity resolution worker uses these (Wave 2-B, not fully deployed)

---

## 6. External Dependencies

From `pyproject.toml:12-24`:

| Dependency | Version | Purpose |
|---|---|---|
| **asyncpg** | >=0.29 | Async PostgreSQL driver; used throughout for DB access |
| **pgvector** | >=0.3 | Python bindings for pgvector; vector type support |
| **pydantic** | >=2.7 | Data validation; all *Row and *Request/Response models |
| **fastapi** | >=0.110 | Web framework; async routing, dependency injection |
| **uvicorn[standard]** | >=0.29 | ASGI server; runs the gateway on :8000 |
| **httpx** | >=0.27 | Async HTTP client; calls LLM APIs, rendering service |
| **structlog** | >=24.1 | Structured logging; JSON context binding |
| **python-dotenv** | >=1.0 | .env file loading at startup |
| **psycopg2-binary** | >=2.9 | Sync PostgreSQL driver (used only by scripts, not main app) |
| **anthropic** | >=0.34 | Claude API client (fallback LLM provider) |
| **openai** | >=1.40 | OpenAI API client (fallback LLM provider) |

### **Development Dependencies** (`pyproject.toml:26-38`):
- **pytest**, **pytest-asyncio**, **pytest-timeout** — test runners
- **hypothesis** — property-based testing (Wave 4-C precipitation)
- **respx** — httpx mocking for unit tests
- **freezegun** — datetime mocking
- **hdbscan**, **scikit-learn**, **numpy** — clustering for precipitation module
- **pyyaml** — YAML scenario loader

---

## 7. Configuration & Environment

### **.env.example** (`/Users/rachinkalakheti/fyraliscore/.env.example`)

| Variable | Purpose | Example |
|---|---|---|
| `DATABASE_URL` | Postgres connection | `postgresql://company_os:company_os@localhost:5432/company_os` |
| `OLLAMA_URL` | Embeddings service | `http://localhost:11434` |
| `OLLAMA_EMBED_MODEL` | Embedding model name | `nomic-embed-text` |
| `LLM_PROVIDER` | Active provider | `anthropic` \| `openai` |
| `LLM_API_KEY` | Provider secret | (from env, not in .env) |
| `LLM_MODEL` | Model name | `claude-opus-4-7` |
| `LLM_TIMEOUT_SECONDS` | Call timeout | 30 |
| `NEXUS_URL` | Agent attestation (Phase 4) | `http://localhost:8090` |
| `DEFAULT_TENANT_ID` | Fallback tenant (dev only) | UUID |
| `LOG_LEVEL` | structlog level | `INFO` |
| `SLACK_SIGNING_SECRET` | Slack webhook verification | (from Slack app settings) |
| `AUTH_BOOTSTRAP_SECRET` | Bearer token verification | (optional; if unset, no auth) |
| `GATEWAY_OWNS_POOL` | Gateway manages DB pool lifetime | `0` (tests inject) / `1` (long-running) |

### **.env.dogfood** (`/Users/rachinkalakheti/fyraliscore/.env.dogfood`)

Overrides for the single-gateway dogfood topology:

| Variable | Value | Purpose |
|---|---|---|
| `COMPANY_OS_ENV` | `dev` | Enables synthetic-bypass guard, sim mount |
| `COMPANY_OS_TENANT_ID` | fixed UUID | Single tenant for dogfood |
| `COMPANY_OS_CEO_ACTOR_ID` | fixed UUID | Rachin's actor ID |
| `GATEWAY_PORT` | 8000 | Main gateway port |
| `GATEWAY_OWNS_POOL` | 1 | Gateway manages DB pool |
| `GATEWAY_START_GRT_SCHEDULER` | 1 | Start greeting scheduler at boot |
| `GATEWAY_MOUNT_SIM` | 1 | Mount `/simulation/*` routes |
| `THINK_WORKER_POLL_INTERVAL_S` | 2 | Rapid polling in dogfood |
| `GREETING_REFRESH_INTERVAL_SECONDS` | 900 | Refresh every 15 min |
| `GRT_RENDERING_BASE_URL` | `http://127.0.0.1:8000` | Self-call for RND in single-gateway |
| `DEBUG_ARTIFACT_CAPTURE` | 1 | Enable think_run_artifacts logging |

---

## 8. Tests

### **Structure** (`tests/` directory):

```
tests/
  unit/              # Fast unit tests (no DB, no LLM)
  integration/       # Require live Postgres + Ollama
    test_*.py
    captures/        # Saved home payloads for regression
    real_llm/        # Require DEEPSEEK_API_KEY, RUN_REAL_LLM=1
  e2e/               # Playwright-based browser tests (mock server)
```

### **Markers** (from `pyproject.toml:66-71`):
- **`@pytest.mark.integration`** — requires live Postgres (check `DATABASE_URL`)
- **`@pytest.mark.ollama`** — requires live Ollama (check `OLLAMA_URL`)
- **`@pytest.mark.slow`** — slow test (> 1s)
- **`@pytest.mark.real_llm`** — requires DEEPSEEK_API_KEY; runs only with `RUN_REAL_LLM=1` or `-m real_llm`

### **Key Test Files**:

- **tests/integration/test_ceo_view_smoke.py** — smoke test of full `/view/ceo/home` assembly
- **tests/integration/test_section7_full_scenarios.py** — end-to-end scenario replays (acme_tuesday, two_fires)
- **tests/integration/test_card_reasoning_live.py** — card reasoning endpoint with real LLM
- **tests/real_llm/** — real-LLM tests (gated by env flag)
- **tests/e2e/** — Playwright tests with mock server (can run without Postgres)

### **Test Conventions**:
- Fixtures: `@pytest_asyncio.fixture` for async setup; use `respx` for HTTP mocking
- Database: tests use a real Postgres connection (required by design; see BUILD-PLAN §0)
- Time: `freezegun` to mock `datetime.now()` for deterministic replay

---

## 9. UI — Tech Stack & Pages

### **Pages** (routes in `ui/src/App.tsx`):
- **/** — CEO home view (main surface)
  - Greeting (top)
  - Query grid (chips)
  - Cards (observations, decisions, questions)
  - Close line (metrics)
  - Conversation turns (from Ask/Answer)
  - Ground input (CEO Ask field)

### **WebSocket Connection** (`useHome()` hook):
- **`WS /view/ceo/stream`** — subscribe to real-time updates
- Receives messages: `{type: 'greeting_updated'}`, `{type: 'cards_updated'}`, `{type: 'query_grid_updated'}`, `{type: 'status_updated'}`
- Heartbeat every 30s to detect disconnection

### **Styling**:
- **Tailwind CSS** for utility classes
- **Custom CSS variables**: serif font, highlight tint, citation styling (`.cite`, `.note`)
- Inline spans from backend prose:
  - `.serif` — serif-emphasized phrase
  - `.hl` — highlight tint
  - `.cite` — evidence citation ("Alice — Sun 03:12")
  - `.note` — secondary/parenthetical

### **Keyboard Shortcuts**:
- **`/`** — focus ground input
- **`Esc`** — close expanded card

### **Error States**:
- Missing cache keys → render partial with staleness warning
- Offline mode → show cached state + "offline" indicator
- Rendering failure → fallback prose + mock reasoning

---

## 10. Cross-Cutting Concerns

### **Authentication & Authorization**

- **Gateway-level** (`services/gateway/auth.py`):
  - Bearer token validation via `validate_token(token)` → `(actor_id, tenant_id)`
  - Simple static token map in dev (`DEV_BEARER_TOKEN=dogfood-ceo-token`)
  - Real auth (OAuth2, SAML, etc.) deferred to Wave 5

- **Per-service** (`services/access_control/`):
  - Stub layer for future RBAC / field-level ACL
  - Currently: all authenticated users can read all tenant data

### **Logging & Observability**

- **structlog** with JSON output:
  - Every request bound with `request_id`, `tenant_id`, `actor_id`
  - Service logs: e.g., `"ingest_signal", actor_handle="alice", signal_kind="slack_message"`
  - Metrics: via `services/think/observability.py` (latency, cost, token counts)

- **Cost Attribution**:
  - Every LLM call logs to `view_render_costs`: `{render_kind, model_used, tokens_used, cost_usd, latency_ms}`
  - Used for budget tracking and cost allocation

- **Logs Output**:
  - **`/tmp/company_os_logs/{gateway,think_worker,post_commit_worker,ui}.log`** (dogfood)
  - Structured JSON lines; can be ingested into Datadog/Splunk

### **Error Handling**

- **Custom Exception Hierarchy** (`lib/shared/errors.py`):
  - `CompanyOSError` (base)
  - `ValidationError` (Pydantic-alike)
  - `RetrievalError`
  - `PayloadTooLarge`
  - `SlackSignatureError`
  - etc.

- **Error Responses**:
  - HTTP 4xx for client errors (bad request, auth failure, rate limit)
  - HTTP 5xx for server errors (should not happen in normal operation)
  - JSON error body: `{error: "code", detail: "message"}`

### **Rate Limiting**

- **Token-bucket per (tenant, actor)** (`services/gateway/rate_limit.py`):
  - Configurable bucket size and refill rate
  - Enumerated tiers: `free`, `standard`, `premium`
  - Dogfood: single actor, unlimited tier

### **Data Validation**

- **Pydantic v2** for all wire protocols:
  - `strict=False` (coerce types from DB driver)
  - `extra="forbid"` (reject unknown fields early)
  - Field validators for domain constraints (e.g., confidence 0.05–0.95)

---

## 11. Workflows & Lifecycles

### **End-to-End Scenario: The Acme Renewal Risk**

Drawn from `simulation/scenarios/acme_tuesday.yaml:1-38`:

**Timeline** (7 days, ending Tuesday morning):

1. **Tuesday -7 days, 15:30** (`T1` trigger)
   - Tomas (AE) posts in Slack: "Acme calling with renewal decision on the 22nd"
   - **Ingest**: source_actor_ref="tomas", channel="slack:sales", occurred_at=-7d
   - **Observe**: ObservationRow inserted; embedding computed
   - **Think T1**: primary_retrieve() finds Models about customer relationships, revenue commitments
   - **Reason**: LLM generates Model: "Acme renewal decision is looming (confidence 0.88)"
   - **Apply**: Insert new Model; update activation on customer_relationship Models

2. **Wednesday -6d, 09:15** (`T1` trigger)
   - Marcus (Head of Eng) posts: "scaling bottleneck in payment service identified"
   - **Ingest**: channel="slack:eng"
   - **Think T1**: retrieves Models about scaling, payment service ownership, recent PRs
   - **Reason**: LLM generates: "Infrastructure health is degrading (confidence 0.72)"
   - **Apply**: Insert Model; enqueue Goal check

3. **Friday -4d, 14:20** → **Sunday -2d, 11:45** (multiple T1 triggers)
   - Alice ships rate limiter fix (#2311)
   - David (CFO) flags budget overspend
   - Nora reports payment service still unstable
   - Priya (CS) notes Acme is concerned about reliability
   - **Each**: separate ingest + T1 cycle
   - **Observations** accumulate: 7 events total
   - **Models**: confidence scores update as new observations arrive
     - "Payment service scaling is risky" (0.68 → 0.81)
     - "Acme is at risk due to reliability concerns" (0.55 → 0.89)

4. **Monday -1d, 09:00** (Manual CEO refresh or scheduled greeting)
   - **Greeting scheduler** wakes up every 15 min
   - **Build snapshot**:
     - Top models: "Acme at risk", "payment scaling", "Alice velocity high"
     - Recent state changes: all 7 observations
     - Active commitments: payment fix (due Monday), Acme followup (due Tuesday)
     - Anomalies: none (within expected variance)
   - **Render greeting**: RND calls DeepSeek-chat
     - Input: "Here are the key Models and recent signals. Write a concise greeting for the CEO."
     - Output: "Your Acme renewal is at risk due to payment service reliability concerns. Alice shipped the fix Monday morning; we're stabilizing. Decision call is Tuesday morning."
   - **Cache**: store in `view_ceo_cache` with cache_key='greeting'
   - **WebSocket**: push to all connected UIs

5. **Tuesday 08:00** (CEO asks a question)
   - CEO types in GroundInput: "what's our position on the Acme renewal?"
   - **Ask endpoint**: `POST /view/ceo/ask` with `{query: "what's our position on the Acme renewal?"}`
   - **Classify**: strategic query
   - **Retrieve**: second_pass context; re-rank Models with "Acme" in scope
   - **Render**: RND calls DeepSeek-chat with Models, Commitments, Decisions about Acme
   - **Response**: "Acme decision is this morning at 10 AM. As of Monday morning, payment infrastructure passed Alice's fix and is stabilizing. Sales (Tomas) is prepped with talking points. Priya's team monitors stability through the call."
   - **UI**: renders ConversationTurn with verbs: 'save', 'followup', 'done'

6. **Tuesday 10:00** (Decision recorded)
   - Rachin records in Slack: "Acme renewed; decided on 3-year term"
   - **Ingest**: channel="slack:ceo"
   - **Think T1**: Models about Acme relationship, revenue, trust → Decision: "Acme renewal confirmed (3-year term)"
   - **Apply**: Insert Decision; update related Models' confidence upward
   - **Greeting refresh**: next scheduled refresh (15-min interval) includes "Acme renewed" in greeting

---

### **Background Workflow: Model Decay & Calibration**

(Runs continuously in workers, not event-driven)

1. **Calibration updater** (background job):
   - Daily: query Models with `status='active'` and `created_at > 30 days ago`
   - For each: compute decay penalty based on age, lack of recent supporting evidence
   - Update `confidence` downward if no new observations confirm the Model
   - Mark as 'archived' with reason='decay' if confidence < 0.05

2. **Relationship maintenance** (Wave 4-C):
   - Periodic job: identify Models with shared scope_actors or scope_entities
   - Insert rows in `signal_memory_fabric` linking related Models
   - Used by Pathway D (pattern discovery) to find emerging themes

3. **Anomaly processor** (Wave 4-B):
   - Monitor incoming observations for outliers (HDBSCAN clustering)
   - Outliers → queue T3 triggers to Think
   - Think generates "Anomaly: unusual payment spike" Models

---

## 12. Summary: Key Insights for New Engineers

1. **Four Foundations Philosophy**: Observations (signals), Models (beliefs), Acts (declarations), Resources (assets) are epistemologically distinct. This structure enables:
   - Append-only audit trails (Observations)
   - Evolving confidence scores (Models can be updated, archived, disputed)
   - Executable commitments (Acts with state machines and deadlines)
   - Resource tracking (clear attribution of what's deployed vs. available)

2. **Universal Flow Rule**: Every signal → Observation → Think pass → (Models always, Acts/Resources sometimes). This is the core cognitive loop.

3. **Retrieval is Critical**: Four pathways (structural, semantic, temporal, pattern) ensure context diversity. Think receives the best-ranked Models from all paths merged and scored.

4. **Rendering is Separate from Reasoning**: Think (DeepSeek-reasoner, complex reasoning) ≠ Rendering (DeepSeek-chat, prose generation). This separation allows:
   - Fast iteration on voice/tone (Rendering)
   - Reuse of reasoning across multiple output formats
   - Cost optimization (reasoning is expensive, rendering is cheaper)

5. **Caching Strategy**: CEO view is cached (`view_ceo_cache` table) and refreshed on a schedule, not regenerated per request. This handles:
   - High latency of LLM calls (greeting rendering takes 2-5s)
   - Consistent experience (all users see the same greeting)
   - Cost control (one render per 15 min, not per user per second)

6. **Single-Tenant Dogfood**: The system is architecturally multi-tenant but currently deployed as single-tenant. All endpoints carry `tenant_id` for future scaling.

7. **Synthetic Data Guard**: `COMPANY_OS_ENV` env var gates the `services/synthetic` module (simulation injection). Prevents accidental data pollution in production.

8. **Deterministic Retrieval**: Retrieval pathways are designed to be deterministic given the same snapshot of the database. This enables:
   - Reproducible Think outcomes
   - Testable reasoning pipelines
   - Hypothesis-driven debugging

---

This concludes the comprehensive architectural analysis. The codebase is well-structured for its phase (MVP/dogfood), with clear separation of concerns, extensive type safety (Pydantic), and intentional deferral of production concerns (auth, multi-tenancy UI, observability ingestion) to later waves.