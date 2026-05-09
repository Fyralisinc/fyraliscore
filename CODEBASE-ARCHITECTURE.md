# **Company OS — Comprehensive Architectural Analysis**

## Executive Summary

**Company OS** is an organizational intelligence runtime designed to surface real-time insights to a founder/CEO by combining continuous signal ingestion, probabilistic reasoning (Models), executable commitments (Acts), and resource tracking (Resources). The system is multi-tenant capable and is currently deployed in two modes: a single-tenant dogfood and a public multi-tenant **demo** environment (`demo.fyralis.xyz`) running under Docker Compose with Nginx + Let's Encrypt. It consists of:

- A **Python FastAPI gateway** ([services/gateway/main.py](services/gateway/main.py), port 8000) that coordinates all backend services
- A **React/Vite UI** ([ui/src/App.tsx](ui/src/App.tsx)) running on port 5173, organized as a multi-surface cockpit (`/` Today, `/structure`, `/history`, `/mind`, `/demo`)
- A **PostgreSQL 16** database with pgvector (vector search)
- **Ollama** for local embeddings (`nomic-embed-text:v1.5`)
- A **multi-provider LLM stack** (default **Anthropic `claude-opus-4-7`**, plus OpenAI and DeepSeek) for Think reasoning and Rendering
- A **simulation harness** for authoring test scenarios, plus an in-UI **Signal Simulator** for multi-channel injection
- A **demo company** subsystem (Pelago, Truss, Northwind, Meridian) with per-tenant model routing, budget caps, and live signal-injection sessions
- An **LSOB benchmark suite** (Longitudinal Synthetic-Organization Benchmark) for evaluating reasoning quality
- A **V1 substrate** track (audit chain, reconciliation, region locks, falsifier predicates) tracked in [services/think/SUBSTRATE_SEMANTICS.md](services/think/SUBSTRATE_SEMANTICS.md) and [V1_PR_PROMPTS.md](V1_PR_PROMPTS.md)

The core workflow is: **Ingest signal → Create Observation → Trigger Think → Generate Models / Acts / Recommendations → Cache & Render → Display to CEO across Today / History / Structure surfaces**

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
│  │ Multi-surface cockpit (react-router):                        │  │
│  │   /          Today  — recommendations, cards, ground input   │  │
│  │   /structure        — relationship graph, resources, commits │  │
│  │   /history          — chronicle, arcs, predictions           │  │
│  │   /mind             — loops, notes, reminders                │  │
│  │   /demo             — DemoPicker → DemoLanding (VC pitch)    │  │
│  │ Streams: WS /view/ceo/stream + SSE /v1/recommendations/stream│  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                            ↕ HTTP / WS / SSE
┌─────────────────────────────────────────────────────────────────────┐
│                 Gateway (FastAPI, :8000)                            │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐     │
│  │ /ingest/*    │ /view/ceo/*  │ /rendering/* │ /simulation/ │     │
│  │ /v1/today    │ /v1/history  │ /v1/structure│ /v1/demo/*   │     │
│  │ /v1/recommendations/*  /v1/cards/{id}/conversation         │     │
│  └──────────────┴──────────────┴──────────────┴──────────────┘     │
│                            ↓                                        │
│  ┌─────────────────────────────────────────────────────────────┐  │
│  │ Service Layer (in-process)                                  │  │
│  │  • Ingestion (handlers, core.ingest)                        │  │
│  │  • Greeting (cache, scheduler, snapshot, rendering_adapter) │  │
│  │  • Query (ask/answer, prefetch, strategies)                 │  │
│  │  • Rendering (voice rules, prompts)                         │  │
│  │  • Think (reason, applier, validator, audit, reconciler,    │  │
│  │           cascade, region_locks)                            │  │
│  │  • Retrieval (primary, second-pass, maintenance, pathways)  │  │
│  │  • Recommendations (handlers, watchers, repo)               │  │
│  │  • Conversations (per-card probe / dialogue)                │  │
│  │  • Today / History (UI aggregators)                         │  │
│  │  • Demo (sessions, budget, model routing, simulator, sse)   │  │
│  │  • Models, Observations, Acts, Resources repos              │  │
│  │  • Entity aliases, Actors                                   │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                            ↓ asyncpg                                │
└─────────────────────────────────────────────────────────────────────┘
                            ↕
┌──────────────────────────────────────────────────────────────────────┐
│           PostgreSQL 16 (pgvector/pg16)                              │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐      │
│  │ Observations │ Models       │ Acts         │ Resources    │      │
│  │ (partitioned │ (indexed)    │ (Goals,      │ (tracked)    │      │
│  │  by time)    │              │  Commits,    │              │      │
│  │              │              │  Decisions)  │              │      │
│  │ + Actors     │ + Audit chain│ + Triggers   │ + Xacts      │      │
│  │ + Cache      │ + Watchers   │ + Queue      │              │      │
│  │ + Recs       │ + Reconcile  │ + Artifacts  │ + Demo cfg   │      │
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
        │ LLM Providers (pluggable)           │
        │ • Anthropic claude-opus-4-7 (default)│
        │ • OpenAI gpt-4o                      │
        │ • DeepSeek (reasoner / chat)         │
        │ Per-tenant routing in demo mode      │
        └─────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────┐
│ Background Workers (separate processes, polling Postgres)           │
│  • Think Worker: drains think_trigger_queue (T1-T4 + T6 triggers)  │
│  • Post-Commit Worker: persists model state changes                 │
│  • Greeting Scheduler: refreshes CEO view cache every 15 min       │
│  • Recommendation watchers: falsifier predicate firing              │
│  • Edge Drift Detector (S1): samples Models per tenant every       │
│    30 min, verifies legacy arrays match typed model_edges; logs    │
│    a metric per drifted kind so dual-write violations are visible  │
│  • Topology Updater (S2): drains topo_dirty_queue every 60s;        │
│    recomputes Model topo_embedding via the alpha-anchored rule;     │
│    propagates significant deltas to neighbors with damping (γ=0.5)  │
│  • Neighborhood Detector (S2/S3): hourly; runs connected-components │
│    on the active edge graph per tenant, matches communities to      │
│    prior neighborhoods for stable IDs, refreshes membership table.  │
│    S3: detects emergence/dissolution/split/merge/drift phase events,│
│    writes them to topology_events, enqueues a T6 trigger per event  │
│  • (Future) Entity Resolver: resolves actor/entity refs             │
│  • (Future) Anomaly Processor: detects outliers                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Production Deployment (demo.fyralis.xyz, AWS Lightsail)             │
│  docker-compose.yml services:                                       │
│   postgres (pgvector/pg16) │ ollama │ gateway │ think_worker        │
│   post_commit_worker       │ ui (nginx) │ nginx-proxy │ acme-companion │
│  TLS via Let's Encrypt (acme-companion); SPA cache in nginx-ui.conf │
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
     - Pass observation + retrieved Models to the configured Think model (default `claude-opus-4-7`)
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
     - RND calls the configured rendering model with prose prompt: "Write a concise greeting for the CEO mentioning these situation Models..."
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
| `lib/llm` | LLM provider abstraction | `provider.py` — pluggable backend; **default Anthropic `claude-opus-4-7`**, plus OpenAI and DeepSeek (OpenAI-API-compatible w/ strict tool mode); per-tenant override via demo `model_routing`; cost ledger per model |
| `lib/embeddings` | Vector embedding service | `ollama.py:1-80` — wraps Ollama HTTP; `tests/test_ollama.py` |
| `lib/nexus` | Agent attestation stub | `client.py` — Phase 4 integration point (currently mock) |
| `lib/shared` | Shared types, DB, errors | `types.py:1-150` — Pydantic models for all rows; `db.py` — asyncpg helpers; `errors.py` — domain exceptions; `ids.py` — UUID7 generation; `trust.py` — trust tier logic; `edge_registry.py` — declarative per-kind semantics for the Model-to-Model `model_edges` graph (S1) |
| `lib/topology` | Positional-embedding math (S2/S3/S4) | `embeddings.py` — `content_anchor` (768→128 random projection), `compute_topo_embedding` (alpha-anchored neighbor-mean rule), `delta_magnitude`. Tunables: `TOPO_ALPHA` (0.3), `TOPO_DELTA_EPSILON` (0.05), `TOPO_DAMPING_GAMMA` (0.5). `community.py` — connected-components detection, greedy stable-ID matching by Jaccard, density + centrality. **S3:** `naming.py` — heuristic `derive_signature(member_summaries)` produces a short human-readable label for a neighborhood (kinds + top scope entities/actors), stable for the same member set; `member_summaries_from_rows` adapts asyncpg records into the namer's input shape. **S4:** `relocate.py` — `RelocateTarget`, `parse_relocate_target` (claim_op shape validation), `blend_topo(current, target, alpha)` (L2-normalized blend), `select_bounded_neighbors` (top-K by centrality), `damped_magnitude` (γ^depth · base). All bounded-cascade tunables env-overridable via `TOPO_RELOCATE_*` |

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
  - `repo.py` — `ModelsRepo` class; methods: `insert(ModelCreate)`, `retrieve(ids, conn)` (activation bump), `get_by_id(id)`, `list_active_by_tenant(tenant_id)`, `update_status(..., reason)`. Also exports `_set_model_relations(...)` — the dual-write chokepoint that keeps `model_edges` in sync with the legacy array columns (S1).
  - `edges_repo.py` — `EdgesRepo` for the unified Model-to-Model edge primitive (S1, migration 0030). Single writer for `model_edges`. Public methods: `link`, `unlink`, `traverse_forward`, `traverse_backward` (new bidirectional capability), `mark_inert`, `check_no_cycle`, `get_drift_sample`.
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
    supporting_model_ids: list[UUID]  # legacy: ids that support this Model
    contributing_models: list[UUID]   # legacy: ids that resolve this prediction
    status: ModelStatus  # 'active' | 'archived' | 'superseded' | 'contested_false'
    evaluate_at: datetime | None  # for predictions
  ```
- **DB Table**: `models` (indexed on tenant_id, status, confidence, activation)
- **Model-to-Model graph (S1, migration 0031)**: the seven pre-S1 connection mechanisms (two array columns, pattern back-link, supersession lifecycle flag, transient queue, two latent proposition-encoded edges) are unified into a single typed-edge primitive `model_edges` with a declarative registry at [lib/shared/edge_registry.py](lib/shared/edge_registry.py). Six edge_kinds in v1: `supports`, `contributes_to_resolution`, `instance_of`, `superseded_by` (enabled producers), plus `contradicts` and `weakens` (reserved names). The registry owns per-kind invariants (DAG cycle scope, weight rules, archive cascade callbacks, mutually-exclusive-with). Dual-write phase: arrays remain authoritative; the chokepoint helper `_set_model_relations` writes both sides; the [edge_drift](services/workers/edge_drift/) worker verifies parity. New capability: O(log n) reverse traversal for every kind via `model_edges_target_idx`.
- **Topology layer (S2, migration 0032)**: every Model carries a learned **positional embedding** (`topo_embedding VECTOR(128)`) — distinct from its content embedding (768d, semantic). The position is initialized from `content_anchor()` at insert (synchronous, inside `_insert_core`) and refined continuously by the [topology_updater worker](services/workers/topology_updater/) via the alpha-anchored neighbor-mean rule (α = 0.3 default; `topo(M) = (1-α)·weighted_mean(neighbor_topos) + α·content_anchor(M)`). Edge mutations enqueue both endpoints in `topo_dirty_queue`; archives enqueue every neighbor (`mark_inert` walks the inerted edges and enqueues the other endpoints). The [neighborhood_detector worker](services/workers/neighborhood_detector/) runs hourly community detection (connected-components on the active edge graph in v1; Louvain swap-out is a single function later) and materializes neighborhoods with stable IDs across re-clusterings via greedy Jaccard matching.
- **Topology becomes consequential (S3, migration 0033)**: arrangement now flows into reasoning end-to-end.
  1. **Phase events** — every neighborhood recompute runs `detect_phase_events()` over prev/new community snapshots and writes a `topology_events` row per `emergence`/`dissolution`/`split`/`merge`/`drift`. Naming is heuristic (`lib/topology/naming.py:derive_signature` — top proposition_kinds + top scope entity/actor labels) and lands on both `model_neighborhoods.named_signature` (preserving any LLM-assigned name) and on the event itself.
  2. **T6 trigger** — the neighborhood_detector enqueues a `T6` row in `think_trigger_queue` per fresh phase event in the same transaction (per-kind cap: 10 by default), giving the LLM a chance to name the cluster, surface a recommendation, or no-op. T6 is non-authoritative; it routes through `llm_reason` with weights F=0.5/A=0.3/B=0.2.
  3. **Pathway F** — `pathway_f_topological(seed → topo_embedding HNSW + neighborhood expansion)` lives in [services/retrieval/pathways.py](services/retrieval/pathways.py) and is wired into every existing trigger as a 0.15-weight contribution (T1/T2/T3/T4) plus dominant for T6. When the bundle has Models with neighborhoods, the assembler attaches a `topology_context` summary that the prompt renders inside `<topology_context>`. Empty topology degrades gracefully — F returns `notes['reason']='empty_seed'` and other pathways carry the load.
- **Loop closes — `relocate` claim_op + bounded cascade (S4, migration 0034)**: arrangement is now a first-class diff operation. Reasoning can deliberately reposition a Model in topology space, and that move propagates through the substrate with bounded fan-out so a single relocate doesn't tsunami-cascade.
  1. **`relocate` claim_op** — `ClaimOp(op="relocate", model_id, relocate_target, reason)` where `relocate_target = {"kind": "model_id"|"vector"|"neighborhood_id", "value": <ref>, "alpha": <float in (0,1]>}`. `alpha=1.0` snaps to the target topo; `alpha=0.5` blends halfway. The system prompt in [services/think/prompt.py](services/think/prompt.py) instructs the LLM to use it sparingly — only when reasoning concludes a Model belongs in a different region than the edge graph has placed it.
  2. **TopoRepo.relocate** ([services/topology/topo_repo.py](services/topology/topo_repo.py)) — resolves target into a 128-d topo vector (lookup by model_id, lookup by neighborhood centroid, or use the explicit vector), reads the Model's current topo, calls `lib.topology.relocate.blend_topo(current, target, alpha)`, writes the new topo + `topo_updated_at`, records a `topology_events` row with `kind='relocate'` (magnitude = L2 delta, payload carries audit metadata), and runs `bounded_cascade` if delta > epsilon.
  3. **Bounded cascade** — `TopoRepo.bounded_cascade(origin_model_id, base_delta, max_depth=2, max_fanout=20, damping=0.5)` walks the active edge graph BFS from origin out to `max_depth` hops; at each hop, a single batched query reads neighbors + their centralities; `select_bounded_neighbors` picks the top-K by centrality; each is enqueued in `topo_dirty_queue` with damped magnitude (`base_delta · γ^depth`). De-duplication via the queue's UNIQUE NULLS NOT DISTINCT constraint. Differs from S2's `enqueue_neighbors` (no fan-out cap) — bounded cascade is the one safe for explicit reasoning-driven moves.
  4. **Applier + validator** ([services/think/applier.py](services/think/applier.py), [services/think/validator.py](services/think/validator.py)) — `_apply_claim_op` adds a `relocate` branch that calls `TopoRepo.relocate`; `_validate_claim_op` adds shape validation via `parse_relocate_target` (UUID parsing, dim checks, alpha range). A relocate emits `state_changes=0` (no Model row mutation) — the `topology_events` row is the audit primary key.
  5. **`lib/topology/relocate.py`** — pure helpers: `RelocateTarget` dataclass; `parse_relocate_target` (claim_op → typed target with validation); `blend_topo(current, target, alpha)` (L2-normalized blend); `select_bounded_neighbors(candidates, max_fanout)` (top-K by centrality); `damped_magnitude(base, hop_depth, gamma)` (geometric damping). All env-tunable: `TOPO_RELOCATE_DEFAULT_ALPHA` (1.0), `TOPO_RELOCATE_CASCADE_MAX_DEPTH` (2), `TOPO_RELOCATE_CASCADE_MAX_FANOUT` (20), `TOPO_RELOCATE_CASCADE_DAMPING` (0.5).

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

#### **services/topology** ([services/topology/](services/topology/)) — S2/S3
- **Purpose**: Positional embedding layer + materialized neighborhoods + phase-event log. The substrate's emergent geometry, distinct from the relational store ([services/models/](services/models/)).
- **Key Files**:
  - `topo_repo.py` — `TopoRepo`. Methods: `set_initial_topo(content_emb)` — synchronous content_anchor at insert; `enqueue / enqueue_neighbors / dequeue_pending / mark_processed` — `topo_dirty_queue` mechanics; `recompute_topo(model_id)` — runs the alpha-anchored update rule for one Model (reads all active neighbors via any edge_kind, weights them per `_TOPO_EDGE_WEIGHTS`, blends with `content_anchor`, writes back). Callers always pass `conn` so writes participate in their transaction.
  - `neighborhoods_repo.py` — `NeighborhoodsRepo.recompute_for_tenant(conn, tenant_id)` — single-pass detection: load Models + edges, run `detect_communities`, prune singletons, match to existing neighborhoods for stable IDs, upsert + dissolve unmatched, refresh membership table. **S3:** also reads each Model's `proposition_kind` + `scope_actors` + `scope_entities` to build a `MemberSummary` map, computes `derive_signature()` for each new/matched neighborhood (writes `named_signature` only when previously NULL — LLM-assigned names are preserved), and at the end runs `detect_phase_events()` against the prev/new snapshot to emit emergence/dissolution/split/merge/drift rows into `topology_events`. `RecomputeReport` carries `phase_events_emitted` + `phase_event_ids`.
  - `events_repo.py` (S3) — `TopologyEventsRepo` for `topology_events` (record / list_recent / pending / mark_processed / for_neighborhood) plus the pure detector `detect_phase_events(prev_snapshots, new_communities, label_to_neighborhood_id, matched_prev_ids, member_summaries)`. Closed-set kinds: `emergence`, `dissolution`, `split`, `merge`, `drift`. The detector classifies each prev/new pair and computes a per-kind magnitude (size for emergence/merge/dissolution, split-balance `1 - largest_share/total` for split, Jaccard distance for drift). `DRIFT_JACCARD_THRESHOLD=0.4` (env-tunable) gates drift emission.
- **Direction convention**: edges are treated as **undirected** for topology (arrangement is symmetric — if A is positionally near B, B is near A). Edge `weight` flows into `neighbor_weights`; future `contradicts` edges contribute NEGATIVE weight to push topo embeddings apart.
- **Hooks back into models**: ModelsRepo.`_insert_core` calls `set_initial_topo` synchronously so a fresh Model has non-NULL `topo_embedding` at commit; EdgesRepo.`link / unlink / mark_inert` enqueues both endpoints in `topo_dirty_queue` (inline helper, not via `TopoRepo.enqueue`, to avoid a circular import — same SQL).
- **Hook into trigger queue (S3)**: the [neighborhood_detector worker](services/workers/neighborhood_detector/) enqueues a T6 row in `think_trigger_queue` per fresh phase event in the same transaction as the events; per-kind cap (`NEIGHBORHOOD_DETECTOR_T6_LIMIT_PER_KIND=10`) prevents tenant-wide recomputes from drowning the Think queue. Over-the-cap events are still recorded in `topology_events` (CEO view consumes the table, not the queue) but flagged `processed_at=now()` so they don't re-emit on the next sweep.

#### **services/think** ([services/think/](services/think/))
- **Purpose**: Cognitive engine — reasons about signals to generate Models / Acts / Recommendations; enforces V1 substrate semantics (audit chain, reconciliation, region locks)
- **Canonical spec**: [services/think/SUBSTRATE_SEMANTICS.md](services/think/SUBSTRATE_SEMANTICS.md) (V1 baseline)
- **Key Files**:
  - `worker.py` — main loop; polls `think_trigger_queue` with FOR UPDATE SKIP LOCKED; spawns `think()` calls
  - `reason.py` — core reasoning logic; calls LLM with prompt template; parses response into schema
  - `applier.py` — applies LLM output (inserts/updates Models, enqueues Acts); enforces region lock and trigger-id uniqueness
  - `validator.py` — schema validation post-LLM
  - `llm_reason.py` — LLM call wrapper with retries, cost attribution
  - `prompt.py` — prompt engineering; templates for T1/T2/T3/T4
  - `deterministic.py` — unit tests with frozen LLM responses
  - `circuit_breaker.py` — fallback behavior if LLM is unavailable
  - `observability.py` — metrics (latency, cost, errors)
  - `post_commit.py` — post-commit side effects (cache invalidation, notification)
  - **V1 substrate additions**:
    - `audit.py` — Q5 audit chain: `emit_audit_event()`, `get_audit_chain()`, `emit_reconciliation_merge_audit()`; backed by `audit_events` table
    - `region_locks.py` — W3.Q4 advisory-lock region serialization via `pg_advisory_xact_lock`
    - `reconciler.py` — auto-merge / human-review flow for duplicate Models; emits `reconciliation_events`
    - `cascade.py` — cascade operations and error handling for downstream Model updates
    - `auto_create_commitment.py` — automatic commitment creation from Think output
    - `strict_schema.py`, `diff_schema.py` — strict JSON schema validation and diff detection
    - `thresholds.py`, `anomaly_integration.py` — per-tenant anomaly thresholds (P90/P95/P99) feeding T3 triggers
    - `debug_capture.py` — sidecar capture into `think_run_artifacts` (debug UI)
- **Trigger Types** (`ARCHITECTURE-FINAL.md §7`; **S3** adds T6 + threads F into every existing trigger):
  - **T1** — New signal (observation): pathway mix A+B+C+F (weights 0.35/0.35/0.15/0.15)
  - **T2** — Prediction resolution due: pathways A+B+D+F (0.35/0.35/0.15/0.15)
  - **T3** — Anomaly detected: pathways A+B+C+F (0.4/0.25/0.2/0.15)
  - **T4** — Background pattern/model-reeval: pathways D+A+F (0.5/0.35/0.15)
  - **T6 (S3)** — Topology phase event: pathways F+A+B (0.5/0.3/0.2). Enqueued by the [neighborhood_detector worker](services/workers/neighborhood_detector/) when a phase event is recorded in `topology_events`. Non-authoritative (LLM-driven). The trigger payload carries `topology_event_id`, `topology_event_kind` (`emergence`/`dissolution`/`split`/`merge`/`drift`), `neighborhood_id`, `predecessor_neighborhood_ids`, `sibling_neighborhood_ids`, `member_model_ids`, and a generated `seed_natural_text` so Pathway B has something to embed. The LLM may name the cluster (claim_op.update on a representative member) or surface a CEO-facing recommendation.
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
    - **Pathway B** (semantic): vector similarity search over `models.embedding` (768d content)
    - **Pathway C** (temporal): recent Models in time window
    - **Pathway D** (pattern): precipitation patterns + background relationships
    - **Pathway F (S3, topological)**: HNSW cosine NN over `models.topo_embedding` (128d positional) **plus** materialized-neighborhood expansion. Where B asks "what does this signal MEAN", F asks "where does this signal LIVE in the substrate's emergent arrangement". Seed resolution priority: `precomputed_topo_vector` → `seed_model_id` (uses the model's topo + its membership) → `seed_natural_text + embedder` (Ollama embed → `content_anchor` projection). Expansion: for each Model surfaced (NN or seed), look up its active neighborhood via `model_neighborhood_membership` and pull co-members ordered by `centrality DESC`. Scored as `DIMENSION_TOPOLOGICAL` in [scoring.py](services/retrieval/scoring.py); maps to RRF dimension weight in `_merge_and_rank_models_rrf`.
  - `second_pass.py` — re-rank Models with fresh conversational context
  - `maintenance.py` — background relationship updates (links Models to related Models)
  - `scoring.py` — RRF (Reciprocal Rank Fusion) + position decay. **S3:** `DIMENSION_TOPOLOGICAL` added (default weight 0.7), pathway-to-dimension map `{A,B,C,D,F}` complete.
  - `assembler.py` — rebuild complex objects from DB rows. **S3:** `_compute_topology_context(conn, tenant_id, models, seed_model_id, seed_neighborhood_id)` reads `model_neighborhood_membership` for every bundle Model, joins to `model_neighborhoods` for `named_signature`/`density`/status, ranks neighborhoods by intersection-with-bundle DESC, and tail-fetches `topology_events` for the seed neighborhood. Result lands on `ContextBundle.topology_context`; the prompt builder renders a `<topology_context>` section.
  - `config.py` — tuning knobs (top_n=80, decay_base=0.9, etc.). **S3:** `topological_k` (40), `topological_expand_neighborhoods` (true), `topological_max_neighborhood_members` (30).
- **TriggerContext** (`primary.py`):
  ```python
  @dataclass
  class TriggerContext:
    kind: TriggerKind  # T1, T2, T3, T4, T6 (S3)
    observation_id: UUID | None
    model_id: UUID | None
    seed_entity_ids: list[UUID]
    seed_natural_text: str | None
    seed_occurred_at: datetime | None
    scope_actors: list[UUID]
    # S3 — topology trigger payload (T6)
    topology_event_id: UUID | None
    topology_event_kind: str | None  # emergence|dissolution|split|merge|drift
    neighborhood_id: UUID | None
    member_model_ids: list[UUID]
    # S3 — Pathway F seed override
    precomputed_topo_vector: list[float] | None
    topological_k: int = 40
    topological_expand_neighborhoods: bool = True
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
    - `system.py` — system-emitted observations (state changes from Recommendations/Acts handlers)
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

#### **services/gateway** ([services/gateway/](services/gateway/))
- **Purpose**: HTTP entry point; auth, rate limiting, request context; mounts all sub-routers
- **Key Files**:
  - `main.py` — FastAPI app builder; `build_app(pool, actor_repo, alias_repo, embedder, rate_limiter)` factory
  - `auth.py` — `validate_token(token)`, `create_session(body)` helpers
  - `db_bootstrap.py` — pool creation, codec registration, schema validation
  - `logging_config.py` — structlog setup
  - `rate_limit.py` — token-bucket rate limiter; per-(tenant, actor) buckets
- **Middlewares**:
  - `BearerAuthMiddleware` — extracts `Authorization: Bearer <token>` or `X-Tenant-Id` header
  - `RateLimitMiddleware` — enforces per-tenant/actor quotas
  - `RequestContextMiddleware` — binds request_id, tenant_id to structlog context
- **Mounted routers**:
  - Rendering (RND) — `/rendering/*` via `include_router(rnd_router)`
  - Greeting / CEO view (GRT) — `build_ceo_api_router()` + `build_ceo_stream_router()`
  - Query (QRY) — `build_query_router()` (`/view/ceo/ask`, `/turn-action`)
  - **Conversations** — `build_conversations_router()` (`/v1/cards/{id}/conversation`, `/probe`)
  - **Recommendations** — `/v1/recommendations/*` plus `/stream` (SSE)
  - **Today / History / Structure** — `/v1/today`, `/v1/history`, `/v1/structure`
  - **Demo** — `/v1/demo/*` (gated on demo tenant config)
  - Simulation — `/simulation/*` (when `GATEWAY_MOUNT_SIM=1`)
  - Debug router — optional, gated on env

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

#### **services/recommendations** ([services/recommendations/](services/recommendations/))
- **Purpose**: First-class CEO action list — surfaces actionable Model-derived recommendations with archival, watch, and triage flows
- **Key Files**:
  - `repo.py` — `RecommendationsRepo`; backed by `recommendations` table (migration [0022](db/migrations/0022_recommendations.sql))
  - `handlers.py` — wraps Acts/Resources mutation entry points; emits audit-trail `state_change` observations
  - `watchers.py` — per-actor "watch for revision" subscriptions on recommendation cards via falsifier predicates (table `model_watchers`, migration [0027](db/migrations/0027_model_watchers.sql))
- **FastAPI routes** (mounted in gateway):
  - `GET /v1/recommendations` — list for actor
  - `POST /v1/recommendations/{id}/act` — apply proposed change
  - `POST /v1/recommendations/{id}/dismiss` — archive
  - `POST/DELETE /v1/recommendations/{id}/watch` — falsifier subscription
  - `POST /v1/recommendations/{id}/triage` — triage workflow
  - `GET /v1/recommendations/stream` — **SSE** stream of created/updated events

#### **services/conversations** ([services/conversations/](services/conversations/))
- **Purpose**: Per-card conversation persistence — replaces static card detail sections with a probe-driven dialogue per the Driftwood revision
- **Key Files**:
  - `repo.py` — `card_conversations` and `card_exchanges` tables (migration [0024](db/migrations/0024_card_conversations.sql)); session-scoped dialogue state with probed phrases and used chip IDs
  - `handler.py` — probe resolution: phrase/chip templates (cheap path) + free-form Ask routed through `QueryHandler` with card context
  - `api.py` — routes mounted at `/v1/cards/{card_id}/conversation` and `/v1/cards/{card_id}/probe`

#### **services/today** ([services/today/](services/today/))
- **Purpose**: Read-only UI aggregator for the `/` Today page; derives severity/kind/tag/stats from Recommendations + Acts + Resources
- **Key Files**:
  - `aggregator.py` — severity formula (`expected_impact × confidence`) + proposition_kind classification
  - `triage.py` — triage state aggregation
- **Routes**: served by gateway at `GET /v1/today`; no DB tables of its own

#### **services/history** ([services/history/](services/history/))
- **Purpose**: Read-only UI aggregator for the `/history` page; reads observations/models/commitments/decisions chronologically
- **Key Files**:
  - `aggregator.py`
- **Routes**: served by gateway at `GET /v1/history`; no DB tables of its own

#### **services/demo** ([services/demo/](services/demo/))
- **Purpose**: VC-pitch demo tenant infrastructure — multi-company demos (Pelago, Truss, Northwind, Meridian) with budget caps, per-tenant model routing, deterministic seeds, signal-injection sessions
- **Key Files**:
  - `repo.py` — backed by `tenants`, `demo_configs`, `demo_sessions`, `demo_session_costs` (migration [0023](db/migrations/0023_demo_infrastructure.sql))
  - `sessions.py` — session lifecycle
  - `budget.py` — per-session cost caps and enforcement
  - `model_routing.py` — per-tenant LLM provider/model selection (overrides global default)
  - `simulator.py` — signal-injection simulator for live VC walkthroughs
  - `snapshot.py` — pre-baked substrate snapshots (see [demo/snapshots/](demo/snapshots/))
  - `notifications.py`, `sse.py` — SSE plumbing for demo recommendation streams
  - `router.py` — FastAPI routes under `/v1/demo/*`
- **Corpora**: synthetic event streams in [corpora/pelago/](corpora/pelago/) (LSOB simulator + synthesis)

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

- **`db/migrations/0002_*.sql` through `0019_*.sql`** — early waves:
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

- **`db/migrations/0020_*.sql` through `0032_*.sql`** — V1 substrate, demo, audit, and self-organizing-substrate waves:
  - **0020** `think_run_artifacts` — sidecar capture of each Think pipeline stage (trigger, retrieval, prompt, response, validation, apply, post_commit, cascade, error) for the debug UI
  - **0021** Review-1 remediation: `commitments.is_maintenance`, `anomaly_thresholds` (per-tenant P90/P95/P99 rolling stats), `dedup_keys_seen` (publisher debounce ledger)
  - **0022** Recommendations: `recommendations` table; `propositions.target_actor_id` generated column and `caused_act_change_id`; partial index for the CEO action-list ranker
  - **0023** Demo infrastructure: `tenants` registry, `demo_configs` (model routing, cost cap, determinism seed), `demo_sessions`
  - **0024** `card_conversations` — per-card conversation persistence with probed phrases and used chip IDs
  - **0027** `model_watchers` — per-actor "watch for revision" subscriptions on recommendation cards with falsifier predicates
  - **0028** Pelago demo company registration (alongside Truss / Northwind / Meridian)
  - **0029** `reconciliation_events` — audit trail for reconciler decisions (`auto_merge`, `human_review`, `no_match`)
  - **0030** **Audit chain (Q5)**: per-Model `audit_events` table with state transitions, `changed_fields`, re-assertion tracking, and reconciliation-merge provenance
  - **0031** **Unified Model-to-Model edge primitive (S1)**: `model_edges` table — one row per typed edge (source, target, edge_kind, weight, metadata, status, detected_by, lifecycle audit) with 3 partial indexes (forward, backward, by-kind on `status='active'`). Also drops the `model_reeval_queue.cause_kind` CHECK so new edge_kinds can produce new cause_kinds via the registry's cascade callbacks. Dual-write phase: arrays on `models` remain authoritative; the chokepoint helper [_set_model_relations](services/models/repo.py) writes both sides; the [edge_drift worker](services/workers/edge_drift/) verifies parity. Six edge_kinds: `supports`, `contributes_to_resolution`, `instance_of`, `superseded_by` (enabled), `contradicts`, `weakens` (reserved). Per-kind semantics declared in [lib/shared/edge_registry.py](lib/shared/edge_registry.py).
  - **0032** **Topology layer (S2)**: adds `models.topo_embedding VECTOR(128)` and `models.topo_updated_at` (positional embedding maintained by the updater worker); `topo_dirty_queue` (propagation queue with NULLS-NOT-DISTINCT dedup, priority-ordered by delta_magnitude); `model_neighborhoods` (materialized communities with stable IDs across re-clusterings, centroid + density + lifecycle); `model_neighborhood_membership` (reverse lookup with per-Model centrality). HNSW index on `topo_embedding` partial WHERE `status='active' AND topo_embedding IS NOT NULL`, ready for S3's Pathway F. Algorithms in [lib/topology/](lib/topology/) (content_anchor random projection, alpha-anchored update rule, connected-components detection, greedy Jaccard matching). Repos in [services/topology/](services/topology/). Workers in [services/workers/topology_updater](services/workers/topology_updater/) and [services/workers/neighborhood_detector](services/workers/neighborhood_detector/). Topology is **observable but not yet consequential** in S2 — S3 makes it consequential.
  - **0033** **Topology phase events + T6 (S3)**: adds `topology_events` table — a durable log of phase transitions (`emergence`, `dissolution`, `split`, `merge`, `drift`) emitted by the neighborhood detector, with magnitude + heuristic `named_signature` + denormalized member snapshot + processed_at. Three indexes (tenant-recent, pending, by neighborhood). T6 trigger kind needs no schema change (the existing `think_trigger_queue.trigger_kind` is plain TEXT). Companion app changes: phase-event detector + heuristic naming wired into [NeighborhoodsRepo.recompute_for_tenant](services/topology/neighborhoods_repo.py); T6 enqueue in [neighborhood_detector worker](services/workers/neighborhood_detector/worker.py) (per-kind cap, processed-in-same-tx idempotency). Pathway F in [pathways.py](services/retrieval/pathways.py) (HNSW over `topo_embedding` + neighborhood-membership expansion), wired into [primary_retrieve](services/retrieval/primary.py) for T1-T4 (0.15 weight) and T6 (0.5 weight). Prompt in [services/think/prompt.py](services/think/prompt.py) gains a `<topology_context>` section + T6-specific operating instructions. `RetrievalConfig` gains `topological_*` knobs.
  - **0034** **`relocate` claim_op + bounded cascade (S4)**: extends `topology_events.kind` CHECK to include `'relocate'` (the only schema change S4 needs — it sits on top of S2/S3 substrate). Companion app changes: `ClaimOp.op` literal in [services/think/diff_schema.py](services/think/diff_schema.py) gains `"relocate"` plus a `relocate_target: dict` field. Validator + applier ([services/think/validator.py](services/think/validator.py), [services/think/applier.py](services/think/applier.py)) route relocate ops through `TopoRepo.relocate`. New methods on TopoRepo: `relocate(model_id, target, reason)` (resolve target → blend → write → record `topology_events` row with kind='relocate' → bounded cascade) and `bounded_cascade(origin_model_id, base_delta, max_depth=2, max_fanout=20, damping=0.5)` (BFS over active edges, top-K-by-centrality fan-out cap per hop, geometric damping per depth, `topo_dirty_queue` UNIQUE-constraint dedup). New pure helpers in [lib/topology/relocate.py](lib/topology/relocate.py): `RelocateTarget` dataclass, `parse_relocate_target`, `blend_topo`, `select_bounded_neighbors`, `damped_magnitude`. System prompt in [services/think/prompt.py](services/think/prompt.py) documents the `relocate` shape + when to use it. Closes the substrate's reasoning loop: arrangement is now a first-class diff operation equal in standing to content.
  - 0025 / 0026 are intentionally absent (skipped numbers).

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
- **react-router-dom** 6.26.2 (multi-page routing)
- **ws** 8.18.0 (WebSocket client; types via `@types/ws`)
- **Playwright** 1.47.2 (e2e tests)
- **Vitest** 2.1.2 (unit tests)
- **autoprefixer**, **postcss** (CSS pipeline)

**Pages** ([ui/src/pages/](ui/src/pages/)):
- `/` — **Today** (CEO cockpit; default surface)
- `/structure` — **Structure** (relationship graph, resource aggregates, commitments)
- `/history` — **History** (chronicle, arcs, predictions)
- `/mind` — **MyMind** (loops, notes, reminders)
- `/demo` — **DemoPicker** → **DemoLanding** (VC pitch entry)

**Key Components** ([ui/src/components/](ui/src/components/)):
- **App.tsx** — root layout; routes between Today/Structure/History/Mind/Demo via `react-router`; mounts global Sidebar, ShortcutsOverlay, TriageToast, ArtifactDrawer
- **Sidebar.tsx** — left-rail nav across the five surfaces
- **PageHeader.tsx**, **FilterBar.tsx**, **JustUpdated.tsx** — shared chrome
- **RecCard.tsx** — recommendation/card shell (observation, decision, or question kind)
- **CardExpanded.tsx** — expanded card with reasoning, evidence, action verbs
- **AskZone.tsx** (formerly GroundInput) — text input for CEO Ask; `/` shortcut to focus
- **Conversation.tsx**, **ConversationTurn.tsx**, **ThinkingTurn.tsx** — Ask/Answer dialogue rendering
- **EmptyState.tsx**, **RoutedCoda.tsx**
- **components/mind/** — `HoldPicker`, `LoopCard`, `NoteCard`, `ReminderCard`, `MindList`, `FilterPanel`, `MindLayerStrip`, `MindNarrativeBand`, `PromoteModal`
- **components/history/** — `Chronicle`, `Arcs`, `EventPanel`, `Predictions`, `HistoryLayerStrip`, `HistoryNarrativeBand`
- **components/structure/** — `RelationshipGraph`, `ResourceAggregateView`, `CommitmentList`, `MapControls`
- **components/SignalSimulator/** — multi-tab in-UI signal injector (Email, GitHub, Calendar, Slack, Custom, Suggested)

**Hooks** ([ui/src/hooks/](ui/src/hooks/)):
- `useToday()` — fetches `GET /v1/today`; subscribes to `WS /view/ceo/stream` and SSE updates (replaces the legacy `useHome`)
- `useHistory()` — fetches `GET /v1/history`
- `useMind()` — loops/notes/reminders state
- `useConversation()` — per-card probe + dialogue state, posting to `/v1/cards/{id}/conversation` and `/probe`
- `useRecommendationStream()` — **SSE** subscription to `/v1/recommendations/stream` for live recommendation events (used in demo mode)
- `useAsk()` — Ask/Answer pipeline; `ask(query)` → `POST /view/ceo/ask`

**API Layer** ([ui/src/api/](ui/src/api/)):
- `types.ts`, `client.ts` — base contracts and HTTP client with auth-token injection
- `stream.ts` — WebSocket subscription manager (`/view/ceo/stream`)
- `today-types.ts`, `today-client.ts`, `today-mock.ts` — Today payloads
- `history-client.ts`, `structure-client.ts` — page-specific clients
- `recommendation-stream.ts` — **SSE** client for `/v1/recommendations/stream`
- `demo-client.ts`, `demo-picker-client.ts` — demo session lifecycle
- `auth.ts` — bearer / dev-token bootstrap
- `mock-data.ts` — fixtures for offline UI dev

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

- **`recommendations`** (migration 0022)
  - `id, tenant_id, target_actor_id, kind, proposed_change JSONB, expected_impact, confidence, state, created_at, archived_at`
  - Indexed for the CEO action-list ranker via `(tenant_id, target_actor_id, state)` partial index

- **`model_watchers`** (migration 0027)
  - `id, tenant_id, actor_id, model_id, falsifier_predicate JSONB, fired_at, cleared_at`
  - Per-actor "watch for revision" subscriptions tied to recommendation cards

- **`card_conversations`** + **`card_exchanges`** (migration 0024)
  - Per-card dialogue state: `probed_phrases JSONB`, `used_chip_ids UUID[]`
  - Backs `/v1/cards/{id}/conversation` and `/probe`

- **`think_run_artifacts`** (migration 0020)
  - Per-stage sidecar for each Think run (trigger, retrieval, prompt, response, validation, apply, post_commit, cascade, error)
  - Powers the debug UI

- **`anomaly_thresholds`**, **`dedup_keys_seen`** (migration 0021)
  - Per-tenant P90/P95/P99 rolling stats (T3 anomaly trigger gating)
  - Publisher debounce ledger to suppress redundant ingest

- **`reconciliation_events`** (migration 0029)
  - Audit trail for the reconciler: `auto_merge` / `human_review` / `no_match`, with matched-model tracking

- **`audit_events`** (migration 0030, Q5 audit chain)
  - Per-Model state transitions, `changed_fields`, re-assertion tracking, reconciliation-merge provenance
  - Written by `services/think/audit.py`; powers chain replay and contestability views

- **`model_edges`** (migration 0031, S1 unified Model-to-Model edge primitive)
  - `id, tenant_id, source_model_id, target_model_id, edge_kind, weight, metadata JSONB, status, detected_by, created_at, created_by_event_id, status_changed_at, status_reason`
  - Three partial indexes on `status='active'` — forward (source, kind), backward (target, kind, **the new bidirectional capability**), and per-kind scan
  - Replaces seven pre-S1 ad-hoc connection mechanisms (two array columns on `models`, pattern back-link, supersession lifecycle flag, transient queue, two latent proposition-encoded edges) with a single typed-edge table whose semantics are declared in [lib/shared/edge_registry.py](lib/shared/edge_registry.py) (per-kind: directed / symmetric, DAG cycle scope, weight rules, archive cascade callbacks, mutually-exclusive-with)
  - Edge kinds in v1: `supports`, `contributes_to_resolution`, `instance_of`, `superseded_by` (enabled producers); `contradicts`, `weakens` (reserved names — repo refuses to insert until producers ship in later stages)
  - Single writer: [services/models/edges_repo.py](services/models/edges_repo.py) (`EdgesRepo.link / unlink / traverse_forward / traverse_backward / mark_inert / check_no_cycle`)
  - Dual-write chokepoint at [services/models/repo.py](services/models/repo.py) (`_set_model_relations`) keeps the legacy array columns (`supporting_model_ids`, `contributing_models`) in lockstep with `model_edges` rows. Drift detector worker at [services/workers/edge_drift/](services/workers/edge_drift/) samples every tenant on a 30-min cadence and emits a metric per drifted kind

- **`models.topo_embedding`** + **`models.topo_updated_at`** (migration 0032, S2 topology layer columns)
  - `VECTOR(128)`, NULL until first computed, indexed via partial HNSW (`models_topo_embedding_idx`) WHERE `status='active' AND topo_embedding IS NOT NULL`
  - The Model's learned **positional vector**, distinct from the 768d content embedding. Initialized to `content_anchor()` (a fixed random projection of the content embedding, deterministic and L2-normalized) at INSERT inside `_insert_core`; refined continuously by the topology updater worker via the alpha-anchored neighbor-mean rule
  - The HNSW index is in place now so it gets time to populate during the S2 soak window; S3's Pathway F will read it for topological retrieval

- **`topo_dirty_queue`** (migration 0032, S2 propagation queue)
  - `id, tenant_id, model_id, cause_model_id, hop_depth, delta_magnitude, enqueued_at, processed_at, attempts, last_error`
  - Same dedup pattern as `model_reeval_queue`: UNIQUE NULLS NOT DISTINCT on `(tenant, model, processed_at)`. Pending rows ordered by `delta_magnitude DESC NULLS FIRST` so high-priority recomputes preempt routine ones
  - Enqueued by: ModelsRepo.insert (initial topo computed synchronously, then enqueued for refinement); EdgesRepo.link / unlink / mark_inert (every endpoint affected by an edge change); the topology updater itself (when a recompute produces a significant delta, neighbors get enqueued at hop_depth + 1 with damped magnitude)
  - Drained by [services/workers/topology_updater](services/workers/topology_updater/)

- **`model_neighborhoods`** (migration 0032, S2 materialized communities; **S3** populates `named_signature`)
  - `id, tenant_id, centroid_topo_embedding VECTOR(128), member_model_ids UUID[], emergence_at, predecessor_neighborhood_ids UUID[], named_signature, named_at, density, status (active|dissolved|merged), status_changed_at, status_reason, last_recomputed_at`
  - Detected by [services/workers/neighborhood_detector](services/workers/neighborhood_detector/) via connected-components on the active edge graph (Louvain swap-out is a single function later); communities below `MIN_COMMUNITY_SIZE` (2) pruned to drop isolated singletons
  - Stable IDs across re-clusterings: greedy Jaccard matching against existing active neighborhoods, threshold 0.3 (configurable). New communities without a match get a fresh id + emit `predecessor_neighborhood_ids` for any prior overlap. Unmatched previous neighborhoods → `status='dissolved'` with `status_reason='no_match_in_recompute'`
  - **S3:** `named_signature` is now populated heuristically at recompute via [lib/topology/naming.py](lib/topology/naming.py) (top proposition_kinds + top scope entity/actor labels). LLM-assigned names are preserved (`COALESCE(named_signature, $heuristic)` on UPDATE; INSERT always sets it). T6 reasoning may overwrite via a claim_op.

- **`model_neighborhood_membership`** (migration 0032, S2 reverse lookup)
  - `tenant_id, model_id, neighborhood_id, centrality, joined_at`. PK `(model_id, neighborhood_id)`
  - Per-Model centrality (degree centrality in v1, eigenvector / PageRank are stable swap-outs) for use by S3's neighborhood-summary in the LLM prompt
  - Wholesale-refreshed by `recompute_for_tenant` — DELETE all + INSERT new is O(n) and cheaper than per-row diff tracking
  - **S3 consumers:** Pathway F's neighborhood expansion query (top-K members per neighborhood by `centrality DESC`); the assembler's `_compute_topology_context` joins on it to build the prompt's neighborhood summary

- **`topology_events`** (migration 0033, S3 phase-event log; **0034 (S4)** extends `kind` with `'relocate'`)
  - `id, tenant_id, kind (emergence|dissolution|split|merge|drift|relocate), neighborhood_id, predecessor_neighborhood_ids UUID[], sibling_neighborhood_ids UUID[], member_model_ids UUID[], magnitude, named_signature, payload jsonb, occurred_at, processed_at`
  - Three indexes: tenant-recent (CEO view / audit), pending (T6 dispatcher), by-neighborhood (history lookup)
  - Written inside `NeighborhoodsRepo.recompute_for_tenant` in the same transaction as the upsert — atomicity matters: a recompute rollback rolls back the events too. The pure detector lives at [services/topology/events_repo.py](services/topology/events_repo.py): `detect_phase_events()` consumes the (prev neighborhoods, new communities, label-to-id, matched-prev-by-label, member-summaries) tuple and produces `PhaseEvent` rows.
  - Magnitude semantics: `emergence` = new-community size, `dissolution` = old size, `merge` = combined size, `split` = balance `1 - largest_share/total`, `drift` = Jaccard distance from prior membership, **`relocate`** = L2 distance from previous topo to new topo. The relocate kind has `member_model_ids = [model_id]` (singleton) and a `payload` with `target_kind`/`target_ref`/`alpha`/`reason`/`applied_by_diff_id`.
  - T6 enqueue happens in [neighborhood_detector worker](services/workers/neighborhood_detector/worker.py) `_enqueue_t6_for_events`: per-event payload includes event id, kind, neighborhood id, predecessors/siblings, members, generated `seed_natural_text`. Per-kind cap (`NEIGHBORHOOD_DETECTOR_T6_LIMIT_PER_KIND=10`) prevents tenant-wide recomputes from saturating the Think queue. Events past the cap are still recorded (CEO view sees them) but flagged `processed_at=now()` so they don't re-emit.

- **`tenants`**, **`demo_configs`**, **`demo_sessions`**, **`demo_session_costs`** (migration 0023)
  - Multi-tenant registry; per-tenant model routing + cost cap + determinism seed; per-session lifecycle and cost ledger
  - Backs the public demo at `demo.fyralis.xyz` (Pelago, Truss, Northwind, Meridian)

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
| `LLM_PROVIDER` | Active provider (default `anthropic`) | `anthropic` \| `openai` \| `deepseek` |
| `LLM_API_KEY` | Provider secret | (from env, not in .env) |
| `LLM_MODEL` | Model name (default `claude-opus-4-7`) | `claude-opus-4-7` |
| `LLM_TIMEOUT_SECONDS` | Call timeout | 30 |
| `NEXUS_URL` | Agent attestation (Phase 4) | `http://localhost:8090` |
| `DEFAULT_TENANT_ID` | Fallback tenant (dev only) | UUID |
| `LOG_LEVEL` | structlog level | `INFO` |
| `SLACK_SIGNING_SECRET` | Slack webhook verification | (from Slack app settings) |
| `AUTH_BOOTSTRAP_SECRET` | Bearer token verification | (optional; if unset, no auth) |
| `GATEWAY_OWNS_POOL` | Gateway manages DB pool lifetime | `0` (tests inject) / `1` (long-running) |
| `DEBUG_ARTIFACT_CAPTURE` | Write `think_run_artifacts` rows | `1` (dogfood) |

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

### **Demo deployment env**

The public demo (`demo.fyralis.xyz`) layers additional config used by `services/demo` and the per-tenant model-routing path:

| Variable | Purpose |
|---|---|
| `DEMO_DEFAULT_TENANT` | Tenant slug when `/demo` is opened with no selection |
| `DEMO_BUDGET_USD_PER_SESSION` | Per-session cost cap enforced by `services/demo/budget.py` |
| `DEMO_MODEL_ROUTING_*` | Per-tenant LLM provider/model overrides |
| `DEMO_DETERMINISM_SEED` | Optional seed for reproducible demo runs |
| `LETSENCRYPT_HOST`, `VIRTUAL_HOST`, `VIRTUAL_PORT` | Picked up by `nginx-proxy` + `acme-companion` containers |

---

## 7a. Deployment & Infrastructure

The repo ships a full container topology for the public demo at **`demo.fyralis.xyz`** (AWS Lightsail, 4 GB).

### **Docker images**
- [Dockerfile](Dockerfile) — Python 3.11; runs `uvicorn services.gateway.main:app` on `:8000`. Same image is reused for `gateway`, `think_worker`, and `post_commit_worker` (entrypoint overridden).
- [Dockerfile.ui](Dockerfile.ui) — multi-stage Node 20 build → Nginx Alpine static server; cache-busting headers for hashed assets, long-lived cache for static, no-cache for `index.html`.

### **docker-compose.yml** ([docker-compose.yml](docker-compose.yml))
Nine services:

| Service | Image | Role |
|---|---|---|
| `postgres` | `pgvector/pgvector:pg16` | Primary DB; healthcheck-gated |
| `ollama` | `ollama/ollama:latest` | Embeddings; auto-pulls `nomic-embed-text` on boot |
| `gateway` | local `Dockerfile` | FastAPI app on `:8000` |
| `think_worker` | local `Dockerfile` | Runs `scripts/run_think_worker.py` |
| `post_commit_worker` | local `Dockerfile` | Runs `scripts/run_post_commit_worker.py` |
| `ui` | local `Dockerfile.ui` | React SPA served via Nginx |
| `nginx-proxy` | `nginxproxy/nginx-proxy` | Reverse proxy; reads `VIRTUAL_HOST` labels |
| `acme-companion` | `nginxproxy/acme-companion` | Let's Encrypt TLS issuance/renewal |
| (volumes) | — | `pg_data`, `ollama_models`, `acme_certs`, `acme_html`, `nginx_vhost` |

### **Nginx**
- [nginx-ui.conf](nginx-ui.conf) — SPA routing (`try_files → index.html`); 1-year cache for static assets, no-cache for `index.html`.
- [nginx/](nginx/) — vhost-overrides directory mounted into `nginx-proxy` (currently empty placeholder).

### **TLS**
- `acme-companion` issues and renews Let's Encrypt certs for `demo.fyralis.xyz` based on `LETSENCRYPT_HOST` labels on the `ui` and `gateway` services.

### **Demo & corpora artifacts**
- [demo/](demo/) — `generation/` (model generation pipeline), `snapshots/` (pre-baked substrate snapshots loaded for VC pitches), `SPEAKER-NOTES.md`.
- [corpora/](corpora/) — `pelago/` (LSOB-generated event corpus), `pelago.jsonl.zst` (compressed event stream).
- [V1_PR_PROMPTS.md](V1_PR_PROMPTS.md) — sequenced prompt plan for the V1 substrate PRs (PR 0 audit → PR 1 audit chain → PR 2 confidence-as-strength → PR 3 preconditions → PR 4 LLM reconciliation → PR 5 entity hierarchy).
- [truss_run/](truss_run/), [truss_run_2/](truss_run_2/) — adversarial harness execution artifacts (manifest, signals, snapshots, `model_events.jsonl`, `ground_truth.jsonl`, `final_substrate.json`, `summary_stats.json`, `errors.log`).

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

### **Pages** (routes in [ui/src/App.tsx](ui/src/App.tsx) via `react-router`):

| Route | Page | Surface |
|---|---|---|
| `/` | [Today](ui/src/pages/) (default) | CEO cockpit: recommendations, cards, AskZone, conversation turns |
| `/structure` | [Structure](ui/src/pages/Structure.tsx) | Relationship graph, resource aggregates, commitments |
| `/history` | [History](ui/src/pages/History.tsx) | Chronicle, arcs, event panel, predictions |
| `/mind` | [MyMind](ui/src/pages/MyMind.tsx) | Loops, notes, reminders |
| `/demo` | [DemoPicker](ui/src/pages/DemoPicker.tsx) → [DemoLanding](ui/src/pages/DemoLanding.tsx) | VC-pitch demo entry; selects company (Pelago/Truss/Northwind/Meridian), opens session |

The legacy single-page "CEO home" surface has been split into Today (recommendations + ground input), Structure (the org graph), and History (the chronicle).

### **Real-time Streams**:
- **`WS /view/ceo/stream`** — view-cache updates (`greeting_updated`, `cards_updated`, `query_grid_updated`, `status_updated`); 30s heartbeat
- **`SSE /v1/recommendations/stream`** — live recommendation `created` / `updated` events (preferred path in demo mode); managed by `useRecommendationStream()`

### **Styling**:
- **Tailwind CSS** for utility classes
- **Custom CSS variables**: serif font, highlight tint, citation styling (`.cite`, `.note`)
- Inline spans from backend prose:
  - `.serif` — serif-emphasized phrase
  - `.hl` — highlight tint
  - `.cite` — evidence citation ("Alice — Sun 03:12")
  - `.note` — secondary/parenthetical

### **Keyboard Shortcuts**:
- **`/`** — focus AskZone
- **`Esc`** — close expanded card
- **`?`** — toggle ShortcutsOverlay

### **Error States**:
- Missing cache keys → render partial with staleness warning
- Offline mode → show cached state + "offline" indicator
- Rendering failure → fallback prose + mock reasoning
- Demo budget exhausted → banner + read-only fallback

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
   - **Render greeting**: RND calls the rendering model
     - Input: "Here are the key Models and recent signals. Write a concise greeting for the CEO."
     - Output: "Your Acme renewal is at risk due to payment service reliability concerns. Alice shipped the fix Monday morning; we're stabilizing. Decision call is Tuesday morning."
   - **Cache**: store in `view_ceo_cache` with cache_key='greeting'
   - **WebSocket**: push to all connected UIs

5. **Tuesday 08:00** (CEO asks a question)
   - CEO types in GroundInput: "what's our position on the Acme renewal?"
   - **Ask endpoint**: `POST /view/ceo/ask` with `{query: "what's our position on the Acme renewal?"}`
   - **Classify**: strategic query
   - **Retrieve**: second_pass context; re-rank Models with "Acme" in scope
   - **Render**: RND calls the rendering model with Models, Commitments, Decisions about Acme
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

4. **Rendering is Separate from Reasoning**: Think (reasoning model, e.g. `claude-opus-4-7` or `deepseek-reasoner`) ≠ Rendering (chat model, e.g. `claude-sonnet-4-5` or `deepseek-chat`). This separation allows:
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