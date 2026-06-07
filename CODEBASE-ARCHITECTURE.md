# Fyralis Core Architecture

Last reviewed from the codebase on 2026-06-02.

Fyralis Core is an organizational intelligence runtime. It ingests company signals, stores them as tenant-scoped observations, reasons over them into a live model of the organization, and renders the result into CEO-facing product surfaces.

The repository is intentionally a monolith at the source level: the FastAPI gateway, domain services, workers, migrations, simulation tooling, and React UI live together. Operationally, the system is split into a gateway process, a small set of polling/background workers, PostgreSQL with pgvector, Ollama for embeddings, external LLM providers for reasoning/rendering, and a Vite/React frontend.

Since the previous review, the largest architectural addition is the **Sage** subsystem under [services/sage](services/sage): a deterministic, observable read/write feedback loop that intercepts inquiry execution with query-conditioned synthesis (cue extraction, intent inference, activation, structural gating, subgraph selection, evidence projection) and feeds inquiry outcomes into a topology optimizer that learns the Discovery Utility Layer (affordances, shortcuts, negative memory, region summaries) without mutating canonical truth. A separate **Fyralis Benchmarks** harness under [benchmarks](benchmarks) and [tests/benchmarks](tests/benchmarks) measures retrieval/answer quality against public datasets (LongMemEval, HotpotQA, HaluMem) and an in-repo toy memory benchmark.

## 1. System Map

```text
React/Vite UI (:5173 in dev)
  /today, /model, /forecasts, /ledger, /debug
        |
        | HTTP /api/*, WS /stream/*
        v
FastAPI gateway (:8000)
  auth, rate limits, ingest, CEO view, query, rendering,
  demo sessions, today/model/spec routes, history, forecasts,
  recommendations, conversations, debug, simulation
        |
        | asyncpg
        v
PostgreSQL 16 + pgvector
  observations, models, acts, resources, queues, cache,
  audit/reconciliation/topology/demo/prediction tables
        |
        +--> Ollama /api/embeddings (nomic-embed-text, 768 dimensions)
        +--> LLM providers (Anthropic/OpenAI/DeepSeek)

Background execution:
  ThinkWorker              drains think_trigger_queue and model_reeval_queue
  PostCommitWorker         drains pending_post_commit_actions
  Gateway scheduler        refreshes view_ceo_cache and pushes WS events
  TopologySweeper          reruns the latent relationship field over a
                            bounded high-activation frontier
  Sage TopologyOptimizer   reads inquiry_outcome_events, updates the
                            Discovery Utility Layer (affordances/shortcuts/
                            negative memory/region summaries)
  RelationshipOntology     clusters edge-type candidates into reviewable
                            proposals; accepted proposals are runtime-writable
                            dynamic edge kinds with fallback semantics
  Additional worker modules exist for anomaly, entity, calibration,
                            deadline, precipitation, maintenance
```

The core path is:

```text
source event
  -> ingestion handler
  -> observations row
  -> think_trigger_queue row
  -> Think retrieval (inquiry: baseline + questions + Sage reader)
  -> reasoning + validation
  -> diff application to Models / Acts / Resources
  -> audit, reconciliation, cascades, post-commit queue
  -> Sage outcome events on inquiry_outcome_events
  -> Sage topology optimizer updates affordances / shortcuts /
     negative memory / region summaries (Discovery Utility Layer)
  -> relationship ontology proposals turn repeated edge-type gaps into
     accepted dynamic edge kinds
  -> cached/rendered CEO views and UI routes
```

## 2. Runtime Components

| Component | Code | Responsibility |
|---|---|---|
| Gateway | [services/gateway/main.py](services/gateway/main.py) | Main FastAPI app, dependency lifecycle, middleware, core routes, router mounting. |
| UI | [ui/src/main.tsx](ui/src/main.tsx) | React Router app for `/today`, `/model`, `/forecasts`, `/ledger`, `/debug`. |
| Database | [db/migrations](db/migrations) | Schema for substrate data, queues, cache, demo, topology, predictions, RLS policies. |
| Execution routing | [services/execution](services/execution) | Deterministic route gate for signals and future query/job entry points. The current rollout records shadow decisions without changing T1 Think enqueue behavior. |
| Embeddings | [lib/embeddings](lib/embeddings) | Ollama/OpenAI embedder abstraction. Current schemas expect 768-dimensional vectors. |
| LLM | [lib/llm/provider.py](lib/llm/provider.py) | Structured-output provider abstraction over Anthropic, OpenAI, and DeepSeek, with retry and cost tracking. |
| Think worker | [services/think/worker.py](services/think/worker.py) | Polls reasoning queues and invokes the `think()` pipeline. |
| Post-commit worker | [services/think/post_commit.py](services/think/post_commit.py) | Durable at-least-once side effects after reasoning commits. |
| Topology sweeper | [services/workers/topology_sweeper](services/workers/topology_sweeper) | Periodically refreshes latent relationship candidates over a bounded high-activation frontier. |
| Relationship ontology proposals | [services/relationships](services/relationships), [services/workers/relationship_ontology_proposals](services/workers/relationship_ontology_proposals) | Clusters `edge_type` candidates into proposal rows and lets accepted dynamic edge kinds resolve through runtime fallback semantics. |
| Judgment scoring | [services/judgment](services/judgment) | Shared leverage scoring for relationship/situation candidates and future attention-ranking surfaces. |
| Sage Synthesis | [services/sage](services/sage) | Deterministic query-conditioned read pipeline (cue → intent → activation → gate → subgraph → projection), Discovery Utility Layer (affordances, shortcuts, negative memory, region summaries), and the topology optimizer that learns from inquiry outcome events. |
| Rendering | [services/rendering](services/rendering) | LLM-backed UI prose generation with voice-rule checks and render cost records. |
| CEO view cache | [services/greeting](services/greeting) | Snapshot composition, cache writes, `/view/ceo/home`, and WS streaming. |
| Demo subsystem | [services/demo](services/demo) | Demo companies, per-session tenants, snapshots, auth tokens, simulator, SSE. |
| Benchmarks | [benchmarks](benchmarks), [tests/benchmarks](tests/benchmarks) | Offline retrieval/answer quality harness with adapters for LongMemEval, HotpotQA, HaluMem, and an in-repo toy dataset; runners, baselines, and report writers under `benchmarks/`. |

Local/prod compose currently defines `postgres`, `ollama`, `gateway`, `think_worker`, `post_commit_worker`, `topology_sweeper`, `sage_structural_features_worker`, `sage_topology_optimizer_worker`, `relationship_ontology_proposals_worker`, `ui`, `nginx-proxy`, and `acme-companion` in [docker-compose.yml](docker-compose.yml). Several additional worker packages are implemented but are not first-class compose services yet.

## 3. Cross-Cutting Conventions

**Tenant boundary.** Almost every persisted domain row carries `tenant_id`. Gateway auth resolves bearer tokens into an actor and tenant, then request handlers use that tenant in queries. Later migrations add tenant FKs and permissive RLS defaults, but application-level tenant scoping is still the main runtime discipline.

**Identifiers.** Backend-generated IDs use `uuid7()` from [lib/shared/ids.py](lib/shared/ids.py) for time-ordered UUIDs. Demo/session/token code may use database UUID generation in SQL in a few adapter paths.

**Database access.** Python services use `asyncpg` pools and repositories. Tests generally run against real PostgreSQL, not an in-memory fake. The gateway pool registers codecs for JSON/vector compatibility via [services/gateway/db_bootstrap.py](services/gateway/db_bootstrap.py).

**Vectors.** `observations`, `models`, and `entity_aliases` store `VECTOR(768)`. Ollama's `nomic-embed-text` is the default local backend; [lib/embeddings/factory.py](lib/embeddings/factory.py) can choose OpenAI when configured.

**Structured LLM calls.** Reasoning and rendering ask providers for Pydantic-shaped outputs through [lib/llm/provider.py](lib/llm/provider.py). Think's provider schemas include a full diff schema with `claim_ops`, `edge_ops`, `act_ops`, and resource/prediction buckets, plus a smaller claims-only schema for reasoning calls where graph/action/resource surfaces are not available. Think's prompt is cost-tuned but graph-forward: selected and graph-anchor Models are explicitly surfaced, empty diffs must cite full UUIDs, and new same-workstream claims are instructed to attach back to graph anchors. Each Think system prompt also starts with a source-tuned reasoning profile: ingestion carries normalized `signal_type`/trust metadata into the queue, and [services/think/prompt.py](services/think/prompt.py) chooses a working stance and abstraction level based on signal provenance, trigger kind, and whether the call can touch claims, graph edges, Acts, Resources, or topology. `.env.example` sets DeepSeek as the local default; `LLM_PROVIDER=codex` uses OpenAI Responses when API-key auth is present and a persistent Codex app-server for local ChatGPT/Codex login auth. For local Codex dogfood, app-server/CLI transport follows the model in `~/.codex/config.toml`; use `CODEX_MODEL`, not generic `LLM_MODEL`, only when deliberately overriding that local Codex model. The provider library itself falls back to Anthropic if no provider env is set.

**Observability records.** Runtime state is heavily persisted: `think_runs`, `think_run_costs`, `think_run_artifacts`, `view_render_costs`, `audit_events`, `reconciliation_events`, `relationship_maintenance_log`, and debug routes all exist to make reasoning inspectable.

## 4. Data Model

The database schema starts in [0001_foundation.sql](db/migrations/0001_foundation.sql) and is extended through migration `0058`. Migrations `0049`–`0058` add the Sage Discovery Utility Layer and structural-feature store (see §8.5 below).

### Foundation Tables

| Area | Tables | Notes |
|---|---|---|
| Actors | `actors`, `actor_identity_mappings`, `actor_sessions` | People/agents, source identity mapping, bearer-session auth. |
| Observations | `observations` | Append-oriented signals, partitioned by `occurred_at`, indexed by actor/channel/kind/entities/vector. |
| Models | `models`, `model_status_notes`, `model_signal_readings`, `model_scope_entities`, `model_scope_actors`, `model_composition_members` | Beliefs/propositions with confidence, activation, falsifiers, signal readings, lifecycle, structural memory grammar, normalized scope anchors, normalized situation membership, and vector search. |
| Acts | `goals`, `commitments`, `decisions`, `commitment_contributors` | Executable organizational state. State machines live under [services/acts](services/acts). |
| Act graph | `contributes_to`, `depends_on`, `constrained_by` | Relationships among goals, commitments, and decisions. |
| Resources | `resources`, `resource_transactions`, `resource_deployments`, `customer_commitments` | Assets, transactions, deployments, and customer/revenue linkage data. |
| Entity aliases | `entity_aliases` | Fast-path entity resolution by alias text/vector. |

### Reasoning and View Tables

| Area | Tables | Purpose |
|---|---|---|
| Queues | `think_trigger_queue`, `model_reeval_queue`, `pending_post_commit_actions` | Durable work queues polled with `FOR UPDATE SKIP LOCKED`; `topo_dirty_queue` remains only as legacy schema compatibility and is not part of active processing. |
| Idempotency | `applied_triggers`, `dedup_keys_seen` | Prevent duplicate application of the same trigger/diff. |
| Think observability | `think_runs`, `think_run_costs`, `think_run_artifacts`, `think_anomalies_raw` | Run status, cost, debug capture, anomaly staging. |
| Reconciliation/audit | `reconciliation_events`, `audit_events` | Duplicate-model decisions and model state-change chain. |
| CEO view | `view_ceo_cache`, `view_render_costs`, `viewer_state`, `card_conversations`, `card_exchanges` | Cached product payloads, render costs, per-viewer last-seen state, card probes. |
| Recommendations | `model_watchers`; recommendation columns on `models`; `decision_deltas` and evidence | Recommendation workflow and Today review surface. |
| Forecasts | `predictions`, `prediction_signals`, calibration tables | Forecast creation, resolution, and hit-rate/cost views. |
| Demo | `tenants`, `demo_configs`, `demo_sessions`, `demo_session_costs` | Per-demo tenant provisioning and cost/session accounting. |
| Topology and relationship intelligence | `relationship_candidates`, `relationship_ontology_proposals`, `model_edges`; legacy `model_neighborhoods`, `model_neighborhood_membership`, `topology_events` | Active topology is the latent relationship field that creates pre-truth relationship/situation/edge-type candidates from impact signatures. Ontology proposals cluster repeated edge-type gaps; accepted proposals become writable dynamic edge kinds. Typed edges store accepted pairwise meaning. Legacy accepted-memory topology tables remain for compatibility and map history. |
| Execution routing/inquiry | `signal_routing_decisions`, `inquiry_sessions`, `inquiry_question_runs`, `inquiry_evidence_items` | Route ledger plus durable adaptive-inquiry sessions, question paths, context packets, and evidence reservoir cards linked to questions, hypotheses, retrieval paths, and sufficiency verdicts. |
| Sage inquiry traces (gap fillers) | `retrieval_plans`, `omitted_evidence`, `inquiry_outcome_events`, `reader_activation_traces` | Planned retrieval per question, dropped-evidence ledger with reasons, append-only outcome events that drive the topology optimizer, and per-Model activation traces written by the Sage reader. |
| Sage structural feature store | `model_structural_features`, `model_edge_structural_features` | Per-Model and per-edge topological properties (degree, clustering, k-core, bridge_score, hub_score, etc.) computed by graph algorithms and consumed by the structural gate and subgraph selector. |
| Sage Discovery Utility Layer | `retrieval_affordance_profiles`, `affordance_context_lookup`, `discovery_shortcuts`, `negative_memory`, `region_sufficient_state` | Learnable retrieval policy: question→Model affordance profiles, fast context indexing, learned shortcuts, rejected-path memory, and per-region hypothesis/constraint/frontier summaries. Loose references (no FK) on shortcuts/negative memory so archived Models do not cascade-delete history. |
| Sage Model expectations | `model_predictions`, `model_prediction_errors` | Substrate-level expected observations emitted by Models and residual errors when incoming signals contradict them. Distinct from CEO-facing `predictions`. |
| Sage reader/writer attribution | `sage_reader_decision_attributions`, `sage_question_policy_stats` | Per-reader-decision credit ledger and learned utility of question primitives by signal shape. Retrieval-policy bookkeeping, not canonical truth. |

## 5. Gateway Architecture

[services/gateway/main.py](services/gateway/main.py) is the main process entry point.

Startup through `build_app()`:

1. Configures structlog.
2. Creates or accepts an asyncpg pool.
3. Ensures demo seed config exists.
4. Constructs `ActorRepo`, `EntityAliasRepo`, an optional Ollama client, and `RateLimiter`.
5. Starts the realtime dispatcher.
6. Wires the CEO-view stack when `GATEWAY_CEO_VIEW_ENABLED != 0`.
7. Optionally starts the greeting scheduler.
8. Closes owned resources on lifespan shutdown.

Middleware order:

| Middleware | Role |
|---|---|
| `RequestContextMiddleware` | Creates request IDs, binds tenant/actor context for logs, emits access summaries. |
| `BearerAuthMiddleware` | Validates bearer tokens against `actor_sessions`; injects auth context and sometimes `X-Tenant-Id` for CEO/demo routes. |
| `RateLimitMiddleware` | Per-tenant/actor token-bucket limiting. |

Important public or auth-bypassed route families include `/healthz`, `/auth/session`, `/view/ceo/*`, `/rendering/*`, `/simulation/*`, `/debug/*` in dev/test, and the public demo picker/session-start endpoints.

### Mounted Route Families

| Routes | Owner | Notes |
|---|---|---|
| `/ingest/{channel}` | gateway + ingestion | Uniform signal ingestion path. |
| `/observations`, `/models`, `/commitments`, `/goals`, `/decisions`, `/resources` | gateway | Basic substrate list/read surfaces. |
| `/dashboard/*`, `/v1/structure/*`, `/v1/recommendations/*`, `/v1/artifacts/*` | gateway | Product/data adapter endpoints. |
| `/rendering/*` | rendering router | In-process rendering service mounted into gateway. |
| `/view/ceo/home`, `/view/ceo/force-refresh` | greeting router | Cached CEO view. |
| `/view/ceo/ask`, turn actions | query router | Ask/query orchestration through retrieval + rendering. |
| `/v1/cards/{id}/conversation`, `/probe` | conversations | Card-scoped follow-up probes. |
| `/v1/demo/*`, `/v1/recommendations/stream` | demo | Demo lifecycle, simulator, SSE. |
| `/v1/decision-deltas/*`, `/today/*` | decision delta / today routes | Today v2 proposed-change workflow. |
| `/model/*`, `/map/*`, `/v1/model/*` | model/map/model trace | Model page, topology/map, trace. |
| `/v1/history`, `/v1/forecasts/*`, spec routes | history/forecast/spec routers | Ledger/forecast/spec surfaces. |
| `/debug/*` | debug router | Dev/operator inspector for raw runtime state, including `/debug/think-quality`, `/debug/think-quality/cases`, `/debug/sage-health`, and relationship ontology proposal listing/review for Think/Retrieval quality, replay-case extraction, Sage freshness, and ontology promotion. |
| `/internal/synthesis-reader/read`, `/internal/topology-optimizer/optimize`, `/internal/evidence-projector/project` | Sage internal routes | Direct entry points into the Sage reader, topology optimizer, and evidence projector for tooling, evals, and the upcoming optimizer worker. Not part of the public product surface. |

## 6. Ingestion Path

The ingestion implementation lives in [services/ingestion/core.py](services/ingestion/core.py). It normalizes all channels into an `ObservationDraft` and persists an observation.

Flow:

1. Gateway receives `POST /ingest/{channel}` and verifies channel-specific requirements such as Slack signatures.
2. `get_handler(channel)` returns a handler from [services/ingestion/handlers](services/ingestion/handlers).
3. The handler emits `ObservationDraft`: source channel, content text, raw JSON content, actor ref, external ID, occurred time, trust tier, entity hints, and kind.
4. Ingestion pre-assigns an observation UUID.
5. `ActorRepo` maps source actor refs to actor IDs when possible.
6. `EntityAliasRepo` performs fast-path entity lookup from 1-3 gram candidate phrases.
7. The embedder generates a 768-dimensional vector. Failures store `embedding_pending=True`.
8. `ObservationRepository.insert()` writes to the partitioned `observations` table and dedups on source channel/external ID behavior.
9. [services/execution/routing.py](services/execution/routing.py) computes a deterministic routing decision for the normalized signal.
10. A `think_trigger_queue` row is written unless the observation was deduped, trigger enqueueing was disabled, or routing is explicitly enforced and the route is `IGNORE_OR_ARCHIVE` / `HUMAN_VALIDATION_PATH`. In shadow mode, ingestion preserves the current T1 enqueue. In enforced mode, background/anomaly routes can enqueue T3/T4 work and deterministic state changes can use the `state_change` subkind.
11. The routing decision is recorded in `signal_routing_decisions` when `EXECUTION_ROUTING_SHADOW=1` or routing enforcement is enabled.
12. Post-commit observation notifications are emitted for downstream workers/listeners.

Candidate phrases from actual signal text that do not match known aliases are stored at `content._unresolved_phrases`. [services/workers/entity_resolver/worker.py](services/workers/entity_resolver/worker.py) is the lightweight deferred pass for those vague human-language aliases: it reads top-level unresolved phrases plus legacy metadata shapes, builds a small LLM context from the source observation's resolved entities, recent same-channel observations, exact prior aliases, active Models, and a bounded known-alias candidate set, then asks the configured LLM to return an existing canonical ref or null. High-confidence resolutions insert a new alias, append the entity to `observations.entities_mentioned`, emit an entity-resolution state-change observation, and re-enqueue T1 for material entity types. Medium-confidence resolutions go to `entity_review_queue`.

The trust map is centralized in [services/ingestion/handlers/__init__.py](services/ingestion/handlers/__init__.py). Handler files exist for Slack, system/internal, email, GitHub, Linear, calendar, and related channels; confirm import/registration behavior when adding a new production channel.

## 7. Think Pipeline

The core reasoning entry point is [services/think/reason.py](services/think/reason.py). The queue runner is [services/think/worker.py](services/think/worker.py).

### Trigger Kinds

| Kind | Typical source | Meaning |
|---|---|---|
| T1 | Ingestion | A new signal arrived. |
| T2 | Prediction/belief updates | A prediction or belief needs reevaluation. |
| T3 | Anomaly processor | An anomalous region needs reasoning. |
| T4 | Background/pattern/topology-candidate work | Maintenance, precipitation, or latent relationship candidate interpretation. |
| T6 | Legacy topology events | Retired accepted-memory neighborhood/graph phase shifts. |

`ThinkWorker` polls `think_trigger_queue`, promotes pending `model_reeval_queue` rows to T4 triggers, applies per-tenant concurrency caps, backs off under queue pressure, and marks failed rows after retry exhaustion. The poller only leases as many rows as it has local in-flight capacity left, so a large single-tenant backlog does not get locked by tasks waiting behind that tenant's semaphore. The safe default is one in-flight Think transaction per tenant. Higher `THINK_MAX_CONCURRENCY_PER_TENANT` values are treated as explicit stress settings because the current Think transaction still spans retrieval, LLM reasoning, validation, and apply; large real-LLM durability runs exposed model-row deadlocks when multiple long Think transactions mutated the same tenant memory graph concurrently.

### Retrieval

The active product retrieval entrypoint is [services/execution/inquiry.py](services/execution/inquiry.py). It defaults on through `EXECUTION_RETRIEVAL_ENGINE=inquiry` and can roll back to the legacy/current pathway resolver with `EXECUTION_RETRIEVAL_ENGINE=legacy`.

The inquiry runtime implements the execution proposal's end-to-end retrieval path:

1. Route the trigger into fast/deep/background/deterministic/human/archive semantics.
2. Seed baseline evidence through the existing pathway resolver.
3. Generate competing hypotheses, always including `H0` for no-update/noise.
4. Generate and score discriminating questions.
5. Compile selected questions into structural, semantic, temporal, pattern, and model-edge retrieval actions.
6. Execute retrieval actions through the existing low-level pathway code.
7. In parallel (per question, when `SAGE_READER_ENABLED=1`, default on), execute the Sage `synthesis_reader` action: deterministic cue extraction → intent inference → activation (explicit triggers, shortcuts, affordances, lexical scan, with negative-memory suppression) → structural-gate-scored edge propagation → cardinality-bounded subgraph selection → token-budgeted evidence projection. Sage results are merged into the same evidence reservoir alongside traditional pathway results.
8. Store retrieved rows as evidence cards in an in-memory reservoir and, when the tenant exists, persist them to `inquiry_evidence_items`.
9. Run a sufficiency gate with statuses such as `sufficient_for_reasoning`, `human_validation_required`, `no_update_needed`, and `budget_exhausted`.
10. Compile a Synthesis Context Packet with mandatory frame, decisive evidence, supporting groups, background summaries, and an omission ledger.
11. Attach the packet to `RetrievalResult.notes["inquiry"]`, `ContextBundle.notes["inquiry_context_packet"]`, Think debug artifacts, and the LLM prompt's `<inquiry_context_packet>` section. The `sage_reader` notes (activation breakdown, selection coverage, omissions) are attached to `inquiry.sage_reader` for telemetry and replay.
12. Persist Sage activation traces to `reader_activation_traces` and emit Sage inquiry-trace events (planned retrieval, omitted evidence, reader-decision attribution) via [services/sage/inquiry_traces/emitter.py](services/sage/inquiry_traces/emitter.py) once the inquiry session row exists.

Query strategies use the same execution layer in `FAST_PATH` mode: baseline retrieval and packet compilation only, no full inquiry loop. Think uses deep mode before validation/apply.

The low-level pathway resolver remains in [services/retrieval/primary.py](services/retrieval/primary.py).

Pathways:

| Pathway | Role |
|---|---|
| A structural | Scope/entity/model graph overlap, including bidirectional goal/commitment/customer and decision/commitment traversal. |
| B semantic | Vector similarity against seed text. |
| C temporal | Recent relevant context. |
| D pattern | Pattern/background retrieval. |
| G memory graph | Typed Model graph traversal over support, tension, causal, blocking, analogy, co-occurrence, and warning edges, plus normalized situation-member traversal through `model_composition_members`; explicit Model triggers traverse from the Model itself, while entity-only triggers derive graph seeds from normalized scope sidecars. Rejected/retired/expired edges do not retrieve. |

Trigger-specific weights combine pathway outputs. T1 retrieval uses entity seeds supplied by ingestion and also backfills `entities_mentioned`, actor, text, and embedding from the triggering Observation row, so older/sparse queue payloads still retrieve against the real customer/commitment/resource scope that ingestion resolved. T2/model-triggered retrieval is intentionally graph-forward so typed Model edges can outrank generic semantic or structural neighbors when evaluating an existing belief. Results are merged/ranked, then `ModelsRepo.retrieve()` reconsolidates returned models by increasing retrieval count/activation and updating `last_retrieved_at`.

[services/retrieval/assembler.py](services/retrieval/assembler.py) compresses retrieval results into a bounded context bundle: observations, models, acts, resources, and customer context. It includes access-control filtering, optional MMR selection for model diversity under a token budget, relevance anchors that preserve the strongest retrieved Models before diversity pressure is applied, graph anchors that preserve the strongest Pathway G model-edge hits under MMR, and prompt-survival telemetry in `bundle.notes["model_selection"]` so retrieval evals and production traces can see which pathway candidates actually reached the LLM-facing context. [services/think/context_use.py](services/think/context_use.py) then grades Think diffs against that selected context, reporting which selected Models, graph-selected Models, and selected observations were referenced by claim updates, edge endpoints/evidence, act confidence bases, or, for empty diffs, exact full UUID references in `reasoning_trace`. This separates a justified no-op that actually inspected memory from a successful run that ignored retrieval. The final context-use report is stored in `think_runs.ops_applied["context_use"]` and emitted into in-memory Think metrics by grade (`graph_context_used`, `justified_noop_context_used`, `model_context_used`, `observation_context_used`, `unused_selected_context`, or `no_selected_context`), making it possible to audit whether a successful Think run merely had good retrieval available or actually used that retrieved memory in the diff it committed. [services/think/quality_report.py](services/think/quality_report.py) builds the operator report used by `/debug/think-quality`, aggregating recent grade counts, quality gates, ignored memory ids, graph-context misses, low-use runs, missing telemetry, and LLM cost/token totals. Its telemetry coverage gate is scoped to successful Think runs, its no-op analysis backfills exact trace UUID references from stored artifacts, and its flags distinguish justified no-op successes from graph-applicable mutating runs that ignored graph-selected Models. `/debug/think-quality/cases` extracts flagged successful runs into replay-ready cases with trigger, observation, captured artifacts, context-use data, and suggested eval assertions; [services/think/quality_promoter.py](services/think/quality_promoter.py) and [scripts/promote_think_quality_cases.py](scripts/promote_think_quality_cases.py) promote those cases into versioned JSON fixtures under [tests/quality_replay](tests/quality_replay).

### Reason, Validate, Apply

The Think transaction performs:

1. Insert/update a `think_runs` record.
2. Run active retrieval through `retrieve_for_execution()`. In the default inquiry engine this performs baseline seeding, question-conditioned retrieval, evidence reservoir construction, sufficiency, and context-packet compilation before returning a normal `RetrievalResult`.
3. Capture retrieval, inquiry, sufficiency, and context-packet debug artifacts when `DEBUG_ARTIFACT_CAPTURE=1`.
4. Assemble context.
5. Build a `ReasoningFrame` from the trigger and retrieved Models. The frame normalizes T1/T2/T3/T4/T6 into the concrete question the run should answer, with `T4:latent_relationship_candidate` dedicated to topology candidate interpretation. It records seed/candidate Model ids, sets allowed op surfaces and small budgets, includes ephemeral dynamic signals when detectors find them, and is stored in retrieval/debug/apply artifacts.
6. Route authoritative/deterministic cases to deterministic handlers; otherwise call the configured LLM through `llm_reason`. `T2:belief_updated` is deterministic bookkeeping, so belief-update cascades drain without a secondary LLM call unless the trigger has prediction-resolution shape.
7. Validate the raw diff against [services/think/diff_schema.py](services/think/diff_schema.py) and semantic rules in `validator.py`. Same-diff Model placeholders are allowed through `born_from_event_id`, `entry.model_id`, or `entry.id` so live LLMs can refer to a newly inserted Model from edge/action ops in the same response.
8. Compute context-use telemetry for the validated diff against the assembled prompt context.
9. Acquire advisory region locks based on touched tenant/entities, plus a short per-tenant model-write advisory lock around apply-time memory mutation.
10. Reconcile model inserts against existing models before applying.
11. Apply claim ops, edge ops, act ops, and resource ops with [services/think/applier.py](services/think/applier.py). The applier strips LLM-invented persistent ids from new Model entries, maps same-diff placeholders to the actual inserted ids, canonicalizes `superseded_by` edges so the stored direction is old Model -> replacement Model, skips edges that collapse to self-edges after reconciliation, and keeps `model_edges` strictly Model-to-Model; customer, commitment, goal, decision, and resource ids belong in `scope_entities` instead.
12. Emit state-change observations, audit events, cascades, and reeval triggers.
13. Enqueue durable post-commit actions.
14. Record LLM cost, applied-op summaries, context-use telemetry, and run status.

Diffs mutate four surfaces:

| Diff bucket | Target |
|---|---|
| `claim_ops` | Models: insert, update, archive. |
| `edge_ops` | Model graph relationships: add/reconfirm/retire typed edges with evidence and explanations. |
| `act_ops` | Goals, commitments, decisions, and act graph edges. |
| `resource_ops` | Resources, transactions, deployments, releases. |

For LLM ergonomics, validation and apply support same-diff references: an `edge_op` endpoint or `act_op.confidence_basis` may point at the `born_from_event_id` of a `claim_ops.insert` in the same raw diff. The validator checks that the pending claim exists and is strong enough, then [services/think/applier.py](services/think/applier.py) rewrites that event id to the actual inserted Model id before applying edges or act transitions. This lets the LLM express "new observation creates a Model, and that Model supports/justifies this edge/action" in one production transaction without inventing a future UUID.

[services/think/auto_create_commitment.py](services/think/auto_create_commitment.py) is the deterministic safety-net layer for high-value cases the LLM may under-emit. It is intentionally narrow and idempotent: self-reported new work can become a create-commitment recommendation, explicit blocked/on-hold signals can transition the best matching commitment, explicit decision-revisit signals can add a decision-scoped concern plus `transition_decision`, explicit future-dated plans can be split out of state-only output into a prediction with a deadline falsifier, and explicit customer churn/renewal-risk signals can become customer-scoped concern Models when a resolved customer id exists. The goal is not to replace reasoning; it is to preserve production invariants when live model output is semantically close but operationally incomplete.

[services/think/llm_reason.py](services/think/llm_reason.py) selects the smallest safe output surface for each reasoning call. It uses the claims-only schema and compact system prompt for signals with no Models, Acts, or Resources in context, and for `T2:belief_updated` calls that have no selected graph-anchor Models. It keeps the full schema whenever selected graph memory is present, any Act or Resource context is present, or the trigger kind can mutate graph/action/resource surfaces. The current measured static prompt/schema floors are roughly 3.5k input tokens for claims-only calls versus 4.9k for full graph/action calls before retrieved observations/models/acts/resources are added; live DeepSeek runs show small no-surface calls around 6.2k input tokens, non-graph `T2:belief_updated` calls around 6.7k-8.2k, and graph/action-bearing calls higher by design.

[services/actors/operating_context.py](services/actors/operating_context.py) derives actor operating context from existing substrate data instead of introducing an `actor_model` proposition kind. It summarizes actor-scoped Models, owned commitments, blocked work, recent observations, capability assessments, concerns, patterns, and relations into the existing `<actor_context>` prompt section. Actor operating claims remain ordinary Models scoped through `scope_actors`: capability assessments, concerns, relations, patterns, hypotheses, or states depending on the evidence.

[services/dynamics/detectors.py](services/dynamics/detectors.py) similarly keeps organizational dynamics ephemeral. It reads existing `audit_events`, legacy `topology_events`, observations, and active Models to surface signals such as oscillation, recurring updates, stale memory, legacy graph phase shifts, and high actor activity into the `ReasoningFrame`. Important dynamics can be promoted through existing proposition kinds (`pattern`, `pattern_instance`, `environmental_trend`, `concern`, or `situation`) rather than a separate dynamics table.

Application is idempotent through `applied_triggers`. A duplicate trigger short-circuits rather than re-running side effects.

## 8. Models, Reconciliation, Audit, and Topology

[services/models/repo.py](services/models/repo.py) is the main Models repository. Inserts validate proposition shape, falsifier adequacy above confidence thresholds, scope actor existence, confidence clipping, embeddings, recommendation shape, state-change emission, audit events, typed edges, and latent topology candidate generation.

Key model-side concepts:

| Concept | Code/schema | Purpose |
|---|---|---|
| Proposition kind | generated from `models.proposition` | Type-level discriminator for state, concern, prediction, recommendation, etc. |
| Memory grammar | `models.claim_role`, `abstraction_level`, `time_mode`, `modality`, `polarity`, `domain_tags`; [lib/shared/memory_grammar.py](lib/shared/memory_grammar.py) | Structural organization layer for synthesis. It separates belief role and abstraction from product/UI categories so `proposition_kind` no longer has to carry every ontology decision. |
| Situation Models | `models.proposition_kind = 'situation'` | First-class composite conditions over multiple existing Models; used when a subgraph is one operational reality rather than a simple pairwise edge. |
| Situation composition | `model_composition_members` | Normalized membership sidecar for composite Models. Situation JSON still carries `member_model_ids` for compatibility, but queries and lifecycle work can traverse membership without JSONB-only parsing. |
| Confidence | model column + calibration modules | Main strength/credence signal. |
| Activation | model column | Recency/importance signal raised by retrieval and decayed by maintenance. |
| Falsifier | [services/models/falsifier.py](services/models/falsifier.py) | Required for strong claims and recommendations. |
| Signal readings | `model_signal_readings` sidecar | Per-signal evidence contributions. |
| Typed edges | `model_edges` + [services/models/edges_repo.py](services/models/edges_repo.py) | First-class model graph with support, tension, causal, blocking, analogy, co-occurrence, warning relationships, and accepted dynamic ontology-proposal kinds. Edges carry confidence, evidence ids, explanations, review status, confirmation counts, and decay/expiry hints. Reconfirmations merge evidence and cannot downgrade an accepted review status. |
| Normalized scope | `model_scope_entities`, `model_scope_actors` | Insert-time sidecars mirror `models.scope_entities` / `scope_actors` so retrieval and graph expansion can anchor Models without JSONB-only scans; malformed legacy non-UUID entity ids stay in JSONB but are skipped by the sidecar. |
| Scope linkage | `customer_commitments` + Pathway A + `ModelsRepo.search_by_scope` + Think prompt resource context | Customer lookups normalize `customer` and `customer_resource`, expand across linked commitments, and commitment lookups can expand back to customers. When an explicit customer-commitment link is missing, customer lookup falls back to obvious commitment-title matches against the customer identity. Think prompts render resource identity/description and instruct customer-linked commitment Models to carry both scopes, preserving precise commitment workflows while making customer-level memory retrieval robust. |
| Topology | [services/topology/field.py](services/topology/field.py), [services/topology/eval_harness.py](services/topology/eval_harness.py), [services/workers/topology_sweeper](services/workers/topology_sweeper), `relationship_candidates` | Active latent relationship field. Each new/changed Model is converted into an impact signature over flows, pressures, surfaces, stakes, time shape, evidence, and action/falsifier surface. The field searches bounded semantic, surface, consequence, and evidence pools, scores consequence interactions, persists only high-yield edge/situation candidates, and enqueues at most a small T4 Think pass for top candidates. The sweeper periodically revisits high-activation Models so older memory can connect to newer memory without a full all-pairs recompute. The eval harness measures whether expected hidden pairs/situations are found before accepted typed edges exist. |
| Accepted-memory topology (legacy schema) | `model_neighborhoods`, `model_neighborhood_membership`, `topology_events`, `topo_dirty_queue` | Retired graph-derived positional embeddings, neighborhoods, topology events, and projection helpers. The Python repos/workers for this engine have been removed; database tables remain for compatibility/history and current map/Today/decision-delta surfaces that read historical rows. |
| Relationship candidates | [services/relationships](services/relationships), `relationship_candidates`, `relationship_ontology_proposals` | Ranked pre-acceptance hypotheses for new edges, situation Models, or edge types, scored by shared judgment leverage. Topology candidates carry score components and impact signatures in metadata. Candidate adjudication marks edge/situation rows accepted, rejected, or needs-review after T4 Think interprets the proposal; edge-type rows feed ontology proposals. Accepted ontology proposals are resolved by [services/relationships/ontology_runtime.py](services/relationships/ontology_runtime.py) so validators and `EdgesRepo` can write the new kind. |

Reconciliation is first-class in [services/think/reconciler.py](services/think/reconciler.py). Insert claim ops are checked against existing models; decisions are recorded in `reconciliation_events`. Auto-merge decisions convert inserts into evidence-preserving updates: confidence is updated, `supporting_event_ids` gains the new event, `signal_readings` records a confirmation reading, and confirmation timestamps/counts advance. Human-review/no-match decisions preserve auditability and avoid silent destructive merges. Live strict-schema LLM outputs usually omit embeddings, so the reconciler uses the same deterministic lexical fallback as the applier before insert; this lets production-like diffs deduplicate before duplicate Models land.

Audit events in `audit_events` record model changes, reversals, and reconciliation merge chains. This is the main answer to "why did this belief change?"

### 8.5 Sage: Query-Conditioned Synthesis and the Self-Evolution Loop

The Sage subsystem under [services/sage](services/sage) sits on top of the existing inquiry retrieval and the canonical memory graph. It does not own canonical truth: it owns the **Discovery Utility Layer** that learns how to retrieve, organize, and synthesize that truth more effectively over time. Migrations `0049`–`0058` introduce the storage backbone; the Python code is wired into the gateway, [services/execution/inquiry.py](services/execution/inquiry.py), and [services/think/reason.py](services/think/reason.py) (for inquiry-trace context).

The read pipeline is deterministic and explainable end-to-end:

1. **Cue extraction** ([services/sage/cue_extractor.py](services/sage/cue_extractor.py)) — regex + entity-alias lookup over signal/question text produces `StructuredCues` (entities, aliases, systems, relationship clues, time/status/source/access constraints).
2. **Intent inference** ([services/sage/intent_inferer.py](services/sage/intent_inferer.py)) — maps cues to one or more `RetrievalIntent`s (`test_dependency`, `find_owner`, `find_counterevidence`, `find_pattern_recurrence`, `find_action_candidates`, `test_falsification`, etc.) with paths, budgets, success conditions, and expected value/cost priors.
3. **Activation** ([services/sage/reader.py](services/sage/reader.py)) — combines explicit-trigger Models, learned `discovery_shortcuts`, `retrieval_affordance_profiles` matched by question primitive and entities, and a bounded lexical scan, then applies `negative_memory` as a suppression layer.
4. **Edge propagation + structural gating** ([services/sage/structural_gates.py](services/sage/structural_gates.py)) — neighbors of activated seeds gain score weighted by a per-edge gate multiplier combining relation-type weight, trust, freshness, role compatibility, bridge bonus, and hub penalty. The gate reads `model_structural_features` and `model_edge_structural_features` populated by the [services/sage/structural_features](services/sage/structural_features) graph-algorithms job.
5. **Subgraph selection** ([services/sage/subgraph_selector.py](services/sage/subgraph_selector.py)) — cardinality-constrained selection enforcing per-primitive thresholds, bridge preservation, hub roll-up, edge gate floors, and required-evidence-role coverage. Excluded nodes carry typed reasons.
6. **Evidence projection** ([services/sage/evidence_projection.py](services/sage/evidence_projection.py)) — converts selected Models into ranked `EvidenceCandidate`s (raw excerpt, evidence card, summary-only, ref-only) under a token budget, with counterevidence promotion for CONTRADICTION/FALSIFICATION primitives.

Discovery Utility Layer:

| Layer | Repo / module | Storage |
|---|---|---|
| Affordance profiles | [services/sage/affordances](services/sage/affordances) | `retrieval_affordance_profiles`, `affordance_context_lookup` |
| Discovery shortcuts | [services/sage/discovery](services/sage/discovery) | `discovery_shortcuts` (loose `to_model_id` refs, expiry required) |
| Negative memory | [services/sage/discovery](services/sage/discovery) | `negative_memory` (rejected hypotheses, noisy paths, failed shortcuts) |
| Region sufficient state | [services/sage/region_summaries](services/sage/region_summaries) | `region_sufficient_state` (active hypotheses, constraints, frontiers, falsification watch) |
| Model substrate expectations | [services/sage/model_predictions](services/sage/model_predictions) | `model_predictions`, `model_prediction_errors` (distinct from CEO `predictions`) |

Writer-side feedback loop:

1. After Think apply, [services/sage/outcome_evaluator.py](services/sage/outcome_evaluator.py) inspects the inquiry session, the Think run, and validated `ops_applied` to emit typed events into `inquiry_outcome_events`: `retrieved_evidence_used_in_packet`, `retrieved_evidence_omitted`, `omitted_evidence_later_requested`, `node_used_in_valid_diff`, `path_used_in_valid_diff`, `reader_decision_used_in_valid_diff`, `outcome_quality_assessed`, `validation_failed_due_to_missing_evidence`, `validation_failed_due_to_bad_reference`, plus human-in-the-loop signals (`user_accepted_node`, `user_contested_node`) and temporal-validation signals (`model_later_confirmed`, `model_later_falsified`, `recommendation_acted_on`, `recommendation_ignored`). `outcome_quality_assessed` payloads carry the anti-proxy breakdown of writer-failure vs missing-evidence vs low-value-leakage and per-axis quality features.
2. Per-decision attribution is written to `sage_reader_decision_attributions` and aggregated into `sage_question_policy_stats` so the system can learn which question primitives pay off for which signal shapes.
3. The **Topology Optimizer** ([services/sage/topology_optimizer](services/sage/topology_optimizer)) reads `inquiry_outcome_events` for a session and applies append-only Discovery Utility Layer updates: reinforce affordances and shortcuts that made it into valid diffs, decay stale ones, record negative memory for rejected paths, and refresh region sufficient states. Canonical mutations (merges, splits, promotions, demotions) are surfaced as candidates on the report and routed through a future validation queue, never applied directly. The optimizer is invoked by `POST /internal/topology-optimizer/optimize` and by the `sage_topology_optimizer_worker` compose service, which checkpoints processed inquiry sessions.

Sage is designed to fail open. All Sage tables are optional at read time (readers use `to_regclass` and degrade gracefully); missing structural features become neutral 1.0 in the gate; `SAGE_READER_ENABLED=0` removes the Sage reader from inquiry without breaking the baseline pathway resolver.

## 9. CEO View, Rendering, Query, and Conversations

The CEO-facing product surface is composed from cached backend state rather than issuing a fresh LLM render on every page load.

### Greeting/CEO Cache

[services/greeting/scheduler.py](services/greeting/scheduler.py) keeps `view_ceo_cache` fresh for registered tenants.

Refresh triggers include scheduled intervals, time-of-day boundaries, Postgres `LISTEN view_ceo_refresh`, and polling of post-commit actions as a fallback. The scheduler composes substrate snapshots via [services/greeting/snapshot.py](services/greeting/snapshot.py), sends render requests, writes cache keys, and publishes WebSocket updates.

Cache keys:

| Key | Meaning |
|---|---|
| `greeting` | Opening summary. |
| `query_grid` | Suggested questions. |
| `cards` | CEO-relevant cards. |
| `status` | Health/calibration/needs-you summary. |
| `close_line` | Closing summary line. |

[services/greeting/api.py](services/greeting/api.py) assembles these into `GET /view/ceo/home`. [services/greeting/stream.py](services/greeting/stream.py) exposes WS streaming.

### Rendering

[services/rendering/core.py](services/rendering/core.py) builds prompts for greetings, cards, query chips, card reasoning, and conversation turns. It calls the LLM provider, runs voice-rule checks, retries once on reject-level violations, and writes `view_render_costs` when a pool is configured.

### Query

[services/query/core.py](services/query/core.py) powers Ask flows:

```text
query
  -> classifier
  -> strategy
  -> retrieval + context assembly
  -> rendering adapter
  -> AnswerQueryResponse with retrieval trace and cost
```

Strategies live in [services/query/strategies](services/query/strategies). The gateway wires query routes during CEO-view setup and shares the gateway embedder so semantic retrieval works for `/view/ceo/ask`.

### Conversations

[services/conversations](services/conversations) stores and handles card-scoped probe threads. The Today UI can ask follow-up questions against a specific card without losing card context.

## 10. UI Architecture

The frontend is a Vite + React + TypeScript app in [ui](ui).

Routes in [ui/src/main.tsx](ui/src/main.tsx):

| Route | Page | Backend surface |
|---|---|---|
| `/today` | Today briefing/review | `/today`, decision deltas, card probes, ask, streams. |
| `/model` | Model page v2 | `/model/*`, `/v1/model/*`, map/topology APIs. |
| `/forecasts` | Forecasts spec page | `/v1/forecasts/*`. |
| `/ledger` | Ledger/history spec page | `/v1/history`, spec/ledger APIs. |
| `/debug/*` | Debug inspector | `/debug/*`, dev/test only. |

Legacy routes redirect into the current four-product-surface model: `/structure` and `/map` redirect to `/model`; `/history` redirects to `/ledger`; `/mind`, `/demo`, `/ask` redirect to `/today`.

API clients live under [ui/src/api](ui/src/api). The Vite dev server proxies `/api/*` to gateway `http://localhost:8000` and `/stream/*` to gateway WebSockets unless `USE_MOCK=1`, in which case [ui/mock-server.ts](ui/mock-server.ts) and fixture data serve the app locally.

The UI has a demo-session wrapper, [ui/src/shell/AutoDemoSession.tsx](ui/src/shell/AutoDemoSession.tsx), that provisions or reuses demo auth tokens in local/demo flows. Tokens are stored in local storage and sent as bearer auth by shared API helpers.

## 11. Demo and Simulation

The demo system lets anonymous visitors choose a company, provision a fresh tenant, and interact with realistic seeded data.

Flow:

1. `GET /v1/demo/companies` lists configured companies.
2. `POST /v1/demo/sessions/start` creates a new tenant, loads a snapshot, finds/mints the CEO actor, creates an `actor_sessions` token, and returns session metadata.
3. Authenticated demo calls use that token and tenant.
4. Reset/end endpoints manage session lifecycle.
5. Simulator endpoints inject signals and increment demo counters.
6. `/v1/recommendations/stream` streams recommendation/demo events.

Implementation lives in [services/demo/router.py](services/demo/router.py), [services/demo/sessions.py](services/demo/sessions.py), and [services/demo/snapshot.py](services/demo/snapshot.py). Demo model routing in [services/demo/model_routing.py](services/demo/model_routing.py) can choose cheaper/faster models per tenant/call kind.

The gateway can also mount simulation helpers and static Slack UI from [simulation](simulation) when `GATEWAY_MOUNT_SIM=1`.

## 12. Background Workers

### Deployed by Compose

| Worker | Launcher | Behavior |
|---|---|---|
| Think | [scripts/run_think_worker.py](scripts/run_think_worker.py) | Creates pool/provider and runs `ThinkWorker.run()`. |
| Post-commit | [scripts/run_post_commit_worker.py](scripts/run_post_commit_worker.py) | Polls `pending_post_commit_actions`, dispatches handlers, retries with backoff, dead-letters after max attempts. |
| Topology sweeper | [scripts/run_topology_sweeper.py](scripts/run_topology_sweeper.py) | Runs the latent relationship field on a bounded high-activation frontier and logs candidate lifecycle metrics. |
| Sage structural features | [scripts/run_sage_structural_features_worker.py](scripts/run_sage_structural_features_worker.py) | Recomputes model and edge structural features consumed by Sage gates and subgraph selection. |
| Sage topology optimizer | [scripts/run_sage_topology_optimizer_worker.py](scripts/run_sage_topology_optimizer_worker.py) | Polls completed inquiry sessions with outcome events, checkpoints them, and updates the Discovery Utility Layer. |
| Relationship ontology proposals | [scripts/run_relationship_ontology_proposals_worker.py](scripts/run_relationship_ontology_proposals_worker.py) | Clusters open edge-type candidates into `relationship_ontology_proposals` rows for review and promotion. |

### In-Process in Gateway

| Worker | Code | Behavior |
|---|---|---|
| Realtime dispatcher | [services/realtime](services/realtime) | WebSocket dispatch/replay machinery. |
| Greeting scheduler | [services/greeting/scheduler.py](services/greeting/scheduler.py) | Scheduled and trigger-driven cache refresh. |

### Implemented Worker Modules

Additional worker packages exist under [services/workers](services/workers):

| Package | Purpose |
|---|---|
| `anomaly_processor` | Detects and stages anomalies. |
| `entity_resolver` | Resolves unresolved actors/entities from observations. |
| `calibration_updater` | Computes calibration/hit-rate updates. |
| `deadline_resolver` | Resolves due predictions/deadlines. |
| `precipitation` | Clusters candidate patterns and proposes background reasoning. |
| `edge_drift` | Checks typed model edges against legacy relationship arrays. |
| `topology_sweeper` | Periodically reruns the latent relationship field over a bounded high-activation frontier. |
| `sage_structural_features` | Periodically recomputes SAGE model/edge structural features consumed by reader gates and subgraph selection. |
| `sage_topology_optimizer` | Polls completed inquiry sessions with outcome events, checkpoints them, and runs the SAGE optimizer to update the Discovery Utility Layer. |
| `relationship_ontology_proposals` | Aggregates open edge-type candidates into reviewable proposal rows. |
| `maintenance` | Daily/weekly/monthly maintenance routines. |

The Sage **topology optimizer** ([services/sage/topology_optimizer](services/sage/topology_optimizer)), **structural-features job** ([services/sage/structural_features](services/sage/structural_features)), and relationship ontology proposal aggregator now have worker wrappers under [services/workers](services/workers) plus launch scripts in [scripts](scripts). The optimizer worker uses `sage_topology_optimizer_runs` as a durable per-session checkpoint so outcome events are not reinforced repeatedly by polling.

Treat these as available architecture modules, not all as currently deployed services.

## 13. Security and Access Control

Authentication is bearer-token based through `actor_sessions` and [services/gateway/auth.py](services/gateway/auth.py). `/auth/session` can mint sessions, optionally guarded by `AUTH_BOOTSTRAP_SECRET`.

Authorization layers:

| Layer | Current implementation |
|---|---|
| Gateway auth | Bearer token -> `AuthContext(actor_id, tenant_id, expires_at)`. |
| Rate limiting | Per actor/tenant token buckets. |
| Request tenant scoping | Request handlers use tenant from auth/header/default env. |
| Access-control services | [services/access_control](services/access_control) contains role hierarchy, materialized visibility, checks, audit. |
| RLS | Later migrations enable permissive tenant policies on many tables. |
| Debug routes | Mounted only for `dev`, `staging`, or `test` environment names. |

The current dogfood/demo configuration has deliberate dev shortcuts: default tenant fallback, static CEO tokens, unauthenticated demo picker/session-start, and optional simulation/debug mounts. Shared or production deployments should review those env flags carefully.

## 14. Deployment and Local Development

Local setup is described in [README.md](README.md).

Important env groups:

| Group | Examples |
|---|---|
| Database/embedding | `DATABASE_URL`, `OLLAMA_URL`, `OLLAMA_EMBED_MODEL`. |
| LLM | `LLM_PROVIDER`, `LLM_MODEL`, provider API keys, Codex `CODEX_MODEL`/auth file overrides, timeouts. |
| Tenant identity | `DEFAULT_TENANT_ID`, `COMPANY_OS_CEO_ACTOR_ID`, `DEV_BEARER_TOKEN`, `VIEW_CEO_TOKEN`. |
| Gateway | `COMPANY_OS_ENV`, `GATEWAY_OWNS_POOL`, `GATEWAY_CEO_VIEW_ENABLED`, `GATEWAY_START_GRT_SCHEDULER`, `GATEWAY_MOUNT_SIM`. |
| Workers | `THINK_*`, `POST_COMMIT_WORKER_POLL_INTERVAL_S`, `GREETING_REFRESH_INTERVAL_SECONDS`. |
| Execution retrieval | `EXECUTION_RETRIEVAL_ENGINE`, `INQUIRY_MAX_ROUNDS`, `INQUIRY_QUESTIONS_PER_ROUND`, `INQUIRY_EVIDENCE_RESERVOIR_LIMIT`, `INQUIRY_REASONING_PACKET_TOKENS`, `INQUIRY_TEMPORAL_WINDOW_DAYS`, `INQUIRY_SEMANTIC_BUDGET`. |
| Execution routing | `EXECUTION_ROUTING_SHADOW`, `EXECUTION_ROUTING_ENABLED`. |
| Sage | `SAGE_READER_ENABLED` (default `1`). Additional Sage tunables live in module constants on the topology optimizer (`REINFORCE_DELTA`, `DECAY_FACTOR`, `SHORTCUT_POSITIVE_DELTA`, `NEGATIVE_MEMORY_TTL`). |
| Debug | `DEBUG_ARTIFACT_CAPTURE`, `LOG_LEVEL`. |

The production-ish compose topology builds the Python gateway image from [Dockerfile](Dockerfile) and the UI from `Dockerfile.ui`, fronts the UI with nginx-proxy/acme, and expects `.env.production` for secrets.

## 15. Testing Strategy

Python tests are mostly real integration tests:

```bash
pytest
pytest -m integration
pytest -m ollama
RUN_REAL_LLM=1 pytest -m real_llm
```

The suite is organized by service package (`services/*/tests`) plus cross-service tests under [tests](tests). `pyproject.toml` configures pytest, strict markers, async mode, and warning filters. The integration harness installs a test-only tenant auto-registration trigger and reseeds demo company configs after destructive table truncation so older raw-SQL fixtures still exercise the current tenant-FK schema and demo routes do not run against an empty registry. Retrieval quality cases in [services/retrieval/tests/test_retrieval_quality_harness.py](services/retrieval/tests/test_retrieval_quality_harness.py) assert business-level reachability and exclusions across customer scope, commitment links, typed model edges, rejected/archived edge neighbors, decision constraints, actor-only signals, pattern/instance retrieval, mixed-size top-N behavior, recall aggregation, latency smoke limits, context-assembly survival for high-value graph hits, and multi-tenant isolation. The inquiry E2E harness in [tests/retrieval_e2e](tests/retrieval_e2e) seeds blocker, fast-query, recurrence, and cross-tenant distractor scenarios, then asserts route, question path, retrieval actions, evidence reservoir, sufficiency, context packet, compact model prompt, and efficiency metrics; it writes [tests/retrieval_e2e/_last_run.json](tests/retrieval_e2e/_last_run.json) when `RETRIEVAL_E2E_REPORT` is not disabled. Think context-use tests in [services/think/tests/test_context_use.py](services/think/tests/test_context_use.py), Think quality-report/promotion tests in [services/think/tests](services/think/tests), quality replay contract tests in [tests/quality_replay](tests/quality_replay), and the opt-in real-LLM eval in [tests/real_llm/tests/test_context_use_outcome.py](tests/real_llm/tests/test_context_use_outcome.py) check whether selected context is actually referenced by generated diffs and whether production records expose actionable quality failures, quality gates, replayable cases, and promotable fixtures; the integration case also runs a full Think transaction, applies an edge derived from selected graph context, and verifies the resulting `context_use` report persisted in `think_runs.ops_applied`. Deterministic Think-safety tests cover the production fallbacks for commitment creation, block transitions, decision revisits, future-plan prediction splitting, cheap `T2:belief_updated` cascade drain, live-LLM same-diff Model placeholder handling, strict-schema no-embedding reconciliation, supersession direction canonicalization, and bounded worker leasing under local in-flight pressure. The real-LLM harness under [tests/real_llm](tests/real_llm) provisions a proper tenant row before materializing scenario fixtures, seeds aliases for foundation customers/goals/commitments/decisions, infers obvious customer-commitment links from scenario titles, then runs DeepSeek-backed Think flows through the same migrations, queues, embeddings, provider cache, validator, and applier path used in production. Scenario 04, [tests/real_llm/scenarios/04_scale_chaos_b2b.yaml](tests/real_llm/scenarios/04_scale_chaos_b2b.yaml), is a large synthetic-customer corpus for memory/retrieval stress: 22 actors, 10 customers, 24 commitments, 8 decisions, and 119 signals across customer risk, aliases, stale replays, billing disputes, legal/security constraints, forecast contradictions, and hidden-connection evidence. Its lightweight structural test guards corpus coherence without services, while [tests/real_llm/tests/test_scale_chaos_ingestion.py](tests/real_llm/tests/test_scale_chaos_ingestion.py) can be opted in with `RUN_SCALE_CHAOS_FULL=1` to inject the entire corpus through production ingestion, embeddings, observation storage, and T1 enqueue without spending LLM tokens. [tests/real_llm/tests/test_entity_resolver_real_llm.py](tests/real_llm/tests/test_entity_resolver_real_llm.py) hits DeepSeek on the actual-content alias path, proving `NBI` can be resolved from a scenario signal that says "Nimbus Bank as NBI" into the existing Nimbus customer resource. [tests/real_llm/tests/test_scale_chaos_end_to_end.py](tests/real_llm/tests/test_scale_chaos_end_to_end.py) is the curated full-chain proof: it ingests that alias signal, resolves it with DeepSeek, injects a six-signal Nimbus crisis slice, runs Think/DeepSeek through validation and apply, then verifies Models/state changes and customer-link surfaces. Scenarios 05 and 06, [tests/real_llm/scenarios/05_industrial_ops.yaml](tests/real_llm/scenarios/05_industrial_ops.yaml) and [tests/real_llm/scenarios/06_fintech_risk.yaml](tests/real_llm/scenarios/06_fintech_risk.yaml), are deep synthetic-company corpora for durability: industrial safety/telemetry/supplier risk and fintech ledger/KYC/fraud/regulatory risk. [tests/real_llm/tests/test_deep_durability_end_to_end.py](tests/real_llm/tests/test_deep_durability_end_to_end.py) can be opted in with `RUN_DURABILITY_E2E=1` to inject every signal in those corpora, resolve actual-content aliases (`TFI`, `MRA`, `ACS`, `BRCU`) with DeepSeek, drain Think, assert no pending or failed runs, verify context-use telemetry, confirm model/state-change creation, and check customer-link surfaces. [scripts/run_1000_signal_model_layer_probe.py](scripts/run_1000_signal_model_layer_probe.py) is the heavyweight single-customer scale probe: it materializes one company, injects up to 1000 diverse production-shaped signals across incidents, sales, security, legal, finance, roadmap, telemetry, aliases, contradictions, stale replays, market moves, and noise, optionally drains live DeepSeek-backed Think, then writes `run_summary.json`, `model_layer_summary.md`, `models.jsonl`, `model_edges.jsonl`, and `signal_manifest.jsonl` under `tests/real_llm/reports/runs/` for later graph-shape analysis; the summary now includes graph-health metrics for component shape, isolated Models, soft/actionable edge ratios, duplicate directed edges, self/orphan edges, and exact duplicate natural-language groups. The latest live hardening loop used these harnesses to test real output behavior for proposition-kind diversity, decision revisits, act cascades, customer-link queries, graph-context edge creation, cross-component customer-crisis chains, and full-corpus durability, then tightened prompt guidance and deterministic safety nets where live behavior was semantically close but operationally incomplete.

The inquiry adversarial slice currently covers stale counterevidence, ambiguous ownership, bounded human-validation routing, question-conditioned semantic embedding, and same-actor semantic-noise cases.

Sage has its own unit suite under [tests/unit/sage](tests/unit/sage) covering cue extraction, intent inference, structural gates, subgraph selection, evidence projection, the discovery/affordance/region/model-prediction repos, inquiry-trace persistence, outcome evaluation, and end-to-end Phase 1 wiring plus a "Sage flywheel" test that proves outcome events feed the topology optimizer. A standalone offline-evaluation harness lives under [benchmarks](benchmarks) with dataset adapters for LongMemEval (S/M, cleaned JSON), HotpotQA distractor/fullwiki, HaluMem-Medium, and an in-repo toy memory dataset. The runner ([benchmarks/run_benchmark.py](benchmarks/run_benchmark.py), also exposed as `python -m benchmarks.run_benchmark`) exercises adapter contracts, normalized observations/queries, in-memory ingestion, a lexical BM25 baseline, context-packet compilation, a fixed extractive answerer, and metric/report writers (JSONL, JSON, trace, Markdown) under `benchmarks/reports/generated/`. [tests/benchmarks](tests/benchmarks) holds the foundational contract tests for adapters, the Fyralis DB reader, the packet compiler, and the fixed answerers.

UI tests:

```bash
cd ui
npm test
npm run test:e2e
npm run typecheck
```

Playwright E2E uses the in-repo mock backend. The UI can be developed against either gateway proxy mode or `USE_MOCK=1` mode.

## 16. How to Extend the System

### Add a New Ingestion Channel

1. Add a handler in [services/ingestion/handlers](services/ingestion/handlers).
2. Register it with `@register("channel:name")`.
3. Add the channel trust tier to `CHANNEL_TRUST_MAP`.
4. Ensure the handler module is imported so registration runs.
5. Add ingestion and gateway tests.
6. Decide whether the payload needs signature/auth verification in gateway.

### Add a New Model Proposition Kind

1. Update proposition validation in [services/models/propositions.py](services/models/propositions.py).
2. Add migration/check constraints if needed.
3. Update prompts, `diff_schema`, validator/applier logic if the LLM can emit it.
4. Add retrieval/rendering behavior if it should appear in UI context.
5. Add tests for insert, validation, retrieval, and rendering.

### Add a New UI Surface

1. Add route in [ui/src/main.tsx](ui/src/main.tsx).
2. Add API client/types in [ui/src/api](ui/src/api).
3. Prefer a gateway adapter route over direct table-shaped UI coupling.
4. Add mock fixture support for `USE_MOCK=1`.
5. Add Vitest and, if user-facing flow matters, Playwright coverage.

### Add a New Worker

1. Keep core work idempotent and safe under multiple worker instances.
2. Use Postgres queues or clear cursor state.
3. Prefer `FOR UPDATE SKIP LOCKED` for durable queue drains.
4. Record observability rows or logs for every meaningful mutation.
5. Add a launcher script and compose service only when it should run by default.

## 17. Architectural Risks and Active Edges

| Risk | Why it matters | Where to look |
|---|---|---|
| Gateway is large | Many product adapters and legacy routes live in one file, increasing coupling. | [services/gateway/main.py](services/gateway/main.py), route modules under [services/gateway](services/gateway). |
| Worker deployment gap | Some worker modules still exist as available packages before becoming default compose services. Topology sweeper, Sage structural features, Sage topology optimization, and relationship ontology proposal aggregation are now launched by compose and local scripts, but other implemented workers remain environment-dependent. | [services/workers](services/workers), [docker-compose.yml](docker-compose.yml). |
| Dev auth shortcuts | Static tokens/default tenant are convenient but easy to misconfigure in shared envs. | `.env.example`, gateway public path config. |
| Handler registration drift | Handler files and trust map can diverge from imported registered handlers. | [services/ingestion/handlers/__init__.py](services/ingestion/handlers/__init__.py). |
| Spec references are historical | Many docstrings reference older `ARCHITECTURE-FINAL.md`, `SCHEMA-LOCK.md`, and `CONTRACTS.md` files not present in this checkout. | Code and migrations are the effective source of truth. |
| Mixed old/new UI API surfaces | `/view/ceo/*`, `/today/*`, `/model/*`, spec routes, and legacy redirects coexist. | [ui/src/main.tsx](ui/src/main.tsx), [services/gateway](services/gateway). |
| RLS vs app-level tenancy | RLS policies exist, but most correctness still depends on passing tenant IDs through app code. | migrations `0036`-`0041`, repositories. |
| Sage canonical-mutation queue | The topology optimizer surfaces canonical merge/split/promote/demote candidates but does not yet write them; the validation/promotion queue is a stub. | [services/sage/topology_optimizer](services/sage/topology_optimizer). |
| Ontology promotion governance | Edge-type candidates now cluster into proposal rows and accepted proposals are writable through debug/operator review, but there is no product-grade review workflow, audit surface, or label/facet management yet. | [services/relationships/ontology_proposals.py](services/relationships/ontology_proposals.py), `relationship_ontology_proposals`, `/debug/relationship-ontology-proposals`. |
| Structural-feature freshness | Sage structural gates and subgraph selection assume `model_structural_features` and `model_edge_structural_features` are reasonably current; missing features degrade to neutral gating but stale features can subtly mis-rank. | [services/sage/structural_features](services/sage/structural_features), migration `0050`. |
| Discovery Utility Layer drift | Affordances, shortcuts, and negative memory are loose-ref and time-decayed. If the optimizer runs too rarely or expiry is too aggressive, reader recall can quietly degrade. | [services/sage/topology_optimizer/optimizer.py](services/sage/topology_optimizer/optimizer.py), migrations `0051`–`0053`. |

## 18. Source of Truth

When code and docs disagree, prefer this order:

1. Database migrations in [db/migrations](db/migrations).
2. Repository/service implementations under [services](services) and [lib](lib).
3. Route wiring in [services/gateway/main.py](services/gateway/main.py) and [ui/src/main.tsx](ui/src/main.tsx).
4. Tests under [services](services), [tests](tests), and [ui/src/tests](ui/src/tests).
5. Design documents such as this one.

This document is a map, not a lockfile. Update it when new routes, queues, worker deployments, schema families, or UI surfaces become first-class.
