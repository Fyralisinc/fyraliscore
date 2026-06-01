# Glossary

The proprietary/domain vocabulary of Fyralis Core. Definitions here are
**derived from the code** (types, docstrings, table schemas, prompt builders) —
they describe what a term *does* in the system.

!!! warning "On certainty"
    Terms marked **TODO(human)** below have a definition that the code *implies*
    but does not pin down — the precise intended meaning needs an author's
    confirmation. Everything else is unambiguous from source. If you find a
    definition that contradicts current code, fix it in the same PR.

## Propositions & memory

The epistemic substrate — what the system believes and how beliefs are structured.

| Term | Definition (code-derived) |
|------|---------------------------|
| **Model** | A first-class belief/proposition about the organization, with confidence, falsifiers, supporting evidence, and typed edges to other Models. Stored in `models`. The central abstraction of the system. |
| **Proposition** | The semantic payload of a Model — the claim itself, expressed as a discriminated union over *proposition kinds*. Legacy kinds (state, relation, concern, recommendation, …) are normalized into the canonical stances. |
| **Proposition kind** | The epistemic stance discriminator: `observation` (past fact), `belief` (current inferred), `prediction` (future expected), `norm` (normative/recommendation). |
| **Confidence** | Credence on a Model, clipped to `[0.05, 0.95]` at insert. The original `confidence_at_assertion` is immutable; calibration can adjust live confidence without rewriting it. |
| **Activation** | Recency/importance on a Model `[0, 1]`. Raised by retrieval (+0.15, capped), decayed exponentially (≈5-day half-life). Low-activation, long-untouched Models are archived. |
| **Falsifier** | A condition that would contradict a Model's claim; mandatory for high-confidence Models (≥0.7). Kinds: `observation_pattern`, `commitment_outcome`, `prediction_deadline`, `resource_threshold`, `explicit_contestation`. |
| **Memory grammar** | Five structural axes that classify a Model's role in synthesis independent of proposition kind: **claim role**, **abstraction level**, **time mode**, **modality**, **polarity**. |
| **Claim role** | Grammar axis: `fact`, `concern`, `hypothesis`, `prediction`, `pattern`, `situation`, `capability`, `relation`, `recommendation`. |
| **Abstraction level** | Grammar axis: `atomic`, `relationship`, `composite`, `pattern`. |
| **Time mode** | Grammar axis: `past`, `current`, `future`, `recurring`, `unspecified`. |
| **Modality** | Grammar axis (evidence type): `observed`, `inferred`, `expected`, `normative`. |
| **Polarity** | Grammar axis (valence): `positive`, `negative`, `mixed`, `neutral`. |
| **Signal reading** | A sub-claim within a Model (`signal_readings`) tracking which Observations contributed evidence; can be contested independently. |
| **Scope** | The organizational context a Model applies to — a tuple of `scope_actors` and `scope_entities` (normalized into `model_scope_actors` / `model_scope_entities`). |
| **Embedding** | A 768-dimensional semantic vector (`nomic-embed-text` via Ollama), computed at ingest. Used for semantic retrieval (Pathway B) and reconciliation. |

## Acts & execution primitives

The executable organizational state.

| Term | Definition (code-derived) |
|------|---------------------------|
| **Act** | Any executable organizational primitive: a **Goal**, **Commitment**, or **Decision**. Each has a state machine (in `services/domain/acts`). |
| **Goal** | An aspirational/operational outcome with target date, parent, and altitude (strategic/operational); state machine `active → archived`. |
| **Commitment** | A work item or promise with owner, due date, and ambition level; state machine `proposed → active → resolved/terminal`. Bridges to customers and decisions. |
| **Decision** | A binary or multi-option choice point with a revisit mechanism; state `drafted → active → revisited → archived`. |
| **Resource** | An asset or capacity (financial, relational, IP, regulatory, infrastructure), tracked via transactions and deployments. |

## Observations & ingest

| Term | Definition (code-derived) |
|------|---------------------------|
| **Observation** | An ingested signal event — a fact from a source channel with timestamp, content, actor, and trust tier. Stored in the partitioned `observations` table. The source of a T1 trigger. |
| **Signal** | Synonym for an Observation in the ingestion context: raw information flowing in from an external source. |
| **ObservationDraft** | The normalized intermediate shape a handler emits before persistence: source channel, content text, raw JSON, actor ref, external ID, occurred time, trust tier, entity hints, kind. |
| **Trust tier** | A per-channel confidence label (`CHANNEL_TRUST_MAP`), e.g. authoritative / high / medium / low; influences Think prompt routing. |
| **Entity alias** | A fast-path text→entity resolution (e.g. "NBI" → Nimbus Bank), stored with an embedding in `entity_aliases`; unresolved phrases are deferred for LLM resolution. |
| **Per-source ingestion lane** | The Kafka topic quad (`raw`, `normalized`, `embedding`, `dlq`) per source, isolating lag/failures between sources. |
| **Control-plane topic** | `ingestion.tenant_traffic_signal` — carries per-tenant coordination signals (not per-source data). |
| **`embedding_pending`** | Flag set on an Observation when embedding failed; the embedding-backlog worker retries these with Redis rate-limiting. |

## Reasoning pipeline

| Term | Definition (code-derived) |
|------|---------------------------|
| **Think** | The core reasoning pipeline: retrieve context → LLM reason → validate diffs → reconcile duplicates → apply → cascade. |
| **Think trigger** | An enqueued reasoning request (kind + payload) in `think_trigger_queue`, polled by the Think worker. |
| **T1** | Ingestion trigger — a new Observation arrived. Carries seed entities/text/time and scope actors. |
| **T2** | Prediction-reevaluation trigger — a Model's `evaluate_at` deadline passed (often deterministic bookkeeping). |
| **T3** | Anomaly trigger — a detected organizational anomaly; carries a region spec for focused retrieval. |
| **T4** | Background/maintenance trigger — topology relationship candidates, precipitation proposals, reeval promotions. |
| **T6** | Legacy topology-event trigger from the retired accepted-memory graph; kept for backward compatibility. |
| **Retrieval** | The context-gathering phase of Think across six pathways plus inquiry hypotheses; routes through inquiry (default) or a legacy resolver. |
| **Pathway** | One of the retrieval strategies — **A** structural, **B** semantic, **C** temporal, **D** pattern, **G** model-edge — weighted per trigger kind. |
| **Inquiry** | The adaptive retrieval engine: baseline seeding → hypotheses → discriminating questions → evidence reservoir → sufficiency gate → context packet. (Lives in `services/platform/execution`.) |
| **Diff** | The LLM output structure: `claim_ops`, `edge_ops`, `act_ops`, `resource_ops` — validated, reconciled, then applied atomically. |
| **Claim op** | A Model mutation: `insert`, `update` (shallow merge), or `archive` (with reason). |
| **Edge op** | A Model-relationship mutation: create / retire / reconfirm; may reference same-diff claim placeholders. |
| **Act op** | A Goal/Commitment/Decision mutation (`create_*`, `update_*`, `transition_*`, `add_edge_*`), validated against confidence-basis thresholds. |
| **Resource op** | A Resource/transaction mutation: acquire, deploy, release, spend, expire. |
| **Reconciliation** | Content-level dedup *before* apply — matches by embedding cosine + scope + proposition kind + recency, deciding `auto_merge` / `human_review` / `no_match`. Logged to `reconciliation_events`. |
| **Audit** | The immutable event log (`audit_events`) of Model state changes, reversals, and reconciliation decisions. |
| **Post-commit** | The durable side-effect queue (`pending_post_commit_actions`) executed after Think commits — cascades, reevals, alerts — with retry/backoff in the post-commit worker. |
| **Context bundle** | The compressed retrieval result handed to the LLM: selected Models, Observations, Acts, Resources, bridge context, with access-control filtering and MMR diversity. |
| **Deterministic handler** | A Think path that resolves a trigger without an LLM call (e.g. T2 cascade drain). **TODO(human):** confirm exactly which trigger classes are guaranteed deterministic and what invariant that preserves. |
| **Anomaly** | A detected organizational signal anomaly (contradiction, silence, velocity), staged in `think_anomalies_raw` and enqueued as T3. **TODO(human):** specify which concrete signals trigger a T3 enqueue and the detection thresholds. |
| **Reasoning frame** | A normalized representation of what a Think transaction should answer, derived from the trigger. **TODO(human):** confirm the intended scope/semantics. |

## Relationship intelligence (topology)

| Term | Definition (code-derived) |
|------|---------------------------|
| **Topology** | The *latent relationship field* — generates pre-truth relationship/situation candidates from impact signatures. The active upstream layer; legacy accepted-memory positional embeddings are retired. |
| **Topology sweeper** | A background worker that periodically reruns the latent relationship field over high-activation Models so older memory can connect to newer. |
| **Model edge** | A first-class directed/symmetric relationship between two Models (`model_edges`), with confidence, evidence, and review status. |
| **Edge kind** | The edge-semantics discriminator: `supports`, `contradicts`, `causes`, `blocks`, `enables`, `predicts`, `early_warning_for`, `same_issue_as`, `analogous_to`, … |
| **Relationship candidate** | A pre-acceptance hypothesis for an edge or situation, generated by topology and scored by judgment; awaits T4 Think adjudication before becoming an edge op. |
| **Situation** | A composite Model whose `member_model_ids` bridge multiple pairwise edges into one operational reality. |
| **Judgment leverage** | A composite score (impact, uncertainty, urgency, actionability, authority required, novelty, reversibility, confidence) ranking candidates by human-judgment worthiness. |
| **Precipitation** | Background pattern clustering that proposes composite Models / pattern-instance edges (a T4 worker). **TODO(human):** document the clustering algorithm and proposal thresholds. |
| **Edge drift** | A worker concerned with model-edge change over time. **TODO(human):** confirm precisely what `edge_drift` detects and acts on. |

## Contestability & calibration

| Term | Definition (code-derived) |
|------|---------------------------|
| **Contestation** | A first-person override of a Model claim via `contest_model()`. Types: `direct` (whole claim), `reading` (single sub-claim), `implicit` (anomaly). Reduces confidence via multipliers. |
| **Standing** | The condition for an actor to contest a Model: scope-actor membership, resource/commitment ownership, or a manager-chain relationship. |
| **Calibration** | Hit-rate tracking per claim class. Read-only and conservative: returns `None` rather than fabricating an estimate when data is insufficient. |
| **Forecast** | A `prediction`-kind Model carrying `evaluate_at` and `resolution_criteria`; resolves true/false at its deadline. |

## Product & CEO view

| Term | Definition (code-derived) |
|------|---------------------------|
| **CEO view** | The cached, pre-rendered organizational snapshot (`view_ceo_cache`) — greeting, cards, query grid, status — precomputed by the greeting scheduler. |
| **Greeting** | The opening summary prose in the CEO view, composed from top Models/Commitments/Resources; cached under `view_ceo_cache['greeting']`. |
| **Card** | A CEO-facing micro-surface showing a curated insight (observation / decision / question), selected for relevance and diversity. |
| **Query grid** | Suggested questions derived from active Models and CEO context, rendered as chips. |
| **Ask** | The interactive follow-up surface where the CEO poses questions; routes through classifier → strategy → retrieval + rendering (shares the execution-layer Fast Path). |
| **Rendering** | The LLM-backed prose generation service; builds prompts for greetings/cards/queries with voice-rule enforcement and retry; writes `view_render_costs`. |
| **Today surface** | The CEO briefing/review page aggregating recommendations, severity, tags, and decision deltas (severity ≈ `expected_impact × confidence`). |
| **Recommendation** | A `norm`-kind Model proposing a state change (proposed change, target actor, confidence basis); bridges to decision deltas on Today. |
| **Decision delta** | A first-class proposed-change object surfacing before/after state, falsification, consequence preview, and evidence chain as an independently reviewable artifact. |
| **Ledger** | The historical view page showing audit/reconciliation/state-change events chronologically. |
| **Model map** | A visualization of the Model graph (typed edges, situation compositions, density) built from `model_edges` and situation membership. |
| **Forecasts page** | A three-tab surface: active predictions, resolved outcomes, calibration accuracy. |
| **Demo** | The anonymous multi-tenant sandbox: provisions fresh tenants, loads snapshots, mints CEO actors, and manages session tokens. |

## Platform, access & runtime

| Term | Definition (code-derived) |
|------|---------------------------|
| **Tenant** | The logical isolation boundary; nearly every row carries `tenant_id`. Gateway auth resolves a bearer token to a tenant. RLS exists, but application-level scoping is the primary discipline. |
| **Bearer auth** | `Authorization: Bearer <token>` validated against `actor_sessions`, resolving `(actor_id, tenant_id, session_id, expires_at)`. |
| **Five-layer access control** | The sequential `can_read` decision: (1) tenant isolation [mandatory], (2) observation scope, (3) act ownership, (4) resource-kind roles, (5) model visibility + admin/leadership override. |
| **Manager chain** | The ancestor list following `actors.metadata.manager_id` pointers (with cycle guards), enabling observation visibility for non-HR channels. |
| **Shared channel** | A channel marked in `shared_channels` (or implicitly `internal:*`/`system:*`) that actors with `audience='all'` can see. |
| **HR channel** | A `hr:` / `legal:` / `incident:` channel, exempt from manager-chain and shared-channel visibility and from admin overrides. |
| **Execution routing** | A deterministic gate (`decide_route`) classifying each signal into one of six routes; records shadow decisions to `signal_routing_decisions`. |
| **Routing route** | `IGNORE_OR_ARCHIVE`, `DETERMINISTIC_UPDATE`, `FAST_PATH`, `DEEP_INQUIRY_PATH`, `BACKGROUND_PATH`, `HUMAN_VALIDATION_PATH`. |
| **Shadow mode / enforced mode** | `EXECUTION_ROUTING_SHADOW=1` (default) records routing decisions without changing Think enqueue; enforced mode gates the queue and enables T3/T4 work. |
| **Inquiry sufficiency** | The evidence-loop gate: `sufficient_for_reasoning`, `human_validation_required`, `no_update_needed`, `budget_exhausted`. |
| **Context packet** | The synthesis input compiled by the sufficiency gate: frame, decisive evidence, supporting groups, background, omission ledger. |
| **Materialized view** | `actor_visible_commitments/goals/models`, refreshed nightly and on role/hierarchy changes, as a fast path for common visibility queries. |
| **RLS (Row-Level Security)** | PostgreSQL policies (migrations ~0036–0041); present but application-level tenant filtering remains the primary enforcement. |

> **TODO(human):** A few subsystem code-names carry ticket prefixes (e.g. **IN-08**
> secret store + tenant resolver) whose product intent isn't fully recoverable
> from code. If these are durable concepts, define them here; otherwise note that
> they're internal milestone tags.
