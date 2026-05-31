# Fyralis Core: Model, Think, and Retrieval Deep Dive

Last reviewed against the codebase on 2026-05-30.

Fyralis is an organizational intelligence runtime. The three layers below form a closed cognitive loop:

```
signal ──► retrieval ──► think ──► models/edges/acts/resources ──► (retrieval reads them next time)
```

Everything else (gateway, UI, demo, ingestion handlers, CEO view) is plumbing around this loop.

---

## 1. The Model / Synthesis Layer — *the memory substrate*

This is the durable, queryable representation of "what the company believes is true."

### 1.1 What a Model *is*

A row in [models](db/migrations/0001_foundation.sql#L99-L145) (extended through migration [0047](db/migrations/0047_memory_grammar_and_composition.sql)). It carries four orthogonal dimensions:

| Dimension | Fields | Purpose |
|---|---|---|
| **Content** | `proposition` (JSONB, discriminated by `kind`), `natural` (text), `embedding` (VECTOR(768)) | The claim itself |
| **Scope** | `scope_actors`, `scope_entities`, `scope_temporal` (+ normalized sidecars `model_scope_actors`, `model_scope_entities`) | Who/what/when this claim is about |
| **Epistemic** | `confidence` ∈ [0.05, 0.95], `confidence_at_assertion` (immutable), `activation`, `falsifier`, `signal_readings` | How strong, how fresh, how to disprove |
| **Provenance** | `born_from_event_id`, `supporting_event_ids`, `supporting_model_ids`, audit chain in `audit_events` | Why we believe it |

### 1.2 Proposition stances and claim roles

`proposition.kind` is now the four-stance ontology validated in [services/models/propositions.py](services/models/propositions.py): `observation`, `belief`, `prediction`, `norm`.

Subject semantics live in `claim_role` and the other memory-grammar axes. Legacy payloads such as `state`, `concern`, `pattern`, `situation`, and `recommendation` are accepted at the boundary and canonicalized into the four stances while preserving the old semantic bucket as `legacy_kind`.

[lib/shared/claim_role_registry.py](lib/shared/claim_role_registry.py) gives each `claim_role` a structural contract, so the system gets role-specific validation without returning to a large discriminator taxonomy.

`claim_role='situation'` is special — it's a **composite Model** referencing other Models via [model_composition_members](db/migrations/0047_memory_grammar_and_composition.sql) (e.g., "Acme's enterprise rollout is at risk" composing five member Models).

### 1.3 Memory grammar (the new orthogonal layer, migration 0047)

[lib/shared/memory_grammar.py](lib/shared/memory_grammar.py) derives **generated columns** from each Model's proposition + scope:

- `claim_role` — `fact | concern | hypothesis | prediction | pattern | situation | capability | relation | recommendation`
- `abstraction_level` — `atomic | relationship | composite | pattern`
- `time_mode` — `past | current | future | recurring | unspecified`
- `modality` — `observed | inferred | expected | normative`
- `polarity` — `positive | negative | mixed | neutral`
- `domain_tags` — text scanning into `{customers, finance, people, systems, execution, risk}`

This lets retrieval/rendering/topology filter memory by *structural role* without re-parsing JSONB. `proposition_kind` no longer has to carry every product decision.

### 1.4 The ModelsRepo pipeline ([services/models/repo.py](services/models/repo.py))

`ModelsRepo.insert()` is a 9-step pipeline:
1. **Falsifier adequacy** — required if confidence > 0.7. Five kinds in [services/models/falsifier.py](services/models/falsifier.py): `observation_pattern`, `commitment_outcome`, `prediction_deadline`, `resource_threshold`, `explicit_contestation`.
2. **Proposition shape** validated via Pydantic discriminated union plus the claim-role registry contracts.
3. **Confidence calibration** (Wave 4-C) and clipping to [0.05, 0.95].
4. **Scope actor existence** check.
5. **Embedding** — uses Ollama `nomic-embed-text` if not supplied.
6. **INSERT** with generated columns (memory grammar + `confidence_at_assertion` immutable snapshot).
7. **Scope sidecar sync** — mirrors JSONB into `model_scope_entities`/`model_scope_actors` for indexed reverse lookup.
8. **State-change observation** emitted ("a Model was born/changed").
9. **Audit event** in `audit_events`.

Other key operations:
- `retrieve(ids)` — **reconsolidation**: bumps `activation += 0.15`, increments `retrieval_count`, updates `last_retrieved_at`. **Never touches confidence.**
- `archive(model_id, reason)` — marks status, cascades by calling `EdgesRepo.mark_inert()` (all incident edges become inert), enqueues dependents into `model_reeval_queue`.
- `search_by_embedding`, `search_by_scope`, `get_predictions_due`, `bulk_confidence_update`.

### 1.5 Typed edges ([services/models/edges_repo.py](services/models/edges_repo.py))

[model_edges](db/migrations/0031_model_edges.sql) replaced older array-based relationships. Edges are **strictly Model-to-Model** (customers/commitments/etc. belong in `scope_entities`). Edge kinds include `supports, contradicts, weakens, causes, blocks, enables, predicts, explains, instance_of, superseded_by, co_occurs_with, analogous_to, alternative_to, early_warning_for, same_issue_as, contributes_to_resolution`.

Each edge carries `confidence`, `evidence_event_ids`, `evidence_model_ids`, `explanation`, `review_status` (`accepted | candidate | needs_review | rejected | retired`), `confirmed_count`, `decay_after`, `expires_at`. **Reconfirmation merges evidence and cannot downgrade review status.** Self-edges are rejected; DAG cycle checks run per kind.

### 1.6 Reconciliation ([services/think/reconciler.py](services/think/reconciler.py))

Before applying any `claim_op.insert`, the reconciler hunts for duplicates: candidates within 30 days, overlapping scope, same `proposition_kind`, cosine ≥ 0.70. Scoring blends cosine with shared evidence/supporting_models/falsifier semantics.

Decision thresholds:
- **auto_merge** (score ≥ 0.85) — insert becomes a confidence update + new confirmation reading.
- **human_review** ([0.70, 0.85)) — `reconciliation_events` row + `same_issue_as` candidate; insert still proceeds.
- **no_match** — pass-through.

Strict-schema live LLMs omit embeddings, so the reconciler uses a **deterministic lexical fallback embedding** to dedupe anyway.

---

## 2. The Retrieval Layer — *how memory is fetched and shaped for reasoning*

There are now **two stacked layers**: the legacy pathway resolver (low-level), and the active **Inquiry engine** (orchestration). Default since 2026-05-28 is `EXECUTION_RETRIEVAL_ENGINE=inquiry`.

### 2.1 Active entrypoint ([services/execution/inquiry.py](services/execution/inquiry.py))

`retrieve_for_execution(trigger, conn, embedder, llm_provider, route, mode, top_n, config)`:
- `mode="deep"` for Think transactions (2 inquiry rounds max).
- `mode="fast"` for Query/UI reads (baseline only).
- Returns `InquiryResult` (new) or `RetrievalResult` (legacy).

### 2.2 Legacy pathway resolver ([services/retrieval/primary.py](services/retrieval/primary.py), [pathways.py](services/retrieval/pathways.py))

Five pathways with trigger-aware weights:

| Pathway | What it queries | Weights (T1 / T2 / T4) |
|---|---|---|
| **A structural** | Scope/entity overlap, customer↔commitment bridge | 0.34 / 0.18 / 0.28 |
| **B semantic** | pgvector cosine on `models.embedding` | 0.34 / 0.18 / — |
| **C temporal** | Recent observations within window | 0.16 / — / — |
| **D pattern** | Pattern/background-kind Models | — / — / 0.42 |
| **G model-edge** | Typed `model_edges` traversal (active edges only) | 0.16 / **0.52** / 0.30 |

**T2 is intentionally graph-forward** — when reasoning about an existing belief, typed edges should outrank generic semantic neighbors. Scores combine via weighted decay (or RRF if `RETRIEVAL_SCORING_MODE=rrf`). Final tiebreak: `-score, -activation, id`.

Every returned Model triggers `ModelsRepo.retrieve()` reconsolidation (activation +0.15, count++).

### 2.3 Inquiry engine — the new active pipeline ([services/execution/inquiry.py:338-544](services/execution/inquiry.py#L338-L544))

The big move: instead of "run pathways, hand it to LLM," it runs an **adaptive question-conditioned loop**.

```
1. Baseline retrieval (primary_retrieve)            → seed EvidenceCards
2. Generate hypotheses (always includes H0: noise)  → competing claims about the signal
3. For each round (≤ 2):
   a. Score candidate questions (deterministic + optional LLM)
   b. Select up to 3 with diversity by primitive
   c. Compile each question into retrieval actions
   d. Execute in parallel via legacy pathways
   e. Upsert results into Evidence Reservoir
   f. Answer questions deterministically from evidence
   g. Sufficiency gate: stop or continue
4. Rank evidence by value density (usefulness / token_estimate)
5. Compile tiered Synthesis Context Packet
6. Attach packet to RetrievalResult.notes["inquiry"]
```

**Hypothesis templates** are heuristic: risk language → H1 ("real operational blocker"); commitment language → H2 ("affected commitment"); recurrence → H3 ("broader pattern"); H0 always present.

**Question primitives**: `DEPENDENCY, COMMITMENT_STATUS, OWNER, GOAL_IMPACT, CUSTOMER_IMPACT, COUNTEREVIDENCE, RECURRENCE, PATTERN, MODEL_EDGE, FALSIFIER, TIMELINE, HUMAN_VALIDATION`. Selection prioritizes counterevidence first, then high-value novelty.

**EvidenceCard** ([services/execution/inquiry.py](services/execution/inquiry.py)) is the core unit — keyed by `(source_type, source_ref)` so multi-path hits merge their provenance (`retrieval_paths: set[str]`, `retrieved_for_questions`, `supports_hypotheses`, etc.). Stored in-memory; persisted to [inquiry_evidence_items](db/migrations/0046_inquiry_execution.sql) when tenant exists.

**Sufficiency verdicts**: `sufficient_for_reasoning | insufficient_continue | insufficient_defer | human_validation_required | no_update_needed | budget_exhausted`.

**Synthesis Context Packet** (tiered, ~24k token budget):
- **Tier 0 mandatory frame** — trigger, hypotheses, baseline counts (always included)
- **Tier 1 decisive evidence** — counterevidence preserved
- **Tier 2 supporting groups** — summarized
- **Tier 3 background summaries**
- **Tier 4 omission ledger** — what was excluded and why

### 2.4 Routing gate ([services/execution/routing.py](services/execution/routing.py))

Deterministic, cheap scoring at ingestion time. Routes: `IGNORE_OR_ARCHIVE, DETERMINISTIC_UPDATE, FAST_PATH, DEEP_INQUIRY_PATH, BACKGROUND_PATH, HUMAN_VALIDATION_PATH`. Currently runs in **shadow mode** (`EXECUTION_ROUTING_SHADOW=1`) — decisions recorded in [signal_routing_decisions](db/migrations/0046_inquiry_execution.sql) but T1 enqueue is preserved. Enforcement is a future rollout milestone.

### 2.5 Assembler ([services/retrieval/assembler.py](services/retrieval/assembler.py))

Compresses `RetrievalResult` → `ContextBundle` for the LLM:
- Access-control filtering
- Hard caps (12 obs, 24 models, 10 acts, 5 resources) — **never truncate mid-item**
- MMR diversity selection
- **Relevance anchors** preserve top-5 retrieved Models before diversity pressure
- **Graph anchors** preserve top-3 Pathway G hits
- Bridge context (customer/commitment/resource linkage)
- `bundle.notes["model_selection"]` carries prompt-survival telemetry for quality audits

### 2.6 Query layer ([services/query/core.py](services/query/core.py))

Used by `/view/ceo/ask`. Classifier picks strategy → strategy calls `retrieve_for_execution(mode="fast", route="FAST_PATH")` → assembler → rendering. Returns `AnswerQueryResponse` with HTML, retrieval trace, and cost.

---

## 3. The Think Pipeline — *the cognitive transaction*

This is the layer that *changes* the memory substrate. Everything else is read-mostly.

### 3.1 Worker ([services/think/worker.py](services/think/worker.py))

Polls [think_trigger_queue](db/migrations/0001_foundation.sql) and `model_reeval_queue` with `FOR UPDATE SKIP LOCKED`. Key safety:
- **Per-tenant `asyncio.Semaphore`** (default 1). Higher values exposed model-row deadlocks in production.
- **Bounded leasing** — only leases as many rows as in-flight slots remain, so a busy tenant doesn't lock rows for everyone else.
- **Priority**: T4 latent_relationship_candidate > T2 > T4 others > T3 > T1.
- **Heartbeats** refresh locks; failures retry with exponential backoff up to `THINK_TRIGGER_MAX_ATTEMPTS=5`.
- **Cascade depth bound** (TK-3): payloads carry `cascade_depth`; if ≥ 50, trigger fails non-retryable.

### 3.2 Trigger kinds and the ReasoningFrame

| Kind | Source | Frame question | Allowed ops |
|---|---|---|---|
| **T1** | Ingestion | "What changed, which Models connect, what action warranted?" | claims, edges, acts, resources |
| **T1:state_change** | Cascade | Deterministic bookkeeping | minimal |
| **T2:belief_updated** | Reconciliation cascades | "What downstream beliefs/edges shift?" | claims, edges, acts (no resources). Often **deterministic-only** unless graph anchors present |
| **T2:prediction_***| Deadline checks | Resolve prediction | deterministic |
| **T3** | Anomaly processor | "What explains this anomaly?" | claims, edges only |
| **T4:latent_relationship_candidate** | Topology sweeper | "Is this candidate edge/situation real?" | claims, edges only |
| **T4:model_reeval / pattern_review / maintenance** | Background workers | varies | varies |
| **T6** | Legacy topology | retired | minimal |

`ReasoningFrame.from_trigger()` normalizes all of these into `frame_kind, stimulus_kind, question_to_answer, seed_model_ids, seed_entity_ids, allowed_ops, budget, policy` — a structured contract the prompt and validator both honor.

### 3.3 The Think transaction ([services/think/reason.py](services/think/reason.py))

`_run_once()` orchestrates ~18 ordered steps inside a single DB transaction:

1. **Retrieve** via `retrieve_for_execution()` → `InquiryResult` (with packet).
2. **Capture debug artifacts** to `think_run_artifacts` (stages: trigger, retrieval, inquiry, sufficiency, context_packet, prompt, response, validation, apply) when `DEBUG_ARTIFACT_CAPTURE=1`.
3. **Optional second pass** for sparse/bridge cases.
4. **Dynamic signal detection** ([services/dynamics/detectors.py](services/dynamics/detectors.py)) reads existing audit/topology/observations for oscillation, recurring updates, stale memory, high-actor-activity. **Important:** these are ephemeral — they're surfaced into the frame, not persisted as a separate "dynamics" table.
5. **Build ReasoningFrame** from trigger + dynamic signals.
6. **Compute region lock key** — extract (type, id) pairs from retrieval, deterministically pick a primary entity (TK-4 — commitment > goal > decision > resource/customer > actor, then id asc), hash to (tenant_hash, entity_hash).
7. **Insert `think_runs` row** with initial status + region hashes.
8. **Acquire region lock** — `pg_advisory_xact_lock(tenant_hash, entity_hash)` inside the transaction.
9. **Assemble context bundle** with MMR + anchors + access filter.
10. **Dispatch**:
    - `is_authoritative(trigger)` → deterministic handler in [services/think/deterministic.py](services/think/deterministic.py)
    - Otherwise → [llm_reason()](services/think/llm_reason.py) which picks `RawDiffClaimsOnly` (cheaper, ~3.5k floor) vs full `RawDiff` (~4.9k floor) based on whether graph/act/resource context is present
11. **Validate** ([services/think/validator.py](services/think/validator.py)): falsifier adequacy, confidence clipping, scope existence, act-state legality (`can_transition`), threshold checks, `doneverified` semantics (authoritative trust required), region containment. **Partial-accept**: drop failing ops, continue; only raise if all fail.
12. **Compute context-use telemetry** for the validated diff against the bundle.
13. **Reconcile** claim inserts (auto-merge / human-review / no-match).
14. **Apply** ([services/think/applier.py](services/think/applier.py)):
    - Idempotency check against `applied_triggers`
    - Order: claim_ops → edge_ops → act_ops → resource_ops
    - **Same-diff placeholders**: edge/act endpoints may reference an inserted Model's `born_from_event_id` — the applier rewrites to the actual inserted UUID
    - LLM-invented persistent ids stripped
    - `superseded_by` edges canonicalized old→new
    - Self-edges (after rewriting) skipped
15. **Emit state-change observations** and **audit events** for each mutation.
16. **Cascades** ([services/think/cascade.py](services/think/cascade.py)) — BFS bounded at depth 50: unblock dependent commitments, recompute goal/customer health, flag decision-constrained commitments for review, inform resource deployments.
17. **Enqueue post-commit actions** inside the same transaction (`pending_post_commit_actions` — durable side effects: anomaly publish, prediction scheduling, WebSocket broadcast, metrics invalidation).
18. **Record cost** (`think_run_costs`) and **finalize `think_runs`** with status, ops applied, context-use telemetry, elapsed time.

### 3.4 Prompt construction ([services/think/prompt.py](services/think/prompt.py))

System prompt encodes reasoning discipline, falsifier rules, full diff schema, proposition kinds, situation compositional fields, scope rules. User prompt sections:
- `<triggering_event>`
- `<reasoning_frame>`
- `<retrieved_context>` with `<observations>` (4k char), `<models>` (4k), `<acts>` (12k), `<resources>` (1k)
- `<actor_context>` from [services/actors/operating_context.py](services/actors/operating_context.py) — actor-scoped Models/commitments/blocks/concerns synthesized into the prompt instead of an `actor_model` proposition kind
- `<bridge_context>` (customer↔commitment↔resource)
- `<inquiry_context_packet>` when present
- `<operating_instructions>` — source-tuned working stance (channel/trust/trigger-aware)

### 3.5 Diff schema ([services/think/diff_schema.py](services/think/diff_schema.py))

Four mutation buckets:
- `claim_ops` — insert/update/archive Models
- `edge_ops` — add/retire typed `model_edges`
- `act_ops` — create/transition/edge for goals, commitments, decisions
- `resource_ops` — create/transaction/deploy/release/update

### 3.6 The deterministic safety net ([services/think/auto_create_commitment.py](services/think/auto_create_commitment.py))

Idempotent narrow rules for cases where live LLMs are semantically close but operationally incomplete: self-reported new work → `create_commitment` recommendation; blocked/on-hold → transition + concern; decision revisit signals → concern + `transition_decision`; explicit future-dated plans → split into prediction with deadline falsifier; customer churn/renewal-risk → customer-scoped concern Model.

### 3.7 Context-use telemetry ([services/think/context_use.py](services/think/context_use.py))

After validation, grades whether the diff actually *referenced* selected/graph-selected context:
- `graph_context_used`, `model_context_used`, `observation_context_used` — diff cited those Models/observations
- `justified_noop_context_used` — empty diff but `reasoning_trace` cites exact UUIDs (a thoughtful no-op, not a blind null)
- `unused_selected_context` — diff ignored what retrieval surfaced
- `no_selected_context` — retrieval returned nothing relevant

Stored in `think_runs.ops_applied["context_use"]`. Powers `/debug/think-quality` and replay-case extraction in [services/think/quality_report.py](services/think/quality_report.py) / [quality_promoter.py](services/think/quality_promoter.py).

### 3.8 Post-commit worker ([services/think/post_commit.py](services/think/post_commit.py))

Why durable: side effects used to run inline after commit — a worker crash between commit and side-effect lost them silently. Now `enqueue_post_commit_actions()` writes rows inside the apply transaction; a separate worker polls with `FOR UPDATE SKIP LOCKED`, retries with backoff, dead-letters after 5 attempts. Dedup via `UNIQUE NULLS NOT DISTINCT (tenant_id, trigger_id, action_kind)`.

---

## 4. How they relate — *the data flow*

```
                                  ┌───────────────────────────────────────┐
                                  │           Models / Edges /            │
                                  │      Acts / Resources / Audit         │ ◄──┐
                                  │            (the substrate)            │    │
                                  └───────────────┬───────────────────────┘    │
                                                  │ reads                       │
ingestion → observation → routing → think_trigger_queue                         │
                              │             │                                    │
                       (shadow rec.)        ▼                                    │
                                    ┌──────────────────┐    ┌──────────────┐    │
                                    │   ThinkWorker    │───►│ retrieval/   │    │
                                    │  (semaphore,     │    │ inquiry      │    │
                                    │   priority,      │    │ engine       │    │
                                    │   leasing)       │    │              │    │
                                    └────────┬─────────┘    │ pathways A/B │    │
                                             │              │ /C/D/G +     │    │
                                             ▼              │ hypotheses + │    │
                                       reason._run_once     │ questions +  │    │
                                       (region-locked       │ evidence +   │    │
                                        transaction)        │ packet       │    │
                                             │              └──────┬───────┘    │
                                             ▼                     │            │
                                       LLM (or det.)               │            │
                                       → RawDiff                   ▼            │
                                             │             ContextBundle        │
                                             ▼             + packet → prompt    │
                                       validator                                 │
                                             │                                   │
                                             ▼                                   │
                                       reconciler                                │
                                             │                                   │
                                             ▼                                   │
                                       context_use telemetry                     │
                                             │                                   │
                                             ▼                                   │
                                       applier ──────────────────────────────────┤
                                             │           writes                  │
                                             ▼                                   │
                                       cascades + state_change observations ─────┘
                                             │
                                             ▼
                                       post_commit (durable side effects)
```

### Concretely

- **Retrieval reads Models, Edges, Acts, Resources, Observations** and produces a `ContextBundle` (+ tiered packet). It also writes back via `ModelsRepo.retrieve()` reconsolidation (activation bumps).
- **Think reads** that bundle, frames the trigger, calls the LLM (or deterministic handler), validates, reconciles, then **writes** to the Model/Synthesis substrate.
- **Every Model write** generates a state-change observation, which can enqueue cascade T2/T4 triggers, which re-enter the loop.
- **The Inquiry packet** bridges retrieval's adaptive search with Think's prompt — it's the structured "what we found and why" that the LLM reasons over.
- **Context-use telemetry** closes the quality loop: did the diff actually use what retrieval surfaced? Failing rates feed `/debug/think-quality` and the replay-case promoter.
- **Region locks** ensure two Think transactions touching the same commitment/customer/etc. serialize at the substrate-mutation layer without locking the whole tenant.
- **Reconciliation, falsifiers, memory grammar, typed edges, situation composition, scope sidecars** are all substrate-side invariants that exist *because* Think writes can be wrong, redundant, or under-specified — they keep the substrate coherent without requiring perfect LLM output.

### The product insight encoded by this architecture

The system separates **what** is true (Models with falsifiers + audit chains), **how strongly** we believe it (confidence/activation/calibration), **why** we believe it (provenance + signal readings), **how it connects** (typed edges + composition), and **how we find it** (retrieval pathways + inquiry packet). Each can evolve independently — which is why the recent revamp could swap retrieval from a static 5-pathway resolver to an adaptive question-conditioned loop without touching the Model schema, validator, or applier.
