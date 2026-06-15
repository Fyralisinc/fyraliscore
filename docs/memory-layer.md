# The Memory Layer — Implementation Reference

*Status snapshot: `main` @ `be468b6` ("Model-layer quality chain: split →
reconcile → quality gate → topology init", PR #42), 2026-05-26.*

This document is a **verbose, implementation-level reference** for the memory
layer of Company OS. It explains not just *what* exists but *why each piece of
logic is there* and *what problem it solves*. It is written against the code as
it stands on `main` after the model-layer quality-chain buildout and the
topology rewrite (latent field + sweeper). Where a file/line is named, it is a
clickable reference relative to the repository root.

> **Reading order.** §1–§5 describe the **memory substrate** (how beliefs are
> stored, related, and audited). §6 describes the **model layer / Think
> pipeline** (how observations become beliefs through the quality chain). §7
> describes **topology** (latent relationship discovery). §8–§9 cover the
> **supporting epistemic subsystems** and the **validation harness**. §10 is a
> consolidated **configuration reference**. §11 is a **file map**.

---

## 1. What the memory layer is

The "memory layer" is the durable epistemic substrate of Company OS. It is built
around **Models** — one of the Four Foundations:

| Foundation | Meaning | Primary table |
|---|---|---|
| **Observations** | Append-only empirical signals (Slack, GitHub, email, calendar, finance, …) | `observations` (time-partitioned) |
| **Models** | *The memory.* Epistemic beliefs: states, relations, hypotheses, patterns, predictions, concerns, assessments — each with confidence, a falsifier, provenance, and a lifecycle | `models` |
| **Acts** | Goals / Commitments / Decisions | `goals`, `commitments`, `decisions` |
| **Resources** | Organizational assets | `resources` |

**Universal Flow Rule:**
`input → Observation → Think → always Models, sometimes Acts/Resources`.

The thing that "remembers" is the accreting, decaying, contested, superseded,
and reconciled set of **Models**. A Model is *not* a fact — it is a *belief with
an epistemic posture*: it carries a confidence in `[0.05, 0.95]`, a falsifier
(the condition that would prove it wrong), the evidence that supports it, and a
status that moves through a defined lifecycle.

Two distinct concerns therefore live in this layer:

1. **The substrate** — the storage model for beliefs and the relationships
   between them: the `models` table, the typed `model_edges` graph, the
   `audit_events` chain, and the `observations` input store. (§2–§5)
2. **The model layer / Think pipeline** — the machinery that turns observations
   into well-formed beliefs and keeps the belief set coherent: reasoning,
   splitting, reconciliation, quality-gating, application, cascade, and latent
   topology. (§6–§7)

The canonical end-to-end validator is the **synthesis harness**
([tests/synthesis_harness/__main__.py](tests/synthesis_harness/__main__.py)),
which describes itself as *"black-box tests for the memory layer."* (§9)

---

## 2. The Models substrate — storage of beliefs

### 2.1 The `models` table

Defined in [db/migrations/0001_foundation.sql](db/migrations/0001_foundation.sql)
and extended by
[db/migrations/0002_models_amendments.sql](db/migrations/0002_models_amendments.sql).
Every column exists for a reason; they are grouped below by concern.

```sql
CREATE TABLE IF NOT EXISTS models (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  born_from_event_id UUID NOT NULL,           -- the observation that created it

  -- Content
  proposition JSONB NOT NULL,                 -- kind-discriminated structured claim
  "natural" TEXT NOT NULL,                    -- human-readable rendering
  embedding VECTOR(768) NOT NULL,             -- semantic vector (Ollama / nomic, 768-d)

  -- Scope (who/what/when the belief is about)
  scope_actors UUID[] DEFAULT '{}',
  scope_entities JSONB DEFAULT '[]'::jsonb,   -- list of {type, id}
  scope_temporal JSONB NOT NULL,

  -- Epistemic posture
  confidence FLOAT NOT NULL CHECK (confidence >= 0.05 AND confidence <= 0.95),
  activation FLOAT NOT NULL DEFAULT 1.0,      -- "heat" / recency of use
  falsifier JSONB,                            -- the condition that disproves it

  -- Signal readings (per-signal interpretations that can be individually contested)
  signal_readings JSONB DEFAULT '[]'::jsonb,
  reading_contestable BOOLEAN DEFAULT TRUE,

  -- Provenance
  supporting_event_ids UUID[] DEFAULT '{}',   -- observations that back it
  supporting_model_ids UUID[] DEFAULT '{}',   -- other models that back it
  evidential_weight FLOAT DEFAULT 0.5,

  -- Lifecycle
  status TEXT NOT NULL DEFAULT 'active',
  archived_at TIMESTAMPTZ,
  archive_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_retrieved_at TIMESTAMPTZ,
  retrieval_count INTEGER DEFAULT 0,

  -- Prediction-specific
  evaluate_at TIMESTAMPTZ,                     -- when a prediction should be resolved
  resolution_criteria JSONB,
  contributing_models UUID[] DEFAULT '{}',

  -- Access
  visible_to_subjects BOOLEAN DEFAULT TRUE
);
```

**Amendments** (migration 0002) add the columns the model layer leans on for
calibration, confirmation tracking, and prediction resolution:

```sql
-- A1: hot-path discriminator, generated from the JSONB so it never drifts
ALTER TABLE models ADD COLUMN proposition_kind TEXT
  GENERATED ALWAYS AS (proposition->>'kind') STORED;

-- A2: confirmation / contestation counters
ALTER TABLE models ADD COLUMN confirmed_count   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE models ADD COLUMN contested_count   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE models ADD COLUMN last_confirmed_at TIMESTAMPTZ;

-- A3: immutable snapshot of the confidence the belief was *born* with
ALTER TABLE models ADD COLUMN confidence_at_assertion FLOAT NOT NULL;  -- range [0.05,0.95]

-- A4: prediction resolution (resolved_at and outcome must be set together)
ALTER TABLE models ADD COLUMN resolved_at        TIMESTAMPTZ;
ALTER TABLE models ADD COLUMN resolution_outcome BOOLEAN;

-- A5: per-model recalibration lever
ALTER TABLE models ADD COLUMN activation_coefficient FLOAT NOT NULL DEFAULT 1.0;
```

**Why these design choices matter:**

- **`confidence` is clamped to `[0.05, 0.95]`** at the DB level *and* in code.
  The memory layer never asserts certainty (1.0) or impossibility (0.0): every
  belief stays falsifiable and every disbelief stays revisable. The clamp is a
  hard invariant enforced in three places — the `CHECK` constraint, the
  validator, and `ModelsRepo.insert`.
- **`confidence_at_assertion` is immutable after insert.** `confidence` drifts
  over time (calibration, confirmation, contestation), but the *birth*
  confidence is preserved so calibration can later ask "of the beliefs asserted
  at 0.8, how many came true?" without the answer being polluted by subsequent
  drift.
- **`proposition_kind` is a `GENERATED ... STORED` column.** It is derived from
  `proposition->>'kind'`, so it can never disagree with the JSONB body, yet it
  can be indexed and filtered on the hot path.
- **`activation` vs `confidence` are orthogonal.** `activation` is *heat* (how
  recently/often the belief was used); `confidence` is *belief strength*.
  Retrieval bumps activation but **never** touches confidence (§2.6).
- **`models_resolution_consistency` CHECK** forbids a half-resolved prediction
  (`resolved_at` set but `resolution_outcome` NULL, or vice versa).

#### Indexes (and why)

From migration 0001 (most are **partial on `status='active'`**, because the hot
path only ever queries live beliefs):

| Index | Shape | Purpose |
|---|---|---|
| `models_embedding_idx` | HNSW `vector_cosine_ops` | semantic similarity search (retrieval & reconciliation) |
| `models_actors_idx` / `models_entities_idx` | GIN | scope routing by actor / entity |
| `models_evaluate_idx` | B-tree on `evaluate_at` | find predictions due for resolution |
| `models_retrieved_idx` | B-tree on `last_retrieved_at` | heat / decay tracking |
| `models_tenant_status_idx` | (tenant_id, status) | lifecycle sweeps |
| `models_supporting_idx` | GIN on `supporting_model_ids` | dependency traversal |
| `models_proposition_kind_idx` | (tenant_id, proposition_kind) | kind-scoped retrieval |
| `models_topo_embedding_idx` | HNSW on `topo_embedding` (128-d) | topology nearest-neighbour (§7) |

### 2.2 Lifecycle states and archive reasons

A Model's `status` is one of (from
[lib/shared/types.py](lib/shared/types.py)):

```python
ModelStatus = Literal["active", "archived", "superseded", "contested_false"]
```

Beliefs are **archived, never deleted** — the audit chain (§4) and edge graph
(§3) depend on rows persisting. When archived, `archive_reason` records *why*:

```python
ModelArchiveReason = Literal[
    "decay",                       # confidence decayed below the activity floor
    "falsifier_triggered",         # the falsifier's condition was observed
    "contested_incorrect",         # consensus marked the belief false
    "contested_reading_incorrect", # a signal reading was marked false
    "superseded",                  # replaced by a newer proposition
    "manual",                      # human-initiated
    "resolved_confirmed",          # a prediction resolved TRUE
    "resolved_violated",           # a prediction resolved FALSE
    "severe_drift",                # topology divergence alarm
    "deprecated",                  # sunset without contradiction
    # recommendation-lifecycle reasons:
    "acted_upon", "dismissed_by_user", "situation_resolved",
]
```

This enum is the single most informative field for *understanding how a belief
left the active set*. The cascade engine (§6.10) and the edge cascade callbacks
(§3.2) both branch on it.

### 2.3 Proposition kinds

The `proposition` JSONB is a **kind-discriminated union**. The kind drives
retrieval, reconciliation thresholds, quality-gate kind-fit, and rendering:

```python
PropositionKind = Literal[
    "state", "relation", "prediction", "pattern", "pattern_instance",
    "capability_assessment", "hypothesis", "concern",
    "market_assessment", "environmental_trend",
    "recommendation", "situation",
]
```

Validation of each shape lives in
[services/models/propositions.py](services/models/propositions.py).
`recommendation` and `situation` carry extra cross-field constraints
(recommendation must target a real entity and a reachable state transition;
`situation` — extended in PR #42 — carries `pressure_type`, `shared_mechanism`,
`judgment_change`, `open_falsifier`, affected decisions/customers/teams, and
`evidence_event_ids`; see migration 0045).

### 2.4 Falsifiers — the heart of falsifiability

A falsifier is *the condition under which the belief becomes false*. The model
layer **requires an adequate falsifier whenever `confidence > 0.7`** — you may
not hold a strong belief you cannot, in principle, disprove. Authoritative
implementation:
[services/models/falsifier.py](services/models/falsifier.py) (re-exported by
[services/falsifiers/](services/falsifiers/)).

Five legal falsifier kinds, each with its own adequacy rule:

| Kind | Adequate when… |
|---|---|
| `observation_pattern` | `pattern` ≥ 20 chars **and** `within_window` parses to a positive duration |
| `commitment_outcome` | `commitment_ref` set **and** `contradicting_state` non-empty (async variant checks the commitment exists) |
| `prediction_deadline` | `evaluate_at` parses to a **future** datetime **and** `check` non-empty |
| `resource_threshold` | `resource_ref` set **and** `threshold` present |
| `explicit_contestation` | `contesting_actors` non-empty list (optional `within_window` must parse if given) |

The window parser is **dual-grammar**: it tries strict ISO-8601 first (`P7D`,
`PT4H`) then a human-readable fallback (`"7 days"`, `"4 weeks"`), raising
`MalformedFalsifierError` on a non-empty unparseable string and rejecting
zero/negative durations. This dual grammar is deliberate — LLMs emit both forms,
and silently dropping a malformed falsifier would let a high-confidence belief
slip through without a disproof condition.

### 2.5 Signal readings and contestability hooks

`signal_readings` is a JSONB list of *per-signal interpretations* — "this Slack
message reads as customer frustration." `reading_contestable` says whether those
readings may be individually disputed. This is the substrate the contestability
service (§8.1) writes to when an actor contests a *reading* (as opposed to the
*belief* itself).

### 2.6 Activation, reconsolidation, and decay

`activation` models memory *heat*:

- **Reconsolidation on retrieve** — `ModelsRepo.retrieve()`
  ([services/models/repo.py:1493](services/models/repo.py#L1493)) sets
  `last_retrieved_at = now()`, increments `retrieval_count`, and bumps
  `activation = LEAST(1.0, activation + 0.15)`. It uses `SKIP LOCKED` so it is
  best-effort under concurrency and **never** touches `confidence`.
  `get_by_id()` is the side-effect-free read.
- **Decay** — [services/models/decay.py](services/models/decay.py) drives
  time-decay of activation; a belief that falls below the activity floor can be
  archived with `archive_reason='decay'`.

The separation is the whole point: *a belief you stop using fades from working
memory (activation) without its truth-value (confidence) silently eroding.*

### 2.7 Embeddings and the pgvector codec registry

Models carry a 768-d content `embedding` and a 128-d `topo_embedding` (§7).
Semantic search is HNSW cosine over the partial `status='active'` index.

pgvector's binary codec must be registered **per connection**. The contract is
documented in
[services/models/PGVECTOR_REGISTRY.md](services/models/PGVECTOR_REGISTRY.md) and
implemented in [services/models/repo.py](services/models/repo.py):

- `pgvector_pool_init` — the recommended pool `init=` callback.
- `register_pgvector_on_pool(pool)` — retrofit for already-built pools.
- `_ensure_vector_codec(conn)` — per-conn lazy fallback.
- `PGVECTOR_REGISTERED_POOL_IDS` — a process-wide set so readers
  (`services/retrieval/pathways.py`) can branch: a registered connection binds
  the vector as a fast `float32` array; an unregistered one falls back to the
  slow text form. **Every consumer that does semantic search must register the
  codec or silently take the slow path** — this is a recurring footgun the
  harness explicitly checks.

### 2.8 In-code representation

`ModelRow` and `ModelCreate` ([lib/shared/types.py](lib/shared/types.py)) are
strict (extra-forbidding) pydantic models mirroring the table. `ModelCreate`
requires both `confidence` and `confidence_at_assertion` at insert (both range
`[0.05, 0.95]`); `ModelRow` adds the lifecycle/amendment fields and two
recommendation-only generated fields (`target_actor_id`,
`caused_act_change_id`).

---

## 3. The belief graph — `model_edges`

Beliefs do not live in isolation; they support, contradict, supersede, block,
and explain each other. The typed relationship graph is `model_edges`
([db/migrations/0031_model_edges.sql](db/migrations/0031_model_edges.sql)).

### 3.1 Schema

```sql
CREATE TABLE model_edges (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  source_model_id UUID NOT NULL,             -- app-enforced FK, no CASCADE
  target_model_id UUID NOT NULL,
  edge_kind TEXT NOT NULL,                    -- validated against the edge registry
  weight FLOAT,                               -- optional [0,1], validity per-kind
  metadata JSONB NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'active',       -- active | inert | disputed
  detected_by TEXT NOT NULL,                   -- provenance of the edge
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by_event_id UUID,
  status_changed_at TIMESTAMPTZ,
  status_reason TEXT,
  CHECK (source_model_id != target_model_id),
  CONSTRAINT model_edges_unique
    UNIQUE (tenant_id, source_model_id, target_model_id, edge_kind)
);
```

Indexes give **O(log n) forward and backward traversal** — the reverse index
(`model_edges_target_idx`) is the capability the typed-edge migration was really
about: before it, "what relates *to* this belief?" required a scan.

Edges go **inert** (not deleted) when either endpoint is archived, so the graph
remains historically faithful.

### 3.2 The edge registry — single source of truth

[lib/shared/edge_registry.py](lib/shared/edge_registry.py) is the *one place*
that defines edge semantics. Each `EdgeKindSpec` declares whether the kind is
directed or symmetric, its **cycle scope** (which kinds it must remain acyclic
across), weight rules, which kinds it is **mutually exclusive** with, and its
**cascade callbacks** (what re-evaluation fires when an endpoint is archived).

The 16 registered kinds:

| Kind | Directed? | Notable semantics |
|---|---|---|
| `supports` | yes | acyclic within {supports, instance_of}; archiving source re-evaluates target; mutually exclusive with `contradicts`/`weakens` |
| `contributes_to_resolution` | yes | acyclic; archiving source re-evals target |
| `instance_of` | yes | acyclic within {supports, instance_of}; archive cascades **both** ways (instance↔pattern) |
| `superseded_by` | yes | acyclic within itself; no cascade |
| `contradicts` | **symmetric** | requires weight; archiving either side re-evals the other; excludes `supports`/`enables` |
| `weakens` | yes | requires weight; archiving source re-evals target |
| `causes`, `explains`, `predicts` | yes | optional weight; no cascade |
| `blocks` | yes | excludes `enables`/`supports` |
| `enables` | yes | excludes `blocks`/`contradicts`/`weakens` |
| `same_issue_as`, `co_occurs_with`, `analogous_to`, `alternative_to` | **symmetric** | optional weight |
| `early_warning_for` | yes | optional weight |

**Symmetric edges are stored as two rows** (one per direction), kept in sync by
`EdgesRepo.link()`, so every consumer can treat the graph as directed and never
special-case symmetry.

`detected_by` records provenance: `llm_explicit`, `think_edge_op`, `link_miner`,
`tension_miner`, `retrieval_critic`, `precipitation`, `reconciler`, `cascade`,
`manual`, `backfill`, `falsifier_overlap`.

### 3.3 `EdgesRepo` — the edge primitive

[services/models/edges_repo.py](services/models/edges_repo.py):

- `link(...)` — idempotent insert (one or two rows). Validates kind exists & is
  writable, weight rules, mutual exclusion, no self-edges, and **DAG cycle scope
  via a recursive CTE** before committing.
- `unlink(...)` — hard delete (used by the diff path).
- `traverse_forward` / `traverse_backward` — indexed neighbour lookups.
- `mark_inert(...)` — flips every active edge touching a model to `inert` inside
  the model's archive transaction.
- `check_no_cycle(...)` — the generalized acyclicity check across a kind's
  `cycle_scope`.

### 3.4 Dual-write with legacy arrays

`supporting_model_ids` and `contributing_models` are *legacy array columns* that
predate typed edges. During the S1 migration they remain authoritative, so
`ModelsRepo._set_model_relations()`
([services/models/repo.py](services/models/repo.py)) is a single chokepoint that
writes the typed edges **and** the legacy arrays inside the same transaction. A
drift detector samples models and asserts array/edge parity. The mapping:
`supports → supporting_model_ids`, `instance_of → supporting_model_ids`,
`contributes_to_resolution → contributing_models`.

---

## 4. The audit chain — `audit_events`

Every state transition of a Model is recorded immutably in `audit_events`
([db/migrations/0030_audit_events.sql](db/migrations/0030_audit_events.sql),
emitter in [services/think/audit.py](services/think/audit.py)).

```sql
CREATE TABLE audit_events (
  event_id BIGSERIAL PRIMARY KEY,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  cause_id UUID,                              -- observation.id or think_run.id
  cause_type TEXT NOT NULL CHECK (cause_type IN (
    'create','archive','field_update','confidence_update','reconciliation_merge')),
  previous_state JSONB,                       -- NULL only on the first 'create'
  new_state JSONB NOT NULL,
  changed_fields TEXT[] NOT NULL DEFAULT '{}',
  re_asserts_event_id BIGINT REFERENCES audit_events(event_id),
  source_model_ids UUID[] NOT NULL DEFAULT '{}'  -- only for reconciliation_merge
);
```

Key design points:

- **Full before/after snapshots** of ~30 fields are stored
  (`_SNAPSHOT_FIELDS`), with **`embedding` deliberately excluded** — 768 floats
  per snapshot would balloon storage with no audit value.
- **Reversal-of-reversal tracking** — if a belief cycles A→B→A, the third
  event's `re_asserts_event_id` points back at the first, so a reviewer can see
  the belief *returned to* a prior state rather than reaching a novel one.
- **Reconciliation-merge unions** — when two beliefs merge (§6.7), the survivor
  gets a `reconciliation_merge` event listing the absorbed `source_model_ids`;
  `get_audit_chain()` walks that array transitively to reconstruct the *complete*
  history of everything that fed the surviving belief.

This chain is one of three distinct logs and they must not be conflated:
`audit_events` = per-model state history; `reconciliation_events` = the
reconciler's decision log (§6.7); `observations` = the empirical signal log that
drives cascade and realtime NOTIFY.

---

## 5. Observations — the input substrate

[services/observations/repo.py](services/observations/repo.py). The
`observations` table is append-only, **range-partitioned monthly on
`occurred_at`**, and vector-searchable (768-d HNSW).

Salient columns: `kind` (e.g. `state_change`, `contestation`), `source_channel`,
`actor_id`, `content` / `content_text`, `embedding` (+ `embedding_pending`
fallback when the embedder is down), `trust_tier`
(`authoritative > attested > reputable > inferential > unvetted`), `external_id`
(dedup key, unique with `source_channel` + `occurred_at`), `cause_id` (recursive
pointer to a prior observation), and `entities_mentioned`.

`insert()` assigns a UUIDv7, computes the embedding (or marks it pending),
dedups on `(source_channel, external_id)`, and schedules a post-commit NOTIFY
that ultimately wakes the Think worker. `trust_tier` matters downstream:
promoting a commitment to `doneverified` *hard-requires* an authoritative
observation (§6.9).

---

## 6. The model layer — the Think pipeline & quality chain

This is the machinery that turns an Observation into well-formed, deduplicated,
quality-gated Models. The defining change on `main` is PR #42's **model-layer
quality chain**, whose ordering inside `apply_diff` is:

```
split (P2) → reconcile (P3) → quality_gate (P4)
   → insert + topo_embedding init (P0)
   → per-kind relationship candidates (P5)
   → T4 adjudication structural validation (P5)
```

The full single-run pipeline, orchestrated by
[services/think/reason.py](services/think/reason.py) `_run_once()`:

```
1. Retrieval        → ContextBundle (models + observations + assembled context)
2. Region locking   → serialize concurrent mutations on (tenant, scope)
3. Reasoning        → RawDiff (deterministic handler OR LLM)
4. Validation       → ValidatedDiff (partial-accept; drop bad ops)
5. Apply (the chain)→ split → reconcile → quality-gate → insert → topo init → candidates
6. Anomalies        → detect + publish
7. Post-commit queue→ durable async side-effects
8. Cascade          → BFS downstream state transitions
```

### 6.1 The worker and the trigger queue

[services/think/worker.py](services/think/worker.py). A per-tenant async
consumer polling two queues:

- **`think_trigger_queue`** — T1/T2/T3/T4 triggers, locked with
  `FOR UPDATE SKIP LOCKED`, priority-ordered, max 5 attempts with exponential
  backoff. Trigger taxonomy:
  - **T1** — state change (deterministic/authoritative).
  - **T2** — prediction due / belief updated (deterministic).
  - **T3** — contestation (inferential; needs the LLM).
  - **T4** — background maintenance, pattern review, and **latent relationship
    candidate adjudication** (§7).
- **`model_reeval_queue`** — pending re-evaluation rows promoted to T4
  `model_reeval` triggers; on terminal failure a row moves to
  `model_reeval_dead_letter` and its `processed_at` is set to collapse dedup.

Backpressure: when queue depth exceeds `THINK_QUEUE_BACKPRESSURE_LIMIT` (500)
the poll interval is multiplied by 1.5. A **cascade-depth bound** (TK-3) rejects
T1 triggers whose `cascade_depth >= MAX_CASCADE_DEPTH` (50) non-retryably, so a
runaway cascade can never loop forever (§6.10).

### 6.2 Orchestration, region locks, and retries

`think()` runs the whole pipeline inside **one transaction** and returns a rich
`ThinkRunOutcome` (op counts, cascade depth, anomaly count, LLM latency/cost,
region hashes). Two retry families:

- **OutOfRegionError** — if validation finds the diff touching entities outside
  the retrieved region, retrieval re-runs with an expanded entity set (up to
  `max_retrieval_reruns=2`), rolling back the `think_runs` row so the same
  `run_id` is reused.
- **Deadlock / serialization** — exponential backoff + jitter, up to
  `THINK_TRANSACTION_RETRY_ATTEMPTS` (8).

**Region locks** ([services/think/region_locks.py](services/think/region_locks.py))
serialize concurrent diffs that touch the same `(tenant, scope)` via
`pg_advisory_xact_lock(tenant_hash, entity_hash)` — transaction-scoped, so it
auto-releases at COMMIT/ROLLBACK. The lock key is **order-invariant**: entity
tuples are sorted and hashed canonically (SHA-256 → signed int32), so two
triggers naming the same entities in different order serialize on the same lock.
`compute_primary_entity()` (TK-4) picks a deterministic "primary" entity by a
type-precedence table (commitment > goal > decision > resource/customer > actor)
so scope routing is reproducible.

### 6.3 Reasoning — deterministic vs LLM

`deterministic.py::is_authoritative(trigger)` decides the path. **Authoritative**
triggers (T1 state_change, T2 prediction/belief_updated, T4 background
maintenance) take a **deterministic handler** — no LLM, fully reproducible.
Everything else is **inferential** and calls
[services/think/llm_reason.py](services/think/llm_reason.py), which:

- selects the output schema (full `RawDiff` vs `RawDiffClaimsOnly` depending on
  whether graph-anchor models exist),
- calls `provider.structured(...)` with retries (parse errors are terminal;
  transport errors back off),
- and is fronted by a per-provider **circuit breaker**
  ([services/think/circuit_breaker.py](services/think/circuit_breaker.py)):
  CLOSED → (failure rate ≥ 0.5 over a 60 s window, ≥ 10 samples) → OPEN (30 s,
  all calls raise) → HALF_OPEN (one probe) → CLOSED/OPEN. This prevents retry
  storms during a provider outage.

The LLM is handed a **`ReasoningFrame`**
([services/think/reasoning_frame.py](services/think/reasoning_frame.py)) — a
frozen context object that abstracts the trigger into a frame kind, a single
`question_to_answer`, seed/candidate model & entity IDs, dynamic signals (§8.4),
allowed op types, per-op **budgets** (e.g. 3 claim ops, 2 edge ops, 1 act op),
and policy flags (`prefer_existing_models`, `emit_edges_for_pairwise_relationships`).

After reasoning, **idempotent deterministic injectors** patch the diff for
behaviours the LLM commonly under-produces (create-commitment, block-transition,
decision-revisit, future-prediction, customer-risk); each no-ops if the LLM
already emitted the equivalent.

### 6.4 The "diff" abstraction

A **diff** is a proposed set of mutations as four op lists
([services/think/diff_schema.py](services/think/diff_schema.py)):

- **`claim_ops`** — Model insert / update / archive.
- **`edge_ops`** — belief-graph add / retire (carries weight, evidence,
  `review_status`, `detected_by`).
- **`act_ops`** — Goal/Commitment/Decision create & transition (+ act edges),
  each pointing at a `confidence_basis` Model.
- **`resource_ops`** — Resource create/update/deploy/release/transaction.

`RawDiff` is the unvalidated LLM/handler output; `ValidatedDiff` is the post-
validation shape carrying partial-accept bookkeeping (`dropped_op_count`,
`dropped_op_errors`).

### 6.5 Validation — the choke point

[services/think/validator.py](services/think/validator.py) enforces, with
**partial-accept** (keep good ops, drop bad, only raise if *all* fail):

1. `claim insert` with `confidence > 0.7` ⇒ falsifier must be adequate, else
   drop.
2. confidence clipped to `[0.05, 0.95]`, calibration applied.
3. `act_ops` ⇒ `basis.confidence ≥ compute_threshold(op)` (§6.8), else drop.
4. act transitions ⇒ legal per the state machine (`can_transition`).
5. `transition_commitment_to_doneverified` ⇒ ≥1 resolving observation and
   **every** one at `authoritative` trust tier (**hard fail**, not a drop).
6. resource ops ⇒ shape validation.
7. region containment ⇒ entities outside region trigger the retrieval re-run.

### 6.6 Split — decompose compound claims

[services/think/splitter.py](services/think/splitter.py). LLMs love to emit
compound claims ("X is delayed, Y is dropping, and Z is at risk"). Left intact,
such rows collapse under embedding dedup and ruin reconciliation and
adjudication. The splitter:

- detects compounds with three heuristics — **multi-conjunction** (≥2
  verb-bearing top-level clauses), **multi-kind** (text matches ≥2 of
  state/concern/prediction patterns), **multi-entity** (≥3 distinct entities);
- decomposes a compound `insert` into **N atomic ops** (one per verb-bearing
  conjunct, each re-kinded via `_atomic_kind_for`) **plus one synthesized
  `situation`** that records the joint context, with `member_model_pending=True`
  so the applier can patch the member IDs after the atomics are inserted.

It is conservative: when in doubt it returns the op unchanged (a false negative
is safer than fragmenting a genuinely atomic claim).

### 6.7 Reconcile — content-level dedup at insert time

[services/think/reconciler.py](services/think/reconciler.py) (design in
[RECONCILIATION_DESIGN.md](services/think/RECONCILIATION_DESIGN.md)). Runs
**between validate and apply** on each `claim insert`. Its job: if the incoming
belief is *the same belief* as one already held, don't create a duplicate —
fold it in as a confirming/weakening observation.

A candidate is a match only if **all four signals** agree:

1. **embedding cosine ≥ `human_review_cosine`** (default 0.70), via the HNSW
   index;
2. **scope overlap** — at least one shared entity (type+id), plus actor overlap
   when both have actors;
3. **exact proposition-kind match**;
4. **recency** — the existing belief was created within
   `recency_window_days` (default 30).

The base cosine is then adjusted by **graph-structural boosts**: shared evidence
events `+0.10`, ≥2 shared supporting models `+0.05`, falsifier cosine ≥ 0.80
`+0.05`. **Per-kind rules** override the global thresholds
(`market_assessment`/`concern` auto-merge at 0.78; `recommendation` *never*
auto-merges and always needs human review; `situation` matches by member
overlap; `pattern_instance` requires the same parent pattern).

Decision and effect:

- **`auto_merge`** (adjusted ≥ auto threshold) — the `insert` op is rewritten
  into a **confidence update** against the matched belief, reusing
  `bulk_confidence_update`. A stronger new reading raises confidence and bumps
  `confirmed_count`/`last_confirmed_at`; a weaker one drifts it down; the new
  `supporting_event_ids` and a `signal_readings` entry are appended. The
  survivor gets a `reconciliation_merge` audit event (§4).
- **`human_review`** (adjusted in `[human_review, auto)`) — the original insert
  proceeds, a row is written to `reconciliation_events`, and a `same_issue_as`
  relationship candidate is emitted so the two beliefs are linked pending triage.
- **`no_match`** — pass through (still logged when `RECONCILE_LOG_NO_MATCH`, to
  build tuning data).

Every decision is recorded in `reconciliation_events` (with the original op,
matched model, cosine, and trigger/run IDs; reviewers can later set
`resolved_at` / `resolved_decision` / `resolved_by_actor_id`). The reconciler is
**fail-open**: any internal error logs and returns `decision="skipped"` so apply
still proceeds — it can never abort an insert. Kill switch: `RECONCILE_ENABLED=false`.

### 6.8 Quality gate — keep the belief store clean

[services/think/quality_gate.py](services/think/quality_gate.py). Three
*pure-Python heuristic* dimensions (no LLM) covering what validator and
reconciler don't:

- **atomicity** — did the splitter leave this as a single claim? (split output ⇒
  1.0; un-split compound ⇒ 0.2; already atomic ⇒ 0.9)
- **durability** — is the claim about something enduring (structural terms /
  far-future timeframes ⇒ ~0.8) or ephemeral ("yesterday", "this morning",
  sentiment ⇒ 0.25–0.4)? Ephemeral facts shouldn't become long-lived memory.
- **kind-fit** — does the text shape match the declared kind? (`state` text with
  "will/should" ⇒ 0.3 mismatch; matching patterns ⇒ 0.95).

Decision matrix on the averaged score (with hard rejects on any dimension
floor):

```
any dimension below its floor          → reject
overall < 0.45                         → downgrade_to_evidence (concern_only / pattern_instance / evidence)
overall < 0.65                         → needs_review
otherwise                              → accept
```

PR #42 detail: **downgrade precedes reject when durability is the sole failing
dimension** — an ephemeral-but-otherwise-fine claim is demoted to evidence
rather than discarded.

### 6.9 Apply — idempotent persistence

[services/think/applier.py](services/think/applier.py) `apply_diff()` runs
inside the open transaction and is **idempotent by construction**: it inserts an
`applied_triggers` row with `outcome='pending'` *before* any op (keyed on the
trigger id), updates it to `success` at commit, and rolls it back on failure. A
second delivery of the same trigger sees the row and raises `AlreadyAppliedError`,
which the worker maps to `status='skipped_idempotent'`.

Ordering: splitter expansion → per-claim (reconcile → quality-gate → insert) →
edge ops (after claims, since they reference models) → act ops (threshold-
checked) → resource ops. After atomic inserts, the synthesized situation's
`member_model_ids` are patched in; if every atomic member was gated out, the
situation is skipped (`situation_skipped_no_atomic_members_after_quality_gate`).

**Topology init (P0)** happens at the model-insert boundary, not in the applier:
`ModelsRepo.insert()` writes the 128-d `topo_embedding` via
`content_anchor(...)` and then calls `_TOPOLOGY.generate_for_model(...)` in a
**nested, best-effort transaction**
([services/models/repo.py:1390](services/models/repo.py#L1390)) so topology
failures never poison the canonical model insert (§7).

`compute_threshold()` ([services/think/thresholds.py](services/think/thresholds.py))
gives each act op a minimum basis confidence: a baseline by op kind
(`create_commitment` 0.55 … `transition_commitment_to_doneverified` 0.80 …
`transition_decision_to_archived` 0.75) plus modulators (+0.10 external
counterparty, +0.05 critical path, −0.15 first-person override), clipped to
`[0.30, 0.95]`. Higher-stakes transitions demand stronger beliefs.

### 6.10 Cascade — downstream effects

[services/think/cascade.py](services/think/cascade.py). After apply, a bounded
**BFS** propagates the consequences of act transitions and emits `state_change`
observations:

- a commitment reaching `doneverified`/`closed` can unblock dependents whose
  other dependencies are satisfied, recompute critical-path goal health, and
  refresh served-by customer-resource health;
- a revisited decision flags every `constrained_by` commitment **for review**
  (no auto-transition — humans decide);
- a terminal resource releases commitments deployed on it.

Depth is bounded at `MAX_CASCADE_DEPTH=50` both *within* a cascade and *across*
triggers: `enqueue_cascade_t1()` stamps an incremented `cascade_depth` on each
new T1 and refuses to enqueue past the bound (logging `cascade_bound_violation`
rather than raising).

### 6.11 Post-commit queue — durable side-effects

[services/think/post_commit.py](services/think/post_commit.py). Side-effects
(publish anomalies, schedule predictions, broadcast realtime, invalidate
metrics) used to run *inline* after commit — a crash in that window lost them,
and idempotency then suppressed re-execution on retry. The fix: enqueue rows
into `pending_post_commit_actions` **inside the apply transaction** (atomic with
the data), then dispatch asynchronously in a separate worker with
`FOR UPDATE SKIP LOCKED`, exponential backoff, and a dead-letter after 5
attempts. Handlers must be idempotent (at-least-once). Dedup is a
`UNIQUE NULLS NOT DISTINCT (tenant_id, trigger_id, action_kind, processed_at)`.

### 6.12 Context-use telemetry

[services/think/context_use.py](services/think/context_use.py) measures whether
the models/observations retrieved into the bundle were *actually referenced* by
the resulting diff — reference ratios, `model_context_used`, and a trace-based
fallback that, for a justified no-op, scans the reasoning trace for the IDs of
selected context. This is how the system detects "we retrieved context but the
LLM ignored it."

---

## 7. Topology — the latent relationship field

PR-level summary: *"Replace topology subsystem: latent field + sweeper, drop
neighborhood detection."* In this codebase, **"topology" now means the upstream
latent relationship field**: a cheap, high-recall layer that notices where
Models may share *consequence* before an accepted typed edge exists.

### 7.1 Why the rewrite

The **old** design ran two background workers: a `neighborhood_detector`
(hourly community detection over the active-edge graph, materializing clusters)
and a `topology_updater` (continuously nudging each model's positional embedding
toward its neighbours' mean). It was dropped because:

- hourly re-clustering was expensive and the stable-ID matching across
  re-clusters was brittle;
- neighbour-mean drift could pull a model's position *away* from its semantic
  meaning;
- and, fundamentally, it could only describe relationships *after* edges already
  existed — it could not **discover** them.

The **new** latent field is consequence-sensitive, bounded in compute, and
discovers candidates *before* edges exist. Core engine:
[services/topology/field.py](services/topology/field.py).

### 7.2 Impact signatures

Every Model gets a deterministic **consequence fingerprint** (`ImpactSignature`)
from keyword tables over its proposition + scope — **no LLM**:

- **flows** — work, money, trust, risk, capacity, decision, attention;
- **pressures** — blocker, overload, deadline, contradiction, dependency, decay,
  acceleration, opportunity;
- **stakes** — enterprise_value, revenue, customer_trust, legal_compliance,
  execution;
- **surfaces** (actor/entity refs), **time_shape**, **proposition_kind**,
  **action_surface**, and a blended **evidence_strength** from
  confidence+activation.

### 7.3 Scoring — 11 dimensions

When two Models interact, `TopologyScore` blends eleven signals
(latent_affinity, consequence_overlap, scope_fit, temporal_coupling,
business_leverage, structural_surprise, evidence_quality, actionability,
novelty, existing_explanation_gap) minus a noise penalty:

```python
score = clamp(
    0.15*latent_affinity + 0.20*consequence_overlap + 0.12*scope_fit
  + 0.08*temporal_coupling + 0.14*business_leverage + 0.10*structural_surprise
  + 0.08*evidence_quality + 0.08*actionability + 0.03*novelty
  + 0.02*existing_explanation_gap
  - 0.15*noise_penalty)
```

`structural_surprise` (high text-distance despite shared scope) and `novelty`
(penalize already-existing edges) are what make it surface *non-obvious* links.
Defaults: persist at `score ≥ 0.46`, enqueue a Think pass at `score ≥ 0.66`.

### 7.4 Bounded recall and candidate emission

`generate_for_model()` never materializes all pairwise scores. It collects
neighbours through **four bounded lanes** — latent semantic (pgvector cosine),
surface (shared scope), consequence (shared flows/pressures even without
semantic overlap), and evidence (direct support/contribution references) —
caps at `raw_candidate_limit` (160), scores pair- and situation-candidates,
ranks, keeps the top `candidate_insert_limit` (8) above threshold, dedups
against existing candidates (14-day window), and enqueues **at most one** T4
think trigger. Candidate edge kinds are restricted to
`TOPOLOGY_EMITTABLE_EDGE_KINDS` (8 consequence-preserving kinds: `same_issue_as`,
`supports`, `analogous_to`, `blocks`, `early_warning_for`, `contradicts`,
`enables`); LLM-only kinds (`explains`, `causes`, `predicts`, `weakens`,
`co_occurs_with`) are explicitly *not* fabricated by topology. Output rows go to
`relationship_candidates` (§8.7), which T4 adjudication then validates
structurally.

### 7.5 The content anchor (768 → 128)

[services/topology/anchor.py](services/topology/anchor.py) projects the 768-d
content embedding to the 128-d `topo_embedding` via a **fixed random
projection** seeded at import (`0xF00DCAFE`), L2-normalized. It is deterministic
and *not learned* — a learned projection would force a full backfill on every
retrain. Johnson–Lindenstrauss keeps cosine similarity within ~10%, which is all
the anchor needs.

### 7.6 The sweeper

Insert-time generation catches relationships around *new* models;
[services/workers/topology_sweeper/worker.py](services/workers/topology_sweeper/worker.py)
periodically revisits a bounded frontier of **high-activation** models
(`sweep_tenant()`, default every 900 s, 50 models/tenant, `activation ≥ 0.15`)
so older-but-important memory can form new candidates as the organization
changes. Each model is processed in a nested transaction so one error doesn't
poison the sweep. Env: `TOPOLOGY_SWEEPER_INTERVAL_S`,
`TOPOLOGY_SWEEPER_LIMIT_PER_TENANT`, `TOPOLOGY_SWEEPER_MIN_ACTIVATION`.

### 7.7 Schema and legacy tables

Migration 0032 adds `models.topo_embedding VECTOR(128)` + `topo_updated_at` and
the `models_topo_embedding_idx` HNSW index. The tables from the *old* design —
`model_neighborhoods`, `model_neighborhood_membership`, and `topology_events`
(migrations 0032-0034) — remain for schema/map compatibility but are **not
populated by the latent field**. `topo_dirty_queue` was dropped by migration
`0127`. `services/topology/umap_projector.py` projects `topo_embedding` to 2D for
the CEO map view, caching coordinates with a trustworthiness score.

---

## 8. Supporting epistemic subsystems

### 8.1 Contestability — disputing beliefs and readings

[services/contestability/](services/contestability/). `contest_model()` lets an
actor dispute either a **belief** or a **signal reading**, but only after a
**standing** check ([standing.py](services/contestability/standing.py)) grants
authority on one of three bases: `scope` (the actor is in the model's scope),
`owner`, or `manager_chain`.

- **belief contestation** applies a first-person **confidence override**
  (primary-subject multiplier 0.3, secondary 0.5, floor 0.15), writes a
  `model_status_notes` row (`first_person_override`), and enqueues a T3 think
  trigger to re-evaluate;
- **reading contestation** marks the `signal_readings` entry `contested=true`
  and does **not** move confidence.

Both write a `contestation` observation at `authoritative` trust tier.

### 8.2 Falsifiers

Covered in §2.4 — the package [services/falsifiers/](services/falsifiers/)
re-exports the authoritative adequacy logic from
[services/models/falsifier.py](services/models/falsifier.py).

### 8.3 Calibration — are our confidences honest?

[services/calibration/hit_rate.py](services/calibration/hit_rate.py) tracks
per-class hit rates over a rolling window (default 30 days) — e.g. of
`delivery_estimate` commitments, what fraction finished by their due date; of
`belief_movement` models, what fraction resolved TRUE. It is **conservative**:
below `MIN_SAMPLES_FOR_CALIBRATION` (5) it returns `None` rather than fabricate a
rate. These rates feed `confidence_at_assertion`-based recalibration (§2.1).
Backing tables: `calibration_stats`, `calibration_offsets`.

### 8.4 Dynamics — ephemeral signals for reasoning context

[services/dynamics/detectors.py](services/dynamics/detectors.py)
`detect_dynamic_signals()` reads audit events, models, observations, and
topology events to surface time-bounded signals — `oscillating` (a belief
re-asserting a prior audited state), `recurring_update`, `stale` (activation <
0.12 and untouched > 30 d), `phase_shift` (topology event touched these models),
and `high_activity` — each with a strength and confidence. These are injected
into the `ReasoningFrame` so the LLM knows, e.g., that a belief keeps flip-flopping.

### 8.5 Judgment — decision-worthiness

[services/judgment/scoring.py](services/judgment/scoring.py) `JudgmentScores`
blends impact, uncertainty, urgency, (ir)reversibility, authority_required,
actionability, novelty, and confidence into a single `judgment_leverage` score
(impact weighted highest at 0.22; raw confidence lowest at 0.04 — high-confidence
trivia must not outrank uncertain-but-material decisions).

### 8.6 Model trace — walking the evidence graph

[services/model_trace/repo.py](services/model_trace/repo.py) `trace_back()` /
`trace_forward()` do a bounded (`max_depth=4`), deterministic BFS over active
edges to produce ordered evidence chains for the UI. *Back* follows incoming
`supports`/`contributes_to_resolution` and outgoing `instance_of`/`superseded_by`
(what underpins this node); *forward* follows the mirror set (what this node
underpins).

### 8.7 Decision deltas & relationship candidates

- [services/decision_deltas/](services/decision_deltas/) elevates a "proposed
  change" to a first-class object with its own lifecycle (`proposed → accepted |
  delegated | contested | dismissed | superseded`), evidence items, and
  `accept_and_apply()` consequence execution. Invariant: `confidence > 0.7`
  requires a non-empty `falsification_condition`.
- `services/relationships/candidates.py` + `adjudication.py` (PR #42 P5) are the
  per-kind candidate registry and structural validator that topology and the
  reconciler feed: `blocks`/`early_warning_for`/`explains` require mechanism
  evidence; `situation` candidates require `pressure_type` + `shared_mechanism`.

---

## 9. Validation — the synthesis harness

[tests/synthesis_harness/](tests/synthesis_harness/) is the canonical black-box
validator of the memory layer. `python -m tests.synthesis_harness [stages…]`
runs migrations (idempotent, per-file transactions), registers the pgvector
codec on the pool, and exercises staged cases:

| Stage | ~Cases | What it locks in |
|---|---|---|
| retrieval | 6 | pathways A–D, RRF fusion, sparse-result handling |
| scope_routing | 5 | entity precedence, lock-key invariance, tenant isolation |
| contestation | 7 | standing bases, primary/secondary multipliers, reading vs belief |
| falsifier | 14 | adequacy of all 5 kinds, dual-grammar window parser |
| cascade | 6 | unblock / no-unblock, decision-revisit, depth bound |
| reconciliation | 14 | auto-merge / human-review / no-match, end-to-end idempotency |

`--calibration` additionally computes Expected Calibration Error and diffs it
against a stored baseline to gate regressions. PR #42 adds
`services/models/tests/test_model_quality_suite.py`, which drives the whole
split → reconcile → quality-gate chain end-to-end through `apply_diff` (~530
tests pass across think/models/topology/relationships/scripts).

---

## 10. Configuration reference

| Env var | Default | Effect |
|---|---|---|
| `THINK_POLL_INTERVAL_S` | 2.0 | worker poll cadence |
| `THINK_POLL_BATCH` | 10 | triggers fetched per poll |
| `THINK_MAX_CONCURRENCY_PER_TENANT` | 1 | per-tenant parallelism |
| `THINK_QUEUE_BACKPRESSURE_LIMIT` | 500 | depth that slows polling 1.5× |
| `THINK_TRIGGER_MAX_ATTEMPTS` | 5 | retries before failure |
| `THINK_RUN_TIMEOUT_S` | 600 | per-run wall clock |
| `THINK_TRANSACTION_RETRY_ATTEMPTS` | 8 | deadlock/serialization retries |
| `RECONCILE_ENABLED` | true | reconciler master switch |
| `RECONCILE_AUTO_MERGE_COSINE` | 0.85 | auto-merge threshold |
| `RECONCILE_HUMAN_REVIEW_COSINE` | 0.70 | review-band floor |
| `RECONCILE_RECENCY_WINDOW_DAYS` | 30 | candidate recency filter |
| `RECONCILE_LOG_NO_MATCH` | true | log no-match for tuning data |
| `TOPOLOGY_SWEEPER_INTERVAL_S` | 900 | sweeper cadence |
| `TOPOLOGY_SWEEPER_LIMIT_PER_TENANT` | 50 | models per sweep |
| `TOPOLOGY_SWEEPER_MIN_ACTIVATION` | 0.15 | sweep activation floor |

---

## 11. File map

| Concern | Path |
|---|---|
| Models table & amendments | [db/migrations/0001_foundation.sql](db/migrations/0001_foundation.sql), [0002_models_amendments.sql](db/migrations/0002_models_amendments.sql) |
| Audit chain | [db/migrations/0030_audit_events.sql](db/migrations/0030_audit_events.sql), [services/think/audit.py](services/think/audit.py) |
| Edge graph | [db/migrations/0031_model_edges.sql](db/migrations/0031_model_edges.sql), [lib/shared/edge_registry.py](lib/shared/edge_registry.py), [services/models/edges_repo.py](services/models/edges_repo.py) |
| Models DAO | [services/models/repo.py](services/models/repo.py), [propositions.py](services/models/propositions.py), [falsifier.py](services/models/falsifier.py), [calibration.py](services/models/calibration.py), [decay.py](services/models/decay.py), [status_notes.py](services/models/status_notes.py) |
| In-code types | [lib/shared/types.py](lib/shared/types.py) |
| Think pipeline | [services/think/](services/think/) — `worker.py`, `reason.py`, `llm_reason.py`, `reasoning_frame.py`, `splitter.py`, `reconciler.py`, `quality_gate.py`, `validator.py`, `applier.py`, `diff_schema.py`, `cascade.py`, `post_commit.py`, `thresholds.py`, `region_locks.py`, `circuit_breaker.py`, `deterministic.py`, `context_use.py` |
| Topology | [services/topology/](services/topology/) — `field.py`, `anchor.py`, `umap_projector.py`, `eval_harness.py`; [services/workers/topology_sweeper/worker.py](services/workers/topology_sweeper/worker.py) |
| Observations | [services/observations/repo.py](services/observations/repo.py) |
| Epistemic support | [services/contestability/](services/contestability/), [services/calibration/](services/calibration/), [services/dynamics/](services/dynamics/), [services/judgment/](services/judgment/), [services/model_trace/](services/model_trace/), [services/decision_deltas/](services/decision_deltas/), [services/relationships/](services/relationships/) |
| Validation | [tests/synthesis_harness/](tests/synthesis_harness/) |

---

*Generated against `main` @ `be468b6`. The pipeline ordering, thresholds, and
schema quoted here are drawn from the code and from the PR #42 commit record;
re-verify env defaults against the running deployment before relying on them
operationally.*
