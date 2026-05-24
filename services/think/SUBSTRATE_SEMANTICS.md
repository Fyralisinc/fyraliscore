# Fyralis v1 Substrate Semantics

> Canonical specification for the five v1 substrate decisions. This document is the source of truth — every implementation PR (see [V1_PR_PROMPTS.md](../../V1_PR_PROMPTS.md)) refers back here. Update this file when decisions evolve; do not let the code drift ahead of the spec.

## How to use this document

- **For implementation:** each section "Q1"-"Q5" defines what gets built. The "Decision" subsection is binding; "Rationale" explains why; "Trade-offs accepted" lists known costs we're choosing to bear; "Implementation references" links to the PRs and code where the decision lands.
- **For design review:** PRs 3, 4, and 5 require explicit user-review gates. The reviewer reads the relevant Q section here, then the PR's design document. If the design contradicts this file, the design is wrong; if reality has shifted, this file is wrong — surface the deviation, don't paper over it.
- **For future architects:** the "Interactions" subsections describe how each decision constrains the others. Don't change one in isolation.

## The five decisions at a glance

| Q | Topic | One-line decision |
|---|---|---|
| **Q1** | Reconciliation | Three-tier merge: cosine ≥ 0.85 auto-merge, 0.65–0.85 LLM second-pass, < 0.65 create new. Cached + budget-capped. |
| **Q2** | Entity hierarchy | Explicit `entities` and `entity_relationships` tables; LLM-driven extraction; hierarchy-aware retrieval and reconciliation. |
| **Q3** | Preconditions | First-class `precondition` + `state` on Commitments; three precondition forms; cascade satisfies them. |
| **Q4** | Confidence-as-strength | Confidence is the unified strength signal. Linguistic markers calibrate the prompt; Commitment merges use `max(...)` as a documented exception. |
| **Q5** | Audit chain | Every Model state change emits a structured audit event. Reversal-of-reversal preserves three events with `re_asserts_event_id`; reconciliation-merge unions source chains. |

## Interaction map

```
              ┌──────────────────────────────────┐
              │  Q5 Audit chain                  │
              │  (foundation — every other PR    │
              │   emits audit events)            │
              └──────────────┬───────────────────┘
                             │ records all merges, transitions
              ┌──────────────┴───────────────────┐
              │  Q4 Confidence-as-strength       │
              │  (Commitment merge exception     │
              │   recorded as audit cause)       │
              └──────────────┬───────────────────┘
                             │ merge rule applies in reconciler
              ┌──────────────┴───────────────────┐
              │  Q3 Preconditions                │
              │  (latent state transitions       │
              │   recorded; precondition         │
              │   resolution may use Q1)         │
              └──────────────┬───────────────────┘
                             │ reconciler sees latent Commitments
              ┌──────────────┴───────────────────┐
              │  Q1 LLM second-pass reconciler   │
              │  (extends with hierarchy ctx     │
              │   in Q2)                         │
              └──────────────┬───────────────────┘
                             │ second-pass receives hierarchy
              ┌──────────────┴───────────────────┐
              │  Q2 Entity hierarchy             │
              │  (extends retrieval, reconciler, │
              │   precondition resolver)         │
              └──────────────────────────────────┘
```

Sequencing rationale (mirrors PR order in V1_PR_PROMPTS.md):

1. **Q5 first** — every other decision emits audit events.
2. **Q4 second** — no schema change; affects all subsequent reconciliation.
3. **Q3 third** — first major schema change; hierarchy (Q2) extends precondition resolution, so this comes before Q2.
4. **Q1 fourth** — reconciler changes; hierarchy extends the second-pass.
5. **Q2 last** — largest change; depends on Q1, Q3, Q5.

---

## Q1 — Reconciliation: three-tier with LLM second-pass

### Decision

When the extractor proposes a new Model that overlaps with an existing one, decide whether to merge or create new based on cosine similarity:

| Cosine band | Action | Cost |
|---|---|---|
| ≥ 0.85 | Auto-merge (deterministic) | None |
| 0.65 – 0.85 | LLM second-pass: prompt LLM with both naturals + scopes; ask `{same_proposition: bool}` | One LLM call |
| < 0.65 | Create new (deterministic) | None |

Decisions are cached in `reconciliation_decisions` keyed by `(model_id_a, model_id_b)`. Cache invalidates when either Model's state changes or on policy change. Per-`think_run` budget cap on second-pass calls (default 5); over-cap candidates default to create-new (alternative: human-review queue — see open questions).

### Rationale

The current single-pass implementation (cosine threshold only — see [V1_BASELINE.md](V1_BASELINE.md) Q1) is brittle in the borderline zone. Real customer signals show two failure modes simultaneously:

- **False positives near 0.85.** "We'll ship by EOQ for premium" and "We'll ship by EOQ" embed almost identically but are different propositions. A pure cosine threshold merges them, silently destroying the scoping nuance.
- **False negatives in the 0.70–0.85 range.** Paraphrases of the same commitment ("ship by Q3 end" vs "deliver before quarter close") sit here. A pure cosine threshold creates duplicates that pollute the ledger.

Two tiers (auto-merge above some threshold, create-new below) cannot resolve both failure modes — any single threshold makes one or the other worse. Three tiers acknowledge that the borderline zone is exactly where deterministic similarity is unreliable and human-language judgment is needed; the LLM is given both naturals plus full scope context and asked the binary question.

**Threshold choices.** 0.85 keeps the existing auto-merge floor (already in code as `RECONCILE_AUTO_MERGE_COSINE`). 0.65 lowers from the current 0.70 floor — pulling more borderline pairs into LLM evaluation rather than silently creating duplicates. Both are starting values; the threshold-tuning infrastructure (PR 4) exists explicitly to adjust them against labeled data.

### Trade-offs accepted

- **Inference cost.** Every think_run with borderline candidates incurs LLM calls. Budget cap is the brake; threshold tuning is the lever.
- **Non-determinism in the middle band.** The LLM may decide differently across runs. Caching reduces but doesn't eliminate this; downstream consumers must tolerate occasional re-classifications until cache invalidates.
- **Threshold guesses.** 0.65 / 0.85 are starting values; threshold-tuning infrastructure exists explicitly to revisit them with labeled data.

### Implementation references

- PR 4 — see [V1_PR_PROMPTS.md PR 4](../../V1_PR_PROMPTS.md) for scope and gates.
- Schema: `reconciliation_decisions` (PR 4).
- Code: `services/think/reconciler.py`, `services/think/reconciliation_prompt.py` (PR 4).
- Pre-PR-4 baseline: [V1_BASELINE.md](V1_BASELINE.md) Q1.

### Interactions

- **Q5:** every reconciliation decision is an audit event. Auto-merge, second-pass merge, second-pass non-match, below-threshold create are all distinct audit causes.
- **Q4:** when reconciler decides to merge two Commitment Nodes, the confidence rule is `max(...)` (Q4's exception), not the default merge rule.
- **Q2:** the second-pass prompt is extended in PR 5 to include hierarchy context. Without Q2, the second-pass relies on flat scope_entities only.
- **Q3:** latent Commitments may be reconciliation candidates. Open question: can a latent merge with an active? See open questions.

---

## Q2 — Entity hierarchy: explicit relationships

### Decision

Introduce two new tables:

- **`entities`** — canonical registry of entity instances (`(tenant_id, entity_type, canonical_name)` unique). `aliases` array captures known surface forms.
- **`entity_relationships`** — directed `child → parent` edges with `relationship_type`, `confidence`, optional `source_signal_id`, and `invalidated_at` for soft-deletion.

The existing `scope_entities` JSONB on Models stays as-is — its IDs reference into `entities`. Hierarchy walk during retrieval reads `scope_entities`, looks up the entity, and walks `entity_relationships` to a configurable depth.

Authoring is **LLM-driven for v1**. The extractor emits relationships alongside Models; duplicate-parent detection runs as part of entity reconciliation. Hybrid LLM-proposed-human-confirmed authoring is deferred to v2.

### Rationale

Three concrete capabilities cannot work without explicit hierarchy:

1. **Cross-scope retrieval.** Pathway A is actor-scoped only (see [V1_BASELINE.md](V1_BASELINE.md) Q2). A query about "customer X" today does not surface deal-level Models even though deals belong to that customer. Without hierarchy, retrieval recall is bounded by exact-scope match.
2. **Precondition resolution under hierarchy (Q3).** An event-reference precondition scoped to "customer X" needs to match signals scoped to "deal Y" when deal Y belongs to customer X. The flat `scope_entities` array has no traversal — the precondition resolver has nothing to walk.
3. **Reconciliation precision (Q1).** Two deals under the same customer may share near-identical commitment text. Q1's second-pass needs to know "same parent" vs "different customer entirely" to make the right call. Without hierarchy, the LLM is told they share no scope when they actually share a parent.

Today's flat `scope_entities` makes these failures invisible — they manifest as missed retrieval results, latent commitments that never satisfy, and reconciliation precision that depends on textual similarity alone. Hierarchy makes the implicit explicit — a deal *belongs to* a customer, and that relationship is queryable.

### Trade-offs accepted

- **LLM authoring inconsistency.** The LLM will produce inconsistent relationships across signals — some emit them, some don't. The substrate's correctness now depends on hierarchy correctness in a way it didn't before.
- **Operational burden.** Hierarchy health is now a metric. Nightly audit job surfaces duplicate parents and stale relationships.
- **Retrieval performance.** Hierarchy walk adds query overhead; depth bounds and indexes are mitigations.
- **Backfill choice.** Forward-looking only by default. Existing implicit relationships are not inferred unless the user opts in (see open questions).

### Implementation references

- PR 5 — see [V1_PR_PROMPTS.md PR 5](../../V1_PR_PROMPTS.md). Largest PR; explicit user-review gate.
- Schema: `entities`, `entity_relationships` (PR 5).
- Code: `services/think/hierarchy.py`, `services/retrieval/primary.py` extensions, `services/jobs/hierarchy_audit.py` (PR 5).
- Pre-PR-5 baseline: [V1_BASELINE.md](V1_BASELINE.md) Q2.

### Interactions

- **Q1:** second-pass receives hierarchy context after PR 5. Reconciler can distinguish "same proposition different scopes" from "same proposition same parent scope" (e.g., two deals under the same customer).
- **Q3:** precondition resolver walks hierarchy when matching event-reference preconditions. A signal scoped to a deal can satisfy a precondition scoped to the parent customer.
- **Q5:** entity reconciliation events (e.g., merging duplicate parents) emit audit events.

---

## Q3 — Preconditions: first-class on Commitment

### Decision

Add two columns to `commitments`:

- **`state`** — converted to a typed enum: `commitment_state` ∈ `{latent, active, completed, cancelled}`.
- **`precondition`** — nullable JSONB carrying one of three forms:
  1. **Decision reference** — `{"kind": "decision", "decision_id": ..., "satisfied_when": "state in [...]"}` 
  2. **Event reference** — `{"kind": "event", "scope_entities": [...], "satisfaction_predicate": "natural language"}`
  3. **Commitment reference** — `{"kind": "commitment", "commitment_id": ..., "satisfied_when": "state in [...]"}`

Lifecycle: a Commitment with non-null `precondition` is created `latent`. Cascade extension scans for satisfaction on relevant state changes; satisfaction transitions `latent → active`. Subsequent transitions follow existing rules. A completed Commitment cannot regress to active.

Event-reference satisfaction uses an LLM judgment ("does this signal satisfy the predicate?") within the referenced scope_entities.

Downstream consumers (recommendation feed, capacity calculations) filter by state; latent is excluded by default.

### Rationale

Real customer signals routinely express conditional commitments: "we'll do X if/when Y." Without first-class preconditions, the substrate has two equally bad options:

1. **Drop the conditional**, creating an unconditional `proposed` Commitment. The recommendation feed surfaces it as if it were active, generating false urgency. The conditional is lost as data.
2. **Drop the signal**, refusing to extract anything. The intent is lost entirely, and the eventual unconditional version (when Y happens) has no provenance back to the original conditional.

Preconditions resolve this by making the conditional a first-class `latent` Node. It exists in the substrate (queryable, auditable, reconcilable) but is excluded from active workflows until its precondition is satisfied.

**Three forms** because three referent shapes cover the practical cases:

- **Decision reference.** "We'll launch if leadership approves." Referent is a Decision Model; the Commitment activates when the Decision transitions to a satisfying state.
- **Event reference.** "We'll fix it if customer escalates again." Referent is a future signal pattern within scoped entities; the satisfaction predicate is judged by LLM against incoming signals.
- **Commitment reference.** "We'll start phase 2 once phase 1 ships." Referent is another Commitment; satisfaction follows the referent's state.

**Time-based preconditions are deferred to v2** because they are scheduler infrastructure, not substrate logic — the v1 cases this work resolves are condition-based (waiting for a thing to happen), not date-based (waiting for a wall-clock instant).

### Trade-offs accepted

- **Cascade infinite-loop risk.** Chained preconditions can spiral; depth limits enforced explicitly.
- **Event-reference false positives.** LLM may judge a signal as satisfying a predicate it shouldn't. Mitigation: log every satisfaction with reasoning for manual audit review.
- **Backfill complexity.** Existing `commitments.state` is varchar with values outside the new enum (`doneverified`, `at_risk`); migration must map. See [V1_BASELINE.md](V1_BASELINE.md) Q3 for the full set.
- **Recommendation feed UX shift.** Latent commitments are hidden by default. Users who relied on seeing pending commitments need a separate view.

### Implementation references

- PR 3 — see [V1_PR_PROMPTS.md PR 3](../../V1_PR_PROMPTS.md). Explicit design-review gate; design doc is `services/think/PR3_DESIGN.md`.
- Schema migration: adds enum + columns + indexes; rollback documented (PR 3).
- Code: `services/think/precondition_resolver.py` (new), `services/think/cascade.py` extension, recommendation-feed filter (PR 3).
- Pre-PR-3 baseline: [V1_BASELINE.md](V1_BASELINE.md) Q3.

### Interactions

- **Q5:** every state transition (`latent → active → completed/cancelled`) emits an audit event.
- **Q4:** Commitment merge under Q4's `max(...)` rule applies even when one is latent — the merge logic doesn't branch on state. Open question: should it? See open questions.
- **Q1:** latent Commitments are eligible reconciliation candidates. Reconciler decisions are stored regardless of state.
- **Q2:** event-reference preconditions walk hierarchy when matching scope_entities.

---

## Q4 — Confidence-as-strength

### Decision

Confidence is the unified strength signal — there is **no separate `strength` field**. The extraction prompt is calibrated against linguistic markers:

| Linguistic mode | Confidence range |
|---|---|
| Aspirational ("would love to," "ideally," "in a perfect world," "fingers crossed") | 0.30 – 0.55 |
| Targeted ("targeting," "aiming for," "planning to," "expecting to") | 0.55 – 0.75 |
| Committed ("will," "promised," "guaranteed," "by [date], no later") | 0.75 – 0.95 |
| Stacked hedges (multiple aspirational markers in one statement) | ≤ 0.55 |

When reconciling **Commitment Nodes specifically**, confidence merge takes `max(existing, new)` rather than the default `bulk_confidence_update`. This is a documented exception, encapsulated in `commitment_merge_confidence(existing, new)` and called only for Commitments. For all other Node types (state, concern, expectation), the default merge rule applies.

### Rationale

The substrate has one unified scalar for "how much weight does this carry." Adding a separate strength field (Q4-B) would double the calibration surface — every prompt, every merge, every retrieval would need to reason about both confidence and strength independently — without a clear functional split. The dominant signal in real text is *speaker conviction*, which is exactly what confidence is supposed to capture. Conviction *is* strength for Commitments; treating them as separate axes adds machinery for no observable benefit.

The failure mode that made this urgent is visible in the adversarial harness today (`hedged_commitment_low_confidence`, `compound_linguistic_pressure` — see [V1_BASELINE.md](V1_BASELINE.md) Q4). Aspirational language was producing too-high confidence values; those values then survived merges and elevated noise to the recommendation feed. The fix is calibrating the prompt against linguistic markers so that aspirational text *produces* low confidence in the first place — not introducing a second field to compensate downstream.

**The Commitment-merge `max(...)` exception** is necessary because committed-language updates ("we will ship by Q3 — final, no more discussion") should not be downgraded by lingering aspirational confidence from earlier signals about the same proposition. The default merge rule (`bulk_confidence_update`) averages or weights, which would re-introduce the original failure mode. `max(...)` preserves the most recent committed assertion; for non-Commitment Nodes (state, concern, expectation), the default rule remains because state observations *should* average across evidence.

The current code already takes `max()` in reconciliation. The work is making this explicit, Commitment-only, and named — plus calibrating the prompt so confidence reflects linguistic strength rather than the LLM's overall certainty in extraction.

### Trade-offs accepted

- **Load-bearing exception.** The Commitment-merge `max(...)` rule is a single hard-coded branch. If a second similar exception appears in a future PR, escalate to reconsidering the unified-confidence model — don't quietly add another exception.
- **Linguistic ambiguity.** Aspirational/targeted/committed boundaries are fuzzy. Some real signals are inherently ambiguous; calibration measurement (ECE) tracks drift but cannot eliminate it.
- **No separate strength field.** Q4-B (a true strength column) is deferred to v2. If strength turns out to need its own provenance and lifecycle, this decision will need revisiting.

### Implementation references

- PR 2 — see [V1_PR_PROMPTS.md PR 2](../../V1_PR_PROMPTS.md). No schema change; prompt + reconciler changes only.
- Code: `services/think/prompt.py` (calibration anchor), `services/think/reconciler.py` and `services/think/applier.py` (`commitment_merge_confidence` branch).
- Pre-PR-2 baseline: [V1_BASELINE.md](V1_BASELINE.md) Q4. Note: current code already uses `max()` uniformly; PR 2 is mostly the prompt calibration plus naming the function.
- Calibration measurement: `tests/synthesis_harness/calibration.py` (already in place).

### Interactions

- **Q5:** Commitment merge audit event records which rule was used (`commitment_max` vs default `bulk_confidence_update`). Frequency tracked for the load-bearing-exception watch.
- **Q1:** the second-pass reconciler may decide two Commitments are the same proposition — at which point Q4's merge rule applies.
- **Q3:** Q4 doesn't currently branch on Commitment state. Latent + active merge under the same `max(...)` rule. See open questions.

---

## Q5 — Audit chain: structured per-Model history

### Decision

Every Model state change emits an `audit_events` row. Schema:

```sql
CREATE TABLE audit_events (
    event_id BIGSERIAL PRIMARY KEY,
    model_id UUID NOT NULL REFERENCES models(id),
    tenant_id UUID NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    cause_id UUID,                       -- pointer into observations / think_runs / etc.
    cause_type TEXT NOT NULL,            -- e.g. 'create', 'update', 'archive',
                                         --      'reconciliation_merge', 'state_transition'
    previous_state JSONB,                -- nullable for 'create'
    new_state JSONB NOT NULL,
    changed_fields TEXT[],
    re_asserts_event_id BIGINT REFERENCES audit_events(event_id),
    source_model_ids UUID[]              -- non-null only for 'reconciliation_merge'
);
```

**Reversal-of-reversal preservation.** A → B → A produces three distinct audit events; the third event has `re_asserts_event_id = id_of_first_event`. Chains are NOT collapsed.

**Reconciliation-merge audit union.** When two Models merge (under Q1's auto-merge or second-pass-merge), the merged Model's audit chain is the union of both source chains, ordered by `occurred_at`. The merge itself is an event with `cause_type = 'reconciliation_merge'` and `source_model_ids` populated.

**Default Model query returns current state only**; full audit chain is reachable via a separate API: `get_audit_chain(model_id) -> List[AuditEvent]`.

### Rationale

A parallel `audit_events` table — rather than extending observations or `reconciliation_events` — because the three serve fundamentally different purposes:

- **Observations** are *signals coming in*. Tenant-scoped events from external sources (channels, integrations). Their schema is signal-shaped: source channel, content, embedding, trust tier. They are not state-transition records.
- **`reconciliation_events`** is *decision history* for the reconciler — a queue plus an audit of each reconcile choice. Keyed by `(model_id_a, model_id_b)`; not Model-history shaped.
- **`audit_events`** is *per-Model state history*. Keyed by `model_id`; carries `previous_state` / `new_state` / `changed_fields`. None of those fields fit observations or `reconciliation_events`.

Trying to overload either existing table would either (a) bloat its schema with mostly-NULL columns or (b) lose the typed structure that makes audit chains queryable. A separate table is cheaper to reason about and to index for `get_audit_chain(model_id)` queries.

**Reversal-of-reversal as three events, not one.** Oscillation is itself information. A Model that goes A → B → A is not in "no net change" state — it is in "the position keeps flipping" state. Compressing to a single net-zero event hides the volatility, and volatility is diagnostic for downstream consumers (recommendation feed weighting, contestability surfacing, customer-health detection). The `re_asserts_event_id` pointer preserves the rhetorical shape ("we're back where we started") while keeping the three transitions as distinct facts.

### Trade-offs accepted

- **Storage growth.** Every state change is a row. For high-churn Models this is meaningful but bounded by think_run rate.
- **Audit emission performance.** Audit writes are part of the same transaction as the state change (atomic). For very high write throughput, async-indexing for downstream consumption is a v2 concern.
- **Co-existence with `reconciliation_events`.** The existing reconciliation-decision table stays alongside `audit_events`. They serve different purposes: `reconciliation_events` is decision history (queue + audit of reconciler choices); `audit_events` is per-Model state history. Not duplicated; documented in PR 1.
- **No backfill.** Pre-PR-1 Models have no audit chain. Their history starts at PR 1's deploy.

### Implementation references

- PR 1 — landed.
- Schema: [db/migrations/0030_audit_events.sql](../../db/migrations/0030_audit_events.sql).
- Code: [services/think/audit.py](audit.py); emission points in [services/models/repo.py](../models/repo.py) (`insert`, `archive`, `bulk_confidence_update`) and [services/think/applier.py](applier.py) `_apply_claim_op` (`field_update` and `reconciliation_merge` paths).
- Public API: `emit_audit_event`, `get_audit_chain`, `find_re_assertable_event`, `model_state_snapshot`, `emit_reconciliation_merge_audit` — all in [services/think/audit.py](audit.py).
- Cause-type vocabulary: `create`, `archive`, `field_update`, `confidence_update`, `reconciliation_merge` — enforced by both the migration's CHECK constraint and `_CAUSE_TYPES` in audit.py.
- Tests: [tests/synthesis_harness/cases_audit_chain.py](../../tests/synthesis_harness/cases_audit_chain.py) (15 scenarios spanning basic emission, reversal-of-reversal, reconciliation-merge audit union, chain ordering, cause linkage, multi-tenant isolation).

### Interactions

- **Foundation.** Every other Q emits audit events. Q1 records reconciliation decisions; Q3 records state transitions; Q4 records merge-rule choice; Q2 records hierarchy changes (relationship invalidation, parent reconciliation).
- **Q1 specifically:** reconciliation-merge audit's union-of-chains logic is subtle when source Models have overlapping or out-of-order timestamps. Test thoroughly.

---

## Open questions

These are decisions that V1_PR_PROMPTS.md flags but does not resolve. Each blocks the corresponding PR's design-review gate.

| ID | Question | Blocks |
|---|---|---|
| **OQ1** | Q1 budget cap behavior when exceeded: default to "create new" (non-blocking) or "flag for human review" (blocking, queues item)? | PR 4 design |
| **OQ2** | Q1 second-pass LLM choice: production LLM (correlated failure modes) or different model family (cost + latency)? | PR 4 design |
| **OQ3** | Q1 cache invalidation: when does a cached "same_proposition: false" stale out? Time-based, state-change-based, both? | PR 4 design |
| **OQ4** | Q3 backfill: existing `commitments.state` values (`doneverified`, `at_risk`, etc.) — explicit map to the new enum? Default to `active`? | PR 3 design |
| **OQ5** | Q3 lifecycle: can a completed Commitment regress to active? (Currently: no.) Can latent → cancelled directly without going through active? | PR 3 design |
| **OQ6** | Q3+Q4 interaction: when reconciling a latent Commitment with an active Commitment, does the merge rule still use `max(...)`? Does the merged Commitment inherit `latent` or `active`? | PR 3 design |
| **OQ7** | Q2 backfill: should existing implicit relationships (e.g., from observed actor-team patterns) be inferred during PR 5 backfill, or is hierarchy purely forward-looking? | PR 5 design |
| **OQ8** | Q2 hierarchy update: when evidence contradicts an existing relationship (a deal moves customers, an employee changes teams), are old Models rebound to the new parent or preserved with the old relationship? | PR 5 design |
| ~~**OQ9**~~ | ~~Q5 audit chain query depth: is full-chain retrieval bounded?~~ **Resolved (2026-05-09):** `get_audit_chain(model_id)` returns the full chain, unbounded, ordered by `occurred_at`. v1 audit chains are expected to stay in the dozens-per-Model range; deferring paging keeps the substrate API simple. If a consumer hits a multi-thousand-event Model in production, paging gets added at the API layer (gateway), not the substrate layer. | ~~PR 1~~ Resolved |

---

## Revision history

| Date | Change | Author |
|---|---|---|
| 2026-05-09 | Initial draft. Five decisions captured from V1_PR_PROMPTS.md content. Rationale paragraphs filled in by Claude based on derivable signals (adversarial harness failure modes, baseline current-state findings, V1_PR_PROMPTS.md scope/risks sections); user delegated rationale-drafting via "go with your best choices" rather than pasting authoritative paragraphs — surface for amendment if any rationale misreads the original intent. OQ9 resolved (audit chain unbounded for v1). OQ1–OQ8 remain open for their respective PR design gates. | Rachin + Claude |
