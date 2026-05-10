# Fyralis v1 Substrate Implementation — Master Prompt File

This file contains five sequenced Claude Code prompts implementing the v1 substrate decisions from `services/think/SUBSTRATE_SEMANTICS.md`. Each PR is run in its own focused Claude Code session. PRs must be merged in order. Do not parallelize.

## How to use this file

1. **PR 0 (this section, run before PR 1):** Establish baseline understanding. Run the audit prompt below.
2. **PR 1-5:** Run each prompt in a fresh Claude Code session, in order. Each PR must be merged and verified before the next begins.
3. **Between PRs:** Run the full harness (`python -m tests.synthesis_harness`) and confirm green. If anything regresses, fix before proceeding.
4. **User review gates:** PRs 3, 4, and 5 require explicit design review with the user before implementation. The prompts enforce this — do not let Claude Code skip the gate.

## Sequence rationale

- **PR 1 (Q5 — Audit chain):** Smallest change. Establishes audit invariants every other PR depends on. Lowest risk.
- **PR 2 (Q4 — Confidence-as-strength):** No schema change. Affects all subsequent reconciliation. Must come before PR 4.
- **PR 3 (Q3 — Preconditions):** First major schema change. Must come before PR 5 because hierarchy extends precondition resolution.
- **PR 4 (Q1 — LLM second-pass reconciliation):** Reconciler changes. Must come before PR 5 because hierarchy extends the second-pass with hierarchy context.
- **PR 5 (Q2 — Entity hierarchy):** Largest change. Depends on PR 1, 3, 4. Ships last.

## Prerequisite fixes

Before PR 1 starts, the following findings from the prior adversarial run must be merged:

- B1: Direct `apply_diff` callers race-create duplicates (relocate region lock)
- B2: Trigger-id race leaks UniqueViolationError (translate to AlreadyAppliedError)
- Falsifier within_window parser silent failure (loud failure)
- Cascade silent failure (loud failure)
- Multi-statement migration transaction safety

If these aren't merged, PR 1 should NOT start. The substrate is unsafe under concurrency until these land.

---

# PR 0 — Baseline audit (run before PR 1)

## Mission

Before any v1 implementation begins, produce a written audit of the current state of the codebase relative to the five substrate decisions. This is read-only work. Do not modify any code.

## Steps

1. Read `services/think/SUBSTRATE_SEMANTICS.md` fully. Internalize the five decisions and their interaction map.
2. Read `tests/synthesis_harness/REPORT.md`, `tests/synthesis_harness/adversarial/TRIAGE.md`, and `tests/synthesis_harness/adversarial/COVERAGE_GAPS.md`. Note which findings are upstream of v1 work.
3. Read three existing migration files to understand the migration framework (raw SQL, Alembic, custom?).
4. Read three existing test files in `tests/synthesis_harness/` to understand harness conventions.
5. For each of the five decisions, audit current state:

### Q5 — Audit chain
- Where is the event log today (table name, schema)?
- What information does each event carry (cause_id, timestamps, fields changed)?
- Does reversal-of-reversal currently preserve all three events, or does it collapse?
- Is the audit chain reachable from a Model query?

### Q4 — Confidence-as-strength
- What does the extraction prompt say about confidence ranges today?
- Is there any current notion of commitment strength?
- How does `bulk_confidence_update` currently work? Where is it called?

### Q3 — Preconditions
- Does the `commitments` table have a `precondition` field today? A `state` field?
- How does cascade currently handle Commitments? What triggers state changes?
- Are there any existing latent/conditional concepts in the codebase?

### Q1 — Reconciliation
- What does the reconciler do today (code location, current logic)?
- What thresholds, if any, are configured?
- Is there a `reconciliation_decisions` cache table or equivalent?

### Q2 — Hierarchy
- Is there any existing entity_relationships concept in schema or code?
- How does Pathway A (graph walk) currently work — does it walk relationships at all?
- Are scope_entities currently flat?

## Output

Produce `services/think/V1_BASELINE.md` with sections for each decision and a summary at the top. Each section should be 1-2 paragraphs of concrete findings — not abstractions, but specific code references and current behavior.

End the document with a "Readiness check" section that confirms all prerequisite fixes (B1, B2, falsifier parser, cascade, migration safety) are merged. If any are not merged, list them as blockers and stop.

**Show this document to the user before any implementation begins.** Do not start PR 1 until the user confirms the baseline is accurate.

---

# PR 1 — Q5: Audit chain preservation

## Mission

Implement audit chain preservation per `services/think/SUBSTRATE_SEMANTICS.md` Section Q5. Every state change on a Model creates a structured audit entry. Reversal-of-reversal sequences preserve all three events distinctly. Reconciliation merges union both source Models' chains.

## Pre-work

Read `services/think/V1_BASELINE.md` Q5 section to confirm current state. The implementation plan depends on what already exists.

## Scope

In scope:
- Audit chain entries with required fields: `event_id`, `timestamp`, `cause_id`, `previous_state`, `new_state`, `changed_fields`
- Reversal-of-reversal preservation: A → B → A produces three distinct events, with the third event including a `re_asserts_event_id` reference to the first
- Reconciliation-merge audit: when two Models merge, the merged Model's audit chain is the union of both source chains, ordered by timestamp; the merge itself is an event with `cause_type = "reconciliation_merge"` and `source_model_ids` populated
- Querying a Model returns current state by default; full chain is reachable via separate query
- API surface for querying audit chain: `get_audit_chain(model_id) -> List[AuditEvent]`

Out of scope:
- UI changes to render audit chains (substrate-only PR)
- Migration of pre-existing audit data (greenfield audit chain; existing event log can remain alongside)
- Performance optimization for very long chains (defer to v2)

## Files touched

- `services/think/audit.py` — new module for audit chain logic
- `services/models/repo.py` — emit audit events on state changes
- Schema migration for audit chain table (if not present) or extension (if event log already exists)
- `tests/synthesis_harness/cases_audit_chain.py` — new adversarial scenarios
- `services/think/SUBSTRATE_SEMANTICS.md` — update with implementation references

## Schema work

Decide based on baseline: extend existing event log or create new audit_events table. If creating new:

```sql
CREATE TABLE audit_events (
    event_id BIGSERIAL PRIMARY KEY,
    model_id UUID NOT NULL REFERENCES models(id),
    tenant_id UUID NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    cause_id UUID,
    cause_type TEXT NOT NULL,
    previous_state JSONB,
    new_state JSONB NOT NULL,
    changed_fields TEXT[],
    re_asserts_event_id BIGINT REFERENCES audit_events(event_id),
    source_model_ids UUID[]
);
CREATE INDEX idx_audit_events_model_id ON audit_events(model_id, occurred_at);
CREATE INDEX idx_audit_events_tenant ON audit_events(tenant_id, occurred_at);
```

The migration must be transaction-safe. Wrap in BEGIN/COMMIT or use the project's migration framework correctly.

Document rollback in the migration file.

## Tests required

Add ~15 scenarios to `tests/synthesis_harness/cases_audit_chain.py`:

- 3 basic audit creation (create, update, archive — verify events emitted with correct fields)
- 3 reversal-of-reversal (A → B → A, verify three events, verify `re_asserts_event_id` linkage)
- 3 reconciliation-merge audit union (two Models merge, verify chain is the union)
- 2 chain query correctness (long chain, verify ordering)
- 2 cause linkage (verify every audit event has a valid cause_id)
- 2 multi-tenant isolation (audit events for tenant A don't leak to tenant B)

Each scenario must specify expected behavior precisely. Use exact assertions where possible.

## Verification gates

1. All existing 149+ harness scenarios pass.
2. New audit chain scenarios pass.
3. Schema migration applies and rolls back cleanly in a test database.
4. Manual smoke test: create a Model, update it, archive it, query the chain — verify all three events present with correct ordering.
5. No regression in existing reconciliation tests (audit emission shouldn't change reconciliation behavior).

## Engineering estimate

3-4 days focused work. The Q5 work is mostly additive — emit audit events at existing state-change points. The complexity is in the reconciliation-merge audit union, which requires careful handling of timestamps and source_model_ids tracking.

## Risks

- **Audit emission performance:** if every state change writes to the audit table synchronously, write throughput drops. Mitigation: ensure audit writes are part of the same transaction as the state change (atomic) but consider async indexing for downstream consumption.
- **Existing event log conflict:** if there's already an event log doing some of this work, the new audit chain may duplicate. Baseline document should clarify this. If duplication is unavoidable, document the relationship between the two and which is canonical.
- **Reconciliation-merge audit complexity:** the union-of-chains logic is subtle, especially when both source Models have their own audit chains with potentially conflicting timestamps. Test thoroughly.

## Discipline

- Match existing project conventions for logging, error handling, type discipline.
- No new dependencies without justification.
- Read at least three existing test files before writing new test code.
- If the baseline shows the existing event log already does this, scope this PR down to documenting and adding tests rather than building parallel infrastructure.

## Done criteria

1. Audit chain table exists with the specified schema (or existing event log is extended and documented).
2. Every state change in `services/models/repo.py` emits an audit event.
3. Reversal-of-reversal correctly produces three distinct events with `re_asserts_event_id`.
4. Reconciliation-merge correctly unions audit chains.
5. `get_audit_chain(model_id)` is a clean public API.
6. All harness scenarios pass.
7. SUBSTRATE_SEMANTICS.md is updated with a "Q5 implementation" subsection pointing to the code.

---

# PR 2 — Q4: Confidence-as-strength + max-confidence merge exception

## Mission

Implement confidence-as-strength semantics on Commitment Nodes per SUBSTRATE_SEMANTICS.md Q4. Update the extraction prompt to map linguistic markers to confidence ranges. Add the max-confidence exception to Commitment reconciliation.

## Pre-work

PR 1 must be merged. Audit chain emission must work for the merge events this PR introduces.

Read `services/think/V1_BASELINE.md` Q4 section to confirm current prompt state and reconciliation behavior.

## Scope

In scope:
- Update the extraction prompt to map linguistic markers to confidence ranges:
  - Aspirational ("would love to," "ideally," "in a perfect world") → 0.3-0.55
  - Targeted ("targeting," "aiming for," "planning to") → 0.55-0.75
  - Committed ("will," "promised," "guaranteed") → 0.75-0.95
- Add the max-confidence exception to the reconciler: when reconciling Commitment Nodes, take `max(existing.confidence, new.confidence)` instead of `bulk_confidence_update`
- The exception must be a single, well-named function: `commitment_merge_confidence(existing, new)`
- Audit chain (PR 1) must record the exception application: the audit event for a Commitment merge should note which confidence rule was used (max vs bulk_update)

Out of scope:
- Adding a separate `strength` field to Commitments (Q4-B; deferred to v2)
- Changing other Node types' merge logic
- Strength-based filtering in downstream consumers

## Files touched

- `services/think/prompt.py` (or equivalent) — update extraction prompt
- `services/think/reconciler.py` — add commitment_merge_confidence function and branch
- `services/think/applier.py` — call the right merge confidence function based on Node type
- `tests/synthesis_harness/cases_commitment_merge.py` — new adversarial scenarios
- `services/think/SUBSTRATE_SEMANTICS.md` — update with implementation references

## Schema work

None. This is a behavioral change.

## Prompt update specification

The extraction prompt currently produces confidence values. The update adds an explicit calibration anchor:

> When extracting a Commitment, use the speaker's linguistic markers to set confidence:
> - If the speaker uses aspirational language ("we'd love to," "ideally," "in a perfect world," "hopefully," "fingers crossed"), set confidence between 0.3 and 0.55.
> - If the speaker uses targeted language ("targeting," "aiming for," "planning to," "expecting to"), set confidence between 0.55 and 0.75.
> - If the speaker uses committed language ("will ship," "promised," "guaranteed," "committed to," "by [date], no later"), set confidence between 0.75 and 0.95.
> - If the speaker stacks hedges ("I think we'll probably get to it eventually, maybe sometime this quarter or next, no promises"), set confidence ≤ 0.55.

This calibration anchor should be tested explicitly against the linguistic adversarial scenarios that previously failed (`hedged_commitment_low_confidence`, `compound_linguistic_pressure`).

## Tests required

Add ~12 scenarios to `tests/synthesis_harness/cases_commitment_merge.py`:

- 3 calibration anchor tests (aspirational / targeted / committed language each maps to expected confidence range)
- 4 max-confidence exception tests:
  - Aspirational + committed → max wins (0.85, not averaged)
  - Two committed → still max
  - Two aspirational → max
  - Targeted + committed → max wins
- 3 audit chain tests: the merge event records the rule used and the source confidences
- 2 regression tests: ensure non-Commitment Nodes still use bulk_confidence_update (not max)

Re-run the previously failing linguistic adversarial scenarios. They should now pass.

## Verification gates

1. All 149+ existing harness scenarios pass.
2. New scenarios pass.
3. The two previously-failing real-LLM scenarios (`hedged_commitment_low_confidence`, `compound_linguistic_pressure`) now pass.
4. Calibration baseline shows ECE has not regressed for non-Commitment Node types.
5. Manual smoke test: create an aspirational Commitment, then a committed paraphrase, verify merge produces max confidence.

## Engineering estimate

3-4 days focused work. The exception is a single branch but the prompt calibration and the test scenarios take real time. Expect 1-2 days of prompt iteration to get the calibration anchor right.

## Risks

- **Prompt calibration regression:** changing the prompt may shift confidence values for non-Commitment types unintentionally. Calibration measurement is the safety net here. If calibration measurement isn't yet in place, this PR depends on it.
- **The exception is a load-bearing hack:** documented in SUBSTRATE_SEMANTICS.md Q4. If a second similar exception appears in a later PR, escalate to reconsidering Q4. The reconciler should log every application of the exception so frequency can be tracked.
- **Linguistic edge cases:** aspirational/targeted/committed boundaries are fuzzy in real language. Some scenarios will be inherently ambiguous. Document this explicitly.

## Done criteria

1. Extraction prompt updated with calibration anchor.
2. `commitment_merge_confidence` function exists and is called only for Commitment merges.
3. All harness scenarios pass.
4. Two previously-failing linguistic scenarios now pass.
5. Audit chain records the merge rule.
6. SUBSTRATE_SEMANTICS.md updated with Q4 implementation reference.

---

# PR 3 — Q3: Preconditions on Commitment

## Mission

Add precondition support to Commitment Nodes per SUBSTRATE_SEMANTICS.md Q3. Schema migration adds `precondition` and `state` fields. Lifecycle logic transitions latent Commitments to active when preconditions are satisfied. Cascade extends to handle precondition satisfaction.

## Pre-work

PR 1 and PR 2 must be merged. Audit chain must record state transitions. Commitment merge logic must already use the max-confidence exception.

Read `services/think/V1_BASELINE.md` Q3 section.

## Design review gate

Before implementing, produce `services/think/PR3_DESIGN.md` answering:

1. **Precondition forms:** the three forms (Decision reference, event reference, Commitment reference) — what's the JSON structure for each? What are the validation rules?
2. **State transitions:** what state changes are allowed (latent → active → completed → cancelled)? Can a completed Commitment go back to active?
3. **Cascade trigger semantics:** when a Decision's state changes, how does cascade find latent Commitments referencing it? When a signal arrives that might satisfy an event-reference precondition, how is matching done (LLM judgment? structured matching)?
4. **Downstream consumer behavior:** how does the recommendation feed handle latent Commitments — separate view, downweighted, or hidden?
5. **Reconciliation interaction:** can latent Commitments reconcile with active Commitments? With each other? What about Q4's max-confidence exception when one is latent?
6. **Backfill:** existing Commitments don't have `state`. Default to `active`. Existing Commitments don't have `precondition`. Default to NULL. Backfill is a single SQL statement.

**Stop and ask the user for review of the design document before any code is written.** This is non-negotiable.

## Scope (post-design-review)

In scope:
- Schema migration: add `precondition` (nullable JSONB) and `state` (enum, default 'active') to commitments table
- Three precondition forms with validation
- Lifecycle: Commitments with non-null precondition created as `latent`; transition to `active` on satisfaction
- Cascade extension: when a Decision/Commitment changes state, scan for latent Commitments with that reference
- Event-reference precondition satisfaction: a new signal triggers an LLM check ("does this signal satisfy the precondition?") within the referenced scope_entities
- Downstream consumer updates: recommendation feed filters by state; capacity calculations exclude latent
- Audit chain records all state transitions

Out of scope:
- UI for "pending preconditions" view (substrate work only; UI is separate)
- Bulk-edit operations on preconditions
- Time-based precondition forms (e.g., "after Jan 1") — deferred

## Files touched

- Schema migration file
- `services/models/repo.py` — Commitment creation handles precondition; emit state-change audit events
- `services/think/cascade.py` — extension for precondition satisfaction
- `services/think/precondition_resolver.py` — new module for resolving each precondition form
- `services/think/applier.py` — handle `state` field in claim_ops
- `services/think/validator.py` — validate precondition forms
- Recommendation feed filter logic (location TBD from baseline)
- `tests/synthesis_harness/cases_preconditions.py` — new adversarial scenarios
- `services/think/SUBSTRATE_SEMANTICS.md` — update with implementation references

## Schema migration

```sql
BEGIN;

CREATE TYPE commitment_state AS ENUM ('latent', 'active', 'completed', 'cancelled');

ALTER TABLE commitments
    ADD COLUMN state commitment_state NOT NULL DEFAULT 'active',
    ADD COLUMN precondition JSONB;

CREATE INDEX idx_commitments_state ON commitments(state);
CREATE INDEX idx_commitments_precondition_gin ON commitments USING gin(precondition);

-- Backfill: all existing commitments are 'active' (default).
-- Existing commitments have no precondition (NULL).

COMMIT;
```

Rollback:

```sql
BEGIN;
ALTER TABLE commitments DROP COLUMN state, DROP COLUMN precondition;
DROP TYPE commitment_state;
COMMIT;
```

The migration must be transaction-safe.

## Tests required

Add ~25 scenarios to `tests/synthesis_harness/cases_preconditions.py`. This is a substantial test surface because preconditions interact with cascade, reconciliation, and audit.

Categories:

- 4 precondition creation (each form, plus invalid forms that should fail validation)
- 4 lifecycle (latent → active → completed; latent → cancelled; active without precondition; completed Commitment cannot regress)
- 6 cascade satisfaction (Decision-ref triggered by Decision state change; event-ref triggered by signal; Commitment-ref triggered by referent completion; cascade depth limits respected; failed satisfaction logged loudly; satisfaction during active cascade)
- 4 downstream consumer (latent excluded from capacity; latent excluded from recommendation feed default view; pending view shows latent; transitions to active surface to feed)
- 4 reconciliation (latent-with-latent merge under Q4 max-confidence rule; latent-with-active blocked; precondition preservation on merge; conflicting preconditions on merge)
- 3 audit (each state transition recorded with cause; pending-time tracked)

The harness needs a multi-stage scenario mechanism if it doesn't already have one — preconditions span multiple signals and require simulated time. This may require a new harness submodule.

## Verification gates

1. All 149+ existing harness scenarios pass.
2. New precondition scenarios pass.
3. Schema migration applies and rolls back cleanly.
4. Backfill completes cleanly on a copy of production-like data.
5. Manual smoke test: create a conditional Commitment (Decision reference), make the Decision, verify the Commitment transitions to active automatically.
6. Cascade depth limits respected — chained precondition satisfaction doesn't infinite-loop.

## Engineering estimate

10-14 days focused work. This is the heaviest schema-bearing PR. The lifecycle logic and cascade extensions are non-trivial. The event-reference precondition (LLM-judged satisfaction) is an LLM integration that needs its own prompt design, testing, and threshold tuning.

## Risks

- **Cascade infinite loop:** chained preconditions (A precondition is B's completion, B's precondition is C's completion, C's precondition is A's completion) could spiral. Cascade depth limits must be enforced.
- **Event-reference satisfaction false positives:** the LLM may judge a signal as satisfying a precondition when it shouldn't. Mitigation: log every event-ref satisfaction with the LLM's reasoning; manual review audit.
- **Backfill correctness:** if any existing Commitment is in a state that isn't naturally 'active' (perhaps already-completed Commitments?), the backfill default may be wrong. Verify backfill logic against actual data shape during baseline review.
- **Recommendation feed regression:** filtering changes may hide commitments that users were seeing before. Communicate this change clearly.

## Done criteria

1. Design document reviewed and approved by user before implementation.
2. Schema migration applies, rolls back, and backfills cleanly.
3. Three precondition forms implemented with validation.
4. Lifecycle works for all transitions.
5. Cascade extends to precondition satisfaction.
6. Downstream consumers handle `state` correctly.
7. All harness scenarios pass.
8. Audit chain records all state transitions.
9. SUBSTRATE_SEMANTICS.md updated with Q3 implementation reference.

---

# PR 4 — Q1: LLM second-pass reconciliation

## Mission

Implement LLM-second-pass reconciliation per SUBSTRATE_SEMANTICS.md Q1. Three tiers: cosine ≥ 0.85 auto-merge; 0.65-0.85 LLM second-pass; < 0.65 create new. Add caching, budget caps, and threshold tuning infrastructure.

## Pre-work

PR 1, PR 2, PR 3 must be merged. Audit chain handles merges. Commitment merge exception is in place. Preconditions are in place (latent Commitments may need reconciliation under PR 1's audit rules).

Read `services/think/V1_BASELINE.md` Q1 section.

## Design review gate

Before implementing, produce `services/think/PR4_DESIGN.md` answering:

1. **LLM choice for second-pass:** use the production LLM (correlated failure modes risk) or a different model family (cost and latency)?
2. **Second-pass prompt template:** what's the exact prompt? Inputs: both Models' naturals, scope_entities, proposition_kind, current confidence values. Output: `{same_proposition: bool, reason: string}` (strict JSON). Include calibration: be conservative — when in doubt, return false.
3. **Cache invalidation:** when does the cache invalidate? On Model state change? On reconciliation policy change? Time-based?
4. **Budget cap behavior:** when budget exceeded, default to "create new" or to "flag for human review"?
5. **Threshold tuning:** how is the labeled dataset built? Where are disagreements logged for review?
6. **Audit:** every reconciliation decision (auto-merge, second-pass match, second-pass non-match, create-new) is logged. What level of detail?

**Stop and ask the user for review of the design document before any code is written.**

## Scope (post-design-review)

In scope:
- Three-tier reconciliation in the reconciler module
- New `reconciliation_decisions` table for caching second-pass results
- Per-think_run budget cap on second-pass LLM calls (default: 5)
- Detailed audit logging of every reconciliation decision
- Threshold tuning infrastructure: a labeled dataset format and tooling to compute precision/recall on different thresholds
- The LLM second-pass prompt with calibration

Out of scope:
- Background re-evaluation of past reconciliation decisions when thresholds change
- Multi-LLM ensemble for second-pass (single LLM only)
- UI for human review queue (just the queue itself; UI is separate work)

## Files touched

- `services/think/reconciler.py` — three-tier logic
- `services/think/reconciliation_prompt.py` — new module for second-pass prompt
- Schema migration for `reconciliation_decisions` table
- `services/think/reconciler_cache.py` — cache lookup and invalidation
- `services/think/audit.py` — audit logging extensions for reconciliation decisions
- `tests/synthesis_harness/cases_reconciliation_q1.py` — new adversarial scenarios
- `services/think/SUBSTRATE_SEMANTICS.md` — update with Q1 implementation reference

## Schema migration

```sql
BEGIN;

CREATE TABLE reconciliation_decisions (
    decision_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    model_id_a UUID NOT NULL REFERENCES models(id),
    model_id_b UUID NOT NULL REFERENCES models(id),
    cosine_similarity FLOAT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('auto_merge', 'second_pass_merge', 'second_pass_create', 'below_threshold_create', 'cache_hit')),
    reason TEXT,
    llm_response JSONB,
    decided_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_by_think_run_id UUID,
    invalidated_at TIMESTAMPTZ,
    UNIQUE (model_id_a, model_id_b)
);

CREATE INDEX idx_reconciliation_lookup ON reconciliation_decisions(model_id_a, model_id_b) WHERE invalidated_at IS NULL;

COMMIT;
```

Rollback drops the table.

## Tests required

Add ~20 scenarios to `tests/synthesis_harness/cases_reconciliation_q1.py`:

- 4 auto-merge scenarios (cosine ≥ 0.85, verify merge happens, audit recorded)
- 6 second-pass scenarios:
  - Paraphrase same proposition (LLM says yes, merge)
  - Similar but different proposition (LLM says no, create new)
  - Same proposition different scopes (LLM says related-not-identical, create new)
  - Borderline confidence cases (LLM produces conservative answer)
- 3 below-threshold scenarios (cosine < 0.65, no LLM call, create new)
- 3 budget cap (more than 5 candidates in one think_run, verify graceful degradation)
- 2 cache (same pair re-evaluated within window, cache hit; invalidation on Model state change)
- 2 commitment merge interaction: a paraphrased aspirational/committed pair, verify Q4 max-confidence exception applied after second-pass merge

Probabilistic scenarios: the second-pass LLM is non-deterministic. The harness must handle this — either through seeded LLM calls, recorded responses, or marked-probabilistic scenarios. Document the chosen approach.

## Verification gates

1. All 149+ existing harness scenarios pass.
2. New scenarios pass at the specified rate (probabilistic scenarios may have a tolerance).
3. Schema migration applies and rolls back cleanly.
4. Manual smoke test: two paraphrased Commitments arrive in sequence, verify second-pass triggers, verify merge result, verify audit chain.
5. Budget cap test: simulate 10 borderline candidates in one think_run, verify graceful degradation.
6. Cache test: same pair queried twice in quick succession, verify second query is cache hit.

## Engineering estimate

10-14 days focused work. The reconciler logic itself is straightforward (three-tier branching). The complexity is in the second-pass prompt design (probably 2-3 days of iteration to get right), the caching invalidation rules, and the threshold tuning infrastructure.

## Risks

- **Second-pass prompt calibration:** the LLM may be too aggressive or too conservative. Probably needs 1-2 weeks of real usage with logged disagreements to tune.
- **Cost:** second-pass LLM calls add inference cost per think_run. Budget cap is the primary mitigation, but in heavy-merger scenarios (e.g., a customer with many similar commitments), costs can spike.
- **Cache invalidation correctness:** if a cached "same proposition: false" stays cached after the underlying Models change in a way that should re-trigger evaluation, you have stale decisions.
- **Threshold sensitivity:** the 0.65/0.85 boundaries are guesses. Wrong boundaries produce systematically wrong merge behavior. Tracking and tuning is essential.

## Done criteria

1. Design document reviewed and approved.
2. Schema migration applies and rolls back cleanly.
3. Three-tier reconciliation works as specified.
4. Cache implemented with correct invalidation.
5. Budget cap enforced with graceful degradation.
6. Audit logging records every decision.
7. All harness scenarios pass (probabilistic ones at specified tolerance).
8. SUBSTRATE_SEMANTICS.md updated.

---

# PR 5 — Q2: Entity hierarchy

## Mission

Implement explicit entity hierarchy per SUBSTRATE_SEMANTICS.md Q2. New `entity_relationships` table. LLM-driven extraction of relationships. Hierarchy-aware retrieval (Pathway A) and reconciliation (second-pass extension). Nightly audit job for hierarchy health.

## Pre-work

PR 1-4 all merged. Audit chain works. Reconciliation second-pass works. Preconditions work.

Read `services/think/V1_BASELINE.md` Q2 section.

## Design review gate

This PR's design is the hardest. Before implementing, produce `services/think/PR5_DESIGN.md` answering:

1. **Hierarchy authoring (v1):** the user has decided on LLM-driven extraction. Document how this works: when does the extractor emit a relationship, what's the JSON structure, how is duplicate parent detection handled?
2. **Entity reconciliation:** when two parent entities look like they might be the same ("engineering" vs "the eng team"), the duplicate detection logic. Threshold for auto-merge of parent entities.
3. **Retrieval extension:** Pathway A walks how far up and down by default? Configurable per query?
4. **Reconciliation second-pass extension:** when does the second-pass receive hierarchy context? What's the prompt update?
5. **Precondition extension:** how does the precondition resolver walk hierarchy when checking event-reference satisfaction?
6. **Hierarchy update rules:** what happens when a relationship needs to change (deal moves to a different customer, employee changes teams)? Are old Models rebound or preserved?
7. **Audit job:** what runs nightly? What metrics are tracked? Where are duplicate-parent and stale-relationship findings surfaced?
8. **Backfill:** existing Models have flat scope_entities. Should existing implicit relationships be inferred (e.g., from existing actor-team patterns) or is hierarchy purely forward-looking?

**Stop and ask the user for review of the design document before any code is written.** This is the most important review gate of the entire v1 sequence.

## Scope (post-design-review)

In scope:
- Schema: new `entity_relationships` table with parent/child relations and confidence
- LLM extraction extension: emit relationships alongside Models
- Entity duplicate detection and reconciliation
- Pathway A extension to walk hierarchy
- Reconciliation second-pass receives hierarchy context
- Precondition resolver walks hierarchy
- Nightly audit job (or manual command if scheduling infrastructure isn't ready)
- Hierarchy health metrics

Out of scope:
- Hybrid LLM-proposed-human-confirmed authoring (deferred to v2)
- UI for managing hierarchy (substrate-only PR)
- Cross-tenant hierarchy patterns (each tenant's hierarchy is isolated)

## Files touched

This is the largest PR. Expected files:

- Schema migrations (entity_relationships, hierarchy_health_metrics)
- `services/think/hierarchy.py` — new module
- `services/think/extractor.py` — emit relationships
- `services/retrieval/primary.py` — Pathway A walk
- `services/think/reconciler.py` — second-pass extension
- `services/think/precondition_resolver.py` — hierarchy walk
- `services/jobs/hierarchy_audit.py` — new nightly job
- Multiple test files

## Schema migration

```sql
BEGIN;

CREATE TABLE entities (
    entity_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL,
    entity_type TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    aliases TEXT[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, entity_type, canonical_name)
);

CREATE TABLE entity_relationships (
    relationship_id BIGSERIAL PRIMARY KEY,
    tenant_id UUID NOT NULL,
    child_entity_id UUID NOT NULL REFERENCES entities(entity_id),
    parent_entity_id UUID NOT NULL REFERENCES entities(entity_id),
    relationship_type TEXT NOT NULL,
    confidence FLOAT NOT NULL DEFAULT 1.0,
    source_signal_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    invalidated_at TIMESTAMPTZ,
    UNIQUE (tenant_id, child_entity_id, parent_entity_id)
);

CREATE INDEX idx_relationships_child ON entity_relationships(child_entity_id) WHERE invalidated_at IS NULL;
CREATE INDEX idx_relationships_parent ON entity_relationships(parent_entity_id) WHERE invalidated_at IS NULL;

COMMIT;
```

Existing Models reference scope_entities as JSONB. The migration does NOT change this — entities and entity_relationships are a parallel structure. The hierarchy walk during retrieval reads scope_entities, looks up the entity in `entities`, walks `entity_relationships`.

## Tests required

Add ~30 scenarios. This is the largest test addition because hierarchy interacts with retrieval, reconciliation, preconditions, and cascade.

Categories:

- 5 entity creation and duplicate detection
- 5 hierarchy walk in retrieval (default depth, configurable depth, depth bounds, hierarchy walk crossing tenant boundary correctly blocked)
- 5 hierarchy in reconciliation second-pass (hierarchy context provided, LLM uses it correctly, related-not-identical case, scope-precision case)
- 4 precondition with hierarchy (deal-scoped event satisfies customer-scoped precondition, hierarchy walk in resolver)
- 4 hierarchy update (relationship invalidated when conflicting evidence, audit recorded, downstream effects)
- 4 audit job (duplicate parents flagged, stale relationships flagged, metrics computed correctly)
- 3 multi-tenant isolation (hierarchy in tenant A doesn't leak to tenant B)

## Verification gates

1. All 149+ existing harness scenarios pass.
2. All new hierarchy scenarios pass.
3. Schema migration applies, rolls back cleanly.
4. Retrieval performance does not regress significantly (hierarchy walk is bounded).
5. Manual smoke test: create entities, create a relationship, verify retrieval walks correctly.
6. Audit job runs cleanly on a populated test database.

## Engineering estimate

20-30 days focused work. This is the largest PR by 2-3x. Hierarchy authoring is unsolved at the customer experience level (the user accepted this tradeoff; the LLM-driven approach is documented as a known limitation). Expect significant time in:

- Entity duplicate detection tuning (1 week)
- Hierarchy walk in retrieval, including performance work (1 week)
- Reconciliation second-pass extension (3 days)
- Precondition resolver extension (3 days)
- Audit job and metrics (1 week)
- Test scenarios (1 week)

## Risks

- **Hierarchy authoring correctness:** LLM extraction will produce inconsistent relationships. Some signals will produce them, some won't. The substrate's correctness now partially depends on hierarchy correctness.
- **Performance:** hierarchy walk in retrieval adds query overhead. Depth bounds and indexing are mitigations but worth measuring.
- **Multi-tenant isolation:** the new hierarchy structure must not leak across tenants. Tenant-scoped queries everywhere.
- **Backfill:** if you choose to backfill existing implicit relationships, the backfill logic is non-trivial and risky. The simpler choice is forward-looking only — the user should decide.
- **Operational burden:** maintaining hierarchy is now an operational concern. Hierarchy health is a metric you'll watch.

## Done criteria

1. Design document reviewed and approved by user.
2. Schema migrations apply and roll back cleanly.
3. Entity reconciliation works.
4. Retrieval Pathway A walks hierarchy correctly with depth bounds.
5. Reconciliation second-pass uses hierarchy context.
6. Precondition resolver walks hierarchy.
7. Nightly audit job runs and produces metrics.
8. All harness scenarios pass.
9. Multi-tenant isolation verified.
10. SUBSTRATE_SEMANTICS.md updated with Q2 implementation reference.

---

# Final notes

## Total estimated time

PR 1: 3-4 days
PR 2: 3-4 days
PR 3: 10-14 days
PR 4: 10-14 days
PR 5: 20-30 days

**Total: 7-11 weeks of focused engineering.**

This is a real range, not a sandbagged estimate. If you're solo, the high end is more realistic. If you're using Claude Code aggressively with good prompts, the low end. Plan for the high end and celebrate if you beat it.

## Between-PR practices

After each PR merges:

1. Run the full harness. Confirm green.
2. Tag the release in git for clean rollback if needed.
3. Update SUBSTRATE_SEMANTICS.md with the implementation reference.
4. Run calibration (when in place) to ensure ECE hasn't regressed.
5. Take a short break before starting the next PR. Decision fatigue compounds across long architectural work.

## When to stop and ask the user

Each PR has explicit review gates. In addition, stop and ask if:

- The codebase contradicts the baseline document
- A schema migration touches data in unexpected ways
- An LLM prompt change shifts calibration measurably for non-target Node types
- A test reveals a structural finding that wasn't in the original triage report
- Engineering estimates blow past the documented range by more than 50%

The plan is a contract, but it's a living contract. Surface deviation, don't hide it.
