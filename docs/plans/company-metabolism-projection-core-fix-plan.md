# Company Metabolism And Projection Core Fix Plan

## Purpose

This plan turns the company-metabolism postmortem and the latest projection-layer
audit into a small set of high-leverage fixes. The target is not more machinery.
The target is a cleaner organism:

- the model layer remains canonical memory;
- SAGE remains the adaptive learner for routing, salience, and inquiry utility;
- projection remains the rebuildable read surface for employees, commitments,
  customers, goals, decisions, resources, and other living organizational objects;
- T4 remains the expensive review lane, not the default place to discover basic
  structure;
- the harness must prove these boundaries instead of merely proving that queues
  drained.

The 40-batch failed run showed a metabolic failure: repair waste could keep
creating new obligations after the parent obligation should have received a
terminal fate. The later 10-batch 5k run passed operationally, but exposed a
second, deeper shape problem: the projection layer is mechanically alive while
still semantically under-shaped for the product surface. Employees are projected
through `employee_profiles`, but commitments, customers, goals, and decisions are
mostly present only as subjects inside abstract projections such as
`constraints` and `decision_surfaces`.

The fix should make the system simpler to reason about, not broader. One memory
graph, one projection runtime, one harness scorecard, and a bounded set of
entity-first projection families.

## Current Evidence

### 40-batch failed run

Run: `company-metabolism-40b-20260702-101606`

Hard evidence from the saved postmortem:

- 7 T1 waves were persisted before interruption: 175 signals, 17.5% of the
  intended run.
- T1 work was valuable, but adaptive/control work grew too expensive.
- A T4 representation-repair loop repeatedly tried to repair invalid artifacts
  around a rejected self-edge.
- The deepest failure was missing terminal fate for repair waste:
  a repair run must consume a parent obligation before it can create new ones.
- Post-commit work accumulated faster than the harness drained it.
- Retrieval had low prompt ROI: across saved T1 waves, selected context
  reference ratio averaged 0.347.
- Post-commit batch semantics made heavy action rollback risk larger than
  necessary.

The residual-fate fix addresses the exact repair loop, but the surrounding
metabolism still needs stronger proof gates.

### 10-batch 5k passed run

Run: `company-metabolism-10batch-5k-freshdb3-20260702`

Hard evidence from `benchmark_summary.md`:

- Status: passed.
- Signals: 250.
- T1 batches: 10 successful, 0 exhausted, 0 retries.
- Think runs: 26 successful, 0 failed.
- Pending triggers: 0.
- Post-commit pending/dead-lettered: 0.
- Company intelligence score: 0.9545.
- Product value score: 0.8929.
- Efficiency score: 0.75.
- Calibration ECE: 0.2146.
- Main T1 LLM time: 475.578s.
- Non-main T1 residual: 243.471s.
- `context_plan` total: 255.936s, p95 46.548s.
- Adaptive inquiry total: 253.245s, p95 46.431s.
- Top costly inquiry paths:
  - `focused_index`: 55.920s total, p95 13.174s.
  - `semantic_terms`: 42.538s total, p95 13.406s.
  - `sage_reader`: 30.681s total, p95 619ms, one long outlier.
- Background maintenance cost was too large for a passed product run:
  T4/background maintenance cost was roughly 44.8% of total LLM cost in the
  later cost audit.
- Relationship candidates: 231 total, 12 accepted, 173 candidate, 17
  needs_review, 29 retired.
- Active graph was dominated by generic support edges: about 770 supports out of
  874 active edges.
- Relation frames were accepted and mostly projected: 13 accepted frames, 38
  relation edge projections.

### Projection-layer audit

Operationally healthy:

- `materialize_projections` processed all observed actions in the 10-batch run.
- Projection refresh jobs drained: no pending, leased, dead, or last-error rows
  in the inspected run state.
- Projection snapshots existed for current core families:
  `constraints`, `decision_surfaces`, `employee_profiles`, and `resources`.
- Projection dependencies were present and deduped.
- Relation edge projections existed for `blocks`, `early_warning_for`, and
  `contributes_to_resolution`.

Semantically incomplete:

- There is no first-class `commitments` projection family.
- There is no first-class `customers` projection family.
- There is no first-class `goals` projection family.
- There is no first-class `decisions` projection family.
- Commitments currently appear as subjects in generic projections, for example
  `commitment:<id>:constraints` or `commitment:<id>:decision_surface`, but the
  product surface should be able to ask for "commitments" directly.
- `employee_profiles` is a real employee surface, but product-facing harness
  language should treat it as the current `employees` family until a rename or
  alias is justified.

Compatibility gap:

- The delta projection runtime updates snapshots, dependencies, and jobs.
- `projection_checkpoints` can remain empty in delta-only operation.
- `ProjectionRepo.list_staleness` still relies on checkpoints, so a caller can
  report `no_checkpoint` even when delta snapshots are fresh and the queue is
  drained.

Efficiency gap:

- The projection queue is correctness-safe but noisy. The inspected run had a
  high refresh-jobs-to-final-snapshots ratio, with many subjects refreshed
  repeatedly.
- `_projection_names_for_apply_summary` routes broadly. This avoids missed
  updates, but it spends projection work where a more precise entity/family
  router would be cleaner.

## Design Doctrine

### The simplest powerful architecture

Do not add a second brain.

The system should understand the company through three different, cooperating
forms of memory:

1. Canonical model graph: durable claims, relations, events, provenance, and
   pattern models.
2. SAGE adaptive profile: learned routing utility, salience, company-local
   priors, inquiry policy, and retrieval feedback.
3. Projection read surfaces: rebuildable entity-first views that make the graph
   usable by product paths, retrieval, and health checks.

Patterns belong in the model layer when they are stable and explainable. SAGE
helps discover where to look and how hard to look. Projections make the resulting
company state legible and fast to consume.

### What success looks like

The next successful system should pass four tests at once:

- Semantic shape: the system can show first-class projected employees,
  commitments, customers, goals, decisions, and resources.
- Freshness: delta projection freshness is reported accurately without relying
  on legacy checkpoints.
- Metabolic efficiency: repeated refreshes, zero-yield retrieval branches, and
  background T4 maintenance are bounded by objective budgets.
- Product intelligence: company-intelligence and product-value scores do not
  regress while the system becomes cheaper and more interpretable.

## Implementation Phases

## Phase 0: Add Projection And Metabolism Proof Gates First

### Why

The current harness can pass while projection semantics are under-shaped. That
is the wrong failure boundary. The harness already knows queues drained and
Think succeeded; it also needs to know whether the projected company is actually
usable.

### Change

Extend the existing company vitals and storyline benchmark report paths. Do not
create a parallel checker.

Add a projection-metabolism section to the report with:

- available projection families;
- required product projection families;
- snapshot count by projection family;
- subject prefix matrix by projection family;
- projection refresh jobs by status;
- refresh-jobs-to-snapshots ratio;
- max refresh count per subject in the run;
- delta staleness state by projection family;
- relation frame projection expected count, actual count, skipped count, and
  skip reasons;
- first-class entity projection coverage for employees, commitments, customers,
  goals, decisions, and resources.

Add non-LLM metabolism metrics with:

- inquiry action p50/p95/max by path;
- zero-yield action count by path;
- `context_plan` p95 and total;
- adaptive inquiry p95 and total;
- T4/background LLM cost as a share of total cost;
- relationship candidate funnel: candidate, needs_review, accepted, retired;
- generic `supports` edge share.

### Success Definition

Objectively successful when all are true:

- Re-rendering the current 10-batch 5k report exposes the known projection shape
  gap: missing first-class `commitments`, `customers`, `goals`, and `decisions`.
- The report distinguishes "projection queue healthy" from "projection product
  surface complete".
- The report computes refresh-jobs-to-snapshots ratio from saved artifacts or
  DB-backed vitals when DB access is available.
- The report computes relation frame projection expected vs actual, and does not
  hide skipped projections inside a generic success count.
- Existing pass/fail behavior is unchanged unless a new strict flag is enabled.

### Tests

- Unit test the report renderer with a fixture containing:
  - healthy queue but missing required projection families;
  - one skipped relation projection;
  - high refresh-jobs-to-snapshots ratio.
- Unit test that old report artifacts without the new fields render with
  `unknown` instead of crashing.
- Run focused benchmark/vitals tests:
  - `.venv/bin/python -m pytest tests/unit/test_storyline_batch_benchmark.py -v --tb=short`
  - `.venv/bin/python -m pytest tests/unit/test_company_vitals.py -v --tb=short`
- Artifact-rerender the 10-batch 5k report and verify the new section names the
  current gaps.

## Phase 1: Fix Delta Projection Freshness Semantics

### Why

The delta runtime is the current operational path, but staleness still speaks
legacy checkpoint language. This creates false doubt: a projection can be fresh
by delta queue evidence while `list_staleness` says `no_checkpoint`.

### Change

Make projection freshness mode-aware.

Minimal implementation:

- Keep legacy checkpoint behavior for full-scan/checkpoint projection runs.
- Add delta-aware freshness calculation to `ProjectionRepo.list_staleness`.
- If a checkpoint exists, preserve the current checkpoint result.
- If no checkpoint exists:
  - stale if any matching refresh job is pending, leased, retrying, failed, or
    dead;
  - stale if there are model events and no snapshots for that projection;
  - current with reason `delta_queue_current` when snapshots exist and all
    matching delta jobs are processed;
  - unknown with reason `delta_queue_no_route_evidence` only when neither
    snapshot nor route evidence exists.
- Expose freshness mode in the returned object or metadata so callers can tell
  checkpoint freshness from delta freshness.

More robust follow-up, if needed:

- Add a small `projection_route_watermarks` table for "router observed model
  events through event X for projection Y". Only add this if the repo-level query
  cannot answer freshness honestly from jobs and snapshots.

### Success Definition

Objectively successful when all are true:

- A delta-only run with empty `projection_checkpoints`, processed refresh jobs,
  and snapshots reports `is_stale=False` with reason `delta_queue_current`.
- A projection with pending refresh jobs reports stale with reason
  `pending_refresh_jobs`.
- A projection with dead/failed jobs reports stale with reason
  `failed_refresh_jobs`.
- A projection with model events but no snapshot reports stale with reason
  `no_snapshot`.
- Existing checkpoint-based tests still pass unchanged.
- No caller can present `no_checkpoint` as the primary state for a healthy
  delta-queue projection.

### Tests

- Extend `services/domain/projections/tests/test_repo.py`:
  - delta-only current with empty checkpoints;
  - pending job stale;
  - failed/dead job stale;
  - no snapshot stale;
  - checkpoint current path unchanged;
  - requested projection order and dedupe behavior unchanged.
- Extend `services/domain/projections/tests/test_runtime.py` only if the runner
  needs to persist extra freshness metadata.
- Run:
  - `.venv/bin/python -m pytest services/domain/projections/tests/test_repo.py -v --tb=short`
  - `.venv/bin/python -m pytest services/domain/projections/tests/test_runtime.py -v --tb=short`

## Phase 2: Add Entity-First Projection Families

### Why

The product and retrieval surfaces should not have to reverse-engineer a company
from abstract axes. `constraints` and `decision_surfaces` are valuable, but they
are not a substitute for direct entity surfaces like commitments and customers.

This is the main projection-layer fix.

### Change

Add thin first-class projectors, not a new reasoning system:

- `commitments`
- `customers`
- `goals`
- `decisions`

Keep `employee_profiles` as the current employee projection family. Treat it as
the `employees` family in harness coverage, unless a product/API rename later
justifies a compatibility alias.

Use one shared helper for entity projectors so the implementation remains small:

- subject discovery by entity key, scope entity, claim role, and relation
  participant;
- evidence collection from canonical models, model events, relation frames, and
  accepted edges;
- payload validation;
- dependency emission for source models and source events;
- stable sorting and confidence aggregation.

Do not store new truth in these projections. Every payload must be rebuildable
from the canonical model graph.

### Projection Payload Contract

Every entity-first projection snapshot should include:

- `kind`: projection family name;
- `subject_key`: stable entity subject key;
- `entity_type`: `employee`, `commitment`, `customer`, `goal`, `decision`, or
  `resource`;
- `canonical_label`: best known label/name;
- `status`: current lifecycle/status when known;
- `confidence`: numeric confidence, bounded 0 to 1;
- `last_evidence_at`: latest supporting event timestamp when known;
- `evidence_model_ids`: bounded, stable list;
- `evidence_event_ids`: bounded, stable list;
- `related_entity_refs`: bounded refs to connected employees, commitments,
  customers, goals, decisions, and resources;
- `open_questions`: bounded refs or summaries for unresolved uncertainty;
- `needs_review`: boolean or reason list when evidence conflicts.

Family-specific payloads:

- `commitments`: owner, promise, due/evaluate-at signal, status, blockers,
  downstream risk, customer/goal/decision linkage, latest fulfillment evidence.
- `customers`: current health, risk drivers, commitments, active blockers,
  renewal/implementation state, recent incidents, open questions.
- `goals`: objective, owner, progress signal, blockers, commitments,
  customer/product impact, decision dependencies.
- `decisions`: decision pressure, options/tradeoffs, owner, affected commitments,
  affected customers/goals/resources, blockers, status, rationale evidence.
- `employee_profiles`: capabilities, load/capacity, work style/preferences,
  commitments owned, support needs, recurring coordination role.

### Success Definition

Objectively successful when all are true:

- `available_projection_names()` includes `commitments`, `customers`, `goals`,
  and `decisions`, plus the existing core projectors.
- In a synthetic projection fixture, each new family produces at least one
  snapshot with nonempty model/event dependencies.
- In the 10-batch 5k harness, the report shows nonzero first-class coverage for:
  - employees via `employee_profiles`;
  - commitments via `commitments`;
  - customers via `customers`;
  - goals via `goals`;
  - decisions via `decisions`;
  - resources via `resources`.
- For every Think-touched entity of those types, either a matching projection
  snapshot exists or the report records a deterministic not-applicable reason.
- Every snapshot payload satisfies the common contract above.
- Company intelligence score does not fall by more than 0.02.
- Product value score does not fall by more than 0.02.
- No new architecture import violations are introduced.

### Tests

- Add projector unit tests:
  - `services/domain/projections/tests/test_commitments.py`
  - `services/domain/projections/tests/test_customers.py`
  - `services/domain/projections/tests/test_goals.py`
  - `services/domain/projections/tests/test_decisions.py`
- Extend:
  - `services/domain/projections/tests/test_catalog.py`
  - `services/domain/projections/tests/test_subjects.py`
  - `services/domain/projections/tests/test_router.py`
  - `services/domain/projections/tests/test_runtime.py`
- Add payload contract tests for required keys, bounded evidence lists, stable
  sorting, and dependency replacement.
- Run:
  - `.venv/bin/python -m pytest services/domain/projections/tests -v --tb=short`
  - `ruff check --select E9,F63,F7,F82,F821,F811,F401 services/domain/projections`
  - `lint-imports`

## Phase 3: Make Projection Routing Precise And Coalesced

### Why

The projection runtime currently favors safety by routing broadly. That was the
right early move, but at 5k models it creates metabolic noise: too many refresh
jobs for too few final snapshots.

### Change

Replace broad projection-family selection with an explicit entity/family router.

Implementation shape:

- Keep `_projection_names_for_apply_summary` as the compatibility entry point.
- Internally route by operation type, domain tags, roles, entity refs, and
  relation participants.
- Route `commitments` only when commitment entities, commitment roles, or
  promise/obligation signals are present.
- Route `customers` only when customer entities, renewal/churn/relationship
  terms, customer commitments, or customer incidents are present.
- Route `goals` only when goals/objectives/initiatives/outcomes are present.
- Route `decisions` only when decisions, options, tradeoffs, approvals, owners,
  or prioritization pressures are present.
- Route `employee_profiles` only when actor/employee/capacity/support/work-style
  evidence is present.
- Route `constraints` and `decision_surfaces` for their abstract purposes, but
  no longer use them as a substitute for every entity shape.
- Coalesce refresh jobs by `(tenant_id, projection_name, projection_version,
  subject_key)` across a drain window.
- If a matching job completed very recently in the same drain cycle and no newer
  source event exists for that subject, skip re-enqueue and record
  `coalesced_recent_refresh`.

### Success Definition

Objectively successful when all are true:

- On the 10-batch 5k harness, refresh-jobs-to-final-snapshots ratio is at or
  below 3.0.
- No required projection family coverage is lost.
- No pending or dead projection jobs remain after drain.
- Max refresh count for the same projection subject in one run is below 4,
  unless newer source events justify each refresh.
- Post-commit `materialize_projections` total time does not increase after
  adding entity-first families.
- Projection route reports include selected families and coalescing counts.

### Tests

- Extend `services/reasoning/think/tests/test_post_commit_op1.py` for routing
  from apply summaries:
  - commitment-only update routes `commitments` and relevant abstract surfaces;
  - customer-only update routes `customers`;
  - actor capacity update routes `employee_profiles`;
  - decision tradeoff routes `decisions` and `decision_surfaces`;
  - generic claim does not route every family.
- Extend projection store/router tests for:
  - pending-job dedupe;
  - recent-refresh coalescing;
  - source-event newer-than-refresh bypass.
- Run projection domain tests and post-commit tests.
- Run a 1-batch canary with projection stats enabled before any 10-batch run.

## Phase 4: Classify Relation Projection Skips

### Why

The inspected run had 13 accepted projectable relation frames and 38 projected
relation edges. That is nearly complete, but one projected edge was skipped.
This may be fine if it was a self-edge or missing participant, but it must not
be invisible.

### Change

Make relation projection attempts produce explicit outcomes:

- `projected`
- `skipped_self_edge`
- `skipped_missing_role`
- `skipped_conflicting_edge`
- `skipped_unsupported_relation_kind`
- `failed_unexpected`

Unexpected failures should fail a strict harness gate. Expected skips should be
counted and attached to the relation frame in the report.

### Success Definition

Objectively successful when all are true:

- For every accepted relation frame with `write_policy='project_edges'`, the
  report can compute expected projected edge slots.
- Actual projected plus expected skipped equals expected slots.
- `failed_unexpected` count is 0 in strict mode.
- The 10-batch harness no longer has an unclassified `edge_projection_failed`
  reason.

### Tests

- Unit test relation projection for complete participants.
- Unit test self-edge skip.
- Unit test missing role skip.
- Unit test conflicting existing edge skip.
- Add report fixture with one expected skip and one unexpected failure.

## Phase 5: Bound Non-LLM Retrieval And Inquiry Latency

### Why

The 10-batch run passed, but `context_plan` and adaptive inquiry were too slow.
The system should not spend 13 seconds on a zero-yield retrieval branch inside a
batch that already has enough useful context.

This belongs mostly in SAGE and retrieval policy, not projection.

### Change

Add bounded retrieval branch economics:

- Per-action wall budgets for `focused_index`, `semantic_terms`, and
  `sage_reader`.
- A cheap answerability preflight before expensive `focused_index` scans.
- Zero-yield suppression keyed by tenant, lane, question type, source set, and
  entity neighborhood.
- Early stop when cheap branches already satisfy evidence sufficiency.
- Route-utility decay when selected models/observations are repeatedly unused.
- Overloaded-anchor penalty for high-support models after a small number of
  anchor slots.

Keep SAGE responsible for learning the routing utility, not for storing a
separate company truth model.

### Success Definition

Objectively successful when all are true:

- On 5k seed:
  - `focused_index` p95 is below 500ms and max below 2s;
  - `semantic_terms` p95 is below 500ms;
  - repeated zero-yield `semantic_terms` branches abort below 100ms after the
    first miss in the same lane/neighborhood;
  - `context_plan` p95 is below 5s;
  - adaptive inquiry p95 is below 5s;
  - non-main T1 residual share falls below 20%.
- Product value score does not fall by more than 0.02.
- Retrieval usefulness score does not fall by more than 0.02.
- Context-use telemetry shows selected context reference ratio improves or the
  report explains why a larger context was still needed.

### Tests

- Unit tests for retrieval action budget enforcement.
- Unit tests for zero-yield suppression and utility decay.
- Unit tests for overloaded-anchor selection penalties.
- Integration retrieval probe over a 5k seeded DB:
  - repeated owner/counterevidence questions;
  - known zero-yield semantic terms;
  - high-support anchor plus recent local evidence.
- Harness comparison:
  - run 1-batch canary;
  - run 5-batch canary;
  - then run 10-batch 5k if canaries preserve quality.

## Phase 6: Add A T4 Maintenance Governor

### Why

T4 is necessary for expensive pattern review, repair, and open-question search,
but it should not consume nearly half the run cost during a product-path harness
unless the product path explicitly needed it.

### Change

Add a run-scoped maintenance budget:

- `max_background_maintenance_llm_calls` default must be finite in harness mode.
- Separate budgets for:
  - latent relationship candidates;
  - open-question search;
  - representation repair.
- Prioritize maintenance by expected product value:
  - unblock product path;
  - prevent known repair loop;
  - improve accepted relation/pattern quality;
  - defer low-value candidate review.
- Add deterministic no-op handlers before LLM T4:
  - invalid self-edge repair is terminal without another LLM pass;
  - duplicate candidate signatures are auto-retired or batched;
  - low-value topology candidate no-op remains auto-completed.
- Emit maintenance budget decisions into the report.

### Success Definition

Objectively successful when all are true:

- Background maintenance LLM cost is at or below 25% of total LLM cost in a
  10-batch product-path harness.
- Background maintenance LLM calls are at or below 3 in the 10-batch harness
  unless strict report evidence shows a product-path dependency.
- No repeated T4 repair loop can be created from the same parent residual,
  source, validator reason, and op signature.
- Pending T4 does not grow while T1 progress is flat.
- Company intelligence and product value scores remain within 0.02 of baseline.

### Tests

- Unit test run-scoped T4 budget accounting.
- Unit test duplicate repair signature fuse.
- Unit test deterministic terminal handling for invalid self-edge repair.
- Unit test that product-critical T4 can still run when the budget allows it.
- Harness:
  - 5-batch canary with maintenance budget enabled;
  - 10-batch 5k comparison with cost ledger assertions.

## Phase 7: Improve Relationship Candidate Quality Before T4 Review

### Why

The system proposed 231 relationship candidates and accepted 12. That is too
much review debt. Pattern finding should be abundant internally, but promotion
to review should be selective.

### Change

Add a deterministic pre-review gate for relationship candidates:

- novelty: not already represented by an accepted edge/frame/pattern;
- support: either multi-source support or one high-confidence source with a
  concrete action implication;
- specificity: avoid generic `supports` unless no better typed relation exists;
- actionability: would accepting this change retrieval, projection, routing, or
  a product recommendation;
- counterexample survival: candidate is not immediately contradicted by known
  stronger evidence;
- graph delta: candidate would connect a meaningful component, explain a
  repeated failure, or improve a first-class projection.

Batch candidates by motif/neighborhood before T4 review so one Think call can
judge a cluster rather than one low-value candidate at a time.

### Success Definition

Objectively successful when all are true:

- Accepted relationship candidate ratio is at least 15% in the 10-batch harness,
  or the report explains that candidates were deliberately held below review
  threshold.
- `needs_review + candidate` backlog per signal is below 0.04.
- Generic `supports` active-edge share falls below 55%, or the report shows a
  typed-promotion backlog that accounts for the generic support edges.
- Exact duplicate natural groups fall by at least 50% from the current measured
  baseline.
- Largest component ratio improves without creating a giant meaningless
  component.

### Tests

- Unit tests for candidate gate scoring.
- Unit tests for support-edge typed promotion suggestions.
- Unit tests for cluster-level T4 candidate review.
- Graph-health report fixture for duplicate natural groups and support share.
- Harness graph metrics after 5-batch and 10-batch runs.

## Phase 8: Reduce Seed And Sidecar Cost After Correctness Is Locked

### Why

Seed cost is not the deepest correctness problem, but it slows every serious
run. The 5k seed is smaller than 15k but still expensive enough to discourage
iteration.

### Change

Optimize only after projection/freshness correctness gates exist:

- Bulk materialize sidecars instead of per-row work where possible.
- Cap or compress low-value feature postings.
- Normalize common sparse terms through a dictionary or shared postings path.
- Retain only useful processed projection refresh jobs in local harness mode, or
  summarize old processed jobs into run metrics before pruning.
- Add DB-size and table-bloat metrics to the seed report.

### Success Definition

Objectively successful when all are true:

- 5k seed wall time is below 45s on the local benchmark machine.
- `ANALYZE` time is below 5s.
- Feature postings stay below 100k rows for the 5k seed unless the report shows
  a deliberate quality reason.
- Answerability plus feature indexes stay below 80MB for 5k seed.
- Retrieval usefulness score does not fall by more than 0.02.
- The 10-batch harness remains projection-complete.

### Tests

- Add a seed microbenchmark script or extend the existing seed report path.
- Unit test sidecar generation cardinality caps.
- DB contract test for sparse-term and answerability completeness.
- Run:
  - 5k seed-only timing;
  - retrieval probe;
  - 5-batch harness;
  - 10-batch harness only after probes pass.

## Phase 9: Final Harness Staircase

Do not jump straight to the full expensive run after the first implementation
slice. Use a proof staircase.

### Step 1: Static And Unit Proof

Run:

```bash
ruff check --select E9,F63,F7,F82,F821,F811,F401 .
lint-imports
.venv/bin/python -m pytest services/domain/projections/tests -v --tb=short
.venv/bin/python -m pytest services/reasoning/think/tests/test_post_commit_op1.py -v --tb=short
.venv/bin/python -m pytest tests/unit/test_storyline_batch_benchmark.py tests/unit/test_company_vitals.py -v --tb=short
```

Success:

- all pass;
- no architecture boundary regression;
- no projection test relies on live LLM;
- old projection behavior remains compatible.

### Step 2: Artifact Rerender Proof

Re-render the saved 10-batch report with the new report code.

Success:

- report exposes known current projection gaps;
- report separates queue health from semantic projection completeness;
- no saved-artifact compatibility break.

### Step 3: DB Contract Proof

Run against a small synthetic DB fixture:

- insert models/events for one employee, one customer, one commitment, one goal,
  one decision, and one resource;
- route deltas;
- drain projection jobs;
- query snapshots, dependencies, freshness, and report metrics.

Success:

- every required family has at least one snapshot;
- dependencies are nonempty;
- `list_staleness` returns delta-current;
- no pending/dead jobs remain.

### Step 4: 1-batch Canary

Run one company-metabolism batch with projection strict metrics enabled but
without full expensive maintenance.

Success:

- T1 succeeds;
- post-commit drains;
- required projection families appear when applicable;
- no unexpected relation projection skip.

### Step 5: 5-batch Canary

Run five batches.

Success:

- quality scores remain within 0.02 of the 10-batch baseline where comparable;
- `context_plan` p95 below 5s after retrieval changes;
- background T4 cost share below 25%;
- projection jobs-to-snapshots ratio below 3.0;
- no strict projection gaps.

### Step 6: 10-batch 5k Harness

Only after Steps 1-5 pass, rerun the 10-batch 5k harness.

Success:

- status passed;
- no required failures;
- pending triggers 0;
- failed Think runs 0;
- pending/dead post-commit actions 0;
- company intelligence score at least 0.93;
- product value score at least 0.87;
- efficiency score above the previous 0.75 baseline;
- calibration ECE improves below 0.18 or is explicitly explained by future
  validation distribution;
- all required projection families covered;
- delta freshness current;
- relation projection unexpected skips 0;
- background T4 cost share below 25%;
- non-main T1 residual share below 20%.

## Recommended Execution Order

1. Add report/vitals proof gates.
2. Fix delta projection freshness semantics.
3. Add entity-first projection families.
4. Tighten projection routing and coalescing.
5. Classify relation projection skips.
6. Bound retrieval/inquiry latency.
7. Add T4 maintenance governor.
8. Improve relationship candidate quality.
9. Optimize seed/sidecar cost.

This order is deliberate. First make the system tell the truth about itself,
then fix projection correctness, then reduce waste. Speed work before semantic
proof would make the next run cheaper but less trustworthy.

## Non-Goals

- Do not move durable truth into projections.
- Do not make SAGE own stable pattern memory.
- Do not add a second projection runtime.
- Do not loosen harness gates to make runs look healthier.
- Do not run the full 40-batch harness until the 10-batch 5k staircase passes.

## The Core Bet

The strongest version of this system is not a giant learner that absorbs every
responsibility. It is a disciplined metabolism:

- models remember;
- SAGE attends and adapts;
- projections make the organization readable;
- T4 reviews expensive uncertainty;
- the harness proves the loop is alive, bounded, and useful.

That is the elegant path because each layer does one kind of work and gives the
next layer a sharper surface.
