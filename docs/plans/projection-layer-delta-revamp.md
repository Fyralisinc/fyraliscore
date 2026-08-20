# Projection Layer Delta Revamp

## Boundary

The model layer remains the canonical memory graph. It stores durable facts,
claims, edges, events, and provenance. It does not know which projection needs
which model.

The projection layer owns rebuildable views over that graph. A projection can be
discarded and recomputed from models. Projection freshness is maintained by
mining model-event deltas rather than repeatedly scanning the full graph.

## Components

### Model Event Delta Source

Input:
- Tenant id.
- Recently changed model ids from a Think post-commit action.
- Model events already emitted by the model layer.

Process:
- Load model events for the changed model ids.
- Preserve event payload, changed fields, model scope, claim roles,
  proposition kinds, domain tags, and event ids.

Output:
- Ordered `ModelEvent` records used as projection invalidation input.

Connects to:
- `services.domain.projections.store.fetch_events_for_models`.
- `services.reasoning.think.post_commit`.

### Projection Dependency Ledger

Input:
- A materialized projection snapshot.
- The evidence graph returned by its projector.

Process:
- Store precise dependency refs such as model ids and model event ids.
- Replace a snapshot's dependencies after every successful refresh.
- Keep dependencies tenant-scoped and rebuildable.

Output:
- `projection_dependencies` rows keyed by projection, subject, dependency
  kind, and dependency id.

Connects to:
- `ProjectionRunner.run_queued_refresh_jobs_once`.
- `ProjectionStore.replace_projection_dependencies`.
- `ProjectionStore.list_projection_subjects_for_dependency`.

### Projection Watch Keys

Input:
- Projector-declared broad interests such as event type, model id,
  proposition kind, claim role, domain tag, changed field, and scope entity.

Process:
- Store coarse invalidation subscriptions that are useful when a precise
  dependency is not yet present.
- Normalize and dedupe watch keys.

Output:
- `projection_watch_keys` rows.

Connects to:
- `ProjectionStore.replace_projection_watch_keys`.
- `ProjectionStore.list_projection_subjects_for_watch_key`.
- `services.domain.projections.router.watch_keys_for_event`.

### Delta Router

Input:
- A tenant id.
- A single `ModelEvent`.
- The projection registry.
- Optional selected projector keys from the caller.

Process:
- Derive precise dependency refs from the event.
- Derive broad watch keys from the event payload and metadata.
- Find affected subjects by dependency hits, watch-key hits, and cold-start
  projector matching.
- Filter all hits to the selected projector set when the caller runs a subset.
- Enqueue idempotent refresh jobs for affected projection subjects.
- Isolate projector match failures so one broken projector does not block
  routing for the rest.

Output:
- A `ProjectionRouteReport` with routed event count, enqueued job count, and
  route errors.

Connects to:
- `services.domain.projections.router.enqueue_refreshes_for_event`.
- `ProjectionStore.enqueue_projection_refresh_job`.
- `ProjectionRunner.run_queued_refresh_jobs_once`.

### Projection Refresh Queue

Input:
- Refresh jobs emitted by the delta router.
- Job reason, source event ids, projector identity, subject key, and subject
  payload.

Process:
- Deduplicate pending jobs for the same tenant/projection/subject.
- Lease bounded batches of pending jobs.
- Complete, retry, fail, or dead-letter jobs with structured error state.

Output:
- `projection_refresh_jobs` rows with status and retry metadata.

Connects to:
- `ProjectionStore.lease_projection_refresh_jobs`.
- `ProjectionStore.complete_projection_refresh_job`.
- `ProjectionStore.fail_projection_refresh_job`.
- `ProjectionRunner.run_queued_refresh_jobs_once`.

### Queued Projection Runner

Input:
- Leased refresh jobs.
- Projection registry.
- Projection store.

Process:
- Resolve the projector from the registry.
- Recompute the target subject from the canonical model graph.
- Validate the returned snapshot.
- Upsert the projection snapshot.
- Replace dependencies and watch keys.
- Mark the queue job complete.
- Fail only the affected job when a projector is missing or emits an invalid
  snapshot.

Output:
- Fresh `projection_snapshots`.
- Refreshed dependency/watch ledgers.
- A `ProjectionRefreshRunReport`.

Connects to:
- `services.domain.projections.runtime.ProjectionRunner`.
- `services.domain.projections.store.ProjectionStore`.

### Think Post-Commit Integration

Input:
- Think post-commit action with affected model ids.
- Existing projection materialization selector.

Process:
- Detect whether delta projection tables are available.
- If unavailable, fall back to the previous checkpoint materializer.
- If available, load model events, route deltas into refresh jobs, and run the
  queued refresh runner.
- Emit structured telemetry with `mode="delta_queue"`, routed event counts,
  enqueued job counts, processed job counts, failed job counts, route errors,
  and projection errors.

Output:
- Projection refreshes completed as part of post-commit work.
- Backward-compatible fallback when migration tables are absent.

Connects to:
- `services.reasoning.think.post_commit`.
- `services.domain.projections.router`.
- `services.domain.projections.runtime`.

## Validation Matrix

Unit and component checks:
- Projection router dedupes dependency/watch/direct hits.
- Router filters dependency/watch hits to the selected projector subset.
- Router isolates individual projector match failures.
- Store replacement functions delete old dependencies and insert normalized new
  dependencies.
- Store event loading dedupes and hydrates model events.
- Queued runner completes successful jobs.
- Queued runner fails unknown projectors.
- Queued runner fails invalid snapshots without falsely completing the job.

Integration checks:
- Projection domain test suite: 53 passed.
- Think post-commit DB tests: 17 passed.
- Think applier DB suite: 51 passed.
- Benchmark unit checks for downstream drain and efficiency accounting: 4
  focused tests passed.
- Focused ruff import/syntax check: passed.
- Architecture ratchets: passed.
- Schema drift check against local DB: passed.

End-to-end harness:
- Run id: `projection-delta-10batch-drain-20260630`.
- Tenant: `019f185a-c1dc-7000-943c-0473cd33207f`.
- Report directory:
  `tests/real_llm/reports/runs/projection-delta-10batch-drain-20260630`.
- All 10 required T1 batches completed successfully.
- Think runs: 31 successful, 0 failed.
- Post-commit actions: 93 processed, 0 dead-lettered, 0 pending.
- Projection queue DB check: 848 processed jobs, 0 open or failed jobs,
  108 snapshots, 1888 dependency rows.
- Harness status remained failed because 3 triggers were still pending and the
  efficiency score was below the required floor.

Follow-up harness/fix evidence:
- Drain selector was widened so root T2/T3/T4 downstream work is eligible for
  bounded/adaptive drain, not only the earlier narrow T2/T3/T4 subset.
- Efficiency accounting now separates product-path triggers from background T4
  maintenance; T4 trigger-family names such as `T4:open_question_search` are
  classified as background maintenance.
- Fresh run id: `projection-delta-10batch-familyfix-20260630`.
- Tenant: `019f1893-be59-7000-b487-64b1327eccfd`.
- All 10 required T1 batches completed successfully.
- Product-path efficiency score was above the required floor: 0.6048.
- Projection dispatch stayed in `mode="delta_queue"` with zero route errors and
  zero failed projection jobs.
- The run still failed health because one `T4:open_question_search` trigger
  remained pending after repeated `EdgeRegistryError` failures from a lifecycle
  support-edge sync (`supports` conflicting with an existing `weakens` edge).

Targeted replay after support-sync hardening:
- Trigger replayed: `019f189f-b1ed-7000-99b5-0876fef9f585`.
- Replay run: `019f18b2-34d9-7000-81b7-89a910a1892c`.
- Result: success; retrieval, validation, apply, post-commit enqueue, and commit
  completed.
- Post-commit drain for the tenant processed 3 actions, failed 0, dead-lettered
  0.
- Projection queue after drain: 779 processed jobs, 0 open jobs, 109 snapshots,
  1266 dependency rows, 0 pending post-commit actions, 0 pending root triggers.
- No additional full 10-batch run was started after this replay; the existing
  tenant still contains historical failed `think_runs`, so it is not an honest
  replacement for a fresh pass/fail harness result.

Canary evidence before the final full run:
- Run id: `projection-delta-1batch-canary-20260630`.
- Tenant: `019f18b7-163a-7000-a3c0-c5d0463cbf60`.
- Status: passed; required failures empty.
- Run health: 0 pending triggers, 0 failed Think runs, 0 pending/failed/dead
  post-commit actions, 2 successful Think runs.
- Projection evidence: 23 processed refresh jobs, 0 open jobs, 23 snapshots,
  129 dependency rows.
- Run id: `projection-delta-3batch-canary-20260630`.
- Tenant: `019f18ba-b650-7000-9d1b-52f3e1a114b7`.
- Status: passed; required failures empty.
- Run health: 0 pending triggers, 0 failed Think runs, 0 pending/failed/dead
  post-commit actions, 9 successful Think runs.
- Projection evidence: 135 processed refresh jobs, 0 open jobs, 53 snapshots,
  477 dependency rows.
- The 3-batch canary exercised `T4:open_question_search`; a lifecycle
  validation op was dropped safely and the worker run still succeeded.

Final 10-batch harness:
- Run id: `projection-delta-10batch-final-20260630`.
- Tenant: `019f18c2-0230-7000-b049-6d066067b0ac`.
- Report directory:
  `tests/real_llm/reports/runs/projection-delta-10batch-final-20260630`.
- Status: passed.
- Required failures: none.
- Overall score: 0.7933.
- Average storyline score: 0.6334.
- Product-path efficiency score: 0.5964 with required floor 0.5.
- Run health: 35 successful Think runs, 0 failed Think runs, 0 pending
  triggers, 0 pending post-commit actions, 0 failed post-commit actions, 0
  dead-lettered post-commit actions.
- Adaptive drain: cycle 1 processed 83 post-commit actions and left 21
  downstream triggers; cycle 2 processed those triggers and 22 more post-commit
  actions, ending at 0 pending triggers and 0 pending post-commit actions.
- Downstream profile ended with 0 pending for `T1:event_arrival`,
  `T1:event_batch`, `T2:belief_updated`, `T3:missing_transition`,
  `T4:latent_relationship_candidate`, `T4:open_question_search`, and
  `T4:representation_repair`.
- Post-commit action profile: `materialize_projections` processed 20 actions,
  dead-lettered 0; `search_open_questions` processed 3 actions, dead-lettered
  0; total post-commit processed 105, failed 0, dead-lettered 0.
- Projection materialization source profile included T1, T3, and
  `T4:open_question_search` sources, proving the delta queue was exercised by
  background maintenance as well as product-path model updates.
- Live logs showed projection dispatch in `mode="delta_queue"` with
  `failed_jobs=0`, `route_errors=0`, and empty `projection_errors` during
  materialization.
- Direct tenant-scoped DB aggregate verification after the final run was not
  repeated because the Codex sandbox rejected local Postgres escalation. The
  persisted harness report and run summary are the authoritative evidence for
  final pass/fail state in this doc.

## Remaining Work

The projection delta revamp is clean at component level, canary level, and final
10-batch harness level. Remaining work is no longer about proving the delta
queue works; it is about quality and operating efficiency:

- Keep monitoring retrieval bounded lookup timeout warnings under benchmark
  load; they did not implicate the projection queue but remain a useful
  efficiency signal.
- Improve noise/no-op handling and alias review debt. The final scorecard
  passed health gates, but `noise_noop_score` remained 0.0 and review debt is
  still a product-quality drag.
- Consider adding a lightweight projection-health summary to the harness report
  so future validation can read processed/open/failed projection-refresh counts
  without a separate direct DB probe.

The next focused fix should be drain governance and retrieval efficiency, not
the projection delta queue.

## Post-Final Product-Quality Revamp

The final projection-delta run proved operational health, but it did not prove
enough product intelligence. The next optimizer pass therefore targets the
measured product-value gaps without changing the model/projection boundary.

Source run:
- Run id: `projection-delta-10batch-final-20260630`.
- Product-value overall: 0.664.
- Average storyline score: 0.6334.
- Company intelligence overall: 0.7933.

### Noise And Negative Learning

Measured weakness:
- `noise_noop_score` was 0.0.
- `negative_memory_count` and `negative_memory_inserts` were 0.
- The noise wave still used main Think LLM time and produced a dropped self-edge
  validation artifact.

Fix:
- Add a pre-retrieval noise-only T1 fast path.
- Build an empty accepted RawDiff without LLM use for confirmed non-actionable
  noise.
- Record durable `negative_memory` for the skipped noisy path after validating
  that the triggering observations are still non-actionable noise.
- Surface `negative_memory_inserts` and `negative_memory_ops` in Think
  `ops_applied`.

Success signal:
- Noise-only T1 waves should use zero main Think LLM latency, skip adaptive
  retrieval, emit no durable positive model writes, and record one durable
  negative-memory insert.
- Product-value `negative_learning` should rise through real Think write events,
  not topology-only accounting.

Current proof:
- Focused fake-backed tests cover the LLM no-op path, negative-memory write
  summary, and scorecard aggregation from direct Think ops.
- Full DB-backed noise fast-path integration has not been rerun in this
  continuation because local Postgres access was unavailable in the Codex
  sandbox.

### Question Policy Learning

Measured weakness:
- `question_policy_probe_count` was 7, but `question_policy_events`,
  `question_policy_stats`, and `question_policy_updates` were all 0.

Fix:
- Upsert `sage_question_policy_stats` directly when an accepted
  question-policy probe survives validation, even when SAGE trace emission is
  disabled or unavailable.
- Surface direct Think `question_policy_updates` in `ops_applied`.
- Count Think-origin question-policy updates separately from topology-origin
  updates in the benchmark report.

Success signal:
- Accepted question-policy probe models should produce at least one durable
  stats/update row.
- Product-value `question_policy` should rise because the system learned a
  reusable ask/don't-ask policy, not because a probe merely existed.

Current proof:
- Focused fake-backed tests verify direct update reporting and benchmark
  aggregation.
- Existing DB-backed applier tests cover policy stats, but they were not rerun
  in this continuation.

### Latent Bridge Structure

Measured weakness:
- `latent_bridge_inference` scored 0.678.
- `transition_support_score` was 0.0 despite bridge models and later
  confirmation.

Fix:
- Add structured `transition_support` to inferred bridge propositions with
  `before_state_event_ids`, `after_state_event_ids`, and `gap_review_event_ids`.
- Teach the benchmark scorer to read structured transition support instead of
  relying only on raw supporting event ids.

Success signal:
- Bridge models should be counted as transition-supported only when they bind
  before state plus after or gap evidence.
- The scorer should still penalize fabricated specifics and unsupported bridge
  claims.

Current proof:
- Unit tests cover structured bridge support generation and scorer recognition.
- Old artifacts cannot retroactively show this runtime improvement.

### Decision Impact

Measured weakness:
- `decision_impact` scored 0.7222.
- Recommendation coverage was 0.6667 and there was only 1 act op across 9
  storylines.

Fix:
- Inject a scoped decision-pressure recommendation when an accepted high-pressure
  situation or concern has source evidence and the diff has no existing
  recommendation.
- Keep the recommendation inert: it is a durable model signal, not an automatic
  Act mutation.

Success signal:
- More storylines should have recommendation models without increasing unsafe
  act transitions.
- Act ops remain conservative unless the trigger proves a real commitment or
  decision state transition.

Current proof:
- Unit tests cover recommendation insertion, duplicate suppression, noise
  suppression, and missing-source suppression.

### Counterfactual And Alias Deferral Scoring

Measured weakness:
- `counterfactual_trap` scored 0.3819.
- The alias story deferred most ambiguous relationship candidates, but the
  scorer counted only `needs_review` rows as deferral and under-credited plain
  `candidate` rows.

Fix:
- Count deferred alias candidates as `review_candidate_count -
  accepted_candidate_count`.
- Report `alias_deferred_candidate_count` and strong acceptance pressure
  separately.

Success signal:
- Alias deferral should credit both `candidate` and `needs_review` as not yet
  accepted, while separately penalizing strong accepted pollution pressure.

Current proof:
- Artifact-only rerender:
  `projection-delta-10batch-final-20260630-rerender-current`.
- Product-value overall moved from 0.664 to 0.6817 without Postgres or LLM use.
- `counterfactual_trap` moved from 0.3819 to 0.5406.
- Alias deferral is now 186/194 = 0.9588, with 8/194 strong accepted pressure.

### Compression Context-Use Accounting

Measured weakness:
- `compression_loss` scored 0.8274.
- `model_or_graph_context_use_score` was only 0.5714 even when claim payloads
  embedded model references inside propositions and entries.

Fix:
- Count explicit model references embedded in claim entries and propositions,
  including member, evidence, and source model id fields.
- Count graph claim references even when the model id appears inside structured
  payload fields rather than only on the top-level op.

Success signal:
- Later reasoning should get credit when compressed model/graph context is
  actually used through structured references.
- Unused selected context should still be reported when selected graph memory is
  not referenced by the diff or rationale.

Current proof:
- Focused context-use tests cover situation member model references, evidence
  model ids, unused selected models, and relation-frame graph work.

## Current Verification Boundary

Verified in this continuation:
- Focused runtime and benchmark tests: 148 passed.
- Touched-file ruff check: passed.
- Touched runtime `py_compile`: passed.
- `git diff --check`: passed.
- Architecture ratchets: passed.
- Production environment contract: passed.
- Broad critical ruff selectors: passed.
- Artifact-only rerender of the final 10-batch report: passed and produced a
  sibling report with no LLM or Postgres use.

Not yet verified:
- A fresh DB-backed full integration run of the noise fast path and
  negative-memory write.
- A fresh DB-backed full integration run of direct question-policy stats.
- A fresh 10-batch product-value run proving that runtime fixes improve the
  actual final harness, not only unit tests and artifact-only rerender scoring.
- The broad tech-debt budget still fails on repo-wide existing debt; this pass
  did not attempt a repository-wide debt cleanup.
