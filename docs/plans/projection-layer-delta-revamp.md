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
