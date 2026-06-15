# Learning Loops: Deletion, Merge, and Simplification Plan

**Date:** 2026-06-12
**Source:** Follow-up analysis to `docs/reference/LEARNING-LOOPS-PLAN.md`,
grounded against the current working tree. This plan asks a narrower question:
if the learning-loop architecture lands, what existing components become
redundant, mergeable, or simpler?

---

## 1. Executive conclusion

The learning-loop plan should **reduce** the component count, not add another
parallel runtime. The safest interpretation is:

1. Use the existing `MaintenanceScheduler` as the Housekeeper engine. Do not add
   a second scheduler abstraction.
2. Treat prediction-kind Models as the prediction source of truth. Fold
   `new_predictions` and product Forecasts behind projections instead of keeping
   three independent prediction paths.
3. Treat `model_reeval_queue` as a migration bridge. Once durable obligations
   exist, move its semantics into obligations and delete the extra queue and
   ThinkWorker promotion loop.
4. Do not create a broad new op-outcome event log. The existing audit surfaces
   plus a compact aggregate stats table are enough for the policy digest.
5. Merge all low-frequency lifecycle jobs into one Housekeeper process, but keep
   their pure job functions small and separately testable.
6. Delete verified residue before adding lifecycle code: pycache-only legacy
   service directories, stale docs around already-dropped queues, and no-op /
   duplicate prediction side effects after projection is in place.

This keeps the learning-loop work from becoming "scheduler + queue + ledger +
projection, again."

---

## 2. Component disposition matrix

| Component | Current role | Decision | Why |
|---|---|---|---|
| `services/workers/maintenance/scheduler.py` | Generic advisory-locked scheduler | **Keep and rename-by-wrapper** | This is already the Housekeeper engine. New code should register jobs against it instead of introducing another scheduler. |
| `services/workers/deadline_resolver/worker.py` | Due prediction poller with `run_once()` | **Merge into Housekeeper** | Keep the resolver and evaluator logic; remove the idea of a standalone deployed worker. |
| `services/workers/maintenance/{daily,weekly,monthly}.py` | Decay, archival, relationship maintenance, calibration wrapper, partitions | **Merge into Housekeeper jobs** | These are job bodies, not independently meaningful services. |
| `services/workers/calibration_updater/worker.py` | Calibration-offset writer | **Merge into Housekeeper** | Must run, but as a scheduled job. Keep compute/repo boundaries. |
| `services/workers/edge_drift/worker.py` | Dual-write parity sampler | **Temporary Housekeeper job, then delete** | Needed until legacy arrays are dropped. Delete after 14 clean days and array-read migration. |
| `services/workers/topology_sweeper/worker.py` | Relationship candidate generator | **Merge into Housekeeper** | Low-frequency bounded sweep. The standalone launcher becomes unnecessary once Housekeeper can run it. |
| `services/workers/relationship_ontology_proposals/worker.py` | Aggregates ontology-gap edge-type candidates | **Demote to admin/offline or monthly Housekeeper job** | If strict edge enums are enforced, dynamic edge-kind proposal should not be a hot worker. |
| `services/workers/sage_structural_features/worker.py` | Recomputes SAGE structural features | **Merge or keep separate by cost** | It is pure `run_once()` and safe to schedule centrally. Keep separate only if runtime is too high. |
| `services/workers/precipitation/worker.py` | Pattern-candidate clustering | **Merge into Housekeeper, gated** | It is nightly/bounded and already exposes `run_once()`. |
| `services/workers/anomaly_processor/worker.py` | Poll loop and T3 anomaly enqueue | **Keep separate for now** | It has its own high-frequency poll loop, debounce, and tenant token bucket. Add `run_once()` later if Housekeeper can own it safely. |
| `services/workers/entity_resolver/worker.py` | LISTEN/POLL alias resolution with LLM budget | **Keep separate for now** | LISTEN mode and LLM budget make it unlike maintenance. Add a poll-mode adapter later, not in the first consolidation. |
| `services/workers/sage_topology_optimizer/worker.py` | Optimizes SAGE topology from inquiry outcome events | **Keep worker, merge evaluator call into it** | The worker should become `evaluate_then_optimize`, not remain a consumer of only pre-existing outcome events. |
| `services/reasoning/sage/outcome_evaluator.py` | Emits inquiry credit/outcome events | **Keep; do not delete** | C10 makes it load-bearing. Simplify by having one production caller. |
| `inquiry_outcome_events` | Inquiry-bound outcome event log | **Keep** | Already has the event vocabulary needed for question-policy closure. |
| Proposed C9 `op_outcomes.py` append ledger | New op-outcome event stream | **Do not build as proposed** | Use direct aggregate upserts into `think_op_outcome_stats`; `think_runs`, dropped-op errors, audit events, and lifecycle events already preserve detail. |
| Proposed `think_op_outcome_stats` | Decayed op outcome counters | **Build** | Needed for policy digest, but as an aggregate table, not a second detailed ledger. |
| `recommendation_feedback_stats` | Ranking feedback for recommendation patterns | **Keep and extend** | It is already the correct durable aggregate for acted/dismissed feedback. Add optional lifecycle/event bridges only when origin session is known. |
| `model_predictions` | Internal prediction lifecycle and residual matching | **Keep; make load-bearing** | It is now written by `prediction_lifecycle.py` and should feed residual matching and lifecycle metrics. |
| Product `predictions` / `prediction_signals` | Forecasts UI table | **Keep as projection for now** | Do not let it compete with prediction-kind Models long term. Convert to read projection before considering deletion. |
| `RawDiff.new_predictions` + `schedule_predictions` post-commit action | Legacy forecast scheduling path, currently default no-op | **Freeze, fold, then delete** | Prediction-kind `claim_ops.insert` plus `model_predictions` should be the creation path. |
| `model_reeval_queue` | Reeval fan-out queue promoted by `ThinkWorker` into T4 | **Migrate into obligations, then delete** | New obligations can carry due/dedup/max-fire semantics. This extra queue becomes redundant after C3/C4. |
| `think_trigger_queue` raw SQL writers | Trigger entrypoint scattered across layers | **Simplify behind helper** | Implement `services/domain/triggers.py`; then ban raw inserts outside helper/tests. |
| `relationship_maintenance_log` | Maintenance flags/snapshots | **Consume, then split/fold** | Actionable rows should become obligations; snapshots should become metrics. Avoid a permanent second lifecycle ledger. |
| `relationship_candidates` | Single pre-truth proposal lifecycle for edge, situation, and edge-type-gap candidates | **Keep** | It is the right buffer between inference and accepted edges. Topology is an origin stage for these rows, not a separate candidate lifecycle. |
| Candidate promoter | Promotes safe candidates into edges | **Merge into Housekeeper edge jobs** | Keep promotion logic; remove dormant weekly-only reachability. |
| `model_edges` legacy array dual-write | Keeps typed edges and arrays in sync | **Temporary; retire after drift gate** | Do not drop during learning-loop work. Edge demotion/reclass depends on stable typed edges first. |
| `model_edges.contested_count` | Edge contestation counter | **Simplify or wire** | Either add a writer and use it for reclass/dispute obligations, or drop it from scoring. Do not leave SELECT-only. |
| `_EDGE_KIND_ENUM` vs strict schema regex | Duplicate edge-kind contract | **Use enum, delete regex freedom** | If ontology-gap ops remain, they are the extension path; edge_ops should use registered kinds. |
| `attach_evidence` as implicit fallback only | Evidence attachment path hidden in quality downgrade | **Promote to explicit op** | Once explicit, delete fallback-only wording and narrow anchor hacks that exist because the op is missing. |
| `services/*` pycache-only legacy dirs | Old top-level package residue | **Delete now** | They contain no `.py` files and create import/navigation ambiguity. |
| `signal_routing_decisions` / `topo_dirty_queue` docs | Docs still describe dropped tables | **Update now** | Migration `0127` already drops both. Docs should stop claiming they exist. |
| `model_neighborhoods` / `model_neighborhood_membership` | Legacy map/topology read model | **Defer** | App map routes still read these. Delete only after map/Today move to model_edges/candidates. |
| `topology_events` | Product-facing change/history stream | **Keep** | Today, map, decision-delta, and dynamics readers still consume it. A lifecycle ledger can later replace part of its meaning, but not immediately. |

---

## 3. Simplification principles

### 3.1 One scheduler, many job bodies

Housekeeper should be a launcher and registry over existing `run_once()` job
bodies. It should not own business logic. The target shape:

```text
scripts/run_housekeeper_worker.py
  -> services/workers/housekeeper/worker.py
    -> MaintenanceScheduler(JobDescriptor...)
      -> deadline_resolver.run_once
      -> maintenance.hourly_decay_job
      -> maintenance.archive_decayed_job
      -> maintenance.relationship_maintenance_per_tenant
      -> calibration_updater.run_once
      -> edge_drift.run_once
      -> topology_sweeper.run_once
      -> relationship_ontology_proposals.run_once
      -> precipitation.run_once
      -> obligation_due_sweep.run_once
      -> policy_digest_compaction.run_once
```

Delete only launchers/compose entries after this lands. Keep the job modules.

### 3.2 One source of truth for predictions

Prediction-kind Models should own epistemic prediction state:

```text
models row where claim_role='prediction'
  -> models.evaluate_at / resolution_criteria
  -> model_predictions projection
  -> optional product predictions projection
```

`RawDiff.new_predictions` should stop being a second creation surface. The
post-commit `schedule_predictions` handler is currently a log-only no-op, so the
delete path is straightforward once product Forecasts has a projection from
Model predictions.

### 3.3 One future-work carrier

After `think_obligations` exists, do not keep both:

- `model_reeval_queue`
- ad hoc T2/T4 requeue rows
- obligation due rows

The target is:

```text
event/change happens
  -> open_obligation(kind, object_kind, object_id, due_at/due_condition, max_fires)
  -> Housekeeper obligation_due_sweep
  -> enqueue_trigger(...)
```

Predictions remain the exception because `models.evaluate_at` is already a clock.

### 3.4 Detailed logs only when needed

The plan should avoid adding a broad `op_outcomes` event log. We already have:

- `think_runs.ops_applied`
- `think_run_artifacts`
- `audit_events`
- `reconciliation_events`
- `inquiry_outcome_events`
- `recommendation_feedback_stats`
- planned `object_lifecycle_events`

For policy digest, write aggregate counters directly:

```text
validator/gate/applier outcome site
  -> upsert think_op_outcome_stats
  -> policy_digest.compact()
```

Do not store per-op append events unless a real debugger/user story requires
them after aggregate stats ship.

---

## 4. Implementation plan

### Phase A — Guardrails and stale deletion (1-2 days)

Goal: reduce ambiguity before adding new lifecycle wiring.

1. Delete pycache-only legacy service directories or add a cleanup script that
   removes them from the working tree:
   `services/{gateway,models,think,query,ask,forecasts,greeting,history,...}`
   where `find <dir> -name '*.py'` returns zero.
2. Update docs that still describe `signal_routing_decisions` and
   `topo_dirty_queue` as live after migration `0127`.
3. Add a CI grep or import-linter rule:
   - no raw `INSERT INTO think_trigger_queue` outside the trigger helper after
     Phase C;
   - no references to dropped tables outside historical migrations and schema
     drift exceptions.
4. Add a short ADR rescinding the stale CAPABILITY-PLAN deletion of
   `model_predictions` and `outcome_evaluator`.

Exit criteria:

- `rg "signal_routing_decisions|topo_dirty_queue" services lib scripts docs`
  has only historical migration/schema-drift references.
- No importable top-level legacy service package shadows the current layered
  package.

### Phase B — Tiny loop closures before consolidation (1-2 days)

Goal: prove the core loop closures before changing process topology.

1. `sage_topology_optimizer/worker.py`: call
   `OutcomeEvaluator(pool, tenant_id=tid).evaluate(inquiry_session_id=session_id)`
   before `optimize_topology(...)` inside the claimed-session loop.
2. `prediction_lifecycle.py`: read `falsifier["evaluate_at"]` in
   `_infer_evaluate_at`; fix the `born_from_event_id` datetime parse bug.
3. `deterministic.py`: replace the prediction UUID-substring heuristic with the
   residual matcher.
4. Benchmark drain: add direct calls for these closures before any Phase C
   Housekeeper process exists.

Exit criteria:

- Question-policy stats update in a fresh storyline run.
- Prediction Models with `prediction_deadline.evaluate_at` receive non-null
  `models.evaluate_at`.
- Residual matcher tests cover confirmed, falsified, and inconclusive outcomes.

### Phase C — Trigger helper and Housekeeper shell (3-5 days)

Goal: create one entrypoint for future work and one process for scheduled jobs.

1. Add `services/domain/triggers.py` with:
   - `enqueue_trigger(...)`
   - `enqueue_model_reeval(...)` as a compatibility wrapper
   - shared dedup/payload/scheduled fields
2. Port non-worker raw trigger insert sites first:
   ingestion, recommendations, ask feedback, contestability, cascade, dynamics.
3. Port worker raw trigger insert sites next:
   deadline resolver, anomaly processor, precipitation, entity resolver,
   topology field.
4. Add `services/workers/housekeeper/worker.py` using
   `MaintenanceScheduler` and `JobDescriptor`.
5. Add `scripts/run_housekeeper_worker.py` with health/metrics parity to the
   other worker launchers.
6. Register low-risk jobs:
   - deadline resolver
   - hourly decay
   - archive decayed
   - weekly relationship maintenance
   - calibration updater
   - edge drift
7. Register high-cost jobs behind flags:
   - topology sweeper
   - precipitation
   - relationship ontology proposals
   - structural feature recompute

Exit criteria:

- One Housekeeper `run_once_all()` can be called from benchmark drain.
- Existing individual job tests still pass.
- Raw trigger insert grep fails only for helper/tests/legacy migrations.

### Phase D — Prediction surface fold (3-6 days)

Goal: make prediction-kind Models the only creation path.

1. Deprecate prompt/docs references to `new_predictions`; keep parser field for
   backward compatibility.
2. Validator coercion: convert valid `new_predictions.insert` into equivalent
   `claim_ops.insert` with `claim_role='prediction'`.
3. Product projection:
   - create a read/projection adapter that can render Forecasts from
     prediction-kind Models + `model_predictions`;
   - or materialize product `predictions` from prediction Models in a
     Housekeeper projection job.
4. Remove the no-op `schedule_predictions` post-commit action after projection
   is live.

Exit criteria:

- No production code needs `RawDiff.new_predictions`.
- Forecasts page still renders active/resolved/calibration views.
- Prediction lifecycle scoring reads transitions from `model_predictions` or
  Model lifecycle state, not post-commit forecast rows.

### Phase E — Obligation migration and queue deletion path (5-8 days)

Goal: collapse future re-evaluation semantics into one durable carrier.

1. Add `think_obligations` with open-dedup uniqueness:
   - `object_kind`, `object_id`, `obligation_kind`
   - `due_at`, `due_condition`
   - `max_fires`, `fire_count`
   - `status`, `last_fired_at`, `closed_at`
2. Implement `open_obligation(...)` in `services/domain/triggers.py`.
3. Add Housekeeper `obligation_due_sweep`.
4. Add producers for:
   - `situation_review`
   - `hypothesis_review`
   - `edge_dispute_review`
   - `model_reeval` compatibility rows
5. Update ThinkWorker to consume obligation-generated triggers.
6. Migrate pending `model_reeval_queue` rows to obligations.
7. Stop new writes to `model_reeval_queue`.
8. Delete the ThinkWorker `_promote_reeval_rows()` loop and later drop
   `model_reeval_queue` / dead-letter tables.

Exit criteria:

- No new `model_reeval_queue` rows are written in a full benchmark run.
- Obligation dedup prevents duplicate `model_reeval` triggers.
- Dead-letter behavior is preserved through obligation outcome fields.

### Phase F — Evidence and edge simplification (4-7 days)

Goal: remove hidden fallback paths and duplicate edge contracts.

1. Add explicit `ClaimOp(op='attach_evidence')` and route it to
   `_append_observe_reading`.
2. Once explicit attachment is covered, simplify or delete the
   downgrade-to-evidence fallback branches that only exist because the op was
   missing.
3. Wire `_EDGE_KIND_ENUM` into strict schema and remove the free-form regex for
   `edge_ops.edge_kind`.
4. Add `EdgeOp(op='reclassify')` as retire-and-replace.
5. Decide `model_edges.contested_count`:
   - wire a writer from edge dispute/drop events, or
   - remove it from scoring and do not use it as a reclass trigger.
6. Run edge drift for the dual-write gate. After the gate is green, plan a
   separate migration to drop `supporting_model_ids` / `contributing_models`
   array dependence.

Exit criteria:

- Evidence attachments are scored as first-class transitions.
- Edge kind reclassification no longer fights mutual-exclusion insert rules.
- `supports` dominance metrics separate dual-write edges from Think edges.

### Phase G — Feedback digest without another ledger (3-5 days)

Goal: close feedback learning while avoiding event-log sprawl.

1. Add `think_op_outcome_stats` aggregate table.
2. At validator/gate/applier sites, upsert counters by:
   - `tenant_id`
   - `signal_type`
   - `trigger_kind/subkind`
   - `op_kind`
   - `outcome`
   - `reason_code`
3. Extend `recommendation_feedback_stats` writes to update the same aggregate
   stats when the recommendation pattern has enough metadata.
4. Add `policy_digest.py` that compacts stats into the dynamic prompt profile.
5. Do not add `op_outcomes.py` unless post-launch debugging proves aggregates
   insufficient.

Exit criteria:

- Prompt digest appears only in dynamic prompt content.
- Static prompt cache is unaffected.
- Digest has n>=3 floor and no per-op append table.

### Phase H — Relationship worker consolidation cleanup (2-4 days)

Goal: remove obsolete launchers after Housekeeper has proven stable.

1. Remove or profile-disable standalone launchers for jobs now owned by
   Housekeeper:
   - `run_topology_sweeper.py`
   - `run_sage_structural_features_worker.py` if merged
   - `run_relationship_ontology_proposals_worker.py` if demoted
2. Keep modules and tests; delete only process entrypoints/compose services.
3. Move relationship promotion into a Housekeeper edge job namespace.
4. If strict edge enum reduces ontology-gap volume, make ontology proposals an
   admin command rather than an always-on worker.

Exit criteria:

- Compose/process manifest has one Housekeeper service for scheduled lifecycle
  work.
- Worker package count does not increase as a result of learning loops.

---

## 5. Explicit non-goals

- Do not delete `outcome_evaluator`; wire it.
- Do not delete `model_predictions`; make it the internal prediction lifecycle
  projection.
- Do not drop legacy model arrays during the same tranche as edge reclass.
- Do not replace `topology_events` until product readers have moved.
- Do not merge anomaly/entity resolver loops into Housekeeper in the first pass;
  their cadence and LLM budgets are different.
- Do not create both `object_lifecycle_events` and a broad op-outcome event log
  unless aggregate stats fail a concrete debugging use case.

---

## 6. Recommended first PR stack

1. **PR 1: Stale cleanup and ADR**
   - docs cleanup for dropped queues
   - ADR for `model_predictions`/`outcome_evaluator`
   - pycache-only service directory cleanup

2. **PR 2: Tiny loop closures**
   - `OutcomeEvaluator.evaluate` before topology optimize
   - `_infer_evaluate_at` fixes
   - residual matcher in prediction resolution

3. **PR 3: Trigger helper**
   - add `services/domain/triggers.py`
   - port raw trigger inserts
   - add grep/CI guard

4. **PR 4: Housekeeper shell**
   - reuse `MaintenanceScheduler`
   - register existing safe jobs
   - add benchmark drain hook

5. **PR 5: Prediction surface fold**
   - deprecate `new_predictions`
   - project Models to Forecasts
   - remove no-op schedule path

6. **PR 6: Obligations**
   - add `think_obligations`
   - migrate `model_reeval_queue`
   - delete promotion loop after soak

The highest-leverage simplification is PRs 3-4: once trigger enqueue and
scheduled work have a single owner, most later learning-loop features become
small producers/consumers instead of new subsystems.
