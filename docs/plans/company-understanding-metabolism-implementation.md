# Company Understanding Metabolism Implementation Plan

Status: active implementation plan

This plan turns the model-metabolism roadmap into validation-first work. For
each change, success is defined before implementation. The implementation should
reuse existing Fyralis surfaces first and add new tables or workers only where
current provenance cannot prove the criterion.

## System Shape

The target loop is:

```text
signal
  -> observation
  -> trigger or explicit route decision
  -> retrieval and Think
  -> model/edge/relation/projection/product outcome
  -> residual only when compression failed
  -> coherence repair
  -> outcome-weighted retrieval learning
  -> bounded latent-gap modeling
```

Models remain the primary memory. Raw observations are audit evidence, source
material, counterevidence, or residual debt; they are not the default center of
Think.

## 1. Signal-To-Model Loss Measurement

### Success Criteria

- Every planned or inserted signal has a recorded final fate in
  `signal_metabolism.jsonl`.
- DB-backed runs resolve at least 95% of report observation ids to one of:
  `model_created`, `model_updated`, `evidence_attached`,
  `counterevidence_attached`, `falsifier_created`, `open_question_created`,
  `edge_created`, `relation_frame_created`, `projection_updated`,
  `decision_outcome_recorded`, `self_improvement_event_created`,
  `human_feedback_requested`, `noise_correctly_ignored`,
  `think_noop_justified`, or a named leak fate.
- Valuable signals with `no_think_trigger`, `trigger_pending`,
  `think_failed`, `raw_only_unmodeled`, `validation_dropped`, or
  `think_noop_suspicious` appear in ranked findings with counts.
- The scorecard distinguishes healthy compression from silent loss: a signal
  does not need to become a model, but it must have a correct durable fate.
- Artifact-only mode remains available and clearly reports unresolved trace
  coverage instead of pretending it measured full metabolism.

### Implementation

- Reuse `scripts/company_vitals.py` as the evaluator.
- Reuse existing provenance from `think_trigger_queue`, `think_runs`,
  `models`, `model_signal_readings`, `model_edges`, `relation_claims`,
  `relation_instances`, `model_events`, `projection_snapshots`,
  `omitted_evidence`, and `inquiry_outcome_events`.
- Add a narrow append-only `signal_metabolism_events` table only if DB
  reconstruction cannot reach the trace-coverage criterion.

### Validation

- Unit test each fate classification.
- Run artifact-only vitals against a saved report and confirm unresolved trace
  is explicit.
- Run DB-backed vitals against a fresh benchmark and confirm trace coverage,
  fate counts, and leak counts are populated.

## 2. Missing Trigger And Skipped Metabolism Hardening

### Success Criteria

- Ingest dedupe can never cause a valuable observation to skip metabolism.
- For every non-summary-pending observation, one of these is true:
  - an idempotent T1 `event_arrival` trigger exists,
  - the observation already completed Think,
  - an explicit route/noise decision exists.
- Duplicate ingest of the same external signal returns the existing
  observation and also returns or verifies the existing trigger id.
- Raw production inserts into `think_trigger_queue` trend toward zero outside
  `services/domain/triggers.py`.
- The vitals harness reports orphan observations and skipped metabolism as a
  hard control-plane finding.

### Implementation

- Extend `services/domain/triggers.py` with an idempotent
  `ensure_event_arrival_trigger` helper.
- Use that helper from ingestion and benchmark/probe enqueue paths.
- Keep direct SQL in worker/test leasing paths; the choke point is producer
  creation of future Think work, not worker queue maintenance.
- Later, add a DB unique or advisory-lock-backed idempotency migration once
  all producers use the helper.

### Validation

- Unit test that `ensure_event_arrival_trigger` returns an existing trigger
  without inserting a duplicate.
- Unit test that it inserts a trigger when none exists.
- Ingest integration test: duplicate external id still has an event-arrival
  trigger.
- Architecture ratchet: raw production trigger inserts do not grow.
- Vitals run: `no_think_trigger` count is zero for valuable observations in a
  healthy run.

## 3. Model Metabolism Residual Channel

### Success Criteria

- Raw observations are not broadly reintroduced into Think.
- When model compression misses value, the system writes a compact residual
  with one of: `valuable_unmodeled`, `counterevidence_unattached`,
  `relation_unanchored`, `open_question_needed`, `validation_dropped_value`,
  `authority_blocked`, or `compression_uncertain`.
- Residuals are small, capped, source-backed, and self-retiring.
- A residual becomes `absorbed`, `rejected`, or `expired`; open residuals do not
  accumulate without a coherence or human-loop action.
- Context packets may include a tiny residual spine, but models remain the main
  context body.

### Implementation

- Prefer extending existing residual/prediction and omitted-evidence surfaces
  before adding a new table.
- If needed, add `model_residual_evidence` with `tenant_id`,
  `observation_id`, `think_run_id`, `model_id`, `residual_kind`,
  `compact_summary`, `reason`, `status`, absorption target, and timestamps.
- Add residual creation where successful Think produces no durable fate,
  validation drops useful ops, counterevidence cannot attach, or relation
  evidence cannot bind.
- Add residual absorption when later model/readings/edges/projections cite the
  source observation.

### Validation

- Unit test that a useful validation-dropped op creates a residual.
- Unit test that a later model update absorbs the residual.
- Context-packet test: residual cards are capped and do not cause raw
  observation bloat.
- Vitals test: open residual count and residual absorption rate appear in the
  metabolism and coherence sections.

### Persistent Residual Slice

Success for the first persistent implementation is intentionally narrow:

- A residual row is lifecycle debt, not canonical truth.
- The table can represent open, absorbed, rejected, and expired residuals.
- Inserting the same tenant/source/kind/reason while an open residual exists is
  idempotent.
- Absorption records the absorbing object kind/id and closes the residual.
- Rejection and expiry preserve reason metadata for later audit.
- DB-backed vitals can see persisted residuals and include their ids/statuses in
  signal metabolism, residual, repair, and latent-gap artifacts.
- Prediction residuals in `model_prediction_errors` remain separate; this table
  is for general compression debt that did not fit prediction-error semantics.

### Live Think Residual Slice

Success for the first live writer is:

- Successful Think runs with useful validation/apply drops create compact
  `validation_dropped_value` residuals tied to the source observation,
  think run, and trigger.
- Successful Think runs that produce no durable model, reading, edge, relation,
  open question, projection, act, resource, or justified-noise outcome create
  `compression_uncertain` residuals for their source observations.
- Noise/no-op fast paths and justified no-op runs do not create residuals.
- Residual creation is idempotent for the same tenant/source/kind/reason.
- Residual persistence is best-effort and post-success; a residual write
  failure must never turn a successful Think run into a failed Think run.
- The residual summary is bounded and derived from validation/apply summaries,
  not from reinserting large raw observations into Think.

### Live Residual Absorption Slice

Success for the first live absorber is:

- A later successful Think run that writes a durable model/readings/edge/
  relation/open-question outcome for a residual's source observation closes
  matching open residuals as `absorbed`.
- Absorption records the most specific available object kind/id:
  `model_signal_reading`, `model`, `model_edge`, `relation_claim`,
  `relation_instance`, `model_open_question`, `projection_snapshot`,
  `inquiry_outcome_event`, or `clarification_request`.
- Runs without a durable target do not close residuals just because Think ran.
- Absorption is best-effort and post-success; failure to close residuals must
  never turn a successful Think run into a failed Think run.
- The applied ops summary records absorption count for observability when
  absorption succeeds.

### Compact Residual Spine Slice

Success for the first Think-facing residual spine is:

- Context packets can carry compact residual cards without widening the normal
  evidence reservoir or reintroducing raw observations.
- Only open residual debt is surfaced; absorbed/rejected/expired residuals stay
  out of the writer packet.
- The residual spine is capped by item count and a small token slice.
- Every residual card is marked non-canonical and tells Think to repair, absorb,
  reject, or ask a question only through ordinary model-layer evidence.
- The Think prompt renders the residual spine explicitly so unpaid compression
  debt can influence repair without becoming truth.

## 4. Coherence Repair

### Success Criteria

- Duplicate, contradictory, unsupported, isolated, and unanchored model-layer
  fragments become repair tasks instead of permanent incoherence.
- Repair actions reduce at least one measured coherence debt:
  duplicate pressure, isolated model ratio, unsupported high-confidence models,
  unanchored relation claims, open contradiction residuals, or stale residuals.
- Repair preserves provenance and authority constraints.
- Relationship candidates eventually promote into canonical
  `relation_claims`/`relation_instances` or remain explicitly pending.

### Implementation

- Reuse Think reconciliation, `model_signal_readings`, `model_edges`,
  `relation_claims`, `relation_instances`, model trace, and SAGE topology
  optimizer surfaces.
- Build one coherence repair entry point that can:
  merge/supersede duplicate models, attach orphan evidence, create
  contradiction readings, promote supported relation claims, create open
  questions, retire stale unsupported models, and resolve residuals.
- Keep existing duplicate/relationship scripts as wrappers until parity is
  proven, then merge them behind the repair entry point.

### Validation

- Synthetic duplicate cluster is reduced or explicitly marked for human review.
- Contradictory evidence attaches as contest/falsify instead of creating an
  unrelated parallel fact.
- Orphan relation evidence becomes a relation claim/frame or a residual.
- Vitals graph-coherence report shows debt before and after repair.

### Residual-Driven Repair Scheduling Slice

Success for the first repair scheduler is:

- Open residuals become bounded T4 `representation_repair` triggers in the
  existing repair lane; no new worker or parallel truth lane is introduced.
- Each residual uses a stable `repair_key` based on its residual id, so
  repeated scheduling dedupes instead of producing trigger storms.
- Absorbed, rejected, expired, or id-less residual rows do not schedule repair.
- Repair payloads carry only compact provenance: residual id/kind, source
  observation id, optional model id, residual summary/reason, repair intent,
  and success metric.
- The scheduler can run after successful Think without changing the Think
  result if enqueueing fails.
- Scheduled repair counts are visible in `think_runs.ops_applied` when the
  post-success observability patch succeeds.

## 5. Outcome-Based Retrieval Learning

### Success Criteria

- Retrieval reward is based on downstream outcome, not packet survival alone.
- The retrieval policy can distinguish:
  - evidence selected and used in a valid diff,
  - evidence selected but unused,
  - evidence omitted and later requested,
  - evidence that caused validation failure,
  - routes that repeatedly produce no durable fate.
- Useful retrieval paths improve future route utility; low-value routes create
  negative memory only after repeated downstream failure.
- Packet-survival metrics remain available as diagnostics, not the objective.

### Implementation

- Extend the SAGE outcome path to consume signal fates from the vitals/metabolism
  trace.
- Reuse `inquiry_outcome_events`, `OutcomeEvaluator`, route utilities,
  discovery shortcuts, and negative memory.
- Add reward features such as durable write rate, counterevidence capture rate,
  projection follow-through, residual creation rate, missed-later-requested
  rate, and token per useful fate.

### Validation

- Unit test that evidence leading to a valid model update receives positive
  reward.
- Unit test that selected-unused evidence is weaker than selected-used evidence.
- Unit test that omitted-later-requested evidence penalizes the omission path.
- Future inquiry test: learned route utility changes retrieval order or budget.

### Metabolism Reward Feature Slice

Success for the first outcome-based retrieval learning slice is:

- `OutcomeEvaluator` reward features include downstream metabolism signals:
  durable fate rate, selected context use, selected-unused rate, validation drop
  rate, residual creation rate, omitted-later-requested rate, token per useful
  fate, and an aggregate retrieval outcome reward.
- A selected-used context path with a durable model-layer write receives high
  reward.
- A selected-unused successful no-op receives materially lower reward even if
  the run status is `success`.
- Residual creation and validation/apply drops reduce reward instead of looking
  like successful retrieval.
- The feature set remains additive, preserving existing reward keys for older
  consumers.

### Route Utility Outcome Bridge Slice

Success for the first route-utility bridge is:

- Existing SAGE route utility memory remains the learner; no new retrieval
  policy table is introduced.
- When downstream `retrieval_outcome_reward` is available, route quality credit
  is shaped by that reward before utility compression.
- `outcome_quality_assessed` persists the reward features so optimizers can
  consume downstream metabolism reward after the evaluator returns.
- Selected evidence with negative downstream outcome credit no longer counts as
  a route win merely because it survived into the packet.
- Future retrieval order/skip behavior can change through the already-existing
  positive/negative route utility policy.

## 6. Dark Matter And Latent-Gap Modeling

### Success Criteria

- Latent hypotheses are created only from measured missingness:
  residual clusters, repeated open questions, relation gaps, future validation
  failures, or repeated retrieval misses.
- Every hypothesis has source residual ids, missing-evidence statement,
  falsifier, confidence, and next evidence needed.
- Latent hypotheses are clearly non-canonical until confirmed.
- The system prefers clarification/open questions when evidence is insufficient.

### Implementation

- Reuse `bridge_inference`, `model_open_questions`, clarification requests,
  model predictions, and residual clusters.
- Route synthetic dark-matter outputs through residual/open-question/hypothesis
  pathways instead of creating a parallel truth lane.

### Validation

- Unit test that no latent hypothesis is emitted without supporting residuals.
- Unit test that hypotheses include falsifiers and next-evidence fields.
- Future validation test: a later signal confirms, revises, or rejects the
  hypothesis and updates the residual cluster.

### Persistent Latent-Gap Lifecycle Slice

Success for the first persistent latent-gap implementation is:

- Latent-gap hypotheses live in a SAGE-owned non-canonical table, not in
  `models`, until separately confirmed by normal model-layer evidence.
- Every candidate has at least one source residual id, a gap kind, missing
  evidence statement, falsifier, next evidence needed, bounded confidence, and
  source observation ids.
- Candidate insertion is idempotent by tenant, residual-cluster hash, and gap
  kind while the hypothesis is still active.
- Candidates can be `candidate`, `confirmed`, `rejected`, `expired`, or
  `superseded`, and terminal states record resolution metadata.
- Think may create candidates only from measured open residuals; if no residuals
  exist, no latent hypothesis is emitted.
- Vitals can read persisted candidates and distinguish them from derived
  artifact-only candidates.

## 7. Packaged Company Intelligence Loop

### Success Criteria

- One command can run the benchmark, locate the report directory, run DB-backed
  vitals, and write a ranked fix plan.
- The same command can also run against an existing report directory without
  rerunning benchmark or LLM work.
- Failures distinguish benchmark failure from vitals hard-gate failure.
- The generated fix plan ranks measured leaks before speculative architecture
  work.

### Implementation

- Add `scripts/run_company_intelligence_loop.py`.
- It shells out to `scripts/run_storyline_batch_benchmark.py` and parses the
  printed `report_dir=...` handoff instead of importing benchmark internals.
- It then calls `write_vitals_artifacts(...)` and writes
  `highest_leverage_fixes.md`.

### Validation

- Unit test report-dir extraction from benchmark output.
- Unit test existing-report mode writes vitals plus fix plan.
- Unit test nonzero exit when benchmark fails before vitals.
- Smoke run against a saved report directory.
