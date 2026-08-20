# Company Metabolism 40-Batch Failed Run Postmortem

Run: `company-metabolism-40b-20260702-101606`
Artifact directory: `tests/real_llm/reports/runs/company-metabolism-40b-20260702-101606`
Tenant observed in artifact: `019f2118-4cb7-7000-bf73-0fd689035f45`

## Evidence Boundary

The saved artifact contains `run_config.json`, `planned_signals.jsonl`,
`storyline_gold.json`, and `waves.json`. It does not contain a final
`run_summary.json`, benchmark scorecard, vitals output, or DB-backed trace.
The configured local database was empty during this postmortem, so the DB rows
from the interrupted run were not available.

Hard evidence below comes from `waves.json` and the current code paths. Terminal
failure evidence comes from the interrupted console snapshot captured during the
run: after manual stop, the run had reached roughly 8 successful T1s, 3 T2s, 7
T3s, 11 T4s, 3 pending T4s, and 89 pending post-commit actions, with repeated
T4 repair attempts around a `self-edge not allowed` dropped operation.

## What Happened

The run was configured for 40 T1 batches of 25 signals each, with 15k seeded
models. It completed and persisted artifacts for 7 waves: 175 signals, or 17.5%
of the intended 1000-signal run.

The first 7 T1 batches did useful work:

- T1 elapsed time: 456.383s total, 65.198s average per batch.
- T1 LLM latency: 207.975s total, 29.711s average per batch.
- Non-LLM T1 overhead: about 248.408s total, or 54.4% of T1 wall time.
- Applied model ids: 47.
- State changes emitted: 92.
- Claim ops: 47.
- Relation claim ops: 26.
- Relation frame ops: 11.
- Edge ops: 44.
- Formation resolutions: 10.
- Open question ops: 1.
- Apply dropped op count in saved T1 waves: 0.

The run then failed operationally, not semantically. T1 was producing value, but
adaptive/control work started to dominate. A T4 representation-repair loop
turned a correctly rejected invalid artifact into more repair fuel. Post-commit
work also accumulated faster than the harness drained it.

## Primary Failure

The deepest structural failure was missing terminal fate for repair waste.

The system had states for residual evidence (`open`, `absorbed`, `rejected`,
`expired`), but the residual-repair success path did not first resolve the
specific parent residual being repaired. A T4 repair that produced an invalid
self-edge could be treated as a new dropped-value residual. That new residual
could schedule another T4 repair, creating metabolic waste as if it were fresh
uncertainty.

The correct invariant is:

> A repair run must consume one parent obligation and assign it a fate before it
> is allowed to create new obligations.

The implemented residual-fate fix addresses this exact loop by resolving the
parent residual as absorbed, rejected, or expired before generic residual
creation, repair scheduling, or latent-gap creation.

## Secondary Findings

### 1. Retrieval Had Low Prompt ROI

Every saved T1 batch selected 16 models and 12 observations. Across 7 waves:

- Selected models: 112.
- Unused selected models: 69.
- Selected observations: 84.
- Unused selected observations: 59.
- Average selected model reference ratio: 0.384.
- Average selected observation reference ratio: 0.298.
- Average selected context reference ratio: 0.347.

Four of seven waves referenced none of the selected observations. The large-batch
raw observation floor is valuable as an evidence anchor, but it is currently
static. It should become outcome-gated: reduce or rerank sources when the last
few batches repeatedly ignore them, and expand only when model compression or
grounding quality drops.

### 2. Retrieval Favored Increasingly Heavy Models

The audit metric `max_selected_model_supporting_events` rose by wave:

- Wave 1: 1
- Wave 2: 26
- Wave 3: 51
- Wave 4: 75
- Wave 5: 100
- Wave 6: 125
- Wave 7: 150

This suggests repeated selection of high-support, high-gravity models. Those can
be useful anchors, but they can also crowd out fresher, more local evidence and
make Think spend prompt budget on obvious context.

Optimization: add an overloaded-anchor penalty or quota. Keep a few high-gravity
models, but reserve slots for recent, local, counterevidence, and low-support
models that can actually change the run.

### 3. Context-Use Telemetry Is Observed But Not Yet Controlling Enough

The system records `unused_selected_model_ids`,
`unused_selected_observation_ids`, and reference ratios. That telemetry should
feed SAGE retrieval policy and/or a deterministic context shrinker:

- If selected observation reference ratio is 0 for repeated T1 batches, lower
  the raw observation floor for the next similar batch.
- If selected model reference ratio is low, apply MMR or diversify by role,
  source pressure, recency, and unresolved uncertainty.
- If a model is repeatedly selected but unused, decay its route utility for that
  trigger lane and company region.

### 4. Post-Commit Work Can Roll Back As A Batch

`process_batch` fetches multiple post-commit actions and dispatches them inside
one transaction. Some handlers are heavy (`materialize_projections`,
`discover_model_edges`, `search_open_questions`). If the harness timeout cancels
the batch, all processed marks in that transaction can roll back, making a slow
action pin a whole batch of otherwise finished actions.

Optimization: claim rows in one short transaction, process each action with its
own timeout and commit boundary, then mark each action independently. Separate
heavy action kinds into dedicated queues or budgets.

### 5. The Post-Commit Pending Index Is Not Tenant-First

The worker and harness query pending post-commit actions by `tenant_id` and
`scheduled_at`, but the hot partial index is only on `scheduled_at`. Existing
debt docs already call this out. Add a tenant-first partial index:

```sql
CREATE INDEX IF NOT EXISTS post_commit_pending_tenant_scheduled_idx
  ON pending_post_commit_actions (tenant_id, scheduled_at)
  WHERE processed_at IS NULL AND dead_lettered_at IS NULL;
```

This is a small optimization with low blast radius.

### 6. Downstream Forensics Are Too Thin

`waves.json` stores T1 run bodies, but downstream steps only store queue counts.
For this failure, that means the precise T4 run bodies were lost when the DB was
cleared. Future expensive runs should persist downstream run ids, trigger ids,
trigger payload summaries, status, elapsed time, validation/drop counts, and
compact `ops_applied` summaries.

This does not make the system smarter, but it makes every failure cheaper.

## Optimization Plan

### P0: Stop The Exact Loop

Status: implemented in the residual-fate slice.

- Resolve the parent residual before generic residual creation.
- Reject terminal invalid self-edge repairs.
- Absorb parent residuals when repair produces a durable model/relation/open
  question outcome.
- Expire parent residuals after a bounded cascade depth.
- Skip generic residual creation, repair scheduling, and latent-gap creation when
  parent residual fate is terminal.

### P1: Add Harness Forensics Before The Next Expensive Run

- Persist downstream run bodies in `waves.json`, not just queue counts.
- Emit periodic DB snapshots during the run: Think counts, pending triggers,
  residuals by kind/status, latent gaps, post-commit pending by action kind.
- Add a stall detector: if T4 count increases while T1 progress is flat and the
  same repair reason repeats, stop early with a structured failure report.
- Store final partial report even on `KeyboardInterrupt`.

### P2: Make Post-Commit Drain Metabolic

- Add the tenant-first pending index.
- Process each post-commit action with independent commit/timeout semantics.
- Split heavy handlers from light handlers:
  - light: broadcast, metrics invalidation, anomaly publish
  - medium: open-question search
  - heavy: projection materialization, edge discovery
- Coalesce projection materialization and edge discovery by wave or model batch
  instead of creating many per-trigger actions.
- In benchmark mode, optionally disable or lower priority for realtime broadcast
  and other product-surface actions that do not affect company-understanding
  metabolism.

### P3: Make Retrieval Learn From Context Waste

- Feed context-use ratios into SAGE route utility.
- Add a short-term deterministic shrinker for T1 batches:
  - lower raw observation floor when repeated selected observations are unused;
  - lower model budget when repeated selected models are unused;
  - re-expand only when absorption, grounding, or representation-audit quality
    falls.
- Enable or test MMR selection for model context in this harness.
- Penalize overloaded high-support anchors after the top few slots so they do
  not crowd out local/fresh/counterevidence context.

### P4: Bound T4 Repair Economics

- Add a per-run T4 budget: max T4 repairs per completed T1 wave and max T4 wall
  time before T1 progress must resume.
- Add a semantic duplicate fuse: if the same tenant/source/reason/op signature
  repeats, mark it terminal or operator-routed instead of asking the LLM again.
- Prefer deterministic repair handlers for validator failures. A self-edge
  validator error should not need another LLM pass.

### P5: Tune Latent Gap Formation

- Raise `min_support` for latent gaps from single residuals in T4 repair lanes.
- Do not create latent gaps from terminal validator artifacts.
- Require residual clusters to survive at least one non-repair source or repeated
  independent source before becoming a SAGE hypothesis.

## Next Validation

Before rerunning 40 batches, run a cheaper staged proof:

1. Artifact-only unit/regression checks for residual fate.
2. A 3-5 batch run with downstream run-body persistence enabled.
3. A 10 batch run with DB-backed vitals and post-commit action profiling.
4. Only then rerun the 40 batch company metabolism harness.

Success for the next run is not just a higher semantic score. The operational
success criteria are:

- no repeated T4 repair loop for the same residual reason;
- pending T4 does not grow without T1 progress;
- post-commit pending returns to zero or a bounded known backlog;
- selected context reference ratio improves or is explicitly justified;
- downstream artifacts are sufficient to diagnose the next failure without the
  original database.
