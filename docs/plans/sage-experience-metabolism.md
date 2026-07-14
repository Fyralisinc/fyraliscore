# SAGE Experience Metabolism Plan

## North Star

Fyralis should have two canonical memories:

- Models remember semantic truth about the world.
- SAGE remembers system experience: which retrieval, reasoning, projection, and
  product choices worked, failed, wasted effort, or should change future policy.

The elegant boundary is:

```text
Models = world memory
SAGE = experience memory and adaptive policy
Projections = rebuildable views
Product = reads projections and applies feedback
```

SAGE should not mutate canonical truth directly. It should turn outcomes into
bounded utility policy, negative memory, question policy, structural features,
and canonical candidates that must still pass validation.

## Components

### Experience Events

Input:
- Reader, writer, validation, product, and later-outcome events.

Process:
- Preserve what was attempted, which evidence/path/model was involved, and
  whether the result was useful, low-value, contested, confirmed, or dismissed.

Output:
- Append-only outcome events such as `node_used_in_valid_diff`,
  `reader_decision_low_value`, `outcome_quality_assessed`,
  `recommendation_acted_on`, and `recommendation_ignored`.

Connects to:
- `inquiry_outcome_events`.
- `services.reasoning.sage.outcome_evaluator`.
- Product feedback emitters.

### Experience Metabolism Report

Input:
- Outcome events.
- SAGE policy effects written by the optimizer.
- Canonical topology candidates enqueued for validation.

Process:
- Classify whether the loop is idle, merely sensed, evaluated, or metabolized.
- Count future-behavior levers such as affordance policy, shortcut policy,
  negative memory, question policy, and structural features.
- Emit simple metrics that benchmark harnesses can aggregate.

Output:
- `experience_loop` report plus numeric optimizer metrics.

Connects to:
- `services.reasoning.sage.experience`.
- `services.reasoning.sage.topology_optimizer`.
- Internal SAGE routes and stress harness reports.

### Policy Effects

Input:
- Outcome events and quality signals.

Process:
- Reinforce/decay affordances.
- Create or decay shortcuts.
- Insert negative memory.
- Update question policy.
- Refresh structural features.
- Propose canonical graph candidates without applying them directly.

Output:
- Utility-layer writes and candidate payloads.

Connects to:
- SAGE reader.
- Retrieval policy.
- Think validators/appliers through candidate validation.

### Future Behavior Enforcement

Input:
- Utility-layer policy effects.

Process:
- Reader suppresses known-bad paths.
- Retrieval policy adjusts route budgets/modes.
- Product ranking consumes feedback aggregates.
- Think still validates before canonical mutation.

Output:
- Measurably different future retrieval, reasoning, and product behavior.

Connects to:
- `services.reasoning.sage.reader`.
- `services.reasoning.sage.retrieval_policy`.
- `services.product.recommendations.feedback`.

## Success Gates

Component tests:
- Experience report identifies idle, sensed/evaluated-only, and metabolized
  loops.
- Optimizer metrics include closure score and future-behavior lever counts.
- Internal route and stress reports expose the same `experience_loop`.

Fresh run gates:
- `experience_loop.status` should be `metabolized` for successful SAGE optimizer
  passes with useful outcomes.
- `experience_loop.experience_loop_closed` should aggregate above zero in the
  storyline harness.
- Negative memory and question policy must be proven by DB-backed assertions
  before spending a fresh 10-batch run.

## Current Slice

This pass adds the pure SAGE experience contract and wires optimizer reporting
through it. It deliberately does not add another table because the existing
outcome-event and utility-policy tables already represent the necessary atoms.

The readiness slice now also adds:

- Think-side noise no-op experience events so durable negative memory can close
  a SAGE experience loop even when the optimizer is not in the same trace.
- Direct question-policy stats updates for accepted capability/lifecycle probe
  Models.
- Context-use telemetry that treats selected Model/graph context as used when
  the reasoning trace uses that context to reject a bad edge or duplicate
  mutation.
- Deterministic decision-pressure action creation when a pressure Model has a
  scoped owner, target entity, and sufficient confidence; otherwise it remains
  an inert recommendation for human review.
- A benchmark overhead gate,
  `--max-background-maintenance-llm-calls`, so small canaries and fresh runs can
  fail loudly when T4/background maintenance becomes too expensive.

Validated without DB:

- `tests/unit/test_storyline_batch_benchmark.py`
- `tests/unit/sage/test_experience.py`
- `services/reasoning/think/tests/test_auto_decision_revisit.py`
- Pure subset of `services/reasoning/think/tests/test_context_use.py`
- `ruff check`, `py_compile`, `git diff --check`, and architecture ratchets.

Still requires live proof:

- Targeted Postgres proof:
  `test_think_noise_only_t1_fast_path_skips_retrieval_and_llm`,
  `test_question_policy_probe_feedback_reaches_policy_stats`, and
  `test_noise_noop_negative_memory_emits_sage_experience_event`.
- The 2-batch capability/noise canary emitted by `rerun_readiness`.
- A fresh 10-batch run only after the DB proof and canary pass.
