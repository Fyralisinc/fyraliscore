# ADR 0004: Keep Model Predictions and Outcome Evaluator

Date: 2026-06-12

## Status

Accepted

## Context

Older planning notes suggested deleting `model_predictions` and
`services/reasoning/sage/outcome_evaluator.py` as redundant capability
surfaces. The learning-loop implementation made both components load-bearing:

- prediction-kind Models materialize internal `model_predictions` rows for
  residual detection, lifecycle metrics, and deterministic deadline resolution;
- the SAGE topology optimizer now evaluates inquiry outcomes before optimizing
  topology, so question-policy learning depends on `OutcomeEvaluator`.

## Decision

Keep `model_predictions` and `OutcomeEvaluator`.

Prediction-kind Models are the source of truth. `model_predictions` is the
internal projection used by residual matching and lifecycle accounting, not an
independent creation surface.

`OutcomeEvaluator` remains the production evaluator for inquiry outcome events.
The optimizer worker owns the production call path: evaluate first, then
optimize topology from the emitted events.

## Consequences

- Do not follow older capability-plan guidance that deletes these components.
- Future simplification should fold legacy prediction creation paths into
  prediction-kind Models instead of dropping `model_predictions`.
- Outcome-event semantics should be simplified by reducing callers, not by
  removing the evaluator.
