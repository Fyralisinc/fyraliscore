# Authoritative Large Company Simulation Gate

This gate evaluates the one authoritative cold-start company simulation. It
does not launch the benchmark or make a smaller run stand in for the finish-line
run.

## Required Run Contract

The accepted artifact set must prove all of the following:

- one fresh, non-append run;
- exactly 45 successful T1 batches;
- exactly 25 signal members and 25 observations in every T1 batch;
- exactly 1,125 processed signals;
- `mode=run`, `target_t1_batches=45`, and `seed_models=0`;
- batching is configured and every observed T1 run is genuinely batched;
- Models, model edges, pattern candidates, and hypotheses are all zero
  immediately before the first signal wave;
- tenant, source, actor, and company scaffolding is reported separately from
  semantic memory;
- Company Vitals and company-learning Assurance v7 are attached;
- trigger and post-commit queues drain with no non-compensatory safety failure.

Any failed run-contract check makes the run `not_credible`, even when its
continuous quality score is high.

Think failure accounting is fate-aware. Append-only failed-attempt history is
retained as reliability, latency and cost degradation, but a required T1 batch
that later succeeds is not terminal workload loss. A terminal failed batch, or
a failed-run count that cannot be reconciled to saved successful retry
receipts, remains non-compensatory.

## Report

The report combines:

- planted hidden-pattern and independent thesis recovery;
- temporal improvement and future-validation memory reuse;
- entity, Model, graph, compression, and metabolism quality;
- adaptive-versus-frozen learning lift, correction, retention, and negative
  controls;
- operational drain and terminal errors;
- evidence coverage and explicit proof gaps;
- wave-by-wave Model-versus-Observation retrieval evolution.

Retrieval evolution reports early, middle, and late phases, the linear change in
Model share, and any late return to raw Observations. If the saved artifacts do
not say whether selected references were actually used, or why late raw evidence
was reopened, the report preserves that as a proof gap rather than inferring an
answer.

## Run Against Saved Artifacts

```bash
.venv/bin/python scripts/evaluate_large_company_simulation.py \
  --report-dir tests/real_llm/reports/runs/<run-id> \
  --assurance /path/to/company_learning_assurance_summary.json \
  --fail-on-not-credible
```

Outputs:

- `large_simulation_gate/large_company_simulation_evaluation.json`
- `large_simulation_gate/large_company_simulation_evaluation.md`

The Markdown artifact is the operator-facing deep postmortem. The JSON artifact
contains the same state as structured evidence for future comparisons.
