# Codex Learning Log

Last reviewed: 2026-07-01.

This file is the durable cross-run memory for Fyralis Core. Use it for lessons
that should compound across Codex sessions: failed benchmark interpretation,
environment traps, recurring debugging patterns, validation boundaries, and
architecture facts that are too operational for an ADR but too important to
rediscover.

This is not a raw run log. Do not paste terminal transcripts, secrets, local
paths outside the repo, provider keys, generated report bodies, or speculative
claims that were not checked against code or artifacts.

## How to Use This File

- Read this file before deep debugging, benchmark analysis, migration work,
  authority/security work, or broad architecture edits.
- Add a dated entry when a run teaches something reusable, especially when the
  first interpretation was wrong or incomplete.
- Link to stable repo artifacts when possible: scripts, docs, migrations, tests,
  or checked-in report summaries. Avoid linking to untracked local run output
  unless the user explicitly asks to preserve that evidence.
- Keep entries short enough to scan. The goal is to change the next agent's
  first move, not to preserve every detail.
- If the lesson implies a code or docs fix, record whether that fix landed or is
  still follow-up work.

## What Deserves an Entry

- A failed or misleading benchmark run, and the exact gate or health signal that
  mattered.
- A test, migration, or runtime failure with a reusable cause and fix.
- A validation command that proved less than it appeared to prove.
- A dirty-worktree or environment constraint that changes attribution.
- A recurring architecture invariant that agents should preserve.
- A decision made during debugging that is not large enough for an ADR.

## Entry Template

```markdown
### YYYY-MM-DD - short title

- Context: What was being changed or measured.
- Symptom: What looked broken, surprising, or easy to misread.
- Cause: The checked explanation.
- Lesson: The reusable rule for future agents.
- Evidence: Stable repo artifact, command, migration, test, or report path.
- Status: Landed fix, open follow-up, or interpretation-only.
```

## Durable Lessons

### 2026-07-16 - Reusable database harnesses must normalize JSONB boundaries

- Context: Extracting the correction-convergence integration test into a
  callable harness used by both the combined assurance CLI and pytest.
- Symptom: The combined CLI passed with its own asyncpg pool, while the same
  harness failed through the shared `fresh_db` fixture because a JSONB field was
  returned as a string rather than a mapping.
- Cause: The harness installed JSON codecs only when it created the pool
  itself; caller-provided pools legitimately had different codec setup.
- Lesson: Reusable database harnesses must normalize JSON/JSONB values at their
  public boundary and must be tested through both self-created and
  caller-provided pools. A passing CLI does not prove the injected-pool path.
- Evidence: `scripts/run_company_learning_correction_harness.py`;
  `services/reasoning/think/tests/test_correction_end_state_integration.py`;
  `services/workers/entity_resolver/tests/test_company_learning_assurance_suite_cli.py`.
- Status: Landed; both real-Postgres paths now pass.

### 2026-07-16 - Freeze experiments at the earliest memory consumer

- Context: Building a paired adaptive-versus-frozen experiment for governed
  entity corrective-memory reuse.
- Symptom: A resolver configured as frozen could still receive no unresolved
  phrase, making the control appear clean while the adjudicated alias had
  already influenced the signal.
- Cause: Ingestion's alias fast-path consumes entity memory before the resolver
  builds context or attempts governed replay. Freezing only the component where
  the learned decision is most visible did not freeze the full treatment path.
- Lesson: For causal learning-loop tests, enumerate every pre-outcome consumer
  of the learned state and disable treatment at the earliest one. Keep stored
  correction state, company foundation, provider behavior and held-out cases
  matched across arms; vary only whether learned memory may be consumed. Also
  prove that the frozen case still reaches the intended evaluation opportunity.
- Evidence: `services/ingest/ingestion/core.py`;
  `services/workers/entity_resolver/worker.py`;
  `services/workers/entity_resolver/tests/test_corrective_memory_control.py`;
  `lib/evaluation/company_learning_experiment.py`;
  `scripts/run_company_learning_pair_harness.py`;
  `docs/plans/revised-system-architecture-discovery-log.md` DISC-019.
- Status: Control boundary, paired evidence contract, real-Postgres execution
  and fail-closed Company Vitals attachment are implemented on the
  autonomous-company-learning branch. Negative controls, larger held-out
  populations and non-exact recurrence families remain follow-up work. The
  scenario and lift metric are registered under INV-05, and canonical evidence
  aggregation preserves the lower E3 runtime tier instead of manufacturing E4
  substantiation.

### 2026-07-16 - Correction and evaluation must preserve their truth boundaries

- Context: Closing the clarification-to-replay company-learning loop and folding
  its proof into Company Vitals.
- Symptom: A clean replay test was contradicted because clarification acceptance
  still rewrote the original Observation and emitted an authoritative
  resolver-authored Observation. Separately, a recorded evaluator cutoff did
  not initially bind all component queries to one database snapshot.
- Cause: Legacy convenience writes blurred source evidence with downstream
  annotation, while independently executed read-committed queries blurred a
  report timestamp with an actual reproducible evaluation state.
- Lesson: Human or model correction must advance immutable annotation and
  canonical-belief generations without rewriting source evidence or re-entering
  perception as authority. Multi-component proof collection must use one
  repeatable-read snapshot, and persisted proof must be schema-, identity-,
  population- and architecture-digest-validated before rerender reuse.
- Evidence: `services/app/gateway/clarifications_router.py`;
  `lib/evaluation/company_learning.py`; `scripts/company_vitals.py`;
  `services/workers/entity_resolver/tests/test_worker.py`;
  `tests/unit/test_company_vitals.py`.
- Status: Landed in the autonomous-company-learning branch; general
  bitemporal as-of reconstruction remains follow-up work.

### 2026-07-01 - Make agent learning explicit

- Context: Future Codex runs need repo-local memory of failed runs and hard-won
  debugging lessons.
- Symptom: Useful lessons existed in prior session memory, but the repo itself
  had no obvious agent-facing place to preserve them.
- Cause: `AGENTS.md` pointed agents at setup, architecture, management, and docs
  conventions, but not at a living learning log.
- Lesson: When a run teaches something reusable, update this file in the same
  change or final cleanup pass. Prefer a short dated lesson over a sprawling
  transcript.
- Evidence: `AGENTS.md`; this file; `mkdocs.yml`.
- Status: Landed as a docs convention.

### 2026-06-29 - Benchmark health and benchmark score are different claims

- Context: Storyline batch benchmark reliability and interpretation work.
- Symptom: A run can produce useful semantic output or acceptable-looking scores
  while still being operationally failed because required Think runs failed,
  pending triggers remained, or explicit benchmark gates failed.
- Cause: Terminal tails and aggregate quality scores hide operational health
  details unless the generated report artifacts are inspected.
- Lesson: For benchmark interpretation, read the generated report directory
  first, especially `benchmark_summary.md`, `run_summary.json`, and related JSON
  summaries. Report system health separately from benchmark pass/fail status.
- Evidence: `scripts/run_storyline_batch_benchmark.py`;
  `tests/unit/test_storyline_batch_benchmark.py`;
  `docs/evaluation/company_intelligence_harness.md`.
- Status: Interpretation rule; keep applying it.

### 2026-06-29 - Do not game benchmark status to hide reliability failures

- Context: Storyline benchmark hardening after required T1 batch failures and a
  weak efficiency result.
- Symptom: It is tempting to loosen score math or exit behavior when a benchmark
  fails loudly.
- Cause: The valuable fix was execution reliability and status gating, not a
  cosmetic score change.
- Lesson: Preserve strict required-run health semantics. If a run should be
  allowed to exit zero while degraded, make that explicit via the benchmark's
  degraded/override option and state the validation boundary.
- Evidence: `scripts/run_storyline_batch_benchmark.py`;
  `tests/unit/test_storyline_batch_benchmark.py`.
- Status: Interpretation rule; verify current CLI behavior before relying on
  exact option names.

### 2026-06-13 - Attribute benchmark deltas to the actual workspace state

- Context: Comparing lifecycle-cleanup benchmark runs against prior baselines.
- Symptom: A benchmark delta can be over-attributed to one change when the
  working tree contains unrelated code and doc edits.
- Cause: The report reflects the combined workspace that produced it, not the
  conceptual patch the agent is focused on.
- Lesson: Before interpreting benchmark deltas, run `git status --short`, inspect
  the run configuration, and state whether the comparison is isolated or
  combined-state.
- Evidence: `scripts/run_storyline_batch_benchmark.py`; generated benchmark
  `run_config.json` when available.
- Status: Interpretation rule; keep applying it.

### 2026-06-03 - Static architecture checks do not prove runtime file paths

- Context: The service re-layering moved packages one level deeper.
- Symptom: Static import checks and collection can pass while runtime path
  construction using `Path(__file__).parents[N]` or hardcoded service path
  fragments breaks.
- Cause: Import resolution and runtime filesystem resolution exercise different
  invariants.
- Lesson: After package moves, search for depth-based path construction and run
  a targeted runtime slice that touches filesystem-loading paths.
- Evidence: `docs/reference/CODEBASE-MANAGEMENT.md`.
- Status: Historical lesson; apply during future restructures.
