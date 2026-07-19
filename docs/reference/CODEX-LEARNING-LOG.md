# Codex Learning Log

Last reviewed: 2026-07-17.

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

### 2026-07-19 - A clean rewrite must not inherit the old runtime implicitly

- Context: Choosing a clean Fyralis Core implementation after mapping 573,000
  service lines and several mixed architecture generations.
- Symptom: A package-level rewrite inside the existing repository would still
  inherit legacy imports, migrations, tests, database assumptions, and
  compatibility pressure before the first company-learning loop existed.
- Cause: Treating “from scratch” as reorganizing old code instead of creating a
  new authority, schema, dependency, and proof boundary.
- Lesson: Build in a separate repository with a fresh schema and no runtime
  imports from legacy Fyralis. Treat the old repository as a read-only quarry:
  port only small reviewed behavior after the new component contract and tests
  exist. Qualify one synthesis -> correction -> corrected-reuse loop before
  data migration, connectors, task autonomy, or broad product parity.
- Evidence:
  `docs/plans/fyraliscore-clean-reimplementation-handoff-20260719.md`;
  `docs/reference/LATEST-SYSTEM-COMPONENTS.md`.
- Status: Strategy and operating handoff recorded; new repository creation is
  intentionally deferred to the next implementation session.

### 2026-07-19 - End-to-end runs must consume component proof, not create it

- Context: Reconciling the revised Physics–Brain–Intent architecture with the
  TI3 recovery handoff, live packages, tests, and architecture registry.
- Symptom: An expensive provider experiment discovered exact provider-schema
  identity drift and incomplete failure durability. Separately, the registry
  could call a writer implemented without checking whether its named package
  existed on the branch.
- Cause: Broad architecture and end-to-end gates existed before every logical
  component had a checked physical boundary and cheap L0 contract, L1 pure,
  L2 durable, and L3 adjacent-integration proof.
- Lesson: Register logical components, owned paths, shared legacy hotspots,
  tests, writers, contracts, dependencies, and forbidden responsibilities.
  Validate physical paths and implemented writer packages. A higher-cost gate
  must consume green lower-gate evidence; it must not be the first place a
  component contract or terminal-failure path is exercised.
- Evidence: `architecture/registry.yaml`;
  `docs/reference/LATEST-SYSTEM-COMPONENTS.md`;
  `docs/plans/latest-system-component-cleanup-plan.md`;
  `lib/contracts/tests/test_registry.py`.
- Status: Initial separation and registry enforcement landed; per-component
  physical inventories, splits, and proof manifests remain.

### 2026-07-17 - Characterize epistemic bypasses before repairing them

- Context: Starting the company-learning epistemic repair after a contaminated
  45-batch benchmark and later bounded proofs.
- Symptom: A green bounded portfolio could obscure four production-reachable
  benchmark-hook families, five unreconciled telemetry levels, fragmented
  canonical writers, and fifteen representable illegal truth states.
- Cause: Mechanical durability and bounded component quality had stronger
  enforcement/evidence than benchmark blindness, single-writer authority,
  cross-table truth invariants, and whole-run receipt identity.
- Lesson: Freeze direct writer/reader, hook, telemetry, truth-state, and evidence
  inventories before semantic repair. Keep characterization tests green while
  the constitutional gates remain explicitly red; a passing characterization
  test proves inventory coverage, not repaired behavior.
- Evidence: `docs/plans/epistemic-repair/p0/`;
  `tests/epistemic_repair/p0/`.
- Status: P0 characterization landed; P1/P2 production repairs remain open.

### 2026-07-17 - Batch extraction must isolate identical surfaces by focal signal

- Context: A precommitted entity holdout placed the same literal in a typed
  company-entity statement and an explicit metadata negative within one batch.
- Symptom: The model transferred uncertainty between signals, abstaining on an
  exact `system NAME` designation because another signal marked `NAME` as test
  data. It also omitted an explicitly introduced alias after seeing the alias
  used as metadata elsewhere in the batch.
- Cause: Batching is operationally correct, but shared prompt context can leak
  entity status between focal signals unless isolation is explicit. The run
  receipt also bound corpus/report digests but not the runtime-source digest.
- Lesson: Require one call per genuine batch while stating that entity status,
  type and abstention are focal-signal properties. Let exact source role
  designators override model abstention only after explicit metadata rejection.
  Pre-call receipts for semantic holdouts must bind corpus, prompt/runtime source,
  provider configuration and attempt count—not only the eventual report.
- Evidence: `services/domain/entity_grounding/learned_discovery.py`;
  `tests/evaluation/learned_entity_discovery_quality_corpus_v4.py`;
  `services/domain/entity_grounding/tests/test_learned_discovery.py`;
  `scripts/run_learned_entity_discovery_quality_v4.py`.
- Status: Isolation and source-role containment landed after the holdout and have
  unit evidence only. Fresh disjoint generalization evidence remains required.

### 2026-07-17 - Company-learning Postgres harnesses require UTF8

- Context: Running active-surface and combined company-learning assurance in a
  disposable PostgreSQL cluster.
- Symptom: Dedicated active-surface tests passed, but the combined run stopped
  before summary creation on the sealed Unicode collision case with
  `UntranslatableCharacterError`.
- Cause: The disposable cluster used `SQL_ASCII`, so PostgreSQL could not store
  the required Unicode fixture.
- Lesson: Initialize disposable assurance clusters as UTF8 and rerun from a
  fresh cluster. Classify database-encoding failures as environment bootstrap
  failures, separately from evaluator or system assertion failures.
- Evidence: `scripts/run_company_learning_assurance_suite.py`;
  `services/ingest/ingestion/tests/test_company_learning_active_surfaces_db.py`;
  `services/workers/entity_resolver/tests/test_company_learning_assurance_suite_cli.py`.
- Status: Active-surface database tests are green; the combined assurance run
  must be repeated on a fresh UTF8 cluster.

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

### 2026-07-17 - Separate logical LLM calls from physical provider attempts

- Context: Epistemic-repair telemetry hardening for autonomous company learning.
- Symptom: Aggregate LLM counters looked coherent while parse repair, transport
  retry, and SDK-internal retry could create more provider attempts than the
  runtime could identify or reconcile.
- Cause: Logical requests, wrapper attempts, and provider-wire attempts were
  treated as one event and retry ownership was split across layers.
- Lesson: Give every logical call a stable ID and every physical attempt its own
  chained ID and receipt. Persist failures as well as successes, attach tenant
  and Think-run coordinates in the service layer, and permit only one retry
  owner with SDK retry disabled before claiming complete attempt telemetry.
- Evidence: `lib/llm/telemetry.py`;
  `db/migrations/0224_llm_call_attempt_receipts.sql`;
  `services/reasoning/think/llm_receipts.py`.
- Status: P1 contract landed; runtime retry unification and end-to-end proof are
  still required.
