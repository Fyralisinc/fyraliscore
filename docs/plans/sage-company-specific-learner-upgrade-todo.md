# SAGE Company-Specific Learner Upgrade TODO

Date: 2026-07-01

This plan captures the SAGE upgrade direction after the design clarification:
Fyralis should learn a company the way a strong operator learns an organization,
but SAGE should not become a symbolic object layer or a second Model store.

The goal is to make SAGE the adaptive company-specific learner adjacent to the
canonical Model layer.

## Reason

The tempting implementation is to add new canonical objects such as
`coordination_pattern` candidates for attention, authority, ritual, commitment,
and local organizational physics. That would bloat SAGE and duplicate what the
Model layer already knows how to represent with `claim_role="pattern"`,
`claim_role="situation"`, and typed edges.

The better split is:

```text
SAGE = latent company-specific adaptive learner
Think = judgment and translation bridge
Models = explicit accepted memory
Projections = rebuildable read views
Authority = guardrail over all derived reads
```

SAGE should mostly learn policy, salience, route utility, source calibration,
question utility, negative memory, structural features, prediction residuals,
and drift. Only when a learned regularity becomes stable, useful, explainable,
and action-shaping should Think promote it into an explicit Pattern Model.

## Current System Anchors

- Pattern Models already exist through the Model memory grammar:
  `claim_role="pattern"`, `abstraction_level="pattern"`,
  `time_mode="recurring"`.
- Think already has a first-class pattern proposition shape and can create
  Pattern Models from repeated behavior when evidence supports it.
- SAGE already has adaptive utility surfaces:
  - `sage_retrieval_route_utilities`
  - `sage_question_policy_stats`
  - affordance profiles
  - discovery shortcuts
  - negative memory
  - inquiry outcome events
  - structural features
- Retrieval motifs are learned procedural memory for retrieval, not canonical
  company truth.
- Precipitation currently clusters active hypothesis/concern Models into
  `pattern_candidates`, but it is a weak statistical signal and is disabled by
  default behind expensive-job flags.

## Non-Goals

- Do not add a `coordination_pattern` canonical Model kind.
- Do not make SAGE a symbolic candidate bureaucracy.
- Do not create a second knowledge graph beside Models and `model_edges`.
- Do not let SAGE directly mutate canonical truth.
- Do not enable expensive SAGE or precipitation jobs by default without proof.
- Do not treat dense embedding clusters as sufficient proof of an
  organizational pattern.

## Design Principle

Most learning should remain latent until it is worth saying out loud.

Latent examples that belong in SAGE:

- This actor's signals often predict actionability.
- This source is high precision for incidents but noisy for roadmap intent.
- This retrieval path is usually wasteful for this trigger family.
- This kind of concern matters only when it touches revenue or customer risk.
- This question primitive has high utility for T4 but low utility for T1.

Explicit examples that belong in Models:

- In this company, customer-risk concerns become actionable only when Sales and
  Support independently reinforce them.
- Planning rituals tend to convert vague capacity concerns into owner-scoped
  commitments.
- Security-review delays recurring near enterprise renewals are an early warning
  for revenue risk.

## Implementation Progress

Updated: 2026-07-01

Completed foundation slices:

- Added non-canonical structural signature extraction in
  `services/reasoning/sage/patterns/signatures.py`.
- Added bounded global scouts, counterexample search, and promotion-readiness
  assessment in `services/reasoning/sage/patterns/`.
- Added a compact tenant-scoped company learning profile digest in
  `services/reasoning/sage/company_profile/`.
- Wired the profile into inquiry bootstrap, question policy, and SAGE retrieval
  action adaptation as optional policy memory.
- Added a bounded live profile loader that reads existing route/question,
  negative-memory, shortcut, affordance, structural-feature, and residual
  surfaces when their tables exist.
- Ensured profile and scout notes explicitly carry `canonical_write=false`.
- Changed `T4:pattern_review` routing so precipitation candidates no longer
  promote through the deterministic/reflex lane.
- Added `pattern_review` prompt instructions and a
  `<pattern_review_candidate>` trigger block so inferential Think reviews the
  candidate with the stable/useful/explainable/falsifiable/action-shaping
  rubric.
- Enriched newly enqueued precipitation review triggers with weak-evidence
  payload fields: proposed signature, observed tendency, constituent Models,
  cluster size, and density.
- Fed accepted/rejected precipitation pattern-review outcomes back into SAGE
  utility memory: accepted reviews upsert discovery shortcuts to the promoted
  Pattern Model; rejected reviews insert expiring negative memory for the
  candidate shape.
- Added an offline/background profile input adapter that turns bounded global
  scout output into company-profile latent-pattern and structural priors without
  running expensive scouting in the inquiry hot path.
- Added latent-pattern drift/decay handling in the profile builder and
  Think-facing Pattern Model repair proposals for decayed or contradicted
  explicit patterns. SAGE proposes repair payloads; it still does not mutate
  canonical Models.
- Added richer precipitation review features beyond embedding density:
  lexical recurrence, shared actors, shared entities, cross-domain support,
  outcome recurrence, candidate-local counterexample count, temporal span, and
  explicit review caution are included in the weak-evidence candidate payload
  for Think review.
- Added bounded cross-cluster counterexample search for precipitation: similar
  non-member Models that share terms, actors, or entities but carry conflicting
  observed outcomes are attached to the weak-evidence review payload for Think.
- Expanded the company learning profile to include recent drift signals plus
  source and actor reliability priors from existing reader-attribution and
  calibration surfaces. These priors are marked as salience-only and
  `authority_effect="none"`.
- Made profile policy notes redact raw evidence references by default, exposing
  evidence counts and aggregate provenance instead of private refs.
- Hardened the profile policy-note boundary so reference-shaped metadata keys
  such as `evidence_refs`, `source_refs`, `observation_ids`, and residual IDs
  are also redacted into counts by default. Internal priors may retain learning
  provenance, but surfaced notes carry `authority_effect="none"` and
  `canonical_write=false` unless a prior explicitly marks its references as
  explanation-safe.
- Fed negative-memory path priors into retrieval policy so known-bad primary
  routes and inquiry actions can be suppressed without overriding required
  routes.
- Added persisted inquiry-note telemetry for company-profile prior outcomes:
  profile-shaped actions now record whether the prior led to skipped work,
  returned evidence, context-packet-selected evidence, and downstream outcome
  reward alignment.
- Added best-effort residual recording for contradicted company-profile priors:
  when prior outcome telemetry contradicts the prior, the system inserts an
  open non-canonical `model_residual_evidence` row if that table exists.
- Updated precipitation status/docs to describe clustering as weak evidence,
  not an automatic Pattern Model factory.
- Added `services/workers/precipitation/quality_gate.py` so precipitation
  enablement requires labeled evidence for precision, recall, false-positive
  rate, semantic-review gating, review-feature coverage, counterexample-search
  coverage, and optional runtime ceilings.

Current precipitation enablement state:

- The implemented quality gate can classify evidence as `weak_evidence_only`,
  `shadow_ready`, or `enablement_candidate`.
- Current smoke evidence is sufficient for shadow/weak-evidence operation only;
  broad enablement remains disallowed unless a representative run passes the
  `enablement_candidate` gate.

## Phase 1: Write The SAGE Boundary Contract

Primary files:

- `docs/architecture/reasoning.md`
- `docs/reference/CODEBASE-ARCHITECTURE.md`
- `docs/reference/CURRENT_SYSTEM_DEEP_DIVE.md`
- `services/reasoning/sage/__init__.py`

Work:

- [x] Document SAGE as the tenant/company-specific adaptive learner, not a
  canonical truth store.
- [x] Document the promotion bridge:
  `SAGE latent signal -> Think judgment -> Pattern Model or Situation Model`.
- [x] Document that retrieval motifs and SAGE route utilities are optimization
  memory, not truth.
- [x] Document precipitation as a weak pattern-proposal source, not the pattern
  learner.
- [x] Add a short glossary entry for "latent SAGE memory" versus "explicit
  Model memory".

Exit criteria:

- A reader can tell where adaptive policy lives, where explicit truth lives, and
  who is allowed to promote one into the other.

## Phase 2: Build A Company Learning Profile Digest

Primary files:

- `services/reasoning/sage/experience.py`
- `services/reasoning/sage/retrieval_policy.py`
- `services/reasoning/sage/outcome_evaluator.py`
- `services/reasoning/sage/topology_optimizer/optimizer.py`
- `services/platform/execution/retrieval_learning.py`

Work:

- [x] Define a compact "company learning profile" shape assembled from existing
  SAGE surfaces, not a new truth table.
- [x] Include route utility, question utility, negative memory, shortcuts,
  affordance profiles, structural features, prediction residuals, and recent
  drift signals.
- [x] Keep the profile cheap to compute or cache as a rebuildable projection.
- [x] Include confidence, decay, and sample counts for every learned prior.
- [x] Make the profile tenant-scoped and authority-safe by carrying source
  provenance wherever any surfaced explanation could reveal private evidence.

Exit criteria:

- SAGE can produce a compact tenant-specific policy digest without creating new
  canonical facts.

## Phase 3: Feed The Profile Into Runtime Behavior

Primary files:

- `services/reasoning/retrieval/primary.py`
- `services/reasoning/sage/reader.py`
- `services/reasoning/sage/retrieval_policy.py`
- `services/platform/execution/inquiry_rounds.py`
- `services/platform/execution/question_policy.py`
- `services/reasoning/think/context_planner.py`

Work:

- [x] Use SAGE profile priors to adjust retrieval route admission and budgets.
- [x] Use source/actor reliability priors to influence salience, not authority.
- [x] Use question utility priors to bias inquiry questions by trigger lane.
- [x] Use negative memory to suppress known-bad routes and repeated low-value
  context.
- [x] Expose profile effects in inquiry/Think notes so benchmark reports can
  show what changed.
- [x] Preserve exploration so SAGE does not get stuck in stale local optima.

Exit criteria:

- Same trigger shape can produce better retrieval/questioning over time because
  SAGE has learned from prior outcomes.

## Phase 4: Define The Latent-To-Explicit Promotion Rule

Primary files:

- `services/reasoning/think/context_planner.py`
- `services/reasoning/think/llm_reason.py`
- `services/reasoning/think/compiled_reasoning.py`
- `services/reasoning/think/validator.py`
- `services/domain/models/propositions.py`

Work:

- [x] Add a promotion rubric for learned regularities:
  stable, useful, explainable, falsifiable, and action-shaping.
- [x] Require evidence and counterevidence summaries before a latent prior can
  become a Pattern Model.
- [x] Promote explicit patterns through normal Think `claim_ops.insert` /
  validation when possible.
- [x] Prefer `claim_role="pattern"` for recurring behavior and
  `claim_role="situation"` for active composite conditions.
- [x] Keep SAGE confidence separate from Model confidence; Think translates
  learned utility into epistemic belief only when justified.

Exit criteria:

- SAGE can influence behavior silently, but explicit organizational laws only
  enter Models through a reviewed Think path.

## Phase 5: Fix Precipitation's Role

Primary files:

- `services/workers/precipitation/clustering.py`
- `services/workers/precipitation/proposer.py`
- `services/reasoning/think/deterministic.py`
- `services/workers/housekeeper/worker.py`
- `docs/status/feature-status.md`

Work:

- [x] Treat precipitation clusters as weak evidence, not proof.
- [x] Stop mechanically promoting `pattern_candidates` without a richer review
  gate, or keep the job disabled until the richer review exists.
- [x] Add first review features beyond embedding density:
  temporal span, lexical recurrence, outcome recurrence, candidate-local
  counterexample count, shared actors/entities, and cross-domain support.
- [x] Add cross-cluster counterexample search before trusting precipitation as
  more than weak evidence.
- [x] Feed accepted/rejected precipitation outcomes back into SAGE so it learns
  when embedding clusters are useful or noisy.
- [x] Keep precipitation behind `HOUSEKEEPER_ENABLE_PRECIPITATION` /
  `HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS` until cost and quality are proven.
- [x] Add a repeatable quality gate before any broad precipitation enablement:
  precision, recall, false-positive rate, review gating, review features,
  counterexample-search coverage, and optional runtime ceiling.

Exit criteria:

- Precipitation becomes one evidence source for pattern discovery, not an
  automatic pattern factory.

## Phase 6: Add Drift And Residual Learning

Primary files:

- `services/reasoning/sage/model_predictions/residual.py`
- `services/reasoning/sage/outcome_evaluator.py`
- `services/reasoning/sage/topology_optimizer/optimizer.py`
- `services/reasoning/think/post_commit.py`

Work:

- [x] Track when SAGE priors predicted useful context, actionability, or
  outcomes.
- [x] Track residuals when expected follow-on effects did not happen.
- [x] Decay stale priors automatically.
- [x] Surface severe drift as a Think candidate for revising or archiving
  explicit Pattern Models.

Exit criteria:

- SAGE learns not only what usually works, but when its learned company profile
  has become stale.

## Phase 7: Validation Plan

Targeted checks:

- [x] Unit tests for company profile digest assembly.
- [x] Unit tests proving SAGE profile effects alter route/question policy while
  preserving defaults when no profile exists.
- [x] DB-backed tests for negative memory and question policy updates.
- [x] Tests that latent priors do not create Models directly.
- [x] Tests that explicit pattern promotion still uses normal Pattern Model
  grammar.
- [x] Tests that precipitation candidates do not auto-promote without the chosen
  review gate.
- [x] Tests that precipitation quality evidence can justify shadow mode while
  still blocking broad enablement until representative evidence passes.

Static checks:

```bash
ruff check --select E9,F63,F7,F82,F821,F811,F401 .
lint-imports
python scripts/check_architecture_ratchets.py
python scripts/check_tech_debt_budget.py
```

Runtime proof:

- [x] Run a small synthetic scenario where repeated signals teach SAGE a route or
  question prior.
- [x] Re-run the same trigger family and verify retrieval/question behavior
  changes without new canonical truth.
- [x] Then provide enough evidence for Think to promote an explicit Pattern
  Model and verify retrieval can use that Pattern Model later.

Runtime proof evidence, 2026-07-01:

- `tests/unit/sage/test_retrieval_policy.py::test_repeated_route_outcomes_teach_later_primary_policy_without_canonical_truth`
  proves repeated route outcomes produce utility memory that changes rerun route
  admission without creating profile effects or canonical truth.
- `services/workers/precipitation/tests/test_precipitation.py::test_candidate_enqueues_t4_trigger_and_think_t4_promotes`
  proves a reviewed T4 pattern candidate can promote through Think into a
  Pattern Model.
- `services/reasoning/retrieval/tests/test_pathways.py::test_pathway_d_returns_patterns_and_instances`
  proves existing Pattern Models are retrieved through Pathway D.
- `services/workers/precipitation/tests/test_quality_gate.py` proves the
  precipitation evidence gate blocks unsafe candidate flow, keeps smoke evidence
  from broad enablement, and can pass a representative shadow evidence set.
- `services/reasoning/sage/outcome_evaluator.py` and `_evaluate` are under their
  explicit tech-debt line budgets after the reward-feature payload change. The
  benchmark script's `run_benchmark`, `_company_intelligence_scorecard`, and
  `_product_value_evals` budgets are also back under their explicit caps, and
  `pathway_a_structural` plus `SynthesisReader.read` are back under their
  explicit caps. Reader-profile integration tests also caught and verified the
  fix for a `discovery_shortcuts` SQL type mismatch in the company-profile
  loader. The repo-wide `scripts/check_tech_debt_budget.py` gate still fails on
  older broad budget debt outside this SAGE/pattern slice.
- Continuation validation on 2026-07-01 reduced every changed explicit-budget
  function touched by the SAGE/Think/ingest learning path back under its cap:
  `reason._run_once` is 139 lines, `context_use.summarize_context_use` is 184,
  `applier.apply_diff` is 120, `validator.validate` is 37, and
  `ingestion.core.ingest_from_draft` is 59.
- The same continuation run passed a broad 436-test behavior suite covering SAGE
  patterns/profile/retrieval/outcome learning, precipitation review and quality
  gates, Think reason/context/apply/validate/residual paths, retrieval pathways,
  storyline benchmark helpers, and ingest event-arrival trigger idempotency.
- Static checks passed for `ruff --select E9,F63,F7,F82,F821,F811,F401 .`,
  `lint-imports`, `scripts/check_architecture_ratchets.py`,
  `scripts/check_production_env_contract.py`, and `git diff --check`.
  `scripts/check_schema_drift.py` was not runtime-verified because this local
  run had no `DATABASE_URL`.
- `tests/unit/sage/test_company_profile.py::test_policy_notes_redact_reference_shaped_metadata_by_default`
  proves company-profile policy notes redact reference-shaped nested metadata,
  preserve aggregate counts, and carry `authority_effect="none"`.
- `tests/unit/sage/test_retrieval_policy.py::test_primary_policy_uses_latent_pattern_prior_without_authority_effect`
  plus the route and negative-memory profile policy tests prove runtime profile
  effects are salience/routing hints with `authority_effect="none"`, not
  authority grants.
- The repo-wide `scripts/check_tech_debt_budget.py` gate still fails, but after
  the changed-function cleanups its remaining explicit line-budget failures are
  older broad debt outside the implemented SAGE/pattern/Think/ingest slice:
  `services/platform/execution/inquiry.py`,
  `services/reasoning/think/reconciler.py`,
  `services/app/gateway/debug_router.py`,
  `services/app/gateway/recommendations_router.py`,
  `services/domain/models/repo.py`,
  `services/ingest/ingestion/writers/observation_writer.py`, and
  `services/reasoning/retrieval/primary.py`.

## Open Questions

- Should the company learning profile be computed on demand from existing SAGE
  tables, or materialized as a rebuildable projection for latency?
- Which SAGE priors are allowed to affect Think prompt construction versus only
  retrieval admission and question selection?
- What is the minimum evidence threshold for promoting a SAGE-learned prior into
  an explicit Pattern Model?
- Authority propagation answer: profile-derived explanations expose aggregate
  provenance and counts by default, mark all policy notes/effects with
  `authority_effect="none"`, and must not expose raw evidence refs unless the
  prior explicitly marks them explanation-safe.
- Should precipitation be retired into SAGE profile learning if richer review
  makes standalone `pattern_candidates` redundant?
