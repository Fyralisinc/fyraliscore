# Latest-System Component Cleanup And Proof Plan

**Date:** 2026-07-19

**Branch:** `codex/autonomous-company-learning`

**Baseline before this plan:** `eec044a5`
**Status:** active cleanup plan; no provider or end-to-end run authorized

## Objective

Make the latest Physics–Brain–Intent system understandable and changeable by:

1. assigning every live responsibility to a checked component;
2. exposing mixed old/new architecture seams instead of hiding them inside
   broad packages;
3. proving each component rigorously and cheaply before integration; and
4. deleting compatibility and obsolete code only after liveness, migration,
   replay, and rollback evidence permits it.

The human map is
`docs/reference/LATEST-SYSTEM-COMPONENTS.md`. The checked source is
`architecture/registry.yaml`.

## Baseline Debt

The repository is large enough that intuition is not a safe cleanup method:

| Root | Python files | Lines |
| --- | ---: | ---: |
| `services` | 1,849 | 573,189 |
| `lib` | 224 | 83,424 |
| `scripts` | 223 | 80,844 |
| `tests` | 362 | 82,514 |

The current technical-debt ratchet is red:

- 61 files exceed its threshold against a budget of 29;
- 156 functions exceed its threshold against a budget of 16;
- 36 classes exceed its threshold against a budget of 21;
- newly over-budget hotspots include inquiry, Think reconciliation/reasoning,
  primary retrieval, observation writing, model insertion, gateway routers, and
  benchmark orchestration.

The architecture ratchets are green, but they enforce broad layer direction,
not the new P1–P10 semantic ownership. Before this pass, the architecture
registry did not contain components, did not validate component/test paths, and
did not verify that an `implemented` writer's package existed. It also carried
a stale digest for the normative evaluation projection.

## Priority And Risk

| Priority | Debt | Impact | Risk if deferred | Response |
| --- | --- | --- | --- | --- |
| Critical | Semantic ownership is spread across old and new abstractions | Bugs appear only after many components interact | False truth, authority bypass, invalid learning, expensive failed runs | Freeze component registry and proof ladder |
| Critical | Evaluation/provider failures were not fully durable | Missing attempts invalidate experiments | Unreconcilable cost, score, and policy evidence | Component-test all terminal outcomes before TI3 |
| Critical | Frozen and implemented synthesis schema identities differ | No valid production or experiment binding | A green-looking run would violate its frozen contract | Resolve within P3 before provider authorization |
| High | SAGE, outcomes, execution, intent, models/graph, and platform execution mix responsibilities | Refactors can move bugs rather than remove them | Cross-plane writers and private DB coupling persist | Split behind ports one seam at a time |
| High | Registry implementation status could outpace physical code | Planning artifacts appear greener than reality | Cleanup decisions use false premises | Validate paths and downgrade unmaterialized writers |
| High | Component test suites are not uniformly named or gated | Developers reach integration too early | Repeat of E2E-as-debugger workflow | Create per-component manifests and CI lanes |
| Medium | Status/reference documents preserve superseded maps | Humans follow conflicting architecture | Reintroduction of retired patterns | Archive or label after live inventory |
| Medium | Large files/functions obscure owners | Changes have wide blast radius | Slow review and weak negative-path coverage | Split only after behavior/ports are pinned |

## Non-Negotiable Cleanup Rules

1. Do not move code merely to make folders match the target diagram.
2. Do not delete a migration. Retire schema through a new idempotent migration
   after reader/writer and replay analysis.
3. Do not let lower layers import private higher-layer implementations.
4. Cross-plane calls use public ports, commands, results, and events; direct
   cross-plane table writes are cleanup defects.
5. One semantic class has one logical writer during and after cutover.
6. Old and new readers may coexist only through an explicit compatibility
   window and WatermarkVector/removal condition.
7. Evaluation code, gold, simulator facts, and benchmark hooks never influence
   production decisions.
8. A characterization test preserves knowledge of current behavior. It does
   not turn that behavior into the desired contract.
9. Every failure, retry, abstention, no-op, review, deferral, and unknown state
   has a terminal or explicitly retryable fate.
10. No L5 end-to-end run is used to discover an L0–L3 defect.

## Required Artifact Per Component

Before restructuring a component, create one evidence manifest with:

- component and contract versions;
- purpose, semantic plane, writer, aggregate boundary, and public ports;
- current owned, shared, compatibility, derived, evaluation, and candidate-
  retirement paths;
- tables, columns, migrations, readers, writers, queues, processes, routes, and
  environment variables;
- direct and transitive imports across component boundaries;
- failure, idempotency, concurrency, tenant, authority, time, correction, and
  observability behavior;
- exact L0, L1, L2, and L3 test commands and results;
- known blind spots;
- proposed move/delete adapters and rollback condition; and
- evidence digest tied to commit and schema state.

The manifest must be generated or checked where practical. A prose-only list is
not sufficient for a destructive cleanup.

## Proof Policy

### L0 — contract proof

Must pass before implementation or movement:

- exact schema/class/version identity;
- one source for contract digest and policy receipt;
- allowed and forbidden field/property tests;
- writer and authority ownership;
- compatibility and removal law;
- hidden-required-field attack;
- cross-field invariant attack; and
- production/evaluator separation.

### L1 — pure component proof

Must cover happy, abstain/unknown, invalid, adversarial, correction, duplicate,
reorder, and boundary cases without a provider or database when possible.

### L2 — durable component proof

Uses disposable UTF8 PostgreSQL and proves:

- atomic state/event/outbox or complete rollback;
- replay and idempotency;
- lease/concurrency/fencing where relevant;
- tenant and authority isolation;
- exact failure evidence and terminal accounting;
- bitemporal/correction behavior; and
- no evaluator or provider dependence unless the component is E0.

### L3 — adjacent integration proof

Tests one provider/consumer port using frozen fixtures. The manifest names both
owners, versions, authority mode, migration state, failure semantics, and the
acceptance test. A broad multi-component harness is not an L3 test.

### L4/L5 — bounded vertical and end to end

These run only after every participating lower gate is green. L5 additionally
requires preregistration, immutable run identity, all-call reconciliation,
independent scoring, and a written stop rule. Provider failures are outcomes,
not excuses to patch and immediately rerun.

## Execution Waves

### Wave 0 — Freeze The Map And Stop Architectural Drift

Deliverables:

- [x] Add C0, P1–P10, and E0 to the machine-readable registry.
- [x] Record current owned and shared legacy paths.
- [x] Validate component and component-test paths.
- [x] Reject `implemented` writers whose package does not exist.
- [x] Correct the stale normative evaluation-document digest.
- [x] Publish the human-readable component map and proof ladder.
- [x] Add `scripts/check_architecture_registry.py` to the static architecture CI
  job with its minimal parser/model dependencies.
- [ ] Make new production packages declare a component owner.

Exit: architecture registry internally valid; production freeze may remain
false and must remain visibly false while contracts are partial.

### Wave 1 — Complete The Physical Inventory

Run independently for each component:

- [ ] Python import and symbol ownership inventory.
- [ ] Table/column/migration reader-writer inventory.
- [ ] Route, worker, launcher, compose, and process-manifest inventory.
- [ ] Test and fixture reachability inventory.
- [ ] Environment/configuration inventory.
- [ ] Mark each path canonical, compatibility, derived, temporary,
  evaluation-only, planned, or retirement candidate.
- [ ] For every shared legacy path, assign a split owner and sequence.

Exit: no production file or table in the chosen component is unclassified.

### Wave 2 — Isolate C0, P10, P9, And E0

These boundaries make every later cleanup safer.

- [ ] C0: contract versions, compatibility, writer IDs, and cross-plane ports.
- [ ] P10: consolidate live authority decisions and eliminate stale/manual
  bypasses; prove revocation and tenant isolation.
- [ ] P9: separate semantic writers from work/lease/failure/repair mechanics;
  prove crash, replay, owner terminalization, and quiescence.
- [ ] E0: remove production reachability of gold, evaluator defaults, and
  benchmark-only hooks; bind every run to code/schema/policy/population.

Exit: all later component tests can rely on neutral contracts, runtime, access,
and evaluation boundaries.

### Wave 3 — Separate And Prove P1A–P1I

Work in the order in `LATEST-SYSTEM-COMPONENTS.md`. Do not begin with a full
ingest-to-Think replay.

- [ ] Source and conversational revision fidelity.
- [ ] Context selection and sufficiency without circular identity evidence.
- [ ] Source semantics and mention/type/role extraction.
- [ ] Candidate recall and one-set-or-terminal fate.
- [ ] Referent lifecycle and consumer-specific grounding admission.
- [ ] Correct destination-plane admission and independent outcomes.
- [ ] Correction dependency and repair convergence.
- [ ] Complete calibration/fate/cost denominators.

Exit: one signal and one correction can traverse P1 with every intermediate
object and terminal fate observable, before invoking P3.

### Wave 4 — Separate P3 Belief And Relation Truth

- [ ] Resolve the frozen `SynthesisSemanticDecision` identity mismatch without
  a provider call.
- [ ] Separate provider semantic judgment from trusted identity binding,
  compilation, validation, and apply.
- [ ] Identify the canonical belief/relation writer and remove direct legacy
  write bypasses.
- [ ] Separate n-ary plane-owned relation truth from `model_edges`, topology,
  Bridge, retrieval sidecars, and graph projections.
- [ ] Keep optional inferred representations behind utility hypotheses and
  admission decisions; utility never proves truth.
- [ ] Split Think/reconciler/retrieval hotspots only after contracts and
  characterization tests are pinned.

Exit: P3 L0–L3 green on frozen dossiers, adversarial relations, correction,
atomicity, and current-head behavior. This still does not authorize TI3.

### Wave 5 — Separate P2 And P6 From Shared Intent/Outcome/Execution Code

- [ ] Split ProposalAppender from IntentApplier ownership.
- [ ] Split AuthorizationApplier and agency/effect state from observed outcomes.
- [ ] Split independent OutcomeRecorder from prediction/settlement/attribution.
- [ ] Prove exact typed constitutive commands cannot be confused with
  interpreted proposals.
- [ ] Prove proposal, no-action, prediction, specification, authorization,
  effect, outcome, settlement, residual, and attribution identities end to end
  across adjacent ports only.

Exit: a proposed intervention may be investigated, accepted/rejected, executed
or reconciled, observed, and settled without any stage impersonating another.

### Wave 6 — Decompose P4/P5 And The SAGE Hotspot

Assign every `services/reasoning/sage` path to belief support, inquiry, concern,
control learning, derived projection, compatibility, or retirement.

- [ ] P4 owns temporary context/inquiry only.
- [ ] P5 owns Concern and attention lifecycle only.
- [ ] P7 owns policy learning/promotion only.
- [ ] P8 owns rebuildable views only.
- [ ] Remove or adapt SAGE paths that duplicate a canonical owner.

Exit: SAGE is no longer an architectural plane or miscellaneous holding area.

### Wave 7 — Separate P8 Derived Graph And Product

- [ ] Inventory every graph edge/table by canonical versus derived ownership.
- [ ] Route projections only from accepted plane-owned assertions.
- [ ] Prove rebuild, cutoff, correction, deletion, tenant, and access behavior.
- [ ] Keep Ask/rendering epistemically explicit.
- [ ] Route outbound delivery through P6 proposal, P2 authorization/effect, and
  P9 work ports; P8 selects and renders but does not write those ledgers.
- [ ] Retire compatibility graph/model readers only after replay and product
  consumers have crossed their removal watermark.

Exit: deleting every derived projection and rebuilding it does not destroy or
invent canonical company truth.

### Wave 8 — Prove P7 Learning Last

- [ ] Bootstrap policy and frozen fallback.
- [ ] Independent experiment assignment and exposure.
- [ ] Attributable outcome and eligibility checks.
- [ ] Candidate, shadow, promotion, canary, active, freeze, rollback lifecycle.
- [ ] Correction, revocation, deletion, reward retraction, and recomputation.
- [ ] Tenant influence lineage.

Exit: learning can change a bounded control policy but cannot self-corroborate,
cross tenants, mutate semantic truth, or survive invalidated evidence.

### Wave 9 — Retire Compatibility And Contradictory Documentation

For every retirement candidate:

- [ ] zero required runtime readers/writers;
- [ ] process/compose/route import proof;
- [ ] table and migration successor proof;
- [ ] backlog/replay/rollback watermark proof;
- [ ] characterization and replacement tests;
- [ ] new migration for schema retirement if needed;
- [ ] focused validation and rollback note.

Then archive or rewrite status documents that describe superseded UI, demo,
worker, graph, or architecture states. Keep historical evidence labeled as
historical rather than deleting useful learning.

### Wave 10 — Bounded Integration And E2E

Only after component evidence manifests are green:

- [ ] one signal cross-plane acceptance;
- [ ] one correction and repair convergence;
- [ ] one Ask path;
- [ ] one intervention/no-intervention path;
- [ ] one governed learning candidate and rollback;
- [ ] chaos/replay/tenant/quiescence slices;
- [ ] preregistered provider experiments and full E2E.

TI3 and CF3-C retain their separate handoff authorization rules. This cleanup
plan does not unlock them.

## Quick Wins

These are low-risk and high-leverage:

1. Keep the component registry checker in every architecture-changing PR.
2. Fail the checker on missing component/test paths and absent implemented
   writer packages.
3. Add component labels to new test files and evidence manifests.
4. Run component tests before integration in CI order.
5. Move evaluator-only imports out of production packages when found.
6. Replace duplicated schema/policy digest literals with one contract source.
7. Mark stale status docs historical before relying on them for deletion.
8. Reduce technical-debt budgets only after a component split lands; never
   raise them to make a regression green.

## Metrics

Track per commit and per component:

- classified production paths / total production paths;
- owned paths versus unresolved shared legacy paths;
- canonical writer count per semantic class;
- direct cross-component DB writes;
- upward imports and allowlist size;
- implemented registry writers with missing packages;
- L0/L1/L2/L3 pass state and runtime;
- failure-fate and terminal-outcome coverage;
- tables without named writer/reader/removal gate;
- compatibility paths without consumer/removal watermark;
- production imports from E0;
- large files/functions/classes and technical-debt budget delta;
- component defects first discovered at L4/L5; target is zero; and
- expensive run minutes/tokens lost to lower-gate defects; target is zero.

## Validation For This Initial Separation

Required before committing this plan:

```bash
.venv/bin/python scripts/check_architecture_registry.py
.venv/bin/python -m pytest lib/contracts/tests/test_registry.py -q
.venv/bin/python scripts/check_architecture_ratchets.py
.venv/bin/python -m compileall -q lib/architecture_registry.py
git diff --check
```

The technical-debt budget is expected to remain red at this checkpoint. This
pass exposes and organizes the debt; it does not claim to have reduced the
61/156/36 over-threshold counts yet.
