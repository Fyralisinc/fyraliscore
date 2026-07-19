# Fyralis Core Clean Reimplementation — Implementation Handoff

**Date:** 2026-07-19

**Decision:** Build the latest system as a clean implementation in a separate
repository. Treat the current repository as read-only evidence and a source of
individually reviewed ideas, fixtures, and utilities—not as the foundation of
the new runtime.

**Primary objective:** Deliver the smallest coherent company-learning system
that can form an evidence-grounded belief, revise it when later evidence
changes, retrieve and use the corrected current belief, and prove that behavior
without evaluator leakage or accidental authority.

**Explicitly not the objective:** General task autonomy, a broad agent platform,
automatic workflow execution, a universal ontology, or preservation of every
legacy Fyralis feature.

## 1. Why This Handoff Exists

The previous repository accumulated several architectural generations in one
runtime. The code remained mechanically capable, but responsibilities such as
evidence, grounding, canonical belief, relation truth, inquiry, projections,
control learning, evaluation, and runtime scheduling became entangled.

That produced a damaging operating pattern:

```text
large end-to-end run
  -> component contract failure
  -> narrow patch
  -> new commit invalidates prior exact-commit evidence
  -> large rerun
  -> next component failure
```

Examples included:

- a 12-batch run discovering production SQL/schema drift that disposable
  component PostgreSQL tests should have caught;
- a synthesis run discovering conflicting compiler/validator relation
  authority that contract tests should have caught;
- a provider experiment discovering a frozen schema-name mismatch that exact
  schema-identity tests should have caught;
- concurrent provider attempts completing without every terminal outcome being
  durably recorded;
- a green evaluator missing disagreement between proposition text, canonical
  natural text, and relation mechanism;
- retrieval being counted as learning even when the provider never used the
  retrieved memory; and
- queues draining successfully while the system still formed the wrong company
  model.

The clean implementation must reverse that workflow. End-to-end runs consume
green component evidence; they do not create it.

## 2. Authority And Source Order

Use the following order when deciding what to build:

1. This handoff's scope, operating rules, and proof order.
2. `docs/reference/LATEST-SYSTEM-COMPONENTS.md` for component ownership.
3. `architecture/registry.yaml` for machine-readable writers, contracts,
   dependencies, forbidden responsibilities, and component status.
4. The revised Physics–Brain–Intent architecture:
   `docs/plans/revised-reality-belief-intent-system-implementation.md`.
5. The frozen Think Intelligence contract for the specific lessons about LLM,
   compiler, validator, applier, and evaluator ownership.
6. Learning logs and old artifacts as historical evidence.
7. Current legacy code only as evidence of behavior or a possible source of a
   small reviewed implementation idea.

Legacy code does not redefine the latest architecture. A behavior being live in
the old repository proves that compatibility exists; it does not prove the
behavior belongs in the new system.

## 3. The Product To Build First

The product is an organizational memory substrate that becomes more accurate
and useful over time. The agent is downstream of that substrate.

The first complete loop is:

```text
normalized company signal
  -> immutable source evidence
  -> bounded conversational context
  -> mention and entity grounding
  -> admitted atomic belief
  -> later evidence retrieves the exact prior belief
  -> explicit supports / weakens / contradicts / none judgment
  -> governed belief transition
  -> scope-local synthesis and relation when warranted
  -> correction supersedes the belief and retires or revises its relation
  -> later retrieval and Ask use only the corrected current head
  -> independent report shows every input, decision, mutation, and fate
```

The first milestone is not “processed many signals.” It is:

> synthesis -> correction -> retrieval and use of the corrected current head.

Use one small development company, one structurally different positive case,
and one null/adversarial case. Do not start with dozens of connectors, a
45-batch stream, or an open-ended company simulation.

## 4. What “From Scratch” Means

### 4.1 Create a genuinely separate repository

Use a new sibling repository rather than another package inside the existing
repository. A suggested working name is `fyraliscore-next`; the human may choose
the final name before initialization.

The new repository must have:

- a new Git history and clean initial commit;
- no editable install or import path pointing at the legacy repository;
- no copied `.env`, secrets, caches, reports, databases, or provider artifacts;
- no dependency on legacy service packages;
- a fresh schema baseline and new migration sequence;
- its own architecture registry, process manifest, and test configuration; and
- backend-only boundaries. UI/demo/simulation overlays remain outside the
  production dependency graph.

### 4.2 The old repository is a quarry

Allowed uses:

- inspect a specific algorithm or test fixture;
- extract a tiny dependency-free helper after review;
- port a behavior only after writing the new component contract and tests;
- compare new results to an old immutable artifact;
- use old failures as adversarial test cases; and
- consult migrations to understand previously discovered database invariants.

Forbidden uses:

- copying an entire service directory;
- preserving an API or table merely because it already exists;
- importing legacy packages at runtime or in tests;
- replaying the full legacy migration history;
- copying SAGE, Bridge, Models/model_edges, Think, retrieval, or worker code as
  a unit;
- treating old test pass rates as evidence for the new implementation; or
- maintaining dual writes to the old database during the first milestone.

### 4.3 No migration project before a working core

Build and prove the clean core with synthetic/new data first. Data migration
from the old system is a later, separately designed import project. A migration
must consume public export contracts and create new canonical objects through
normal admission ports; it must not bypass writers with table-to-table copies.

## 5. Constitutional Architecture

### 5.1 Core planes

The runtime has three semantic planes and several constraining/support planes:

| Plane | Owns | Must never own |
| --- | --- | --- |
| Evidence and grounding | Source records, revisions, interpretation context, mentions, candidate sets, resolution assessments | Belief truth, company intent, evaluator gold |
| Physical/institutional reality | Admitted operational state and independently observed outcomes | Predictions, explanations, goals inferred from behavior |
| Brain/epistemic | Revisable beliefs, knowledge gaps, explanatory relations, confidence and lifecycle | Raw evidence mutation, intent, authorization, external effects |
| Intent/agency | Goals, priorities, decisions, commitments, grants, proposals, authorization, workflows, tasks, effects | Facts inferred from relevance, outcomes inferred from execution |
| Inquiry | Temporary context, questions, evidence packets, stop rules | A second persistent knowledge graph |
| Control learning | Governed policy candidates, assignments, promotion, rollback | Semantic truth or reward from self-authored outcomes |
| Derived/product | Rebuildable graph, indexes, Ask, rendering, briefs | Canonical propositions or direct effect writes |
| Runtime | Transactions, outbox, work, leases, failures, repair, budgets, quiescence | Any component's semantic decision |
| Authority/audit | Access decisions, revocation, neutral traces and fate facts | Competing domain writes or evaluator verdicts |
| Independent evaluation | Fixtures, gold, metrics, experiments, reports | Production imports, canonical state, policy inputs during the evaluated run |

### 5.2 Non-negotiable invariants

1. Evidence, belief, intent, execution, outcome, and evaluation remain distinct.
2. A semantic class has one logical writer.
3. Derived views and learned policy are never source evidence.
4. Intent influences relevance and direction; it does not establish factual
   truth.
5. A recommendation is a proposal until a separate authority accepts it.
6. A prediction belongs to the Brain; an outcome belongs to observed reality.
7. Execution is not outcome, residual is not causal proof, and repetition is
   not independent corroboration.
8. Candidate, assessment, admission, application, correction, and projection
   are separate stages.
9. Corrections append and supersede. They do not rewrite source evidence.
10. Canonical relations are plane-owned, typed, versioned, and usually N-ary.
    Binary graph edges are projections.
11. Temporary inquiry state cannot become a shadow truth store.
12. Every eligible input or work item has one terminal fate or an explicitly
    retryable fate with a wake condition.
13. Tenant, source, authority, temporal cutoff, and correction constraints are
    mechanical, not prompt conventions.
14. Learning requires an independently observed, attributable outcome or a
    controlled experiment.
15. The runtime returns to quiescence over a complete obligation denominator.

## 6. Recommended Repository Layout

Do not encode old team names into every package. Use semantic ownership in the
physical layout and retain C0/P1–P10 identifiers in the architecture registry.

```text
src/fyralis/
  contracts/                 # C0 pure schemas, IDs, versions, ports
  evidence/                  # P1 raw/normalized evidence and revisions
  conversation/              # P1 topology and interpretation context
  grounding/                 # P1 mentions, candidates, assessment, admission
  reality/                   # P1 physical state and observed outcomes
  beliefs/                   # P3 epistemic compiler, validator, applier
  relations/                 # P3 canonical relation instances and lifecycle
  inquiry/                   # P4 temporary context and inquiry sessions
  concerns/                  # P5 attention and concern lifecycle
  intent/                    # P2 constituted intent and grants
  proposals/                 # P6 cross-plane proposals and review fate
  interventions/             # P6 predictions, specs, episodes, settlement
  control/                   # P7 governed policy learning
  projections/               # P8 graph and non-graph projectors
  product/                   # P8 Ask and epistemic rendering
  runtime/                   # P9 transaction, outbox, work, lease, repair
  authority/                 # P10 access, revocation, audit, trace
  app/                       # composition roots and transport only

tests/
  contract/                  # exact schemas and compatibility laws
  component/                 # pure owner-specific tests
  postgres/                  # one-writer durable tests
  ports/                     # adjacent producer/consumer tests
  vertical/                  # bounded provider-free loops
  provider/                  # preregistered provider canaries
  evaluation/                # independent scorer/oracle tests

evaluation/                  # evaluator package outside src/fyralis imports
architecture/                # registry and generated ownership/proof reports
db/migrations/               # new schema only
docs/                        # current architecture, ADRs, plans, runbooks
```

Production code must not import `evaluation` or test fixtures. The evaluator may
read public production artifacts and database views through a read-only role.

## 7. Component Implementation Contract

Before writing a component, create a short checked manifest containing:

- purpose and non-goals;
- semantic plane and architecture component ID;
- inputs and their authority;
- outputs and their durability;
- one logical writer and aggregate boundary;
- public commands, results, events, and reads;
- allowed dependencies and forbidden imports;
- time semantics: occurred-at, observed-at, valid-time, transaction-time,
  cutoff, and current-head rules;
- tenant and authority behavior;
- idempotency key and replay behavior;
- concurrency/fencing behavior;
- correction, revocation, and deletion behavior;
- failure vocabulary and terminal fates;
- telemetry and evidence receipts;
- compatibility/version law;
- L0, L1, L2, and L3 tests;
- known blind spots; and
- definition of done.

No component enters integration with “TODO” ownership for writer, authority,
failure, or correction semantics.

## 8. Build Order

### Phase 0 — Repository and contract kernel

Build only:

- typed IDs and version identities;
- orthogonal semantic axes;
- AuthorityContext;
- bitemporal intervals;
- command/result/event/outbox envelopes;
- writer registry and writer epochs;
- transaction boundary and idempotency convention;
- contract compatibility manifest;
- architecture component registry; and
- independent evaluator compatibility package.

Exit conditions:

- contracts have exact JSON/schema snapshots;
- no domain vocabulary is smuggled into generic types;
- implemented package paths and component test paths are checked in CI;
- production/evaluator imports are mechanically separated; and
- the database can apply and roll back the initial migration in a disposable
  UTF8 PostgreSQL cluster.

### Phase 1 — Evidence and perception

Implement P1 in this order:

1. raw evidence and normalized EvidenceRecord;
2. source event revisions/tombstones;
3. conversation topology and bounded context snapshots;
4. source assertions, semantic frames, and speech acts;
5. mention, type, and local-role extraction;
6. candidate generation with one-set-or-terminal fate;
7. resolution assessment;
8. canonical referent lifecycle;
9. consumer-specific grounding admission;
10. destination-plane admission and independently observed outcomes;
11. correction dependencies and repair; and
12. calibration/fate telemetry.

Do not call a reasoning provider until the deterministic and scripted-provider
versions of these stages are independently green.

### Phase 2 — Atomic belief memory

Implement only evidence-backed atomic beliefs:

- immutable candidate/proposed belief;
- exact typed evidence references;
- admission decision;
- canonical belief version and current-head CAS;
- confidence derived from unique signed evidence;
- correction/supersession history;
- accepted-current read view; and
- deterministic rendering from structured proposition.

No composite synthesis, graph optimization, SAGE, or control learning yet.

### Phase 3 — Retrieval and explicit prior-memory use

Prove the distinction between presence and use:

- exact prior version selected;
- scope matched through authoritative coordinates;
- provider-visible context names a local handle, not a canonical UUID;
- provider classifies `supports | weakens | contradicts | none`;
- compiler authorizes the prior and new evidence;
- the applied lifecycle change is consistent with the decision; and
- telemetry agrees across retrieval, prompt, raw decision, compile, validate,
  apply, and current-head result.

Retrieval selection, durable references, or lifecycle maintenance alone do not
count as memory use.

### Phase 4 — Scope-local synthesis and relations

Build one small `SynthesisProposal | AbstentionDecision` interface.

The LLM owns:

- mechanism;
- direction;
- alternatives;
- counterevidence;
- novelty;
- uncertainty;
- confidence; and
- abstention or inquiry recommendation.

Trusted code owns:

- dossier identity and digest;
- local-handle binding;
- canonical IDs and exact head versions;
- scope and tenant closure;
- evidence validity;
- allowed operation and relation vocabulary;
- transaction construction; and
- atomic composite/relation apply.

The compiler must not infer company meaning through fixture-shaped keywords.
The provider must not copy trusted IDs or digests.

### Phase 5 — Correction, retirement, and later reuse

A correction is one governed transition:

- advance proposition and deterministic natural text together;
- update the current relation's semantic mechanism when justified;
- retire or supersede the old relation version explicitly;
- preserve historical accepted versions;
- invalidate and repair dependent projections and learning eligibility;
- expose the corrected current head; and
- prove a later query/reasoning step uses only the corrected head.

This phase closes the first working-company-learning milestone.

### Phase 6 — Product and independent value proof

Add only:

- Ask over accepted-current memory;
- epistemically explicit answers with evidence and uncertainty;
- one inspectable company-learning report; and
- adaptive versus frozen versus observation-only comparison.

Memory has not earned a central role until it beats simpler controls on sealed
semantic outcomes without safety regression.

### Later phases

Only after the core loop is green:

- concerns and governed attention;
- proposals and intervention episodes;
- authorization and effect adapters;
- workflow/task state;
- governed control-policy learning;
- broader projections and topology;
- connectors;
- scale, chaos, deployment, and product polish.

## 9. Testing Method

### 9.1 The proof ladder

| Level | Purpose | Typical dependencies | Authorization |
| --- | --- | --- | --- |
| L0 Contract | Exact identity, ownership, compatibility, forbidden states | Pure schemas/static checks | Component coding |
| L1 Pure component | Reducers, compilers, parsers, state machines, negative cases | No DB/provider when possible | Durable component tests |
| L2 Durable component | Transactions, schema, outbox, replay, tenant, authority | Fresh UTF8 PostgreSQL | One adjacent integration |
| L3 Port integration | One producer/consumer contract using frozen fixtures | Two components at most | Bounded vertical |
| L4 Vertical | One small company-learning loop, scripted provider first | Required components only | Provider canary readiness |
| L5 E2E/experiment | Frozen population, provider, scorer, manifests | Preregistered isolated environment | Only the exact stated claim |

A missing or red lower level blocks every higher level. Coverage percentage does
not override a failed invariant.

### 9.2 L0 contract tests

Every contract test suite must include:

- exact class/schema/version identity;
- serialized golden fixtures;
- unknown and removed field rejection;
- hidden required-field attack;
- cross-field semantic violation;
- old-reader/new-writer and new-reader/old-writer compatibility where allowed;
- producer/consumer version range;
- writer identity and one-writer negative test;
- production/evaluator separation; and
- digest derived from one source of truth.

The mismatch between frozen
`SynthesisSemanticDecision/think-synthesis-semantic-decision-v1` and implemented
`SynthesisProviderDecision/think-synthesis-provider-decision-v2` must become a
permanent example fixture.

### 9.3 L1 pure tests

Use table-driven and property-based tests for:

- every state transition and illegal transition;
- duplicate/reordered input;
- missing, stale, foreign-tenant, and unauthorized references;
- abstention, review, unknown, defer, reject, supersede, retire, and no-op;
- correction and revocation;
- claim-local evidence selection;
- relation direction and participant roles;
- confidence idempotence under duplicate evidence;
- malformed JSON, schema-invalid JSON, and arbitrary non-JSON provider text;
- local handle collision/unknown handle;
- batch perturbation and distractor isolation; and
- deterministic semantic digests independent of generated UUIDs.

### 9.4 L2 PostgreSQL tests

Use a fresh disposable PostgreSQL database initialized as UTF8. Apply the exact
new migration set from zero. Do not reuse a developer database as component
proof.

Test:

- atomic state/event/outbox commit;
- injected failure and full rollback;
- idempotent replay;
- current-head compare-and-swap;
- concurrent writer fencing;
- tenant isolation and RLS/application checks;
- authority freshness and revocation;
- immutable history and accepted-current views;
- JSON/JSONB normalization for both self-created and caller-provided pools;
- correction and dependency repair;
- lease takeover and terminal work fate; and
- schema drift using executable SQL, not only static string checks.

Mocks may test pure repository orchestration. They may not qualify executable
SQL, migrations, views, triggers, transaction isolation, or rollback.

### 9.5 L3 adjacent-port tests

Each test names:

- producer component and version;
- consumer component and version;
- fixture owner;
- writer/commit authority;
- migration/cutover owner;
- authority mode;
- retry and failure semantics;
- expected terminal fates; and
- exact acceptance assertion.

Do not call a six-component fixture an integration test. Split it into the
individual ports first.

### 9.6 L4 provider-free vertical

Use an injected scripted provider through the actual production provider port.
Do not bypass compiler, validator, applier, receipts, or database writes.

The minimum development vertical should prove:

1. facts are admitted from exact evidence;
2. prior accepted memory is selected and materially used;
3. synthesis and its relation apply atomically;
4. later evidence corrects the synthesis;
5. the old relation is retired or revised coherently;
6. a later batch/Ask reads the corrected current head;
7. distractors remain outside the canonical scope;
8. every batch barrier closes over truth-critical work; and
9. all terminal fates reconcile.

Run the vertical from zero seed at least once. Founder-assisted identity may be
the preferred product mode, but zero-seed behavior must fail safely and retain
bounded mention-scoped facts without inventing cross-observation identity.

### 9.7 L5 provider and E2E testing

Use a fail-fast ladder:

1. one-call transport/schema canary;
2. one-batch semantic canary;
3. two-batch prior-memory-use canary;
4. small frozen positive/positive/null comparison;
5. correction/reuse canary;
6. bounded mixed-stream development proof;
7. sealed unseen-company holdout;
8. matched memory ablation; and
9. robustness/scale only after semantics are green.

Never jump directly to a long company simulation.

## 10. Evaluation Method

### 10.1 Separate mechanical and epistemic verdicts

Always report separately:

- ingestion/work completion;
- queue/barrier/quiescence;
- entity and scope fidelity;
- evidence precision and coverage;
- atomic belief correctness;
- synthesis/mechanism correctness;
- relation direction and participant correctness;
- lifecycle/correction coherence;
- prior-memory material use;
- contamination;
- calibration;
- tokens, latency, retries, and cost; and
- product usefulness.

A run can be mechanically green and semantically red.

### 10.2 Noncompensatory hard gates

The following cannot be averaged away:

- cross-tenant influence;
- unauthorized canonical write;
- unresolved or invented handle;
- missing/foreign evidence;
- partial composite/relation transaction;
- source evidence mutation;
- candidate/review state exposed as accepted truth;
- relation authorized by a lifecycle-only operation;
- current proposition/text/relation disagreement;
- evaluator gold in production input;
- missing terminal provider attempt;
- invalid run identity or digest;
- learning from unattributable outcomes; and
- stale or revoked authority.

### 10.3 Evaluator causality

An evaluator verdict must bind to:

- exact commit;
- exact schema/migration digest;
- exact database snapshot/cutoff;
- tenant and population;
- fixture/gold version;
- provider/model/effort/prompt/schema/policy versions;
- logical calls and physical attempts;
- raw output and parsed output as different representations;
- compiler, validator, applier, and current-head fates; and
- report/scorer version.

Use repeatable-read or an equivalent frozen snapshot for multi-query reports.
Unavailable facts remain unavailable. Do not infer stronger coordinates from
labels, current projections, or summary rows.

### 10.4 Gold discipline

- Gold must not contradict authenticated runtime facts.
- Sparse gold reports required recall separately from open-world precision.
- Unadjudicated discoveries are review spans, not automatic false positives.
- Production code and prompts cannot contain expected thesis names, benchmark
  labels, fixture branches, or scorer thresholds.
- Repair the evaluator independently and rerender the immutable run. Never
  rewrite the raw run to match a repaired evaluator.

### 10.5 Memory-value proof

Use matched arms:

- adaptive memory;
- frozen memory;
- observation-only;
- corrupted/hidden memory where useful.

Freeze treatment at the earliest pre-outcome consumer. Freezing only the final
resolver or reasoning step is invalid if ingestion aliases, retrieval caches,
or another earlier path can already consume learned state.

Measure semantic outcome, safety, cost, and latency. If memory does not beat the
simpler control, simplify the system.

## 11. Provider And LLM Operating Method

### 11.1 Responsibility boundary

```text
deterministic dossier and allowed handles
  -> LLM semantic judgment
  -> trusted identity/evidence compiler
  -> validator
  -> atomic applier
  -> independent evaluator
```

Do not ask the LLM to:

- echo UUIDs or trusted digests;
- decide tenant or authority;
- invent relation types outside a closed vocabulary;
- choose database operations directly;
- carry stale head versions;
- reconcile many unrelated candidates in one flat response; or
- output evaluator labels.

Do not ask the compiler to infer business meaning from lexical keywords. The
compiler closes identity, evidence, legality, and transactions.

### 11.2 Observable cognition

Every logical call records:

- cognitive purpose;
- exact sanitized system/user prompts;
- selected dossier/context manifest;
- prompt, schema, model, effort, routing, and policy versions;
- logical call ID;
- every physical attempt ID;
- raw exact provider text before normalization;
- raw-text digest and parsed-object digest separately;
- parse outcome;
- compiler normalizations/rejection predicate;
- validation and apply fate;
- token/cache/cost/latency data; and
- semantic score reference after the run.

Do not store secrets or inaccessible evaluator gold.

### 11.3 Failure durability

Schema-invalid JSON and arbitrary non-JSON text are valid terminal experiment
outcomes. Preserve them without fabricating a parsed object. Concurrent calls
must all reach a durable terminal outcome; an exception in one task cannot make
sibling calls disappear.

Separate:

- logical operation;
- physical provider attempt;
- parser outcome;
- semantic decision;
- compiler result;
- validator result;
- apply result; and
- evaluator result.

### 11.4 Rerun policy

Before a provider run, write one hypothesis and the exact result that would
falsify it. After a run:

1. freeze the artifact;
2. classify the primary failure;
3. score before editing;
4. reproduce the failure provider-free where possible;
5. make one bounded repair;
6. rerun lower gates;
7. review whether another provider call is authorized; and
8. use a new run identity for a new experiment population.

Never resume, overwrite, selectively complete, or splice calls from an
incomplete experiment into a fresh experiment.

## 12. Database And Data-Model Lessons

### 12.1 Source evidence is immutable

Correction creates new annotations, beliefs, and heads. It never edits the
original observation or re-enters a resolver-authored observation as source
authority.

### 12.2 Candidate is not truth

Persist candidates and admission decisions separately. Admission creates a new
canonical version referencing the immutable candidate. Do not mutate candidate
rows into accepted truth.

### 12.3 Structured proposition is authoritative

Natural text is a deterministic rendering of the structured proposition. A
revision must advance both coherently. If product text, proposition, and
relation mechanism disagree, the current truth is invalid even if every row is
present.

### 12.4 Current visibility is not write success

A successful apply receipt does not prove the result is accepted-current. A
same-transaction later head advance can make a newly written composite stale.
Tests and reports must inspect both physical history and accepted-current views.

### 12.5 Relations are not graph edges

Canonical business relations are typed, versioned relation instances with
roles, evidence, and lifecycle. Binary edges are rebuildable navigation
projections. Projected popularity, activation, or retrieval heat cannot change
relation truth.

### 12.6 Online barriers are explicit

End-of-run queue drain is not online learning. Batch N's truth-critical state
must be visible to batch N+1 while optional projections remain asynchronous.
The zero-pending check and barrier receipt must be serialized against new
truth-critical enqueue.

## 13. Entity, Context, And Scope Lessons

1. Transport batches are scheduling units, not semantic episodes.
2. Slack messages are not self-contained case files. Preserve thread, reply,
   edit, deletion, quote, participant, source-reference, and temporal context.
3. Generate multiple bounded context hypotheses before consequential grounding.
4. Context sufficiency is consumer-specific. Context may be useful for review
   but insufficient for automatic admission.
5. Context selection must not use the identity decision it is meant to prove.
6. Mention detection, type assessment, candidate generation, resolution
   assessment, identity mutation, and consumer admission are distinct.
7. Entity existence, identity, type, and relevance are different judgments.
8. Resolver confidence is not canonical identity authority.
9. Empty candidates do not prove a new entity.
10. Claim scope is derived from exact claim-local evidence, not inherited from
    a whole transport batch.
11. The same literal can have different meanings in different focal signals;
    batch prompts must prevent cross-signal status leakage.
12. Founder bootstrap is the preferred product cold start, but it must be
    versioned, revisable, provenance-bound, and contain no seeded behavioral
    conclusions.
13. Zero-seed mention-scoped atomics are a safe fallback only when bound to an
    exact detection and span. They confer no cross-observation identity or
    synthesis authority.

## 14. Retrieval, Reasoning, And Learning Lessons

### Retrieval

- Track returned, selected, prompt-included, cited, influential,
  counterevidence, background, and unused separately.
- Retrieval heat belongs in rebuildable sidecars.
- A selected Model does not count as used merely because maintenance touched
  it or its ID appeared in a durable reference.
- Scope matching must use authoritative coordinates and provenance, never label
  similarity or arbitrary proposition strings.

### Reasoning

- Present a compact causal dossier, not a flat batch-wide operation list.
- Use local handles and closed vocabularies.
- Separate reconciliation, synthesis, inquiry, and abstention operations.
- Make cross-field constraints visible in the schema before compilation.
- Fail-closed semantic compilers are valuable; strong prose cannot compensate
  for failed identity, evidence, or relation gates.

### Learning

- Activity, storage growth, retrieval, and feedback logging are not learning.
- Learning means an attributable update changed a later decision under an
  explicit version barrier.
- Batch-level reward fanout fabricates evidence. Credit one exact decision and
  mutation chain; normalized hierarchical credit must sum to one.
- Calibration requires independent future outcomes, not scripted
  self-confirmations.
- Every adaptive policy needs bootstrap, shadow evidence, promotion, fallback,
  correction, freeze, rollback, and tenant influence lineage.

## 15. Operational Method

### 15.1 Before any implementation phase

Record:

- clean branch/worktree/repository status;
- exact HEAD;
- component and file ownership;
- migration-number owner;
- database and artifact roots;
- prerequisites and dependencies;
- provider prohibition or authorization;
- falsifiable success criteria;
- stop conditions; and
- intended commit checkpoint.

### 15.2 Before every database proof

- create a fresh database with UTF8;
- apply migrations from zero;
- record PostgreSQL version and migration digest;
- install required JSON/JSONB codecs or normalize at public boundaries;
- use a unique tenant;
- ensure no concurrent shared-database run;
- verify cleanup/teardown is scoped to that database only; and
- save the exact command and result.

### 15.3 Before every provider run

- clean detached/frozen commit;
- exact contract, prompt, schema, model, effort, and policy versions;
- supported CLI/model preflight;
- authenticated provider;
- unique never-used run ID and artifact path;
- isolated database and tenant;
- no active competing run;
- cache/retry/concurrency policy frozen;
- raw/non-JSON failure attacks green;
- exact call-population accounting green;
- scorer/gold isolation green; and
- written hypothesis and stop rule.

For controlled experiments, default to no response cache, zero provider retry,
one physical attempt per planned call, and bounded concurrency. If retries are
part of the product behavior being tested, preregister them and account for
every physical attempt separately.

### 15.4 After every run

- freeze raw artifacts before inspecting conclusions;
- reconcile expected and actual calls/attempts/fates;
- score with the preregistered evaluator;
- record strongest and weakest concrete examples;
- separate environment, contract, semantic, evaluator, and infrastructure
  failures;
- state what is proven and unproven;
- update the learning log;
- commit documentation separately when useful; and
- authorize only the smallest next action.

### 15.5 Commit discipline

Commit after each coherent contract, component, database proof, integration
port, run artifact decision, or documentation checkpoint. Every commit should
be reversible and attributable to one hypothesis. Do not accumulate unrelated
cleanup, performance, and feature changes in the same evidence commit.

## 16. Parallelization Method

Parallelize only after shared contracts are frozen.

Good parallel lanes:

- contract/schema implementation;
- component implementation in non-overlapping packages;
- independent evaluator/fixture work;
- documentation and adversarial review; and
- read-only code/reference audits.

Serialize:

- edits to the architecture registry;
- migrations and shared schemas;
- composition roots;
- shared provider/telemetry envelopes;
- production/evaluator compatibility packages;
- integration commits;
- shared PostgreSQL runs;
- provider experiments; and
- final scoring/authorization.

Every lane gets:

- exclusive file list;
- allowed migrations or no-migration rule;
- input commit;
- exact tests;
- evidence artifact;
- forbidden scope; and
- one integration owner.

More agents without ownership create merge debt and contradictory contracts.

## 17. Failure Classification

Assign one primary class before changing code:

| Class | Examples | Response |
| --- | --- | --- |
| Contract/schema | Name/version mismatch, hidden field, illegal vocabulary | Reproduce and fix at L0/L1; no provider rerun |
| Evidence/context | Wrong cutoff, scope, source, episode, missing counterevidence | Fix P1/P4 with frozen fixtures |
| Entity/grounding | Missing fate, circular context, false canonical link | Fix the exact P1 stage; do not weaken admission |
| Semantic/model | Wrong mechanism, direction, alternative, or abstention | Compare frozen interfaces/policies only after lower gates are green |
| Compiler | Wrong binding, closure, operation, transaction plan | Add exact negative test and remain fail-closed |
| Validator/applier | Re-promotion, partial write, stale head, lifecycle corruption | Stop integration; repair before any higher run |
| Runtime/durability | Lost attempt, retry invisibility, lease/barrier race | Fix P9 and complete terminal accounting |
| Evaluator | Gold leak, wrong denominator, stale view, missing coherence check | Preserve run; repair scorer independently and rerender |
| Infrastructure | Unsupported model, DB unavailable, encoding/config failure | Fix environment and rerun the same smallest rung if identity law allows |
| Performance | Cold/warm mismatch, concurrency tail, retry cost | Diagnose after semantic correctness unless it blocks completion |
| Noncore | UI, connector breadth, rare ontology, polish | Log and defer |

One symptom may have many downstream red metrics. Repair the earliest causal
defect, not every red metric independently.

## 18. Do

- Do start in a new repository with a fresh schema.
- Do build one company-learning loop before broad features.
- Do freeze a small contract before parallel work.
- Do assign one writer per semantic class.
- Do keep evaluator code physically and mechanically separate.
- Do use exact typed evidence and claim-local lineage.
- Do preserve raw and parsed provider representations separately.
- Do make every failure and no-op observable.
- Do use real PostgreSQL before adjacent integration.
- Do create fresh UTF8 databases for proof runs.
- Do test injected rollback, replay, concurrency, tenant, and revocation.
- Do inspect physical history and accepted-current views.
- Do record selected versus materially used context separately.
- Do use local handles in LLM interfaces.
- Do make relation vocabularies closed and direction explicit.
- Do treat correction as a governed transaction.
- Do keep derived graph/index state rebuildable.
- Do use founder bootstrap as revisable prior, not seeded truth.
- Do score before editing after a run.
- Do preserve red artifacts as learning evidence.
- Do write one hypothesis per rerun.
- Do keep exact commit/config/run identity.
- Do commit small reversible checkpoints.
- Do let simpler architecture win if memory fails its ablation.
- Do report what each result does not prove.

## 19. Do Not

- Do not copy the old repository into a new folder and call it a rewrite.
- Do not import old packages or share its database.
- Do not port hundreds of migrations.
- Do not preserve legacy APIs without a named first-milestone consumer.
- Do not begin with connectors, UI, task autonomy, or workflow execution.
- Do not create a second persistent working graph.
- Do not treat a batch as a semantic scope.
- Do not let entity assessment mutate canonical identity.
- Do not let intent establish factual truth.
- Do not let retrieval presence count as learning.
- Do not let graph edges become canonical relations.
- Do not mutate source observations during correction.
- Do not let the LLM author trusted identity, UUIDs, or digests.
- Do not let the compiler invent business semantics through keyword rules.
- Do not weaken a validator or evaluator to pass a run.
- Do not average away an invariant violation.
- Do not infer missing evidence from a current projection or display label.
- Do not call mocked SQL proof.
- Do not reuse a stale developer database for qualification.
- Do not run a provider before malformed-output durability is proven.
- Do not launch long E2E tests as preflight.
- Do not patch and immediately rerun without a written hypothesis.
- Do not resume or overwrite incomplete experiment identities.
- Do not combine artifacts from different commits into one system verdict.
- Do not optimize latency before semantic correctness unless latency prevents
  completion.
- Do not raise debt budgets to make new debt green.
- Do not follow every edge case off the current milestone.
- Do not call the core complete before correction and later reuse are proven.

## 20. Technical-Debt Prevention

The new repository begins with a zero-growth policy rather than inheriting the
old debt budget.

CI should require:

- architecture registry validity;
- no forbidden imports;
- production/evaluator separation;
- migration prefix uniqueness and migration-from-zero;
- component path/test ownership;
- exact contract snapshots;
- focused unit/component/PostgreSQL tests;
- lint and type checks;
- no committed secrets or generated reports;
- no increase in long-file/function/class counts without explicit exception;
- no new unclassified tables, routes, workers, or environment variables; and
- documentation update when a public contract changes.

Track:

- owned versus shared/unclassified production paths;
- writer count per semantic class;
- cross-component private DB access;
- component defects first discovered at L4/L5;
- expensive run minutes and tokens lost to lower-gate defects;
- terminal-fate coverage;
- test runtime by proof level;
- tables without writer/reader/removal law;
- compatibility paths without retirement condition;
- production imports from evaluation;
- long files/functions/classes;
- flaky/retried tests; and
- time from red component test to localized cause.

The primary operational success metric is:

> zero L0–L3 defects first discovered by an L5 run.

## 21. Minimum Evidence Artifact

Every major phase or run produces a manifest containing:

- repository and commit;
- dirty/clean status;
- component and contract versions;
- database identity, version, encoding, migration digest, tenant, and cutoff;
- fixture/population/gold digests;
- provider/model/effort/prompt/schema/policy versions;
- logical calls, physical attempts, retries, cache, tokens, latency, and cost;
- raw/parsed/compiled/validated/applied/current-head digests and fates;
- expected versus actual work/fate counts;
- continuous metrics and noncompensatory gates;
- strongest and weakest example;
- failure classification;
- what is proven;
- what is unproven;
- deferred findings; and
- exact next authorized action.

Artifacts are immutable. Reports may be rerendered from them with a new scorer,
but the rerender must identify the original run and new evaluator version.

## 22. Definition Of Done

### First working core

- one clean repository and fresh schema;
- evidence -> grounding -> atomic belief path green;
- exact prior memory selected and materially used;
- one scope-local synthesis and canonical relation admitted atomically;
- later evidence corrects the synthesis coherently;
- old relation version retires or revises correctly;
- later retrieval/Ask uses the corrected current head;
- null/distractor case does not create false truth;
- all terminal fates reconcile;
- provider-free vertical green on a fresh database;
- smallest provider canaries green under frozen contracts; and
- independent report shows no hard-gate failure.

### Core system qualified

- mixed-stream development proof;
- sealed unseen-company proof;
- adaptive memory beats frozen/observation-only controls or the design is
  simplified;
- correction, replay, interruption, tenant, revocation, bounded growth, and
  quiescence proven;
- exact operational cost and latency characterized; and
- final report states the bounded guarantee without extrapolation.

## 23. First Actions For The Next Implementation Session

Perform these in order:

1. Read this handoff completely.
2. Read `docs/reference/LATEST-SYSTEM-COMPONENTS.md` and the revised target
   architecture's constitutional invariants and component catalog.
3. Choose and create the separate repository name/location. Do not initialize
   it inside the old repository.
4. Add a one-page README containing the first loop and non-goals.
5. Create the proposed package/test layout with empty ownership boundaries,
   not placeholder implementations.
6. Port the architecture registry schema and reduce it to the components and
   contracts needed for the first loop.
7. Freeze Phase 0 C0 contracts and exact test fixtures.
8. Create a fresh PostgreSQL schema baseline and migration-from-zero test.
9. Implement P1A evidence ingestion and fate accounting with L0–L2 proof.
10. Continue one component at a time through the build order.
11. Keep provider access disabled until the provider-free vertical and failure
    durability attacks are green.
12. Update this handoff or a new learning log whenever a failure changes a
    reusable implementation rule.

Suggested continuation instruction:

> Read `docs/plans/fyraliscore-clean-reimplementation-handoff-20260719.md`
> completely. Create a genuinely separate clean Fyralis Core repository; do
> not import or copy the legacy runtime. Implement only Phase 0 first: the
> contract kernel, architecture registry, clean package/test boundaries, fresh
> PostgreSQL migration baseline, and their L0–L2 tests. Keep task autonomy,
> connectors, UI, broad learning, and provider runs out of scope. Commit a clean
> reversible checkpoint and report exactly what is proven and unproven.

## 24. Final Operating Principle

Build the smallest system whose truth is hard to violate and whose learning is
easy to inspect.

The previous system proved that a large runtime can process signals, drain
queues, generate Models, and still misunderstand the company. The new system
must make evidence, authority, semantic judgment, mutation, correction,
evaluation, and learning visibly separate. Only then should scale and autonomy
be added.
