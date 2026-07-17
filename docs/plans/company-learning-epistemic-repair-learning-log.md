# Company-Learning Epistemic Repair — Learning Log

**Document type:** Append-only working scratchpad and evidence index

**Status:** Active

**Started:** 2026-07-17

**Active branch:** `codex/autonomous-company-learning`

**Active worktree:** `/Users/rachinkalakheti/fyraliscore-autonomous-learning`

**Execution contract:**
[Company-Learning Epistemic Repair Agent Coordinator](company-learning-epistemic-repair-agent-coordinator.md)

## 1. Purpose And Authority

This file is the coordinator's scratchpad for the implementation journey. It
records observations, hypotheses, failed assumptions, decisions, evidence,
work-package state, and reusable lessons while the repair program runs.

This file is deliberately separate from the main architecture documents.
Entries here are not architecture merely because they were written down.
A finding becomes normative only when it has:

1. an evidence reference;
2. an explicit coordinator decision;
3. implementation or contract ownership;
4. passing validation at the declared proof boundary; and
5. a durable architecture/documentation update at the appropriate phase.

The coordinator document is normative for execution. This log explains what
was learned and why the coordinator changes over time.

## 2. Update Protocol

The root coordinator updates this log when:

- a package is claimed, completed, reverted, or blocked;
- a hard gate fails;
- a benchmark or production-shaped test teaches a reusable lesson;
- an implementation assumption is falsified;
- a design choice is accepted, rejected, or deferred;
- a threshold, gold set, or scenario version changes;
- an agent discovers overlapping ownership or reuse;
- a phase checkpoint is committed; or
- the strategic role of memory changes after ablation.

Rules:

- Do not rewrite an old entry to make the journey look cleaner.
- Append a correction that references the superseded entry.
- Distinguish `observed`, `inferred`, `hypothesized`, and `decided`.
- Link to member-level evidence, not only a summary score.
- Record negative results and proof gaps.
- Never use this log as an excuse to bypass a coordinator hard gate.
- Keep raw provider transcripts and generated reports out of git; record their
  manifests, hashes, and local paths only when safe.

## 3. Current Snapshot

| Item | Current state |
| --- | --- |
| Repository checkpoint before these docs | `d4335afa4b0d` |
| Repair implementation | Not started under the new coordinator |
| Current phase | P0 characterization complete; P1 implementation next |
| Historical large run | `autonomous-learning-cold-start-45-be401f25`; 45 batches x 25 signals = 1,125 signals |
| Historical large-run verdict | `not_credible` for system/product proof |
| Historical run role now | Immutable forensic baseline; not a benchmark to optimize or rerender into new semantic proof |
| Current execution boundary | Begins with normalized, source-attributed signals already persisted in PostgreSQL |
| Explicitly excluded | Connectors/listeners, OAuth/webhooks, task autonomy, external consequential action, second 45-batch run |
| Highest-priority outcome | One benchmark-blind, evidence-grounded, online company-learning loop whose memory value is proven against controls |

The repository contains meaningful bounded component proofs, especially around
entity extraction, corrective entity memory, structured source identity, and
SAGE/retrieval effects. They are evidence about their stated populations, not
retroactive repair of the historical 45-batch run and not proof of one coherent
integrated company-learning system.

## 4. Journey So Far

### 4.1 Product abstraction converged

The system's highest-level purpose became clearer over the design discussion:
build an increasingly faithful, evidence-grounded model of how a company
works, changes, and relates—not autonomous task execution.

The graph is valuable only when it represents accepted company semantics with
evidence and lifecycle. Retrieval neighborhoods, episode candidates, and
adaptive inquiry already perform the temporary working-set role; a second
persistent working graph would add ambiguous authority without adding meaning.

### 4.2 Feedback became a constitutional loop

Feedback and learning cannot be a reporting add-on. The intended loop is:

```text
new signals
  -> interpretation and grounded decision
  -> accepted mutation, correction, or justified no-op
  -> later outcome
  -> exact decision/evidence attribution
  -> policy update
  -> changed later reasoning
```

Activity is not learning. The later system must prove that an attributed update
changed a later decision under an online version barrier.

### 4.3 Company physics moved upstream

Entity extraction and grounding became first-order product boundaries. If the
system links the wrong customer, team, actor, resource, commitment, or project,
every later Model, relation, retrieval result, and feedback update becomes
misleading even if the runtime is mechanically flawless.

Slack-like signals exposed a more general upstream problem: messages are not
self-contained case files. Temporal arrival batches are not semantic episodes.
Threads, replies, edits, quotations, participants, long-range recurrence,
topic drift, and cross-source links must produce multiple bounded hypotheses
before mention grounding and claim extraction.

### 4.4 The implementation journey became too broad and too slow

Early work mixed architectural discovery, edge-case repair, framework growth,
documentation, and implementation in a dirty primary repository. This caused
repeated inspection, staging caution, ownership conflict, and work on surfaces
that did not yet prove the core loop.

The durable corrections were:

- isolate work in a clean integration worktree and branch;
- record the clean baseline;
- reuse existing entity, retrieval, SAGE, Model, clarification, projection,
  and Company Vitals owners before creating anything;
- pursue a three-batch end-to-end vertical before broad hardening;
- keep edge cases in a ledger until the core path is green;
- divide work into agent-sized, non-overlapping packages;
- commit coherent checkpoints frequently; and
- keep discoveries out of the main architecture document until accepted.

### 4.5 Bounded components improved, but integration remained unproven

The journey produced bounded improvements and proofs for mention extraction,
entity clarification/correction, source identity, retrieval/SAGE behavior, and
evaluation reporting. These are reusable rather than discarded.

The lesson is not that bounded tests are weak. The lesson is that a portfolio
of green bounded tests from different commits/configurations is not the same
thing as one green end-to-end system on one commit.

### 4.6 The 45-batch cold-start run proved metabolism

Observed historical facts from the postmortem:

- one fresh tenant began with zero semantic memory;
- 1,125 signals were processed in 45 real 25-signal batches;
- all 45 batches eventually completed;
- later evidence changed memory;
- queues ultimately drained;
- the saved state contained 85 active Models and 137 active of 140 total
  binary edges; and
- the independent thesis judge recovered only 5 of 9 hidden theses.

This is meaningful evidence that the mechanical substrate can process a long
evolving stream. It is not credible evidence that the substrate understood the
company correctly.

### 4.7 The same run exposed the semantic failure cluster

The postmortem found:

- entity grounding did not execute end to end over its intended population;
- the resolver wrote 50 aliases into canonical identity state;
- retrieval did not become Model-first or actually use retrieved Models enough;
- hidden-pattern proxies overstated independent thesis recovery;
- Model scope was effectively batch-wide rather than claim-local;
- control/benchmark language entered canonical memory;
- relation direction and rationale were unsafe;
- confidence was not credibly calibrated;
- repair/projection metabolism was expensive; and
- one recovered Think incident consumed a disproportionate amount of wall
  time.

These were not ten unrelated bugs. They formed a contamination cascade:

```text
batch-wide scope
  -> false semantic overlap
  -> polluted retrieval and candidate generation
  -> cross-story relations and generic hubs
  -> duplicated credit and invalid scores
  -> policy reinforcement of activity
  -> more contamination
```

### 4.8 The adversarial audit invalidated the reassuring headline

The follow-up audit found benchmark answer leakage, production fixture hooks,
circular calibration, a gameable scorer, mixed proof states, and normalized
metrics that could exceed `1.0`.

The most valuable behavior was evaluative self-correction: the system's own
evaluation process preserved the evidence and downgraded the conclusion. That
discipline must become a permanent run contract.

### 4.9 Reviewer synthesis: metabolism works; epistemics were bypassed

The external reviewer identified one structural distinction:

- mechanism-enforced properties such as queue durability, outbox behavior, and
  some validator constraints held;
- prose/convention-enforced properties such as single-writer authority,
  admission, lifecycle closure, evidence scope, representation authority, and
  benchmark blindness were bypassed.

Therefore the next phase is subtraction and enforcement. The system does not
need more semantic degrees of freedom before the small truth core becomes
mechanically difficult to violate.

### 4.10 The memory premise remains a hypothesis

The product premise is that accumulated, compressed company memory lets later
reasoning become better, safer, and cheaper than reasoning mainly from raw
observations.

The 45-batch run did not prove that premise. Models were created, but retrieval
plateaued, late reasoning still leaned heavily on observations, several
situation composites were isolated, and the missed theses were integration
failures.

The accepted next step is a matched ablation. Until adaptive memory beats
frozen and observation-only controls on sealed semantic outcomes without
safety regression, Model-layer expansion remains unearned.

## 5. Durable Lessons

| ID | Lesson | Consequence |
| --- | --- | --- |
| L-001 | Mechanical throughput is not epistemic correctness. | Always report metabolism and semantic truth separately. |
| L-002 | A constitutional invariant implemented as convention will eventually be bypassed. | Put authority, admission, lifecycle, and tenant boundaries in storage/repository constraints and independent CI. |
| L-003 | A transport batch is scheduling, not meaning. | Build multiple episode/context hypotheses and measure boundary quality explicitly. |
| L-004 | Exact claim evidence is the organizing primitive. | Derive scope, admission, relation evidence, and feedback credit from claim-local lineage. |
| L-005 | Entity inference must not choose the evidence that confirms itself. | Freeze context/candidates before assessment; separate assessment from admission. |
| L-006 | Resolver confidence is not identity authority. | Only governed source identity or adjudication may mutate canonical identity. |
| L-007 | Candidate/review truth must be physically isolated. | Default readers, relations, projections, and SAGE exclude nonaccepted state. |
| L-008 | Natural and structured claims cannot be coequal writers. | Make structured proposition/version authoritative and render natural text deterministically. |
| L-009 | Lifecycle is a transaction, not a label update. | Fence retrieval, reconcile dependencies, invalidate projections, and append history atomically. |
| L-010 | Relation meanings require separate planes. | Keep epistemic evidence, lifecycle history, and business relations distinct. |
| L-011 | Binary edges are a poor canonical home for N-ary business meaning. | Make versioned relation instances canonical and binary edges projections. |
| L-012 | Selected context is not automatically useful context. | Record returned, selected, included, cited, influential, counterevidence, background, and unused separately. |
| L-013 | Batch-level reward fanout fabricates evidence. | Credit one decision/mutation chain and normalize any hierarchical attribution to total weight one. |
| L-014 | Online learning requires a version barrier, not final queue drain. | Make batch N truth-critical state visible to N+1 while optional derived work remains async. |
| L-015 | A bounded green test proves only its frozen population. | Label proof boundaries and keep open-world/company-scale claims separate. |
| L-016 | Evidence from multiple commits/configurations is a portfolio, not one system run. | Release verdicts use one coherent commit/configuration; historical evidence stays separate. |
| L-017 | Benchmark vocabulary in production destroys benchmark blindness and product safety. | Scan production paths and quarantine test hooks before semantic evaluation. |
| L-018 | Calibration requires external future outcomes. | Exclude scripted self-confirmations from Brier/ECE product claims. |
| L-019 | Memory must earn complexity empirically. | Run adaptive/frozen/observation-only/hidden/corrupted arms and accept simplification as a valid outcome. |
| L-020 | Parallelism needs ownership, not only more agents. | Freeze contracts first, assign exclusive files/migrations, then parallelize independent packages. |
| L-021 | Edge cases discovered before the vertical loop can consume the entire project. | Log noncritical cases; fix only those blocking the current phase hard gates. |
| L-022 | Provider retry time can dominate a cheap run. | Use one shared physical-attempt ledger and cap the entire logical operation. |
| L-023 | Admission must preserve the pre-truth record. | Create a canonical version that references an immutable candidate and AdmissionDecision; do not mutate the candidate into truth. |
| L-024 | Retrieval heat is utility, not semantics. | Store activation/count/last-use in rebuildable sidecars; reads never alter Model identity, confidence, or lifecycle. |
| L-025 | Repetition is not independent confirmation. | Derive confidence from unique signed evidence; duplicate delivery and projection rebuild are epistemically idempotent. |
| L-026 | Evidence references are polymorphic but must be typed. | Validate Observation, ModelVersion, or other registered evidence kinds explicitly; never infer source type from list position. |

## 6. Accepted Decisions

| ID | Date | Decision | Status | Evidence/Reason |
| --- | --- | --- | --- | --- |
| DEC-001 | 2026-07-17 | Task autonomy remains out of scope. | accepted | The goal is autonomous company learning and modeling. |
| DEC-002 | 2026-07-17 | Begin from normalized persisted signals; do not implement connector transport. | accepted | User scope; ingestion transport is simulated. |
| DEC-003 | 2026-07-17 | Use three logical planes and one accepted graph; no persistent working graph. | accepted | Retrieval/context hypotheses already provide temporary working state. |
| DEC-004 | 2026-07-17 | Preserve the 45-batch run as immutable forensic evidence and retire it as semantic proof. | accepted | Benchmark leakage and incoherent proof boundary. |
| DEC-005 | 2026-07-17 | Make invariant violations noncompensatory. | accepted | Reviewer/audit showed averages hid illegal truth states. |
| DEC-006 | 2026-07-17 | Treat versioned N-ary relation instances as canonical business relations and binary edges as projections. | proposed; P0 must ratify against all readers/writers | Repository currently contains contradictory truth claims. |
| DEC-007 | 2026-07-17 | Run the deterministic three-batch vertical before another integrated semantic benchmark. | accepted | Fastest proof of the online causal contract. |
| DEC-008 | 2026-07-17 | Use a 12-batch mixed-stream decisive run, not another 45-batch run, for the repaired core. | accepted | Enough chronology for creation/reuse/correction while controlling cost and ambiguity. |
| DEC-009 | 2026-07-17 | The Model layer must earn its primary role through paired memory ablation. | accepted | Current memory value is unproven. |
| DEC-010 | 2026-07-17 | Main architecture docs are updated only after validated contract decisions. | accepted | User asked for a separately inspectable discovery record. |
| DEC-011 | 2026-07-17 | Commit each coherent work-package/checkpoint. | accepted | Easier review, bisect, and reversion. |

## 7. Active Hypotheses

| ID | Hypothesis | Falsifying evidence | Planned test | State |
| --- | --- | --- | --- | --- |
| HYP-001 | Claim-local citations and scope derivation will eliminate most contamination rather than requiring a new graph algorithm. | High cross-story contamination remains after exact evidence/scope enforcement. | P2 fixtures, P6 mixed stream | open |
| HYP-002 | Existing retrieval/SAGE machinery is sufficient once telemetry and attribution are exact. | It cannot produce measurable online policy change or adaptive lift without a second learner. | P4 online proof, P7 ablation | open |
| HYP-003 | Episode hypotheses plus adaptive context selection can model Slack without a persistent working graph. | Stable boundary/context quality requires durable second-graph semantics. | P3 perturbation suite, P6 | open |
| HYP-004 | A small relation vocabulary will outperform the current broad/ambiguous edge vocabulary. | Semantic recall collapses or unknown-candidate debt becomes unmanageable. | P2 relation oracle, P6 | open |
| HYP-005 | The current mechanical substrate can support truth barriers without draining all background work. | Truth-critical backlog or visibility cannot be isolated from optional work. | P4 barrier and P8 fault/scale | open |
| HYP-006 | Adaptive memory materially improves multi-facet company understanding. | Adaptive does not beat frozen/observation-only under P7 decision rules. | P7 ablation | open |
| HYP-007 | The 45-batch generic hub and cross-story edges were consequences of scope/evidence contamination, not useful synthesis. | They remain semantically correct under claim-local relation gold. | P2/P6 oracle | open |

## 8. Known Architectural Contradictions To Resolve

| ID | Contradiction | Coordinator phase | Required resolution |
| --- | --- | --- | --- |
| ARC-001 | `model_edges` and `relation_instances` are both described/used as accepted relation truth. | P0/P2 | One canonical business-relation authority; legacy fate manifest; projector-only binary writes. |
| ARC-002 | Candidate/review states exist but some readers/relations historically treated them as active truth. | P0/P2 | One admission view used by every canonical reader and endpoint validator. |
| ARC-003 | Natural claim text and structured proposition can diverge. | P2 | One immutable semantic version/digest and deterministic rendering. |
| ARC-004 | Resolver produces assessments while also historically mutating identity authority. | P2/P3 | Assessment/admission/application separation and database/repository permissions. |
| ARC-005 | End-of-run quiescence can be green while online learning is unavailable between batches. | P4 | Truth-critical version barrier plus separate background backlog. |
| ARC-006 | Existing evaluation artifacts can combine strong bounded evidence with a failed historical run. | P0/P9 | Coherent-run report identity and explicit evidence portfolio separation. |

## 9. Work-Package Ledger

This table is the scratchpad mirror of coordinator state. The coordinator owns
authoritative dependency and success rules.

| Package/phase | Owner | Input commit | State | Evidence | Last note |
| --- | --- | --- | --- | --- | --- |
| P0 contract/baseline/preregistration | root + three parallel lanes | `841f6f93e4de` | validated | `docs/plans/epistemic-repair/p0/epistemic-repair-p0-baseline-v1.json`; 21 focused tests | Characterization complete; repairs intentionally not mixed into P0. |
| P1 blindness/observability/retry | unassigned | P0 checkpoint | pending | — | Four hook families and five telemetry reconciliation gaps are the exact repair surface. |
| P2 truth/evidence/lifecycle/relation | unassigned | — | pending | — | Contracts may start after P0. |
| P3 boundary/context/entity | unassigned | — | pending | — | Contracts may start after P0/P1. |
| P4 online barrier/retrieval/feedback | unassigned | — | pending | — | Integration waits on P2/P3. |
| P5 three-batch vertical | unassigned | — | pending | — | First integrated deterministic proof. |
| P6 12-batch mixed stream | unassigned | — | pending | — | No run before P5 green. |
| P7 matched memory ablation | unassigned | — | pending | — | Strategic fork. |
| P8 fault/scale/characterization | unassigned | — | pending | — | Only earned architecture proceeds. |
| P9 bounded release decision | unassigned | — | pending | — | Requires one coherent evidence set. |

## 10. Open Questions

These do not all require user input. Agents should resolve routine questions
using the coordinator priority order and record evidence here.

1. Which durable object currently owns the authoritative ModelVersion head?
2. Which database roles can directly mutate canonical entity/Model/relation
   tables today?
3. Can existing evidence-link tables express exact source coordinates and
   decisive/context/counterevidence roles without migration?
4. Which accepted readers still consume `model_edges` without a canonical
   relation-instance source?
5. Which current post-commit tasks are truth-critical versus optional derived
   enrichment?
6. Can SAGE policy updates be causally attributed with existing route IDs and
   propensity data, or only associatively?
7. Which entity correction paths fully invalidate dependent scopes,
   relations, and retrieval indexes?
8. What is the smallest business-relation vocabulary that covers the sealed
   P6 world without type coercion?
9. Which historical artifacts can be reopened reproducibly, and which should
   remain summary-only evidence?
10. What real-provider sample is sufficient after deterministic
    characterization without recreating a costly broad benchmark?

## 11. Evidence Index

| Evidence | Role |
| --- | --- |
| [45-Batch Cold-Start Postmortem](../evaluation/autonomous-company-learning-cold-start-45-postmortem-20260717.md) | Authoritative interpretation of the saved 45-batch state and its proof limits |
| [Autonomous Company-Learning Journey Status](autonomous-company-learning-journey-status.md) | Detailed historical implementation/commit/evaluation journey |
| [Autonomous Company-Learning Reuse Audit](autonomous-company-learning-reuse-audit.md) | Required reuse/consolidate/defer/remove boundary |
| [Autonomous Company-Learning Edge-Case Ledger](autonomous-company-learning-edge-case-ledger.md) | Deferred noncritical cases and later hardening work |
| Reviewer note `/Users/rachinkalakheti/.codex/attachments/68697d1e-46ef-4104-949f-03197eb04edf/pasted-text.txt` | External structural synthesis: metabolism versus epistemics, contamination cascade, memory-value fork |

## 12. Chronological Entries

### 2026-07-17 — LOG-001 — Historical run classified

**Type:** observed + decided

The saved 45-batch cold-start run remains evidence of mechanical metabolism
and an immutable source of failure examples. It is `not_credible` as semantic
or product proof because entity, scope, relation, retrieval, lifecycle, and
evaluation trust boundaries failed. No later bounded rerender changes that
run's company state.

**Effect:** retire its scenario from optimization; use it for P0 invariant
reproduction and regression fixtures only.

### 2026-07-17 — LOG-002 — Reviewer structural synthesis accepted

**Type:** inferred + decided

Most defects reduce to constitutional rules implemented as conventions. The
repair program will first make illegal truth states unrepresentable or
unreadable, then improve semantic quality. The contamination cascade is
treated as one root-cause family beginning at evidence/scope rather than as a
collection of graph-tuning defects.

**Effect:** P0-P2 precede topology sophistication. Hard gates cannot be offset
by continuous scores.

### 2026-07-17 — LOG-003 — Target architecture simplified

**Type:** decided

Use immutable evidence, governed accepted truth, and derived/adaptive state.
Keep one accepted company graph. Episode hypotheses, candidates, retrieval
packets, projections, and SAGE policy remain noncanonical. The coordinator
must resolve the repository's `model_edges` versus `relation_instances`
authority conflict before implementation agents modify writers.

**Effect:** no new working graph or parallel learner/registry is authorized.

### 2026-07-17 — LOG-004 — Execution strategy frozen

**Type:** decided

Execution proceeds through contract/blindness, truth kernel and perception,
online feedback, a deterministic three-batch vertical, a 12-batch mixed stream,
matched memory ablation, then fault/scale characterization. Each package has
exclusive ownership, independent evaluation, objective gates, and a coherent
commit.

**Effect:** implementation agents should be able to proceed without routine
user input. They may not soften gates or broaden scope.

### 2026-07-17 — LOG-005 — Truth/relation contract review tightened

**Type:** inferred + decided

The relation/lifecycle review found four subtle ways the repaired system could
still manufacture certainty: mutating candidates into truth, changing Model
semantics when they are merely retrieved, monotonically increasing relation
confidence on duplicate observations, and inferring evidence source type from
an untyped event list. It also confirmed that current relation participant
replacement must become immutable version append rather than delete/reinsert.

**Effect:** the coordinator now requires AdmissionDecision-linked canonical
versions, retrieval sidecars, signed unique evidence, typed evidence
references, compare-and-swap relation versions, and historical participant
bindings.

### 2026-07-17 — LOG-006 — P0 authority and illegal-state baseline completed

**Type:** observed

The direct production SQL census found 22 canonical writer modules and 86
canonical reader modules across seven truth tables. Authority is fragmented
across domain, reasoning, product, and maintenance code. Six bypass families
are confirmed, including maintenance alias deletion, accepted binary-edge
writes, relation/projection co-location, and retrieval heat on canonical Model
rows. Fifteen cross-table illegal truth-state classes remain representable.

**Evidence:** `docs/plans/epistemic-repair/p0/authority-writer-reader-inventory.json`;
`docs/plans/epistemic-repair/p0/truth-state-inventory.json`.

**Effect:** P2 must consolidate registered authority and reader fences. Static
inventory coverage is not proof against dynamic SQL, database procedures, or
external writers.

### 2026-07-17 — LOG-007 — Benchmark blindness and telemetry both fail baseline

**Type:** observed

Four hook families are reachable from production Think: dynamic reasoning
augmentors, an explicitly benchmark-only capability injector, a fixture-
phrase-sensitive pricing bridge, and a fixture/scorer-aligned noise fast path.
Physical failed provider attempts have no durable receipt; logical calls have
no stable IDs; retry classes are conflated; stage timings lack parent/exclusive
semantics; and no coherent queue/batch/run receipt reconciles the whole run.

**Evidence:** `docs/plans/epistemic-repair/p0/benchmark-hook-inventory.json`;
`docs/plans/epistemic-repair/p0/telemetry-inventory.json`.

**Effect:** HG-01 and HG-13 are red. P1 starts with hook quarantine and a
physical-attempt/logical-call ledger before semantic evaluation resumes.

### 2026-07-17 — LOG-008 — Preregistration and evidence identity are now explicit

**Type:** observed + decided

The P0 contract now seals scenario, gold, evaluation policy, runtime sources,
provider configuration, repository overlay, seeds, hard gates, proof
boundaries, allowed executions, and whole-operation budgets. Reopening detects
tampering and exhausted execution allowances. Eight historical/bounded
evidence items are inventoried without composing them into one system score;
nine required populations remain open.

**Evidence:** `lib/evaluation/epistemic_repair/preregistration.py`;
`docs/plans/epistemic-repair/p0/evidence-inventory.json`;
`docs/plans/epistemic-repair/p0/epistemic-repair-p0-baseline-v1.json`.

**Effect:** later runs cannot change inputs or mix code states silently. The
contract proves preregistration identity, not runtime or semantic success.

### 2026-07-17 — LOG-009 — P0 validation boundary recorded

**Type:** observed

All 21 P0 focused tests, JSON parsing, compile checks, import contracts,
architecture ratchets, and the production-environment contract passed. Ruff is
not installed in this worktree. The repository technical-debt budget remains
red on pre-existing aggregate debt and named existing paths; no P0 production
file appeared in the reported path violations.

**Effect:** P0 is valid as a characterization checkpoint. The debt-budget
failure is visible but does not justify mixing unrelated cleanup into P1.

### 2026-07-17 — LOG-010 — Logical calls and physical attempts are different facts

**Type:** observed + decided

Provider instrumentation showed that a logical structured-output request may
produce several physical attempts through parse repair, transport retry, or an
SDK's own retry policy. Aggregate call counters cannot prove attempt count,
latency, failure rate, or cost when those layers are conflated.

**Evidence:** `lib/llm/telemetry.py`;
`db/migrations/0224_llm_call_attempt_receipts.sql`;
`tests/epistemic_repair/p1/test_llm_attempt_receipts.py`.

**Effect:** P1 records immutable logical-call and physical-attempt identities
separately. Provider code emits facts; the Think-owned adapter attaches tenant,
trigger, run, batch, context, validation, and apply coordinates. The remaining
retry work must establish one owner and disable opaque SDK retries before the
ledger can claim wire-attempt completeness.

### 2026-07-17 — LOG-011 — Timing reconciliation requires exclusive leaves

**Type:** decided

Summing nested stage timings double-counts work, while recording only a run wall
time hides uninstrumented gaps. Failed attempts also consume time and cost and
cannot be omitted from health reports.

**Evidence:** `lib/evaluation/epistemic_repair/reconciliation.py`;
`tests/epistemic_repair/p1/test_timing_cost_reconciliation.py`.

**Effect:** P1 evaluation treats inclusive parent spans as navigation only and
reconciles non-overlapping exclusive leaf spans against logical wall time. It
reports gap, overlap, error, token coverage, cost coverage, and actual-versus-
estimated deltas continuously, with the hard timing threshold fixed at 1%.

### 2026-07-17 — LOG-012 — Receipt post-commit gap found and closed

**Type:** observed + corrected

Think can now collect provider receipts task-locally and persist them with
tenant, trigger, run, and batch coordinates. The first integration persisted
only after domain finalization, allowing ledger failure after domain effects
committed. The corrected path writes receipts inside the semantic mutation
transaction, then performs an identical idempotent durability check outside.

**Evidence:** `services/reasoning/think/reason.py`;
`tests/epistemic_repair/p1/test_think_receipt_runtime.py`.

**Effect:** deterministic tests prove isolation, returned-failure retention,
fail-closed persistence, and rollback of surrounding semantic effects when the
ledger rejects a receipt. Missing receipt evidence can no longer be created by
committing domain effects first.

### 2026-07-17 — LOG-013 — Real-provider failure is now truthful evidence

**Type:** observed

The bounded clean DeepSeek smoke reached the provider and received HTTP 402
`Insufficient Balance`. The wrapper classified it as permanent, made no retry,
and emitted one logical-call receipt plus one physical-attempt receipt with the
context digest, failure timing, unavailable usage basis, and zero claimed cost.

**Evidence:**
`docs/plans/epistemic-repair/p1/epistemic-repair-p1-real-smoke-v1.json`;
`scripts/run_epistemic_repair_p1_real_smoke.py`.

**Effect:** HG-13 behavior is correct for this provider failure, but the clean
real-provider success and clean-batch latency criteria remain unproven. Do not
retry this smoke until provider balance or credentials change.

### 2026-07-17 — LOG-014 — Receipt schema is proven on PostgreSQL

**Type:** observed

Migration 0224 was applied to the local development PostgreSQL database. A
transactional integration test proved insert, identical replay, guarded
conflict rejection, logical-to-attempt linkage, and rollback cleanup. Schema
drift checking still reports one unrelated pre-existing column,
`model_edges.source_relation_instance_id`.

**Evidence:** `tests/epistemic_repair/p1/test_llm_receipt_postgres.py`.

**Effect:** basic receipt durability is proven on PostgreSQL. Semantic writes
and receipts are now ordered in one transaction; the outside replay is a
durability assertion rather than the first write.

### 2026-07-17 — LOG-015 — Static migration contracts missed executable SQL

**Type:** observed + corrected

Migration 0225 passed twelve static schema tests but failed on PostgreSQL
because `natural` was used as an unquoted column name. After correcting it to
`natural_text`, runtime admission exposed a second issue: globally unique
evidence-reference IDs prevented the same immutable citation from carrying
forward to a new Model version. The primary key is now version-bound.

**Evidence:** `db/migrations/0225_epistemic_truth_kernel.sql`;
`tests/epistemic_repair/p2/test_truth_kernel_postgres.py`.

**Effect:** schema token tests are not migration proof. Every P2 migration must
be applied and replayed on PostgreSQL, then exercised through the actual
repository adapter before its schema lane is considered validated.

### 2026-07-17 — LOG-016 — Canonical truth and legacy payload are now asymmetric

**Type:** decided

Immutable truth versions deliberately do not contain embeddings, activation,
or the full historical `ModelRow` payload. Existing retrieval still needs that
shape. Admission now creates a zero-embedding compatibility projection in the
legacy `models` table in the same transaction, while accepted membership and
lifecycle come only from truth heads/views. Unadmitted legacy rows are
therefore unreadable through the cut-over pathways.

**Evidence:** `services/domain/truth_kernel/repository.py`;
`services/domain/models/read_shapes.py`;
`tests/epistemic_repair/p2/test_accepted_truth_reader_cutover.py`.

**Effect:** this restores end-to-end readability without making embeddings or
retrieval activity canonical semantics. The zero embedding is a temporary
derived placeholder and must be replaced by the normal embedding projector;
it is not evidence and cannot affect admission or confidence.

### 2026-07-17 — LOG-017 — Source references are part of conversational context

**Type:** observed + corrected

The first P3 grounding evaluator exposed that reply topology alone cannot
recover the antecedent for email, document, and cross-source references. The
production grounding episode now selects authorized `source_reference`
context alongside temporal and reply context.

**Evidence:** `services/domain/entity_grounding/episode.py`;
`tests/epistemic_repair/p3/`.

**Effect:** the sealed 120-signal P3 population now has complete sufficient-
context recall with zero cross-tenant or future-evidence contamination. This
is a production context-selection rule, not a benchmark-specific hook.

### 2026-07-17 — LOG-018 — Online learning requires an explicit causal barrier

**Type:** observed + decided

Queue drain and eventual projection refresh do not prove that batch N learning
was available to batch N+1 reasoning. P4 introduced a durable barrier plus
decision-level context and outcome links, and the six-batch evaluator checks
the exact selected evidence, actual use, outcome, and policy credit chain.

**Evidence:** `db/migrations/0229_company_learning_causal_barrier.sql`;
`services/domain/company_learning/barrier.py`;
`services/reasoning/think/company_learning_feedback.py`.

**Effect:** every batch must close with zero truth-critical pending work before
the next batch can claim online learning. Activity counts cannot substitute for
causal attribution.

### 2026-07-17 — LOG-019 — An evaluator must bind its verdict to queried rows

**Type:** observed + corrected

The first P5 oracle could be handed a plausible artifact containing a forged
accepted-state boolean. Adversarial tests demonstrated that digesting an
artifact is insufficient when the fields inside it are not independently
reconstructed from durable state.

**Evidence:** `lib/evaluation/epistemic_repair/p5_oracles.py`;
`tests/epistemic_repair/p5/test_p5_oracle_adversarial.py`.

**Effect:** the P5 oracle now binds all 75 sealed signal identities, positions,
and digests to queried observations and context-decision fates and rejects
missing, duplicated, stale, or fabricated state.

### 2026-07-17 — LOG-020 — Deterministic vertical success is not provider parity

**Type:** observed + decided

The three-batch zero-seed vertical processes 75 signals through grounding,
source semantics, admission, accepted-memory retrieval, relation lifecycle,
correction, dependent repair, decision credit, and causal barriers. Its seven
focused tests pass on a freshly migrated PostgreSQL database.

**Evidence:** commit `096a4812`; `scripts/run_epistemic_repair_p5_vertical.py`;
`tests/epistemic_repair/p5/`.

**Effect:** this is sufficient to continue simulated P6-P8 work without waiting
for provider funding. It is not evidence of real-provider semantic parity,
which remains a visible P1/P5/P9 proof boundary.

## 13. Entry Template

Copy this section for every new learning:

```markdown
### YYYY-MM-DD — LOG-NNN — Short title

**Type:** observed | inferred | hypothesized | decided | corrected

**Work package / commit:**

**What happened:**

**Evidence:**

**Interpretation:**

**Decision or next test:**

**Coordinator impact:** none | describe exact versioned change

**Edge cases added:** IDs or none
```
