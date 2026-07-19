# Company-Learning Epistemic Repair — Learning Log

**Document type:** Chronological working scratchpad, current-state mirror, and
evidence index

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

- Preserve chronological observations and failed-run entries. Current snapshot,
  ledger, hypothesis, and decision tables must be reconciled in place when
  their state changes; do not leave a stale checkpoint merely to stay
  append-only.
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
| Latest verified coordination checkpoint | CF3-B green for tenant `e188354c-4a88-406d-bf25-f005cf9af275`: two batches in `227.633s`, B1 `14/14`, B2 selected 14 with 12 authorized/material trace-and-durable effects, two barriers, 12 receipts, no failed gates |
| Repair implementation | Provenance-bound exact semantic scope bridges substrate-normalized entity scope; arbitrary proposition scope strings remain unauthorized; strict CF3-B evaluator is green |
| Current phase | CF3-A and CF3-B are green; the provenance-bound exact-scope join produced 12 authorized/material applied effects in a fresh provider run; CF3-C is unlocked |
| Historical large run | `autonomous-learning-cold-start-45-be401f25`; 45 batches x 25 signals = 1,125 signals |
| Historical large-run verdict | `not_credible` for system/product proof |
| Historical run role now | Immutable forensic baseline; not a benchmark to optimize or rerender into new semantic proof |
| Current execution boundary | Begins with normalized, source-attributed signals already persisted in PostgreSQL |
| Explicitly excluded | Connectors/listeners, OAuth/webhooks, task autonomy, external consequential action, second 45-batch run |
| P0-P5 evidence state | Strict raw-member P9 regeneration/sidecar paths ready; current-release artifacts still require clean regeneration |
| P6 evidence state | Historical 300-signal failure preserved; Run 13 closes the bounded four-batch CF2 single-execution proof, but does not establish the full twelve-batch P6 claim or determinism |
| P7 evidence state | Historical run preserved as insufficient; strict sidecar plus clean-worktree CLI lock ready, not launched |
| P8 evidence state | Strict sidecar and repeated warm-pair plan ready; historical scale ratio remains red |
| P9 evidence state | Manifest and independent reviewer-reproduction contract ready; no final manifest sealed |
| Highest-priority outcome | Execute CF3-C without broadening the now-green CF3-B seam; preserve strict provenance and keep temporal deduplication deferred |

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
| DEC-006 | 2026-07-17 | Treat versioned N-ary relation instances as canonical business relations and binary edges as projections. | accepted and mechanically cut over | P2 truth kernel, accepted-current readers, projector boundaries, and reader-cutover ratchet. |
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
| P1 blindness/observability/retry | regeneration lane | clean release commit required | sidecar ready; regenerate | `db44386d` | Real economics require durable `reported` Codex usage; summaries alone cannot normalize. |
| P2 truth/evidence/lifecycle/relation | regeneration lane | clean release commit required | sidecar ready; regenerate | `db44386d`; reader ratchet `fdb6796d` | Five readers legitimately used the governed shared read shape; scanner now recognizes it without permitting raw `models`. |
| P3 boundary/context/entity | regeneration lane | P2 current artifact | sidecar ready; regenerate | `b057a20e`, `5a7a30ce` | Eligible raw probe denominators are retained. |
| P4 online barrier/retrieval/feedback | regeneration lane | P2/P3 current artifacts | sidecar ready; regenerate | `e476f9fa` | Bounded causal evidence cannot substitute for P6. |
| P5 three-batch vertical | regeneration lane | P1-P4 current artifacts | sidecar ready; regenerate | `ffaf1341` | Deterministic vertical remains bounded proof. |
| P6 12-batch mixed stream | isolated execution owner | pinned clean worktree | active diagnostic/decisive run | `5382f2da`, `fdb6796d`, `73fc8059` | Raw run must pass barrier reopen, complete member extraction, independent oracle, and strict normalization. |
| P7 matched memory ablation | waiting owner | P6 exit artifact | locked runner and sidecar ready; not launched | `ba800d97`; historical `f8375cdf` | Requires CLI transport, reported usage, exact raw gate members, and exclusive run lock. |
| P8 fault/scale/characterization | waiting owner | exclusive DB after P6/P7 | strict sidecar and warm-pair rerun ready | `345eb31c`, `f594cc16` | Historical fault evidence is bounded; scale remains red until repeated paired diagnosis. |
| P9 bounded release decision | release coordinator + independent reviewer | current P0-P8 artifacts | contract ready; manifest unsealed | `63809479`, `ca850161` | Exact artifact sets, digests, evidence classes, and reviewer receipt are nonoptional. |

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

### 2026-07-18 — LOG-021 — Codex CLI replaces the funded-provider dependency

**Type:** observed + corrected

The original real-provider smoke was coupled to DeepSeek and stopped at HTTP
402. The repository already had a first-class Codex provider using local
subscription authentication. The configured `gpt-5.6-terra` required a newer
CLI than the installed `0.142.4`; pinning the documented `gpt-5.4` workflow
model made the direct CLI transport succeed.

**Evidence:** `/tmp/epistemic-repair-p1-complete-codex.json`; commit
`d5a43734`; `scripts/run_epistemic_repair_p1_real_smoke.py`.

**Effect:** P1 now combines the deterministic two-batch reconciliation, one
clean 10-signal Codex batch, and the exact provider receipts persisted,
reopened through a new PostgreSQL connection, and replayed idempotently. The
provider blocker is removed. Wrapper receipts still cannot claim visibility
inside opaque Codex service retries.

**Permanent policy:** all subsequent real-model qualification uses only the
Codex CLI receipt path. The earlier provider is retained here solely as
historical failure evidence and is not an allowed fallback.

### 2026-07-18 — LOG-022 — Reference vectors cannot qualify fault or scale behavior

**Type:** observed + corrected

The first P8 implementation generated plausible fault and scale distributions
from sealed formulas. Those values are useful evaluator reference vectors but
are not production execution evidence and were initially at risk of being
labeled deterministic qualification.

**Evidence:** `lib/evaluation/epistemic_repair/p8_oracles.py`;
`lib/evaluation/epistemic_repair/p8_postgres_runner.py`; commit `521b46b8`.

**Effect:** P8 now fails closed unless production evidence binds queried
durable state. Five of twelve fault boundaries have ten genuine PostgreSQL
normal/duplicate executions; the remaining seven boundaries, scale matrix,
contention, and provider canaries remain explicit rather than inferred.

### 2026-07-18 — LOG-023 — Legacy Model writes made learned memory invisible

**Type:** observed + corrected

A genuine 25-signal production Think batch retrieved context, generated two
claims, passed validation, and committed two state changes. Both writes landed
only in legacy `models`; `model_truth_versions` and
`accepted_current_models` remained empty. The next batch therefore retrieved
zero learned Models exactly as the accepted-reader boundary requires.

**Evidence:** `/tmp/p6-think-smoke-6.json`; `services/reasoning/think/applier.py`;
`services/reasoning/think/truth_admission.py`.

**Effect:** P2 reader enforcement exposed the missing writer cutover. Think
claim insertion must compile a governed truth command with exact claim-local
observation evidence; the truth kernel, not a parallel legacy insert, owns the
compatibility projection.

### 2026-07-18 — LOG-024 — Batch-wide evidence defaulting recreates contamination

**Type:** observed + decided

The legacy applier filled missing claim evidence by merging every trigger
observation into every claim. In a 25-signal batch this makes unrelated signals
appear to support each claim and recreates the original batch-scope failure
inside the new truth kernel.

**Evidence:** `services/reasoning/think/applier.py::_with_claim_evidence_defaults`;
live P6 admission failure.

**Effect:** multi-signal claims must carry explicit claim-local observation
IDs and fail closed when they do not. Trigger fallback is permitted only for a
single normalized observation. A passing admission count is not success if its
lineage is batch-wide.

### 2026-07-18 — LOG-025 — Historical adaptive run did not earn complexity

**Type:** observed

The clean P7 run executed 3 worlds x 5 arms x 3 stages with 45 successful
Codex calls and 45 durable attempt receipts. Adaptive mature thesis-facet
completeness was 0.1667 versus 0.25 for observation-only; adaptive atomic F1
was 0.1267 and direct-thesis accuracy was 0. All paired facet-lift intervals
included zero or were negative. All three injected corrupted Models remained
active beyond two batches.

**Evidence:** `/tmp/epistemic-repair-p7-real-provider-clean-v1.json`; commit
`f8375cdf`.

**Effect:** memory did not earn its role in this historical run. The artifact remains
`insufficient_evidence` because corrupted-memory safety is red, and the runner
also revealed that provider answers were evaluated without applying adaptive
memory evolution between stages. P7 must exercise the real mutation/lifecycle
loop before choosing the strategic fork. This result remains falsifying
diagnostic evidence; it is not the current P7 exit artifact and may not be
silently replaced by the readiness of the new runner.

### 2026-07-18 — LOG-026 — Fault correctness passed while scale efficiency failed

**Type:** observed

P8 bound all 12 required fault boundaries across normal and duplicate delivery
using 18 PostgreSQL restart executions and six durable Codex provider receipt
reads. The 27-cell scale matrix executed 358,020 observation writes and 12,636
barriers. Literal cloned-database isolation was proven separately. Repeated
isolated measurements still exceeded the concurrency ratio gate; pool wait was
negligible and database execution dominated.

**Evidence:** `/tmp/p8-production-fault-evidence.json`;
`/tmp/p8-postgres-scale-matrix.json`; `/tmp/p8-isolated-warm-pair.json`;
commits `ee3a7ec6`, `33a1d7a0`, and `a0470516`.

**Effect:** P8 fault/restart correctness is green, but scale qualification is
not. Absolute latencies are small, yet the preregistered concurrency ratio may
not be waived or diluted. Queue-family, projector-refresh, and exact token
measurement also remain fail-closed until their real pipelines execute.

### 2026-07-18 — LOG-027 — Gold must not contradict authenticated runtime evidence

**Type:** observed + corrected

The first 1,200-observation Slack boundary characterization scored 49 false
merges because topic-drift gold silently moved messages into episode N+1 while
their visible text and authenticated thread/object metadata still identified
episode N. A label-blind production system could only satisfy that gold by
ignoring stronger runtime evidence or learning the evaluator's hidden labels.

**Evidence:** `/tmp/p8-component-characterization-invalid-runtime-gold-contradiction.json`;
`/tmp/p8-component-characterization-db-v2.json`; commit `baf821ac`.

**Effect:** the contradictory run remains historical falsifying evaluator
evidence. Population v2 adds ordinary explicit topic/object cues and a generic
production projection in which topology is strong evidence but not an
unbreakable boundary. The corrected sealed run scores 1.0 B-cubed F1 over all
1,200 observations and every registered slice, with zero false merges. Future
gold changes require runtime/gold consistency and gold-mutation-invariance
tests before they can qualify production behavior.

### 2026-07-18 — LOG-028 — One configured model does not imply one executed model

**Type:** observed + corrected

P6 and P7 were configured with Codex `gpt-5.4`, but production question
planning independently selected `gpt-5.3-codex-spark`. A provider preflight
and a manifest that named only the main model therefore described a mixed-model
run as single-configuration evidence.

**Evidence:** `/tmp/epistemic-repair-p7-production-mixed-model-invalid-v1.json`;
P6 physical attempt receipts; commit `092f4843`.

**Effect:** mixed attempts are preserved but invalid for semantic comparison.
P6/P7 now pin question planning and fallback to the main provider, reconcile
every logical receipt to every physical attempt after execution, and fail on
missing or mismatched provider/model/purpose identity. Configuration equality
is not evidence; the durable attempt ledger is.

### 2026-07-18 — LOG-029 — Codex CLI exposes exact terminal token usage

**Type:** observed + corrected

The P8 provider runner previously wrote zero token counts with
`usage_exactness=unavailable`. Raw Codex JSONL includes exact usage on the
`turn.completed` event, including total input, cached input, output, and
reasoning output tokens.

**Evidence:** `/tmp/p8-codex-canary.jsonl` SHA-256
`f5a7f8626b186a1ea5f5aa887b3c6311ded663e477826ecd7362365b04eb819d`;
commits `2d4713e4` and `acab46ab`.

**Effect:** successful terminal attempts persist only provider-reported usage;
timeouts without a terminal event remain explicitly unavailable. The observed
canary reported 16,711 input, 16,256 cached input, 21 output, and 14 reasoning
output tokens. Evaluators must parse the provider's terminal event rather than
infer cost from prompts or substitute zeros.

### 2026-07-18 — LOG-030 — Admission cut-over exposes every stale accepted reader

**Type:** observed + corrected

After Think began admitting canonical Models, batch-two retrieval found both
accepted heads, but downstream dynamics failed because it queried legacy
payload columns directly from the membership-oriented
`accepted_current_models` view. Earlier SAGE, deterministic, retrieval-action,
and reconciler readers had the same shape assumption.

**Evidence:** `/tmp/p6-think-2batch-governed.json`;
`services/domain/models/read_shapes.py`;
`services/reasoning/dynamics/detectors.py`; commit `12a70d85`.

**Effect:** accepted membership and compatibility payload are distinct read
concerns. Consumers requiring the ModelRow shape must use the canonical
accepted-row adapter, while membership/lifecycle checks may use truth heads or
views directly. Each newly admitted production batch is also a cut-over probe
for stale readers; a passing insert test alone cannot prove the reader graph.

### 2026-07-18 — LOG-031 — Confidence is guarded as truth but absent from truth versions

**Type:** observed + decided

The first single-model two-batch P6 run admitted two Models and then retrieved
and referenced both in batch two. Apply failed when a confidence update reached
legacy SQL: migration 0225 correctly guards confidence as accepted semantics,
but immutable `ModelVersion` does not contain confidence. Smuggling it into a
reserved proposition metadata block would hide the mismatch and corrupt the
claim representation.

**Evidence:** `/tmp/p6-think-2batch-single-model.json`; migration
`0225_epistemic_truth_kernel.sql`; `lib/contracts/truth_admission.py`;
commit `12a70d85`.

**Effect:** accepted semantic updates must use a truth-kernel advance. Semantic
confidence should become a first-class immutable version field with compatible
projection, digest, fencing, and idempotency behavior. Retrieval counters and
activation remain activity sidecars. The failed batch-two result proves
model-first retrieval but is not terminal semantic evidence and cannot be
scored as a successful batch.

### 2026-07-18 — LOG-032 — P8 latency red mixes cold statements with real concurrent tail spikes

**Type:** observed + hypothesized

**Work package / commit:** P8 saved scale artifact
`/tmp/p8-isolated-27-matrix-head-v3.json`; no runtime change made.

**What happened:** The declared concurrency gate compared the per-cell p95 of
`CompanyLearningBarrierService.complete` at tenant concurrency 20 versus 1.
Its maximum was 3.829790 for batch size 10, horizon 12: 16.1965 ms versus
4.2291 ms. In both arms, batch one was the slowest/cold barrier. Because a
12-sample t1 cell puts one sample inside the p95 denominator, cold latency
becomes the t1 p95; longer horizons dilute that same cold sample below p95.

**Evidence:** The 10×12×1 first barrier was 4.2291 ms and its steady barriers
were at most 0.7500 ms. Across 10×12×20 tenants, batch-one median/max were
16.9344/21.9045 ms; excluding batch one reduced p95 to 1.8469 ms. However,
exclusion is not a valid fix: 25×12×20 had later batch-4 through batch-9 tail
spikes of 10.6308–14.4785 ms. Excluding one, two, or three initial batches made
the maximum concurrency ratio 12.6094, 12.6275, and 12.7081 respectively.
Every recorded barrier used exactly one counted SQL call. Retrieval and write
timings also rose under concurrency, and pool acquisition was outside the
barrier timer.

**Interpretation:** The exact reported 3.83 value is denominator-sensitive and
substantially cold-start-shaped, but the red state is not merely cold-start
noise. Later t20 outliers show shared PostgreSQL execution, index/page, or host
scheduling contention. The saved one-pass matrix cannot distinguish these.
Changing p95 to median, dropping batch one, or relaxing the 2× threshold would
hide observed tail behavior and is not justified.

**Decision or next test:** Keep the gate red and preserve both cold and steady
latency. In the next exclusively locked rerun, execute at least five warm
paired repetitions per selected t1/t20 comparison, alternate arm order, and
report separately: connection/pool wait; tenant bootstrap/admission; first
barrier; steady barrier p50/p95/p99 after an explicit unscored warmup; write
and retrieval latency; SQL-call count; cell wall time; and server-side
`pg_stat_statements` execution time if available. First diagnose the 25×12
pair, then one long-horizon control. A structural optimization is authorized
only if the repeated server-side measurements localize a stable operation;
otherwise classify the threshold as an environment-capacity SLO rather than
an algorithmic-complexity result.

**Coordinator impact:** The safe P8 coordinator must retain exclusive DB
ownership for this diagnostic and must not run it beside P6/P7 or provider
work. The 27-cell exit remains fail-closed.

**Edge cases added:** short-horizon percentile denominator; cold prepared
statement/table-page path; later concurrent barrier tail spikes; arm-order and
host-load confounding.

### 2026-07-18 — LOG-033 — A live run is evidence only when its proof harness is causal

**Type:** observed + corrected

**Work package / commit:** P6 production Think; `a81045b5`, `8cfdf62a`,
`fb5402e3`, `b2ce3138`, `ec0bb334`, and `5382f2da`.

**What happened:** The original live runner reported a generic pending-work
count but never completed a durable company-learning barrier. A long run from
the shared agent worktree was also vulnerable to code changing beneath it.
After causal barriers were added, downstream repair processing exposed three
accepted-model readers that still expected legacy columns from the truth-only
view. The initial 300-second outer batch deadline was identical to the worker
attempt timeout, preventing the established one-retry owner from running.
Finally, Codex CLI receipts estimated tokens even though `codex exec --json`
reports exact turn usage.

**Evidence:** `/tmp/p6-think-invalid-no-barrier-partial.json`;
`/tmp/p6-think-1batch-barrier-smoke-ec0bb334.json`; failed pinned smoke
exceptions for `proposition_kind`; `/tmp/p6-think-12batch-pinned-ec0bb334.json`.

**Interpretation:** Production-shaped execution alone is not proof. The runner
must have a stable code identity, drain truth-changing work, atomically fence
exact accepted versions, reopen the receipt, preserve retry ownership, and
record provider-reported economics. Compatibility adapters must cover every
reader of fields not yet present in immutable truth versions.

**Decision or next test:** The detached clean-worktree, 300-second attempt,
650-second batch deadline, exact barrier reopen, reported CLI usage, and
post-freeze extraction/scoring requirements have now been implemented. A
clean diagnostic/decisive run is active. Its existence does not change the
status until the frozen artifact and sidecar reopen successfully.

**Coordinator impact:** P6 raw runs are frozen execution artifacts, never exit
artifacts. The independent scorer remains fail-closed until boundary, signal
fate, retrieval opportunity, scope, lifecycle, relation, calibration, and
contamination evidence is complete.

**Edge cases added:** missing barrier invocation; shared-worktree drift; stale
accepted readers; retry budget shadowing; estimated usage despite reported
provider telemetry.

### 2026-07-18 — LOG-034 — Normalization readiness is separate from current evidence

**Type:** observed + decided

**Work packages / commits:** P0-P2 `db44386d`; P3 `b057a20e` and
`5a7a30ce`; P4 `e476f9fa`; P5 `ffaf1341`; P6 `73fc8059`; P7
`ba800d97`; P8 `f594cc16`; P9 `63809479` and `ca850161`.

**What changed:** Each phase now has, or is connected to, a strict path that
retains raw member contributions, preregistered gate and metric sets, full
commit provenance, and content digests. P6's independent evaluator now scores
batch-level retrieval decisions rather than fabricating target-by-context
Cartesian rows. P7's oracle derives every hard gate from raw members and its
runner refuses dirty or non-CLI execution under an exclusive lock. P8 has a
strict sidecar and preregistered repeated warm-pair diagnostic. P9 rejects
stale summaries, mixed commits, malformed metrics, and coordinator-authored
review receipts.

**Failed evidence preserved:** the no-barrier and timed-out P6 runs, the P6
reader-shape failures, the historical P7 run without adaptive lifecycle, the
mixed-model attempt ledger, the contradictory P8 gold run, and the red P8
scale ratio remain diagnostic evidence. New code or a passing bounded test
does not rewrite their verdicts.

**Decision:** distinguish three states everywhere: `runner/sidecar ready`,
`current artifact regenerated`, and `phase exit proven`. P0-P5 are currently
in the first state for P9 assembly; P6 is executing toward the second and
third; P7 and the P8 warm-pair rerun wait behind the isolated execution lock;
P9 remains unsealed until one release commit supplies every required artifact
and an independent reviewer reproduces the result.

**Proof boundary:** no customer value, connector transport, task autonomy, or
large open-world semantic guarantee follows from these harnesses. Reported
Codex usage is required for economics; estimated or unavailable usage remains
visible but cannot qualify it.

### 2026-07-18 — LOG-035 — Transport batches are not semantic scope

**Type:** observed + corrected

**Work package / commit:** P6 semantic repair; `4cf00cd2`, `3c968f03`,
`d919a9d6`, and `add17c4c`.

**What happened:** The first mixed-stream diagnostic mechanically learned and
retrieved Models, but its early Models described source cadence, generic
curiosity, and the 25-signal batch wrapper instead of company mechanisms.
Those Models then became attractive retrieval anchors and could merge across
unrelated workstreams. The failure originated before truth admission: batch-
wide deterministic fallbacks and missing claim-local coordinates supplied
formally valid but semantically incoherent evidence.

**Evidence:** the immutable incomplete diagnostic
`/tmp/p6-think-12batch-fdb6796d.json`; focused representation, reconciliation,
quality, and admission regressions; 34 combined representation/reconciliation
tests passing with eight database integrations skipped because this worktree
has no exported `DATABASE_URL`.

**Interpretation:** A scheduling batch must never become an epistemic unit.
Canonical claims need exact local evidence and positive business or episode
coordinates. Source identity, arrival proximity, and wrapper-wide curiosity
are not sufficient evidence of a shared company mechanism.

**Decision or next test:** Preserve exactly one 25-signal T1 transport batch,
but partition claim evidence internally by typed entity, episode/thread, or a
named business phrase. Reject generic/source-only and high-entropy claims at
representation, reconciliation, quality, and truth-admission boundaries. Run
one clean 25-signal Codex smoke before paying for another 12-batch proof.

**Coordinator impact:** P6 cannot advance merely because retrieval shifts
from Observations to Models. The retrieved Models must be business-scoped,
claim-local, and independently entailed by their recorded evidence.

**Edge cases added:** mixed workstreams in one transport batch; noise sharing a
source system; hallucinated batch event IDs; source-only merge keys; wrapper
primary observation used as evidence.

### 2026-07-18 — LOG-036 — Positive formation must begin before the final claim

**Type:** observed + corrected

**Work package / commit:** P6 one-batch semantic smoke; `af4e84bb` and
`40078761`.

**What happened:** The first repinned smoke stopped before Codex because the
runner used `ON CONFLICT (id)` against the partitioned observation key. After
correcting it to `(id, occurred_at)`, Codex produced claims but truth admission
correctly rejected missing claim-local evidence. Per-claim quarantine then let
the same 25-signal batch finish with a durable barrier and zero accepted
Models. The preserved response showed the upstream cause: inquiry compiled a
single `MDC_H2` claim that “multiple active work items in this batch” lacked
owners, marked it `about=batch`, and attached the first twelve batch member
IDs. The quality gate correctly rejected it as `claim_scope_batch_wrapper`.

**Evidence:** `/tmp/p6-think-1batch-semantic-af4e84bb.json` is a failed safety
diagnostic; `/tmp/p6-think-1batch-semantic-40078761.json` is a complete
zero-Model safety smoke. The latter used two successful Codex calls for the
main run, then completed repair work, preserved response/validation/apply
artifacts, and wrote no unsupported canonical truth.

**Interpretation:** Downstream evidence narrowing cannot rescue a hypothesis
whose subject is already the transport batch. Positive formation must start
where inquiry creates hypotheses and memory-decision candidates. Each
candidate needs a typed business scope plus its exact item-level observation
IDs and bodies before the single final Codex call.

**Decision or next test:** Compile workstream/episode-local candidates inside
the one 25-signal transport batch; never prepend the batch trigger or all
member IDs; expose exact candidate-local ID/body manifests; keep one main
Codex call; suppress the generic batch hypothesis when local material coverage
exists. The next smoke must form useful local Models without noise or
cross-workstream evidence.

**Coordinator impact:** A complete barrier with zero Models proves safe
abstention, not company learning. P6 semantic readiness requires both zero
contamination and positive, workstream-local Model formation.

**Edge cases added:** partitioned observation conflict target; unsupported
claim rolling back siblings; first-twelve batch ID inheritance; `about=batch`;
batch-level ownership aggregation; successful zero-Model barrier.

### 2026-07-18 — LOG-037 — Claim locality must survive every transformation seam

**Type:** observed + corrected

**Work package / commit:** P6 semantic formation; `b70b3b11`, `6e84b32f`,
`31bb8b3e`, and `a9877010`.

**What happened:** Closed one-observation candidates were correct at formation,
but same-scope reconciliation and a second evidence-defaulting pass resurrected
sibling evidence. Strict canonical manifest authorization rejected both runs
before truth was written. After repairing the complete compiler -> splitter ->
reconciler -> apply -> truth path, a clean 25-signal Codex smoke accepted 11
Models with one exact observation each and no noise truth. A final audit found
group-wide contextual metadata even though canonical evidence was local; that
metadata is now rebuilt from the same singleton manifest.

**Evidence:** `/tmp/p6-think-1batch-closed-b70b3b11.json` and
`/tmp/p6-think-1batch-closed-6e84b32f.json` are immutable failed safety
diagnostics. `/tmp/p6-think-1batch-closed-31bb8b3e.json` completed with atomic
precision 1.0, recall 0.9167, F1 0.9565, scope precision/recall 1.0, and zero
noise-derived Models or relations after population-scoped rescoring.

**Interpretation:** Claim-local evidence is an end-to-end invariant, not a
compiler property. IDs, bodies, semantic evidence counts, contextual frames,
watch selectors, and source channels must all remain aligned through every
mutation seam. Canonical admission must continue to fail closed on any drift.

**Decision or next test:** Keep compiler-authored closed assertions immutable;
let the model accept or reject but not rewrite them. Require full-path DB tests
for evidence locality before any provider rerun. Repin the next smoke after
population-v3 mention instrumentation, then freeze the full P6 release commit.

**Coordinator impact:** P6 now has a positive semantic readiness gate rather
than treating transport completion or evidence-row existence as success.

**Edge cases added:** same-scope distinct atomics; stale proposition evidence;
post-split evidence resurrection; semantic metadata wider than canonical refs;
same assertion confirmed by a later observation.

### 2026-07-18 — LOG-038 — Entity existence, identity, and relevance are different

**Type:** observed + corrected

**Work package / commit:** P6 perception/evaluation; `4655c86a`, `32db1800`.

**What happened:** The first mention report exposed useful surfaces but lost
their persisted nested spans and type assessments, while the evaluator treated
`Facilities` and `Beacon office ticket` as false mentions because their signals
were distractors. That would reward erasing legitimate local company entities.

**Evidence:** The population-v2 smoke produced correct storyline surfaces plus
local distractor surfaces, but reported null spans/types/fates. Repository
inspection proved span and type live in candidate-plane detection structures;
canonical resolution did not yet exist. Population v3 now distinguishes
required storyline mentions, optional organizational units, required local
work items, identity-link correctness, and zero truth contamination.

**Interpretation:** Extraction answers whether text names an entity; resolution
answers which entity it is; relevance answers whether it matters to the current
storyline; truth admission answers whether it supports a durable Model. These
must never be collapsed into one binary label.

**Decision or next test:** Persist provisional extraction coordinates and
explicit unresolved fate, report them without fabricating canonical resolution,
and require local distractor entities to remain distinct from storyline refs.

**Coordinator impact:** P6 population advanced to v3, intentionally
invalidating prior digest-bound artifacts and requiring a fresh smoke.

**Edge cases added:** optional organizational-unit mention; named local ticket;
same tokens as a storyline but distinct identity; provisional versus resolved
coordinate; rejected detection must not count as a positive mention fate.

### 2026-07-18 — LOG-039 — Population-v3 smoke clears decisive P6 pre-run gate

**Type:** observed + corrected + decided

**Work package / commit:** P6 population-v3 smoke and independent audit;
`19219aa8`, `c7d936a6`, `4a67493b`, `bd17c6f8`, `b946f10c`, `0d703818`,
`36ccb3f8`.

**What happened:** The first population-v3 run,
`/tmp/p6-think-1batch-popv3-19219aa8.json`, failed closed after fourteen
`no_match` reconciliation decisions. Candidate 15,
`MDC_ATOM_cobalt_renewal_e5cd0847-2dad-5a63-832c-3dfa1a20f9e4`, authorized
only observation `e5cd0847-2dad-5a63-832c-3dfa1a20f9e4` but arrived at
admission with four foreign Cobalt siblings. No truth was committed. Forensics
identified the remaining widening seam: semantic representation was resolved
from same-scope observations before the singleton compiler manifest dominated
synthetic/default evidence.

The repair sequence routed questions and unresolved references outside truth
formation (`c7d936a6`), prevented downgrade paths from advancing accepted truth
or unioning transport evidence (`4a67493b`), made closed-atomic representation
manifest-first (`bd17c6f8`), and expanded uncertainty recognition beyond the
fixture's exact wording (`b946f10c`). The clean repinned run at `0d703818`,
`/tmp/p6-think-1batch-popv3-0d703818.json`, completed one 25-signal batch and
accepted exactly the twelve intended atomics: three each for Atlas, Beacon,
Cobalt, and Delta. Every accepted claim had one exact supporting observation,
one exact evidence signal, correct provisional scope, and no cross-storyline or
distractor lineage. Eight nonassertable storyline signals were retained as
four open questions and four clarification residuals; five distractors produced
no canonical mutation.

A post-freeze claim-by-claim audit then found a defect hidden by the headline
lineage score. Four dashboard claims had correct canonical singleton evidence
and `evidence_event_ids`, but their derived proposition metadata still said
`evidence_status=needs_evidence`, `supporting_event_count=0`, and carried empty
contextual-frame observation/source coordinates. `36ccb3f8` now normalizes
those fields from the final authorized manifest and adds the fail-closed
`semantic_evidence_metadata_coherent` evaluator gate.

**Evidence:** The successful frozen score reported atomic precision 1.0,
recall 1.0, F1 1.0; evidence-lineage coverage 1.0; scope precision and recall
1.0; exact mention F1 1.0; entity-type accuracy 1.0; canonical-link precision
and recall 1.0; uncertainty-fate precision and coverage 1.0; and zero false
Models or relations from noise. Twenty storyline mention coordinates were
correct, the two local work items remained distinct, and optional `Facilities`
remained unresolved without entering scope or truth. The runner recorded two
successful exact-usage Codex receipts: question planning used 15,327 input,
1,036 output, and 10,624 cache tokens; main reasoning used 18,731 input, 2,854
output, and 10,112 cache tokens. Focused post-audit validation passed 32 unit
tests plus the manifest-bound PostgreSQL authority test; five unrelated DB
tests were skipped when `DATABASE_URL` was absent in the unit lane.

**Interpretation:** The smoke proves cold-start positive formation without
weakening authority: exact atomic evidence, uncertainty separation, typed
provisional entity coordinates, correct local-entity distinction, and zero
noise truth. Zero relations are expected in batch one because the cold-start
decision has no prior accepted endpoints and these candidates are singleton
atomics; relation obligations require two bound Model endpoints and should
emerge only as later batches establish and revisit the storylines. Provisional
canonical-link scores prove coordinate consistency, not completed entity
resolution.

Headline 1.0 metrics are not a P6 exit. Boundary B-cubed F1 was 0.8247 against
the 0.9 threshold. Direct thesis accuracy and mean thesis-facet completeness
were zero because one batch cannot yet synthesize the four temporal theses.
Relation, lifecycle, mature retrieval, historical reopening, latency-tail,
barrier, and full-run economics gates remain unmeasured or fail closed until
all twelve batches execute. Durable/exact receipt gates also require receipts
for every decisive run even though both calls in this smoke were reported
exactly.

**Decision or next test:** **GO for decisive P6** on one clean pinned commit
containing `36ccb3f8`. Run all twelve batches without changing the population,
provider configuration, scorer, or evidence extractor. Require the final
artifact to reopen all 300 signal fates, semantic evidence metadata, barriers,
relations, lifecycle transitions, mature retrieval decisions, and exact call
receipts before P6 may pass.

**Coordinator impact:** The population-v3 smoke prerequisite is closed. P6 is
no longer waiting for another one-batch semantic run; it is waiting only for
the decisive pinned 12-batch execution and independent post-freeze scoring.

**Edge cases added:** question paraphrase without punctuation; explicit
ambiguity paraphrase; accepted-truth downgrade with full-batch siblings;
manifest-first semantic partition; canonical evidence correct while derived
evidence metadata is stale; optional local entity with no truth relevance.

### 2026-07-18 — LOG-040 — Complete P6 execution falsified coherent synthesis

**Type:** observed and corrected

**Work package / commit:** P6 12 x 25 diagnostic, focused four-batch trace,
repairs through `c04a0445`.

**What happened:** The first complete P6 run processed 300 signals in 12
batches without a terminal error, but it did not build the required company
theses. It accepted 93 Models: 73 singleton atomics and 20 multi-evidence
Models, several of which mixed storylines or noise. A four-batch follow-up
contained 24 accepted Models, all atomic facts; the synthesis provider chose
`operation=edge` for every synthesis candidate, producing relations without a
synthesis Model.

**Evidence:** Immutable artifacts:
`/tmp/p6-think-12batch-c3c4dc43.json`,
`/tmp/p6-think-12batch-c3c4dc43-evidence.json`,
`/tmp/p6-think-12batch-c3c4dc43-score.json`,
`/tmp/p6-think-4batch-core-a2dd5376.json`,
`/tmp/p6-think-4batch-core-a2dd5376-evidence.json`, and
`/tmp/p6-think-4batch-core-a2dd5376-score.json`. The 12-batch score reported
atomic precision 73/93 (0.7849), recall 45/92 (0.4891), F1 0.6027, direct
thesis accuracy 0/4, and mean thesis-facet completeness 0/4. Execution, fates,
receipts, token reporting, truth barriers, and noise controls passed their
available hard gates; relation joint precision/recall, scope precision/recall,
and refresh duplicate-processing ratio were not measured.

**Interpretation:** Transport completion and atomic activity are not coherent
company learning. Closed-atomic early return initially suppressed synthesis;
after opening that lane, an untyped multi-evidence candidate allowed relation
obligation to dominate. Later absorption could then contaminate multi-evidence
Models. Proposition JSON strings were also an evaluator parsing defect, but
parsing alone could not repair the genuine 0/4 synthesis outcome.

**Decision or next test:** Treat the 12-batch run as immutable failed evidence.
Use focused tests to prove that accepted synthesis creates a scope-local
hypothesis with claim-local evidence, lifecycle probes do not enter truth,
accepted relations replay, and explicit no-write decisions remain no-ops.
Repeat the expensive decisive P6 run only after those focused gates pass.

**Coordinator impact:** P6 changed from GO/active to failed-semantic-repair. P7
and P8 remain gated. Commits `0d703818`, `881a6fac`, `36ccb3f8`, `53f31c7f`,
`3b143e53`, `ae0b8441`, `a2dd5376`, `c479da91`, `b958076a`, `4fe2a338`,
`0128aaf2`, and `c04a0445` are repair readiness, not integrated proof.

**Edge cases added:** closed-atomic synthesis suppression; edge-only accepted
synthesis; relation obligation dominating hypothesis creation; cross-story
absorption; extracted proposition encoded as JSON text; replayed accepted
relation missing from projections; explicit no-write veto mistaken for missing
grounding.

### 2026-07-18 — LOG-041 — Clean four-batch core closes the semantic repair gate

**Type:** observed and corrected

**Work package / commit:** P6 clean four-batch integrated proof; repairs
`1e089459`, `7a268344`, `9ddf2970`, `70247268`, `49c853e0`, and `bf26d622`.

**What happened:** A clean zero-seed run completed four batches and produced 56
atomic Models plus one Atlas composite. No synthesis was created in B2. The
Atlas composite appeared at the first mature conclusion opportunity and bound
only `p6-b04-s09` directly; earlier evidence arrives through its two member
Models rather than through same-scope sibling observations.

**Evidence:** `/tmp/p6-think-4batch-core-bf26d622.json`,
`/tmp/p6-think-4batch-core-bf26d622-evidence.json`, and
`/tmp/p6-think-4batch-core-bf26d622-score.json`. Atomic precision was 56/56,
atomic recall 32/32, and atomic F1 1.0. Uncertainty fate precision and coverage
were 24/24. Evidence-lineage coverage was 57/57. Scope precision and recall
were 4/4. The `zero_seed_canonical_truth` hard gate was true.

**Interpretation:** Closed compiler-authorized atomics now survive downstream
splitting even when their predicate is a status, timestamp corroboration, or
referent clarification. The evaluator separates legitimate composite synthesis
from the atomic denominator without losing its lineage. Synthesis maturity and
claim-local evidence boundaries now behave correctly over the first four
batches.

Zero canonical relations are correct for this prefix: the observed adaptive
support edge remained candidate-only and therefore could not enter governed
relation truth. Lifecycle accuracy is correctly unmeasured because no
correction-phase terminal transition is available. Direct-thesis accuracy and
thesis-facet completeness are also unmeasured because the four-batch prefix has
not executed every preregistered synthesis opportunity.

**Decision or next test:** Preserve these artifacts as the bounded core exit
evidence and proceed to the clean twelve-batch P6 run. The full run must retain
all prefix invariants while proving Beacon, Cobalt, and Delta synthesis,
contradiction/correction/outcome lifecycle, mature retrieval, governed
relations when an opportunity exists, barriers, and complete receipts.

**Coordinator impact:** The focused semantic-repair gate is closed. P6 remains
open only because four batches cannot qualify the twelve-batch phase exit.

**Edge cases added:** `EDGE-038` — candidate-only adaptive support edges must
remain outside canonical relation truth and relation metrics until a governed,
bound relation opportunity is accepted.

## 13. DEFERRED BACKLOG

This ledger is deliberately quarantined from the core milestone. The milestone
starts with normalized signals already stored in PostgreSQL and ends with an
autonomous company-understanding and learning/feedback loop. Agents should log
these items and continue core work unless one directly blocks semantic proof.

### Explicitly outside the current milestone

- Slack/Jira/email listeners and connectors, OAuth, webhooks, polling, delivery
  retries, and transport durability.
- Task or action autonomy and externally consequential actions.
- A second 45-batch run.

### Deferred until core semantic correctness is proven

- Production hardening, high availability, deployment and operational polish.
- Efficiency tuning for latency, tokens, call counts, throughput, and cost.
- Broad framework/schema refactors and non-blocking edge-case fixes.
- Another expensive P6 replay before focused synthesis/lifecycle/relation tests
  are green.
- `EDGE-038`: candidate-only adaptive support edges require separate
  characterization; they must not be promoted or scored as canonical relation
  truth without a governed, bound semantic relation decision.

Evaluator work required to make the eventual core verdict truthful is not
permanently deferred. In particular, relation joint precision/recall, scope
precision/recall, and refresh duplicate-processing measurement must exist for
P6 exit, but should not displace the immediate synthesis repair proof.

## 14. Entry Template

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

### 2026-07-18 — LOG-042 — Decisive P6 preserved as semantic failure before freeze

**Type:** observed and corrected

**Work package / commit:** Full P6 artifact at `bf26d622`; focused production
repairs `77796e77`, `45b0bbad`, `3a903aa8`, `39ea688a`, and `b01e8290`; P7/P8
repairs `bf268c99`, `9771c083`, `6828d168`, `940851a7`, and `32f3ea9e`.

**What happened:** The zero-seed 12-batch run completed all 300 signals and
kept its atomic, uncertainty, scope, and direct-lineage boundaries exact, but
it failed to form an autonomous company model. It produced only one canonical
synthesis Model, for Beacon, rather than one for each of Atlas, Beacon, Cobalt,
and Delta. Lifecycle transitions and canonical semantic relations therefore
did not materialize.

**Evidence:** `/tmp/p6-think-12batch-bf26d622.json`,
`/tmp/p6-think-12batch-bf26d622-evidence.json`, and
`/tmp/p6-think-12batch-bf26d622-score.json`. Complete execution was true.
Atomic claim F1, uncertainty-fate coverage, scope precision, scope recall, and
evidence-lineage coverage were all `1.0`. Direct thesis accuracy was `0/4`;
lifecycle expected-transition accuracy was `0/4`; canonical relation count was
zero. Relation joint precision/recall and refresh duplicate-processing ratio
were unmeasured.

**Interpretation:** This was a production synthesis failure, not an evaluator
or evidence-extraction artifact. Beacon's direct conclusion evidence was
correct, but its member IDs were not persisted as exact Model-version truth
evidence, leaving no transitive phase lineage. Its natural text expressed a
recurring co-occurrence without an explicit dependency marker. The other three
scope conclusions depended on whichever Models ordinary retrieval happened to
select; after Beacon existed, later multi-scope evidence could be reconciled
into it instead of opening separate canonical-scope synthesis. A generic prose
composite cannot anchor lifecycle or governed relations.

**Decision or next test:** Preserve this run permanently as falsifying
evidence. The next expensive P6 execution must start from zero seed on one final
clean commit and reopen exact member-version lineage. It must produce four
scope-local structured theses with explicit supported mechanisms, advance each
through the expected phases, and admit only governed semantic relations. Do not
patch the scorer or relabel the failed artifact.

**Coordinator impact:** The focused repair set is complete enough to justify a
new semantic run, but it is not integrated proof. P7/P8 repair artifacts also
require regeneration on the exact release commit before P9. Efficiency and
refresh behavior remain deliberately deferred and unmeasured.

**Edge cases added:** conclusion scope omitted by selected retrieval; same
display label mapping to multiple canonical refs; cross-scope composite merge;
inactive or stale member head; proposition member ID without immutable
Model-version evidence; vague co-occurrence presented as a mechanism; direct
observation siblings substituted for transitive prior phases.

### 2026-07-18 — LOG-043 — Stop repeated P6 execution and preserve a clean handoff

**Type:** observed, corrected, and decided

**Work package / commit:** P6 preparation through clean commit
`a34533b8e1dca0ca84b40a3dd460f33b477c2f5c`; interrupted execution artifact
`/tmp/p6-think-12batch-a34533b8.json`.

**What happened:** P6 consumed disproportionate goal time because several runs
started before all cheap cross-phase contracts, real-schema paths, evaluator
manifests, and synthesis invariants had converged. Each late correction changed
HEAD, and P9's exact-commit rule then invalidated prior runtime evidence. The
final attempt began only after the complete epistemic-repair suite passed, but
the user explicitly stopped P6 after two successful batches. The process was
terminated and no P6 process remains active.

**Evidence:** The final preflight on `a34533b8` passed `415` epistemic-repair
tests with PostgreSQL enabled. P0 through P5 exact-commit evidence was green.
The interrupted raw artifact records clean exact-commit provenance, two waves,
`completed_batches=2`, and `elapsed_s=422.982`; it is deliberately incomplete
and must not be scored or normalized as P6 release evidence. Batch 1 started
from zero accepted Models and created 12. Batch 2 retrieved accepted Model
context, committed successfully, and raised the total to 24. Earlier failed
artifacts remain diagnostic evidence for the schema, synthesis, relation,
scope-identity, and manifest defects repaired during this run.

**Interpretation:** The dominant failure was execution sequencing, not the
nominal cost of one 12-batch simulation. Expensive runs repeatedly served as
preflight. Mocks missed production SQL/view drift; paired P0/P2 authority
registrations were checked after provider execution began; synthesis producer,
validator, truth-admission, and applier contracts were reviewed serially rather
than frozen together. Frequent restarts obscured milestone progress and made
P6 appear continuously active for hours.

**Decision or next test:** Stop all P6 work now. Do not resume automatically.
If P6 is resumed later, treat `a34533b8` as the handoff baseline and first
verify that HEAD and the coordinator contract are still intended. Run one
single preflight containing: the complete epistemic-repair suite; clean P0-P5
regeneration; exact P0/P2 reader-manifest reconciliation; provider-free sealed
batch-4 packet-to-validator-to-apply proof; and clean-worktree provenance.
Freeze HEAD only after every check passes. Then run one 12x25 zero-seed P6
execution, score it before editing anything, and accept the resulting verdict.
Only a terminal execution failure or a core truth-corruption failure may justify
a repair/replay. Efficiency, refresh ratios, optional retrieval improvements,
and new edge cases must go to the backlog instead of triggering another P6
cycle.

**Coordinator impact:** P6 remains incomplete by explicit user direction. The
interrupted artifact is diagnostic only. P1 provider evidence, P7, P8, and P9
remain downstream and must not claim release closure without a complete green
P6 artifact from the same final commit. No architecture document was modified.

**Edge cases added:** `EDGE-039` remains the compatibility backlog for governed
mention-coordinate phrasing and legacy fixtures. No new runtime edge case was
opened by the interrupted final attempt.

### 2026-07-18 — LOG-044 — CF0 resets execution around the provider-free core

**Type:** observed, reflected, and decided

**Work package / commit:** Core fast-path coordinator `9f8bdad0`; CF0 baseline
and three parallel reuse audits.

**What happened:** Implementation resumed without restarting P6. The first
provider-free suite passed `385` tests but skipped 31 PostgreSQL tests because
the shell lacked `DATABASE_URL`. Treating that as a green baseline would repeat
the earlier mistake of using the expensive integrated run as the first real
schema check. A stale local proof database then failed six targeted truth/P5/P6
tests because it lacked current columns and barrier functions. Rather than
patching code around stale schema, CF0 created a fresh isolated database,
applied the current migration set, and reran the same slice. The targeted
PostgreSQL tests passed `12/12`; the complete epistemic-repair suite reached
`415 passed`. The one additional P8 test requires an explicitly configured real
Codex provider and is outside the provider-free CF0 gate.

**Reflection:** The work is still aligned with the main goal. No provider run,
P7/P8 expansion, ingestion work, task autonomy or broad refactor began. The
first discovered failure was classified as environment/schema readiness, not
semantic learning. This prevented the previous loop of provider run -> schema
failure -> patch -> new commit -> full restart. The parallel audits converged on
one reuse-heavy seam rather than three new subsystems.

**Decision or next test:** Complete CF0 with the fresh-database receipt, reuse
matrix and file ownership. CF1 begins with the smallest core truth defect: make
the T1 observation read tenant-scoped, then introduce the governed semantic
episode and accepted-memory/evidence/atomic-command seams behind existing
components. CF2 must prove the real Think path using an injected scripted
provider before CF3 may call Codex.

**Coordinator impact:** P6 remains stopped. The existing P6 population is now a
development regression. The next authorized milestone is M0, not a full
twelve-batch execution.

**Deferred behaviors:**

### DEFER-001 — Schema-drift checker lags current Think columns

- Date: 2026-07-18
- Discovered in phase/run: CF0 fresh-database migration
- Artifact or reproduction: `scripts/check_schema_drift.py` after applying all
  current core migrations
- Category: `COMPATIBILITY`
- Observed behavior: the checker reports `think_runs.execution_mode` and
  `think_runs.validation_result` as unexpected even though current migrations
  add those columns.
- Affected component: schema drift tooling
- Core invariant affected: no
- Severity: low
- Why deferred: migrated truth/P5/P6 PostgreSQL slices pass; repairing the
  checker does not advance the M0 learning vertical.
- Revisit trigger: before production-release schema qualification or if the
  checker begins hiding a real missing column.
- Recommended future phase: CF8/production hardening
- Status: open

### DEFER-002 — Real-provider P8 fault slice is outside CF0

- Date: 2026-07-18
- Discovered in phase/run: CF0 complete epistemic-repair suite
- Artifact or reproduction:
  `tests/epistemic_repair/p8/test_p8_provider_fault_slice.py`
- Category: `ROBUSTNESS`
- Observed behavior: the test requires `LLM_PROVIDER=codex` and
  `CODEX_TRANSPORT=cli`; CF0 intentionally ran without a live provider.
- Affected component: provider fault characterization
- Core invariant affected: no
- Severity: low for M0
- Why deferred: CF0-CF2 must remain provider-free and prove the learning path
  before fault-characterization work resumes.
- Revisit trigger: CF8 bounded robustness or an explicit provider-fault phase.
- Recommended future phase: CF8
- Status: open

### 2026-07-18 — LOG-045 — CF1 authority seam closed without reopening P6

**Type:** implemented, validated, reflected, and decided

**Work package / commits:** `23e39d73`, `235a86b6`, `7d5404c1`,
`daab7975`, `e2fc0b06`, `8040feff`, `abd235ef`, and `62080642`.

**What happened:** Three disjoint lanes added immutable company-learning
contracts, governed semantic episodes, tenant-scoped batch reads, an injected
actual-Think execution seam, a provider-blind four-batch population, a
provider-free structured-call adapter, source-authenticated grounding, and an
accepted-memory snapshot adapter. Integration then made governed episodes the
authoritative candidate source whenever present. Only assertions whose entity
coordinate reached `resolved_for_consumer` may become truth candidates;
provisional and unresolved assertions remain uncertainty signals. Synthesis
hydration now reads accepted Models by canonical reference instead of mutable
display label.

**Evidence:** The joined contract/episode/context/snapshot/grounding/provider
slice passed `55/55` tests, including `31/31` context-packet tests against the
fresh CF0 PostgreSQL database. The earlier focused worker join passed `16/16`.
No live provider or full P6 execution ran.

**Reflection:** This checkpoint still advances the shortest loop: signal batch
to governed entity scope to accepted-memory reasoning. It did not add connector
transport, task autonomy, broad ontology behavior, deployment hardening, or a
second truth store. The largest implementation risk was accidentally treating
provisional mention detection as entity authority. Investigation showed that a
mention adapter cannot legitimately admit identity; the CF2 fixture must pass
through the existing source-binding and grounding admission rules. The main
drift risk is now further contract polishing before the worker runs.

**Decision or next test:** Freeze further CF1 breadth. Build and execute the
single CF2 four-batch provider-free runner. Integrate accepted-memory snapshot
and composite/relation transaction behavior only to the extent the actual
vertical requires. Any optional abstraction, style issue, provider realism,
or legacy compatibility defect is deferred rather than allowed to delay the
first executable loop.

**Coordinator impact:** CF2 is active. CF1 contracts are available and the
entity/retrieval authority path is integrated, but CF1 is not independently
claimed complete until the CF2 runtime proves snapshot reuse, stale-head
rejection, and composite/relation atomicity through PostgreSQL.

**Deferred behaviors:**

### DEFER-003 — Worktree Git automatic cleanup is disabled by stale objects

- Date: 2026-07-18
- Discovered in phase/run: CF1 checkpoint commits
- Artifact or reproduction: every `git commit` prints the existing worktree
  `gc.log` warning and reports too many unreachable loose objects.
- Category: `DEVELOPER_OPERATIONS`
- Observed behavior: automatic repository packing does not run.
- Affected component: local Git object maintenance
- Core invariant affected: no
- Severity: low for M0
- Why deferred: commits succeed and this does not affect runtime semantics or
  evaluation evidence; pruning shared repository objects during active work is
  unnecessary risk.
- Revisit trigger: after the core branch is backed up/pushed or if Git commands
  become materially slow or fail.
- Recommended future phase: CF8/repository maintenance
- Status: open

### 2026-07-18 — LOG-046 — First CF2 worker run exposed a coverage-plane veto

**Type:** executed, failed honestly, reflected, repaired, and awaiting rerun

**Work package / commits:** `9a7dbca2`, `31983ff6`, and `9fe88aee`.

**What happened:** A fresh zero-seed CF2 run passed source-authenticated
grounding and executed the actual first 25-signal T1 batch. The first attempt
had previously stopped before Think because source-authenticated grounding and
learned discovery used different extractor identities for the same mention;
aligning the exact span and extractor identity closed that duplicate-processing
collision. The next run reached a successful Think commit, selected 20 of 25
observations, and made one valid `BatchMemoryDecisionSet` call, but admitted no
Models. The barrier then failed because representation-repair triggers remained
pending.

**Root cause:** The governed episode compiler had already produced ten valid,
tenant-bound atomic candidates from five Harbor and five Delta observations.
It then divided their coverage by all 25 assertions, including 15 observations
already routed to unresolved uncertainty episodes. The resulting `10/25 =
0.40` fell below the `0.60` materiality gate, so the runtime discarded the ten
closed atomics and substituted four generic, non-actionable hypotheses. The
provider-free handler correctly rejected those hypotheses. This was a plane
accounting defect: uncertainty-plane rows were allowed to veto independent
resolved truth.

**Repair and evidence:** Canonical-scope coverage now excludes unresolved
singleton episodes while still counting malformed or provisional assertions
inside the same canonical episode. A 25-signal regression proves that ten
resolved atomics survive alongside fifteen unresolved uncertainty rows. The
focused context-packet suite passed `31` tests with one database test skipped
when `DATABASE_URL` was absent; the joined CF2 source/runner slice passed `6/6`.
The RawDiff prompt now also exposes its required tenant and trigger coordinates,
and the provider-free repair-prompt regression passes.

**Reflection:** The run advanced the core path rather than repeating P6: it
reached real T1 retrieval, reasoning, validation and apply, then stopped at the
first new semantic blocker. No connector, task-autonomy, broad ontology, or
production-hardening work was started. The next action is one clean CF2 rerun;
no additional contract polishing is authorized before that result.

**Decision or next test:** Execute one fresh four-batch provider-free run at the
frozen commit. Score the result before editing. If it fails, classify only the
first new root cause and decide whether it violates a CF2 truth invariant.

### DEFER-004 — Barrier drain bypasses retry backoff for observation-only T4 work

- Date: 2026-07-18
- Discovered in phase/run: first actual CF2 worker run
- Artifact or reproduction: tenant
  `d0b30a2c-5a82-4378-8258-b8eb35f3e2e9` in the local CF2 database
- Category: `ROBUSTNESS`
- Observed behavior: three observation-only representation-repair warnings
  became one batched T4 attempt plus unbatched retries; the evaluator drain
  forced fourteen rapid attempts despite normal retry scheduling.
- Affected component: truth-critical drain and representation-repair queue
- Core invariant affected: not for the current CF2 semantic path, provided all
  accepted-truth invalidation and repair obligations remain barrier-critical
- Severity: medium operational cost; low for the next M0 rerun
- Why deferred: prompt coordinates now let these provider-free calls terminate
  normally. Reclassifying barrier criticality and retry scheduling is broader
  queue policy and does not create the missing atomics, synthesis, correction,
  or reuse proof.
- Revisit trigger: CF8 bounded recovery, or any run where observation-only T4
  retries again dominate barrier time after valid provider execution.
- Recommended future phase: CF8
- Status: open

### 2026-07-18 — LOG-047 — A committed synthesis can be non-current immediately

**Type:** executed, failed honestly, localized, repaired, and awaiting rerun

**Artifact:** `/tmp/fyralis-cf2-provider-free-run5.json`; tenant
`8bf9c5d6-ca8a-4b8c-8364-d6cca6cd87d1`.

**What happened:** The actual worker completed four 25-signal batches from zero
seed. Batch 3 committed ten new atomics, one composite situation and one
dependency relation. The saved accepted-current snapshot nevertheless exposed
30 Models and no composite. Batch 4 therefore created ordinary correction
facts but could not revise the missing current situation.

**Root cause:** The composite truth version cited exact member version
`c32de567...`. In the same semantic diff, a deterministic closed-atomic confirm
then advanced that member's head to `a7f9acd8...`. The truth rows and admission
receipt remained durable, but `accepted_current_models` correctly excluded the
composite because its exact model-version evidence was no longer current. An
apply receipt is not by itself proof of current accepted visibility.

**Repair and evidence:** When a compiler-owned exact confirm targets a member
of a newly emitted synthesis, the compiler now preserves the new observation
as its own atomic insert instead of advancing that member in the same diff. It
does not rewrite user/LLM lifecycle semantics. Eleven compiler tests and a
focused PostgreSQL production-path test pass; the latter proves the new
composite is current and the cited member head does not advance.

**Reflection:** This was a productive single-root-cause loop: inspect one failed
run, prove the exact truth/version sequence, implement the smallest generic
conflict rule, and stop before rerunning. The investigation briefly appeared to
be a missing-write problem because accepted-current and canonical physical
truth were conflated; future postmortems must query both before proposing a
write-path repair. No connector, provider realism, task autonomy, broad
lifecycle redesign or later phase was opened.

**Evaluator lesson:** A first runtime-receipt adapter attempted to infer
accepted models, participant versions, commit identity, per-signal processing
and barrier matching from weaker coordinates. Those inferences would hide this
exact failure. The adapter is not accepted evidence until every scorer field is
backed by current heads, exact relation participants, actual per-input fate and
durable lifecycle provenance; unavailable coordinates must remain unavailable.

**Decision or next test:** Finish the gold-blind receipt and coupled rollback
test, commit the focused checkpoint, then run one fresh four-batch CF2 vertical.
Score it before any deterministic replay or CF3 work.

### 2026-07-18 — LOG-048 — Evidence-faithful scoring before another replay

**Type:** bounded implementation, evaluator audit, and pre-run reflection

**What changed:** The gold-blind runtime receipt now derives processing from
the exact successful T1 batch parent and its 25 completed member triggers;
derives accepted outputs as changes between digest-valid chained barrier
receipts; maps selected Model IDs to exact prior barrier versions; reads exact
canonical relation participants; and assigns a shared Think transaction
envelope only when the applied diff, accepted composite, admitted relation and
immediate barrier all agree. Cross-run determinism now hashes a semantic
projection instead of tenant-bound UUIDs.

**Evaluator correction:** The sealed gold required 80 named storyline
groundings and evidence-bound atomics, while the source-authenticated fixture
recognized only `release` and `handoff`. Exact `pilot` and `review` workstream
suffixes are now supported. A full-batch unit proof resolves all 20 named
signals and abstains on all five noise/distractor signals; gold was not
weakened.

**Evidence:** The focused adapter/source/queue/replay suite passes `34` tests.
A dedicated PostgreSQL database has all `218` repository migrations applied,
and `2/2` production-shaped PostgreSQL tests pass against that exact schema.
The database checks exercise the receipt queries and coupled transaction path;
they do not substitute for the next fresh four-batch CF2 execution. The first
canary also exposed and removed a unit-only query for nonexistent canonical
`supporting_model_ids` rather than papering over it.

**Final integrity audit:** Barrier head IDs receive credit only when they
resolve to canonical truth versions for the exact tenant. Observation evidence
receives credit only when the exact tenant, observation ID and `occurred_at`
revision resolve together and its digest and coordinates validate. Two focused
regressions protect these fail-closed rules. Determinism intentionally remains
red with only one execution digest; an independent second replay is required
and is deferred by user instruction.

**Reflection:** This remained on the CF2 critical path: it prevented a known
false-green report and an unattainable score before spending another full-run
cycle. No connector, live provider, task autonomy, broad ontology, deployment
or later phase was opened. Canonical revise/natural-text coherence was found
but recorded as EDGE-042 instead of expanding the current change.

**Decision or next test:** Commit this evaluator checkpoint, freeze the SHA,
then execute exactly one fresh zero-seed four-batch provider-free vertical.
Build and score its receipt before deciding on any runtime repair or second
replay.

### 2026-07-18 — LOG-049 — Run 6 isolated one batch-4 authorization defect

**Type:** frozen execution, postmortem, bounded repair, and reflection

**Frozen evidence:** Commit `48cb02741574`; tenant
`5dab01e7-38b0-4c61-b6ce-77e555f1f2bc`. The zero-seed provider-free run
attempted four waves in `118.969s`; batch times were
`45.491/21.731/25.300/26.277s`, with `14` provider-free calls. Batches 1–3
succeeded and batch 4 failed closed with `RELATION_ENDPOINT_VERSION_MISMATCH`.
The frozen receipt reports processed signals `25/25/25/0`, groundings
`20/20/20/20`, atomics `16/16/16/0`, Model deltas `16/17/18/0`, relation
deltas `0/0/1/0`, and exact barriers for the first three batches.

**Root causes and bounded fixes:** A lifecycle-only revision could accidentally
authorize an accepted relation; accepted relation admission now requires an
explicit relation-bearing operation. Closed atomics select exact claim-local
evidence from the batch-wide manifest. The evaluator receipt emits canonical
`abstraction_level` and `claim_role`, and synthesis requires the exact
`composite` + `situation` shape rather than choosing a same-source atomic.
Blindness proof no longer depends on execution completion. Rebuilding and
rescoring the frozen run makes synthesis, relation atomicity and contamination
green. Focused repaired-seam validation passes `80` tests on the dedicated
database. Remaining reds are the batch-4 cascade, the frozen pre-fix Access
atomic omissions, and determinism, which still requires the independent replay
deferred by user instruction.

**Backlog, not blockers:** Splitter-empty telemetry remains observable but is
not expanded on this critical path. Five known broad-file failures—three in
`test_llm_reason` and two in `compiled_candidate_scope`—are classified as
existing test-contract drift and recorded for later cleanup.

**Reflection:** The run produced bounded defects, not a reason to open latency,
repair-policy, noise-handling or broader architecture work. Preserve the
core-fast framing and validate only the repaired batch-4 path next.

### 2026-07-18 — LOG-050 — Run 7 closed mechanics and isolated one coherence ordering defect

**Type:** frozen execution, canonical scoring, bounded runtime repair

**Frozen evidence:** Commit `32f484e0fcdd`; tenant
`c907278e-0ef4-42be-a462-9c9f2a359b33`. The zero-seed provider-free run
completed all four 25-signal batches in `107.575s`. All `100/100` signals were
processed, all `80/80` named signals grounded and produced exact evidence-bound
atomics, all 20 distractors abstained, and barriers matched
`20/20`, `40/40`, `61/61`, and `81/81` with no missing or stale heads.
Retrieval shifted from observations toward memory: observations
`20/10/10/2`, accepted Models `0/20/20/20`.

**Scoring and root cause:** Batch integrity, grounding, atomic evidence,
retrieval, barriers and contamination are green. Synthesis, correction history
and shared composite/relation atomicity are red because representation
enrichment advanced one immutable member head after admitting the batch-3
composite. The composite became non-current immediately; batch 4 therefore
inserted the corrected thesis as a new atomic instead of revising the
composite. The evaluator correctly refused credit.

**Bounded repair:** Lifecycle-pressure target selection now excludes every
Model used by a new same-diff composite and selects another eligible Model, or
emits no maintenance operation. The focused representation, compiler,
transaction and evaluator slice passes `47/47` tests on the dedicated database.

**Reflection:** This is one causal defect with three downstream red gates, not
three independent workstreams. Do not open general lifecycle policy, latency,
provider, connector or stale broad-test cleanup. Commit this repair and rerun
the exact four-batch vertical once.

### 2026-07-18 — LOG-051 — Run 8 proved synthesis and exposed validator re-promotion

**Frozen evidence:** Commit `df55e849`; tenant
`25b27238-5822-4292-a96f-63f6704f8165`. Batch 3 retained 61 accepted Model
heads and canonical scoring made both synthesis and composite/relation
atomicity `1.0`. Batch 4 failed closed with
`RELATION_ENDPOINT_VERSION_MISMATCH` while applying the correction.

**Root cause and repair:** The compiler correctly marked a relation inferred
beside a non-relation lifecycle decision as `needs_review`, but validator
auto-admission promoted it back to `accepted_edge`. The authorization guard is
now durable metadata recognized by validation, so a lifecycle or claim-only
decision cannot indirectly authorize relation truth. The focused core slice
passes `48/48` tests.

**Reflection:** Run 8 falsified the assumption that compiler disposition alone
survived the full pipeline. This is the same relation-authorization invariant
at the next consumer, not permission to broaden relation semantics. Validate
once more on the exact four-batch path.

### 2026-07-18 — LOG-052 — Run 9's old evaluator green was a semantic false-positive

**Frozen evidence:** Commit `f02df04f`; tenant
`f8c222db-88f9-4e1d-b215-be08a36400b7`; four batches completed in `105.855s`.
Every then-implemented gate except cross-run determinism reported green. The system
processed `100/100`, grounded and admitted `80/80` exact atomics, retained the
batch-3 composite and relation, revised that same composite in batch 4 with
history, closed all barriers, and showed zero contamination.

Independent canonical inspection subsequently found that composite v2 changed
`proposition.summary` to the corrected “no longer blocked” thesis while
canonical `natural_text` and `supported_relation.mechanism` retained the v1
“blocked by incomplete certificate renewal” thesis. The evaluator checked
lineage, current-head state and relation atomicity, but not agreement among
these semantic surfaces. Its green result was therefore a false-positive and
does not complete CF2.

**Learning:** Authorization has to survive every consumer, immutable composite
evidence has to survive same-diff ordering, and a revision is not coherent
unless proposition, natural text and its embedded relation envelope describe
the same current judgment. The bounded repair should version revised natural
text through canonical truth and refresh only the relation envelope's semantic
fields from exact correction evidence while retaining governed endpoint
identity. Add the missing evaluator coherence assertion.

**Reflection:** The mechanical loop is intact, but the semantic exit is not.
Repair and rescore this exact defect before opening CF3. Do not run the deferred
second replay or expand into transition-enum redesign, broad relation lifecycle,
latency, production hardening, stale broad-test cleanup, or other ledger items.

### 2026-07-18 — LOG-053 — Correction coherence is now one governed transition

**Implementation:** A composite correction now advances proposition and
canonical natural text together, refreshes the embedded relation mechanism from
the exact correction evidence, and emits an explicit retirement operation for
the superseded relation. Canonical relation discovery reads current relation
truth and exact typed participants rather than trusting mutable legacy
projections. Retirement advances the immutable canonical head first, retires
the relation instance/projection binding second, and preserves the historical
accepted edge row as an immutable artifact.

**Evaluator repair:** The gold and scorer now require the exact corrected
thesis, equality between canonical natural text and proposition, and an exact
active-to-retired relation successor. The receipt binds that successor to the
same successful batch-4 Think/apply envelope and diff hash. Semantic replay
fingerprints now include natural text and relation fate so these defects cannot
be hidden by a stable digest.

**Evidence:** The joined compiler, applier, truth-admission, atomicity, receipt,
scorer, semantic-replay and CF2 decision slice passed `55/55` against
`fyralis_cf2_core_20260718`. Architecture ratchets passed. The repository-wide
technical-debt budget still fails on pre-existing global thresholds and
unrelated named files; none of the reported file/function overruns points to
this patch.

**Reflection:** This repair stayed inside the core truth-consistency boundary.
The next action is a checkpoint commit followed by exactly one fresh zero-seed
four-batch run and strengthened score. Do not open CF3 or chase unrelated debt
before that proof.

### 2026-07-18 — LOG-054 — Runs 10–12 isolated validator transformation of retirement

**Frozen evidence:** Tenant `43e56d9c-faf2-4896-9d61-7fca4e84e34b` completed
batches 1–3. Batch 4 failed after `27.310s` with `accepted relation edge is an
immutable projection`. The transaction rolled back, leaving no partial
revision, retirement, unrelated relation mutation or completed barrier.

**Corrected root cause:** Diagnostics proved the explicit retirement itself was
transformed by `_canonicalize_relation_claim_semantics` from
`retired`/`no_edge` to `accepted`/`accepted_edge`. The other operation was an
unrelated `weakens`/`needs_review` relation on different endpoints; it did not
reassert the retired identity. Compiler conflict-fold work is defense-in-depth,
not the root repair.

**Proof:** Run 12 tenant `fa3f367f-a95e-4ad4-a0ce-e664a56daac0` failed B4 after
`26.631s` before the validator guard. Exact pending B4 retry run
`019f75e5-f01f-7000-877c-edfaed6d009c` succeeded afterward. A direct PostgreSQL
regression proves retirement status/policy survive validation and canonical
apply. One fresh clean zero-seed run remains pending.

### 2026-07-18 — LOG-055 — Run 13 closes CF2 single-execution evidence

**Result:** Run 13 at commit `27e37b5e`, tenant
`2a14a6bf-fe59-4efd-a52d-ad7ffcfa7d30`, completed all four clean zero-seed
batches. Every authorized CF2 single-execution gate was green, including
relation retirement and relation atomicity.

**Evaluator lesson:** Retirement proof must resolve the exact historical
relation instance advanced by the correction. Reading only the current-instance
surface can hide a valid immutable lifecycle transition. The corrected focused
evaluator suite passed `14/14`.

**Boundary:** Independent receipt and canonical database audits passed, so M0
is complete for the authorized single-execution scope. This run does not prove
determinism; the replay count is one, so determinism remains explicitly deferred
and unproven.

### 2026-07-18 — LOG-056 — CF3-A failed before semantic output

**Observed:** At commit `f869dd82`, tenant
`50270994-753d-465f-b87e-7d794cf2d3a7`, the one-batch attempt used Codex model
`gpt-5.6-terra`. The installed CLI said the model requires a newer Codex
version. Batch 1 consumed `124.181s`; no semantic output was produced and zero
Models were accepted.

**Classification:** Infrastructure/configuration failure. CF3-A remains red;
this artifact neither falsifies nor supports semantic company-learning quality.

**Next hypothesis:** Run the identical one-batch rung with an explicitly
supported model, keeping the population fixed so compatibility is the only
intended variable.

**Artifact:** `/tmp/fyralis-cf3a-codex-one-batch.json`.

### 2026-07-18 — LOG-057 — Supported model reaches semantics; grounding fate is incomplete

**Observed:** Commit `8b027197`, tenant
`97b210f5-28c9-4206-b8a1-9c1f25335809`, ran one batch with
`gpt-5.3-codex-spark` at low reasoning effort. The batch was mechanically
complete and emitted 25 signal fates; barrier 0 remains pending. All four
physical attempts succeeded with exact input/output/cache tokens:

- question planning: `13,314 / 4,769 / 4,224`;
- main reasoning: `20,874 / 9,858 / 9,856`;
- main reasoning: `16,825 / 3,567 / 4,352`;
- main reasoning: `16,902 / 1,306 / 11,264`.

**CF3-A verdict:** Red. Two of 24 mentions have no grounding fate:
`Atlas certificate training example` on `p6-b01-s24`, and `Facilities` on
`p6-b01-s21`. This is the governing defect; CF3-B is held.

**Scorer boundary:** The full P6 scorer reports missing evidence because one
batch is only a partial prefix. Those fields are expected to be absent and do
not determine the CF3-A verdict.

**Artifacts:** `/tmp/fyralis-cf3a-codex-one-batch-spark.json`,
`/tmp/fyralis-cf3a-codex-one-batch-spark-evidence.json`, and
`/tmp/fyralis-cf3a-codex-one-batch-spark-score.json`.

### 2026-07-18 — LOG-058 — Detection fate and grounding continuity are separate contracts

**Finding:** `detection.fate=detected` is already the durable mention-detection
fate. The post-freeze mention row omitted it and exposed only optional
downstream grounding fate, conflating a completed detection stage with missing
grounding continuity.

**Evaluator repair:** Mention evidence now carries `detection_fate`, nullable
`grounding_fate`, and `grounding_stage`. A detected mention without a trace or
provisional grounding disposition is `not_started`; the new
`complete_detected_mention_grounding_continuity` HG gate fails and lists exact
incomplete members.

**Boundary and proof:** No runtime enqueue or barrier behavior changed in this
evaluator slice. Focused evidence and scorer tests passed `44/44`. CF3-A remains red until a fresh
one-batch run proves the gate; CF3-B remains held.

### 2026-07-18 — LOG-060 — Receipted CF3-A rerun closes the transport canary

**Result:** Commit `e7de1c3a`, tenant
`08d19975-2c39-4fef-a820-27d29c30fd9b`, completed exactly 25 signals in one
batch over `269.295s`. All `24/24` detected mentions have explicit trace fates.

**Drain and barrier:** Truth-critical pending work drained `27 -> 3 -> 0` over
two cycles. The batch barrier then closed with
`truth_critical_pending_count=0`.

**Receipts and residue:** The run recorded 28 logical calls and 29 physical
attempts, all with exact reported usage; 28 physical attempts succeeded and one
ended in parse failure. Post-freeze evidence shows zero active reviews,
candidates, Models, and relations.

**Decision:** CF3-A is green. CF3-B is unlocked; CF3-C remains gated on CF3-B.

**Artifacts:** `/tmp/fyralis-cf3a-codex-one-batch-spark-receipted.json`,
`/tmp/fyralis-cf3a-codex-one-batch-spark-receipted-evidence.json`, and
`/tmp/fyralis-cf3a-codex-one-batch-spark-receipted-score.json`.

### 2026-07-18 — LOG-059 — Durable pending work must remain consumable and version-owned

**Confirmed repair:** The resolver's phrase-readiness filter treated every work
status except due `retry_scheduled` as ineligible. Once detected mentions began
creating durable `pending` work, that defensive filter made the work invisible
to the resolver. `pending` is now immediately ready, `retry_scheduled` is ready
only when due, and terminal statuses remain ineligible. The persisted-batch
grounding test is the governing regression.

**Deferred atomicity lesson:** Counting pending work is not itself a barrier
fence. Grounding enqueue and barrier completion must eventually share a
tenant-scoped transaction serialization mechanism; otherwise a concurrent
enqueue can cross the zero-count/receipt-write interval.

**Deferred replay lesson:** A future-path enqueue does not repair existing
detected heads. Replay must idempotently backfill missing work, and grounding
work identity must follow the current detection version. A fixed generation and
phrase-only correlation can let stale terminal work mask a superseding
detection. These cases remain explicitly deferred in EDGE-049 through EDGE-051
and are not part of the current narrow readiness repair.

### 2026-07-18 — LOG-061 — Cold-start fact memory must not require resolved identity

**Failure:** The first CF3-B batch completed mechanically but admitted zero
Models. Its provider trace correctly refused canonical writes because every
entity coordinate remained provisional. Batch 2 could therefore not prove
retrieval or material reuse, and the run was stopped rather than spending a
second provider batch on an impossible gate.

**Root cause:** The runtime coupled two distinct judgments: whether an exact
source assertion is safe to remember, and whether the mentioned company entity
has a resolved canonical identity. A zero-seed company could detect mentions
but could not accumulate its first reusable factual memory.

**Bounded repair:** A provisional assertion with an exact current detection
UUID and exact `content_text` span may compile into one singleton atomic scoped
to `mention:<detection UUID>`. The scope grants no canonical identity, entity
type, alias, relation, or cross-observation grouping authority. The final
validator reopens PostgreSQL and verifies the current same-tenant detection
head, sole observation, exact anchor, compiler-owned nonidentity contract, and
exact assertion before admission. Mention scopes are excluded explicitly from
synthesis hydration and relation obligations.

**Proof boundary:** `69/69` focused compiler, homonym-isolation,
truth-admission, and PostgreSQL tests pass. This is not yet an end-to-end
runtime result; a provider-free one-batch vertical and fresh CF3-B run are the
next gates.

### 2026-07-18 — LOG-062 — Founder bootstrap is the preferred operating mode

**Product insight:** A founder-assisted onboarding that establishes people,
teams, workstreams, products, customers, systems, aliases, source identities,
and key relationships gives entity resolution a vivid company-specific prior.
This should be the preferred product cold start, with versioned and revisable
authority rather than infallible seed truth.

**Scope decision:** Do not pivot the active fast path into a new onboarding
subsystem. Mention-scoped atomics remain the bounded fallback for absent,
incomplete, or newly outdated bootstrap knowledge. Evaluate founder-bootstrap
usefulness and zero-seed safety as separate modes after the core reuse loop is
green.

### 2026-07-18 — LOG-063 — Retrieval and mechanical reference are not semantic learning

**Execution:** Commit `e8bbe033`, tenant
`fd1588ed-1379-49c6-b6da-ac69b5f25b79`, processed two real Codex batches of 25
signals in `481.491s`. Batch 1 admitted 14 evidence-bound mention-scoped
atomics. Batch 2 retrieved all 14 batch-1 Models and finished with 28 accepted
Models. Both exact barriers closed with zero truth-critical pending work.

**False-positive telemetry:** Runtime telemetry labeled batch 2
`model_context_used` because a generic lifecycle-pressure obligation touched
one selected batch-1 Model. The provider reasoning trace instead states that
each decision followed from its direct member observation and never cites or
reasons from prior Model content. Independent audit therefore classifies the
CF3-B material-use gate red.

**Architectural meaning:** Mention-scoped atomics solve safe factual retention,
but intentionally cannot group different unresolved detections. Retrieval alone
does not create the identity continuity required for company-level learning.
Do not weaken that safety boundary or tune the use metric to pass. Evaluate the
existing founder/bootstrap and canonical-identity reuse surfaces before another
provider run; retain zero-seed mention memory as fallback evidence.

**Artifacts:** `/tmp/fyralis-cf3b-codex-two-batch-spark-r2.json`, its
`-evidence.json` and `-score.json` companions. The generic full-P6 score is
expected red on a two-batch prefix and is not the CF3-B verdict.

### 2026-07-18 — LOG-064 — Identity bootstrap belongs before reasoning, not inside belief truth

**Architecture chosen:** Founder onboarding now materializes a versioned,
provenance-bound exact identity vocabulary without seeding Models, relations,
resources, observations, or behavioral conclusions. Unknown names still use
the unresolved/provisional path.

**Runtime repair:** A detected mention that has exactly one active, governed
founder alias receives a deterministic grounding episode before Think builds
its batch payload. Missing, stale, malformed, or conflicting aliases abstain;
the path makes no LLM call. This gives reasoning canonical continuity without
weakening zero-seed safety.

**Evaluator repair:** Retrieval evaluator v2 requires decision-level semantic
dependence on a prior accepted Model. Retrieval presence and lifecycle-only
references are reported but cannot earn material-use credit.

**Proof boundary:** Bootstrap passed `4/4`, evaluator v2 passed `6/6`, and
pre-Think grounding passed `9/9` pure plus `3/3` focused PostgreSQL tests. A
joined PostgreSQL proof at `58688b8c` also passed `1/1`: two Atlas observations
were grouped into one resolved canonical Think episode while novel Zephyr
remained an unresolved singleton. The gold-blind replay preparer and dependency
ordering passed `8/8`; architecture ratchets and diff checks are green. This is
not yet a fresh multi-batch provider run materially using prior Models. CF3-B
remains red until rerun evidence satisfies v2; CF3-C remains locked.

**Deliberate deferrals:** Per-phrase alias query scaling and lifecycle support
for generic referents (rename, split, merge, and retirement) are logged rather
than expanded into the core repair. Three failures found only in the wider
worker lane are also kept separate because they do not exercise the new exact
founder-grounding path. They were not investigated, so any preexisting/flaky
classification remains provisional rather than evidence.

### 2026-07-19 — LOG-065 — Canonical identity and retrieval do not guarantee a learning effect

**Execution:** The fresh founder-assisted CF3-B run at
`/tmp/fyralis-cf3b-founder-two-batch-spark-r1.json` completed for tenant
`cb3a8a53-5222-4b31-90ee-f86bf1b68589` in `414.720s`: batch 1 took `259.934s`
and batch 2 took `154.671s`. Both exact barriers were complete with zero
pending work. The founder receipt was valid for four aliases, its manifest
digest was
`ce1aff1ed00777432f872ac88778c3f5745e76181aeb0d54a6f2a0c7f1b50187`,
and its semantic deltas were all zero. The run recorded 26 matching LLM
attempt receipts totaling 376,056 input tokens, 38,852 output tokens, and
248,064 cache tokens.

**Result:** Batch 1 produced 14/14 evidence-backed accepted Models. Batch 2
selected all 14 exact batch-1 versions and durably referenced two, but the
provider reasoning trace referenced zero and materially used zero. The CF3-B
evaluator therefore reports every gate green except
`b2_materially_uses_exact_b1_model_version`; its verdict is correctly red.

**Root cause:** Independent dataflow audits found retrieval healthy. The
remaining gap is between retrieved prior truth and semantic application: the
runtime lacks a sufficiently explicit exact-scope join, prompt obligation, and
validated output effect requiring the provider to compare new evidence with
the relevant prior Model and expose how that comparison changed its decision.
Increasing retrieval volume, weakening the evaluator, or adding more founder
aliases would not repair this boundary.

**P0 decision:** Implement one broader, observable effect contract spanning
exact prior-version selection, scope-matched prompt context, provider decision
provenance, and validated application. The proof must show a concrete
conclusion, confidence, correction, or lifecycle decision that differs because
of an exact prior Model version. Keep CF3-C locked until that contract passes.

**Deferred observation:** Some repeated exact envelopes may eventually be
deduplicated before reasoning, but repetition can mean corroboration, continued
state, or a later recurrence. Exact-envelope deduplication is therefore not the
current fix and remains deferred until its temporal semantics are specified.

### 2026-07-19 — LOG-066 — Exact-scope prior memory now has a governed application path

**Repair:** Atomic decision candidates now bind only to active prior Models
whose explicit canonical scope exactly matches the candidate. The provider must
classify each comparison as `supports`, `weakens`, `contradicts`, or `none` and
cite claim-local evidence. The compiler authorizes the prior Model and evidence
against the candidate, then maps accepted effects into the existing lifecycle
boundary. `supersedes` remains blocked until an accepted successor Model exists
rather than being approximated as revision.

**Telemetry and evaluator:** Material prior-memory use now requires agreement
between exact selected prior identity, authorized effect metadata, a coherent
applied lifecycle action, and a provider/compiler reasoning trace naming the
prior Model, candidate, and relation. Retrieval presence, durable reference,
unchanged output, or generic lifecycle pressure cannot independently count.

**Deterministic proof:** Focused contract and telemetry suites passed, and the
PostgreSQL vertical in
`services/reasoning/think/tests/test_prior_memory_effect_apply.py` passed
compile → validate → apply. A `supports` decision advanced the exact prior
Model truth head from version 1 to version 2, attached only the new claim-local
observation, and simultaneously preserved the new observation as a separate
version-1 singleton atomic Model. Ruff and diff checks are green.

**Reflection:** This repair targets the actual failed boundary instead of
increasing retrieval, changing identity policy, or deduplicating temporal
evidence. It is not yet proof that the real provider follows the new output
contract. Run exactly one fresh two-wave CF3-B canary next; if it fails, inspect
that artifact and repair only the single governing defect. Keep all unrelated
worker-suite and temporal-compaction issues in the ledger.

### 2026-07-19 — LOG-067 — Semantic effects reached the provider but failed the production scope join

**Execution:** The fresh CF3-B run for tenant
`feb50ba5-d87f-42d3-b77c-6fe3bd55936e` completed in `282.745s`. Batch 1
admitted `14/14` Models. Batch 2 selected all 14 prior Models, referenced eight
in its reasoning trace, and attempted eight explicit prior-memory effects. Its
strict report is
`/tmp/fyralis-cf3b-prior-effect-two-batch-spark-r1-cf3b-v1.json`.

**Result:** All eight effects were compiler-blocked, so material use remained
red. Production `scope_entities` had been substrate-normalized while each
Model's semantic `proposition.scope_ref` still retained the exact candidate
coordinate. The binder therefore did not authorize the referenced prior Models
under its entity-only exact-scope comparison.

**Bounded repair direction:** Treat matching semantic scope type plus exact
`proposition.scope_ref` as a fallback only when the substrate-normalized entity
coordinate does not directly match and the prior Model carries the compiler's
closed-atomic provenance contract. An independent safety audit rejected a
broader string-only fallback before rerun. The provenance-bound fallback is
locally tested, including an untrusted-proposition negative case; a fresh CF3-B
rerun is still required. Do not broaden matching by label or similarity, and do
not reopen the deferred temporal-deduplication work. Legacy/composite semantic
scope authorization remains deferred until it has its own explicit authority
contract.

### 2026-07-19 — LOG-068 — Provenance-bound exact scope closes CF3-B

**Execution:** Tenant `e188354c-4a88-406d-bf25-f005cf9af275` completed the
fresh two-batch run in `227.633s` (`113.399s` / `114.124s`). Batch 1 admitted
`14/14` Models. Batch 2 selected all 14 prior Models and recorded 12 authorized
and material effects; all 12 were reasoning-trace referenced, durably applied,
and receipted. Both barriers closed. Usage was 204,011 input, 42,466 output,
and 121,856 cache tokens.

**Verdict:** The strict CF3-B report has no failed gates. The exact semantic
scope fallback succeeds only when bound to compiler-owned provenance, bridging
substrate-normalized `scope_entities` without treating arbitrary proposition
scope strings as authority. CF3-B is green and CF3-C is unlocked.

**Artifacts:** `/tmp/fyralis-cf3b-provenance-scope-two-batch-spark-r1.json`
and `/tmp/fyralis-cf3b-provenance-scope-two-batch-spark-r1-cf3b-v1.json`.
Temporal exact-envelope deduplication remains deferred under EDGE-058.

### 2026-07-19 — LOG-069 — CF3-C isolated synthesis identity, distractor admission, and evaluator defects

**Execution:** The first dedicated CF3-C run processed four intact batches of
25 for tenant `4ab38d7f-5a24-4aac-b3f7-5d4ce4ce5503` in `718.814s` with 62
provider calls. Boundary reconstruction, canonical linking, entity typing,
evidence lineage, atomic recall, and material earlier-Model use passed. The
frozen verdict remained red: no composite or canonical Atlas relation entered
truth, atomic precision was `0.875`, exact mention F1 was `0.818605`, and scope
precision was `0.333333`.

**Root-cause classification:** The Atlas synthesis candidate existed, had eight
exact accepted member Models and one exact relation-evidence bundle, but its
80-character candidate identifier was returned non-identically and discarded
as `ignored unknown candidate id`. Relation perception also interpreted
generic `links`/`connects` wording as `blocks` rather than causal influence.
Separately, each of four distractor observations produced two identical
whole-observation Models under distinct unresolved `mention:*` coordinates;
those eight rows explain every atomic-precision, scope-precision, and noise
offender. The barrier aggregate was an evaluator defect: all four barriers and
100 signal fates were complete, while 48 entity-grounding receipts correctly
had no Think-run identifier. Required mention recall was complete (`88/88`);
most additional spans are legitimate nested company objects absent from sparse
gold, so suppressing extraction would be a product regression.

**Bounded repair:** Candidate identifiers are now short digest-bound opaque
keys derived from their complete basis. Mature synthesis-only evidence that
links ownership/handoff state to an adverse temporal outcome compiles as
causal influence; generic atomic links retain their prior conservative
behavior. Duplicate unresolved mention candidates can no longer admit the same
full observation as multiple Models, while observations, mentions, and fates
remain intact. Receipt validation is now purpose-aware, and the CF3-C report is
cryptographically bound to both frozen execution and evidence-wrapper digests.
Sparse-gold mention evaluation now separates required recall, adjudicated
precision, confirmed false positives, and unadjudicated open-world review
spans. The same frozen artifact rerenders at required recall `1.0`, adjudicated
precision `0.967033`, exact mention F1 `0.983240`, three confirmed false
positives, and 36 review spans; the mention gate is green without suppressing
production extraction.

**Reflection:** This is one classified CF3-C repair cycle, not permission to
open CF4 or pursue unrelated lifecycle, efficiency, or ontology work. The
original artifact remains red. Focused PostgreSQL and pure validation is green;
one fresh four-batch rerun is required before any M1 claim.

### 2026-07-19 — LOG-070 — Second CF3-C failure forces explicit synthesis ownership

**Execution:** Tenant `bc18e40b-cadc-4ff8-bbc8-6ba7cbd76df0` processed four
intact batches of 25 in `796.150s` with 57 provider calls. The strict report at
`/tmp/fyralis-cf3c-four-batch-spark-r2-cf3c-v1.json` made every continuous
grounding, atomic, linking, typing, lineage, scope, receipt, barrier and
prior-memory metric green. Exactly 12 intended atomics entered each of the
first three batches. The only six failed gates were consequences of one absent
Atlas composite and its absent canonical causal relation.

**Root cause:** The repaired short candidate identifier joined. The compiled
trace then reported `invalid or stale closed-set relation binding`: one causal
obligation was perceived, none emitted, and the synthesis was blocked. The
provider controlled both endpoint roles and a second member-id list, while the
compiler required those two independently authored fields to agree exactly.
This redundant authority could silently discard a semantically valid decision;
the raw structured provider decision was not retained, so the precise predicate
was not observable after the run.

**Architecture decision and bounded repair:** The provider owns the thesis and
semantic source/target roles. The compiler owns the hydrated closed set, exact
accepted-head versions, evidence bounds and final member normalization.
Provider-selected members remain advisory and are preserved when closed-set
valid; selected distinct endpoints are always included. The compiler does not
force all hydrated Models into the composite because some are only background.
Structured `situation_and_edge` decisions now require present, distinct
endpoints, and every rejection names its exact failed predicate. Focused
synthesis contracts and PostgreSQL composite-plus-relation atomicity pass
`21/21`.

**Reflection / next action:** This is the explicit architecture decision
required after the same semantic boundary failed twice. It is still narrowly
about the one missing end-to-end synthesis transition. Do not widen entity,
ontology, lifecycle or efficiency work. Run at most one confirmation CF3-C
canary; if it remains red, preserve the artifact and reconsider the synthesis
boundary rather than entering another local patch/rerun loop.

### 2026-07-19 — LOG-071 — Think Intelligence contracts freeze before another provider run

**Decision:** The LLM-interface audit supersedes LOG-070's immediate
confirmation-canary authorization. The compiler repair remains locally green,
but another CF3-C run would still present an overloaded and insufficiently
observable cognitive task. CF3-C is locked until the bounded Think Intelligence
Gate is green.

**Contract checkpoint:** The integration owner reconciled independent
telemetry, dossier, and decision/scorer audits into
`think-intelligence-contract-freeze-v1.md` v2, SHA-256
`b1e234eee1cdfaf279a431efda4abe39bb7aff5896d1f1d2de1f0b5fbcb48717`.
The freeze defines one trace envelope, one scope-local dossier, one
`SynthesisProposal | AbstentionDecision`, deterministic local-handle binding,
one canonical relation path, independent scorer contracts, minimum policy
governance, artifact rules, and non-overlapping file/database ownership.

**Invariant:** The LLM owns mechanism, direction, alternatives, novelty,
counterevidence, uncertainty, and abstention. The compiler owns canonical
identity, evidence closure, allowed operations, exact versions, and atomic
transaction construction. Runtime contracts contain no evaluator gold and the
provider transports no canonical UUIDs.

**Preflight:** Worktree `fyraliscore-autonomous-learning` was clean at
`c5297848`; the CF3-B, CF3-C r2, and batch-four context artifacts existed with
their documented digests; Python 3.12.13 and PostgreSQL were available; and no
provider or shared-database evidence process was active. No provider call or
CF3-C run occurred.

**Next action:** Commit this reversible freeze, create isolated TI0, TI1, and
TI2 worktrees/databases/artifact roots with the frozen digest, and implement
only their falsifiable provider-free contracts.

### 2026-07-19 — LOG-072 — TI0 preflight corrects one file-ownership omission

**Finding:** The frozen TI0 trace requires the exact structured provider
response before compiler normalization. The active provider path keeps that
text local to `lib/llm/provider.py`; the v1 ownership manifest assigned only
downstream call sites, which cannot reconstruct it.

**Amendment:** Contract v2 adds only a narrow sanitized raw-response trace
emission in `lib/llm/provider.py` and its focused provider test to TI0's owned
surface. Field meanings, semantic authority, scorer separation, and every
other lane boundary are unchanged. The new contract SHA-256 is
`b1e234eee1cdfaf279a431efda4abe39bb7aff5896d1f1d2de1f0b5fbcb48717`.

**Proof boundary:** This is a provider-free contract correction. No runtime
implementation, database state, provider call, or CF3-C evidence run occurred.

### 2026-07-19 — LOG-073 — TI0-TI2 Wave 1 integrates provider-free

**Integration:** TI1 added a standalone evaluator-blind scope-local dossier
assembler. Its twelve-batch mechanical inspection built 48 scope snapshots;
Atlas became structurally mature only at batch four, Cobalt only at batch nine,
and the null case never matured. TI2 added the closed local-handle
`SynthesisProposal | AbstentionDecision` seam. Accepted synthesis compiles to
exactly one composite command and one canonical relation command; abstention
emits no mutation. TI0 added immutable prompt, raw provider response, compiler,
validation, and apply events joined to existing logical and physical receipts.
Parse-invalid raw bodies are retained, sanitized, and linked to their physical
attempts.

**Validation:** On integrated HEAD `68b5a0e8`, 54 combined TI0-TI2 focused
tests passed against the assigned databases, plus two PostgreSQL synthesis
atomicity tests. Architecture ratchets and the production environment contract
passed. The isolated TI1 twelve-batch mechanical proof, TI2 contract suite,
and TI0 DB-backed observability suite were independently green before merge.

**Proof boundary:** This proves the provider-free shared seams and atomic
compiler boundary. Production dossier hydration/call-site selection, the
independent semantic scorer, and TI4-min evaluation receipts are not yet
integrated. No provider call or CF3-C run has occurred.

**Next action:** Freeze this integration checkpoint and run exactly one
observation-only current-interface four-batch canary in a clean proof worktree
and isolated database. Its output is immutable Arm A evidence and authorizes no
patch or rerun. TI3 and the new-interface work proceed regardless of its color.

### 2026-07-19 — LOG-074 — Observation-only Arm A is complete and red

**Execution:** The single preregistered current-interface baseline ran from
clean detached commit `43dcb197` against isolated database
`fyralis_ti0_baseline_43dcb197`, Codex CLI, model
`gpt-5.3-codex-spark`, and explicit medium effort. An initial pre-provider
attempt found the fresh database missing the July 2025 observation partition;
zero observations and zero provider receipts existed. The partition was added
without code change and the same frozen configuration resumed once.

Tenant `64d91147-9a2e-4ccf-83ac-559d50e7d6cb` completed four intact batches in
`982.222s`. Batches one through three admitted 12 atomics each. Batch four's
legacy compiled path validated 21 claim operations and one canonical relation
operation, but the frozen evaluator found no expected Atlas composite/current
version and therefore no exact expected Atlas relation bound to that
composite. The verdict is red on six composite/relation consequences.

**Observable cognition:** The trace preserves exact prompt and raw response
events by purpose, including 42 entity-resolution calls, seven main Think
logical paths, and three question-planning calls. It exposed one TI0 defect:
the generic forbidden-key matcher treats legitimate runtime fields such as
`sparse_threshold` and `relevance_gate.threshold` as evaluator gold, causing
best-effort retrieval debug-capture rows to be skipped. Prompt, raw response,
compiler, validation, apply, and provider receipts remained available.

**Artifacts:**

- raw SHA-256 `8355fac92daaaa018cb870bfb297040de7d393978e291e76fa764bb127bfaa73`;
- postfreeze evidence SHA-256
  `adec20dbcfb6be6bf2d8be41798918ad84d9bc36c95064aa2ee684198b24137d`;
- report SHA-256
  `4d02ae2ac4c58ea3644d4641db73182d981d3edf49cb9a0b9f105a590144ea3d`.

**Decision:** This is immutable Arm A evidence only. It authorizes no legacy
prompt/compiler patch and no rerun. The primary semantic class remains
context/interface design, which TI1-TI3 were already created to test. The
threshold matcher is an observability classification defect to assess in the
Wave 2 audit; it must be repaired before qualifying CF3-C only if it prevents
complete required trace reconstruction. Proceed to scorer, fixture, and audit
lanes regardless of the red semantic result.

### 2026-07-19 — LOG-075 — TI3 r1 terminates incomplete on an unretained parse-failure outcome

**Execution:** The preregistered TI3 run `ti3-live-8d7d9b05-r1` started from
clean commit `8d7d9b05c4081889a93b26cff8af2fb4cb4347de` with Codex CLI,
`gpt-5.3-codex-spark`, medium effort for Arms A/B and high effort for Arm C,
zero retries, and concurrency capped at three. Nine screening calls were
initiated. Six successful Atlas/Cobalt outcomes have durable per-attempt
manifests. One null-case Arm A `BatchMemoryDecisionSet` response is known to
have failed parsing because it placed `no_op` in `decision` rather than the
separate `operation` field. The terminal outcomes of the other two initiated
null-case calls are unknown from the governed artifacts. Do not infer either
success or failure for them.

**Verdict and artifact boundary:** Classify the run as incomplete terminal red,
primary class `schema_binding`. It is not a completed nine-call screening, a
21-outcome TI3 experiment, or a semantic comparison. No arm or policy was
selected. The partial directory remains untouched at
`/tmp/fyralis-ti3-live-8d7d9b05/ti3/ti3-live-8d7d9b05-r1`; it contains six
durable successful attempt directories and no governing run manifest. The
failed call's exact raw body and receipt are not present there, so this record
does not claim terminal raw preservation or exact token/cost reconciliation for
all nine initiated calls.

**Contract impact review:** Contract-freeze v2 already requires raw provider
response plus parse outcome in the cognition trace, names schema validity as a
hard gate, permits `schema_binding` as the failure class, and requires unique,
never-overwritten artifacts. The accepted-only TI3 capture adapter therefore
failed to carry an existing v2 outcome into independent evaluation. This is an
implementation/evaluator-durability nonconformance, not a change to semantic
field ownership. Contract v2 and SHA-256
`b1e234eee1cdfaf279a431efda4abe39bb7aff5896d1f1d2de1f0b5fbcb48717`
remain frozen. Provider, model, effort, schema vocabulary, one-attempt policy,
quality tolerance, scorer gold, and hard gates are unchanged.

**Written rerun hypothesis and next action:** If the evaluator durably retains
and scores a terminal provider parse failure as `schema_valid = false` and
`schema_binding`, without fabricating a parsed decision or changing provider
policy, then the preregistered experiment can preserve complete arm/case
accounting and fail the affected hard gates honestly instead of aborting before
an outcome artifact exists. Reproduce the exact failure shape provider-free,
repair only that capture/evaluation durability seam, and validate it before the
integration owner considers one fresh full TI3 run with a new run identity.
Never resume or overwrite r1, never substitute its six completed calls into a
selective rerun, and do not run CF3-C. CF3-C remains locked until a complete TI3
run selects and freezes a policy.
