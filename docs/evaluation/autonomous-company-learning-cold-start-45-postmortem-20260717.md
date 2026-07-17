# Autonomous Company Learning — 45-Batch Cold-Start Postmortem

**Run:** `autonomous-learning-cold-start-45-be401f25`

**Code checkpoint:** `be401f25`

**Date:** 2026-07-17

**Verdict:** `not_credible`

**Current evaluator-only rerender quality:** `0.8883`

**Evidence coverage:** `1.0000`

## Evidence Artifacts

- Benchmark:
  `/tmp/fyralis-authoritative-45-be401f25/autonomous-learning-cold-start-45-be401f25/benchmark_summary.json`
- Per-wave execution:
  `/tmp/fyralis-authoritative-45-be401f25/autonomous-learning-cold-start-45-be401f25/waves.json`
- Model-layer state:
  `/tmp/fyralis-authoritative-45-be401f25/autonomous-learning-cold-start-45-be401f25/run_summary.json`
- DB-backed Company Vitals:
  `/tmp/fyralis-authoritative-45-be401f25/autonomous-learning-cold-start-45-be401f25/vitals/vitals_scorecard.json`
- Authoritative aggregate:
  `/private/tmp/fyralis-authoritative-45-be401f25/autonomous-learning-cold-start-45-be401f25/authoritative_evaluation_postfix_entity_v5/large_company_simulation_evaluation.json`
  (SHA-256 `cae61794…dfa77`)
- Assurance v7:
  `/tmp/fyralis-company-learning-assurance-be401f25/company_learning_assurance_summary.json`

## Executive Verdict

The run proves that Fyralis has a functioning large-scale autonomous
company-learning metabolism. Starting with zero seeded semantic memory, it
processed 1,125 simulated company signals in 45 genuine batches, created and
updated durable Models, formed a substantial graph, used later evidence to
change earlier memory, safely ignored pure-noise batches and drained all
trigger, post-commit and topology work.

It does **not** prove that the resulting company model is trustworthy.

The original Vitals artifact reported three hard failures. Forensic
classification leaves two noncompensatory company-physics failures and one
recovered operational incident:

1. the mention-detection fate protocol executed for none of the measured
   entity-mention opportunities;
2. the entity resolver directly polluted the canonical identity registry with
   50 aliases;
3. one T1 reasoning attempt exhausted three 180-second provider calls before
   the same batch recovered on a run-level retry. This is real reliability,
   latency and cost degradation, but not terminal workload loss.

The run also contradicts the desired retrieval behavior. Retrieval became
mixed rather than Model-dominant: mature batches continued to select roughly
20 Models and 20 historical observations, and late reasoning referenced
observations more often than Models.

The deepest product conclusion is:

> Fyralis can learn a lot from an evolving company simulation, but its entity
> grounding, claim-local scope, retrieval selectivity, graph directionality
> and calibration are not yet precise enough to trust the learned graph.

## Forensic Failure Classification

The classifications below distinguish the state that the saved run proves
from what later bounded tests prove. “Evaluator mismatch” does not mean the
underlying incident was harmless; it means the gate assigned the wrong fate to
the incident.

| Original hard failure | Primary classification | What the saved run proves | Current gate treatment |
| --- | --- | --- | --- |
| `Think failures present: failed=1` | Evaluator mismatch over a real recovered operational incident | Wave 19 attempt one failed after three 180-second provider timeouts; attempt two completed the same 25-member batch, all 45 required batches succeeded and all queues drained | Operational degradation and proof gap, not a hard failure |
| `mention_opportunity_without_detection_fate=10325` | Observability/fate-accounting integration defect, plus evaluator-denominator weakness | All 10,325 heuristic opportunities across all 1,125 signal observations lack a governed detection/rejection fate; this does not prove 10,325 missed gold entities | Hard failure remains |
| `resolver_mutated_identity_registry=50` | Real runtime authority/semantic defect; row-level attribution is only partially preserved | The evaluator serialized 50 `resolver_worker` alias creations while governed traces claim zero identity-registry mutation | Hard failure remains |

The corrected evaluator-only rerender remains `not_credible`: score `0.8883`,
coverage `1.0`, and two hard failures. This did not rerun or modify the
simulation. The original append-only `think_runs_failed=1` counter remains in
the report alongside `think_failures_recovered=1` and
`think_failures_terminal=0`.

## Evaluation Boundary

This was the one authoritative large run requested for this milestone. No
second large run was performed.

The input boundary began after connector transport. The test injected
simulated, normalized, source-attributed signals into PostgreSQL. It did not
test Slack listeners, Jira polling, OAuth, webhooks, source backfills or
delivery durability.

The database began with zero semantic memory:

| Surface | Pre-wave count |
| --- | ---: |
| Models | 0 |
| Model edges | 0 |
| Pattern candidates | 0 |
| Latent-gap hypotheses | 0 |

The scenario materializer did create structural scaffolding before wave one:

| Scaffold | Count |
| --- | ---: |
| Tenant | 1 |
| Actors | 20 |
| Commitments | 28 |
| Goals | 10 |
| Decisions | 10 |
| Resources | 14 |
| Entity aliases | 91 |
| Foundation observations | 63 |

This is therefore a **zero-semantic-memory cold start**, not a completely blank
organization. That distinction matters.

## Run Contract

All requested execution constraints were satisfied:

- 1,125 signals;
- exactly 45 T1 batches;
- exactly 25 members and 25 trigger observations in every batch;
- no unbatched T1 execution;
- fresh, non-append tenant;
- `seed_models=0`;
- zero pre-first-wave semantic memory;
- 45/45 batches eventually successful;
- all three background-noise batches processed through the batched fast path;
- final trigger queue drained;
- final post-commit queue drained;
- final topology work drained.

## System-Level Scorecard

| Dimension | Score | Coverage | Interpretation |
| --- | ---: | ---: | --- |
| Hidden-pattern recovery | 0.8561 | 1.0000 | Useful patterns formed, but independent thesis recovery was only 5/9 |
| Temporal improvement | 0.9350 | 1.0000 | Later evidence changed memory and graph state |
| Entity/model quality | 0.9376 | 0.8333 | Internal proxies were strong, but entity truth was not established |
| Learning/correction lift | 0.9976 | 1.0000 | Sealed learning components remained strong |
| Operational drain | 0.7596 | 1.0000 | Work drained, but provider failure and poor efficiency reduced confidence |
| Proof completeness | 0.9864 | 1.0000 | Most named evidence was present |

The high continuous score is useful for locating strength. It cannot override
the hard failures.

## What Worked

### 1. Cold-start learning was real

The first batch retrieved 25 observations and zero Models. The system then
created the first durable Models. By the late waves it reused memory heavily:
the final nine non-noise waves created only six Models while updating twenty.

Across T1 batches:

- Model inserts: 86;
- Model updates: 119;
- edge operations: 247;
- representation repairs scheduled from T1: 5;
- latent gaps created from T1: 4.

Final canonical memory:

- 85 active Models;
- 1 archived Model;
- 140 total edges, 137 active;
- 60 beliefs;
- 16 predictions;
- 10 norms;
- no exact duplicate natural-language Model groups.

### 2. Noise abstention was excellent

Waves 20, 30 and 40 were pure background-noise batches. Every one:

- contained 25 signals;
- retrieved zero Models;
- retrieved zero observations;
- made zero Model inserts;
- made zero Model updates;
- made zero edge mutations;
- avoided an LLM call through the noise fast path.

This is strong evidence that batch processing does not automatically manufacture
company memory.

### 3. Temporal learning existed

The run included 50 future-validation events. Later evidence:

- touched earlier memory 37 times;
- updated existing Models;
- confirmed lifecycle state;
- created nine future edge operations;
- archived one Model;
- exercised predictions, evidence attachment, staleness review, ambiguity
  review, resources and question policy.

This proves that the system does not merely summarize the current batch. It can
carry memory forward and revise it.

### 4. The graph runtime was mechanically healthy

The final graph had:

- eight edge kinds;
- no self edges;
- no orphan edges;
- no duplicate directed edges;
- 124 actionable active edges;
- a largest connected component containing 55 of 85 active Models;
- graph-cycle prevention that correctly rejected one unsafe `supports` edge.

### 5. Operational quiescence was achieved

The final adaptive drain completed in one cycle:

- 242 post-commit actions processed;
- zero post-commit failures;
- zero dead-lettered post-commit actions;
- zero post-commit pending;
- 65 topology items completed;
- zero topology failures;
- zero pending triggers.

## Why The Run Is Not Credible

## 1. Entity grounding did not execute end to end

Vitals reported:

`entity_grounding.mention_opportunity_without_detection_fate = 10,325`

This does not mean 10,325 gold entities were definitely missed. It means the
evaluator generated 10,325 `(observation, phrase)` opportunities across 1,125
eligible observations and found:

- zero entity-grounding work items;
- zero detection heads;
- zero detection records;
- zero grounding traces.

The denominator is noisy. It includes real entity names alongside phrases such
as `Enterprise`, `Evidence`, `Local`, `Long-horizon`, `Protect`, `T1`, `It`,
`The team` and `the renewal`.

The failure covers every signal observation, not one source or one difficult
Slack corner: 1,125/1,125 eligible observations have at least one unfated
opportunity. Slack accounts for 2,867 opportunities across 336 observations:
executive 665, risk 540, customer escalations 431, contradictions 427, noise
300, implementation 231, aliases 156, and general messages 117. Non-Slack
sources are also fully implicated: Salesforce accounts contributes 688,
finance email 647, Zendesk 630, and security email 612 opportunities. The fault is therefore
a missing cross-source protocol integration, even though Slack remains the
harder semantic extraction surface.

Two failures are therefore present simultaneously:

1. **Runtime/wiring failure:** none of the opportunities received a governed
   detection or terminal fate.
2. **Evaluator-denominator weakness:** broad heuristic opportunities are not a
   gold precision/recall population.

Classification: the absence of detection/rejection receipts is an objective
fate-accounting defect. Whether legacy extraction missed or correctly handled
any particular phrase is unresolved because this run did not preserve a gold
mention population or bridge legacy entity refs into the new detection
protocol. The gate is valid for protocol closure and invalid as a recall
estimate.

The hard gate is correct as a total-fate integration gate. It must not be read
as entity-extraction recall.

### Required correction

- Wire every eligible opportunity through
  opportunity -> detection head -> detection/rejection fate.
- Split protocol-fate coverage from gold entity precision, recall, boundary
  accuracy, linking accuracy and abstention quality.
- Type opportunity classes and exclude obviously non-entity heuristic noise
  from extraction-quality denominators.

## 2. The resolver mutated canonical identity truth

Vitals reported:

`entity_grounding.resolver_mutated_identity_registry = 50`

Database inspection confirms 50 canonical aliases with
`entity_metadata.source='resolver_worker'`:

- 11 customer references;
- 19 system references;
- 20 workstream references;
- spread across 13 source events;
- 46 alias strings were raw UUIDs;
- only four were human-readable strings.

No false merge was directly observed: each alias mapped one-to-one and no alias
string mapped to multiple referents. The proven defect is canonical-registry
pollution and premature promotion.

The saved scorecard preserves the count as `resolver_created_alias_count=50`
but only one coarse incident reference for the run. The 11/19/20 type split,
13 source events, and 46 UUID-like strings came from the contemporaneous
database inspection summarized here; the exported artifact set does not retain
the 50 row receipts needed to independently recompute that distribution. This
is a second observability defect, but it does not erase the serialized mutation
count or make direct resolver promotion safe.

The resolver is bypassing the intended candidate/adjudication boundary. The
evaluator's generic `identity_registry_mutation_count=0` alongside
`resolver_created_alias_count=50` shows why resolver-originated promotions need
their own immutable receipts instead of relying on governed-trace counters.

### Required correction

- Prohibit resolver writes to canonical `entity_aliases`.
- Allow the resolver to create candidate identity evidence only.
- Require explicit promotion/adjudication authority for canonical aliases.
- Reconcile every alias mutation to an immutable promotion trace.
- Fail closed on untraced identity-registry writes.

## 3. Retrieval never became Model-first

The desired behavior was:

1. observation-heavy retrieval while no Models exist;
2. gradual compression into Models;
3. mature Model-first retrieval;
4. raw observations reopened only for provenance, contradiction, uncertainty,
   correction or missing evidence.

Observed phase behavior:

| Phase | Models retrieved | Observations retrieved | Average Model share |
| --- | ---: | ---: | ---: |
| Early, waves 1–15 | 234 | 305 | 0.4095 |
| Middle, waves 16–30 | 260 | 260 | 0.5000 |
| Late, waves 31–45 | 278 | 280 | 0.4982 |

The Model-share slope was only `+0.004182` per wave. The evaluator correctly
classified this as `flat_mixed_retrieval`.

In mature normal waves, the system commonly selected exactly twenty Models and
twenty historical observations. Late non-noise waves referenced:

- about 31.3% of selected Models;
- about 65.6% of selected observations.

Fourteen late raw-observation reopenings were detected, but none carried a
recorded reopening reason.

This is the opposite of the desired memory metabolism. The system learns Models
but continues to reason primarily from a large raw-evidence floor.

### Required correction

- Replace the fixed observation floor with an outcome-gated policy.
- Require an explicit reason for every mature raw-observation reopening.
- Penalize repeatedly selected but unused Models and observations.
- Reserve observation slots for provenance, contradiction, novelty,
  uncertainty and correction.
- Measure selected-versus-referenced Model and observation ratios per wave.

## 4. Hidden-pattern proxies overstated real understanding

The structural scorer reported:

- average latent-pattern score: `0.9833`;
- concrete latent Model coverage: `9/9`;
- average best pattern coverage: `0.9722`.

The independent thesis judge recovered only `5/9` hidden theses.

Correctly recovered:

- Borealis confidence contradiction;
- Cobalt security packet;
- DeltaFleet capacity slip;
- Keystone bespoke drag;
- Northstar off-sensor discount bridge.

Incomplete or wrong:

- Atlas found security/procurement pressure but missed concurrent usage decline;
- FoundryWorks found recurring connector failures but missed the specific
  churn/policy-change causal thesis;
- Runway missed the cash-runway/hiring/controls tradeoff;
- alias ambiguity never formed the unified “resolve identity before strong
  graph mutation” rule.

The current latent scorer rewards keyword groups, edge presence and existence
of a concrete pattern. It does not adequately penalize missing causal clauses
or cross-storyline contamination.

### Required correction

- Make independent thesis correctness the primary hidden-pattern metric.
- Penalize incomplete causal chains.
- Penalize attribution to the wrong company object or storyline.
- Add counterfactual variants that preserve terms but break the causal
  relationship.

## 5. Model scope was batch-wide instead of claim-local

Every final Model had between nine and twelve scoped entities:

- average scope size: 11.73;
- 84 of 86 Models had at least ten scoped entities.

The repeated maximum of twelve strongly indicates that the batch/context entity
set is copied into individual claims rather than selecting only claim-local
entities.

Consequences:

- unrelated Models appear relevant to a storyline;
- evidence support counts inflate, reaching 240 events for one Model;
- graph construction sees false overlap;
- projections refresh unrelated subjects;
- confidence calibration is contaminated;
- evaluator scores are artificially optimistic.

### Required correction

Distinguish:

- entities explicitly mentioned by the claim;
- entities decisive to the proposition;
- contextual entities;
- entities merely retrieved in the batch.

Only the first two should normally become durable Model scope.

## 6. Canonical memory contained control-plane language

Observed canonical Models included:

- wrapper text such as “Evidence window containing 25 source signals...”;
- “Question-policy learning...” as a company situation;
- incomplete fragments such as “The missing implementation-owner handoff.”

This means prompt, benchmark or inquiry-control language can leak into company
truth.

### Required correction

- Reject wrapper, evaluator, prompt and inquiry-policy language at admission.
- Require self-contained grammatical propositions.
- Store questions, missingness and reasoning instructions in inquiry/residual
  surfaces, not as factual Models.
- Add contamination gold and negative controls.

## 7. Directional graph semantics were unsafe

Mechanical graph shape was healthy, but direction-sensitive semantics were not:

- eight reciprocal `early_warning_for` pairs, comprising 16 of 48 such edges;
- four reciprocal `blocks` pairs, comprising 8 of 25;
- one reciprocal `contradicts` pair;
- reciprocal edges often shared identical explanations.

The 23 `blocked_workstream` relation frames produced a suspiciously templated
shape:

- 23 `blocks`;
- 23 `early_warning_for`;
- 22 `contributes_to_resolution`.

This suggests participant roles are being flattened into an unordered cluster
during projection.

Other graph concerns:

- 11 active `supports` edges lacked explanations;
- 29.4% of Models remained isolated;
- graph-selected context failed its relationship contract in 3 of 79 runs;
- 578 relationship candidates accumulated, with 454 still candidates and 83
  needing review;
- accepted edge creation was far more common than retirement or explicit
  uncertainty.

### Required correction

- Define source and target role contracts per asymmetric edge kind.
- Reject reciprocal asymmetric edges unless independently justified.
- Stop projecting `early_warning_for` from every blocked-workstream frame.
- Keep uncertain edges as candidates/review state.
- Repair or retire existing reciprocal edges.

## 8. Confidence was materially overcalibrated

Expected calibration error was `0.2442` over 165 future-validation samples.

| Confidence band | Observed accuracy |
| --- | ---: |
| 0.5–0.6 | 0.5588 |
| 0.6–0.7 | 0.3400 |
| 0.7–0.8 | 0.3871 |
| 0.8–0.9 | 0.5676 |
| 0.9–1.0 | 0.6154 |

The system is systematically overconfident, particularly in the 0.6–0.9
range.

### Required correction

- Calibrate by proposition kind, source class and lifecycle stage.
- Separate evidence strength, entity-resolution confidence and causal
  confidence.
- Cap confidence at empirically supported accuracy.
- Recompute calibration after claim-local scope is fixed.

## 9. Repair and projection metabolism was expensive

Run cost and duration:

- elapsed time: 4,841.8 seconds, about 80.7 minutes;
- total reported cost: approximately `$1.56`;
- total LLM calls: 74;
- T1 product-path cost: approximately `$1.23`;
- T4/background cost: approximately `$0.325`;
- T1 p95 wall time: approximately 121 seconds;
- maximum batch wall time: approximately 866 seconds due to provider retries.

T4 processed 67 members in 21 batches and saved an estimated 46 calls through
batching. Its value was uneven:

- latent-relationship review: 14 durable outcomes from 14 calls;
- open-question search: 1 durable outcome from 3 calls;
- representation repair: 2 durable outcomes from 7 calls.

Projection metabolism produced:

- 6,969 refresh jobs;
- 261 snapshots;
- roughly 26.7 jobs per final snapshot.

The projection queue drained reliably, but it was highly amplifying.

### Required correction

- Coalesce projection work by subject, family and wave.
- Add per-repair-kind ROI budgets.
- Prefer deterministic repair for validator-known failures.
- Stop repeatedly retrieving large context packets for no-op T4 work.

## 10. The failed Think attempt was recovered but expensive

Wave 19’s first run exhausted three 180-second Codex app-server attempts. The
same trigger was retried at the run level and succeeded. Therefore:

- no T1 batch was terminally lost;
- all 45 batches eventually succeeded;
- the queue drained;
- the historical failed attempt is real reliability and cost evidence.

Treating any recovered attempt as an unconditional hard failure was an
evaluator bug. The benchmark already encoded the correct contract and its unit
test explicitly accepts a required T1 batch that recovers. The aggregate gate
was blindly promoting the append-only Vitals attempt counter. The evaluator now
distinguishes:

- terminal/unrecovered failure;
- recovered batch failure;
- individual provider-call retries;
- retry cost and latency.

The run is still penalized operationally, but the recovered failure is no
longer conflated with missing company learning. The rerender records 84
successful Think runs, one historical failed run, one recovered failure, zero
terminal failures, two validation errors, and an operational score of `0.7833`.

## Product and Projection Findings

Positive:

- company-intelligence score: `0.9354`;
- product-value proxy score: `0.9431`;
- projection freshness: `0.9444`;
- five of six major entity projection families populated;
- no failed or pending projection refresh jobs.

Limitations:

- commitment projection was missing;
- internal product metrics exceeded 1.0 in several places, indicating
  normalization defects;
- no real user decision-quality or customer-outcome oracle was run;
- projection success measured durable processing more strongly than actual
  consumption or decision usefulness.

The product score is useful as an internal proxy. It is not customer-value
proof.

## Residual Uncertainty

Final uncertainty surfaces included:

- 80 residual evidence rows;
- 20 latent-gap candidates;
- zero measured human-feedback requests in the large simulation;
- dark-matter-loop score: `0.5122`;
- coherence-repair score: `0.4000`.

The system detects missingness, but the simulation did not prove that this
uncertainty is routed to a human and closed.

## Root-Cause Hypotheses

The most likely structural causes are:

1. batch-level entities are propagated into each claim;
2. full-window evidence is attached to Models rather than decisive evidence;
3. relation-frame participants lose roles during projection;
4. retrieval budgets are fixed rather than outcome-adaptive;
5. hidden-pattern scoring rewards vocabulary and topology more than causal
   completeness;
6. future-validation scoring rewards processing and memory contact more than
   truth adjudication;
7. confidence is derived from reasoning certainty without empirical
   recalibration;
8. projection jobs are scheduled per mutation rather than coalesced per
   affected projection.

## Prioritized Path Forward

### P0 — Trust boundary

1. Wire the governed mention-detection fate protocol into the simulation
   runtime.
2. Prohibit resolver mutation of canonical aliases.
3. Enforce claim-local Model scope.
4. Add directional and reciprocal-edge invariants.
5. Reject control-plane text from canonical Models.

### P1 — Learning quality

6. Make mature retrieval Model-first and require raw-evidence reopening reasons.
   **Bounded post-fix pass:** nine genuine batches now measure early observation
   share `1.0`, late Model selection `8/11`, late actual Model reference `0.8`
   and reopening-reason coverage `1.0`. The historical run remains unchanged.
7. Use independent thesis correctness as the primary hidden-pattern measure.
8. Replace temporal contact proxies with explicit claim lifecycle outcomes.
9. Calibrate confidence against later evidence.
10. Close latent gaps through a measured human-feedback path.

### P2 — Efficiency and product proof

11. Coalesce projection refreshes.
12. Govern T4 repair by measured ROI.
13. Add the missing commitment projection.
14. Fix evaluator rates that exceed 1.0.
15. Measure actual projection consumption, decision quality and customer
    outcomes.

## Post-fix evidence reconciliation

This section updates the remediation state; it does not alter the run above or
its `not_credible` verdict.

- Entity physics now has objective entity v5. It binds audited broad-v4
  extraction (40 signals in four genuine batches, exact F1 `0.970588`, type
  accuracy `1.0`, negative cleanliness `1.0`, workstream `6/6`) independently
  from the positive DB vertical and adversarial v2. The latter rejects four
  harmful graph writes without mutation, exercises a two-hop chain and
  immediate correction propagation. V5's broad component scores `0.990196`
  and readiness is clear. The missing pre-call runtime-source digest,
  post-holdout current-runtime generalization, and wider graph populations
  remain explicit gaps.
- The first real company-model ablation v2 and postfix v3 are preserved as
  failures. Each runs matched three-by-six batches; both arms recover `0/3`,
  lift is zero, learned ECE is `0.5725`, score `0.7 below_policy`. V3 proves the
  SAGE seam fix selects three then six prior Models but references none.
- Development v4 (`ce6ea870`) closes that use seam with a generic summary
  consumer that cannot see hidden truth. Learned selects/references exactly
  `0/0`, `3/3`, `6/6`, recovers `3/3` versus frozen `0/3`, lift `1.0`, ECE
  `0.1925` versus `0.5725`, Brier `0.037056` versus `0.327756`, score `1.0`.
  This is versioned development evidence, not a new untouched generalization
  run. V2/v3 remain part of the discovery trail.
- Normalized source equivalence passes at `1.0` across two semantic cases and
  eight Slack/email/Jira/document-meeting batches while preserving provenance
  and conversational boundaries. Connector behavior and source drift remain
  outside scope.
- Correction homeostasis passes its real-Postgres bounded proof at `1.0`: two
  corrections, eight fenced Models, eight reevaluation pairs, two cycle-write
  rejections, idempotent replay and exact restart stability.
- The governing SHA-bound bounded objective artifact observes all eight
  independently mandatory components, including joined runtime, matched
  feedback quality and strict single-Model synthesis. Coverage,
  observed-component score and coverage-adjusted score are `1.0`, with no
  below-policy component or blocker; verdict
  `meets_bounded_policy`. Its proof gaps explicitly retain non-open-world,
  non-customer, no-connector and unbounded-recovery limits. It does not alter
  this historical large-run verdict.

## What We Can Claim

The run supports this bounded claim:

> Fyralis can process a large batched simulated company stream from zero
> semantic memory, create and evolve Models and relations, recognize several
> hidden operating patterns, learn from later evidence, abstain on pure noise
> and drain its learning/control work.

## What We Cannot Claim

The run does not establish that:

- entity extraction is accurate or complete;
- identity resolution is safely governed;
- Model scope is semantically precise;
- the graph is directionally trustworthy;
- hidden-pattern recovery is consistently causal and uncontaminated;
- confidence is calibrated;
- mature retrieval primarily uses compressed Models at company scale (the
  bounded nine-batch proof passes, but the authoritative run does not);
- customer-facing decisions improve;
- production connectors or transport are durable;
- open-world company understanding is reliable.

## Final Assessment

This is not a failed architecture. It is a successful metabolism with an
untrusted semantic boundary.

The cold-start loop, durable memory, temporal update machinery, abstention,
batching and operational drain are real. The next phase should not add more
autonomous behavior. It should make the company model substantially harder to
pollute:

- every entity opportunity gets a governed fate;
- no resolver silently creates canonical identity truth;
- every Model has claim-local scope and clean company language;
- every directional relation preserves roles;
- mature reasoning prefers compressed Models;
- raw evidence is reopened only for an explicit reason;
- hidden-pattern success requires recovering the actual causal thesis.

Until those conditions hold, the system can learn—but we should not yet fully
believe what it has learned.

## Later bounded evidence does not revise this run

The later integrated company-learning v2 vertical passed all 17 joined-runtime
checks on six bounded batches, including exact relation-ID correction fencing
and material prior-Model synthesis with an ablation. It is required in the
current objective evidence portfolio. This is new bounded capability evidence,
not a rerun of the 45-batch simulation and not grounds to change this
postmortem's `not_credible` verdict. The current failure-fate-corrected and
proof-boundary-aware rerender is
`/private/tmp/fyralis-authoritative-45-be401f25/autonomous-learning-cold-start-45-be401f25/authoritative_evaluation_postfix_entity_v5/large_company_simulation_evaluation.json`
(SHA-256 `cae61794…dfa77`). It includes objective entity v5 and objective
company learning v8; intermediate rerenders are historical, not the current
aggregate truth.

The later matched feedback-quality DB proof is also now mandatory and distinct
from the older SAGE salience-effect evidence. In two matched arms it applies one
governed correction only to the adaptive arm, then runs three identical later
two-signal batches per arm. Adaptive later conclusion quality is `1.0` versus
frozen `0.0`, with exact Model/relation lineage, immutable matched source truth,
tenant isolation, and all 17 objective checks passing. The seven-component
composition `/private/tmp/objective_company_learning_evidence_v7.json`
(`785ec239…f30911d7`) is preserved as a historical checkpoint. The current
eight-component composition is
`/private/tmp/objective_company_learning_evidence_v8_boundaries.json`. The
saved 45-batch artifact was rerendered with this evidence in the aggregate
lineage above. The current rerender score is
`0.8883`, coverage is `1.0`, and its verdict remains `not_credible` because the
two company-physics hard failures remain. It reports 73 aggregate proof gaps
and 16 successful scope limitations separately as proof boundaries. The
simulation was
not rerun: it remains the sole 45-batch, 1,125-signal, zero-semantic-seed,
batch-only large run.

The active v7 holdout establishes collective cross-batch facet availability,
not strict synthesis into one learned Model. The separate frozen v1 synthesis
holdout now establishes the stricter claim: each of three new subjects produced
exactly one persisted complete Model with exact prior-Model lineage across six
batch-only, zero-seed batches; the frozen arm recovered none. Distributed facets
across Models do not count. This is now the eighth mandatory component in
`/private/tmp/objective_company_learning_evidence_v8_boundaries.json` (file
SHA-256 `9816c876…25706`, composition SHA `a6b9c9a5…67e125`). Its
successful bounded scope is emitted as a `proof_boundary`, not a `proof_gap`.
The 45-batch run was only evaluator-rerendered; its company state remains
unchanged. Strict synthesis evidence does not repair or overwrite either
historical company-physics incident. No second large simulation was run or
authorized.
