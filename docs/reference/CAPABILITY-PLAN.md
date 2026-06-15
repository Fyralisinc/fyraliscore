# Fyralis Core — Capability Plan

Last reviewed from code: 2026-06-10

## What this document is

The companion to `ARCHITECTURE-REPAIR-PLAN.md`. That plan makes the system
*structurally sound* (layering, dead code, queues, embedding provenance). This
plan answers the second question: **what makes the system its most powerful
self** — true, calibrated, compounding beliefs about a company, rendered into a
CEO loop that gets faster every week.

It is the output of a second multi-agent study (24 agents) with a different
instrument from the first: instead of reading import graphs, it mined the
recorded evaluation artifacts (storyline benchmark runs, the truss scenario,
the synthesis harness, 247 recorded real-LLM runs), ran the offline benchmarks,
audited every learning loop end-to-end, and stress-tested the ontology against
ground-truth facts. Three competing investment portfolios (belief-quality-first,
decision-loop-first, evaluation-flywheel-first) were then adversarially judged
on impact realism, founder feasibility, and measurement honesty. The plan below
is the synthesis. Every headline claim was re-verified by hand against source
on 2026-06-10.

**Assumes the repair plan lands.** Several items below are gated on its Move 2
(de-fang ModelsRepo / embedding lifecycle) and Move 4 (housekeeper wires the
dormant workers). They are marked.

---

## 1. Epistemic state: what the project actually knows about its own quality

This section exists because two numbers that look like measured system
performance are not, and steering by them would be steering by fiction.

### 1.1 The truss "results" are authored, not measured

`truss_run/` and `truss_run_2/` look like end-to-end runs of the Think pipeline
over a 120-day simulated company, with headline numbers (90%/92% prediction
accuracy, 17/18 fact coverage). Forensics say otherwise — **they are an
LLM-authored scenario in which the same author wrote both the company's signals
and the "observer's" belief stream**:

- All 45 run-1 `model_id`s are **uuid5** (deterministic, name-based — verified:
  45/45 have version nibble `5`). The runtime assigns random DB UUIDs. Run-2
  "model_ids" are human-written slugs (`m_seriesb_timeline_pull`).
- The entire 60-day run-1 artifact set was written in **~31 minutes of wall
  clock** (file mtimes 11:06–11:37 on 2026-05-01) — impossible for the real
  LLM-per-trigger worker at tenant concurrency 1.
- `summary_stats.json` is arithmetically impossible (run 1: "10 predictions
  evaluated" vs 9 correct + 1 early + 4 falsified = 14 outcomes). Run-2's
  manifest claims 354 signals; 681 are on disk.
- The run-2 "continuation" has **zero memory carry-over**: 0/45 run-1 models
  are touched by any of the 209 run-2 events.

**Consequences:** (a) the 90%/92% accuracy numbers must never be cited as
system performance; (b) **the repo contains zero measured end-to-end Think
quality** — the system's core competency has never been evaluated; (c) the
*scenario data itself* is excellent (99%/95% of evidence refs resolve, 983
labeled signals, 8 ground-truth checkpoints, 308 typed state deltas) and is the
best benchmark asset the project owns; (d) it is **gitignored and machine-local**
(`.gitignore:57-58`) — a lost laptop deletes it. Commit it.

### 1.2 The flagship benchmark score is contaminated

The storyline / Company-Intelligence benchmark (June 9 long-horizon run:
**CI 0.8879, "strong_company_intelligence"**, 10k signals, $10.92) is the best
harness in the repo — and its truth-scoring is structurally unable to measure
truth:

- Scoring is **substring matching** of `expected_terms` that the benchmark
  itself planted into the signal text
  (`scripts/run_storyline_batch_benchmark.py:1654-1661`).
- The gold label is **leaked**: `storyline_id` and `storyline_title` are
  written into reasoner-visible observation content (`:823-827`, `:903-907`,
  3 more sites) and the scorer keys on it (`:1632`).
- A fluent *wrong* belief that reuses the planted vocabulary scores identically
  to a true one.
- Two same-day runs scored **0.58 vs 0.79** under config changes, with no
  variance bound — so no storyline delta between commits is currently
  interpretable.
- The LLM judge identity is env-dependent and unpinned
  (`benchmarks/fyralis_eval/judge.py:57`) — judged numbers don't compare across
  machines or weeks.

### 1.3 What IS genuinely measured (and is concerning)

| Measurement | Value | Source |
|---|---|---|
| Isolated models (no edges) in best real run | **92.0%**; largest connected component 1.5% | run 20260609T220653Z `run_summary.json` |
| Graph-selected context failed its own relationship contract | 174/543 think runs | same run, proof gap |
| Scoped retrieval (model-layer probe, 15k models) | expected_scope_hit_rate **0.467**, failure_case_rate 0.933 | run 20260608 |
| Stress retrieval: cases reaching full expected-model coverage | 12/50 (24%) | retrieval stress run |
| question_policy adaptation events across 10k signals | **0** (dimension score 0.42) | run 20260609T220653Z |
| Calibration | ECE 0.266 at **n=4**, dated 2026-05-06, never re-run — the only calibration number in the repo | `tests/synthesis_harness/baselines/calibration.json` |
| Retained regression baselines | **zero** (`benchmarks/baselines/` empty; `reports/generated/` gitignored) | verified |
| Quality-replay cases | 5/5 are `known_failure` mode — the net asserts nothing | `tests/quality_replay/cases/` |
| relationship_candidates accumulated, promotion path | 3,553 candidates, no promoter (~296 judged truly promotable) | same run + critic audit |

And one stranded asset: `corpora/pelago` — 67,055 signals over 270 simulated
days with 9 monthly ground-truth snapshots — has **zero consumers**; its
evaluators (LSOB L1–L6) left the repo in the demo-overlay split.

---

## 2. The capability ceilings (ranked)

What actually caps how good this system can get, independent of bugs. The
structural-repair plan does not touch most of these.

### C1 — The system is entirely feed-forward: no error signal ever returns *(critical, hard)*

All 8 learning loops audited end-to-end are open, inert, or aspirational:

- **Approve/Discuss/Not-now — the founder's daily labels — are captured then
  discarded.** `dismiss_recommendation`
  (`services/product/recommendations/handlers.py:346-404`) *requires* a reason,
  archives the model, emits a `recommendation_dismissed` state change — and
  grep finds **zero consumers** of that signal anywhere in reasoning or
  ranking. `act_on_recommendation` never marks the recommendation correct,
  never bumps supporting models. Ranking stays `impact*confidence` forever,
  regardless of 5 approvals of pattern A and 15 dismissals of pattern B.
- Ask/card conversations are write-only audit traces (zero readers anywhere).
- `omitted_evidence`, `reader_activations` are pure observability — nothing
  consumes them to adjust retrieval.
- Calibration and prediction-resolution loops have never closed in any
  deployed topology (repair plan D5 wires the workers; this plan must verify
  outcomes actually accrue and feed back).

The founder's daily usage is the only data that encodes the true value
function, and the system throws it away. This is the single
highest-compounding gap.

### C2 — The memory graph does not compound: 92% isolated nodes *(critical, measured)*

The product's premise — connected, compressed memory beats per-item RAG —
requires a connected graph. Pathway G (weighted up to 0.52 for model
re-evaluation triggers) traverses near-nothing; situations and `model_trace`
get stubs; contradiction machinery has nothing to propagate over. Causes:
reconciler is dedup-only; retrieval scores claims independently; 8/15 edge
kinds essentially never produced (`predicts`=0, `alternative_to`=0, `causes`=1
across a 300-file scan); 3,553 relationship candidates with no promotion path;
and edges can't be mined from fake-hash similarity (repair plan D4).

### C3 — Truth is unjudgeable and nothing remembers baselines *(critical, meta)*

§1.2 + §1.3. This caps the improvement *rate* of everything else: the de-facto
optimization target is lexical mimicry, and regressions through refactor waves
(7 port waves merged the week of June 9) are undetectable.

### C4 — Confidence is uncalibrated and conflates truth with liveness *(high)*

Single float [0.05, 0.95]; no source-attributed disagreement; no second-order
beliefs ("Maya doubts the deadline" is unrepresentable). Even the authored
reference trace is incoherent: a *successfully resolved* prediction dropped
0.95→0.02. The Uncertainty band — the surface the founder scrolls to daily —
is a drift heuristic (`|confidence_at_assertion − confidence| > 0.1`,
`snapshot.py:716-745`) **padded with anomalies to guarantee 3 cards even when
nothing is genuinely uncertain**, while the workers that would make drift mean
something never ran. Forecasts renders bins over predictions that barely form
(`prediction` claim role: 7 production usages) and never resolve.

### C5 — Single-shot reasoning over statically-planned context *(high)*

One LLM pass per trigger (retries are transport/parse only), no validation-
failure retry (dead-letter only), context planner hardcodes `mode='deep'`,
fixed per-trigger pathway weights, no recency decay, no importance weighting —
a high-stakes contradiction gets the identical treatment as a routine note.
Median 9 models selected from ~490 retrieved evidence items. With C2+C3
unfixed, **context selection is the de-facto belief bottleneck** — whatever
retrieval misses, the belief permanently lacks.

### C6 — The ontology cannot represent what a CEO needs *(high, hard)*

Unrepresentable today: conditional commitments ("deliver X if Y" flattens to
one path), quantitative trends/rates (ARR +10% MoM becomes a static fact; no
runway arithmetic), belief TTL (Q2 plans stay active in Q3), contract-as-entity
(renewal dates and feature gates live in freeform dicts), and contradiction
settlement (no resolve op, no `disputed` state — both models stay active
forever). Meanwhile the edges advice actually needs — `gates`, `triggers`,
`fulfills_condition`, `timeline_of` — don't exist, while 8 of 15 existing
kinds go unused.

### C7 — The product's value surface is invisible to every eval *(medium)*

No harness touches greeting/today/recommendations/decision_deltas/forecasts —
only `ask/orchestrator.py` is wired into any benchmark
(`fyralis_db.py:37`). The storyline `decision_impact` 0.94 **counts that
recommendation-kind rows exist in the DB**, not whether the rendered surface is
right, ranked well, or buried. The founder's value definition (low cognitive
load, time-to-map, time-to-decide) has no metric anywhere.

---

## 3. The plan

Three tranches. The shape comes from the strongest cross-portfolio consensus
the judges produced: **instruments first (they're days, not weeks), then
measurements that price the big decisions, then capability investments — each
of which must name its measurement before it's built.**

### Tranche A — Instruments (~1 week, <$10 LLM, mostly agent-delegable)

Each item pays for itself alone; all are independent of the repair plan.

| # | Action | Why / what it buys |
|---|---|---|
| A1 | **Commit the truss scenario data today** (fix run-2 manifest counts 354→681; relabel artifacts as authored-scenario, not results) | The best labeled long-horizon dataset exists on one laptop, gitignored |
| A2 | **Truth gate**: stop writing `storyline_id`/`storyline_title` into reasoner-visible content (map at score time via `external_id`); add a thesis-recovery judge pass — feed each `StorylineSpec.thesis` (gold-only, never leaked) + the run's relevant models to the existing `LLMAnswerJudge`; **pin judge identity** in `run_config`; add a fixed ~20-item judge-agreement set. ~9 judge calls/run, <$0.10 | The core product claim ("true beliefs") becomes judgeable; the 0.89 stops rewarding lexical mimicry. Expect the headline score to drop — that drop is the finding |
| A3 | **Variance band**: re-run the identical 225-signal scorecard config 3× ($0.29/run) — *with a cache-off arm* (the critics caught that cache-on measures pipeline determinism, not run variance) — and publish min/max/stddev. This number is the **kill-switch**: if stddev ≈ 0.1, storyline is a release gate, never a per-PR gate | Without it, no storyline delta between commits is interpretable (0.58 vs 0.79 same-day spread) |
| A4 | **Regression memory**: run the existing bm25/lexical lanes over longmemeval_s (500), lme_v2, hotpotqa, halumem (all on disk); commit each `metrics_summary.json` into the empty `benchmarks/baselines/`; add a CI job (toy + stress10 + storyline build-only + one bm25 lane — all verified offline-green, zero keys/docker) that diffs recall@k. Verify the gate by deliberately perturbing a scoring constant | 14k lines of adapter code become a working canary; refactor waves stop being able to silently regress retrieval |
| A5 | **Price two big decisions with A/Bs on the existing DB lane**: (a) `--embedding-mode hash` vs `ollama` on lme_v2-small (23,946 cached vectors make the ollama leg nearly free) → the measured value of repair-plan D4; (b) `fyralis_sage_reader` vs base lane → the first lift metric for 16.4k lines of synchronous hot-path SAGE code (decides timeout-hardening vs flag-off) | Both lanes already exist (`run_benchmark.py:45-46,101,106`); hours of compute, near-zero spend |

One critic amendment adopted as a standing rule: **judged rates carry n and
binomial confidence intervals, and nothing gates on deltas inside the
interval.** A one-time founder-labeled validation set (~30 thesis/edge
judgments) grounds the judge before it arbitrates anything contentious.

### Tranche B — Measurements that gate investment (~2 weeks, gated on repair-plan Moves 2+4)

| # | Action | Decision it gates |
|---|---|---|
| B1 | **Calibration at usable n**: extend `score_storylines` to bucket confidence against future-validation-wave outcomes and emit per-run ECE (pure SQL); expand synthesis-harness labeled scenarios 4→~20; verify deadline_resolver/calibration_updater actually accrue `resolution_outcome` rows once deployed (sample-audit resolved predictions for *correctness*, not just count>0) | Any Uncertainty-band redesign; any confidence-representation change (the diff-expressiveness verdict: no probabilistic logic without exactly this harness) |
| B2 | **Truss replay adapter** (`benchmarks/adapters/truss_adapter.py` on the longmemeval_v2/`fyralis_db` pattern): map signals→`ObservationCreate`, materialize, drain `think_trigger_queue`, grade models against the GT checklist **filtered to signal-derivable facts (filter frozen in a committed file before the first scored run)** and acts against the 308 typed snapshot deltas (commitment-completion detection rate + lag). ~3-5 days + ~$1-3/run | **The first real end-to-end Think number in the project's history.** Gates all Think-architecture spending (multi-pass reasoning, cascade triage, concurrency). The acts-detection rate is the first quality measurement under Approve/Discuss/Not-now |
| B3 | **Memory-compounding probe**: replay truss run-1 then run-2 into the *same* tenant vs fresh-tenant run-2-only; compare month-4 fact coverage + Ask QA. Pre-register which GT facts *require* run-1 memory (else a null result is uninterpretable); add a scoped RAG-baseline arm | **The over-RAG premise — the most expensive architectural intuition in the repo** (its only current support is a proxy-scored 0.55). Also gates the pelago decision: positive → build PelagoAdapter for the 9-month corpus; negative/null → architecture review, and pelago moves to the overlay repo |
| B4 | **Golden-day CEO-surface eval**: after each storyline/truss materialization, invoke the real `SnapshotComposer` + today/recommendations repos, serialize rendered surfaces, judge against planted gold: planted-risk coverage in top-10, surfaced-item precision vs noise, uncertainty-band correspondence to genuinely contested facts. Plus a **failure decomposition** per miss: belief *absent*, *present-but-unretrieved*, or *retrieved-but-unrendered* | The founder's value definition becomes measurable. The decomposition is the standing instrument that says whether belief quality or the loop binds — it re-allocates every subsequent sprint |
| B5 | **Chained Ask eval**: point the existing `FyralisAskReader`/`AskOrchestrator` lane (`fyralis_db.py:1151,1215`) at the storyline tenant; one gold thesis-question per storyline, judged (~$1/run) | "Memory is useful, not just present." Gates all Ask investment |

### Tranche C — Capability investments (each gated on its measurement)

In consensus priority order; every item names its gate.

1. **Close the Approve/Dismiss loop** *(attacks C1 — highest-compounding gap)*.
   A per-(kind, scope) stats table fed by the already-emitted
   `recommendation_dismissed`/acted-upon state changes (reasons are already
   persisted — this is a consumer job, not instrumentation); ranking consumes
   it as a **bounded, decaying** down-weight on dismissed patterns and a
   positive prior on acted-upon ones; `act_on_recommendation` bumps
   `confirmed_count` on supporting models. Guardrail: approved-kind suppression
   stays ~0; n=1-founder labels adjust *ranking only*, never belief content.
   *Gate: B4 exists so the change is verifiable; measured by same-kind
   regeneration rate post-dismissal and golden-day recommendation precision.*
2. **Graph compounding** *(attacks C2)*. Post-D4, re-mine edges over real
   embeddings; a housekeeper job promotes high-confidence relationship
   candidates (~296 truly promotable today) and prunes stale ones.
   *Success metric is thesis-recovery and latent_bridge_inference (0.598
   baseline), NOT connectivity — the critics killed isolated_model_ratio as a
   target because edge spam can max it while beliefs degrade. Edge-precision
   sample: 30 judged edges/run.*
3. **Honest uncertainty** *(attacks C4)*. Re-base the Uncertainty band on real
   epistemic events — contestation overrides, prediction
   resolutions/overdues (accruing via B1), `contradicts`-edge disputes — each
   card carrying *why* (the contesting model, the resolving observation);
   delete the anomaly back-fill that fabricates cards. Add the cheap
   `disputed` 3-state edge status from the diff-expressiveness verdict.
   *Gate: B1 shows outcome data actually accruing. Measured by golden-day
   uncertainty-correspondence and P(archived within 7d | card shown).*
4. **Consequence preview on Approve/Discuss/Not-now** *(attacks C7/decide-loop)*.
   ~200-400 LOC composing affected commitments from the *deterministic*
   acts-layer typed joins (`contributes_to`/`depends_on`/`constrained_by`), a
   cascade dry-run, and linked predictions — it does not depend on the
   fragmented LLM-mined graph. *Gate (critic-mandated): a one-day SQL count of
   acts-edge density on the dogfood tenant first — if the substrate is as
   sparse as the benchmark run suggests (3 `blocks`, 2 `contradicts` edges per
   10k signals), this renders empty and the effort goes to item 2 instead.*
5. **Ask writeback** *(attacks C1)*. Founder-accepted Ask conclusions persist
   as models through the standard diff/validator path (capped confidence,
   falsifier required, conversation provenance) — memory gains the one input
   it structurally lacks: what the CEO cares about this week. *Gate: lands
   after the repair plan's domain command seam (the critics flagged the
   collision); measured by B5 and decision-provenance models/week > 0.*
6. **Retrieval tuning** *(attacks C5)*. Post-D4 only: offline grid search over
   RRF k and the hard-coded `_TRIGGER_WEIGHTS`, add recency decay; promote
   winners via existing env vars; re-run the scope probe (success = materially
   above 0.467). *Gate: A4 baselines committed (tuning against fake embeddings
   optimizes the wrong landscape).*
7. **Targeted ontology additions** *(attacks C6)* — only the three cheap,
   verdict-backed ones: `disputed` edge status (above), informal second-order
   beliefs (`hypothesis` + `about_model_id` + doubter actor — zero schema),
   `scope_temporal.valid_until` enforcement in retrieval (one query
   predicate, auto-fades stale beliefs). *The full expansion (conditional
   commitments, quantitative trends, contract-as-entity, temporal edge kinds)
   waits for B2/B4 to show which gaps actually bind — 8/15 existing edge kinds
   are already unused; adding vocabulary to a system not using its vocabulary
   inverts the priority.*

---

## 4. Explicitly rejected (for now)

Consensus rejections across all three adversarially-judged portfolios:

- **Multi-pass / self-critique Think reasoning** — multiplies LLM COGS per
  trigger while the measured evidence (scope_hit_rate 0.467; 9 models selected
  from ~490 retrieved) says *context selection* binds, not reasoning depth.
  Re-enters only with B2 numbers in hand.
- **Probabilistic confidence (Bayesian posteriors, subjective logic)** — B1 is
  the harness that decides; representation changes only if ECE stays bad after
  calibration actually runs.
- **PelagoAdapter 9-month replay now** — ~$70+ LLM and multi-day wall clock at
  concurrency 1; gated on B3 showing carried memory helps at 120 days at all.
- **Production telemetry (scroll depth, dwell)** — n=1 user; months to
  significance vs same-day signal from golden-day evals against planted gold.
  Revisit at multi-tenant.
- **A fifth eval stack / external eval framework** — the gap is retention,
  truth-judging, and wiring, not framework count; four stacks (~20k lines)
  already exist.
- **Greeting synthesis tier before the graph compounds** — a synthesis tier
  over 92%-isolated beliefs renders prettier lists. Ships together with
  Tranche C item 2, against B4's coverage metric.
- **SAGE hot-path hardening before its lift is measured** — A5(b) decides flag-off
  vs harden. Independent of the outcome: delete `model_predictions` (0
  production imports, tables never written), `outcome_evaluator` (test-only,
  55KB), `lsob/` residue, `simulation/__pycache__`.

---

## 5. How this changes the answer to "most powerful self"

The structural repair plan makes the system *trustworthy to change*. This study
found that its *power* is capped by three compounding absences, none of which
is a feature:

1. **It cannot learn** — every signal that could teach it (founder actions,
   prediction outcomes, retrieval omissions, conversations) is discarded (C1).
2. **Its memory does not compound** — 92% of beliefs are disconnected islands,
   so the "company map" is a recency-filtered list, not a map (C2).
3. **It cannot tell whether it is improving** — its flagship score rewards
   vocabulary mimicry over leaked labels, its only end-to-end "results" are
   authored fiction, and nothing retains baselines (C3).

The most powerful self is reached not by adding reasoning horsepower but by
closing loops in this order: **judge truth → remember baselines → calibrate
confidence → connect the graph → feed the founder's daily actions back in**.
Each Tranche C capability lands inside instruments that can prove it worked —
which, for a system maintained by one founder and AI agents, is the only kind
of progress that compounds.

## 6. Methodology and scores

9 inventory readers (eval machinery — which ran the offline benchmarks live;
truss forensics — quantitative artifact analysis; Think cognition; diff
expressiveness; ontology fitness vs ground-truth facts; SAGE triage; retrieval
IR audit; learning-loop closure audit; product-loop fitness vs the founder's
value definition) → 3 synthesis lenses (capability ceilings, measurement
foundation, product power) → 3 competing portfolios → 9 adversarial critics.

| Portfolio | Impact (1-10) | Feasibility | Critic consensus |
|---|---|---|---|
| Evaluation flywheel | 4–6 | 7–8 | Most evidence-honest; fund Tranche-A core immediately; it buys provability, not direct power |
| Decision loop | 5–7 | 5–7 | Loop-closure verified on the consumption side; flagship surfaces gated on substrate-density pre-checks |
| Belief quality | 5–7 | 6–7 | Instruments half real and cheap; substrate half over-claimed (296 promotable candidates, not 3,553; contradiction class observed twice) |

The plan above is the intersection the critics converged on: the flywheel's
Tranche A, the measurement layer all three independently proposed (every
portfolio contained the truth gate, the variance band, committed baselines, the
truss adapter, and the golden-day eval — proposed from three different
theses), and the decision-loop portfolio's capability items resequenced behind
their gates.
