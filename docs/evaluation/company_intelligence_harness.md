# Company Intelligence Evaluation Harness

This harness answers a stronger question than "did the end-to-end run pass?"

It asks whether Fyralis is behaving like a durable company cognition layer:
given noisy company signals over time, does it build a smaller, truer, more
useful model layer, and does that model layer improve future reasoning?

## Code Locations

- Benchmark runner:
  `scripts/run_storyline_batch_benchmark.py`
- Long-term vitals proposal:
  `docs/evaluation/company_understanding_vitals_harness.md`
- Unit tests:
  `tests/unit/test_storyline_batch_benchmark.py`
- Real-run reports:
  `tests/real_llm/reports/runs/<run_id>/`
- Main JSON field:
  `storyline_scores.json -> company_intelligence_scorecard`
- Main markdown section:
  `benchmark_summary.md -> Company Intelligence Scorecard`

## Design Principle

A large run is not automatically a proof. A proof needs to show that important
conditions occurred and that each condition went the way the system intends.

The harness therefore separates:

- scenario scale: number of signals, storylines, seeded models, batches
- component health: trigger drain, validation errors, latency, review debt
- intelligence quality: hidden patterns, compression, retrieval usefulness,
  reasoning value, edge intelligence, temporal improvement, robustness, and
  efficiency
- relationship accountability: graph-selected context becomes durable edges,
  N-ary relation frames, ontology-gap proposals, stronger model mutations, or
  explicit no-edge rationales
- product value: decisions, memory lifecycle, prediction lifecycle,
  counterfactual traps, latent bridge inference, compression loss, negative
  learning, question policy, and customer account health
- proof gaps: important paths the run did not exercise

This matters because a run can drain successfully while still failing to prove
resource ops, predictions, model archival, negative memory, future validation,
or ontology-gap behavior.

## Scenario Shape

The storyline benchmark uses planted company storylines. Each storyline has:

- fragmented evidence distributed across 20-30 signals
- future validation signals that arrive after the initial memory-building waves
- hidden thesis stored in gold metadata, not leaked into signal text
- latent pattern concept groups, such as security/evidence, usage/decay,
  procurement/wait, renewal/risk
- expected terms, actions, relationships, customers, commitments, goals, and
  decisions
- unobserved transition gaps where before/after state changes imply an
  off-sensor interaction without revealing the cause until future validation

Signals should be evidence fragments, not answers. The system gets credit only
when it compresses fragmented evidence into concrete durable Models.

## Scorecard Dimensions

The top-level score is a weighted combination:

| Dimension | Weight | Meaning |
| --- | ---: | --- |
| Memory Truth | 0.18 | Hidden company truths become evidence-backed Models and expected durable edges. |
| Compression | 0.12 | Meaning is preserved with bounded model growth and useful updates. |
| Retrieval Usefulness | 0.14 | Later reasoning receives useful compressed context, not mostly raw observations. |
| Reasoning Value | 0.16 | Think creates useful situations, recommendations, and low review debt. |
| Edge Intelligence | 0.12 | Registered edge kinds and N-ary relation frames are created, precisely chosen, accepted, projected, and evolved before new ontology is proposed; graph context is not left only in prose. |
| Temporal Improvement | 0.14 | Prior memory improves later reasoning, especially on future validation waves. |
| Robustness | 0.09 | Batches complete, queues drain, validation passes, noise is safely ignored. |
| Efficiency | 0.05 | Low trigger amplification, LLM calls, cost, and latency. |

The scorecard deliberately includes proof gaps. A high score with major proof
gaps should still be treated as "not fully proven."

## Product Value Evals

The scorecard also includes:

`company_intelligence_scorecard.product_value_evals`

These evals are orthogonal to raw pipeline health. They ask whether the system
is approaching product value, not merely completing work.

| Eval | What It Proves |
| --- | --- |
| Decision Impact | Hidden understanding turns into recommendations, actions, and resource decisions. |
| Memory Lifecycle | Models are updated, evidenced, archived, merged, and touched by future evidence. |
| Prediction Lifecycle | Forecasts become durable Predictions and later evidence validates, updates, or retires them. |
| Counterfactual Trap | Noise, alias ambiguity, and contradictory evidence do not create harmful durable memory. |
| Latent Bridge Inference | Irregular state transitions create bounded inferred Models without fabricated specifics. |
| Compression Loss | Compressed Models preserve hidden company patterns without raw observation replay. |
| Negative Learning | The system learns what not to retrieve, ask, or amplify. |
| Question Policy | The system learns when to ask, when not to ask, and what missing context matters. |
| Customer Value | Intelligence lands in account-health objects: scoped customer memory, precise edges, and useful recommendations. |

Each eval emits:

- `score`
- `metrics`
- `findings`
- product-value-specific `proof_gaps`

The product-value score is intentionally not folded into the existing weighted
Company Intelligence score yet. It is a second lens: useful for deciding what
the next end-to-end run actually proved and which missing behaviors deserve
system work.

## Proof Gaps

The harness currently reports gaps such as:

- T1 batch timeout before a Think run exists
- no future validation events
- expected edge kinds not observed as accepted durable edges or projected
  relation-frame structure
- precise registered edge kinds are underused
- N-ary relation frames are not exercised
- graph-selected context failed the relationship contract
- graph-selected context never produced durable relationship ops
- future validation did not evolve or reconfirm durable edges or relation frames
- latent bridge inference missing, unsupported, overconfident, or fabricating
  off-sensor details
- ontology-gap ops occurred while registered expected edge kinds were missing
- future validation did not use compressed Model/graph context
- prediction memory barely exercised
- resource ops untested
- ontology-gap write path untested
- model archival/staleness cleanup untested
- evidence attachment behavior untested
- negative memory untested
- question-policy learning untested
- topology optimizer skipped references to non-canonical/generated model IDs
- trigger queue did not drain

These gaps are not test failures by themselves. They are a map of what the run
did not prove.

## Running

Build-only smoke check:

```bash
.venv/bin/python scripts/run_storyline_batch_benchmark.py \
  --mode build-only \
  --signals-per-storyline 20 \
  --future-validation-signals-per-storyline 3 \
  --noise-signals 5 \
  --run-id storyline-batch-buildonly-check
```

Reusable seeded baseline for retrieval optimization:

```bash
.venv/bin/python scripts/run_storyline_batch_benchmark.py \
  --mode seed-only \
  --run-id retrieval-opt-seed-5000 \
  --target-t1-batches 0 \
  --seed-models 5000 \
  --seed-families 100
```

Before any expensive Codex-backed append validation, run the retrieval hot-path
probe against the seeded tenant:

```bash
.venv/bin/python scripts/run_storyline_batch_benchmark.py \
  --mode retrieval-probe \
  --append-to-run-id retrieval-opt-seed-5000 \
  --run-id retrieval-opt-seed-5000-probe \
  --target-t1-batches 0 \
  --retrieval-probe-max-ms 1000 \
  --skip-migrations
```

This probe is intentionally LLM-free and non-mutating. It exercises focused
answerability, focused scoped sparse lookup, focused direct scope lookup, and
SAGE answerability over static noisy cases plus the tenant's highest-DF sparse
and answerability terms. The probe checks both latency and minimum non-empty
recall for positive cases, so a path cannot pass merely by returning quickly with
no useful rows. Treat a failed probe as a blocker for a full batch run; fix the
hot path first, then rerun the probe.

By default, `retrieval-probe` fails if it cannot find scoped Model sidecars,
because that means the focused scope paths were not exercised. Use
`--retrieval-probe-allow-missing-scope` only for tiny local smoke tests, not as a
gate before a full E2E run.

Before spending another full real-LLM batch on a previously reported run, use
the artifact-only rerender gate. It recomputes the scorecard from saved
`run_summary.json`, `waves.json`, and storyline scores using the current harness
logic, then emits an explicit `rerun_readiness` block:

```bash
.venv/bin/python scripts/run_storyline_batch_benchmark.py \
  --mode rerender-report \
  --append-to-run-id projection-delta-10batch-final-20260630 \
  --run-id projection-delta-10batch-final-20260630-rerender-current
```

The rerender mode does not touch Postgres and does not call an LLM. Treat
`rerun_readiness.ready_for_fresh_10batch=false` as a blocker for another
expensive batch; run the emitted targeted DB proof command first. The optional
capability/noise canary starts at horizon batch 8 so it exercises
`capability_probe_wave_009` and `background_noise_wave_010`; it is only a live
health smoke and does not replace the DB assertions for noise negative-memory or
question-policy stats.

Then run repeated append validations without paying the 5k-model seed cost:

```bash
RUN_REAL_LLM=1 LLM_PROVIDER=codex CODEX_TRANSPORT=cli \
.venv/bin/python scripts/run_storyline_batch_benchmark.py \
  --mode run \
  --append-to-run-id retrieval-opt-seed-5000 \
  --run-id retrieval-opt-validation-5batch-a \
  --target-t1-batches 5 \
  --signals-per-storyline 20 \
  --seed-models 0 \
  --downstream-steps-per-wave 0 \
  --adaptive-drain-cycles 1 \
  --adaptive-drain-steps-per-cycle 0 \
  --skip-topology-optimizer
```

Do not pass `--cleanup` to the seed-only baseline if you want to reuse it. Append
runs intentionally mutate the reused tenant, so this workflow is fast and useful
for iterative retrieval validation, not a clean identical A/B baseline for every
run. Use a fresh seed-only run when you need an untouched baseline.

Do not try to clean only an append run out of a reused tenant yet. The appended
observations and Think runs can be identified, but Think may also revise
pre-existing Models and relationship structures. Current Model rows do not carry
a benchmark `run_id`, and these revisions are not fully reversible from the
benchmark harness. Treat append runs as cumulative until the system has explicit
run-scoped provenance or tenant clone/restore support.

Real LLM run:

```bash
RUN_REAL_LLM=1 LLM_CACHE_BYPASS=1 \
.venv/bin/python scripts/run_storyline_batch_benchmark.py \
  --mode run \
  --target-t1-batches 400 \
  --signals-per-storyline 25 \
  --future-validation-signals-per-storyline 3 \
  --noise-signals 25 \
  --seed-models 5000 \
  --seed-families 100 \
  --t1-batch-window-s 0.1 \
  --t1-batch-min-size 20 \
  --t1-batch-max-size 30 \
  --downstream-batch-window-s 1.0 \
  --downstream-batch-min-size 2 \
  --t2-batch-max-size 8 \
  --t4-batch-max-size 4 \
  --downstream-steps-per-wave 8 \
  --worker-poll-batch 6
```

With `--target-t1-batches 400`, the harness switches into long-horizon mode.
It creates exactly 400 T1 waves and uses `--signals-per-storyline` as the
per-wave signal count. With the command above, that means 10,000 signals total:
400 batches x 25 signals. The sequence mix includes repeated storyline waves,
numbered future-validation waves, and numbered background-noise waves so the
run tests long-term memory compounding, drift, cleanup, and retrieval health.

## Reading The Report

Start with:

1. `company_intelligence_scorecard.overall_score`
2. `company_intelligence_scorecard.interpretation`
3. `company_intelligence_scorecard.dimensions`
4. `company_intelligence_scorecard.product_value_evals`
5. `company_intelligence_scorecard.proof_coverage`
6. `company_intelligence_scorecard.proof_gaps`

Then inspect storyline-level scores:

- `latent_pattern_score`
- `latent_pattern_evidence_supported_model_count`
- `latent_pattern_best_coverage`
- `missing_latent_pattern_groups`
- `recommendation_model_count`
- `scoped_edge_count`
- `needs_review_candidate_count`

The strongest evidence that the system is approaching its true potential is not
"many models were written." It is:

- hidden patterns are represented as concrete Models
- those Models are evidence-backed
- future validation retrieval uses those Models instead of historical raw observations
- future reasoning changes because the compressed memory exists
- registered edge kinds like `blocks`, `weakens`, `explains`, and
  `contributes_to_resolution` are used precisely, either directly or through
  projected relation-frame structure, before proposing new ontology
- multi-party business relations keep their role semantics as relation frames
  instead of being flattened into vague binary links
- graph-selected context becomes an edge, relation frame, ontology-gap proposal,
  stronger model mutation, or auditable `no edge` rationale
- irregular before/after state transitions become explicitly inferred bridge
  Models with bounded confidence, not invented hallway-level details
- later phases evolve or reconfirm durable edges and relation frames instead of
  leaving one-shot graph guesses
- the system avoids harmful graph mutations under ambiguity
- review debt stays bounded
- useful decisions arrive with low amplification

## Current Limitations

The current harness is a strong first scorecard, but not the final proof.

The current future-validation wave proves only one later phase. To fully prove
temporal improvement, a scenario should contain multiple company days: early
weak signals, later outcomes that reveal the truth, and assertions that the
system used earlier compressed memory before the outcome became obvious.

Next extensions should add:

- day-by-day company simulation
- required and forbidden Models per phase
- required and forbidden edges per phase
- future retrieval assertions
- prediction outcome validation
- correction/retraction scenarios
- explicit stale-memory and archival expectations
- negative-memory expectations
- question-policy learning expectations
