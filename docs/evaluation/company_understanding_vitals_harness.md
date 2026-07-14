# Company Understanding Vitals Harness

Status: proposal

This document defines the empirical harness Fyralis needs to understand its
own long-term company-intelligence health.

The existing Company Intelligence harness asks the right scorecard question:
given noisy company signals over time, does Fyralis build a smaller, truer,
more useful model layer, and does that model layer improve future reasoning?
The vitals harness proposed here is the measurement layer underneath that
scorecard. Its purpose is to show where information gains value, loses value,
becomes stale, becomes unsafe, becomes useful, or fails to close a loop.

## North Star

Each month of company activity should make Fyralis cheaper, sharper, safer, and
more useful for understanding the next month.

The harness should prove or falsify that claim across the whole lifecycle:

```text
signals
  -> observations
  -> triggers and inquiry
  -> retrieval packet
  -> Think reasoning
  -> validation and apply
  -> Models and edges
  -> projections and product surfaces
  -> human and decision outcomes
  -> future validation
  -> retrieval/Think/system self-improvement
```

The harness must preserve the system's model-first philosophy. Models are the
intended compression of noisy signals. Raw observations should not become the
default center of Think. Instead, the harness should measure whether compressed
Models retained the valuable information, and use raw observations as residual
audit evidence, counterevidence, falsification material, or gap diagnosis.

## Why Architecture Is Not Enough

Architecture tells us where value could flow. The harness must tell us where
value actually changes state.

The highest-value system fixes should be ranked empirically by measured leak:

- valuable signals that never become durable memory
- durable memory that is duplicated, stale, unsupported, or incoherent
- retrieval context that is selected but not used
- useful proposed operations dropped by validation or reconciliation
- Models that never influence later reasoning
- projections that lag the belief layer
- product surfaces that are correct but unused
- human corrections that never improve the system
- decisions that were supported by Fyralis but later proved wrong
- safety or provenance gaps that make coherent intelligence unsafe

## Existing Anchors

The harness should extend existing surfaces instead of replacing them.

- `docs/evaluation/company_intelligence_harness.md` already separates scenario
  scale, component health, intelligence quality, relationship accountability,
  product value, and proof gaps.
- `scripts/run_storyline_batch_benchmark.py` already writes storyline score
  reports and includes dimensions for memory truth, compression, retrieval,
  reasoning value, edge intelligence, temporal improvement, robustness, and
  efficiency.
- `scripts/run_1000_signal_model_layer_probe.py` already drives production
  ingestion, T1 Think, model-layer reporting, signal manifests, and graph
  health reports.
- `services/reasoning/think/observability.py` already records Think run
  lifecycle events, costs, validation drops, context-use grades, lock waits,
  cascade depth, and reconciliation decisions.
- `services/reasoning/think/quality_report.py` already asks whether selected
  memory, graph memory, and observations were actually used by successful Think
  diffs.
- `db/migrations/0151_think_representation_ledger.sql` already creates a
  representation ledger for whether an evidence window improved the company
  twin.
- `scripts/report_think_representation_health.py` already reports model
  coverage, evidence absorption, substrate readiness, truth-seeking, and
  company-question coverage.
- `db/migrations/0084_sage_inquiry_trace_gap_fillers.sql` already creates
  retrieval plans, omitted evidence, and inquiry outcome events, including
  events for later-requested omitted evidence, user-accepted nodes,
  user-contested nodes, model confirmation, model falsification,
  recommendation action, and recommendation ignore.
- `db/migrations/0193_model_events_and_projection_snapshots.sql` already
  defines the Model event stream and rebuildable projection snapshots.
- `services/domain/projections/runtime.py` and
  `services/domain/projections/router.py` already expose projection routing,
  checkpointing, refresh jobs, failures, direct matches, dependency matches,
  and watch matches.
- `tests/synthesis_harness/REPORT.md` already shows how to test individual
  structural stages while avoiding brittle exact assertions against the LLM's
  semantic choices.
- `tests/real_llm/README.md` already frames real-LLM checks as opt-in,
  production-shaped, tolerance-band tests across ingestion, Think, Acts, and
  Bridge.

## Harness Shape

Create a runner named either:

- `scripts/run_company_vitals_harness.py`, or
- an explicit `--vitals` mode inside `scripts/run_storyline_batch_benchmark.py`

The second option is less disruptive at first because the storyline benchmark
already owns planted gold, waves, run directories, scorecards, rerender mode,
and retrieval-probe mode. The clean long-term end state can still split the
vitals renderer into a reusable module.

### Modes

| Mode | Purpose | LLM | DB mutation |
| --- | --- | ---: | ---: |
| `artifact-rerender` | Recompute vitals from saved run artifacts. | no | no |
| `db-rerender` | Recompute vitals from an existing tenant/run in Postgres. | no | no |
| `seed-only` | Create planted company scenario and optional model corpus. | no | yes |
| `retrieval-probe` | Test hot-path retrieval, sidecars, omissions, latency. | no | optional |
| `think-drain` | Run ingestion through Think, validation, apply, and drain. | yes | yes |
| `projection-drain` | Drain model-event projection routing and refresh jobs. | no | yes |
| `product-probe` | Query Ask/Bridge/model trace/customer surfaces. | optional | optional |
| `future-validation` | Add later waves that confirm, revise, or falsify memory. | yes | yes |
| `human-feedback` | Simulate accepts, contests, answers, ignores, and acts. | no | yes |
| `decision-outcome` | Inject later business outcomes for prior recommendations. | no | yes |
| `stress` | Exercise concurrency, retries, locks, timeouts, noisy inputs. | optional | yes |
| `longitudinal` | Run multiple months/waves with drift and strategy changes. | yes | yes |

### Output Artifacts

Every run should write a single `vitals/` directory under the existing report
directory:

```text
tests/real_llm/reports/runs/<run_id>/vitals/
  vitals_run.json
  vitals_summary.md
  vitals_scorecard.json
  vitals_timeseries.jsonl
  signal_metabolism.jsonl
  trigger_trace.jsonl
  retrieval_trace.jsonl
  think_trace.jsonl
  validation_trace.jsonl
  model_delta.jsonl
  graph_coherence.json
  projection_trace.jsonl
  product_surface_trace.jsonl
  human_feedback_trace.jsonl
  decision_outcome_trace.jsonl
  self_improvement_trace.jsonl
  governance_trace.jsonl
  authority_safety_trace.jsonl
  db_trace_summary.json
  proof_gaps.json
  residual_trace.jsonl
  coherence_repair_candidates.jsonl
  retrieval_outcome_learning.jsonl
  latent_gap_candidates.jsonl
```

The `vitals_summary.md` file should lead with ranked findings:

1. highest measured value leak
2. highest operational chokepoint
3. highest coherence risk
4. highest product-value gap
5. highest long-term learning gap
6. strongest system behavior observed
7. next cheapest proof to run

### Current CLI

The initial implementation is additive and can run against any existing E2E
report directory:

```bash
.venv/bin/python scripts/run_company_vitals_harness.py \
  --report-dir tests/real_llm/reports/runs/<run_id> \
  --fail-on-hard-gates
```

For full signal-metabolism tracing, pass a live Postgres URL. The harness then
joins report `observation_id`s back through the DB:

```bash
.venv/bin/python scripts/run_company_vitals_harness.py \
  --report-dir tests/real_llm/reports/runs/<run_id> \
  --database-url "$DATABASE_URL" \
  --fail-on-hard-gates
```

To run the complete benchmark -> DB-backed vitals -> ranked-fixes lifecycle in
one command, use the full-loop wrapper. It streams benchmark output, kills the
benchmark subprocess if the parent-level timeout is reached, and only starts
vitals after the benchmark has emitted a final `report_dir`:

```bash
.venv/bin/python scripts/run_company_intelligence_loop.py \
  --database-url "$DATABASE_URL" \
  --benchmark-timeout 3600 \
  --print-summary \
  -- \
  --run-id <run_id> \
  --target-t1-batches 15 \
  --post-commit-batch-size 25 \
  --post-commit-batch-timeout 60
```

The final adaptive drain prints `adaptive_drain_cycle=...` records. Treat
`post_commit_timed_out=true` or nonzero `post_commit_pending` as a benchmark
health failure to fix before reading semantic vitals.

The DB-enriched path measures, when present:

- `think_trigger_queue`: direct and batched observation -> trigger lineage
- `think_runs`: trigger -> reasoning outcome, retrieval counts, context use,
  validation errors, and applied ops
- `models`: born/supporting observation provenance
- `model_signal_readings`: confirm/contest/observe/falsify evidence attachment
- `model_edges`: edge creation and evidence-event provenance
- `relation_claims` and `relation_instances`: relationship and N-ary frame
  provenance
- `model_events` and `projection_snapshots`: belief-event and product
  projection propagation
- `signal_routing_decisions`, `inquiry_sessions`, `inquiry_evidence_items`,
  `omitted_evidence`, and `inquiry_outcome_events`: retrieval/human/outcome
  feedback loops
- `pending_post_commit_actions`: post-commit work tied back to source triggers

## Core Ledger: Signal Metabolism

The most important new artifact is `signal_metabolism.jsonl`.

One row per planned or observed signal:

```json
{
  "signal_id": "storyline-wave-001-signal-003",
  "observation_id": "uuid-or-null",
  "storyline_id": "renewal-risk-security-evidence",
  "wave": "initial|future_validation|noise|outcome",
  "source_channel": "slack:customer-success",
  "gold_value_class": "core_fact|counterevidence|noise|decision|outcome",
  "gold_hidden_facts": ["fact_key_1"],
  "trigger_ids": ["uuid"],
  "think_run_ids": ["uuid"],
  "retrieved_model_ids": ["uuid"],
  "retrieved_observation_ids": ["uuid"],
  "applied_model_ids": ["uuid"],
  "applied_edge_ids": ["uuid"],
  "projection_subjects": ["customer:acme"],
  "product_surface_refs": ["bridge:customer_health:acme"],
  "final_fate": "model_updated",
  "fate_reasons": ["evidence_attached", "future_validation_confirmed"],
  "latency_ms": {
    "signal_to_observation": 12,
    "observation_to_trigger": 44,
    "trigger_to_think_started": 820,
    "think_to_model_delta": 14422,
    "model_delta_to_projection": 512
  },
  "leak_flags": [],
  "safety_flags": []
}
```

Allowed `final_fate` values:

- `noise_correctly_ignored`
- `raw_only_unmodeled`
- `trigger_pending`
- `think_failed`
- `think_noop_justified`
- `think_noop_suspicious`
- `validation_dropped`
- `model_created`
- `model_updated`
- `evidence_attached`
- `counterevidence_attached`
- `falsifier_created`
- `open_question_created`
- `relationship_candidate_created`
- `edge_created`
- `relation_frame_created`
- `projection_updated`
- `product_surface_updated`
- `human_feedback_requested`
- `decision_outcome_recorded`
- `self_improvement_event_created`
- `unsafe_or_unauthorized`

This ledger makes compression empirical. It should not ask "did every signal
become a model?" It should ask "did every valuable signal have the correct
fate?"

## Vitals Scorecard

The existing Company Intelligence scorecard should stay as the semantic
quality layer. The new vitals scorecard should sit beneath it:

| Vital | Question |
| --- | --- |
| Metabolism Yield | Do valuable signals reach the right durable state? |
| Compression Health | Do Models preserve important information with bounded growth? |
| Retrieval ROI | Is selected context used, not merely returned? |
| Reasoning Throughput | Does Think convert context into valid, useful mutations? |
| Model Atomicity | Are active Models small, supported, bounded, and falsifiable? |
| Company Object Spine Health | Do Models bind to existing durable company anchors like actors, customers, commitments, decisions, goals, and resources? |
| Model Coherence | Is the model layer a connected company map, not a claim pile? |
| Edge Specificity | Are graph edges specific, justified, and ontology-rich rather than generic? |
| Active Frontier Health | Is the active model frontier stable relative to signal volume and graph density? |
| Create/Update Balance | Does Think both create new Models and update/attach to existing memory? |
| Temporal Learning | Does future evidence improve prior memory? |
| Projection Freshness | Do product surfaces track the belief layer quickly and safely? |
| Product Utility | Do outputs help answer, decide, recommend, and prioritize? |
| Human Loop Closure | Do human corrections and accepts improve later behavior? |
| Decision Outcome Learning | Are supported decisions later judged and learned from? |
| Self-Improvement | Do retrieval and reasoning failures become durable system changes? |
| Dark Matter Loop | Do measured missing-signal gaps become latent hypotheses and human validation? |
| SAGE Policy Effect | Do SAGE outcomes change later retrieval, topology, or policy behavior? |
| Pattern Cascade | Do detected patterns create downstream model, edge, product, or decision effects? |
| Ask Signal Learning | Do Ask/user interactions improve questions, blind-spot handling, and future behavior? |
| Simplification Pressure | Is duplicate, orphan, isolated, or redundant structure visible for repair/removal? |
| Governance Health | Are stale, unsupported, ownerless, or unsafe beliefs controlled? |
| Authority Safety | Is every surfaced claim authorized and provenance-backed? |
| Control Plane Health | Do queues drain, locks stay bounded, and failures remain visible? |
| Efficiency | Are dollars, tokens, latency, and amplification buying useful state? |

## Metric Catalog

### 1. Run And Scenario Shape

Measurements:

- planned signals, inserted observations, seeded models, seeded entities
- storyline count, wave count, future-validation count, noise count
- gold hidden facts, expected relationships, expected customer facts
- run mode, provider, prompt epoch, git sha, migration state
- append run vs fresh run, tenant reuse, cleanup status

Insights:

- Prevents comparing incomparable runs.
- Separates small smoke runs from real long-horizon proof.
- Makes rerender results honest about what was not exercised.

### 2. Ingest And Substrate Readiness

Measurements:

- planned signal -> observation insertion ratio
- normalization failures
- duplicate external ids
- source-channel distribution
- actor/source alias coverage
- entity extraction coverage
- unresolved source actor refs
- source-root diversity
- canonical actors/customers/resources/commitments created
- substrate candidates by kind and lifecycle state
- observation repetition and unique ratio

Existing anchors:

- `scripts/report_think_representation_health.py`
- `services/domain/observations/repo.py`
- substrate candidate reports

Insights:

- A model-layer issue may really be an ingest/substrate issue.
- Thin actor/customer substrate makes all later scope and relationship reasoning
  look incoherent.

### 3. Signal Metabolism

Measurements:

- final fate by gold value class
- useful fate ratio for non-noise signals
- raw-only ratio for valuable signals
- suspicious no-op ratio
- validation-dropped valuable op ratio
- evidence-attachment ratio
- counterevidence/falsifier/open-question creation ratio
- signal-to-model latency
- signal-to-projection latency
- signal-to-product latency

Core formulas:

```text
metabolism_yield =
  valuable_signals_with_correct_fate / max(1, valuable_signals)

silent_loss_rate =
  valuable_signals_with_raw_only_or_no_trace / max(1, valuable_signals)

healthy_noise_suppression =
  noise_signals_correctly_ignored / max(1, noise_signals)
```

Insights:

- Reveals the highest-probability value leaks.
- Distinguishes healthy compression from lost information.
- Shows whether "more raw observations in Think" is needed or whether Models
  are failing to absorb value.

### 4. Retrieval And Inquiry

Measurements:

- retrieval plans by question and revision
- selected models, observations, graph models, projection evidence
- omitted evidence by reason
- omitted evidence later requested
- rank of planted gold facts
- sidecar coverage and stale sidecar ratio
- pathway contribution and pathway survival
- retrieval latency p50/p95/p99 by route
- context packet model/observation/projection ratio
- answerability and sufficiency status
- human-validation route selection
- second-pass activation rate

Existing anchors:

- `retrieval_plans`
- `omitted_evidence`
- `inquiry_outcome_events`
- `tests/retrieval_e2e/test_inquiry_e2e.py`
- `services/reasoning/think/context_use.py`

Insights:

- Shows when retrieval is finding the right material but packet selection is
  dropping it.
- Shows when omitted evidence becomes a later regret signal.
- Shows when raw evidence is required because compressed memory did not carry
  the needed information.

### 4A. Invariant Health Over The Model Layer

The vitals harness also acts as the retroactive diagnostic surface for the
Fyralis invariants. It should not become a separate harness. Instead, invariant
checks sit beside signal metabolism and reuse the same saved artifacts and
optional DB trace.

Measurements:

- wrapper-like, conjunction-heavy, broad-scope, unsupported, and unfalsifiable
  Models
- existing-anchor binding via `scope_actors` and `scope_entities`, without
  introducing a new universal-object layer
- object-like Model text that mentions customers, actors, commitments,
  decisions, or recurring events without matching scope bindings
- anchor richness: durable anchors that accumulate multiple related Models
- generic edge share, edge-kind diversity, missing edge explanations, and
  ontology-gap evidence
- active Models, active edges, isolated-model ratio, and model-to-observation
  compression ratio
- create/update/attach fate balance for resolved signal metabolism rows
- residual clusters, latent-gap candidates, and human-validation routing
- SAGE/topology completions, adaptive lifecycle score, experience metabolism,
  and negative-learning proxies
- pattern/recurrence signals that cascade into edges, relation frames,
  projections, decisions, or self-improvement events
- Ask/question-policy, negative-learning, and experience-metabolism proxies
- duplicate natural groups, duplicate edges, orphan edges, and isolated model
  pressure

Artifact-only mode should emit proxy scores only where saved reports are enough.
When DB lineage is missing, the harness should mark proof gaps rather than
pretending the invariant is proven. This lets old simulated runs diagnose graph
shape, model atomicity, edge specificity, frontier health, SAGE policy effects,
and simplification pressure while reserving create/update balance, residual
absorption, and human-validation completion for DB-backed rerenders.

Core formulas:

```text
model_atomicity =
  avg(supported_share?, falsifier_share?, bounded_scope_share,
      non_wrapper_share, non_conjunction_heavy_share)

edge_specificity =
  avg(1 - generic_edge_share, explained_edge_share,
      edge_kind_diversity_score, confident_edge_share?)

company_object_spine_health =
  avg(model_anchor_binding_share, anchor_diversity_score,
      object_mention_binding_score?, anchor_richness_score,
      customer_presence_score?)

create_update_balance =
  1 - abs(create_share - 0.5) * 2

dark_matter_loop =
  avg(latent_bridge_score?, question_policy_score?,
      residual_to_gap_conversion, gap_to_human_route_share?)
```

Insights:

- Keeps the invariant list operational without creating another scorecard.
- Lets simulated runs be rerendered after the fact as the harness improves.
- Separates "not proven by this artifact" from "measured and unhealthy."
- Keeps universal company objects as existing anchors that Models should bind to,
  not as a second canonical object system.
- Turns model/edge cleanup, SAGE policy closure, and Dark Matter completion into
  ranked findings instead of architectural taste.

### 5. Prompt And Token Economics

Measurements:

- prompt tokens by section
- model-card tokens
- raw-observation tokens
- projection/context tokens
- instruction/reasoning-frame tokens
- selected context referenced by committed diff
- unused selected model ids
- unused graph model ids
- unused selected observation ids
- LLM cost per applied model update
- LLM cost per useful edge
- LLM cost per product-surface improvement

Core formulas:

```text
context_roi =
  useful_applied_ops / max(1, prompt_input_tokens)

model_context_roi =
  referenced_selected_models / max(1, selected_models)

raw_residual_roi =
  referenced_selected_observations / max(1, selected_observations)
```

Insights:

- Defends the model-first thesis quantitatively.
- Flags raw evidence bloat only when it fails to buy useful operations.
- Flags over-compressed Models when raw residuals are repeatedly required to
  recover facts Models should have retained.

### 6. Think Reasoning, Validation, And Apply

Measurements:

- Think runs by trigger kind and lane
- success, failed, skipped-idempotent
- retrieval model/observation counts
- LLM latency and cost
- proposed ops by type
- validation drops by reason and op type
- deterministic injected ops
- reconciliation decisions
- idempotent skips
- apply errors
- cascade depth and invariant violations
- no-op reason quality
- context-use grade

Existing anchors:

- `think_runs`
- `applied_triggers`
- `think_representation_ledger`
- `reconciliation_events`
- `services/reasoning/think/observability.py`
- `services/reasoning/think/quality_report.py`

Insights:

- Finds whether value dies in reasoning, validation, reconciliation, apply, or
  post-commit work.
- Prevents blaming retrieval when validation is dropping the useful ops.

### 7. Model Compression Health

Measurements:

- active, archived, inactive model counts
- model inserts vs updates vs evidence attachments
- observations per active model
- claim token growth per observed token
- duplicate and near-duplicate clusters
- near-duplicate absorptions
- model specificity: missing actor/entity scope
- supporting event distribution
- high-confidence unsupported models
- active models without falsifiers
- model_signal_readings confirms/contests/contextual readings
- source digest count
- model adaptiveness

Core formulas:

```text
compression_ratio =
  active_models / max(1, observations)

evidence_absorption =
  evidence_attachments / max(1, valuable_signals)

duplicate_pressure =
  duplicate_or_near_duplicate_models / max(1, active_models)
```

Insights:

- Shows whether Models are doing the compression work.
- Catches both failure modes: model explosion and over-compression.

### 8. Model-Layer Coherence

Measurements:

- active edges, accepted edges, candidate edges
- evidence-backed edge ratio
- isolated model ratio
- graph component distribution
- expected edge-kind coverage
- relation claims and relation frames
- N-ary relation-frame usage
- contradiction and weakening relations
- ontology-gap operations before registered edge kinds are used
- relationship candidate lifecycle: proposed -> clarified -> promoted -> stale
- model trace back/forward path length and authority-filtered path gaps

Existing anchors:

- `model_edges`
- `relation_claims`
- `projection_snapshots`
- `services/product/model_trace/repo.py`
- storyline edge-intelligence score

Insights:

- Shows whether Fyralis is building a company map or a bag of isolated claims.
- Exposes relationship debt that architecture diagrams often hide.

### 9. Temporal Learning

Measurements:

- future signals that touch prior models
- future signals that confirm prior models
- future signals that revise prior models
- future signals that falsify prior models
- predictions created, resolved true, resolved false, inconclusive
- old models retrieved in later waves
- old graph context used in later valid diffs
- stale models archived or downgraded
- negative memory later prevents repeated bad retrieval
- open questions later answered

Core formulas:

```text
temporal_learning_score =
  weighted(confirmations + revisions + falsifications + useful_retrieval_reuse)
  / max(1, future_validation_signals)

future_context_reuse =
  future_runs_using_prior_model_or_graph_context / max(1, future_runs)
```

Insights:

- Proves whether memory improves future reasoning.
- Prevents mistaking accumulation for learning.

### 10. Projections And Product Surfaces

Measurements:

- model events emitted by type
- projection route direct/dependency/watch matches
- projection refresh jobs enqueued, leased, completed, failed
- projection checkpoint lag
- snapshot source_model_ids and source_event_ids coverage
- projection staleness
- product query hit rate on projections vs raw/model fallback
- Bridge/customer-health surface freshness
- recommendations/actions/resources surfaced
- product-surface provenance completeness

Existing anchors:

- `model_events`
- `projection_checkpoints`
- `projection_snapshots`
- `ProjectionRunner`
- `ProjectionRouteReport`

Insights:

- Shows whether product surfaces are current and causally tied to belief
  changes.
- Prevents a good model layer from hiding a stale product layer.

### 11. Human Feedback Loop

Measurements:

- human validation requested
- human answer received
- human accepted node/model/recommendation
- human contested node/model/recommendation
- ignored recommendation
- acted-on recommendation
- time to human response
- response incorporated into model layer
- response incorporated into retrieval/negative memory/question policy
- repeated ignored surfaces by type

Existing anchors:

- `inquiry_outcome_events` has event types for user accepts, user contests,
  recommendation acted on, and recommendation ignored.
- Inquiry tests already exercise human-validation routing.

Insights:

- Measures trust, usefulness, and correction flow.
- Reveals whether Fyralis learns from the organization or only from passive
  signals.

### 12. Decision Outcome Loop

Measurements:

- Fyralis-supported recommendation or decision
- decision owner and affected customer/project/resource
- action taken or ignored
- later outcome signal
- positive, negative, neutral, or inconclusive business result
- evidence supporting the outcome judgment
- model/recommendation revised after outcome
- future similar recommendation improved or suppressed

New scenario requirement:

- Add planted decision-outcome waves where a prior recommendation is later
  shown to be good, harmful, incomplete, or no longer relevant.

Insights:

- Separates coherent memory from useful judgment.
- Finds when Fyralis optimizes proxy metrics instead of business reality.

### 13. Organizational Change Memory

Measurements:

- strategy pivot detected
- customer segment shift detected
- org/team ownership changed
- product surface/feature renamed
- pricing/procurement motion changed
- competitor/market condition changed
- old model contradicted by new regime
- old retrieval motif suppressed after regime change
- phase-specific memory created

Scenario requirement:

- Longitudinal runs should contain explicit company phase changes, not only
  more of the same signal distribution.

Insights:

- Tests whether Fyralis can understand a changing company rather than freezing
  the first coherent story it learned.

### 14. Self-Improvement Loop

Measurements:

- retrieval route utility events emitted
- negative memory inserts
- discovery shortcut updates
- topology optimizer actions
- question-policy updates
- omitted evidence later requested
- validation failed due to missing evidence
- validation failed due to bad reference
- route utility improved on repeated cases
- failed retrieval path suppressed in future
- successful retrieval motif reused in future

Existing anchors:

- `inquiry_outcome_events`
- `services/reasoning/sage/outcome_evaluator.py`
- SAGE flywheel tests
- discovery shortcut and negative memory repos

Insights:

- Measures whether system errors become system improvements.
- Prevents optimizers from reinforcing a vague success scalar instead of the
  actual bottleneck.

### 15. Governance And Lifecycle

Measurements:

- stale active models
- ownerless important models
- unsupported high-confidence models
- active models without falsifiers
- predictions without later resolution
- archived/replaced/superseded models
- model merge/supersession chains
- open questions unresolved past window
- review debt and human-review queue size
- model age by confidence and usage
- last retrieved, last confirmed, last contested

Core formulas:

```text
governance_debt =
  stale_high_confidence
  + unsupported_high_confidence
  + unresolved_predictions
  + overdue_open_questions
  + human_review_backlog
```

Insights:

- Keeps memory from becoming a landfill.
- Forces the system to forget, downgrade, archive, ask, or revalidate.

### 16. Authority, Safety, And Provenance

Measurements:

- tenant isolation probes
- unauthorized evidence expansion
- cross-tenant distractor leakage
- authority-filtered model trace gaps
- derived access labels attached
- provenance edge coverage
- projection snapshot source_model_ids/source_event_ids coverage
- product-facing claim without source trace
- omitted evidence due to access denied
- authority cache invalidation coverage

Hard invariant:

- Unsafe coherence is failure. A correct answer that uses unauthorized evidence
  should fail the run.

Insights:

- Prevents the company model from becoming powerful but unusable.

### 17. Control Plane, Reliability, And Cost

Measurements:

- trigger queue depth by kind/lane
- trigger drain time
- pending triggers after run
- dead letters
- retry counts
- post-commit backlog
- topology optimizer pending/completed/failed
- region lock waits p50/p95/p99
- transaction duration
- LLM time inside transaction
- worker utilization
- timeouts
- run flake rate
- cost by trigger kind/lane
- cost per useful durable mutation

Existing anchors:

- `think_runs`
- `model_reeval_dead_letter`
- post-commit drain reports
- real-LLM flake tracking

Insights:

- Separates "benchmark scored well" from "system is healthy."
- Shows whether a semantic improvement is operationally affordable.

## Longitudinal Scenario Design

The long-horizon suite should use monthly waves:

1. **Month 1: cold start**
   - noisy company signals
   - fragmented evidence
   - seeded distractors
   - hidden gold not leaked into text

2. **Month 2: consolidation**
   - repeated patterns
   - customer risk becomes visible
   - recommendations should emerge
   - some noise should be suppressed

3. **Month 3: future validation**
   - prior Models confirmed
   - prior Models revised
   - prior Models falsified
   - predictions resolved or remain inconclusive

4. **Month 4: organizational change**
   - strategy shift
   - ownership changes
   - product/customer vocabulary changes
   - old retrieval motifs can become misleading

5. **Month 5: human loop**
   - user accepts some outputs
   - user contests some outputs
   - human answers previously open questions
   - ignored recommendations produce negative learning

6. **Month 6: business outcome**
   - decisions lead to measurable results
   - recommendations are judged good, harmful, obsolete, or incomplete
   - future recommendations should adapt

Each month should have:

- planted gold facts
- planted distractors
- expected model deltas
- expected edge/projection/product surfaces
- expected proof gaps if the current system cannot exercise a loop yet

## Hard Gates

The vitals harness should fail the run, not merely lower a score, when:

- trigger queue does not drain for required lanes
- required post-commit/projection queues do not drain
- dead letters exist for required work
- authority or tenant isolation leaks
- product-facing claims lack source provenance
- a high-value signal class has zero valid fates
- future-validation waves do not touch any prior memory
- all retrieval context is unused by committed diffs
- model-layer duplicate/orphan pressure exceeds threshold
- projection lag exceeds threshold for required product probes
- decision-outcome feedback is recorded but not incorporated into any later
  memory or policy surface

## Insight Report

The summary should emit plain-language diagnoses such as:

- "Signals are entering, but valuable evidence is staying raw-only."
- "Models compress evidence, but future waves do not reuse them."
- "Retrieval selects good Models, but Think ignores them."
- "Think proposes useful edge ops, but validation drops them."
- "The model layer grows, but graph coherence does not."
- "Projection routing works, but refresh jobs lag product surfaces."
- "Human feedback exists, but it does not change later retrieval or memory."
- "Recommendations are acted on, but outcomes do not train future behavior."
- "The system is semantically improving but operationally unhealthy."
- "The system is cheap because it is under-thinking, not because it is
  efficient."

## Implementation Plan

### Phase 0: No-Runtime-Change Renderer

Build `vitals/artifact-rerender` from existing outputs:

- `run_summary.json`
- `storyline_scores.json`
- `benchmark_summary.json`
- `models.jsonl`
- `model_edges.jsonl`
- `signal_manifest.jsonl`
- `waves.json`
- `planned_signals.jsonl`

Add optional DB reads when a tenant is available:

- `think_runs`
- `applied_triggers`
- `think_representation_ledger`
- `reconciliation_events`
- `retrieval_plans`
- `omitted_evidence`
- `inquiry_outcome_events`
- `model_events`
- `projection_checkpoints`
- `projection_snapshots`

Deliverables:

- `vitals_scorecard.json`
- `vitals_summary.md`
- initial ranked findings

### Phase 1: Signal Metabolism Ledger

Add a builder that maps:

```text
planned signal
  -> observation
  -> trigger
  -> think_run
  -> retrieval/context_use
  -> applied ops
  -> Models/edges/readings
  -> model events
  -> projection snapshots
  -> product probes
```

This is the highest-value addition. It turns "the model layer is incoherent"
into measurable fates and leak points.

### Phase 2: Missing Instrumentation

Add only the fields the ledger cannot reconstruct:

- explicit run/scenario id provenance on model mutations or event metadata
- validation dropped-op reason records tied to specific proposed ops
- projection refresh lag and product-surface usage records
- human feedback incorporation markers
- decision-outcome incorporation markers
- raw residual reason when Think needs observation evidence despite model
  context

### Phase 3: Longitudinal Scenario Pack

Extend planted storylines with:

- human accepts/contests/answers
- decision outcomes
- org and strategy changes
- product/customer vocabulary drift
- stale memory traps
- repeated recommendation motifs
- authority distractors

### Phase 4: Gates And Trend Baselines

Convert vitals into regression gates:

- hard safety/control gates
- semantic score thresholds
- cost and latency envelopes
- trend comparison against prior run
- A/B comparison for retrieval or prompt changes

### Phase 5: Monthly Company Twin Audit

Make the harness useful outside synthetic runs:

- run against a real tenant window
- compare current month vs prior month
- report new understanding, stale memory, unresolved questions, product-surface
  impact, human feedback closure, and decision-outcome learning
- keep sensitive details authority-filtered in exported artifacts

## First Implementation Target

The first useful PR should be small:

1. Add `docs/evaluation/company_understanding_vitals_harness.md`.
2. Add a pure artifact renderer module that reads an existing storyline report
   directory.
3. Emit `vitals/vitals_scorecard.json` and `vitals/vitals_summary.md`.
4. Compute the first seven vitals from existing artifacts:
   - control health
   - retrieval usefulness
   - context-use ROI
   - model compression health
   - graph coherence
   - temporal learning
   - proof gaps
5. Add `signal_metabolism.jsonl` in best-effort form using
   `signal_manifest.jsonl`, `planned_signals.jsonl`, `think_runs`, and applied
   ops when DB access is available.

Do not start by adding many new tables. The system already has enough trace
surface to produce better insight. New persistence should be added only where
the first renderer proves reconstruction is impossible or unreliable.

## End-State Test

The harness covers the long-term Fyralis ambition only when one run can answer:

- What did the company do?
- What changed?
- What did Fyralis learn?
- What did Fyralis compress?
- What did Fyralis forget or downgrade?
- What did Fyralis retrieve later because it mattered?
- What decisions did Fyralis influence?
- Were those decisions good?
- What human corrections changed the system?
- What system policies improved?
- What became cheaper, sharper, safer, or more useful than last month?

If the harness can answer those questions with artifacts, counts, traces, and
proof gaps, then it covers the system end to end across the long term.
