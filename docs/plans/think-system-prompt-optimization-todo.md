# Think System Prompt Optimization TODO

Date: 2026-07-02

This is the implementation TODO for replacing the current monolithic Think
system prompt with a surface-aware prompt assembler aligned with the current
Fyralis Core architecture.

The goal is not just to make the prompt shorter. The goal is to make the prompt
more correct: Models are the semantic memory backbone, while Acts and Resources
are typed operational sidecars. Think should preserve that distinction while
still using the full operational diff surface when the trigger and context
actually require it.

## North Star

Think should reason like this:

```text
Observation or internal trigger
  -> retrieve relevant Models, graph, Acts, Resources, and packet hints
  -> choose the smallest safe output surface
  -> propose a minimal typed diff
  -> validator constrains it
  -> applier mutates Models, graph, Acts, Resources, events, and projections
```

The prompt should teach the LLM the durable architecture:

- Observations are immutable evidence.
- Models are semantic memory: beliefs, observed facts, predictions, norms,
  recommendations, situations, hypotheses, concerns, capabilities, and patterns.
- Relation claims, relation frames, and model edges express Model-to-Model
  structure.
- Lifecycle ops, formation resolutions, and open questions are accountability
  surfaces around Models.
- Acts are typed operational state: goals, commitments, decisions, and their
  graph edges.
- Resources are typed operational/resource state: capacity, financial,
  infrastructure, deployment, regulatory, and customer-resource records.
- LLMs propose; validators constrain; appliers mutate.

## Current Findings

Measured with a rough chars/4 token estimate:

- `_SYSTEM_PROMPT` is about `32,938` chars, or about `8.2k` tokens.
- `_CLAIMS_ONLY_SYSTEM_PROMPT` is about `11,094` chars, or about `2.8k` tokens.
- The relationship/topology tail is about `9,838` chars, or about `2.5k`
  tokens.
- The act-ops section is about `1,378` chars, or about `344` tokens.

Important code reality:

- `ClaimOp` mutates the Models surface.
- `ActOp` mutates the Acts surface.
- `ResourceOp` mutates the Resources surface.
- `RelationClaimOp`, `RelationFrameOp`, and `EdgeOp` mutate relationship or
  Model graph surfaces.
- `OpenQuestionOp`, `MemoryLifecycleOp`, and `FormationResolutionOp` are Model
  accountability surfaces, not separate truth stores.
- `llm_reason._select_output_schema(...)` currently has a coarse routing split:
  claims-only for no-surface calls or non-graph belief-update cascades,
  otherwise full `RawDiff`.

The current prompt is stale in framing because it says Think produces a diff
against "Observations, Models, Acts, and Resources" as if they are peer memory
planes. The current code is more precise: Think proposes a typed diff over
semantic memory plus operational sidecars.

## Expected Improvement

Expected static prompt reduction after surface-aware assembly:

| Call shape | Expected static-token reduction |
| --- | ---: |
| Simple T1 claims-only signal | 55-70% |
| Normal memory reconciliation | 40-60% |
| Graph/topology-heavy call | 25-40% |
| Act/resource-heavy call | 25-45% |
| Worst-case all-surface call | 15-30% |

Expected qualitative gains:

- Fewer over-eager commitment recommendations.
- Fewer act mutations from vague semantic implication.
- Clearer graph reasoning only when graph context is active.
- Less prompt-cache churn from dynamic context staying in the user message.
- Cleaner distinction between accepted memory and typed operational state.

## Current System Anchors

Primary files:

- `services/reasoning/think/prompt.py`
- `services/reasoning/think/llm_reason.py`
- `services/reasoning/think/reasoning_frame.py`
- `services/reasoning/think/diff_schema.py`
- `services/reasoning/think/validator.py`
- `services/reasoning/think/applier.py`
- `services/reasoning/think/context_use.py`
- `services/reasoning/think/auto_create_commitment.py`

Reference docs:

- `docs/reference/CODEBASE-ARCHITECTURE.md`
- `docs/reference/CURRENT_SYSTEM_DEEP_DIVE.md`

Tests to inspect or extend:

- `services/reasoning/think/tests/`
- `tests/quality_replay/`
- `tests/real_llm/tests/test_context_use_outcome.py`

## Non-Goals

- Do not remove `act_ops` or `resource_ops`.
- Do not weaken validation or rely on prompt text for invariants that validators
  can enforce.
- Do not move Acts or Resources into the Model layer.
- Do not introduce a second memory store.
- Do not make T1 more expensive by default.
- Do not split the prompt into many dynamic system prompts if it destroys
  provider prefix-cache stability without measurable benefit.

## Phase 0: Baseline Prompt And Cost Measurements

Primary files:

- `services/reasoning/think/prompt.py`
- `services/reasoning/think/llm_reason.py`
- `services/reasoning/think/quality_report.py`

Work:

- [ ] Add or update a small script/test helper that reports static prompt chars
  and estimated tokens for each prompt mode.
- [ ] Measure current `_SYSTEM_PROMPT`, `_CLAIMS_ONLY_SYSTEM_PROMPT`, and
  compiled prompt variants.
- [ ] Capture baseline call distribution by selected schema:
  `RawDiff`, `RawDiffClaimsOnly`, compiled batch decision, compiled
  relationship candidate.
- [ ] Capture baseline average input tokens by trigger kind and output schema.
- [ ] Preserve current behavior snapshots from quality replay fixtures.

Exit criteria:

- There is a repeatable before/after measurement path.
- We can report static-prompt reduction separately from dynamic context changes.
- We can distinguish prompt cost from retrieved-context cost.

## Phase 1: Rewrite The Core Architecture Contract

Primary file:

- `services/reasoning/think/prompt.py`

Work:

- [ ] Replace the current opening frame with the new architecture frame:
  "semantic memory plus typed operational sidecars."
- [ ] Make the LLM role explicit:
  "LLM proposes; validator constrains; applier mutates."
- [ ] Say that Observations are evidence, not a mutation target.
- [ ] Say that Models are the semantic memory backbone.
- [ ] Say that Acts and Resources are typed sidecars, not Model-layer truth.
- [ ] Keep the existing scoping, UUID, falsifier, confidence, and selected
  context discipline.
- [ ] Remove universal language that makes act/resource mutation feel like a
  normal result of every signal.

Candidate core prompt:

```text
You are Fyralis Think, the proposal stage of a validated organizational memory
runtime.

Your job is to propose the smallest useful diff for the triggering signal. The
LLM proposes; validators constrain; appliers mutate. Empty diffs are valid when
current memory already captures the signal or the signal is not durable.

Architecture:
- Observations are immutable evidence.
- Models are the semantic memory backbone.
- Model relationships live in relation claims, relation frames, and model_edges.
- Lifecycle, formation, and open questions are accountability surfaces around
  Models.
- Acts are typed operational state, not Model-layer beliefs.
- Resources are typed operational/resource state, not Model-layer beliefs.
- Product projections are rebuildable views over applied mutations and events.
```

Exit criteria:

- The base prompt no longer implies that commitments/goals/decisions are part of
  the Model layer.
- The prompt still preserves all safety and evidence-grounding constraints.
- Static system prompt text is smaller before any capability-pack split.

## Phase 2: Split Prompt Into Capability Packs

Primary file:

- `services/reasoning/think/prompt.py`

Work:

- [ ] Create an always-on core pack.
- [ ] Create a compact Model memory pack for claim inserts, proposition stance,
  scope, confidence, semantic terms, falsifiers, and granularity.
- [ ] Create a lifecycle pack for `memory_lifecycle_ops`,
  `open_question_ops`, prediction resolution, and formation candidates.
- [ ] Create a graph pack for `relation_claim_ops`, `relation_frame_ops`,
  `edge_ops`, and `ontology_gap_ops`.
- [ ] Create an Acts pack for goals, commitments, decisions, and act graph
  edges.
- [ ] Create a Resources pack for create/update/transaction/deploy/release.
- [ ] Create a batch pack for T1 event batches and compiled memory decision
  boundaries.
- [ ] Create a topology/candidate pack for T4 latent relationship candidates
  and legacy T6 topology shifts.
- [ ] Keep exact JSON shape examples as compact as possible, relying on schema
  enforcement where the provider supports it.

Exit criteria:

- Each prompt section has one reason to exist.
- Graph-heavy prose is absent from non-graph calls.
- Act/resource prose is absent from calls where those ops are not allowed.
- Batch-specific rules are absent from non-batch calls.

## Phase 3: Add Surface-Aware Prompt Assembly

Primary files:

- `services/reasoning/think/prompt.py`
- `services/reasoning/think/llm_reason.py`
- `services/reasoning/think/reasoning_frame.py`

Work:

- [ ] Introduce a compact `PromptSurface` or equivalent internal structure:
  `claims`, `lifecycle`, `graph`, `acts`, `resources`, `batch`,
  `topology_candidate`, `strict_schema`.
- [ ] Derive surfaces from `ReasoningFrame.allowed_ops`.
- [ ] Also derive surfaces from retrieved context:
  selected graph anchors, relationship candidates, acts summary, resources
  summary, formation candidates, inquiry packet mode, trigger metadata.
- [ ] Keep dynamic trigger/context details in the user message for system prefix
  cache stability.
- [ ] Ensure prompt assembly is deterministic for the same trigger/context
  surface shape.
- [ ] Add debug metadata recording which packs were loaded.

Initial routing rules:

```text
core: always
model_memory: always
lifecycle: selected Models, formation candidates, prediction resolution, T2/T3/T4
graph: graph anchors, relation candidates, T4/T6, or edge ops allowed and relevant
acts: act_ops allowed and acts context or explicit performative signal exists
resources: resource_ops allowed and resource context or explicit resource signal exists
batch: trigger.is_batch or compiled memory decision mode
topology_candidate: T4 latent_relationship_candidate or T6
strict_schema: provider.enforces_output_schema(schema)
```

Exit criteria:

- Prompt assembly can load fewer packs than the selected output schema exposes.
- Pack selection is visible in debug artifacts or run metadata.
- Full prompt behavior remains available for high-risk rollout fallback.

## Phase 4: Improve Output Schema Selection

Primary file:

- `services/reasoning/think/llm_reason.py`

Work:

- [ ] Keep `RawDiffClaimsOnly` for truly claims-only calls.
- [ ] Consider adding intermediate schemas if worthwhile:
  - `RawDiffMemoryOnly`
  - `RawDiffGraphOnly`
  - `RawDiffActsOnly`
  - `RawDiffResourcesOnly`
- [ ] Avoid schema explosion unless measurements show the prompt-only split is
  insufficient.
- [ ] Ensure `_coerce_raw_diff(...)` can safely normalize each intermediate
  schema into `RawDiff`.
- [ ] Keep validator and applier unchanged unless the new schemas expose a real
  contract issue.

Exit criteria:

- The cheapest schema is used for each call without suppressing needed ops.
- Intermediate schemas are introduced only if they materially reduce static
  input or parse errors.
- Existing deterministic handlers still return `RawDiff` safely.

## Phase 5: Rebalance Acts And Commitment Guidance

Primary files:

- `services/reasoning/think/prompt.py`
- `services/reasoning/think/auto_create_commitment.py`
- `services/reasoning/think/reasoning_frame.py`

Work:

- [ ] Move universal self-reported-work commitment guidance out of the core
  prompt and into the Acts pack.
- [ ] Clarify that Acts mutate typed operational state, not semantic truth.
- [ ] Require explicit operational evidence for act transitions.
- [ ] Preserve same-diff `confidence_basis` support using `born_from_event_id`.
- [ ] Keep deterministic safety net behavior for high-value missed cases.
- [ ] Review whether T2 `propagate_consequence` should still include act ops by
  default or only when act context is present.

Exit criteria:

- Ordinary semantic memory calls do not see commitment-creation mandates.
- Acts remain available when the signal clearly warrants operational mutation.
- Existing tests for commitment creation, block transitions, and decision
  revisits still pass.

## Phase 6: Rebalance Graph And Relationship Guidance

Primary files:

- `services/reasoning/think/prompt.py`
- `services/reasoning/think/reasoning_frame.py`
- `services/reasoning/relationships/`

Work:

- [ ] Move relation/edge/ontology detail into the graph pack.
- [ ] Keep a short core reminder that edges are strictly Model-to-Model.
- [ ] Keep relationship decision requirements only when graph context is
  selected or graph ops are allowed.
- [ ] Preserve topology candidate caution:
  candidate is pre-truth evidence, not a mandate to promote.
- [ ] Keep the sharp-edge-vs-no-edge discipline for graph calls.
- [ ] Ensure same-diff placeholders still work for edges from new claims.

Exit criteria:

- Non-graph T1 calls no longer carry the full relationship ontology.
- Graph-heavy calls retain the current edge-quality discipline.
- Quality replay cases still catch ignored selected graph context.

## Phase 7: Prune Dynamic Context Rendering

Primary file:

- `services/reasoning/think/prompt.py`

Work:

- [ ] Do not render `<acts>` when acts are irrelevant and the Acts pack is not
  loaded, except for compact scope refs needed by Models.
- [ ] Do not render `<resources>` when resources are irrelevant and the
  Resources pack is not loaded, except for compact scope refs needed by Models.
- [ ] Keep `actors_in_context`, candidate substrate, and customer context because
  they materially improve Model scoping.
- [ ] Keep compiled memory decision candidate fields that affect quality:
  `suggested_edge_kinds`, `write_preconditions`, and `answer_summary`.
- [ ] Prefer compact answer/status hints over raw-observation widening.

Exit criteria:

- Static prompt shrink is not erased by unnecessary context sections.
- Scope quality does not regress.
- Prompt-survival telemetry remains useful.

## Phase 8: Tests And Replay Coverage

Primary test areas:

- `services/reasoning/think/tests/`
- `tests/quality_replay/`

Work:

- [ ] Unit test pack selection for:
  - simple T1 no prior context
  - T1 with selected Models only
  - T1 with graph anchors
  - T1 with Acts context
  - T1 with Resources context
  - T1 batch compiled decision mode
  - T2 belief update with and without graph anchors
  - T4 latent relationship candidate
  - T6 topology shift
- [ ] Snapshot-test the assembled system prompt for representative calls.
- [ ] Add tests proving Acts pack is absent for graph-only topology calls.
- [ ] Add tests proving graph pack is absent for simple claims-only calls.
- [ ] Add tests proving selected context no-op discipline remains present.
- [ ] Run existing context-use tests.
- [ ] Run quality replay fixtures that cover graph/action/resource behavior.

Exit criteria:

- Pack routing is deterministic and covered.
- Existing semantic behavior does not regress.
- Static prompt size assertions prevent accidental monolith regrowth.

## Phase 9: Rollout Flags And Telemetry

Primary files:

- `services/reasoning/think/prompt.py`
- `services/reasoning/think/llm_reason.py`
- `services/reasoning/think/metrics.py`
- `services/reasoning/think/quality_report.py`

Work:

- [ ] Add a rollout flag such as `THINK_SURFACE_AWARE_PROMPT`.
- [ ] Keep a fallback to current monolith prompt during rollout.
- [ ] Record loaded prompt packs in Think run artifacts.
- [ ] Record static prompt chars and estimated prompt tokens by pack.
- [ ] Compare input tokens, output tokens, parse failures, validation drops,
  context-use grades, and applied op mix.
- [ ] Add quality-report summaries for prompt mode and loaded surfaces.

Exit criteria:

- Rollout can be enabled tenant-wide or environment-wide.
- Operators can compare old and new prompt modes without reading raw artifacts.
- Prompt cost and quality are measured together.

## Phase 10: Validation Commands

Run the narrowest useful checks first:

```bash
.venv/bin/python -m pytest services/reasoning/think/tests -v --tb=short
.venv/bin/python -m pytest tests/quality_replay -v --tb=short
ruff check --select E9,F63,F7,F82,F821,F811,F401 services/reasoning/think
```

Then widen if the change touches retrieval, inquiry packets, or runtime docs:

```bash
lint-imports
python scripts/check_architecture_ratchets.py
```

Opt-in only when explicitly requested:

```bash
.venv/bin/python -m pytest tests/real_llm -v --tb=short
```

## Open Decisions

- Should we introduce intermediate Pydantic schemas, or is prompt-pack routing
  enough?
- Should pack selection be based only on `ReasoningFrame.allowed_ops`, or also
  on trigger text classifiers for performative/resource signals?
- Should dynamic `<acts>` and `<resources>` context be compacted into scope-only
  summaries when those packs are absent?
- Should the old monolith remain as a fallback forever, or be deleted after
  rollout evidence is strong?
- What exact quality gate should block rollout: context-use regression,
  validation-drop increase, act-op false positives, graph-edge quality, or cost
  only?

## Completion Criteria

- Common calls no longer pay for graph/Acts/Resources prose they cannot use.
- The base prompt accurately reflects the current architecture.
- Prompt pack selection is deterministic, observable, and tested.
- Input token reduction is measured against the baseline.
- Context-use and validation quality do not regress.
- The implementation preserves the invariant:

```text
LLM proposes -> validator constrains -> applier mutates
```
