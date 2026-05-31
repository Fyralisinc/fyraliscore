# Fyralis Retrieval Revamp Implementation Plan

Last reviewed against the codebase on 2026-05-28.

Source proposal: `/Users/rachinkalakheti/Downloads/fyralis-execution-architecture (1).md`.

Implementation status on 2026-05-28: the first end-to-end version is now
active behind `EXECUTION_RETRIEVAL_ENGINE=inquiry`. It lives in
`services/execution/inquiry.py` and reuses the existing retrieval
pathways as executors while adding hypotheses, questions, retrieval
actions, evidence cards, sufficiency, durable inquiry sessions, and a
Synthesis Context Packet that reaches Think prompts.

## 1. Executive Summary

The proposal should be implemented as an execution-layer revamp around the current Think and retrieval system, not as a replacement of the memory substrate.

Today the product path is roughly:

```text
ingest signal
-> write observation
-> enqueue T1 Think trigger
-> primary_retrieve A/B/C/D/G
-> optional heuristic second pass
-> assemble_context
-> LLM or deterministic handler
-> validate
-> apply
```

The target path is:

```text
ingest signal
-> normalize signal envelope
-> route by value/risk/cost
-> fast, deterministic, deep, background, human, or archive path
-> for deep path, run question-conditioned inquiry
-> persist broad evidence reservoir
-> compile compact synthesis context packet
-> deep reason once
-> validate, optionally retry once for fixable errors
-> apply in the existing substrate
```

The most important product choice: keep `models`, `model_edges`, Acts, Resources, observations, audit events, and existing validation/apply semantics as the Synthesis substrate. The proposal's "Node" language maps onto the existing substrate rather than introducing a parallel Node table.

## 2. Current System Fit

### Existing strengths to preserve

- `services/ingestion/core.py` already normalizes channel payloads into tenant-scoped `observations`, resolves actors/entities, embeds text, assigns trust, and enqueues T1 triggers.
- `services/retrieval/primary.py` already has trigger-aware retrieval pathways:
  - A structural
  - B semantic
  - C temporal
  - D pattern/background
  - G typed model-edge traversal
- `services/retrieval/second_pass.py` already provides a small adaptive expansion mechanism.
- `services/retrieval/assembler.py` already applies access filtering, MMR support, prompt-survival telemetry, bridge context, and context budgets.
- `services/think/reason.py` already coordinates retrieval, reasoning frame construction, LLM/deterministic reasoning, validation, region locking, apply, anomalies, cascades, post-commit durability, context-use telemetry, and cost tracking.
- `services/think/validator.py` and `services/think/applier.py` are already the right state-change choke points.
- Background foundations already exist through anomaly, topology, precipitation, calibration, deadline, maintenance, and edge-drift workers.
- Real-LLM and retrieval quality suites already test the product shape better than toy mocks.

### Main gaps relative to the proposal

- Every non-deduped ingestion event currently enqueues T1 Think. There is no production routing gate deciding archive, deterministic update, fast path, deep inquiry, background, or human validation.
- The current "second pass" is heuristic expansion, not a persistent inquiry loop with hypotheses, discriminating questions, question plans, answered questions, and sufficiency status.
- Retrieval currently returns rows and assembled bundles; it does not produce durable evidence cards linked to questions, hypotheses, retrieval paths, omission reasons, token estimates, sensitivity, and access scope.
- Deep reasoning currently receives a bounded `ContextBundle`, not a tiered Synthesis Context Packet with mandatory frame, decisive evidence, supporting summaries, background summaries, and an omission ledger.
- The deep LLM call still happens inside the same Think transaction as retrieval and apply. This is operationally expensive and contributes to deadlock risk under concurrency.
- Human validation is not first-class. There is no durable request/response path for offline decisions, unclear ownership, or sensitive actor-intent questions.
- Heat diffusion as described in the proposal is not implemented as a question-conditioned local graph operation.

## 3. Design Principles

1. Do not create a second memory system. Persist new execution artifacts, but keep truth in the existing substrate.
2. Make routing deterministic first. LLMs can explain or improve routing later, but the first gate should be cheap, testable, and auditable.
3. Make the new inquiry loop shadowable. Run it beside the existing path before enforcing it.
4. Preserve current context-use telemetry and improve it for packet evidence.
5. Keep broad retrieval off-prompt. Store it in an Evidence Reservoir and compile only high-value evidence into the reasoning packet.
6. Make fast path read-only by default. If a user query or dashboard request reveals risk, enqueue deep inquiry asynchronously.
7. Frontier-model calls should be rare before the final reasoning step.
8. Prefer schema and JSON contracts that can be replayed from `think_run_artifacts`.
9. Split read/inquiry and write/apply transactions after behavior is stable.
10. Roll out by tenant and route type with feature flags.

## 4. Proposed Module Layout

Add these modules:

```text
services/execution/
  contracts.py          # SignalEnvelope, RoutingDecision, route enums
  intake.py             # Build normalized signal envelopes from observations/queries/jobs
  routing.py            # Deterministic routing gate and scoring
  queueing.py           # Enqueue helpers for deep/background/human paths

services/inquiry/
  contracts.py          # EvidenceState, Hypothesis, Question, EvidenceCard, Packet
  baseline.py           # Baseline Evidence Seeder
  hypotheses.py         # Heuristic and optional small-model hypothesis generation
  questions.py          # Candidate question generation and scoring
  compiler.py           # Question -> retrieval action plans
  executor.py           # Parallel retrieval action executor
  reservoir.py          # Evidence reservoir writes/reads
  sufficiency.py        # Stop/continue gate
  packet.py             # Synthesis Context Packet compiler
  orchestrator.py       # Deep Inquiry Path orchestration
  fast_path.py          # Fast Context Packet builder

services/human_validation/
  contracts.py
  service.py
  router.py             # Later UI/API surface
```

This names the new execution layer without disturbing existing retrieval/think ownership.

## 5. Data Model Additions

Create migration `0046_inquiry_execution.sql`.

### signal_routing_decisions

Purpose: audit every routing decision, including shadow-mode decisions.

Fields:

```text
id uuid primary key
tenant_id uuid not null
signal_ref_type text not null       # observation, query, scheduled_job, anomaly, internal
signal_ref_id uuid
route text not null                 # IGNORE_OR_ARCHIVE, DETERMINISTIC_UPDATE, FAST_PATH, DEEP_INQUIRY_PATH, BACKGROUND_PATH, HUMAN_VALIDATION_PATH
decision_status text not null       # shadow, enforced, skipped, failed
score numeric not null
score_breakdown jsonb not null
estimated_cost jsonb not null default '{}'
risk_level text
sensitivity text
reason text not null
enqueued_trigger_id uuid
created_at timestamptz not null default now()
```

Indexes:

```text
(tenant_id, created_at desc)
(tenant_id, route, created_at desc)
(signal_ref_type, signal_ref_id)
```

### inquiry_sessions

Purpose: durable record of a Deep Inquiry Path execution.

Fields:

```text
id uuid primary key
tenant_id uuid not null
signal_ref_type text not null
signal_ref_id uuid
route_decision_id uuid references signal_routing_decisions(id)
think_run_id uuid
status text not null                # running, sufficient_for_reasoning, no_update_needed, human_validation_required, budget_exhausted, failed, applied
stop_status text
round_count integer not null default 0
budget jsonb not null
hypotheses jsonb not null default '[]'
open_questions jsonb not null default '[]'
answered_questions jsonb not null default '[]'
unknowns jsonb not null default '[]'
candidate_state_changes jsonb not null default '[]'
notes jsonb not null default '{}'
created_at timestamptz not null default now()
updated_at timestamptz not null default now()
```

### inquiry_evidence

Purpose: Evidence Reservoir. It stores evidence cards, not copied full raw payloads.

Fields:

```text
id uuid primary key
tenant_id uuid not null
inquiry_session_id uuid references inquiry_sessions(id)
source_ref_type text not null        # observation, model, model_edge, commitment, goal, decision, resource, audit_event, candidate, summary
source_ref_id uuid
source_type text
summary text not null
trust_tier text
occurred_at timestamptz
retrieval_paths text[] not null default '{}'
retrieved_for_questions uuid[] not null default '{}'
supports_hypotheses text[] not null default '{}'
weakens_hypotheses text[] not null default '{}'
contradicts_hypotheses text[] not null default '{}'
token_estimate integer not null default 1
access_scope text
sensitivity text
raw_content_ref text
metadata jsonb not null default '{}'
created_at timestamptz not null default now()
```

Indexes:

```text
(tenant_id, inquiry_session_id)
(tenant_id, source_ref_type, source_ref_id)
gin(retrieval_paths)
```

### inquiry_question_runs

Purpose: record the question path, retrieval actions, and results.

Fields:

```text
id uuid primary key
tenant_id uuid not null
inquiry_session_id uuid references inquiry_sessions(id)
round_number integer not null
question_id text not null
question text not null
primitive text not null
tests_hypotheses text[] not null default '{}'
expected_value numeric
expected_cost numeric
selected boolean not null default false
selection_reason text
retrieval_plan jsonb not null default '{}'
answer_status text
answer_summary text
supporting_evidence_ids uuid[] not null default '{}'
counterevidence_ids uuid[] not null default '{}'
created_at timestamptz not null default now()
completed_at timestamptz
```

### inquiry_packets

Purpose: persist the exact Synthesis Context Packet sent to the deep reasoning agent.

Fields:

```text
id uuid primary key
tenant_id uuid not null
inquiry_session_id uuid references inquiry_sessions(id)
think_run_id uuid
packet jsonb not null
omission_ledger jsonb not null default '[]'
token_estimate integer not null default 0
created_at timestamptz not null default now()
```

### human_validation_requests

Purpose: first-class low-friction validation path.

Fields:

```text
id uuid primary key
tenant_id uuid not null
inquiry_session_id uuid references inquiry_sessions(id)
status text not null                # pending, answered, expired, cancelled, promoted, rejected
target_actor_ids uuid[] not null default '{}'
prompt text not null
question_kind text not null          # offline_alignment, owner_unknown, decision_ambiguous, sensitive_intent, conflict_resolution
speculative_model_id uuid
evidence_ids uuid[] not null default '{}'
response_observation_id uuid
response_summary text
confidence_update jsonb not null default '{}'
created_at timestamptz not null default now()
answered_at timestamptz
expires_at timestamptz
```

### think_run_artifacts stage extension

Extend the stage check to allow:

```text
routing
inquiry
context_packet
sufficiency
```

This keeps debugging unified.

## 6. Routing Policy

Implement `services/execution/routing.py` as deterministic scoring with explicit reasons.

### Inputs

Use only cheap signals:

- source channel
- observation kind / signal type
- trust tier
- resolved entities
- actor id
- customer/resource importance
- active goal overlap
- active commitment overlap
- deadline proximity
- user urgency
- novelty
- anomaly flag
- sensitivity
- estimated retrieval/LLM cost
- route source: ingestion, user query, scheduled job, worker

### Routes

```text
IGNORE_OR_ARCHIVE
DETERMINISTIC_UPDATE
FAST_PATH
DEEP_INQUIRY_PATH
BACKGROUND_PATH
HUMAN_VALIDATION_PATH
```

### Initial route rules

Use these as the first implementation. They are intentionally conservative.

```text
IGNORE_OR_ARCHIVE
- low-trust chatter
- no resolved entities
- no actor/customer/commitment/goal overlap
- no durable verbs or risk language
- no recent related anomalies

DETERMINISTIC_UPDATE
- authoritative system-of-record status changes
- legal state-machine update can be derived without interpretation
- no conflicting memory or ambiguous ownership

FAST_PATH
- explicit user query/dashboard context request
- low-risk read path
- no state mutation requested

DEEP_INQUIRY_PATH
- customer, revenue, active goal, active commitment, deadline, or decision overlap
- blocker/risk/commitment/approval/revisit/customer stance language
- contradiction or counterevidence against an active Model
- high-value alias/entity resolution changed retrieval scope

BACKGROUND_PATH
- anomaly, topology, precipitation, edge drift, scheduled falsification
- cross-region discovery or broad pattern mining

HUMAN_VALIDATION_PATH
- offline decision likely but not evidenced
- ownership is required and unresolved
- actor-intent or sensitive interpretation would be unsafe to infer
- conflicting human accounts with no authoritative source
```

### Rollout flags

Add:

```text
EXECUTION_ROUTING_ENABLED=0
EXECUTION_ROUTING_SHADOW=1
EXECUTION_ROUTE_T1_DEEP_ONLY=0
EXECUTION_FAST_PATH_ENABLED=0
EXECUTION_DETERMINISTIC_ROUTE_ENABLED=0
EXECUTION_HUMAN_VALIDATION_ENABLED=0
```

Shadow mode records route decisions but preserves current T1 behavior.

## 7. Fast Path Plan

Fast Path should be a read-only service that produces a Fast Context Packet.

Implementation:

```text
services/inquiry/fast_path.py
```

Inputs:

- normalized signal envelope or query
- tenant id
- access context
- optional card context

Execution:

1. Run baseline retrieval actions in parallel:
   - exact entity lookup
   - recent observations
   - semantic nearest neighbors
   - active Models around entities
   - active goals and commitments
   - bridge/customer resources
2. Build a packet:
   - signal/query summary
   - resolved entities
   - top Models
   - top evidence cards
   - active commitments
   - related goals
   - open unknowns
   - `deep_analysis_recommended`
3. Return to UI/query rendering.
4. If `deep_analysis_recommended=true`, enqueue a Deep Inquiry trigger asynchronously.

Integration points:

- Existing `services/query/core.py` should consume this for Ask-style UI reads after the packet is stable.
- Existing strategy-specific query retrieval can remain initially and be compared against Fast Path in shadow.

## 8. Deep Inquiry Plan

Deep Inquiry should run inside Think for state-changing/high-value signals.

### 8.1 Baseline Evidence Seeder

Add:

```text
services/inquiry/baseline.py
```

Responsibilities:

- Run cheap baseline retrieval before hypotheses.
- Reuse current retrieval pathway code where possible.
- Return EvidenceCards plus a baseline summary.

Actions:

```text
exact_entity_lookup
recent_observations
semantic_neighbors
structural_neighbors
active_goal_lookup
active_commitment_lookup
existing_model_lookup
recent_state_change_lookup
bridge_resource_lookup
model_edge_lookup
```

Initial implementation can adapt `primary_retrieve()` into evidence cards. Later, split exact actions into dedicated SQL functions for better latency and traceability.

### 8.2 Evidence State

Add an in-memory/Pydantic object in `services/inquiry/contracts.py`, persisted into `inquiry_sessions`.

Required fields:

```text
signal_id
known_entities
baseline_evidence_ids
candidate_hypotheses
open_questions
answered_questions
retrieval_history
unknowns
candidate_synthesis_updates
stop_status
budgets
```

### 8.3 Hypothesis Engine

Add:

```text
services/inquiry/hypotheses.py
```

First implementation should be deterministic templates with optional small-model fallback later.

Always include H0:

```text
H0: This signal is local noise or already captured and does not require a Synthesis update.
```

Template examples:

- customer + blocker language -> "customer commitment may be blocked"
- commitment + done/merged/closed -> "commitment status may have changed"
- decision + revisit/concern -> "decision may need reconsideration"
- repeated customer/system dependency -> "broader pattern may be forming"
- anomaly trigger -> "missing causal relationship may explain synchronized movement"

### 8.4 Question Generator and Scorer

Add:

```text
services/inquiry/questions.py
```

Question primitives:

```text
COMMITMENT_STATUS
DEPENDENCY
OWNER
GOAL_IMPACT
CUSTOMER_IMPACT
COUNTEREVIDENCE
RECURRENCE
PATTERN
MODEL_EDGE
FALSIFIER
TIMELINE
HUMAN_VALIDATION
```

Question score:

```text
hypothesis_discrimination
+ missing_evidence_coverage
+ expected_decision_impact
+ counterevidence_value
+ source_availability
- retrieval_cost
- latency_cost
- noise_risk
- permission_risk
```

Selection:

- 1 to 3 questions per round.
- Diversity by primitive and target entity.
- Do not select variants of the same question.

### 8.5 Retrieval Compiler

Add:

```text
services/inquiry/compiler.py
```

Compile selected questions into retrieval actions.

Action schema:

```text
path
target
query
filters
entity_ids
model_ids
window
budget
stop_condition
requires_heat_diffusion
sensitivity
```

Initial paths:

```text
exact
semantic
temporal
structural
model_edge
supporting_evidence
counterevidence
pattern
audit
```

Heat diffusion should compile only when the primitive requires local operational circuit discovery.

### 8.6 Retrieval Executor

Add:

```text
services/inquiry/executor.py
```

Responsibilities:

- Run retrieval actions concurrently when safe.
- Enforce per-action budgets.
- Apply tenant/access filters.
- Convert rows into EvidenceCards.
- Deduplicate by source ref.
- Record latency, count, and path.

This should be read-only.

### 8.7 Evidence Reservoir

Add:

```text
services/inquiry/reservoir.py
```

Responsibilities:

- Upsert evidence cards into `inquiry_evidence`.
- Link evidence to questions/hypotheses.
- Preserve raw references, not full raw content copies.
- Provide read APIs for packet compilation and debug.

### 8.8 Evidence State Update

Add deterministic update logic first:

- Mark question answer statuses.
- Raise/lower hypothesis confidence based on evidence links.
- Add new unknowns.
- Add candidate state changes.
- Track counterevidence coverage.

Use small-model summarization only for messy multi-evidence summaries after deterministic behavior is proven.

### 8.9 Sufficiency Gate

Add:

```text
services/inquiry/sufficiency.py
```

Stop states:

```text
sufficient_for_reasoning
insufficient_continue
insufficient_defer
human_validation_required
no_update_needed
budget_exhausted
```

Limits:

```text
max_inquiry_rounds=2 by default
max_questions_per_round=3
max_evidence_cards=300 initially
max_heat_diffusion_calls=1 initially
max_pre_reasoning_frontier_calls=0
```

Continue when:

- important hypotheses are close
- counterevidence has not been checked
- affected goal/commitment/customer is unknown
- owner is unknown and important
- another round has high expected value

Stop when:

- evidence is sufficient
- H0 wins
- cost/latency budget is reached
- human validation is required
- more retrieval is unlikely to alter the action

## 9. Heat Diffusion Plan

Implement after the basic inquiry loop is stable.

Add:

```text
services/inquiry/heat_diffusion.py
```

Use existing `models`, `model_edges`, `relationship_candidates`, scope sidecars, observations, and Acts as the local graph. Do not add a new truth graph.

Controls:

```text
default max candidate nodes: 150
high-stakes async max: 300
top edges per node: 10 to 25
restart probability: configurable
cache TTL: configurable, probably 15 to 60 minutes
```

Algorithm:

1. Harvest candidates from exact, semantic, structural, temporal, entity, and model-edge paths.
2. Prune by trust, recency, confidence, activation, access, and role relevance.
3. Build sparse adjacency from typed edges, shared scope, support links, and relationship candidates.
4. Apply question-conditioned edge weights.
5. Handle hubs by downweighting generic high-degree nodes and soft edges.
6. Run personalized random walk with restart.
7. Return heat-ranked Models, bridge Models, and optional basin summaries.

Cache:

Add a small cache table only if profiling shows repeated regions:

```text
retrieval_diffusion_cache(id, tenant_id, cache_key, question_hash, payload, created_at, expires_at)
```

## 10. Context Packet Compiler

Add:

```text
services/inquiry/packet.py
```

Output: Synthesis Context Packet.

Tiers:

```text
Tier 0: mandatory frame
Tier 1: decisive evidence
Tier 2: supporting evidence cards
Tier 3: background summaries
Tier 4: omitted long tail
```

Evidence value density:

```text
marginal_decision_usefulness / token_cost
```

Usefulness:

- hypothesis discrimination
- counterevidence
- affected customer/goal/commitment/decision
- trust tier
- freshness
- bridge value
- falsification value
- action relevance

Penalties:

- redundancy
- staleness
- low trust
- generic hub relevance
- token cost
- permission/sensitivity risk

Prompt integration:

- Add an optional `<synthesis_context_packet>` section to `services/think/prompt.py`.
- Keep existing `<retrieved_context>` during migration.
- Add packet ids and evidence ids to context-use telemetry.

Deep agent expansion requests:

- Later phase: extend `RawDiff` with optional `request_expansion`.
- The orchestrator may allow one targeted expansion from the omission ledger before final validation.
- Do not let the LLM run broad retrieval directly.

## 11. Think Integration

Initial integration should be behind a feature flag:

```text
THINK_DEEP_INQUIRY_ENABLED=0
THINK_DEEP_INQUIRY_SHADOW=1
```

### Step 1: shadow inquiry

In `services/think/reason.py::_run_once`:

1. Run current `primary_retrieve()` as today.
2. Run `run_deep_inquiry(..., shadow=True)` and capture artifacts.
3. Compare selected evidence/model ids against current assembler selection.
4. Do not change prompt input yet.

### Step 2: packet-fed prompt

When shadow metrics are healthy:

1. Use inquiry packet as primary prompt input.
2. Keep the old `ContextBundle` available for compatibility and telemetry.
3. Validate that context-use metrics can still prove whether selected evidence was used.

### Step 3: route-enforced Think

When routing is healthy:

- Only DEEP_INQUIRY_PATH and selected BACKGROUND_PATH triggers enter full Think.
- DETERMINISTIC_UPDATE uses deterministic handlers without frontier LLM.
- IGNORE_OR_ARCHIVE stores observation and route decision only.

### Step 4: split transaction

Current Think holds a transaction across retrieval, LLM, validation, and apply. After behavior is stable, split into:

```text
read phase:
  routing/inquiry/retrieval/context packet in read-only transactions

LLM phase:
  no DB transaction held

write phase:
  re-check idempotency
  acquire region lock
  validate against current region
  apply transactionally
```

This should reduce deadlock risk and improve worker throughput.

## 12. Validation and Apply Changes

Keep the existing validator/apply layer as the authority.

Add validation outcome classification:

```text
VALID
INVALID_RETRYABLE
INVALID_FINAL
```

Implementation:

- Current validator partially drops invalid ops.
- Add a wrapper in Think that treats all-bad diffs or material dropped ops as retryable once when the error is fixable.
- Feed validation errors and the same packet back to the deep reasoning agent for one correction attempt.
- If still invalid, dead-letter the inquiry session and trigger row.

Do not loosen existing safeguards:

- references must resolve
- state transitions must be legal
- high-confidence inferred Models need falsifiers
- region containment still applies
- access control still applies
- idempotency still applies
- model-edge endpoints remain Model-to-Model

## 13. Human Validation Path

Add a durable service, not direct ad hoc prompts.

Flow:

```text
human_validation_required
-> create human_validation_requests row
-> optionally create low-confidence speculative Model or relationship candidate
-> route prompt to appropriate actor(s)
-> ingest answer as observation
-> attach response_observation_id
-> re-enter Deep Inquiry Path
```

Prompt style:

- non-accusatory
- specific to observed evidence
- low-friction
- no sensitive evidence leakage

Confidence states:

```text
speculative
actor_attested
multi_actor_confirmed
contested
rejected
```

Initial UI can be deferred. Backend table and service should exist first so background workers can create requests.

## 14. Background Path

Reuse existing workers and add routing metadata.

Existing targets:

- anomaly processor -> T3
- topology sweeper -> T4 latent relationship candidates
- precipitation -> T4 pattern review
- deadline resolver -> scheduled checks
- calibration updater -> model quality
- edge drift -> relationship maintenance

New background capability:

### Dark Matter Detection

Add to anomaly processor or a sibling worker:

```text
services/workers/dark_matter/
```

First detector:

- synchronized changes across Linear/GitHub/Slack/CRM/calendar within a window
- no explicit decision/model/edge explains the movement
- affected active commitment/customer/goal exists

Output:

- relationship/situation candidate or low-confidence hypothesis Model
- human validation request when machine evidence cannot resolve cause
- T3/T4 trigger only when leverage score is high

## 15. Observability

Add metrics and debug surfaces before enforcing behavior.

Metrics:

```text
routing_decision_count by route/status/source
routing_shadow_disagreement_count
fast_path_latency_ms
deep_inquiry_latency_ms
inquiry_round_count
questions_selected_count
evidence_cards_retrieved_count
evidence_card_survival_ratio
packet_token_estimate
omission_ledger_count
sufficiency_stop_status_count
validation_retry_count
human_validation_request_count
route_to_apply_success_rate
cost_per_route
context_use_by_packet_tier
```

Debug:

- Extend `/debug/think-runs/{run_id}` to show routing, inquiry, evidence cards, questions, packet, omissions, and sufficiency.
- Add `/debug/inquiry-sessions/{id}` later if needed.
- Make replay cases include inquiry packet and evidence reservoir ids.

## 16. Test Plan

Every phase needs local correctness tests and global product tests.

### Phase 0: baseline capture

Run before changes:

```bash
.venv/bin/python -m pytest services/retrieval/tests services/think/tests services/query/tests -q
.venv/bin/python -m pytest tests/synthesis_harness -q
```

With services running:

```bash
docker compose up -d postgres ollama
docker compose exec ollama ollama pull nomic-embed-text:v1.5
.venv/bin/python -m pytest -m integration services/retrieval/tests services/think/tests -q
```

Real-LLM baseline when provider is configured:

```bash
RUN_REAL_LLM=1 .venv/bin/python -m pytest tests/real_llm/tests/test_smoke.py -v
RUN_REAL_LLM=1 .venv/bin/python -m pytest tests/real_llm/tests/test_context_use_outcome.py -v
```

### Phase 1: contracts and schema

Local tests:

- Pydantic/dataclass validation for all contracts.
- Migration idempotency.
- RLS/tenant isolation for new tables.
- Artifact stage check allows new stages.

Commands:

```bash
.venv/bin/python -m pytest services/inquiry/tests/test_contracts.py services/execution/tests/test_contracts.py -q
.venv/bin/python -m pytest tests/db_baseline.py -q
```

Global tests:

```bash
.venv/bin/python -m pytest services/gateway/tests/test_debug_router.py services/think/tests/test_observability.py -q
```

### Phase 2: routing gate

Local tests:

- Route matrix for low-value chatter, authoritative state changes, user queries, customer blocker, anomaly, offline decision.
- Score breakdown stability.
- Shadow mode records decisions and preserves current T1 enqueue.
- Enforced mode suppresses or redirects T1 correctly.
- Multi-tenant isolation.

Global tests:

```bash
.venv/bin/python -m pytest services/gateway/tests/test_ingest_endpoint.py services/think/tests/test_worker.py -q
.venv/bin/python -m pytest services/workers/anomaly_processor/tests -q
```

Quality checks:

- No important current T1 fixture should be incorrectly archived in shadow mode.
- Route decisions must include enough reason text to debug.

### Phase 3: Fast Path

Local tests:

- Fast packet includes resolved entities, top evidence, commitments, goals, unknowns.
- No writes except route decision and optional async enqueue.
- Access filtering prevents cross-tenant leakage.
- With fake embedder, semantic action can be tested deterministically.

Latency tests:

- Synthetic fixture: p95 under 2 seconds without LLM.
- Hard cap under 5 seconds.

Global tests:

```bash
.venv/bin/python -m pytest services/query/tests services/gateway/tests/test_today_routes.py -q
```

### Phase 4: baseline seeder and evidence reservoir

Local tests:

- Each retrieval action returns EvidenceCards with source refs and trust.
- Dedup preserves multi-path provenance.
- Reservoir upsert is idempotent.
- Token estimates are bounded.
- Raw refs are present; raw payload copies are not required.

Global tests:

```bash
.venv/bin/python -m pytest services/retrieval/tests/test_retrieval_quality_harness.py -q
.venv/bin/python -m pytest services/retrieval/tests/test_end_to_end.py -q
```

### Phase 5: inquiry loop

Local tests:

- Hypothesis generation always includes H0.
- Question generation links questions to hypotheses.
- Question scoring prefers discriminating/counterevidence/decision-impact questions.
- Compiler emits correct retrieval actions for primitives.
- Sufficiency gate stops for enough evidence and continues for missing counterevidence.
- Budget exhaustion is deterministic.

Scenario tests:

- Acme/SSO blocker example:
  - H1 critical path found
  - counterevidence checked
  - active commitment and goal surfaced
  - packet includes decisive CRM/Linear-like evidence
  - ownership unknown remains an unknown or human validation request

Global tests:

```bash
.venv/bin/python -m pytest services/think/tests/test_reason.py services/think/tests/test_context_use.py -q
.venv/bin/python -m pytest tests/synthesis_harness/cases_retrieval.py -q
```

### Phase 6: packet compiler

Local tests:

- Tier 0 always present.
- Decisive evidence survives compression.
- Redundant evidence becomes Tier 2 summary.
- Omission ledger records groups and expansion conditions.
- Token budget is respected without truncating mid-item.
- Sensitive or inaccessible evidence is omitted/redacted.

Prompt tests:

```bash
.venv/bin/python -m pytest services/think/tests/test_llm_reason.py services/think/tests/test_t6_prompt.py -q
```

Context-use tests:

- Packet evidence ids referenced by diff are counted.
- Empty diffs must cite exact ids from the packet or selected bundle.

### Phase 7: heat diffusion

Local tests:

- Hard cap on candidates.
- Hub downweighting prevents generic high-degree Models from dominating.
- Question-conditioned weights change ranking.
- Cache hit returns identical result.
- Access filter applies before packet compilation.
- Deterministic results for fixed graph.

Performance tests:

- 150 nodes under target CPU budget.
- 300 nodes allowed only for async high-stakes mode.

Global tests:

```bash
.venv/bin/python -m pytest services/retrieval/tests/test_retrieval_adversarial_cases.py -q
```

### Phase 8: validation retry

Local tests:

- Retryable missing reference/fixable schema error triggers one correction attempt.
- Illegal transition stays final.
- All-bad diff dead-letters after retry.
- Partial-valid behavior remains available when non-material ops drop.

Global tests:

```bash
.venv/bin/python -m pytest services/think/tests/test_validator.py services/think/tests/test_end_to_end.py -q
```

### Phase 9: human validation

Local tests:

- Request creation chooses allowed actors only.
- Prompt avoids sensitive evidence leakage.
- Response observation re-enters inquiry.
- Confidence state updates are bounded.

Global tests:

```bash
.venv/bin/python -m pytest services/gateway/tests services/think/tests/test_auto_decision_revisit.py -q
```

### Phase 10: route enforcement and transaction split

Local tests:

- Idempotency still prevents duplicate apply.
- Region locks still protect apply.
- Out-of-region retry still works.
- Worker completion marks queues correctly.
- Transaction retry on deadlock/serialization still works.

Concurrency tests:

```bash
.venv/bin/python -m pytest services/think/tests/test_worker.py services/think/tests/test_region_locks.py -q
```

Real-LLM global tests:

```bash
RUN_REAL_LLM=1 .venv/bin/python -m pytest tests/real_llm/tests/test_smoke.py -v
RUN_REAL_LLM=1 .venv/bin/python -m pytest tests/real_llm/tests/test_think_reasoning.py -v
RUN_REAL_LLM=1 .venv/bin/python -m pytest tests/real_llm/tests/test_context_use_outcome.py -v
```

Scale/durability when ready:

```bash
RUN_REAL_LLM=1 RUN_SCALE_CHAOS_FULL=1 \
  .venv/bin/python -m pytest tests/real_llm/tests/test_scale_chaos_ingestion.py -v

RUN_REAL_LLM=1 RUN_DURABILITY_E2E=1 \
  DURABILITY_SCENARIOS=industrial_ops,fintech_risk \
  .venv/bin/python -m pytest tests/real_llm/tests/test_deep_durability_end_to_end.py -v
```

## 17. Rollout Plan

### Milestone A: no behavior change

- Add contracts, schema, route decision records.
- Run routing and inquiry in shadow.
- Current T1 Think remains unchanged.

Exit criteria:

- No baseline test regressions.
- Shadow route decisions are visible in debug.
- No cross-tenant leakage in new tables.

### Milestone B: fast path read-only

- Enable Fast Path for query/dashboard surfaces.
- Deep analysis recommendation enqueues background deep inquiry but does not block UI.

Exit criteria:

- Fast path p95 under 2 seconds locally with seeded corpora.
- Existing query tests pass.
- Answers still cite relevant Models/observations.

### Milestone C: deep inquiry shadow

- Run inquiry loop before LLM but keep old prompt.
- Compare packet contents against old bundle and context-use outcomes.

Exit criteria:

- Packet recall covers existing selected high-value Models/observations.
- No material increase in false no-ops.
- Context-use telemetry improves or stays flat.

### Milestone D: packet-fed Think

- Deep Reasoning Agent receives Synthesis Context Packet.
- Existing bundle retained as compatibility fallback.

Exit criteria:

- Validation drop rate does not rise.
- Ops applied quality holds in real-LLM smoke tests.
- Cost/latency within budget.

### Milestone E: routing enforcement

- Enforce archive/deterministic/fast/deep/background route decisions for selected tenants.
- Keep emergency rollback env flags.

Exit criteria:

- No high-value signals silently archived in replay.
- Deterministic updates match or outperform old Think path.
- Worker backlog and cost drop without quality regression.

### Milestone F: transaction split

- Move LLM outside write transaction.
- Keep apply idempotent and region locked.

Exit criteria:

- Deadlock/serialization retries drop.
- Throughput improves under worker concurrency tests.
- No duplicate applies.

## 18. Risk Register

### Risk: routing archives important signals

Mitigation:

- Shadow mode first.
- Conservative thresholds.
- Archive route still stores observations.
- Add route replay tests from real corpora.

### Risk: inquiry loop increases latency without quality gain

Mitigation:

- Cap rounds/questions/evidence.
- Run cheap deterministic steps first.
- Measure packet recall and context-use improvement.
- Disable per route or tenant.

### Risk: packet compression drops decisive counterevidence

Mitigation:

- Counterevidence gets explicit value boost.
- Omission ledger records excluded groups.
- Tests assert decisive/counterevidence survival.

### Risk: heat diffusion creates CPU spikes

Mitigation:

- Non-default.
- Candidate caps.
- Async for high-stakes broad graph questions.
- Cache region/question outputs.

### Risk: more tables make observability harder

Mitigation:

- Keep `think_run_artifacts` as the main debug timeline.
- Link route decision, inquiry session, packet, and think run ids.
- Add debug routes only after core persistence is stable.

### Risk: validation retry encourages LLM overreach

Mitigation:

- One retry only.
- Same packet only, no broad retrieval.
- Illegal/sensitive/final failures do not retry.

## 19. Concrete First Engineering Slice

The first pull request should be small and non-invasive:

1. Add `services/execution/contracts.py`.
2. Add `services/execution/routing.py`.
3. Add migration `0046_inquiry_execution.sql` with `signal_routing_decisions` only, plus artifact stage extension.
4. Record shadow route decisions in `services/ingestion/core.py` after observation insert.
5. Keep current T1 enqueue behavior unchanged.
6. Add route matrix tests.
7. Add debug visibility for route decision in signal detail if easy.

Why this first:

- It starts the architectural migration without risking Think quality.
- It gives real production-shaped data about how the gate would behave.
- It creates the audit trail needed before any route is enforced.

## 20. Ambiguity Decisions

These choices resolve underspecified parts of the proposal in the direction of Fyralis' larger product goal.

- "Synthesis Node" maps to existing `models`, `model_edges`, Acts, Resources, and observations.
- Evidence Reservoir stores evidence cards with raw refs, not duplicated raw content.
- Fast Path is read-only and can enqueue Deep Inquiry asynchronously.
- Deep Inquiry becomes a Think pre-reasoning layer, not a separate applier.
- Existing validator/apply remains the sole state-change gate.
- Human validation is a durable backend service first; UI can follow.
- Heat diffusion uses existing memory graph data and is question-gated.
- Small-model steps start deterministic; optional LLM summarization comes only after correctness tests.
- Routing starts in shadow and becomes tenant/route enforceable later.
- Cost optimization is measured, but correctness and context quality are the first gates.
