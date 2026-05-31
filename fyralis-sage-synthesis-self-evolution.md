# Fyralis Synthesis Graph Self-Evolution Architecture

## SAGE-Inspired Proposal for Making the Fyralis Synthesis Layer Faster, Sharper, and More Efficient

### Status

Draft implementation specification.

### Audience

This document is written for developers and system designers implementing the next-generation Fyralis Synthesis layer.

### Purpose

Fyralis already has the core architectural foundation: raw company signals enter as immutable observations, the Synthesis layer stores interpreted Nodes and Models, and the Reasoning engine updates Synthesis through retrieval, reasoning, validation, and apply stages.

The remaining bottleneck is not just retrieval. The deeper bottleneck is how the Synthesis graph writes, organizes, and evolves its own memory so that future retrieval becomes cheaper and more accurate.

This document proposes a SAGE-inspired, Fyralis-native self-evolving graph architecture.

The central idea:

> Every read should teach Fyralis how the graph should have been written. Every write should be judged by whether it makes future company-state inquiry cheaper, cleaner, and more action-relevant.

This is not decorative graph intelligence. This is how we stop the Synthesis layer from becoming an expensive pile of true but useless claims. Humanity has already invented that. It is called a meeting archive.

---

## 1. Source Inspiration: What SAGE Contributes

SAGE stands for **Self-Evolving Agentic Graph-Memory Engine for Structure-Aware Associative Memory**.

The SAGE paper argues that conventional RAG and GraphRAG systems often treat memory graphs as static retrieval middleware. Once the graph is built, the system mostly focuses on retrieving from it. SAGE reframes graph memory as a dynamic, self-evolving substrate with two coupled roles:

1. **Memory Writer**: incrementally writes structured graph memory from histories or documents.
2. **Memory Reader**: performs query-conditioned graph reading and returns feedback that improves future writing.

The paper identifies three key problems in static graph memory systems:

1. Fragmented cues often require recovering long evidence chains, not just semantically similar snippets.
2. Graph structure should be used selectively; hubs, bridges, shortcuts, and dense neighborhoods should not be expanded uniformly.
3. Retrieval failures should improve memory writing, rather than being forgotten after the answer.

Reference:

- SAGE paper: https://arxiv.org/abs/2605.12061
- SAGE HTML: https://arxiv.org/html/2605.12061v1

### Key SAGE Mechanics Relevant to Fyralis

SAGE contributes the following mechanics:

| SAGE Mechanic | Meaning | Fyralis Translation |
|---|---|---|
| Memory writer | Writes graph memory from histories | Synthesis Writer creates/updates Nodes, Models, links, summaries, anchors |
| Memory reader | Reads graph using query-conditioned propagation | Inquiry Retrieval Engine reads Synthesis for signal/question context |
| Reader feedback | Retrieval results improve future writing | Inquiry outcomes improve graph topology and write policy |
| Structured query planning | Extract entities, aliases, relation clues, constraints, intents | Extract company-state cues and retrieval intents from signal + hypothesis + question |
| Soft addressing | Activate relevant memory even if not exact match | Activate Nodes/Models through semantic, structural, alias, entity, temporal, and role cues |
| Structural gates | Suppress noisy hubs, preserve bridges, avoid uniform propagation | Question-conditioned graph traversal with learned/heuristic gates |
| Context-schema decomposition | Combine target graph context with reusable structural priors | Combine tenant-specific graph with cross-company organizational priors |
| Query-conditioned selector | Select compact reading subgraph | Select compact Node/Model subgraph before packet compilation |
| Entity-to-document projection | Convert entity activation to evidence/document ranking | Convert Node/Model activation to observation/evidence ranking |
| Writer reward | Train writer by downstream retrieval/answer usefulness | Score Synthesis writes by future inquiry value and valid state-change utility |

---

## 2. Fyralis Compatibility Analysis

This proposal is compatible with Fyralis because it strengthens existing architecture rather than replacing it.

### Existing Fyralis Principles Preserved

Fyralis already assumes:

- Synthesis is the center of interpreted company state.
- Every meaningful thing is a Node: goal, commitment, decision, pattern, belief, recommendation, state, relationship, or Model.
- Observations are immutable raw signals.
- Nodes carry evidence, confidence, authority, falsification conditions, status, and lifecycle metadata.
- The Reasoning engine retrieves context, reasons, validates a structured diff, and applies updates transactionally.
- High-confidence inferential Nodes require falsification conditions.
- Supporting-Node dependencies must remain acyclic.
- Access control, subject rights, calibration, and validation are core invariants.

The proposed self-evolution layer preserves those invariants.

### What Changes

The current Synthesis layer stores and updates interpreted state.

The proposed architecture adds a **self-evolution layer** around Synthesis:

```text
Canonical Synthesis Graph
  Stores validated truth.

Discovery Topology
  Stores learned retrieval utility.

Synthesis Reader
  Reads the graph for inquiry.

Synthesis Writer
  Writes or proposes graph updates.

Outcome Evaluator
  Judges whether reads/writes helped.

Topology Optimizer
  Updates discovery structures after outcomes.
```

### Critical Design Separation

Do not let learned discovery utility corrupt canonical truth.

Use two layers:

```text
Canonical Truth Layer:
  Slow, validated, evidence-backed.
  Contains actual Nodes, Models, relationships, evidence, confidence, falsification, lifecycle.

Discovery Utility Layer:
  Faster, adaptive, probabilistic.
  Contains retrieval affordances, bridge scores, hub penalties, discovery shortcuts, residuals, prediction-error signals, utility weights.
```

A discovery shortcut does **not** mean a causal relationship is true.

It means:

> When this kind of inquiry appears, this region/path/node has historically been useful to inspect.

This distinction is mandatory. Otherwise Fyralis becomes an impressively confident gossip engine with ACID transactions, and frankly the world has suffered enough.

---

## 3. Core Proposal

### Name

**Synthesis Graph Self-Evolution Loop**

### Mission

Continuously improve the structure of Fyralis's Synthesis graph so that future inquiry becomes cheaper, sharper, and more action-relevant.

### One-Line Description

> A SAGE-inspired reader-writer feedback loop where the Inquiry Engine reads from Synthesis, the Reasoning Engine writes validated updates, and the Topology Optimizer learns from outcomes to improve future graph reading and writing.

### Full Flow

```text
Signal arrives
  ↓
Inquiry Reader asks questions and retrieves evidence
  ↓
Context Packet Compiler prepares compact reasoning context
  ↓
Deep Reasoning Agent proposes Synthesis diff
  ↓
Validation accepts, rejects, or corrects diff
  ↓
Apply Layer writes canonical Synthesis updates
  ↓
Outcome Evaluator records what helped, failed, or was missing
  ↓
Topology Optimizer updates discovery topology
  ↓
Future inquiry becomes cheaper and more precise
```

---

## 4. What Each Change Adds to Fyralis

| Change | What It Adds | Efficiency Impact | Quality Impact | Risk / Guardrail |
|---|---|---|---|---|
| Reader-aware Synthesis writing | Writes are evaluated by future retrieval/reasoning utility | Reduces future inquiry cost | Produces Nodes/Models that are easier to use later | Must not let utility override truth validation |
| Discovery topology layer | Learned search utility separate from canonical truth | Faster retrieval without bloating canonical graph | Better non-obvious path discovery | Must clearly label as non-truth metadata |
| Structured cue extraction | Separates signal parsing from inquiry planning | Reduces wasted retrieval | Better initial activation | Needs entity/alias hygiene |
| Soft activation | Activates relevant Nodes even when not exact matches | Improves recall without broad brute force | Finds bridge Nodes and latent context | Needs caps to avoid activation sprawl |
| Structural gates | Learns/estimates which graph edges should transmit relevance | Reduces noisy traversal | Suppresses hubs, preserves bridges | Initial heuristics must be observable and debuggable |
| Context-schema decomposition | Uses tenant context plus reusable organizational priors | Scales across customers while adapting locally | Avoids too-generic or too-custom behavior | Must isolate tenant data/privacy |
| Query-conditioned subgraph selector | Selects compact Node/Model subgraph for each question | Reduces token and reasoning cost | Higher packet precision | Needs regularization to avoid selecting everything or local-only nodes |
| Node-to-evidence projection | Converts hot Nodes/Models into only the evidence needed | Major token savings | Better evidence grounding | Must preserve counterevidence and provenance |
| Writer utility rewards | Scores graph writes by downstream usefulness | Reduces graph bloat over time | Better Models and retrieval affordances | Rewards must include counterevidence and falsification, not just recall |
| Outcome-based topology optimization | Learns from validation, user feedback, and later falsification | Compounding improvement | Self-correcting graph memory | Requires strong instrumentation before learning |
| Negative memory | Stores rejected hypotheses/paths | Avoids repeated wasted retrieval | Less recurring noise | Rejections need expiry because reality changes |
| Prediction-error attention | Focuses graph updates where Models fail expectations | Prioritizes high-value updates | Better adaptation to changing company state | Needs model predictions and falsification hooks |

---

## 5. New Core Concepts

## 5.1 Canonical Synthesis Graph

The validated source of truth.

Contains:

- Nodes
- Models
- relationships
- relationship participants
- evidence links
- confidence
- authority
- status
- falsification conditions
- lifecycle metadata
- access control
- state-change ledger

Properties:

- Conservative
- Evidence-backed
- Validated before mutation
- Auditable
- Permission-safe
- Supports trace-back and trace-forward

This layer should not be updated by learned topology logic directly.

## 5.2 Discovery Topology

A derived, adaptive layer that helps retrieval and inquiry.

Contains:

- retrieval affordance profiles
- discovery shortcut edges
- bridge scores
- hub suppression scores
- graph structural features
- query-conditioned utility weights
- sufficient-state summaries
- residuals
- prediction-error fields
- negative memory
- region priority scores

Properties:

- Rebuildable
- Probabilistic
- Learned over time
- Used for retrieval planning and graph reading
- Not treated as canonical truth

## 5.3 Synthesis Reader

Reads from Synthesis for a given inquiry.

Input:

```text
signal + hypotheses + current question + evidence state
```

Output:

```text
activated Nodes/Models + selected subgraph + evidence candidates
```

Pipeline:

```text
structured cue extraction
→ retrieval intent inference
→ soft activation
→ structurally gated propagation
→ query-conditioned subgraph selection
→ Node-to-evidence projection
```

## 5.4 Synthesis Writer

Writes or proposes updates to the graph.

There are two write modes:

```text
Canonical write:
  Validated state change to Nodes/Models/relationships.

Discovery write:
  Utility update to retrieval affordances, shortcuts, bridge scores, summaries, residuals, negative memory.
```

Canonical writes go through validation.

Discovery writes can update faster, but must be clearly separated from truth.

## 5.5 Outcome Evaluator

Evaluates the result of each inquiry and write.

Inputs:

- retrieval traces
- selected evidence
- omitted evidence
- context packet
- reasoning diff
- validation result
- user acceptance / contestation
- later confirmation / falsification
- latency and token cost

Output:

- labels for useful paths
- labels for noisy paths
- reward features for writer/reader policies
- topology update events

## 5.6 Topology Optimizer

Updates the discovery topology based on outcomes.

Responsibilities:

- update retrieval affordance profiles
- strengthen useful graph paths
- weaken noisy paths
- update bridge scores
- update hub penalties
- update sufficient-state summaries
- update negative memory
- update residuals and prediction-error fields
- recommend canonical merge/split/promote/demote operations for Models

---

## 6. Detailed Runtime Architecture

### 6.1 Existing Fyralis Reasoning Pipeline

Current inferential path:

```text
Retrieve
→ Reason
→ Validate
→ Apply
```

New SAGE-inspired path:

```text
Read from Synthesis
→ Compile context
→ Reason
→ Validate
→ Apply canonical update
→ Evaluate outcome
→ Optimize discovery topology
```

This extends the pipeline without breaking it.

### 6.2 End-to-End Flow

```text
1. Signal enters Ingestion.
2. Signal becomes immutable observation.
3. Routing decides Fast Path, Deep Inquiry Path, or Background Path.
4. Inquiry Reader receives signal + hypothesis/question context.
5. Reader extracts structured cues.
6. Reader creates retrieval intents.
7. Reader softly activates Nodes/Models.
8. Reader performs structurally gated propagation.
9. Reader selects a compact query-conditioned subgraph.
10. Reader projects selected Nodes/Models into evidence candidates.
11. Context Packet Compiler creates Synthesis Context Packet.
12. Deep Reasoning Agent proposes Node/Model diff.
13. Validator checks invariants.
14. Apply Layer writes canonical Synthesis update.
15. Outcome Evaluator scores the inquiry and write.
16. Topology Optimizer updates discovery topology.
17. Future retrieval improves.
```

---

## 7. Synthesis Reader Design

The Synthesis Reader is the Fyralis adaptation of SAGE's memory reader.

It should not simply run semantic search or k-hop traversal.

It should perform query-conditioned graph reading.

## 7.1 Inputs

```json
{
  "tenant_id": "tenant_1",
  "signal_id": "obs_123",
  "current_question": "Is SSO actually on Acme's critical path?",
  "active_hypotheses": ["H1", "H2", "H3", "H0"],
  "known_entities": ["Acme", "SSO", "Sales"],
  "evidence_state_id": "evs_456",
  "budget": {
    "max_nodes": 300,
    "max_evidence_items": 100,
    "max_tokens_for_packet": 30000
  }
}
```

## 7.2 Stage A: Structured Cue Extraction

Extract company-state cues from signal/question/context.

Cue fields:

```text
explicit_entities
aliases
actor_mentions
team_mentions
customer_mentions
system_mentions
goal_mentions
commitment_mentions
relationship_clues
time_constraints
status_constraints
source_constraints
access_constraints
expected_synthesis_decision_type
```

Example:

```json
{
  "explicit_entities": ["Acme", "SSO"],
  "aliases": ["single sign-on", "enterprise login"],
  "relationship_clues": ["depends_on", "blocks", "critical_path"],
  "time_constraints": {
    "recent_window_days": 30
  },
  "expected_synthesis_decision_type": [
    "update_commitment_risk",
    "create_emerging_bottleneck_model"
  ]
}
```

### Impact

- Prevents the reader from relying on raw text similarity.
- Gives retrieval precise company-state handles.
- Makes implicit aliases and constraints explicit.
- Reduces wasted retrieval.

## 7.3 Stage B: Retrieval Intent Inference

Turn cues into retrieval intents.

Example intents:

```json
[
  {
    "intent": "find_active_commitment",
    "target": "Acme onboarding",
    "paths": ["exact", "structural"]
  },
  {
    "intent": "test_dependency",
    "target": "SSO critical path",
    "paths": ["structural", "temporal", "semantic"]
  },
  {
    "intent": "find_counterevidence",
    "target": "non-SSO blockers",
    "paths": ["counterevidence", "semantic", "recent_observations"]
  }
]
```

### Impact

- Makes retrieval question-specific.
- Prevents one-size-fits-all retrieval.
- Creates auditable retrieval rationale.

## 7.4 Stage C: Soft Activation

Compute an initial activation score for Nodes/Models.

Activation signals:

```text
exact entity match
alias match
semantic similarity
shared subject
shared goal
shared resource
shared customer
shared actor/team
recent temporal proximity
hypothesis relevance
question relevance
existing retrieval affordance match
```

Example formula:

```text
activation(node, query) =
    w_exact * exact_match
  + w_alias * alias_match
  + w_semantic * semantic_similarity
  + w_subject * subject_overlap
  + w_goal * goal_overlap
  + w_resource * resource_overlap
  + w_temporal * temporal_relevance
  + w_affordance * retrieval_affordance_match
```

This should be capped and normalized.

### Impact

- Recovers relevant Nodes not explicitly mentioned.
- Finds bridge concepts like platform capacity or security review.
- Reduces dependence on literal matching.

## 7.5 Stage D: Structurally Gated Propagation

Propagate activation through the graph, but not uniformly.

Each edge gets a gate score for the current question.

Gate features:

```text
edge type
edge confidence
source node degree
target node degree
source clustering coefficient
target clustering coefficient
core number
average neighbor degree
common neighbors
Jaccard overlap
bridge score
hub score
community boundary score
relationship recency
status
source trust
role compatibility
access compatibility
```

Heuristic gate v1:

```text
gate(edge, question) =
    relation_type_weight
  * trust_weight
  * freshness_weight
  * role_compatibility
  * bridge_bonus
  * hub_penalty
  * access_allowed
```

Learned gate v2:

```text
small ranking model(edge_features, question_features) -> gate_probability
```

### Impact

- Suppresses generic hubs.
- Preserves bridge Nodes.
- Avoids uniform graph diffusion.
- Improves evidence signal-to-noise ratio.
- Reduces number of Nodes needed for retrieval.

## 7.6 Stage E: Query-Conditioned Subgraph Selection

Select a compact Node/Model subgraph for the current question.

Selector input:

```text
activated nodes
propagated scores
topological features
question embedding
hypothesis links
budget constraints
```

Selector output:

```json
{
  "selected_nodes": ["node_1", "node_2", "node_3"],
  "selected_relationships": ["rel_1", "rel_2"],
  "bridge_nodes": ["node_bridge_1"],
  "excluded_high_score_nodes": [
    {
      "node_id": "node_generic_platform",
      "reason": "generic hub; summarized instead"
    }
  ]
}
```

Regularization rules:

- Penalize selecting too many nodes.
- Penalize generic hub selection without role justification.
- Penalize redundant local clusters.
- Reward bridge coverage.
- Reward counterevidence inclusion.
- Reward coverage of required evidence roles.

### Impact

- Prevents graph-reading sprawl.
- Reduces packet size before token pruning.
- Improves reasoning precision.

## 7.7 Stage F: Node-to-Evidence Projection

Convert selected Nodes/Models into evidence candidates.

For each selected Node/Model, choose:

```text
decisive supporting evidence
counterevidence
freshest confirmation
falsification-relevant evidence
evidence explaining confidence
minimal provenance chain
```

Projection output:

```json
{
  "node_id": "node_acme_onboarding_risk",
  "projected_evidence": [
    {
      "evidence_id": "crm_note_45",
      "reason": "directly supports SSO critical path",
      "include_level": "raw_excerpt"
    },
    {
      "evidence_id": "linear_issue_91",
      "reason": "authoritative delivery status",
      "include_level": "evidence_card"
    },
    {
      "evidence_id": "support_ticket_22",
      "reason": "counterevidence: possible data migration blocker",
      "include_level": "evidence_card"
    }
  ]
}
```

### Impact

- Prevents activated Nodes from dragging full history into prompts.
- Makes evidence selection role-aware.
- Reduces token cost sharply.
- Preserves support and counterevidence.

---

## 8. Synthesis Writer Design

The Synthesis Writer is responsible for generating or proposing graph writes.

Unlike SAGE, Fyralis does not merely write entity-relation triples. Fyralis writes evidence-backed claims.

## 8.1 Write Types

### Canonical Writes

Validated writes to the Synthesis graph.

Examples:

```text
create Node
update Node
archive Node
create Model
update Model
create relationship
add evidence link
update confidence
add falsification condition
create recommendation
```

Canonical writes must pass validation.

### Discovery Writes

Adaptive utility writes to the discovery topology.

Examples:

```text
update retrieval affordance profile
create discovery shortcut
update bridge score
update hub penalty
update residual field
update prediction-error score
update region summary
update negative memory
update Node-to-evidence projection preference
```

Discovery writes do not change truth. They change retrieval utility.

## 8.2 Canonical Write Schema

```json
{
  "write_type": "canonical_model_create",
  "claim": "Enterprise onboarding risk is emerging around an unresolved SSO/platform dependency.",
  "participants": [
    { "role": "affected_customer", "node_id": "node_acme" },
    { "role": "dependency", "node_id": "node_sso_delivery" },
    { "role": "resource_constraint", "node_id": "node_platform_capacity" },
    { "role": "affected_goal", "node_id": "node_q3_revenue" }
  ],
  "supporting_evidence": ["crm_note_45", "linear_issue_91"],
  "counterevidence": ["support_ticket_22"],
  "confidence": 0.71,
  "falsification_conditions": [
    "Affected enterprise accounts launch without SSO.",
    "SSO is removed from critical path.",
    "Platform capacity improves while onboarding risk remains unchanged."
  ],
  "expected_predictions": [
    "Other enterprise accounts requiring SSO will face similar launch risk if platform/security capacity remains constrained."
  ],
  "action_affordances": [
    "assign_security_review_owner",
    "reprioritize_platform_capacity"
  ]
}
```

## 8.3 Discovery Write Schema

```json
{
  "write_type": "discovery_shortcut_update",
  "from_signature": {
    "signal_type": "enterprise_customer_blocker",
    "entities": ["customer", "SSO"],
    "question_primitive": "DEPENDENCY"
  },
  "to_region": "platform_security_capacity",
  "utility_delta": 0.17,
  "evidence": {
    "inquiry_session_id": "inq_123",
    "used_in_valid_diff": true,
    "supporting_evidence_ids": ["crm_note_45", "linear_issue_91"]
  },
  "expires_at": "2026-08-01T00:00:00Z"
}
```

### Impact

- Canonical truth remains safe.
- Search policy can learn quickly.
- The graph becomes more useful without corrupting evidence-backed state.

---

## 9. Retrieval Affordance Profiles

Every important Node/Model should maintain a derived retrieval affordance profile.

This profile answers:

```text
What kinds of questions does this Node help answer?
What hypotheses does it support or weaken?
What abstractions does it commonly participate in?
What future signals should activate it?
What evidence should be projected if it becomes relevant?
```

## 9.1 Schema

```sql
CREATE TABLE retrieval_affordance_profiles (
  node_id uuid PRIMARY KEY REFERENCES nodes(id),
  tenant_id uuid NOT NULL,
  answers_question_primitives text[] NOT NULL DEFAULT '{}',
  supports_hypothesis_types text[] NOT NULL DEFAULT '{}',
  weakens_hypothesis_types text[] NOT NULL DEFAULT '{}',
  common_composition_types text[] NOT NULL DEFAULT '{}',
  action_affordances text[] NOT NULL DEFAULT '{}',
  activation_signatures jsonb NOT NULL DEFAULT '{}',
  projection_policy jsonb NOT NULL DEFAULT '{}',
  utility_score float NOT NULL DEFAULT 0,
  last_updated_at timestamptz NOT NULL
);
```

## 9.2 Example

```json
{
  "node_id": "node_platform_capacity_low",
  "answers_question_primitives": [
    "CONSTRAINT",
    "CAUSE",
    "DEPENDENCY",
    "ACTION"
  ],
  "supports_hypothesis_types": [
    "resource_bottleneck",
    "execution_risk",
    "delivery_slippage"
  ],
  "weakens_hypothesis_types": [
    "customer_readiness_primary_blocker"
  ],
  "common_composition_types": [
    "enterprise_onboarding_bottleneck",
    "platform_dependency_risk"
  ],
  "action_affordances": [
    "reallocate_capacity",
    "de_scope_work",
    "assign_owner"
  ]
}
```

### Impact

- Makes Nodes searchable by reasoning function, not only content.
- Reduces search entropy.
- Speeds up question-driven retrieval.

---

## 10. Structural Feature Store

SAGE shows that structural features help distinguish hubs, bridges, dense neighborhoods, and useful paths.

Fyralis should compute explicit graph features for Nodes and relationships.

## 10.1 Node Structural Features

```sql
CREATE TABLE node_structural_features (
  node_id uuid PRIMARY KEY REFERENCES nodes(id),
  tenant_id uuid NOT NULL,
  degree_total int NOT NULL,
  degree_in int NOT NULL,
  degree_out int NOT NULL,
  clustering_coefficient float,
  core_number int,
  avg_neighbor_degree float,
  bridge_score float,
  hub_score float,
  community_id uuid,
  region_ids uuid[],
  updated_at timestamptz NOT NULL
);
```

## 10.2 Edge / Relationship Structural Features

```sql
CREATE TABLE relationship_structural_features (
  relationship_id uuid PRIMARY KEY REFERENCES relationships(id),
  tenant_id uuid NOT NULL,
  source_node_id uuid NOT NULL,
  target_node_id uuid NOT NULL,
  degree_difference float,
  common_neighbors int,
  jaccard_overlap float,
  edge_betweenness_approx float,
  bridge_likelihood float,
  redundancy_score float,
  updated_at timestamptz NOT NULL
);
```

### Impact

- Supports structural gating.
- Helps detect graph bloat.
- Enables hub suppression and bridge preservation.
- Creates measurable topology quality.

---

## 11. Discovery Shortcuts

Discovery shortcuts are learned utility paths.

They are not truth edges.

## 11.1 Schema

```sql
CREATE TABLE discovery_shortcuts (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  from_signature jsonb NOT NULL,
  to_node_id uuid,
  to_region_id uuid,
  to_affordance text,
  utility_score float NOT NULL,
  success_count int NOT NULL DEFAULT 0,
  failure_count int NOT NULL DEFAULT 0,
  last_success_at timestamptz,
  last_failure_at timestamptz,
  expires_at timestamptz,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
```

## 11.2 Example

```json
{
  "from_signature": {
    "signal_type": "enterprise_customer_blocker",
    "entities": ["customer", "SSO"],
    "question_primitive": "DEPENDENCY"
  },
  "to_region": "platform_security_capacity",
  "utility_score": 0.84,
  "success_count": 27,
  "failure_count": 3
}
```

### Impact

- Makes future inquiry faster.
- Captures tenant-specific operating patterns.
- Avoids recomputing search paths from scratch.
- Keeps search utility separate from canonical truth.

---

## 12. Sufficient-State Summaries

Important regions and Models should maintain compact summaries of what matters for future reasoning.

## 12.1 Region Summary Schema

```sql
CREATE TABLE region_sufficient_state (
  region_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  summary text NOT NULL,
  active_hypotheses jsonb NOT NULL DEFAULT '[]',
  active_constraints jsonb NOT NULL DEFAULT '[]',
  known_counterevidence jsonb NOT NULL DEFAULT '[]',
  unresolved_unknowns jsonb NOT NULL DEFAULT '[]',
  affected_goals uuid[] NOT NULL DEFAULT '{}',
  affected_commitments uuid[] NOT NULL DEFAULT '{}',
  priority_score float NOT NULL DEFAULT 0,
  prediction_error_score float NOT NULL DEFAULT 0,
  next_best_frontiers jsonb NOT NULL DEFAULT '[]',
  falsification_watch jsonb NOT NULL DEFAULT '[]',
  updated_at timestamptz NOT NULL
);
```

## 12.2 Example

```json
{
  "region": "enterprise_onboarding",
  "summary": "Enterprise onboarding risk is currently concentrated around SSO readiness, platform/security capacity, and unclear security review ownership.",
  "active_constraints": [
    "SSO delivery",
    "platform capacity",
    "security review ownership"
  ],
  "known_counterevidence": [
    "data migration appears as secondary blocker for Acme"
  ],
  "unresolved_unknowns": [
    "security review owner",
    "formal Sales commitment status"
  ],
  "next_best_frontiers": [
    "SSO delivery state",
    "security review ownership",
    "other enterprise accounts requiring SSO"
  ]
}
```

### Impact

- Gives retrieval compressed starting points.
- Reduces repeated raw traversal.
- Improves reasoning over active regions.
- Helps prioritize high-gravity areas.

---

## 13. Residuals and Prediction Error

Strong Models should imply expectations.

When reality violates an expectation, Fyralis should focus inquiry there.

## 13.1 Model Prediction Schema

```sql
CREATE TABLE model_predictions (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  model_id uuid NOT NULL REFERENCES nodes(id),
  prediction text NOT NULL,
  expected_observation jsonb NOT NULL,
  check_after timestamptz,
  status text NOT NULL DEFAULT 'active',
  confidence float,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);
```

## 13.2 Prediction Error Schema

```sql
CREATE TABLE model_prediction_errors (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  model_id uuid NOT NULL REFERENCES nodes(id),
  prediction_id uuid REFERENCES model_predictions(id),
  observed_signal_id uuid,
  error_summary text NOT NULL,
  severity float NOT NULL,
  impact_score float NOT NULL,
  status text NOT NULL DEFAULT 'open',
  created_at timestamptz NOT NULL
);
```

### Example

Model predicts:

```text
If SSO/security ownership resolves, enterprise onboarding risk should decrease.
```

Observed:

```text
Acme remains blocked after SSO shipped.
```

Residual inquiry:

```text
What unexplained constraint remains?
What evidence invalidates the current Model?
What new blocker appeared?
```

### Impact

- Moves retrieval from relevance search to residual search.
- Makes Synthesis self-correcting.
- Prioritizes high-impact model failures.

---

## 14. Negative Memory

Store rejected hypotheses, failed paths, and noisy shortcuts.

## 14.1 Schema

```sql
CREATE TABLE negative_memory (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  memory_type text NOT NULL,
  signature jsonb NOT NULL,
  rejected_claim text,
  rejected_path jsonb,
  reason text NOT NULL,
  evidence_snapshot_hash text,
  confidence float,
  created_at timestamptz NOT NULL,
  expires_at timestamptz
);
```

## 14.2 Example

```json
{
  "memory_type": "rejected_hypothesis",
  "rejected_claim": "Acme onboarding is primarily blocked by data migration.",
  "reason": "Only weak support; CRM and Linear evidence indicate SSO is primary blocker.",
  "expires_at": "2026-06-30T00:00:00Z"
}
```

### Impact

- Prevents repeated wasted inquiry.
- Reduces graph noise.
- Improves token efficiency.
- Requires expiry because company reality changes.

---

## 15. Outcome Evaluation

Every inquiry and reasoning run should produce labels for self-evolution.

## 15.1 Outcome Events

```sql
CREATE TABLE inquiry_outcome_events (
  id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  inquiry_session_id uuid NOT NULL,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL
);
```

Event types:

```text
retrieved_evidence_used_in_packet
retrieved_evidence_omitted
omitted_evidence_later_requested
node_used_in_valid_diff
path_used_in_valid_diff
validation_failed_due_to_missing_evidence
validation_failed_due_to_bad_reference
user_accepted_node
user_contested_node
model_later_confirmed
model_later_falsified
recommendation_acted_on
recommendation_ignored
```

## 15.2 Outcome Labels

Derived labels:

```text
useful_node
useful_relationship
useful_path
useful_bridge
noisy_hub
redundant_evidence
missing_evidence_anchor
bad_write_pattern
good_write_pattern
```

### Impact

- Converts usage into training data.
- Makes graph improvement measurable.
- Allows the topology optimizer to learn from reality, not vibes.

---

## 16. Topology Optimizer

The Topology Optimizer runs after inquiries and Synthesis writes.

## 16.1 Trigger Conditions

Run when:

```text
validated Synthesis diff applied
reasoning diff fails validation
user contests a Node
user accepts/rejects recommendation
prediction is confirmed/falsified
omitted evidence is later requested
inquiry session ends with insufficient evidence
background region scan completes
```

## 16.2 Responsibilities

```text
1. Update retrieval affordance profiles.
2. Update discovery shortcuts.
3. Update bridge and hub scores.
4. Update sufficient-state summaries.
5. Update prediction-error fields.
6. Create negative memory for failed paths.
7. Recommend model merge/split/promote/demote actions.
8. Refresh retrieval frontiers.
9. Emit topology metrics.
```

## 16.3 Pseudocode

```python
def optimize_topology(event):
    session = load_inquiry_session(event.inquiry_session_id)
    outcomes = collect_outcome_events(session)

    useful_paths = infer_useful_paths(outcomes)
    noisy_paths = infer_noisy_paths(outcomes)
    useful_nodes = infer_useful_nodes(outcomes)
    missing_anchors = infer_missing_anchors(outcomes)

    update_retrieval_affordances(useful_nodes, outcomes)
    update_discovery_shortcuts(useful_paths, noisy_paths)
    update_bridge_scores(useful_paths)
    update_hub_penalties(noisy_paths)
    update_negative_memory(noisy_paths, rejected_hypotheses=outcomes.rejections)
    refresh_region_sufficient_state(session.affected_regions)
    update_prediction_error_fields(outcomes)

    candidate_topology_ops = propose_canonical_topology_ops(outcomes)
    enqueue_for_validation(candidate_topology_ops)
```

Canonical topology ops might include:

```text
merge duplicate Models
split overloaded Model
promote repeated composition into Model
archive stale Model
demote low-utility Model
add missing evidence anchor
```

These must go through validation before changing canonical truth.

### Impact

- Creates a flywheel.
- Makes Synthesis improve as it operates.
- Reduces future inquiry cost.
- Turns graph arrangement into a learned product capability.

---

## 17. Writer Utility Reward

A Synthesis write should be scored by more than local truth.

## 17.1 Reward Formula

```text
Synthesis Write Utility =
    evidence_coverage
  + diff_deducibility
  + future_inquiry_cost_reduction
  + compression_gain
  + prediction_falsification_value
  + action_value
  + counterevidence_preservation
  - graph_bloat
  - redundancy
  - noise_introduced
  - token_cost
  - permission_risk
```

## 17.2 Definitions

### Evidence Coverage

Did the write preserve enough evidence for future retrieval?

### Diff Deducibility

Could a future reasoning agent derive a valid Synthesis diff from retrieved context?

### Future Inquiry Cost Reduction

Did this write reduce rounds, tokens, or retrieval steps in later related inquiries?

### Compression Gain

Did the write compress lower-level Nodes into a useful Model?

### Prediction / Falsification Value

Did it produce expectations or falsifiers?

### Action Value

Did it identify possible interventions or recommendations?

### Counterevidence Preservation

Did it keep track of evidence that could weaken the claim?

### Graph Bloat

Did it add redundant, weak, or low-utility structure?

### Permission Risk

Did it increase sensitive graph exposure?

### Impact

- Prevents graph bloat.
- Rewards useful abstraction, not just recall.
- Aligns graph writing with future reasoning.

---

## 18. Implementation Plan

This is the recommended end-to-end implementation plan.

Do not start with a Graph Foundation Model or RL writer. That is how product teams accidentally become research labs with invoices. Start observable, deterministic, and incremental.

---

## Phase 0: Prerequisites

### Goal

Ensure existing architecture exposes enough data to instrument read/write outcomes.

### Required Existing Components

- Nodes table
- Observations table
- Evidence links
- Relationship representation
- Reasoning diff ledger
- Validation stage
- Apply stage
- Trigger/inquiry session concept
- Access control metadata

### Deliverables

- Confirm canonical Node IDs are stable.
- Confirm evidence references are stable.
- Confirm reasoning diffs are logged.
- Confirm validation failures are logged.
- Confirm user accept/contest events can be captured.

---

## Phase 1: Instrument Inquiry and Outcome Logging

### Goal

Capture the data needed to learn later.

### Build Tables

- `inquiry_sessions`
- `inquiry_questions`
- `retrieval_plans`
- `retrieved_evidence`
- `context_packets`
- `omitted_evidence`
- `inquiry_outcome_events`

### Required Events

Log:

```text
signal received
hypotheses generated
questions generated
retrieval plans executed
evidence retrieved
evidence included in packet
evidence omitted
reasoning diff proposed
validation result
diff applied
user accepted/contested
prediction confirmed/falsified
```

### Acceptance Criteria

A developer can inspect one inquiry session and answer:

```text
What was asked?
What was retrieved?
What was used?
What was omitted?
What changed in Synthesis?
Did validation pass?
What happened later?
```

### Impact

This phase does not yet improve intelligence. It makes improvement possible. Boring, essential, and therefore likely to be underrated by someone with a roadmap.

---

## Phase 2: Add Structured Cue Extraction

### Goal

Split inquiry planning into cue extraction and intent inference.

### Build Component

`CueExtractor`

### Input

```json
{
  "signal": {},
  "question": "...",
  "hypotheses": [],
  "evidence_state": {}
}
```

### Output

```json
{
  "explicit_entities": [],
  "aliases": [],
  "relation_clues": [],
  "time_constraints": {},
  "source_constraints": {},
  "access_constraints": {},
  "expected_synthesis_decision_type": []
}
```

### Implementation v1

- deterministic entity extraction
- alias table lookup
- regex/entity dictionary for known systems/customers/projects
- small model fallback for messy text

### Acceptance Criteria

- Extracts explicit and alias entities with confidence.
- Extracts relation clues like dependency, ownership, contradiction, blocker, goal impact.
- Emits constraints usable by retrieval compiler.

### Impact

- Reduces retrieval ambiguity.
- Improves first-stage activation.
- Creates better auditability.

---

## Phase 3: Add Retrieval Intent Inference

### Goal

Convert cues into concrete retrieval intents.

### Build Component

`RetrievalIntentInferer`

### Input

Cue output + Evidence State.

### Output

```json
[
  {
    "intent": "test_dependency",
    "question_id": "Q1",
    "target": "SSO critical path for Acme launch",
    "paths": ["structural", "temporal", "semantic"],
    "success_condition": "evidence for or against critical path found"
  }
]
```

### Implementation v1

Rule-based mappings:

```text
DEPENDENCY -> structural + temporal + semantic
OWNERSHIP -> exact + structural + actor/team graph
CONTRADICTION -> same-subject + counterevidence + recent observations
PATTERN -> semantic + historical + region summaries
ACTION -> downstream dependency + ownership + goal impact
```

### Acceptance Criteria

- Every selected question has at least one retrieval intent.
- Every intent has path, budget, constraints, and success condition.

### Impact

- Makes retrieval question-conditioned.
- Prevents generic retrieval by default.

---

## Phase 4: Add Soft Activation Layer

### Goal

Activate relevant Nodes/Models through multiple cues, not only exact matches.

### Build Tables / Indexes

- entity alias index
- Node basis fields
- retrieval affordance profile table
- semantic embeddings already exist

### Activation Inputs

```text
exact entity match
alias match
semantic similarity
subject overlap
goal overlap
resource overlap
time relevance
retrieval affordance match
```

### Output

```json
{
  "activated_nodes": [
    {
      "node_id": "node_sso_delivery",
      "activation_score": 0.91,
      "activation_reasons": ["exact:SSO", "dependency intent"]
    }
  ]
}
```

### Acceptance Criteria

- Recovers relevant non-explicit Nodes in test cases.
- Activation is capped and explainable.
- Activation reasons are stored.

### Impact

- Finds bridge candidates earlier.
- Reduces reliance on brute-force retrieval.

---

## Phase 5: Add Structural Feature Store

### Goal

Compute topological features needed for structural gating.

### Build Jobs

- local graph feature computation job
- incremental update on graph writes
- periodic full recomputation

### Compute

```text
degree_total
degree_in
degree_out
clustering_coefficient
core_number
average_neighbor_degree
community_id
bridge_score
hub_score
relationship common neighbors
relationship Jaccard overlap
relationship bridge likelihood
```

### Acceptance Criteria

- Features available for active Nodes/relationships.
- Updated after relevant graph changes.
- Queryable by reader in low latency.

### Impact

- Enables hub suppression and bridge preservation.
- Creates measurable graph topology health.

---

## Phase 6: Add Heuristic Structural Gates

### Goal

Replace uniform propagation with question-conditioned gated propagation.

### Build Component

`StructuralGateScorer`

### Gate v1

```python
def score_edge(edge, question, features):
    score = 1.0
    score *= relation_type_weight(edge.type, question.primitive)
    score *= trust_weight(edge.trust_tier)
    score *= freshness_weight(edge.updated_at)
    score *= role_compatibility(edge, question)
    score *= bridge_bonus(features.bridge_score)
    score *= hub_penalty(features.hub_score, question.primitive)
    score *= access_allowed(edge)
    return clamp(score, 0, 1)
```

### Acceptance Criteria

- Hubs are dampened in root-cause/counterevidence questions.
- Resource hubs can remain active for bottleneck/constraint questions.
- Bridge Nodes survive pruning when useful.
- Gate scores are logged and explainable.

### Impact

- Improves signal-to-noise.
- Avoids naive heat diffusion failure modes.
- Gives a bridge from current heuristics to future learned reader.

---

## Phase 7: Add Query-Conditioned Subgraph Selector

### Goal

Select compact Node/Model subgraphs before packet compilation.

### Build Component

`SubgraphSelector`

### Input

- activated Nodes
- propagated scores
- structural features
- question/hypothesis context
- budget

### Output

```json
{
  "selected_nodes": [],
  "selected_relationships": [],
  "bridge_nodes": [],
  "summarized_hubs": [],
  "excluded_nodes": []
}
```

### Selection Rules v1

Keep Nodes that:

```text
answer current question
fill missing evidence role
support or weaken active hypothesis
connect two otherwise separate regions
provide counterevidence
support candidate state change
```

Drop/summarize Nodes that:

```text
are generic hubs
are redundant local confirmations
are stale
are low trust and unsupported
are outside access scope
```

### Acceptance Criteria

- Reduces candidate graph size while preserving decisive evidence.
- Produces omission reasons.
- Includes counterevidence when available.

### Impact

- Cuts token cost before text-level compression.
- Improves reasoning packet quality.

---

## Phase 8: Add Node-to-Evidence Projection

### Goal

Convert activated Nodes/Models into minimal evidence candidates.

### Build Component

`EvidenceProjector`

### Projection Policy

For each selected Node/Model:

```text
include direct support evidence
include direct counterevidence
include freshest confidence-updating evidence
include falsification-relevant evidence
summarize redundant support
omit stale low-value evidence
```

### Output

```json
{
  "node_id": "node_sso_delivery",
  "projected_evidence": [
    {
      "evidence_id": "linear_issue_91",
      "include_level": "evidence_card",
      "reason": "authoritative delivery state"
    }
  ]
}
```

### Acceptance Criteria

- Evidence projection includes support and counterevidence.
- Raw excerpts are included only for decisive evidence.
- Projection choices are logged.

### Impact

- Major token savings.
- Better evidence-grounded reasoning.
- Less prompt contamination.

---

## Phase 9: Add Retrieval Affordance Profiles

### Goal

Make Nodes searchable by reasoning function.

### Build Table

`retrieval_affordance_profiles`

### Populate v1

From:

```text
Node content shape
relationship roles
evidence type
past inquiry usage
manual heuristics
reasoning outcome labels
```

### Update Rules

Increase utility if Node:

```text
appears in final packet
is used in valid diff
is later confirmed
helps answer recurring question
acts as bridge
provides counterevidence
```

Decrease utility if Node:

```text
frequently retrieved but omitted
causes validation failure
is contested/falsified
acts as noisy hub
```

### Acceptance Criteria

- Reader can retrieve by affordance.
- Affordance updates are explainable.
- Utility scores decay over time unless reinforced.

### Impact

- Reduces search entropy.
- Makes future inquiry faster.
- Helps Fyralis learn each tenant’s operating model.

---

## Phase 10: Add Discovery Shortcuts and Negative Memory

### Goal

Learn which graph paths are useful or noisy.

### Build Tables

- `discovery_shortcuts`
- `negative_memory`

### Update Rules

When a path appears in a valid diff:

```text
increase shortcut utility
increase bridge score
update activation signature
```

When a path produces noise:

```text
create negative memory
reduce shortcut utility
increase hub penalty if applicable
```

### Acceptance Criteria

- Similar future inquiries use learned shortcuts.
- Rejected hypotheses are not repeatedly rediscovered.
- Negative memory expires or is invalidated when evidence changes.

### Impact

- Reduces repeated retrieval waste.
- Speeds up tenant-specific discovery.

---

## Phase 11: Add Sufficient-State Summaries

### Goal

Maintain compact active summaries for important regions.

### Build Table

`region_sufficient_state`

### Update Triggers

```text
validated model update
new high-impact signal
prediction error
user contestation
scheduled review
region anomaly
```

### Acceptance Criteria

- Region summaries are current and evidence-backed.
- Inquiry can start from summary before raw graph traversal.
- Summary includes unknowns and counterevidence, not only narrative.

### Impact

- Reduces repeated retrieval.
- Improves region-level reasoning.
- Helps background intelligence.

---

## Phase 12: Add Prediction and Residual Tracking

### Goal

Make Models produce expectations and detect when reality violates them.

### Build Tables

- `model_predictions`
- `model_prediction_errors`

### Writer Requirement

New high-level Models should include at least one of:

```text
prediction
expected observation
falsification condition
residual uncertainty
```

### Acceptance Criteria

- Scheduled checks can test predictions.
- Prediction errors trigger inquiry.
- Residual search uses failed expectations as retrieval anchors.

### Impact

- Makes Synthesis self-correcting.
- Prioritizes high-impact surprises.
- Improves company-state freshness.

---

## Phase 13: Add Outcome Evaluator and Topology Optimizer

### Goal

Close the reader-writer loop.

### Build Components

- `OutcomeEvaluator`
- `TopologyOptimizer`

### Initial Implementation

Rule-based updates using outcome labels.

### Later Implementation

Train ranking models for:

```text
edge utility
Node affordance utility
subgraph selection
Node-to-evidence projection
shortcut ranking
```

### Acceptance Criteria

- Every inquiry produces topology update events.
- Future retrieval metrics improve over time.
- Graph bloat metrics do not worsen.

### Impact

- Turns Fyralis into a self-improving Synthesis system.
- Makes retrieval and graph writing mutually reinforcing.

---

## Phase 14: Learned Reader v1

### Goal

Replace heuristic gates and selectors with learned models where enough data exists.

### Training Data

From inquiry logs:

```text
question
hypotheses
activated nodes
selected nodes
used evidence
omitted evidence
valid diff labels
user feedback
future confirmation/falsification
```

### Models

Start with simple models:

```text
GBDT / XGBoost / LightGBM for edge utility
logistic regression for subgraph selection
small embedding ranker for evidence projection
```

Do not start with a large GFM unless enough data exists.

### Acceptance Criteria

- Learned models outperform heuristics on held-out inquiry sessions.
- Improvements measured by evidence recall, token value density, valid diff rate, and latency.

### Impact

- Moves Fyralis from manually tuned retrieval to learned graph reading.

---

## Phase 15: Reader-Aware Writer Training

### Goal

Train writing policies by downstream inquiry utility.

### Candidate Writer Actions

```text
create relationship
add source anchor
update affordance profile
create region summary
add prediction
add residual uncertainty
add discovery shortcut
add negative memory
recommend Model merge/split/promote/demote
```

### Reward

Use `Synthesis Write Utility`.

### Safety

Writer training proposes updates.

Canonical changes still go through validation.

### Acceptance Criteria

- Writer policies reduce future inquiry cost.
- No increase in invalid diffs.
- No increase in graph bloat.
- No reduction in counterevidence recall.

### Impact

This is the full SAGE-like loop.

Fyralis now learns not only how to read the graph, but how to write the graph so future reading improves.

---

## 19. Developer Build Order Summary

Recommended order:

```text
1. Instrument inquiry traces and outcomes.
2. Add cue extraction and retrieval intents.
3. Add soft activation.
4. Add structural feature store.
5. Add heuristic structural gates.
6. Add query-conditioned subgraph selector.
7. Add Node-to-evidence projection.
8. Add retrieval affordance profiles.
9. Add discovery shortcuts and negative memory.
10. Add sufficient-state summaries.
11. Add prediction/residual tracking.
12. Add outcome evaluator and topology optimizer.
13. Train learned reader models.
14. Train reader-aware writer policies.
```

Do not jump to Phase 14 or 15 before instrumentation and labels exist.

---

## 20. APIs and Interfaces

## 20.1 Reader API

```http
POST /internal/synthesis-reader/read
```

Request:

```json
{
  "tenant_id": "tenant_1",
  "signal_id": "obs_123",
  "question": "Is SSO actually on Acme's critical path?",
  "hypotheses": ["H1", "H2", "H3", "H0"],
  "evidence_state_id": "evs_456",
  "budget": {
    "max_nodes": 300,
    "max_evidence_items": 100
  }
}
```

Response:

```json
{
  "activated_nodes": [],
  "selected_subgraph": {},
  "projected_evidence": [],
  "omission_candidates": [],
  "debug": {
    "cue_extraction": {},
    "activation_reasons": {},
    "gate_scores": {},
    "selector_reasons": {}
  }
}
```

## 20.2 Topology Optimizer API

```http
POST /internal/topology-optimizer/optimize
```

Request:

```json
{
  "tenant_id": "tenant_1",
  "inquiry_session_id": "inq_123",
  "trigger_event": "validated_diff_applied"
}
```

Response:

```json
{
  "discovery_updates_applied": [],
  "canonical_update_candidates": [],
  "metrics": {}
}
```

## 20.3 Evidence Projection API

```http
POST /internal/evidence-projector/project
```

Request:

```json
{
  "tenant_id": "tenant_1",
  "node_ids": ["node_1", "node_2"],
  "question": "Is SSO on the critical path?",
  "hypotheses": ["H1", "H0"],
  "budget": {
    "max_evidence_per_node": 5
  }
}
```

Response:

```json
{
  "projected_evidence": []
}
```

---

## 21. Evaluation Metrics

## 21.1 Reader Metrics

| Metric | Meaning |
|---|---|
| Evidence Recall@K | Did the reader retrieve evidence later used in valid diff? |
| Counterevidence Recall | Did the reader retrieve evidence against leading hypothesis? |
| Bridge Node Recall | Did the reader find cross-region connectors? |
| Token Value Density | Useful evidence per token in packet |
| Subgraph Precision | Fraction of selected subgraph used in final reasoning |
| Subgraph Recall | Fraction of final-useful Nodes included by selector |
| Query Latency | Time to produce selected subgraph/evidence |
| Hub Pollution Rate | How often generic hubs dominate selected subgraph |

## 21.2 Writer Metrics

| Metric | Meaning |
|---|---|
| Write Deducibility | Can future context from this write support valid diff? |
| Compression Gain | Lower-level evidence compressed per Model |
| Future Inquiry Cost Reduction | Fewer rounds/tokens needed later |
| Redundancy Rate | Duplicate or low-utility writes |
| Falsification Coverage | High-confidence Models with testable falsifiers |
| Counterevidence Preservation | Whether writes preserve weakening evidence |
| Action Affordance Rate | Fraction of Models that support interventions |

## 21.3 Loop Metrics

| Metric | Meaning |
|---|---|
| Valid Diff Rate | Proposed diffs accepted by validator |
| Validation Failure Cause | Bad reference, missing evidence, bad confidence, etc. |
| User Contestation Rate | How often users contest generated Nodes |
| Later Confirmation Rate | Generated Models later confirmed |
| Later Falsification Rate | Generated Models later disproven |
| Omitted Evidence Expansion Rate | Agent later requests omitted evidence |
| Inquiry Rounds to Sufficiency | How many loops before sufficient packet |

---

## 22. Failure Modes and Guardrails

### 22.1 Utility Corrupts Truth

Risk:

Discovery shortcuts get mistaken for canonical relationships.

Guardrail:

- Separate canonical and discovery tables.
- Clearly label discovery metadata as retrieval utility.
- Never expose discovery shortcut as fact.

### 22.2 Graph Bloat

Risk:

Writer adds too many Nodes/relationships to improve recall.

Guardrail:

- Penalize redundancy.
- Track deducibility, not just recall.
- Require compression/action/falsification value for high-level Models.

### 22.3 Counterevidence Loss

Risk:

Optimization favors clean support chains and drops disconfirming evidence.

Guardrail:

- Reward counterevidence preservation.
- Require counterevidence projection in packet compiler.
- Track counterevidence recall.

### 22.4 Learned Shortcut Overfitting

Risk:

Tenant-specific shortcuts become stale or overfit to old workflows.

Guardrail:

- Add expiry and decay.
- Revalidate after workflow changes.
- Track failure rate.

### 22.5 Hub Suppression Removes True Causes

Risk:

A hub is noisy in many cases but causal in this one.

Guardrail:

- Hub policy must be question-conditioned.
- Resource hubs should survive constraint/bottleneck questions.
- Authority hubs should survive ownership questions.

### 22.6 Selector Degenerates

Risk:

Subgraph selector selects everything or only local high-frequency Nodes.

Guardrail:

- Penalize size.
- Reward bridge coverage.
- Reward evidence role coverage.
- Monitor subgraph precision and recall.

### 22.7 Privacy and Access Leakage

Risk:

Graph propagation crosses sensitive boundaries.

Guardrail:

- Apply ACL before propagation where required.
- Store sensitivity on Nodes/evidence.
- Packet compiler redacts or omits inaccessible evidence.

### 22.8 Bad Feedback Loops

Risk:

Incorrect accepted updates reinforce bad topology.

Guardrail:

- Use later falsification and contestation to reverse rewards.
- Keep provenance of topology updates.
- Decay shortcut weights unless reinforced.

---

## 23. Open Engineering Questions

1. Should discovery topology be stored entirely in Postgres, or should high-volume features move to a graph/vector sidecar?
2. How frequently should structural features be recomputed?
3. What is the smallest useful feature set for structural gating v1?
4. How should we represent tenant-specific schema priors without leaking cross-tenant data?
5. How should discovery shortcuts decay over time?
6. What is the correct threshold for promoting a repeated composition into a canonical Model?
7. How should omitted evidence expansion requests update projection policy?
8. Should query-conditioned subgraph selection run synchronously or as part of background packet compilation?
9. What is the first wedge domain for evaluation: customer onboarding, engineering execution, or commitment drift?
10. What is the minimum set of outcome labels needed before training a learned reader?

---

## 24. Recommended First Wedge

Implement this first for one high-value domain:

```text
Enterprise customer onboarding risk
```

Why:

- clear entities: customers, commitments, goals, systems, teams
- clear outcomes: launch, delay, revenue risk
- clear evidence sources: CRM, Linear/Jira, Slack, calendar, GitHub
- clear actions: assign owner, reprioritize work, update commitment, escalate risk
- clear falsification: customer launches, dependency removed, blocker changes

Initial question families:

```text
Is there an active customer commitment?
What dependency blocks it?
Who owns the dependency?
What goal/revenue is affected?
Is there counterevidence?
Is this recurring across other customers?
What action unblocks the most value?
```

This wedge provides strong training signal for reader/writer self-evolution.

---

## 25. Final Architecture Summary

The SAGE-inspired Fyralis architecture is:

```text
Canonical Synthesis Graph
  validated company truth

Discovery Topology
  learned retrieval utility

Synthesis Reader
  question-conditioned graph reading

Context Packet Compiler
  token-efficient evidence packet

Deep Reasoning Agent
  state-change proposal

Validation Layer
  invariant enforcement

Apply Layer
  canonical write

Outcome Evaluator
  usefulness labels

Topology Optimizer
  self-evolution of graph reading/writing
```

The final principle:

> Fyralis should not merely store company memory. It should learn how to write company memory so that future inquiry can read it with less search, less noise, fewer tokens, and higher action value.

This is the practical Fyralis adaptation of SAGE.

Or, less politely:

> If the graph does not get easier to read every time the system uses it, the Synthesis layer is just hoarding corporate trivia with a nicer schema.

---

## 26. References

- SAGE: A Self-Evolving Agentic Graph-Memory Engine for Structure-Aware Associative Memory. arXiv:2605.12061. https://arxiv.org/abs/2605.12061
- SAGE HTML version. https://arxiv.org/html/2605.12061v1
- Fyralis internal architecture document: Synthesis layer, Ingestion layer, Reasoning engine, validation/apply pipeline.
