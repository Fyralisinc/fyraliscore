# Think Intelligence Gate — Shared Contract Freeze v3

**Status:** Frozen implementation contract

**Authority:** Integration owner only

**Freeze commit:** To be recorded at checkpoint commit

**Contract digest:** Recorded outside this file after its bytes are frozen

**Amendment from v2:** Provider-facing TI2 output now contains only the
discriminated semantic decision. The trusted call-site adapter binds the known
`dossier_id` and `dossier_digest` from the exact capture request before
compilation. Provider-authored dossier identity is neither requested nor
accepted. This removes identity bookkeeping from the LLM without changing its
semantic authority, compiler closure checks, provider/model/effort policy,
scorer gold, or hard gates.

**Amendment from v1:** TI0 additionally owns a narrow raw-response trace
emission in `lib/llm/provider.py` and its focused provider test. The provider
boundary is the only layer that possesses the exact response before Pydantic
parsing, so the v1 file manifest could not satisfy its own observability
contract without this addition. No field meaning or semantic authority changed.

This checkpoint freezes only the interfaces required for TI0, TI1, TI2, TI3,
and TI4-min. It is not a general reasoning framework. Runtime implementations
must not import evaluator gold, transport canonical identifiers through the LLM,
or create a second relation truth path.

## 1. Ownership Rule

The LLM owns semantic judgment: mechanism, direction, alternatives, novelty,
counterevidence, uncertainty, and abstention. The dossier owns governed runtime
context and local handles. The compiler owns canonical identity, evidence
closure, legality, exact versions, and transaction construction. Validators
constrain commands. The existing applier mutates atomically. The independent
scorer owns gold and verdicts.

Every frozen field below has exactly one owner. Other lanes may carry a value
by reference or digest but may not reinterpret it.

## 2. Shared Identifiers And Digests

- Schema and policy identifiers are immutable lowercase kebab-case values with
  a `-vN` suffix.
- JSON content digests are SHA-256 over canonical sorted-key JSON, excluding a
  self-digest and nondeterministic timestamps.
- A policy digest binds prompt-policy version, provider schema version,
  compiler version, model, explicit effort, and routing policy version.
- `default` is a valid effort only when it is the actual provider default. It
  must never be inferred after a run.
- Runtime objects and scorer gold always have separate digests.
- A frozen identifier is never relabeled. Rollback selects a prior immutable
  policy envelope.

## 3. TI0 Cognition Trace Envelope

`ThinkCognitionTraceV1` has `schema_version = think-cognition-trace-v1` and the
following owned fields:

| Field | Owner | Meaning |
| --- | --- | --- |
| `trace_id`, `tenant_id`, `trigger_id`, `think_run_id`, `batch_id` | Think orchestrator | Runtime execution coordinates |
| `logical_call_id` | Provider wrapper | Join to one logical receipt |
| `cognitive_purpose` | Call site | One of `mention_discovery`, `entity_resolution`, `question_planning`, `main_reconciliation`, `main_synthesis` |
| `versions` | Policy owners | Prompt policy, provider schema, compiler, model, effort, and routing policy versions |
| `context_manifest` | Dossier/context builder | Selected runtime object handles and their versions/digests plus a context digest |
| `prompt` | Prompt call site | Exact system text, exact user text, and prompt digest |
| `raw_provider_response` | Provider boundary | Raw structured response and digest before normalization plus parse outcome |
| `compiler` | Compiler | Ordered normalizations and exact rejection predicates |
| `validated_command` | Validator | Payload or immutable reference, digest, and outcome |
| `applied_result` | Applier | Payload or immutable reference, digest, outcome, and optional transaction identifier |
| `evaluation_ref` | Independent scorer | Opaque score receipt ID and digest only |
| `terminal` | Orchestrator/classifier | Failed stage and one failure class |
| timestamps | Emitting stage | UTC stage boundaries |

The existing `PhysicalAttemptReceipt`, `LogicalCallReceipt`, receipt sink, and
durable Think receipt collector remain authoritative for provider, attempts,
tokens, cache, cost, and call timing. The trace joins them; it does not replace
or duplicate their facts.

### 3.1 Reconciliation invariants

- One logical receipt exists per `(tenant_id, logical_call_id)`.
- Physical attempt IDs are unique; ordinals are contiguous from one.
- Attempt count equals `physical_attempt_count`; cache hits have zero attempts.
- Attempts agree with the logical provider/model. The first parent is null and
  each later parent is the preceding attempt.
- `retry_scheduled` is true exactly when a following attempt exists.
- At most one success exists and it is terminal. Logical success has exactly
  one terminal successful attempt unless it is a cache hit.
- Physical usage sums are authoritative for logical/run totals.
- Parse repair is a physical sub-purpose attributed to its parent logical
  cognitive purpose.
- Logical wall latency and per-attempt latency are reported separately.
- Validation and apply outcomes are attributed per logical call, never copied
  collector-wide across unrelated calls.

### 3.2 Trace safety

Prompts and raw responses are restricted evidence, not ordinary logs. Before
persistence, recursively reject or redact credentials, authorization headers,
cookies, API keys, tokens, connection strings, environment dumps, and provider
auth paths. Error strings receive the same treatment. Runtime traces must not
contain storyline IDs, expected thesis/mechanism/direction, oracle labels,
thresholds, per-case gold, or gold-derived explanations. Forbidden-key and
sentinel scans are required tests; a `gold_blind` flag is not proof.

## 4. TI1 Scope-Local Synthesis Dossier

`SynthesisDossier` has `schema_version = synthesis-dossier-v1`:

```text
SynthesisDossier
  dossier_id, tenant_id
  scope: ScopeCoordinate
  window: TimeWindow
  handles: tuple[DossierObject]
  event_order: tuple[O-handle]
  accepted_model_heads: tuple[M-handle]
  direct_observations: tuple[O-handle]
  supporting_evidence, contradictory_evidence, auxiliary_evidence
  open_uncertainty
  candidate_mechanism_slots
  considered_explanations
  discriminating_missing_evidence
  assembly_receipt
```

The dossier assembler consumes governed learning episodes, exact accepted
current Model heads, and explicitly supplied durable uncertainty/history. It
must not treat the full generic context packet, hypotheses, generated
questions, omission budgets, evaluator annotations, barriers, prompts, or
receipts as context authority.

### 4.1 Scope and time

`ScopeCoordinate` contains one resolved `canonical_ref`, display-only label,
`coordinate_authority = resolved`, and governed episode ID. Provisional
`mention:*`, ambiguous, unresolved, multi-scope, wrapper, or batch coordinates
fail closed.

`TimeWindow` contains `start_at`, `end_at`, `as_of_at`, and
`ordering = occurred_at_observation_id`. Semantic chronology uses occurrence
time with observation ID as the deterministic tie-breaker. Ingestion order and
transport batch number are not semantic time. Future evidence or Model heads
are forbidden.

### 4.2 Closed handle registry

- `O1..On`: same-scope persisted observations.
- `M1..Mn`: exact accepted current Model head and truth version.
- `X1..Xn`: previously considered explanations backed by a durable runtime
  record.
- `U1..Un`: open uncertainty or missing discriminating evidence backed by a
  durable runtime record.

Allocation is deterministic: observations sort by occurrence time and ID;
Models by valid/advanced time then Model ID and version ID; explanations and
uncertainties by durable provenance ID. One object/version receives exactly one
handle. Unknown, duplicate, wrong-prefix, cross-tenant, cross-scope, stale,
unauthorized, or unclosed handles fail before provider invocation and again
before mutation.

Provider-facing dossier serialization exposes handles and semantic content,
not canonical UUIDs.

### 4.3 Dossier objects and evidence

An observation object owns its exact observation ID, occurrence time, assertion
text, evidence address/field/span, source channel and optional source identity,
authority tier, independence group, canonical scope, and evidence role.

A Model object owns its exact Model ID and truth-version ID, semantic content,
canonical scope, current-accepted status, valid-as-of time, members, and closed
observation lineage. Stale or malformed Models are excluded.

Evidence roles are `direct`, `transitive`, `contradictory`, and `auxiliary`.
Direct evidence is claim-local current-scope evidence. Transitive evidence is
reachable through an exact accepted Model head and is never promoted to direct.
Contradiction is semantic and independent of authority tier. Auxiliary context
is same-scope in v1; cross-scope analogies are excluded.

Authority and independence are separate. Source channel alone never proves
independence. Missing stable source identity yields a conservative unknown
independence group.

Candidate cause, condition, and outcome slots contain only `O*` or `M*`
handles. They are runtime reasoning affordances, not asserted truth or hidden
gold. Previously considered explanations and missing-evidence questions exist
only with durable provenance; otherwise their lists are empty.

### 4.4 Assembly receipt and maturity

The receipt contains input episode IDs, included/excluded counts and reasons,
handle-binding and content digests, closure checks, and
`mechanism_opportunity = mature | immature | none` with structural reasons.
Maturity may use only resolved scope, exact accepted heads, a current
scope-level state change, typed mechanism material, evidence closure, and
handle closure. It must not inspect company/storyline names, target batches,
fixture signal IDs, expected thesis facets, expected relation semantics, or
scorer gold.

## 5. TI2 Semantic Decision Contract

The provider-facing `SynthesisSemanticDecision` has
`schema_version = think-synthesis-semantic-decision-v1` and exactly one
discriminated decision:

```text
SynthesisProposal | AbstentionDecision
```

All schemas forbid extra fields. Provider fields contain no UUIDs.

After provider parsing, the trusted call-site adapter constructs the
compiler-facing `SynthesisDecisionEnvelope` with
`schema_version = think-synthesis-decision-v1`, the semantic decision, and the
exact `dossier_id` and `dossier_digest` already bound to the capture request.
The adapter must not derive, repair, or accept either identity from provider
text. The compiler still compares both values to its immutable compile context
and fails closed on any mismatch. The trace separately preserves the raw
provider semantic decision and the adapter binding operation.

### 5.1 SynthesisProposal

- `kind = synthesis`
- thesis and mechanism
- one to eight cause/condition handles
- one to eight effect handles
- one to sixteen supporting evidence handles
- bounded counterevidence assessments with `weakens | contradicts`
- strongest alternative and why it is weaker
- novelty classification `novel | extends | confirms | duplicates`, exact
  relative Model handles, and explanation
- confidence in `[0,1]`
- one to eight descriptions of falsifying evidence
- exactly one `SemanticRelationProposal`

The relation proposal contains one allowed governed relation kind, source
handles, `target = synthesis_output`, `direction = source_to_target`, and an
explanation. Relation sources are a subset of cause/condition handles. This is
the sole authoritative semantic relation input for the synthesis path.

The compiler creates the composite target, binds exact canonical versions and
evidence, and emits exactly one composite claim command plus one existing
canonical relation claim command. It must not additionally emit an edge,
relation frame, lexical obligation, or hinted relation. It may not infer
business meaning from fixture-shaped keywords.

### 5.2 AbstentionDecision

- `kind = abstain`
- reason code `insufficient_evidence | conflicting_evidence |
  no_coherent_mechanism | not_novel | out_of_scope`
- explanation, one to eight missing-evidence descriptions, relevant handles,
  optional strongest alternative, and confidence

Abstention compiles to zero mutation.

### 5.3 Binding and atomicity

The compiler-owned immutable binding table maps each handle to object kind,
canonical ID, exact Model version when applicable, tenant, canonical scope,
authority, and allowed semantic roles. It is bound to the dossier digest.

Before constructing a mutation, compilation fails the whole proposal on:
unknown or duplicate handles; repeated semantic members; stale/non-current
heads; tenant or scope mismatch; unauthorized role/evidence; observation
outside trigger closure; missing exact Model version; dossier/digest mismatch;
source/effect overlap; unsupported relation kind; relation sources outside the
causes; no direct observation support; or conflicting decision variants. No
bad handle may be ignored or silently dropped.

Composite, canonical relation, projected edge, outbox, receipts, and applied
trigger commit atomically or not at all. Existing expected-head/CAS fences
remain authoritative.

## 6. Semantic Scorer Contracts

Scorer code and gold live outside runtime services. Runtime packages must not
import them.

`SemanticScorerCase` has `schema_version = think-semantic-case-v1`, case ID,
dossier digest, `positive | null`, and independent gold. Positive gold holds
required thesis/mechanism facets, allowed relation kinds and direction,
allowed cause-handle sets, required support/counterevidence, alternative and
novelty expectations, forbidden handles, and confidence band. Null gold allows
only abstention and specifies allowed reasons, missing-evidence facets,
forbidden handles, and maximum synthesis confidence.

`SemanticScorerResult` has `schema_version = think-semantic-result-v1`, case
and artifact digests, optional compiler receipt digest, hard gates, continuous
scores, tokens/latency/cost, semantic value per thousand tokens, consistency,
failure class, and verdict.

Hard gates are: schema valid; handles resolved; evidence complete; scope clean;
relation supported; compiler accepted for accepted proposals; correct mechanism
and direction for positives; correct abstention for null; zero unsupported
canonical relations; zero partial writes; and zero validator/applier failures.

Continuous scores are those preregistered in the handoff: scope precision,
mechanism correctness, thesis completeness, causal direction, evidence
precision/coverage, counterevidence, alternative, novelty, confidence,
abstention, schema/compiler acceptance, contamination, consistency, tokens,
latency, cost, and semantic value per thousand tokens.

## 7. TI4-Min Policy And Evaluation Receipt

`PolicyIdentity` binds prompt-policy version, provider schema version, compiler
version, routing-policy version, model, and explicit effort.

`EvaluationReceipt` has
`receipt_version = think-semantic-evaluation-receipt-v1`, attempt and case IDs,
dossier digest, policy identity/digest, raw decision digest, optional compiler
receipt digest, scorer case/version/result digests, evaluation time, and at most
one failure class:

`context_dossier | semantic_model | schema_binding | compiler |
validator_applier | evaluator | infrastructure`.

Provider-free artifact replay must reproduce compiler and scorer digests.

## 8. Artifact Contract

Artifacts live under
`<root>/<phase>/<utc-run-id>/<logical-call-or-attempt-id>/` with unique,
never-overwritten directories. Standard files are `trace.json`, `prompt.json`,
`raw-response.json`, `compiler.json`, `validated-command.json`,
`applied-result.json`, `evaluation-receipt.json`, and `manifest.json`.

The manifest uses `think-cognition-artifact-manifest-v1` and records commit,
contract digest, trace/call IDs, relative paths, canonical content digests,
byte SHA-256 digests, sensitivity, and creation time. It contains no absolute
paths. Writes are atomic. Evidence under `/tmp` is not durable until existence
and byte digest are verified and copied to its governed artifact location.

## 9. File And Database Ownership Manifest

The frozen digest is amendment authority. A lane must stop and return to the
integration owner rather than editing another lane's contract or files.

| Lane | Owned files | Forbidden files | Database | Provider |
| --- | --- | --- | --- | --- |
| TI0 telemetry | `lib/llm/telemetry.py`; narrow raw-response trace emission in `lib/llm/provider.py` and its focused provider test; `services/reasoning/think/llm_receipts.py`; `services/reasoning/think/debug_capture.py`; narrow call-site wiring in `llm_reason.py`, `reason.py`, `run_pipeline.py`; focused TI0 tests; required migration | Dossier semantics, synthesis schema/compiler, scorer gold/thresholds | `fyralis_ti0` | Forbidden until observational canary authorization |
| TI1 dossier | new `services/platform/execution/synthesis_dossier.py`; its tests; narrow governed-episode/context-packet integration | Telemetry receipts, canonical admission/compiler, scorer gold/thresholds | `fyralis_ti1` | Forbidden |
| TI2 decision | new `services/reasoning/think/synthesis_contract.py`; its tests; one narrow adapter in `compiled_reasoning.py`; atomicity tests | Dossier assembly, telemetry framework, evaluator gold/thresholds | `fyralis_ti2` | Forbidden |
| Scorer/TI4 | frozen replay/scorer and evaluation receipt modules under `services/evaluation/epistemic_repair`; evaluation tests/fixtures | Runtime dossier, prompt, compiler, truth mutation | separate scorer artifacts; read-only proof DB if needed | Only during TI3 authorization |
| Integration owner | This contract, coordinator/journey/journal, merge adapters, policy selection | Weakening gold or truth gates without explicit architecture authority | integration/proof DBs only at authorized gates | Sole run authorization |

Potential overlapping files require an explicit integration-owner adapter
commit after lane commits; they are not shared-write permission.

## 10. Required Contract Tests

- Deterministic schema serialization and digest tamper detection.
- Synthetic provider-free prompt -> raw response -> compile -> validate ->
  apply reconstruction with logical/physical reconciliation.
- Purpose partition, retries, cache hit, parse repair, usage and latency tests.
- Secret/error redaction plus evaluator-gold forbidden-key/sentinel tests.
- Dossier determinism, scope/time/as-of closure, exact current-head binding,
  evidence roles, conservative independence, and wrapper/gold exclusion.
- Provider-free dossier construction over all twelve development batches:
  Atlas and Cobalt each mature structurally at the earned point; the null case
  does not; no runtime implementation imports those expectations.
- Synthesis/abstention union, no provider UUIDs, and all invalid handle classes
  fail before mutation.
- Provider-facing synthesis output omits dossier ID/digest; the trusted adapter
  binds the capture-request identity exactly, rejects provider identity fields,
  and compiler mismatch tests remain fail-closed.
- Exactly one composite plus one canonical relation path and transaction
  rollback at relation, projection, outbox, receipt, and stale-head fences.
- Independent Atlas, Cobalt, and null scorer cases; scorer tamper detection;
  provider-free compiler/scorer replay; deterministic policy rollback.

## 11. Amendment And Stop Rule

Only the integration owner may amend this contract. Any amendment creates a
new contract version and digest and requires explicit impact review for all
active lanes. Do not expand this freeze into general episode discovery, all
cognitive operations, a prompt platform, entity redesign, debating agents, or
production hardening. CF3-C remains locked until TI0-TI3 and the selected
versioned policy are green and frozen.
