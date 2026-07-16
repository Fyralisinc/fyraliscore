# Autonomous Company Learning Reuse And Consolidation Audit

## Purpose

This document is the review boundary for simplifying the revised-system
checkpoint into the active autonomous-company-learning system.

It does not replace the normative architecture documents. It records which
existing Fyralis components own each responsibility, which revised components
close a real semantic gap, and which additions should be consolidated,
deferred, or removed from the active architecture.

The active objective is autonomous improvement of evidence-grounded company
understanding. Autonomous Workflow, Task, Work scheduling, external effects,
and company-operation execution are outside the current scope.

Connector/listener ingestion transport is also outside the active objective.
Evaluation begins from simulated normalized, source-attributed signals already
persisted in PostgreSQL. Existing connectors remain their own owners, but this
work neither extends nor evaluates their polling, webhook, backfill or delivery
behavior.

## Preserved Baseline

- Preserved commit: `c4b476a1`
- Preservation tag: `revised-system-checkpoint-c4b476a1`
- Active branch: `codex/autonomous-company-learning`
- Isolated worktree: `/Users/rachinkalakheti/fyraliscore-autonomous-learning`
- Main checkout state at isolation: 151 dirty paths, left untouched
- Focused clean-baseline result: 49 grounding, source-semantic, context, and
  evaluation tests passed
- Local database state: revised-system migrations `0188`, `0189`, `0194`,
  `0195`, and `0203` through `0217` are already recorded in
  `schema_migrations`

Because these migrations have been applied to a persistent local database, the
active branch will not rewrite or renumber them. Out-of-scope schema will be
kept dormant or retired forward if runtime cleanup eventually requires it.

## Decision Vocabulary

| Decision | Meaning |
| --- | --- |
| `reuse` | Existing component already owns the responsibility. |
| `extend` | Add the missing behavior to the existing owner. |
| `adapt` | Keep a narrow compatibility boundary during cutover. |
| `consolidate` | Merge parallel implementations behind one owner. |
| `keep-new` | The revised component closes a genuinely missing semantic gap. |
| `defer` | Valid future capability outside autonomous company learning. |
| `remove` | Redundant or unjustified active implementation. |

## Required Decision Test

Every new table, repository, contract, worker, and evaluator must answer:

1. Which existing Fyralis component was considered?
2. What semantic or operational requirement is missing there?
3. Why is extending or adapting the existing owner insufficient?
4. Which representation is authoritative?
5. How is parallel truth prevented?
6. How are correction, replay, authority, and tenant isolation preserved?
7. What is the retirement or cutover path?

No new component proceeds while its decision remains `unclassified`.

## System-Level Invariants

1. There is one canonical company-belief store: Models and accepted canonical
   graph/relationship structures.
2. Observations and source-native records remain evidence, not inferred truth.
3. Grounding annotations assess and admit referents; they do not create a
   second entity registry.
4. Learned state steers future computation but cannot assert canonical truth.
5. Derived projections and indexes are rebuildable and cannot re-enter as
   independent evidence.
6. Every correction has an explicit current fate and dependent-repair fate.
7. Every active worker belongs to the company-understanding or learning loop.
8. Autonomous task execution remains absent from production topology, and no
   task-autonomy domain or worker is imported by the active epistemic path.
   Shared command/event mechanics currently named for agency must be isolated
   behind a neutral kernel boundary before dormant source deletion.
9. The existing benchmark-to-company-vitals artifact flow owns the system
   evaluation report.
10. Applied migration history is not rewritten.

## Initial Component Classification

This table is deliberately conservative. Detailed file evidence and final
decisions will be added after the parallel audit lanes complete.

| Responsibility | Existing Fyralis anchor | Revised component | Initial decision | Reason |
| --- | --- | --- | --- | --- |
| Source transport | Existing provider handlers, listeners and ingestion core | No active transport change | `defer` | Connector delivery is outside this objective; tests begin from normalized source-attributed signals already persisted in PostgreSQL. |
| Observation evidence | Observation repository and writer | Grounding references to observations | `reuse` | No second evidence store is needed. |
| Conversational context | Entity resolver context builder and retrieval seams | Context-selection contracts, snapshots, and repository | `keep-new` plus `consolidate` | Exact as-known selected context is a real missing annotation, but retrieval mechanics should reuse existing seams. |
| Mention detection | Existing unresolved-phrase opportunity heuristics | Exact detected/rejected mention fate and source coordinates | `keep-new` | An opportunity is not a reconstructable mention; the missing semantic boundary is real. |
| Entity candidates | Aliases, actors/entities, source identity and resolver candidates | Closed candidate request/set artifacts | `keep-new` plus `extend` | Candidate provenance and none-of-the-above are needed; candidate sources remain existing registries. |
| Entity assessment and admission | Entity resolver, clarification, review, create-new paths | ResolutionAssessment, GroundingAdmissionDecision, GroundingTrace | `keep-new` plus `consolidate` | Assessment/admission separation is valid; lifecycle duplication must be reduced. |
| Grounding work | Think obligations, workflow signals, worker polling patterns | `entity_grounding_work_items` | `keep-new` plus `consolidate` | Phrase/generation fates are domain-specific. Preserve the typed ledger while sharing claim/retry mechanics where useful. |
| Source meaning | Think interpretation and model construction | SourceAssertion/SemanticFrame/SpeechAct extraction | `keep-new` plus `adapt` | Expression must be separated from truth, but the lane should converge with normal synthesis. |
| Belief application | ModelsRepo, model constructor, Think applier | EpistemicApplier | `adapt` | The narrow adapter correctly writes through the canonical Models repository. |
| Source-semantic work | Existing obligations/workflow patterns | `source_semantic_work_items` and worker | `keep-new` plus `extend` | Deferred embeddings, fenced claims and atomic Model terminalization require stronger semantics than current workflow signals. Add generations and correction successors rather than replacing the queue. |
| Canonical beliefs | Models and model events | Grounded belief Model | `reuse` | Models remain canonical truth. |
| Graph and relations | Model edges and relation tables | Revised graph/protocol concepts | `reuse` or `extend` | No parallel graph authority is permitted. |
| Clarification | Existing clarification requests and answer path | Entity-resolution clarification linkage | `extend` plus `consolidate` | Preserve `clarification_requests`; make `entity_review_queue` a projection or compatibility surface. |
| Corrective memory | Aliases, model correction, repair obligations | Grounding feedback artifacts | `extend` | Clarification adjudication now upgrades existing aliases with governed identity basis and exact grounding lineage; dependent repair remains. |
| Policy learning | Retrieval learning, reflective rules, SAGE utilities, calibration | Governed control-policy concepts | `consolidate` | Reuse candidate/replay/shadow/promotion behavior instead of a second learning engine. |
| Intent and Concern | Goals, decisions, commitments, residuals, open questions | Governed intent and Concern repositories | `defer` pending reconciliation | These overlap existing objects and are not on the first learning-loop critical path. |
| Intervention manifest | No active company-learning requirement | Intervention episode coordinator | `remove` from active runtime; preserve schema/code pending audit | It advances the wrong form of autonomy. |
| Agency activation | Existing acts/workflow concepts | Agency activation repository and worker | `defer` | Future task-autonomy scaffolding. |
| Work scheduling | Existing obligations/workflow signals | Work scheduling repository and worker | `defer` | Future task-autonomy scaffolding. |
| External effects | No current active requirement | Execution/effect ledgers and contracts | `defer` | Consequential execution remains human-controlled. |
| Component evaluators | Existing benchmark and company-vitals reporting | `lib/evaluation/*` | `consolidate` | Preserve denominator, incident, proof-tier and company-physics metrics, but emit one system report through company vitals. |
| Architecture registry | Existing architecture checks and docs | Large revised registry and proof matrix | `consolidate` | Retain the assurance kernel and active-slice proof contracts. Do not let each component CLI emit a competing system-readiness report. |

## Detailed Reuse Findings

### Perception And Company Physics

- Observation remains source evidence.
- Interpretation-context snapshots, mention detections, candidate sets,
  assessments, admissions and source-semantic interpretations are valid new
  annotations because the previous resolver could not reconstruct those
  decisions.
- Context selection is not identical to ordinary retrieval: it requires exact
  as-known cutoffs, source topology, processing authority and material omission
  records. It should still reuse safe retrieval operators and scoring utilities.
- `_unresolved_phrases` is acceptable only as a bootstrap opportunity adapter.
  It must not remain the authoritative long-term scheduler or mention identity.
- Grounding and source-semantic queues have different semantic units and should
  remain typed. Lease, claim, retry and terminal-fate mechanics may be shared.

### Canonical Knowledge

- Models, accepted graph structures and model events remain the sole company
  belief substrate.
- `EpistemicApplier` is a narrow adapter over `ModelsRepo`, not permission for a
  parallel source-semantic ontology. Its eventual destination is the normal
  canonical belief validation/apply boundary.
- Corrected grounding must eventually supersede or retract the dependent source
  interpretation and Model rather than only improving future occurrences.
- Projections, retrieval indexes and learned rankings remain noncanonical and
  rebuildable.

### Human Feedback And Learning

- `clarification_requests` is the canonical human-question object.
  `entity_review_queue` currently duplicates the same human-work identity and
  should become a projection or be retired after compatibility is proven.
- Clarification acceptance previously bypassed `EntityAliasRepo` and omitted
  the identity basis required by the resolver. The active branch now uses the
  repository's connection-scoped write contract and records an
  `independently_adjudicated` basis plus exact grounding feedback lineage.
- Retrieval route utilities, motifs, reflective rules, SAGE feedback, negative
  memory, calibration, residuals, latent gaps and Think obligations already
  form the learning substrate. The missing work is supplying grounded
  correction outcomes and proving held-out improvement.
- The intended role layering is: measured route utility -> candidate procedural
  motif -> replay-tested promoted reflective rule.

### Governed Alias Families And Entity Lifecycle

The next entity-learning slice must extend the existing resolver rather than
create a second alias or entity subsystem.

| Responsibility | Reused owner | Required extension |
| --- | --- | --- |
| Candidate recall and ranking | Resolver context, existing alias registry and lexical/acronym scorer | Add versioned variant-family features and collision reasons; ranking remains nonauthoritative. |
| Reusable alias authority | Clarification adjudication, `EntityAliasRepo` and governed replay transaction fence | Promote only an independently adjudicated, uniquely scoped mapping with revocable lineage. |
| Human ambiguity resolution | `clarification_requests` and adjudicated grounding successors | Present every colliding target plus none-of-the-above; preserve the candidate-set digest and scope in the answer lineage. |
| Canonical entity state | Existing actor/resource/entity repositories and their archive/create/update paths | Rename, merge, split, replacement and name reuse must be explicit lifecycle operations, not alias side effects. |
| Adaptive policy | SAGE retrieval policy, route utility, shadow and exploration controls | Tune candidate ordering, lane budgets and clarification thresholds without alias-promotion or canonical-write authority. |

The safe semantic boundary is:

1. **Orthographic variant:** case, whitespace, Unicode normalization, or a
   declared separator/punctuation transformation of an already adjudicated
   surface. It may join that alias family only under a versioned normalizer,
   after tenant-wide collision checks. The current case/whitespace exact replay
   remains the only implemented automatic equivalence.
2. **Abbreviation, acronym, nickname or shortened name:** a useful candidate
   feature, not an orthographic equivalence. Repetition, confidence or lexical
   score must never promote it. It becomes reusable only through the same
   governed adjudication and transaction-time uniqueness fence as an exact
   alias.
3. **True rename with identity continuity:** a canonical lifecycle decision.
   The entity ID may remain stable only when the entity owner establishes
   continuity; the prior name becomes a time-scoped historical alias. The
   timeless `tenant_global_exact` scope must not represent a rename until alias
   validity intervals and lifecycle lineage exist.
4. **Merger, split, acquisition, replacement, re-created team or reused name:**
   potentially different referents. These require canonical create/archive/
   supersession decisions and dependent repair. An alias write cannot merge
   identities or redirect history.

Before any non-exact variant can be promoted, one atomic promotion contract
must prove all of the following:

- an active governed anchor alias and active tenant-local canonical target;
- the variant rule and normalizer version, candidate-set digest, source/time
  scope, entity type and adjudication lineage;
- exactly one eligible target across active aliases and canonical names, plus
  no conflicting historical-name reuse in the applicable validity interval;
- explicit privileged confirmation for tenant-global reuse, with
  transaction-time revalidation and a revocation/rollback path;
- no contextual or source-local phrase globalization; and
- success on the collision suite below. Usage counts may tune ranking but are
  never identity evidence or promotion authority.

Promotion must abstain or open clarification for:

- one acronym expanding to multiple entities, including collisions across
  customer, workstream, team, system and actor types;
- the same full surface naming two active entities or a former name being
  reused by a new entity;
- legal rename versus acquisition, merger, split, spin-out or replacement;
- Slack-local nicknames, channel-specific shorthand, pronouns, deictic phrases,
  common nouns and substring-only matches;
- Unicode confusables, punctuation-sensitive names and transliterations that
  collapse under the proposed normalizer;
- a source-native ID contradicting the learned human-readable alias;
- archived, inactive, superseded, cross-tenant or unsupported targets; and
- evidence created after the as-known cutoff, unauthorized feedback, revoked
  adjudication or a candidate set that omitted none-of-the-above.

SAGE may consume governed candidate features and adjudication outcomes to tune
lane weights, ordering, budgets, confidence calibration and when to ask a
clarification. Its output must remain a versioned, auditable policy decision
over an already authorized candidate set. It may not introduce hidden
candidates, widen tenant/source/time scope, suppress collision checks, promote
an alias, select a canonical referent, mutate entity lifecycle, or write a
Model. Safety fences and canonical writers remain deterministic owners outside
the learned policy.

The first non-exact implementation now reuses this boundary without promoting
new aliases. A sealed 24-pair population exercises six mechanically rankable
variant families across four entity types. The adaptive arm may expose the
governed target candidate; the frozen arm may not consume clarification-learned
candidate memory. Both still use the normal closed-set model path. This proves
unambiguous candidate-memory lift, not collision safety, deterministic replay,
canonical alias promotion or entity lifecycle.

### Runtime And Dormant Autonomy

- Applied migrations are historical facts and remain untouched.
- `agency_command_results`, `agency_canonical_events`, outbox records and the
  shared command write context are used by active context/grounding code.
  The active contract is now `SemanticWriteContext`; `AgencyWriteContext`
  remains a compatibility alias for dormant code. Database and service-module
  naming remains to be neutralized without rewriting migration history.
- Intent, Concern, external execution, activation, scheduling and intervention
  episode repositories are dormant future scaffolding. They are not all safe to
  delete yet because repair/control work and the active command kernel retain
  dependencies.
- The intervention episode coordinator, agency activation worker and work
  scheduler are all absent from production topology as of commit `ecaa28cc`.
- `think_obligations` is the existing product-level future epistemic-work seam.
  It should gain stronger generation/fencing/terminal-fate behavior rather than
  being replaced by external task Work.

### Evaluation

- Keep raw numerator/denominator records, evidence tiers, substantiation state,
  incident preservation, architecture digests, proof gaps and fail-closed
  disjoint-population aggregation.
- Company context, grounding and source-semantic metrics belong in a
  `company_physics` section of company vitals.
- Protocol assurance must remain noncompensatory; it must not be averaged into
  the semantic/product value score.
- Component CLIs must not claim system readiness or emit mostly empty copies of
  the complete invariant matrix.
- Zero exposure must render as unknown, not `1.0`.
- The canonical report path is storyline/synthetic run -> proof manifests ->
  company vitals -> company intelligence loop.

## Implemented Consolidation And Learning Slices

### Slice 1 — Production autonomy boundary

- Removed the intervention episode coordinator from Compose and the production
  process manifest.
- Preserved its code, tests and applied schema for later source-level
  consolidation.
- Confirmed agency activation and work scheduling remain absent from production
  topology.
- Validation: six runtime-manifest tests passed.

### Slice 2 — Adjudicated entity corrective memory

- Added a caller-transaction-compatible alias write path to the existing
  `EntityAliasRepo` module instead of retaining clarification-specific SQL.
- Human adjudication can now upgrade an existing ungoverned guess, attach an
  independently adjudicated identity basis and preserve the exact context,
  mention, candidate, assessment, admission and grounding-trace lineage.
- Resolver review clarifications now carry that stage-level feedback lineage.
- A real-Postgres test proves an initially untrusted alias routes to review,
  adjudication upgrades its basis, and a later occurrence resolves for the
  consumer.
- The entity-grounding evaluator now reports answered-clarification lineage,
  adjudicated-alias lineage and observed future corrective-memory reuse. Missing
  lineage is a localized incident; absence of a later matching signal is not
  misreported as failed reuse.
- This is corrective memory, not yet statistical policy learning or repair of
  the original dependent Model.

### Slice 3 — Neutral semantic command context

- Extracted the generic authority/idempotency/write-scope context from the
  consequential-agency contract module into `SemanticWriteContext`.
- Kept `AgencyWriteContext` as a compatibility alias, so dormant intent,
  execution and repair contracts remain source-compatible.
- Changed active context, mention, grounding and shared protocol code to import
  the neutral contract directly.
- Replaced active resolver imports from the eager `lib.contracts` facade with
  direct context/mention modules.
- This removes one large accidental dependency from the company-physics path
  without renaming applied database objects.

### Slice 4 — Governed exact-alias autonomous replay

- Human correction defaults to `source_context_only`; tenant-global memory is
  never inferred from lexical shape.
- `tenant_global_exact` requires an explicit answer flag plus a currently
  active tenant admin or leadership role. Obvious context-dependent phrases
  cannot be promoted.
- The learned replay decision is admitted only for an exact source-anchored
  mention, one exact normalized canonical ref, a supported active tenant-local
  target and complete clarification -> predecessor -> adjudicated-successor
  lineage.
- Alias, answer digest, authority, target and normalized ambiguity are
  revalidated under the same transaction that appends grounding. A failed
  fence falls through to the existing model/review path.
- Replayed decisions use their own deterministic calibration cohort and scorer
  identity, remain non-evidence policy memory and still traverse normal
  grounding, source-semantics and canonical Model application.
- The evaluator reports exposure, resolution rate, model calls avoided,
  unsafe replay and contextual replay continuously; zero exposure is unknown.

### Slice 5 — Annotation-only correction and original-Model repair

- Clarification acceptance no longer mutates `observations.entities_mentioned`
  or emits an authoritative `internal:state_change` Observation for the
  resolver's own conclusion.
- The correction remains an independently adjudicated alias plus an immutable
  successor grounding generation, source-semantic interpretation and canonical
  Model outcome.
- This preserves the evidence/annotation boundary while still repairing the
  originally reviewed signal and improving later exact-alias occurrences.
- A real-Postgres test proves the original and future Observations remain
  unchanged, no self-authoritative resolution Observation is created, the
  corrected successor closes safely, and the later replay reaches one Model.

### Slice 6 — One company-learning proof path through Company Vitals

- Conversational-context, entity-grounding and source-semantic evaluators now
  accept exact manifest Observation IDs instead of relying only on tenant/time
  windows.
- `lib/evaluation/company_learning.py` owns one active-slice state vector and
  one evidence manifest assembled from the existing registered invariant
  evidence builders.
- Runtime health and proof-backed substantiation are separate. A clean E3
  runtime slice is reported as healthy but insufficient until registered
  scenarios, trace facts and evidence tiers satisfy the architecture proof
  compiler.
- Company Vitals remains the sole operator-facing report. Company physics is a
  noncompensatory section, confirmed incidents are hard failures, and its
  metrics never enter the existing overall-score average.
- `company_learning_evaluation.json` and
  `company_learning_evidence_manifest.json` are supporting evidence artifacts,
  not competing readiness reports. Artifact-only rerenders preserve a saved
  DB-backed evaluation instead of replacing it with `not_observed`.

### Slice 7 — One canonical human-review object

- New entity-review work is represented only by `clarification_requests`.
  Its object identity is the exact grounding trace under review, and its payload
  carries the candidate set and full grounding feedback lineage.
- The resolver no longer writes a parallel `entity_review_queue` row. Review
  admission and clarification creation share the grounding transaction, so a
  terminal review fate cannot commit without its human obligation.
- The answer path still recognizes historical `entity_review` clarification
  objects and resolves/dismisses their legacy queue rows for compatibility.
  Applied migration history and old data remain intact.
- The grounding evaluator now proves review-obligation coverage from the
  canonical clarification lineage rather than the retired compatibility table.

### Slice 8 — One first epistemic owner for unresolved Slack signals

- Slack Observations with unresolved entity phrases no longer enqueue the
  generic `T1:event_arrival` path at ingest time. Their durable grounding work
  is the exclusive first epistemic obligation.
- Resolved grounding continues through source-semantic admission to one
  canonical Model; questions, ambiguity and unsupported expressions retain
  explicit no-admission/review fates without a competing generic Think write.
- Clarification acceptance no longer emits the legacy
  `entity_resolved_late` T1. Its adjudicated grounding successor is consumed by
  the same source-semantic lane.
- The gate is intentionally Slack-specific. Structured/self-contained sources
  continue through the existing Think path until they have an equivalent
  grounded-semantic ownership contract.

### Slice 9 — Paired adaptive-versus-frozen corrective-memory experiment

- The causal question is whether reusing an adjudicated correction improves a
  later held-out recurrence, not whether the system can store a correction or
  close one replay trace.
- A sealed experiment assigns matched company foundations to distinct adaptive
  and frozen tenants, gives both arms the same training correction and scripted
  provider behavior, and then replays the same held-out recurrence cases. The
  adaptive arm may consume clarification-learned identity memory; the frozen
  arm preserves that memory in storage but may not consume it.
- Freezing only the entity resolver is insufficient. Ingestion's alias
  fast-path runs first and can resolve the held-out phrase from the adjudicated
  alias, remove the grounding opportunity and prevent the frozen resolver from
  ever observing the case. The experiment control must therefore disable
  corrective-memory reuse at every pre-outcome consumer, currently both ingest
  entity resolution and resolver context/replay, while leaving unrelated
  manually supplied candidates available.
- The harness uses the same public domain adjudication operation as the HTTP
  gateway. Authorization, alias persistence, successor grounding and feedback
  lineage therefore have one owner rather than a private router implementation.
- The paired report preserves case, arm, tenant, training correction,
  clarification, adjudicated alias, recurrence Observation, grounding,
  source-semantic and Model lineage. It reports continuous correctness,
  review/abstention, semantic-admission, exactly-one-Model, model-call, latency,
  estimated-cost and paired-discordance metrics. Unsafe autonomous resolution,
  wrong replay Models, ignored conflict, contextual globalization, incomplete
  fates, source mutation, self-authored evidence and Model-cardinality
  violations remain noncompensatory incidents.
- The executable harness and Company Vitals attachment now provide synthetic
  E4 evidence for the first exact-alias positive population: three held-out
  recurrences resolve correctly in the adaptive arm versus zero in the frozen
  arm, avoid three model calls and record no hard-safety incident. This is not a
  completed system-substantiation claim. It does not yet establish confidence
  intervals, unseen-spelling generalization, negative-control performance,
  open-world E5 benefit or customer value.
- Entity identity correctness is measured independently from semantic
  admission. In the sealed population, renewal produces one new Model while
  support and risk terminalize as `no_admission`; all three still resolve the
  learned company object correctly. The evaluator therefore does not reward
  duplicate or unsupported belief creation merely to make the learning loop
  appear productive.
- The experiment scenario and adaptive-minus-frozen lift metric are registered
  under INV-05 and translated into canonical invariant evidence before proof
  compilation. Aggregation preserves the E3 tier of the runtime grounding
  slice, so the added E4 experiment cannot silently upgrade incomplete
  structural proof.

### Slice 10 — Governed non-exact candidate-memory variants

- Added one immutable 24-case registry covering acronym, punctuation-compaction,
  hyphen/spacing, anchored-short-form, omitted-letter and possessive/plural
  variants across customer, project, team and system entities.
- Reused the existing alias registry, lexical candidate scorer, closed candidate
  set, resolver assessment, grounding and adaptive/frozen correction controls.
  No second alias store, variant worker or canonical writer was added.
- The causal mechanism is explicit: both arms receive the same scripted model
  response, but only the adaptive arm may receive the governed target candidate.
  The frozen arm must review or abstain because the model cannot invent IDs.
- The full integration harness requires `24/24` adaptive correctness, zero
  frozen target exposure, `24/24` frozen safe review/abstention, immutable source
  evidence, zero control-integrity violations and zero hard-safety incidents.
- Assurance v3 makes the typed artifact mandatory and noncompensatory, reopens
  it and cross-binds its evidence, registry, population-report,
  experiment-report and mechanism-metric digests. Collision/homonym and
  entity-lifecycle cases remain deliberately outside this positive population.

## Authoritative 45-Batch Reuse Findings

The large cold-start run confirms that the correct path is to repair existing
owners, not add parallel subsystems:

- The mention-opportunity, detection and grounding protocols already exist, but
  the large simulation bypassed them. Wire the existing protocol rather than
  creating a second extractor.
- The existing candidate/promotion boundary is correct. Remove resolver-owned
  canonical alias writes rather than adding another identity registry.
- Models remain the right canonical memory store. Tighten claim-local scope and
  admission instead of introducing a second “working memory” graph.
- Existing retrieval and SAGE already learn and reuse outcomes. Make their
  observation/Model budgets outcome-adaptive rather than adding a new retrieval
  controller.
- Relation frames and Model edges remain the right graph owners. Preserve
  participant roles and add directionality invariants rather than creating a
  replacement graph.
- Existing residuals, open questions and latent gaps are sufficient uncertainty
  surfaces. Prove human closure and improve repair ROI rather than adding a new
  gap system.
- Company Vitals and the authoritative aggregate evaluator remain the sole
  system report. Fix entity denominators, recovered-failure semantics and
  normalization inside those owners.

## Next Consolidation Targets

1. Validate the implemented mention-fate, adjudication-only alias, claim-local
   scope, directionality and quality-weighted retrieval changes in a bounded
   multi-batch regression before another expensive authoritative run.
2. Measure gold entity extraction quality independently from protocol-fate
   closure; the existing fate ledger is reused and is not an extraction score.
3. Validate causal-thesis recovery and signed calibration bias on held-out
   batched storylines after the new prompt, confidence caps and evaluator
   weighting.
4. Coalesce current projection jobs and govern existing T4 repair lanes by
   measured durable-outcome ROI.

The first five previous consolidation targets were implemented, without new
parallel truth stores or controllers, in `b38faf87` through `e04da5e8`.
`8f4e75e8` extends the existing Think validator and large-company evaluator for
causal thesis and calibration. These are focused implementation proofs, not a
retroactive repair of the pre-change 45-batch artifact.

## Proof Required Before Behavioral Expansion

- Clean targeted baseline remains green after each consolidation.
- One simulated normalized, source-attributed signal already persisted in
  PostgreSQL reaches one canonical grounded Model.
- Questions, unsupported mentions, ambiguity, and insufficient context do not
  create false beliefs.
- Duplicate delivery is idempotent.
- Adaptive and frozen arms share the same stored correction, company
  foundation, provider behavior and held-out cases; the only intended
  difference is whether clarification-learned corrective memory may be
  consumed before outcome.
- Every upstream consumer that could use corrective memory is frozen in the
  control arm, including persisted-signal alias resolution and resolver replay.
- Every active worker is named in the current product scope.
- No task-autonomy domain or worker is imported by the active epistemic path.
- The report represents zero exposure as unknown, not success.
