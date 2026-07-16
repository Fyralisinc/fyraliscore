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
| Slack source ingestion | Existing Slack handler and ingestion core | Slack metadata and opportunity handoff changes | `extend` | The existing source adapter remains authoritative. |
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

## First Consolidation Targets

1. Keep the source-semantic worker active until its Slack-to-Model vertical is
   preserved by an equivalent integrated path.
2. Add successor grounding/source-semantic generations so a correction can
   repair the original interpretation and canonical Model.
3. Consolidate `entity_review_queue` behind `clarification_requests`.
4. Continue isolating shared command/event/outbox persistence from
   task-autonomy service and database naming while retaining compatibility.
5. Fold revised metric output into the existing benchmark/company-vitals
   artifact contract.
6. Feed typed grounding outcomes into existing reflective/retrieval learning
   before adding any new policy lifecycle.

## Proof Required Before Behavioral Expansion

- Clean targeted baseline remains green after each consolidation.
- One production-shaped Slack signal reaches one canonical grounded Model.
- Questions, unsupported mentions, ambiguity, and insufficient context do not
  create false beliefs.
- Duplicate delivery is idempotent.
- Every active worker is named in the current product scope.
- No task-autonomy domain or worker is imported by the active epistemic path.
- The report represents zero exposure as unknown, not success.
