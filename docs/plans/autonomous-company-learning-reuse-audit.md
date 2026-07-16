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
8. Autonomous task execution remains absent from production topology.
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
| Grounding work | Think obligations, workflow signals, worker polling patterns | `entity_grounding_work_items` | `unclassified` | Exact retry semantics must be compared before retaining a separate queue. |
| Source meaning | Think interpretation and model construction | SourceAssertion/SemanticFrame/SpeechAct extraction | `keep-new` plus `adapt` | Expression must be separated from truth, but the lane should converge with normal synthesis. |
| Belief application | ModelsRepo, model constructor, Think applier | EpistemicApplier | `adapt` | The narrow adapter correctly writes through the canonical Models repository. |
| Source-semantic work | Existing obligations/workflow patterns | `source_semantic_work_items` and worker | `unclassified` | Keep the active vertical, then decide whether its queue can share a substrate. |
| Canonical beliefs | Models and model events | Grounded belief Model | `reuse` | Models remain canonical truth. |
| Graph and relations | Model edges and relation tables | Revised graph/protocol concepts | `reuse` or `extend` | No parallel graph authority is permitted. |
| Clarification | Existing clarification requests and answer path | Entity-resolution clarification linkage | `extend` | Preserve the existing human-feedback surface. |
| Corrective memory | Aliases, model correction, repair obligations | Grounding feedback artifacts | `extend` | Corrections should update existing canonical memory and repair paths. |
| Policy learning | Retrieval learning, reflective rules, SAGE utilities, calibration | Governed control-policy concepts | `consolidate` | Reuse candidate/replay/shadow/promotion behavior instead of a second learning engine. |
| Intent and Concern | Goals, decisions, commitments, residuals, open questions | Governed intent and Concern repositories | `defer` pending reconciliation | These overlap existing objects and are not on the first learning-loop critical path. |
| Intervention manifest | No active company-learning requirement | Intervention episode coordinator | `remove` from active runtime; preserve schema/code pending audit | It advances the wrong form of autonomy. |
| Agency activation | Existing acts/workflow concepts | Agency activation repository and worker | `defer` | Future task-autonomy scaffolding. |
| Work scheduling | Existing obligations/workflow signals | Work scheduling repository and worker | `defer` | Future task-autonomy scaffolding. |
| External effects | No current active requirement | Execution/effect ledgers and contracts | `defer` | Consequential execution remains human-controlled. |
| Component evaluators | Existing benchmark and company-vitals reporting | `lib/evaluation/*` | `consolidate` | Preserve useful metrics, but emit one system report. |
| Architecture registry | Existing architecture checks and docs | Large revised registry and proof matrix | `unclassified` | Retain only the portion that proves the active slice without creating a second architecture product. |

## First Consolidation Targets

1. Remove the intervention episode coordinator from active Compose and process
   manifest registration while preserving applied schema and historical code.
2. Confirm that agency activation and work scheduling remain absent from active
   runtime topology.
3. Keep the source-semantic worker active until its Slack-to-Model vertical is
   preserved by an equivalent integrated path.
4. Decide whether entity-grounding and source-semantic work can converge on one
   existing durable epistemic-work substrate.
5. Fold revised metric output into the existing benchmark/company-vitals
   artifact contract.
6. Reconcile grounding feedback with existing clarification, alias, repair, and
   reflective-learning surfaces before adding a new policy family.

## Proof Required Before Behavioral Expansion

- Clean targeted baseline remains green after each consolidation.
- One production-shaped Slack signal reaches one canonical grounded Model.
- Questions, unsupported mentions, ambiguity, and insufficient context do not
  create false beliefs.
- Duplicate delivery is idempotent.
- Every active worker is named in the current product scope.
- No dormant agency module is imported by the active epistemic path.
- The report represents zero exposure as unknown, not success.
