# Revised System Architecture Discovery Log

## Purpose

This log records architecture findings made while implementing and evaluating
the revised reality-belief-intent system. It is the inspection boundary between
runtime discovery and the two normative documents:

- `docs/plans/revised-reality-belief-intent-system-implementation.md`
- `docs/evaluation/revised-system-objective-evaluation-framework.md`

Implementation work must not silently rewrite either normative document as new
facts are discovered. A finding is recorded here first. Normative documents are
updated only in an explicit reconciliation milestone with a reviewable,
coherent diff.

## Status Vocabulary

| Status | Meaning |
| --- | --- |
| `observed` | The repository or runtime exhibits the stated condition. |
| `proposed` | A specific architecture or evaluation change is recommended. |
| `accepted` | The proposal is approved for incorporation. |
| `rejected` | The proposal will not be incorporated; the reason remains recorded. |
| `deferred` | The proposal is valid but outside the active release slice. |
| `incorporated` | The approved change has been reconciled into the normative documents. |

## Discovery Entries

### DISC-001 — Phrase opportunities are not entity mentions

- **Date:** 2026-07-16
- **Milestone:** Exact mention boundary
- **Status:** `proposed`
- **Affected documents:** implementation and evaluation
- **Affected components:** ingestion phrase heuristics, conversational context,
  mention extraction, candidate generation, grounding trace and entity evaluator
- **Observation:** The live resolver begins from ingestion-produced unresolved
  phrase strings. A phrase may be absent from the focal source text, has no
  durable source coordinate and currently becomes a phrase-hash `mention_ref`.
  Treating that opportunity as an `EntityMention` hides false opportunities and
  makes missed or unanchored mentions impossible to evaluate precisely.
- **Evidence:** `services/domain/entity_grounding/episode.py` constructs the
  current candidate request from an observation-and-phrase hash; migration
  `0205_entity_mention_detection_protocol.sql` and the new mention contracts add
  a separate detected/rejected fate with exact anchors.
- **Proposed architecture change:** Treat an ingestion phrase only as a mention
  opportunity. Before candidate generation, persist exactly one versioned
  `EntityMentionDetection` fate. Explicit detections carry reconstructable
  source coordinates; unanchored opportunities terminate without an LLM call;
  implicit mentions require a typed implicit anchor rather than a fabricated
  span. Candidate generation must bind the durable mention identity.
- **Proposed evaluation change:** Add opportunity-to-detection coverage,
  explicit-anchor reconstructability, rejected-opportunity correctness,
  mention-to-candidate continuity and gold mention recall/precision as separate
  continuous dimensions. Zero eligible opportunities remain unknown rather
  than perfect.
- **Runtime consequence if accepted:** The resolver and grounding transaction
  gain a pre-candidate mention stage; downstream records carry detection and
  mention identities; metadata-only false phrases cannot consume model budget
  or enter company identity reasoning.
- **Implementation evidence:** Pending milestone commit.

### DISC-002 — Context exposure and automatic admission are different decisions

- **Date:** 2026-07-16
- **Milestone:** Conversational context selection
- **Status:** `observed`
- **Affected documents:** implementation and evaluation
- **Affected components:** context selection, entity resolver prompt, grounding
  admission and context evaluator
- **Observation:** A vague Slack phrase can benefit from bounded historical
  context even when that context is not stable enough to justify automatic
  consumer admission. Conversely, an explicit context-independent identifier
  should not inherit nearby channel chatter merely because it is available.
- **Evidence:** The current selector records candidate probes and chooses either
  the cheapest sufficient context or the most informative safe partial context;
  grounding admission independently enforces the sufficiency disposition.
- **Architecture consequence:** Preserve context selection and consumer
  admission as separate semantic decisions sharing one exact snapshot. Context
  visible to the assessor must never imply that the resulting identity is safe
  for a consequential consumer.
- **Evaluation consequence:** Report context usefulness, sufficiency,
  contamination and downstream admission safety separately.
- **Implementation evidence:** Existing conversational-context component tests;
  commit reference pending worktree stabilization.

### DISC-003 — Pre-model grounding state needs durable retry identity

- **Date:** 2026-07-16
- **Milestone:** Exact mention boundary
- **Status:** `proposed`
- **Affected documents:** implementation and evaluation
- **Affected components:** grounding work item, context selection, mention
  detection, provider retry and command idempotency
- **Observation:** Persisting context and mention state before a fallible model
  call is desirable for stage-fate visibility, but a retry that regenerates
  snapshot, detection and idempotency identities would collide with the current
  append-only heads or create misleading successor history. Persisting nothing
  until the model returns avoids that collision but leaves an in-flight or
  retryable opportunity without durable pre-model stage artifacts.
- **Proposed architecture change:** Give each grounding work generation one
  durable preparation identity and store the exact context-selection and
  mention-detection command references on that generation. Provider retries in
  the same generation reuse those prepared artifacts. A materially changed
  source, authority, policy or cutoff creates an explicitly related successor
  generation rather than silently rebuilding the same attempt.
- **Proposed evaluation change:** Separate preparation coverage from terminal
  grounding coverage and measure same-generation retry reuse, successor reasons,
  stale-preparation rejection and orphaned prepared-state rate.
- **Runtime consequence if accepted:** Provider timeout, rate limit and parse
  failure retain a reconstructable pre-model state without creating duplicate
  context or mention histories.
- **Implementation evidence:** Deferred until the exact terminal mention path is
  complete; current retryable work remains explicit but pre-model preparation is
  not yet durable.

### DISC-004 — Mention linkage can be hardened with tenant-composite constraints

- **Date:** 2026-07-16
- **Milestone:** Exact mention boundary
- **Status:** `deferred`
- **Affected documents:** implementation
- **Affected components:** mention-detection schema, candidate requests and
  grounding traces
- **Observation:** Migration `0205` links downstream rows to a globally unique
  detection ID and the writer supplies matching detection/mention IDs. The
  database could additionally enforce tenant equality and ID-pair coherence
  through composite foreign keys and checks.
- **Reason for deferral:** The existing foreign key and writer/evaluator checks
  are sufficient to prove the first end-to-end vertical path. Strengthening the
  database now would optimize an edge boundary before the core path runs.
- **Return condition:** Revisit after the detected and rejected resolver paths
  pass end to end and before production cutover or cross-tenant adversarial proof.

## Reconciliation Procedure

At a reconciliation milestone:

1. Review every `proposed` or `accepted` entry.
2. Decide whether it is accepted, rejected or deferred.
3. Apply accepted changes to both normative documents as one coherent edit.
4. Run architecture-registry digest and documentation consistency checks.
5. Record the reconciliation commit in each incorporated entry.
6. Do not delete historical entries.
