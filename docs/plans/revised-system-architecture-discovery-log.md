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
- **Implementation evidence:** Commit `97a95023` implements detected and
  rejected mention fates, exact anchors, transactional lineage and continuous
  evaluator coverage.

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
- **Implementation evidence:** Commit `97a95023` plus the conversational-context
  component and evaluator tests.

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

### DISC-005 — Live Slack mention discovery precedes the context that gives it meaning

- **Date:** 2026-07-16
- **Milestone:** Slack-to-grounding vertical
- **Status:** `observed`
- **Affected documents:** implementation and evaluation
- **Affected components:** ingestion mention opportunities, conversational
  context, mention detection and entity resolver polling
- **Observation:** The exact context, mention, candidate, assessment and
  admission chain works once an unresolved phrase exists, but the live producer
  only creates phrases that contain capitalization or a hyphen. Characteristic
  Slack references such as `the project`, `the customer` and `it` therefore
  never reach context selection unless a test or caller injects them manually.
- **Architecture consequence:** Mention-opportunity creation must be a bounded,
  source-anchored recall step rather than a proxy mention decision. It may
  nominate proper names, source-native handles and a conservative vocabulary of
  definite conversational references; exact mention detection and context-aware
  grounding still decide their fate downstream.
- **Evaluation consequence:** A production-shaped Slack fixture must begin at
  ingestion with no hand-authored `_unresolved_phrases` and measure source-to-
  opportunity recall separately from opportunity-to-detection correctness.
- **Active slice:** Implement the bounded live opportunity handoff and prove a
  lowercase definite reference reaches the already-working grounding chain.
- **Deferred follow-up:** Learned open-class mention discovery, implicit-anchor
  semantics and full gold recall/precision calibration remain after the thin
  vertical is green.

### DISC-006 — Successful grounding is not yet a consumed model dependency

- **Date:** 2026-07-16
- **Milestone:** Grounding-to-belief vertical
- **Status:** `observed`
- **Affected documents:** implementation and evaluation
- **Affected components:** resolver trigger handoff, source semantics,
  epistemic admission, Think and Models
- **Observation:** The resolver commits an exact GroundingAdmissionDecision and
  enqueues a late T1 trigger, but the legacy reasoning path can reload the raw
  Observation and create a Model without validating or persisting the grounding
  dependency. Before the current fix, the trigger also used `entity_ref` while
  Think hydrates `seed_entity_ids`, so even the admitted referent was dropped
  from the effective reasoning seed.
- **Architecture consequence:** First preserve the admitted referent and exact
  assessment/admission references at the trigger boundary. Then add one narrow
  ordinary asserted-report lane that persists source-semantic annotations, an
  explicit belief-admission fate and exact grounding continuity before creating
  one canonical belief Model. Other speech acts must terminate without belief
  admission in this slice.
- **Evaluation consequence:** Measure admission-to-trigger coverage, referent-
  to-Think-scope continuity, trigger terminal fate, model source-provenance
  closure, model grounding-dependency closure and complete vertical continuity.
- **Active slice:** Trigger payload continuity and one isolated asserted-report
  grounding-to-belief path with a question/non-admitted negative control.
- **Deferred follow-up:** Legacy Think cutover, full source-native revision
  storage, intent/action routing, graph edges and correction repair are outside
  this first working vertical.

### DISC-007 — Source-native conversation revisions remain a known adapter boundary

- **Date:** 2026-07-16
- **Milestone:** Slack-to-grounding vertical
- **Status:** `deferred`
- **Affected documents:** implementation and evaluation
- **Affected components:** Slack create/edit/delete/reaction reconstruction,
  ConversationEventRevision and conversational topology
- **Observation:** Slack ingestion preserves revision and topology metadata, but
  the current context selector still adapts legacy Observation rows into
  synthetic `observation:<id>:v1` revision references. There is no canonical
  ConversationEventRevision store yet.
- **Reason for deferral:** The normative checkpoint explicitly permits the
  legacy-Observation adapter, and it can prove the first source-to-grounding-to-
  belief loop. Building the full revision store before that proof would delay
  the core vertical.
- **Return condition:** Implement before claiming edit/delete/reaction replay,
  late reinterpretation or source-native conversational fidelity as complete.

## Reconciliation Procedure

At a reconciliation milestone:

1. Review every `proposed` or `accepted` entry.
2. Decide whether it is accepted, rejected or deferred.
3. Apply accepted changes to both normative documents as one coherent edit.
4. Run architecture-registry digest and documentation consistency checks.
5. Record the reconciliation commit in each incorporated entry.
6. Do not delete historical entries.
