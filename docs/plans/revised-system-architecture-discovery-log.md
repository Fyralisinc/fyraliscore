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

### DISC-008 — The first grounded-belief lane is an honest narrow cut, not production cutover

- **Date:** 2026-07-16
- **Milestone:** Grounding-to-belief vertical
- **Status:** `deferred`
- **Affected documents:** implementation and evaluation
- **Affected components:** source-semantic extraction, consumer-specific
  grounding admission, EpistemicApplier and operational wiring
- **Observation:** A grounding decision for
  `observation-grounding-sidecar` cannot authorize a canonical belief Model.
  The thin vertical therefore derives a separate live
  `epistemic-applier` GroundingAdmissionDecision from the same immutable
  ResolutionAssessment and binds GroundingContinuity to that new decision.
  Reusing the resolver's original decision would erase the consumer/purpose/
  operation boundary even when the selected referent is identical.
- **Implemented core:** A deterministic extractor admits only a small explicit
  copular asserted/report grammar in which the durable primary mention's exact
  source span/surface is the sentence prefix immediately followed by the
  supported predicate. The only admission entrypoint reloads and re-extracts
  that source truth inside its transaction, persists SourceAssertion,
  SemanticFrameCandidate and SpeechActCandidate, and applies exactly one
  grounded belief through the public Models repository. Questions,
  unsupported expression classes and non-admitted grounding terminate with an
  explicit no-admission fate.
- **Deferred hardening:** The extractor does not yet cover general Slack
  discourse, quotation, recommendations, promises, corrections, quantities or
  temporal operators. The epistemic admission currently uses a fixed service
  policy rather than the full command/result/event/outbox and writer-cutover
  protocol. The live resolver atomically enqueues one durable source-semantic
  work head after each terminal grounding fate. A dedicated leased worker waits
  until the source Observation has a durable embedding, then commits the
  interpretation, admission fate and optional Model with the work
  terminalization in one transaction. The legacy Models repository remains the
  transitional physical writer.
- **Return condition:** Add each deferred capability only after the direct
  Slack -> mention -> grounding -> deterministic source semantics -> one
  admitted belief path is green, then require matched semantic fixtures and
  exact named-writer/fate proof before production cutover.

### DISC-009 — Deferred work after the first live Slack-to-belief proof

- **Date:** 2026-07-16
- **Milestone:** First live end-to-end vertical
- **Status:** `deferred`
- **Affected documents:** implementation and evaluation
- **Affected components:** embedding recovery, source semantics, tenant
  constraints, idempotency and source identity
- **Working core boundary:** A Slack message with either an inline embedding or
  an embedding completed after grounding, one
  source-anchored entity mention, independently supported single-referent
  grounding and the supported asserted/report grammar reaches one canonical
  belief Model without a manual semantic handoff. Grounding atomically creates
  a unique semantic work head. The interpretation, consumer-specific admission,
  Model and terminal work fate then commit atomically under a leased fencing
  token.
  The exact source-native Slack author is retained in the SourceAssertion and
  Model proposition. The legacy late-Think trigger is suppressed because the
  resolver's sidecar admission cannot authorize Think or a second belief write.
- **Implemented recovery:** Every terminal grounding trace idempotently creates
  one `(tenant_id, grounding_trace_id)` work item. Pending embeddings remain in
  `awaiting_embedding`; a bounded poller claims them only after the observation
  proves embedding readiness. Per-claim UUID fencing tokens, expiring leases,
  retry schedules and terminal fates prevent concurrent or crashed workers from
  producing a second semantic result. Runtime manifest and Compose wiring make
  this an owned production process.
- **Recorded edge cases:**
  - Lease heartbeats are deferred because the current deterministic processor
    performs no provider call; unusually slow future extractors will need them.
  - A batch receives one lease timestamp and is processed sequentially. If
    future per-item processing becomes slow enough for later items to approach
    lease expiry before they start, the worker must claim per item, process with
    bounded concurrency or renew the affected leases.
  - Failure handling retries all exceptions up to a fixed cap. A durable
    transient-versus-poison error taxonomy is still required before broad
    production traffic.
  - A grounding admission can expire while work waits a long time for an
    embedding. That condition needs a governed reassessment or explicit
    no-admission fate rather than generic infrastructure retry exhaustion.
  - Terminal embedding failure currently lands in the ingestion DLQ without
    terminalizing the corresponding semantic work item. Operator replay can
    recover it, but permanent failure needs an explicit cross-plane fate.
  - There is no reconciliation sweep yet for historical grounding traces that
    predate the work queue or for an impossible missing work row.
  - The source-semantic evaluator exposes the resulting interpretation and
    admission coverage gap, but does not yet break incomplete work down by queue
    status, embedding wait age, retry age or terminal failure class.
  - The thin worker waits for an embedding before every semantic fate. Future
    optimization may allow deterministic no-admission outcomes to terminalize
    without an embedding, while preserving the invariant that Model admission
    always requires one.
  - Focused tests prove sequential claim, retry, lease recovery and the two
    embedding/grounding orderings. Two-worker contention, mid-processing lease
    loss, authority expiry, embedding-DLQ linkage and historical reconciliation
    remain explicit proof gaps.
  - Sources without a stable native actor reference still carry an explicit
    unresolved-author marker; they need later adjudication rather than a
    fabricated channel identity.
  - Mention, interpretation and admission links are tenant-filtered in the
    writer, but not every relationship has a tenant-composite foreign key.
  - The deterministic grammar intentionally abstains on compound clauses,
    quotations, incidental mentions, temporal qualifiers and all non-report
    speech acts beyond the tested question fate.
- **Reason for deferral:** None of these cases prevents the bounded, honest
  primary path from running end to end. Solving them before that proof would
  expand the slice into recovery infrastructure and general language
  understanding.
- **Return condition:** Add failure classification, reconciliation and lease
  heartbeats when processor behavior requires them, then add source-native
  revision identity before production cutover. Any future Think handoff must
  mint a separate live
  Think-consumer admission and prove it cannot duplicate the source-semantic
  belief. Add grammar classes only with matched positive and negative semantic
  fixtures.

### DISC-010 — The first joined feedback loop is mechanical proof, not learned autonomy

- **Date:** 2026-07-16
- **Milestone:** First closed intervention and feedback loop
- **Status:** `deferred`
- **Affected documents:** implementation and evaluation
- **Affected components:** governed intent, Concerns, consequential agency,
  workflow/task/work ledgers, external effects, outcomes, settlement,
  attribution and joined evaluation
- **Working core boundary:** One real Slack payload now reaches one grounded
  belief, one exactly accepted Goal, one plural-contributor Concern, one
  proposal and preregistered prediction, one exact authorization, one
  workflow/task/work/lease/effect chain, one independently simulated Outcome,
  one Settlement and one conservative Attribution. The final attribution
  explicitly withholds causal credit, and the Concern resolves from the
  independent Outcome rather than from task completion. Duplicate intent,
  effect-transition, episode and semantic-worker submissions preserve
  cardinality-one heads. A cross-plane evaluator reports all thirteen required
  stages and ten joined continuity checks continuously; zero exposure remains
  unknown rather than successful E3 evidence.
- **Implementation evidence:** The production-shaped E2E begins at Slack
  ingestion and uses the canonical domain writers against a disposable
  Postgres database. The simulated adapter and outcome oracle are test-only.
  Migrations were moved after `0207`, and the execution migration preserves the
  complete later writer constraint rather than replaying an older subset.
- **Deferred architecture gaps:**
  - The coordinator is a deterministic test harness, not a durable runtime
    trigger/queue/orchestrator.
  - Intent and Concern commands carry validated writer-scope epochs but do not
    yet consult the canonical writer-epoch registry in the transaction.
  - Intent application does not reject execution before `issued_at`; Concern
    relies on construction-time validation rather than full transactional
    revalidation; attention-binding registration still accepts a plain
    registrar reference.
  - Model-to-Concern and Concern-to-Proposal linkage is enforced through exact
    durable references and the evaluator, not database foreign keys.
  - Joined evaluation does not yet bind the Goal head to its exact interpreted
    proposal/acceptance/source assertion and frame, nor every workflow/task/
    effect authorization, episode, work-target, criterion and attention-binding
    identity as one database-enforced chain.
  - Exact intent acceptance uses wall-clock time internally, so fully
    deterministic simulation-clock replay is not yet supported.
  - Component evaluators still have some presentation-level zero-denominator
    inconsistencies and metric identifier drift outside the joined report.
  - One withheld-credit episode proves explicit feedback capture only. It does
    not prove that accumulated feedback changes future inquiry, prediction,
    proposal, abstention or routing policy, or that such changes improve
    held-out company worlds.
- **Reason for deferral:** None of these gaps invalidates the disposable
  happy-path proof. Expanding into production orchestration, registry fencing,
  complete relational hardening and adaptive-policy learning before this loop
  turned green would have repeated the earlier horizontal implementation
  failure mode.
- **Return condition:** First replace the test coordinator with durable runtime
  orchestration while preserving the same episode and evaluator. Then close the
  exact authority/lineage checks, add failure/recovery scenarios and finally
  demonstrate statistically useful policy improvement across held-out
  simulated company worlds before claiming adaptive learning.

## Reconciliation Procedure

At a reconciliation milestone:

1. Review every `proposed` or `accepted` entry.
2. Decide whether it is accepted, rejected or deferred.
3. Apply accepted changes to both normative documents as one coherent edit.
4. Run architecture-registry digest and documentation consistency checks.
5. Record the reconciliation commit in each incorporated entry.
6. Do not delete historical entries.
