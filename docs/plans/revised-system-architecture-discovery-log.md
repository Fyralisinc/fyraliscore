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

### DISC-011 — Episode-manifest projection is now durable, but it is not the intervention saga

- **Date:** 2026-07-16
- **Milestone:** First production-shaped intervention runtime process
- **Status:** `deferred`
- **Affected documents:** implementation and evaluation
- **Affected components:** agency canonical events, InterventionEpisode,
  EpisodeCoordinator, runtime process manifest, leased work, joined evaluation
- **Working core boundary:** A production-registered worker now discovers the
  ten supported version-one intervention-stage events from canonical agency
  history, revalidates each event against its exact CommandResult and source
  object/version, and establishes one durable work head per episode stage.
  Work is claimed with an expiring lease and fresh fencing token. The worker
  locks the current episode head, adds or replaces only the exact stage link,
  invokes `EpisodeCoordinator` under a narrow manifest-projection context and
  acknowledges the work in the same transaction. An identical link is a
  no-op; a different present object is a terminal invariant violation.
  Retry, terminal-failure, expired-lease recovery and stale-token rejection are
  explicit. Runtime manifest, Compose and health wiring make the worker an
  owned production process. The joined evaluator continuously reports queue
  fate counts, completion rate, incomplete work and terminal failures.
- **Implementation evidence:** The real Slack-to-feedback E2E no longer asks
  the test harness to complete the final episode. The durable worker links
  Proposal, Prediction, Authorization, Workflow, Task, Work, Effect, Outcome,
  Settlement and Attribution through ten separately committed episode
  versions. A replay discovers and claims zero new work, preserves one episode
  head and leaves all ten work items applied. Focused repository tests prove
  idempotent discovery, exact source revalidation, retry scheduling,
  expired-lease takeover and stale-worker fencing. The worker's ten commands
  are checked to carry only `intervention_episode` object authority derived
  from their exact canonical-event reference.
- **Deferred architecture gaps:**
  - This process is a projection consumer, not the intervention saga. It does
    not decide, authorize, schedule, execute, reconcile, observe Outcomes,
    settle or learn. Each named semantic writer remains authoritative.
  - It assumes an InterventionEpisode already exists. The current joined test
    still opens the episode and supplies its belief, intent and Concern links
    before the durable worker begins. Runtime episode creation and earlier
    Concern/inquiry continuity remain unimplemented.
  - Discovery scans immutable canonical events into a dedicated leased queue.
    It is not yet a partitioned, position-aware fan-out consumer with
    ConsumerReceipts, schema-gap detection and a durable high-water mark.
    Existing single-destination agency outboxes cannot safely be marked
    delivered by this additional consumer.
  - `EpisodeStageLink` permits one object per stage. Multi-task, multi-work or
    multi-effect episodes cannot be represented honestly. A second distinct
    object for the same stage is surfaced as
    `INTERVENTION_MANIFEST_STAGE_CONFLICT`; until a repeated/grouped stage
    contract and durable discovery-rejection fate exist, that poison event can
    block later discovery in the same poll transaction.
  - The worker issues a deliberately narrow embedded service-processing
    context because the current agency protocol has no canonical authority
    broker for this projection role. It cannot inherit or mint action
    authority, but broker issuance, revocation and audit are not yet proven.
  - The embedded `EpisodeCoordinator` writer scope is compatible with the
    current strangulation protocol, but is not yet registered and fenced
    through the canonical writer-scope registry for every tenant.
  - Batch claims have no heartbeat. The current deterministic work is short,
    but slower future validation or high contention will require per-item
    claims, bounded parallelism or lease renewal.
  - The worker links successful `PRESENT` objects only. Rejected, expired,
    infeasible and censored paths still need exact typed-absence projection and
    a contract capable of retaining the governing decision reference.
  - The clean E2E proves crash-safe transaction shape, replay and repository
    lease takeover, but does not yet inject a process crash between every
    discovery, claim, episode-CAS and acknowledgment boundary under concurrent
    workers.
  - Queue completion proves audit-manifest convergence, not that the action was
    beneficial or that feedback changed future policy behavior.
- **Reason for deferral:** The bounded process removes a real manual handoff
  from the only complete vertical and provides a durable place to measure
  orchestration health. Expanding it into a semantic super-writer, generic
  queue platform or full adaptive controller would violate the single-writer
  architecture and repeat the earlier horizontal implementation failure.
- **Return condition:** Keep this worker narrow. Next add a separate durable
  intervention saga that advances existing named writers through exact
  commands, beginning with eligible Concern to Proposal/spec, preregistered
  Prediction and authorized Workflow/Work. Add scheduler/lease and effect
  reconciliation lanes independently. Before multi-step production episodes,
  introduce fan-out consumer receipts/high-water tracking, registered
  projection authority and a repeated-stage manifest contract.

### DISC-012 — Exact authorization can create planned agency, but not honest Work economics

- **Date:** 2026-07-16
- **Milestone:** Durable authorization-to-planned-agency activation
- **Status:** `deferred`
- **Affected documents:** implementation and evaluation
- **Affected components:** AuthorizationDecision, InterventionSpec,
  WorkflowRun, Task, WorkObligation, activation runtime and joined evaluation
- **Working core boundary:** A production-registered activation worker now
  consumes only the exact version-one canonical event for an authorized
  `AuthorizationDecision`. Discovery revalidates the canonical event against
  its CommandResult, accepted Proposal, immutable InterventionSpec, exact
  operation, target and parameter-field scope. It freezes a versioned plan with
  deterministic WorkflowRun and Task UUIDs, activation time, workflow-spec
  reference and grounded target. Leased workers recover abandoned claims with
  a new fencing token. In one transaction the worker asks
  `AgencyStateApplier` to create one planned Workflow and one planned
  external-effect Task, verifies their exact first versions and terminalizes
  the activation work. Expired authorization, retry and terminal failure are
  explicit fates. The worker's processing context is restricted to the exact
  internal Workflow or Task object and cannot schedule, lease or dispatch an
  external effect.
- **Implementation evidence:** The joined Slack-to-feedback E2E no longer lets
  the test harness create the Workflow or Task. The activation worker creates
  both from the frozen plan, after which the harness advances their later
  lifecycle states. Replaying the activation worker discovers and claims zero
  new work. Its two commands use deterministic version-five command IDs and
  exact object-restricted processing authority. Repository tests prove
  idempotent discovery, source-chain revalidation, deterministic plan identity,
  exact planned-object verification, lease takeover, stale-worker rejection,
  retry, authorization-expired and failed-terminal fates. The joined evaluator
  continuously reports activation exposure, fate counts, completion rate,
  incomplete work, authorization expiry and terminal failure.
- **Deferred architecture gaps:**
  - This worker instantiates internal agency only. It does not make the
    Workflow active, make the Task ready or in progress, register Work, select
    a processing class, lease a worker, reserve an effect or call a provider.
  - `workflow_spec_version_ref` is an exact string identity, but there is not
    yet a canonical WorkflowSpec registry/body that the worker can resolve and
    compile. The first slice therefore uses a fixed versioned one-task
    activation policy. It cannot yet prove that an arbitrary referenced
    workflow definition was faithfully instantiated.
  - The immutable Proposal and Authorization do not carry a defensible
    `WorkObligation.expected_value` or an exact economic assessment reference.
    The test harness still supplies `expected_value=0.8` and related priority
    scores. Copying those constants into production would fabricate company
    physics, so Work registration intentionally remains outside this worker.
  - The InterventionSpec contains one target and operation, while real
    workflows may require multiple tasks, prerequisites, owners, checkpoints
    and non-effect work. The current deterministic one-task identity scheme is
    only the first template family.
  - Rejected authorization events do not establish activation work because
    they are outside the activation-eligible denominator. The authorization
    ledger remains the canonical rejected fate; a future full saga report may
    still want an explicit no-activation ConsumerReceipt.
  - Discovery is another direct canonical-event scan rather than a partitioned
    fan-out consumer with high-water, gap detection and ConsumerReceipts.
  - The narrow processing grant and writer scope remain embedded protocol
    contexts rather than broker-issued and registry-fenced runtime authority.
  - A single transaction prevents a partial Workflow-without-Task activation,
    but concurrent-worker crash injection and historical queue reconstruction
    have not yet been exercised beyond lease/replay tests.
  - Activation completion proves exact internal object creation, not action
    value, outcome improvement or policy learning.
- **Reason for deferral:** Workflow and Task identity are derivable from exact
  authorization without making a new company judgment. Work economics are not.
  Stopping at this semantic boundary removes another manual runtime handoff
  while refusing to turn a convenient test estimate into canonical production
  truth.
- **Return condition:** Add a canonical WorkflowSpec/template registry and a
  governed economic-assessment object that supplies expected value, cost,
  priority, uncertainty and envelope provenance. Then let a distinct Work
  registrar create the exact obligation, and let a scheduler/governor decide
  eligibility and lease under its own policy and authority. Preserve the
  activation worker as planned-agency-only.

### DISC-013 — Registered Work can be scheduled durably, but execution ownership is still absent

- **Date:** 2026-07-16
- **Milestone:** Durable registered-Work scheduling and initial lease fencing
- **Status:** `deferred`
- **Affected documents:** implementation and evaluation
- **Affected components:** WorkObligation, WorkDecision, LeaseToken, Task,
  AuthorizationDecision, scheduler runtime and joined evaluation
- **Working core boundary:** A production-registered scheduler now consumes the
  exact version-one canonical registration event for task-targeted Work. It
  revalidates the event and CommandResult, immutable Work specification,
  current Task and Workflow versions, episode, exact AuthorizationDecision and
  InterventionSpec. Discovery freezes deterministic decision and lease
  identities, the minimum permitted processing class, scheduling time, lease
  owner, heartbeat, expiry and policy version. A leased queue claim is distinct
  from the canonical Work lease. In one transaction the worker asks
  `WorkLedgerApplier` to move Work from `registered` to `eligible`, issue Lease
  version one with fence one and move Work to `leased`, then acknowledges only
  after exact decision/lease/head verification. Work-deadline and authorization
  expiry first transition canonical Work to `expired`; the queue fate cannot
  claim expiry by itself.
- **Implementation evidence:** The joined Slack-to-feedback E2E no longer
  creates the WorkDecision or LeaseToken in the test harness. The scheduler
  creates Work version two, Work version three and Lease version one/fence one,
  after which the harness begins effect reservation. Replay discovers and
  claims zero scheduling work. Repository tests cover deterministic planning,
  exact Work-to-Task-to-Workflow-to-Authorization-to-spec continuity, lease
  reclaim and stale-token rejection, retry, canonical Work expiry, canonical
  authorization-expiry closure and terminal failure. Scheduler commands use
  deterministic command IDs and authority restricted to the exact Work object.
  The joined evaluator reports scheduling exposure, leased rate, backlog,
  incomplete work, both expiry classes and terminal failure continuously.
- **Deferred architecture gaps:**
  - The scheduler consumes already-registered Work. The test harness still
    creates the WorkObligation and supplies its provisional economic and
    priority values; DISC-012's WorkflowSpec and governed economic-assessment
    requirements remain unresolved.
  - The first scheduling policy selects the Work obligation's declared minimum
    processing class. It does not yet compare capacity, tenant budgets,
    attention limits, opportunity cost, queue fairness, urgency or competing
    Work. Those are scheduler policy inputs, not safe constants to infer here.
  - The Lease names a fixed external-effect executor role, but that executor is
    not yet a production process. Effect reservation, dispatch intent,
    provider call, observation and reconciliation remain in the test harness.
  - No runtime currently heartbeats, releases, resolves or safely takes over
    the canonical Work lease. The repository proves initial queue-claim
    takeover, which is distinct from proving effect-safe Work lease takeover.
  - A frozen schedule whose first heartbeat deadline elapses before processing
    is terminally surfaced as stale. A governed replan/redrive protocol is
    needed rather than silently extending the original plan.
  - The scheduler independently scans canonical events instead of consuming a
    partitioned fan-out stream with ConsumerReceipts and high-water/gap
    detection.
  - Its narrow processing context and writer scope are still embedded rather
    than broker-issued and registry-fenced.
  - The current policy covers initial generation-one leasing. Deferred work,
    suppression, useful-safe fates, owner terminalization, retry-wait,
    reconciliation-required, redrive and later-generation scheduling still
    need separate scenarios.
  - A leased Work item is not proof of an external effect, an Outcome or useful
    intervention value. Unknown effects must continue to block completion.
- **Reason for deferral:** Registered Work already carries the minimum facts
  required for one conservative initial scheduling decision, so removing this
  manual handoff does not invent new company semantics. Provider execution and
  effect reconciliation have materially different crash and duplicate-effect
  hazards and must remain a separate implementation boundary.
- **Return condition:** Implement a fenced external-effect executor that
  revalidates the exact active Work lease, AuthorizationDecision,
  InterventionSpec and adapter capability; commits dispatch intent before the
  provider call; and records provider observations only through
  `ExecutionLedgerApplier`. Add an independent reconciler for `unknown` and
  ambiguous effects before automating retries or takeovers.

### DISC-014 — Current autonomy belongs in company learning, not task execution

- **Date:** 2026-07-16
- **Milestone:** Product-boundary correction after the first agency-runtime
  slices
- **Status:** `accepted`
- **Affected documents:** implementation and evaluation
- **Affected components:** entity grounding, conversational context, source
  semantics, company models, retrieval, adaptive inquiry, feedback, Workflow,
  Task and Work runtimes
- **Observation:** The current product objective is not autonomous company
  operation. Fyralis should autonomously improve its evidence-grounded,
  temporal model of the company while consequential company work remains under
  human control. The implemented agency activation and Work scheduling
  protocols are useful future scaffolding, but launching them as production
  workers makes the present runtime optimize the wrong form of autonomy.
- **Working core boundary:** The contracts, repositories, launch scripts and
  tests for planned agency activation and registered-Work scheduling remain in
  the repository as dormant, directly invocable scaffolding. They are removed
  from the production process manifest and Compose topology. The active
  production path continues through signal capture, conversational/entity
  grounding, source semantics and company-model learning; it does not
  automatically instantiate or lease consequential tasks.
- **Implementation evidence:** Commit `65b407dd` preserves a fully tested
  future external-effect executor and commit `c21358c9` removes it from the
  active tree. This milestone additionally removes `agency_activation_worker`
  and `work_scheduler_worker` from production runtime registration while
  retaining their isolated implementations and tests.
- **Immediate implementation consequence:** The next vertical must close a
  real epistemic loop: a correction, adjudication or later evidence item must
  alter future context selection, entity resolution, source-semantic admission
  or company-model construction through governed learned state, and evaluation
  must measure the resulting model-quality change against an unchanged
  baseline.
- **Deferred architecture gaps:** Existing feedback tables and traces often
  prove that feedback was recorded, not that future company understanding
  improved. Slack context selection still uses deterministic heuristics,
  selected episode hypotheses are snapshot-local, general discourse
  preconditions remain incomplete, and downstream correction closure across
  grounding, source semantics and canonical models is not yet proven.
- **Reason for correction:** Automating Work can make the system more active
  without making its company model more accurate. Until the epistemic feedback
  loops are autonomous and objectively beneficial, task execution would
  amplify model errors rather than solve the central product problem.
- **Return condition:** Reconsider production agency workers only after
  learning-loop evaluation proves stable company-model improvement, bounded
  regressions, correction propagation, temporal/authority integrity and
  explicit human control over consequential actions.

### DISC-015 — Human correction is an annotation successor, not new source truth

- **Date:** 2026-07-16
- **Milestone:** Corrective-memory constitutional boundary
- **Status:** `accepted`
- **Affected documents:** implementation and evaluation
- **Affected components:** Observation, clarification answer path, entity alias,
  grounding generations, source semantics, Model and correction evaluator
- **Observation:** The legacy clarification path appended the accepted entity
  to the original Observation and emitted an authoritative
  `internal:state_change` Observation describing the resolver's own
  conclusion. That made a downstream interpretation rewrite source evidence
  and then re-enter perception as authority.
- **Working core boundary:** Source Observations remain immutable evidence.
  Human adjudication records an independently governed alias and creates a new
  immutable grounding successor linked to the rejected/reviewed predecessor.
  Source semantics consumes that successor and may write a canonical Model
  through the normal epistemic admission path. Later signals may reuse the
  governed alias, but neither the original signal nor a synthetic resolver
  assertion becomes source truth.
- **Implementation evidence:** The clarification router no longer updates or
  inserts Observations for an accepted entity resolution. The real-Postgres
  correction/replay vertical asserts both source Observations remain unchanged,
  no `entity_late_resolution` Observation exists, successor semantics closes,
  and the replayed signal still creates exactly one grounded Model.
- **Deferred architecture gaps:** The first vertical proves repair of the
  reviewed source's grounding and semantic consequence. It does not yet prove
  complete invalidation and repair of every pre-existing Model, graph relation,
  projection or retrieval artifact that may depend on an incorrect grounding.
- **Return condition:** Add an evaluator-oracle dependency population and prove
  generation-aware repair closure for every materially affected canonical and
  derived dependent without altering historical as-known source evidence.

### DISC-016 — Runtime health and system substantiation are different claims

- **Date:** 2026-07-16
- **Milestone:** Canonical company-learning evaluation integration
- **Status:** `accepted`
- **Affected documents:** implementation and evaluation
- **Affected components:** conversational-context evaluator, entity-grounding
  evaluator, source-semantic evaluator, architecture proof compiler, Company
  Vitals and company-intelligence loop
- **Observation:** Filtering out unknown component rates allowed a partially
  exposed run to appear `substantiated`. Separately, component CLIs could each
  print a mostly empty full invariant matrix, producing several competing
  readiness views.
- **Working core boundary:** Exact report Observation IDs define the active
  evaluation population. The three component evaluators feed one
  `CompanyLearningEvaluationState` and one
  `InvariantEvidenceManifest`. Operational closure may be `healthy`,
  `incomplete`, `contradicted` or `not_observed`; the stronger
  `substantiated` claim is available only when the registered invariant proof
  compiler confirms every active company-learning invariant. Company Vitals is
  the single operator-facing report and company physics remains
  noncompensatory.
- **Implementation evidence:** Company Vitals records an explicit evaluation
  cutoff from one read-only repeatable-read database snapshot, verifies the
  complete manifest Observation population, preflights every table used by the
  active evaluators, persists the combined state and evidence manifest, and
  reports proof gaps without converting missing exposure into success. An
  artifact-only rerender revalidates the saved state, manifest, report
  population, tenant, system version, experiment digest and current
  architecture digest before recomputing assurance.
- **Deferred architecture gaps:**
  - Current live DB evaluation is E3 protocol evidence. The registered E4
    scenario suites and held-out adaptive-versus-frozen comparisons remain to
    be executed and attached.
  - Persisted artifacts freeze the report once written, but an explicit DB
    refresh observes current heads and latest generations. General bitemporal
    as-of reconstruction for every mutable evaluator input remains future work.
  - Older benchmark artifacts may not declare an exact system version or
    executed scenario IDs; the manifest reports these as unreported/empty and
    therefore cannot support a stronger proof claim.
  - Focused component CLIs remain useful diagnostics. They are outside the
    canonical operator path but have not yet been reduced to state-plus-manifest
    output.
- **Return condition:** Run sealed E4 simulation suites with registered scenario
  IDs, frozen system/version manifests and paired adaptive baselines; then use
  the existing proof compiler to determine whether the active learning
  invariants become substantiated.

### DISC-017 — Clarification is the human-work record; review queue is compatibility

- **Date:** 2026-07-16
- **Milestone:** Entity-review ownership consolidation
- **Status:** `accepted`
- **Affected documents:** implementation and evaluation
- **Affected components:** entity resolver, clarification requests, grounding
  traces, clarification answer path and entity-grounding evaluator
- **Observation:** `entity_review_queue` and `clarification_requests` described
  the same unresolved human judgment with separate IDs and lifecycle fields.
  No independent production consumer required the older queue.
- **Working core boundary:** A new entity review opens one
  `clarification_requests` row whose object is the exact grounding trace. The
  candidate set and stage lineage remain in its payload. Review admission and
  clarification creation are atomic with grounding. The applied queue schema
  remains historical compatibility only.
- **Implementation evidence:** The resolver no longer inserts queue rows; the
  evaluator derives review-obligation coverage from exact clarification
  lineage; the answer path handles new `grounding_trace` objects directly and
  updates queue rows only for historical `entity_review` objects.
- **Deferred architecture gaps:** Existing legacy queue rows are not backfilled
  into a formal projection or deleted. Schema-drift tooling still describes the
  applied table, as it must until a forward retirement migration is justified.
- **Return condition:** After production data confirms no active non-clarification
  consumer, add a forward compatibility projection or archival migration and
  remove the legacy answer branch.

## Reconciliation Procedure

At a reconciliation milestone:

1. Review every `proposed` or `accepted` entry.
2. Decide whether it is accepted, rejected or deferred.
3. Apply accepted changes to both normative documents as one coherent edit.
4. Run architecture-registry digest and documentation consistency checks.
5. Record the reconciliation commit in each incorporated entry.
6. Do not delete historical entries.
