# Autonomous Company Learning Edge-Case Ledger

## Purpose

This ledger records valid but noncritical cases discovered while delivering the
working company-understanding and learning loop. It prevents the implementation
from expanding into every edge case before the central end-to-end behavior is
green.

An issue must be fixed immediately when it can corrupt canonical truth, violate
tenant or authority isolation, break provenance, invalidate feedback
attribution, prevent correction, or block the active end-to-end path.

Everything else receives an explicit safe behavior and return condition here.

The active evaluation boundary starts after transport: tests consume simulated,
normalized, source-attributed signals already persisted in PostgreSQL.
Connector polling, listeners, webhooks, provider backfill and ingestion
transport are excluded from this goal.

## Status Vocabulary

| Status | Meaning |
| --- | --- |
| `open` | Recorded and not yet scheduled. |
| `bounded` | Current behavior is safe but incomplete. |
| `active` | Included in the current implementation milestone. |
| `resolved` | Implemented and verified. |
| `rejected` | Intentionally unsupported, with reason recorded. |

## Entries

### EDGE-001 — Source-native Slack revision history

- **Status:** `resolved` for the sealed Slack operating envelope
- **Current behavior:** Slack ingestion and context selection preserve
  source-derived thread/reply and edit succession, immutable deletion
  tombstones and reaction-evidence revisions. The sealed edit,
  deletion/tombstone and reaction families all reconstruct with their expected
  revision fates.
- **Evidence:** The nine-family Slack gold is `9/9`; edit/delete correctness,
  topology recall, selected-context precision and sufficient-set recall are
  `1.0`, with contamination `0.0`.
- **Remaining boundary:** This does not prove arbitrary historical Slack export
  fidelity, every Slack subtype, source retention beyond the captured event
  envelope, large-history replay, or unseen edit/delete/reaction forms.
- **Return condition:** Reopen for production Slack export/backfill,
  long-duration retention and previously unseen revision families.

### EDGE-002 — Durable pre-model preparation identity

- **Status:** `open`
- **Current behavior:** Retryable grounding work is durable, but exact prepared
  context and mention identities are not guaranteed to be reused across every
  same-generation provider retry.
- **Risk:** Duplicate or misleading preparation histories after timeout,
  rate-limit, or parse failure.
- **Safe boundary:** Do not treat provider-retry preparation coverage as
  complete.
- **Return condition:** Implement before long-running production retry and
  crash/restart proof.

### EDGE-003 — Tenant-composite grounding constraints

- **Status:** `bounded`
- **Current behavior:** Global IDs, foreign keys, writer validation, and
  evaluator checks bind mention and grounding records.
- **Missing behavior:** Additional tenant-composite database constraints.
- **Safe boundary:** Existing writer and evaluator checks remain mandatory.
- **Return condition:** Add before cross-tenant adversarial pilot proof.

### EDGE-004 — Open-class implicit mention discovery

- **Status:** `bounded`
- **Current behavior:** The live path can nominate proper names, source-native
  identifiers, and a conservative set of contextual references.
- **Missing behavior:** Learned open-class implicit/coreferential mention
  discovery.
- **Safe boundary:** Unsupported opportunities abstain or route to
  clarification rather than fabricating a mention.
- **Return condition:** Begin only after the first correction-driven grounding
  policy loop is green.

### EDGE-005 — Long-range and cross-channel Slack recurrence

- **Status:** `resolved` for the sealed Slack families
- **Current behavior:** Governed phrase-anchor recurrence can recover bounded
  long-range, cross-thread and cross-channel Slack dependencies while
  preserving the cutoff and rejecting high-similarity distractors.
- **Evidence:** The sealed long-range recurrence, cross-thread dependency and
  cross-channel dependency families are all correct; long-range and
  cross-channel recall are `1.0`, selected contamination is `0.0`, and the
  high-similarity case abstains safely.
- **Remaining boundary:** Open-ended channel discovery, arbitrary recurrence
  distance, very large histories, cross-workspace Slack and equivalent
  conversational reconstruction across Jira, email, documents and meetings
  remain unproven. Persisted Jira-, Linear-, Google Drive- and Gmail-attributed
  structured identity semantics are proven, but connector transport is excluded
  and those semantics are narrower than conversational reconstruction.
- **Return condition:** Reopen during multi-source and production-scale
  recurrence evaluation.

### EDGE-006 — Multilingual and code-switched entity mentions

- **Status:** `open`
- **Current behavior:** No declared comprehensive multilingual operating
  envelope.
- **Safe boundary:** Report the regime as unmeasured; use clarification or
  abstention where confidence is insufficient.
- **Return condition:** Add after English Slack/entity quality is stable and
  evaluator gold contracts support multilingual ambiguity.

### EDGE-007 — Full intent, agency, Work, and external-effect activation

- **Status:** `rejected` for the current release slice
- **Current behavior:** Some contracts, schema, repositories, and tests exist
  as dormant future scaffolding.
- **Safe boundary:** No production worker may instantiate, schedule, lease, or
  execute consequential company work.
- **Return condition:** Reconsider only after autonomous company-learning lift,
  correction closure, authority integrity, and human control are proven.

### EDGE-008 — Generic universal learned-policy controller

- **Status:** `rejected`
- **Reason:** Fyralis already contains retrieval learning, reflective rules,
  route utilities, calibration, and feedback statistics. A universal parallel
  controller would duplicate lifecycle and authority.
- **Preferred direction:** Extend the existing lifecycle one policy family at
  a time, beginning with contextual entity grounding.

### EDGE-009 — Original grounding supersession after adjudication

- **Status:** `resolved`
- **Current behavior:** An accepted clarification appends an explicitly typed
  N+1 grounding generation over the exact reviewed trace, reuses the immutable
  context and mention annotations, gives both generations a source-semantic
  fate, and can apply exactly one Model from the original source coordinates.
  The N generation remains immutable historical review/no-admission evidence.
- **Evidence:** The real-Postgres Slack vertical proves review -> clarification
  -> adjudicated successor -> original no-admission -> successor belief
  application -> replay-stable single Model. The evaluator selects the latest
  work generation as current while preserving both trace generations.

### EDGE-010 — Clarification compatibility mutates observation annotations

- **Status:** `active`
- **Current behavior:** Legacy clarification finalization appends the chosen
  entity to `observations.entities_mentioned` and emits an authoritative state
  change.
- **Risk:** A later consumer may confuse human adjudication with a source-native
  mention, while the original grounding trace says the source observation was
  not mutated.
- **Safe boundary:** Treat `entities_mentioned` as a compatibility annotation,
  never as independent source evidence. Grounding lineage remains authoritative.
- **Return condition:** Replace the mutation with a versioned correction/
  annotation projection before Observation immutability is claimed.

### EDGE-011 — Repair of an already admitted wrong identity

- **Status:** `resolved` for the seeded company-learning cascade
- **Current behavior:** An authoritative grounding successor can supersede an
  already admitted wrong identity. The runtime archives the wrong Model,
  immediately hides direct and second-hop dependents, queues
  correction-specific re-evaluation with immediate-parent lineage, retires
  contaminated relation frames and relation-edge projections, removes stale
  projection state and consumes the queued refresh through the existing
  projector runtime. Dependents explicitly archive or unfence rather than
  receiving a confidence-only nudge.
- **Evidence:** The serialized real-Postgres correction proof establishes
  source immutability, tenant isolation, bounded cycle-safe recursive fanout,
  fresh uncontaminated projection rebuild and replay idempotency.
- **Remaining boundary:** The proof is a seeded second-hop cascade, not an
  oracle-complete production dependency census. Very large/deep graphs,
  sustained refresh load, kill/restart/reorder, hidden dependents, distinct
  revocation/deletion and policy/reward/intent repair remain unproven.
- **Return condition:** Reopen for production-scale repair, oracle dependency
  coverage and non-identity correction classes.

### EDGE-012 — Governed exact-alias autonomous replay

- **Status:** `resolved`
- **Current behavior:** A privileged human may explicitly promote a stable,
  non-contextual correction to `tenant_global_exact`. A later exact anchored
  mention can reuse it without an LLM only after exact clarification/successor
  lineage, current admin or leadership authority, one unambiguous normalized
  alias population, an active supported tenant-local canonical target, and
  transaction-time state revalidation. The normal grounding, admission,
  source-semantic and Model path remains mandatory.
- **Safety behavior:** Contextual phrases, conflicting source hints, malformed
  or unsupported targets, stale authority, invalid lineage, revoked state and
  normalized alias conflicts fall back to the ordinary model/review path.
- **Evidence:** Real-Postgres adaptive tests compare the corrected later
  occurrence against the frozen/model path, prove one avoided model call,
  preserve exact grounding lineage and report continuous replay exposure,
  resolution, safety and contextual-replay metrics.

### EDGE-013 — Legacy adjudicated aliases without explicit scope

- **Status:** `bounded`
- **Current behavior:** Pre-scope adjudicated aliases remain available as
  evidence to the existing LLM assessment path for compatibility, but they are
  never eligible for deterministic autonomous replay.
- **Safe boundary:** No legacy alias may gain tenant-global replay authority
  from lexical shape, usage counters or historical confidence.
- **Return condition:** Backfill only through explicit privileged review with
  canonical-target and scope validation; otherwise leave legacy rows model-
  mediated.

### EDGE-014 — Governed non-exact candidate-memory variants

- **Status:** `resolved` for the sealed unambiguous population
- **Current behavior:** The resolver can use an independently adjudicated long
  form to expose the same governed target as a candidate for a later acronym,
  punctuation/spacing form, anchored short form, omitted-letter form or
  possessive/plural form. The normal closed-set model assessment and grounding
  path remains mandatory; this is not deterministic replay or alias promotion.
- **Evidence:** The sealed 24-pair real-Postgres harness covers six variant
  families and four entity types. The adaptive arm resolves every target, the
  frozen arm has zero target-candidate exposure and safely reviews or abstains,
  and source immutability and zero hard-safety incidents are required.
- **Remaining boundary:** Every case has one deliberately unambiguous target.
  The proof does not cover competing variants, lifecycle changes or open-world
  novelty. Assurance v3 now treats this population as a mandatory,
  noncompensatory component and reopens its typed evidence and digests.

### EDGE-015 — Variant collision and homonym safety

- **Status:** `resolved` for the sealed 16-case E4 scope
- **Trigger:** An acronym, short form, normalized spelling or typo can rank two
  permitted entities, including entities of different types or validity times.
- **Risk:** Candidate-memory lift can become a high-confidence false merge or
  wrong downstream Model even though the learned target was correct elsewhere.
- **Safe boundary:** Candidate ranking may improve recall but must not remove
  any colliding candidate, none-of-the-above or review/abstention fate. Usage,
  confidence and SAGE utility cannot promote identity.
- **Current evaluator:** A deterministic 16-case registry now covers eight
  collision families: same- and cross-type acronyms, ambiguous short forms,
  punctuation/Unicode normalization, channel-local nicknames, authenticated
  source-ID conflicts, inactive targets and historical-name reuse. It measures
  candidate visibility, none-of-the-above availability, unsafe versus
  authoritative resolution, wrong Models, alias promotion, source immutability
  and exact family/entity/lifecycle strata.
- **Current runtime evidence:** The real-Postgres runner executes `16/16`
  cases. Adaptive and frozen arms safely contain every ambiguous collision
  with zero unsafe resolutions, zero incidents, zero wrong Models and zero
  alias promotions. Candidate visibility, none-of-the-above availability and
  source immutability are all `1.0`. Both authenticated source-native cases
  resolve the authorized conflicting target in both arms with a two-case
  authoritative-resolution rate of `1.0`. Archived and inactive targets are
  fenced, and multiple live exact candidates without decisive authority force
  review instead of allowing model confidence to select one.
- **Assurance boundary:** Assurance v5 makes the complete collision artifact
  mandatory and noncompensatory. `working` now requires all `16/16` cases,
  both authenticated source-native cases and every safety metric to satisfy
  the sealed contract.
- **Return condition:** Execute the sealed registry on real PostgreSQL. Require
  zero learned-target or other unauthorized resolutions, zero wrong Models,
  zero promotion and complete safe containment for every sealed case.
  Authenticated source-native identifiers may resolve only their authorized
  active conflicting target. New source surfaces must not claim this coverage
  until their simulated normalized, source-attributed fixtures persist the same
  authenticated binding evidence.

### EDGE-016 — Rename and lifecycle changes cannot be aliases

- **Status:** `resolved` for customer rename/archive/name-reuse and the sealed
  canonical resource-replacement vertical; broader lifecycle remains `active`
- **Trigger:** A company object is renamed, archived and re-created, merged,
  split, replaced or resurrected.
- **Risk:** A timeless alias redirect can rewrite history, join distinct
  entities or keep stale authority and dependencies alive.
- **Safe boundary:** Keep the current referent and alias state unchanged unless
  a canonical lifecycle owner establishes identity continuity, valid time,
  predecessor/successor lineage and dependent repair. An alias adjudication
  alone cannot merge or split referents.
- **Current runtime evidence:** The sealed eight-case real-Postgres proof
  preserves one customer UUID through rename and archive, resolves aliases by
  valid time, rejects stale and archived names, safely reuses historical names,
  leaves old Observation and Model references immutable, prevents interval
  overlap, isolates tenants and makes rename/archive replay idempotent. All
  continuous metrics are `1.0`, all `8/8` cases execute and no violations are
  recorded.
- **Assurance boundary:** Customer lifecycle remains a v5/v6 component.
  Assurance-v7 schema foundation makes the separate canonical resource
  replacement component mandatory and noncompensatory.
- **Replacement runtime evidence:** Commits `7ad02256`, `3c7dff0c` and
  `8ce4b555` materialize resource retirement, alias closure, exact
  source-binding supersession, projection invalidation and lineage verification
  in one transaction while preserving Observations, old attachments and Models.
  The UTF8 PostgreSQL runner in `eb1f9a84` observes all `20/20` sealed
  obligations with zero unsupported cells or violations. `3a03981d` adds
  tenant-scoped, bitemporal lineage-aware resource reads.
- **Return condition:** Prove rename continuity, archive/name reuse and
  stale-alias rejection first — complete for customers. Canonical resource
  replacement is complete for its sealed exact vertical. Reopen for merge,
  split, resurrection, other referent types and downstream consumers that have
  not adopted the lineage-aware read seam. Source-binding close, revoke and
  supersede have a separate bounded contract in EDGE-019.

### EDGE-017 — Structured source claims cannot create identity authority

- **Status:** `resolved` for the persisted Jira-, Linear-, Google Drive- and
  Gmail-attributed identity surfaces; connector coverage is excluded
- **Trigger:** An authenticated structured source contains a stable object ID
  and a human-readable key/name that may refer to a canonical company entity.
- **Risk:** Treating handler JSON or matching text as authority can fabricate
  identity, leak a binding across sources or apply one observation-level
  binding to unrelated phrases.
- **Safe boundary:** A handler may emit only a typed, source-namespaced claim.
  Ingestion may attach it only to one pre-existing governed binding, one exact
  normalized source surface and a matching source system. Missing or ambiguous
  bindings fail closed; free text never creates authority.
- **Current evidence:** Simulated normalized, source-attributed fixtures already
  persisted in PostgreSQL carry Jira project, Linear project/team, Google Drive
  file/comment/revision and Gmail thread identity semantics. Focused
  real-Postgres tests prove exact resolver consumption, event-time liveness,
  forged-text rejection, missing-field and missing-binding inertness, source
  isolation and tenant isolation. They do not prove listener or connector
  transport.
- **Evaluator boundary:** Assurance v6 now seals and reopens six identity
  surfaces: Jira project, one Linear issue bundle covering project/team claims,
  Google Drive file/comment/revision and Gmail thread. All `6/6` are observed
  with zero violations, alongside `5/5` source-salience cases. Exact expected
  and observed source systems, native IDs, surfaces and authority references
  are digest-bound, and foreign-tenant consumption is probed directly.
- **Return condition:** Extend the same contract to simulated normalized
  meeting and remaining source-attributed populations; add mention-specific
  multi-object populations and causal learning proofs per source. Connector
  transport requires a separate future objective.

### EDGE-018 — Company-learning feedback cannot become self-authorizing truth

- **Status:** `resolved` for the first SAGE matching-source salience bridge;
  calibration and route-specific attribution remain `active`
- **Trigger:** Grounding and source-semantic terminal outcomes accumulate and
  can improve future retrieval policy.
- **Risk:** Rewarding model confidence or admitted Models as truth would create
  a self-reinforcing loop that amplifies incorrect identity or company beliefs.
- **Safe boundary:** Feedback is bounded operational-yield evidence only.
  Corrected predecessors lose credit; correction successors, review and
  no-admission remain low/near-neutral. The resulting profile is tenant-scoped,
  `canonical_write=false`, `salience_only=true` and `authority_effect=none`.
- **Current evidence:** Repeated useful source outcomes raise matching-source
  retrieval salience, a corrected source does not, foreign-tenant outcomes are
  excluded and before/after snapshots show no Model or grounding truth changes.
- **Return condition:** Add empirical weight calibration, temporal decay,
  route-specific causal attribution and production-scale read-cost proof.
  Bounded retention is now measured separately in EDGE-020.

### EDGE-019 — Source-binding lifecycle and historical attachments

- **Status:** `resolved` for the sealed E4 close, revoke and supersede contract;
  broader referent/source coverage and historical continuity remain `bounded`
- **Trigger:** A structured source object is renamed, invalidated, rebound or
  superseded while observations still refer to an earlier binding version.
- **Risk:** Silent redirection can rewrite evidence history; overlapping
  intervals can make source authority ambiguous; partial lifecycle writes can
  leave inconsistent current state.
- **Current behavior:** `SourceIdentityBinding` repository operations preserve
  valid-time history, append transaction-time versions, record replayable
  operation references, reject stale expected versions, isolate tenants and
  permit a successor exactly at the predecessor boundary. New bindings whose
  current-knowledge intervals overlap an existing binding are rejected.
- **Attachment boundary:** Existing attachments remain storage-exact and pinned
  to v1. After v1's transaction interval closes, operational
  `resolve_observation_source` returns no result rather than redirecting to a
  successor. A delayed historical Observation may attach the visible closure
  version v2. This is safe stale fencing, not reconstruction of the old
  attachment.
- **Enforcement boundary:** Migration `0223` adds a database-native GiST
  exclusion constraint for transaction-current valid-time overlap. The sealed
  runner proves that a direct SQL writer bypassing the repository is rejected
  with SQLSTATE `23P01`.
- **Evaluator boundary:** The E4 runner uses two populated tenants with
  colliding source-native identifiers, full persisted Observation and
  attachment snapshots, cross-tenant mutation probes and a digest-bound
  query/row/error manifest. Assurance v7 reopens and recomputes all `12/12`
  lifecycle obligations.
- **Evidence:** `53083717`, `f97f0ab2`, `5032b15c` and `84c6d199`; exact
  Assurance v7 at `be401f25` reports `working` with zero blockers.
- **Return condition:** Reopen for every-source/every-referent coverage,
  independent writers beyond the sealed direct-SQL probe, or a requirement to
  resolve a historically attached predecessor after its transaction interval
  closes rather than failing closed.

### EDGE-020 — Retention metrics can overstate restart and learning durability

- **Status:** `resolved` for the bounded assurance v6 population; long-duration
  durability remains `active`
- **Trigger:** Learned exact, variant or corrected behavior is reevaluated after
  intervening system activity or a nominal restart.
- **Risk:** A perfect retention score can be mistaken for proof of process,
  queue, database or deployment durability, or for resistance to unrelated
  end-to-end learning interference.
- **Current evidence:** The sealed real-Postgres retention component observes
  `14/14` cases. Exact, governed-variant and corrected retention, restart
  survival, correction authority, source immutability, Model consistency and
  evidence-lineage consistency are `1.0`; forgetting, unsafe globalization
  and hard-safety incidents are `0.0`; all four existing negative controls and
  three representative collision families remain safe; retention-horizon AUC
  is `1.0`. Safety regressions are noncompensatory.
- **Assurance integration:** V6 binds the exact 14 observations, named negative
  and collision families, and 2/2/10 horizon distribution. It reopens raw
  evidence, recomputes the report and blocks `working` on forgetting, coverage,
  authority, consistency, immutability or safety regression.
- **Restart boundary:** The restart metric constructs fresh
  `EntityResolverWorker` and `SourceSemanticWorker` objects in the same process
  against the same connection pool and database. It does not terminate the
  process, recover queues, reconnect, restart the database or redeploy.
- **Interference boundary:** The 0/4/16 intervening cycles are direct governed
  `EntityAliasRepo` writes over newly seeded resources. They represent
  alias-registry growth, not unrelated clarification, worker or complete
  company-learning cycles.
- **Consistency boundary:** Model consistency is a cardinality/ID round trip
  against IDs read from the same recurrence rows. Evidence-lineage consistency
  proves the Observation, answered clarification and adjudicated alias rows
  exist. Neither independently proves proposition semantics, canonical
  referents, lifecycle/projection validity, complete relational linkage,
  digests or correction propagation.
- **Correction and population boundary:** Corrected retention reuses the final
  exact result and original clarification replay authority; it does not apply a
  second correction that replaces a learned wrong target. One governed variant
  and three of eight collision families are covered. Tenant isolation is not
  independently measured in this adaptive-only runner.
- **Return condition:** Add true process/queue/database/deployment restart,
  unrelated end-to-end learning interference, a second correction, independent
  semantic/lineage validation, all five remaining collision families and
  long-duration horizons.

### EDGE-021 — Standalone learning evidence is not combined assurance

- **Status:** `resolved` for the current Assurance v7 component set
- **Trigger:** A focused evaluator passes before its evidence is registered,
  reopened and recomputed by the combined assurance contract.
- **Risk:** A standalone result can be reported as system-wide proof without
  digest binding, component-accounting checks or fail-closed combined status.
- **Current behavior:** Assurance v7 emits, reopens, digest-checks, recomputes
  and noncompensatorily gates identity, salience, retention, canonical
  replacement and source-binding lifecycle evidence. It binds the exact Git
  commit and clean/dirty worktree digest, rejects unsupported lifecycle evidence
  tiers and reopens digest-bound raw database manifests.
- **Evidence:** Exact `be401f25` artifact:
  `/tmp/fyralis-company-learning-assurance-be401f25/company_learning_assurance_summary.json`;
  status `working`, zero blockers, replacement `20/20`, source binding `12/12`,
  repository state `clean`.
- **Safe boundary:** This is E4 sealed simulation/mechanism evidence, not
  certification, open-world accuracy or customer-value proof.
- **Return condition:** Reopen when adding a mandatory component, changing the
  active evidence tier, or seeking independently reconstructable certification
  beyond the sealed database manifests.

### EDGE-022 — Assurance database encoding and Unicode verifier parity

- **Status:** `resolved`
- **Trigger:** A disposable company-learning database or harness verifier
  handles Unicode collision fixtures differently from production
  normalization.
- **Risk:** Environment bootstrap or verifier defects can stop summary creation
  and be misreported as company-learning system failures.
- **Observed behavior:** The first disposable cluster used `SQL_ASCII` and
  rejected the sealed Unicode collision before evaluation. A fresh UTF8 cluster
  then exposed a harness-only mismatch for fullwidth `Ａ`: PostgreSQL
  `lower()` under the C locale did not match Python `casefold()`, even though
  the corrective-memory row existed with the correct target and authority.
- **Resolution:** Disposable assurance clusters must use UTF8. Commit
  `3a21f6b1` makes harness verification use the production Python normalizer
  and still checks exact target, resolution scope, clarification lineage,
  adjudication state and answer digest.
- **Evidence:** The focused 16-case collision suite and the full assurance v6
  CLI pass on fresh UTF8 PostgreSQL. The final CLI completes in `31.43s`; the
  commit-labelled v6 artifact is the exact evidence recorded under EDGE-021.
- **Return condition:** Reopen if a new database locale/encoding or Unicode
  normalization family causes harness and production lookup semantics to
  diverge.

### EDGE-023 — Connector and listener transport is outside the active goal

- **Status:** `rejected` for this objective
- **Trigger:** A proposed milestone requires implementing or validating Slack,
  Jira, email, document or meeting polling, listeners, webhooks, backfills or
  provider delivery.
- **Reason:** The active goal is company understanding, learning, correction
  and lifecycle behavior after a signal has been normalized and persisted.
  Transport work would widen the objective without improving the epistemic
  proof currently being completed.
- **Safe boundary:** Every active test starts from a simulated normalized,
  source-attributed signal already stored in PostgreSQL. Source identity and
  authority must still be explicit; fabricated provenance is not allowed.
- **Return condition:** Reconsider only as a separately authorized connector
  reliability objective with its own delivery, replay, outage and backfill
  evaluation.

### EDGE-024 — Mention candidates bypass the governed detection-fate protocol

- **Status:** `implemented; focused proof only`, P0 validation
- **Trigger:** The 45-batch DB-backed Vitals run generated 10,325 phrase
  opportunities across 1,125 observations but found zero detection heads,
  detections, work items or grounding traces.
- **Current behavior:** `74f3149c` closes deterministic detected/rejected fates
  for eligible candidates at the persisted-observation batch boundary. The
  evaluator now labels this as protocol-fate coverage and explicitly does not
  equate it with gold extraction precision, recall, typing or linking quality.
- **Risk:** Entity extraction is outside the authoritative audit trail; missed
  and rejected candidates are indistinguishable; every downstream Model and
  graph-quality claim is weakened.
- **Safe boundary:** Treat this as zero protocol-fate coverage, not as 10,325
  proven missed gold entities.
- **Return condition:** Every eligible candidate receives one immutable
  terminal disposition, and protocol coverage is reported separately from
  labeled span/linking precision and recall.
- **Evidence:** Authoritative cold-start postmortem; Vitals incident
  `entity_grounding.mention_opportunity_without_detection_fate`.

### EDGE-025 — Resolver writes opaque aliases into canonical identity

- **Status:** `implemented; focused proof only`, P0 validation
- **Trigger:** The large run persisted 50 canonical aliases whose metadata
  names `resolver_worker` as the source.
- **Current behavior:** `ed93bf50` rejects canonical alias persistence without
  an authorized adjudication trace bound to the exact reviewed grounding
  lineage. Resolver output is withheld pending grounded adjudication.
- **Risk:** Opaque identity pollution, likely duplicate canonicalization and
  fragmentation, misleading product labels and a bypass of candidate/
  adjudication authority.
- **Observed non-failure:** These 50 rows do not directly prove a false merge:
  each alias maps one-to-one to a distinct resolved ID.
- **Safe boundary:** Resolver output may nominate candidates but must not be
  treated as canonical identity truth.
- **Return condition:** Canonical alias writes require an explicit promotion or
  adjudication record and matching grounding trace; resolver-owned writes are
  rejected.

### EDGE-026 — Mature retrieval remains flat and mixed

- **Status:** `implemented; focused proof only`, P0 validation
- **Trigger:** The requested cold-start behavior expected early observation use
  to give way to mostly Model retrieval.
- **Current behavior:** `666ae2ee` introduces cold/developing/mature budgets,
  Model-first mature context and typed raw-evidence reopening reasons;
  `e04da5e8` gates maturity on quality-weighted Model mass so a weak Model tail
  cannot suppress necessary evidence. The quoted flat shares remain the
  unrerun 45-batch baseline.
- **Risk:** High prompt cost, raw-evidence dependence, weak compression value
  and repeated selection of unused context.
- **Use evidence:** Late normal waves referenced roughly 31.3% of selected
  Models and 65.6% of selected observations. Fourteen late raw reopenings had no
  recorded reason.
- **Safe boundary:** Do not claim Model-first memory metabolism.
- **Return condition:** Outcome-gated observation budgets, explicit reopening
  reasons and stable late Model dominance with improved selected/use ratios.

### EDGE-027 — Batch context contaminates canonical Model scope and text

- **Status:** `implemented; focused proof only`, P0 validation
- **Trigger:** Final Models average 11.73 scoped entities; 84/86 Models have at
  least ten. Canonical memory also contains benchmark-wrapper and inquiry-policy
  language.
- **Current behavior:** `b38faf87` requires claim-local semantic evidence for
  inferred scope, excludes context-only entities and filters event-batch
  wrapper claims plus tagged question-policy, retrieval-policy and capability
  control claims before admission.
- **Risk:** Cross-storyline relevance inflation, false graph overlap,
  calibration contamination, projection amplification and untrustworthy
  company propositions.
- **Safe boundary:** High storyline and product proxy scores remain provisional
  until scope and contamination are corrected.
- **Return condition:** Claim-local mentioned/decisive scope, exclusion of
  context-only entities and admission guards against prompt/wrapper/inquiry
  language.

### EDGE-028 — Asymmetric graph relations become reciprocal

- **Status:** `implemented; focused proof only`, P0 validation
- **Trigger:** The final graph contains eight reciprocal
  `early_warning_for` pairs, four reciprocal `blocks` pairs and one reciprocal
  `contradicts` pair.
- **Current behavior:** `a68ecd5d` registers role-stable asymmetric kinds and
  rejects an unjustified reciprocal edge in the canonical edge repository;
  relation projection preserves the registered source/target roles.
- **Risk:** Reversed causality and dependency semantics in the company graph.
- **Safe boundary:** Mechanical no-self/no-orphan/no-duplicate checks do not
  establish relation correctness.
- **Return condition:** Per-kind source/target role contracts, reciprocal-edge
  rejection unless independently justified, and repair/retirement of invalid
  existing pairs.

### EDGE-029 — Recovered Think failure is counted as terminal failure

- **Status:** `bounded`
- **Trigger:** Wave 19 exhausted three 180-second provider attempts, then the
  same 25-signal trigger succeeded on its configured run-level retry.
- **Current behavior:** All 45 batches ultimately succeeded and queues drained,
  but Vitals hard-fails on the historical failed run count.
- **Risk:** Reliability incidents are either understated as success or
  overstated as lost company learning.
- **Safe boundary:** Preserve the failed attempt and retry cost, but distinguish
  it from an unrecovered trigger.
- **Return condition:** Report attempt failures, recovered trigger failures,
  terminal failures, recovery latency and retry cost separately; reserve the
  noncompensatory failure gate for terminal/unrecovered work or an explicit SLA
  breach.
- **Current implementation state:** Still bounded rather than closed. None of
  the seven postmortem commits changes recovered-versus-terminal run accounting;
  this distinction must remain explicit in the next evaluator/runtime proof.

## Entry Template

### EDGE-NNN — Short title

- **Status:** `open`
- **Trigger:** What reveals the case.
- **Current behavior:** What the active system does.
- **Desired behavior:** What complete support would do.
- **Risk:** Truth, safety, quality, cost, or operability impact.
- **Safe boundary:** Acceptable behavior until implementation.
- **Return condition:** Exact milestone that brings the item back into scope.
- **Evidence:** Tests, traces, reports, or source references.
