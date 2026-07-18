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
- **Additional homeostasis evidence:** `933b048c` runs two DB corrections,
  fences eight Models, records eight reevaluation pairs, rejects two cycle
  writes, creates no work on replay and preserves the exact durable fingerprint
  across runtime restart. Every registered check scores `1.0`; objective SHA
  begins `382df`.
- **Remaining boundary:** The proof is bounded, not an oracle-complete
  production dependency census. Very large graphs, infrastructure loss,
  sustained refresh load, hidden dependents, revocation/deletion and
  policy/reward/intent repair remain unproven.
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

- **Status:** `bounded pass`; authoritative large-run failure retained
- **Trigger:** The requested cold-start behavior expected early observation use
  to give way to mostly Model retrieval.
- **Current behavior:** The post-fix nine-batch proof meets preregistered policy:
  early observation share `1.0`, late Model selection `8/11`, late actual Model
  reference `0.8`, and late reopening-reason coverage `1.0`. The quoted flat
  shares remain the immutable 45-batch baseline.
- **Risk:** High prompt cost, raw-evidence dependence, weak compression value
  and repeated selection of unused context.
- **Use evidence:** Historical late waves referenced roughly 31.3% of selected
  Models and 65.6% of selected observations with 14 unjustified reopenings;
  bounded post-fix use is `0.8` with full reason coverage.
- **Safe boundary:** Claim bounded retrieval-policy correction only, not
  company-scale metabolism or learning lift.
- **Return condition:** Untouched long-horizon evidence preserves Model
  dominance/use and improves a terminal company-model measure.

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

### EDGE-030 — Deterministic fallback can conceal learned-discovery absence

- **Status:** `bounded`
- **Trigger:** Persisted batch processing can continue through the deterministic
  locator when learned discovery is disabled or the provider fails.
- **Current behavior:** The discovery result records `learned` versus
  `deterministic_fallback` and provider readiness is preflighted at worker
  startup.
- **Risk:** Healthy fate closure may be mistaken for learned entity quality.
- **Safe boundary:** Report mode, provider error and learned-provider exposure;
  never aggregate fallback rows into a learned-quality claim.
- **Return condition:** Company Vitals noncompensatorily separates learned,
  fallback and unavailable populations and their quality/cost distributions.

### EDGE-031 — Holdout adaptation masquerades as generalization

- **Status:** `bounded`
- **Trigger:** The first deterministic holdout became strong after fixes made
  against it, while a later untouched deterministic holdout remained weak.
  This historical failure mode remains relevant even though learned v3 is
  independently strong at the complete extraction-path level.
- **Risk:** Repeated fixture-driven tuning produces an attractive score without
  robust open-company entity discovery.
- **Safe boundary:** An adapted holdout is regression evidence only. A
  generalization claim requires a newly sealed organization/entity/time split
  scored once after policy and thresholds freeze.
- **Current evidence:** Sealed v3 was organization/entity/time/text-disjoint
  and executed exactly once. Raw model exact F1 was `0.762590`; 13 uniquely
  source-repaired coordinates lifted the complete extraction path to F1
  `0.942857`, type accuracy `0.985714`, negative cleanliness `1.0`, with source
  strata and the workstream F1 `0.5` tail reported. Report SHA is
  `4427b73f…2263eca`.
- **Return condition:** Preserve v3 without selective rerun and require a new
  sealed population for any materially changed prompt/policy. Canonical-link
  and company-scale claims still require their own gold.

### EDGE-032 — Entity pipeline stops before canonical-link proof

- **Status:** `bounded`; exact production-shaped vertical populated, broader P0 remains
- **Current behavior:** Pipeline evaluator metrics exist for discovery, typing,
  resolver handoff and canonical linking, and learned candidates enter the
  existing resolver. The learned v3 benchmark deliberately uses null canonical
  referents. Separately, `eaa02f3f` executes a sealed seven-signal DB-backed
  vertical. Candidate recall at 1/3/5 is `1.0`; canonical coverage and accuracy
  are `5/5`; harmful false links and resolver-owned canonical writes are zero.
- **Risk:** Good spans and types can coexist with wrong company identity, making
  downstream Models and graph structure meaningless.
- **Safe boundary:** State `no v3 canonical-link claim` and `bounded vertical
  link proof`; do not generalize five eligible cases to open-world accuracy.
- **Return condition:** Widen sealed referents across organizations,
  common/tail/homonymous entities, temporal transitions, source drift and
  consequential downstream uses with calibrated automatic coverage.

### EDGE-033 — Post-hoc learned-output repair is not an independent run

- **Status:** `closed for extraction generalization; historical warning retained`
- **Trigger:** The original real `gpt-5.4` run scored P/R/F1
  `0.8163/0.6452/0.7207`; uniquely source-verifiable offset repair plus threshold
  rescore of the saved outputs scored `0.8500/0.8226/0.8361`.
- **Risk:** The latter can be misreported as fresh provider or generalization
  evidence.
- **Safe boundary:** Label it an artifact rescore and preserve both metric sets.
- **Return condition:** Satisfied by sealed one-shot v3 at `0d9d8e65`; keep the
  historical v1 rescore labeled post-hoc and separate forever.

### EDGE-034 — Learned discovery has batch-atomic schema failure and systematic semantic tails

- **Status:** `bounded`; v2 failure retained, v3 complete extraction path
  strong, raw-coordinate and workstream-boundary tails remain
- **Immutable evidence boundary:** The sealed v2 corpus has 80 unique signals,
  eight genuine ten-signal batches, 40 hard negatives and 114 exact typed gold
  spans (SHA-256
  `8285d139f8346bbf0dce73bb65a5f0547d9a8c768a72c533602fc71592f81d51`).
  The report in
  `/tmp/learned_entity_discovery_quality_v2_report.json` is an exact recovery
  of eight completed `gpt-5.4` structured turns after report rendering failed;
  it is not a provider rerun. Operational latency and usage are unavailable.
  These results must not be rescored under later code.
- **Observed aggregate:** Production verification admitted 101 predictions:
  81 exact spans, 98 overlap matches, 16 unmatched gold mentions, three
  unmatched predictions and 18 type errors among the 98 matches. Exact span
  precision/recall/F1 is `0.8020/0.7105/0.7535`; type accuracy is `0.8163` and
  hard-negative cleanliness is `0.9500`. Canonical referents are all null, so
  this supplies no canonical-link evidence.
- **P0 — isolate invalid structured items instead of losing a batch:** Batch 7
  returned one unsupported type, `service`. Strict `LearnedMentionBatch`
  validation rejected the whole response; deterministic fallback emitted zero
  candidates, losing all 15 gold mentions in that batch. This accounts for
  15/16 omissions. Across the seven schema-valid learned batches, overlap
  coverage was 98/99 and the only semantic omission was `Kýma contract`.
  `6967e605` now proves schema failure, timeout and malformed-sibling handling
  preserve terminal fate for all ten signals, award zero learned-quality credit
  and replay idempotently. This closes accountability loss; fallback recovery
  quality remains separate and can never be counted as learned success.
- **P0 — preserve role-bearing mention boundaries:** Seventeen overlap matches
  were not exact: Jira seven, Slack six and email four. The dominant pattern is
  removal of a type-bearing prefix or suffix, for example `Decision D-β12` to
  `D-β12`, `Team Varde` to `Varde`, `Copper Finch rollout` to `Copper Finch`,
  and `Serra CRM project` to `Serra CRM`. By gold type this affects workstream
  4/5, decision 3/5, goal 3/5, project 3/11, team 2/13, and one each of
  commitment and resource. Exact-span verification cannot repair a valid but
  semantically truncated span; boundary policy needs explicit role-designator
  training examples and a deterministic, evidence-preserving expansion rule.
- **P0 — type classification needs company-ontology evidence:** Eighteen of 98
  overlap matches had the wrong type: Jira eight, Slack six and email four.
  The concentrated confusions are resource to `other` (3), product to customer
  (3), team to customer (2), system to product (2), plus eight singleton
  confusions. Gold-type error counts are resource 5/8 matched, product 5/9,
  system 4/11, team 2/11, commitment 1/5 and project 1/9. Person, customer,
  decision, goal and workstream matches were typed correctly. Discovery should
  retain span evidence separately from type assessment, allow calibrated
  `other`/review rather than forced precision, and use relation/context cues
  plus existing company ontology without letting type uncertainty erase the
  mention.
- **P1 — reject syntax and transport artifacts with calibrated abstention:**
  The three unmatched predictions were all typed `resource`: Jira environment
  value `test-2` (`0.82`), Slack thread reference `#392` (`0.83`) and Jira path
  `/api/search` (`0.75`). Two polluted hard-negative signals, so the problem is
  not solved by the current confidence threshold. Candidate policy needs
  negative examples for environment values, message/thread locators, paths,
  trace IDs and other transport syntax, with source-aware abstention metrics.
- **Context/source distribution:** Omissions were email 7, Slack 6 and Jira 3,
  but 15/16 came from the batch-atomic failure. Boundary errors were
  Jira/Slack/email `7/6/4`; type errors were `8/6/4`; false positives were
  `2/1/0`. At the original Slack strata, the failed batch caused three
  omissions each in temporal-sequence and cross-thread-reference signals.
  Schema-valid Slack errors were otherwise cross-channel-temporal (three
  boundaries, one type), cross-thread (one boundary, one type, one false
  positive), temporal-sequence (one boundary, two types), and delayed threaded
  replies (one boundary, two types). Standalone, ordinary threaded and channel
  follow-up negatives stayed clean, but these small strata do not establish
  general reliability.
- **Separate development corpus:** Do not tune on sealed v2 or v3. The unsealed,
  versioned development set contains independently authored organizations and
  surfaces, balanced by source, Slack context and all 11 entity types. Include
  (1) valid responses containing one deliberately invalid enum or malformed
  sibling, (2) role-bearing prefix/suffix boundary pairs, (3) ambiguous
  product/customer/team/system/resource contexts, (4) multilingual and symbol
  identifiers, and (5) source-specific syntax negatives. Track item-isolation
  recovery, exact boundary F1, per-type confusion, negative cleanliness,
  abstention/coverage and fallback-only recall separately. Any future sealed
  organization/entity/time-split evidence must remain separate from it.
- **V3 update:** The sealed one-shot run completed all four ten-signal batches
  with exactly four calls and no schema/provider errors. It matched 70/70 gold
  mentions by overlap and 66/70 exactly. Raw exact F1 was `0.762590`; 13 model
  coordinate errors were uniquely source-repaired, so post-verification F1
  `0.942857` measures the complete extraction path, not direct model offsets.
  Type accuracy was `0.985714`, cleanliness `1.0`, and source F1 Slack `0.9545`,
  email `0.9286`, Jira `0.95`. Four boundary errors remain. Workstream exact
  F1 is `0.5` (2/4), though all four overlap and workstream type is `1.0`.
- **Return condition:** Learned and fallback populations remain separately
  attributable; malformed-sibling isolation remains regression tested; and a
  future prompt change addresses workstream boundaries on development data and
  earns new sealed evidence rather than rescoring/rerunning v3.
- **Supplemental v2 evidence:** A separate untouched boundary/type holdout now
  reports exact F1 `0.984127` over 31 gold mentions and worst-type resource F1
  `0.888889`. It is additive evidence, not a v3 rescore: v3 workstream F1 stays
  `0.5`. One of 15 negative signals admitted `request AB-22` as a resource, and
  the executed artifact lacks raw provider output and a pre-call running
  receipt. Objective readiness therefore records narrow exceptional quality,
  below-budget negative cleanliness (`14/15`) and unknown auditability blockers
  simultaneously.
- **Protocol closure:** A generic source-verifiable guard now rejects a literal
  only when the focal source explicitly labels it as syntax/example/schema
  metadata; positive controls preserve genuine Request/Resource mentions. A
  separate untouched ten-signal holdout produced 10/10 exact mentions, type
  accuracy `1.0` and 5/5 clean negatives with the running receipt, raw output,
  checkpoint and completed one-attempt receipt all present. Its scope is small;
  it does not erase v2's historical false positive/audit gap or broader v3's
  workstream tail.

### EDGE-035 — High-confidence wrong type can drive consequential admission

- **Status:** `bounded`, P0 noncompensatory risk
- **Trigger:** Sealed v3 contains one exact-span, high-confidence (`0.92`)
  prediction routed as `resource` where evaluator gold is `goal`. The frozen
  example is not a tuning fixture; the risk class is what matters.
- **Risk:** Exact detection and high confidence can conceal the wrong
  company-object physics, causing a Model, edge, authority decision or learned
  outcome to be admitted under the wrong semantics.
- **Safe boundary:** Span correctness never compensates for consequential type
  error. Preserve the type distribution and abstain/review before any
  consequential write when the consequence-specific type threshold is unmet.
- **Current behavior:** `200d7e48` separates detection confidence from type
  confidence. Ambiguous code-like identifiers without a nearby role cue retain
  their mention but cap type confidence below resolver narrowing. The sealed
  vertical shows zero known-wrong-type consequential admissions. The four
  frozen-v3 boundary errors and its type error remain immutable evaluation
  evidence; v3 is not rescored or rerun.
- **Return condition:** A sealed production-shaped pipeline suite shows zero
  wrong-type consequential admissions, calibrated type risk at automatic
  coverage, and complete downstream containment/repair for injected mistakes.

### EDGE-036 — Entity correctness can silently corrupt relation topology

- **Status:** `bounded adversarial DB pass`; broader P0 remains
- **Trigger:** A missed, false or wrongly linked mention can create a wrong
  active edge, reverse direction, choose the wrong relation type, or contaminate
  Models beyond the originating grounding case.
- **Current behavior:** `a8487036` extends the persisted entity-pipeline
  evaluator with evaluator-owned admission/non-admission expectations, endpoint
  identity, type, direction, exact mention-lineage, unexpected-edge, harmful
  relation/model propagation, unknown-endpoint and unlineaged-active-edge
  metrics. Shared endpoint adjacency is not accepted as causal origin. The
  positive vertical admits one directed `blocks` edge with exact source-mention
  lineage and preserves an explicit no-edge expectation; admission, endpoint,
  type, direction and lineage are `1.0` on their exact denominators, with zero
  unexpected or harmful edges. Adversarial v2 then rejects four harmful writes
  without mutation across wrong direction, mutually exclusive type, self-link
  and cycle closure; it measures a two-hop chain and immediate correction
  propagation. Objective entity v2 scores `0.9901315789`, blockers clear.
- **Risk:** Strong extraction can coexist with a meaningless or poisoned
  company graph; a five-case link population cannot bound broad identity risk.
- **Safe boundary:** Four adversarial attempts and two open-world cases do not
  establish company-scale topology quality; completed transitive repair is not
  claimed while the second hop awaits reevaluation.
- **Return condition:** Widen relation gold across organizations, relation
  families, consequence tiers, unlabeled cases, temporal drift and completed
  multi-hop repair under load.

### EDGE-037 — Selected Models are not necessarily used by reasoning

- **Status:** `resolved on versioned development data; generalization open`
- **Trigger:** Real ablation v2 selected no prior Models. After the SAGE seam
  fix, postfix v3 selected three Models in learned batch two and six in batch
  three, but referenced zero; both arms still recovered `0/3` hidden theses.
- **Risk:** Retrieval dashboards can look Model-first while reasoning continues
  to reconstruct from raw evidence, producing no causal learning advantage.
- **Current behavior:** `ce6ea870` adds a generic summary consumer with no
  hidden-truth access. On the same matched three-by-six development experiment,
  learned selects/references `0/0`, `3/3`, `6/6`, recovers `3/3` versus frozen
  `0/3`, lift `1.0`, ECE `0.1925` versus `0.5725`, Brier `0.037056` versus
  `0.327756`, score `1.0`.
- **Safe boundary:** V2/v3 failures remain in the evidence trail. V4 is
  development mechanism evidence, not untouched or customer-value proof.
- **Return condition:** Repeat on a newly sealed organization/entity/time split
  with independent hidden truth and retain Model-reference and terminal lift.

### EDGE-038 — Exact truth replay can double-count confirmation sidecars

- **Status:** `deferred to P8 fault/idempotency`
- **Trigger:** The canonical truth command correctly replays the same target,
  transition and exact evidence without advancing its head, but the legacy
  applier can still increment `confirmed_count` and update confirmation audit
  sidecars after receiving the replayed command receipt.
- **Risk:** Canonical truth remains correct while compatibility counters and
  operational telemetry overstate independent corroboration.
- **Safe boundary:** Do not interpret legacy confirmation counters as unique
  epistemic evidence; canonical signed evidence and truth versions govern.
- **Return condition:** The P8 duplicate-delivery probe demonstrates that exact
  replay leaves both the canonical head and every confirmation sidecar/audit
  count unchanged, while distinct evidence advances once.

### EDGE-039 — Governed synthesis coordinates need broader compatibility coverage

- **Status:** `deferred after minimal governed-coordinate vertical`
- **Trigger:** Synthesis now requires one exact nonempty canonical reference on
  the parsed scope surface across the conclusion and every auxiliary signal.
- **Current behavior:** Current detection heads, consumer-resolved traces, and
  provisional detection references are supported. Untethered typed-entity
  sidecars are deliberately not synthesis coordinates. The sealed P6 envelope
  and direct assertion forms are covered.
- **Risk:** Older fixtures or additional production phrasing without a governed
  surface coordinate will safely abstain from synthesis.
- **Safe boundary:** Do not restore label-only synthesis to preserve legacy
  behavior; missing or conflicting coordinates must remain a no-write path.
- **Return condition:** Inventory other production assertion envelopes and
  migrate affected fixtures through the real mention-detection boundary.

### EDGE-040 — Non-compiler lifecycle work can stale a new same-diff synthesis

- **Status:** `open; compiler-owned confirm path resolved`
- **Trigger:** A newly admitted composite cites an exact active member version,
  while another lifecycle operation in the same diff advances that member.
- **Current behavior:** Compiler-owned exact confirms are converted to separate
  atomic inserts, preserving both durable fates. User- or LLM-authored
  lifecycle operations retain their requested semantics and can still conflict
  with a new synthesis member.
- **Desired behavior:** The compiler should either bind the composite to the
  successor member version, schedule synthesis after the lifecycle transition,
  or reject the coupled proposal before apply. It must never report a useful
  current synthesis that becomes stale inside its own command envelope.
- **Risk:** A physically committed but non-current composite disappears from
  retrieval and cannot receive later correction or feedback.
- **Safe boundary:** Only the proven compiler-owned conflict is repaired for
  CF2. Any other same-diff lifecycle/synthesis collision fails current-visibility
  evaluation and remains in the ledger rather than triggering a broad ordering
  rewrite.
- **Return condition:** CF8 fault/lifecycle testing covers every lifecycle
  action against synthesis members and proves an explicit rebind, defer or
  fail-closed fate.
- **Evidence:** Run-5 canonical sequence and
  `test_compiled_confirm_and_synthesis_preserve_visible_composite`.

### EDGE-041 — Evaluator adapters can manufacture proof from correlation

- **Status:** `bounded P0 repair implemented; fresh-runtime validation pending`
- **Trigger:** A report adapter labels model IDs as version IDs, treats Think
  run correlation as commit identity, assumes successful batches processed
  every signal, or declares stored barrier expectations matched without
  comparing actual heads.
- **Current behavior:** The rejected draft has been replaced. The adapter now
  requires exact completed queue membership for every observation, a
  digest-valid stored barrier matching the saved artifact and prior chain,
  per-batch truth-version deltas, exact relation participants and admission,
  and a successful applied-diff envelope before assigning shared transaction
  identity. Its replay digest removes tenant/run UUIDs while preserving
  semantic multiplicity, support/predecessor topology, relation endpoints and
  commit equivalence. Focused negative tests fail closed on queue, barrier and
  envelope mismatches. Barrier heads must resolve to canonical truth versions
  owned by the exact tenant. Observation evidence must resolve to the exact
  tenant, observation ID and `occurred_at` revision, with digest and coordinate
  validation. The current checkpoint passes `34` focused tests. A
  dedicated PostgreSQL database has all `218` repository migrations applied,
  and both production-shaped PostgreSQL tests pass against it. This is focused
  evaluator and transaction-path evidence, not a completed CF2 replay or
  production-hardening claim.
- **Final P0 tightening:** Receipt Models now expose canonical
  `abstraction_level` and `claim_role`; synthesis credit requires the exact
  `composite` + `situation` shape rather than support count alone. The repaired
  seam subsequently passes `80` focused tests on the dedicated database. This
  later evidence does not rewrite the earlier `34`-test checkpoint.
- **Desired behavior:** Every reported coordinate must come from an exact
  durable row or signed artifact field. Missing transaction identity,
  per-signal processing fate, current-head match or correction provenance must
  lower coverage/score instead of being inferred.
- **Risk:** The evaluator can award synthesis, relation atomicity, barrier or
  correction credit while the product behavior is actually broken.
- **Safe boundary:** Do not use the rejected draft or claim determinism from one
  replay digest. The new adapter remains provisional until it scores a fresh
  retained CF2 tenant. Determinism intentionally remains red after one run and
  requires an independent second replay; that replay is deferred by user
  instruction.
- **Return condition:** Independent field-by-field review plus a negative test
  reproducing run 5 and showing that the stale composite receives zero current
  synthesis credit.
- **Evidence:** LOG-047, LOG-048, the P0 receipt review performed after run 5,
  `34` focused tests, and `2/2` production-shaped PostgreSQL tests on the
  dedicated fully migrated database.

### EDGE-042 — Canonical revision envelope contradicts its corrected proposition

- **Status:** `resolved for CF2 by Run 13; broader revision-transition enum remains backlog`
- **Trigger:** A compiled `revise` lifecycle operation supplies a changed
  proposition, but `advance_validated_think_model` retains the prior canonical
  `natural` text and records the active-head transition through the existing
  `confirm` enum because canonical truth has no distinct `revise` transition.
- **Current behavior:** Run 9 remains frozen false-green evidence. The bounded
  repair now versions proposition and natural text together, rebuilds the
  embedded relation mechanism from exact correction evidence, advances the
  matching canonical relation to an immutable retired successor, and retires
  its projection binding. The strengthened receipt/scorer require exact text
  coherence, successor lineage and same-envelope retirement; semantic replay
  includes both natural text and relation fate. Canonical transition labeling
  still uses the existing confirm transition, but that broader enum issue is
  not required for this repair.
- **Desired behavior:** One canonical successor version should keep natural
  text and proposition coherent and expose a durable revision transition or
  equivalent typed revision receipt without inventing a second truth source.
- **Risk:** Retrieval or reporting that privileges natural text can repeat a
  corrected belief, and lifecycle analytics can undercount genuine revisions.
- **Safe boundary:** Carry exact revised natural text through the governed
  truth-version update; retain supported-relation identity/endpoints while
  rebuilding its mechanism and evidence from the exact correction; and make
  the evaluator require proposition summary, natural text and embedded relation
  mechanism to agree. Do not widen the transition enum, redesign relation
  lifecycle, or pull unrelated backlog items onto the CF2 critical path.
- **Return condition:** Satisfied for CF2: focused compiler/applier/truth tests
  pass and Run 13 proves the corrected composite is coherent on all three
  surfaces. The deferred independent determinism replay is not part of this
  return condition.
- **Evidence:** Frozen Run 9 commit `f02df04f`, tenant
  `f8c222db-88f9-4e1d-b215-be08a36400b7`; `_memory_lifecycle_op_from_batch_decision`,
  `_compile_memory_lifecycle_update`, `_apply_claim_update`, and
  `advance_validated_think_model` source audit; `55/55` joined focused tests on
  `fyralis_cf2_core_20260718`. A fresh four-batch runtime receipt is still
  required before resolution.

### EDGE-043 — Lifecycle-only revisions can accidentally re-authorize relations

- **Status:** `bounded authorization defect resolved in Run 9; CF2 remains open for EDGE-042`
- **Trigger:** Frozen CF2 run 6 reached batch 4, then failed closed with
  `RELATION_ENDPOINT_VERSION_MISMATCH` while attempting an accepted relation
  whose endpoint versions did not match the exact current heads.
- **Current behavior:** Lifecycle-only revision decisions no longer authorize an
  accepted relation as a side effect. The compiler's forced-review disposition
  is now durable through validator auto-admission; accepted relation admission
  requires an explicit relation-bearing operation. Closed atomics also select exact
  claim-local evidence from the batch-wide evidence manifest, preventing an
  unrelated batch coordinate from becoming the mutation basis.
- **Risk:** A valid correction wave can roll back before applying its lifecycle
  update, causing correction, barrier and downstream evaluation failures.
- **Safe boundary:** Keep the repair limited to authorization and exact local
  evidence. Do not expand relation semantics, retry policy, latency work or
  general repair behavior before the next core run.
- **Return condition:** Satisfied by Run 9: batch 4 applies the revision and
  closes its exact barrier without admitting the inferred relation.
- **Evidence:** Frozen commit `48cb02741574`, tenant
  `5dab01e7-38b0-4c61-b6ce-77e555f1f2bc`; batches 1–3 succeeded and batch 4
  failed with the exact version-mismatch invariant. Run 8 at `df55e849` proved
  synthesis and relation atomicity, then exposed validator re-promotion in
  batch 4. Focused repaired-seam validation passes `48` tests on the dedicated
  database.

### EDGE-044 — Splitter-empty telemetry and stale broad assertions obscure focus

- **Status:** `backlog; not a CF2 blocker`
- **Trigger:** Run 6 exposed splitter-empty telemetry, while broad-file testing
  reported five known stale contract assertions: three in `test_llm_reason`
  and two in `compiled_candidate_scope`.
- **Current behavior:** Focused core-fast-path proofs remain authoritative for
  the bounded repair. The broad failures are classified as existing test
  contract drift, not evidence that the run-6 relation or evidence fixes fail.
- **Risk:** Noisy telemetry and stale assertions can redirect work away from the
  first executable company-learning loop.
- **Safe boundary:** Record both without expanding the current runtime repair.
- **Return condition:** Reconcile splitter-empty observability and refresh the
  broad assertions in a dedicated test-contract cleanup after the core path.
- **Evidence:** Run-6 postmortem and focused validation at the frozen checkpoint.

### EDGE-045 — Post-compiler lifecycle pressure can stale a new composite member

- **Status:** `bounded same-diff defect resolved in Run 9; CF2 remains open for EDGE-042`
- **Trigger:** Run 7 created the exact batch-3 composite and relation, then a
  representation-contract `unchanged` operation advanced one member head later
  in the same diff.
- **Current behavior:** Lifecycle-pressure target selection excludes every
  Model referenced by a newly inserted same-diff composite. It selects another
  eligible Model or emits no maintenance operation.
- **Risk:** A valid composite disappears from accepted-current truth
  immediately, so later evidence cannot revise it and relation atomicity cannot
  be proven.
- **Safe boundary:** Preserve the narrow selection exclusion. Do not redesign
  lifecycle policy or add broad repair machinery before the core rerun.
- **Return condition:** Run 9 proves the exact composite remains current and is
  versioned in batch 4, closing this same-diff ordering defect. Its separate
  revision-envelope contradiction is tracked only in EDGE-042.
- **Evidence:** Frozen Run 7 tenant
  `c907278e-0ef4-42be-a462-9c9f2a359b33`; focused repaired seam `47/47` green.

### EDGE-046 — Semantic canonicalization re-promotes explicit retirement

- **Status:** `resolved for CF2 by Run 13`
- **Trigger:** Runs 10–12 failed at batch 4. Run 12 tenant
  `fa3f367f-a95e-4ad4-a0ce-e664a56daac0` failed after `26.631s` before the fix.
- **Current behavior:** `_canonicalize_relation_claim_semantics` transformed the
  explicit correction retirement from `retired`/`no_edge` into
  `accepted`/`accepted_edge`. The concurrent `weakens`/`needs_review` operation
  used different endpoints and was unrelated.
- **Desired behavior:** Explicit retirement status and no-edge policy survive
  semantic canonicalization unchanged and reach canonical retirement apply.
- **Risk:** A valid correction cannot commit, although canonical truth remains
  protected. Weakening the immutable projection trigger would risk partial or
  contradictory relation truth.
- **Safe boundary:** Guard explicit retirement in validation; retain compiler
  conflict folds only as defense-in-depth and preserve immutable-edge
  enforcement.
- **Return condition:** Satisfied: Run 13 completed a fresh clean zero-seed
  four-batch execution with canonical score, relation retirement and relation
  atomicity green.
- **Evidence:** Run 13, commit `27e37b5e`, tenant
  `2a14a6bf-fe59-4efd-a52d-ad7ffcfa7d30`; evaluator historical-instance fix;
  focused evaluator suite `14/14` green. Determinism remains outside this
  closure and unproven at `replay_count=1`.

### EDGE-047 — Provider model requires a newer Codex CLI

- **Status:** `resolved by supported-model attempt; superseded by EDGE-048`
- **Trigger:** CF3-A one-batch attempt at commit `f869dd82`, tenant
  `50270994-753d-465f-b87e-7d794cf2d3a7`, used `gpt-5.6-terra`.
- **Current behavior:** The installed Codex CLI rejected every provider attempt
  before semantic output because the model requires a newer version. Batch 1
  ended after `124.181s` with zero accepted Models.
- **Desired behavior:** Verify a model supported by the installed transport
  before beginning the bounded semantic run.
- **Risk:** Compatibility failure can be mistaken for company-learning quality
  failure despite producing no semantic evidence.
- **Safe boundary:** Preserve the artifact as infrastructure evidence only; do
  not score zero Models as semantic behavior or advance CF3-A.
- **Return condition:** Retry the same one-batch rung with an explicitly
  supported model and obtain semantic output suitable for evaluation.
- **Evidence:** `/tmp/fyralis-cf3a-codex-one-batch.json`.

### EDGE-048 — Extracted mentions can lack a grounding fate

- **Status:** `resolved by receipted CF3-A rerun; CF3-B unlocked`
- **Trigger:** Supported-model CF3-A attempt at commit `8b027197`, tenant
  `97b210f5-28c9-4206-b8a1-9c1f25335809`.
- **Current behavior:** The original attempt emitted 25 signal fates but left
  two detected mentions without downstream grounding disposition. The
  receipted rerun now gives all `24/24` detected mentions explicit trace fates
  and drains truth-critical work to zero before barrier closure.
- **Desired behavior:** Every extracted mention receives an explicit governed
  fate, including unresolved, rejected or intentionally ignored outcomes.
- **Risk:** A mention can disappear between extraction and grounding without an
  auditable disposition, making company-physics coverage unknowable.
- **Safe boundary:** Evidence exposes detection fate separately from nullable
  grounding fate and fails incomplete grounding continuity. CF3-B is unlocked,
  while full-P6 scorer fields missing from a one-batch prefix remain outside
  the CF3-A verdict.
- **Return condition:** Satisfied at commit `e7de1c3a`: a fresh bounded rung passes
  `complete_detected_mention_grounding_continuity`: both exact mentions receive
  durable explicit grounding dispositions, with no regression across all 24.
- **Evidence:** `/tmp/fyralis-cf3a-codex-one-batch-spark-evidence.json` and
  `/tmp/fyralis-cf3a-codex-one-batch-spark-score.json`.
  Evaluator separation and continuity gate are focused-test green `44/44`.
  Receipted rerun tenant `08d19975-2c39-4fef-a820-27d29c30fd9b` processed 25
  signals, produced `24/24` trace fates, drained `27 -> 3 -> 0`, and closed its
  barrier at zero. See `/tmp/fyralis-cf3a-codex-one-batch-spark-receipted.json`
  and its `-evidence.json` and `-score.json` companions.

### EDGE-049 — Barrier count can race a concurrent grounding enqueue

- **Status:** `deferred; must close before concurrency proof`
- **Trigger:** Review of the CF3-A grounding enqueue and truth-critical barrier
  accounting path.
- **Current behavior:** Barrier completion reads the tenant's pending count and
  later writes its receipt under the default transaction isolation. A
  concurrent detected-mention enqueue can commit after the zero count without
  sharing a tenant-scoped lock with barrier closure.
- **Desired behavior:** Grounding enqueue and barrier truth snapshot serialize
  on one tenant-scoped transaction fence, so a receipt can never close over
  concurrently created truth-critical work.
- **Risk:** A barrier can certify zero pending work while a detected mention is
  already becoming durable outside its snapshot.
- **Safe boundary:** Do not claim concurrent barrier atomicity from the bounded
  single-run proof. Preserve the new pending-work gate.
- **Return condition:** A two-connection race test proves enqueue either commits
  before and blocks closure, or commits after a fully fenced receipt boundary.
- **Evidence:** Review of
  `services/evaluation/epistemic_repair/p6_think_runner.py` and
  `services/domain/company_learning/barrier.py`.

### EDGE-050 — Existing detected heads do not backfill missing grounding work

- **Status:** `deferred; legacy repair required`
- **Trigger:** Review against the two fate-less detections preserved by the
  supported CF3-A tenant.
- **Current behavior:** Mention-fate replay stops when a detection head already
  exists. It does not idempotently ensure grounding work for an existing
  `detected` head, so legacy rows or a partially committed caller can remain
  stranded.
- **Desired behavior:** Replay ensures one current grounding work identity for
  every current detected head, regardless of whether detection was newly
  written or already durable.
- **Risk:** Deploying the enqueue repair protects new detections but does not
  repair existing fate gaps.
- **Safe boundary:** Keep historical tenants red; do not infer backfill from
  future-path unit tests.
- **Return condition:** A bounded backfill/replay test begins with a detected
  head and no work item, then creates exactly one idempotent pending item.
- **Evidence:** Supported CF3-A tenant
  `97b210f5-28c9-4206-b8a1-9c1f25335809` and
  `services/domain/entity_grounding/mention_fates.py`.

### EDGE-051 — Superseded detections can reuse stale grounding work

- **Status:** `deferred; versioned work identity required`
- **Trigger:** Idempotency review of grounding work identity and barrier
  correlation.
- **Current behavior:** Enqueue fixes `processing_generation=1` and ignores a
  uniqueness conflict on tenant, observation, phrase and generation. Barrier
  accounting treats any work row for the phrase as ownership, without proving
  it belongs to the current detection version.
- **Desired behavior:** Each current detection version is correlated to its own
  current grounding work or terminal trace; repeated delivery of the same
  version remains idempotent.
- **Risk:** A terminal row for an older detection can hide an ungrounded
  superseding detection and permit false barrier closure.
- **Safe boundary:** Do not exercise supersession in CF3-A; retain it as a
  required fault/concurrency case rather than broadening the current repair.
- **Return condition:** Tests prove old-terminal/new-detected remains pending,
  while replaying the same detection version creates no duplicate work.
- **Evidence:** Review of `services/domain/entity_grounding/repo.py` and
  truth-critical accounting in
  `services/evaluation/epistemic_repair/p6_think_runner.py`.

### EDGE-052 — Founder-bootstrap quality and staleness are not yet evaluated

- **Status:** `bounded implementation; product qualification deferred`
- **Trigger:** Cold-start review after CF3-B produced zero Models without
  canonical entity seed state.
- **Current behavior:** A governed founder manifest can now establish exact
  canonical names and aliases without seeding behavioral truth. A unique,
  active exact match grounds synchronously before Think; unknown or conflicting
  names retain the zero-seed fallback. Product-level quality and staleness are
  not yet measured.
- **Desired behavior:** Founders can establish a vivid, versioned initial
  entity and relationship map; the runtime resolves ordinary signals against
  it while detecting and safely routing novel or stale concepts.
- **Risk:** Treating zero-seed inference as the normal product path can make
  early company understanding slower and less useful. Treating founder input
  as permanent truth can instead freeze mistakes and stale structure.
- **Safe boundary:** Keep onboarding identity-only and versioned; do not add a
  broad UI, behavioral seeds, or automatic cross-mention merging during CF3-B.
- **Return condition:** After the core reuse proof, preregister a bootstrap
  simulation and compare time-to-useful-model, resolution accuracy, correction
  behavior, and novel-entity handling against zero seed.
- **Evidence:** LOG-061, LOG-062, LOG-064, `1fc88737`, and `9af2275a`.

### EDGE-053 — Mechanical Model references can overstate semantic memory use

- **Status:** `resolved as evaluator-integrity defect; fresh run correctly red`
- **Trigger:** The completed real two-batch run at commit `e8bbe033`.
- **Current behavior:** Context-use telemetry reports `model_context_used` when
  a generic lifecycle obligation references a selected Model, even when the
  provider reasoning trace bases all semantic decisions on direct observations.
- **Desired behavior:** Material-use credit requires a decision-level trace or
  output whose conclusion, confidence, correction, or selected action depends
  on prior accepted Model content. Maintenance bookkeeping is reported
  separately.
- **Risk:** Retrieval presence and generic lifecycle maintenance can create a
  false-green autonomous-learning claim.
- **Safe boundary:** Evaluator v2 rejects lifecycle-only references. The fresh
  founder-assisted run selected 14 prior versions and referenced two but still
  received zero material-use credit because its trace referenced none.
- **Return condition:** Reopen only if a future evaluator awards material-use
  credit without decision-level evidence.
- **Evidence:** LOG-063, LOG-065,
  `/tmp/fyralis-cf3b-codex-two-batch-spark-r2.json`, and
  `/tmp/fyralis-cf3b-founder-two-batch-spark-r1-cf3b-v1.json`.

### EDGE-054 — Founder alias lookup is query-per-phrase

- **Status:** `backlog; scale optimization after core proof`
- **Current behavior:** Pre-Think grounding performs one exact governed-alias
  lookup per unique mention phrase.
- **Desired behavior:** Bulk-load the batch's exact aliases while preserving
  tenant, valid-time, conflict, and authority fail-closed behavior.
- **Safe boundary:** Keep the simple auditable query on the current small-batch
  path; measure before optimizing.
- **Return condition:** Profiling shows alias lookup materially affects batch
  latency or target batch sizes make query count operationally significant.

### EDGE-055 — Generic founder referents lack a complete identity lifecycle

- **Status:** `backlog; not required for first learning-loop proof`
- **Current behavior:** Bootstrap can bind typed generic referents and versioned
  exact aliases, but rename, split, merge, replacement, and retirement are not
  yet one closed lifecycle for those referents.
- **Desired behavior:** Founder corrections preserve immutable provenance and
  redirect or supersede affected aliases, grounding, and learned scope without
  rewriting history.
- **Safe boundary:** Treat the v1 manifest as initial identity authority and
  fail closed on active conflicts; do not broaden identity mutation during
  CF3-B.
- **Return condition:** Before production founder onboarding or any correction
  simulation that changes canonical identity.

### EDGE-056 — Wider worker lane has three failures outside founder grounding

- **Status:** `backlog; classify separately from focused proof`
- **Current behavior:** The focused bootstrap/evaluator/pre-Think grounding
  slices pass. The wider sweep observed:
  `test_clarification_adjudication_changes_future_grounding_fate` expected
  `company_learning.status=insufficient` but received `contradicted`;
  `test_llm_timeout_is_requeued` depleted its scripted provider response during
  retry; and `test_review_queue_tenant_isolated` lacked the other tenant needed
  by a foreign key. None exercises the new deterministic founder alias branch.
- **Desired behavior:** The full wider worker lane is green and each failure has
  an independently reproduced cause.
- **Safe boundary:** Do not claim broad worker-suite health or call these
  preexisting/flaky without investigation, but do not expand the core CF3-B
  repair until a failure is shown to share its cause.
- **Return condition:** After the fresh CF3-B core proof, reproduce and triage
  the three failures independently; promote only a causally related failure.

### EDGE-057 — Retrieved exact Models do not yet have a required semantic effect

- **Status:** `P0; current CF3-B blocker`
- **Trigger:** Founder-assisted CF3-B tenant
  `cb3a8a53-5222-4b31-90ee-f86bf1b68589` selected all 14 exact batch-1 Model
  versions in batch 2 and referenced two durably, while its provider trace
  referenced and materially used zero.
- **Current behavior:** Founder grounding and retrieval establish the right
  canonical scope and deliver prior Models, but the join from exact scope into
  prompt obligations and validated application does not require those Models
  to affect a decision.
- **Desired behavior:** For a scope with relevant prior truth, the runtime
  records the exact selected version, exposes a scope-matched comparison to the
  provider, and validates a decision-level effect on conclusion, confidence,
  correction, or lifecycle action.
- **Risk:** The system can look memory-aware while independently re-deriving
  each batch from observations, so it stores company facts without learning
  across time.
- **Safe boundary:** Keep evaluator v2 strict and CF3-C locked. Do not inflate
  retrieval, add identity seeds, or count durable references as material use.
- **Return condition:** A fresh two-batch run has a non-empty trace reference
  and evaluator-accepted material effect tied to an exact batch-1 Model
  version, with both barriers still complete and pending zero.
- **Evidence:** LOG-065 and
  `/tmp/fyralis-cf3b-founder-two-batch-spark-r1-cf3b-v1.json`.

### EDGE-058 — Exact-envelope deduplication has unresolved temporal semantics

- **Status:** `backlog; explicitly deferred from the CF3-B fix`
- **Trigger:** Audit observed repeated exact evidence envelopes while tracing
  why batch 2 did not materially use prior Models.
- **Current behavior:** Repeated exact envelopes can reach reasoning as distinct
  observations; no general temporal dedup rule decides whether they are
  duplicates, corroboration, continued state, or a later recurrence.
- **Desired behavior:** Any future compaction preserves event time, validity,
  recurrence, source independence, and evidence multiplicity while removing
  only semantically redundant envelopes.
- **Risk:** Eager string/envelope dedup can erase corroboration or temporal
  change; retaining everything can increase prompt cost and obscure the most
  useful prior/new evidence comparison.
- **Safe boundary:** Retain the evidence and solve the exact-scope effect
  contract first. Do not use deduplication to manufacture CF3-B material use.
- **Return condition:** After CF3-B is green, define temporal equivalence and
  add evaluator cases for duplicate, corroborating, continuing, and recurring
  observations before changing runtime compaction.
- **Evidence:** LOG-065 and the founder-assisted CF3-B run artifacts.

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
# Joined-runtime evidence integrity

- **Aggregate edge-state false positive (resolved in v2):** integrated v1
  treated `total_edges > active_edges` as proof that the intended correction
  fenced its cross-stage relation, even though the correction report exposed
  zero fenced relations. V2 captures the exact edge ID before correction and
  requires its own state transition plus archived-source, dependent-target and
  reevaluation-pair linkage. V1 remains historical and must not be used as the
  governing joined-runtime artifact.
