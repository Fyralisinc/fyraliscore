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
  remain unproven. Jira and Linear structured identity transport is now proven,
  but that is narrower than conversational reconstruction.
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
  until they transport and persist the same authenticated binding evidence.

### EDGE-016 — Rename and lifecycle changes cannot be aliases

- **Status:** `resolved` for customer rename/archive/name-reuse; broader
  lifecycle remains `active`
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
- **Assurance boundary:** Assurance v5 reopens and recomputes the lifecycle
  artifact and blocks `working` on any unsupported case, metric below `1.0`,
  overlap, mutation, tenant leak or replay divergence.
- **Return condition:** Prove rename continuity, archive/name reuse and
  stale-alias rejection first — complete for customers. Reopen for merge,
  split, replacement, resurrection, non-customer identity lifecycle and
  `SourceIdentityBinding` rebind/revocation through canonical lifecycle writers
  with correction closure.

### EDGE-017 — Structured source claims cannot create identity authority

- **Status:** `resolved` for Jira project and Linear project/team identity
  surfaces; broader connector coverage remains `active`
- **Trigger:** An authenticated structured source contains a stable object ID
  and a human-readable key/name that may refer to a canonical company entity.
- **Risk:** Treating handler JSON or matching text as authority can fabricate
  identity, leak a binding across sources or apply one observation-level
  binding to unrelated phrases.
- **Safe boundary:** A handler may emit only a typed, source-namespaced claim.
  Ingestion may attach it only to one pre-existing governed binding, one exact
  normalized source surface and a matching source system. Missing or ambiguous
  bindings fail closed; free text never creates authority.
- **Current evidence:** Jira project and Linear project/team claims survive
  inline and Kafka normalization and attach atomically to the durable
  Observation. Real-Postgres tests prove exact resolver consumption, event-time
  liveness, forged-text rejection, missing-binding inertness and cross-source
  isolation.
- **Return condition:** Extend the same contract to email, documents, meetings
  and remaining connectors; add mention-specific multi-object populations and
  causal learning proofs per source.

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
  route-specific causal attribution, retention/regression evaluation and
  production-scale read-cost proof.

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
