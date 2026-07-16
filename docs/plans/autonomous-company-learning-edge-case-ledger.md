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

- **Status:** `bounded`
- **Current behavior:** Context selection adapts Observation rows and synthetic
  revision references.
- **Missing behavior:** Canonical create/edit/delete/reaction/tombstone history
  with exact source-native revisions.
- **Safe boundary:** Do not claim edit/delete replay or retrospective source
  fidelity beyond the Observation adapter.
- **Return condition:** Implement before edit/delete/reaction correction
  closure is included in the pilot operating envelope.

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

- **Status:** `open`
- **Current behavior:** Context is channel/thread/cutoff bounded.
- **Missing behavior:** Governed recurrence across long gaps, channels, and
  authorized source boundaries.
- **Safe boundary:** Missing context lowers confidence or produces unresolved
  fate; the resolver must not bridge boundaries speculatively.
- **Return condition:** Add during source-breadth expansion after the thin
  learning loop.

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

### EDGE-009 — Original grounding is not yet superseded after adjudication

- **Status:** `active`
- **Current behavior:** An accepted clarification records governed corrective
  memory and improves later occurrences.
- **Missing behavior:** The original review grounding, no-admission source
  semantics and any dependent Model do not yet receive an N+1 successor,
  retraction or repair fate.
- **Safe boundary:** Do not claim correction closure; report the first result as
  historically reviewed and the later improvement as future corrective memory.
- **Return condition:** Implement grounding/source-semantic successor
  generations and dependent Model repair before policy-learning promotion.

### EDGE-010 — Clarification compatibility mutates observation annotations

- **Status:** `open`
- **Current behavior:** Legacy clarification finalization appends the chosen
  entity to `observations.entities_mentioned` and emits an authoritative state
  change.
- **Risk:** A later consumer may confuse human adjudication with a source-native
  mention, while the original grounding trace says the source observation was
  not mutated.
- **Safe boundary:** Treat `entities_mentioned` as a compatibility annotation,
  never as independent source evidence. Grounding lineage remains authoritative.
- **Return condition:** Replace the mutation with a versioned correction/
  annotation projection when successor grounding is implemented.

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
