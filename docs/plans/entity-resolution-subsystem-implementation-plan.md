# Entity Resolution Subsystem — Three-Phase Implementation Plan

## Objective

Deliver a source-grounded, explainable, versioned entity-resolution subsystem
that sits durably between observation creation and episode intake.

## Phase 1 — Source-grounded foundations

Status: **Completed** in `912dddb5`.

Deliverables:

- Consolidated architecture and constitutional invariants.
- Capability registry covering every current source-contract connector.
- Explicit admission policy separating source references, canonical entities,
  conditional entities, and contextual referents.
- Tenant/install-scoped source-reference registry.
- Provenance-bound mention registry.
- Reproducible resolver-run ledger.
- RLS, composite tenant/evidence constraints, deterministic keys, and tests.

Exit criteria:

- Connector catalog and capability registry are set-equal.
- Replaying a source reference or mention is idempotent.
- Same native ID in different installations cannot collide.
- Every non-query mention is tied to exact evidence.

## Phase 2 — Resolution and immutable identity snapshots

Status: **Completed** in `17a625ea`.

Deliverables:

- Typed candidate and feature contracts.
- Deterministic source-reference and principal-mapping candidates.
- Alias and supplied-context candidate providers.
- Must-link/cannot-link constraint engine.
- Explainable, deterministic scoring and type-specific decision policy.
- Candidate, constraint, assertion, and snapshot persistence.
- Assertion semantics for `refers_to`, `same_as`, `not_same_as`, `represents`,
  `part_of`, and `version_of`.
- Unit and PostgreSQL integration tests for resolved, ambiguous, negative, and
  unresolved outcomes.

Exit criteria:

- Every decision is reproducible from persisted features and versions.
- Hard negatives cannot be overridden by scores.
- Ambiguous candidates remain visible.
- Snapshots are immutable and content-addressed.

## Phase 3 — Orchestration, lifecycle, and episode handoff

Status: **Completed** on the feature branch; see the final implementation report.

Deliverables:

- Transactional `observation.ready_for_identity` outbox.
- Resolver worker with leasing, retries, dead-letter behavior, and atomic
  episode handoff.
- Ingestion and summarization cutover from direct episode intake to identity
  intake.
- Identity snapshot attached to episode intake contract v2.
- Targeted re-resolution requests based on assertion dependents.
- Access-aware, non-mutating query-resolution path.
- Audit-week fixture demonstrating cross-source source references, a mapped
  person, an ambiguous name, a contextual audit referent, and episode handoff.
- Final implementation report and final regression evidence.

Exit criteria:

- No normal observation reaches episode intake without an identity snapshot.
- Ambiguous or unresolved mentions do not stall episode construction.
- Corrections request targeted re-evaluation without rewriting history.
- Query resolution cannot expose inaccessible supporting evidence.
- The audit-week scenario is deterministic under replay.
