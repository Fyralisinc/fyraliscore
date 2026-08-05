# Fyralis Episode Creation Subsystem — Three-Phase Implementation Plan

## Goal

Turn identity-grounded observations from all connector sources into explainable,
cross-source episodes and immutable snapshots suitable for coherent reasoning.

## Phase 1 — Topics, routing, and membership

Status: **Completed** in Phase 1.

Deliverables:

- Durable automatic, query-seeded, and human-pinned topic intents.
- Durable episode identities and explicit topic-equivalence decisions.
- A source-agnostic signal assembler over observations, identity snapshots,
  perception claims, source structure, and lexical keyphrases.
- Deterministic candidate retrieval and explainable include/hold/exclude scoring.
- Append-only membership assertions with evidence, identity, claim, router, and
  feature provenance.
- Multi-membership, replay idempotency, tenant/install isolation, and tests.

Exit criteria:

- Slack, Notion, Jira, meeting, and other observations use one routing path.
- A shared stable anchor can connect observations from different sources.
- Lexical similarity alone cannot collapse conflicting stable anchors.
- Replay creates no duplicate topics, episodes, or memberships.
- Every membership decision has exact evidence and structured reasons.

## Phase 2 — Lifecycle, access, contradictions, and snapshots

Status: **Completed** in Phase 2.

Deliverables:

- Append-only lifecycle events for open, dormant, settled, reopened,
  superseded, split, and merge transitions.
- Event-time and ingestion-time watermarks with late-evidence reopening.
- Versioned settlement policies for quiet period, explicit close,
  query-scope completion, and supersession.
- Contradiction materialization from included evidence-bound claims.
- Conservative access-policy intersection.
- Immutable, content-addressed episode snapshots and snapshot history/diff APIs.
- Query topic creation and requester-authorized historical construction.

Exit criteria:

- Old snapshots survive edits, deletions, corrections, and late evidence.
- Settled snapshots name their policy, watermarks, coverage, contradictions,
  citations, and exact evidence.
- Unknown ACLs are non-shareable and restricted inputs only narrow access.
- Query and automatic topics converge only through explicit equivalence.

## Phase 3 — Durable construction and reasoning handoff

Status: **Completed** in Phase 3.

Deliverables:

- Lease/retry/dead-letter constructor worker consuming perception outbox v2.
- Atomic membership/lifecycle/snapshot writes and intake completion.
- Durable, idempotent `episode.snapshot_settled` reasoning outbox.
- Structured reasoning input reader constrained to the snapshot manifest.
- Read service for episode heads, history, memberships, explanations,
  contradictions, citations, and snapshot replay.
- Dependency-driven reconstruction after identity corrections.
- Cross-source Alpen Audit Week end-to-end scenario and implementation report.

Exit criteria:

- Every completed intake row has durable membership decisions.
- Settled episode snapshots reach reasoning exactly once per snapshot hash.
- Exact input replay produces the same membership decisions and snapshot
  content apart from explicitly versioned lifecycle timestamps.
- Cross-source evidence, contradictions, unresolved identity, and ACL boundaries
  remain visible through the complete flow.

## Non-goals

- Resolving organizational contradictions into a single company belief.
- Implementing the downstream reasoning model.
- Claiming production canary or general-cutover results without production
  traffic and an explicit rollout decision.
