# Read Authority Remaining Work

Date: 2026-06-24

This file is the resume checklist for finishing the authority-based read plane
after the first Ask, model trace, debug, Today, recommendation, provenance, and
labeling slices.

The invariant remains:

```text
principal + purpose + object/provenance -> authorized view
```

Read authority is incomplete until an actor cannot learn unauthorized company
state through raw rows, Models, summaries, Ask evidence, cached cards,
projections, realtime events, exports, debug surfaces, or prompt attacks.

## Current Completed Baseline

- Core authority primitives exist in
  `services/platform/access_control/authority.py`.
- Authority schema exists through migrations `0171` through `0175`.
- Models, resources, observations, state-change observations, and persisted Ask
  evidence now get provenance and/or labels on the paths implemented so far.
- `/v1/ask` filters retrieved Models, Observations, projected evidence, omitted
  evidence, evidence expansion, and accepted-answer writeback through live
  authority.
- Ask sessions/scopes/answers persist compact authority snapshots.
- Query prefetch cache keys include authority fingerprints.
- Model trace, raw debug observation/model/act reads, legacy Today,
  `/v1/recommendations`, and active v2 `/today` have first-pass authority gates.

## Highest Priority Remaining Work

### 1. Projection Authority Coverage

Projection artifacts still need first-class object refs, provenance, labels, and
read checks.

- Identify all product and reasoning projection tables/read models.
- For each projection artifact, record source refs into
  `object_provenance_edges`.
- Copy inherited labels into `object_access_labels`.
- Authorize projection reads through `authorize_read(..., purpose=<surface>)`.
- Fail closed for projection rows with unknown provenance until explicitly
  declassified.
- Add tests proving unauthorized source facts do not appear through projection
  summaries or projection-backed evidence.

### 2. Cache Authority Coverage

Some caches are fingerprinted; others still need durable object refs or
authority-aware invalidation.

- Inventory cache tables and in-memory cache keys used by Ask, Query, Today,
  recommendations, model trace, and cards.
- Add authority fingerprints to all user-facing cache keys.
- Add object refs and provenance for cached artifacts that can be replayed or
  inspected later.
- Re-check live authority before returning any cached payload that contains
  derived company state.
- Increment or observe grant epochs on grant/revocation paths so stale cache
  hits miss after authority changes.
- Add tests for two actors with different grants receiving different cache
  entries and for revocation invalidating future reads.

### 3. Retrieval Authority-Safe Candidate Selection

Current product surfaces filter many results after retrieval. The retrieval
layer itself still needs to avoid selecting unauthorized candidates in the first
place.

- Thread `Principal` and `Purpose` through retrieval entrypoints.
- Constrain lexical, semantic, temporal, graph, SAGE, and second-pass retrieval
  to authorized candidates.
- Ensure retrieval telemetry counts denied candidates without exposing object ids
  or content to the user.
- Preserve Think context-use telemetry while adding authority filters.
- Add tests where unauthorized high-ranking evidence exists and must never
  enter retrieval packets, prompts, summaries, or validation context.

### 4. Raw Substrate Read Paths

Remaining raw substrate/list/detail routes need to move onto the authority API.

- Inventory gateway and product routes that list observations, resources,
  commitments, goals, decisions, Models, edges, candidates, and topology events.
- Require authenticated actor context for human-facing reads.
- Use `authorize_read` or an `AuthorizedReader` equivalent before returning row
  contents, counts, relationship edges, or existence-sensitive 404/403 behavior.
- Add tests for list filtering, direct unauthorized reads returning non-leaky
  responses, and cross-tenant denial.

### 5. Decision Delta And Evidence Gaps

The v2 Today path has first-pass authority checks, but some delta shapes still
lack durable provenance.

- Record provenance and inherited labels for decision-delta evidence rows.
- Record provenance for direct/unbacked decision deltas.
- Ensure summary counts, next-delta selection, detail, evidence, delegate,
  correct, and apply flows all use the same authority refs.
- Add tests for unsupported/unbacked deltas failing closed unless explicitly
  declassified or backed by authorized source refs.

### 6. Realtime Delivery

Realtime must enforce the same read authority as pull-based APIs.

- Attach principal, purpose, and tenant context to subscriptions.
- Authorize each event payload before delivery.
- Drop unauthorized events without leaking restricted object ids/content.
- Apply grant/revocation changes to future delivery.
- Add tests for authorized subscriber delivery, unauthorized subscriber drop, and
  revocation before the next event.

### 7. Export Paths

Exports are high-risk because they package many facts into durable artifacts.

- Inventory all export/report/download routes and background jobs.
- Require explicit `purpose="export"` authority.
- Authorize every row or derived object before adding it to an export.
- Persist export provenance and authority snapshot/fingerprint.
- Deny or redact mixed-authority exports when policy cannot safely split output.
- Add tests for restricted finance/HR/legal facts excluded from unauthorized
  exports and for delegated export-only grants.

### 8. Debug Cache And Operator Surfaces

Debug routes have first-pass object checks, but cache rows need finer object
authority.

- Add stable object refs/provenance to debug cache rows that contain company
  state.
- Replace broad admin/leadership-only fallback where per-object checks become
  possible.
- Keep production debug behavior locked down.
- Add tests proving debug cache payloads cannot be read with tenant headers
  alone and cannot bypass per-object authority.

### 9. Ask Replay And Read Semantics

There is no active answer replay endpoint in the current Ask API, but the rule
should be explicit before one is added.

- If an Ask answer read/replay endpoint is introduced, require live viewer
  authority at replay time.
- Re-check persisted answer evidence and derived answer provenance before
  returning answer payloads.
- Decide whether stale answers should be denied, redacted, or returned only as
  audit metadata.
- Add tests for revocation after original Ask answer creation and before replay.

### 10. Delegation Workflow And Revocation Hardening

The primitives exist, but the user-facing workflow and invalidation story are
not complete.

- Build request/approve/deny/revoke flows for object, label, and scope grants.
- Resolve eligible grantors from actual authority, not hardcoded product roles.
- Store expiry, purpose, scope, reason, grantor, grantee, and audit trail.
- Increment grant epoch on grant/revocation and make cache/session behavior use
  that epoch consistently.
- Add tests for non-authoritative grantor rejection, expiry, purpose mismatch,
  revocation, and audit capture.

### 11. Runtime Database Validation

Most current evidence is unit/static. The migrations and backfills need database
proof.

- Apply migrations `0171` through `0175` against a representative local or test
  Postgres database.
- Run schema drift checks after applying migrations.
- Validate backfills are idempotent.
- Validate dense-tenant label/provenance queries use the intended indexes.
- Run a small production-shaped read-authority scenario covering finance, HR,
  legal, engineering, Ask, Today, model trace, and cache reads.

## Objective Completion Tests

The remaining work is complete only when these scenarios pass across targeted
unit, integration, and selected end-to-end lanes:

- Unauthorized finance, HR, legal, incident, executive, and board state is hidden
  from actors without matching role, grant, ownership, or manager authority.
- Derived Models, projections, caches, Ask evidence, card summaries, exports,
  and realtime events inherit the strictest source authority unless explicitly
  declassified.
- Unauthorized material never enters Ask retrieval packets, prompts, answer
  payloads, persisted evidence expansion, answer replay, or accepted-answer
  writeback.
- Cache hits are scoped by tenant, actor, purpose, grant epoch, role set, scope,
  and policy version.
- Grant and revocation changes affect future reads, cache hits, realtime
  delivery, evidence expansion, and replay paths immediately.
- Direct object reads and list reads do not leak restricted object existence
  through ids, counts, summaries, errors, ordering, or relationship edges.
- Debug and export surfaces require purpose-specific authority and never rely on
  tenant headers alone.
- Cross-tenant reads deny everywhere, including delegated and override paths.
- Runtime migrations and backfills are idempotent and schema-drift clean.

## Recommended Next Slice

Do projection and cache coverage next.

Those surfaces are the most likely remaining laundering paths because they can
store summarized company state and replay it outside the original retrieval
moment. The clean shape is:

```text
source rows -> provenance edges -> inherited labels -> authority fingerprint
-> live read check before serving
```
