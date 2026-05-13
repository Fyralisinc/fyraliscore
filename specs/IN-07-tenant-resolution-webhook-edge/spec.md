# Feature Specification: IN-07 — Tenant Resolution at Webhook Edge

**Feature Branch**: `feat/IN-07-tenant-resolution-webhook-edge`
**Created**: 2026-05-13
**Status**: Draft
**Input**: Source description: `specs/IN-07-tenant-resolution-webhook-edge/source.md`

## Clarifications

### Session 2026-05-13

- Q: Can `secret_ref` be updated on an existing `provider_installations` row, or is it write-once? → A: Updatable via a dedicated admin action — register / disable / re-enable / update-secret-ref are all supported, preserving the row's identity and `installed_at`.

## Context & Substrate Alignment

This feature is **plumbing**, not substrate. The Universal Flow Rule
(`input → Observation → Think → Models → Acts / Resources`,
Constitution §I) does **not** apply directly to this feature because
nothing here produces a Model, an Act, or a Resource. What it does:
takes an unauthenticated webhook hitting the gateway, looks at the
provider-native identifier carried in the payload, and tells the
downstream ingestion pipeline which Company OS tenant the request
belongs to. The Observation rows produced *downstream* (by IN-06)
will carry the resolved `tenant_id` and continue to obey the flow
rule.

The new table `provider_installations` is a **per-feature side
table for a cross-cutting concern** — exactly the case the
constitution explicitly permits: *"Per-feature side tables for
cross-cutting concerns (cache, queue, audit, sidecar) are allowed
and encouraged — they are not new foundations."* (§I).

However, the table **is tenant-scoped**, so Constitution §III
applies in full: `tenant_id` FK + RLS + tenant-prefixed indexes are
non-negotiable. The plan phase MUST satisfy that bar; the spec
records it here so it does not get lost.

**Change boundary** — files in scope, copied verbatim from `source.md`
(any expansion is a user decision, not the spec's):

- `services/webhooks/tenant_resolver.py` (new)
- `db/migrations/NNNN_provider_installations.sql` (new — number resolved at plan time; see Assumption A1)

The spec is dependency-aware of IN-06 (Webhook Gateway Router) — that
feature is the only intended consumer of this resolver — but the
resolver MUST be independently testable and shippable.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — A Slack webhook from Acme Corp routes to Acme's tenant (Priority: P1)

A customer ("Acme Corp") installs the Company OS Slack app into
their workspace via the OAuth flow. From that moment on, every Slack
event their workspace generates (messages, reactions, app mentions)
arrives at our webhook ingress unauthenticated-by-bearer-token,
identified only by the Slack-internal `team_id`. The system uses the
new mapping to determine that `team_id = T_ACME_123` belongs to
Acme Corp's Company OS tenant, and the rest of the pipeline runs
under Acme's tenant context.

**Why this priority**: This is the entire reason the feature exists.
Without it, no real Slack-sourced webhook can be ingested by Company
OS — the gateway has no way to attribute the event to a tenant.

**Independent Test**: With a `provider_installations` row inserted
mapping `(slack, T_ACME_123) → acme_tenant_id`, calling the resolver
with a Slack-shaped payload containing `"team_id": "T_ACME_123"`
returns `acme_tenant_id`. With **no** row inserted, calling the same
resolver yields the structured "unknown installation" result that
the router translates to 401.

**Acceptance Scenarios**:

1. **Given** an installation row exists for `(slack, T_ACME_123)
   → acme_tenant_id` and is `enabled=true`, **When** the resolver
   is invoked with a Slack payload carrying `team_id=T_ACME_123`,
   **Then** it returns `acme_tenant_id`.
2. **Given** no installation row exists for `(slack, T_UNKNOWN_999)`,
   **When** the resolver is invoked with that payload, **Then** it
   returns the "unknown installation" outcome (downstream router
   converts this to HTTP 401, never 404).
3. **Given** an installation row exists for `(slack, T_ACME_123)`
   but is `enabled=false`, **When** the resolver is invoked,
   **Then** it returns the same "unknown installation" outcome as
   case 2 — the disabled state is not externally distinguishable
   from "never registered."

---

### User Story 2 — Administrator registers a new installation (Priority: P1)

For the resolver to know anything, someone has to put rows in the
table. An administrator (Company OS operator, not the tenant
themselves at this stage) registers an installation by providing the
tuple `(provider, installation_id, tenant_id)` plus an optional
`secret_ref` pointer to the per-installation signing secret. The
mapping becomes effective for subsequent webhooks within seconds.

**Why this priority**: Equally critical with Story 1 — without the
ability to register installations, the lookup table is empty and
Story 1 is impossible. They ship together.

**Independent Test**: Invoking the admin install path with a fresh
`(provider, installation_id, tenant_id)` writes a row. A subsequent
resolver call for that `(provider, installation_id)` returns the
tenant. Invoking it a second time with the **same** `(provider,
installation_id)` but a **different** tenant fails (uniqueness is
enforced).

**Acceptance Scenarios**:

1. **Given** an empty `provider_installations` table, **When** an
   admin registers `(slack, T_NEW_001, tenant_x)`, **Then** the row
   is persisted and a subsequent webhook payload for `T_NEW_001`
   resolves to `tenant_x` within 5 seconds of registration.
2. **Given** an existing installation `(slack, T_DUP_002, tenant_a)`,
   **When** an admin attempts to register `(slack, T_DUP_002,
   tenant_b)`, **Then** the operation is refused with a structured
   error indicating the conflict; no tenant ownership transfer
   happens silently.
3. **Given** an enabled installation, **When** an admin disables
   it, **Then** subsequent webhooks for that installation are
   rejected (same outcome as never-registered) within 5 seconds of
   the disable action.

---

### User Story 3 — Unknown installations cannot be enumerated (Priority: P1)

A would-be attacker probes the webhook endpoint with random
`team_id` / `installation.id` / `guild_id` / `organizationId` /
`account` values, attempting to discover which workspaces have
installed Company OS. Whether or not a given identifier is
registered MUST NOT be inferable from the response. Registered-
but-disabled and never-registered MUST be indistinguishable
externally.

**Why this priority**: Security-critical and listed as an explicit
acceptance criterion in `source.md` ("Unknown teams get 401 (not
404)"). Indistinguishability is a property of the response set, not
of a single response — must be tested as a set.

**Independent Test**: Send 100 requests, half with registered
installation IDs but invalid signatures, half with unregistered IDs.
After signature failure has been excluded (IN-06 handles that), the
response distribution for "unknown installation" must be uniform —
same status code (401), same response body shape (`{code: "...",
message: "..."}` with no tenant-revealing fields), same response-
time distribution within noise tolerance.

**Acceptance Scenarios**:

1. **Given** the resolver's "unknown installation" outcome,
   **When** the response is observed at the HTTP boundary, **Then**
   it carries HTTP 401 and a structured body that does NOT contain
   the installation_id, does NOT contain "not found" / "unknown" /
   "doesn't exist" semantics that distinguish from auth failure.
2. **Given** a registered-but-disabled installation versus a
   never-registered installation, **When** the resolver is invoked
   for each, **Then** both yield the **same** structured outcome
   (same code, same message, same context shape).

---

### User Story 4 — Resolver supports all five launch providers (Priority: P2)

The webhook router (IN-06) ingests from Slack, GitHub, Linear,
Stripe, and Discord. Each provider names its workspace identifier
differently and places it in a different part of the request. The
resolver MUST be able to extract the identifier for each of them
using only the parsed payload (or, for Stripe, parsed request
headers).

**Why this priority**: Required for full launch but not for the
MVP demonstration. Stories 1–3 can ship with Slack alone and prove
out the design; the other four are mechanical fan-out.

**Independent Test**: For each provider, a recorded vendor-sample
payload + a corresponding installation row produces the correct
tenant. No actual HTTP transit involved — the resolver is a pure
function of `(provider, payload, headers)`.

**Acceptance Scenarios**:

1. **Given** a Slack `event_callback` payload, **When** the
   resolver runs, **Then** the `team_id` field is the lookup key.
2. **Given** a GitHub webhook payload, **When** the resolver runs,
   **Then** the `installation.id` field (stringified) is the
   lookup key.
3. **Given** a Linear webhook payload, **When** the resolver runs,
   **Then** the `organizationId` field is the lookup key.
4. **Given** a Stripe Connect webhook with a `Stripe-Account`
   header, **When** the resolver runs, **Then** the header value
   is the lookup key (NOT a payload field).
5. **Given** a Discord interaction payload, **When** the resolver
   runs, **Then** the `guild_id` is the lookup key for guild-
   scoped interactions, falling back to `application_id` only for
   application-scoped interactions where `guild_id` is absent.
6. **Given** a payload that is malformed for the named provider
   (key missing, wrong type), **When** the resolver runs, **Then**
   it returns a structured "payload missing installation id"
   outcome distinct from "unknown installation."

---

### User Story 5 — Hot-path resolution does not hit the database every time (Priority: P2)

Webhook traffic is high-volume and the installation mapping changes
rarely. Every resolution path that hits Postgres is a wasted round-
trip. The resolver MUST cache lookups so that, in steady state,
the overwhelming majority of resolution attempts are served from
cache.

**Why this priority**: A correctness/cost concern, not a
correctness/integrity concern. Stories 1–3 are correct without
caching; this story makes them affordable.

**Independent Test**: After warmup, repeated resolution attempts
for the same `(provider, installation_id)` produce the same answer
with no incremental database query count. Invalidating the cache
entry (via the install/disable path) forces the next resolution to
hit the database exactly once.

**Acceptance Scenarios**:

1. **Given** a cache cold start, **When** the resolver is invoked
   100 times with 10 distinct keys, **Then** at most 10 of those
   invocations result in a database read.
2. **Given** a steady-state cache, **When** an admin disables an
   installation, **Then** the next resolution for that installation
   reflects the disabled state within the cache TTL or the explicit
   invalidation window (whichever is shorter).
3. **Given** a steady-state cache, **When** the cache backend is
   unavailable, **Then** the resolver falls back to direct database
   lookup and continues serving correct answers (degraded latency,
   not degraded correctness).

---

### User Story 6 — Cross-tenant isolation of the installation table (Priority: P2)

The `provider_installations` table is tenant-scoped. Tenant A's
operator (in any future flow where tenants can self-manage their
installations) MUST NOT be able to read, modify, or even prove the
existence of tenant B's installation rows. Today's admin path is
operator-only (one privileged caller), but the table participates
in the RLS regime so that defense-in-depth holds when self-serve
arrives.

**Why this priority**: P2 today because the admin path is single-
caller; would become P1 the moment self-serve tenant install lands.
Enforcing now is cheaper than retrofitting.

**Independent Test**: Within a database session under
`tenant_transaction(tenant_a)`, queries against
`provider_installations` return only tenant A's rows. The same
queries under `tenant_transaction(tenant_b)` return only tenant B's
rows. Neither session can see the other's rows even by joining or
counting.

**Acceptance Scenarios**:

1. **Given** rows for both tenant A and tenant B in
   `provider_installations`, **When** a database session sets
   `app.current_tenant = tenant_a`, **Then** `SELECT * FROM
   provider_installations` returns only tenant A's rows.
2. **Given** the same data, **When** an attempted INSERT under
   `app.current_tenant = tenant_a` tries to write a row with
   `tenant_id = tenant_b`, **Then** the write is rejected (RLS
   policy).

---

### Edge Cases

- A webhook arrives **after** an admin has disabled the
  installation but **before** the cache TTL expires — story 5
  acceptance 2 governs: invalidation MUST occur on disable, not
  rely on TTL alone.
- A webhook arrives with an empty / null / non-string
  `installation_id` (e.g. malformed Slack payload missing
  `team_id`) — distinct "payload missing installation id"
  outcome, not silently coerced to lookup-fail.
- A provider sends an event for an installation that was never
  registered (true unknown) versus one that was registered and
  later **deleted** — the design currently treats deletion as out
  of scope (disable is the supported revocation), so deletion-then-
  webhook is an admin-initiated edge case rather than a runtime
  case.
- Two different providers happen to use the **same string** for
  their installation identifiers (e.g. Slack `team_id =
  T123ABC` collides numerically with a GitHub installation id of
  `123` — unlikely but trivially possible across providers) —
  uniqueness is per-`(provider, installation_id)`, not per
  `installation_id`. Lookup is keyed jointly.
- Stripe Connect webhooks deliver the account ID in a request
  header (`Stripe-Account`), not in the JSON body — the resolver
  contract MUST accept a header bag alongside the body.
- Discord interactions are sometimes scoped to a guild and
  sometimes to an application (DMs, global app commands) — the
  resolver MUST prefer `guild_id` and fall back to `application_id`
  in a deterministic, documented order.
- A network partition leaves the cache unreachable — the resolver
  degrades to direct database lookup (story 5 acceptance 3), not
  to a 5xx.
- The admin install path is called twice concurrently for the same
  `(provider, installation_id)` tuple — uniqueness constraint
  resolves it; one call wins, the other receives the structured
  conflict error from story 2 acceptance 2.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST persist a directional mapping from
  `(provider, installation_id)` to `tenant_id` for every active
  integration installation.
- **FR-002**: The system MUST enforce that each `(provider,
  installation_id)` pair maps to at most one `tenant_id`. Re-
  registering the same pair to a different tenant MUST fail with a
  structured conflict error; silent ownership transfer is
  prohibited.
- **FR-003**: The system MUST extract the provider-native
  installation identifier from each launch provider's
  request using the following sources:
    - **Slack**: `team_id` from the JSON payload root.
    - **GitHub**: `installation.id` from the JSON payload (treated
      as a string).
    - **Linear**: `organizationId` from the JSON payload.
    - **Stripe**: `Stripe-Account` request header (NOT a payload
      field).
    - **Discord**: `guild_id` from the JSON payload, falling back
      to `application_id` if `guild_id` is absent.
- **FR-004**: The system MUST return a structured "resolved"
  outcome carrying the `tenant_id` and the matched
  `provider_installations.id` when an enabled installation is
  found.
- **FR-005**: The system MUST return a structured "unknown
  installation" outcome that is **identical** for the cases
  "never registered" and "registered but disabled." External
  callers (the webhook router) MUST NOT be able to distinguish
  these two cases from the resolver response alone.
- **FR-006**: The system MUST return a structured "payload missing
  installation id" outcome — distinct from "unknown installation"
  — when the provider-native identifier is absent or malformed in
  the request.
- **FR-007**: The system MUST provide an administrator-callable
  interface (CLI **or** admin HTTP endpoint — the plan phase picks
  one) that registers a new `(provider, installation_id,
  tenant_id, secret_ref?)` tuple.
- **FR-008**: The system MUST provide administrator-callable
  interfaces to (a) disable an existing installation by setting
  `enabled=false`, (b) re-enable a previously-disabled
  installation, and (c) update the `secret_ref` pointer on an
  existing installation without changing its identity or
  `installed_at`. The update-secret-ref action MUST invalidate
  the resolver cache entry for the affected installation per
  FR-010 so that downstream consumers (signature verifiers)
  re-read the pointer within the consistency window.
- **FR-009**: The system MUST cache `(provider, installation_id) →
  tenant_id` lookups such that, after warmup, the hot path does
  not require a database round-trip on every webhook.
- **FR-010**: The system MUST invalidate the cache entry for an
  installation whenever an administrator action mutates that
  installation's row (register / disable / re-enable). Time-to-
  consistency from admin action to next webhook MUST be ≤5 seconds.
- **FR-011**: When the cache backend is unavailable, the system
  MUST fall back to direct database lookup. Cache unavailability
  is a latency event, not a correctness event.
- **FR-012**: The `provider_installations` table MUST participate
  in the existing tenant isolation regime per Constitution §III:
  `tenant_id UUID NOT NULL` with a deferrable-initially-immediate
  FK to `tenants(id)` (per migration 0037), row-level security
  enabled and forced with the migration 0036 permissive policy,
  and tenant-prefixed indexes on all common query predicates.
- **FR-013**: All primary keys generated for rows in
  `provider_installations` MUST use `uuid7()` from
  `lib/shared/ids.py` (per Constitution §VII). `uuid.uuid4()` is
  prohibited for substrate and quasi-substrate rows.
- **FR-014**: All resolver outcomes MUST be expressed as instances
  of the existing `CompanyOSError` hierarchy (or pure-value
  success types) per Constitution §VIII. Each failure carries a
  stable string `code` and a `context: dict` that names the
  provider but NEVER contains the installation_id, the secret_ref,
  or any payload bytes.
- **FR-015**: All log lines emitted by the resolver MUST use
  `structlog`. No `print()`. Log records MUST name the provider
  and outcome but MUST NOT include the installation_id verbatim,
  the secret_ref, or the request body (defense against
  enumeration via logs).
- **FR-016**: The resolver MUST be a pure function of `(provider,
  payload, headers, time, db, cache)` — no module-level globals
  for cache or DB, in keeping with the `build_*_router()` factory
  pattern used elsewhere (Constitution stack constraints).
- **FR-017**: The admin interface MUST require operator-level
  authorization (the existing privileged-caller path used by other
  administrative endpoints). Self-serve tenant access to the
  installation table is out of scope and MUST be denied.

### Key Entities

- **Installation** (`provider_installations` row): represents one
  registered link between an external provider's workspace
  identifier and a Company OS tenant. Carries: an internal id
  (uuid7), the owning `tenant_id`, the `provider` name (low-
  cardinality enum-like string), the `installation_id` (provider-
  native string), an optional `secret_ref` (pointer into an
  external secrets manager, not the secret material itself), an
  `enabled` boolean for soft revocation, and an `installed_at`
  timestamp. Uniqueness is enforced jointly on `(provider,
  installation_id)`.
- **Resolver Outcome**: the value the resolver returns. Three
  variants: **Resolved** (carries `tenant_id` and the matched
  installation id), **UnknownInstallation** (carries provider
  only — the disabled case and the never-registered case
  collapse into this single variant by design), **PayloadMissing**
  (carries provider only — the payload didn't contain the
  expected identifier in a parseable form).
- **Installation Registration Request**: the input to the admin
  interface — provider, installation_id, tenant_id, optional
  secret_ref. Refused on uniqueness violation.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An administrator can register a new (provider,
  installation_id, tenant_id) tuple and see the next webhook for
  that installation route to the correct tenant within 5 seconds
  of the registration completing — measured by webhook-arrival-
  timestamp minus registration-completion-timestamp on a synthetic
  end-to-end run.
- **SC-002**: For 100% of webhooks delivered with an unknown
  installation identifier across the five launch providers, the
  response is HTTP 401 (not 404, not 5xx). Verified by replaying a
  recorded suite of vendor-sample payloads against unregistered
  identifiers.
- **SC-003**: A registered-but-disabled installation and a never-
  registered installation produce externally indistinguishable
  responses, as confirmed by an automated test that hashes the
  response body (with timestamps stripped) for each case and
  asserts equality.
- **SC-004**: After cache warmup, ≥95% of resolution attempts in a
  representative 10-minute load window are served without a
  database read. Measured via a cache-hit metric.
- **SC-005**: Recorded vendor-sample payloads for all five launch
  providers each resolve to the expected test tenant when a
  matching installation row is present, and to the
  "UnknownInstallation" outcome when it is not. Coverage of every
  provider is a release gate.
- **SC-006**: Under cross-tenant probing — a session-scoped query
  with `app.current_tenant = tenant_a` cannot read, count, or
  modify any row belonging to `tenant_b`. Verified by an
  integration test on the live Postgres fixture.
- **SC-007**: Cache backend unavailable for the duration of a
  10-second test window degrades latency but preserves
  correctness: 100% of requests still produce the right resolver
  outcome (with one DB round-trip per request).
- **SC-008**: No log line emitted by the resolver, across all
  branches of the test suite, contains the installation_id verbatim
  or any portion of the request body. Verified by a structured-log
  assertion in the integration test suite.

## Assumptions

- **A1 — Migration number**: The source.md names
  `0041_provider_installations.sql`. Per Constitution §II.1, the
  authoritative number is the **next free** filename in
  `db/migrations/` at the time of merge. The latest applied
  migration on this branch is `0038_signal_readings_sidecar.sql`;
  the migration created by this feature is therefore
  `0039_provider_installations.sql` unless an earlier-merging
  branch claims it first. The constitution wins over the literal
  number in `source.md`; the plan records the choice explicitly.
- **A2 — Module name**: `source.md` names
  `services/webhooks/tenant_resolver.py`. IN-06's plan currently
  references `services/webhooks/tenant_resolution.py` for the
  same module (§"Project Structure" of IN-06 plan). The task
  body's `tenant_resolver.py` wins; IN-06's plan is updated to
  reference the resolver module under its IN-07-canonical name
  when IN-06 lands. Recorded so the divergence is intentional.
- **A3 — Cache backend**: source.md names Redis. Redis is not
  presently listed under "Stack Constraints" in the constitution,
  so its introduction is a stack-shape decision deferred to the
  plan phase. The spec is technology-agnostic and says "cache";
  the plan picks the backend (Redis, in-process LRU, or a
  cache abstraction with both backends).
- **A4 — Admin interface form factor**: source.md says "CLI or
  admin endpoint." The choice is a plan-phase decision. The spec
  is agnostic to which one ships first; both forms satisfy
  FR-007.
- **A5 — Stripe Connect assumption**: The `Stripe-Account` header
  is the right key only for Stripe Connect platforms. Direct
  (non-Connect) Stripe accounts have a single account globally
  and would not need per-installation routing. We assume the
  Stripe integration is Connect-based — confirm in the plan.
- **A6 — Discord scope choice**: `guild_id` is preferred over
  `application_id` because most events of interest to Company OS
  are guild-scoped (server activity). DM-only / global-command
  edge cases fall back to `application_id`. Confirm with a vendor
  sample in the plan.
- **A7 — RLS strictness**: The current RLS regime is the
  permissive default from migration 0036
  (`current_setting('app.current_tenant', true) IS NULL` allows
  the row through). Hand-rolled `WHERE tenant_id = $1` predicates
  remain authoritative per Constitution §III. This feature
  follows that bar; the eventual flip to strict RLS is a separate
  migration, out of scope here.
- **A8 — Substrate exemption**: This feature creates no
  Observation, Model, Act, or Resource. It is a per-feature side
  table for tenant routing (Constitution §I). The Universal Flow
  Rule does not apply directly; downstream consumers (IN-06)
  inherit the resolved `tenant_id` and continue to satisfy the
  rule for the Observation rows they produce.
- **A9 — IN-06 dependency**: This resolver is consumed by the
  IN-06 webhook router. It MUST be developable and testable
  without IN-06 having shipped — the resolver is invoked by tests
  directly. End-to-end webhook-to-Observation tests live in
  IN-06's test suite, not here.
- **A10 — Out of scope**: The OAuth callback / install flow UI,
  the secrets manager itself (only a `secret_ref` pointer is
  stored), the per-installation signature verification (IN-06),
  the bulk import of existing installations, and any auditing or
  history of installation lifecycle events.
