# Feature Specification: IN-06 — Webhook Gateway Router

**Feature Branch**: `IN-06-webhook-gateway-router`
**Created**: 2026-05-13
**Status**: Draft
**Input**: User description: "Real webhook sources (Slack, GitHub, Linear, Stripe, Discord) don't authenticate with Bearer tokens — they sign requests. Today's `/ingest/{channel}` endpoint is Bearer-only and is structurally incapable of accepting them. This blocks shipping to real customers."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — A real Slack workspace delivers signed messages and they become Observations (Priority: P1)

A customer (or our own dogfood workspace) configures a Slack app to send
`message` events to the system. Slack signs each delivery with its
v0 HMAC envelope. The system accepts the request without a Bearer token,
verifies the signature against the per-tenant Slack secret, rejects any
replay older than the configured window, and converts the verified
payload into an Observation in the correct tenant context.

**Why this priority**: Slack is the dominant signal source for the
dogfood and for early customers. Until this works end-to-end, the
product cannot be connected to any real workspace — the legacy
`/ingest/{channel}` path requires a Bearer token that no real provider
sends. This single story unblocks "ship."

**Independent Test**: Configure a Slack-signed request against the new
webhook path; assert (a) HTTP 2xx, (b) a row in `observations` with
`source_channel='slack:message'` and the correct `tenant_id` and
`trust_tier`, (c) no row created for a request with a tampered body.

**Acceptance Scenarios**:

1. **Given** a valid Slack signing secret is configured for tenant T,
   **When** Slack POSTs a signed `message` event whose signature
   matches the body and whose timestamp is inside the replay window,
   **Then** the system responds 2xx and an Observation appears under
   tenant T with the existing Slack ingestion semantics preserved.
2. **Given** the same configuration, **When** the request body is
   modified after signing (a single byte changed), **Then** the
   system responds 401 with a structured error naming the provider
   and the failure reason, and no Observation is created.
3. **Given** the same configuration, **When** a request arrives whose
   signed timestamp is older than the configured replay window,
   **Then** the system responds 401 with a distinct failure reason
   (`expired_timestamp`) so an operator can tell it apart from a
   signature mismatch.

---

### User Story 2 — Spoofed or unsigned requests are rejected as a security guarantee (Priority: P1)

Anyone on the public internet can POST to the webhook endpoint. The
system must treat unverified requests as adversarial: no side effects,
no downstream visibility into the request body, no information leakage
that would help an attacker forge a valid signature.

**Why this priority**: The webhook path is unauthenticated at the
transport layer (no Bearer); the cryptographic verification IS the
authentication. If verification can be bypassed, the substrate's
trust-tier invariants and tenant isolation collapse. This is a
non-negotiable companion to US1.

**Independent Test**: Send (a) a request with no signature header,
(b) a request with a signature for a different secret, (c) a request
whose signature comparison short-circuits on a length mismatch. Each
returns 401 with structured error context; none produces an
Observation, a queue entry, or a log line containing the body.

**Acceptance Scenarios**:

1. **Given** no secret is configured for a provider, **When** a
   request arrives for that provider, **Then** the system responds
   401 with a distinct reason (`secret_not_configured`) so the
   operator can distinguish configuration error from attack.
2. **Given** any of {missing header, malformed header, wrong digest,
   wrong key} conditions, **When** the request is received, **Then**
   the system responds 401 and increments the verification-failure
   metric labeled with the provider.
3. **Given** the system is under sustained spoofing load, **When**
   comparing signatures, **Then** the comparison MUST NOT leak
   timing information that would let an attacker reconstruct a valid
   signature byte-by-byte.

---

### User Story 3 — GitHub, Linear, and Stripe deliver HMAC-signed webhooks (Priority: P2)

Each of these providers signs requests with an HMAC over the body (or
over a `timestamp.body` envelope, in Stripe's case) and delivers the
digest in a provider-specific header. The system supports each
provider as an additional verifier, producing Observations through the
same ingestion pipeline.

**Why this priority**: GitHub, Linear, and Stripe are the next-tier
high-value sources after Slack. They share the HMAC pattern, so
adding each is a thin extension once US1's framework is in place,
but each is independently shippable and demonstrable.

**Independent Test**: For each provider, replay a vendor sample
payload (signed with a known test secret) against the corresponding
webhook path; observe a 2xx and a downstream Observation; tamper with
the body and observe 401.

**Acceptance Scenarios**:

1. **Given** a GitHub webhook with a valid `X-Hub-Signature-256`
   header, **When** delivered to the GitHub webhook path, **Then**
   the system verifies, ingests, and produces an Observation with
   `source_channel='github:<event_type>'`.
2. **Given** a Stripe webhook with a valid `Stripe-Signature` header
   (containing a `t=` timestamp), **When** delivered within the
   replay window, **Then** the system verifies and ingests; outside
   the window, returns 401 with `expired_timestamp`.
3. **Given** a Linear webhook with a valid `Linear-Signature`,
   **When** delivered, **Then** the system verifies and ingests.

---

### User Story 4 — Discord delivers ed25519-signed webhooks (Priority: P2)

Discord does not use HMAC; it signs interaction payloads with ed25519
over `timestamp + body`. The system must support this non-HMAC
signature scheme so Discord-driven signals can flow through the same
ingestion pipeline.

**Why this priority**: P2 because Discord traffic volume is smaller
than Slack/GitHub, but the story matters structurally: it proves the
verification framework is not HMAC-only, which it must not be in
order to admit future providers (Twilio's request validation, etc.).

**Independent Test**: Sign a request with a known ed25519 keypair,
deliver it, assert ingestion; deliver with a tampered timestamp or
body, assert 401.

**Acceptance Scenarios**:

1. **Given** a configured Discord public key, **When** a request
   arrives with a valid `X-Signature-Ed25519` and
   `X-Signature-Timestamp` pair signing a payload, **Then** the
   system verifies and ingests.
2. **Given** the same configuration, **When** the timestamp is
   altered without re-signing, **Then** the system rejects 401.

---

### User Story 5 — Operator rotates a webhook secret with no rejected legitimate traffic (Priority: P3)

Webhook secrets rotate (compromise response, scheduled rotation,
customer self-service). The system must accept either the prior or
the new secret during a rotation overlap window, so the provider
side and the operator side don't need to swap atomically.

**Why this priority**: P3 because at MVP the operator can tolerate
a brief outage by rotating during a quiet window, but once we have
real customers, rotation-with-overlap becomes a requirement to avoid
visible breakage. The behavior must be designed in now even if the
operator UX comes later.

**Independent Test**: Configure two valid secrets (old + new) for
the same provider+tenant; deliver requests signed with each; both
succeed. Remove the old secret; requests signed with old fail.

**Acceptance Scenarios**:

1. **Given** two valid secrets configured for the same
   (provider, tenant), **When** requests signed with either arrive,
   **Then** both verify successfully.
2. **Given** the old secret is removed from configuration,
   **When** a request signed with the old secret arrives, **Then**
   the system rejects 401.
3. **Given** secret rotation is in progress, **When** the operator
   changes configuration, **Then** the system does NOT require a
   process restart for the new secret to take effect.

---

### User Story 6 — Verification failures are observable per provider (Priority: P3)

When verification fails — whether from operator misconfiguration,
provider clock skew, or active attack — the operations team must be
able to see the rate and distribution of failures by provider, and
distinguish failure reasons.

**Why this priority**: P3 because the system continues to function
correctly without dashboards, but missing this signal means an
ongoing attack or a broken rotation goes unseen.

**Independent Test**: Generate failures of each distinct reason
(missing header, expired, mismatch, secret_not_configured) and
observe that each reason is recorded with the correct provider
label and increments a counter the operator can query.

**Acceptance Scenarios**:

1. **Given** an observability backend is connected, **When** any
   verification fails, **Then** a counter metric is incremented
   labeled with the provider name AND the failure reason.
2. **Given** a structured log is emitted for each failure,
   **When** an operator queries logs by provider, **Then** the
   reason, the tenant (if resolvable), and the request timestamp
   are present without leaking the raw body or signature.

---

### Edge Cases

- **Raw vs. reparsed body.** Some providers sign the literal request
  bytes. The system MUST verify against the exact bytes received,
  not against a re-serialized form, or it will fail on whitespace,
  key ordering, and Unicode normalization.
- **Missing tenant resolution.** A webhook arrives for a provider
  configured globally but not yet associated with any tenant. The
  system MUST reject with a distinct reason (`tenant_not_resolved`)
  rather than producing an orphan Observation.
- **Clock skew on the receiver.** Replay windows are bidirectional:
  a clock that is fast can reject legitimate requests as "future."
  The system MUST accept timestamps within `±window` of local time.
- **Multiple secrets accepted simultaneously.** During rotation,
  the verifier tries each configured secret in turn; the time spent
  trying must not become a side channel for which secret was the
  match.
- **Provider URL collision.** The path layout MUST NOT collide with
  the existing `/ingest/{channel}` Bearer-protected path; the two
  must coexist for the duration of any migration.
- **Empty body / GET probes.** Providers occasionally probe the
  endpoint with empty bodies (URL verification handshakes). The
  system MUST handle the per-provider handshake without producing
  an Observation.
- **Payload too large.** The existing body-size precheck (IN-01)
  applies to this endpoint as well; oversized payloads are rejected
  before verification.
- **Per-provider configurability of the replay window.** Slack
  documents 300s; Stripe documents 300s; clock-skew tolerance per
  provider may differ. The window is configurable per provider.
- **Concurrent ingestion of the same external event.** A provider
  may retry on transient receiver errors; the existing ingestion
  dedup contract (by `external_id`) MUST continue to hold after
  verification.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST expose a webhook ingress path distinct
  from the existing `/ingest/{channel}` Bearer-protected path, so the
  two can coexist during migration without behavior change to either.
- **FR-002**: The webhook ingress MUST NOT require a Bearer token —
  the cryptographic signature on each request IS the authentication.
- **FR-003**: The system MUST cryptographically verify the
  authenticity of every webhook request using the originating
  provider's published signature scheme BEFORE any side effect
  (database write, queue enqueue, downstream call, log of the body).
- **FR-004**: Any request whose signature cannot be verified MUST
  receive HTTP 401 and a structured error response. The error
  response MUST carry a stable `code` and a context dictionary
  identifying the provider and the failure reason (in line with
  the project's `CompanyOSError.to_dict()` contract).
- **FR-005**: Failure reasons MUST be distinguishable so operators
  can separate misconfiguration from attack. At minimum:
  `missing_signature_header`, `malformed_signature_header`,
  `expired_timestamp`, `signature_mismatch`,
  `secret_not_configured`, `tenant_not_resolved`.
- **FR-006**: For every provider that includes a timestamp in its
  signed envelope, the system MUST enforce a replay-protection
  window. The default window MUST be 300 seconds. The window MUST
  be configurable per provider.
- **FR-007**: Signature comparison MUST be timing-attack-resistant.
- **FR-008**: The system MUST support the following providers at
  launch: Slack, GitHub, Linear, Stripe, Discord.
- **FR-009**: Per-provider, per-tenant secrets MUST be supported, so
  a single deployment can serve multiple tenants whose secrets
  differ.
- **FR-010**: The system MUST accept either the prior secret or the
  new secret during a configured rotation overlap, so providers and
  operators do not need to swap atomically. Rotation MUST NOT
  require a process restart.
- **FR-011**: The system MUST emit a counter metric for every
  verification failure, labeled with the provider name and the
  failure reason.
- **FR-012**: Verification logic MUST operate on the literal request
  bytes received, not on any re-serialization of them.
- **FR-013**: After successful verification, the system MUST route
  the payload through the existing ingestion pipeline so that the
  resulting Observation carries the correct `source_channel`,
  `trust_tier`, `tenant_id`, and `external_id` for dedup — and
  participates in the existing tenant-isolation regime (FK + RLS +
  `tenant_id`-prefixed queries) per Constitution Principle III.
- **FR-014**: The system MUST resolve the originating tenant for
  each verified webhook before producing Observations. A webhook
  that cannot be resolved to a tenant MUST be rejected with the
  `tenant_not_resolved` reason, not ingested under a fallback.
- **FR-015**: The existing Slack signature verification behavior
  (HMAC v0, 300-second replay window, constant-time compare) MUST
  be preserved end-to-end; users with Slack apps already pointed at
  the existing verifier MUST continue to work without resigning.
- **FR-016**: Verification failure logs MUST NOT contain the raw
  request body or the candidate signature, so an attacker who
  triggers log emission gains no information beyond confirmation of
  rejection.
- **FR-017**: Provider-specific URL-verification handshakes (e.g.
  Slack's `url_verification` event) MUST be supported without
  producing Observations.
- **FR-018**: The endpoint MUST honor the existing body-size
  precheck (IN-01) — oversized payloads are rejected before
  signature verification is attempted.

### Key Entities

- **Webhook request** — an inbound HTTP request from an external
  provider, characterized by the provider name, the raw body bytes,
  and the provider-specific signature header(s).
- **Provider** — one of `slack`, `github`, `linear`, `stripe`,
  `discord` at launch; each is associated with a published signature
  protocol (HMAC variant or ed25519) and a published header
  contract.
- **Webhook secret** — the credential the provider uses to sign and
  the system uses to verify. Scoped per (provider, tenant). Supports
  one or more concurrently-valid values to enable rotation overlap.
- **Verification result** — the outcome of checking a webhook
  request: either *verified* (carrying provider, tenant, and the
  payload-as-bytes for downstream ingestion) or *rejected* (carrying
  a structured reason).
- **Observation** — the downstream artifact produced after successful
  verification, conforming to the existing Observation contract
  (`tenant_id`, `source_channel`, `trust_tier`, `occurred_at`,
  `external_id`, `content`, etc.). No new Observation shape is
  introduced by this feature.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An end-to-end test using a real Slack workspace
  delivers messages that appear as Observations with the correct
  `source_channel`, `tenant_id`, and `trust_tier`.
- **SC-002**: For every supported provider, a unit test against
  the vendor's sample payload signed with a known test secret
  verifies successfully; the same payload with a single tampered
  byte returns 401 with `signature_mismatch`.
- **SC-003**: Replay protection is observable: a request whose
  signed timestamp is older than the configured window returns 401
  with `expired_timestamp`, distinctly from `signature_mismatch`.
- **SC-004**: A secret-rotation test demonstrates zero rejected
  legitimate requests when old and new secrets are accepted in
  parallel, and immediate rejection of the old secret after it is
  removed from configuration.
- **SC-005**: A spoofed-request test produces a 401 in under 50ms
  p95 and produces a metric increment labeled with provider and
  reason.
- **SC-006**: After this feature ships, no provider in the
  supported set requires a Bearer token to deliver webhooks — the
  product can be pointed at a real Slack/GitHub/Linear/Stripe/
  Discord application without code changes on the customer side.
- **SC-007**: Verification failure logs and metrics never contain
  the raw request body or the candidate signature, verified by
  inspecting log output and metric labels during the spoofed-
  request test.
- **SC-008**: The existing `/ingest/{channel}` Bearer-protected
  path continues to function unchanged for callers that still use
  it (e.g. the simulation harness and internal test fixtures).

## Assumptions

- The five provider signature protocols match each vendor's
  documented contract as of the spec date; vendor protocol changes
  are tracked as separate work.
- Secret storage is provided out of band — by environment variables
  in dogfood and by an external secrets manager in production. This
  spec does not define the secret-management UI or backing store; it
  only requires that secrets can be rotated without process restart.
- Tenant resolution for incoming webhooks uses configuration the
  operator establishes when connecting a provider for a tenant
  (e.g., a Slack team_id → tenant_id mapping). The mapping
  mechanism itself is in scope only insofar as FR-014 requires it
  exist and behave deterministically.
- The downstream ingestion pipeline (`services/ingestion/core.py`
  and its handlers) is the canonical Observation producer and is
  not modified by this spec beyond being invoked from a new entry
  point.
- The existing observability stack (structlog + the project's
  metric exporters) is the surface where the new counter metric
  appears.
- Discord's ed25519 verification is the only non-HMAC scheme at
  launch; future non-HMAC providers (e.g. Twilio) will follow the
  same shape.
- The new path coexists with the legacy Bearer-protected
  `/ingest/{channel}` until migration to webhook-only ingestion is
  complete; this spec does not retire `/ingest/{channel}`.

### Out of Scope

- Outbound webhook dispatch (the system sending webhooks elsewhere).
- A user-facing UI for managing webhook secrets, rotation schedules,
  or per-tenant provider connections.
- Auto-discovery of new providers; each new provider is an explicit
  code addition.
- Replacing or modifying the Bearer-token path used by internal
  callers (simulation harness, test fixtures) — that path remains
  for non-webhook use.
- Changes to the Observation schema, the Think trigger pipeline, or
  any downstream consumer of Observations.
- Retirement of `/ingest/{channel}` for the five supported providers;
  that cutover is a separate plan.
