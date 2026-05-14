# Feature Specification: Slack Production Integration — OAuth Install, DB-Backed Secrets, End-to-End Customer Self-Serve

**Feature Branch**: `feat/IN-08-slack-production-integration`
**Created**: 2026-05-14
**Status**: Draft
**Input**: ClickUp task `IN-08 [P0] Slack production integration` (verbatim in [source.md](./source.md))

## Summary

Today, Fyralis can *receive* Slack webhooks (IN-06 verifies HMAC, runs the `slack:message` ingestion handler) and IN-07 shipped a DB-backed `TenantResolver` that maps `team_id → tenant_id` via the `provider_installations` registry. But the `TenantResolver` is unwired, signing secrets still live in environment variables, and a customer onboarding requires a Fyralis operator to hand-insert a `provider_installations` row and set a tenant-prefixed env var. This feature closes that gap: a Slack workspace admin clicks "Add to Fyralis", completes OAuth, and within seconds their messages land as `Observations` under the correct `tenant_id` — no operator touch, no plaintext secrets, and uninstalls correctly disable the row.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Production-Grade Per-Installation Secret Storage (Priority: P1)

As a Fyralis platform operator, I need every workspace signing secret and OAuth token at rest to be stored under encrypted storage referenced by `provider_installations.secret_ref`, so that no tenant secret ever lives in a deployment environment variable or in plaintext inside the database.

**Why this priority**: Every downstream flow (OAuth callback, signature verification, outbound API calls) reads secret material through the same store. Until the store is the canonical retrieval path, all other phases would either dual-write secrets into env vars (a regression on tenant isolation) or store them in plaintext (an outright security violation). This is the foundation for Constitution §III defense-in-depth as it pertains to per-tenant credential material.

**Independent Test**: Insert a fake provider installation with a known plaintext signing secret via the secret store API, fire a signed Slack webhook for that workspace, and verify HMAC validation succeeds while no env var contains the secret. Validate that the existing IN-07 admin tests in `services/webhooks/tests/test_tenant_resolver_admin.py` continue to pass without modification.

**Acceptance Scenarios**:

1. **Given** the secret store contains a Slack signing secret for installation `inst-A`, **When** a webhook arrives carrying a valid HMAC for `team_id` mapped to `inst-A`, **Then** signature verification succeeds and no env-var lookup occurs.
2. **Given** no `WEBHOOK_SECRET_SLACK__<TENANT_HEX>` env var is set and the env-var fallback flag is OFF, **When** a webhook arrives for a workspace whose secret_ref is missing or unresolvable, **Then** the request is rejected with the same error shape IN-07 returns for `unknown_installation`, and no plaintext secret is ever logged.
3. **Given** `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` is set in a development environment, **When** the secret store has no entry but the legacy env var exists, **Then** the env-var path is used and the response carries the same outcome as before this feature shipped, so dev loops are not broken.
4. **Given** an existing secret stored under ref `r-1`, **When** a rotation is performed via the secret store, **Then** subsequent reads return the new plaintext, prior reads of the old plaintext are no longer possible, and the previous ciphertext is removed.

---

### User Story 2 — Webhook Router Uses the IN-07 DB-Backed TenantResolver Exclusively (Priority: P2)

As the operator of the multi-tenant gateway, I need the webhook router to resolve tenants only via the IN-07 `TenantResolver` (i.e., from `provider_installations` rows), and to no longer import the env-var resolver, so that there is exactly one path from `team_id` to `tenant_id` and the negative case returns the IN-07 `unknown_installation` outcome.

**Why this priority**: AC #5 ("`services/webhooks/router.py` contains zero references to `services.webhooks.tenant_resolution`") and AC #7 (forged `team_id` → 401 `unknown_installation`, no log leak) are both about this single cutover. Without it, the OAuth-installed rows in Phase 3 would not actually drive routing.

**Independent Test**: Send a webhook whose `team_id` matches a `provider_installations` row → expect 2xx and a resolved tenant in the request scope. Send a webhook with a forged/unknown `team_id` → expect 401 with the IN-07 `unknown_installation` code, no 404, no 500, and the `team_id` does not appear in logs.

**Acceptance Scenarios**:

1. **Given** the router is on the new code path, **When** a Slack webhook carries a `team_id` that maps to an enabled `provider_installations` row, **Then** the request resolves with the `Resolved` outcome and proceeds to the existing IN-06 ingestion handler.
2. **Given** the router is on the new code path, **When** a Slack webhook carries a forged `team_id` for which no row exists, **Then** the response is HTTP 401 carrying the IN-07 `unknown_installation` error code and the offending `team_id` value is not present in any log line written by the router.
3. **Given** the router is on the new code path, **When** the webhook payload is malformed in a way that prevents identifier extraction, **Then** the response is HTTP 400 with the IN-07 `PayloadMissing`-shape error.
4. **Given** a static scan of `services/webhooks/router.py`, **When** it is grepped for `services.webhooks.tenant_resolution`, **Then** zero matches are returned.

---

### User Story 3 — Slack Workspace Admin Completes OAuth Install End-to-End (Priority: P3)

As a Slack workspace admin, I need to click "Add to Fyralis" from an authenticated Fyralis session, consent to the requested Slack scopes, and land back on Fyralis with my workspace bound to my tenant, so that I can begin sending Slack messages that Fyralis will ingest under the correct tenant without any operator interaction.

**Why this priority**: This is the headline acceptance criterion (AC #1) and the reason this feature exists. P3 because it depends on P1 (the secret store must exist to persist the bot/signing secrets) and P2 (the router must already read from `provider_installations` so the freshly inserted row drives routing).

**Independent Test**: From an authenticated browser session, GET `/integrations/slack/install`, follow the redirect to Slack, consent, and observe that the callback creates a `provider_installations` row mapped to the session's `tenant_id`, that the bot/user/signing secrets are stored in the secret store (not env vars), and that a subsequent test message in any subscribed channel arrives as an `Observation` under that tenant within 30 seconds.

**Acceptance Scenarios**:

1. **Given** an authenticated Fyralis session, **When** the admin visits the install endpoint, **Then** the response is a redirect to Slack's OAuth consent URL carrying a state token whose payload is bound to the session's `tenant_id` (the `tenant_id` is NEVER taken from a client-controllable query parameter).
2. **Given** Slack redirects back to the callback endpoint with a valid `code` and a state token that this gateway issued, **When** the callback runs, **Then** it (a) verifies the state token's HMAC and expiry, (b) exchanges the code for tokens via Slack, (c) stores bot/user/signing secrets in the secret store, (d) inserts or upserts a `provider_installations` row keyed by `(provider='slack', installation_id=team_id)` with `enabled=true` and `secret_ref` pointing at the stored bot token, and (e) writes an `installation_audit_log` row with `action='install'`.
3. **Given** the callback endpoint receives a request without a valid state token, **When** the handler runs, **Then** the request is rejected without performing any token exchange and no `provider_installations` row is created or modified.
4. **Given** Slack returns an error during the `oauth.v2.access` exchange, **When** the callback handles the error, **Then** no `provider_installations` row is written, no secret_ref is materialized, and an audit row of `action='install'` with a failure status is written.
5. **Given** the install flow completes successfully, **When** the workspace sends its first Slack message in any subscribed channel, **Then** an `Observation` is persisted with that tenant's `tenant_id` within 30 seconds.

---

### User Story 4 — Workspace Uninstall Disables Installation and Zeroes Token Material (Priority: P4)

As a Slack workspace admin who has removed Fyralis from my workspace, I expect Fyralis to detect the uninstall, immediately stop accepting webhooks from my workspace, and erase the OAuth tokens it held for me.

**Why this priority**: Required by AC #3 and by the "no orphan rows" invariant in AC #4. Without uninstall handling, `provider_installations` rows would leak indefinitely and a future re-install would collide on the `(provider, installation_id)` unique constraint.

**Independent Test**: With an enabled installation in the DB, simulate an `app_uninstalled` event by POSTing a properly-signed Slack webhook with that event type. Then send another webhook for the same workspace and verify it is rejected with `unknown_installation` (401), that `installation_audit_log` carries an `uninstall` row, and that the row's `secret_ref` no longer resolves to plaintext.

**Acceptance Scenarios**:

1. **Given** an enabled installation in `provider_installations`, **When** an inbound webhook for that workspace carries an `app_uninstalled` (or `tokens_revoked`) event type, **Then** the installation row is disabled, the underlying token material is deleted from the secret store, and an `installation_audit_log` row with `action='uninstall'` is written.
2. **Given** an installation that was just uninstalled, **When** any subsequent webhook arrives for the same `team_id`, **Then** the response is HTTP 401 with IN-07 `unknown_installation`; the disabled row does not resolve.
3. **Given** uninstall handling fails partway (e.g., DB write succeeds but secret-store delete fails), **When** the failure is observed, **Then** the failure is surfaced via the existing structured-error path (no bare exceptions), an audit row reflects the partial state, and the operator has a deterministic recovery procedure.

---

### User Story 5 — Re-Install After Uninstall Reuses the Same Installation Row (Priority: P5)

As a Slack workspace admin who removed and later re-added Fyralis, I expect Fyralis to recognize my workspace, restore service, and not duplicate the installation record.

**Why this priority**: AC #4 requires that re-install reuses the same `provider_installations.id` row. Without this, the unique constraint on `(provider, installation_id)` would block re-install entirely.

**Independent Test**: After completing User Story 4, run the install flow again for the same `team_id`. Verify the `provider_installations.id` is preserved, `enabled` flips back to `true`, `secret_ref` points to a freshly stored bot token (a new `secret_ref`, not the deleted one), and `installation_audit_log` contains an `install` row whose audit context references the prior `uninstall`.

**Acceptance Scenarios**:

1. **Given** a `provider_installations` row exists for `(slack, team_id=T)` with `enabled=false`, **When** the OAuth callback completes for the same `team_id=T`, **Then** the same row is updated (not duplicated), `enabled` is set to `true`, `secret_ref` is updated to the new bot-token ref, and the original `provider_installations.id` value is preserved.
2. **Given** the same scenario, **When** the row is inspected after re-install, **Then** there is exactly one row for `(slack, team_id=T)` and no orphan token entries remain in the secret store.

---

### User Story 6 — Per-Installation Outbound Slack API Calls with Rate-Limit Backoff (Priority: P6)

As the Slack ingestion handler, I need a thin async client that performs `chat.postMessage`, `users.info`, and `conversations.info` calls using each installation's bot token (resolved through the secret store), with Slack rate-limit (Tier 1–4) backoff, so that I can enrich incoming `Observations` with human-readable user and channel names.

**Why this priority**: P6 because the receive path works without it; enrichment improves data quality but is not blocking for the headline AC #1. Becomes the substrate for Slack-outbound Acts in a follow-up task (IN-10).

**Independent Test**: Given an installed workspace, call the client to look up `users.info` for a known user ID and verify it returns the canonical display name and uses the per-installation bot token (not a global token). Trigger a 429 response from a mock Slack endpoint and verify the client honors the `Retry-After` header.

**Acceptance Scenarios**:

1. **Given** an installed workspace and a known user ID, **When** the ingestion handler asks the client for that user's profile, **Then** the call uses the installation's bot token retrieved through `secret_store.get(secret_ref)` and returns the user record.
2. **Given** a Slack rate-limit response (429 with `Retry-After`), **When** the client receives it, **Then** it waits the indicated duration and retries up to the configured cap, then surfaces a structured error if still rate-limited.
3. **Given** a transient network error, **When** the client encounters it, **Then** it retries with exponential backoff bounded by the same configured cap.

---

### Edge Cases

- **Forged team_id with no installation row**: returns 401 `unknown_installation`. Must NOT return 404 (which would let an attacker enumerate which workspaces are installed). Must NOT return 500. Must NOT log the offending `team_id` in plaintext — IN-07 SC-008 governs this and remains the controlling rule.
- **State token replay**: a state token replayed after expiry must be rejected. State tokens are single-use; replay before expiry is also rejected.
- **State token cross-tenant binding attempt**: an attacker who acquires a state token for tenant A and attempts to use it from a Slack workspace controlled by tenant B cannot bind workspace-B to tenant A. The `tenant_id` is bound at issuance time, signed, and verified at callback.
- **OAuth callback received without a recognized state token**: rejected before any Slack API call is made. No partial install.
- **Concurrent install attempts for the same `team_id`**: the OAuth callback is idempotent under the unique key `(provider, installation_id)`; the second attempt either updates the existing row to the new tokens (if it was already this tenant) or fails closed (if it would re-bind to a different tenant — Slack workspaces are not multi-tenant).
- **Race: webhook arrives mid-uninstall**: the disable-row write and the secret-delete must happen under the same transaction (or the secret-delete must be tolerant of "secret not found"), so a webhook arriving during the gap returns `unknown_installation`, not a 500.
- **Secret store unavailable**: if the secret store cannot be reached, the webhook signature path fails closed (rejects), it does not fall through to the legacy env-var path in prod. In dev (`WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`) it may fall back.
- **MASTER_KEK rotation**: out of scope for IN-08 (no operational rotation flow required), but the store interface must accept a rotated KEK on restart without re-encrypting at-rest data eagerly. Re-encryption-on-read is acceptable.
- **`app_uninstalled` arrives for a `team_id` we never installed**: handler must return 2xx (Slack expects ack) but write no audit row beyond a `received_uninstall_for_unknown_team` marker; must not 500.

## Requirements *(mandatory)*

### Functional Requirements

**Secret store (P1)**

- **FR-001**: The platform MUST provide a secret-store interface offering `put(plaintext, label) → ref`, `get(ref) → plaintext`, `rotate(ref, new_plaintext)`, and `delete(ref)`.
- **FR-002**: Stored material MUST be envelope-encrypted at rest using a master key (`MASTER_KEK`) injected from the deployment secret manager, never committed to the repo.
- **FR-003**: The secret store MUST expose a single concrete backend MVP (Fernet-with-MASTER_KEK) behind an interface that admits a future AWS KMS / GCP KMS backend without changing call sites.
- **FR-004**: `provider_installations.secret_ref` MUST become a typed pointer into the secret store (concrete ref), not an opaque string.
- **FR-005**: Webhook signature loading MUST resolve `provider_installations.secret_ref` via the secret store; the legacy env-var path MUST be reachable only when `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` is set, and that path MUST be off in environments marked production.

**Router cutover (P2)**

- **FR-006**: `services/webhooks/router.py` MUST resolve tenants by calling the IN-07 `TenantResolver` from `app.state` and MUST NOT import or call `services.webhooks.tenant_resolution.resolve_tenant`.
- **FR-007**: The router MUST translate the IN-07 outcomes `Resolved`, `UnknownInstallation`, and `PayloadMissing` into the existing 2xx-pass / 401 / 400 error shapes respectively.
- **FR-008**: A static check (grep or AST scan) MUST confirm zero references to `services.webhooks.tenant_resolution` in `services/webhooks/router.py` after the cutover; the legacy module MAY be deleted after Phase 1 + Phase 2 are both live in staging for 24 hours, but the references-must-be-zero invariant ships in this feature.

**Slack OAuth install (P3)**

- **FR-009**: The platform MUST expose `GET /integrations/slack/install` behind the existing Bearer-auth middleware; the response MUST be a 302 redirect to Slack's `oauth/v2/authorize` URL.
- **FR-010**: The state token in the OAuth redirect MUST be an HMAC-signed payload containing at minimum `tenant_id` (taken from the authenticated session, never from a query parameter), a `nonce`, and an `expiry_ts`; the HMAC key MUST be a server-only secret.
- **FR-011**: The platform MUST expose `GET /integrations/slack/callback` as a public (no Bearer) route whose authentication is the state token alone; this route MUST be added to the gateway's public-path allowlist as a *specific* route, not a prefix.
- **FR-012**: The callback handler MUST (a) verify state-token HMAC and expiry, (b) exchange `code` for tokens via Slack's `oauth.v2.access`, (c) persist bot/user/signing secrets via the secret store, (d) UPSERT a `provider_installations` row keyed by `(provider='slack', installation_id=team_id)` setting `tenant_id` from the verified state token, `secret_ref` to the new bot-token ref, and `enabled=true`, and (e) write an `installation_audit_log` row with `action='install'`.
- **FR-013**: The requested Slack OAuth scopes MUST be (minimum viable): `channels:history`, `groups:history`, `im:history`, `mpim:history`, `users:read`, `team:read`, plus event subscriptions for `message.*`, `app_mention`, `app_uninstalled`, `tokens_revoked`.
- **FR-014**: All state-token rejection paths and all OAuth-exchange failure paths MUST refuse to create or update a `provider_installations` row.

**Uninstall handling (P4)**

- **FR-015**: The Slack ingestion handler MUST branch on event type; for `app_uninstalled` and `tokens_revoked`, it MUST (a) look up the installation by `team_id`, (b) call `TenantResolver.disable_installation(installation_row_id)`, (c) delete the corresponding token material from the secret store, and (d) write an `installation_audit_log` row with `action='uninstall'`.
- **FR-016**: After a successful uninstall, the next inbound webhook for the same `team_id` MUST resolve to `UnknownInstallation` and return HTTP 401 with the IN-07 `unknown_installation` error code.
- **FR-017**: An `app_uninstalled` event for a `team_id` that has no matching row MUST be acknowledged (2xx) without raising and without modifying any state beyond an optional debug-level audit marker.

**Re-install handling (P5)**

- **FR-018**: When the OAuth callback runs for a `(provider, installation_id)` pair that already exists as a disabled row, it MUST update that same row (preserving `provider_installations.id`) and MUST NOT insert a duplicate; the new `secret_ref` MUST replace the prior one and the prior secret material MUST already be (or be eligible to be) deleted from the secret store.

**Outbound Slack client (P6)**

- **FR-019**: The platform MUST provide an async outbound Slack Web API client wrapping at minimum `chat.postMessage`, `users.info`, and `conversations.info`, using a per-installation bot token resolved through the secret store.
- **FR-020**: The client MUST honor Slack's rate-limit response codes (Tier 1–4 / 429 with `Retry-After`) and retry within a bounded budget; exhaustion produces a structured error consumable by callers.

**Security and audit invariants (cross-cutting)**

- **FR-021**: `installation_audit_log` MUST be tenant-scoped: `tenant_id UUID NOT NULL REFERENCES tenants(id)`, RLS enabled with the `tenant_isolation` policy, and indexes prefixed with `tenant_id` for all common predicates (Constitution §III).
- **FR-022**: Any token-storage table introduced under `lib/shared/secrets/` (or its companion migration) MUST be tenant-scoped to the same standard as FR-021, OR MUST encode `tenant_id` in the row that *references* it (i.e., `provider_installations`) and provide tenant-prefixed lookup paths.
- **FR-023**: No webhook code path may log the offending `team_id` (or any plaintext secret) when rejecting with `unknown_installation`; IN-07 SC-008 remains the controlling rule.
- **FR-024**: Substrate row IDs (any new `installation_audit_log` row, any new token-store row) MUST be allocated with `lib.shared.ids.uuid7()`; `uuid.uuid4()` is prohibited per Constitution §VII.
- **FR-025**: All new migrations MUST be additive (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, idempotent `DO` blocks) and MUST be numbered as the next free slot after `0039_provider_installations.sql` (Constitution §II).

### Key Entities *(this feature touches existing + new entities)*

- **`provider_installations` (existing, IN-07)**: Registry mapping `(provider, installation_id)` → `tenant_id`. After this feature, `secret_ref` becomes a typed pointer into the secret store; `enabled` controls whether webhooks route. NOT a Foundation — it is a per-tenant config registry, a permitted "side table for cross-cutting concerns" (Constitution §I).
- **`installation_audit_log` (new)**: Per-installation lifecycle audit trail (`install`, `uninstall`, future `token_refresh`). Tenant-scoped, RLS-enabled. NOT a Foundation — it is a side table for cross-cutting auditing concerns (Constitution §I explicitly permits this category). Distinct from `audit_events`, which captures Model state transitions (Constitution §VII).
- **Slack-installation token storage (new, location TBD by plan)**: Stores envelope-encrypted bot tokens, user tokens, signing secrets, and any future refresh metadata. Either a dedicated tenant-scoped table or a typed-row encoding inside the secret store. Tenant linkage is via `provider_installations.secret_ref` plus the tenant FK on `provider_installations`.
- **State token (new, ephemeral)**: HMAC-signed payload `{tenant_id, nonce, expiry_ts}` used as the *only* authentication on the OAuth callback. Not a substrate row; ephemeral in transit.
- **`Observation` (existing Foundation)**: The eventual downstream output of any inbound Slack message *after* this feature's plumbing succeeds. The install flow itself does NOT produce `Observations`; subsequent `slack:message` webhooks (per IN-06) do. This preserves the Universal Flow Rule (`input → Observation → Think → …`).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A Slack workspace admin can complete the install flow end-to-end without operator intervention — click → consent → return to Fyralis — and the first message sent in any subscribed channel appears as an `Observation` under the correct `tenant_id` within 30 seconds.
- **SC-002**: In any environment marked production, zero workspace signing secrets are present in environment variables, and zero token material is stored in plaintext in the database.
- **SC-003**: After a workspace uninstall, the very next inbound webhook for that workspace returns HTTP 401 `unknown_installation`, and `installation_audit_log` contains exactly one new `uninstall` row for that installation.
- **SC-004**: A re-install for a previously-uninstalled `team_id` reuses the same `provider_installations.id` row; the row count for `(provider='slack', installation_id=team_id)` remains 1 across the install→uninstall→install cycle.
- **SC-005**: A grep of the merged `services/webhooks/router.py` for `services.webhooks.tenant_resolution` returns zero matches.
- **SC-006**: Within one hour of merge to staging, the metric `webhook_resolver_outcomes_total{provider="slack", outcome="resolved"}` is non-zero.
- **SC-007**: A request carrying a forged `team_id` (no matching `provider_installations` row) is rejected with HTTP 401 `unknown_installation`; the response is neither 404 nor 500, and no log line contains the offending `team_id`.
- **SC-008**: The existing IN-07 admin test suite (`services/webhooks/tests/test_tenant_resolver_admin.py`) passes unchanged against the new code (i.e., `secret_ref` semantics remain compatible).
- **SC-009**: The OAuth state token is bound to the authenticated tenant at issuance time; an integration test exercising a swapped `tenant_id` in the redirect or callback is rejected before any Slack API call.
- **SC-010**: All new tenant-scoped tables (`installation_audit_log`, any token-storage table) carry `tenant_id` FK, ENABLE/FORCE RLS with the `tenant_isolation` policy, and at least one tenant-prefixed index on hot predicates.

## Constitution Alignment

The constitution check is not a separate gate document; it is encoded inline so the reviewer sees what was considered and why.

- **§I Four Foundations**: This feature introduces no new Foundation. `installation_audit_log` and any new token-storage table fall under "Per-feature side tables for cross-cutting concerns (cache, queue, audit, sidecar) are allowed and encouraged — they are not new foundations." The install flow does not generate `Observations`; it configures routing so subsequent webhook traffic can. Universal Flow Rule is preserved.
- **§II Append-only migrations**: Both new migrations (`NNNN_slack_installation_tokens.sql`, `NNNN_installation_audit_log.sql`) are additive and idempotent. Numbering is the next free slot after `0039_provider_installations.sql`. No edits to applied migrations.
- **§III Tenant isolation**: Both new tables get `tenant_id UUID NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY IMMEDIATE`, ENABLE + FORCE RLS with the `tenant_isolation` policy, and tenant-prefixed indexes. Hand-rolled `WHERE tenant_id = $1` remains required.
- **§IV Real DB in integration tests**: The new tests will use the `fresh_db` fixture and live Postgres; the Slack HTTP boundary may be mocked with `respx`, but Postgres is not.
- **§VII Audit & determinism**: Substrate-adjacent IDs use `uuid7()`. The `installation_audit_log` is a side audit table; it does NOT replace `audit_events`, which continues to govern Model state transitions.
- **§VIII Structured errors**: All new failure modes raise `CompanyOSError` subclasses (or reuse `UnknownInstallation` / `PayloadMissing` from IN-07) carrying `{code, message, context}`. No bare `Exception` or `ValueError` in domain code.
- **§X YAGNI**: A pluggable secret-store interface earns its keep because we have ≥2 deployment targets in plan (Fernet for MVP, KMS later) — same bar as the `Embedder` and LLM-provider abstractions. State-token issuance does not earn its own framework; a single signed-payload helper suffices.

### Flagged misalignments (none, but flagged for reviewer awareness)

- The task body uses the phrase "audit chain" in two senses: the constitution's `audit_events` chain (Model state transitions) and the new `installation_audit_log` (integration lifecycle). They are *not* the same chain. This spec uses "installation_audit_log" exclusively for the integration-lifecycle table to avoid that ambiguity in implementation.

## Scope Boundary (Verbatim from ClickUp `Files relevant`)

These are the only files this feature may create or modify. Any divergence is a user decision, not an implementation decision.

**New**:

- `services/integrations/slack/oauth.py` — install + callback handlers
- `services/integrations/slack/uninstall.py` — `app_uninstalled` / `tokens_revoked` event handler
- `services/integrations/slack/client.py` — outbound Slack Web API client (`chat.postMessage`, `users.info`, `conversations.info`)
- `services/integrations/router.py` — FastAPI router for `/integrations/slack/*` endpoints
- `lib/shared/secrets/` — encrypted-at-rest secret store (Fernet / KMS-pluggable) backing `provider_installations.secret_ref`
- `db/migrations/NNNN_slack_installation_tokens.sql` — per-installation bot/user OAuth tokens + refresh metadata
- `db/migrations/NNNN_installation_audit_log.sql` — install / uninstall / token-refresh audit trail

**Changed**:

- `services/webhooks/router.py` — swap env-var `resolve_tenant` for the IN-07 DB-backed `TenantResolver`
- `services/webhooks/secrets.py` — `load_secrets()` reads `provider_installations.secret_ref` via the secret store; env-var path demoted to dev-only fallback gated by `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`
- `services/webhooks/signatures/slack.py` — accept per-installation signing secret from the secret store
- `services/gateway/main.py` — mount the integrations router; add `/integrations/slack/install` and `/integrations/slack/callback` to the public-path allowlist (single-route, not blanket public)

New tests are *not* listed in `Files relevant` but are implicit per the constitution; new test files live under each touched service's `tests/` directory and are permitted.

## Assumptions

- Slack app registration (client_id, client_secret, signing secret for the *app* itself, not per-workspace) is provisioned out-of-band before merge and surfaced via deployment secrets (`SLACK_CLIENT_ID`, `SLACK_CLIENT_SECRET`).
- The authenticated session in Fyralis already exposes the active `tenant_id` to request handlers (this is what IN-07 and the Bearer middleware already assume); no new session machinery is in scope.
- The `provider_installations` table already has `enabled` boolean and `secret_ref` text columns (migration `0039`); the meaning of `secret_ref` becomes a typed reference but the column type does not change.
- `MASTER_KEK` is injected from the deployment secret manager (env in dev, K8s/AWS secret in prod). Operational rotation of `MASTER_KEK` is out of scope; the store interface must merely tolerate a value change at restart.
- Slack's `oauth.v2.access` returns the bot token (`xoxb-…`), optional user token (`xoxp-…`), and the per-installation signing secret in the response payload as documented; if Slack splits these across multiple endpoints, the OAuth handler fans out as needed within Phase 3's day-budget.
- The gateway's public-path allowlist is a configuration on `services/gateway/main.py` and accepts exact-route entries.
- Metrics labels (`webhook_resolver_outcomes_total{provider, outcome}`) were introduced in IN-07 and the labels `provider="slack"` and `outcome="resolved"` are already valid; SC-006 measures emission rate against the existing meter.
- "Production environment" is identifiable in code via an existing config flag (e.g., `ENV`, `DEPLOY_ENV`) that the secret-fallback gate can read.

## Dependencies

- **IN-06**: `slack:message` ingestion handler — receive-side already verifies HMAC and produces `Observations`.
- **IN-07**: DB-backed `TenantResolver`, `provider_installations` table (`0039`), and the `webhook_resolver_outcomes_total` meter — this feature *consumes* IN-07.
- **Slack API**: `oauth.v2.access`, `users.info`, `conversations.info`, `chat.postMessage`, plus the `app_uninstalled` / `tokens_revoked` event subscriptions.

## Out of Scope (Per ClickUp Task Body)

- App Home tab, slash commands, interactive components — tracked as **IN-10** once Acts can emit Slack-outbound messages.
- Migrating GitHub / Linear / Stripe / Discord onto the same OAuth pattern — tracked as **IN-09**, **IN-11**, etc. IN-08 is Slack-only by design so the pattern can be validated end-to-end on one provider before generalizing.
- Per-channel subscription management UI (which Slack channels Fyralis listens to) — out of scope; Slack app-level scopes cover MVP.
- Operational `MASTER_KEK` rotation flow — out of scope; only the interface tolerance is in scope.
