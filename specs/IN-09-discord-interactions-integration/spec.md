# Feature Specification: Discord Production Integration — Interactions HTTP, OAuth Install, Slash Command Self-Serve

**Feature Branch**: `feat/IN-09-discord-interactions-integration`
**Created**: 2026-05-14
**Status**: Draft
**Input**: ClickUp task `IN-09 [P0] Discord production integration (Interactions / OAuth)` (verbatim in [source.md](./source.md))

## Clarifications

### Session 2026-05-14

- Q: Concurrent bot-kick chokepoint semantics (two parallel 401s on a freshly-kicked installation) → A: **Idempotent re-runs** — match the IN-08 Slack uninstall pattern (`services/integrations/slack/uninstall.py::_disable_and_zeroize`). Both racing paths execute disable + delete + audit. The second `UPDATE provider_installations SET enabled=FALSE` is a benign no-op on an already-disabled row. The second `secret_store.delete(...)` raises `SecretNotFoundError` which is suppressed. Two `installation_audit_log` rows with `action='uninstall', status='ok'` and identical `installation_row_id` are accepted as the cost of lock-free correctness; dashboards that need a "unique kick events" count must `SELECT DISTINCT ON (installation_row_id, action)`. No row-level locking is added in the hot outbound path.
- Q: Slash-command registration verb (POST upsert vs PUT bulk vs one-time bootstrap) → A: **POST upsert per install** — every successful OAuth callback issues exactly one `POST /applications/{app_id}/commands` carrying the `/fyralis` command spec. Discord auto-upserts on `name` collision (returns the same command `id` on re-install). The HTTP call is redundant after the first install for the application's lifetime, but the redundancy is an acceptable cost for keeping the install flow self-contained: no admin bootstrap step, no pre-check GET, and the US4.1 contract test is the simple "mock recorded exactly one POST." PUT bulk-overwrite was rejected because it would silently delete commands added later (IN-13 buttons/modals would have to coordinate verb usage). A one-time bootstrap script was rejected because it adds an out-of-band setup step that breaks the "click → consent → working" self-serve contract in SC-001.
- Q: `content.text` rendering for slash-command Observations → A: **Option value verbatim, command metadata in `content.metadata`** — for `/fyralis ask "<query>"`, `content.text` is exactly `<query>` (e.g., `"What's our churn rate?"`), nothing prefixed, nothing JSON-wrapped. The substrate treats the slash-command shell as transport: `source_channel='discord:interaction'` already encodes "what application was invoked," and recording the verb in the text body would be structural noise that pollutes embeddings and retrieval. Command name, options, application_id, guild_id, channel_id, and member metadata land in `content.metadata` (a sub-object inside the existing `content` JSONB column — no new column). The `token` field from the interaction payload (Discord's per-interaction follow-up credential) is **stripped** before persistence: it is short-lived but credential-grade, and substrate rows must not leak it. This matches Slack's `content.text` semantics where the user's message lands verbatim, not `"slack:message: hello"`.

## Summary

Today, Fyralis can verify Discord webhook signatures (IN-06 shipped Ed25519 verification in `services/webhooks/signatures/discord.py`) and resolve `guild_id → tenant_id` via IN-07's `TenantResolver`. But there is no way for a Discord server admin to install the Fyralis bot themselves: onboarding still requires an operator to hand-insert a `provider_installations` row and ensure the application's Ed25519 public key is in `WEBHOOK_SECRET_DISCORD`. The existing ingestion handler at `services/ingestion/handlers/discord.py` is a stub that does not emit Observations. And the `/fyralis` slash command is not registered with Discord, so it does not appear in the command picker for any user. This feature closes that gap: a Discord server admin clicks "Add Fyralis to Server", completes OAuth + bot install, and within seconds `/fyralis ask "…"` invocations from any channel of that guild land as Observations under the correct `tenant_id` — no operator touch, no plaintext bot tokens, and bot-kicks correctly disable the row through an outbound 401 chokepoint.

The shape mirrors IN-08 (Slack production integration), but three distinctions are non-negotiable for Discord and must be preserved through plan and implementation: Ed25519 signatures (not HMAC-SHA256); the OAuth scope strings are `applications.commands` + `bot`; and the uninstall detection model is outbound-401-chokepoint, not webhook-driven (Discord does not emit a "bot removed" event over Interactions HTTP).

Passive ingest of every message in every channel (analogous to Slack's `message.channels`) requires Discord Gateway WebSocket, a third worker class, and the `MESSAGE_CONTENT` privileged intent. That work is explicitly **out of scope** for this task and tracked as IN-12.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Slash-Command Invocations Land as Observations (Priority: P1)

As a Fyralis-using team using Discord, when a member invokes `/fyralis ask "…"` in any channel of a guild where the bot is installed, the invocation must arrive at the gateway as a verified, tenant-resolved request and produce exactly one Observation under that guild's tenant within 3 seconds.

**Why this priority**: This is the ingestion contract. Until the existing stub handler at `services/ingestion/handlers/discord.py` actually emits Observations from `INTERACTION_CREATE` payloads (type=2 ApplicationCommand), the entire OAuth flow is a no-op from the user's point of view — they install the bot, type `/fyralis ask`, and nothing happens substrate-side. Every downstream User Story builds on this contract holding.

**Independent Test**: Seed a `provider_installations` row for a known `guild_id` mapped to a test tenant, then POST a synthetic signed Discord `INTERACTION_CREATE` (type=2) payload to `/webhooks/discord/events` (or whatever route IN-06 mounted) with the registered command name and an option value. Assert HTTP 200, exactly one new row in `observations` with `tenant_id` matching the test tenant, `source_channel='discord:interaction'`, `source_actor_ref='discord:<user_id>'`, and `content.text` containing the command + option value.

**Acceptance Scenarios**:

1. **Given** a guild has a valid `provider_installations` row and the bot's Ed25519 public key is loadable via the secret store, **When** a member invokes `/fyralis ask "foo"`, **Then** the gateway returns the Discord interaction acknowledgement within Discord's 3 s window AND a single Observation is committed under the right tenant.
2. **Given** the same setup, **When** two invocations of `/fyralis ask` arrive with the same interaction `id` (Discord retry within a few seconds), **Then** exactly one Observation is committed (idempotent on interaction `id`).
3. **Given** Discord sends an `INTERACTION_CREATE` with `type=1` (PING), **When** the gateway receives it, **Then** the response is `{"type": 1}` with HTTP 200 and **no Observation is committed** (PING carries no `guild_id` and is purely a handshake).
4. **Given** the Discord application public key is set in `WEBHOOK_SECRET_DISCORD` env var (dev fallback for PING) AND no `provider_installations` row yet exists, **When** Discord sends a PING during initial endpoint registration, **Then** signature verification still succeeds and the PING returns `{"type": 1}` — bootstrapping is possible without a pre-existing installation row.

---

### User Story 2 — Self-Serve OAuth Install Without Operator Intervention (Priority: P1)

As a Discord server admin onboarding to Fyralis, I want to click "Add Fyralis to Server" inside the Fyralis app, complete Discord's consent screen, and be redirected back to Fyralis with my bot installed in my chosen guild — no Fyralis operator action required.

**Why this priority**: This is the headline acceptance criterion. Without a self-serve OAuth flow, every new Discord customer requires manual `INSERT INTO provider_installations` and manual addition of the application public key to env vars — the exact anti-pattern IN-08 closed for Slack. This is the "production posture" half of the task; US1 alone is just receive-side wiring.

**Independent Test**: From a browser session authenticated as a Fyralis tenant, navigate to `GET /integrations/discord/install`. Verify (a) HTTP 302 to a `discord.com/oauth2/authorize` URL with the right `client_id`, `scope`, `permissions`, `redirect_uri`, and `state`. With Discord's `oauth.token` endpoint mocked, simulate the callback `GET /integrations/discord/callback?code=…&state=…`. Verify (b) a new `provider_installations` row exists for the returned `guild_id`, (c) a `discord_bot_token:<guild_id>` row exists in `encrypted_secrets`, (d) an `installation_audit_log` row with `action='install', status='ok'`, and (e) HTTP 302 to `/integrations/discord/installed?guild=<short_hash>`.

**Acceptance Scenarios**:

1. **Given** an authenticated Fyralis tenant clicks the Discord install button, **When** `/integrations/discord/install` is invoked, **Then** the response is a 302 to a Discord OAuth URL carrying a signed state token bound to that tenant's id (NOT a tenant id from a query parameter).
2. **Given** the user grants consent and Discord redirects back to `/integrations/discord/callback?code=…&state=…`, **When** the state-token HMAC and expiry verify and the nonce has not been consumed, **Then** the bot token from Discord's `oauth2/token` response is encrypted and stored, the installation row is created or updated, the slash command is registered with Discord, the audit row is written, and the user is 302'd to a Fyralis success page.
3. **Given** the state token is expired or already consumed, **When** the callback is invoked, **Then** the user is 302'd to `/integrations/discord/install-error?reason=state_invalid|state_expired|state_consumed` and no DB writes occur.
4. **Given** the `guild_id` returned by Discord already maps to a *different* tenant in `provider_installations`, **When** the callback runs, **Then** the response is a 302 to `/integrations/discord/install-error?reason=installation_collision`, an audit row is written with `status='rejected_collision'`, and the conflicting foreign tenant id is **not** leaked in logs or in the error response (mirrors IN-08 cross-tenant collision behavior).
5. **Given** the OAuth callback succeeds but the subsequent slash-command registration call to Discord returns a 4xx, **When** the failure is observed, **Then** the installation row is still written and audit row is `action='install', status='error'` with the Discord error code in the context; the install is recoverable via a documented manual re-register step (no orphaned half-installs).

---

### User Story 3 — Bot-Kick Detection via Outbound 401 Chokepoint (Priority: P2)

As a Discord server admin who removed Fyralis from a guild, I expect that the very next time Fyralis tries to talk back to that guild (a slash-command follow-up, a member lookup), the installation is automatically disabled and the bot token is zeroed — and any further `/fyralis` invocations from that guild are rejected with `unknown_installation`.

**Why this priority**: Discord does **not** emit a webhook event when a bot is kicked from a guild (this is the fundamental architectural difference from Slack's `app_uninstalled`). The only signal Fyralis can act on is the outbound API call returning 401. Without this chokepoint, removed installations would leak: their `provider_installations` row stays `enabled=TRUE` forever, the bot token sits encrypted but live in the secret store, and stray follow-up messages would 401 silently on every send.

**Independent Test**: Seed an enabled `provider_installations` row + valid bot token. Stub the Discord REST client to return 401 on the next outbound call. Trigger any outbound (e.g., a slash-command follow-up). Assert: installation row's `enabled` flips to FALSE, the bot token row in `encrypted_secrets` is deleted, an audit row with `action='uninstall', status='ok'` (or a dedicated status) is written, and the next inbound `/fyralis` invocation from that `guild_id` returns 401 `unknown_installation`.

**Acceptance Scenarios**:

1. **Given** an enabled installation and a stubbed outbound 401 on the next REST call to Discord, **When** any outbound from `services/integrations/discord/client.py` fires, **Then** the chokepoint disables the installation, deletes the bot token from the secret store, and writes an audit row.
2. **Given** a 403 with Discord `code=50001` (Missing Access) on an outbound, **When** the chokepoint observes it, **Then** the installation is treated equivalently to a 401 — disabled and zeroed.
3. **Given** the installation was just disabled via the chokepoint, **When** the next inbound `/fyralis` interaction arrives carrying that guild's `guild_id`, **Then** the response is 401 with `unknown_installation`, the `guild_id` does not appear in logs, and no Observation is committed.
4. **Given** the bot is re-added to the same guild after a prior kick, **When** the OAuth callback runs for the same `guild_id`, **Then** the existing (disabled) `provider_installations.id` is reused — `enabled` flips back to TRUE, `secret_ref` is updated, no duplicate row and no unique-constraint conflict.

---

### User Story 4 — `/fyralis` Slash Command Appears in Discord's Command Picker (Priority: P2)

As a Discord member in a guild that has just installed Fyralis, I expect to type `/` in any channel and see `/fyralis ask` in the suggestion list with its registered description, so I can discover and invoke it.

**Why this priority**: The OAuth install (US2) is the *plumbing* for command availability, but command **registration** is the user-facing payoff. If we install the bot but never call Discord's `POST /applications/{app_id}/commands` endpoint, the command never appears in any user's typeahead and the integration is invisible. Registration runs once per install (on OAuth callback success) — not on every gateway boot, which would be a thundering-herd waste on a busy fleet.

**Independent Test**: After running the OAuth flow against a mocked Discord API, assert that the mock recorded a single `POST /applications/{app_id}/commands` call carrying the expected command spec (`name='fyralis'` with subcommand or option `ask` of type STRING, required, with a non-empty description). Re-running the install for the same guild should be a no-op (or idempotent overwrite) — never a duplicate.

**Acceptance Scenarios**:

1. **Given** the OAuth callback succeeds with a fresh bot token, **When** the post-callback hook runs, **Then** Discord's commands API is called exactly once with the `/fyralis` command spec using the freshly-issued token (not an env-var token).
2. **Given** a re-install of the same guild after the command was already registered, **When** the callback runs again, **Then** the OAuth callback issues exactly one `POST /applications/{app_id}/commands` carrying the `/fyralis` command spec, Discord upserts by `name` and returns the same command `id` as the prior registration, and no duplicate command surfaces in Discord's UI.
3. **Given** registration fails with a Discord 4xx (e.g., scope insufficient), **When** the callback observes the failure, **Then** the install completes (US2.5) but the audit row carries `status='error'` and the error code; a documented manual recovery path exists.

---

### User Story 5 — Outbound Discord REST Client With Per-Installation Tokens and Rate-Limit Backoff (Priority: P3)

As the platform, I need a single outbound Discord REST client that always resolves the bot token via the secret store keyed on the target `guild_id`, honors Discord's rate-limit headers, and never retries beyond a bounded budget — so that downstream features (interaction follow-ups, member enrichment, channel-name lookup) cannot accidentally exhaust a guild's rate-limit budget or fall back to an env-var token.

**Why this priority**: Phase 1 (US1) emits Observations but the user only sees Discord's auto-ack ("This interaction failed" or the slash command echo). Real product value requires Fyralis to *reply* with a follow-up message — and that requires the outbound client. This is also the chokepoint where US3's 401 detection lives.

**Independent Test**: Construct a mock Discord API that returns 200, then 429 with `Retry-After: 1`, then 200. Issue three sequential outbounds via `DiscordClient.post_followup_message`. Assert: 3 attempts, retry honors the `Retry-After`, total wall < 30 s, returns the final 200 response. Separately, a mock returning 401 on the first attempt must trigger the US3 chokepoint exactly once and raise a `DiscordApiError` upstream.

**Acceptance Scenarios**:

1. **Given** an outbound `chat.postFollowupMessage`-style call for installation `inst-A`, **When** the client constructs the request, **Then** the bot token comes from `secret_store.get` keyed on the `discord_bot_token:<guild_id>` label — never from `os.environ` and never from a per-class default.
2. **Given** a 429 response with `Retry-After: 2` on attempt 1, **When** the client retries, **Then** the second attempt is dispatched ≥ 2 seconds later, the budget tracks total wall ≤ 30 s, and no more than 3 total attempts occur.
3. **Given** a 401 on attempt 1, **When** the client observes it, **Then** US3's chokepoint fires exactly once and the call raises `DiscordApiError` to the caller — no silent retry against a token we already know is dead.
4. **Given** the bot token is missing from the secret store (orphaned `secret_ref`), **When** the client tries to load it, **Then** the call fails with a structured error and the installation is disabled; the caller does not retry against a dangling installation.

---

### Edge Cases

- **PING bootstrap**: Before any `provider_installations` row exists, Discord's Developer Portal lets you set an Interactions Endpoint URL and immediately PINGs it. The handler must succeed using only the env-var Ed25519 public key (`WEBHOOK_SECRET_DISCORD`) — there is no `guild_id` to look up.
- **Concurrent re-install + uninstall race**: An admin re-installs a bot just as the chokepoint is about to disable the row from a 401. The UPSERT in the OAuth callback must reconcile cleanly (later write wins; audit chain reflects both events) without losing the new bot token.
- **Slash command registration after token revocation**: An admin manually rotates the application secret in Discord's Developer Portal while a callback is mid-flight. The `POST /applications/.../commands` call uses the *just-issued* bot token, which Discord will accept; the audit row records success even though the application secret rotated.
- **Cross-tenant collision via guild transfer**: A guild that was installed under tenant A is transferred to tenant B (Discord ownership transfer is possible). On the next OAuth callback for that guild under tenant B, the collision detection in US2.4 fires — admin is shown `installation_collision`, no foreign tenant id leaks.
- **Forged guild_id**: An attacker probes `/webhooks/discord/events` with a valid Ed25519 signature for one guild but a forged `guild_id` for another. Signature verification will *fail first* (the deferred-rejection ordering IN-08 established must hold for Discord too) — the response is 401 with no guild_id in logs.
- **PING signature using the wrong application's key**: A different Discord application PINGs our endpoint. Ed25519 verification fails because the public keys differ — return 401 without claiming to know which app is wrong.
- **Bot kicked between OAuth callback and command registration**: The OAuth callback succeeds, then in the same handler the command registration call returns 401 (kicked in the ~50 ms gap). The installation row is rolled back via the chokepoint in the same request; the user is 302'd to `install-error?reason=installation_unstable` or similar (the redirect is a 200 from Discord's POV but the user sees the error page).
- **Long-option slash command**: A user types `/fyralis ask "<very long text...>"`. The handler must accept up to Discord's documented max (currently 6000 chars for command options) without truncation; longer inputs are rejected by Discord before reaching us.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST emit exactly one Observation per Discord `INTERACTION_CREATE` payload of `type=2` (ApplicationCommand), with `source_channel='discord:interaction'`, `source_actor_ref='discord:<user_id>'`, `kind='signal'`, `trust_tier='attested_agent'`, `content.text` set to the primary string option's value verbatim (the user's query, with no command-verb prefix), and `content.metadata` carrying the full interaction payload with the per-interaction `token` field stripped (command name, application_id, guild_id, channel_id, member info, all options).
- **FR-002**: Observations from Discord interactions MUST be idempotent on the interaction `id` — a duplicate POST for the same interaction must NOT produce a second Observation.
- **FR-003**: The webhook router MUST recognise Discord PING payloads (`type=1`) and return `{"type": 1}` with HTTP 200, but only AFTER successful Ed25519 signature verification using the application-level public key from the `WEBHOOK_SECRET_DISCORD` env-var fallback (PING carries no `guild_id`, so the DB-backed secret-store path is bypassed via the same `tenant_id=None` mechanism Slack's `url_verification` uses). An unsigned or wrongly-signed PING MUST be rejected with the standard signature-verification 401 — never short-circuited. No Observation is emitted for PING.
- **FR-004**: The webhook router MUST verify Discord interaction signatures using Ed25519 (NOT HMAC-SHA256) against the per-installation public key from the secret store (label `discord_public_key:<guild_id>`), falling back to `WEBHOOK_SECRET_DISCORD` env var ONLY when (a) the payload has no `guild_id` (PING) or (b) the dev-only env fallback flag `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` is set.
- **FR-005**: For any non-PING Discord interaction, the system MUST resolve `tenant_id` from `provider_installations` by `(provider='discord', installation_id=guild_id)`. A missing or disabled row MUST cause a 401 `unknown_installation` response, and the `guild_id` MUST NOT appear in log lines for the rejection.
- **FR-006**: `GET /integrations/discord/install` MUST require Bearer authentication via the existing gateway middleware and MUST 302 to a Discord OAuth URL whose state parameter is a signed token derived from the authenticated tenant's id (NOT from any query parameter or request header).
- **FR-007**: The state token MUST be single-use: the OAuth callback MUST atomically mark the nonce consumed in `oauth_install_states` and refuse a second use of the same token.
- **FR-008**: `GET /integrations/discord/callback` MUST be on the gateway public-path allowlist as an exact-match path (NOT `/integrations/discord/*` as a prefix).
- **FR-009**: On successful OAuth callback the system MUST: (a) call Discord's `oauth2/token` endpoint to exchange the code; (b) encrypt and store the bot token in `encrypted_secrets` with label `discord_bot_token:<guild_id>`; (c) encrypt and store the application Ed25519 public key in `encrypted_secrets` with label `discord_public_key:<guild_id>`; (d) UPSERT a `provider_installations` row for `(provider='discord', installation_id=guild_id)` with `secret_ref` pointing at the public-key row; (e) write an `installation_audit_log` row with `action='install', status='ok'`; (f) register the `/fyralis` slash command with Discord via `POST /applications/{app_id}/commands` using the freshly-issued bot token (Discord upserts on `name` collision; the response carries the persistent command `id`); (g) 302 to `/integrations/discord/installed?guild=<short_hash>`.
- **FR-010**: When the OAuth callback's `guild_id` already maps to a *different* tenant in `provider_installations`, the system MUST treat this as a cross-tenant collision: write an audit row with `action='install', status='rejected_collision'`, 302 the admin to `/integrations/discord/install-error?reason=installation_collision`, and emit no log line containing the conflicting tenant's id or the guild_id.
- **FR-011**: When the OAuth callback's `guild_id` already maps to the *same* tenant but the row is disabled (post-kick re-install), the system MUST reuse the existing `provider_installations.id`: flip `enabled` to TRUE, refresh `secret_ref`, and write an audit row — never insert a duplicate row.
- **FR-012**: If the slash-command registration call to Discord fails with a 4xx after a successful token exchange, the installation row MUST still be written and the audit row MUST carry `status='error'` with the Discord error code in `context`; the user is 302'd to a recoverable error page.
- **FR-013**: An outbound Discord REST client MUST be the single chokepoint for all calls to `discord.com/api`. Every call MUST resolve the bot token from the secret store keyed on the target `guild_id`. No outbound MAY read a bot token from `os.environ` or a per-class default.
- **FR-014**: The outbound client MUST honor Discord's `X-RateLimit-Remaining` and `Retry-After` headers and MUST bound retries to ≤ 3 attempts and ≤ 30 seconds total wall time per call. Beyond the budget, the call MUST raise a structured error to its caller.
- **FR-015**: When the outbound client observes a 401 (or a 403 with Discord `code=50001`), it MUST trigger the bot-kick chokepoint: disable the installation row, delete the bot token from `encrypted_secrets`, write an audit row with `action='uninstall', status='ok'`, and invalidate any in-process tenant-resolver cache entry for that `guild_id`. The original call MUST then raise to its caller (no silent retry). The chokepoint MUST be safe to invoke concurrently: each operation is independently idempotent — `UPDATE provider_installations SET enabled=FALSE` is a no-op on an already-disabled row, `secret_store.delete()` suppresses `SecretNotFoundError`, and concurrent fires may produce up to N audit rows for N concurrent observers of the same kick (acceptable; see Clarifications). No row-level locking (`SELECT … FOR UPDATE`) is used.
- **FR-016**: The system MUST NOT modify any file under `services/integrations/slack/`. Every existing IN-08 test MUST continue to pass byte-for-byte.
- **FR-017**: The system MUST mount `/integrations/discord/install` and `/integrations/discord/callback` via the existing IN-08 integrations router factory (`services/integrations/router.py`) — no new top-level router class.
- **FR-018**: The system MUST reuse IN-08's `lib/shared/secrets/` module unchanged for all bot-token and public-key storage. No Discord-specific secret-store code may be added.
- **FR-019**: The system MUST reuse IN-08's `oauth_install_states` table for Discord state-token tracking, and reuse `installation_audit_log` for all install/uninstall audit rows. No Discord-specific tables are added.
- **FR-020**: When `FYRALIS_ENV=prod` AND `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` are both set, the gateway MUST fail-fast at startup with the same invariant assertion IN-08 established (`assert_prod_safety_invariants`). This guarantees the Discord env-var path is never the production path.

### Key Entities

- **Discord Installation**: A row in `provider_installations` where `provider='discord'` and `installation_id` is the Discord `guild_id`. Maps a guild to a tenant. Holds an `enabled` flag (FALSE after bot-kick), a `secret_ref` pointing at the encrypted application public key in `encrypted_secrets`, and timestamps.
- **Encrypted Bot Token**: A row in `encrypted_secrets` with label `discord_bot_token:<guild_id>`, tenant-scoped, holding the AES-via-Fernet ciphertext of the OAuth-issued bot token. Deleted on bot-kick chokepoint.
- **Encrypted Application Public Key**: A row in `encrypted_secrets` with label `discord_public_key:<guild_id>`, tenant-scoped, holding the ciphertext of the Discord application's Ed25519 public key. The value is identical across rows of the same Fyralis Discord application; it is mirrored per-installation so `load_secrets`'s DB path resolves cleanly. Deleted on bot-kick chokepoint.
- **OAuth State Token**: A signed, expiring nonce binding a tenant to an in-flight OAuth callback. Stored in the `oauth_install_states` table (reused from IN-08); single-use via atomic `UPDATE … RETURNING` on the `consumed_at` column.
- **Installation Audit Row**: A row in `installation_audit_log` (reused from IN-08) recording each install / uninstall / token_refresh / rejected_collision event with `provider='discord'`, `tenant_id`, `installation_row_id`, `action`, `status`, and a JSONB `context`.
- **Discord Interaction (type=2)**: A `INTERACTION_CREATE` payload from Discord representing a user invoking an ApplicationCommand (`/fyralis ask "..."`). Carries `id` (idempotency key), `application_id`, `guild_id`, `channel_id`, `member.user.id` (user_id), and `data.name` + `data.options[]`.
- **Discord PING (type=1)**: A handshake payload Discord sends when an admin sets the Interactions Endpoint URL in the Developer Portal. Has no `guild_id`. The handler returns `{"type": 1}` after Ed25519 verification using the env-var public key.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A Discord server admin can complete the install flow end-to-end (click → consent → redirect → first `/fyralis` invocation lands as an Observation) in under 60 seconds of wall clock, with zero operator interventions.
- **SC-002**: For 100% of slash-command invocations from any guild with a valid installation, the corresponding Observation row exists in the database within 3 seconds of Discord delivering the interaction.
- **SC-003**: For 100% of duplicate Discord interaction deliveries (Discord retries with the same `interaction.id`), exactly one Observation is produced — zero duplicates.
- **SC-004**: Re-installing a bot into a guild that was previously installed under the same tenant reuses the same `provider_installations.id` row in 100% of cases — never inserts a duplicate, never triggers a unique-constraint conflict.
- **SC-005**: 100% of bot-kick events surface in the database (installation `enabled=FALSE`, bot token row deleted, audit row written) within one outbound Discord REST call of the kick happening — no idle-rot.
- **SC-006**: Zero log lines mention a forged or unknown `guild_id` in error paths. The only log information about a 401 `unknown_installation` is the outcome itself.
- **SC-007**: Zero bot tokens or application public keys exist in plaintext on disk, in environment variables, or in database columns in any environment where `FYRALIS_ENV=prod`. (Env-var fallback for PING bootstrap is dev-only and the gateway refuses to boot in prod with both flags on.)
- **SC-008**: `webhook_resolver_outcomes_total{provider="discord", outcome="resolved"}` is non-zero in staging within 1 hour of merge, observed via the existing IN-07 metrics surface.
- **SC-009**: Zero modifications to any file under `services/integrations/slack/`; all IN-08 tests pass without changes — verified by re-running the IN-08 test suite as part of the IN-09 CI gate.
- **SC-010**: A re-installable, idempotent installation: starting from `enabled=FALSE` (post-kick), running the OAuth callback restores `enabled=TRUE`, refreshes `secret_ref`, deletes the prior bot-token row from `encrypted_secrets` if it lingered, and inserts the new one — all in a single transaction with zero orphans.

## Assumptions

- **Discord application is pre-created**: A Fyralis operator has already created the Discord application in the Developer Portal, configured the OAuth redirect URI to `<gateway-base>/integrations/discord/callback`, set the Interactions Endpoint URL to `<gateway-base>/webhooks/discord/events`, and copied the application's Client ID, Client Secret, and Public Key into the deployment env (`DISCORD_CLIENT_ID`, `DISCORD_CLIENT_SECRET`, `WEBHOOK_SECRET_DISCORD`). These env vars hold app-level identifiers/secrets, not per-tenant material — they do not violate the "no plaintext per-tenant secrets in env" rule.
- **One Discord application per Fyralis deployment**: For v1 there is exactly one Discord application backing all tenants (one Client ID / Client Secret / Public Key triple). Multi-tenant brand differentiation (each tenant gets their own Discord app) is a follow-up.
- **Bot installs to one guild per OAuth flow**: Each OAuth callback corresponds to a single `guild_id`. A user installing into multiple guilds goes through the flow once per guild — Discord's standard behavior.
- **Slash command spec is fixed at `/fyralis ask <query>`**: A single global command, one required string option. Per-guild or per-tenant command customisation is out of scope.
- **Discord rate limits operate as documented**: Tier-based rate limits with `X-RateLimit-Remaining` and `Retry-After` headers. No special enterprise-tier handling.
- **Discord's `oauth2/token` returns `guild` object on bot install**: Standard Discord OAuth-v2 behavior when the `bot` scope is in the request — the response includes the `guild.id` we need for the installation row.
- **The IN-08 secret store is the only encrypted-at-rest path**: No KMS integration in v1; Fernet over `MASTER_KEK` env-injected at startup, identical to Slack. (Pluggable backend remains open for future GCP/AWS KMS migration without schema change.)
- **The existing `services/webhooks/router.py` is provider-agnostic post-IN-08**: The Discord receive path runs through the same router that already serves Slack and Stripe. This task should not touch router-internal control flow except to verify the Discord PING short-circuit ordering.
- **IN-08 must merge before IN-09 ships**: IN-09 hard-depends on `lib/shared/secrets/`, `encrypted_secrets`, `oauth_install_states`, and `installation_audit_log`. The IN-09 branch is created off `feat/IN-08-slack-production-integration` and will rebase onto `main` once IN-08 merges.
- **Discord Gateway WebSocket ingest is explicitly deferred (IN-12)**: Full message-stream ingestion (every message in every channel, analogous to Slack's `message.channels`) requires a third worker class, the `MESSAGE_CONTENT` privileged intent (Discord manual verification once >100 guilds), reconnection/heartbeat logic, and gateway sharding. None of that is in this task.
