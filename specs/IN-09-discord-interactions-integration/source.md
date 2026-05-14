IN-09 [P0] Discord production integration (Interactions / OAuth)

Files relevant

New

services/integrations/discord/__init__.py — package init
services/integrations/discord/oauth.py — install + callback handlers (mirrors services/integrations/slack/oauth.py)
services/integrations/discord/commands.py — registers / synchronises the `/fyralis` slash command(s) with Discord (POST /applications/{app_id}/commands)
services/integrations/discord/client.py — outbound Discord REST client (interaction-token follow-ups, GET /guilds/{guild_id}/members/{user_id}, GET /channels/{channel_id}) with rate-limit + Retry-After handling
services/integrations/discord/uninstall.py — handle "bot removed from guild" via the outbound 401 chokepoint (Discord does not push a clean uninstall webhook to the Interactions HTTP endpoint)
services/integrations/discord/metrics.py — discord_install_outcomes_total / discord_uninstall_outcomes_total counters mirroring slack/metrics.py

Changed

services/integrations/router.py — mount `/integrations/discord/install` (Bearer-authed) and `/integrations/discord/callback` (public, state-token-authed) alongside the existing Slack routes
services/gateway/main.py — add `/integrations/discord/callback` to `_PUBLIC_PATHS` exact-match set (single-route, not the `/integrations/` prefix); reuse existing `_wire_in08_state` (secret_store + tenant_resolver) — no new app-state plumbing
services/ingestion/handlers/discord.py — extend the existing stub to handle Interaction type=2 (ApplicationCommand) payloads: extract command name, options, member, guild_id, channel_id → emit Observation with `source_channel='discord:interaction'` and `kind='signal'` (same shape Slack uses)
services/webhooks/router.py — confirm the Discord PING short-circuit (`_is_discord_ping`) remains intact after IN-08's deferred-tenant-rejection ordering; add a regression test if missing
services/webhooks/tenant_resolver.py — verify `guild_id` extraction works for Discord interaction payloads (top-level `guild_id` for slash commands; absent for PING which short-circuits earlier)
services/webhooks/signatures/discord.py — accept per-installation Ed25519 public key from the secret store (label `discord_public_key:<guild_id>`) with env-var `WEBHOOK_SECRET_DISCORD` as the app-level fallback for the PING handshake (PING carries no guild_id and uses the application's app-level public key)

Why it is needed

After IN-06 + IN-07 + IN-08, the Slack stack is fully self-serve:
- /webhooks/slack/* verifies HMAC + resolves tenant + ingests
- /integrations/slack/install + /callback let a workspace admin onboard without operator intervention

Discord is half-built:
- services/webhooks/signatures/discord.py (Ed25519) exists from IN-06
- services/webhooks/tenant_resolver.py and the router already accept Discord traffic
- services/ingestion/handlers/discord.py is a stub that does not actually emit Observations
- There is no OAuth install flow; onboarding still requires INSERT INTO provider_installations by hand AND the per-app public key in WEBHOOK_SECRET_DISCORD — the same anti-pattern IN-08 closed for Slack
- The /fyralis slash command is not registered with Discord, so users cannot invoke it

This task closes the gap so a Discord server admin can click "Add Fyralis to Server", complete the OAuth + bot install flow, and immediately have /fyralis invocations land as Observations under the right tenant within 3 s — with zero operator intervention.

Full passive message ingest (every message in every channel) is a different architecture — Discord Gateway WebSocket — and is deferred to IN-12. This task is intentionally scoped to the Interactions HTTP pattern that matches our existing webhook router and the two-worker substrate.

How it can be done

Land in 5 ordered phases, each independently deployable:

Phase 1 — Ingestion + slash command (1 d)

Extend services/ingestion/handlers/discord.py to:
- handle Interaction type=1 (PING) → ack with `{"type": 1}` (router already does this; handler must be safe if called)
- handle Interaction type=2 (ApplicationCommand) → emit Observation with source_channel='discord:interaction', source_actor_ref='discord:<user_id>', content.text = the command + its option values rendered as plain text, content.metadata = full interaction payload minus the token, kind='signal', trust_tier='attested_agent'

services/integrations/discord/commands.py registers the `/fyralis ask` global slash command (one option: `query: string, required`) via POST https://discord.com/api/v10/applications/{app_id}/commands using the bot token from the secret store. Run-on-install — not on every gateway boot.

Phase 2 — Tenant resolver wiring (0.25 d)

Confirm services/webhooks/tenant_resolver.py extracts top-level guild_id from Discord interactions. PING has no guild_id; the router's `_is_discord_ping` short-circuit must run BEFORE tenant resolution (analogous to Slack url_verification). Add a regression test that posts a real interaction payload through the resolver and asserts Resolved with the right tenant.

Phase 3 — Discord OAuth install flow (1 d)

GET /integrations/discord/install — Bearer-authed. Mints a state token via the existing services.integrations.slack.oauth.issue_state_token helper (refactor to a provider-agnostic name as part of this task — small, contained move). 302s to:
  https://discord.com/oauth2/authorize?client_id=<DISCORD_CLIENT_ID>&scope=applications.commands+bot&permissions=<minimal>&redirect_uri=<callback>&state=<token>&response_type=code

GET /integrations/discord/callback?code=...&state=... — public route, state-token-verified. Steps:
1. Verify state-token HMAC + expiry + nonce single-use consumption (reuse the existing oauth_install_states table).
2. Exchange code via POST https://discord.com/api/v10/oauth2/token (form-urlencoded grant_type=authorization_code).
3. Read guild_id from the response (Discord returns the chosen guild on bot install).
4. secret_store.put the bot token, label `discord_bot_token:<guild_id>`.
5. secret_store.put the application public key (from env, app-level), label `discord_public_key:<guild_id>` — this gives per-installation lookups while reusing IN-08's load_secrets DB path.
6. INSERT/UPSERT provider_installations(provider='discord', installation_id=guild_id, tenant_id, secret_ref=<public_key_ref>, enabled=TRUE). Same cross-tenant collision detection pattern as Slack (`ON CONFLICT WHERE tenant_id = EXCLUDED.tenant_id`).
7. INSERT installation_audit_log(action='install', status='ok').
8. POST the /fyralis slash command registration (Phase 1 helper).
9. 302 to /integrations/discord/installed?guild=<short_hash>.

Scopes (minimum viable): applications.commands (register & receive slash commands), bot (install the bot user). Permissions: send_messages, view_channel — Phase 5 follow-up messages need send_messages; ingestion does not need view_channel for slash commands but it is convention to request it.

Phase 4 — Uninstall / removal handling (0.5 d)

Discord does not emit a webhook event when a bot is kicked from a guild. Detection strategy:
- Outbound REST 401 chokepoint: services/integrations/discord/client.py wraps every Discord API call; on 401 Unauthorized or 403 with `code=50001`, calls handle_token_invalid(installation_row_id) which disable_installation + secret_store.delete(<bot_token_ref>) + audit row.
- No periodic reconciliation job in v1 (would require a third worker class — out of scope, deferred to IN-12 sibling).
- Manual disable still available via scripts/webhook_install.py disable --id <uuid>.

Re-install after uninstall: the OAuth callback detects the existing-but-disabled row and calls enable_installation + update_secret_ref — same code path as Slack (FR-018 / SC-004), no duplicate row.

Phase 5 — Outbound Discord REST client (0.5 d)

services/integrations/discord/client.py: thin async wrapper around:
- POST /webhooks/{app_id}/{interaction_token} — follow-up messages after the 3 s initial ack window
- GET /guilds/{guild_id}/members/{user_id} — enrich source_actor_ref with display name
- GET /channels/{channel_id} — enrich channel name on Observations

Per-installation bot-token lookup via secret store. Discord rate limits: honor X-RateLimit-Remaining + Retry-After headers with the same bounded-budget pattern as slack/client.py (3 attempts, 30 s wall, no infinite backoff).

Acceptance criteria

A Discord server admin can complete the install flow end-to-end without operator intervention: click → Discord consent screen → land back on Fyralis → first /fyralis invocation in any channel of that guild lands as an Observation under the correct tenant_id within 3 s.

Bot tokens are never stored in env vars or plaintext at rest in any environment marked prod (FYRALIS_ENV=prod + WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1 still hard-fails startup as IN-08 established).

The /fyralis slash command appears in Discord's command picker (with the registered description) in any guild where the bot is installed, immediately after the OAuth callback completes.

Removing the bot from a guild causes the very next outbound Discord API call from Fyralis to that guild to flip the installation to enabled=FALSE and zero the bot token; the subsequent /fyralis invocation from that guild returns unknown_installation (401) — same shape as Slack uninstall.

Re-installing after uninstall reuses the same provider_installations.id (no orphans, no unique-constraint conflict, audit row chain preserved).

services/integrations/slack/* is not modified; all existing IN-08 tests continue to pass byte-for-byte.

webhook_resolver_outcomes_total{provider="discord", outcome="resolved"} is non-zero in staging within 1 h of merge.

Negative case: a request with a forged guild_id for which no row exists returns 401 with unknown_installation (not 404, not 500, no log leak of the guild_id — IN-07 SC-008 holds).

Discord PING (interaction type=1) at the callback URL — a fresh Discord App config will probe this — returns `{"type": 1}` with HTTP 200 even when no provider_installations row exists for the originating application (PING uses app-level public key from WEBHOOK_SECRET_DISCORD env fallback).

Security / constitution notes

No new tables — reuses encrypted_secrets, oauth_install_states, installation_audit_log, provider_installations from IN-08. Schema is provider-agnostic by design.

The OAuth state token MUST carry tenant_id from the authenticated session (same constraint as Slack). Discord callback is public-route but state-token-authed; added to allowlist as a single exact-match path, not a prefix.

Discord bot tokens at rest: envelope-encrypted via the IN-08 secret store. MASTER_KEK injected from the deployment secret manager.

The Discord application public key is app-level (one per Fyralis Discord App, shared across all guilds). It is mirrored into encrypted_secrets per-installation only so that load_secrets's DB-backed path returns it cleanly — the value is identical across rows. WEBHOOK_SECRET_DISCORD env-var fallback covers the PING handshake (no guild_id, no installation) the same way WEBHOOK_SECRET_SLACK covers Slack's url_verification.

The /fyralis command registration call uses the freshly-issued bot token from the OAuth response — never an env-var bot token. If registration fails (Discord 4xx), the callback returns the OAuth as successful but writes an audit row action='install', status='error' with the Discord error code; manual recovery via scripts/webhook_install.py is documented.

Out of scope (follow-up tasks)

Full message ingest via Discord Gateway WebSocket — track as IN-12. Requires a third worker class (persistent WS connection), MESSAGE_CONTENT privileged intent (Discord manual verification once >100 guilds), reconnection/heartbeat logic, gateway sharding for scale. Out of scope here.

Discord interactive components beyond slash commands (buttons, select menus, modals) — track as IN-13.

Per-guild slash command customization UI — for v1 the `/fyralis ask` command is a single global registration with one string option. Customisation is a follow-up.

GitHub / Linear / Stripe OAuth via the same pattern — IN-10 / IN-11 / etc. This task is Discord-only by design so the pattern generalises cleanly from Slack (IN-08) to a second provider before being extrapolated to N.

Periodic guild membership reconciliation (sweep for bots that were kicked while idle) — deferred to IN-12 sibling. The outbound 401 chokepoint covers active guilds; idle guilds will detect-on-next-use.

Estimated effort

3.25 days (1 d Phase 1, 0.25 d Phase 2, 1 d Phase 3, 0.5 d Phase 4, 0.5 d Phase 5).
