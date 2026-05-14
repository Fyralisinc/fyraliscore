IN-08 [P0] Slack production integration — OAuth install, DB-backed secrets, end-to-end customer self-serve
IN-08 [P0] Slack production integration

Files relevant

New

services/integrations/slack/oauth.py — install + callback handlers
services/integrations/slack/uninstall.py — app_uninstalled / tokens_revoked event handler
services/integrations/slack/client.py — outbound Slack Web API client (chat.postMessage, users.info, conversations.info)
services/integrations/router.py — FastAPI router for /integrations/slack/* endpoints
lib/shared/secrets/ — encrypted-at-rest secret store (Fernet / KMS-pluggable) backing provider_installations.secret_ref
db/migrations/NNNN_slack_installation_tokens.sql — per-installation bot/user OAuth tokens + refresh metadata
db/migrations/NNNN_installation_audit_log.sql — install / uninstall / token-refresh audit trail

Changed

services/webhooks/router.py — swap env-var resolve_tenant for the IN-07 DB-backed TenantResolver
services/webhooks/secrets.py — load_secrets() reads provider_installations.secret_ref (resolved via the secret store); env-var path demoted to dev-only fallback gated by WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1
services/webhooks/signatures/slack.py — accept per-installation signing secret from the secret store
services/gateway/main.py — mount the integrations router; add /integrations/slack/install and /integrations/slack/callback to the public-path allowlist (single-route, not blanket public)

Why it is needed

Post IN-06 + IN-07, the receive side of Slack works:

/webhooks/slack/* verifies HMAC signatures and runs the slack:message ingestion handler.
provider_installations maps team_id → tenant_id (IN-07).

But there is no way for a customer to actually install our Slack app themselves:

Onboarding requires a Fyralis operator to INSERT INTO provider_installations by hand AND set WEBHOOK_SECRET_SLACK__<TENANT_HEX> env vars on the gateway. That is a private-demo posture, not a SaaS posture.
services/webhooks/router.py still imports the env-var resolver services.webhooks.tenant_resolution.resolve_tenant, not the DB-backed TenantResolver from IN-07 — IN-07 shipped the engine, but nothing calls it.
Workspace signing secrets live in plaintext environment variables — not acceptable for multi-tenant prod.
We cannot detect or react to a workspace uninstalling our app, so provider_installations rows leak forever and a re-install collides on the (provider, installation_id) unique constraint.

This task closes the gap so a real Slack workspace admin can click "Add to Fyralis", complete OAuth, and have their messages land as Observations in the right tenant within seconds — with no operator intervention.

How it can be done

Land in 5 ordered phases, each independently deployable:

Phase 1 — DB-backed secret store (foundation, 1.5 d)

New lib/shared/secrets/ module with put(plaintext, label) → ref / get(ref) → plaintext / rotate(ref, new_plaintext) / delete(ref).
Backend: envelope-encrypted column (Fernet, MASTER_KEK env) for MVP. Pluggable interface for AWS KMS / GCP KMS later.
Migrate the meaning of provider_installations.secret_ref from "opaque string" to "concrete pointer into the secret store".
Rewrite services/webhooks/secrets.py::load_secrets so it resolves provider_installations.secret_ref → secret_store.get(ref). Keep the env-var path behind WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1 (dev only, off in prod).

Phase 2 — Wire IN-07 into the router (0.5 d)

services/webhooks/router.py swaps resolve_tenant(provider, raw) for app.state.tenant_resolver.resolve(provider, payload, headers).
Map Resolved / UnknownInstallation / PayloadMissing to the existing 401 / 400 error shapes.
Delete services/webhooks/tenant_resolution.py (env-var resolver) once Phase 1 + 2 are both live in staging for 24 h.

Phase 3 — Slack OAuth install flow (2 d)

GET /integrations/slack/install — authenticated by existing Bearer middleware. Generates a signed state token (HMAC over {tenant_id, nonce, expiry_ts} where tenant_id comes from the session, never a query param) and 302s to https://slack.com/oauth/v2/authorize?client_id=…&scope=…&state=….
GET /integrations/slack/callback?code=…&state=… — public route (no Bearer; state token is the auth). Steps:
Verify state-token HMAC + expiry.
Exchange code for tokens via Slack oauth.v2.access.
secret_store.put the bot token, user token (if granted), and signing secret.
INSERT INTO provider_installations(provider='slack', installation_id=team_id, tenant_id, secret_ref=<bot_token_ref>, enabled=true) — or UPDATE on (provider, installation_id) conflict if re-installing.
INSERT INTO installation_audit_log(action='install', …).
Slack scopes (minimum viable): channels:history, groups:history, im:history, mpim:history, users:read, team:read, event subscriptions for message.*, app_mention, app_uninstalled, tokens_revoked.

Phase 4 — Uninstall / token revocation (1 d)

Extend the Slack ingestion handler to branch on event type:
app_uninstalled / tokens_revoked → look up installation by team_id, call TenantResolver.disable_installation(installation_row_id), then secret_store.delete(ref) to zero the token material. Write an audit row.
Re-install after uninstall: the OAuth callback path detects the existing-but-disabled row, calls enable_installation + update_secret_ref — does NOT create a duplicate.

Phase 5 — Outbound Slack Web API client (1 d)

lib/integrations/slack/client.py: thin async wrapper around chat.postMessage, users.info, conversations.info with per-installation token lookup and Slack rate-limit (Tier 1–4) backoff.
Used immediately by the ingestion handler to enrich user / channel names on Observations.
Becomes the substrate for Slack-outbound Acts in a follow-up.

Acceptance criteria

A Slack workspace admin can complete the install flow end-to-end without operator intervention: click → Slack consent screen → land back on Fyralis → first message in any subscribed channel appears as an Observation under the correct tenant_id within 30 s.
Workspace signing secrets are never stored in env vars or plaintext at rest in any environment marked prod.
Uninstalling the app from Slack causes the next inbound webhook from that workspace to return unknown_installation (401), and installation_audit_log carries an uninstall row.
Re-installing after uninstall reuses the same provider_installations.id row (no orphans, no unique-constraint conflict).
services/webhooks/router.py contains zero references to services.webhooks.tenant_resolution; the DB resolver is the only path.
webhook_resolver_outcomes_total{provider="slack", outcome="resolved"} is non-zero in staging within 1 h of merge.
Negative case: a request with a forged team_id for which no row exists returns 401 with unknown_installation (not 404, not 500, no log leak of the team_id — IN-07 SC-008 must still hold).
IN-07 secret_ref semantics remain compatible: existing tests in services/webhooks/tests/test_tenant_resolver_admin.py continue to pass.

Security / constitution notes

New tables (installation_audit_log, any token-storage table) are tenant-scoped → Constitution §III applies: tenant_id FK + RLS + tenant-prefixed indexes are non-negotiable.
The state token MUST carry tenant_id from the authenticated session, never from a client-controllable query param — otherwise an attacker with their own Slack workspace could bind it to another tenant.
The OAuth callback route is public (no Bearer) but state-token-verified; add it to the gateway public-path allowlist as a specific route, not a prefix.
Token material at rest: envelope-encrypted via the new secret store; MASTER_KEK injected from the deployment secret manager, not committed.

Out of scope (follow-up tasks)

App Home tab / slash commands / interactive components — track as IN-10 once Acts can emit Slack-outbound messages.
Migrating GitHub / Linear / Stripe / Discord onto the same OAuth pattern — track as IN-09, IN-11, etc.; this task is Slack-only by design so the pattern can be validated end-to-end on one provider before generalising.
Per-channel subscription management UI (which Slack channels Fyralis listens to) — Slack app-level scopes cover MVP.

Estimated effort

6 days (1.5 d Phase 1, 0.5 d Phase 2, 2 d Phase 3, 1 d Phase 4, 1 d Phase 5).
