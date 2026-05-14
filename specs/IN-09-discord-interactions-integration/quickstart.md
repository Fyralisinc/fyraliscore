# Quickstart — IN-09 End-to-End Validation

This is the on-the-box validation flow for a developer who just merged the IN-09 branch and wants to confirm the Discord integration works end-to-end against a local stack. It mirrors the IN-08 quickstart in shape.

## Prerequisites

- IN-08 substrate must be in place (`encrypted_secrets`, `oauth_install_states`, `installation_audit_log`, `provider_installations` tables exist; `MASTER_KEK` env is set; `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` for dev only).
- Docker Postgres on host port **5433** (`DATABASE_URL=postgresql://company_os:company_os@localhost:5433/company_os`).
- Ollama on `localhost:11434` with `nomic-embed-text`.
- The `.venv` at `.venv/` (Python 3.12) with `pip install -e ".[dev]"`.
- A public tunnel for the gateway: `ngrok http 8000` (free tier is fine for dev).
- A Discord application in the Developer Portal with **Bot** and **OAuth2** sections configured (see Setup below).

## Setup (one-time per Discord application)

1. **Create the Discord application** at https://discord.com/developers/applications. Note the **Application ID**, **Public Key**, and from OAuth2 → **Client ID** + **Client Secret**.
2. **OAuth2 → Redirects**: add `<ngrok-url>/integrations/discord/callback`. (The exact match matters — Discord rejects redirect URIs that don't appear here.)
3. **General Information → Interactions Endpoint URL**: set to `<ngrok-url>/webhooks/discord/events`. Discord will PING immediately; the gateway must respond `{"type": 1}` for the field to save.
4. **Bot → Privileged Gateway Intents**: leave all off — IN-09 does not use the Gateway. (`MESSAGE_CONTENT` is for IN-12, deferred.)
5. **Env vars** in `.env`:

```sh
DISCORD_CLIENT_ID=<from step 1>
DISCORD_CLIENT_SECRET=<from step 1>
WEBHOOK_SECRET_DISCORD=<Public Key from step 1>
DISCORD_REDIRECT_URI=<ngrok-url>/integrations/discord/callback
DISCORD_APPLICATION_ID=<Application ID from step 1>
```

The existing `MASTER_KEK`, `OAUTH_STATE_HMAC_KEY`, `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`, `FYRALIS_ENV=dev` from the IN-08 setup remain in place.

## Boot

```sh
.venv/bin/uvicorn services.gateway.main:app --host 127.0.0.1 --port 8000 > /tmp/fyralis_logs/gateway.log 2>&1 &
echo $! > /tmp/gateway.pid
```

Check `/healthz` and the gateway log for any IN-09 import errors.

## Smoke test (no real Discord workspace required)

This synthetic POST verifies the Interactions endpoint round-trip without touching a Discord server:

```sh
set -a; source .env; set +a
BODY='{"id":"123456789012345678","application_id":"'$DISCORD_APPLICATION_ID'","type":1}'
TS=$(date +%s)
# PING is signed with the application's PRIVATE key (which only Discord has).
# For local dev, generate a test key pair and sign; or skip the PING smoke
# test and rely on Discord itself for end-to-end PING verification.
```

In practice, the PING smoke test is best left to Discord (set the Interactions URL → Discord PINGs immediately → field flips to "verified"). The Synthetic POST below covers the slash-command path with a known good signing setup.

## Self-serve install (the happy path)

1. From a browser logged into Fyralis with a Bearer token for an authenticated tenant, hit `<gateway>/integrations/discord/install`.
2. Browser redirects to `https://discord.com/oauth2/authorize?...` — Discord's consent screen.
3. Pick a guild you administer. Click **Authorize**.
4. Discord redirects to `<ngrok-url>/integrations/discord/callback?code=...&state=...`.
5. Gateway:
   - Atomically consumes the state token.
   - POSTs to Discord's `oauth2/token` with the code.
   - Encrypts the bot token in `encrypted_secrets[label='discord_bot_token:<guild_id>']`.
   - Encrypts the app public key in `encrypted_secrets[label='discord_public_key:<guild_id>']`.
   - UPSERTs `provider_installations(provider='discord', installation_id=<guild_id>, ...)`.
   - POSTs the `/fyralis ask` command spec to Discord's `applications/<app_id>/commands`.
   - Writes `installation_audit_log(action='install', status='ok')`.
   - 302s to `/integrations/discord/installed?guild=<short_hash>`.

Total wall time should be < 1.5 s.

## End-to-end verification

In the Discord client (web/desktop), in any channel of the just-installed guild:

1. Type `/fyralis` — the command should appear in the typeahead within a few seconds.
2. Pick `/fyralis ask`, fill the `query` option with anything (e.g., `"smoke test from claude"`), submit.
3. Discord delivers an interaction to `<ngrok-url>/webhooks/discord/events`.
4. Gateway:
   - Verifies the Ed25519 signature using the DB-backed public key (label `discord_public_key:<guild_id>`).
   - Resolves the tenant via `provider_installations(installation_id=<guild_id>)`.
   - Dispatches `services/ingestion/handlers/discord.py::handle_discord_webhook`.
   - Inserts an Observation: `source_channel='discord:interaction'`, `content_text='<your query>'`, `external_id='discord:<interaction_snowflake>'`, `trust_tier='attested_agent'`.
   - Returns Discord's expected ack (type=5 deferred or type=4 with body).

Query the DB to verify:

```sql
SELECT occurred_at, content_text, source_actor_ref, external_id
FROM observations
WHERE tenant_id = '<your tenant id>'
  AND source_channel = 'discord:interaction'
ORDER BY occurred_at DESC LIMIT 5;
```

A row with `content_text='<your query>'` should appear within 3 seconds of submitting the slash command.

## Bot-kick verification

1. In Discord, remove the Fyralis bot from your guild (Server Settings → Integrations → Fyralis → Remove).
2. From the gateway, trigger any outbound Discord call (the next `/fyralis` invocation will, post-Observation-commit, send a follow-up message — that fires the chokepoint). Or run a one-shot:

```python
# In .venv/bin/python -c"..."
import asyncio
from uuid import UUID
import asyncpg
from cryptography.fernet import Fernet  # noqa
from lib.shared.secrets import build_secret_store
from services.integrations.discord.client import DiscordClient

# (construct DiscordClient with the just-disabled installation)
# call client.get_guild_member(user_id='...') → expect DiscordApiError(code='discord_api_unauthorized')
```

3. Verify in DB:

```sql
SELECT enabled FROM provider_installations
WHERE provider='discord' AND installation_id='<guild_id>';
-- expect: enabled=false

SELECT count(*) FROM encrypted_secrets
WHERE label='discord_bot_token:<guild_id>';
-- expect: 0

SELECT action, status, created_at FROM installation_audit_log
WHERE installation_row_id=(SELECT id FROM provider_installations WHERE installation_id='<guild_id>')
ORDER BY created_at DESC LIMIT 3;
-- expect: action='uninstall', status='ok' as the most recent row
```

4. Re-issue any `/fyralis` invocation from that (now-removed) guild → gateway returns 401 `unknown_installation` (because the install row is `enabled=false`).

## Re-install verification

1. From the browser, hit `/integrations/discord/install` again as the same tenant.
2. Pick the same guild. Authorize.
3. Verify in DB:

```sql
SELECT id, installation_id, enabled, installed_at FROM provider_installations
WHERE installation_id='<guild_id>';
-- expect: SAME id as before (no duplicate), enabled=true, installed_at recent
```

This confirms FR-011 (same-tenant re-install reuses the row, no orphan, no UNIQUE conflict).

## Cross-tenant collision verification (manual)

To verify SC's collision-detection without two real tenants, manually insert a `provider_installations` row for a fake `guild_id` under tenant A, then attempt an OAuth callback for the same `guild_id` from a session authenticated as tenant B. Expect a 302 to `install-error?reason=installation_collision`, and no log line containing tenant A's id.

This is hard to script without two real Bearer tokens. Reserve for the integration test (`test_oauth_callback_discord.py::test_cross_tenant_collision`).

## Tearing down

```sh
kill $(cat /tmp/gateway.pid /tmp/ngrok.pid)
```

Or full `scripts/stop.sh` if you booted via that path.

## Common failures

| Symptom | Cause | Fix |
|---|---|---|
| Interactions URL field won't save in Developer Portal | `WEBHOOK_SECRET_DISCORD` env var doesn't match the application's Public Key | Copy-paste the Public Key from Developer Portal → General Info exactly into `.env` and restart the gateway. |
| `/integrations/discord/install` 401 unauthorized | Bearer middleware rejecting | Mint a fresh session token via `services.gateway.auth.create_session` (the IN-08 seed script pattern). |
| OAuth callback 302 to `install-error?reason=state_invalid` | `OAUTH_STATE_HMAC_KEY` env var changed between `install` and `callback` | Keep the env var stable across restarts; if you rotated it, re-issue the install. |
| `/fyralis` doesn't appear in Discord typeahead | Slash-command registration call failed during callback, OR Discord's global command propagation hasn't caught up (up to 1 h) | Check `installation_audit_log` for `status='error'` rows; if absent, wait for Discord's propagation OR switch to per-guild commands (deferred — see plan.md Risk #3 area). |
| Bot ingest works but tenant_id is wrong | `provider_installations.tenant_id` was bound to the wrong tenant during install | DELETE the row + re-run `/integrations/discord/install` from a session authenticated as the correct tenant. |
