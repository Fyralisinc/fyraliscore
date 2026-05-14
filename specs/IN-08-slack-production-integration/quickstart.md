# Quickstart — Exercising the Slack Install Flow Locally

This walks a developer through verifying every acceptance criterion locally using the dev Docker stack (Postgres on 5433, Ollama on 11434) and a Slack dev workspace.

## Prerequisites

1. `docker compose up -d postgres ollama` and `ollama pull nomic-embed-text:v1.5`.
2. Python venv at `.venv` on Python 3.12 with `pip install -e ".[dev]"`.
3. A Slack workspace you control (a free Slack dev workspace is fine).
4. A Slack App registered at <https://api.slack.com/apps>:
   - **OAuth & Permissions** → Redirect URLs → add `https://<your-dev-tunnel>/integrations/slack/callback` (use `ngrok` or `cloudflared`).
   - **Bot Token Scopes**: `channels:history`, `groups:history`, `im:history`, `mpim:history`, `users:read`, `team:read`.
   - **Event Subscriptions**: enabled; Request URL `https://<your-dev-tunnel>/webhooks/slack/events`; subscribed bot events `message.channels`, `message.groups`, `message.im`, `message.mpim`, `app_mention`, `app_uninstalled`, `tokens_revoked`.
   - Note the `Client ID`, `Client Secret`, and `Signing Secret` from the **Basic Information** page.

## Step 0 — env vars

Add to your local `.env`:

```bash
SLACK_CLIENT_ID=<from Slack App basics>
SLACK_CLIENT_SECRET=<from Slack App basics>
SLACK_SIGNING_SECRET=<from Slack App basics>
SLACK_REDIRECT_URI=https://<your-dev-tunnel>/integrations/slack/callback

# Envelope-encryption KEK — generate once with:
#   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
MASTER_KEK=<that base64 string>

# Dev fallback allowed (do NOT set this in prod)
WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1
FYRALIS_ENV=dev

# Server-side state-token HMAC key (32 bytes URL-safe base64, same generator)
OAUTH_STATE_HMAC_KEY=<another fernet key works>
```

## Step 1 — apply migrations

```bash
psql "$DATABASE_URL" -f db/migrations/0040_slack_installation_tokens.sql
psql "$DATABASE_URL" -f db/migrations/0041_installation_audit_log.sql
```

Re-running is a no-op (`CREATE … IF NOT EXISTS`). Verify with:

```bash
psql "$DATABASE_URL" -c "\d encrypted_secrets"
psql "$DATABASE_URL" -c "\d oauth_install_states"
psql "$DATABASE_URL" -c "\d installation_audit_log"
```

Each should show RLS enabled and the tenant-prefixed index present.

## Step 2 — start the gateway and confirm wiring

```bash
uvicorn services.gateway.main:app --reload --port 8000
```

Watch for log lines:

- `secret_store_initialized backend=fernet`
- `oauth_install_states_sweep_scheduled interval_s=300`
- `webhook_router_using_db_tenant_resolver` (confirms Phase 2 cutover took effect)

If any are missing, stop and check the relevant wiring point (the integration tests are how you'd catch this; this checklist is for local sanity).

## Step 3 — exercise the install route

Obtain a Bearer token via the existing dev session-mint path (`/auth/session` with your dev actor). Then:

```bash
curl -i -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/integrations/slack/install
```

Expected response: `HTTP/1.1 302 Found` with `Location: https://slack.com/oauth/v2/authorize?...&state=...`.

Verify in DB:

```bash
psql "$DATABASE_URL" -c "SELECT id, tenant_id, nonce, provider, expires_at FROM oauth_install_states ORDER BY created_at DESC LIMIT 1;"
```

One new row, `provider='slack'`, `consumed_at IS NULL`, `expires_at` ~10 min in the future. (SC-009)

## Step 4 — complete OAuth consent on Slack

Open the `Location` URL from Step 3 in a browser. Consent. Slack redirects to `…/integrations/slack/callback?code=…&state=…`.

Expected: another 302 to `/integrations/slack/installed?team=<short_hash>`. (Your browser may show 404 for that path since the UI isn't shipping in this task — that's fine; the redirect is the contract.)

Verify in DB:

```bash
psql "$DATABASE_URL" -c "SELECT id, tenant_id, provider, installation_id, enabled FROM provider_installations WHERE provider='slack' ORDER BY installed_at DESC LIMIT 1;"
psql "$DATABASE_URL" -c "SELECT action, status, context FROM installation_audit_log ORDER BY created_at DESC LIMIT 1;"
psql "$DATABASE_URL" -c "SELECT id, label FROM encrypted_secrets WHERE tenant_id = '<your tenant>' ORDER BY created_at DESC;"
psql "$DATABASE_URL" -c "SELECT consumed_at FROM oauth_install_states WHERE consumed_at IS NOT NULL;"
```

You should see:

- One new `provider_installations` row, `enabled=true`, `secret_ref` is the UUID of the bot-token row in `encrypted_secrets`. (SC-001 part 1)
- One audit row, `action='install'`, `status='ok'`.
- 2–3 `encrypted_secrets` rows (`slack_bot_token:<team_id>`, optional `slack_user_token:<team_id>`, `slack_signing_secret:app`). (SC-002 — none of these in env vars; check `printenv | grep WEBHOOK_SECRET_SLACK` returns empty.)
- The corresponding `oauth_install_states` row has `consumed_at IS NOT NULL`. (Single-use enforced.)

## Step 5 — send a Slack message and watch it land as an Observation

In the connected Slack workspace, post any message in any channel where Fyralis is invited. Within 30 seconds:

```bash
psql "$DATABASE_URL" -c "SELECT id, tenant_id, source_channel, content_text FROM observations WHERE source_channel='slack:message' ORDER BY occurred_at DESC LIMIT 1;"
```

A new `observations` row under the install's `tenant_id`. (SC-001 part 2)

## Step 6 — verify forged team_id is rejected without log leak

Construct a synthetic webhook with a `team_id` that has no installation row, sign it with a random secret (will fail signature, but we test the resolver outcome separately):

```bash
# Send a payload with team_id=T_FORGED_12345 (no row exists) and a bogus signature
curl -i -X POST http://localhost:8000/webhooks/slack/events \
  -H "Content-Type: application/json" \
  -H "X-Slack-Signature: v0=<bogus>" \
  -H "X-Slack-Request-Timestamp: $(date +%s)" \
  -d '{"team_id":"T_FORGED_12345","event":{"type":"message"}}'
```

Expected: HTTP 401 with body `{"code":"unknown_installation",...}`. Check the gateway logs — `T_FORGED_12345` MUST NOT appear in any log line. (SC-007)

## Step 7 — uninstall from Slack and verify

In the Slack workspace admin: **Apps** → Fyralis → **Remove App**. Slack POSTs an `app_uninstalled` event to your webhook.

```bash
psql "$DATABASE_URL" -c "SELECT enabled FROM provider_installations WHERE installation_id='<your team_id>';"
psql "$DATABASE_URL" -c "SELECT action, status FROM installation_audit_log WHERE installation_row_id=(SELECT id FROM provider_installations WHERE installation_id='<your team_id>') ORDER BY created_at DESC;"
psql "$DATABASE_URL" -c "SELECT count(*) FROM encrypted_secrets WHERE tenant_id='<your tenant>' AND label LIKE 'slack_%token:<your team_id>';"
```

Expected:

- `enabled=false`. (SC-003 part 1)
- New audit row `action='uninstall'`, `status='ok'`.
- Zero `encrypted_secrets` rows for that team_id's tokens (signing-secret row may remain, that's fine — it's per-tenant not per-team).

Post another Slack message (you can't because the app is uninstalled, but you can simulate the webhook):

```bash
# Same forged payload as Step 6 — should still get 401
```

HTTP 401 `unknown_installation`. (SC-003 part 2)

## Step 8 — re-install and verify the row is reused

Repeat Steps 3–4. After the second callback completes:

```bash
psql "$DATABASE_URL" -c "SELECT count(*) FROM provider_installations WHERE installation_id='<your team_id>';"
psql "$DATABASE_URL" -c "SELECT id, enabled, secret_ref FROM provider_installations WHERE installation_id='<your team_id>';"
```

Expected:

- Exactly **1** row (no duplicate from the second install). (SC-004)
- `enabled=true`, `secret_ref` points at a **new** `encrypted_secrets.id` (rotated by the re-install).
- The `id` matches the original install's id (preserved per FR-018).

## Step 9 — automated test suite

```bash
.venv/bin/pytest services/integrations/tests -m integration -v
.venv/bin/pytest lib/shared/secrets -v
.venv/bin/pytest services/webhooks/tests/test_router.py services/webhooks/tests/test_tenant_resolver_admin.py -v
```

All should pass. The IN-07 admin suite passing unchanged is SC-008.

## Step 10 — schema drift check

```bash
python scripts/check_schema_drift.py
```

Zero exit. Any non-zero exit means the live DB schema diverged from the migration directory — debug before merging.

## Constitution review checklist (run before opening the PR)

- [ ] `grep -rn 'uuid.uuid4()' services/integrations lib/shared/secrets` → zero hits.
- [ ] `grep -rn 'print(' services/integrations lib/shared/secrets` → zero hits.
- [ ] `grep -n 'services.webhooks.tenant_resolution' services/webhooks/router.py` → zero hits. (SC-005)
- [ ] `ruff check services/integrations lib/shared/secrets services/webhooks` → clean.
- [ ] `python scripts/check_schema_drift.py` → zero exit.
- [ ] Every new tenant-scoped table has `ENABLE ROW LEVEL SECURITY` + `FORCE` + `tenant_isolation` policy.
- [ ] No `team_id` appears in any structured log line for the negative-case test paths.

If any check fails, the PR is not ready.
