# Real-API sandbox runbook — Slack · Jira · Notion · GitHub · Discord · Telegram

Bring the **full ingestion data plane up locally** (docker-compose) and ingest
**real signals from the six sources** using **real provider credentials** —
backfill + live, end to end into `observations`. An ngrok tunnel gives the local
gateway a public HTTPS URL so real webhooks + OAuth callbacks reach it. Discord
(WSS gateway) and Telegram (MTProto) need no inbound webhook.

This is the operator handoff: the code is wired (see
`docs/architecture/ingest.md`); what remains is **your provider apps/tokens** and
**pointing each provider at your ngrok URL**.

---

## 0. Prerequisites (one-time)

- Docker + docker-compose, and `ngrok` (a **reserved domain** is strongly
  recommended — a free ephemeral URL changes on every restart and forces you to
  re-edit every provider app).
- A `.env` at the repo root containing at least these durable secrets (generate
  once; **regenerating `MASTER_KEK` orphans every encrypted secret**):
  ```bash
  python -c "from cryptography.fernet import Fernet; print('MASTER_KEK='+Fernet.generate_key().decode())"
  python -c "import secrets; print('OAUTH_STATE_HMAC_KEY='+secrets.token_urlsafe(32))"
  python -c "import secrets; print('AUTH_BOOTSTRAP_SECRET='+secrets.token_urlsafe(32))"
  ```
- `cp .env.sandbox.example .env.sandbox` and work through it — it is the
  authoritative per-source checklist; this runbook is the narrative around it.

> **Prod guards are ON in the sandbox** (`FYRALIS_ENV=prod`,
> `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=0`, `WEBHOOK_TENANT_DEFAULT_ALLOW=0`):
> webhook signing secrets resolve **only** from the encrypted DB store, and a
> tenant is resolved **only** from an install row. This is deliberate — it
> exercises the production trust path.

---

## 1. Bring up the stack

```bash
# Terminal A — public ingress (use --domain for a stable URL if you have one)
ngrok http 8000        # note the https URL it prints

# Edit .env.sandbox: set SANDBOX_PUBLIC_URL + the *_REDIRECT_URI lines to that host.

# Terminal B — the stack (gateway :8000 + all ingestion workers, prod guards)
scripts/sandbox_up.sh
```
`sandbox_up.sh` runs `docker compose -f docker-compose.yml -f docker-compose.sandbox.yml up -d --build`, waits for `/healthz`, and seeds the sandbox tenant + actor. The overlay starts: gateway, the backfill loops (oauth_poller, tenant_onboarding, source_onboarding, shard_fetch, reconciler, periodic_reconciler), the Kafka chain (normalizer, observation_writer, dlq_writer, …), and the two live workers (`discord_gateway_worker`, `telegram_gateway_worker`).

> If you change `.env.sandbox` (e.g. a new ngrok URL), re-run `scripts/sandbox_up.sh` so the gateway reloads it.

A compose shorthand used throughout:
```bash
alias dc='docker compose -f docker-compose.yml -f docker-compose.sandbox.yml'
```

### Minting an auth session (needed to drive OAuth installs)

The `/integrations/<src>/install` and `/connect/*` endpoints are Bearer-authed.
Mint a session token against the bootstrap secret:
```bash
TOKEN=$(curl -s -X POST http://localhost:8000/auth/session \
  -H "X-Bootstrap-Secret: $AUTH_BOOTSTRAP_SECRET" -H 'Content-Type: application/json' \
  -d '{"actor_id":"00000000-0000-0000-0000-000000000002","tenant_id":"00000000-0000-0000-0000-000000000001","ttl_seconds":86400}' \
  | python -c "import sys,json;print(json.load(sys.stdin)['session_token'])")
TENANT=00000000-0000-0000-0000-000000000001
```
To start an OAuth install, hit the install endpoint with that token and follow
the provider redirect in a browser:
```bash
curl -s -H "Authorization: Bearer $TOKEN" -H "X-Tenant-Id: $TENANT" \
  "$SANDBOX_PUBLIC_URL/integrations/slack/install"   # → open the returned authorize URL
```

---

## 2. Per-source setup

Each source needs: **(a)** a provider app/token, **(b)** the matching env in
`.env.sandbox`, **(c)** an install, and (for HTTP sources) **(d)** a webhook URL
registered with the provider. Backfill begins automatically once the install
trigger lands.

### GitHub  (OAuth install · app-level webhook secret)
1. Create a **GitHub App**. Permissions: Issues + Pull requests + Contents
   (read). Subscribe to: Issues, Pull request, Push, Issue comment. Set the
   **Webhook URL** to `$SANDBOX_PUBLIC_URL/webhooks/github` and a **Webhook
   secret**. Generate a private key (`.pem`).
2. `.env.sandbox`: `GITHUB_APP_ID`, `GITHUB_APP_SLUG`, `WEBHOOK_SECRET_GITHUB`,
   and the private key — drop the `.pem` at `./.secrets/github_app.pem` (mounted
   read-only) and leave `GITHUB_APP_PRIVATE_KEY_PATH=/run/secrets/github_app.pem`.
3. Install: open `…/integrations/github/install` (session-authed), pick the
   repos. The callback writes the install **with `secret_ref=NULL`** (the secret
   is app-level), so seed it into the encrypted store:
   ```bash
   dc exec -T gateway python scripts/sandbox_seed_secret.py github <installation_id> "$WEBHOOK_SECRET_GITHUB"
   ```

### Slack  (OAuth install · per-install signing secret)
1. Create a **Slack app**. Bot scopes: `channels:read`, `groups:read`,
   `channels:history`, `groups:history` (DMs additionally need user-token scopes
   — see `docs/ingestion/slack-dm-*`). **OAuth redirect**:
   `$SANDBOX_PUBLIC_URL/integrations/slack/callback`. **Event Subscriptions**
   request URL: `$SANDBOX_PUBLIC_URL/webhooks/slack`; subscribe to
   `message.channels` (+ `message.groups`).
2. `.env.sandbox`: `SLACK_CLIENT_ID`, `SLACK_CLIENT_SECRET`,
   `SLACK_SIGNING_SECRET` (the callback stores it encrypted on install).
3. Install: open `…/integrations/slack/install`.

### Discord  (OAuth install · live via WSS gateway, no webhook)
1. Create a **Discord app + bot**. In the dev portal enable the **MESSAGE
   CONTENT intent**. Copy the **bot token**, **client id/secret**, and the app
   **public key**. **OAuth redirect**:
   `$SANDBOX_PUBLIC_URL/integrations/discord/callback`. Invite the bot to your
   guild (OAuth2 URL with `bot` scope + Read Messages/Message History).
2. `.env.sandbox`: `DISCORD_BOT_TOKEN`, `DISCORD_APPLICATION_ID`,
   `DISCORD_CLIENT_ID`, `DISCORD_CLIENT_SECRET`, `WEBHOOK_SECRET_DISCORD`
   (the Ed25519 public key).
3. Install: open `…/integrations/discord/install`. Live messages arrive over the
   `discord_gateway_worker` WSS connection — **no webhook URL to register**.

### Jira  (token install via seed script · HMAC webhook)
1. Create an **API token** at `id.atlassian.com/manage-profile/security/api-tokens`.
2. `.env.sandbox`: `JIRA_BASE_URL=https://<site>.atlassian.net`,
   `JIRA_ACCOUNT_EMAIL`, `JIRA_API_TOKEN`, and (for live) `JIRA_WEBHOOK_SECRET`.
3. Install (no OAuth — a seed script writes the install + projects + trigger and
   stores the token encrypted):
   ```bash
   dc exec -T gateway python scripts/sandbox_jira_seed.py --tenant "$TENANT"   # --projects KEY1,KEY2 to pin a subset
   ```
4. Live webhook: in Jira → Settings → System → **Webhooks**, point at
   `$SANDBOX_PUBLIC_URL/webhooks/jira/events` with the **same** `JIRA_WEBHOOK_SECRET`
   (HMAC-SHA256, `X-Hub-Signature`); subscribe to issue + comment created/updated.

### Notion  (OAuth install · optional app-level webhook)
1. Create a **public OAuth integration** at `notion.so/my-integrations`; redirect
   `$SANDBOX_PUBLIC_URL/integrations/notion/callback`. **Share** each
   page/database you want ingested with the integration (page ••• → Connections)
   — Notion only exposes shared objects.
2. `.env.sandbox`: `NOTION_CLIENT_ID`, `NOTION_CLIENT_SECRET`.
3. Install: open `…/integrations/notion/install`. Backfill + the 15-min poll are
   the correctness backstop (full object tree).
4. Live webhook (optional, page-only fast path): add a subscription to
   `$SANDBOX_PUBLIC_URL/webhooks/notion/events`. Notion POSTs a one-time
   **unsigned** verification carrying a `verification_token`; grab it from the
   gateway logs and seed it, then restart the gateway:
   ```bash
   dc logs gateway | grep notion_webhook_verification_token_received
   # put the value in .env.sandbox as NOTION_WEBHOOK_VERIFICATION_TOKEN, then:
   dc up -d gateway
   ```

### Telegram  (interactive MTProto install · live via gateway, no webhook)
Telegram uses the **user-account MTProto API** (Telethon) — no bot token, no
OAuth, no webhook. The credential is a `StringSession` from an interactive login.
1. Get an `api_id` / `api_hash` at `my.telegram.org` → "API development tools".
   `.env.sandbox`: `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`.
2. Run the install **inside** the gateway container (shares DATABASE_URL +
   MASTER_KEK; Telethon ships in the image). It prompts phone → code → 2FA, lists
   your dialogs to pick, stores the session encrypted, and writes the install:
   ```bash
   dc exec -it gateway python scripts/sandbox_telegram_install.py --account-label my-tg
   ```
3. Restart the live worker so it connects to the new install:
   ```bash
   dc restart telegram_gateway_worker
   ```
   Backfill fires automatically from the trigger. (Use
   `--separate-backfill-session` for a distinct backfill auth_key under sustained
   concurrent load.)

---

## 3. Validate

```bash
dc exec -T gateway python scripts/sandbox_inspect.py
```
It prints `provider_installations` (OAuth landings) and **observations by
`source_channel`** with the dedup oracle. Success per source = rows under its
channel with `total == distinct external_id` (no live/backfill double-count):

| source | source_channel |
|---|---|
| slack | `slack:message` |
| github | `github:webhook` |
| discord | `discord:message` |
| jira | `jira:issue` |
| notion | `notion:object` |
| telegram | `telegram:message` |

Tail the pipeline while you generate activity (post a message, open an issue, …):
```bash
dc logs -f gateway normalizer observation_writer
```

---

## 4. Gotchas (each has bitten a real run)

- **ngrok URL changed** → update `SANDBOX_PUBLIC_URL` + every `*_REDIRECT_URI` in
  `.env.sandbox`, re-run `sandbox_up.sh`, AND update each provider app's
  redirect/webhook URLs. Discord/Telegram workers reconnect on restart.
- **`kafka_path_enabled`** now defaults **TRUE**, so backfill commits without
  intervention. If you ever see backfill produce zero committed observations,
  check the per-tenant flag:
  `TenantFlags(pool).set_bool(tenant, KAFKA_PATH_ENABLED, True, set_by="op")`.
- **Observations are month-partitioned.** Backfilling items older than the
  current window self-heals up to ~10y (`PARTITION_MAX_BACKFILL_LOOKBACK_DAYS`);
  beyond that they DLQ as `partition_missing`.
- **GitHub** webhook secret is **app-level** and the callback stores
  `secret_ref=NULL` — you MUST run `sandbox_seed_secret.py github …` after install
  or live webhooks 401.
- **Notion** webhook verification token is delivered once, unsigned, in the
  handshake — read it from the logs and seed it, else signed events reject as
  `secret_not_configured` (harmless; backfill+poll still cover everything).
- **Telegram** worker binds the install at startup → **restart it after install**.
  Topology B: backfill and live use separate auth_keys; the single-session
  default can reconnect-churn under heavy overlap (use the two-session flag).
- **Discord** needs the **MESSAGE CONTENT intent** enabled in the dev portal or
  message bodies arrive empty.
- **Webhook trailing slash**: register the exact path (`/webhooks/<src>` or
  `/webhooks/<src>/events`); a redirecting variant may not be re-POSTed by the
  provider.

---

## 5. Teardown

```bash
scripts/sandbox_down.sh            # stop, keep volumes (installs/secrets persist)
scripts/sandbox_down.sh --volumes  # stop + wipe DB/Kafka/MinIO for a clean slate
```
