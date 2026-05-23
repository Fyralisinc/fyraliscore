# Real-API Ingestion Sandbox — Operator Runbook

> Stand up the ingestion pipeline **locally** and exercise it against the
> **real GitHub / Slack / Discord APIs** — backfill and live, concurrently.
> Gmail is intentionally out of scope here (it needs GCP + domain-wide
> delegation; see [sandbox-real-api-research-plan.md](./sandbox-real-api-research-plan.md) §7).
>
> This is the executed counterpart to the research plan. It runs under
> **full prod guards** (`FYRALIS_ENV=prod`): real signature verification,
> real OAuth, real API pagination — only the ingestion *system* is local.

## 0. How it fits together

```
   GitHub / Slack / Discord (real APIs)
        │  webhook POST  /  OAuth redirect          your machine (docker compose)
        ▼                                          ┌───────────────────────────────┐
   ngrok  https://<sub>.ngrok-free.app  ──tunnel──▶│ gateway :8000                 │
        ▲  (you run `ngrok http 8000`)             │   → kafka → normalizer        │
        │                                          │   → observation_writer → pg   │
        │  Discord live = bot WSS (outbound,       │   → embedding_worker          │
        │  no public URL needed)                   │ backfill loops + reconcilers  │
        └── backfill: fetchers → REAL provider API │ discord_gateway_worker (live) │
                                                   └───────────────────────────────┘
```

- **Backfill** and **live** run at the same time: the moment OAuth install
  lands, the onboarding loops plan + drain backfill shards against the real
  API, while webhooks (GitHub/Slack) and the Discord gateway WSS deliver
  live events. Cross-path dedup is enforced by
  `observations UNIQUE (source_channel, external_id, occurred_at)`.
- The stack is the production `docker-compose.yml` plus the
  `docker-compose.sandbox.yml` overlay (ngrok ingress, `.env.sandbox`, and
  the gmail/UI/edge/think services parked under the `full` profile).

## 1. Prerequisites

- Docker + Docker Compose v2 (≥ 2.24 for the `!override` env_file merge).
- `ngrok` (installed). A **reserved/static ngrok domain** is strongly
  recommended — a free ephemeral URL changes on every `ngrok` restart and
  you must then re-update all three provider apps. One static domain (free
  tier offers one) makes the provider URLs a one-time setup.
- A GitHub account/org, a throwaway Slack workspace, and a Discord server
  you control (test tenants — keep content small to stay under rate limits).

## 2. One-time local setup

```bash
cp .env.sandbox.example .env.sandbox      # gitignored
```

`.env.sandbox` is a thin **override layer applied after `.env`** — your
existing Slack/Discord creds, `MASTER_KEK`, `OAUTH_STATE_HMAC_KEY` and
`AUTH_BOOTSTRAP_SECRET` in `.env` are reused. You only fill in:

- `SANDBOX_PUBLIC_URL`, `SLACK_REDIRECT_URI`, `DISCORD_REDIRECT_URI` — all
  the same ngrok host (set after step 3).
- The GitHub App block (`GITHUB_APP_ID`, `GITHUB_APP_SLUG`,
  `GITHUB_APP_PRIVATE_KEY`, `WEBHOOK_SECRET_GITHUB`) — GitHub creds are
  *not* in `.env`.

> If your `.env` lacks `MASTER_KEK` / `OAUTH_STATE_HMAC_KEY` /
> `AUTH_BOOTSTRAP_SECRET`, generate them **once** and put them in
> `.env.sandbox` (regenerating `MASTER_KEK` later orphans every encrypted
> secret in the DB):
> ```bash
> python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"   # MASTER_KEK
> python -c "import secrets; print(secrets.token_urlsafe(48))"                                  # OAUTH_STATE_HMAC_KEY / AUTH_BOOTSTRAP_SECRET
> ```

## 3. Start the tunnel + first boot

```bash
ngrok http 8000                      # or: ngrok http --domain=<your-static> 8000
```
Copy the `https://…` forwarding URL. Put it in `.env.sandbox` as
`SANDBOX_PUBLIC_URL` and use the same host for `SLACK_REDIRECT_URI`
(`…/integrations/slack/callback`) and `DISCORD_REDIRECT_URI`
(`…/integrations/discord/callback`).

```bash
scripts/sandbox_up.sh
```
This builds + starts the stack under prod guards, waits for
`http://localhost:8000/healthz`, and seeds the sandbox tenant. The gmail
workers, demo UI, nginx-proxy/acme and think workers are **not** started
(they live under `--profile full`).

Sanity: `curl -s https://<your-ngrok-host>/healthz` should return 200 via
the tunnel.

## 4. Per-source provisioning + validation

Each source follows the same arc: **register the app → map secrets into
`.env.sandbox` → OAuth install → confirm backfill → confirm a live event →
confirm dedup.** Easiest → hardest below.

The OAuth install is **Bearer-authenticated** (tenant comes from the
session). Mint a session once and reuse the token:

```bash
PUBLIC="https://<your-ngrok-host>"
BOOT="<AUTH_BOOTSTRAP_SECRET from .env>"
TID="00000000-0000-0000-0000-000000000001"   # COMPANY_OS_TENANT_ID
AID="00000000-0000-0000-0000-000000000002"   # COMPANY_OS_CEO_ACTOR_ID

TOKEN=$(curl -s -X POST "$PUBLIC/auth/session" \
  -H "X-Bootstrap-Secret: $BOOT" -H 'Content-Type: application/json' \
  -d "{\"actor_id\":\"$AID\",\"tenant_id\":\"$TID\"}" | python -c 'import sys,json;print(json.load(sys.stdin)["token"])')
echo "$TOKEN"
```

To start an install, hit the install endpoint with the Bearer token and
follow the `Location` it returns in a browser (that completes provider
consent, which redirects back to the callback and finalizes the install):

```bash
curl -s -D- -o /dev/null -H "Authorization: Bearer $TOKEN" \
  "$PUBLIC/integrations/<provider>/install"
# → 302 Location: <provider authorize/install URL> — open it in a browser.
```

### 4.1 GitHub (App)

1. **Create a GitHub App** (Settings → Developer settings → GitHub Apps):
   - Webhook URL: `$PUBLIC/webhooks/github`, Webhook secret: a strong value.
   - Callback URL (and "Setup URL"): `$PUBLIC/integrations/github/callback`.
   - Permissions: Issues (R), Pull requests (R), Metadata (R).
   - Subscribe to events: Issues, Pull request (+ Issue comment if wanted).
   - Generate a **private key (.pem)**; note the **App ID** and **slug**.
2. **Map to `.env.sandbox`:** `GITHUB_APP_ID`, `GITHUB_APP_SLUG`,
   `GITHUB_APP_PRIVATE_KEY` (PEM contents) or `GITHUB_APP_PRIVATE_KEY_PATH`,
   `WEBHOOK_SECRET_GITHUB`. Re-run `scripts/sandbox_up.sh` to load them.
3. **Install:** `…/integrations/github/install` (Bearer) → follow the
   redirect → install the App on 1–2 test repos. The callback writes a
   `provider_installations` row (**`secret_ref = NULL`**) + an
   `onboarding_triggers` row.
4. **Seed the webhook secret** (GitHub, unlike Slack/Discord, does not
   auto-store it — required for signature verification under prod guards):
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \
     exec gateway python scripts/sandbox_seed_secret.py \
     github <installation_id> '<the WEBHOOK_SECRET_GITHUB value>'
   ```
   (`<installation_id>` is the GitHub App installation id — see the inspect
   output or the install URL.)
5. **Backfill:** shards are planned per accessible repo; observations land
   with `source_channel='github:webhook'`.
6. **Live:** open an issue/PR. A correctly-signed delivery lands as one
   observation; a forged signature → 401. Confirm it dedups against any
   backfilled twin.

### 4.2 Slack

1. **Create a Slack app** (api.slack.com/apps):
   - Event Subscriptions → Request URL: `$PUBLIC/webhooks/slack` (Slack
     sends a `url_verification` challenge — the gateway answers it; the app
     must be running and reachable via ngrok).
   - Subscribe to bot events: `message.channels` (+ `message.groups` if
     including private channels).
   - OAuth & Permissions → Redirect URL: `$PUBLIC/integrations/slack/callback`;
     scopes `channels:history`, `channels:read` (+ `groups:read` if
     `SLACK_BACKFILL_INCLUDE_PRIVATE=1`).
   - Note the **Signing Secret** and **Client ID/Secret**.
2. **Map to `.env.sandbox`** (only if different from `.env`):
   `SLACK_CLIENT_ID`, `SLACK_CLIENT_SECRET`, `SLACK_SIGNING_SECRET`,
   `SLACK_REDIRECT_URI`. Re-run `sandbox_up.sh` if changed.
3. **Install:** `…/integrations/slack/install` (Bearer) → authorize in the
   workspace. The callback stores the bot token **and** the signing secret
   encrypted, and sets `provider_installations.secret_ref` automatically —
   **no manual seed needed.**
4. **Backfill:** channels enumerate (cursor-paginated); history → observations.
5. **Live:** post a message. Signature + 300s replay window enforced
   (`SLACK_MAX_TIMESTAMP_AGE_S`); the message lands deduped.

### 4.3 Discord

1. **Create a Discord app + bot** (discord.com/developers):
   - Interactions Endpoint URL: `$PUBLIC/webhooks/discord` (Discord verifies
     with an Ed25519 PING — the gateway must be reachable). Copy the app's
     **Public Key** (Ed25519, hex).
   - OAuth2 → Redirect: `$PUBLIC/integrations/discord/callback`.
   - Bot → enable **Message Content Intent**. Copy the **Bot Token**.
   - Note **Application ID** and **Client ID/Secret**.
2. **Map to `.env.sandbox`** (only if different from `.env`):
   `DISCORD_BOT_TOKEN`, `DISCORD_APPLICATION_ID`, `DISCORD_CLIENT_ID`,
   `DISCORD_CLIENT_SECRET`, `WEBHOOK_SECRET_DISCORD` (= the **Public Key**),
   `DISCORD_REDIRECT_URI`. `PYTHONHASHSEED=0` is already set. Re-run if changed.
3. **Install:** `…/integrations/discord/install` (Bearer) → authorize → invite
   the bot to your server. The callback mirrors the public key into the
   encrypted store and sets `secret_ref` automatically. The
   `discord_gateway_worker` connects (bot-token WSS) and resolves guild→tenant.
4. **Backfill:** guilds enumerate (paginated); a stable 5% channel sample is
   fetched (seed-stable across restarts).
5. **Live:** post in a guild text channel. The gateway `MESSAGE_CREATE`
   lands as an observation (`source_channel='discord:message'`).

### 4.4 Notion (IN-14)

Notion is the **simplest** source here: no webhook, no signing secret, no
seed step. The live path is a **poll** (Notion has no reliable content
push), so there is nothing to register a webhook URL for.

1. **Create a Notion integration** (https://www.notion.so/my-integrations):
   - Type **Public** (so OAuth works). Redirect URI:
     `$PUBLIC/integrations/notion/callback`.
   - Copy the **OAuth client ID** and **client secret**.
2. **Map to `.env.sandbox`:** `NOTION_CLIENT_ID`, `NOTION_CLIENT_SECRET`,
   `NOTION_REDIRECT_URI`. Re-run `scripts/sandbox_up.sh` if changed.
3. **Share content with the integration** — this is the step people miss:
   open each database/page → **•••** → **Connections** → add your
   integration. Notion's API only returns objects explicitly shared with it.
4. **Install:** `…/integrations/notion/install` (Bearer) → authorize and
   pick the workspace. The callback stores the long-lived **bot token**
   straight into the encrypted store and sets `secret_ref` automatically
   (no `sandbox_seed_secret.py` needed), then emits an `install` trigger.
5. **Backfill (automatic):** the planner enumerates databases
   (`POST /v1/search`) → one `notion_database` shard per DB + one
   `notion_page_tree` shard for loose pages → the fetcher walks rows →
   blocks (depth ≤ `NOTION_BLOCK_DEPTH_CAP`, default 3) → comments. Objects
   land as observations with `source_channel='notion:object'` (a DB row
   with a status property → `kind='state_change'`).
6. **Live (poll):** the `periodic_reconciler` re-runs the fetcher every
   `NOTION_POLL_INTERVAL_SECONDS` (default 900s). Edit a shared page in
   Notion, wait one interval, and the changed object reappears as a new
   observation (dedup keeps the unchanged ones to a single row via
   `external_id = notion:{object}:{id}`).

> No webhook URL is registered for Notion, and `WEBHOOK_SECRET_NOTION`
> does not exist — if you find yourself looking for one, you're on the
> wrong source.

## 5. Validate (the oracle)

```bash
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \
  exec gateway python scripts/sandbox_inspect.py
```

Pass criteria:

| Check | Pass when |
|---|---|
| Install | `provider_installations` row per source, `enabled=True`, `has_secret=True` (github only after step 4.1.4) |
| Kickoff | `onboarding_triggers` has an `install` row per source |
| Backfill | `onboarding_runs` reach `complete`/`feels_onboarded`; `onboarding_shards` mostly `done` |
| Observations | rows with the right `source_channel` (`github:webhook`, `slack:*`, `discord:message`) |
| **Dedup** | per source_channel `total == distinct_ext` (no duplicate `external_id` ⇒ live deduped vs backfill) |
| Embedding | `embedded` climbs toward `total`, `pending → 0` |
| DLQ | `ingestion_failures` (unresolved) is empty |

Tail the pipeline while testing:
```bash
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \
  logs -f gateway normalizer observation_writer discord_gateway_worker
```

## 6. Teardown

```bash
scripts/sandbox_down.sh              # stop, keep data (installs/secrets persist)
scripts/sandbox_down.sh --volumes    # also wipe pg/kafka/minio (fresh DB → re-install)
```
Also revoke the test installs in each provider's settings when finished.
With an ephemeral ngrok URL, the provider webhook/redirect URLs are stale
after teardown — re-point them on next bring-up.

## 7. Troubleshooting

- **Gateway won't boot** — under prod guards it fails closed: missing
  `MASTER_KEK`, or `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`. Check
  `logs gateway`.
- **Webhook 401 (signature)** — secret mismatch. Slack/Discord: re-install
  so the encrypted secret is current. GitHub: re-run the `sandbox_seed_secret.py`
  step with the exact App webhook secret; confirm `has_secret=True`.
- **`secret_ref NULL` on github in inspect** — you skipped step 4.1.4.
- **OAuth redirect mismatch / Slack URL verification fails** — the provider
  app's URL doesn't match your current ngrok host. Update both the app and
  `.env.sandbox`, then re-run `sandbox_up.sh`. (This is why a static ngrok
  domain is worth it.)
- **No backfill observations** — confirm the install wrote an
  `onboarding_triggers` row; check `logs source_onboarding shard_fetch reconciler`.
- **Discord live silent** — `logs discord_gateway_worker`; verify Message
  Content Intent is enabled and the bot is in the channel.
- **Rate limits** — keep test accounts small; bound GitHub with
  `GITHUB_MAX_BACKFILL_REPOS`.

## 8. Files

| File | Role |
|---|---|
| `docker-compose.sandbox.yml` | overlay: ngrok ingress, `.env.sandbox`, parks gmail/UI/edge/think |
| `.env.sandbox(.example)` | override layer on `.env` (prod guards, public URL, GitHub creds) |
| `scripts/sandbox_up.sh` / `sandbox_down.sh` | bring-up / teardown |
| `scripts/sandbox_seed_secret.py` | seed GitHub webhook secret → `secret_ref` |
| `scripts/sandbox_inspect.py` | read-only validation oracle |
