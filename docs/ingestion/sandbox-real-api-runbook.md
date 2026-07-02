# Real API Sandbox Runbook

This runbook brings up the local Fyralis data plane and connects it to real
provider APIs through a public tunnel. The setup-owner UI now uses the same
automation surface for Slack, GitHub, Discord, Notion, Jira, and Telegram.

For Slack, this is the path that has been exercised end-to-end and proves:

- Slack OAuth install lands in `provider_installations`.
- The install writes an `onboarding_triggers` row.
- Historical backfill runs through the worker pipeline.
- Slack Events API webhooks create live observations.
- The onboarding UI reads those rows through `GET /observations?source=slack`.

The same UI status model is used for the other sources: install/finalize state,
onboarding trigger count, shard/run state, unresolved failures, and actual
observations filtered by source.

No provider tokens should be pasted into chat or committed. Store them only in
local env files or the customer-cloud secret store.

## Prerequisites

Required local tools:

```bash
docker compose version
curl --version
python3 --version
npm --version
```

Required local files:

```bash
.env
.env.sandbox
```

If `.env.sandbox` does not exist:

```bash
cp .env.sandbox.example .env.sandbox
```

Fill the provider values in `.env` or `.env.sandbox`. Slack looks like this:

```bash
SLACK_CLIENT_ID=...
SLACK_CLIENT_SECRET=...
SLACK_SIGNING_SECRET=...
OAUTH_STATE_HMAC_KEY=...
AUTH_BOOTSTRAP_SECRET=...
SANDBOX_PUBLIC_URL=https://<your-ngrok-host>
SLACK_REDIRECT_URI=https://<your-ngrok-host>/integrations/slack/callback
```

For the additional providers:

| Source | Customer action Fyralis cannot silently do | Fyralis automation after input/approval |
| --- | --- | --- |
| GitHub | Create/configure the GitHub App and approve org installation. | Generates real GitHub App install URL, validates callback, writes `provider_installations`, emits `onboarding_triggers(source='github')`, then polls observations. |
| Discord | Configure Discord application/bot and approve server install. | Generates real Discord OAuth URL, stores bot/public-key refs in the customer-cloud secret store, emits `onboarding_triggers(source='discord')`, then polls observations. |
| Notion | Create public OAuth integration, approve workspace access, share pages/databases with the integration. | Generates real Notion OAuth URL, stores workspace bot token, emits `onboarding_triggers(source='notion')`, then polls observations. |
| Jira | Create an Atlassian API token and optionally a webhook secret. | UI verifies credentials, enumerates projects, stores encrypted refs, writes `jira_installations`/`jira_projects`, registers live webhook state when a secret is supplied, emits `onboarding_triggers(source='jira')`, then polls observations. |
| Telegram | Create Telegram API ID/hash and provide an authorized MTProto StringSession. | UI verifies the session, enumerates dialogs, stores encrypted refs, writes `telegram_installations`/`telegram_dialogs`, emits `onboarding_triggers(source='telegram')`, then polls observations. |

## Provider URLs

The Slack app must use these URLs for the current sandbox run:

```text
OAuth redirect URL: <SANDBOX_PUBLIC_URL>/integrations/slack/callback
Events request URL: <SANDBOX_PUBLIC_URL>/webhooks/slack/events
```

The other providers use the same public ingress shape:

```text
GitHub callback:   <SANDBOX_PUBLIC_URL>/integrations/github/callback
GitHub webhook:    <SANDBOX_PUBLIC_URL>/webhooks/github
Discord callback:  <SANDBOX_PUBLIC_URL>/integrations/discord/callback
Discord webhook:   <SANDBOX_PUBLIC_URL>/webhooks/discord
Notion callback:   <SANDBOX_PUBLIC_URL>/integrations/notion/callback
Notion webhook:    <SANDBOX_PUBLIC_URL>/webhooks/notion/events
Jira webhook:      <SANDBOX_PUBLIC_URL>/webhooks/jira/events
Telegram live:     customer-cloud MTProto gateway worker, no public webhook
```

If the ngrok URL changes, update both `.env.sandbox` and the Slack app
configuration, then restart the sandbox.

## One-Command Slack Rehearsal

Start the real-API sandbox, mint a local Fyralis session, and print the real
Slack OAuth install URL:

```bash
scripts/slack_real_rehearsal.sh prepare --start-ui
```

If the stack is already running:

```bash
scripts/slack_real_rehearsal.sh prepare --skip-stack
```

The command prints:

- Slack install URL.
- OAuth redirect URL.
- Events request URL.
- Gateway API base for the UI.
- Local Fyralis bearer session token for the UI.

The sandbox overlay builds the core Fyralis image without the optional private
`Fyralisinc/github-intel` package. That keeps Slack validation runnable without
GitHub package credentials.

Open the Slack install URL and approve the app. This approval cannot be
silently automated because Slack requires workspace/user consent and may
require admin approval.

## Historical Backfill

After Slack redirects back to Fyralis:

1. The callback exchanges the Slack code for bot/user tokens.
2. Fyralis stores tokens in the encrypted secret store.
3. Fyralis writes `provider_installations`.
4. Fyralis writes `onboarding_triggers(source='slack')`.
5. The onboarding/source workers plan and fetch historical Slack shards.
6. Normalizer/writer workers land rows in `observations`.

For private channels, invite the Fyralis Slack app to the channel before
expecting channel history to appear. For DMs, the consenting user token must be
granted by the Slack OAuth flow.

Poll until Slack observations are visible:

```bash
scripts/slack_real_rehearsal.sh wait --skip-stack --wait-seconds 300
```

## Live Ingestion

Once Slack Events API points to:

```text
<SANDBOX_PUBLIC_URL>/webhooks/slack/events
```

post a new message in a connected channel or DM. Then poll again:

```bash
scripts/slack_real_rehearsal.sh wait --skip-stack --wait-seconds 300
```

## UI Verification

Open:

```text
http://localhost:3003/onboarding/ingestion-health
```

In the selected source detail view, use **Automated <source> setup**:

- For Slack, GitHub, Discord, and Notion: click **Prepare and open <source>**,
  approve the provider screen, then watch the UI poll install, trigger,
  backfill, and observations.
- For Jira: click **Prepare Jira**, enter site URL, account email, API token,
  and optional webhook secret, then click **Verify and connect Jira**.
- For Telegram: click **Prepare Telegram**, enter API ID/hash and authorized
  StringSession details, then click **Verify and connect Telegram**.

The UI calls the dev-gated gateway rehearsal endpoints:

```http
POST /platform/onboarding/sources/{source}/rehearsal/prepare
GET  /platform/onboarding/sources/{source}/rehearsal/status
POST /platform/onboarding/sources/jira/rehearsal/finalize
POST /platform/onboarding/sources/telegram/rehearsal/finalize
```

When observations exist, the UI also reads:

```http
GET /observations?source={source}&limit=50
Authorization: Bearer <local session token>
```

Rows shown as gateway-backed observations are real rows from the running
Fyralis runtime, not preview cards.

The rehearsal endpoints are only enabled in the local sandbox by
`FYRALIS_SOURCE_REHEARSAL_ENABLED=1`; they must stay disabled in production.
The legacy Slack-only flag remains accepted for backward compatibility.

## Fully Local Proof Path

If you need to prove the UI and gateway observation path without touching real
Slack:

```bash
scripts/slack_real_rehearsal.sh synthetic-proof --skip-stack
```

This uses the local Slack DM control panel route to create synthetic historical
and live Slack events. It proves the Fyralis ingestion/UI path, but it is not a
real Slack workspace integration.

## Troubleshooting

Inspect installation, triggers, shards, observations, and DLQ:

```bash
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \
  exec gateway python scripts/sandbox_inspect.py
```

Follow relevant logs:

```bash
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \
  logs -f gateway source_onboarding shard_fetch normalizer observation_writer
```

Common causes of no rows:

- Slack app was not approved.
- Slack OAuth redirect URL does not match `SLACK_REDIRECT_URI`.
- ngrok URL changed after the Slack app was configured.
- Bot was not invited to the test channel.
- No live message was posted after Events API was configured.
- Worker pipeline is not running or is stuck in DLQ.
