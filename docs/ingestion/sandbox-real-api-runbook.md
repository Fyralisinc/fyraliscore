# Real API Sandbox Runbook

This runbook brings up the local Fyralis data plane and connects it to real
provider APIs through a public tunnel. For Slack, this is the path that proves:

- Slack OAuth install lands in `provider_installations`.
- The install writes an `onboarding_triggers` row.
- Historical backfill runs through the worker pipeline.
- Slack Events API webhooks create live observations.
- The onboarding UI reads those rows through `GET /observations?source=slack`.

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

Fill the Slack values in `.env` or `.env.sandbox`:

```bash
SLACK_CLIENT_ID=...
SLACK_CLIENT_SECRET=...
SLACK_SIGNING_SECRET=...
OAUTH_STATE_HMAC_KEY=...
AUTH_BOOTSTRAP_SECRET=...
SANDBOX_PUBLIC_URL=https://<your-ngrok-host>
SLACK_REDIRECT_URI=https://<your-ngrok-host>/integrations/slack/callback
```

## Slack App URLs

The Slack app must use these URLs for the current sandbox run:

```text
OAuth redirect URL: <SANDBOX_PUBLIC_URL>/integrations/slack/callback
Events request URL: <SANDBOX_PUBLIC_URL>/webhooks/slack/events
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

In the Slack source detail view, use **Automated Slack setup**:

- Click **Prepare and open Slack**.
- Approve the Slack app in the browser.
- Invite the app to a test channel.
- Post a message.
- Watch the same page poll install, trigger, backfill, and observations.

The UI calls the dev-gated gateway rehearsal endpoints:

```http
POST /platform/onboarding/slack/rehearsal/prepare
GET  /platform/onboarding/slack/rehearsal/status
```

When observations exist, the UI also reads:

```http
GET /observations?source=slack&limit=50
Authorization: Bearer <local session token>
```

Rows shown as gateway-backed observations are real rows from the running
Fyralis runtime, not preview cards.

The rehearsal endpoints are only enabled in the local sandbox by
`FYRALIS_SLACK_REHEARSAL_ENABLED=1`; they must stay disabled in production.

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
