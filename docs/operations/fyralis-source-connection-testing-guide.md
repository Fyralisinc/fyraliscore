# Fyralis Source Connection Testing Guide

This guide is for testing Fyralis source onboarding from a customer-cloud setup-owner point of view. Secrets stay in the customer environment. Fyralis-hosted UI receives only setup metadata, status, and sanitized observation counts.

## Current Automation Model

Fyralis supports two setup paths:

- **Provider rehearsal**: `fyralis byoc source rehearse --source <source>` generates provider handoff artifacts, env templates, callback/webhook URLs, and local setup instructions for one real provider.
- **Source autopilot**: `fyralis byoc source autopilot --source all` generates discovery, provider setup, secret-ref, validation, first-sync, and readiness artifacts for every supported source.

Automation can generate manifests, callback URLs, env templates, local Kubernetes secret application commands, install URLs where the provider supports it, validation receipts, and first-sync receipts.

Automation cannot silently bypass provider trust gates such as OAuth consent, admin approval, token creation, private-channel permission grants, or login-code challenges. Those steps must happen in the customer-owned provider account or through preauthorized customer-cloud secret refs.

## Local Test Environment

Use the existing local BYOC rehearsal stack:

```bash
NEXT_PUBLIC_FYRALIS_API_BASE=http://localhost:8000 npm run dev
```

Expected local URLs:

```text
Hosted/customer setup UI: http://localhost:3003
Customer-cloud gateway:  http://localhost:8000
Provider ingress:        https://<your-ngrok-host>
```

For real provider callbacks, expose the gateway:

```bash
ngrok http 8000
export NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL=https://<your-ngrok-host>
```

## Generate All Source Artifacts

```bash
uv run python -m services.platform.cli.fyralis byoc source autopilot \
  --source all \
  --workdir .fyralis/source-autopilot \
  --admin-console-url http://localhost:3003 \
  --provider-ingress-url "$NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL" \
  --sync-mode dry-run \
  --backfill-window 30d \
  --json
```

Artifacts are written under:

```text
.fyralis/source-autopilot/sources/<source>/
```

Each source folder contains provider setup, secret refs, connection, validation, scope, first-sync, and readiness receipts.

## Generate A Real Provider Rehearsal Package

```bash
uv run python -m services.platform.cli.fyralis byoc source rehearse \
  --source <source> \
  --setup-dir .fyralis/local-rehearsal/<source> \
  --public-url "$NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL" \
  --no-start-tunnel \
  --json
```

Then copy and fill the generated env file locally:

```bash
cp .fyralis/local-rehearsal/<source>/<source>.env.example \
   .fyralis/local-rehearsal/<source>/<source>.env
```

Apply or validate the local provider env:

```bash
uv run python -m services.platform.cli.fyralis byoc source rehearse \
  --source <source> \
  --setup-dir .fyralis/local-rehearsal/<source> \
  --public-url "$NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL" \
  --provider-env .fyralis/local-rehearsal/<source>/<source>.env \
  --apply-env \
  --json
```

For Slack, GitHub, Discord, and Notion, add install-url generation when runtime config is present:

```bash
uv run python -m services.platform.cli.fyralis byoc source rehearse \
  --source <source> \
  --setup-dir .fyralis/local-rehearsal/<source> \
  --public-url "$NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL" \
  --provider-env .fyralis/local-rehearsal/<source>/<source>.env \
  --apply-env \
  --print-install-url \
  --tenant-id <tenant-id> \
  --actor-id <actor-id> \
  --json
```

## Source-Specific Notes

| Source | Test path | Human gate |
| --- | --- | --- |
| Slack | OAuth app manifest, Events API, install URL | Workspace app approval/OAuth consent |
| GitHub | GitHub App manifest, install URL, webhook | Org owner app installation |
| Discord | Discord app/bot, OAuth callback, webhook/gateway | Server admin approval and channel permissions |
| Notion | OAuth integration setup | Workspace OAuth consent and webhook verification |
| Jira | API token finalize path | Atlassian API token and project scope |
| Telegram | Local MTProto session finalize path | API ID/hash and login code |
| Gmail | Generic OAuth/ref package | Workspace admin/OAuth or DWD approval |
| Google Calendar | Generic OAuth/ref package | Workspace admin/OAuth approval |
| Google Drive | Generic OAuth/ref package | Workspace admin/OAuth approval |
| Fireflies | Generic OAuth/ref package | Workspace OAuth/API authorization |
| Gusto | Generic OAuth/ref package | Gusto OAuth app consent |
| QuickBooks | Generic OAuth/ref package | Intuit app consent and realm scope |
| Carta | Generic OAuth/ref package | Carta issuer/app authorization |
| LinkedIn | Generic polling/ref package | LinkedIn app approval and rate-limit scope |
| Figma | Generic API token package | Team/file token and webhook scope |
| Miro | Generic API token package | Board/team token and polling scope; no production webhook |
| Grafana | Generic API token package | Service account token and folder scope |
| Mercury | Generic API token package | Banking API token and account scope |
| Brex | Generic API token package | API token and account/card scope |
| Ramp | Generic API token package | API token and entity scope |
| HiBob | Generic API token package | Service user token and people-field scope |
| Ashby | Generic API token package | API token and recruiting scope |
| Deel | Generic API token package | API token and workforce scope |
| AWS | Generic IAM role package | Customer IAM role approval |
| Signal | Generic local gateway session package | Linked-device authorization |
| WhatsApp | Generic webhook package | Meta app, verify token, webhook secret |

## Discord Private Channels

Discord private channels are not special-cased by Fyralis. The bot can read a private channel only if the Discord server grants the bot or its role:

```text
View Channel
Read Message History
```

Fyralis now plans every text channel that Discord exposes to the bot. If Discord later returns `403 Missing Access` for a channel, the fetcher records a bounded skip for that shard and continues. This prevents one private or restricted channel from blocking the whole guild.

## UI Test Flow

1. Open `http://localhost:3003/onboarding/sources`.
2. Pick a source.
3. On the source setup page, use the provider rehearsal command shown in the UI.
4. Complete the provider-side gate if required.
5. Use **Prepare <source>** when the gateway is running.
6. Use **Retry checks** after provider approval.
7. Use **Fetch observations** with the gateway token shown by the setup flow.

For already connected sources, observations appear through:

```http
GET /observations?source=<source>
```

## Verification Commands

Check a source status:

```bash
curl -sS "$NEXT_PUBLIC_FYRALIS_API_BASE/platform/onboarding/sources/<source>/rehearsal/status" | jq
```

Check observations:

```bash
curl -sS "$NEXT_PUBLIC_FYRALIS_API_BASE/observations?source=<source>" \
  -H "Authorization: Bearer <actor-session-token>" | jq
```

Check local Postgres directly:

```bash
docker exec company_os_postgres psql -U company_os -d company_os -c \
  "select source_channel, count(*) from observations group by 1 order by 2 desc;"
```

## Completion Criteria

A source is considered test-complete when:

- Provider setup package exists.
- Required customer-owned refs are present.
- Provider admin/OAuth/login gate is complete, if applicable.
- Fyralis status endpoint reports the source installed or ready.
- Historical backfill or live event trigger has run.
- UI shows sanitized observations or a clear no-observations status.
