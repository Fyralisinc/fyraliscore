# Fyralis Source Automation Guide

This guide describes what the BYOC setup owner should expect when connecting
sources from the Fyralis customer-cloud onboarding UI.

The automation model is an admin-present browser agent launched from customer
BYOC:

- Fyralis launches from inside the customer BYOC browser/session.
- Fyralis opens the provider settings handoff and follows the per-source setup
  recipe for non-secret IDs, scopes, redirect URLs, webhook URLs, and object
  hints.
- Fyralis generates provider setup bundles, Fyralis-owned verifier material,
  and customer-cloud secret refs where the provider allows it.
- Fyralis stores secret-looking values only as encrypted customer-cloud refs.
- Fyralis emits an onboarding trigger and a sanitized connection-proof
  observation after a source is connected.
- Humans only sign in, pass MFA, approve provider scopes, create credentials, or
  accept app/role/token creation where the provider requires an accountable
  admin action.

No source credentials, raw logs, prompts, or customer payloads are sent to the
Fyralis hosted control plane.

| Source | Automation after approval/ref | Human crucial step only |
| --- | --- | --- |
| Slack | Opens OAuth install, registers workspace, triggers sync, reads channels the app can access. | Workspace admin approves the Slack app and scopes. |
| GitHub | Opens GitHub App install, discovers selected repos, triggers backfill/live webhooks. | Org/repo admin approves the GitHub App installation and repository scope. |
| Discord | Opens bot/OAuth install, discovers guilds/channels the bot can access, triggers gateway sync. | Server admin installs the bot and enables required intents/channel access. |
| Notion | Opens integration approval, records workspace install, triggers page/database sync. | Workspace owner approves integration and shares pages/databases. |
| Jira | Verifies token, discovers projects when not provided, registers webhook when secret is present. | Atlassian admin/user creates an API token or approves equivalent app access. |
| Telegram | Verifies MTProto session, discovers dialogs, stores session refs, triggers sync. | Human completes Telegram API/session authorization. |
| Signal | Stores linked-device session, prepares local gateway, triggers connection proof. | Human links/approves the Signal device session. |
| WhatsApp | Stores access/verify refs, prepares Meta webhook route, triggers connection proof. | Meta admin approves app/webhook and business phone scope. |
| Gmail | Prepares Google Workspace DWD payloads, mailbox scope, watch/poll path, and connection proof. | Google Workspace admin authorizes the service account client ID, scopes, and mailbox boundary. |
| Google Calendar | Prepares Google Workspace DWD payloads, calendar scope, watch/poll path, and connection proof. | Google Workspace admin authorizes calendar scopes and inclusion boundary. |
| Google Drive | Prepares Google Workspace DWD payloads, shared-drive/folder discovery, and connection proof. | Google Workspace admin authorizes Drive scopes and shared-drive/folder scope. |
| Fireflies | Uses customer token ref, discovers workspace/transcripts, triggers connection proof. | Workspace admin approves Fireflies access or provides token ref. |
| Figma | Uses API token, discovers teams/files when hints are omitted, triggers connection proof. | Figma admin/user creates a token with file/team read access. |
| Miro | Uses API token, discovers boards when hints are omitted, triggers poll-only connection proof. | Miro admin/user creates token and approves board scope. |
| Grafana | Uses instance URL and service account token, discovers dashboards/alerts, triggers connection proof. | Grafana admin creates service account token. |
| AWS | Uses read-only role ARN, validates role/account/region hints, triggers connection proof. | Cloud owner approves IAM role/trust policy. |
| Mercury | Uses API token, discovers accounts/transactions when hints are omitted, triggers connection proof. | Mercury admin creates API token and optional webhook secret. |
| QuickBooks | Uses token ref, discovers realm/accounting entities when hints are omitted, triggers connection proof. | Intuit admin approves OAuth app or provides preauthorized token ref. |
| Brex | Uses API token, discovers accounts/cards/transactions, triggers connection proof. | Brex admin creates API token and optional webhook secret. |
| Ramp | Uses access token or OAuth client credentials, discovers business entities/transactions, triggers connection proof. | Ramp admin creates least-privilege OAuth credential material with approved scopes. |
| Carta | Uses token ref, discovers issuer/equity entities, triggers connection proof. | Carta admin approves app or provides token ref. |
| Gusto | Uses token ref, discovers company/employees/payroll, triggers connection proof. | Gusto admin approves OAuth/app access or provides token ref. |
| HiBob | Uses service-user token, discovers people fields/reports/company hints, triggers connection proof. | HiBob admin creates the service user/API token. |
| Ashby | Uses API token, discovers jobs/candidates/interviews, triggers connection proof. | Ashby admin creates API token and approves recruiting data scope. |
| Deel | Uses API token, discovers workers/contracts/payments, triggers connection proof. | Deel admin creates API token and approves workforce/payroll scope. |
| LinkedIn | Uses token ref and local polling contract, triggers connection proof. | LinkedIn admin approves organization/page access and rate-limit posture. |

## How To Test In The UI

1. Open the Fyralis onboarding UI.
2. Open `Sources`.
3. Click `Connect` on a source.
4. Keep the provider admin present only for sign-in, MFA, and approval prompts.
5. Let the background agent collect non-secret settings and generate Fyralis-owned local
   material.
6. Confirm the `Observations landed` panel shows a sanitized connection-proof
   observation.
7. For providers with live or historical workers configured, run the first sync
   and refresh the same panel to see provider observations.

The connection-proof observation is not a substitute for provider data. It only
proves that the customer-cloud onboarding path can store refs, register the
source, emit the trigger, and read gateway-backed observations.

## How To Test In The BYOC CLI

Prepare all source artifacts:

```bash
python -m services.platform.cli.fyralis byoc source autopilot \
  --source all \
  --scopes auto \
  --sync-mode dry-run \
  --auto-activate \
  --workdir .fyralis/byoc-agent \
  --json
```

Run the admin-present browser-agent orchestration for every prepared source:

```bash
python -m services.platform.cli.fyralis byoc source browser-agent \
  --source all \
  --workdir .fyralis/byoc-agent \
  --gateway-api-base https://fyralis-ingress.customer.example \
  --json
```

The BYOC Docker image installs the browser-agent extra and Chromium. Source
connections started from the minimal UI run the background browser DOM agent
headless by default, then pause when provider sign-in, MFA, credential reveal,
or final approval is human-only.

The CLI default remains prep-only: it prepares artifacts, native endpoint calls,
browser DOM plans, launcher scripts, and customer-cloud ref metadata. To execute
the provider settings page automation from a visible admin-present CLI run, pass
the execution flags for one source:

```bash
python -m services.platform.cli.fyralis byoc source browser-agent \
  --source ramp \
  --workdir .fyralis/byoc-agent \
  --gateway-api-base https://fyralis-ingress.customer.example \
  --execute-browser-dom \
  --interactive-admin \
  --json
```

The browser agent stops at human-only gates. The customer admin signs in, passes
MFA, creates/reveals provider credentials when the provider requires it, or
approves scopes, then returns to the terminal and continues the same run.

Every source now materializes a source-specific provider setup bundle from the
same run path. Examples:

- Slack: generated app manifest and event subscription manifest.
- Google Workspace sources: DWD preflight/finalize payloads.
- GitHub: GitHub App manifest and webhook contract.
- AWS: read-only IAM role/trust setup contract.
- API-token/OAuth/webhook/local-session sources: provider-specific setup JSON
  with the exact non-secret fields, generated refs, and admin gates.

Sources with existing native connect routers also expose their customer-cloud
preflight/finalize paths to the browser-agent run, including Brex, Carta, Deel,
Figma, Fireflies, Gmail, Google Calendar, Google Drive, Gusto, LinkedIn,
Mercury, Miro, QuickBooks, Ramp, Signal, Slack, Telegram, and WhatsApp. The
runner prepares those calls locally and executes them only when a customer-local
payload and approval flag are provided.
