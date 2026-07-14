# Provider setup handoff — credentials for the 6-source ingestion test

**Audience:** the teammate provisioning the provider apps.
**Goal:** create/configure the apps below and **return the values in the final
table**. Nothing here touches our servers — it's all done in each provider's
developer console.

There are three credential types:
- **OAuth apps** — GitHub, Slack, Discord, Notion (create an app, copy id/secret).
- **API token** — Jira (one token).
- **MTProto app** — Telegram (an `api_id`/`api_hash`, then an account login).

---

## ⚠️ Before you start: the public URL (already running)

Every OAuth **redirect URL** and **webhook URL** you register must point at our
running gateway's **public HTTPS URL**, and must match **exactly** (scheme, host,
path — no trailing slash unless shown).

> **Base URL = `https://angle-briary-valentina.ngrok-free.dev`**
> (a reserved ngrok static domain — stable across restarts; the tunnel is already
> running and forwards to the local gateway on port 8000.)

The redirect/webhook URLs in this doc are **already filled in** with that base URL —
copy them as-is into each provider app. If the tunnel domain ever changes, update
every URL here (and the `*_REDIRECT_URI` lines in `.env.sandbox`).

**Current local status** (so you know what's already done): GitHub, Jira and
Notion are already configured on the requester's machine — **focus on Slack,
Discord, and Telegram**, and only re-do the others if asked.

---

## 1. GitHub  →  a **GitHub App**

**Console:** github.com → top-right avatar → **Settings** → left sidebar
**Developer settings** → **GitHub Apps** → **New GitHub App**.
(Direct link: https://github.com/settings/apps/new)

1. **GitHub App name:** anything unique (e.g. `fyralis-ingest-test`).
2. **Homepage URL:** `https://angle-briary-valentina.ngrok-free.dev` (any valid URL is fine).
3. **Identifying and authorizing users → Callback URL:**
   `https://angle-briary-valentina.ngrok-free.dev/integrations/github/callback`
4. **Webhook:** tick **Active**.
   - **Webhook URL:** `https://angle-briary-valentina.ngrok-free.dev/webhooks/github`
   - **Webhook secret:** generate a random high-entropy string and **save it**.
5. **Permissions → Repository permissions:**
   - Contents → **Read-only**
   - Issues → **Read-only**
   - Pull requests → **Read-only**
   - (Metadata is auto Read-only)
6. **Subscribe to events:** Issues, Issue comment, Pull request, Push.
7. **Where can this GitHub App be installed?** "Only on this account" is fine.
8. **Create GitHub App.**
9. On the app's page: copy the **App ID** (near the top). The **slug** is the last
   path segment of the app URL `github.com/apps/<slug>` (also shown as the public
   link).
10. **Private keys** (lower on the same page) → **Generate a private key** → a
    `.pem` file downloads. Send us that file (treat it like a password).

**Return:** App ID · App slug · the `.pem` private key · Webhook secret.

---

## 2. Slack  →  a **Slack app**

**Console:** https://api.slack.com/apps → **Create New App** → **From scratch** →
name it, pick the workspace.

1. **OAuth & Permissions** (left sidebar) → **Redirect URLs** → **Add New
   Redirect URL** → `https://angle-briary-valentina.ngrok-free.dev/integrations/slack/callback` → **Save URLs**.
2. Same page → **Scopes → Bot Token Scopes** → **Add an OAuth Scope**, add all of:
   `channels:read`, `channels:history`, `groups:read`, `groups:history`,
   `users:read`, `team:read`.
3. *(Optional — only if you want private human↔human DMs)* **User Token Scopes:**
   `im:read`, `im:history`, `mpim:read`, `mpim:history`.
4. **Event Subscriptions** (left sidebar) → toggle **Enable Events** on →
   **Request URL:** `https://angle-briary-valentina.ngrok-free.dev/webhooks/slack`.
   - Slack verifies this URL live, so it only succeeds **once our stack + tunnel
     are running**. If it won't verify yet, skip it and come back after we're up.
   - **Subscribe to bot events:** `message.channels` (and `message.groups` for
     private channels) → **Save Changes**.
5. **Basic Information** (left sidebar) → **App Credentials:** copy **Client ID**,
   **Client Secret**, **Signing Secret**.

> Do **not** use Slack's "Install to Workspace" button — we install through our
> own gateway so the token lands in our encrypted store.

**Return:** Client ID · Client Secret · Signing Secret.

---

## 3. Discord  →  an **application + bot** (live is a WSS gateway, no webhook)

**Console:** https://discord.com/developers/applications → **New Application** →
name it → **Create**.

1. **Bot** (left sidebar):
   - **Reset Token** → copy the **bot token** (shown once).
   - Scroll to **Privileged Gateway Intents** → enable **MESSAGE CONTENT INTENT**
     → **Save Changes**. *(Without this, message text arrives empty.)*
2. **OAuth2** (left sidebar):
   - **Redirects** → **Add Redirect** → `https://angle-briary-valentina.ngrok-free.dev/integrations/discord/callback`
     → **Save Changes**.
   - Copy **Client ID**; **Reset Secret** → copy **Client Secret**.
3. **General Information** (left sidebar): copy **Application ID** and **Public
   Key**.

> Inviting the bot into your Discord server happens through our gateway
> (`/integrations/discord/install`) — you don't build an invite URL by hand.

**Return:** Bot Token · Application ID · Public Key · Client ID · Client Secret.

---

## 4. Jira  →  an **API token** (fastest)

**Console:** https://id.atlassian.com/manage-profile/security/api-tokens →
**Create API token** → label it → **Create** → **Copy** (shown once).

Also note:
- your site URL: `https://<your-site>.atlassian.net`
- the **email** of the Atlassian account that made the token
- *(optional, for live webhooks)* pick any random string to use as the webhook
  secret — we register the Jira webhook ourselves later using it.

**Return:** site URL · account email · API token · (optional) webhook secret.

---

## 5. Notion  →  a **public OAuth integration**

**Console:** https://www.notion.so/my-integrations → **New integration**.

1. **Name** it and associate your workspace.
2. **Integration type → Public** *(this reveals the OAuth section; an "Internal"
   integration won't give you OAuth client credentials)*.
3. Fill the required **Company name** and **Website/homepage URL** (can be
   `https://angle-briary-valentina.ngrok-free.dev` or any real URL).
4. **OAuth Domain & URIs → Redirect URIs** → add
   `https://angle-briary-valentina.ngrok-free.dev/integrations/notion/callback`.
5. **Capabilities** → **Read content**.
6. **Save**, then copy the **OAuth client ID** and **OAuth client secret** (the
   secret is shown **only once** — save it).
7. **Crucial — share content:** Notion only exposes pages/databases you connect
   the integration to. For each page/DB to ingest: open it → top-right **•••** →
   **Connections** (or "Add connections") → select this integration.

**Return:** OAuth client ID · OAuth client secret · confirmation that the target
pages/databases have been shared with the integration.

---

## 6. Telegram  →  **api_id / api_hash** (+ an account login)

Telegram uses the **user-account** API (not a bot). The `api_id`/`api_hash` come
from one developer account; the **login** (next paragraph) is for the account
whose chats you want to ingest — usually the same person.

**Console:** https://my.telegram.org → log in with the **phone number** → **API
development tools** → create an app (**App title** + **Short name**; platform
"Other") → copy **api_id** and **api_hash**.

> The interactive login (phone → SMS/app code → 2FA password) is run **on our
> box** by whoever owns that Telegram account, during install. If that's you, be
> available to enter the code; otherwise the account owner runs that one step.

**Return:** api_id · api_hash · (who will do the login + on which phone number).

---

## ✅ Credentials to return (the handoff checklist)

Fill values and send back (the `.pem` and tokens are secrets — share via a
password manager / secure channel, not plaintext chat):

| Source | What | Value |
|---|---|---|
| **GitHub** | App ID | |
| | App slug | |
| | Private key `.pem` | (file) |
| | Webhook secret | |
| **Slack** | Client ID | |
| | Client Secret | |
| | Signing Secret | |
| **Discord** | Bot Token | |
| | Application ID | |
| | Public Key | |
| | Client ID | |
| | Client Secret | |
| **Jira** | Site URL (`https://….atlassian.net`) | |
| | Account email | |
| | API token | |
| | Webhook secret (optional) | |
| **Notion** | OAuth client ID | |
| | OAuth client secret | |
| | Pages/DBs shared with integration? | yes / no |
| **Telegram** | api_id | |
| | api_hash | |
| | Login owner + phone | |

**Reminder:** every redirect/webhook URL above uses the **same base host**
(`https://angle-briary-valentina.ngrok-free.dev`) and must match exactly. If the
tunnel domain changes, update it in every app **and** in `.env.sandbox`.

---

*UI navigation verified current as of June 2026. Provider consoles occasionally
relabel things — if a label moved, the function (e.g. "where you add redirect
URIs") is what matters. Once we have these values, the run steps live in
[sandbox-real-api-runbook.md](sandbox-real-api-runbook.md).*
