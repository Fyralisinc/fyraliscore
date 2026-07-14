# Figma BYOC OAuth administration

Use this runbook once for each customer BYOC deployment. The deployment owns its
own Figma OAuth app and credentials; it does not reuse a Fyralis-hosted app or
another customer's app.

After this one-time setup, an ordinary user simply chooses Figma file URLs in
Fyralis onboarding and approves consent. They never paste a Figma token or see
the app's credentials.

This runbook configures the Figma side of the connection. The production
onboarding host must separately provide an authenticated Fyralis gateway session
to its browser client. It does not imply that a cookie or BFF session bridge is
already implemented; the local guide's short-lived token bridge is development
only.

## Prerequisites

You need:

- deployment-administrator access to the customer cloud and its secret manager;
- permission to deploy or restart the Fyralis gateway and ingestion workers;
- a Figma account that can create and manage an app for the customer's Figma
  organization or team; and
- a stable public HTTPS gateway hostname.

Use a private Figma app for this BYOC deployment. If a customer needs to support
people outside its Figma organization, treat a public app as a separate product,
security, and Figma-review decision rather than changing this deployment's
private app casually.

## 1. Decide the callback and UI origins

Choose the public gateway hostname and the Fyralis browser UI origin.

The Figma callback must be exactly:

```text
https://<gateway-host>/integrations/figma/oauth/callback
```

Requirements:

- HTTPS is required in a deployed environment.
- The path is fixed: do not add a path suffix, query string, fragment, user
  credentials, or a second callback URL.
- The same exact URL must appear in both deployment configuration and the Figma
  app's OAuth credentials.
- `FIGMA_OAUTH_UI_BASE_URL` is the trusted origin that receives the final
  post-consent redirect. It is not a Figma callback and must be the Fyralis UI
  origin, for example `https://app.customer.example`.

For local-only tunnel testing, see
[Figma OAuth local onboarding](figma-oauth-local-onboarding.md). Do not carry
the local HTTP loopback exception into a deployed customer environment.

## 2. Create the customer-owned private Figma app

1. Sign in as the customer's Figma app owner and open [Figma Developer
   Apps](https://www.figma.com/developers/apps).
2. Create a **private** OAuth app owned by the customer organization/team.
3. In **OAuth credentials**, add the exact callback URL from step 1 and save it.
4. Configure the following scopes, and no broader scopes for the current
   snapshot feature:

   ```text
   current_user:read
   file_metadata:read
   file_content:read
   file_comments:read
   file_versions:read
   ```

5. Record the Client ID and Client Secret in the customer's approved secret
   handling workflow.

Figma requires this app-owner console action. The subsequent end-user consent
flow is automated, but Fyralis cannot create the customer app or register its
redirect URL through a Figma API.

## 3. Configure the customer deployment

Set the Figma configuration in the customer deployment's runtime configuration.
Use the deployment secret manager for the client secret and leave the raw
secret environment variable empty:

```dotenv
FIGMA_OAUTH_ENABLED=1
FIGMA_CLIENT_ID=<customer-figma-client-id>
FIGMA_CLIENT_SECRET_SECRET_REF=<managed-secret-reference>
FIGMA_CLIENT_SECRET=
FIGMA_REDIRECT_URI=https://<gateway-host>/integrations/figma/oauth/callback
FIGMA_OAUTH_UI_BASE_URL=https://<fyralis-ui-host>
FIGMA_OAUTH_ALLOW_HTTP_LOOPBACK=0
FIGMA_OAUTH_SCOPES=current_user:read,file_metadata:read,file_content:read,file_comments:read,file_versions:read
```

The deployment also needs its normal managed
`OAUTH_STATE_HMAC_KEY_SECRET_REF`. It protects signed, tenant-bound OAuth
state. Do not put either the Figma Client Secret or state-HMAC value in a
checked-in file, browser environment variable, browser storage, support ticket,
or chat message.

The checked-in [production environment
template](../../.env.production.example) contains the required key names and
safe placeholders. Run the environment contract check as part of deployment
validation:

```bash
scripts/check_production_env_contract.py
```

## 4. Deploy and verify readiness

Deploy or restart the gateway and the Figma ingestion workers after setting the
configuration.

As an authenticated Fyralis tenant administrator, call:

```text
GET /api/admin/integrations/figma/oauth/readiness
```

The local Fyralis console exposes the same safe check at **Control Panel →
Figma deployment setup** (`/host/control-panel`). It requires the tenant-admin
gateway session; it never asks for or displays the Figma Client Secret.

A ready response has:

```json
{
  "runtime_ready": true,
  "source_enabled": true,
  "recommended_app_mode": "private",
  "raw_secret_values_included": false
}
```

It also shows the safe callback URL and required scopes. Verify them against the
Figma app before allowing users to connect. The endpoint intentionally does not
return a secret, secret reference, access token, refresh token, or Figma
provider response.

If the response is not ready, correct the deployment configuration or secret
provider, redeploy, and check again. Do not ask an ordinary onboarding user to
work around a readiness failure with a personal access token.

## 5. Hand off to the onboarding user

Tell the user to:

1. Open **Onboarding → Sources → Figma**.
2. Paste the one or more Figma file URLs Fyralis may ingest.
3. Select **Continue with Figma**.
4. Approve the private app in Figma.
5. Wait for the Figma card to show the first observation.

The selected file URLs are an explicit allowlist. The current path does not
enumerate the user's whole Figma organization or read a file they did not
select. The callback validates each selected file with the OAuth grant before
persisting the connection.

## 6. Verify the snapshot observation

The onboarding proof includes a durable snapshot observation with:

```text
source_channel = figma:file_snapshot
content.object_type = figma_file_snapshot
```

It contains a bounded design projection and a safe artifact reference. The full
Figma document JSON is stored in tenant-scoped object storage; observation
content does not expose its bucket, object key, presigned URL, or credentials.

For database inspection and artifact retrieval details, see
[Figma design artifacts](../ingestion/figma-design-artifacts.md).

## Rotation, failure, and offboarding

- **Rotate the Figma Client Secret:** update the managed secret value or
  reference, redeploy the gateway/workers, then rerun the readiness check.
  Existing user OAuth grants remain tenant-scoped encrypted references.
- **OAuth grant revoked or expired:** use the source card's reauthorization
  path. Do not substitute a PAT.
- **Figma app disabled or callback changed:** restore the exact registered
  callback and deployment configuration, then redeploy and verify readiness.
- **Disconnect a source:** use the authenticated Figma disconnect action. It
  disables the local installation and removes local credential material.
- **Decommission a BYOC deployment:** delete or disable its Figma app only
  after the Fyralis source is disconnected and the customer's retention policy
  is satisfied.

## Security checklist

- [ ] Exactly one private Figma app is owned by this customer deployment.
- [ ] Callback equals
  `https://<gateway-host>/integrations/figma/oauth/callback` in Figma and
  runtime configuration.
- [ ] Only the five documented read scopes are requested.
- [ ] `FIGMA_OAUTH_ENABLED=1` only after the app and managed secret are ready.
- [ ] `FIGMA_CLIENT_SECRET` and `OAUTH_STATE_HMAC_KEY` are injected through
  managed secret references, never browser-visible configuration.
- [ ] The readiness endpoint is verified by a tenant administrator.
- [ ] The end-user flow uses file URLs plus Figma consent, never a PAT.
