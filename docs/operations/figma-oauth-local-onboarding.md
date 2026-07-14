# Figma OAuth local onboarding

This is the local-development path for the same OAuth-first Figma onboarding
used by a BYOC deployment: a deployment-owned Figma app, explicit file URLs,
Figma consent, a durable design snapshot, and direct database inspection.

Do not reuse this development app or its credentials for a customer deployment.
For the production per-deployment administrator checklist, see
[Figma BYOC OAuth administration](figma-byoc-oauth-admin.md).

## 1. Configure the development deployment's Figma app

Create a private Figma OAuth app for this development environment at
`https://www.figma.com/developers/apps`. The Figma app owner must add the
callback in the Figma console; that registration cannot be made through an API.
After exposing the local gateway through an HTTPS tunnel, add this exact
callback URL:

```text
https://<your-tunnel-host>/integrations/figma/oauth/callback
```

Set these gateway/runtime values in the local `.env.production` file; do not
commit them. The local overlay injects that file into the gateway and workers:

```bash
FIGMA_CLIENT_ID=<figma-oauth-client-id>
FIGMA_CLIENT_SECRET=<figma-oauth-client-secret>
FIGMA_REDIRECT_URI=https://<your-tunnel-host>/integrations/figma/oauth/callback
FIGMA_OAUTH_UI_BASE_URL=http://localhost:3003
# Allow only this loopback HTTP return target locally. Never set this in a
# deployed environment.
FIGMA_OAUTH_ALLOW_HTTP_LOOPBACK=1
FIGMA_OAUTH_ENABLED=1
FIGMA_OAUTH_SCOPES=current_user:read,file_metadata:read,file_content:read,file_comments:read,file_versions:read
```

Copy `ui/.env.example` to `ui/.env.local`, then set the browser-facing values
there (or export them before starting the Next development server), not in
`.env.production`:

```bash
NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL=https://<your-tunnel-host>
# Local development only. Do not use a browser-exposed gateway token in production.
NEXT_PUBLIC_FYRALIS_GATEWAY_TOKEN=<your-local-dev-session-token>
```

Use a private app for this local environment and for a normal customer BYOC
deployment. A public Figma app is a separate review and product decision; it is
not needed to validate the private-app onboarding flow.

### Mint the local UI session

The onboarding UI sends a normal short-lived Fyralis bearer session to the
gateway. Pick a local actor and its tenant (or use the seeded sandbox actor):

```bash
docker compose exec -T postgres psql -U company_os -d company_os -Atc \
  "SELECT id || ',' || tenant_id FROM actors ORDER BY created_at LIMIT 1"
```

Mint a one-hour token, replacing the two UUIDs with that output. Include the
value of `AUTH_BOOTSTRAP_SECRET` from your uncommitted `.env.production` file:

```bash
curl -sS -X POST http://localhost:8000/auth/session \
  -H 'Content-Type: application/json' \
  -H 'X-Bootstrap-Secret: <your-local-bootstrap-secret>' \
  --data '{"actor_id":"<actor-uuid>","tenant_id":"<tenant-uuid>","ttl_seconds":3600}'
```

Put the returned `token` in `NEXT_PUBLIC_FYRALIS_GATEWAY_TOKEN` in
`ui/.env.local`. This is a dev-only in-memory bridge for the running local UI;
never persist it in browser storage or deploy it in a checked-in/browser-facing
environment file. A production host must supply its own authenticated gateway
session. This guide does not define or claim an implemented production
cookie/BFF bridge.

## 2. Start local services

Build the local core image once. The optional `github-intel` extension is
private in this workspace, so this deliberately builds the Figma path without
that unrelated extension:

```bash
docker build --build-arg GITHUB_INTEL_REF= -t fyraliscore-gateway .
```

```bash
docker compose -f docker-compose.yml -f docker-compose.pgadmin.yml -f docker-compose.figma-local.yml up -d postgres minio pgadmin
# Re-run this one-shot initializer even when an older local MinIO volume already exists.
# It creates the durable `fyralis-blobs` bucket used by design snapshots.
docker compose -f docker-compose.yml -f docker-compose.pgadmin.yml -f docker-compose.figma-local.yml up -d --force-recreate minio-init
docker compose -f docker-compose.yml -f docker-compose.pgadmin.yml -f docker-compose.figma-local.yml up -d --no-build gateway oauth_poller tenant_onboarding source_onboarding shard_fetch normalizer observation_writer reconciler periodic_reconciler
```

The gateway needs a public HTTPS URL for Figma OAuth callbacks. For example:

```bash
ngrok http 8000
```

If the local ngrok account already has its free endpoint in use, use the
accountless Cloudflare quick-tunnel fallback and keep that terminal running:

```bash
cloudflared tunnel --url http://localhost:8000 --no-autoupdate
```

Use the public HTTPS URL printed by either tunnel in both places, with the
callback path appended:

```text
https://<tunnel-host>/integrations/figma/oauth/callback
```

1. Set it as `FIGMA_REDIRECT_URI` in `.env.production`.
2. Add that exact URL under **OAuth credentials → Add a redirect URL** for the
   Figma app, then save/publish the app.

`docker-compose.figma-local.yml` is intentionally local-only: it makes the
Figma path run in development posture while preserving stable, local encrypted
secrets from `.env.production`. After changing `.env.production`, restart the
gateway and workers. Restart the Next development server after changing
`ui/.env.local`.

Start the onboarding UI in a second terminal:

```bash
cd ui
npm run dev -- -p 3003
```

## 3. Connect Figma as an onboarding user

Open:

```text
http://localhost:3003/onboarding/sources/figma
```

Paste one or more Figma file URLs and choose **Continue with Figma**. The
callback validates the requested files, creates the connection, queues the
initial snapshot, returns to the trusted onboarding UI, and the UI polls until
an observation is visible. The file URLs are an explicit allowlist; the UI does
not ask for a Figma personal access token, client secret, or callback URL.

OAuth onboarding cannot enumerate every file visible to a user. File URLs are
the explicit Fyralis ingestion allowlist. The completed path snapshots those
files and polls named versions/comments; a Figma webhook subscription is not
required for the initial onboarding proof. When reconciliation detects a newer
version or comment event, it also schedules a version-aware design snapshot
probe; the full document is fetched again only if Figma reports that its file
version changed.

## 4. pgAdmin connection

Start pgAdmin with the compose command above, then open:

```text
http://localhost:5050
```

Local pgAdmin login:

```text
Email:    admin@fyralis.com
Password: admin
```

The preconfigured **Fyralis — Figma onboarding and observations** server points
at the local database. For a desktop pgAdmin connection use:

```text
Host:     127.0.0.1
Port:     5433
Database: company_os
Username: company_os
Password: company_os
SSL mode: Prefer
```

Run [inspect_figma.sql](../../ops/pgadmin/inspect_figma.sql) in pgAdmin to see
OAuth connection metadata, the selected file allowlist, snapshot observations,
and durable blob links. These local defaults are intentionally not production
credentials.
