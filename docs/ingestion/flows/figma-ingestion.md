# Figma ingestion — deployment-owned OAuth

Fyralis connects Figma through a **customer-owned OAuth app in each BYOC
deployment**. A deployment administrator performs the one-time Figma app
registration; an ordinary onboarding user only pastes the Figma file URLs they
want Fyralis to read and approves Figma consent. No user enters a Figma personal
access token, OAuth client secret, or callback URL into the Fyralis UI.

This is the current Figma source contract. The onboarding host must provide an
authenticated Fyralis gateway session to its browser client. Local development
uses a dev-only in-memory bridge documented separately; this document does not
claim that a production cookie or BFF bridge is implemented. The older PAT
`/connect/preflight` and `/connect/finalize` endpoints are retained only as an
operator migration fallback and are not part of normal onboarding.

For the administrator checklist, see
[Figma BYOC OAuth administration](../../operations/figma-byoc-oauth-admin.md).
For the durable design-document artifact contract, see
[Figma design artifacts](../figma-design-artifacts.md).

## Operating model

| Role | Responsibility | Does not do |
| --- | --- | --- |
| BYOC deployment administrator | Creates one private Figma OAuth app for that customer deployment, registers the exact callback, stores the app secret in the deployment secret manager, and verifies readiness. | Does not share the app secret or deploy a shared Fyralis-owned app. |
| Figma user | Chooses explicit file URLs in onboarding and approves the deployment's Figma OAuth consent screen. | Does not create an app, paste a PAT, or handle a client secret. |
| Fyralis gateway and workers | Run PKCE OAuth, exchange and refresh tokens server-side, validate the selected files, write snapshots, and create observations. | Never send an OAuth token, refresh token, client secret, or S3 locator to the browser. |

The isolation boundary is deliberate: each customer cloud owns its Figma app,
redirect URL, client credentials, OAuth state key, encrypted token material,
and data plane. Rotating or disabling one deployment's app cannot affect
another deployment.

## One-time administrator setup

The administrator creates a **private** app in [Figma Developer
Apps](https://www.figma.com/developers/apps), owned by the customer Figma team
or organization. Private is the normal BYOC mode because the app is used only
by members of that customer's Figma organization.

Figma app registration is an app-owner browser action. OAuth authorization can
be automated for users after an app exists, but Figma does not expose an API
that lets Fyralis create the customer's app or silently add its redirect URL.

The app must use this exact callback shape:

```text
https://<customer-gateway-host>/integrations/figma/oauth/callback
```

The gateway rejects a redirect URI with a query string, fragment, credentials,
or a different path. This avoids an overly broad callback allowlist.

The deployment requests only the scopes implemented by the current ingestion
surface:

```text
current_user:read
file_metadata:read
file_content:read
file_comments:read
file_versions:read
```

In particular, the normal snapshot onboarding path does **not** request Dev
Mode, variables, library analytics, or webhook-management scopes. Those need a
separate product decision and scope review before they are added.

The deployment administrator stores the Figma client secret only in the
customer's deployment secret manager. `FIGMA_CLIENT_ID`, the exact redirect
URI, the UI return origin, the requested scopes, and the feature-enable flag
are deployment configuration; the client secret and OAuth-state HMAC key are
secrets. The browser never receives either category of secret value.

An authenticated tenant administrator can inspect the safe setup contract at:

```text
GET /api/admin/integrations/figma/oauth/readiness
```

That endpoint returns readiness booleans, the non-secret callback URI, the
required scopes, and the Figma console link. It intentionally omits client
secrets, secret references, access tokens, refresh tokens, and provider error
bodies. If readiness is false, the normal onboarding status endpoint returns a
generic `deployment_setup_required` state instead of leaking configuration
details to an ordinary user.

## End-user connection flow

Once the deployment passes the administrator setup and the onboarding host has
provided an authenticated Fyralis gateway session, the user journey is short:

1. Open the Figma source card in Fyralis onboarding.
2. Paste one or more `https://*.figma.com/...` file URLs (up to 100). These are
   the explicit Fyralis allowlist; onboarding does not enumerate an entire
   organization or silently discover extra files.
3. Select **Continue with Figma** and approve the deployment-owned app in
   Figma.
4. Figma returns the browser to the exact registered callback. The gateway
   validates and consumes one-use state, exchanges the authorization code, and
   redirects back to the trusted Fyralis onboarding origin.
5. The source card polls connection status until the initial observation is
   visible.

The user sees neither the OAuth client configuration nor Figma token values.
The browser carries a signed state nonce only; the PKCE verifier is stored
server-side as an encrypted, short-lived secret.

### Gateway route contract

| Route | Caller | Purpose |
| --- | --- | --- |
| `POST /integrations/figma/oauth/start` | Authenticated Fyralis user | Validates explicit file URLs, writes one-use state and PKCE verifier, and returns Figma's authorization URL. |
| `GET /integrations/figma/oauth/callback` | Figma redirect | Consumes state, exchanges the code server-side, validates selected-file access, and finalizes the install. |
| `GET /integrations/figma/connect/status` | Authenticated Fyralis user | Shows connection, selected-file, sync, and safe observation status. |
| `POST /integrations/figma/connect/retry` | Authenticated Fyralis user | Requeues the initial sync when a connection exists. |
| `DELETE /integrations/figma/connect` | Authenticated Fyralis user | Disables the local installation and removes local credential material. |
| `GET /api/admin/integrations/figma/oauth/readiness` | Tenant administrator | Shows the sanitized deployment-app setup contract. |

OAuth state is tenant-bound, signed, single-use, and short-lived. The callback
only returns to an allowlisted Fyralis UI origin; a caller cannot turn
`return_path` into an external redirect.

## What Fyralis reads

The current read surface is deliberately file-scoped and supports useful design
intelligence without requesting broad organization administration:

| Figma read | Why Fyralis reads it | Output |
| --- | --- | --- |
| `GET /v1/me` | Verifies the OAuth grant holder during the callback. | OAuth installation identity. |
| `GET /v1/files/{key}?depth=1` | Proves each explicitly selected file is accessible and obtains basic metadata. | Selected-file validation and lightweight change probe. |
| `GET /v1/files/{key}` | Retrieves the complete Figma document JSON when a selected file changed. | Durable design snapshot artifact and `figma:file_snapshot` observation. |
| `GET /v1/files/{key}/versions` | Captures named-version activity. | Derived `figma:event` observation. |
| `GET /v1/files/{key}/comments` | Captures comment activity. | Derived `figma:event` observation. |

Figma has no file `/events` endpoint. Fyralis derives a local event stream from
the versions and comments endpoints, then paginates that derived result for the
standard ingestion worker.

The current normal path does not auto-enumerate teams, projects, or every file
visible to the user. It also does not provision Figma webhooks during OAuth
onboarding. Initial onboarding schedules the snapshot and event work for the
selected-file allowlist; event reconciliation covers versions/comments and
also queues a version-aware snapshot probe. That probe fetches the full design
again only when the Figma file version changed. Webhooks remain a separately
operated, legacy/live-ingress capability and are not required for the initial
observation proof.

## From a Figma file to an observation

Every selected file creates two independent worker shards:

```text
selected Figma file
  ├─ versions + comments shard  ──> figma:event observations
  └─ document snapshot shard     ──> figma:file_snapshot observation
```

The snapshot shard first makes a shallow document request. If Figma reports the
same version as the last captured version, it stops without downloading the
full tree. When the version changed (or on the first run), it fetches the full
document JSON, writes it to the tenant's durable object bucket, and emits one
snapshot record. A file with no comments or named versions still produces a
snapshot observation.

The snapshot handler creates a `figma:file_snapshot` observation with
`object_type: figma_file_snapshot`. Its searchable content includes bounded
metadata such as the file name/key, version, page names, node count, and a
bounded text preview. It also has a safe `content.artifacts` reference:

```json
{
  "kind": "figma_document_json",
  "blob_id": "uuid",
  "content_type": "application/json",
  "content_hash": "blake2b:…",
  "size_bytes": 18273491
}
```

The actual bucket and object key live only in the tenant-scoped `blobs` catalog,
with an `observation_artifacts` link. They are not stored in observation
content, a browser response, or a permanent presigned URL. An authenticated
tenant user can retrieve an authorized artifact through:

```text
GET /integrations/figma/observations/{observation_id}/artifacts/{blob_id}
```

See [Figma design artifacts](../figma-design-artifacts.md) for the full access
and integrity contract.

## Change and refresh behavior

OAuth access and refresh tokens are stored only as encrypted tenant-scoped
references. Workers refresh an OAuth grant before expiry and retry once after a
401/403 when refresh is available. A failed or revoked refresh transitions the
connection toward reauthorization rather than asking an end user to paste a
token.

For each selected file, Fyralis also derives events from named versions and
comments. Event identifiers are versioned so an updated version or comment can
land as a new observation while a re-fetch of the same record deduplicates.

Figma file contents can contain unreleased product plans, customer data, or
other sensitive text. The deployment's normal data-retention, access-control,
and object-storage policies apply to the durable snapshot artifact as well as
to the observation projection.

## Boundaries and future expansion

The current source gives a company-intelligence layer a reliable design-system
anchor: the actual document tree, pages, text content, named versions, and
comments for user-approved files. It intentionally does not yet fetch or infer:

- every file in a team or organization;
- Dev Mode resources, code-connect metadata, variables, or component usage;
- library analytics or organization/admin data;
- webhook subscriptions from the OAuth app; or
- data from a file the consenting user cannot access.

Adding any of these requires a separate scope, privacy, rate-limit, and
onboarding review. It should not silently expand the permissions of an existing
deployment-owned app.

## Legacy PAT fallback

The codebase still contains PAT preflight/finalize routes and dedicated-table
support for operator migrations. They are intentionally outside the normal
BYOC onboarding UI. Do not document a personal access token as the default
customer setup, do not put one in browser storage, and do not use it to avoid
the deployment-owned app setup.
