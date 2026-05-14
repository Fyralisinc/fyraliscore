# HTTP Contract — `/integrations/discord/*`

Mounted under the existing `services/integrations/router.py::build_integrations_router()` factory. The router's `prefix='/integrations'` already applies; sub-routes below are relative.

## `GET /integrations/discord/install`

**Auth**: Bearer (via `BearerAuthMiddleware`). Authenticated tenant id is read from `request.state.auth.tenant_id` — never from a query parameter.

**Request**: no body, no query parameters.

**Response — 302 Found**:

```http
HTTP/1.1 302 Found
Location: https://discord.com/oauth2/authorize?client_id=<DISCORD_CLIENT_ID>&scope=applications.commands+bot&permissions=<MIN_PERMS>&redirect_uri=<DISCORD_REDIRECT_URI>&state=<signed_token>&response_type=code
```

| Discord URL parameter | Source | Notes |
|---|---|---|
| `client_id` | `os.environ['DISCORD_CLIENT_ID']` | app-level, set per deployment |
| `scope` | hardcoded `applications.commands+bot` | space-encoded as `+` per OAuth |
| `permissions` | hardcoded integer (send_messages + view_channel; final value computed once and pinned in `oauth.py`) | minimum-viable for Phase 1 |
| `redirect_uri` | `os.environ['DISCORD_REDIRECT_URI']` | exact-match against the value configured in the Discord App; URL-encoded |
| `state` | `issue_state_token(tenant_id, pool, provider='discord')` | HMAC-SHA256 over `{tenant_id, nonce, expires_at}` with `OAUTH_STATE_HMAC_KEY`; nonce persisted in `oauth_install_states` |
| `response_type` | hardcoded `code` | OAuth v2 |

**Response — 401 Unauthorized**: when Bearer middleware rejects (no token, expired, etc.). Same shape as every other Bearer-required route.

**Idempotency**: each call mints a fresh nonce. Multiple parallel `install` calls from the same browser produce multiple `oauth_install_states` rows, each single-use; only the one whose state token Discord echoes back gets consumed.

---

## `GET /integrations/discord/callback`

**Auth**: **public route** (added to `_PUBLIC_PATHS` in `services/gateway/main.py` as an exact-match path, NOT a prefix). Authentication is the state token, not the Bearer.

**Request — query parameters**:

| Parameter | Required | Notes |
|---|---|---|
| `code` | yes | Discord OAuth authorization code |
| `state` | yes | the signed state token from `install` |
| `guild_id` | optional | Discord may pass `guild_id` separately; if present, must match what `oauth2/token` returns |
| `permissions` | optional | Discord's response of granted bot permissions; informational only |

**Happy path** (all checks pass):

1. Verify state-token HMAC + expiry; atomically consume via `UPDATE oauth_install_states SET consumed_at=now() WHERE nonce=$1 AND provider='discord' AND consumed_at IS NULL AND expires_at > now() RETURNING tenant_id`. Zero rows → state-token failure.
2. POST `https://discord.com/api/v10/oauth2/token` with `grant_type=authorization_code, code=<code>, redirect_uri=<exact match>` using HTTP Basic auth `(client_id:client_secret)` from env. Receive `access_token`, `token_type`, `scope`, `guild.id`, `application.id`.
3. Encrypt + store the bot token at `encrypted_secrets[label=f'discord_bot_token:{guild_id}']`.
4. Encrypt + store the application public key (from `WEBHOOK_SECRET_DISCORD` env) at `encrypted_secrets[label=f'discord_public_key:{guild_id}']`.
5. UPSERT `provider_installations`:

```sql
INSERT INTO provider_installations (id, tenant_id, provider, installation_id, secret_ref, enabled, installed_at)
VALUES ($1, $2, 'discord', $3, $4, TRUE, now())
ON CONFLICT (provider, installation_id) DO UPDATE
  SET tenant_id = EXCLUDED.tenant_id,
      secret_ref = EXCLUDED.secret_ref,
      enabled = TRUE,
      installed_at = now()
  WHERE provider_installations.tenant_id = EXCLUDED.tenant_id
RETURNING id, (xmax = 0) AS inserted
```

Zero rows returned = cross-tenant collision (the `WHERE` predicate filtered out the UPDATE branch). Surface `InstallationCollisionError`, write audit row, 302 to install-error.

6. POST `https://discord.com/api/v10/applications/{application_id}/commands` with the `/fyralis` command spec (research R10). Bot token from step 3.
7. INSERT `installation_audit_log` with `action='install', status='ok'` (or `'error'` if step 6 failed — Phase 4 keeps the install live but logs the error).

**Response — 302 Found**:

| Outcome | Location |
|---|---|
| Happy path | `Location: /integrations/discord/installed?guild=<blake2b(guild_id, digest_size=8).hexdigest()>` (the hash, not the raw guild_id) |
| `state_invalid` (HMAC mismatch) | `Location: /integrations/discord/install-error?reason=state_invalid` |
| `state_expired` (expires_at < now) | `Location: /integrations/discord/install-error?reason=state_expired` |
| `state_consumed` (consumed_at IS NOT NULL) | `Location: /integrations/discord/install-error?reason=state_consumed` |
| `discord_oauth_token_exchange_failed` | `Location: /integrations/discord/install-error?reason=slack_oauth_error` (reused from IN-08 — UI shell treats as "OAuth provider returned an error") |
| `discord_oauth_missing_guild` | `Location: /integrations/discord/install-error?reason=slack_oauth_error` |
| `installation_collision` | `Location: /integrations/discord/install-error?reason=installation_collision` |
| `secret_store_unavailable` | `Location: /integrations/discord/install-error?reason=secret_store_unavailable` |
| Command registration failed (Discord 4xx after install) | Happy-path redirect (install is recoverable), audit row carries `status='error'` and `context.error_code=<discord_code>` |

The HTTP status of the callback itself is 302 in every case; the failure category lives in the redirect's `reason` query parameter. This matches IN-08's contract exactly.

**Idempotency**: re-running the callback with a consumed state-token returns the `state_consumed` redirect (no DB writes). Re-running with a fresh state-token but the same `guild_id` (re-install) reuses the existing `provider_installations.id` per the UPSERT and the audit log gets a second `install/ok` row — by design (research R6 for the analogous chokepoint reasoning).

---

## `GET /integrations/discord/installed`

**Auth**: public (success-page route handled by the UI shell, not this backend). Not implemented in IN-09; the redirect target is owned by the existing UI.

**Query parameter**: `guild=<short_hash>` — opaque identifier, not the raw guild_id (SC-006 / FR-005 — guild ids must not leak through public URLs).

---

## `GET /integrations/discord/install-error`

**Auth**: public.

**Query parameter**: `reason=<code>` from the table above.

Owned by the UI shell. Not implemented in IN-09 backend.

---

## Public-path allowlist (`services/gateway/main.py::_PUBLIC_PATHS`)

The following exact-match paths MUST appear in `_PUBLIC_PATHS` after this task lands:

- `/integrations/discord/callback`
- `/integrations/discord/installed`
- `/integrations/discord/install-error`

The Bearer-required `/integrations/discord/install` is **not** on the allowlist. Constitution §III + IN-08 review-gate: never use a prefix like `/integrations/discord/*` in the allowlist.
