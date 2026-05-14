# HTTP Contract — `/integrations/slack/*`

Mounted by `services/integrations/router.py`. Wired into the gateway via `app.include_router(build_integrations_router())` in `services/gateway/main.py::build_app`.

Two routes:

1. `GET /integrations/slack/install` — Bearer-authenticated. Issues a state token, persists the nonce, 302s to Slack.
2. `GET /integrations/slack/callback` — Public (state-token-authenticated). Consumes the nonce, exchanges the OAuth code, persists tokens, upserts `provider_installations`, writes audit, 302s to a Fyralis UI URL.

## Gateway allowlist

Both routes are **specific entries** in `services/gateway/main.py::_PUBLIC_PATHS` (exact-match, not prefix-match):

```python
_PUBLIC_PATHS = frozenset({
    "/healthz",
    "/auth/session",
    "/integrations/slack/install",     # NEW (Bearer middleware bypass; route handler asserts Bearer separately)
    "/integrations/slack/callback",    # NEW (no Bearer; state-token-authed inside handler)
})
```

> **Wait — the install route is Bearer-required.** The reason it's still in `_PUBLIC_PATHS` is that the Bearer middleware as currently implemented attaches the actor to `request.scope`; the install route then asserts `request.state.actor is not None` itself. This keeps the actor-binding code in one place (the route handler) and avoids fiddly per-route middleware exemptions. Equivalent alternative: leave `/install` OUT of the public list and let the standard Bearer middleware handle it. **Plan locks on the second option** — the install route is Bearer-required and stays subject to the standard middleware. Only `/callback` is added to `_PUBLIC_PATHS`.

Final allowlist edit (locked):

```python
_PUBLIC_PATHS = frozenset({
    "/healthz",
    "/auth/session",
    "/integrations/slack/callback",    # NEW — public; state-token-authed
})
# /integrations/slack/install is NOT public — Bearer-required, standard middleware path.
```

This avoids any "blanket public" exposure (per ClickUp body's "single-route, not blanket public").

---

## Route 1 — `GET /integrations/slack/install`

**Auth**: Bearer (standard middleware).
**Inputs**: none beyond the session (`request.state.actor`, `request.state.tenant_id`).
**Outputs**: HTTP 302 with `Location: https://slack.com/oauth/v2/authorize?…`.

### Handler steps

1. Resolve `tenant_id = request.state.tenant_id`.
2. Generate `nonce = secrets.token_urlsafe(32)`.
3. `expires_at = now() + INSTALL_STATE_TTL` (default 10 min, env-configurable).
4. `INSERT INTO oauth_install_states (id, tenant_id, nonce, provider, expires_at) VALUES (uuid7(), $tenant_id, $nonce, 'slack', $expires_at)`.
5. `state_token = base64url(payload_json) + "." + base64url(hmac_sha256(SERVER_HMAC_KEY, payload_json))` where `payload_json = {"tenant_id": str(tenant_id), "nonce": nonce, "expires_at": iso8601(expires_at)}`. The `tenant_id` in the payload is for client-side debugging only; the **DB nonce is the binding** at consumption time.
6. Construct the Slack URL: `https://slack.com/oauth/v2/authorize?client_id=$SLACK_CLIENT_ID&scope=$SCOPES&state=$state_token&redirect_uri=$REDIRECT_URI`.
   - `$SCOPES = "channels:history,groups:history,im:history,mpim:history,users:read,team:read"` (bot scopes).
   - `$REDIRECT_URI` is a config var that points at the public `/integrations/slack/callback` URL of this deployment.
7. Return `RedirectResponse(slack_url, status_code=302)`.
8. Emit metric: `slack_install_outcomes_total{outcome="initiated"}`.

### Errors

| Condition | Response | Error code | Metric |
|-----------|----------|------------|--------|
| `request.state.actor` is None | HTTP 401 | `missing_bearer` (standard middleware) | n/a |
| `SLACK_CLIENT_ID` not configured | HTTP 500 | `slack_client_unconfigured` (fail-startup recommended; this is a defensive runtime check) | n/a |
| DB insert fails (e.g., pool unavailable) | HTTP 503 | `oauth_state_persist_failed` | n/a |

---

## Route 2 — `GET /integrations/slack/callback`

**Auth**: Public (no Bearer); the **state token is the auth**.
**Inputs**:
- Query param `code: str` — required, non-empty.
- Query param `state: str` — required; the opaque `<payload>.<sig>` string from Route 1.
- Optional query param `error: str` (Slack returns this if the user declines consent).

**Outputs**: HTTP 302 with `Location: /integrations/slack/installed?team=<short_hash>` on success, or `Location: /integrations/slack/install-error?reason=<code>` on failure. The HTTP status of the response itself is `302` for the redirect; automated tests assert on the `Location` header's `reason=` query param to distinguish branches.

### Handler steps (happy path)

1. **Parse state**: split `state` on `.`, base64-decode parts, verify HMAC. On any failure → `state_invalid`.
2. **Parse payload**: extract `nonce`, `tenant_id`, `expires_at`. On JSON/format errors → `state_invalid`.
3. **Atomic consume**:
   ```sql
   UPDATE oauth_install_states
      SET consumed_at = now()
    WHERE nonce = $1
      AND consumed_at IS NULL
      AND expires_at > now()
   RETURNING id, tenant_id, provider
   ```
   - Zero rows returned + nonce exists with `consumed_at IS NOT NULL` → `state_consumed`.
   - Zero rows returned + nonce exists with `expires_at <= now()` → `state_expired`.
   - Zero rows returned + nonce does not exist at all → `state_invalid` (could be a forged or never-issued nonce).
   - Returned row's `tenant_id` differs from the payload `tenant_id` → `state_invalid` (defense-in-depth; should not happen because issuance binds them).
4. **Slack OAuth exchange**: `POST https://slack.com/api/oauth.v2.access` with `code`, `client_id`, `client_secret`. Parse response. On `ok=false` → `slack_oauth_error` (the `error` field becomes a structured context field, never logged verbatim with `team_id`).
5. **Persist tokens** via `SecretStore.put`:
   - `bot_ref = put(access_token, label=f"slack_bot_token:{team_id}", tenant_id)`
   - If `authed_user.access_token` is present: `user_ref = put(authed_user.access_token, label=f"slack_user_token:{team_id}", tenant_id)`
   - If no `slack_signing_secret_ref` exists for this tenant yet: `put(SLACK_SIGNING_SECRET, label="slack_signing_secret:app", tenant_id)`. (The signing secret is per-app, but each tenant keeps its own copy in `encrypted_secrets` to avoid cross-tenant reads.)
6. **Upsert `provider_installations`**:
   ```sql
   INSERT INTO provider_installations (id, tenant_id, provider, installation_id, secret_ref, enabled)
   VALUES ($id, $tenant_id, 'slack', $team_id, $bot_ref, TRUE)
   ON CONFLICT (provider, installation_id) DO UPDATE
     SET tenant_id = EXCLUDED.tenant_id,
         secret_ref = EXCLUDED.secret_ref,
         enabled    = TRUE
     WHERE provider_installations.tenant_id = EXCLUDED.tenant_id
   RETURNING id, (xmax = 0) AS was_inserted
   ```
   - `was_inserted = TRUE` → fresh install.
   - `was_inserted = FALSE` → re-install for same tenant; row reused (preserves `provider_installations.id` per FR-018).
   - Zero rows returned (ON CONFLICT WHERE failed) → cross-tenant rebind attempt → `installation_collision`. Goto step 8 with the collision branch.
7. **Audit**: `INSERT INTO installation_audit_log (id, tenant_id, installation_row_id, provider, action, status, context)` with `action='install'`, `status='ok'`, `context={"scopes": [...], "actor_session_id": ..., "was_reinstall": <bool>}`. Never include `team_id` in `context`.
8. **Invalidate resolver cache** for `(slack, team_id)` so the very next webhook sees the upserted row.
9. **Metric**: `slack_install_outcomes_total{outcome="success"}` + `slack_install_duration_seconds.observe(elapsed)`.
10. **Redirect**: `RedirectResponse(f"/integrations/slack/installed?team={short_hash(team_id)}", status_code=302)`.

### Failure branches

Every failure (steps 1–9) terminates with:

1. Best-effort audit row (`action='install'`, `status='error'` or `status='rejected_collision'`, `context.failure_code=<code>`). Best-effort: if the audit insert itself fails, log structured and continue.
2. Metric: `slack_install_outcomes_total{outcome=<code>}`.
3. Redirect: `302 /integrations/slack/install-error?reason=<code>`.
4. Response status code: **409** for `installation_collision`, **400** for `state_invalid` / `state_expired` / `state_consumed`, **502** for `slack_oauth_error`, **503** for `secret_store_unavailable` / `oauth_state_persist_failed`. The status code is observable to tests; the redirect is what the human sees.

The full `<reason>` code set:

| reason | HTTP | Cause |
|--------|------|-------|
| `state_invalid` | 400 | HMAC mismatch, malformed payload, or unknown nonce |
| `state_expired` | 400 | Nonce exists but `expires_at <= now()` |
| `state_consumed` | 400 | Nonce exists, was already consumed |
| `slack_oauth_error` | 502 | Slack returned `ok=false` from `oauth.v2.access` |
| `installation_collision` | 409 | `team_id` already bound to a different tenant |
| `secret_store_unavailable` | 503 | `SecretStore.put` raised `SecretStoreError` |

### Logging

- On success: structured log `slack_install_success`, fields `{tenant_id, installation_row_id, was_reinstall, scopes_count}`. NO `team_id`, NO secrets.
- On failure: structured log `slack_install_failure`, fields `{tenant_id, reason, http_status}`. NO `team_id` for `installation_collision` (would let an attacker probe which team_ids are bound elsewhere).

### Idempotency

- A second valid callback for the same nonce is rejected at step 3 (`state_consumed`).
- A second install for the same `team_id` + same tenant updates the existing row (re-issues `secret_ref`, deletes prior bot/user refs from `encrypted_secrets` via a follow-up best-effort delete — partial-failure tolerant). This is the FR-018 re-install path.

---

## Cross-reference

- `services/webhooks/router.py` is changed to use `app.state.tenant_resolver.resolve(provider, payload, headers)`. That's a separate contract surface (existing IN-06/IN-07).
- Slack ingestion handler (`services/ingestion/handlers/slack_message.py`, existing) is extended to dispatch on `event.type`; `app_uninstalled` and `tokens_revoked` route to `services/integrations/slack/uninstall.py` (see `contracts/http-webhooks-slack-events.md`).

## Test plan

| Test | Type | Asserts |
|------|------|---------|
| `test_install_redirect_to_slack` | integration | 302 + Location starts with `https://slack.com/oauth/v2/authorize`; `oauth_install_states` has one new row for this tenant. |
| `test_install_requires_bearer` | integration | 401 `missing_bearer` when no Authorization header. |
| `test_callback_state_invalid_hmac` | integration | 302 → `/integrations/slack/install-error?reason=state_invalid`; HTTP 400; no Slack API call. |
| `test_callback_state_expired` | integration | 302 → `…?reason=state_expired`; nonce row remains `consumed_at IS NULL`. |
| `test_callback_state_consumed_replay` | integration | First call succeeds; second call (same `state`) → `…?reason=state_consumed`. |
| `test_callback_success_fresh_install` | integration | 302 → `/integrations/slack/installed?team=…`; `provider_installations` row created with `enabled=true`; `installation_audit_log` row with `action='install'`. |
| `test_callback_success_reinstall_same_tenant` | integration | Same row id preserved; `secret_ref` updated; prior refs cleaned up. |
| `test_callback_installation_collision` | integration | Different tenant tries to bind same team_id → HTTP 409; redirect reason `installation_collision`; audit row with `status='rejected_collision'`. |
| `test_callback_slack_oauth_error` | integration | `respx` mocks `oauth.v2.access` returning `{"ok": false, "error": "invalid_code"}` → HTTP 502; redirect reason `slack_oauth_error`. |
| `test_callback_no_team_id_in_logs` | integration | After collision, the captured structured logs do NOT contain the conflicting `team_id` string. |
| `test_callback_secret_store_unavailable` | integration | `SecretStore.put` raises → HTTP 503; redirect reason `secret_store_unavailable`. |
