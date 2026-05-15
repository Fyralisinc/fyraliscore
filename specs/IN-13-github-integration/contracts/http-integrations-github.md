# HTTP Contract: `/integrations/github/*`

Self-serve GitHub App install flow. Mounted by `services/integrations/router.py::build_integrations_router()`.

## `GET /integrations/github/install`

**Auth**: Bearer token; rejects unauthenticated calls with 401 before this handler runs (gateway middleware).
**Tenant binding**: `request.app.state.tenant` populated by the gateway's Bearer middleware.

### Request

No body. No query parameters honored from the client (defense-in-depth: ANY query params are ignored).

### Response — Success (302)

```
HTTP/1.1 302 Found
Location: https://github.com/apps/<GITHUB_APP_SLUG>/installations/new?state=<state-token>
```

State token shape (HMAC-signed by `STATE_TOKEN_SIGNING_KEY` from IN-08 env):
```
base64url({nonce: <uuid>, tenant_id: <uuid>, expires_at: <epoch+600s>, provider: 'github'}).<hmac-sha256-hex>
```

Side effect: an `oauth_install_states` row is inserted with `(provider='github', nonce, tenant_id, expires_at, consumed_at=NULL)`.

### Response — Error

- 401 — Bearer token missing/invalid (gateway middleware, not this handler)
- 503 — `STATE_TOKEN_SIGNING_KEY` not configured (startup misconfig; logged at ERROR; never expected in production)

### Metrics
- `github_install_initiate_total{result}` (counter; result ∈ `ok|state_token_unavailable`)

---

## `GET /integrations/github/callback`

**Auth**: PUBLIC. Authentication is provided by the state-token HMAC + atomic single-use nonce consume — NOT by Bearer. The route MUST be added to `services/gateway/main.py::_PUBLIC_PATHS` as an exact-match string.

### Request

Query parameters (set by GitHub on redirect):
- `installation_id: str` — required; GitHub's numeric id of the new (or re-activated) installation.
- `setup_action: 'install' | 'update'` — required.
- `state: str` — required; the state token issued by `/install`.

### Response — Success (302)

```
HTTP/1.1 302 Found
Location: /integrations/github/installed?installation=<short-hash>
```

Side effects (atomic per Postgres transaction; ordered):
1. State-token HMAC verified; `oauth_install_states` row consumed (`UPDATE … SET consumed_at=now() WHERE provider='github' AND nonce=$1 AND consumed_at IS NULL AND expires_at > now() RETURNING tenant_id`).
2. UPSERT `provider_installations`: `INSERT … ON CONFLICT (provider, installation_id) DO UPDATE SET enabled=TRUE, tenant_id=EXCLUDED.tenant_id WHERE provider_installations.tenant_id=EXCLUDED.tenant_id RETURNING …` — the `WHERE` clause is the cross-tenant collision guard.
3. Mint installation access token; call `GET /installation/repositories` with up to 3 pages; persist the union as `selected_repositories` JSONB (NULL if the response indicates `repository_selection='all'`).
4. INSERT `installation_audit_log` row with `action='install'` (or `reinstall` if the row's prior `enabled` was FALSE; or `update` if `setup_action='update'`).
5. Issue the 302 to the success page.

### Response — Error (302 to error page; never 4xx body except 400 for missing required query params)

- `?reason=state_invalid` — HMAC mismatch.
- `?reason=state_expired` — `expires_at <= now()`.
- `?reason=state_consumed` — `consumed_at IS NOT NULL`.
- `?reason=installation_collision` — UPSERT's WHERE clause rejected the rebind (same `installation_id` already maps to a different tenant). Audit row written with `status='rejected_collision'`; foreign tenant_id is NOT in the redirect URL, response body, or logs (verified by `test_oauth_callback_github.py::test_cross_tenant_collision`).
- `?reason=missing_installation_id` — required query param absent (400).
- `?reason=token_mint_failed` — JWT mint or installation-token exchange returned 4xx/5xx. Audit row `status='error'` with `context.github_status_code`. **The installation row is still committed** — the customer's grant is real; we recover via the lifecycle webhook arrival.
- `?reason=repository_fetch_failed` — `GET /installation/repositories` returned ≥ 4xx. Same recovery path as `token_mint_failed`.

### Metrics
- `github_install_callback_total{outcome}` per FR-017.

---

## `GET /integrations/github/installed`

**Auth**: PUBLIC (the post-install landing page; no PII in the URL beyond `installation=<short-hash>`).

### Request

Query: `installation: str` — 8-byte BLAKE2b short hash of the installation_id.

### Response — Success (200)

Static landing HTML; no DB reads required. Renders a "Fyralis is now connected to your GitHub org" message and a link to the Fyralis dashboard.

---

## `GET /integrations/github/install-error`

**Auth**: PUBLIC.

### Request

Query: `reason: str` — one of the canonical reasons enumerated under `/callback` errors above.

### Response — Success (200)

Static error HTML keyed off `reason`. Each reason maps to a copy-string explaining the failure and offering a recovery path (re-initiate install, contact operator, etc.). No DB reads.
