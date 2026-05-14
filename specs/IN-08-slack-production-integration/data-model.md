# Phase 1 Data Model — IN-08 Slack Production Integration

Three new tenant-scoped tables. All shipped via two new migration files. All conform to Constitution §II (idempotent, additive) and §III (FK + RLS + tenant-prefixed index).

---

## 1. `encrypted_secrets` (new, in `0040_slack_installation_tokens.sql`)

Generic envelope-encrypted row store. Backs the `lib/shared/secrets/` Python module. Slack's install flow is the first consumer; future providers reuse without DDL.

### Columns

| Column        | Type          | Constraints                                                       | Notes |
|---------------|---------------|-------------------------------------------------------------------|-------|
| `id`          | `UUID`        | `PRIMARY KEY`                                                     | `uuid7()`, allocated app-side. `secret_ref` exposed to callers is this UUID stringified. |
| `tenant_id`   | `UUID`        | `NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY IMMEDIATE`  | Constitution §III, FR-022. |
| `label`       | `TEXT`        | `NOT NULL`                                                        | Free-form human-readable label. E.g. `"slack_bot_token:T123"`. Not security-relevant; for operability. |
| `ciphertext`  | `BYTEA`       | `NOT NULL`                                                        | Fernet token bytes (URL-safe base64 internally; we store the raw bytes). |
| `created_at`  | `TIMESTAMPTZ` | `NOT NULL DEFAULT now()`                                          | |
| `rotated_at`  | `TIMESTAMPTZ` | `NULL`                                                            | Set on `rotate()`. NULL means never rotated. |

### Indexes

- `PRIMARY KEY (id)` — implicit B-tree.
- `CREATE INDEX IF NOT EXISTS idx_encrypted_secrets_tenant ON encrypted_secrets (tenant_id, id);` — tenant-prefixed; supports the tenant-scoped lookup path even though `id` alone is the natural key (defense-in-depth + matches §III phrasing "tenant-prefixed indexes are non-negotiable").

### RLS

- `ENABLE ROW LEVEL SECURITY` + `FORCE`.
- Policy `tenant_isolation` per migration `0036_rls_permissive_default.sql`.

### Operations

| Op | SQL shape | Caller |
|----|-----------|--------|
| `put(plaintext, label, tenant_id) → ref` | `INSERT INTO encrypted_secrets (id, tenant_id, label, ciphertext) VALUES ($1, $2, $3, $4) RETURNING id` | `FernetSecretStore.put` |
| `get(ref, tenant_id) → plaintext` | `SELECT ciphertext FROM encrypted_secrets WHERE id = $1 AND tenant_id = $2` | `FernetSecretStore.get` |
| `rotate(ref, new_plaintext, tenant_id)` | `UPDATE encrypted_secrets SET ciphertext = $1, rotated_at = now() WHERE id = $2 AND tenant_id = $3` | `FernetSecretStore.rotate` |
| `delete(ref, tenant_id)` | `DELETE FROM encrypted_secrets WHERE id = $1 AND tenant_id = $2` | `FernetSecretStore.delete` |

Each operation hand-rolls `WHERE tenant_id = $2` even though RLS would already filter — defense-in-depth per Constitution §III's bottom-line guidance.

### Foundation classification

NOT a Foundation. Side store for cross-cutting credential storage (Constitution §I explicitly permits this).

---

## 2. `oauth_install_states` (new, in `0040_slack_installation_tokens.sql`)

Single-use nonce ledger for OAuth state tokens. The callback inserts on issuance, updates `consumed_at` on first valid use, rejects any token whose nonce is missing, already consumed, or expired.

### Columns

| Column        | Type          | Constraints                                                       | Notes |
|---------------|---------------|-------------------------------------------------------------------|-------|
| `id`          | `UUID`        | `PRIMARY KEY`                                                     | `uuid7()`. |
| `tenant_id`   | `UUID`        | `NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY IMMEDIATE`  | Binds the nonce to a specific tenant at issuance time. |
| `nonce`       | `TEXT`        | `NOT NULL UNIQUE`                                                 | Globally unique (R9 in research.md). `secrets.token_urlsafe(32)`. |
| `provider`    | `TEXT`        | `NOT NULL`                                                        | `'slack'` at MVP. Allows IN-09/IN-11 to reuse without DDL. |
| `expires_at`  | `TIMESTAMPTZ` | `NOT NULL`                                                        | Issuance + 10 min TTL (configurable). |
| `consumed_at` | `TIMESTAMPTZ` | `NULL`                                                            | NULL ⇒ not yet consumed. Set in the same transaction that processes the callback. |
| `created_at`  | `TIMESTAMPTZ` | `NOT NULL DEFAULT now()`                                          | |

### Indexes

- `PRIMARY KEY (id)` — implicit.
- `UNIQUE (nonce)` — implicit.
- `CREATE INDEX IF NOT EXISTS idx_oauth_install_states_tenant_expires ON oauth_install_states (tenant_id, expires_at);` — tenant-prefixed; supports the sweep query.

### RLS

- `ENABLE ROW LEVEL SECURITY` + `FORCE`.
- Policy `tenant_isolation`.

### Operations

| Op | SQL shape | Caller |
|----|-----------|--------|
| Issue | `INSERT INTO oauth_install_states (id, tenant_id, nonce, provider, expires_at) VALUES ($1, $2, $3, $4, $5)` | `services/integrations/slack/oauth.py::install_handler` |
| Consume (atomic check + mark) | `UPDATE oauth_install_states SET consumed_at = now() WHERE nonce = $1 AND tenant_id = $2 AND consumed_at IS NULL AND expires_at > now() RETURNING id, tenant_id` | `services/integrations/slack/oauth.py::callback_handler` |
| Sweep | `DELETE FROM oauth_install_states WHERE expires_at < now() - INTERVAL '1 hour' OR (consumed_at IS NOT NULL AND consumed_at < now() - INTERVAL '1 hour') LIMIT 1000` | Lifespan-attached sweep task |

The consume step uses a single `UPDATE … RETURNING` so the check-and-set is atomic. Returning zero rows = reject; one row = proceed.

### State lifecycle

```
[issued] --(consume within TTL)--> [consumed] --(sweep)--> [deleted]
   |
   +--(expire without consume)--> [expired] --(sweep)--> [deleted]
```

### Foundation classification

NOT a Foundation. Auth-flow side ledger.

---

## 3. `installation_audit_log` (new, in `0041_installation_audit_log.sql`)

Per-installation lifecycle audit trail. Records `install`, `uninstall`, `token_refresh`, and `rejected_collision` events. Distinct from `audit_events` (which records Model state transitions per Constitution §VII).

### Columns

| Column                | Type          | Constraints                                                                                                                          | Notes |
|-----------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------|-------|
| `id`                  | `UUID`        | `PRIMARY KEY`                                                                                                                        | `uuid7()`. |
| `tenant_id`           | `UUID`        | `NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY IMMEDIATE`                                                                     | |
| `installation_row_id` | `UUID`        | `NULL REFERENCES provider_installations(id)`                                                                                         | NULL for `rejected_collision` (no row created). On successful install, points at the upserted row. |
| `provider`            | `TEXT`        | `NOT NULL`                                                                                                                           | `'slack'` at MVP. |
| `action`              | `TEXT`        | `NOT NULL CHECK (action IN ('install','uninstall','token_refresh','rejected_collision'))`                                            | |
| `status`              | `TEXT`        | `NOT NULL CHECK (status IN ('ok','rejected_collision','error'))`                                                                     | `ok` for happy-path install/uninstall. `rejected_collision` for cross-tenant re-bind attempt. `error` for partial failures. |
| `context`             | `JSONB`       | `NOT NULL DEFAULT '{}'::jsonb`                                                                                                       | Free-form structured context: `app_id`, `scopes_granted`, `actor_session_id`, `failure_code`, etc. MUST NOT carry plaintext `team_id` or any secret material. |
| `created_at`          | `TIMESTAMPTZ` | `NOT NULL DEFAULT now()`                                                                                                             | |

### Indexes

- `PRIMARY KEY (id)` — implicit.
- `CREATE INDEX IF NOT EXISTS idx_installation_audit_log_tenant_created ON installation_audit_log (tenant_id, created_at DESC);` — tenant-prefixed; supports "show me the install history for tenant X" admin path.
- `CREATE INDEX IF NOT EXISTS idx_installation_audit_log_installation ON installation_audit_log (installation_row_id) WHERE installation_row_id IS NOT NULL;` — partial index for the "show me the history for installation Y" path. Tenant-prefixed is not needed here because `installation_row_id` already implies a tenant via its FK; this index supports a narrower predicate.

### RLS

- `ENABLE ROW LEVEL SECURITY` + `FORCE`.
- Policy `tenant_isolation`.

### Append-only invariant

The table has no `UPDATE` or `DELETE` callers in service code. (No SQL trigger enforces this — the discipline matches `audit_events` per §VII.) An automated test asserts that the service code touches `installation_audit_log` only via `INSERT`.

### Foundation classification

NOT a Foundation. Side audit table for installation lifecycle (Constitution §I).

---

## Cross-table invariants

1. **`provider_installations.secret_ref` is a stringified `encrypted_secrets.id`** (a UUID). It is NOT a free-form label; it is a typed reference. The migration does not change the column type (still `TEXT`), but the semantic meaning is pinned by this spec.
2. **One `provider_installations` row → multiple `encrypted_secrets` rows** (bot token, optional user token, per-app signing secret stored once per tenant). `provider_installations.secret_ref` points at the **bot token** ref; the other refs are addressable via `label` queries within the tenant.
3. **`installation_audit_log.installation_row_id`** ON DELETE behavior: the FK is `NULL` instead of `CASCADE`. If a `provider_installations` row is ever deleted (not the same as `enabled=false`), audit history MUST survive. The FK keeps the column nullable explicitly.

## Migration ordering and idempotency

Both migration files are independent — `0041` doesn't depend on `0040`. They will be applied in filename order by `lib/shared/migrations.apply_migrations_dir`.

Each is wrapped in a single `BEGIN; … COMMIT;` so partial failure rolls back cleanly (§II.3).

Re-running either against an existing DB is a no-op (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `DO $$ BEGIN … EXCEPTION WHEN duplicate_object THEN NULL; END $$;` for policies).

---

## Entity map for the OAuth flow (cross-reference)

```
Browser (Slack workspace admin)
    │
    ├─[1]── GET /integrations/slack/install (Bearer auth)
    │                  │
    │                  ▼
    │            oauth_install_states.INSERT(id, tenant_id, nonce, provider='slack', expires_at)
    │                  │
    │                  ▼
    │       state_token = HMAC({tenant_id, nonce, expires_at}, SERVER_HMAC_KEY)
    │                  │
    ├─[2]◀────── 302 https://slack.com/oauth/v2/authorize?...&state=<state_token>
    │
    ├─[3]── (consents on Slack)
    │
    ├─[4]── GET /integrations/slack/callback?code=...&state=<state_token>  (public route)
    │                  │
    │                  ▼
    │            verify_hmac(state_token); decode {tenant_id, nonce, expires_at}
    │                  │
    │                  ▼
    │            oauth_install_states.UPDATE(consumed_at=now()) WHERE nonce=$ AND tenant_id=$ AND consumed_at IS NULL AND expires_at > now() RETURNING id
    │                  │ (zero rows → state_consumed | state_invalid | state_expired)
    │                  ▼
    │            httpx.post(slack oauth.v2.access, code=$code)
    │                  │
    │                  ▼
    │            secret_store.put(bot_token, label='slack_bot_token:T<team>') → bot_ref
    │            secret_store.put(user_token, label='slack_user_token:T<team>') → user_ref  (if present)
    │            secret_store.put(SIGNING_SECRET, label='slack_signing_secret:tenant') → sign_ref  (once per tenant)
    │                  │
    │                  ▼
    │            INSERT INTO provider_installations (... secret_ref=bot_ref, enabled=true)
    │              ON CONFLICT (provider, installation_id) DO UPDATE
    │                  SET tenant_id = excluded.tenant_id, secret_ref = excluded.secret_ref, enabled = true
    │                  WHERE provider_installations.tenant_id = excluded.tenant_id  ← prevents cross-tenant rebind
    │                  │ (if WHERE fails → InstallationCollisionError → 409 + install-error redirect)
    │                  ▼
    │            INSERT INTO installation_audit_log (action='install', status='ok', ...)
    │                  │
    └─[5]◀────── 302 /integrations/slack/installed?team=<short_hash>
```

(Failure branches and uninstall flow are diagrammed in `contracts/http-integrations-slack.md`.)
