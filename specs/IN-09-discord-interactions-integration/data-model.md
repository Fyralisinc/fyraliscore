# Phase 1 — Data Model

**Zero new tables. Zero new columns. Zero new indexes.** IN-09 is a second consumer of the substrate IN-08 provisioned. This document enumerates the existing entities IN-09 reads/writes, the new **label conventions** in `encrypted_secrets`, the new **value conventions** in `provider_installations.installation_id`, `observations.source_channel`, and `observations.external_id`, and the new **enum value** in `installation_audit_log.context.provider`.

---

## Reused tables (no DDL)

### `encrypted_secrets` (IN-08 migration 0040)

| Column | Type | Notes |
|---|---|---|
| `id` | uuid | `uuid7()`; pkey |
| `tenant_id` | uuid | FK tenants(id) DEFERRABLE; RLS-scoped |
| `label` | text | application-defined; IN-09 uses two new label patterns (below) |
| `ciphertext` | bytea | Fernet envelope-encrypted plaintext |
| `created_at` | timestamptz | DEFAULT now() |
| `rotated_at` | timestamptz | NULL until rotated |

**IN-09 label conventions** (new in this task, additive — no schema impact):

- `discord_bot_token:<guild_id>` — the per-installation OAuth bot token. Created by `services/integrations/discord/oauth.py::callback_handler`; read by `services/integrations/discord/client.py` for every outbound; deleted by `_disable_and_zeroize_discord` on bot-kick.
- `discord_public_key:<guild_id>` — a per-installation mirror of the Discord application's Ed25519 public key. Created at install; read by `services/webhooks/signatures/discord.py` via the IN-08 `load_secrets` path; deleted on bot-kick. The plaintext is identical across rows (the app key doesn't vary by guild); see research R8 for rationale.

### `oauth_install_states` (IN-08 migration 0040)

| Column | Type | Notes |
|---|---|---|
| `id` | uuid | pkey |
| `tenant_id` | uuid | FK tenants(id); RLS-scoped |
| `nonce` | text | UNIQUE; the random component of the state token |
| `provider` | text | **disambiguates Slack vs Discord** (research R4) |
| `expires_at` | timestamptz | 10-minute TTL |
| `consumed_at` | timestamptz | NULL until consumed; atomic UPDATE … WHERE consumed_at IS NULL RETURNING |
| `created_at` | timestamptz | DEFAULT now() |

IN-09 inserts rows with `provider='discord'`. The atomic consumption query in `oauth.py::verify_and_consume_state` checks both `provider='discord'` AND `nonce=$1` AND `consumed_at IS NULL` AND `expires_at > now()`.

### `provider_installations` (migration 0039 + extensions in IN-08)

| Column | Type | Notes |
|---|---|---|
| `id` | uuid | `uuid7()`; pkey |
| `tenant_id` | uuid | FK tenants(id) |
| `provider` | text | IN-09 uses `'discord'` |
| `installation_id` | text | IN-09: the Discord `guild_id` (snowflake string) |
| `secret_ref` | uuid | FK encrypted_secrets(id); IN-09 points at the `discord_public_key:<guild_id>` row (NOT the bot token — `_load_from_db` resolves via this column for signature verification) |
| `enabled` | bool | FALSE after bot-kick chokepoint |
| `installed_at` | timestamptz | DEFAULT now() |

UNIQUE constraint on `(provider, installation_id)` enforces "one row per guild." IN-09's OAuth callback writes with `ON CONFLICT (provider, installation_id) DO UPDATE WHERE provider_installations.tenant_id = EXCLUDED.tenant_id` so cross-tenant collisions return zero rows (caught and surfaced as `InstallationCollisionError`).

### `installation_audit_log` (IN-08 migration 0041)

| Column | Type | Notes |
|---|---|---|
| `id` | uuid | pkey |
| `tenant_id` | uuid | FK tenants(id); RLS-scoped |
| `installation_row_id` | uuid | FK provider_installations(id); nullable for pre-row actions |
| `provider` | text | IN-09 uses `'discord'` |
| `action` | text | CHECK in (`'install'`, `'uninstall'`, `'token_refresh'`, `'rejected_collision'`) |
| `status` | text | CHECK in (`'ok'`, `'rejected_collision'`, `'error'`) |
| `context` | jsonb | `{guild_id, scopes, registration_response, error_code, …}` |
| `created_at` | timestamptz | DEFAULT now() |

IN-09 contributes audit rows on every install, every bot-kick chokepoint fire, and every cross-tenant collision. Under concurrent chokepoint races (Clarifications Q1), up to N rows per kick are accepted.

### `observations` (existing, pre-IN-08)

| Column | Type | IN-09 value |
|---|---|---|
| `id` | uuid | `uuid7()`; pkey component |
| `tenant_id` | uuid | resolved from `provider_installations(guild_id)` |
| `kind` | text | `'signal'` |
| `source_channel` | text | **`'discord:interaction'`** (NEW — was `'discord:webhook'` in the stub) |
| `source_actor_ref` | text | `f"discord:{member.user.id}"` (guild context) or `f"discord:{user.id}"` (DM) |
| `external_id` | text | `f"discord:{interaction.id}"` (Discord interaction snowflake; globally unique) |
| `content` | jsonb | `{"text": "<query>", "metadata": <stripped payload>}` |
| `content_text` | text | `<query>` verbatim (the primary string option's value) |
| `occurred_at` | timestamptz | `now()` at handler dispatch (Discord doesn't supply an interaction occurred_at) |
| `trust_tier` | text | `'attested_agent'` |
| `entities_mentioned` | jsonb | `[{type:'discord_application', id:<app_id>}, {type:'discord_guild', id:<guild_id>}, {type:'discord_channel', id:<channel_id>}]` |

UNIQUE index on `(source_channel, external_id, occurred_at)` enforces dedup. The handler catches `UniqueViolationError` from the insert and treats it as idempotent success.

---

## Migration impact summary

| Migration file | Status | Action |
|---|---|---|
| 0039_provider_installations.sql | reused as-is | none |
| 0040_slack_installation_tokens.sql | reused (defines encrypted_secrets + oauth_install_states) | none |
| 0041_installation_audit_log.sql | reused as-is | none |
| **NNNN_discord_*.sql** | **NOT CREATED** | none — see Constitution Check §II in plan.md |

---

## Module-level entities (new, in-process)

These are not DB entities. They are Python types added in `services/integrations/discord/` for clarity and structured-error-context coverage.

### `DiscordTokenResponse`

A small dataclass mirroring Discord's `oauth2/token` response after the bot-scope exchange:

```python
@dataclass(frozen=True)
class DiscordTokenResponse:
    access_token: str  # bot token (xoxb-equivalent)
    token_type: str    # always "Bearer"
    scope: str         # space-separated, includes 'applications.commands bot'
    guild_id: str      # extracted from response['guild']['id'] (research R7)
    application_id: str
```

### `RateLimitState`

Tracks budget across retry attempts for `services/integrations/discord/client.py`:

```python
@dataclass
class RateLimitState:
    attempts: int = 0
    total_wall_seconds: float = 0.0
    last_retry_after_seconds: float = 0.0

    def should_retry(self, now: float) -> bool:
        return self.attempts < 3 and self.total_wall_seconds < 30.0
```

### `DiscordApiError`, `DiscordOAuthError`

New exception subclasses of `CompanyOSError` (Constitution §VIII):

- `DiscordOAuthError(code, message, context)` — raised by `oauth.py` on token-exchange failures, response-shape mismatches, command-registration 4xx after a token exchange succeeded.
- `DiscordApiError(code, message, context)` — raised by `client.py` on terminal 4xx/5xx (after 401 chokepoint, after retry budget exhausted, on orphan-secret-ref).

Codes (stable strings consumed by the UI shell + audit log):

| Code | Meaning |
|---|---|
| `discord_oauth_token_exchange_failed` | token-exchange POST returned non-2xx |
| `discord_oauth_missing_guild` | bot-scope response did not include `guild.id` (research R7) |
| `discord_command_registration_failed` | POST /applications/{}/commands returned 4xx |
| `installation_collision` | reused from IN-08 — same code surfaced to UI for both providers |
| `discord_api_rate_limited` | retry budget exhausted |
| `discord_api_unauthorized` | 401 from outbound; chokepoint already fired |
| `discord_secret_unavailable` | secret_ref dangling; installation should be considered unhealthy |

All errors carry `context` with `{tenant_id, guild_id, attempt_count?, http_status?, discord_error_code?}` where applicable. `guild_id` redaction in log lines (FR-005, SC-006) means structured logs emit only `{tenant_id, http_status}` for unknown-installation rejections.
