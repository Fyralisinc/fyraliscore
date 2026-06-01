# Slack (IN-08)

> Workspace messages as signals. The first OAuth-installing provider; it
> established the secret-store + install-audit substrate that GitHub, Discord,
> Notion all reuse.

| Field | Value |
|---|---|
| Source | `slack` |
| Primary channel | `slack:message` |
| Trust tier | `attested_agent` |
| Live ingress | Events API **webhook** → full pipeline (cutover-enabled) |
| Backfill | enumerate channels → `conversations.history` |
| Auth | OAuth bot token (Fernet-encrypted in `encrypted_secrets`) |
| Signature | HMAC-SHA256 (`v0=…`, Slack signing secret) |

## Auth & install

Self-serve OAuth ([services/ingest/integrations/slack/oauth.py](../../../services/ingest/integrations/slack/oauth.py)):

- `/integrations/slack/install` (Bearer-authed) mints an HMAC **state token** over
  `{tenant_id, nonce, expires_at}`, nonce tracked single-use in
  `oauth_install_states`.
- `/integrations/slack/callback` (public, state-token-authed; the **only** Slack
  route in the gateway `_PUBLIC_PATHS`) exchanges the code, persists tokens, and
  upserts the `provider_installations` row.

Outbound client [slack/client.py](../../../services/ingest/integrations/slack/client.py)
(`chat.postMessage`, `users.info`, `conversations.info`) honors 429
`Retry-After`. Uninstall ([slack/uninstall.py](../../../services/ingest/integrations/slack/uninstall.py))
handles inbound `app_uninstalled` / `tokens_revoked`: disables the install row,
zeroes the encrypted secrets, writes an audit row.

## Ingress (live)

`gateway /webhooks/slack` → signature verified
([signatures/slack.py](../../../services/app/webhooks/signatures/slack.py)) → tenant
resolved → `shadow_write_raw` → `ingestion.raw` (`ingress_kind="webhook"`). In
`_CUTOVER_ENABLED_PROVIDERS`: flag `ingestion.kafka_path_enabled` TRUE → `202`;
FALSE or Kafka down → inline `ingest()` → `200`.

## Backfill

[planners/slack.py](../../../services/ingest/ingestion/planners/slack.py) enumerates
channels; [fetchers/slack.py](../../../services/ingest/ingestion/fetchers/slack.py)
pulls history per channel and produces `RawEnvelope`s
(`ingress_kind="backfill"`) — same `slack:message` channel as the webhook, so
`external_id` parity holds and dedup collapses backfill∩live twins.

## Handler & dedup

[handlers/slack.py](../../../services/ingest/ingestion/handlers/slack.py) →
`slack:message`, `trust_tier=attested_agent`. Dedup via
`UNIQUE (source_channel, external_id, occurred_at)` on `observations`.

## Substrate it introduced (migrations 0040 + 0041)

- `encrypted_secrets` — tenant-scoped Fernet ciphertext (RLS); `MASTER_KEK` env-injected.
- `oauth_install_states` — single-use OAuth state nonce ledger.
- `installation_audit_log` — install / uninstall / token_refresh / rejected_collision.

Env-var secret fallback is gated by `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`;
`assert_prod_safety_invariants()` fails gateway startup if `FYRALIS_ENV=prod` and
the flag is on.

Spec: `specs/IN-08-slack-production-integration/`. See [architecture.md](../architecture.md).
