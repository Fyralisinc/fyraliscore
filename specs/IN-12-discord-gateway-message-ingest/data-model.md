# IN-12 Data Model

**TL;DR: Zero new tables, zero migrations, zero schema changes.** Everything below is application-layer convention on top of existing rows.

## New `source_channel` value

A single new value flows through the existing `observations.source_channel` TEXT column:

- `discord:message` — created by the Gateway worker on every MESSAGE_CREATE that passes the author.bot + webhook_id + known-installation filters.

Existing values it sits alongside: `slack:message`, `discord:interaction`, `email:inbound`, etc.

## New `CHANNEL_TRUST_MAP` entry

In `services/ingestion/handlers/__init__.py::CHANNEL_TRUST_MAP`:

```python
CHANNEL_TRUST_MAP: dict[str, str] = {
    ...
    "slack:message":        "attested_human",
    "discord:interaction":  "attested_agent",
    "discord:message":      "attested_human",   # NEW
    ...
}
```

This is a Python module constant, not DDL.

## Observation row shape (when source_channel='discord:message')

| Column | Value source |
|---|---|
| `id` | `uuid7()` (assigned inside the ingestion handler) |
| `tenant_id` | Resolved from `provider_installations` via `TenantResolver.resolve('discord', payload)` |
| `source_channel` | Literal `'discord:message'` |
| `external_id` | `f"discord:{message.id}"` (Discord snowflake) |
| `source_actor_ref` | `f"discord:{author.id}"` (Discord snowflake — same convention as `discord:interaction`) |
| `kind` | `'signal'` |
| `trust_tier` | `'attested_human'` |
| `content_text` | `message.content` (verbatim; no markdown strip, no truncation) |
| `content` (jsonb) | `{ "metadata": { channel_id, short_guild_hash, mention_user_ids, attachment_count } }` |
| `occurred_at` | `datetime.fromisoformat(message.timestamp)` (Discord sends ISO 8601) |
| `entities_mentioned` | `[("discord_user", uid) for uid in mention_user_ids] + [("discord_channel", channel_id)] + [("discord_application", application_id)]` (downstream entity-alias hint) |

## Dedup mechanism

Existing unique index on `observations`:

```sql
CREATE UNIQUE INDEX observations_source_channel_external_id_occurred_at_key
    ON observations (source_channel, external_id, occurred_at);
```

This index already exists (predates IN-12; verified during IN-09 implementation). A second insert for `(source_channel='discord:message', external_id='discord:<message_id>', occurred_at=<…>)` raises `asyncpg.UniqueViolationError` which the ingestion handler converts to a successful no-op return.

**No new index required.** No migration.

## `provider_installations` — read-only

The Gateway worker reads but never writes `provider_installations`. The row for each guild is created by IN-09's OAuth flow; the worker queries it via the existing `TenantResolver` (no new code path).

| Field | Used as |
|---|---|
| `installation_id` | The Discord `guild_id` extracted from MESSAGE_CREATE |
| `tenant_id` | Stamped onto every observation |
| `enabled` | Filter — `enabled=FALSE` rows are treated as unknown (US4) |
| `secret_ref` | NOT read by this worker (signing-secret is for inbound HTTP, not WSS) |

## Encrypted secrets — read-only and unchanged

The worker does NOT read from `encrypted_secrets`. The bot token comes from `DISCORD_BOT_TOKEN` env var (same model as IN-09's `commands.py` and `client.py`).

The `discord_public_key:<guild_id>` rows seeded by IN-09 for inbound HTTP verification are irrelevant to the Gateway worker — outbound WSS auth uses the bot token, not the application public key.

## Worker-internal state (in-memory only)

The worker holds a `GatewaySessionState` dataclass in memory:

```python
@dataclass
class GatewaySessionState:
    session_id: str | None          # set by READY
    resume_gateway_url: str | None  # set by READY
    last_seq: int | None            # bumped on every DISPATCH
    heartbeat_interval_ms: int      # set by HELLO
    last_heartbeat_ack: float       # monotonic seconds
```

**This is not persisted** — on full reconnect it resets. Resumable reconnects preserve `session_id` and `last_seq` to feed RESUME. The state is single-instance; there is no cross-process sharing.

## Metrics (Prometheus-shape, in-process counters)

Defined in `services/integrations/discord/gateway/metrics.py`. None of these touch the DB; they're in-process counters scraped by structlog log emission, matching IN-09's `metrics.py` pattern.

```
discord_gateway_connection_state{state}             # gauge: 0|1 per state
discord_gateway_reconnect_total{reason}             # counter
discord_gateway_dispatch_total{event}               # counter
discord_gateway_messages_total                      # counter (MESSAGE_CREATE post-filter)
discord_gateway_filtered_bot_total{source}          # counter (self|other_bot|webhook)
discord_gateway_dropped_unknown_installation_total  # counter
discord_gateway_connect_failure_total               # counter
discord_gateway_heartbeat_miss_total                # counter
```

## Schema-drift check

Run `python scripts/check_schema_drift.py` after Phase 4 — expect exit 0 and zero new files in `db/migrations/`. This is SC-009.
