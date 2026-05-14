# Contract: Discord Gateway Worker Process

The `scripts/run_discord_gateway_worker.py` process is a long-running Python entrypoint with this externally-visible contract.

## Inputs

### Environment variables (consumed at startup)

| Var | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | yes | asyncpg connection — same shape as the gateway HTTP service |
| `DISCORD_BOT_TOKEN` | yes | App-level Bot Token from Discord Developer Portal |
| `MASTER_KEK` | yes | Used by `lib/shared/secrets/` for `FernetSecretStore`; reused for ingestion-handler dependencies |
| `FYRALIS_ENV` | optional | Used by `assert_prod_safety_invariants` only; the worker has no env-specific behavior in v1 (FR-018 says exit-on-4014 regardless of env) |

### CLI args

None. Run with no arguments: `python scripts/run_discord_gateway_worker.py`.

### Signals

| Signal | Action |
|---|---|
| `SIGTERM` | Graceful drain + exit 0 within 5 s (FR-013) |
| `SIGINT` | Same as SIGTERM (Ctrl-C in dev) |
| `SIGKILL` | Hard kill (process supervisor only) |
| `SIGHUP` | Ignored (no config reload) |

## Outputs

### Side effects

1. **Postgres writes**: `INSERT INTO observations` rows for every dispatched MESSAGE_CREATE that passes filters. No direct writes to any other table.
2. **Discord Gateway connection**: Persistent WSS to `wss://gateway.discord.gg`. Closed cleanly on shutdown.
3. **Discord HTTP**: One `GET /api/v10/gateway/bot` per connect attempt. No other HTTP traffic.

### Logs

`structlog`-emitted JSON lines on stdout (captured by `scripts/start.sh` into `/tmp/fyralis_logs/discord_gateway_worker.log`).

**Required log events** (event name → meaning):
- `discord_gateway_starting` — process boot
- `discord_gateway_connecting` — about to open WSS
- `discord_gateway_ready` — IDENTIFY succeeded, session_id captured
- `discord_gateway_dispatch` — DEBUG-level per-event
- `discord_gateway_filtered_bot` — INFO per drop (rare, but visible)
- `discord_gateway_dropped_unknown_installation` — INFO per drop
- `discord_gateway_heartbeat_miss` — WARN
- `discord_gateway_reconnect_initiated` — INFO with `reason` + close_code
- `discord_gateway_close_fatal` — ERROR with close_code + close_reason; precedes exit 1
- `discord_gateway_shutdown_signal_received` — INFO on SIGTERM
- `discord_gateway_shutdown_complete` — INFO before exit 0

**Forbidden log fields** (SC-006):
- Raw `guild_id` — always use `short_guild_hash` instead.

### Metrics

In-process counters/gauges per `data-model.md` Metrics section. No metric endpoint is exposed by this worker (matches think_worker / post_commit_worker pattern); metrics surface via structured log events that an external collector can aggregate.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Clean shutdown (SIGTERM or SIGINT) |
| 1 | Fatal Discord close code (4004, 4010, 4011, 4012, 4013, 4014) |
| 2 | Startup configuration error (missing DATABASE_URL, etc.) |

## Lifecycle Invariants

1. Between startup and first READY DISPATCH, no observations are written.
2. Between any reconnect-initiation and the following READY, dispatches received over the old connection are completed; dispatches over the new connection use the new session_id.
3. On SIGTERM, all dispatches already accepted into the queue are committed before exit, OR the worker exits at the 5 s grace cap with the in-flight count logged.
4. The worker never holds a Postgres transaction open across a dispatch boundary — each MESSAGE_CREATE is an independent transaction in the ingestion handler.
5. The worker NEVER writes to `provider_installations`, `encrypted_secrets`, or `installation_audit_log` — those are owned by IN-09's OAuth path and IN-09's chokepoint.

## Dependencies

- `asyncpg.Pool` — built from `DATABASE_URL` at startup; min=2, max=4 (small connection budget; one connection for tenant_resolver lookups, one for observation inserts, two reserve).
- `lib.shared.secrets.FernetSecretStore` — instantiated but only consumed transitively by the ingestion handler (which IS shared with IN-09's slash-command path).
- `services.webhooks.tenant_resolver.build_tenant_resolver` — reused; the worker constructs its own resolver instance, not the gateway HTTP service's.
- `services.ingestion.handlers.discord.handle_event` — the existing handler, extended in this task to branch on `source_channel='discord:message'`.

## Operational

### Starting

Via `scripts/start.sh` (preferred):
```bash
./scripts/start.sh    # boots gateway, think_worker, post_commit_worker, discord_gateway_worker, UI
```

Manually:
```bash
set -a && . ./.env && set +a
.venv/bin/python scripts/run_discord_gateway_worker.py
```

### Stopping

Via `scripts/stop.sh` (preferred): sends SIGTERM to all PIDs in `/tmp/fyralis_stack.pids`.

Manually:
```bash
kill -TERM <PID>   # graceful
kill -INT  <PID>   # also graceful
```

### Monitoring

```bash
tail -f /tmp/fyralis_logs/discord_gateway_worker.log | jq -c 'select(.level != "debug")'
```

Look for `discord_gateway_ready` shortly after boot and `discord_gateway_dispatch` events flowing in steady state.

### Restart cadence

A clean reconnect storm in steady state is healthy; expect one full reconnect every few hours under normal conditions. Anything more than one reconnect per minute over a sustained window indicates a problem (network, intent misconfiguration, etc.) — operator should check the close codes in the log.
