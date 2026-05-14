# IN-12 Quickstart — Verify Gateway Worker End-to-End

Run this after Phase 4 to confirm the worker ingests real Discord messages into the substrate.

## Prerequisites

1. IN-07 / IN-08 / IN-09 deployed (this branch builds on `integration/ingestion-hardening`).
2. Postgres on port 5433, Ollama on 11434, `.venv` active.
3. **Discord MESSAGE_CONTENT intent enabled**:
   - Go to https://discord.com/developers/applications/1504474857914499194 → Bot → Privileged Gateway Intents → toggle **Message Content Intent** ON → Save.
   - Without this, MESSAGE_CREATE events arrive with empty `content` for non-mention messages.
4. A `provider_installations` row exists for your test guild (IN-09 OAuth flow already creates this).

## Step 1 — Start the worker

Add to `scripts/start.sh` (Phase 4 task T042) so it boots alongside the other workers. For a one-shot manual test:

```bash
set -a && . ./.env && set +a
.venv/bin/python scripts/run_discord_gateway_worker.py 2>&1 | tee /tmp/fyralis_logs/discord_gateway_worker.log
```

You should see structured log lines:
```json
{"event": "discord_gateway_starting", "level": "info", ...}
{"event": "discord_gateway_connecting", "level": "info", ...}
{"event": "discord_gateway_ready", "level": "info", "session_id": "abc...", ...}
```

If the worker exits 1 with `discord_gateway_close_fatal` and close_code=4014, you missed step 3 above — enable MESSAGE_CONTENT intent and try again.

## Step 2 — Post a message in Discord

In any channel where the Fyralis bot is present, post a message:

> hello from the gateway worker test

You should see in the worker log within ~1 second:
```json
{"event": "discord_gateway_dispatch", "level": "debug", "event_name": "MESSAGE_CREATE", ...}
```

## Step 3 — Verify the observation

```bash
PGPASSWORD=company_os psql -h localhost -p 5433 -U company_os -d company_os -c \
  "SELECT source_channel, content_text, external_id, occurred_at
     FROM observations
    WHERE source_channel='discord:message'
    ORDER BY occurred_at DESC LIMIT 3;"
```

Expected output includes a row with `content_text='hello from the gateway worker test'`.

## Step 4 — Dedup check

Re-post the EXACT SAME message text in the same channel. A new `message_id` means a new observation row — that's correct (Discord generates a fresh id per send).

The dedup property is verified differently: send one message, then in the worker log find the dispatch event, simulate it being re-delivered (or wait for Discord's resume protocol to re-deliver during a forced reconnect). Result: still one observation row for that `external_id`.

For a unit-test-style dedup verification:
```python
# In a Python shell with deps assembled:
from services.integrations.discord.gateway.dispatch import handle_message_create
payload = {... your captured MESSAGE_CREATE ...}
await handle_message_create(payload, deps)
await handle_message_create(payload, deps)
# Then count: SELECT count(*) FROM observations WHERE external_id='discord:<message_id>'  → 1
```

## Step 5 — Filter check (author.bot)

Post a message via a different bot in the same channel (or `@Fyralis` mention with a reply). The Fyralis bot's outbound reply (when IN-13 ships) will have `author.bot=true` — verify in the worker log that you see:

```json
{"event": "discord_gateway_filtered_bot", "source": "self", ...}
```

and no observation is committed for that message.

## Step 6 — Unknown-guild check

Invite the bot to a fresh Discord server you OWN (or use a test server) where you have not run IN-09's OAuth flow. Post a message. Expected:

```json
{"event": "discord_gateway_dropped_unknown_installation", "short_guild_hash": "...", ...}
```

`grep <guild_id> /tmp/fyralis_logs/discord_gateway_worker.log` should return zero matches (SC-006: no raw guild_id in logs).

## Step 7 — Reconnect drill

In the worker log, look for `discord_gateway_reconnect_initiated` events that naturally occur (Discord periodically forces clients to reconnect). After each reconnect:

1. `discord_gateway_ready` appears within ~5 seconds.
2. Subsequent messages still produce observations.
3. No duplicate observations exist for the messages around the reconnect window.

To force a reconnect manually:

```bash
# Kill the worker; supervisor (start.sh) restarts it; observe RESUME path.
kill -TERM <worker_pid>
```

Expected: graceful drain, exit 0, supervisor restarts, new READY within 30 s.

## Step 8 — Metrics scrape

The worker emits metric events via structlog. To count messages ingested in a window:

```bash
grep '"event": "discord_gateway_messages_total"' /tmp/fyralis_logs/discord_gateway_worker.log | wc -l
```

For a Prometheus scrape, see the planned follow-up — IN-12 v1 ships log-based counters only.

## Success criteria

If all 8 steps complete with expected output, the worker satisfies IN-12 SC-001 through SC-010. Capture this as evidence in the PR description.
