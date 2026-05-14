# Feature Specification: Discord Gateway WebSocket Message Ingest

**Feature Branch**: `feat/IN-12-discord-gateway-message-ingest`
**Created**: 2026-05-14
**Status**: Draft
**Input**: Synthesized task body `IN-12 [P1] Discord Gateway WebSocket message ingest` (verbatim in [source.md](./source.md))

## Clarifications

### Session 2026-05-14

- Q: Trust tier value for the new `discord:message` source_channel → A: **`attested_agent`** — match `slack:message`, which is the existing convention in `CHANNEL_TRUST_MAP` for chat-platform messages where a verified bot is the attestation channel for the human author's identity. The same construction applies to Discord: the bot's WSS session is the trust boundary; user identity is asserted by Discord's auth, then attested to us through the bot. Using a different tier from Slack would create cross-platform inconsistency in how downstream consumers weight conversational signal.
- Q: Two Gateway worker instances connected simultaneously (rolling deploy overlap, operator mistake) → A: **Rely on `(source_channel, external_id)` dedup; no explicit single-instance enforcement** — both observers will dispatch the same MESSAGE_CREATE, the second insert hits the unique constraint and is no-op, no duplicate observation. Adding a startup advisory lock would prevent CPU/network waste but is more complex than the value it delivers; the worker is naturally stateless and an extra observer is benign. Discord itself permits multiple gateway connections per app, each gets its own session — there is no Discord-side coordination needed.
- Q: MESSAGE_CREATE with empty `message.content` but non-zero attachments (file-only post) → A: **Ingest the observation with `content_text=''` and `metadata.attachment_count>0`** — losing the "user posted something" signal would silently shrink the activity timeline; downstream consumers can decide whether empty-content observations are noise based on metadata. Same posture as Slack's existing handling for attachment-only messages.
- Q: GUILD_DELETE dispatch (bot kicked from a guild) → A: **Increment `discord_gateway_dispatch_total{event="GUILD_DELETE"}` and do nothing else** — the canonical kick-detection path is IN-09's outbound-401 chokepoint (`services/integrations/discord/uninstall.py::_disable_and_zeroize_discord`). Bridging from inbound GUILD_DELETE to that chokepoint would create a dual-signal race: a legitimate kick-then-reinstall within seconds could fire the bridged chokepoint AFTER the reinstall completes, disabling a freshly-enabled row. Keeping kick detection on a single path is correctness-preserving.
- Q: MESSAGE_CONTENT intent rejected (WSS close 4014) in dev environments → A: **Exit fatally regardless of `FYRALIS_ENV`** — silent degradation to mention-only mode would hide a misconfiguration in dev/staging until prod deploy, where it would manifest as a data-quality regression. Fail-loud is fail-safe; the worker prints a single ERROR with the close code and the documented runbook URL, and the supervisor does NOT auto-restart.

## Summary

Today, Fyralis can observe Discord activity only when a user explicitly invokes `/fyralis ask "…"` — IN-09 ships Interactions HTTP ingest end-to-end, including OAuth install, bot-kick chokepoint, and slash-command registration. But Discord does **not** push normal channel messages over webhooks. The bulk of organisational signal in Discord — strategy threads, blockers, decisions, context-rich conversation — happens in regular messages that today are invisible to Fyralis. Slack achieves parity via the Events API (server-initiated push to our `/webhooks/slack/events` route). Discord requires us to be the client: maintain a persistent WebSocket connection to `wss://gateway.discord.gg`, identify with the bot token plus the `MESSAGE_CONTENT` privileged intent, and dispatch every `MESSAGE_CREATE` event to the existing ingestion handler.

This feature closes that gap by introducing a fourth long-running worker process (alongside the gateway HTTP service, the think worker, and the post-commit worker) whose sole responsibility is to hold a Discord Gateway WSS connection open and stream `MESSAGE_CREATE` events into the substrate as Observations with `source_channel='discord:message'`. Three properties are non-negotiable: (a) the same `provider_installations` row created during IN-09's OAuth flow is the sole tenant-resolution path — no new install model, no new tables; (b) the bot's own messages and other bots' messages are filtered out at dispatch time via `author.bot`, which is the structural guard against an IN-13 outbound reply re-entering ingest and creating an infinite loop; (c) the worker reconnects and resumes through transient network disruption without producing duplicates or gaps — Discord's resume protocol is honored.

Discord's `MESSAGE_CONTENT` is a **privileged gateway intent**. Operator MUST toggle it on in the Developer Portal (Bot tab → Privileged Gateway Intents → Message Content Intent) before deploying. For apps in fewer than 100 servers this is a one-click toggle; beyond 100 servers requires Discord's verification process. Without this intent the bot only receives messages where it is explicitly @-mentioned plus its own slash commands — useless for organisational-intelligence ingest.

Out of scope and explicitly deferred: MESSAGE_UPDATE and MESSAGE_DELETE handling (edit/delete semantics deserve their own clarification round), DM ingest (guild-only in this slice), sharding (we'll start with shard 0/1 of 1 and revisit at 2 500 guilds), outbound replies (that's IN-13), voice/presence/typing dispatches.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Normal Discord Channel Message Lands as Observation (Priority: P1)

As a Fyralis-using team in a Discord workspace where the Fyralis bot is installed, when any team member posts a regular message in a channel the bot can see, that message must arrive at the substrate as exactly one Observation under the correct tenant within 5 seconds — with the message text verbatim and no operator intervention.

**Why this priority**: This is the ingestion contract. Without it, the IN-09 install flow produces a bot that watches in silence — slash commands work but the actual conversational signal does not flow. Every downstream user story (reconnect resilience, filter correctness, operational observability) is in service of this happy path holding.

**Independent Test**: With the worker connected to gateway.discord.gg, post a message in a channel of a guild that has a `provider_installations` row. Within 5 seconds assert: exactly one new row in `observations` with `tenant_id` matching the seeded tenant, `source_channel='discord:message'`, `external_id='discord:<message_id>'`, `source_actor_ref='discord:<author_id>'`, `content_text` exactly equal to the posted message body, `trust_tier='attested_agent'`.

**Acceptance Scenarios**:

1. **Given** the worker is connected and a `provider_installations` row exists for `guild_id=G`, **When** a human user posts `"hello team"` in channel C of guild G, **Then** within 5 seconds exactly one observation exists with `source_channel='discord:message'`, `external_id='discord:<message_id>'`, `content_text='hello team'`, `tenant_id` = the seeded tenant.
2. **Given** the worker just received the same `MESSAGE_CREATE` payload twice (Discord retry within the resume window OR our own internal retry), **When** the dispatcher processes both, **Then** exactly one observation is committed — dedup on `(source_channel, external_id)` holds.
3. **Given** a message contains an attachment, an embed, and three mentions, **When** ingested, **Then** `content_text` is the message body verbatim (no markdown stripping, no truncation) and the metadata sub-object records `attachment_count=1`, `mention_user_ids=[…]`, `channel_id=…`, `guild_id_hash=<short_hash>` (the hash, not the raw guild_id).

---

### User Story 2 — 24h+ Connection Stability with Transparent Reconnect and RESUME (Priority: P1)

As an operator running this worker in production, I need the connection to gateway.discord.gg to survive transient network blips, Discord's periodic disconnects, and INVALID_SESSION resets — without manual restart and without observation duplicates or gaps.

**Why this priority**: A 24h connection that drops twice an hour and loses 30 messages each time produces an incoherent substrate. Discord's documented gateway behavior assumes clients implement HEARTBEAT, RESUME, and full-reconnect-with-re-IDENTIFY correctly; an implementation that doesn't is functionally broken even if it ingests the happy path. This is the difference between a demo and a service.

**Independent Test**: Start the worker connected. Simulate three failure modes back-to-back: (a) WS close 4000 (unknown error — resumable), (b) WS close 4007 (invalid seq — full reconnect), (c) INVALID_SESSION dispatch with `d=false` (full reconnect). Between each failure post a stream of messages. Assert: zero duplicate observations, total observations equal to total posted, connection state metric transitions are recorded.

**Acceptance Scenarios**:

1. **Given** an active WSS connection, **When** Discord sends a non-fatal close (e.g., 4000), **Then** the worker reopens the WSS, sends RESUME with the captured `session_id` and last `seq`, and continues dispatching with no duplicate observations and no observation gap (within Discord's resume window).
2. **Given** Discord sends an INVALID_SESSION dispatch with `d=true`, **When** the worker observes it, **Then** it performs a resumable reconnect (same session_id). With `d=false`, it performs a full reconnect (new IDENTIFY, fresh session).
3. **Given** the worker missed a heartbeat ACK, **When** the next heartbeat tick fires, **Then** the worker closes the socket with close code 4000, reconnects, and resumes — heartbeat-miss is not a fatal condition.
4. **Given** Discord sends a fatal close (4004 authentication failed, 4013 invalid intent, 4014 disallowed intent), **When** the worker observes it, **Then** the worker logs the close code with `reason` and exits with status 1; the supervising process is expected to NOT restart (operator intervention required).

---

### User Story 3 — `author.bot` Filter Prevents Bot Message Pollution and IN-13 Outbound Loops (Priority: P2)

As the platform, I need `MESSAGE_CREATE` events whose `author.bot == true` to be **silently dropped at dispatch time** — never reach the ingestion handler, never produce an observation. This applies to our own bot's messages, to other Discord apps installed in the same guild, and to webhook-source messages.

**Why this priority**: When IN-13 ships outbound replies, the Fyralis bot will post messages back into Discord channels. Those outbound messages produce `MESSAGE_CREATE` events that arrive at *this* worker because the bot is in the same channel it just posted to. Without the `author.bot` filter the bot would observe its own response, the substrate would treat it as inbound signal, the LLM might reply to that, and we'd have an infinite loop. The filter is also the cleanest way to keep other Discord integrations (e.g., GitHub bot relaying PR notifications, Reminder bots) from polluting the human-conversation substrate.

**Independent Test**: Inject three `MESSAGE_CREATE` payloads into the dispatcher: (a) `author.bot=true` and `author.id` matching our application id, (b) `author.bot=true` with a different application id, (c) `author.bot=false` (a human). Assert that only (c) produces an observation; (a) and (b) increment a `discord_gateway_filtered_bot_total` metric and write no rows.

**Acceptance Scenarios**:

1. **Given** a MESSAGE_CREATE with `author.bot=true` and `author.id` = our application id, **When** dispatched, **Then** zero observations are committed and a `discord_gateway_filtered_bot_total{source="self"}` counter increments.
2. **Given** a MESSAGE_CREATE with `author.bot=true` and `author.id` != our application id, **When** dispatched, **Then** zero observations are committed and `discord_gateway_filtered_bot_total{source="other_bot"}` increments.
3. **Given** a MESSAGE_CREATE with `webhook_id` non-null (Discord webhook-sourced message), **When** dispatched, **Then** zero observations are committed and `discord_gateway_filtered_bot_total{source="webhook"}` increments.
4. **Given** a MESSAGE_CREATE with `author.bot=false` and no `webhook_id`, **When** dispatched, **Then** exactly one observation is committed.

---

### User Story 4 — MESSAGE_CREATE from Unknown Guild Drops Silently (Priority: P2)

As an operator, when the bot is in a guild that has no `provider_installations` row (e.g., a guild the bot was invited to outside the Fyralis OAuth flow, or a guild whose install row was disabled by IN-09's bot-kick chokepoint), `MESSAGE_CREATE` events from that guild MUST be dropped silently — no crash, no partial write, no log lines containing the guild_id, but with an incrementing metric so operators can detect "we're in a guild we don't track".

**Why this priority**: There is no way to programmatically prevent a Discord user with admin rights from inviting the bot to a guild without going through OAuth. There's also no way to prevent IN-09's chokepoint from disabling a guild's install row mid-message-stream. Both are normal operational realities and neither should crash the worker or surface a 5xx-style error. But operators still need to see "this is happening" so they can decide whether to onboard the guild or kick the bot.

**Independent Test**: With the worker connected, fabricate a `MESSAGE_CREATE` for a guild_id with no `provider_installations` row. Assert: zero observations, the `discord_gateway_dropped_unknown_installation_total` counter increments, no ERROR-level log line contains the guild_id (an INFO-level log with `installation_row_id=None, tenant_id=None` is acceptable but the raw guild_id must not appear).

**Acceptance Scenarios**:

1. **Given** a MESSAGE_CREATE from a guild with no `provider_installations` row, **When** dispatched, **Then** zero observations and `discord_gateway_dropped_unknown_installation_total` increments by 1.
2. **Given** a MESSAGE_CREATE from a guild whose `provider_installations` row has `enabled=FALSE`, **When** dispatched, **Then** identical behavior to (1) — disabled rows are treated as unknown.
3. **Given** the guild_id from (1) or (2), **When** an operator inspects logs after the drop, **Then** no log message contains the raw guild_id — only the hash (`short_guild_hash`) or `installation_row_id` if available.

---

### User Story 5 — Operational Observability and Graceful Shutdown (Priority: P3)

As an on-call operator, I need to see the worker's connection state, reconnect rate, message rate, and dispatch breakdown via metrics, and the worker MUST shut down cleanly on SIGTERM so deploys don't drop events.

**Why this priority**: The first four user stories define correctness. This one defines operability. A connection that drops every 4 hours and reconnects cleanly is acceptable; the same with no metric exposing the reconnect rate is a silent timebomb. SIGTERM handling matters because zero-downtime deploys (or container restarts) must not leave dispatched-but-uncommitted events in flight.

**Independent Test**: Run the worker for 60 seconds with a mocked Discord WS feeding it 10 messages, 1 RECONNECT dispatch, and a heartbeat-ACK miss. Scrape the metrics endpoint (or the structlog output): verify `discord_gateway_messages_total >= 10`, `discord_gateway_reconnect_total >= 2`, `discord_gateway_dispatch_total{event="MESSAGE_CREATE"} >= 10`. Send SIGTERM; assert the worker closes the socket with code 1000, flushes any in-flight ingestion call, and exits 0 within 5 seconds.

**Acceptance Scenarios**:

1. **Given** the worker is connected and has dispatched messages, **When** an operator scrapes metrics, **Then** the counters `discord_gateway_messages_total`, `discord_gateway_reconnect_total{reason=...}`, `discord_gateway_dispatch_total{event=...}` are all observable and non-decreasing.
2. **Given** the worker is connected, **When** SIGTERM is delivered, **Then** the worker sends a final heartbeat (op 1), closes the WSS with code 1000 (normal closure), drains the dispatch queue, and exits 0 within 5 seconds.
3. **Given** the worker has been unable to connect for 60 seconds (network outage), **When** an operator scrapes metrics, **Then** `discord_gateway_connection_state{state="connected"}` is 0 and `discord_gateway_connect_failure_total` has incremented; backoff seconds are recorded in a histogram.
4. **Given** the worker emits a log line during MESSAGE_CREATE dispatch, **When** the operator parses the log, **Then** the line contains `installation_row_id`, `tenant_id` (UUIDs), `channel_id`, and `message_id` — but never the raw `guild_id` (SC-006 from IN-09 holds).

---

### Edge Cases

- **MESSAGE_CONTENT intent not enabled in Developer Portal**: IDENTIFY will succeed but messages from non-mention channels arrive with `content` empty. The worker MUST detect this at startup (e.g., on first MESSAGE_CREATE with `content=""` from a non-mention) and emit a single WARN log line directing the operator to the Developer Portal, then continue best-effort with empty content_text. The worker MUST NOT crash.
- **Gateway intent rejection (close 4014)**: We requested MESSAGE_CONTENT but it's not enabled. Worker exits with status 1 and an explicit log line; supervisor must NOT auto-restart (operator intervention required to enable the intent).
- **Bot added mid-session**: The bot is added to a new guild while the worker is connected. Discord sends GUILD_CREATE; subsequent MESSAGE_CREATE events for that guild flow through dispatch. If no `provider_installations` row exists, they hit the US4 path (silent drop with metric).
- **Bot kicked mid-session**: Discord sends GUILD_DELETE; the worker MAY surface this as a hint to IN-09's chokepoint, but the canonical kick-detection path remains the outbound-401 chokepoint from IN-09. This worker does NOT call IN-09's chokepoint from a GUILD_DELETE dispatch (avoids dual signaling).
- **Very long message (Discord max is 4 000 chars for Nitro, 2 000 for free)**: content_text holds the full message verbatim; no Fyralis-side truncation.
- **Message with empty content but attachments (file upload, no caption)**: content_text is `""`; metadata.attachment_count is non-zero. The observation is still committed.
- **Resume window expiry**: If the worker is offline for longer than Discord's documented resume window, RESUME returns INVALID_SESSION(`d=false`) and a full reconnect ensues — observations during the offline window are lost. This is an accepted gap; the worker MUST log a single WARN with `outage_seconds` so an operator can decide whether to backfill manually.
- **Network jitter producing rapid reconnect loop**: Backoff (1 s → 2 s → 4 s → 8 s → … cap 60 s, jitter ±25 %) ensures the worker doesn't hot-loop. After 10 consecutive failed connects, the worker emits an ERROR log but does not exit; recovery continues at the capped cadence.
- **Message from a guild we own but channel the bot can't see**: Discord delivers MESSAGE_CREATE only for channels where the bot has the View Channel permission. If a message appears for an unseen channel (Discord bug or perm misconfiguration), treat it as US1 — ingest normally; the bot's permission to read is Discord's concern.
- **MESSAGE_CREATE arrives during shutdown**: A dispatch arrives after SIGTERM but before the dispatch queue drains. The handler completes the ingest call (commits to DB) before the worker exits. This is the "do not drop in-flight events" property of US5.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST run a dedicated long-running worker process whose sole responsibility is the Discord Gateway WSS connection. The worker MUST be startable, stoppable, and restartable independently of the gateway HTTP service and the existing think/post_commit workers.
- **FR-002**: The worker MUST connect to `wss://gateway.discord.gg/?v=10&encoding=json`, await the HELLO op (10), start a heartbeat loop firing every `heartbeat_interval * 0.7` ms, and send IDENTIFY (op 2) with the bot token from `DISCORD_BOT_TOKEN` env and intents set to `GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT`.
- **FR-003**: On READY DISPATCH, the worker MUST capture `session_id` and `resume_gateway_url` for subsequent RESUME attempts.
- **FR-004**: On any WSS close code in the resumable set (4000–4003, 4005–4009), the worker MUST reopen the connection to `resume_gateway_url`, send RESUME (op 6) with the captured `session_id` and last received `seq`, and resume dispatching.
- **FR-005**: On WSS close codes 4004 (authentication failed), 4013 (invalid intents), 4014 (disallowed intents), the worker MUST log the failure with the close code in a single ERROR-level log line and exit with status 1. The supervising process MUST NOT auto-restart in this case (operator must fix configuration).
- **FR-006**: On INVALID_SESSION dispatch (op 9) with `d=true`, the worker MUST resume on the same session. With `d=false`, the worker MUST perform a full reconnect with a fresh IDENTIFY.
- **FR-007**: For every MESSAGE_CREATE dispatch the worker MUST: (a) check `author.bot` and `webhook_id` and silently drop with metric increment if either indicates a non-human source; (b) resolve `guild_id → tenant_id` via the existing `TenantResolver` (same path IN-09 uses for slash commands); (c) on UnknownInstallation, drop silently with metric increment and no raw guild_id in logs; (d) on resolved tenant, call the ingestion handler with `source_channel='discord:message'`, `external_id='discord:<message_id>'`, `source_actor_ref='discord:<author_id>'`, `content_text` set to `message.content` verbatim, `occurred_at` parsed from `message.timestamp`, and metadata containing `channel_id`, `guild_id_hash`, `mention_user_ids`, `attachment_count`.
- **FR-008**: The MESSAGE_CREATE → observation path MUST be idempotent on `external_id='discord:<message_id>'`. A duplicate dispatch (Discord retry or our own) MUST NOT produce a second observation. The dedup mechanism is the existing `(source_channel, external_id)` unique index used by IN-09's slash command ingest — no new index is required.
- **FR-009**: `services/ingestion/handlers/__init__.py::CHANNEL_TRUST_MAP` MUST be extended with `"discord:message": "attested_agent"` so observations from this path carry the right `trust_tier`.
- **FR-010**: Structured log lines emitted by the worker MUST NOT contain the raw `guild_id` of any guild. Acceptable identifiers in logs: `tenant_id`, `installation_row_id`, `channel_id`, `message_id`, `short_guild_hash` (BLAKE2b 8-byte digest of guild_id). This preserves IN-09 SC-006.
- **FR-011**: The worker MUST emit the following metrics: `discord_gateway_connection_state{state}` (gauge), `discord_gateway_reconnect_total{reason}` (counter), `discord_gateway_dispatch_total{event}` (counter), `discord_gateway_messages_total` (counter, MESSAGE_CREATE only, after author.bot filter), `discord_gateway_filtered_bot_total{source}` (counter), `discord_gateway_dropped_unknown_installation_total` (counter), `discord_gateway_connect_failure_total` (counter), `discord_gateway_heartbeat_miss_total` (counter).
- **FR-012**: The worker MUST honour connect-failure backoff: 1 s, 2 s, 4 s, 8 s, 16 s, 32 s, capped at 60 s, with ±25 % jitter. Backoff resets to 1 s on a successful IDENTIFY → READY.
- **FR-013**: On SIGTERM the worker MUST: (a) stop accepting new dispatches into the queue; (b) drain in-flight ingestion calls (await the existing dispatch tasks); (c) send WSS close with code 1000; (d) exit 0. The total grace window is 5 seconds; anything still in flight at that point is logged and abandoned.
- **FR-014**: The worker MUST be wired into `scripts/start.sh` alongside `run_think_worker.py` and `run_post_commit_worker.py`, with its log directed to `/tmp/fyralis_logs/discord_gateway_worker.log` and its PID recorded in `/tmp/fyralis_stack.pids`.
- **FR-015**: The system MUST NOT add any new database table or migration. All persistence reuses the existing `observations` table (new `source_channel` value) and `provider_installations` (read-only here).
- **FR-016**: The system MUST NOT modify the bot-token resolution path in `services/integrations/discord/client.py` (the IN-09 outbound client). The Gateway worker reads `DISCORD_BOT_TOKEN` via the same env-var convention; nothing new in the secret store.
- **FR-017**: The system MUST NOT add code to `services/integrations/slack/` or `services/integrations/discord/` outside the new `services/integrations/discord/gateway/` subpackage. IN-08 and IN-09's existing surfaces remain untouched.
- **FR-018**: When `FYRALIS_ENV=prod` AND the `MESSAGE_CONTENT` intent is rejected by Discord (close 4014), the worker MUST exit immediately with status 1 — never enter a degraded "mention-only" mode silently. Mention-only behavior in production would be a data-quality regression masquerading as success.

### Key Entities

- **Discord Gateway Connection**: A persistent WSS connection to gateway.discord.gg holding session state (`session_id`, last `seq`, `resume_gateway_url`). Owned by the worker process; not persisted to disk; rebuilt on every full reconnect.
- **MESSAGE_CREATE Dispatch**: A Discord gateway event payload containing `id`, `channel_id`, `guild_id`, `author` (with `id`, `bot`, optional `webhook_id`), `content`, `timestamp`, `attachments`, `mentions`, etc. Translated by the dispatcher into an ingestion handler call.
- **Discord Message Observation**: A row in `observations` with `source_channel='discord:message'`, `external_id='discord:<message_id>'`, `source_actor_ref='discord:<author_id>'`, `content_text=<message.content verbatim>`, `trust_tier='attested_agent'`, `kind='signal'`, tenant-scoped via the existing IN-09 install row.
- **Short Guild Hash**: An 8-byte BLAKE2b digest of `guild_id` used in log lines as a tenant-disambiguating identifier that does not expose the workspace's enumerable ID. Same construction as IN-09's `short_guild_hash`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A normal Discord message posted in a watched channel of a known guild appears as an observation row with `source_channel='discord:message'` within 5 seconds of posting, for 100 % of messages in a steady-state connection.
- **SC-002**: For 100 % of MESSAGE_CREATE events with `author.bot=true` or non-null `webhook_id`, zero observations are produced — the filter has no false negatives.
- **SC-003**: For 100 % of duplicate MESSAGE_CREATE deliveries (same `message.id` arriving twice), exactly one observation is produced; no duplicates.
- **SC-004**: For 100 % of MESSAGE_CREATE events from a guild with no `enabled=TRUE provider_installations` row, the worker drops silently, increments `discord_gateway_dropped_unknown_installation_total`, and never crashes.
- **SC-005**: The connection survives at least 24 continuous hours on a stable network without manual intervention; if Discord forces a reconnect or RESUME within that window, the observation count post-reconnect equals the message count posted post-reconnect (no gap, no duplicates).
- **SC-006**: Zero log lines emitted by the Gateway worker contain the raw `guild_id` of any guild. Verified by grepping the worker log file after a 24 h soak run.
- **SC-007**: `discord_gateway_connection_state{state="connected"}` is observable to be 1 within 30 seconds of worker startup on the deployed environment.
- **SC-008**: On SIGTERM with messages in flight, the worker exits 0 within 5 seconds AND zero in-flight ingestion calls are abandoned (verified by counting observations committed before exit vs dispatches received).
- **SC-009**: Zero new migrations, zero new tables, zero changes to existing migrations — verified by `python scripts/check_schema_drift.py` returning 0 with no new files in `db/migrations/`.
- **SC-010**: Zero modifications to `services/integrations/slack/` or to files outside `services/integrations/discord/gateway/` and the documented "Changed" list — verified by `git diff --stat main…HEAD` matching the source.md "Files relevant" inventory.

## Assumptions

- **One Discord application, one DISCORD_BOT_TOKEN**: The Gateway worker uses the same app-level Bot Token as IN-09's slash-command registration and outbound client. We are not running multiple Discord apps in v1.
- **Operator enables MESSAGE_CONTENT intent before deploy**: Documented in the install runbook; verified at startup by exiting with a clear error on close 4014 (FR-005, FR-018).
- **Single shard**: We are in fewer than 2 500 guilds; the worker runs as shard 0 of 1. Sharding remains a future-work item with no v1 schema implications.
- **Discord's resume window is sufficient for routine network jitter**: A 30–60 s outage allows RESUME; longer outages produce an accepted observation gap and a WARN log line.
- **MESSAGE_CREATE arrives within the same connection as READY**: We rely on Discord's documented dispatch sequence; a payload arriving before READY would be a Discord protocol violation and is treated as a fatal protocol error.
- **The existing ingestion handler `services/ingestion/handlers/discord.py` can accept `source_channel='discord:message'` with minimal extension**: The handler's tenant-resolution, idempotency, and observation-shape logic from IN-09 are reusable; the only addition is a code branch on `source_channel` to skip Discord interaction-specific fields (no interaction `token` to strip, etc.).
- **`provider_installations` rows exist for every guild we want to observe**: Either created via IN-09's OAuth flow OR manually inserted by an operator. The worker does not create install rows from GUILD_CREATE dispatches; that bridge is intentionally not built.
- **Trust tier `attested_agent` is the right semantic value for `discord:message`**: A regular Discord user posting in a channel is asserting identity via Discord's auth system, which is comparable to Slack's `slack:message`. Confirmed during the clarify phase.
