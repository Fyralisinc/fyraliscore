IN-12 [P1] Discord Gateway WebSocket message ingest

Files relevant

New

services/integrations/discord/gateway/__init__.py — package docstring referencing IN-12 spec
services/integrations/discord/gateway/client.py — persistent WSS client (connect, IDENTIFY, heartbeat, reconnect, RESUME)
services/integrations/discord/gateway/dispatch.py — opcode + event dispatcher; MESSAGE_CREATE → ingestion
services/integrations/discord/gateway/metrics.py — Prometheus counters for connection state, reconnects, per-guild message rate
services/integrations/discord/gateway/worker.py — long-running asyncio entrypoint with backoff loop
scripts/run_discord_gateway_worker.py — process entrypoint (loads .env, builds dependencies, runs worker)
specs/IN-12-discord-gateway-message-ingest/ — spec artifacts

Changed

services/ingestion/handlers/__init__.py — add "discord:message" trust tier entry to CHANNEL_TRUST_MAP
services/ingestion/handlers/discord.py — extend handler to recognise discord:message events (in addition to discord:interaction); reuse _strip_credentials + tenant-resolution path
services/integrations/discord/__init__.py — re-export gateway worker entrypoint
scripts/start.sh — add the gateway worker to the stack startup (alongside think_worker, post_commit_worker)
CODEBASE-ARCHITECTURE.md — document IN-12

Why it is needed

Post IN-09, Discord ingestion is limited to slash commands (/fyralis) over the Interactions HTTP webhook. That covers explicit asks from users but misses the real organisational signal — the day-to-day chatter in channels where the team discusses work, blockers, decisions, and context.

Discord does NOT push normal messages over webhooks. The only way to receive every message in a guild the bot is in is to maintain a persistent WSS connection to gateway.discord.gg (a "Gateway client"). Slack's Events API does this server-side and posts to our HTTP endpoint; Discord requires us to be the client.

Without IN-12:
- We observe a Discord user only when they explicitly invoke /fyralis. The vast majority of conversation is invisible.
- Fyralis's organisational-intelligence value proposition is thin on Discord — we have the install footprint but not the data stream.
- Slack has parity (full message ingest); Discord does not — uneven cross-channel coverage degrades reasoning quality.

How can it be done

Land in 5 ordered phases, each independently deployable.

Phase 1 — Worker scaffolding (0.5 d)

scripts/run_discord_gateway_worker.py: load .env, build asyncpg pool + secret_store + tenant_resolver + ingestion handler dependencies, instantiate DiscordGatewayClient, run forever with exponential backoff on unrecoverable error.
Wire it into scripts/start.sh alongside think_worker and post_commit_worker.
No DB writes yet — just connect → log "ready" → idle.

Phase 2 — Connection lifecycle (1 d)

services/integrations/discord/gateway/client.py:
  - GET /gateway/bot with Bot token to fetch wss URL + recommended shard count (we'll start with shard 0/1 only)
  - Open WSS, await HELLO (op 10), capture heartbeat_interval
  - Start background heartbeat task (op 1) every heartbeat_interval * 0.7
  - Send IDENTIFY (op 2) with token + intents = GUILDS (1) | GUILD_MESSAGES (1<<9) | MESSAGE_CONTENT (1<<15)
  - Await READY DISPATCH; capture session_id and resume_gateway_url
  - Tolerate HEARTBEAT_ACK (op 11); reconnect if missed

Phase 3 — Reconnect + RESUME (0.5 d)

  - Discord close codes 4001-4014 categorised: resumable vs full-reconnect vs fatal
  - Resumable close → reopen WSS to resume_gateway_url, send RESUME (op 6) with session_id + last_seq
  - INVALID_SESSION DISPATCH (op 9) with d=true → can resume; d=false → full reconnect (re-IDENTIFY)
  - Fatal close codes (4004 authentication failed, 4013 invalid intents, 4014 disallowed intents) → log + exit; supervisor restarts

Phase 4 — MESSAGE_CREATE ingest (1 d)

services/integrations/discord/gateway/dispatch.py:
  - On MESSAGE_CREATE dispatch:
    1. Skip if author.bot == true (don't ingest our own or other bots' messages)
    2. Resolve tenant via TenantResolver.resolve(provider='discord', payload={'guild_id': guild_id}) — reuse IN-07 substrate
    3. If UnknownInstallation → drop silently (bot in guild we don't track); emit metric `discord_gateway_dropped_unknown_installation`
    4. Strip any credential-shaped fields from the raw payload (defence in depth)
    5. Call ingestion_handler.handle({
         source_channel: "discord:message",
         external_id: f"discord:{message.id}",
         occurred_at: message.timestamp (ISO8601 from Discord),
         source_actor_ref: f"discord:{author.id}",
         content_text: message.content (verbatim),
         tenant_id: <resolved>,
         metadata: { channel_id, guild_id_hash, mention_user_ids, attachment_count }
       })
  - Add "discord:message" to services/ingestion/handlers/__init__.py::CHANNEL_TRUST_MAP with trust_tier="attested_agent"

Phase 5 — Operational hardening (1 d)

services/integrations/discord/gateway/metrics.py:
  - discord_gateway_connection_state{state="connected"|"reconnecting"|"resuming"}
  - discord_gateway_reconnect_total{reason}
  - discord_gateway_dispatch_total{event}
  - discord_gateway_dropped_unknown_installation_total
- Exponential backoff on connect failure: 1s, 2s, 4s, 8s, capped at 60s, with jitter
- Structured logging via structlog; NEVER emit raw guild_id (use installation_row_id or short_guild_hash)
- Graceful shutdown on SIGTERM (send op 1 final heartbeat → close 1000 → exit 0)

Acceptance criteria

- Worker connects to gateway.discord.gg and stays connected for 24h+ without manual restart on a stable network.
- A normal message posted in a Discord channel where the Fyralis bot is present lands as an observation with source_channel='discord:message' and external_id='discord:<message_id>' within 5 seconds of being posted.
- The bot's own messages and messages from other bots in the same channel are NOT ingested (author.bot filter).
- Per-guild tenant resolution uses the same provider_installations row that IN-09's OAuth callback created; no new tables required.
- Disconnect + RESUME within Discord's resume window produces no duplicate observations and no observation gap (assuming we hold the buffered seq).
- A second MESSAGE_CREATE for the same message_id (Discord retry or our own retry path) does NOT create a second observation — dedup on (source_channel, external_id) holds.
- content.text on the observation matches the user-visible message.content verbatim (no truncation, no markdown-stripping).
- Worker process can be started/stopped/restarted independently from the gateway HTTP service; no shared in-memory state.
- Structured log records emitted by the gateway client and dispatcher MUST NOT include the raw guild_id; channel_id and message_id are acceptable, guild_id is not (consistent with IN-09 SC-006).
- discord_gateway_messages_total counter increments per ingested MESSAGE_CREATE.
- A MESSAGE_CREATE from a guild without a provider_installations row returns silently (drop + metric increment), never crashes, never writes a partial row.

Security / constitution notes

- MESSAGE_CONTENT is a Discord privileged intent. Operator MUST enable it in the Developer Portal → Bot → Privileged Gateway Intents before deploying. For apps in <100 servers this is a toggle; for >100 it requires Discord verification. Document this in the install runbook.
- Bot token is the same DISCORD_BOT_TOKEN env var that IN-09's commands.py and client.py use — no new secret material.
- New observations are tenant-scoped via the existing observations table (Constitution §III: tenant_id FK + RLS + tenant-prefixed indexes already in place from prior migrations).
- Constitution §VII: any new observation row uses uuid7().
- Constitution §IV: integration tests must hit the real Postgres + real Ollama. Gateway WebSocket itself can be mocked at the WSS boundary (it's an external network dependency, not our substrate), but the dispatch → ingestion → observations path must use real DB.
- Constitution §IX phase ordering: no migrations, no substrate-shape change — phase order collapses to "scaffolding → connection lifecycle → reconnect → ingest → operational hardening" (as listed above).
- author.bot filter is non-negotiable. Without it, an outbound message from a future IN-13 follow-up would re-enter the ingest pipeline and create an infinite loop.
- Author identity: source_actor_ref = `discord:{author.id}` matches IN-09's slash-command ingest, so the same Discord user maps to the same actor across both surfaces (interaction + free-text message).

Out of scope (follow-up tasks)

- MESSAGE_UPDATE / MESSAGE_DELETE — track as a follow-up; for now MESSAGE_CREATE only. Edits are a real signal but ingestion semantics (overwrite vs new observation) need their own clarification round.
- DM (direct-message) ingest — only guild messages in this slice.
- Sharding — we won't be in 2,500+ guilds soon; the WS GET /gateway/bot response gives us shard count and we'll start with shard 0/1.
- Outbound replies (chat.postMessage equivalent / interaction follow-up enrichment) — that's IN-13.
- Voice, presence, typing indicators — none ingested.
- Cross-guild bot rebalancing under guild-ownership transfer — assume guild_id is stable for our purposes; revisit if a real customer hits this.

Estimated effort

4 days (0.5 d Phase 1, 1 d Phase 2, 0.5 d Phase 3, 1 d Phase 4, 1 d Phase 5).
