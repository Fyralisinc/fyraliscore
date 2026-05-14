# IN-12 Research Notes

Decisions taken during planning. Each entry: Decision → Rationale → Alternatives considered.

## R1. WebSocket client library: `websockets` (Python)

**Decision**: Use the standalone `websockets` Python library (already widely deployed; mature; pure-asyncio).

**Rationale**: It's the canonical async WSS client for Python, has no transitive heavy dependencies, and exposes the close-code + close-reason hooks we need for Discord's protocol. The alternative — building on top of httpx-ws or aiohttp — pulls broader HTTP machinery we don't need for a single long-lived connection. Maintained by a known author with a stable API since 10.0.

**Alternatives considered**:
- **discord.py** — full bot framework, ~30k lines, opinionated event loop, eats SIGTERM handling. Rejected: we want a minimal raw-protocol client to keep the surface area small and the failure modes legible. Eight files of our code beats one file with 30k lines of framework behind it.
- **aiohttp.WSClient** — works, but aiohttp brings a full HTTP server/client stack we don't need.
- **raw asyncio + websockets handshake** — too low-level; we'd reinvent close-code parsing and frame masking.

## R2. Gateway URL discovery: `GET /api/v10/gateway/bot` on every connect

**Decision**: On every connect attempt (initial + reconnects), call `GET https://discord.com/api/v10/gateway/bot` with `Authorization: Bot <token>` to fetch the current WSS URL and recommended shard count.

**Rationale**: Discord rotates the gateway URL occasionally; caching it in env or memory risks pointing at a stale endpoint after a Discord-side migration. The cost is one HTTP round-trip per connect (typically per-day with a stable connection). For RESUME, use the `resume_gateway_url` captured in READY — not the `/gateway/bot` URL.

**Alternatives**:
- **Cache the URL for 24 h** — fragile; if Discord rotates we'd retry against a 410 for hours.
- **Hard-code `wss://gateway.discord.gg`** — works today but explicitly discouraged by Discord docs.

## R3. Gateway Intents bitmask: `GUILDS (1<<0) | GUILD_MESSAGES (1<<9) | MESSAGE_CONTENT (1<<15)`

**Decision**: IDENTIFY with intent flags `1 | 512 | 32768 = 33,281`.

**Rationale**:
- `GUILDS (1<<0)` — required to receive GUILD_CREATE / GUILD_DELETE for tracking membership.
- `GUILD_MESSAGES (1<<9)` — required to receive MESSAGE_CREATE / MESSAGE_UPDATE / MESSAGE_DELETE for guild channels.
- `MESSAGE_CONTENT (1<<15)` — **privileged**; required to receive the `content` field populated. Without it, MESSAGE_CREATE arrives with `content=""` for messages where the bot is not mentioned.

We deliberately omit GUILD_MEMBERS (1<<1), MESSAGE_REACTIONS (1<<10), and TYPING (1<<11) — they're noise for substrate ingest.

**Alternatives**:
- **All intents (privileged + non-privileged)** — adds operational toggle requirements (GUILD_MEMBERS and GUILD_PRESENCES are also privileged) without payoff.
- **Just GUILD_MESSAGES + MESSAGE_CONTENT** — works for ingest but loses GUILD_DELETE → metric (Clarifications Q-N).

## R4. Heartbeat strategy: fire at `heartbeat_interval * 0.7` ms with random initial jitter

**Decision**: Compute next heartbeat fire as `heartbeat_interval * 0.7` ms after the previous tick; on first heartbeat after IDENTIFY, jitter the initial wait by `[0, heartbeat_interval]` ms (Discord docs recommend this to spread thundering-herd from many bots starting simultaneously).

**Rationale**: At `0.7` the heartbeat fires comfortably before Discord's `heartbeat_interval` timeout (typically 41.25 s; 70 % is ~28.9 s). Initial jitter spreads load on Discord's gateway if many of us reconnect after a Discord-side incident.

**ACK validation**: Each heartbeat MUST receive op 11 (HEARTBEAT_ACK) before the next tick fires. If two ticks elapse without an ACK, treat as connection broken: close WSS with 4000 and reconnect (RESUME path).

**Alternatives**:
- **0.9 * heartbeat_interval** — closer to the limit, less margin; rejected.
- **Fixed 30 s** — risks drift if Discord changes its baseline. Rejected.

## R5. Reconnect close-code policy

**Decision**: Codify the close-code → action map per Discord's documented table:

| Close Code | Discord Name | Action |
|---|---|---|
| 1000 | Normal closure | Final exit (we initiated) |
| 1006 | Abnormal closure | Reconnect + RESUME |
| 4000 | Unknown error | Reconnect + RESUME |
| 4001 | Unknown opcode | Reconnect + RESUME |
| 4002 | Decode error | Reconnect + RESUME |
| 4003 | Not authenticated | Reconnect + IDENTIFY |
| 4004 | Authentication failed | **Fatal exit 1** — operator must fix `DISCORD_BOT_TOKEN` |
| 4005 | Already authenticated | Reconnect + RESUME |
| 4007 | Invalid sequence | Reconnect + IDENTIFY (RESUME would re-trigger) |
| 4008 | Rate limited | Reconnect + RESUME with extra backoff |
| 4009 | Session timed out | Reconnect + IDENTIFY |
| 4010 | Invalid shard | **Fatal exit 1** — operator must fix shard config |
| 4011 | Sharding required | **Fatal exit 1** — defer to follow-up |
| 4012 | Invalid API version | **Fatal exit 1** — bug; we hard-code v10 |
| 4013 | Invalid intent(s) | **Fatal exit 1** — operator must enable MESSAGE_CONTENT |
| 4014 | Disallowed intent(s) | **Fatal exit 1** — Discord verification needed |

**Rationale**: Matches Discord's official documentation. Fatal codes are always operator-actionable; we never auto-restart on them because retrying without operator action just burns cycles.

## R6. Worker process model: standalone asyncio process

**Decision**: Run the Gateway worker as a separate Python process (`scripts/run_discord_gateway_worker.py`), not as a background task inside the gateway HTTP service.

**Rationale**:
- The WSS connection holds a socket open indefinitely; co-hosting with the HTTP service means an HTTP-service restart drops the gateway connection. Independent processes mean each can be cycled without affecting the other.
- Matches the pattern of `run_think_worker.py` and `run_post_commit_worker.py` — operators already know how to start/stop/observe these.
- The HTTP gateway's request lifecycle assumes short-lived requests; a long-lived WSS task in the same process complicates graceful shutdown.

**Alternatives**:
- **Background task inside gateway HTTP** — simpler ops but couples lifecycle. Rejected.
- **Separate process pool with shard-per-process** — overkill for single-shard v1.

## R7. Author filter at dispatch time (before tenant resolution)

**Decision**: Apply the `author.bot OR webhook_id != None` filter **before** any other work — before tenant resolution, before metadata extraction, before any DB hit.

**Rationale**: It's the fastest filter (in-memory boolean check). It also prevents an outbound IN-13 message from triggering a tenant lookup, which is wasted work even though the dedup index would eventually catch it. And it keeps the bot's own messages from showing up as "would-have-been-ingested-but-for-the-filter" metric noise.

**Filter precedence**:
1. `author.bot == true` → drop, metric `filtered_bot_total{source="self"|"other_bot"}`
2. Otherwise if `webhook_id is not None` → drop, metric `filtered_bot_total{source="webhook"}`
3. Otherwise → proceed to tenant resolution

## R8. Idempotency mechanism: existing `observations` unique index

**Decision**: Rely on the existing `(source_channel, external_id, occurred_at)` unique index on `observations` (from IN-04 era; verified present in `db/migrations/`).

**Rationale**: No new index needed; the constraint is already correct for our usage. On dedup hit, the asyncpg `UniqueViolationError` is caught by the ingestion handler and converted to a successful no-op return — the existing IN-09 path uses this verbatim.

**Verification**: `\d observations` shows `observations_source_channel_external_id_occurred_at_key` UNIQUE — re-confirmed during IN-09 implementation. No change required for IN-12.

## R9. Trust tier for `discord:message`: `attested_agent`

**Decision**: New observations from this path carry `trust_tier='attested_agent'` (Clarifications Q1).

**Rationale**: A regular Discord user posting in a channel asserts identity via Discord's auth system; this is analogous to a Slack user posting in a channel (`slack:message` → `attested_agent`). Discord slash commands (`/fyralis ask`) are `attested_agent` because the user is invoking the bot explicitly — that's a different signal class.

## R10. Logging redaction: short_guild_hash everywhere

**Decision**: All log records emitted by the Gateway worker use `short_guild_hash` (BLAKE2b 8-byte digest of `guild_id`) instead of the raw `guild_id`. `tenant_id`, `installation_row_id`, `channel_id`, `message_id` are acceptable in logs.

**Rationale**: Preserves IN-09 SC-006 (no raw guild_id in logs). Channel/message IDs are not enumerable workspace identifiers the same way guild_id is.

**Implementation**: Reuse `services/integrations/discord/oauth.py::short_guild_hash` — already imported by other Discord modules.

## R11. SIGTERM handling: drain-and-exit pattern

**Decision**: On SIGTERM, set a shutdown flag → stop accepting new dispatches → await in-flight dispatches → send WSS close 1000 → exit 0. Hard cap at 5 s wall.

**Rationale**: Matches the "graceful shutdown" pattern already used in `run_think_worker.py`'s shutdown sequence. 5 s is plenty for our typical dispatch latency (≤ 500 ms for ingest + commit).

**Implementation**: `asyncio.Event` as shutdown flag; `signal.signal(SIGTERM, handler)` sets it from the signal handler synchronously.

## R12. Fake-gateway fixture for tests

**Decision**: Provide a pytest fixture `fake_gateway` that runs an in-process WSS server (using `websockets.serve`), speaks the documented opcode protocol, and lets tests script the message stream and inject failures.

**Rationale**: The Gateway protocol is well-documented and stable. Building a fake we control means our integration tests don't depend on Discord uptime, don't burn rate-limit budget, and can deterministically reproduce edge cases (resume window expiry, INVALID_SESSION, fatal close codes).

**Caveat**: The fake gateway IS a mock of an external boundary. Per Constitution §X, external boundaries CAN be mocked in integration tests. The internal substrate (Postgres, Ollama, secret store) is real — that's where §IV bites.

**Implementation**: `services/integrations/discord/gateway/tests/conftest.py` exports the fixture. ~150 lines of fake-server code; refactor in if it grows beyond 300.
