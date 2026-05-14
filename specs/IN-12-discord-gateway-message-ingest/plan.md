# Implementation Plan: Discord Gateway WebSocket Message Ingest

**Branch**: `feat/IN-12-discord-gateway-message-ingest` | **Date**: 2026-05-14 | **Spec**: [./spec.md](./spec.md)

## Summary

IN-09 closed Discord ingestion for slash commands via Interactions HTTP. IN-12 extends Discord ingestion to cover **all messages** in channels where the bot is present by introducing a fourth long-running worker process that holds a persistent WSS connection to `wss://gateway.discord.gg`. The worker IDENTIFY-s with the `MESSAGE_CONTENT` privileged intent (operator-toggled in Developer Portal), dispatches every `MESSAGE_CREATE` event to the existing `services/ingestion/handlers/discord.py` (extended to recognise `source_channel='discord:message'`), and survives transient disconnects via Discord's RESUME protocol with no observation gaps or duplicates.

**Zero new migrations.** All observations land in the existing `observations` table with a new `source_channel='discord:message'` value (channel values are application-layer, not DDL). Tenant resolution reuses IN-09's `provider_installations` rows — the install path is untouched. Dedup is enforced by the existing `(source_channel, external_id, occurred_at)` unique index on `observations`.

**Zero new secret types.** Bot token comes from `DISCORD_BOT_TOKEN` env (same path IN-09's `commands.py` and `client.py` use). The Gateway worker reads it once at startup; nothing rotates per-message.

**Zero changes to `services/integrations/slack/*` or to `services/integrations/discord/*` outside the new `gateway/` subpackage.** IN-08 and IN-09 suites pass byte-for-byte (verified by re-running them in CI on the IN-12 PR).

The structural-loop guard against IN-13 (outbound replies re-entering ingest) is the `author.bot` filter at dispatch time, called out in spec FR-007 and Clarifications Q-N.

## Technical Context

**Language/Version**: Python 3.11+ (project uses 3.12 in `.venv`).

**Primary Dependencies**:
- **websockets** (Python lib) — async WSS client. **Verify presence in `pyproject.toml`**; add if missing. Rationale in research.md R1.
- **httpx** — async HTTP for the one-shot `GET https://discord.com/api/v10/gateway/bot` lookup at worker startup (already in project).
- **asyncpg** — DB driver, shared with the rest of the stack.
- **structlog** — logging surface (already in project).
- **PyNaCl** — NOT used by this worker. Ed25519 verification is for inbound HTTP (IN-06 / IN-09); the Gateway WSS uses bot-token IDENTIFY.

**Storage**: Postgres 16 + pgvector — reused tables only (no new DDL).

**Testing**: pytest with `integration` marker for the dispatch → ingestion → observations path (real Postgres + real Ollama per Constitution §IV). The WSS boundary itself is mocked via an in-process fake gateway (an asyncio task that speaks the documented opcode protocol) — this is an external network dependency, not our substrate, so mocking is permitted under §IV.

**Target Platform**: Linux server (docker-compose deploy).

**Project Type**: Worker process alongside existing gateway HTTP service, think worker, post-commit worker.

**Performance Goals**:
- Steady-state ingest latency MESSAGE_CREATE → observation row committed ≤ 5 s (spec SC-001).
- Memory footprint of the worker ≤ 200 MB resident (it's a thin asyncio loop + one WSS connection).
- 24h+ continuous connection without manual restart on stable network (spec SC-005).
- Backoff window on connect failure capped at 60 s; jitter ±25 % to avoid thundering-herd.

**Constraints**:
- Discord's `MESSAGE_CONTENT` privileged intent MUST be enabled in the Developer Portal before deploy. The worker exits fatally on close 4014 regardless of `FYRALIS_ENV` (Clarifications Q-N: no silent degraded mode).
- Heartbeat must fire every `heartbeat_interval * 0.7` ms (Discord-defined, typically ~28 s for the 41,250 ms baseline).
- Bot's own messages and other bots' messages MUST be filtered at dispatch time (`author.bot` and `webhook_id` checks) before tenant resolution — both to avoid IN-13 outbound loops AND to avoid touching tenant_resolver for non-substrate events.

**Scale/Scope**: Single shard, single tenant during this task's lifetime. Sharding deferred to a follow-up at the 2,500-guild threshold.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-evaluated at end of Phase 1.*

| Principle | Status | Notes |
|---|---|---|
| §I Four Foundations distinct | PASS | MESSAGE_CREATE → **Observation** with `kind='signal'`, `trust_tier='attested_human'` (Clarifications). No Model / Act / Resource writes. `discord_user` / `discord_channel` entity hints land in `entities_mentioned` for downstream entity-alias resolution. |
| §II Append-only migrations | PASS | **ZERO new migrations.** No schema change. `source_channel='discord:message'` is a new value in an existing TEXT column. |
| §III Tenant isolation structural | PASS | No new tables. Tenant resolution via existing `provider_installations`. New observations carry `tenant_id` via existing FK + RLS + index. Worker queries hand-rolled with `WHERE tenant_id = $1`. |
| §IV Integration tests, real DB | PASS | Plan mandates live Postgres + Ollama for all `services/integrations/discord/gateway/tests/test_*.py` files. WSS boundary mocked via in-process fake gateway (external network dep, not our substrate). |
| §V LLM provider pluggability | N/A | Worker does not call an LLM directly. Downstream ingestion handler does (existing path; nothing new). |
| §VI Voice rules enforcement | N/A | Worker writes observations, not rendered text. |
| §VII uuid7() for substrate rows | PASS | Observation inserts go through the existing ingestion handler, which uses `uuid7()`. No new substrate write path bypasses this. |
| §VIII No `print()` in service code | PASS | Worker uses `structlog` exclusively. Process-entry script (`scripts/run_discord_gateway_worker.py`) is allowed `print()` for boot diagnostics (matches `run_think_worker.py` and `run_post_commit_worker.py` precedent). |
| §IX Phase ordering | PASS | No migrations → no DDL phase. Phases collapse to: scaffolding → connection lifecycle → reconnect/RESUME → ingest → operational. Each phase is a runnable slice. |
| §X No mocking real boundaries | PASS | Postgres, Ollama, and `lib/shared/secrets/` are real in tests. Only the external Discord Gateway WSS boundary is mocked (justified above). |

**Verdict**: All gates PASS. No violations to justify. Proceeding to research.

## Project Structure

```
services/integrations/discord/
├── __init__.py                    (CHANGED: re-export worker entrypoint)
├── client.py                      (UNCHANGED — IN-09's outbound REST client)
├── commands.py                    (UNCHANGED — IN-09's slash command registration)
├── metrics.py                     (UNCHANGED — IN-09's install/uninstall counters)
├── oauth.py                       (UNCHANGED — IN-09's OAuth handlers)
├── uninstall.py                   (UNCHANGED — IN-09's bot-kick chokepoint)
└── gateway/                       (NEW subpackage)
    ├── __init__.py                (NEW: package docstring referencing IN-12 spec)
    ├── client.py                  (NEW: DiscordGatewayClient — WSS connect, HELLO, IDENTIFY, heartbeat, RESUME)
    ├── dispatch.py                (NEW: opcode + event dispatcher; MESSAGE_CREATE → ingestion)
    ├── metrics.py                 (NEW: 8 gateway-specific counters/gauges)
    ├── worker.py                  (NEW: long-running asyncio entry; backoff loop + SIGTERM handler)
    └── tests/
        ├── __init__.py            (NEW)
        ├── conftest.py            (NEW: fake-gateway fixture)
        ├── test_client_lifecycle.py     (NEW: HELLO → IDENTIFY → READY → heartbeat ACK loop)
        ├── test_client_reconnect.py     (NEW: close codes, RESUME, INVALID_SESSION)
        ├── test_dispatch_message_create.py  (NEW: end-to-end → observation)
        ├── test_dispatch_filters.py     (NEW: author.bot, webhook_id, unknown-guild paths)
        └── test_worker_shutdown.py      (NEW: SIGTERM graceful drain)

services/ingestion/handlers/
├── __init__.py                    (CHANGED: add "discord:message" to CHANNEL_TRUST_MAP)
└── discord.py                     (CHANGED: branch on source_channel; minimal extension for discord:message)

scripts/
├── run_discord_gateway_worker.py  (NEW: process entrypoint)
└── start.sh                       (CHANGED: add worker to startup sequence)

CODEBASE-ARCHITECTURE.md           (CHANGED: append §16 — IN-12 Gateway worker)
specs/IN-12-discord-gateway-message-ingest/  (NEW: spec artifacts)
```

## Phases

Per Constitution §IX: no migrations → no DDL phase → phase order collapses to functionality-first.

### Phase 0 — Worker scaffolding (foundational, blocking; 0.5 d)

Goal: A worker process that starts, loads env, builds dependencies, opens a WSS, IDENTIFY-s, receives HELLO + READY, and idles. No DB writes yet.

- T001 Create `services/integrations/discord/gateway/__init__.py` and `services/integrations/discord/gateway/worker.py` skeletons.
- T002 Add `websockets` to `pyproject.toml` if not already present. Verify with `.venv/bin/pip show websockets`.
- T003 Write `scripts/run_discord_gateway_worker.py` (asyncpg pool + secret_store + tenant_resolver + ingestion_handler deps assembly + worker.run()).
- T004 [P] Write `services/integrations/discord/gateway/metrics.py` — define 8 counters/gauges per FR-011. Use the same structlog-counter pattern as `services/integrations/discord/metrics.py`.
- T005 Smoke test: `python scripts/run_discord_gateway_worker.py` boots, connects, logs "ready", idles. Manual verification only; no integration test in this phase.

### Phase 1 — Connection lifecycle (US2 P1; 1 d)

Goal: HELLO → IDENTIFY → READY → heartbeat loop with ACK validation.

- T010 Implement `DiscordGatewayClient.connect()` — GET /gateway/bot (httpx, Bot token), open WSS, await HELLO op 10, capture `heartbeat_interval`.
- T011 Implement heartbeat task — sends op 1 every `heartbeat_interval * 0.7` ms; tracks ACK receipt; flags missed ACK.
- T012 Implement IDENTIFY (op 2) with intents = `GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT`. Verify the integer mask in research R3.
- T013 Implement READY DISPATCH handler — capture `session_id` and `resume_gateway_url`.
- T014 Write `test_client_lifecycle.py` — fake gateway accepts IDENTIFY, sends READY, asserts heartbeat opcode + token. Real Postgres for the connection-state metric increment.

### Phase 2 — Reconnect + RESUME (US2 P1 continuation; 0.5 d)

Goal: Tolerate Discord-initiated disconnects without observation gaps.

- T020 Define close-code policy: resumable {4000, 4001, 4002, 4003, 4005, 4006, 4007, 4008, 4009}, fatal {4004, 4013, 4014}, full-reconnect {1006 + INVALID_SESSION d=false}.
- T021 Implement RESUME (op 6) with `session_id + last_seq`.
- T022 Implement INVALID_SESSION (op 9) handler — resume vs full-reconnect based on `d`.
- T023 Implement fatal-close handler — log + exit 1, supervisor does NOT restart (FR-005).
- T024 Implement backoff: 1 → 2 → 4 → 8 → 16 → 32 → cap 60 s, ±25 % jitter; reset on READY (FR-012).
- T025 Write `test_client_reconnect.py` — fake gateway sends close 4000 mid-stream, asserts RESUME with right seq, no dispatch loss.

### Phase 3 — MESSAGE_CREATE ingest (US1 P1, US3 P2, US4 P2; 1 d)

Goal: Real dispatch → existing ingestion handler → observation row.

- T030 Extend `services/ingestion/handlers/discord.py` to branch on `source_channel`: existing `discord:interaction` path unchanged; new `discord:message` branch reuses tenant resolution + dedup with `content_text=<message.content verbatim>`, no token-strip required (no Discord interaction token in message events).
- T031 Add `"discord:message": "attested_human"` to `services/ingestion/handlers/__init__.py::CHANNEL_TRUST_MAP`.
- T032 Implement `services/integrations/discord/gateway/dispatch.py::handle_message_create()`:
  - Step 1: `author.bot` + `webhook_id` filter → drop with metric (FR-007a).
  - Step 2: Resolve tenant via `TenantResolver.resolve('discord', payload)` (existing IN-07 path).
  - Step 3: UnknownInstallation → drop with metric + no raw guild_id log (FR-007c, US4).
  - Step 4: Build ingestion handler payload — `source_channel='discord:message'`, `external_id=f"discord:{message.id}"`, `source_actor_ref=f"discord:{author.id}"`, `content_text=message.content`, `occurred_at=parse(message.timestamp)`, metadata with `channel_id`, `short_guild_hash`, `mention_user_ids`, `attachment_count`.
  - Step 5: Call ingestion_handler.handle(); on IntegrityError (dedup), log + metric, no error propagation.
- T033 [P] Wire `handle_message_create` into the gateway client's dispatch loop on event `'MESSAGE_CREATE'`.
- T034 Write `test_dispatch_message_create.py` — fake gateway emits MESSAGE_CREATE for a seeded `provider_installations` row, assert single observation row with all fields per spec US1 acceptance scenarios.
- T035 Write `test_dispatch_filters.py` — three cases: `author.bot=true` (self), `author.bot=true` (other), `webhook_id` non-null. Assert zero observations, correct `discord_gateway_filtered_bot_total{source}` increment.
- T036 Write `test_dispatch_unknown_guild.py` (or include in test_dispatch_filters.py) — MESSAGE_CREATE from guild with no install row → silent drop, metric increment, no raw guild_id in caplog.

### Phase 4 — Operational hardening (US5 P3; 1 d)

Goal: Observability, jitter, graceful shutdown.

- T040 Add SIGTERM handler in `worker.py` — set shutdown flag, drain dispatch queue, send WSS close 1000, exit 0 within 5 s (FR-013).
- T041 [P] Verify all log lines in `gateway/client.py` and `gateway/dispatch.py` use `short_guild_hash` instead of raw `guild_id` (manual grep + add a test_no_raw_guild_id_in_logs.py).
- T042 [P] Wire worker into `scripts/start.sh` alongside other workers; verify pidfile + logfile.
- T043 Write `test_worker_shutdown.py` — start worker with fake gateway, SIGTERM mid-stream, assert exit 0 in ≤ 5 s + all dispatched messages committed.
- T044 Append §16 to `CODEBASE-ARCHITECTURE.md` documenting the new worker.

### Phase 5 — Regression sweep (0.5 d)

- T050 Re-run IN-08 + IN-09 test suites — `pytest services/integrations/tests/ services/webhooks/tests/`. Zero diffs.
- T051 Schema-drift check — `python scripts/check_schema_drift.py`. Zero new migrations.
- T052 ruff on all changed paths.
- T053 Manual 30-min soak test against real Discord with the test guild — verify steady-state ingest + at least one reconnect via Discord's natural cadence.

**Total estimate: 4 days** (matches source.md).

## Out of Scope (deferred)

- MESSAGE_UPDATE / MESSAGE_DELETE dispatch handling.
- DM (direct-message) ingest.
- Multi-shard sharding logic.
- Outbound message posting (covered by IN-13 follow-up Acts).
- Voice, presence, typing dispatch routing.
- Sticky-membership flow (handling guilds the bot is removed from mid-session — that path uses GUILD_DELETE which per Clarifications Q-N is a metric-only event).
