# Tasks: IN-12 — Discord Gateway WebSocket Message Ingest

**Feature**: [./spec.md](./spec.md) | **Plan**: [./plan.md](./plan.md)
**Branch**: `feat/IN-12-discord-gateway-message-ingest`
**Total**: 53 tasks across 8 phases.

Per Constitution §IX with no migrations, the foundational phase contains all the WSS protocol plumbing that every user story depends on. User-story phases below the foundational are independently testable.

---

## Phase 1 — Setup

- [X] T001 Verify `websockets` (Python WSS client) is in `pyproject.toml`; if absent, add it under `[project.dependencies]` and run `pip install -e .` in `.venv`.
- [X] T002 Verify Postgres on `localhost:5433` is reachable; verify Ollama on `localhost:11434` responds to `GET /api/tags`.
- [X] T003 Verify `.env` carries `DISCORD_BOT_TOKEN`, `DATABASE_URL`, `MASTER_KEK`. No new env vars are introduced by this task.

## Phase 2 — Foundational (blocking; all user stories depend on this)

- [X] T004 Create `services/integrations/discord/gateway/__init__.py` with a module docstring referencing IN-12 spec.
- [X] T005 Create `services/integrations/discord/gateway/metrics.py` exposing 8 counters/gauges per FR-011.
- [X] T006 Create `services/integrations/discord/gateway/client.py` with `DiscordGatewayClient` class.
- [X] T007 Create `services/integrations/discord/gateway/dispatch.py` with `DispatchDeps` dataclass and `handle_dispatch` router.
- [X] T008 Create `services/integrations/discord/gateway/worker.py` with `GatewayWorker` orchestrator + signal handlers + backoff loop.
- [X] T009 Create `scripts/run_discord_gateway_worker.py` — process entrypoint mirroring `run_think_worker.py`.
- [X] T010 Implement `DiscordGatewayClient.connect()`: GET /gateway/bot, open WSS, await HELLO, capture `heartbeat_interval`.
- [X] T011 Implement `_heartbeat_loop()` — fires op 1 at `heartbeat_interval * 0.7` ms with initial jitter; missed-ACK triggers WSS close 4000.
- [X] T012 Implement IDENTIFY (op 2) with intent bitmask 33281 (GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT).
- [X] T013 Implement READY DISPATCH handler — capture `session_id` and `resume_gateway_url`.
- [X] T014 Implement close-code classifier (`classify_close_code`) returning `ReconnectAction.RESUME` / `IDENTIFY` / `FATAL_EXIT`.
- [X] T015 Implement RESUME (op 6) — reopen to `resume_gateway_url`, send session_id + last_seq.
- [X] T016 Implement INVALID_SESSION (op 9) handler — d=true → resume; d=false → full reconnect.
- [X] T017 Implement fatal-close path — raise `FatalGatewayError` from client; worker exits 1.
- [X] T018 Implement connect-failure backoff in `worker.py::_next_backoff` (1→2→4→8→16→32 cap 60s, ±25% jitter).

## Phase 3 — User Story 1 (Ingest Contract, P1)

**Goal**: MESSAGE_CREATE → observation row committed within 5 s.

- [X] T019 [US1] Add `handle_discord_message` to `services/ingestion/handlers/discord.py` registered as `@register("discord:message")`; reuses ObservationDraft shape from IN-09 handler.
- [X] T020 [US1] Add `"discord:message": "attested_agent"` to `services/ingestion/handlers/__init__.py::CHANNEL_TRUST_MAP`.
- [X] T021 [US1] Implement `handle_message_create(message, deps)` in `gateway/dispatch.py` per `contracts/module-gateway-dispatch.md`.
- [X] T022 [US1] Wire `MESSAGE_CREATE` dispatch in `handle_dispatch` to call `handle_message_create`.
- [X] T023 [US1] Create `gateway/tests/__init__.py` + `gateway/tests/conftest.py` with `FakeGateway`, `dispatch_deps`, `seeded_tenant`, `make_message_create` fixtures.
- [X] T024 [US1] `test_dispatch_message_create.py::test_message_create_lands_as_observation` — happy path, asserts all observation columns.
- [X] T025 [US1] `test_duplicate_message_id_is_idempotent` — two calls, same message_id → one row + dedup metric.
- [X] T026 [US1] `test_content_text_verbatim` — markdown/emoji preserved byte-for-byte.
- [X] T027 [US1] `test_attachment_only_message_ingests_with_empty_content` — Clarifications Q3.
- [X] T027b [US1] `test_mentions_and_channel_in_metadata` (bonus) — covers US1 acceptance scenario 3.

## Phase 4 — User Story 2 (Connection Stability with Reconnect + RESUME, P1)

**Goal**: 24h+ stable connection with transparent reconnect.

- [ ] T028 [US2] Create `services/integrations/discord/gateway/tests/test_client_lifecycle.py::test_hello_identify_ready_loop` — fake gateway responds with HELLO + READY; assert client captures `session_id`, sends heartbeat with correct op + interval, records `discord_gateway_connection_state{state="connected"}=1`.
- [ ] T029 [US2] Add `test_heartbeat_ack_validation` to the same file — fake gateway sends op 11 ACK on schedule for 3 ticks then stops; assert client closes WSS with 4000 after 2 missed ticks and triggers reconnect.
- [ ] T030 [US2] Create `services/integrations/discord/gateway/tests/test_client_reconnect.py::test_close_4000_triggers_resume` — fake gateway sends close 4000 mid-stream; assert client reopens to `resume_gateway_url`, sends RESUME with last_seq, dispatch loop continues without re-IDENTIFY.
- [ ] T031 [P] [US2] Add `test_invalid_session_d_true_resumes` — INVALID_SESSION with `d=true` → RESUME on same session_id.
- [ ] T032 [P] [US2] Add `test_invalid_session_d_false_full_reconnect` — INVALID_SESSION with `d=false` → fresh IDENTIFY with new session.
- [ ] T033 [P] [US2] Add `test_close_4014_exits_fatal` — fake gateway sends close 4014; assert client returns from `run()` and `worker.run_forever()` exits 1 with `discord_gateway_close_fatal` log entry (Clarifications: no degraded mode regardless of FYRALIS_ENV).
- [ ] T034 [US2] Add `test_no_observation_gap_through_resume` — fake gateway dispatches MESSAGE_CREATE A, close 4000, RESUME, dispatch MESSAGE_CREATE B; assert two observations exist, no duplicates.
- [ ] T035 [P] [US2] Add `test_backoff_resets_on_ready` — simulate 3 consecutive connect failures (backoff 1 → 2 → 4 s), then a successful READY; next failure should backoff at 1 s, not 8 s.

## Phase 5 — User Story 3 (author.bot Filter, P2)

**Goal**: Bot/webhook messages never produce observations.

- [ ] T036 [US3] Create `services/integrations/discord/gateway/tests/test_dispatch_filters.py::test_author_bot_self_drops_silently` — payload with `author.bot=true, author.id=APPLICATION_ID` → zero observations, `discord_gateway_filtered_bot_total{source="self"}` increments by 1.
- [ ] T037 [P] [US3] Add `test_author_bot_other_drops_silently` — `author.bot=true, author.id != APPLICATION_ID` → zero obs, `source="other_bot"`.
- [ ] T038 [P] [US3] Add `test_webhook_id_drops_silently` — `webhook_id="123"`, `author.bot=false` → zero obs, `source="webhook"`.
- [ ] T039 [P] [US3] Add `test_filter_runs_before_tenant_resolution` — payload with `author.bot=true` AND no known `provider_installations` row → the bot filter wins; `dropped_unknown_installation_total` does NOT increment (research R7: filter precedence).

## Phase 6 — User Story 4 (Unknown-Guild Silent Drop, P2)

**Goal**: MESSAGE_CREATE from untracked guild drops silently with metric, no raw guild_id in logs.

- [ ] T040 [US4] Add `test_unknown_guild_drops_silently` to `test_dispatch_filters.py` — guild_id with no `provider_installations` row → zero obs, `dropped_unknown_installation_total` increments.
- [ ] T041 [P] [US4] Add `test_disabled_install_treated_as_unknown` — `provider_installations` row with `enabled=FALSE` → same path as no row.
- [ ] T042 [P] [US4] Add `test_no_raw_guild_id_in_logs` — use `caplog` to capture all log records during an unknown-guild dispatch; assert the raw guild_id string does not appear in any record's message (SC-006 invariant).
- [ ] T043 [P] [US4] Add `test_dm_message_drops_silently` — payload with no `guild_id` (DM context) → zero obs, `dispatch_total{event="MESSAGE_CREATE_DM"}` increments.

## Phase 7 — User Story 5 (Operational Hardening, P3)

**Goal**: Metrics observable, SIGTERM clean, no raw guild_id in any worker log path.

- [ ] T044 [US5] Implement SIGTERM handler in `services/integrations/discord/gateway/worker.py` — `signal.signal(SIGTERM, _set_shutdown_flag)` synchronously sets an `asyncio.Event`; main loop checks flag, stops accepting new dispatches, awaits in-flight tasks, sends WSS close 1000, returns from `run_forever()` with exit code 0.
- [ ] T045 [US5] Create `services/integrations/discord/gateway/tests/test_worker_shutdown.py::test_sigterm_drains_and_exits_zero` — start worker with fake gateway, dispatch 3 MESSAGE_CREATE events, send SIGTERM mid-stream; assert worker exits 0 within 5 s AND all 3 observations are committed.
- [ ] T046 [P] [US5] Add `test_sigterm_caps_at_5_seconds` — inject a slow ingestion handler (await sleep 10 s); send SIGTERM; assert worker exits 0 at the 5 s cap and logs the in-flight count of abandoned tasks.
- [ ] T047 [US5] Update `scripts/start.sh` (Phase 4 T042 in plan) to launch the new worker alongside `run_think_worker.py` and `run_post_commit_worker.py`; logfile `/tmp/fyralis_logs/discord_gateway_worker.log`; PID recorded in `/tmp/fyralis_stack.pids`.

## Phase 8 — Polish & Regression

- [ ] T048 [P] Append §16 to `CODEBASE-ARCHITECTURE.md` documenting the new `services/integrations/discord/gateway/` subpackage, the worker process, and the `discord:message` source_channel.
- [ ] T049 [P] Run `pytest services/integrations/discord/gateway/tests/` — all new tests pass with real Postgres + real Ollama.
- [ ] T050 [P] Run `pytest services/integrations/tests/ services/webhooks/tests/` — IN-08 + IN-09 suites pass byte-for-byte (SC-010 — no regressions).
- [ ] T051 [P] Run `python scripts/check_schema_drift.py` — exit 0, zero new files in `db/migrations/` (SC-009).
- [ ] T052 [P] Run `ruff check` on all changed paths under `services/integrations/discord/gateway/`, `services/ingestion/handlers/`, `scripts/`.
- [ ] T053 [P] Run `git diff --stat integration/ingestion-hardening…HEAD` and verify the file inventory matches the "Files relevant" list in source.md (SC-010 hard check).

---

## Dependencies

```
Phase 1 (Setup)
    ↓
Phase 2 (Foundational: scaffolding + lifecycle + reconnect)   ←── blocks every user story
    ↓
Phase 3 (US1 ingest) ─────┐
Phase 4 (US2 stability) ──┤  Each independently testable once Phase 2 lands.
Phase 5 (US3 filter) ─────┤  [P] tasks within a story can run in parallel.
Phase 6 (US4 unknown) ────┤
Phase 7 (US5 ops) ────────┘
    ↓
Phase 8 (Polish)
```

## Parallel-execution examples

Within Phase 3 (US1), tasks T025, T026, T027 are all `[P]` because they each create independent test functions in the same file (different test names = no merge conflict on the file's table of contents). The test functions touch independent rows in `observations` (different `external_id` values), so no DB-state conflict.

Within Phase 8 (Polish), every task is `[P]` — they're independent verification commands or doc-only edits.

## Implementation strategy

**MVP** = Phase 1 + Phase 2 + Phase 3 (US1). A worker that connects, identifies, and ingests one MESSAGE_CREATE end-to-end. Everything beyond is operational quality.

**Incremental delivery**:
1. MVP (Phases 1-3) — green pytest on the happy path.
2. Reconnect resilience (Phase 4) — green pytest on RESUME + INVALID_SESSION paths.
3. Correctness filters (Phases 5-6) — author.bot + unknown-guild paths.
4. Operability (Phase 7) — SIGTERM, metrics, scripts/start.sh integration.
5. Regression sweep (Phase 8) — no IN-08/IN-09 drift, no schema drift, clean ruff.

Each step is a runnable slice. If you stop at step 1, you have a working ingest path missing only stability and observability.
