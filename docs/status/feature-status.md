# Feature Status — Expected vs. Actual

Per-feature status from the audit: what the code was clearly built to do
(`expected`), what actually runs today (`status`), and the `gap`. Organized by
theme (see the [overview](index.md)). Severity reflects impact if the gap is
unintended. **131 findings total: 21 high (6 resolved — cutover circuit
breaker, Google Calendar install, Google Drive install, Jira install, the
Discord single-instance lease, and Discord crash-RESUME), 46 medium (9 resolved — Kafka full-pipeline persistence,
`google_drive` async embedding, onboarding progress events, the
`feels_onboarded_monitor` service, the per-(source,method) API rate limiter,
webhook verification metrics, the Mercury/QuickBooks production install
flow, the Gmail Pub/Sub ingress mount, and the Calendar/Drive live
workers), 64 low** — the high/medium are below; low
findings are summarized at the end. _Resolutions 2026-06-02; see
[ADR-0001](../adr/0001-kafka-first-ingestion-default.md)._

## 🟠 Background worker fabric (Wave-4) — partially wired

The single largest theme. `housekeeper_worker` now runs low-frequency lifecycle
jobs that previously only existed in tests, while `anomaly_processor_worker` and
`entity_resolver_worker` cover the highest-impact LLM/reasoning seams. Several
expensive or non-selected jobs still need deliberate deployment decisions. See
[Workers](../architecture/workers.md).

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| Worker deployment | 8 worker packages run as compose/cron processes | **Partially resolved (2026-06-24).** `housekeeper_worker`, `anomaly_processor_worker`, and `entity_resolver_worker` are now first-class production processes with healthchecks, Prometheus scrape targets, and runtime-manifest entries. Expensive/non-selected jobs such as precipitation still need an explicit launch decision. | medium |
| Activation decay + archival | `hourly_decay`/`archive_decayed` run via maintenance worker | ✅ **Resolved (2026-06-12).** Housekeeper schedules `hourly_decay` and `archive_decayed` through the existing `MaintenanceScheduler`, so Models can decay/archive outside tests. | ✅ resolved |
| Maintenance scheduler (Wave-4-D) | In-proc scheduler runs daily/weekly/monthly upkeep | ✅ **Resolved for default lifecycle jobs (2026-06-12).** `housekeeper_worker` instantiates `MaintenanceScheduler` for deadline resolution, obligations, decay/archive, relationship maintenance, calibration, and edge drift. Monthly/expensive jobs remain separately flagged. | ✅ resolved |
| Anomaly → `T3` generation | `anomaly_processor` detects 6 kinds, debounces, enqueues `T3` | ✅ **Resolved (2026-06-24).** `anomaly_processor_worker` is now production-wired and exports cycle/counter metrics. | ✅ resolved |
| Deadline → `T2` generation | `deadline_resolver` polls overdue predictions → `T2` | ✅ **Resolved (2026-06-12).** Housekeeper runs `DeadlineResolver.run_once()` on `HOUSEKEEPER_DEADLINE_RESOLVER_INTERVAL_S`. | ✅ resolved |
| Deferred entity resolution | `entity_resolver` resolves `_unresolved_phrases` → aliases + `T1` re-enqueue | ✅ **Resolved (2026-06-24).** `entity_resolver_worker` is now production-wired with bounded polling, LLM budget controls, health/metrics, and terminal phrase cleanup so completed aliases do not burn repeated LLM calls. | ✅ resolved |
| Calibration pipeline | `calibration_updater` refreshes `calibration_offsets` weekly | ✅ **Resolved (2026-06-12).** Housekeeper runs `calibration_updater.run_once()` weekly by default, so offsets are no longer test/manual-only. | ✅ resolved |
| Precipitation pattern formation | Nightly clustering writes weak `pattern_candidates` + `T4` review triggers | Partially wired: housekeeper can run precipitation, but it is disabled by default behind `HOUSEKEEPER_ENABLE_PRECIPITATION` / `HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS`. `pattern_review` is no longer deterministic promotion; semantic Think review must justify any Pattern Model. Broad enablement now has an explicit quality gate and remains blocked until representative shadow evidence reaches `enablement_candidate`. | medium |
| `edge_drift` parity check | Worker samples `model_edges` vs. legacy arrays to catch divergence | ✅ **Resolved (2026-06-12).** Housekeeper runs `edge_drift.run_once()` on `HOUSEKEEPER_EDGE_DRIFT_INTERVAL_S`. | ✅ resolved |
| `entity_aliases` slow path | `insert_alias`/`record_usage`/`list_ambiguous` driven by `entity_resolver` | ✅ **Resolved (2026-06-24).** The production `entity_resolver_worker` drives alias insert/usage, `entity_review_queue`, clarification creation, and material `T1` re-enqueue. | ✅ resolved |
| `actor_visible_*` matview refresh | Daily `refresh_all` keeps scope views current; `checks.py` reads them | ✅ **Resolved (2026-06-24).** Housekeeper now schedules `access_matview_refresh` daily through the production housekeeper process, reusing the existing `maintenance.daily.access_matview_refresh` implementation. | ✅ resolved |

## 🔴 Access control & authorization

See [Platform](../architecture/platform.md).

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| `@requires_access` route decorator | Routes declaratively authorized + audited | Applied on **zero** routes; gateway does one inline `can_read_by_id` (on `/dashboard/customer`), comment notes "decorator isn't applied here" → most entity routes have **no `can_read` enforcement** | high |
| `access_override_log` audit | Every admin/first-person override appends an audit row | `record_override` only called from the never-applied decorator → live `can_read` paths set `override_applied=True` but **write no audit rows** | medium |
| Role management (grant/revoke/list) | Runtime path to administer `actor_roles` that drive `can_read` | All three funcs invoked only by tests → `actor_roles` populated only via tests/manual SQL; **no production path to grant roles** | medium |

## 🔴 Reasoning & execution seams

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| Anomaly processor | Six detectors score anomaly candidates, write `signal_memory_fabric`, and enqueue T3 triggers | ✅ **Resolved (2026-06-24).** `anomaly_processor_worker` is now in the production runtime manifest, docker compose, Prometheus scrape config, and exposes health/metrics via `scripts/run_anomaly_processor_worker.py`. | ✅ resolved |
| Post-commit side-effects | Dispatch anomaly handoff / prediction scheduling / realtime broadcast / metric invalidation | Queue + `post_commit_worker` is live. Realtime broadcast, metric invalidation, anomaly-published, and prediction-scheduled actions now emit transactional `view_ceo_refresh` NOTIFYs consumed by the CEO-view scheduler; `schedule_predictions` also rejects payloads missing `evaluate_at`. | ✅ resolved |
| Execution signal routing (`decide_route`) | Classify every signal | `decide_route` has **no caller outside tests**; ingestion never builds a `SignalEnvelope`, and the old `signal_routing_decisions` ledger was dropped by migration `0127` | high |

## 🟠 Ingestion pipeline & data plane

See [Ingest](../architecture/ingest.md).

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| Kafka full-pipeline persistence | `observation_writer` writes observations from the normalized lane | **✅ Resolved (2026-06-02).** Default inverted to kafka-first: a tenant with no flag row takes the full pipeline and the writer persists from the normalized lane by default. `KAFKA_PATH_ENABLED=FALSE` is now an operator / circuit-breaker **kill-switch** (inline fallback), read through one shared helper so ingress + writer can't drift. See [ADR-0001](../adr/0001-kafka-first-ingestion-default.md). | ✅ resolved |
| Cutover circuit breaker | Long-running breaker trips `kafka_path_enabled=FALSE` on sustained lag | ✅ **Wired + source-aware** (`fix/cutover-breaker-source-aware`): runs as the `circuit_breaker` singleton in compose; measures lag across every `ingestion.raw.<source>` lane (group `normalizer.<source>`) and trips a tenant on its worst lane — the legacy single-`ingestion.raw` inertness is gone. Now has `/healthz`+`/metrics`, per-lane failure isolation, an operator re-enable tool (`scripts/reenable_kafka_path.py`), and a live-broker smoke test (`scripts/smoke_circuit_breaker_lag.py`) that verified the real readers + caught a latent `confluent_kafka` import crash | ✅ resolved |
| `google_drive` async embedding | All production sources publish to `ingestion.embedding` on `embedding_pending` | **✅ Resolved (2026-06-02).** `core.py` now derives the embedding allowlist from `INGESTION_SOURCES` (= `RawEnvelope.SourceLiteral`), which includes `google_drive`, so it publishes to `ingestion.embedding.google_drive` on `embedding_pending` like every other source. `shadow_write_raw`'s `source` type was aligned to the same literal to kill the drift class. | ✅ resolved |
| Onboarding progress events | Workflow services emit `onboarding.progress` (shard.fetched, source.complete, …) | **✅ Resolved (2026-06-02).** All 7 event kinds now have a producer call site: `tenant.onboarding.started`/`…complete` from `tenant_onboarding`, `source.onboarding.started` from `source_onboarding`, `shard.fetched` from `shard_fetch`, `source.onboarding.complete` from the `reconciler` clean pass (with `coverage_confidence`/`gaps_resolved` derived from the reconciliation outcome), and `feels_onboarded` + the ops-only `behind_schedule` from `feels_onboarded_monitor`. Each orchestrator takes an optional `kafka_producer` (wired in its `_run_*` entrypoint) and publishes **post-commit** via the shared `publish_progress_events` helper — claim-via-UPDATE ordering, so events fire once per transition with Bridge-side dedup. | ✅ resolved |
| `feels_onboarded_monitor` service | Runs as a long-running `WORKFLOW_SERVICE` | **✅ Resolved (2026-06-02).** Given a per-module `__main__` (sibling to `oauth_poller`/`shard_fetch`/…) + a `feels_onboarded_monitor` compose service (`python -m …workflows.feels_onboarded_monitor`), so it boots and emits `feels_onboarded`. Now also fires the ops-only `behind_schedule` (migration `0080` adds a `behind_schedule_emitted_at` claim slot to `onboarding_runs`) and derives its source allowlist from the `Source` literal — fixing a latent drift that had dropped `google_drive`. The legacy `WORKFLOW_SERVICE` selector still works (kept for the subprocess test). | ✅ resolved |
| Per-(source,method) API rate limiter | `shard_fetch`/`FetchPage` acquires tokens before each upstream call | **✅ Resolved (2026-06-02).** New `rate_limit.FetchRateLimiter` gate is the first non-test importer of `BUCKET_DEFAULTS`: `ShardFetch._run_fetch_loop` now calls `.acquire(source, tenant_id)` **before** each fetcher/page call, consuming one token from the `rate:<tenant>:<source>:<method>` Lua bucket. Throttles the four sources with published budgets (slack/github/gmail/discord via `PRIMARY_FETCH_METHOD`); unbudgeted sources are explicit pass-throughs (no fabricated limits). A bounded wait that's exceeded raises `RateLimitWaitExceeded`, which the loop treats as a transient exit (shard stays `in_progress`, orphan-scan resumes) — same shape as a flush failure. Enabled when `REDIS_URL` is set (compose app-env); `SHARD_FETCH_RATE_LIMIT=0` opts out. | ✅ resolved |
| Webhook verification metrics | Prometheus-scrapable `{provider,reason}` counters | **✅ Resolved (2026-06-02).** `webhooks.metrics.render_prometheus()` renders all in-process counter families as Prometheus text exposition (0.0.4) and the gateway serves them at a public `GET /metrics` (allowlisted in `_PUBLIC_PATHS`, no Bearer). Families: `webhook_verification_failures_total{provider,reason}`, `webhook_resolver_outcomes_total`, `webhook_resolver_cache_total`, `webhook_router_kafka_path_total`, and the `webhook_resolver_duration_p95_seconds` gauge. Hand-rolled text — no `prometheus_client` dependency (mirrors `ingestion/observability.py`), so the simplicity principle holds. | ✅ resolved |

## 🟠 Integrations — per-source install/live coverage

See [Ingest](../architecture/ingest.md). Production source families: slack, github,
discord, gmail, notion, google_calendar, google_drive, jira, mercury, quickbooks.

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| Google Calendar install | Gateway connect endpoint (like Gmail OAuth) emitting onboarding trigger | **✅ Resolved (2026-06-02).** New `google_calendar/oauth.py` exposes `POST /integrations/google_calendar/connect/{preflight,finalize}` — a DWD connect wizard mirroring `gmail/oauth.py`. `preflight` enumerates the domain via the shared `DirectoryClient` (or returns the client_id + scopes to grant when the DWD grant is missing); `finalize` resolves the inclusion_spec and calls the existing `connect()`/`finalize_install()` to write the install + per-calendar rows + the `onboarding_triggers` row (source='google_calendar') in one transaction. Mounted in the gateway under the same `GMAIL_SERVICE_ACCOUNT_JSON*` DWD gate (Calendar reuses Gmail's service account); poll-only, so no Pub/Sub provisioning. Install is no longer sandbox-only. | ✅ resolved |
| Google Drive install | Gateway connect endpoint | **✅ Resolved (2026-06-02).** New `google_drive/oauth.py` exposes `POST /integrations/google_drive/connect/{preflight,finalize}` — a DWD connect wizard mirroring `google_calendar/oauth.py`. `finalize` resolves the inclusion_spec → per-user My-Drive targets and (when `include_shared_drives`) enumerates the org's Shared Drives, then calls the existing `connect()`/`finalize_install()` to write the install + per-target rows + the `onboarding_triggers` row (source='google_drive') in one transaction. Mounted in the gateway under the same `GMAIL_SERVICE_ACCOUNT_JSON*` DWD gate; poll-only (changes-API delta), so no async provisioning. Install is no longer sandbox-only. | ✅ resolved |
| Jira install | A runtime caller that finalizes install + writes `onboarding_triggers` | **✅ Resolved (2026-06-02).** New `jira/oauth.py` exposes `POST /integrations/jira/connect/{preflight,finalize}` — a **Bearer-authed** admin connect wizard (a genuine prod surface, unlike the dev `finance_router` panel). `preflight` verifies the API token via `JiraClient.myself()` and enumerates projects for the selector; `finalize` re-verifies creds **before any write**, stores the API token + optional webhook HMAC secret encrypted via the gateway `secret_store` (only opaque refs reach the DB), then calls `finalize_install()` (jira_installations + jira_projects + `onboarding_triggers` source='jira') and `register_webhook_installation()` (the `provider_installations` row the webhook edge resolves the tenant + signing secret from). Tenant comes from `request.state.auth`, not an `X-Tenant-Id` header. | ✅ resolved |
| Mercury/QuickBooks install | Production credential-collection install flow | **✅ Resolved (2026-06-02).** New `mercury/oauth.py` + `quickbooks/oauth.py` expose `POST /integrations/{mercury,quickbooks}/connect/{preflight,finalize}` — **Bearer-authed** credential wizards (genuine prod surfaces, distinct from the dev `finance_router` panel). Mercury submits a Bearer API token (verified + accounts enumerated via `list_accounts()`); QuickBooks submits `realm_id` + access/refresh tokens (verified via `company_info()` — the repo has no Intuit OAuth-bounce, so install is operator-mediated and `oauth_poller` owns refresh thereafter). Both verify creds **before any write**, store credentials encrypted via the gateway `secret_store` (only opaque refs reach the DB), then call `finalize_install()` (dedicated tables + `onboarding_triggers`) and — when an org id / verifier token + secret are supplied — `register_webhook_installation()`. Tenant from `request.state.auth`, not an `X-Tenant-Id` header. | ✅ resolved |
| Gmail Pub/Sub ingress | Webhook ingress for the Gmail source | **✅ Resolved (2026-06-02).** The `/webhooks/gmail/pubsub` ingress now mounts **unconditionally** (decoupled from the DWD service-account gate), so Google's pushes always hit a real endpoint instead of a silent 404. Readiness is explicit: when the OIDC env (`GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE` + `GMAIL_PUBSUB_PUSH_OIDC_SA`) is absent the route returns `503 not_configured` (was a `RuntimeError` → 500) and boot logs `gmail_pubsub_ingress_mounted_unconfigured` instead of skipping silently. The connect wizard stays gated on the SA JSON it genuinely needs. | ✅ resolved |
| Calendar/Drive live workers | Push/watch workers like `gmail_watch`+`gmail_history` | **✅ Resolved (2026-06-02).** Both halves of Gmail's live path now exist for Calendar + Drive. **Poller** (`gmail_history` analog): `google_{calendar,drive}_live_poller` lease active, cursor-seeded resources (new `last_live_poll_at` claim slot, migration 0082) and drain the delta on a short cadence via the shared `drain_live` → existing fetcher + `ingest()` (dedups at `observations.UNIQUE`). **Native push** (`gmail_watch` analog): `events.watch`/`changes.watch` client methods + a `watch_scheduler` register/renew the channel (state on the per-resource rows, migration 0083), and an always-mounted `/webhooks/google_{calendar,drive}/push` ingress constant-time-verifies the `X-Goog-Channel-Token` and drains via the same path. Channels register only when `GOOGLE_PUSH_WEBHOOK_BASE` is set (they need a domain-verified HTTPS endpoint); the poller is the liveness guarantee regardless. Five compose services + launchers added. | ✅ resolved |
| Discord HA single-instance lease | Worker acquires a Redis lease before connecting (no double-delivery) | **✅ Resolved (2026-06-02).** `run_discord_gateway_worker.py` now composes the M4.1 lease via `lifecycle.py`: it connects Redis, acquires the `gateway:discord:leader_lock` lease through `acquire_lease_with_backoff` **before** the worker connects, and runs `lease_refresh_loop` (30s TTL / 10s refresh) alongside the WS loop — on refresh failure (another pod took over) it requests a graceful worker shutdown instead of fighting for the surface. Redis is **mandatory**: a missing `REDIS_URL` fails loud (exit 2) rather than silently dropping the only double-delivery guard. Lease-acquire timeout / mid-run loss exit `3` (transient → orchestrator restarts to stand by). New `test_launcher_wiring.py` pins the composition. | ✅ resolved |
| Discord crash-RESUME | Session state persisted (`gateway_session_state`) for restart RESUME | **✅ Resolved (2026-06-02).** The launcher now loads the persisted `gateway_session_state` on startup (`load_session_state` → `persisted_to_in_memory`) and hands the worker both `initial_state` (so a restart RESUMEs and Discord replays the buffered frames instead of re-IDENTIFYing) and an `on_dispatched` save hook (`make_save_hook`) that persists the session cursor after every dispatched frame. Keyed by `DISCORD_CLIENT_ID`; when it's unset the lease still guards double-delivery but RESUME is disabled with a visible warning. The in-process RESUME test (`test_session_resume_after_planned_restart`) + the new launcher-wiring test both pass. | ✅ resolved |

## 🟠 `code_intel`

**Extracted to a separate repo (`Fyralisinc/github-intel`)** as the first step of the
interface-platform plan; `code_intel` + `github_intel` are no longer part of core. The
prior readiness gaps (reindex write-path never wired, embedding fill demo-only) travel
with it and resolve when it returns as the first external interface. See
[Interfaces & Extensions](../architecture/interfaces.md) and ADR-0004.

## 🟠 Product surfaces & substrate fidelity

See [Product](../architecture/product.md).

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| Query/Ask rendering adapter | `HttpRenderingAdapter` → `/rendering/conversation-turn` | ✅ **Resolved (2026-06-12).** `build_rendering_adapter()` still allows the deterministic `MockRenderingAdapter` in dev/test, but now fails closed in production (`FYRALIS_ENV` or `COMPANY_OS_ENV` = prod) when `QUERY_RENDERING_BASE_URL` is unset. `.env.production.example` now sets `QUERY_RENDERING_BASE_URL=http://gateway:8000`, matching the existing greeting renderer guard. | ✅ resolved |
| Query/Ask cache adapter | `PostgresCacheAdapter` persisting to `view_ceo_cache` | ✅ **Resolved (2026-06-12).** Dev/test may still use the process-local `InMemoryCacheAdapter`, but production now fails closed unless `QUERY_CACHE_BACKEND=pg` and a DB pool are supplied. `.env.production.example` sets `QUERY_CACHE_BACKEND=pg`, and gateway wiring already calls `build_cache_adapter(pool=pool)`. | ✅ resolved |
| Confidence calibration (insert path) | `apply_calibration` reads empirical `calibration_offsets` (n≥20) | Active in insert/validate, but offsets table never populated (writer worker undeployed) → **permanently cold-start**; `apply_calibration_sync` is dead | medium |
| `model_edges` ↔ legacy arrays | Stage-2/3 cutover drops `supporting_model_ids`/`contributing_models` | Still permanent dual-write (both cycle-checks + cascades run); the `edge_drift` parity guard is itself unwired | medium |
| CEO Map (`/api/map/*`) | Map populated from live neighborhood/topology data or explicit warming state | ✅ **Degrades explicitly (2026-06-24).** Snapshot responses now include `degraded_reasons` (`no_visible_models`, `projection_warming`, `topology_warming`) so clients can render empty/warming topology states intentionally. | ✅ resolved for graceful degradation |
| Spec routes (`/v1/spec/*`, ledger) | Derived from substrate (models/decisions/predictions) or hidden from production | ✅ **Production-hidden (2026-06-24).** Seed-payload routes remain for dev/e2e only, are isolated to `spec_routes.py`, and are unmounted in production through `SPEC_DEMO_ROUTES_ENABLED=0` with route ratchets. | ✅ resolved for prod exposure |

## 🟢 Low-severity findings (64)

Mostly polish, cosmetics, and small inconsistencies. Notable clusters:

- **Dead/duplicate code:** a duplicate `@app.get('/v1/history')` handler (the second
  is shadowed/unreachable).
- **Orphaned helpers:** `query/prefetch.py` (named caller "Agent-GRT" doesn't exist).
- **Schema/migration tidy-ups:** remaining staged maintenance table
  (`orphan_log`). `entity_review_queue` and `signal_memory_fabric` are now owned
  by deployed workers. Orphan `0021` tables were dropped by `0155`; host-owned
  GitHub/code-intel residue was dropped by `0156`.

These are itemized in [Wiring gaps](wiring-gaps.md) and
[Legacy & test-only](dead-legacy.md).

> **TODO(human):** Confirm which of the high-severity gaps are blocking vs.
> intentionally staged. The duplicate-migration-prefix deploy blocker was
> resolved on 2026-06-03 by renumbering the colliding files and restoring
> hard-fail checks.
