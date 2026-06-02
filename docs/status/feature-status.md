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

## 🔴 Background worker fabric (Wave-4) — built, not deployed

The single largest theme. Only `topology_sweeper` has a launcher; every other
`services/workers/*` package is absent from compose and `scripts/`. See
[Workers](../architecture/workers.md).

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| Worker deployment | 8 worker packages run as compose/cron processes | Only `topology_sweeper` launched (dogfood/`start.sh`); 7 others have no launcher | high |
| Activation decay + archival | `hourly_decay`/`archive_decayed` run via maintenance worker | `decay.py` implemented, wrapped by `maintenance/daily.py` — but maintenance has no launcher → **Models never decay/auto-archive**; activation stays inflated | high |
| Maintenance scheduler (Wave-4-D) | In-proc scheduler runs daily/weekly/monthly upkeep | `MaintenanceScheduler` never instantiated outside tests → decay, partition-extend, calibration refresh all dormant | high |
| Anomaly → `T3` generation | `anomaly_processor` detects 6 kinds, debounces, enqueues `T3` | Fully implemented, not-wired → **no `T3` anomaly triggers in prod**; `signal_memory_fabric` never accumulates | high |
| Deadline → `T2` generation | `deadline_resolver` polls overdue predictions → `T2` | Implemented, not-wired → predictions past `evaluate_at` **never auto-resolved** | high |
| Deferred entity resolution | `entity_resolver` resolves `_unresolved_phrases` → aliases + `T1` re-enqueue | Implemented, not-wired → unresolved phrases never aliased/re-triggered; `entity_review_queue` has no consumer | high |
| Calibration pipeline | `calibration_updater` refreshes `calibration_offsets` weekly | Live read path consumes offsets, but the **writer worker never runs** → offsets never refreshed; only cold-start defaults apply | high |
| Precipitation pattern formation | Nightly clustering writes `pattern_candidates` + `T4` | Think's *promotion* half is live (`T4 pattern_review`); the *production* half (clustering/write) is not-wired → **`T4` starved of inputs** | high |
| `edge_drift` parity check | Worker samples `model_edges` vs. legacy arrays to catch divergence | Not-wired → the guard for the dual-write migration never runs (silent divergence undetected) | medium |
| `entity_aliases` slow path | `insert_alias`/`record_usage`/`list_ambiguous` driven by `entity_resolver` | Live ingest uses only `fast_path_resolve`; slow-path funcs consumed only by the unwired worker | medium |
| `actor_visible_*` matview refresh | Daily `refresh_all` keeps scope views current; `checks.py` reads them | Sole caller is the undeployed `maintenance/daily.py` → matviews **never refreshed** at runtime (stale/empty scope data) | high |

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
| Post-commit side-effects | Dispatch anomaly handoff / prediction scheduling / realtime broadcast / metric invalidation | Queue + `post_commit_worker` are live, but all 4 dispatchers are **no-op loggers** ("left for a later integration PR") | high |
| Execution signal routing (`decide_route`) | Classify every signal; persist shadow decisions (`EXECUTION_ROUTING_SHADOW=1`) | `decide_route`/`record_routing_decision` have **no caller outside tests**; ingestion never builds a `SignalEnvelope` → **no `signal_routing_decisions` rows ever** | high |

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

See [Ingest](../architecture/ingest.md).

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| Code-graph reachability | Blast-radius/code-search return real dependents per GitHub signal | Only the **read** path is live (via `github_intel`); the index/embed/reindex **write** path has no production caller → `code_snapshots` never populated → results effectively `indexed:false` | high |
| Self-update / reindex loop | Default-branch advances trigger reindex keeping the graph live | Worker writes `code_intel_index_triggers`, but `reindex` runs only when `CODE_INTEL_REINDEX_ROOT` is set — **absent from the worker's compose env** → triggers accumulate unconsumed | high |
| Code-RAG embedding fill | Pending `code_embeddings` get batch-embedded for `/code-search` | Only caller of `fill_pending_embeddings` is the demo script → embeddings stay pending forever; code-search empty outside demo | medium |

## 🟠 Product surfaces & substrate fidelity

See [Product](../architecture/product.md).

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| Query/Ask rendering adapter | `HttpRenderingAdapter` → `/rendering/conversation-turn` | Code default is `MockRenderingAdapter` (stub HTML, no voice rules) unless `QUERY_RENDERING_BASE_URL` set → Ask silently degrades to mock if env missing | high |
| Query/Ask cache adapter | `PostgresCacheAdapter` persisting to `view_ceo_cache` | Code default is process-local `InMemoryCacheAdapter` unless `QUERY_CACHE_BACKEND=pg` → cache lost on restart, not shared across workers | medium |
| Confidence calibration (insert path) | `apply_calibration` reads empirical `calibration_offsets` (n≥20) | Active in insert/validate, but offsets table never populated (writer worker undeployed) → **permanently cold-start**; `apply_calibration_sync` is dead | medium |
| `model_edges` ↔ legacy arrays | Stage-2/3 cutover drops `supporting_model_ids`/`contributing_models` | Still permanent dual-write (both cycle-checks + cascades run); the `edge_drift` parity guard is itself unwired | medium |
| CEO Map (`/api/map/*`) | Map populated from live neighborhood/topology data | Reads `model_neighborhoods`/`topology_events` (compat-only) + UMAP cache populated only by `topology_sweeper` → empty/degraded on tenants without sweeper output | medium |
| Spec routes (`/v1/spec/*`, ledger) | Derived from substrate (models/decisions/predictions) | Returns in-code seed payloads mirroring the UI mocks → fixture-backed, tenant-agnostic | medium |

## 🟢 Low-severity findings (64)

Mostly polish, cosmetics, and small inconsistencies. Notable clusters:

- **Dead/duplicate code:** a duplicate `@app.get('/v1/history')` handler (the second
  is shadowed/unreachable); the `falsifiers/__init__.py` re-export shim has no importers.
- **Stale relocation docstrings:** `today/freshness.py`, `today/stake.py`,
  `today/map.py` still claim to live under `greeting/`; their logic is computed
  inline in the aggregator (or, for the Map, "deliberately suppressed").
- **Orphaned helpers:** `gmail/status_api.py` + `gmail/uninstall.py` (no route),
  `query/prefetch.py` (named caller "Agent-GRT" doesn't exist).
- **Schema/migration tidy-ups:** orphan tables from `0021` (`anomaly_thresholds`,
  `dedup_keys_seen`); `topo_dirty_queue` unused; tables for undeployed workers
  (`entity_review_queue`, `signal_memory_fabric`, `orphan_log`).

These are itemized in [Wiring gaps](wiring-gaps.md) and
[Legacy & test-only](dead-legacy.md).

> **TODO(human):** Confirm which of the high-severity gaps are blocking vs.
> intentionally staged, and whether the duplicate-migration-prefix
> [deploy blocker](index.md) needs the dup-check softened on this branch (as on
> `origin/cannonical`) or the migrations renumbered.
