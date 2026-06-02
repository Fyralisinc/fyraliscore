# Feature Status — Expected vs. Actual

Per-feature status from the audit: what the code was clearly built to do
(`expected`), what actually runs today (`status`), and the `gap`. Organized by
theme (see the [overview](index.md)). Severity reflects impact if the gap is
unintended. **131 findings total: 21 high (1 resolved — cutover circuit
breaker), 46 medium, 64 low** — the high/medium
are below; low findings are summarized at the end.

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
| Kafka full-pipeline persistence | `observation_writer` writes observations from the normalized lane | Only writes when tenant `KAFKA_PATH_ENABLED=TRUE` (default off) → full pipeline is shadow/no-op until per-tenant cutover; inline ingest is the live path | medium |
| Cutover circuit breaker | Long-running breaker trips `kafka_path_enabled=FALSE` on sustained lag | ✅ **Wired + source-aware** (`fix/cutover-breaker-source-aware`): runs as the `circuit_breaker` singleton in compose; measures lag across every `ingestion.raw.<source>` lane (group `normalizer.<source>`) and trips a tenant on its worst lane — the legacy single-`ingestion.raw` inertness is gone. Now has `/healthz`+`/metrics`, per-lane failure isolation, an operator re-enable tool (`scripts/reenable_kafka_path.py`), and a live-broker smoke test (`scripts/smoke_circuit_breaker_lag.py`) that verified the real readers + caught a latent `confluent_kafka` import crash | resolved |
| `google_drive` async embedding | All production sources publish to `ingestion.embedding` on `embedding_pending` | `google_drive` **missing from the allowlist** (`core.py`) → its pending embeddings rely only on the DB-scan backlog drainer (latency, not correctness) | medium |
| Onboarding progress events | Workflow services emit `onboarding.progress` (shard.fetched, source.complete, …) | Publisher + 7 event models built, but the sole producer (`feels_onboarded_monitor`) isn't in compose → most event kinds have **no producer call site** | medium |
| `feels_onboarded_monitor` service | Runs as a long-running `WORKFLOW_SERVICE` | No own `__main__`, no compose service → never booted; `feels_onboarded` events never emitted | medium |
| Per-(source,method) API rate limiter | `shard_fetch`/`FetchPage` acquires tokens before each upstream call | `RateLimiter` wired only into the embedding backlog (Ollama); `FetchPage` never calls `.acquire()`; `BUCKET_DEFAULTS` has zero non-test importers | medium |
| Webhook verification metrics | Prometheus-scrapable `{provider,reason}` counters | In-process dict only; no exporter → ops dashboards have no scrape path | medium |

## 🟠 Integrations — per-source install/live coverage

See [Ingest](../architecture/ingest.md). Production source families: slack, github,
discord, gmail, notion, google_calendar, google_drive, jira, mercury, quickbooks.

| Feature | Expected | Current status | Severity |
|---------|----------|----------------|:--------:|
| Google Calendar install | Gateway connect endpoint (like Gmail OAuth) emitting onboarding trigger | `onboarding.connect/finalize` reachable only via `scripts/sandbox_google_calendar.py` → **no gateway router**, install is sandbox-only | high |
| Google Drive install | Gateway connect endpoint | Same — **no gateway router**, sandbox-only | high |
| Jira install | A runtime caller that finalizes install + writes `onboarding_triggers` | `finalize_install` invoked only by sandbox scripts/tests → **no production install surface** (unlike Mercury/QBO via `finance_router`) | high |
| Mercury/QuickBooks install | Production credential-collection install flow | Reachable only through the dev `finance_router` panel (synthetic data, tenant-from-header) → **no genuine prod install flow** | medium |
| Gmail Pub/Sub ingress | Webhook ingress for the Gmail source | Mounts **only if** `GMAIL_SERVICE_ACCOUNT_JSON` is set; otherwise silently skipped (warning log, not a startup error) | medium |
| Calendar/Drive live workers | Push/watch workers like `gmail_watch`+`gmail_history` | Calendar/Drive are backfill+reconcile only → no near-real-time live ingestion | medium |
| Discord HA single-instance lease | Worker acquires a Redis lease before connecting (no double-delivery) | `leader_lock`/`lifecycle` built+tested, but `run_discord_gateway_worker.py` constructs the worker directly → **2 replicas would double-deliver** | high |
| Discord crash-RESUME | Session state persisted (`gateway_session_state`) for restart RESUME | `load/save` + table exist, but launcher passes no `on_dispatched` → state never persisted → restart always re-IDENTIFYs, dropping buffered frames | high |

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
  is shadowed/unreachable); orphaned `__main__.py` shims in `dlq_writer`/
  `embedding_worker`/`feature_flags` (compose targets the inner module, not the
  package); the `falsifiers/__init__.py` re-export shim has no importers.
- **Stale relocation docstrings:** `today/freshness.py`, `today/stake.py`,
  `today/map.py` still claim to live under `greeting/`; their logic is computed
  inline in the aggregator (or, for the Map, "deliberately suppressed").
- **Orphaned helpers:** `gmail/status_api.py` + `gmail/uninstall.py` (no route),
  `query/prefetch.py` (named caller "Agent-GRT" doesn't exist), `rate_limit/
  buckets.py` (`BUCKET_DEFAULTS` unused).
- **Schema/migration tidy-ups:** orphan tables from `0021` (`anomaly_thresholds`,
  `dedup_keys_seen`); `topo_dirty_queue` unused; tables for undeployed workers
  (`entity_review_queue`, `signal_memory_fabric`, `orphan_log`).

These are itemized in [Wiring gaps](wiring-gaps.md) and
[Legacy & test-only](dead-legacy.md).

> **TODO(human):** Confirm which of the high-severity gaps are blocking vs.
> intentionally staged, and whether the duplicate-migration-prefix
> [deploy blocker](index.md) needs the dup-check softened on this branch (as on
> `origin/cannonical`) or the migrations renumbered.
