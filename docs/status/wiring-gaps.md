# Wiring Gaps — Built but Not in the Flow

Every backend file classified **`not-wired`** (implemented, but no runtime
launcher or caller reaches it) or **`stub`**, grouped by subsystem, with the
verified reason it isn't in the implementation flow. All rows were
adversarially re-checked (a repo-wide grep for imports, `python -m`, decorator
registries, compose `command:`, env dispatch) before landing here.

!!! note "How to read this"
    "Not in the flow" almost always means **no process launches it**, not that the
    code is wrong. The fix is usually one of: add a compose service / `scripts/run_*`
    launcher, apply a decorator, set an env flag, or add a call site. Items marked
    🗑 have **zero references anywhere** (see [Legacy & test-only](dead-legacy.md) for
    removal guidance).

## Background workers (`services/workers/*`)

The dominant cluster. The highest-impact packages now have production launchers
or run through `housekeeper_worker`; the remaining entries here are kept as the
launch-scope ledger for jobs that are still dogfood-only, flag-gated, or
partially documented. See [Workers](../architecture/workers.md) and the
[feature table](feature-status.md).

| File(s) | What it is | Why not in flow |
|---------|------------|-----------------|
| `anomaly_processor/{__init__,worker,detectors,significance,debounce,memory_fabric}.py` | Wave-4-B: 6 anomaly detectors → significance → debounce → `T3` enqueue + `signal_memory_fabric` writes | ✅ Production-wired (2026-06-24): `anomaly_processor_worker` now has a script launcher, compose service, healthcheck, Prometheus target, and runtime manifest entry. |
| `entity_resolver/{__init__,worker,context}.py` | Deferred LLM resolution of `_unresolved_phrases` → aliases + `T1` re-enqueue | ✅ Production-wired (2026-06-24): `entity_resolver_worker` now has a script launcher, compose service, healthcheck, Prometheus target, runtime manifest entry, LLM budget config, and terminal phrase cleanup. |
| `deadline_resolver/{__init__,worker}.py` | Wave-4-A: poll overdue prediction Models → `T2 prediction_overdue` | ✅ Production-wired through `housekeeper_worker`; `DeadlineResolver.run_once()` runs on `HOUSEKEEPER_DEADLINE_RESOLVER_INTERVAL_S`. (`evaluators.py` remains test-only.) |
| `edge_drift/{__init__,worker}.py` | Samples `model_edges` vs. legacy array cols → drift metrics | ✅ Production-wired through `housekeeper_worker` on `HOUSEKEEPER_EDGE_DRIFT_INTERVAL_S`; the parity guard is no longer test-only. |
| `maintenance/{__init__,daily,weekly,monthly,scheduler}.py` | Wave-4-D: daily decay/archival/cleanup, weekly relationship-maint + calibration + partition-extend, monthly vacuum | ✅ Production-wired through `housekeeper_worker` for default lifecycle jobs. Expensive/monthly jobs remain intentionally flag-gated. |

## Domain substrate

| File | What it is | Why not in flow |
|------|------------|-----------------|
| `services/domain/models/decay.py` | `hourly_decay` + `archive_decayed` activation/decay UPDATEs | ✅ Production-wired through `housekeeper_worker` via the default maintenance scheduler. |

## Platform (access control + execution)

See [Platform](../architecture/platform.md).

| File | What it is | Why not in flow |
|------|------------|-----------------|
| `access_control/middleware.py` | `@requires_access` route decorator (wraps `can_read_by_id` + audit) | Applied on **zero** routes; not even re-exported from the package `__init__`. The gateway does manual inline `can_read_by_id` instead. |
| `access_control/materialized.py` | `actor_visible_*` matview refresh + dirty-queue + point-checks | Refresh is now scheduled by the production `housekeeper_worker` via `access_matview_refresh`; `is_*_visible_to` helper adoption remains optional because `checks.py` reads the matviews directly. |
| `access_control/audit.py` | `record_override` → `access_override_log` writer | Only caller is the never-applied `middleware.py`; the live `can_read` paths (dispatcher, assembler, dashboard) don't call it. |
| `execution/routing.py` | `decide_route` gate + `record_routing_decision` shadow persistence | No non-test caller; ingestion never builds a `SignalEnvelope.from_observation`. Env flags exist only in `.env.example`. Not even shadow-wired. |

## Ingestion pipeline

See [Ingest](../architecture/ingest.md).

| File | What it is | Why not in flow |
|------|------------|-----------------|
| ~~`ingestion/idempotency/__init__.py`~~ | `external_id` constructors (M5 / LLD §6) — one per source dedup key | **✅ Wired (2026-06-02).** The stub now holds the 18 **composed** `external_id` constructors (the namespaced + IN-15-versioned keys: slack, gmail, discord, notion, github-push, grafana ×2, gcal, gdrive ×4, jira ×3, mercury ×2, qbo ×2), and all 11 handlers import `idempotency` and call them — so a source's dedup-key format lives in exactly one place and can't drift between its webhook/backfill/poll paths. Adopted-verbatim keys (Stripe `evt_…`, GitHub `node_id`, RFC-5322 `Message-ID`, Linear ids) stay inline by design (nothing to compose). Byte-for-byte preserved: the per-handler `external_id` assertions + the load-bearing `test_backfill_external_id_parity` stayed green, and a new `idempotency/tests/test_external_ids.py` pins every format. See the [feature table](feature-status.md). |
| ~~`ingestion/rate_limit/buckets.py`~~ | Legacy guessed per-page budgets | **✅ Removed (2026-07-25).** The fixed `BUCKET_DEFAULTS`/`FetchRateLimiter` layer charged logical pages instead of actual provider calls and could sleep while holding a shard. Provider clients now own operation-specific quota requirements through `RedisQuotaCoordinator` + the generic Lua `RateLimiter`; long waits return durable `RetryLater`. |
| ~~`ingestion/workflows/feels_onboarded_monitor.py`~~ | Polls `onboarding_runs`, fires `feels_onboarded` + `behind_schedule` progress events | **✅ Wired (2026-06-02).** Given a per-module `__main__` + a `feels_onboarded_monitor` compose service; the legacy `WORKFLOW_SERVICE` selector is kept for the subprocess test. See the [feature table](feature-status.md). |
| ~~`ingestion/writers/dlq_writer/__main__.py`~~ | CLI entry `python -m …dlq_writer` | **✅ Removed (2026-06-02).** Was a redundant second entrypoint — it only imported `main` from the inner `dlq_writer.dlq_writer` module, which has its own `main()` + `if __name__` block that compose already runs (`python -m …writers.dlq_writer.dlq_writer`, matching the sibling `observation_writer` + `normalizer.worker` convention). Deleted the dead shim; no compose/import change (the package's `__init__`/tests import the module, not `__main__`). |
| ~~`ingestion/writers/embedding_worker/__main__.py`~~ | CLI entry `python -m …embedding_worker` | **✅ Removed (2026-06-02).** Same redundant-shim case as `dlq_writer` above; deleted. (Also fixed `architecture/data-plane.md`, which had documented the now-gone `python -m …writers.embedding_worker` package form instead of the module form compose runs.) |

## Integrations

| File | What it is | Why not in flow |
|------|------------|-----------------|
| ~~`integrations/gmail/status_api.py`~~ | `get_gmail_status`: watch/audit snapshot for a tenant | **✅ Wired (2026-06-02).** `gmail/oauth.py` now mounts `GET /integrations/gmail/status` → `get_gmail_status`, on the already-mounted Gmail connect router (Bearer-authed, tenant from `request.state.auth`). Mirrors the finance (`/{source}/status`) + slack (`/{user}/status`) status endpoints. |
| ~~`integrations/gmail/uninstall.py`~~ | `uninstall_install`/`stop_mailbox`: teardown | **✅ Wired (2026-06-02).** `gmail/oauth.py` now mounts `POST /integrations/gmail/uninstall` → `uninstall_install` (full teardown: stop watches + Pub/Sub teardown + disable install + audit) and `POST /integrations/gmail/mailbox/stop` → `stop_mailbox` (per-mailbox pause). Both idempotent; RLS scopes them to the caller's tenant. |
| ~~`integrations/jira/onboarding.py`~~ | Jira install: upsert installs + `onboarding_trigger` | **✅ Wired (2026-06-02).** This row was stale: `jira/oauth.py` already calls `finalize_install` + `register_webhook_installation` from a real gateway router (`/integrations/jira/connect/{preflight,finalize}`, mounted in `gateway/main.py` as the "Jira production install surface"), exactly like Mercury/QBO. See the [feature table](feature-status.md). |

## Reasoning

| File | What it is | Why not in flow |
|------|------------|-----------------|
| `reasoning/retrieval/maintenance.py` | `background_relationship_maintenance` nightly worker | Imported only by `workers/maintenance/weekly.py`, which has no launcher/compose service. |

## Product surfaces

See [Product](../architecture/product.md). No zero-runtime product-surface
modules remain in this table. `query/prefetch.py` is a dormant helper tracked in
[Legacy & test-only](dead-legacy.md).

## Migrations for undeployed features

These create tables whose reader/writer used to be undeployed. Rows marked
production-wired are no longer runtime-orphans; unresolved rows remain tracked
here until a launch or removal decision lands. See [Legacy & test-only](dead-legacy.md).

| Migration | Table(s) | Why orphaned |
|-----------|----------|--------------|
| `0005_entity_review_queue.sql` | `entity_review_queue` | Owned by the production-wired `entity_resolver_worker`; medium-confidence rows also open clarification requests for human review. |
| `0009_signal_memory_fabric.sql` | `signal_memory_fabric` | Owned by the production-wired `anomaly_processor_worker`; maintenance cleanup policy remains a future lifecycle item. |
| `0013_orphan_log.sql` | `orphan_log` | Written by maintenance daily cleanup through `housekeeper_worker`; retention/reporting ownership still needs a runbook. |

> **TODO(human):** For each cluster, decide *wire it up* (add launcher/route/flag)
> vs. *leave staged* vs. *remove*. The worker fabric is one decision (deploy the
> Wave-4 workers) that clears most of this page at once.
