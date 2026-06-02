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

The dominant cluster. Only `topology_sweeper` has a launcher; these have none
(no compose service, no `scripts/run_*`). See [Workers](../architecture/workers.md)
and the [feature table](feature-status.md).

| File(s) | What it is | Why not in flow |
|---------|------------|-----------------|
| `anomaly_processor/{__init__,worker,detectors,significance,debounce,memory_fabric}.py` | Wave-4-B: 6 anomaly detectors → significance → debounce → `T3` enqueue + `signal_memory_fabric` writes | No launcher/compose; `AnomalyProcessor` constructed only in tests. The post-commit handoff that would feed it is a no-op. |
| `entity_resolver/{__init__,worker,context}.py` | Deferred LLM resolution of `_unresolved_phrases` → aliases + `T1` re-enqueue | No launcher/compose; `process_observation` invoked only in tests. |
| `deadline_resolver/{__init__,worker}.py` | Wave-4-A: poll overdue prediction Models → `T2 prediction_overdue` | No launcher/compose; reached only by tests/synthesis-harness. (`evaluators.py` is test-only.) |
| `edge_drift/{__init__,worker}.py` | Samples `model_edges` vs. legacy array cols → drift metrics | No launcher/compose; only tests call `run_once`. The parity guard for the dual-write migration never runs. |
| `maintenance/{__init__,daily,weekly,monthly,scheduler}.py` | Wave-4-D: daily decay/archival/cleanup, weekly relationship-maint + calibration + partition-extend, monthly vacuum | `MaintenanceScheduler` never instantiated outside tests → the whole upkeep subsystem is dormant. |

## Domain substrate

| File | What it is | Why not in flow |
|------|------------|-----------------|
| `services/domain/models/decay.py` | `hourly_decay` + `archive_decayed` activation/decay UPDATEs | Imported solely by `maintenance/daily.py`, which has no launcher → Models never decay/auto-archive in prod. |
| `services/domain/falsifiers/__init__.py` 🗑 | Thin re-export shim of `models.falsifier` adequacy | No importer anywhere — all callers import `models.falsifier` directly. The "single import site" it intended to be is unused. |

## Platform (access control + execution)

See [Platform](../architecture/platform.md).

| File | What it is | Why not in flow |
|------|------------|-----------------|
| `access_control/middleware.py` | `@requires_access` route decorator (wraps `can_read_by_id` + audit) | Applied on **zero** routes; not even re-exported from the package `__init__`. The gateway does manual inline `can_read_by_id` instead. |
| `access_control/materialized.py` | `actor_visible_*` matview refresh + dirty-queue + point-checks | Sole refresh caller is the undeployed `maintenance/daily.py`; `is_*_visible_to` helpers have zero callers (`checks.py` reads the matviews via raw SQL). |
| `access_control/audit.py` | `record_override` → `access_override_log` writer | Only caller is the never-applied `middleware.py`; the live `can_read` paths (dispatcher, assembler, dashboard) don't call it. |
| `execution/routing.py` | `decide_route` gate + `record_routing_decision` shadow persistence | No non-test caller; ingestion never builds a `SignalEnvelope.from_observation`. Env flags exist only in `.env.example`. Not even shadow-wired. |

## Ingestion pipeline

See [Ingest](../architecture/ingest.md).

| File | What it is | Why not in flow |
|------|------------|-----------------|
| ~~`ingestion/idempotency/__init__.py`~~ | `external_id` constructors (M5 / LLD §6) — one per source dedup key | **✅ Wired (2026-06-02).** The stub now holds the 18 **composed** `external_id` constructors (the namespaced + IN-15-versioned keys: slack, gmail, discord, notion, github-push, grafana ×2, gcal, gdrive ×4, jira ×3, mercury ×2, qbo ×2), and all 11 handlers import `idempotency` and call them — so a source's dedup-key format lives in exactly one place and can't drift between its webhook/backfill/poll paths. Adopted-verbatim keys (Stripe `evt_…`, GitHub `node_id`, RFC-5322 `Message-ID`, Linear ids) stay inline by design (nothing to compose). Byte-for-byte preserved: the per-handler `external_id` assertions + the load-bearing `test_backfill_external_id_parity` stayed green, and a new `idempotency/tests/test_external_ids.py` pins every format. See the [feature table](feature-status.md). |
| `ingestion/rate_limit/buckets.py` 🗑 | Per-(source,method) token-bucket specs (`BUCKET_DEFAULTS`) | Zero importers (even tests); `shard_fetch`'s `FetchPage` never calls a rate limiter. |
| ~~`ingestion/workflows/feels_onboarded_monitor.py`~~ | Polls `onboarding_runs`, fires `feels_onboarded` + `behind_schedule` progress events | **✅ Wired (2026-06-02).** Given a per-module `__main__` + a `feels_onboarded_monitor` compose service; the legacy `WORKFLOW_SERVICE` selector is kept for the subprocess test. See the [feature table](feature-status.md). |
| `ingestion/writers/dlq_writer/__main__.py` | CLI entry `python -m …dlq_writer` | Compose runs `…dlq_writer.dlq_writer` (the inner module) directly; the package `__main__` is unused. |
| `ingestion/writers/embedding_worker/__main__.py` | CLI entry `python -m …embedding_worker` | Compose runs `…embedding_worker.embedding_worker` directly; package `__main__` unused. |

## Integrations

| File | What it is | Why not in flow |
|------|------------|-----------------|
| `integrations/gmail/status_api.py` 🗑 | `get_gmail_status`: watch/audit snapshot for a tenant | Zero refs repo-wide; no router mounts `GET /integrations/gmail/status`. |
| `integrations/gmail/uninstall.py` 🗑 | `uninstall_install`/`stop_mailbox`: teardown | Zero callers; no uninstall route mounted (the `__init__` docstring is aspirational). |
| `integrations/jira/onboarding.py` | Jira install: upsert installs + `onboarding_trigger` | `finalize_install`/`register_webhook_installation` invoked only by `scripts/sandbox_jira*.py` + tests; no gateway router/workflow (unlike Mercury/QBO). |

## Reasoning

| File | What it is | Why not in flow |
|------|------------|-----------------|
| `reasoning/retrieval/maintenance.py` | `background_relationship_maintenance` nightly worker | Imported only by `workers/maintenance/weekly.py`, which has no launcher/compose service. |

## Product surfaces

See [Product](../architecture/product.md). (`query/prefetch.py`, `today/freshness.py`,
`today/stake.py` were flagged unwired but are test-referenced — see
[Legacy & test-only](dead-legacy.md).)

| File | What it is | Why not in flow |
|------|------------|-----------------|
| `services/product/today/map.py` 🗑 | `build_map`: DECIDE/MOVED/HANDLED three-row Map payload | Zero importers repo-wide; the aggregator hard-codes `map_data=None` (comment: "Map deliberately suppressed"). |

## Migrations for undeployed features

These create tables whose only reader/writer is an undeployed worker (see above),
so they're **runtime-orphans** on this branch. See [Legacy & test-only](dead-legacy.md).

| Migration | Table(s) | Why orphaned |
|-----------|----------|--------------|
| `0005_entity_review_queue.sql` | `entity_review_queue` | Only writer is the not-wired `entity_resolver` worker. |
| `0009_signal_memory_fabric.sql` | `signal_memory_fabric` | Only owners are the not-wired `anomaly_processor` + `maintenance`. |
| `0013_orphan_log.sql` | `orphan_log` | Only writer is the not-wired `maintenance/daily.py`. |

> **TODO(human):** For each cluster, decide *wire it up* (add launcher/route/flag)
> vs. *leave staged* vs. *remove*. The worker fabric is one decision (deploy the
> Wave-4 workers) that clears most of this page at once.
