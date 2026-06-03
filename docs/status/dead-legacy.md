# Legacy, Orphaned & Test-Only Code

What's superseded, what has no references, and what only tests reach.

!!! success "No truly-dead files"
    After the adversarial-verify pass (repo-wide grep for every dynamic-use
    pattern), **no file was found with zero references of any kind** — the
    conservative first pass plus verification means every "dead code" candidate
    turned out to be reachable by *something* (a test, a launcher, a re-export, an
    undeployed worker). The closest to dead are the **zero-runtime-reference
    orphans** below. Verify also **overturned ~28** first-pass "dead/unwired"
    claims (e.g. `calibration_updater`/`precipitation` are loaded by the deployed
    `think_worker`; `lib/nexus` is test-only, not dead).

## Legacy / retired (compat-only)

| Item | What it is | Status |
|------|------------|--------|
| `services/app/webhooks/tenant_resolution.py` | The old IN-06 env-var tenant resolver (`WEBHOOK_TENANT_*`) | **Deprecated**, deletion pending per its own docstring (IN-08 T049). Superseded by the live DB-backed `tenant_resolver.py`. No runtime/test importer. |
| `db/migrations/0021_review1_remediation.sql` → `anomaly_thresholds`, `dedup_keys_seen` | Tables from an early review remediation | **Orphan tables** — zero refs in `services`/`lib`/`tests`. |
| `db/migrations/0032_topology_layer.sql` → `topo_dirty_queue` | Dirty-queue for the retired accepted-memory topology | **Retired** — zero non-test refs (only a test conftest `DELETE`). |
| `db/migrations/0032` → `model_neighborhoods`, `model_neighborhood_membership` | Accepted-memory neighborhood tables | **Compat-only read path**: read live by the CEO Map routes, but written only by the *undeployed* topology workers (topology relocated to `services/reasoning/topology` + the sweeper). |

!!! warning "Not legacy, despite appearances"
    `services/ingest/ingestion/handlers/{calendar.py,email.py}` look superseded (by
    `google_calendar.py`/`gmail.py`) but verify confirmed they are the **live
    handlers for the demo simulator's Calendar/Email tabs** (reachable via the
    gateway-mounted demo router). Keep them.

## Orphaned — zero-runtime-reference (safe-to-remove candidates)

These have no runtime caller and no (or only self/docstring) references. They are
the realistic "delete or wire up" candidates. Confirm intent before removing.

| File / item | What it is | Note |
|-------------|------------|------|
| `services/product/today/map.py` | `build_map` Map payload | Zero importers repo-wide; aggregator sets `map_data=None` ("deliberately suppressed"). |
| `services/domain/falsifiers/__init__.py` | Re-export shim of `models.falsifier` | No importer; callers use `models.falsifier` directly. |
| Second `@app.get('/v1/history')` in `gateway/main.py` | Duplicate route handler | Shadowed/unreachable — FastAPI keeps the first match. Copy-paste artifact. |

## Test-only — dormant runtime features

Reached **only by tests** today. Unlike the harness below, these are *product/infra
features* that simply aren't invoked by any running code yet — i.e. latent wiring
gaps worth tracking.

| File(s) | Feature | Why it matters |
|---------|---------|----------------|
| `lib/nexus/{__init__,client}.py` | AI-agent attestation stub (`attest` always returns `attested=True`) | No real crypto; zero non-test importers. Phase-4 deferred (external "Nexus"). |
| `integrations/discord/gateway/{leader_lock,lifecycle,session_state}.py` | Discord HA single-instance lease + crash-RESUME | Built+tested but the launcher bypasses them → 2 replicas double-deliver; restart drops buffered frames. |
| `ingestion/feature_flags/{circuit_breaker,__main__}.py` | Kafka-cutover auto circuit breaker | Built+tested, no launcher (and would watch a now-dead default topic). |
| `ingestion/workflows/__main__.py` | `WORKFLOW_SERVICE` env dispatcher | The 6 deployed workflow services use explicit module commands; this selector isn't invoked. |
| `code_intel/embed.py` | Code-RAG embedding fill | Only the demo script calls `fill_pending_embeddings`; reindex never fills → code-search empty in prod. |
| `reasoning/calibration/{__init__,hit_rate}.py` | Per-class 30-day calibration anchor (`classify_card`) | Only tests + the package `__init__` import it → calibration anchors not surfaced at runtime. |
| `product/demo/{budget,model_routing,notifications}.py` | Demo cost-cap, per-tenant LLM model override, notification suppression | Imported only by tests → the demo router doesn't enforce the cost cap / model routing / suppression at runtime. |
| `workers/deadline_resolver/evaluators.py` | Kind-specific falsifier evaluators | Reached only via the unwired deadline resolver + the falsifier test harness. |
| `reasoning/topology/eval_harness.py` | Offline latent-topology coverage/miss harness | Evaluation tooling (expected test/offline use). |

## Test & simulation harness (expected test-only)

These are *meant* to be test/sim infrastructure — listed for completeness, **not**
a concern. (66 more files are `tooling`: `scripts/` probes, e2e runners, benches.)

- `services/ingest/synthetic/fixtures/*` — deterministic per-source fixtures (`make_*`).
- `services/ingest/synthetic/mock_clients/*` — in-process backfill fakes per source.
- `services/ingest/synthetic/mock_servers/*` — standalone HTTP mock servers (e.g. mock Google Workspace org).
- `services/ingest/synthetic/spammer/discord_gateway.py` — a mock Discord WSS server.

> **TODO(human):** Triage the "orphaned" table into delete vs. wire-up, and decide
> whether the demo cost-cap / model-routing / notification-suppression being
> test-only is acceptable for the demo deployment (it means demo sessions aren't
> actually cost-capped at runtime). Record decisions as [ADRs](../adr/README.md).
