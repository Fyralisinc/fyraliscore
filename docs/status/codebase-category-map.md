# Codebase Category Map

Generated on 2026-06-03 from the tracked tree on
`trim/remove-deprecated-tenant-resolution`.

Purpose: classify each major folder/file family by what it owns, how it links
to the rest of Fyralis, and what cleanup/hardening decision it needs. This is a
working map for trimming, not a permanent architecture spec.

Ignored local output such as `.secrets/`, `company_os.egg-info/`, `__pycache__/`,
`.pytest_cache/`, `.ruff_cache/`, `.venv/`, and `node_modules/` is not product
code. Keep it ignored and out of cleanup discussions unless a secret or build
artifact is accidentally tracked.

## Runtime Graph

```text
ui/ React app
  -> services/app/gateway FastAPI routes
       -> services/product CEO-facing surfaces
       -> services/ingest webhook/connect/install entrypoints
       -> services/domain persisted substrate
       -> services/reasoning query, retrieval, Think, Sage
       -> services/platform cross-cutting access/execution helpers

docker-compose.yml and scripts/start.sh
  -> gateway app
  -> ingest workflow services, normalizer, writers, backlog workers
  -> Think worker, post-commit worker, topology sweeper
  -> live integration pollers/watch schedulers
  -> Postgres, Kafka, Redis, S3/MinIO, Ollama

services/ingest
  -> services/domain + db/migrations
  -> lib/shared, lib/embeddings, lib/integrations

services/reasoning
  -> services/domain + lib/shared/lib/llm/lib/embeddings
  -> services/platform for execution/inquiry support

services/product
  -> services/domain + services/reasoning + lib/shared
  -> gateway routers and ui/src/api clients

services/workers
  -> services/domain + services/reasoning
  -> mostly staged unless launched by scripts/compose
```

## Classification Legend

| Status | Meaning | Cleanup stance |
| --- | --- | --- |
| `runtime` | Launched by Docker, scripts, gateway routes, or imported by live paths. | Harden and test before refactor. |
| `support` | Shared library, migration, config, or doc used by runtime. | Keep, but enforce contracts and ownership. |
| `staged` | Implemented and tested, but not launched or not called in production flow. | Decide wire-up vs. delete. |
| `test-offline` | Harness, mock, scenario, benchmark, or diagnostic tool. | Keep only if named by an active quality workflow. |
| `parked` | Large subsystem not part of current Fyralis runtime. | Isolate, move, or ignore deliberately. |
| `generated-local` | Cache, build output, secrets, egg-info, pycache. | Never track; safe local cleanup. |

## Folder Categories

| Folder/file family | Purpose | Main links | Status | Cleanup/hardening notes |
| --- | --- | --- | --- | --- |
| `services/app/gateway/` | FastAPI app factory, middleware, state wiring, route mounting, gateway routers. | Mounted by `Dockerfile`, `scripts/start.sh`, `docker-compose.yml`; calls product, ingest, domain, reasoning, platform. | `runtime` | `main.py` is now a small factory. Next gateway cleanup should focus on route contracts, router ownership, and large routers such as `spec_routes.py`/finance/product-specific routes. |
| `services/app/webhooks/` | Public webhook edge, signature verification, tenant/install resolution, source-specific ingress. | Gateway mounts it; writes into ingestion paths; uses domain/lib/shared. | `runtime` | Keep resolver path DB-backed. Harden metrics, auth failure taxonomy, replay tests, and per-source install lifecycle. |
| `services/app/realtime/` | WebSocket/SSE style realtime dispatch and stream coordination. | Gateway state and product/demo/recommendation update surfaces. | `runtime/support` | Verify which streams have real browser consumers; trim unused stream event types only after UI grep. |
| `services/product/` | CEO-facing product APIs and composed views. Includes today, query, rendering, greeting, recommendations, forecasts, history, decision deltas, model trace, demo, conversations. | Gateway routers call this layer; UI clients render it; uses domain/reasoning/lib. | `runtime` with some `staged` helpers | Highest current product hotspot is `services/product/today/aggregator.py` at about 1900 lines. Split by data source/section after route/API contract stabilization. |
| `services/product/today/` | Today page aggregation and triage. | `today_core_router.py`, UI Today clients/components, domain/product helpers. | `runtime` | Dead helper files were trimmed. Remaining risk is aggregator size and implicit section contracts. |
| `services/product/query/` | Ask/query classification, adapters, caching, retrieval/rendering bridge. | Gateway/query routes, reasoning retrieval, rendering, lib/llm. | `runtime` with config-sensitive fallbacks | Confirm production env uses Postgres cache and real rendering. Otherwise default mocks hide product behavior. |
| `services/product/demo/` | Demo sessions, snapshots, simulator, SSE, demo router. | Gateway demo routes, `demo/` snapshots, scripts, UI demo paths. | mixed `runtime`/`test-offline` | Budget/model-routing/notification policy helpers have been flagged as test-only in status docs; decide whether demo runtime should enforce them. |
| `services/reasoning/think/` | Core Think pipeline, LLM reasoning, validation, applier, post-commit queue, quality gates. | `scripts/run_think_worker.py`, `scripts/run_post_commit_worker.py`, product query/recommendations, domain. | `runtime` | Harden no-op post-commit dispatchers, LLM strict schemas, and quality gates before broad refactors. |
| `services/reasoning/retrieval/` | Retrieval assembly, pathways, scoring, second-pass retrieval, maintenance helper. | Product query/recommendations, Think, lib/embeddings, domain. | `runtime` plus staged maintenance | Maintenance is only used by unwired maintenance worker. Keep retrieval core separate from offline upkeep. |
| `services/reasoning/sage/` | Sage reader, inquiry traces, affordances, discovery, structural features, model predictions, topology optimizer. | Gateway internal routes, product query/model trace, domain, recent migrations `0084`-`0092`. | `runtime/support` but newer | Treat as active recent work. Cleanup should verify every migration-backed table has a reader/writer and a UI/API consumer. |
| `services/reasoning/topology/` | Latent topology, UMAP projector, field/anchor logic, eval harness. | Topology sweeper, map routes, tests, lib/shared. | mixed `runtime`/`test-offline` | Separate offline eval harness from runtime projection code. |
| `services/reasoning/calibration/`, `contestability/`, `dynamics/`, `relationships/`, `judgment/` | Scoring, calibration, contestability, relationship adjudication, trigger/dynamics helpers. | Think, product recommendations, workers, domain. | mixed | Status docs identify calibration anchors and some dynamics paths as under-wired. Decide if they are product-critical or staged science. |
| `services/ingest/ingestion/` | Kafka-first ingestion data plane: workflows, raw tier, normalizer, writers, idempotency, rate limits, recovery. | Compose services launch many modules; webhooks/integrations feed it; domain stores output. | `runtime` | This is the largest backend package. Keep generated compose and source registry in sync; harden migration fixtures and per-source parity tests. |
| `services/ingest/integrations/` | Source clients and OAuth/connect/install flows for Slack, Discord, Gmail, Google Calendar/Drive, GitHub, Jira, Grafana, Mercury, Notion, QuickBooks. | Gateway connect routes, webhooks, ingestion fetchers/workflows, lib/integrations endpoints. | `runtime` | Cleanup by source family. Keep credential storage, tenant resolution, webhook registration, and backfill path together per source. |
| `services/ingest/github_intel/` and `services/ingest/code_intel/` | GitHub intelligence worker plus code graph/search indexing. | `scripts/run_github_intel_worker.py`, product/model/code UI, lib/llm/embeddings. | `runtime` read path, `staged` write path | Existing status docs flag code-intel indexing/embedding fill as not fully wired. Decide whether to wire or remove the code-search promise. |
| `services/ingest/synthetic/` | Synthetic fixtures, mock clients, mock servers, scenario generators. | Tests, simulation, demo harness. | `test-offline` | Keep as test/sim infrastructure. Do not mix into runtime source clients. |
| `services/domain/` | Persisted business substrate: models, acts, actors, resources, observations, bridge queries, entity aliases. | Ingest writes; reasoning/product/workers read/write; backed by `db/migrations`. | `runtime/support` | Known boundary smell: `services/domain/models/repo.py` imports upward into reasoning/product. Track as a real architecture debt before deleting anything. |
| `services/platform/access_control/` | Cross-cutting authorization checks, hierarchy, roles, materialized visibility, audit, route decorator. | Gateway, dashboard, retrieval, maintenance. | partly `runtime`, partly `staged` | Decorator/audit/materialized refresh are under-applied. Either make declarative access control real or remove the unused wrapper layer. |
| `services/platform/execution/` | Execution routing contracts, route decisions, inquiry helper. | Intended for ingestion/reasoning routing; currently mostly tests/status docs. | `staged` | `routing.py` has no non-test caller. Best next hardening move is to shadow-wire it or delete the promise. |
| `services/workers/` | Background worker packages: anomaly, calibration, deadline, edge drift, entity resolver, maintenance, precipitation, topology sweeper/updater. | Some scripts/compose launch only topology/Think-adjacent workers; many packages are tests-only. | mostly `staged` | Treat as one worker-fabric decision. Either add launchers/compose/env/observability, or trim worker packages plus orphan migrations/tables. |
| `lib/shared/` | Shared DB/env/errors/types/ids/trust/migrations/tenant context/registries/secrets. | Imported by nearly every backend layer. | `support` | Keep small and dependency-free. It is the stable base layer. |
| `lib/llm/` | Provider abstraction, LLM errors/timeouts, backend selection. | Reasoning/product/demo/scripts. Has documented lazy imports into reasoning schemas. | `support` with boundary exception | Keep the exception explicit in import-linter. Avoid adding more services imports here. |
| `lib/embeddings/` | Embedding backend interface and Ollama/OpenAI factory. | Ingest embedding worker, reasoning retrieval/topology, tests. | `support` | Good shared boundary. Harden provider config and failure taxonomy. |
| `lib/integrations/` | Shared endpoint metadata/helpers for integrations. | Ingest integration clients, tests. | `support` with boundary-sensitive tests | Import graph shows service references here; keep production code pure or move endpoint helpers into ingest. |
| `db/migrations/` and `db/seed/` | Database schema and seed data. | Every runtime layer through domain/lib/shared DB helpers; CI enforces unique numeric prefixes. | `support` | 93 migrations. Cleanup should classify tables by live reader/writer before dropping anything. Migration fixtures currently expose some DB-backed test fragility. |
| `ui/` | React/Vite frontend, API clients, pages, components, model/map/today/spec/forecast views, e2e tests. | Calls gateway HTTP/WebSocket APIs; `ui/package.json` owns typecheck/test/build. | `runtime` | API contracts are still mostly handwritten. Generate or validate clients from `contracts/http-routes.json` or OpenAPI before large UI cleanup. |
| `contracts/http-routes.json` | HTTP route inventory/contract artifact. | Unit tests and future UI contract generation. | `support` | Make this generated and checked, or retire it. Manual drift is the current risk. |
| `tests/`, co-located `*/tests/`, `ui/e2e/`, `tests/real_llm/`, `tests/synthesis_harness/` | Unit, integration, e2e, real-LLM, quality replay, synthesis harness. | Exercises all layers; CI runs a conservative subset plus nightly real-LLM. | `support/test-offline` | Use test reachability to distinguish staged features from dead code. Keep test-only harnesses clearly named. |
| `scripts/` | Local/dev/prod launchers, sandbox tools, dogfood scripts, diagnostics, backfills, migration/schema checks. | Compose, local development, CI, production ops. | mixed `runtime/support/test-offline` | Split launchers from diagnostics eventually. Runtime scripts should have env docs and tests; one-off probes can move under a diagnostics namespace. |
| `demo/` | Demo data generation and compressed SQL snapshots. | `scripts/start.sh`, product demo routes, tests. | `support/test-offline` | Keep if demo tenants are active. Otherwise move snapshots out of core runtime path. |
| `simulation/` | Scenario replay, personas, mock Slack UI, simulated workers. | Tests, demo/dev workflows, synthetic ingest. | `test-offline` | Useful for product validation, but should not be imported by runtime packages. |
| `.github/` | CI, deploy workflows, CODEOWNERS. | GitHub branch/deploy process. | `support` | CODEOWNERS is intentionally empty until real teams exist. Branch rules still matter more than docs. |
| `Dockerfile`, `Dockerfile.ui`, `docker-compose*.yml`, `nginx/`, `nginx-ui.conf` | Runtime packaging and local/prod orchestration. | Gateway, UI, data services, workers. | `runtime/support` | Compose is the source of truth for what is actually deployed. Use it to settle "staged vs runtime" arguments. |
| `docs/`, `CODEBASE-ARCHITECTURE.md`, `CODEBASE-MANAGEMENT.md`, `FYRALIS.md`, `CLAUDE.md`, `CONTRIBUTING.md`, `mkdocs.yml`, `docs_hooks.py` | Architecture, operations, status, ADRs, project memory, docs site. | Human process and CI/cleanup planning. | `support` | Several docs still lag recent trims. Update docs only when they constrain decisions; avoid new status sprawl. |
| `.specify/`, `specs/` | Speckit/spec workflow and historical feature specs. | Planning docs and source specs, not runtime. | `support/test-offline` | Keep as planning infrastructure if it is actively used. Otherwise archive old feature specs. |
| `mocks/starscape.html`, `docs/mockups/map.html` | Static mockups/prototypes. | Human design reference only. | `test-offline` | Move active mockups near UI stories/tests or archive them. |
| `lsob/` | Separate benchmark/evaluator ecosystem with its own packages, pyproject, uv.lock, compose, workflows. | Mostly self-contained; a few imports touch shared/services namespaces. | `parked` | Since LSOB is out of scope now, isolate it from active cleanup. Best end-state is separate repo or subtree with explicit ownership. |

## Actual Import Hotspots

The largest measured internal links are:

| Source | Main outbound links | Interpretation |
| --- | --- | --- |
| `services/ingest/ingestion` | ingest internals, `lib/shared`, domain, embeddings | The data plane is the densest subsystem and should be cleaned by workflow/source slice. |
| `services/ingest/integrations` | ingest internals, `lib/shared`, app/domain | Source clients and app install flows are interleaved; cleanup by source is safer than broad moves. |
| `services/reasoning/think` | reasoning internals, `lib/shared`, domain, `lib/llm` | Think is a runtime core; harden before major slicing. |
| `services/app/gateway` | app internals, product, ingest, domain, reasoning | Gateway is now mostly composition, but routers still encode product ownership. |
| `services/domain/models` | `lib/shared`, domain, reasoning/product | This is the key boundary debt to fix when stabilizing architecture. |

## Cleanup Order

1. **Generated/local hygiene**: delete local `__pycache__`, egg-info, caches when convenient; keep `.secrets/` ignored and never tracked.
2. **Docs/status refresh**: update stale root docs after recent trims and this map. Remove contradictory old claims before making new decisions from them.
3. **Runtime contract pass**: make route inventory/OpenAPI/UI clients mechanically checked. This addresses handwritten API drift.
4. **Product hotspot pass**: split `today/aggregator.py` and large product routers by owned section once contracts are pinned.
5. **Platform decision pass**: either wire `access_control` decorator/audit/materialized refresh and `execution/routing`, or delete their staged promises.
6. **Worker fabric decision pass**: for each `services/workers/*`, choose deploy, archive, or delete. Then align migrations/tables with that choice.
7. **Ingest/code-intel pass**: finish or remove the code-intel write/index/embed path so code-search is not a half-promise.
8. **DB table liveness pass**: table-by-table reader/writer inventory before dropping orphan migrations or tables.
9. **LSOB separation pass**: once core Fyralis is clean, move or isolate `lsob/` instead of letting it distort repo ownership and CI.

## Immediate Candidate Work Items

| Candidate | Why now | Validation |
| --- | --- | --- |
| Generate/check route contracts | UI/API drift is a known roast point and a high leverage stabilizer. | `pytest tests/unit/test_http_contract.py`, UI typecheck/build. |
| Split Today aggregator by section | It is now the biggest obvious in-file implementation mass. | Today route tests plus UI Today smoke/e2e. |
| Decide `services/platform/execution/routing.py` | It is explicitly staged with no runtime caller. | Add shadow caller and DB test, or remove table/docs/tests. |
| Decide worker fabric | Many "features" are only tests until launchers exist. | Compose/script inventory plus targeted worker tests. |
| Refresh docs/status stale rows | Cleanup decisions should not rely on stale audit tables. | `mkdocs build` if docs extra is installed. |
