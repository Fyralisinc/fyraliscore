# Fyralis Core — Codebase Management & Organization

Authored 2026-06-01 on branch `refactor/codebase-layered-reorg` (off `cannonical`).

This is the decision record for how Fyralis Core is organized and managed: what
was assessed, what was changed, what was deliberately *not* changed, and the
reasoning behind each call. It is the companion to
[CODEBASE-ARCHITECTURE.md](CODEBASE-ARCHITECTURE.md) (what the system does) and
[CONTRIBUTING.md](CONTRIBUTING.md) (the day-to-day conventions).

---

## 0. TL;DR

- **Management model: single monorepo. Confirmed, not split.** The backend is
  genuinely interdependent and shares one library, one migration chain, one test
  harness, and one deploy. Splitting would add cross-repo cost for no present
  benefit. Boundaries are now made explicit *inside* the repo instead.
- **Structure: `services/` was re-layered** from 37 flat packages of wildly
  different scale/role into **six dependency-ordered layers** (`app`, `product`,
  `reasoning`, `ingest`, `domain`, `platform`) plus the already-layered
  `workers/`. History-preserving; **4,169 references rewritten**; verified
  identical to baseline (3,209 tests collected, 11 pre-existing errors, ruff
  unchanged).
- **Boundaries are now enforced**, not just documented: import-linter contracts
  in CI, a `CODEOWNERS` map, per-layer READMEs, and `CONTRIBUTING.md`.
- **Honesty over theatre:** only invariants that are *empirically true today* are
  enforced. Real coupling that exists (e.g. `domain.models.repo → product`) is
  documented as tracked debt, not hidden behind a contract the code already
  breaks.
- **Deliberately deferred:** the 4,409-line gateway `build_app()` split (not
  runtime-verifiable in this environment — see §8) and risky schema/lock changes.
  Each comes with a concrete plan instead of a blind edit.

---

## 1. The brief and how it was approached

The request: inspect the whole project, identify the best codebase-management
approach, organize the code to be maintainable/scalable/understandable, document
everything, and split into multiple repos if it makes sense.

Because this is a **mature, deployed system** (1,969 tracked files, ~1,358 Python
modules, a React UI, Kafka/Temporal/S3/Redis ingestion, 79 SQL migrations, and
live `production`/`demo-deploy` branches), the work was sequenced to minimize
irreversible risk:

1. **Inspect** the real organizational state (not just architecture) — top-level
   clutter, per-service consistency, dependency hygiene, branch landscape.
2. **Decide the two high-blast-radius questions with the owner** (repo strategy
   and reorganization depth) before touching code.
3. **Execute** on an isolated branch with `git mv` (history-preserving) and a
   **static verification gate at every step** (the integration suite needs live
   Postgres+Ollama, so import-resolution was the provable invariant).
4. **Enforce + document** the new structure so it does not rot.

---

## 2. What the assessment found

The codebase was **not disordered** — it was *illegible at scale*. Concretely:

| Finding | Evidence |
|---|---|
| Good git hygiene | No tracked build/cache cruft; no committed secrets; comprehensive `.gitignore`; `lsob/` (258 files) correctly gitignored as an external tool. |
| Clean lower layer | `lib/` had **no hard layer violations** — only a few guarded lazy imports. |
| **`services/` was a flat grab-bag** | 37 packages with no sub-structure, mixing 261-file subsystems (`ingestion`) and 119-file (`integrations`) with 2-file helpers (`judgment`). Nothing signalled which packages were domain vs. app vs. reasoning vs. product. |
| Cross-package coupling is real | `ingestion ↔ integrations` are bidirectionally coupled; `think → retrieval → models` is a chain; all done via **absolute** imports (only 9 multi-dot relatives, all intra-package). |
| God-file | `gateway/main.py` = **4,409 lines**. |
| Name collisions | `lib/topology` vs `services/topology`, `lib/integrations` vs `services/integrations`, top-level `demo/` (data-gen) vs `services/demo/` (runtime). |
| Convention drift | Router files named `router.py` / `routes.py` / `api.py`; 4 services lacked co-located tests. |
| Loose top-level docs | Stale `V1_PR_PROMPTS.md` + active backlog at the root; `.gitignore` listed `CLAUDE.md`/`V1_PR_PROMPTS.md` as "not published" while both were tracked. |
| Branch drift | `cannonical` is **370 ahead / 0 behind** `main`; `production` is **46 behind** `main`; `integration/ingestion-hardening` 341 ahead / 48 behind. `main` is no longer the real integration point. |
| Latent test coupling | 11 pre-existing `pytest --collect-only` errors from service tests importing `tests.db_baseline`. |
| Schema numbering | Two duplicate migration prefixes (`0014_*`, `0043_*`) from a historical merge. |
| Dead/odd artifacts | `monitoring/` exists empty and root-owned (a container mount), unreferenced by any compose/CI. |

---

## 3. Decision 1 — Management model: **single monorepo**

The owner explicitly invited a multi-repo split "if it makes sense." It does not,
and here is the evidence-based reasoning.

**Why a split was rejected:**
- **Coupling is real and bidirectional.** `ingestion ↔ integrations` import each
  other; the reasoning core (`think`/`retrieval`/`models`) is a tight chain.
  Splitting these forces internal package publishing and cross-repo version
  locks for changes that are routinely made together.
- **One schema, one truth.** A single ordered `db/migrations/` chain is the
  system's backbone. Splitting repos fractures it or forces a shared schema
  package that every repo must pin.
- **One test harness, one deploy.** Tests run against real Postgres across
  service boundaries; compose builds one gateway image + workers. A split
  multiplies CI and integration surface for no isolation benefit today.
- **The monolith is intentional** (stated in CODEBASE-ARCHITECTURE.md) and the
  team is effectively one maintainer — cross-repo overhead would dominate.

**The one clean seam, noted for the future:** `ui/` (React/TS) has *no* Python
import coupling — only an HTTP/WS contract. If the frontend ever needs an
independent release cadence, it is the only low-cost candidate to extract. Not
done now (shared API types, the in-repo mock server, and the dev proxy make it a
net negative until there's a reason).

**What we did instead of splitting:** made the module boundaries explicit and
*enforced* (§5), which delivers the legibility benefit of a split without the
cost.

---

## 4. Decision 2 — Layered structure inside `services/`

The owner chose the **full physical re-layering** option (accepting the
high-blast-radius trade-off). The 37 packages were grouped into six layers,
ordered by intended dependency direction (higher may depend on lower):

```
services/
  app/        gateway, webhooks, realtime                — HTTP/WS entry & dispatch
  product/    greeting, today, forecasts, query,
              conversations, recommendations,
              decision_deltas, history, model_trace,
              rendering, demo                            — CEO-facing surfaces
  reasoning/  think, retrieval, topology, judgment,
              relationships, dynamics, contestability,
              calibration                                — Think pipeline & retrieval
  ingest/     ingestion, integrations, synthetic,
              code_intel, github_intel                   — signal intake
  domain/     models, acts, resources, observations,
              actors, entity_aliases, bridge, falsifiers — persisted substrate
  platform/   access_control, execution                 — cross-cutting infra
  workers/    (10 background worker packages)            — already layer-shaped; unchanged
```

**Why these layers / why these assignments:**
- They mirror the data flow the architecture doc already describes (signal →
  ingest → domain substrate → reasoning → product surface → app transport), so
  the directory tree now *teaches* the architecture.
- It naturally resolves the confusing name collisions: `services/topology` →
  `services/reasoning/topology` (clearly distinct from `lib/topology`);
  `services/integrations` → `services/ingest/integrations` (vs `lib/integrations`);
  `services/demo` → `services/product/demo` (vs top-level data-gen `demo/`).
- `workers/` was already structured as a layer of worker packages, so it was
  left untouched (zero-churn win).

**Why layers are PEP 420 namespace packages (no `__init__.py`):** `services/`
itself is a namespace package. An early attempt to add `__init__.py` to each
layer caused a real failure: pytest's package-root walk then elevated the
`platform` layer to a **top-level** import name, colliding with Python's stdlib
`platform` module (`'platform' is not a package`). Keeping the layers as
namespace packages — consistent with `services/` — eliminates that entire class
of stdlib-shadowing bug. Layer documentation lives in `services/<layer>/README.md`
instead of a module docstring.

**`lib/` was left as-is** — it is already clean (6 well-scoped subpackages:
`shared`, `llm`, `embeddings`, `topology`, `nexus`, `integrations`) with no hard
upward dependencies. Re-layering it would be churn without benefit.

---

## 5. What changed (implementation), commit by commit

All work is on `refactor/codebase-layered-reorg`, in reviewable commits:

| Commit | Change |
|---|---|
| `refactor(structure)` | `git mv` 37 packages into the six layers; rewrite **4,169** absolute references (`services.<pkg>` → `services.<layer>.<pkg>` and the slash form) across Python, docker-compose, CI, scripts, Dockerfile, and docs. 953 renames + 989 content edits. |
| `chore(docs)` | Archive stale `V1_PR_PROMPTS.md` → `docs/history/`; relocate the active hardening backlog → `docs/hardening-backlog.md`; fix the `.gitignore`-vs-tracked contradiction. Root now holds only `README.md`, `CODEBASE-ARCHITECTURE.md`, `CLAUDE.md`. |
| `chore(arch)` | import-linter contracts in `pyproject.toml` + a CI `architecture` job; per-layer READMEs; `CODEOWNERS`; `CONTRIBUTING.md`. |
| `docs` | This document + the architecture-doc refresh. |
| `fix(paths)` | Runtime-verification follow-up (see §6): fix 18 `parents[N]` depth assumptions + 6 hardcoded `"services"/"<pkg>"` path fragments broken by the +1 layer depth — including two **production** sites (`gateway/main.py` slack_ui static dir, `product/demo/snapshot.py` snapshot SQL path). |

**The migration was mechanical and complete by construction.** Cross-package
imports are all absolute, so the rewrite is a pure prefix insertion at the
package boundary; the few relative imports (9, all `..contracts` *within*
`rendering`) stay correct because the target moves with the package.

---

## 6. Verification methodology

Verification ran in two layers: **static** (import resolution) and **runtime**
(an actual test run against a live throwaway database).

### Static gates

| Gate | Baseline | After re-layer | Verdict |
|---|---|---|---|
| `pytest --collect-only` | 3,209 tests / 11 errors | **3,209 / 11** | identical |
| `python -m compileall` | clean | clean | pass |
| `ruff check .` | 996 errors | **996** | no regression |
| Stale-reference scan (`services.<oldpkg>` / `services/<oldpkg>`) | — | **0 matches** | complete |
| Package `__init__` import sweep | — | 36/37 (37th is `synthetic`'s intentional env-guard) | pass |
| Layer entrypoint imports (`build_app`, etc.) | — | all import | pass |
| `lint-imports` | — | **2 kept, 0 broken** | pass |

The 11 collection errors are **pre-existing** (`tests.db_baseline` import
coupling, unrelated to this change) — documented in §7. The editable-install
finder was refreshed (`pip install -e .`) because its hardcoded namespace map
went stale after the moves.

### Runtime gate (and the regression it caught)

A throwaway Postgres database (`company_os_reorg_test` on the local pgvector
instance — never the dev DB, since the harness TRUNCATEs) was created, all 79
migrations applied cleanly, and the suite run against it:

- **Unit slice** (`DATABASE_URL` unset → DB tests auto-skip): **1,743 passed,
  0 failed.**
- **DB-backed slice** (throwaway DB): initially **141 passed**, 1 skipped, 1
  failed — both pre-existing environmental gotchas (not refactor defects), now
  **fixed** (see below). A consolidated re-run is **172 passed, 0 failed**.

Two long-standing test-environment gotchas surfaced and were fixed (they predate
this refactor; the run merely exposed them):

- **`observations` partition gap.** The foundation migration attaches only the
  current month + 3 ahead; tests that insert recent-historical rows (e.g. a
  30-day timeline) hit "no partition … found for row." Fixed by
  `lib.shared.migrations.ensure_test_partition_window`, called at the end of the
  test/dev applier (`apply_migrations_dir`) to widen the window to
  `[−12, +3]` months. Inlined (no `services` import) to preserve the
  `lib ⊥ services` contract; idempotent; no-op for non-partitioned schemas.
- **RLS never actually exercised.** A superuser/BYPASSRLS connection (the typical
  dev role) sees through every policy, so `test_rls_isolation` either skipped or
  was vacuous. Fixed with a `rls_app_pool` fixture (root `conftest.py`) that
  provisions a dedicated **non-super** role and connects as it, so the policies
  are enforced. Both RLS test modules now **run and pass** (11 tests),
  confirming tenant isolation (permissive default, tenant-scoped filtering, and
  `WITH CHECK` rejection of cross-tenant writes).

**This is where static analysis was not enough — and why a real run mattered.**
The first runtime pass surfaced **9 failures of a class `compileall`/`collect-only`
could never catch**: filesystem paths built from depth (`Path(__file__).parents[N]`)
and from *separate* string literals (`… / "services" / "ingestion" / …`). Moving
each package one level deeper invalidated the depth counts and the hardcoded
segment. Fixed in 18 `parents[N]` sites + 6 fragment sites — **including two
production paths** (`gateway/main.py`'s slack-UI static mount and
`product/demo/snapshot.py`'s snapshot-SQL resolver). After the fix, the unit
slice went 9-failed → 0-failed and both production paths resolve to the real
repo root. A repo-wide scan confirmed no other instances of the pattern remain.

**Still not exercised here:** Kafka/Temporal/S3 e2e paths and real-LLM flows
(gated behind `requires_docker`/`requires_infra`/`real_llm`), and the deferred
gateway split (§8).

---

## 7. Governance & enforcement (so it doesn't rot)

Legibility decays without guardrails. Added:

- **import-linter** (`pyproject.toml [tool.importlinter]`, run by the CI
  `architecture` job). Two contracts, both green today and chosen to be
  *empirically true* so a failure always means a real regression:
  1. `lib` never imports `services` (with an explicit, documented whitelist for
     3 lazy provider imports + 5 test-only fixtures).
  2. `reasoning` never *directly* imports `app`/`product`/`ingest` (no new upward
     edges into the reasoning core).
- **`CODEOWNERS`** — ownership mapped to the layers (placeholder team slugs,
  clearly flagged to replace before requiring owner review).
- **Per-layer `README.md`** — role, package list, and import rule for each layer.
- **`CONTRIBUTING.md`** — structure, import discipline, naming conventions
  (standardize new routers on `router.py`), migration rules, how to extend, and
  the local pre-push checklist.

---

## 8. Known coupling & tracked debt (deliberately *not* fixed here)

Honesty matters more than a clean-looking diff. These are real and are documented
rather than hidden or hacked:

1. **`domain.models.repo → product`/`reasoning` (upward edge).** `models/repo.py`
   imports `product.recommendations`, `product.demo`, and
   `reasoning.{think,topology,relationships}` to trigger downstream work at
   insert time. This transitively breaks a strict "reasoning ⊥ product" rule,
   which is why the import-linter contract is scoped to *direct* imports. **Fix
   path:** invert the dependency — have the repo emit events/enqueue rather than
   call product/reasoning directly — then tighten the contract to transitive.
2. **Gateway `build_app()` is 4,409 lines (deferred split).** It is *not* a pile
   of inline routes; it is a module-level `_register_routes(app)` + a ~3,800-line
   `build_app()` whose handlers fetch deps via `_deps(request)`, plus ~1,300
   lines of `_build_*_drawer`/`_fetch_*` helpers. **Why deferred:** a correct
   split must first relocate `_deps`/helpers into support modules (else
   `main ↔ routers` becomes a circular import), then carve `_register_routes`
   into `build_*_router()` modules — and the result is only meaningful if the
   gateway is *run* against a DB to confirm identical routing. That runtime check
   is impossible in this environment, and the gateway serves the entire product,
   so a blind split was judged too risky. **Sequenced plan:**
   - (a) Move pure helpers (`_iso`/`_ago`/`_trim`/`_clip`/`_fmt_quantity`) →
     `gateway/_format.py`.
   - (b) Move drawer/fetch builders → `gateway/_drawers.py`.
   - (c) Extract cohesive route groups (`/v1/structure/*`, `/dashboard/*`,
     `/v1/recommendations/*`, substrate list/read) into
     `gateway/routers/<group>.py` as `APIRouter`s that import from (a)/(b).
   - (d) `main.py` shrinks to lifespan/middleware/`include_router` wiring.
   - (e) Verify: `import` + `collect-only` *and* boot the gateway against a DB,
     diffing the `/openapi.json` route set before/after.
3. **Migration prefix collisions (`0014_*`, `0043_*`).** Both files in each pair
   apply (files run in lexicographic order; no ledger), so this is a legibility
   smell, not data loss. **Not renumbered** because renaming applied migrations
   is correctness-sensitive and orthogonal to organization. CONTRIBUTING.md
   forbids new collisions.
4. **`uv.lock` is stale.** Adding one dev dep produced a 2,667-line `uv lock`
   delta, proving the lock didn't reflect `pyproject.toml` before this change.
   Left untouched (CI installs via `pip install -e .[dev]`, so it's not on the
   critical path). **Fix path:** regenerate `uv.lock` in a dedicated commit.
5. **11 pre-existing `collect-only` errors** from `tests.db_baseline` cross-imports.
   Unrelated to this change; **fix path:** add a root `conftest`/`tests/__init__`
   that makes `tests` importable, or convert those to fixtures.
6. **`monitoring/`** — empty, root-owned, unreferenced container-mount artifact.
   Not in git; safe to `sudo rm -rf` locally. If Prometheus/Grafana config is
   intended to be infra-as-code, add the real files and track them.

---

## 9. Branch & release strategy

The branch landscape is the biggest *process* risk (bigger than any file layout):
`main` has drifted from being the integration point (`cannonical` is 370 ahead of
it; `production` is 46 behind it). Recommended convergence (process, not done
here because it needs owner coordination and is not a code-organization edit):

1. Land this `refactor/codebase-layered-reorg` branch into `cannonical` (it is
   the live working line).
2. **Re-establish `main` as the single integration trunk**: merge `cannonical`
   into `main`, then cut `production`/`demo-deploy` *from* `main` so they stop
   diverging.
3. Adopt short-lived feature branches off `main` (already implied by
   `.github/workflows/enforce-main-source.yml`).
4. Treat `integration/ingestion-hardening` and `security/cat1-hardening` as
   merge-or-close: fold forward or delete to stop long-lived drift.

---

## 10. What was deliberately NOT done, and why

| Not done | Why |
|---|---|
| Split into multiple repos | Coupling + shared schema/tests/deploy make it net-negative today (§3). |
| Split the gateway god-file | Not runtime-verifiable here; too critical to refactor blind. Plan provided (§8.2). |
| Renumber migrations | Correctness-sensitive, orthogonal to organization (§8.3). |
| Regenerate `uv.lock` | Would bury a 2,667-line unrelated churn in a structural PR (§8.4). |
| Rename `lib/` subpackages | Already clean; pure churn. |
| Mass-rename routers to `router.py` | Convention set for *new* code; churning working files adds risk for cosmetics. |
| Push any branch / open a PR | Outward-facing; left for the owner to trigger. |

---

## 11. Net effect

`services/` now communicates the architecture at a glance, the boundaries that
matter are enforced in CI, the root and docs are tidy, and every consequential
choice — including the ones to *stop* — is written down with its rationale. The
change is fully reversible (one branch, history-preserving renames) and was
verified to be behavior-neutral to the limit of what static analysis can prove.

---

## 12. Addendum — partition durability (production fix)

The runtime verification surfaced a real production gap (separate from the
reorg): tenants backfilling arbitrary-age history (2/4/7+ years) can produce
observations whose `occurred_at` month has no partition. The Kafka **writer**
path already self-heals this (`observation_writer._attempt_partition_self_heal`,
±10y), but the **inline** ingestion path did not — a miss aborted the
per-envelope transaction with *"no partition of relation observations found for
row."*

**What shipped:**
- **Inline reactive self-heal** in [services/ingest/ingestion/core.py](services/ingest/ingestion/core.py)
  (`ingest()`): on an unnamed `CheckViolationError` the transaction rolls back
  (releasing its lock), then `ensure_partition_for_occurred_at` creates the
  covering month on a *separate* connection and the envelope retries once. This
  mirrors the writer and lands in-guardrail (≤~10y) backfill in clean, **prunable
  monthly partitions**. Placed in the inline `ingest()` wrapper — NOT the shared
  `ingest_from_draft` — so the writer keeps its own self-heal.
- **New helper** `ensure_partition_for_occurred_at` in
  [services/domain/observations/partitions.py](services/domain/observations/partitions.py)
  (guardrail = `WRITER_PARTITION_MAX_BACKFILL_LOOKBACK_DAYS`, ~10y, configurable;
  process-local cache so steady-state cost is ~zero).
- **Bug fix:** the writer's self-heal logged `extra={"created": …}`, but `created`
  is a reserved `LogRecord` attribute — so once INFO logging was active and a
  backfill row actually auto-created a partition, it raised
  `KeyError: "Attempt to overwrite 'created'"`. A genuine crash on the backfill
  path; renamed to `created_partitions`.

**What was rejected (and why):**
- **DEFAULT catch-all partition.** Considered for an absolute "never fails for
  any date" guarantee, but: (a) it masks the partition-miss, so it would silently
  pile years of backfill into one un-prunable partition (degrading every
  observations query) unless every insert path went proactive; (b) it overrides
  the writer's *deliberate* design to DLQ pathological/out-of-guardrail
  timestamps as bad data. Out-of-guardrail dates therefore remain rejected, by
  design.
- A first attempt put the ensure **proactively inside `ObservationRepository.insert`**
  — which deadlocked: the dedup `SELECT` already held `ACCESS SHARE` on
  `observations` in the open transaction, and the partition `CREATE`
  (`ACCESS EXCLUSIVE`) on a second connection waited on it forever. Reverted in
  favor of the reactive inline approach above (lock released before the CREATE).

**Future option (TODO in code):** if deep/unpredictable backfill (>10y, or very
high partition counts) becomes common, adopt **pg_partman** to own
forward+backward monthly partition management instead of the per-path self-heal +
hand-rolled maintenance worker — a deliberate consolidation project (extension
install + migrating the existing partitioned tables), not a bugfix.

**Verification:** inline 5y-old signal → `observations_2021_06` auto-created and
inserted; >10y → rejected; writer self-heal tests pass (incl. forced INFO
logging); consolidated DB slice 132 passed / 0 failed. No DEFAULT partition; no
schema migration.
