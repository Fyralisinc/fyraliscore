# Repo Split Plan — Core / Demo / (Benchmark)

> **Status:** Plan / proposal. No code moved yet.
> **Author:** Codebase scan + separation design.
> **Scope of this document:** Separate the current `fyraliscore` monorepo into a
> **pure core development repo** and a **demo repo** that consumes core. The
> **benchmark repo** is planned but deferred (see §12).

---

## 0. Decisions locked (from review)

These three choices drive the whole plan:

1. **Core↔Demo link — pip install from git.** Core becomes a normal installable
   Python package (`company-os`). Demo depends on it via a pinned git URL
   (`pip install -e` during local dev, a tag/commit pin for stable demos).
   Demo *overlays* core; core never imports demo.
2. **Cleanliness bar — full decouple via seams.** Core ends up with **zero**
   references to demo. Every current core→demo edge is inverted into an
   extension/hook that the demo repo registers. The unconditional "demo
   augmentation" in the reasoning hot path is removed from core.
3. **UI placement — split.** Core keeps the real product pages wired to real
   auth; the demo repo keeps `AutoDemoSession`, `mock-server.ts`, and the
   `*-mock.ts` files.

---

## 1. Target end state

Three repos, strict one-way dependency (arrows = "depends on"):

```
                 ┌──────────────────────┐
                 │  fyraliscore-core     │   pure runtime + libraries
                 │  (company-os package) │   services/, lib/, db/, core UI
                 └──────────┬───────────┘
                            │ pip install (git pin)
            ┌───────────────┴────────────────┐
            │                                 │
   ┌────────▼─────────┐             ┌─────────▼─────────┐
   │  fyraliscore-demo │             │ fyraliscore-bench │
   │  demo overlay +   │             │ benchmark harness │
   │  snapshots + UI   │             │ (deferred, §12)   │
   │  shell + Pelago   │             │                   │
   └──────────────────┘             └───────────────────┘
```

**Invariants after the split**

- `fyraliscore-core` builds, tests, lints (`lint-imports`), and runs the gateway
  + workers **with the demo package not installed**. No `import` of any demo
  module anywhere in `services/` or `lib/`.
- `fyraliscore-demo` adds: demo snapshots, the demo HTTP surface
  (`/v1/demo/*`), demo session/budget/SSE logic, the Pelago seed, the UI demo
  shell + mocks, and demo-only tests. It pins a core version.
- `fyraliscore-bench` (later) depends on core the same way demo does.

---

## 2. Why this is not a clean cut (the coupling map)

The codebase already has **good layering** (`lib` ⟂ `services`, enforced by
import-linter), and two of the three "demo-ish" trees are clean leaves. But the
**runtime demo** is woven into core in a handful of spots. There are effectively
**two different "demo" concepts**, which must not be confused:

| Concept | Location | Nature | Cut difficulty |
|---|---|---|---|
| **Demo data generator** | root `demo/` (+ `demo/snapshots/`) | Standalone LLM tool, only imports `lib.llm.provider` | **Trivial** — clean leaf, just move it |
| **Runtime demo surface** | `services/product/demo/` (18 files) + gateway/domain/reasoning hooks | Mounted into the live gateway; imported by core | **Hard** — needs the seams in §4 |

The runtime demo's inbound edges from core (the things that make core "know"
about demo today):

| # | Core file | Line(s) | Edge to break |
|---|---|---|---|
| 1 | `services/app/gateway/route_mounts.py` | 50–52 | `from services.product.demo.router import demo_router` |
| 2 | `services/app/gateway/main.py` | 27, 351–366, 730 | calls `ensure_demo_seed(pool)` on startup |
| 3 | `services/app/gateway/demo_seed.py` | whole file | Pelago `demo_configs` seed (demo-only) |
| 4 | `services/app/gateway/middleware.py` | 73–76 | hardcoded `/v1/demo/*` public-path prefixes |
| 5 | `services/domain/models/repo.py` | 1538 | `from services.product.demo.sse import publish_recommendation_event` |
| 6 | `services/product/recommendations/handlers.py` | 319, 388, 857 | same SSE import (3 call sites) |
| 7 | `services/reasoning/think/reason.py` | 907–1007 | **unconditional** "demo augmentation" block in the think hot path |
| 8 | `services/app/gateway/ceo_view_wiring.py` | 151–205 | optional, prod-guarded mount of `simulation.*` (same seam pattern) |
| 9 | DB migrations | `0023/0026/0028` + refs in `0036/0037` | demo tables + seed live in core's migration chain |
| 10 | `conftest.py` | 241–326 | seeds demo companies (Truss/Northwind/Meridian/Pelago) on `fresh_db` |
| 11 | UI | `ui/src/shell/AutoDemoSession.tsx`, `ui/mock-server.ts`, `ui/src/api/*-mock.ts` | UI boots straight into a Pelago demo session |

Notes:
- `lib/shared/types.py:559` `is_demo: bool` is a **benign tenant attribute** —
  it is a schema field, not a coupling. **Stays in core.**
- `/finance/`, `/slack/`, `/debug/`, `/simulation/` public prefixes in the
  middleware are **dev tooling**, not the Pelago demo. They stay in core (behind
  the same extension mechanism so the list isn't hardcoded — see §4, seam C).
- Edge #7 is the riskiest: it is labelled "demo" but **always runs** and even
  emits an always-on `structlog.warning("augmentation.entry", …)`. It mutates
  the reasoning context (full active ledger + region allow-list) on every think
  run. This is treated as a **core-correctness item**, not just a move (see §4
  seam D and §13 Q1).

---

## 3. Allocation table — where every top-level path goes

Legend: **CORE** = fyraliscore-core · **DEMO** = fyraliscore-demo ·
**BENCH** = benchmark repo (later) · **DROP** = delete / don't carry ·
**SEAM** = stays in core but is refactored to remove the demo edge.

| Path | Destination | Notes |
|---|---|---|
| `services/app/` | CORE (SEAM) | except demo router mount, demo_seed call, demo public-paths (§4 A–C) |
| `services/app/gateway/demo_seed.py` | **DEMO** | whole file is Pelago seed |
| `services/domain/` | CORE (SEAM) | remove SSE import in `models/repo.py` (§4 E) |
| `services/ingest/` | CORE | incl. `synthetic/` (core test/dev injection infra) |
| `services/platform/` | CORE | 100% core, no demo |
| `services/product/` | CORE (SEAM) | **except** `services/product/demo/` → DEMO; remove SSE imports in `recommendations/handlers.py` (§4 E) |
| `services/product/demo/` (18 files + tests) | **DEMO** | entire runtime demo surface |
| `services/reasoning/` | CORE (SEAM) | resolve edge #7 augmentation (§4 D) |
| `services/workers/` | CORE | 100% core |
| `lib/` | CORE | foundation; `lib/nexus/`, `lib/topology/` are empty (no tracked source) → DROP the stray dirs |
| `db/migrations/` | CORE | demo **table DDL** stays (low-risk); demo **seed rows** move (§5) |
| `db/seed/predictions_seed.sql` | CORE | not demo-specific |
| `demo/` (generator) + `demo/snapshots/` | **DEMO** | clean leaf; 3.5M of snapshots |
| `simulation/` | CORE (dev tool) | dogfood signal authoring; mount via seam #8. *Alt: DEMO — see §13 Q2* |
| `mocks/starscape.html` | **DEMO** | design artifact (or a design folder) |
| `ui/` (pages, real API clients) | CORE | product frontend |
| `ui/src/shell/AutoDemoSession.tsx`, `ui/mock-server.ts`, `ui/src/api/*-mock.ts` | **DEMO** | demo shell + mock layer (§6) |
| `bench/` | BENCH | **source is gone** (only `.pyc`); see §12 |
| `tests/` | split | core-logic tests → CORE; `tests/integration/test_demo_end_to_end.py` → DEMO (§7) |
| `conftest.py` | CORE (SEAM) | strip demo seeding into a demo conftest (§5, §7) |
| `contracts/http-routes.json` | CORE | API route contract |
| `docker-compose.yml` | split | core stack (gateway+workers+data plane) → CORE; `ui` service + demo env → DEMO overlay (§9) |
| `docker-compose.dev.yml` / `.sandbox.yml` / `.per-source.yml` | CORE | dev/sandbox/ingestion infra |
| `Dockerfile`, `Dockerfile.ui`, `nginx*`, `monitoring/` | CORE (UI image → DEMO if UI demo-only build) | infra |
| `docs/`, `mkdocs.yml`, `docs_hooks.py` | CORE | core engineering docs; demo-specific pages (e.g. `docs/ingestion/*dm-demo*`, `docs/mockups/`) → DEMO |
| `CODEBASE-*.md`, `FYRALIS.md`, `README.md`, `CONTRIBUTING.md` | CORE | narrative docs; add a demo README in DEMO |
| `specs/`, `.specify/`, `.agents/`, `.claude/` | CORE | SDD + agent tooling (demo repo gets its own minimal `.claude/`) |
| `scripts/` | split | core dev/ops scripts → CORE; Pelago/snapshot demo runners → DEMO (§8) |
| `.env*.example` | split | core vars → CORE; demo vars → DEMO |
| **Artifacts / caches** (`.venv`, `__pycache__`, `.ruff_cache`, `.pytest_cache`, `.hypothesis`, `.import_linter_cache`, `company_os.egg-info`, `ui/dist`, `ui/node_modules`, `ui/test-results`, `lsob/`) | DROP | already gitignored; do not carry into new repos |
| `tests/retrieval_e2e/_last_run.json`, `tests/synthesis_harness/_last_run.json` | DROP + untrack | **tracked artifacts** — see §8 |

---

## 4. The decoupling seams (core→demo)

Core gets **one small extension mechanism** plus **one event bus**. Both live in
core; the demo repo registers against them. With the "pip install from git"
model, discovery is by Python **entry points** so core needs no env var and no
knowledge of demo's module name.

### Seam A — Gateway extension registry (covers edges #1, #2, #8)

New core module, e.g. `services/app/gateway/extensions.py`:

```python
# core
@dataclass
class GatewayExtension:
    routers: list[APIRouter] = []
    startup_hooks: list[Callable[[Pool], Awaitable[None]]] = []
    public_path_prefixes: tuple[str, ...] = ()

def discovered_extensions() -> list[GatewayExtension]:
    # importlib.metadata.entry_points(group="company_os.gateway_extensions")
    ...
```

`mount_gateway_routes` / `main.py` lifespan iterate
`discovered_extensions()` instead of importing demo directly. When the demo
package isn't installed, the list is empty and the gateway runs clean.

Demo repo declares (in its `pyproject.toml`):

```toml
[project.entry-points."company_os.gateway_extensions"]
demo = "fyralis_demo.gateway:extension"
```

…and `fyralis_demo/gateway.py` returns a `GatewayExtension` carrying the
`demo_router`, the `ensure_demo_seed` startup hook, and the `/v1/demo/*` public
prefixes. The `simulation` optional mount (#8) moves to the same registry (its
prod-guard logic comes along).

### Seam B — Startup hooks

Folded into Seam A (`startup_hooks`). `demo_seed.py` + the Pelago config move to
the demo repo unchanged.

### Seam C — Public-path prefixes (edge #4)

The middleware's `_PUBLIC_PATH_PREFIXES` becomes: a **core base set**
(`/webhooks/`, `/debug/`, `/finance/`, `/slack/`, `/rendering/`, `/view/ceo/`)
**plus** prefixes contributed by discovered extensions. Demo contributes
`/v1/demo/companies` and `/v1/demo/sessions/start`.

### Seam D — Reasoning augmentation (edge #7) — **needs a correctness call**

Extract the lines 907–1007 block in `reason.py` into a named, **optional
retrieval post-processor hook**. Core ships the strict default (no augmentation);
demo registers the "full active ledger" augmentation via a small reasoning
extension point (mirrors Seam A, e.g. entry-point group
`company_os.reasoning_hooks`). **Also delete the always-on
`augmentation.entry` warning** — that is log noise that belongs in debug capture,
not every think run. See §13 Q1 — if the team decides this behavior is actually
desirable in production, keep it in core but make it conditional and quiet
rather than demo-labelled.

### Seam E — Domain event bus (edges #5, #6)

Add a tiny in-process pub/sub to core (`lib/shared/events.py` or
`services/platform/events.py`):

```python
# core
async def publish(topic: str, **payload): ...
def subscribe(topic: str, handler): ...
```

`models/repo.py` and `recommendations/handlers.py` call
`await publish("recommendation.created", …)` — core knows nothing about SSE.
The demo overlay subscribes `recommendation.created` → its existing SSE fan-out.
(The auto-accept side effect currently next to the SSE publish in
`models/repo.py:1555` is **core logic** and stays; only the SSE publish moves
behind the bus.)

> Net core change for the seams: ~8 files touched, ~2 new small modules, 0 demo
> imports remaining. Verified by adding an import-linter contract:
> `services` + `lib` must not import the demo package.

---

## 5. Database & migrations strategy

Constraints found:
- Demo tables (`demo_configs`, `demo_sessions`, `demo_session_costs`) are created
  mid-chain by `0023/0026/0028`.
- Core migrations `0036` (RLS permissive default — lists `'demo_sessions'` in a
  global/RLS-exempt allowlist) and `0037` (tenant FKs — comments only) reference
  them. Renumbering/removing mid-chain migrations is **destructive and risky**.
- 92 total migrations.

**Recommended (phased, low-risk):**

- **Keep the demo table DDL in the core chain.** Three empty global tables cost
  almost nothing and avoid a dangerous mid-chain rewrite. Core stays runnable
  and its RLS/FK migrations stay valid.
- **Move all demo *data* out of core:**
  - Pelago seed in `demo_seed.py` → demo repo (Seam B).
  - The `INSERT INTO demo_configs …` rows in migration `0028` (and the 3
    companies in `0023`) → replace with no-op / leave table empty; demo seeds
    them at runtime via its startup hook.
  - `conftest.py` `_seed_demo_configs()` (lines 241–326) → move to the **demo
    repo's** `conftest.py`. Core's `fresh_db` stops seeding demo companies.
- **Future demo schema changes** live in a **demo-owned migration namespace**
  applied *after* the core chain (same pattern the project already uses for the
  overlaid ingestion migrations 0049–0079).

**Optional later (full eviction, Phase 2):** add a core migration that drops the
three demo tables and remove their mention from the `0036` allowlist; demo
recreates them in its own migrations. Only do this once demo fully owns its
schema and the team accepts the chain edit.

---

## 6. UI split strategy

The UI is a single Vite/React app that today boots **only** into a Pelago demo
session. The split:

- **CORE keeps:** `ui/src/pages/*` (today-v2, model-v2, forecasts, ledger),
  `ui/src/api/*-client.ts` (real clients keyed off `VITE_API_BASE`),
  `ui/src/debug/*`, routing, build config.
- **DEMO keeps:** `ui/src/shell/AutoDemoSession.tsx`, `ui/mock-server.ts`, the
  ten `ui/src/api/*-mock.ts` files, the `dev:mock` / `USE_MOCK` path, and the
  Pelago e2e tests.
- **Prerequisite:** core needs a **non-demo entry/auth shell** so the product
  pages can mount without `/v1/demo/sessions/start` minting the token. The
  bearer-session system already exists (`VIEW_CEO_TOKEN`, the session token
  model); core just needs a thin login/bootstrap that the demo shell replaces.
  This is the one piece of genuinely new core UI work the split requires.
- **Mechanics:** demo consumes core UI as a dependency. Cleanest is to publish
  the core UI pages/clients as an npm-style internal package (or a git submodule
  for the `ui/` subtree) and have the demo UI import them + swap the shell.
  Practically, since core+demo are co-developed, a git submodule of `ui/` or a
  thin "demo shell wraps core app" composition is fine for v1.

---

## 7. Tests & CI strategy

Tests follow their subject. Current layout: ~393 co-located test files under
`services/`+`lib/`, plus 54 in top-level `tests/`.

- **CORE tests:** all co-located `services/**/tests` and `lib/**/tests` (minus
  `services/product/demo/tests/`), plus `tests/unit/`, `tests/quality_replay/`,
  `tests/e2e/`, `tests/load/`, `tests/real_llm/`, `tests/retrieval_e2e/`,
  `tests/synthesis_harness/`.
- **DEMO tests:** `services/product/demo/tests/*` and
  `tests/integration/test_demo_end_to_end.py` (and any other
  `tests/integration/*` that exercises `/v1/demo/*`).
- **Shared fixtures:** `conftest.py` splits — DB/pool/RLS fixtures stay in core;
  demo-seeding fixtures move to the demo repo's conftest.
- **CI:**
  - Core CI: build package, run lint-imports (with the new "no demo import"
    contract), run core test suite, `mkdocs build --strict`, build the gateway
    + UI(core) images.
  - Demo CI: install core (pinned), install demo overlay, run demo tests, run
    the demo e2e (`USE_MOCK=1` playwright), build demo image(s).
  - `.github/workflows/` split accordingly; the existing hermetic-CI fixes
    (MASTER_KEK / DISCORD_BOT_TOKEN defaults) carry to core.

---

## 8. Mess / cleanup list (do before/while extracting)

**Tracked artifacts to untrack (and gitignore):**
- `tests/retrieval_e2e/_last_run.json`
- `tests/synthesis_harness/_last_run.json`

  These are run outputs. The `.claude/hooks/auto-commit.sh` Stop-hook does
  `git add -A && git commit` and keeps re-snapshotting `_last_run.json` (that is
  the source of the recent `auto: snapshot …` commits). **Recommend:** untrack
  the two files, add them to `.gitignore`, and reconsider/scope the auto-commit
  hook so it doesn't manufacture snapshot commits in the new repos.

**Local-only junk to *not* carry into the new repos** (already gitignored, just
don't copy the working tree blindly — start new repos from a clean checkout):
- `.venv/` (1.1G), `.ruff_cache/` (12M), `.hypothesis/` (6.3M),
  `.pytest_cache/`, `.import_linter_cache/`, `company_os.egg-info/`,
  `__pycache__/` everywhere, `ui/dist/`, `ui/node_modules/`, `ui/test-results/`,
  `lsob/` (external tool, gitignored).

**Empty/dead dirs:** `lib/nexus/`, `lib/topology/` have **no tracked source**
(only stray `__pycache__`). Delete the stray dirs; don't recreate.

**`bench/`:** only `.pyc` bytecode exists — **no tracked `.py`, no git history**
for the source. See §12.

---

## 9. Packaging — core as an installable, demo depends via git

**Core (`fyraliscore-core`):**
- `pyproject.toml` already declares `company-os` with
  `packages.find` over `lib*` + `services*`. Keep that. Add the demo entry-point
  *group definitions* are **not** needed in core (consumers declare them).
- Add a console/extension discovery (Seam A) and the import-linter "no demo"
  contract.
- Tag releases (`v0.1.0`, …). Demo pins a tag or commit.

**Demo (`fyraliscore-demo`):**
```toml
[project]
name = "fyralis-demo"
dependencies = [
  "company-os @ git+ssh://git@github.com/<org>/fyraliscore-core@v0.1.0",
]
[project.entry-points."company_os.gateway_extensions"]
demo = "fyralis_demo.gateway:extension"
```
Local dev uses an **editable** core checkout: `pip install -e ../fyraliscore-core`.

**Compose:** core ships `docker-compose.yml` (gateway + workers + data plane).
Demo ships a `docker-compose.demo.yml` overlay that adds the `ui` (demo build),
sets demo env, and runs the demo-enabled gateway image (core image +
`pip install fyralis-demo`). The demo Dockerfile is `FROM` the core image then
installs the overlay — so demo truly "uses" core.

---

## 10. Execution workflow (phased)

Do the **decoupling inside the current monorepo first** (so it stays green at
every step), *then* physically extract. This keeps `main` releasable throughout.

**Phase 0 — Hygiene (½ day)**
1. Untrack the two `_last_run.json` files; gitignore them; tame the auto-commit
   hook.
2. Delete stray empty dirs (`lib/nexus`, `lib/topology`).

**Phase 1 — Build the seams in-place (the bulk of the work)**
3. Add core extension registry (Seam A/B/C) + event bus (Seam E). No behavior
   change yet — core still calls demo, but via the new indirection.
4. Resolve the reasoning augmentation (Seam D) — extract behind a hook; default
   core path strict; remove the always-on warning. Get sign-off on §13 Q1.
5. Flip `services/product/demo/*`, `demo_seed.py`, and the simulation mount to
   register through the seams. Delete the direct `from services.product.demo…`
   imports from core files (#1, #2, #5, #6, #8).
6. Add the import-linter contract: `lib`+`services` must not import
   `services.product.demo` (will become "must not import the demo package").
7. Keep everything in one repo, all tests green, gateway runs **with demo
   present**. This is the safety checkpoint.

**Phase 2 — Carve core (preserve history)**
8. Create `fyraliscore-core` via `git filter-repo` from a clean checkout,
   keeping core paths and history, **excluding** demo paths, `bench/`, caches,
   and the snapshots. Verify: `pip install -e .`, `pytest`, `lint-imports`,
   `mkdocs build --strict`, gateway boots with **no demo installed**.
9. DB: apply the §5 data-eviction (demo tables stay, seeds leave core conftest).

**Phase 3 — Carve demo**
10. Create `fyraliscore-demo` from history filtered to the demo paths
    (`services/product/demo/`, `demo_seed.py`, root `demo/` + snapshots, demo
    UI shell + mocks, demo tests/migrations/scripts/docs).
11. Add demo `pyproject.toml` pinning core; wire the entry points; move the demo
    conftest seeding; add `docker-compose.demo.yml` + demo Dockerfile.
12. Verify end to end: `pip install -e ../fyraliscore-core && pip install -e .`,
    gateway with demo extension mounts `/v1/demo/*`, UI demo shell boots Pelago,
    `test_demo_end_to_end` passes.

**Phase 4 — Cutover**
13. Freeze the monorepo `main` (or convert it to core). Update CI in both repos.
    Update READMEs/CONTRIBUTING to describe the two-repo dev loop.

> History preservation: use `git filter-repo` (per-repo path include/exclude) so
> blame/history survive. Alternative if history isn't required: fresh repos with
> a single import commit (faster, loses blame).

---

## 11. Day-to-day dev workflow after the split

- **Core-only work:** clone `fyraliscore-core`, `pip install -e ".[dev]"`,
  develop, test, ship. Demo not involved.
- **Demo work / running the demo:** clone both side by side;
  `pip install -e ../fyraliscore-core` inside the demo venv so demo tracks live
  core; run gateway with the demo extension installed → `/v1/demo/*` appears.
- **Releasing a stable demo:** bump the core pin in demo's `pyproject.toml` to a
  core tag; the demo is then reproducible against a frozen core.
- **Adding a product surface (core) that the demo showcases:** build it in core
  behind the normal routers; if it needs a demo-only flourish, the demo overlay
  subscribes to the event bus / registers an extension — never edit core to know
  about the demo.

---

## 12. Benchmark repo (deferred) + a finding to flag

The third repo (`fyraliscore-bench`) is **out of scope for now** but the plan
reserves the same shape: it depends on core via the git pin and adds
benchmark/profiling harnesses + scenarios.

**Important finding:** the existing `bench/` tree has **no recoverable source**
in this repo — `git ls-files bench/` returns 0 `.py` files, there is no `.py` in
the working tree, and no git history touches `bench/*.py`. Only compiled `.pyc`
remain under `bench/__pycache__/` (referencing modules like `cli`, `runner`,
`stats`, `store`, `report`, `dimensions/*`, `profiling/*`). Before standing up
the benchmark repo, the source must be **recovered** (from another branch/clone,
a teammate's checkout, or last resort decompiled from the `.pyc`) or
**rewritten**. Flagging now so it isn't discovered late.

---

## 13. Risks & open questions

- **Q1 — Reasoning "augmentation" (edge #7).** It is demo-labelled but runs
  unconditionally and changes what the LLM sees on every think run, plus emits
  an always-on warning. **Decision needed:** (a) it's a demo crutch → move to
  the demo reasoning hook (core default = strict); or (b) it's actually a
  correctness improvement → keep in core but make it conditional + quiet. This
  is the highest-risk item because it touches the reasoning hot path. Default
  recommendation: (a), and treat the always-on `warning` as a bug to remove
  either way.
- **Q2 — `simulation/` home.** Classified here as **core dev tooling** (it
  injects via core's synthetic path and is the dogfood authoring harness). It is
  defensible to move it to the demo repo instead since it's storytelling-
  adjacent. Pick one; it changes which repo owns ~280K + the gateway sim mount.
- **Q3 — UI distribution.** Submodule vs internal npm package vs "demo shell
  composes core app." Submodule is simplest for co-developed repos; a package is
  cleaner long-term. Needs a call before §6 mechanics are finalized.
- **Q4 — Migration eviction depth.** §5 keeps demo table DDL in core (safe). If
  the team wants core *truly* free of demo schema, schedule the Phase-2
  eviction migration — but it edits the chain and the `0036` allowlist.
- **Q5 — History preservation.** `git filter-repo` (keep blame) vs fresh import
  commit (faster). Affects Phase 2/3 effort.
- **Risk — auto-commit hook.** While doing the split, `.claude/hooks/auto-commit.sh`
  will `git add -A && commit` on Stop. Disable/scope it during the migration so
  it doesn't interleave snapshot commits into the filter-repo work.

---

## Appendix — quick reference: files that move to DEMO

```
services/product/demo/**                      (18 files incl. tests)
services/app/gateway/demo_seed.py
demo/**                                        (generator + snapshots, 4.0M)
mocks/starscape.html
ui/src/shell/AutoDemoSession.tsx
ui/mock-server.ts
ui/src/api/*-mock.ts                           (10 files)
tests/integration/test_demo_end_to_end.py     (+ other /v1/demo integration tests)
conftest.py :: _seed_demo_configs (241–326)    (extract to demo conftest)
scripts/* demo runners                         (Pelago/snapshot/finance-demo)
docs/ demo-specific pages                      (dm-demo, mockups)
demo env vars from .env*.example
```

## Appendix — core files refactored (SEAM, stay in core, demo edge removed)

```
services/app/gateway/route_mounts.py    (#1 → extension registry)
services/app/gateway/main.py            (#2 → startup hooks)
services/app/gateway/middleware.py      (#4 → contributed public prefixes)
services/app/gateway/ceo_view_wiring.py (#8 → simulation via registry)
services/domain/models/repo.py          (#5 → event bus)
services/product/recommendations/handlers.py (#6 → event bus)
services/reasoning/think/reason.py      (#7 → reasoning hook, see Q1)
conftest.py                             (strip demo seeding)
db/migrations/0023,0028                 (drop seed rows; keep DDL)
pyproject.toml                          (+ import-linter "no demo" contract)
+ new: services/app/gateway/extensions.py, lib/shared/events.py
```
