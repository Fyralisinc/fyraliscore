# Contributing to Fyralis Core

This guide covers the **conventions and guardrails** that keep the codebase
maintainable at scale. For *what the system does*, read
[CODEBASE-ARCHITECTURE.md](CODEBASE-ARCHITECTURE.md). For *why the codebase is
organized the way it is*, read [CODEBASE-MANAGEMENT.md](CODEBASE-MANAGEMENT.md).
For local setup, read [README.md](README.md).

## 1. Repository layout

```
lib/            Shared libraries — must not import services/ (enforced).
services/       Backend, grouped into architectural layers:
  app/          HTTP/WS entrypoints & dispatch (gateway, webhooks, realtime)
  product/      CEO-facing surfaces (greeting, today, forecasts, query, ...)
  reasoning/    Think pipeline, retrieval, topology, scoring
  ingest/       Signal intake, integrations, synthetic signals
  domain/       Core persisted substrate (models, acts, resources, ...)
  platform/     Cross-cutting infra (access_control, execution)
  workers/      Background worker packages (already layer-shaped)
db/migrations/  SQL migrations, applied in filename order.
ui/             Vite/React/TypeScript frontend.
tests/          Cross-service integration / real-LLM / e2e suites.
scripts/        CLI utilities, dogfood/sandbox orchestration, worker launchers.
docs/           Reference docs, source integration docs, history archive.
```

Each `services/<layer>/` is a **PEP 420 namespace package** (no `__init__.py`,
matching `services/` itself) and carries a `README.md` describing its role.

## 2. Import discipline (enforced)

Layer boundaries are enforced by **import-linter**. Run it locally before
pushing; CI runs it as the `architecture` job:

```bash
lint-imports
```

Contracts live in `pyproject.toml` under `[tool.importlinter]`. Today they
encode two invariants that are *empirically true* (so a failure is always a
real regression, never an aspirational rule the code already breaks):

1. **`lib` never imports `services`** (a small, documented set of lazy/test
   exceptions is whitelisted).
2. **`reasoning` does not *directly* import `app`/`product`/`ingest`** — i.e.
   no new upward edges from the reasoning core into higher layers.

Rule of thumb: **import downward, not upward.** A higher layer may depend on a
lower one; the reverse is a smell. The one known upward edge that exists today
(`services/domain/models/repo.py` → `product`/`reasoning`) is tracked debt —
see CODEBASE-MANAGEMENT.md §"Known coupling". Do not add more.

## 3. Naming conventions

- **Packages:** one cohesive domain per package, `snake_case`, placed in the
  layer that matches its role.
- **HTTP routers:** new routers should be named `router.py` and expose an
  `APIRouter`. (Historically these were inconsistently `router.py` /
  `routes.py` / `api.py`; standardize new code on `router.py` and migrate
  opportunistically — don't churn working files just to rename.)
- **Repositories:** DB access for a package lives in `repo.py` exposing a
  `*Repo` class built on the shared `lib.shared.db` pool.
- **Tests:** co-locate unit/integration tests in `<package>/tests/`. Reserve
  top-level `tests/` for *cross-service* and real-LLM/e2e suites.

## 4. Database migrations

- Add a new file `db/migrations/NNNN_short_name.sql`; they are applied in
  **filename (lexicographic) order**. There is no migration ledger — files are
  re-applied idempotently (`CREATE ... IF NOT EXISTS`, guarded `DO` blocks).
- Keep the four-digit prefix **unique and monotonically increasing**. Two
  duplicate-prefix pairs exist from a historical merge (`0014_*`, `0043_*`);
  both files in each pair apply, but do not add new collisions.
- After changing schema, run `python scripts/check_schema_drift.py`.

## 5. Adding things

See CODEBASE-ARCHITECTURE.md §16 "How to Extend the System" for step-by-step
recipes (new ingestion channel, new proposition kind, new UI surface, new
worker). When adding a **new service package**, choose the layer whose role it
matches, drop the package under `services/<layer>/`, and add it to the layer
map in CODEBASE-ARCHITECTURE.md.

## 6. Local checks before pushing

```bash
ruff check --select E9,F63,F7,F82,F821,F811,F401 .   # the CI ruleset
lint-imports                                         # layer boundaries
pytest -m "not slow and not real_llm"                # fast tests (needs Postgres)
```

## 7. Branching

`main` is the integration branch; feature work branches off it. See
CODEBASE-MANAGEMENT.md §"Branch & release strategy" for the current branch
landscape and the recommended convergence plan.
