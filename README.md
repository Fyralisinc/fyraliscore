# Fyralis Core

Organizational intelligence runtime — **backend only**. A multi-tenant FastAPI
gateway, a Postgres (pgvector) data store, Ollama-backed embeddings, and worker
processes for asynchronous reasoning and post-commit propagation.

> The **UI, demo, and simulation** live in a separate overlay repo,
> **[fyraliscore-demo](https://github.com/Fyralisinc/fyraliscore-demo)**, which
> installs this package and plugs in via entry-point seams. Core depends on
> nothing in that repo.

For the architecture and module-level reference, see
[CODEBASE-ARCHITECTURE.md](docs/reference/CODEBASE-ARCHITECTURE.md).

This document is the end-to-end setup guide for running the backend locally.

---

## 1. Prerequisites

Install these on your host before starting. Versions below are what the
codebase is developed against; minor patch differences are fine.

| Tool                | Version            | Notes                                                   |
| ------------------- | ------------------ | ------------------------------------------------------- |
| Python              | 3.11+              | `pyproject.toml` requires `>=3.11`                      |
| Docker + Compose v2 | recent             | Brings up Postgres (pgvector) and Ollama                |
| `psql` client       | any 14+            | Used to apply DB migrations                             |
| `curl`              | any                | Used by `dogfood_up.sh` health checks                   |

macOS quick install:

```bash
brew install python@3.11 postgresql@16
brew install --cask docker
```

Make sure Docker Desktop is running before continuing.

---

## 2. Clone and configure environment

```bash
git clone <your-private-repo-url> fyraliscore
cd fyraliscore

# Copy the env template and fill in real values.
cp .env.example .env
```

Open `.env` and set, at minimum:

- `DEEPSEEK_API_KEY` — required when `LLM_PROVIDER=deepseek` (the default).
  If you prefer Anthropic, OpenAI, or Codex, set `LLM_PROVIDER` and the
  matching `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` / `CODEX_API_KEY` instead.
  For local Codex dogfood, `LLM_PROVIDER=codex` can also reuse
  `~/.codex/auth.json` after `codex login`; `CODEX_TRANSPORT=app-server`
  keeps one Codex app-server process warm for faster repeated Think calls.
- All other variables ship with sensible local-dev defaults. Review them
  if you've changed Postgres or Ollama ports.

Optional: create a second overlay file `.env.dogfood` for values that
differ between your day-to-day env and the dogfood stack (model choices,
worker/sweeper intervals, etc.). `scripts/dogfood_up.sh` sources `.env`
first and `.env.dogfood` last, so dogfood values win. Both files are gitignored.

> **Security note.** `.env` and any `.env.*` variant other than
> `.env.example` are gitignored. Never commit real keys. Rotate any key
> that has been pasted into a chat, log, or doc.

---

## 3. Start Postgres and Ollama

The repo ships a `docker-compose.yml` with the backend stack. For local setup
you typically only need `postgres` (pgvector/pg16) and `ollama` (with the
`nomic-embed-text` model auto-pulled on first start):

```bash
docker compose up -d postgres ollama
```

Wait until both are healthy:

```bash
docker compose ps
# postgres should be "healthy" — wait for the healthcheck to pass.
# ollama takes a minute on first start while it pulls the embed model.
```

Verify Ollama has the embedding model:

```bash
curl -s http://localhost:11434/api/tags | grep nomic-embed-text
```

If you don't see it, pull it manually:

```bash
docker compose exec ollama ollama pull nomic-embed-text
```

---

## 4. Python environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

This installs the runtime dependencies plus dev tools (pytest,
hypothesis, respx, hdbscan, scikit-learn, etc.).

---

## 5. Apply database migrations

There is no production migration runner — the integration tests apply
migrations programmatically. For local setup, apply the SQL files in
order with `psql`:

```bash
# Convenience: source the DB DSN from .env.
set -a && source .env && set +a

for f in db/migrations/*.sql; do
  echo "Applying $f"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"
done
```

The migrations are idempotent (`CREATE TABLE IF NOT EXISTS`, partition
DO blocks, etc.), so re-running is safe.

Sanity-check the schema lines up with what the code expects:

```bash
python scripts/check_schema_drift.py
```

A zero exit code means the live DB matches the expected schema.

> Tenant + persona seeding for the demo (the old `seed_dogfood_tenant.py`)
> moved to the overlay repo. The backend creates tenants through the normal
> onboarding flow; for local dev you can use `DEFAULT_TENANT_ID` from `.env`.

---

## 6. Bring up the backend stack

The dogfood script starts the gateway, Think worker, post-commit worker, and
topology sweeper. It writes logs to `/tmp/company_os_logs/` and PIDs to
`/tmp/company_os_dogfood.pids`.

```bash
./scripts/dogfood_up.sh
```

You should see:

```
=== Company OS dogfood backend up ===
  Gateway:         http://localhost:8000
  Healthz:         curl http://localhost:8000/healthz
```

To tail logs / inspect DB state / stop everything:

```bash
./scripts/dogfood_logs.sh
./scripts/dogfood_inspect.sh
./scripts/dogfood_down.sh
```

> To run the **UI** against this backend, check out
> [fyraliscore-demo](https://github.com/Fyralisinc/fyraliscore-demo) and follow
> its README (it points the UI at the gateway via `VITE_API_BASE`).

---

## 7. Running tests

The test suite uses a real Postgres (no mocks), so the `docker compose`
services from step 3 must be running.

```bash
# Fast unit + integration tests.
pytest

# Subset filters:
pytest -m integration       # tests that require live Postgres
pytest -m ollama            # tests that require live Ollama
pytest -m "not slow"        # skip slow tests
```

Real-LLM tests are gated behind `RUN_REAL_LLM=1` and require a working
provider key:

```bash
RUN_REAL_LLM=1 pytest -m real_llm
```

Architecture import boundaries are enforced by import-linter:

```bash
lint-imports
```

---

## 8. Running individual processes

If you don't want the full dogfood stack, you can run the components
individually:

```bash
# Gateway only
uvicorn services.app.gateway.main:app --host 0.0.0.0 --port 8000 --reload

# Think worker
python scripts/run_think_worker.py

# Post-commit worker
python scripts/run_post_commit_worker.py

# Topology sweeper
python scripts/run_topology_sweeper.py
```

---

## 9. Common issues

**`ERROR: .env not found`** — copy `.env.example` to `.env` and fill in
`DEEPSEEK_API_KEY` or your chosen provider's key. For Codex dogfood, set
`LLM_PROVIDER=codex` and either provide `CODEX_API_KEY`/`OPENAI_API_KEY` or
run `codex login` so `~/.codex/auth.json` exists. ChatGPT-style Codex login
uses `CODEX_TRANSPORT=app-server`; platform API keys should use
`CODEX_TRANSPORT=responses`.

**`ERROR: Postgres not running`** — `docker compose up -d postgres` and
wait for the healthcheck. `pg_isready` must succeed.

**`ERROR: Ollama not reachable at http://localhost:11434`** — Ollama
takes ~30s on cold start while pulling the embed model. Check
`docker compose logs ollama`.

**Schema drift errors at startup** — re-run the migrations loop in
step 5; one of the new migrations may not have been applied.

**Port 5432 already in use** — you have a host Postgres running. Stop
it (`brew services stop postgresql`) or change the port in
`docker-compose.yml` and `DATABASE_URL`.

---

## 10. Layout

```
.
├── CONTRIBUTING.md           # Conventions, import rules, how to extend
├── docs/                     # Internal MkDocs site — incl. "Codebase reference":
│                             #   CODEBASE-ARCHITECTURE.md (module map + §0 layer map),
│                             #   CODEBASE-MANAGEMENT.md (the why), FYRALIS.md (comprehensive)
├── README.md                 # This file
├── .env.example              # Env template (copy to .env)
├── docker-compose.yml        # Backend stack (gateway + workers + data plane)
├── pyproject.toml            # Python package, dev deps, import-linter contracts
├── conftest.py               # Pytest fixtures (DB pool, etc.)
├── db/migrations/            # SQL migrations, applied in filename order
├── lib/                      # Shared lower layer (shared/db, llm, embeddings, …)
├── services/                 # Backend, grouped into architectural layers:
│   ├── app/                  #   HTTP/WS entry & dispatch (gateway, webhooks, realtime)
│   ├── product/              #   CEO-facing surfaces (greeting, today, query, …)
│   ├── reasoning/            #   Think pipeline, retrieval, topology, scoring
│   ├── ingest/               #   Signal intake, integrations, synthetic
│   ├── domain/               #   Core persisted substrate (models, acts, resources, …)
│   ├── platform/             #   Cross-cutting infra (access_control, execution)
│   └── workers/              #   Background worker packages
├── scripts/                  # CLI utilities and dogfood orchestration
└── tests/                    # Cross-service integration + real-LLM tests
```

The **UI, demo, and simulation** live in the overlay repo
[fyraliscore-demo](https://github.com/Fyralisinc/fyraliscore-demo).

See [CODEBASE-ARCHITECTURE.md §0](docs/reference/CODEBASE-ARCHITECTURE.md) for the layer map and
[CONTRIBUTING.md](CONTRIBUTING.md) for the enforced import boundaries.
