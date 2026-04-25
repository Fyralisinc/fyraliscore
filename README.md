# Fyralis Core

Organizational intelligence runtime. A multi-tenant FastAPI gateway, Postgres
(pgvector) data store, Ollama-backed embeddings, and a Vite/React UI, plus
worker processes for asynchronous reasoning and post-commit propagation.

For the architecture and module-level reference, see
[CODEBASE-ARCHITECTURE.md](CODEBASE-ARCHITECTURE.md).

This document is the end-to-end setup guide for running the stack locally.

---

## 1. Prerequisites

Install these on your host before starting. Versions below are what the
codebase is developed against; minor patch differences are fine.

| Tool                | Version            | Notes                                                   |
| ------------------- | ------------------ | ------------------------------------------------------- |
| Python              | 3.11+              | `pyproject.toml` requires `>=3.11`                      |
| Docker + Compose v2 | recent             | Brings up Postgres (pgvector) and Ollama                |
| Node.js             | 20+                | For the UI in [ui/](ui/)                                |
| `psql` client       | any 14+            | Used to apply DB migrations                             |
| `curl`              | any                | Used by `dogfood_up.sh` health checks                   |

macOS quick install:

```bash
brew install python@3.11 node postgresql@16
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
  If you prefer Anthropic or OpenAI, set `LLM_PROVIDER` and the matching
  `ANTHROPIC_API_KEY` / `OPENAI_API_KEY` instead.
- All other variables ship with sensible local-dev defaults. Review them
  if you've changed Postgres or Ollama ports.

Optional: create a second overlay file `.env.dogfood` for values that
differ between your day-to-day env and the dogfood stack (model choices,
worker poll intervals, the dev bearer token, etc.). `scripts/dogfood_up.sh`
sources `.env` first and `.env.dogfood` last, so dogfood values win.
Both files are gitignored.

> **Security note.** `.env` and any `.env.*` variant other than
> `.env.example` are gitignored. Never commit real keys. Rotate any key
> that has been pasted into a chat, log, or doc.

---

## 3. Start Postgres and Ollama

The repo ships a `docker-compose.yml` with two services: `postgres`
(pgvector/pg16) and `ollama` (with the `nomic-embed-text` model
auto-pulled on first start).

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

---

## 6. Seed the dogfood tenant

This creates the CEO actor (`Rachin`) and the simulation personas in
your local DB. Idempotent.

```bash
set -a && source .env && set +a
python scripts/seed_dogfood_tenant.py
```

You should see something like:

```
Seeded tenant 00000000-0000-0000-0000-000000000001: 13 actors (CEO + personas).
```

---

## 7. Install UI dependencies

```bash
cd ui
npm install
cd ..
```

---

## 8. Bring up the full stack

The dogfood script starts the gateway, the two workers, and the Vite
dev server. It writes logs to `/tmp/company_os_logs/` and PIDs to
`/tmp/company_os_dogfood.pids`.

```bash
./scripts/dogfood_up.sh
```

You should see:

```
=== Company OS dogfood stack up ===
  Gateway:         http://localhost:8000
  Main UI:         http://localhost:5173
  Slack simulator: http://localhost:8000/simulation/slack_ui/
  Healthz:         curl http://localhost:8000/healthz
```

Open <http://localhost:5173> in a browser. The dev bearer token from
`.env.dogfood` (`DEV_BEARER_TOKEN=dogfood-ceo-token`) is used by the UI
to authenticate without an explicit `/auth/session` round-trip.

To tail logs:

```bash
./scripts/dogfood_logs.sh
```

To inspect database state:

```bash
./scripts/dogfood_inspect.sh
```

To stop everything:

```bash
./scripts/dogfood_down.sh
```

---

## 9. Running tests

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

UI tests:

```bash
cd ui
npm test           # vitest unit tests
npm run test:e2e   # playwright (uses the in-repo mock server)
npm run typecheck
```

---

## 10. Running individual processes

If you don't want the full dogfood stack, you can run the components
individually:

```bash
# Gateway only
uvicorn services.gateway.main:app --host 0.0.0.0 --port 8000 --reload

# Think worker
python scripts/run_think_worker.py

# Post-commit worker
python scripts/run_post_commit_worker.py

# UI dev server (with API mocks, no backend required)
cd ui && npm run dev:mock
```

---

## 11. Common issues

**`ERROR: .env not found`** — copy `.env.example` to `.env` and fill in
`DEEPSEEK_API_KEY` (or your chosen provider's key).

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

## 12. Layout

```
.
├── CODEBASE-ARCHITECTURE.md  # Architecture & module reference
├── README.md                 # This file
├── .env.example              # Env template (copy to .env)
├── docker-compose.yml        # Postgres (pgvector) + Ollama
├── pyproject.toml            # Python package + dev deps
├── conftest.py               # Pytest fixtures (DB pool, etc.)
├── db/migrations/            # SQL migrations, applied in filename order
├── lib/                      # Shared libraries (db, llm, embeddings, nexus)
├── services/                 # Domain services (gateway, think, query, …)
├── simulation/               # Slack-like simulator + personas
├── scripts/                  # CLI utilities and dogfood orchestration
├── tests/                    # Cross-service integration + real-LLM tests
├── lsob/                     # LSOB packages (baselines, evaluators)
└── ui/                       # Vite/React frontend
```
