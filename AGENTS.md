# Fyralis Core Agent Guide

Fyralis Core is a backend-only organizational intelligence runtime. Keep this
repo independent of the demo/UI overlay (`Fyralisinc/fyraliscore-demo`); core
must not import overlay, demo, or simulation code.

## Where to Look First

- Start with `README.md` for local setup and process commands.
- Use `CONTRIBUTING.md` for conventions, import rules, migrations, and PR checks.
- Use `docs/reference/CODEBASE-ARCHITECTURE.md` for the end-to-end system map.
- Use `docs/reference/CODEBASE-MANAGEMENT.md` when changing architecture or debt.
- Use `docs/reference/CODEX-LEARNING-LOG.md` before deep debugging or benchmark
  interpretation, and update it when a run teaches a reusable lesson.
- For docs changes, follow `CLAUDE.md` and keep subsystem docs updated with code.

## Local and Cloud Setup

- Python is 3.11+ locally; CI uses Python 3.12 for most jobs and Python 3.14
  for the dedicated ingestion workflow.
- Create an environment with:

```bash
python -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e ".[dev]"
```

- Use `.venv/bin/python -m pytest ...` when a venv exists.
- Do not commit `.env`, `.env.*`, secrets, keys, local dumps, run logs, or generated
  reports. Only tracked example env files may be edited.
- In Codex cloud, do not assume Docker, Postgres, Ollama, provider keys, or real
  LLM access are available. If a requested check needs missing infrastructure,
  run the strongest static/unit subset and say exactly what was not validated.

## Architecture Rules

- Preserve the layer direction: higher layers may depend downward; lower layers
  must not reach upward.
- `lib` must stay independent of `services`.
- `services.reasoning` must not directly import `services.app`,
  `services.product`, or `services.ingest`.
- `services.domain` must not add new imports of reasoning internals or product
  code beyond existing allowlisted debt.
- `services.ingest` must not add new imports of app code beyond existing
  allowlisted debt.
- Architecture contracts live in `pyproject.toml` under `[tool.importlinter]`.

## Validation Commands

Run the narrowest checks that prove the change. Prefer targeted tests first,
then widen only when the changed surface requires it.

```bash
ruff check --select E9,F63,F7,F82,F821,F811,F401 .
lint-imports
python scripts/check_architecture_ratchets.py
python scripts/check_production_env_contract.py
python scripts/check_tech_debt_budget.py
```

For code paths with test coverage:

```bash
.venv/bin/python -m pytest path/to/test_file.py -v --tb=short
```

For the default PR-style test lane, use this only when Postgres is available:

```bash
.venv/bin/python -m pytest \
  --ignore=tests/real_llm \
  -m "not ollama and not real_llm and not subprocess_e2e" \
  -v --tb=short
```

Real-LLM, Ollama, Docker, subprocess E2E, durability, and large synthetic replay
checks are opt-in. Do not run them unless the user asks or the task specifically
requires that evidence.

## Database Changes

- Add migrations as `db/migrations/NNNN_short_name.sql`.
- Use a unique, monotonically increasing four-digit prefix.
- Make migrations idempotent (`IF NOT EXISTS`, guarded `DO` blocks, safe
  backfills).
- After schema changes, run `python scripts/check_schema_drift.py` if a database
  is available. If not, report that schema drift was not runtime-verified.

## Testing and Evidence Expectations

- Be explicit about the validation boundary. Do not overclaim from a narrow test.
- When a failed run, benchmark, migration, or debugging session reveals a durable
  repo lesson, add a dated entry to `docs/reference/CODEX-LEARNING-LOG.md`.
- For DB cleanup or contract questions, prefer a cheap production-shaped harness
  before expensive full replays unless the user asks for the full run.
- For reasoning changes, inspect the Trigger -> retrieval/context -> validation
  -> apply flow and preserve Think context-use telemetry.
- When changing user-facing API behavior, update the relevant docs in the same
  change.

## Review Guidelines

- Prioritize correctness, data isolation, tenant/RLS behavior, secrets handling,
  architecture boundary regressions, and missing tests.
- Treat new upward imports, broad untested rewrites, committed generated
  artifacts, and accidental live-provider dependence as high-priority findings.
- Prefer small, behavior-preserving fixes over sweeping refactors unless the task
  is explicitly architectural.
