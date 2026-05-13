---
name: sdd-run
description: Run the full Spec-Driven Development pipeline on a ClickUp task. Triggered by phrases like "run SDD on", "sdd this task", or when the user pastes a ClickUp task body and asks to implement it end-to-end. Walks through specify → clarify → plan → analyze → implement with mandatory human review gates at spec and plan stages.
---

# SDD Pipeline Orchestrator (fyraliscore)

You are running the spec-driven development pipeline on a ClickUp task in the **fyraliscore / Company OS** repo. This codebase is an organizational intelligence runtime: FastAPI gateway, asyncpg-driven Postgres 16 + pgvector, Ollama embeddings, multi-provider LLM stack (default `claude-opus-4-7`), two background workers, and a Vite/React cockpit. Treat every output through the lens of the constitution at `.specify/memory/constitution.md` (from repo root) — it is load-bearing, not aspirational.

## Project ground truth (load before phase 1)

Read these once at the start of the run and keep their constraints in working memory. Paths are relative to the repo root:

- `.specify/memory/constitution.md` — ten core principles, the Stack Constraints block, and the Review Gates list. Phases 1–6 must respect every NON-NEGOTIABLE.
- `CODEBASE-ARCHITECTURE.md` — descriptive map of what currently exists. Use to ground "Files relevant" interpretation and to avoid inventing modules that already live elsewhere.
- The active task's `source.md` once it has been written in phase 0.

If the constitution or architecture doc is missing or unreadable, STOP and surface that — do not improvise.

## Input

The user has either pasted a ClickUp task body directly, or referenced a task ID. Expect a structure containing some subset of: task ID (e.g. `IN-06`), priority tag, title, **Files relevant**, **Why it is needed**, **How can it be done**, **Acceptance criteria**, **Estimated effort**. The task ID prefix is meaningful (`IN-*` = ingestion, etc.) and may map onto a `services/<area>` slice.

## Pipeline

Execute these phases IN ORDER. Do not skip. Do not reorder. Announce each phase as you enter it with a one-line banner. Use TodoWrite to track the seven phases as you progress.

### Phase 0 — Setup

1. Parse the ClickUp body. Extract the task ID (e.g. `IN-06`) and derive a kebab-case slug from the title (e.g. `webhook-gateway-router`). The directory is `specs/<task-id>-<slug>/`.
2. Confirm the working tree is clean enough to branch from. If there are uncommitted changes unrelated to specs, surface them and ask before proceeding.
3. Create the directory and write the **verbatim** ClickUp body to `specs/<task-id>-<slug>/source.md`. Do not paraphrase, summarize, or reorder sections — downstream phases reread this file.
4. Invoke the **speckit-git-feature** skill to create branch `feat/<task-id>-<slug>` from `main` (or the appropriate base). The `auto: snapshot` commits the speckit hooks produce are expected — do not suppress them.
5. Print: `Setup complete. Branch: feat/<task-id>-<slug>. Proceeding to spec.`

### Phase 1 — Specify (autonomous, then GATE)

1. Invoke the **speckit-specify** skill. Feed it `source.md` so the generated `spec.md` traces directly to the task body.
2. Sanity-check the generated `spec.md`:
   - Every acceptance criterion in `source.md` is reflected as a testable requirement.
   - The "Files relevant" list is preserved as the boundary of the change.
   - Any substrate-touching language (Observations / Models / Acts / Resources, audit chain, region locks, RLS, voice rules) maps cleanly onto the constitution's Four Foundations. Flag misalignments in the spec rather than silently fixing them.
3. Verify `specs/<task-id>-<slug>/spec.md` exists and is non-empty.
4. STOP. Print exactly:
   ```
   📋 SPEC GATE — specs/<task-id>-<slug>/spec.md is ready for review.
      Reply 'approve spec' to continue, or give feedback to revise.
   ```
5. Wait for user input. On feedback, edit `spec.md` (and only `spec.md`) and re-present the gate. Do not advance until the user says `approve spec`.

### Phase 2 — Clarify (autonomous, conditional gate)

1. After approval, invoke **speckit-clarify**.
2. If clarify surfaces ambiguities, present each question to the user and wait for resolution. Encode answers directly back into `spec.md` under a "Clarifications" heading; do not store them only in chat.
3. If clarify returns clean, proceed silently to Phase 3.

### Phase 3 — Plan (autonomous, then GATE)

1. Invoke **speckit-plan**. Pass it the stack constraints from the constitution (Python ≥3.11, asyncpg, FastAPI factory routers, Postgres 16 + pgvector, Ollama default embedder, pluggable LLM provider, React 18 + Vite 5 + Tailwind 3) and the `source.md` "How can it be done" outline so the plan does not contradict the chosen stack.
2. Read the generated `plan.md` and check it against the constitution's NON-NEGOTIABLEs **before** showing the gate. Catch these silently and rewrite the plan section if violated:
   - Any new substrate write path describes the audit chain entry, the region lock scope, and (where applicable) the dual-write chokepoint.
   - Any new migration is described as additive (`CREATE … IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`) with the next free `db/migrations/NNNN_<slug>.sql` number. No "edit migration X" steps.
   - Any new tenant-scoped table or column lists FK + RLS + tenant-prefixed index requirements.
   - Any new substrate row uses `uuid7()`; any new queue uses `FOR UPDATE SKIP LOCKED` + `UNIQUE NULLS NOT DISTINCT`.
   - Any rendering path references `voice_rules.py` enforcement.
   - Any LLM call uses the pluggable provider (`lib.llm.provider.build_provider`); models are not hardcoded.
3. STOP. Print exactly:
   ```
   🏗️ PLAN GATE — specs/<task-id>-<slug>/plan.md is ready for review.
      Reply 'approve plan' to continue, or give feedback to revise.
   ```
4. Wait. Same iteration loop as Phase 1.

### Phase 4 — Tasks (autonomous, no gate)

1. Invoke **speckit-tasks** to expand `plan.md` into `tasks.md`.
2. Ensure tasks are ordered so that: migrations land first, dual-write/sidecar writers second, reader cutover and tests last (per Constitution §IX). If the generated order violates this, reorder before printing the summary.
3. Print a one-line summary: `N tasks generated. Running analyze.`

### Phase 5 — Analyze (autonomous, conditional gate)

1. Invoke **speckit-analyze**.
2. Treat any of the following as gate-firing findings even if analyze did not flag them:
   - A task references a migration number that already exists in `db/migrations/`.
   - A task implies editing an applied migration.
   - A task adds a domain table without RLS or without `tenant_id` indexing.
   - A task adds a Model write without `born_from_event_id` sourcing.
   - A task adds an integration test that mocks Postgres or Ollama.
3. If analyze (or the checks above) flags inconsistencies, gaps, or coverage failures: STOP, print the findings, and wait for direction.
4. Otherwise print `✅ Analyze passed. Proceeding to implementation.` and continue.

### Phase 6 — Implement (autonomous, conditional gate)

1. Confirm the dev environment can run tests before writing code:
   - Docker Postgres container `company_os_postgres` is mapped to host port **5433** (NOT 5432 — system Postgres occupies 5432). `DATABASE_URL` must be `postgresql://company_os:company_os@localhost:5433/company_os`.
   - Ollama is expected on `localhost:11434` with `nomic-embed-text` pulled.
   - The Python venv at `.venv` (Python 3.12) with the project installed via `pip install -e ".[dev]"`.
   If any of these is missing, STOP and surface it — do not silently mock around it (Principle IV).
2. Invoke **speckit-implement**. The skill iterates over `tasks.md`. After each task:
   - Stay within the "Files relevant" list from `source.md`. Touching files outside that list requires either (a) the task itself naming the file, or (b) stopping and surfacing the divergence to the user.
   - Run the targeted test slice for the affected service area: `pytest services/<area>` (or `pytest -m integration` if the task touches substrate / migrations). Real Postgres + Ollama — no mocks of those boundaries.
   - If the task added or modified a migration, run `python scripts/check_schema_drift.py`. Non-zero exit is a hard fail.
   - Run `ruff` on changed paths.
   - For UI changes: `npm run typecheck` and `npm test`. If the task affects a user-visible surface, also exercise it via `npm run dev:mock` and report what you observed in the browser. Type-check passing alone is not feature-correct (Workflow §1.5).
3. If a task fails any of the above: STOP. Print which task failed, the failing command, and the failure mode. Do not retry the same approach and do not skip the task — wait for direction.
4. When all tasks complete cleanly:
   - Invoke **speckit-git-validate** to confirm branch hygiene.
   - Invoke **speckit-git-commit** with a structured message that includes the task ID (`<task-id>: <short summary>`) and a body summarizing the substrate-shape changes, new endpoints, and tests added.

### Phase 7 — Done

Print:
```
🎉 <task-id> complete. Branch feat/<task-id>-<slug> is ready for PR.
   - Spec: specs/<task-id>-<slug>/spec.md
   - Plan: specs/<task-id>-<slug>/plan.md
   - Tasks: specs/<task-id>-<slug>/tasks.md
   - Tests run: <summary>
```

## Hard rules

- **Gates are non-negotiable.** The spec gate and plan gate exist to prevent unrecoverable downstream waste. Do not auto-advance through them even if the user previously said "just do everything" or invoked a "no clarifying questions" mode — those instructions govern *clarifying questions*, not *review gates*.
- **The constitution wins.** If any phase produces output that contradicts `.specify/memory/constitution.md`, STOP and surface the contradiction. Do not silently resolve it. The constitution explicitly states it supersedes ad-hoc preferences and recent habit.
- **Stay inside the spec directory in phases 1–5.** Never edit files outside `specs/<task-id>-<slug>/` until phase 6.
- **Stay inside "Files relevant" in phase 6.** That list, copied verbatim from the ClickUp task, is the implementation boundary. Expanding it is a user decision, not yours.
- **Real Postgres + Ollama, always.** Integration tests use the live containers (Postgres on port 5433, Ollama on 11434). Mocking either boundary in an integration test is a Principle IV violation.
- **No `uuid.uuid4()` on substrate rows, no `print()` in service code, no hand-rolled tenant queries missing `WHERE tenant_id`.** These are listed under Review Gates in the constitution; they will be caught at PR review if you let them through.
- **One phase, one announcement, one TodoWrite update.** Keep the user oriented — they are reviewing your gate output, not your inner monologue.
