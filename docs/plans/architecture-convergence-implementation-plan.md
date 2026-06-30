# Architecture Convergence Implementation Plan

Date: 2026-06-30

This is the implementation plan for making Fyralis Core simpler, more
optimized, and more elegant without reducing its intelligence. The goal is not
to rewrite the system. The goal is to make the clean architecture already
implied by the code mechanically true.

This plan deliberately excludes "more power" work. Power improvements belong in
separate plans for better memory primitives, causality, simulation, human
validation, and closed-loop action learning. This plan is about structural
convergence.

## North Star

The system should read as one obvious pipeline:

```text
Signals
  -> Observations
  -> Think trigger
  -> Context plan
  -> Evidence packet
  -> Proposed diff
  -> Validated diff
  -> Pure model mutation
  -> model_events
  -> Post-commit workers
  -> projection_snapshots / read indexes
  -> Read facade
  -> Product surfaces / Ask / Today / extensions
```

The core rule:

```text
Canonical truth lives in Models.
Derived operating views live in projections and indexes.
Reasoning proposes.
Validators constrain.
Appliers mutate.
Events announce.
Workers derive.
Product reads.
```

## Problems This Plan Fixes

### Simplicity

The conceptual system is simple, but too many physical code paths expose too
many concepts at once. Product code can know retrieval internals, domain model
code has historical upward dependencies, and compatibility surfaces make it
hard to know which path is canonical.

### Optimization

The write side is durable and production-shaped, but interactive reads are not
yet consistently fast. Ask and product reads need projection-first and
model-first paths before they fall into expensive deep retrieval.

### Elegance

The architecture has an elegant conceptual center, but the codebase still shows
scaffolding: large multi-role files, compatibility facades, stale topology docs,
and guardrails that are not fully green.

## Non-Goals

- Do not delete advanced reasoning capability to make the code look smaller.
- Do not collapse Models, projections, sidecars, retrieval, and product reads
  into one generic table or service.
- Do not rewrite the whole reasoning system in one PR.
- Do not move product behavior into the domain model repo.
- Do not bypass authority checks for caches, projections, or persisted evidence.

## Phase 0: Make The Guardrails Green

Objective: stop architecture debt from growing before deeper cleanup starts.

Work:

- [ ] Fix the broken `lint-imports` contract around domain tests importing
  reasoning internals.
- [ ] Move domain tests onto public fixtures, read shapes, or facade-level test
  helpers instead of importing `services.reasoning.retrieval.*` internals.
- [ ] Bring `scripts/check_tech_debt_budget.py` back under budget, or split the
  budget into explicit temporary categories with ratchets that can only shrink.
- [ ] Add or tighten ratchets for any remaining allowlisted upward imports.
- [ ] Record the current large-file and boundary-debt baseline in the PR so
  later cleanup can be measured.

Primary files and areas:

- `pyproject.toml`
- `scripts/check_architecture_ratchets.py`
- `scripts/check_tech_debt_budget.py`
- `services/domain/models/tests/`
- `services/reasoning/retrieval/`

Validation:

```bash
lint-imports
.venv/bin/python scripts/check_architecture_ratchets.py
.venv/bin/python scripts/check_tech_debt_budget.py
ruff check --select E9,F63,F7,F82,F821,F811,F401 .
```

Exit criteria:

- Architecture contracts pass.
- Debt budget passes or has a stricter shrink-only transition rule.
- No new upward imports are possible without failing checks.

## Phase 1: Purify The Model Write Kernel

Objective: make the model repository a boring, strong canonical memory layer.

Target shape:

```text
ModelsRepo = canonical model and edge mutation only
Reasoning = proposes and validates mutations
Post-commit workers = derive projections, notifications, indexes, and product effects
Product = reads projections or facades
```

Work:

- [ ] Define a small write result shape, for example `ModelWriteResult`, that
  includes changed model ids, edge ids, emitted event ids, and touched subjects.
- [ ] Ensure model writes emit neutral `model_events` for semantic changes.
- [ ] Move product and reasoning side effects out of `ModelsRepo` and into
  post-commit handlers or workers.
- [ ] Move embedding/vector preparation outside the inner model transaction
  where possible, or pass precomputed vectors into repository methods.
- [ ] Keep repository methods narrow: insert, update, upsert, delete, link, and
  canonical reads.
- [ ] Remove direct imports from `services.domain.models` into reasoning or
  product code, except temporary allowlisted debt that shrinks in this phase.

Primary files and areas:

- `services/domain/models/repo.py`
- `services/domain/models/events.py`
- `services/domain/models/read_shapes.py`
- `services/domain/projections/`
- `services/reasoning/think/applier.py`
- `services/reasoning/think/reconciler.py`
- `services/product/`

Validation:

```bash
lint-imports
.venv/bin/python scripts/check_architecture_ratchets.py
.venv/bin/python -m pytest services/domain/models/tests -v --tb=short
```

Exit criteria:

- Model mutation can be understood without reading retrieval, SAGE, Today, or
  product code.
- `model_events` are the durable announcement of semantic changes.
- Product effects happen after commit, not inside the canonical mutation kernel.

## Phase 2: Create One Stable Read Facade

Objective: stop product surfaces from depending on retrieval internals.

Target shape:

```text
Product / Ask / Today / extensions
  -> MemoryReadFacade
      -> projection reads
      -> model-first retrieval
      -> evidence packet construction
      -> authority filtering
      -> optional deep inquiry
```

The facade should expose a small set of product-facing operations:

```python
answer_question(...)
get_context_packet(...)
read_subject_summary(...)
read_projection(...)
retrieve_evidence(...)
```

Work:

- [ ] Choose the stable module location for the facade. Prefer a platform or
  product-safe execution boundary rather than exposing reasoning internals.
- [ ] Wrap the existing inquiry runtime and projection reads behind the facade.
- [ ] Route Ask/query reads through the facade.
- [ ] Route Today and recommendation reads through the facade where they need
  memory context.
- [ ] Keep Think mutation-oriented retrieval separate in behavior, but aligned
  with the same vocabulary: context plan, evidence packet, sufficiency, context
  packet.
- [ ] Attach retrieval telemetry at the facade boundary.
- [ ] Thread principal, purpose, tenant, and authority context through facade
  calls.

Primary files and areas:

- `services/platform/execution/inquiry.py`
- `services/platform/execution/context_packet.py`
- `services/platform/execution/retrieval_actions.py`
- `services/app/gateway/ask_routes.py`
- `services/app/gateway/today_routes.py`
- `services/product/ask/`
- `services/product/today/`
- `services/reasoning/retrieval/`

Validation:

```bash
lint-imports
.venv/bin/python -m pytest services/product tests -k "ask or today or retrieval" -v --tb=short
```

Exit criteria:

- Product code imports one stable read facade instead of many reasoning
  retrieval modules.
- Ask and Think use clear modes of the same conceptual retrieval pipeline.
- Authority and telemetry are visible at the facade boundary.

## Phase 3: Tier Ask For Interactive Speed

Objective: make normal Ask fast while preserving deep inquiry for expensive
questions.

Target tiers:

```text
L0: cached/projection answer        target <100ms
L1: focused model-first retrieval   target ~500ms-1s
L2: compact synthesis               target <2s
L3: deep inquiry                    async / progressive / 10s+
```

Work:

- [ ] Classify Ask requests into L0, L1, L2, or L3 before retrieval starts.
- [ ] Use `projection_snapshots` and hot read packets for common questions.
- [ ] Prefer model-first context packets before expanding raw observations.
- [ ] Make deep inquiry explicit, async, or progressive instead of defaulting
  interactive reads into the slowest path.
- [ ] Invalidate or refresh projection-backed answers from `model_events`.
- [ ] Keep persisted evidence, cached answers, and projection reads
  authority-filtered.
- [ ] Record latency by tier in telemetry.

Primary files and areas:

- `services/product/ask/`
- `services/platform/execution/inquiry.py`
- `services/platform/execution/context_packet.py`
- `services/domain/projections/`
- `services/product/today/aggregator.py`
- `services/platform/access_control/`

Validation:

```bash
.venv/bin/python -m pytest services/product/ask -v --tb=short
.venv/bin/python -m pytest services/platform/execution -k "inquiry or context_packet" -v --tb=short
```

Exit criteria:

- Normal Ask does not hit deep retrieval by default.
- Deep inquiry remains available for hard questions.
- p50 and p95 latency are measured separately for each tier.

## Phase 4: Shrink Large Files Along Real Boundaries

Objective: improve elegance without cosmetic churn.

Only split files after the new boundaries exist. Split by role, not by line
count alone.

Useful split axes:

```text
planning
evidence construction
validation
mutation
projection
API serialization
telemetry
```

Likely targets:

- [ ] `services/reasoning/think/applier.py`
- [ ] `services/reasoning/think/compiled_reasoning.py`
- [ ] `services/reasoning/sage/reader.py`
- [ ] `services/reasoning/retrieval/pathways.py`
- [ ] `services/domain/models/repo.py`
- [ ] `services/platform/execution/retrieval_actions.py`
- [ ] `services/product/today/aggregator.py`
- [ ] `services/app/gateway/today_routes.py`

Validation:

```bash
.venv/bin/python scripts/check_tech_debt_budget.py
ruff check --select E9,F63,F7,F82,F821,F811,F401 .
```

Exit criteria:

- Each new module has one clear reason to exist.
- Public imports stay stable or move through explicit compatibility shims with
  deletion dates.
- Large-file count and long-function count decrease.

## Phase 5: Retire Transitional Compatibility Paths

Objective: remove scaffolding once the canonical routes are proven.

Work:

- [ ] Delete legacy retrieval toggles that are no longer used by production or
  tests.
- [ ] Remove re-export modules that only hide old import locations.
- [ ] Delete compatibility facades after callers move to the stable facade.
- [ ] Remove stale pathway names that expose implementation history instead of
  product meaning.
- [ ] Remove dead runtime/process manifest entries.
- [ ] Update docs that describe package layouts or processes that no longer
  exist.

Validation:

```bash
rg "legacy|compat|deprecated|TODO\\(remove\\)|re-export" services docs
lint-imports
.venv/bin/python scripts/check_architecture_ratchets.py
```

Exit criteria:

- There is one blessed path for model writes.
- There is one blessed path for product memory reads.
- Compatibility code has owners, dates, or is gone.

## Phase 6: Documentation And Runtime Convergence

Objective: make diagrams, docs, process manifests, and code vocabulary describe
the same system.

Work:

- [ ] Update `docs/reference/CURRENT_SYSTEM_DEEP_DIVE.md` after the code
  boundaries change.
- [ ] Update `docs/reference/CODEBASE-ARCHITECTURE.md` with the new read facade
  and model kernel shape.
- [ ] Update per-layer docs if modules move.
- [ ] Update `services/platform/runtime/process_manifest.py` if worker topology
  changes.
- [ ] Update `docker-compose.yml` only for real runtime changes, not cosmetic
  alignment.
- [ ] Keep Mermaid diagrams compatible with MkDocs Material superfences.

Validation:

```bash
git diff --check -- docs/reference/CURRENT_SYSTEM_DEEP_DIVE.md docs/reference/CODEBASE-ARCHITECTURE.md
mkdocs build --strict
```

If docs dependencies or MkDocs are not installed, run `git diff --check` and
state that strict docs rendering was not validated.

## Recommended PR Order

1. Guardrails PR: make `lint-imports` and debt checks green.
2. Model kernel PR: remove upward side effects from `ModelsRepo`.
3. Read facade PR: give Product and Ask one stable retrieval entrypoint.
4. Ask tiering PR: projection-first and fast-path reads.
5. Module shrink PRs: split giant files along proven boundaries.
6. Docs/runtime cleanup PR: make diagrams, manifests, and package docs match
   reality.
7. Deletion PR: remove old compatibility paths once usage is gone.

## Overall Completion Criteria

This plan is complete when:

- Product reads do not import reasoning retrieval internals directly.
- Domain model mutation does not import product or reasoning internals.
- Model writes emit neutral events and product side effects happen post-commit.
- Ask uses fast projection/model-first tiers before deep inquiry.
- Authority is preserved across projections, caches, persisted evidence, and
  read facades.
- Architecture contracts and debt budgets are green.
- Large files have been split along real responsibilities.
- Docs and runtime manifests match the current code.

## Final Validation Bundle

Run the narrowest targeted tests for each PR. Before declaring the full
convergence done, run:

```bash
ruff check --select E9,F63,F7,F82,F821,F811,F401 .
lint-imports
.venv/bin/python scripts/check_architecture_ratchets.py
.venv/bin/python scripts/check_production_env_contract.py
.venv/bin/python scripts/check_tech_debt_budget.py
```

If Postgres is available and the touched paths need runtime database proof, also
run the relevant targeted integration tests and schema drift checks.

## Operating Principle

Simplification here means legibility, not subtraction.

The system should keep its advanced intelligence, but each responsibility should
live in one obvious place. When that is true, the architecture becomes easier to
reason about, faster to operate, and safer to extend.
