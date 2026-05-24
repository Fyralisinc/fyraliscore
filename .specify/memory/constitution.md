# Company OS (fyraliscore) Constitution

Company OS is a multi-tenant organizational intelligence runtime: a FastAPI
gateway, an asyncpg-driven Postgres+pgvector substrate, Ollama-backed
embeddings, an Anthropic/OpenAI/DeepSeek-pluggable LLM stack, two background
workers (Think, post-commit), and a Vite/React cockpit. The system reasons
over ingested signals to produce **Observations → Models → Acts → Resources**.
Because the substrate is an evolving epistemic store — not a CRUD app — the
following principles are load-bearing, not aspirational.

## Core Principles

### I. Four Foundations Are Epistemically Distinct (NON-NEGOTIABLE)

The substrate has four atomic stores and they are not interchangeable:

- **Observations** — immutable, append-only empirical signals, partitioned
  by `occurred_at`, every row keyed by `(tenant_id, trust_tier,
  source_channel)`. Never updated; corrections become new observations
  with a `cause_id` back-link.
- **Models** — epistemic beliefs with `confidence ∈ [0.05, 0.95]`,
  `activation ∈ [0, 1]`, a typed `proposition.kind` (pinned by CHECK in
  migration 0035), a `falsifier`, and a lifecycle `active | archived |
  superseded | contested_false`. Models are mutable through controlled
  transitions, never overwritten in place when provenance is at stake.
- **Acts** — Goals, Commitments, Decisions. Each has a state machine;
  state transitions are domain operations, not field updates.
- **Resources** — organizational assets with utilization, controllability,
  and temporal character. Mutated only through `resource_transactions`.

Every new feature MUST map cleanly onto these four stores. Adding a fifth
"foundation" or collapsing two existing ones requires a written
amendment. Per-feature side tables for cross-cutting concerns (cache,
queue, audit, sidecar) are allowed and encouraged — they are not new
foundations.

**Universal Flow Rule**: `input → Observation → Think → always Models,
sometimes Acts, sometimes Resources`. No code path may produce a Model
without an originating Observation (`born_from_event_id` is NOT NULL).
No code path may produce an Act or mutate a Resource without traceable
Model support.

### II. Schema Is Append-Only, Migrations Are Idempotent (NON-NEGOTIABLE)

`db/migrations/NNNN_<slug>.sql` is the single source of truth for schema.
Rules:

1. Migrations are numbered, applied in filename order, never edited
   after merge. Renumbering or rewriting an applied migration is
   prohibited.
2. Every migration is **idempotent** (`CREATE TABLE IF NOT EXISTS`,
   `ADD COLUMN IF NOT EXISTS`, `DO` blocks for partition/policy
   creation). Re-running the directory against an existing DB must
   be a no-op.
3. Each migration runs in its own transaction
   (`lib/shared/migrations.apply_migrations_dir`) so partial failures
   roll back cleanly.
4. There is no production migration runner. The test conftest applies
   migrations; production applies them via `psql` in deployment.
   Drift between live schema and migrations is detected by
   `scripts/check_schema_drift.py` and raised as `SchemaDriftError`.
5. Destructive changes (DROP COLUMN, DROP TABLE, type narrowing) require
   a staged plan in the migration's leading comment: dual-write → backfill
   → reader cutover → drop. The recommendation-promotion and
   `signal_readings`-sidecar paths (§13.6, §13.7 in `CODEBASE-ARCHITECTURE.md`)
   are the canonical templates.

### III. Tenant Isolation Is Structural, Not Procedural (NON-NEGOTIABLE)

`tenant_id UUID` is on every domain table. Three layers MUST be present
on any new tenant-scoped table:

1. **Column + FK** — `tenant_id UUID NOT NULL REFERENCES tenants(id)
   DEFERRABLE INITIALLY IMMEDIATE` (per migration 0037). IMMEDIATE
   protects production; DEFERRABLE lets test rollback fixtures issue
   `SET CONSTRAINTS ALL DEFERRED` and skip tenant registration.
2. **RLS** — `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` + `FORCE`,
   with the `tenant_isolation` policy from migration 0036 (permissive
   default: `current_setting('app.current_tenant', true) IS NULL OR
   tenant_id = current_setting('app.current_tenant', true)::uuid`).
3. **Index** — every common query predicate is prefixed with `tenant_id`.

Application code SHOULD use `tenant_transaction(tenant_id)` from
`lib/shared/tenant_context.py` for new code paths so RLS bites as
defense-in-depth. The flip from permissive to strict RLS is a separate,
gated migration; until then, hand-rolled `WHERE tenant_id = $1` remains
authoritative and required.

Cross-tenant joins are forbidden. Global registries (`tenants`,
`demo_configs`) are the only tables intentionally outside this regime.

### IV. Integration Tests Use a Real Database (NON-NEGOTIABLE)

The test suite uses live Postgres and Ollama, not mocks of them. This
is a deliberate choice, not a missing feature.

- The `db_pool` and `fresh_db` fixtures in `conftest.py` apply real
  migrations and TRUNCATE-RESTART-CASCADE between tests. Per-test
  pool cost (~30ms) is accepted in exchange for catching driver
  codec, partition, and RLS issues that mocks hide.
- Tests are tagged with markers from `pyproject.toml`:
  `integration` (live Postgres), `ollama` (live Ollama),
  `real_llm` (provider key + `RUN_REAL_LLM=1`), `slow`.
- HTTP boundaries to LLM providers MAY be mocked with `respx` in
  unit tests. The database boundary may not.
- Tests rely on `freezegun` for time and on `uuid7()` for time-ordered
  IDs. Test-only randomness without a seed is prohibited in
  determinism-sensitive paths (retrieval, applier, reconciler).
- `pytest` runs under `filterwarnings = ["error", ...]` —
  any new DeprecationWarning becomes a failing test, not a log line.

A feature ships when its integration test passes against a real
Postgres + Ollama, not when its unit tests pass alone.

### V. Reasoning Is Separated From Rendering

Two LLM concerns live in different services and SHOULD use different
models:

- **Think** (`services/think/`) — reasoning. Receives a
  `TriggerContext`, retrieves Models across pathways A/B/C/D/F, calls
  an LLM (default `claude-opus-4-7`), validates strict JSON
  (`strict_schema.py`, `validator.py`), applies diffs via
  `applier.py` under region locks, and emits audit events.
- **Rendering** (`services/rendering/`) — prose. Receives a
  `SubstrateSnapshot` or `CardInput`, calls a (cheaper) LLM, runs the
  output through `voice_rules.py`, and records cost to
  `view_render_costs`.

The two MUST NOT be merged. A reasoning failure must not corrupt prose;
a prose retry must not re-reason the substrate. Per-tenant model routing
(`services/demo/model_routing.py`) overrides the global default — code
paths SHOULD read provider/model from config, not hardcode them.

**Voice rules** (`services/rendering/voice_rules.py`) are enforced as
REJECT/FLAG violations: no exclamation marks, no marketing language,
no emoji, sentences ≤35 words, cards must reference concrete data
(names, numbers, dates), no hedge preamble. Adding or removing a rule
is a constitution-adjacent decision and SHOULD be discussed before
merge.

### VI. Trust, Confidence, and Falsifiers Are First-Class

Every Observation carries a `trust_tier`
(`authoritative | attested_agent | inferential | derived | speculative`).
Every Model carries a `confidence` and SHOULD carry a `falsifier`. The
following invariants are enforced in code and SHOULD NOT be bypassed:

- A Commitment transition to `doneverified` requires an authoritative
  resolving event. Mismatches raise `TrustTierError`.
- A Model with `confidence > 0.7` requires an adequate falsifier per
  `services/think/validator.is_adequate_falsifier`. Inadequate or
  malformed falsifiers raise `FalsifierInadequateError` /
  `MalformedFalsifierError` — distinct, observable failure modes.
- Confidence is bounded `[0.05, 0.95]` by CHECK constraint; the
  endpoints encode "we are never certain and never absent."
- `proposition_kind` is one of 11 values pinned by migration 0035's
  CHECK. New kinds require a migration, not just a Pydantic update.

### VII. Determinism, Idempotency, and Audit Trails

The substrate is replay-able. Code MUST preserve this:

- **IDs are time-ordered**: `lib/shared/ids.uuid7()` everywhere a new
  primary key is allocated. No `uuid.uuid4()` for substrate rows.
- **Queues use FOR UPDATE SKIP LOCKED** with `UNIQUE NULLS NOT DISTINCT`
  dedup keys (see `think_trigger_queue`, `topo_dirty_queue`,
  `model_reeval_queue`). A retry must not double-apply.
- **Region locks** (`services/think/region_locks.py`) serialize
  reasoning over overlapping scopes via `pg_advisory_xact_lock`.
  Bypassing region locks is prohibited.
- **Audit chain** (`services/think/audit.py`, table `audit_events`)
  records every Model state transition with `changed_fields` and
  re-assertion metadata. Mutations to Model rows that skip
  `emit_audit_event()` are bugs.
- **Sidecar debug capture** (`think_run_artifacts`) is enabled in
  dogfood (`DEBUG_ARTIFACT_CAPTURE=1`) and is read by the debug UI.
  New Think pipeline stages SHOULD emit an artifact row.

### VIII. Errors Carry Structured Context

All domain exceptions derive from `lib.shared.errors.CompanyOSError`
and carry a `context: dict[str, Any]` plus a stable `code` string.
The `to_dict()` shape — `{code, message, context}` — is the
serialization contract for HTTP error responses, structured logs, and
the Think failure ledger.

- Don't raise bare `Exception` or `ValueError` in domain code; use or
  subclass the existing hierarchy.
- Add a new error class when (a) call sites need to branch on type,
  not message, OR (b) a structured `code` will be read by an external
  consumer (the UI, the audit ledger, an alert).
- `ValidationError` for 4xx, `InvariantViolation` for C/G constraints,
  `TrustTierError`, `FalsifierInadequateError`,
  `MalformedFalsifierError`, `CalibrationMissingError`,
  `SchemaDriftError` are the canonical existing kinds.

### IX. Substrate Changes Are Dual-Write Until Proven

The codebase has a repeated pattern for schema migrations that affect
hot paths: introduce the new structure alongside the old, dual-write
through a single chokepoint, run a drift detector, and cut over only
after parity. Canonical examples:

- **Model-to-model edges (S1, migration 0031)** — `_set_model_relations`
  writes both the legacy `supporting_model_ids` / `contributing_models`
  arrays and the typed `model_edges` rows; the `edge_drift` worker
  samples per tenant.
- **Topology (S2-S4, migrations 0032-0034)** — observable before
  consequential. Embeddings and neighborhoods are computed and indexed
  for a soak window before Pathway F threads them into retrieval.
- **`model_signal_readings` sidecar (migration 0038)** — foundation
  table created; producers, backfill, and reader cutover staged.

New substrate-shape changes MUST follow this pattern when (a) the field
is on a hot read path, OR (b) there is meaningful existing data, OR (c)
multiple services read it. Single chokepoint writer. Drift detector.
Cutover behind a flag or after a measured soak.

### X. Simplicity, YAGNI, and No Premature Abstraction

The codebase has earned its complexity by encountering the problem;
new complexity should do the same.

- Don't introduce a config knob, a feature flag, or a plugin point
  without a current second caller or a written reason.
- Don't add a layer of indirection (adapter, factory, strategy) when
  a direct call works. The `Embedder` Protocol (§13.2) and the LLM
  provider abstraction earn their keep because we have ≥2 backends in
  production; copy that bar.
- Read-only aggregators (`services/today/`, `services/history/`)
  intentionally have no DB tables of their own — they derive views
  from the foundations. New read surfaces SHOULD follow that pattern.

## Stack Constraints

These are not preferences; they're load-bearing.

- **Language**: Python `>=3.11`. New code uses `from __future__ import
  annotations`, full type hints, and Pydantic v2 (`strict=False`,
  `extra="forbid"` at wire boundaries).
- **Async DB**: `asyncpg` with explicit pool lifecycle. The sync
  `psycopg2` is used only by `scripts/check_schema_drift.py`.
- **Web**: FastAPI + uvicorn[standard]. Routers are built by factory
  functions (`build_ceo_api_router`, `build_query_router`, ...) that
  receive their dependencies; no module-level globals for pools,
  embedders, or providers.
- **DB**: Postgres 16 with pgvector. HNSW indexes on `VECTOR(768)`
  content embeddings and `VECTOR(128)` topo embeddings, both partial
  on `status='active' AND embedding IS NOT NULL`. Quarterly
  partitions on `observations`.
- **Embeddings**: `nomic-embed-text` via Ollama by default; OpenAI
  `text-embedding-3-small` with `dimensions=768` as the alternate
  backend. Selection via `make_embedder()` /  `EMBEDDER_BACKEND`.
- **LLMs**: pluggable provider (`lib.llm.provider.build_provider`).
  Default reasoning model `claude-opus-4-7`. Per-tenant override
  via `demo_configs.model_routing`. Costs are recorded per call.
- **Logging**: `structlog` with JSON output. Every request bound with
  `request_id`, `tenant_id`, `actor_id`. No `print()` in service
  code.
- **Frontend**: React 18 + Vite 5 + TypeScript 5 + Tailwind 3. Tests
  via Vitest (unit) and Playwright (e2e, with mock server). Real-time
  via WebSocket (`/view/ceo/stream`) and SSE
  (`/v1/recommendations/stream`).
- **Containers**: `docker-compose.yml` is the single deploy topology.
  TLS via Let's Encrypt (`acme-companion`). The dogfood path runs
  the same images under `scripts/dogfood_up.sh`.

## Development Workflow

### Pre-commit gates (local)

Before opening a PR, the contributor MUST:

1. Run the targeted test slice for affected services (`pytest
   services/<area>` or `pytest -m integration`).
2. Run `python scripts/check_schema_drift.py` if migrations or
   models changed; zero exit code required.
3. Run `ruff` (config in `pyproject.toml`).
4. For UI changes: `npm run typecheck`, `npm test`, and at least one
   e2e check (`npm run test:e2e` against the mock server).
5. For UI/frontend changes, exercise the feature in a browser
   (dogfood or `dev:mock`). Type-check passing is not feature-correct.

### Spec-driven changes

Non-trivial features (new endpoint, new substrate column, new worker,
new claim_op) flow through the speckit workflow under `docs/specs/`:

`spec.md → clarify → plan.md → tasks.md → implement → checklist → analyze`

`.specify/extensions.yml` runs `before_*` and `after_*` git hooks
around each stage so plans, specs, and code stay in lockstep. The
`auto: snapshot` commits in history come from this machinery and are
expected.

### Review gates

A PR MUST be rejected if any of the following are true:

- It edits an already-applied migration.
- It introduces a substrate write path that bypasses the audit chain,
  the dual-write chokepoint (where one exists), or the region lock
  for its scope.
- It adds a Model write that omits `born_from_event_id` or a
  Commitment write that skips its state-machine validator.
- It introduces a new tenant-scoped table without FK + RLS + tenant-
  prefixed indexes (Principle III).
- It adds a tenant-scoped query without `WHERE tenant_id` (defense-
  in-depth doesn't excuse hand-rolled predicates).
- It introduces a `uuid.uuid4()` for a substrate row, a `print()` in
  service code, or a mocked Postgres in an integration test.
- It exceeds the voice rules in a rendered output without an explicit,
  reviewed reason.

### Performance & cost expectations

- Greeting render cadence is `GREETING_REFRESH_INTERVAL_SECONDS=900`
  (15 min). A code change that drives more frequent renders MUST
  state why; cost ledger entries in `view_render_costs` are the
  evidence trail.
- Think trigger latency target: T1 processed within ~2× the worker
  poll interval (`THINK_WORKER_POLL_INTERVAL_S=2` default).
- Retrieval `top_n=80`, `decay_base=0.9` are tunable but stable. Per-
  pathway weights are pinned per trigger kind (see
  `services/retrieval/primary.py`). Changes are spec-level.
- Demo sessions are budget-capped by `DEMO_BUDGET_USD_PER_SESSION`;
  exceeding the cap is a banner, not a 500.

## Governance

This constitution supersedes ad-hoc preferences and recent habit. When
in conflict with:

- **Code review comments** — the constitution wins; the comment may
  prompt an amendment.
- **CODEBASE-ARCHITECTURE.md** — the architecture doc is descriptive
  ("what is true today"); this constitution is prescriptive ("what
  must remain true"). The architecture doc is updated to match the
  constitution after each amendment.
- **A specific spec under `docs/specs/`** — a spec may temporarily
  introduce a NON-NEGOTIABLE exception, but the spec MUST list it
  under "Constitution Check" with justification (per the speckit
  plan template's Complexity Tracking table).

### Amendments

1. Open a PR that edits this file alongside the change that motivates
   it. The PR description states which principle is being changed and
   why now.
2. Bump the `Version` line below using semver:
   - **MAJOR** — a principle is removed or its NON-NEGOTIABLE tag is
     dropped/added.
   - **MINOR** — a new principle, section, or workflow gate is added.
   - **PATCH** — wording clarifications, fixed typos, tightened
     phrasing without semantic change.
3. Update `Last Amended` to the merge date.
4. Update any speckit plan currently in flight to re-evaluate its
   "Constitution Check" gate against the new wording.

### Authority

The constitution's authority is the same as the schema's: it is the
contract between contributors. Disagreement is resolved by amendment,
not by exception in code. Where the constitution is silent, defer to
the canonical documents it references (`CODEBASE-ARCHITECTURE.md`,
`services/think/SUBSTRATE_SEMANTICS.md`, `lib/shared/edge_registry.py`,
the migration headers).

**Version**: 1.0.0 | **Ratified**: 2026-05-13 | **Last Amended**: 2026-05-13
