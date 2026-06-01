# GitHub Intelligence Layer — Formalized Spec & Plan

## Context

You want a **GitHub intelligence layer**: a subsystem that (1) knows a connected repo's
codebase fully — its structure, symbols, and dependency graph — kept **current**; and (2)
reasons about what each GitHub action (push, merge, PR/issue comment, review, check, etc.)
*results in* — i.e. the **state change** it causes — and **why**.

This is being built inside **fyraliscore**, where a GitHub integration already exists (IN-13)
but is purely **read-only event ingestion**: webhooks (push, PR, merge, issues, issue_comment,
pr_review, check_run) become `observations` rows and flow into a generic `think`/`models`
reasoning substrate. What is **missing** today, and what this feature adds:

- **No knowledge of the actual code** (file tree, symbols, imports, dependency/call graph).
- **No consolidated current-state model** of the repo (PR lifecycle, CI status, branch heads, issue status).
- **No action→consequence causal reasoning** specific to GitHub, and nothing that re-learns the
  codebase after a state-changing action lands.

### Decisions locked with you
- **Code depth:** full code comprehension — index the real source (files, symbols, dependency/call graph, semantic code embeddings).
- **Graph precision:** **SCIP/LSIF** — precise cross-file symbol references and call graph (true symbol-level blast radius), per-language indexers. Tree-sitter is the breadth *fallback* for languages that lack a mature SCIP indexer.
- **Targets:** **arbitrary multi-language tenant repos** from day one (language-pluggable indexer matrix), dogfooding on this repo first to validate end-to-end.
- **Primary output:** **context tied to every GitHub signal the ingestion system receives**, written **inline into the same observation row's `content` body** as `content.intelligence` (the state transition it caused before→after, the code it affects/blast radius, related entities, and a causal "why"). This is the *default* — a successfully enriched signal. **On failure/timeout of the intelligence layer, the raw GitHub signal is ingested unchanged** (no `content.intelligence` key). In addition, the same reasoning is persisted to structured FSM/enrichment tables as the queryable system-of-record (Option A).
- **Reasoning:** **causal explanation + self-updating** — a state-changing action both advances the GitHub-state model and triggers a re-index of the affected code so the model stays live.
- **Shape:** a **dedicated GitHub-state subsystem** (own schema/FSMs tuned to GitHub semantics), fed by the existing `observations`, *not* a thin extension of the generic think layer.

### The two halves
1. **Code-comprehension index** (`services/code_intel/`) — a living, SCIP-precise code graph + semantic code-RAG per repo, keyed by commit sha, self-updating on push/merge. Provider-agnostic; GitHub is just the fetch source.
2. **State + signal-enrichment engine** (`services/github_intel/`) — GitHub-specific FSMs (PR lifecycle, CI, issues, branches) driven by observations. It enriches each signal **inline at normalize time** (writing `content.intelligence` into the observation, with a bounded timeout that degrades to the raw signal), and maintains structured FSM/enrichment tables as the system-of-record, using the code graph for blast radius and an LLM for the causal "why".

---

## Part A — Code-comprehension index (`services/code_intel/`)

### A1. Code fetch
- Pull repo content with the **existing GitHub App installation token** via `git clone` (token as `x-access-token:<token>@github.com/...`). Reuse `GithubClient.mint_installation_token()` in [services/integrations/github/client.py](services/integrations/github/client.py); add only `clone_url_for(owner, repo, installation_id)` (never logged).
- **Why git clone over the Trees/Blobs API:** the Git Data tree API truncates at ~100k entries / 7MB (silently breaks on monorepos) and per-blob fetches burn the 5000/h REST budget; git transport is exempt from that limit and `git diff` gives exact incremental deltas.
- Initial: `git clone --filter=blob:none --depth=1 --single-branch --branch <default>`. Incremental: prefer the push payload's `added/modified/removed`; fall back to `git fetch` + `git diff --name-status <before>..<after>` when the payload is truncated.
- **Working copy:** ephemeral worker fs (`/tmp/code_intel/<tenant>/<repo>/`) for parsing; durable **S3 cache** of the bare repo (reuse `services/ingestion/raw_tier/s3.py` patterns + existing MinIO/S3) so incrementals fetch only the delta. Cap with `CODE_INTEL_MAX_REPO_MB` → snapshot `status='skipped_too_large'` (never silently partial).
- **GitHub App permission prerequisite:** SCIP indexing + clone require `contents: read`. The event-only IN-13 install may lack it → this is a **re-consent / onboarding prerequisite**; clone 403 must fail cleanly to `status='failed'` with a clear `last_error`.

### A2. Indexing (SCIP-primary, tree-sitter fallback)
- **Precision backbone = SCIP.** Run the appropriate per-language SCIP indexer (`scip-python`, `scip-typescript`, `scip-go`, `scip-java`, `scip-clang`, etc.) against the working copy to produce a SCIP index, then ingest it into the code graph. SCIP gives precise cross-file symbol definitions/references → true symbol-level blast radius.
- **Breadth fallback = tree-sitter.** For languages without a usable SCIP indexer (or when a SCIP run fails/times out), parse with tree-sitter to still capture files→symbols→imports with `precision='heuristic'`. The graph schema carries a `precision` column so SCIP-exact and tree-sitter-heuristic edges coexist and consumers can filter to `exact` only.
- **Language-pluggable indexer interface:** an `Indexer` Protocol (`language_id`, `file_extensions`, `index(working_copy) -> IndexResult`) with a registry dispatching per language. Adding a language = drop one indexer impl + register it; the graph schema, embeddings, and workers stay language-agnostic. Run the SCIP-indexer toolchain as subprocesses inside the worker image.

### A3. Code graph schema — `db/migrations/0063_code_intel.sql`
Next number after `0062_jira.sql`; follows existing conventions (BEGIN/COMMIT, `IF NOT EXISTS`, `tenant_id` FK, ENABLE+FORCE RLS with the `*_tenant_isolation` template). **Header must note: this is NOT an ingestion source — it emits no observations and touches none of the four source-registry CHECK tables**, so it sidesteps the 0061/0062 source-CHECK landmine.

Tables (versioned by commit sha so re-index is incremental):
- `code_snapshots` — one row per `(tenant, repo, commit_sha)`; `branch`, `parent_snapshot_id`, `status (pending|indexing|ready|failed|skipped_too_large)`, `index_kind (full|incremental)`, counts. `UNIQUE(tenant_id, repo_full_name, commit_sha)` = idempotency key.
- `code_files` — `path`, `language` (NULL=binary/unknown), `blob_sha` (dedup across snapshots), size/line counts, `is_generated`. `UNIQUE(snapshot_id, path)`.
- `code_symbols` — `kind`, `name`, `qualified_name`, `parent_symbol_id`, line span, `signature`, `docstring`, `symbol_hash` (change detection). `UNIQUE(snapshot_id, qualified_name, start_line)`.
- `code_edges` — `edge_kind (contains|imports|references)`, src/dst symbol+file, `dst_unresolved` (external specifier), `precision (exact|heuristic)`. Reverse index on `(snapshot_id, dst_symbol_id)` powers blast radius.
- `code_embeddings` — `VECTOR(768)` (nomic-embed-text, matches platform lock), `embedding_pending`, HNSW cosine index, per-symbol chunk.

**Blast radius** = recursive reverse traversal over `code_edges` (`dst_symbol_id` = changed symbol → `src_symbol_id` = dependents), bounded by `CODE_INTEL_MAX_BLAST_HOPS` — same bounded-traversal shape as `services/topology`.

### A4. Embeddings / code-RAG
- Chunk **per symbol** (`qualified_name + signature + docstring + body`, split oversized bodies); file-level only for symbol-less files. Reuse `make_embedder()` ([lib/embeddings/factory.py](lib/embeddings/factory.py)) → Ollama; mirror the existing `embedding_worker` pending-flag fill pass so the graph is queryable before vectors finish.
- Retrieval: embed signal text → HNSW cosine over `code_embeddings` scoped to the **latest `ready` snapshot per repo** → join to symbols/files, optionally expand neighbors via `code_edges`.

### A5. Self-update workers (`LongRunningService`, reuse `services/ingestion/workflows/runtime.py`)
- **Trigger:** [services/ingestion/handlers/github.py](services/ingestion/handlers/github.py) writes a `code_intel_index_triggers` outbox row on default-branch `push` and merged-PR events (one INSERT, flag-gated; observation path untouched).
- **`full_index`** (bootstrap): clone → `code_snapshots(status=indexing)` → walk tree → run SCIP/tree-sitter indexers → write files/symbols/edges + pending embeddings → `status=ready`. Idempotent via snapshot UNIQUE; crash leaves `indexing` and restarts cleanly (children CASCADE off snapshot).
- **`incremental_update`**: resolve prior `ready` snapshot as parent → fetch new sha → diff changed paths → new snapshot row → **copy-forward unchanged files/symbols/edges/embeddings** (by `blob_sha`/`symbol_hash`, vectors intact) → re-index only the delta → `status=ready` → refresh S3 cache. Readers always use the latest `ready` snapshot, so an in-flight update never exposes a half-built model; re-resolving parent to latest-ready self-heals out-of-order pushes.

---

## Part B — State + signal-enrichment engine (`services/github_intel/`)

### B1. Current-state model (FSM tables) — `db/migrations/0064_github_intel_state.sql`
Tenant-scoped + RLS. Each state row carries `state_version BIGINT` and `last_event_at TIMESTAMPTZ`; **transitions apply only when incoming `occurred_at >= last_event_at`** (ordering guard).
- `github_repo_state` — `(tenant, repo)`; `default_branch`, `head_sha` (= code-snapshot join key).
- `github_branch_state` — `(tenant, repo, branch)`; `head_sha`, `is_deleted`.
- `github_pr_state` — `(tenant, repo, pr_number)`; two orthogonal FSMs on one row: `lifecycle (open|draft|review_requested|changes_requested|approved|merged|closed)` and `ci_state (unknown|pending|passing|failing|error)`; `head_sha`, `merge_commit_sha`.
- `github_issue_state` — `(tenant, repo, issue_number)`; `status (open|closed)`.
- `github_check_state` — `(tenant, repo, head_sha, check_name)`; rolls up into `github_pr_state.ci_state`.

**Transitions** are driven by observation `content` (`event_type`/`action`/fields). Examples: `pull_request.opened[draft]`→draft; `ready_for_review`→open; review `changes_requested`/`approved` move `lifecycle`; `closed[merged=true]`→merged (terminal); `check_run.completed` moves only `ci_state`; `push.after`→branch/repo `head_sha`. Unknown/no-op events still produce an enrichment row (so *every* signal is enriched, not only state-changers).

### B2. Output — two writes, one source of truth

**(1) Inline `content.intelligence` (the signal-facing default).** During the normalize stage (see B3), the enrichment step augments the `ObservationDraft.content` before the observation is written, so the *same row* carries the reasoning:
```jsonc
content: {
  /* ...raw GitHub payload fields (event_type, action, pr_number, merged, ...)... */
  "intelligence": {
    "state_change": "open->merged",
    "entity": {"kind": "pr", "ref": "org/name#42"},
    "cause": "...", "effect": "...", "explanation": "...",
    "affected": {"files": [...], "symbols": [...], "blast_radius_count": N},
    "code_snapshot_sha": "abc123",
    "confidence": 0.0-1.0,
    "reasoning_path": "rule|llm",
    "enriched": true
  }
}
```
On timeout/error the key is simply absent → the **raw** signal is what gets ingested. `content_text` (the embedding/think seed) is composed from raw fields + (when present) the `explanation`, so downstream embedding and the generic `think` layer benefit from the insight.

**(2) Structured system-of-record** — `db/migrations/0065_github_intel_enrichment.sql`: `github_signal_enrichment`, joined to `observations` by `observation_id` (`UNIQUE(observation_id)` = idempotency anchor, upsert on reprocess): `state_before`/`state_after`/`state_changed`; `affected_files`/`affected_symbols`/`blast_radius`/`code_snapshot_sha`; `related_entities`; `cause`/`effect`/`explanation`/`confidence`/`reasoning_path`. This is what makes current-state queries, audit, and blast-radius lookups cheap (Option A) — `content.intelligence` is the per-signal view; the tables are the queryable truth.

Consumers (none blocking): query/API "explain this signal" + "current state of repo/PR"; the generic think layer also receives the insight via `content_text` (and optionally a follow-up `think_trigger_queue` row, subkind `github_enriched`).

### B3. Binding point — inline enrichment + ordered state worker

The work splits in two so the *content* is enriched at ingest time (default-enriched, raw-on-failure) **without** putting unbounded/out-of-order state writes on the parallel normalize stage:

**(a) Inline enrichment at normalize (writes `content.intelligence`).** Extend the github handler / a normalize-stage hook ([services/ingestion/handlers/github.py](services/ingestion/handlers/github.py)) so that after building the raw draft it runs a **bounded** enrichment (`GITHUB_INTEL_INLINE_TIMEOUT_MS`):
- `classify(content)` → compute the proposed transition; **read-only** lookups: load the current FSM state snapshot + Part A blast radius at the relevant `head_sha`/commit (no writes here, keeping the stage effectively stateless).
- Rule fast-path for obvious transitions (no LLM); LLM only for non-trivial blast radius / ambiguity, gated by `github_intel.llm_enabled`, and only if it fits the remaining timeout budget.
- Merge the result into `content.intelligence` and recompose `content_text`. **Any exception or timeout → return the raw draft unchanged** (raw is ingested). This step never blocks the webhook 202 (it's on the async normalize path) and never fails the ingest.

**(b) Ordered state-advancement worker** (`scripts/run_github_intel_worker.py`) — owns the authoritative, ordered FSM writes + the structured enrichment row, decoupled from the parallel normalize stage:
- **Work queue** `github_intel_queue` (`UNIQUE(observation_id)`, unclaimed index on `(tenant, repo, occurred_at)`).
- **Feeder:** LISTEN `observations_new` (writer already emits it) + periodic sweep backstop → INSERT `WHERE source_channel='github:webhook'` `ON CONFLICT DO NOTHING`. The existing writer/`ingest_from_draft` is untouched.
- **Loop:** claim oldest unclaimed **ORDER BY occurred_at** (`FOR UPDATE SKIP LOCKED`) under a **per-repo advisory lock** (one repo's FSM is never reordered) → load state `FOR UPDATE` → ordering guard (late/replayed events: write the enrichment row, `state_changed=false`, no state mutation) → **single txn**: upsert `github_signal_enrichment` + upsert state (`state_version+1`, `last_event_at=occurred_at`) + (on default-branch change) emit code re-index signal → `completed_at`. Dead-letter-as-row after 5 attempts (matches think worker).

This means the inline step composes `content.intelligence` against the state *as-of-normalize* (fast, best-effort, may be momentarily stale/out-of-order), while worker (b) keeps the structured tables strictly ordered and authoritative — the per-signal view and the system-of-record reconcile via `observation_id`.

### B4. Causal reasoning
- **Deterministic fast-path** (`is_obvious`) for the bulk: `merged=true`→"merged into <base>", issue close/reopen, check completion, branch head update — confidence ~1.0, **no LLM**.
- **LLM** only for non-trivial blast radius or ambiguous transitions, gated by `github_intel.llm_enabled`. Reuse [services/think/llm_reason.py](services/think/llm_reason.py) patterns (`LLMProvider.structured`, backoff, terminal parse error). Inputs: action + prior state + delta + blast-radius code context + recent related signals. Strict Pydantic `CausalExplanation` (`cause`, `effect`, `state_change`, `affected_entities[]`, `explanation`, `confidence`). On failure: persist rule-derived enrichment with lowered confidence — never drop the signal.

### B5. State ↔ code consistency (self-updating)
A state-changing action: (1) advances GitHub-state tables (cheap, authoritative — first), then (2) emits a `github_code_reindex` signal keyed `reindex:{repo}:{sha}` (sha = `merge_commit_sha` or `push.after`), consumed by Part A's incremental worker with sha dedup. **`(repo, sha)` is the only join key — never wall-clock.** Enrichment records the `code_snapshot_sha` it used; a blast radius computed against an un-indexed sha is recorded as such (reconcilable later), not silently trusted.

---

## Integration & ops
- **Scope:** feeder filters `source_channel='github:webhook'` only.
- **Feature flags** (reuse `TenantFlags`, 30s TTL, [services/ingestion/feature_flags/client.py](services/ingestion/feature_flags/client.py)): `code_intel.enabled`, `code_intel.incremental_enabled`, `github_intel.enabled`, `github_intel.llm_enabled`. All default false → opt-in per tenant (dogfood tenant first).
- **docker-compose** (`x-worker` anchor): `code_intel_full_index`, `code_intel_incremental`, `github_intel_worker`. Add `git` + the SCIP indexer toolchains to the worker image (`Dockerfile`).
- **Deps:** `pyproject.toml`/`uv.lock` — `tree_sitter` + grammars (fallback) pinned exactly; SCIP indexers installed in-image.
- **Env:** `CODE_INTEL_MAX_REPO_MB`, `CODE_INTEL_WORK_DIR`, `CODE_INTEL_S3_PREFIX`, `CODE_INTEL_MAX_BLAST_HOPS`, `CODE_INTEL_CLONE_DEPTH`, `CODE_INTEL_SCIP_TIMEOUT_MS`, `CODE_INTEL_MAX_CONCURRENT_INDEXES`, `CODE_INTEL_SNAPSHOT_RETENTION`, `GITHUB_INTEL_MAX_CONCURRENCY`, `GITHUB_INTEL_INLINE_TIMEOUT_MS` (bounds the inline enrichment before raw fallback); reuse `OLLAMA_URL`, `GITHUB_APP_*`, S3/MinIO vars.

## Scalability & cost controls

Cloning is the right *mechanism* (the Trees/Blobs API truncates and burns the REST budget), but **eagerly indexing every connected repo with full SCIP would not scale**. Scalability comes from the controls below, not the clone itself. Each issue → remedy:

| # | Issue | Remedy |
|---|---|---|
| 1 | **Eager indexing of all repos** across all tenants grows unboundedly. | **Opt-in + selective**: `code_intel.enabled` defaults off (per-tenant), and per-repo via the existing `selected_repositories` allowlist on `provider_installations`. Index a repo **lazily** — on explicit enable or first relevant signal — never all repos up front. |
| 2 | **Clone size / history** — full clones of large repos are huge. | **Shallow + partial clone** `--depth=1 --filter=blob:none --single-branch`: tree metadata + only the blobs actually parsed, no history. |
| 3 | **Disk footprint of working checkouts.** | **Ephemeral working copy** in `CODE_INTEL_WORK_DIR` (`/tmp/...`), **deleted after indexing**. Only the *derived graph* (symbols/edges/embeddings) + a compact bare-repo cache in S3 persist — far smaller than source. |
| 4 | **One-time full-index cost** — SCIP precise indexing is CPU/memory-heavy (scip-python needs resolved deps, scip-java needs a build). | **Bounded SCIP**: per-language subprocess with `CODE_INTEL_SCIP_TIMEOUT_MS` + memory cap; on timeout/failure **fall back to cheap tree-sitter** (`precision='heuristic'`). The graph is still produced, just less precise. |
| 5 | **Onboarding bursts** — a tenant with N repos triggers N full indexes at once. | **Concurrency cap** `CODE_INTEL_MAX_CONCURRENT_INDEXES` (worker claims via `FOR UPDATE SKIP LOCKED`, so excess just queues). Full-index work is a claimable queue, not a fan-out. |
| 6 | **Oversized repos / monorepos.** | `CODE_INTEL_MAX_REPO_MB` → mark snapshot `status='skipped_too_large'` (never silently partial). **Subtree scoping** (later): per-repo path-glob to bound indexing to relevant directories. Default ignore-globs exclude `node_modules`/`vendor`/`dist`/generated files. |
| 7 | **Snapshot storage growth** — copy-forward duplicates rows per snapshot. | **Retention/GC job**: keep the N latest `ready` snapshots per repo (`CODE_INTEL_SNAPSHOT_RETENTION`), CASCADE-delete older ones. Copy-forward already avoids *re-embedding* unchanged symbols (the expensive part). |
| 8 | **Re-indexing churn** — a busy repo pushes constantly. | **Delta-only steady state**: after bootstrap, `git fetch <range>` + parse only changed files; **debounce/coalesce** rapid pushes by deduping `code_intel_index_triggers` on `(repo, latest sha)` so only the newest pending sha indexes. |
| 9 | **Embedding throughput** — thousands of symbols hammer Ollama on a full index. | **Decoupled pending-fill pass** (`embedding_pending` flag) + batching, reusing the existing `embedding_worker` back-pressure pattern. The graph is queryable before vectors finish. |
| 10 | **Inline enrichment latency** on the ingest path. | Strict `GITHUB_INTEL_INLINE_TIMEOUT_MS` budget, rule fast-path (no LLM) for the bulk, **read-only** blast-radius lookups against the pre-built graph (no indexing on the hot path); over budget → raw fallback. |

**Net:** precise full-codebase blast radius needs a full index **once per repo**, but it is gated (opt-in), shallow, ephemeral, capped (size + concurrency + SCIP timeout), GC'd (retention), and **delta-only afterward**. The only thing that doesn't scale — unbounded eager full-SCIP of every repo — is exactly what these controls prevent. New env vars: `CODE_INTEL_SCIP_TIMEOUT_MS`, `CODE_INTEL_MAX_CONCURRENT_INDEXES`, `CODE_INTEL_SNAPSHOT_RETENTION` (added to the env list above).

## Critical files
- New: `services/code_intel/**`, `services/github_intel/**`, `scripts/run_github_intel_worker.py`, migrations `0063`–`0065`.
- Extend: [services/integrations/github/client.py](services/integrations/github/client.py) (clone URL), [services/ingestion/handlers/github.py](services/ingestion/handlers/github.py) (index-trigger INSERT), `docker-compose.yml`, `Dockerfile`, `pyproject.toml`.
- Reuse: [services/ingestion/workflows/runtime.py](services/ingestion/workflows/runtime.py) (worker base), [lib/embeddings/factory.py](lib/embeddings/factory.py), [services/think/llm_reason.py](services/think/llm_reason.py), [services/ingestion/feature_flags/client.py](services/ingestion/feature_flags/client.py).

## Phased build order
1. **Code graph foundation** — migration 0063; indexer interface + registry; SCIP ingest + tree-sitter fallback; graph repo + blast-radius query. Unit-test parsing against this repo. *Gate: query symbols/edges for a synthetic snapshot.*
2. **Fetch + full index** — clone adapter, working-copy/S3 cache, `full_index` worker. *Gate: full index of this repo end-to-end → `code_snapshots.status=ready`.*
3. **Embeddings + code-RAG** — pending-fill pass + semantic retrieval. *Gate: code-RAG returns relevant symbols for a query.*
4. **Incremental self-update** — trigger outbox + handler INSERT + `incremental_update` copy-forward/delta. *Gate: a real push advances the snapshot.*
5. **State FSMs + ordered worker (rule-only)** — migrations 0064/0065, `fsm.py`, `state_store.py`, feeder + worker (b), rule-path structured enrichment. *Gate: webhook → observation → `github_signal_enrichment` row + FSM state with correct before→after.*
6. **Inline content enrichment + raw fallback** — normalize-stage hook writing `content.intelligence` (rule-only), bounded by `GITHUB_INTEL_INLINE_TIMEOUT_MS`. *Gate: success → `content.intelligence.enriched=true`; forced timeout/error → raw content with no `intelligence` key, ingest still succeeds.*
7. **Code integration** — wire blast radius + code-RAG into both the inline step and worker (b); `github_code_reindex` emission on default-branch changes.
8. **LLM causal reasoning** — `reasoner.py` + schema, flag-gated (`github_intel.llm_enabled`), rule fallback; LLM only runs inline if within the timeout budget, else worker (b) upgrades the structured row.
9. **Ops + think feedback** — compose registration, observability/dead-letter, optional `github_enriched` follow-up trigger, backfill sweep validation.
10. **Scalability hardening** — concurrency cap on full-index, trigger debounce/coalesce on `(repo, latest sha)`, snapshot retention/GC job, SCIP timeout→tree-sitter fallback, size/ignore-glob caps (see Scalability & cost controls).
11. **(Later) language matrix expansion** — add SCIP indexers per language behind the pluggable interface.

## Verification
- **Unit:** indexer parsing on in-repo fixtures (symbol counts, edge precision); `fsm.py` transition tables over synthetic `content`; idempotency (re-process → upsert, not duplicate).
- **Integration (live PG):** full index of this repo → assert `code_snapshots.status='ready'` + non-zero symbol/edge counts; blast-radius query returns dependents of a known symbol.
- **End-to-end (docker-compose, dogfood tenant with flags on):** replay a captured push/PR/merge webhook via the existing synthetic/mock framework (`services/synthetic/mock_servers/`) → assert (a) the observation row's `content.intelligence` is present with correct `state_change` + `cause`/`effect`/`explanation` + `affected`, (b) the `github_signal_enrichment` row + FSM state match (`state_before`/`state_after`, `blast_radius`, `code_snapshot_sha`), (c) a merge advances `github_repo_state.head_sha` and emits a `github_code_reindex` signal that produces a new `ready` code snapshot.
- **Raw-fallback (the key new guarantee):** force the inline step to fail/exceed `GITHUB_INTEL_INLINE_TIMEOUT_MS` (fault injection) → assert the observation is still written with **raw content and no `intelligence` key**, ingest returns success, and worker (b) still later writes the structured enrichment row. Confirms "on failure/timeout, the raw GitHub signal is what gets ingested."
- **LLM path:** with `github_intel.llm_enabled`, a non-trivial merge yields `reasoning_path='llm'` and a coherent causal explanation; with it off (or over budget), rule fast-path still produces an enrichment.
