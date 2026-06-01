# GitHub Intelligence Layer — Implementation Status

Status of the build against [plan.md](plan.md). Phase 1 (rule-based, dogfood)
is **complete and verified end-to-end** on the live dev stack.

## What's built

### Part A — code comprehension (`services/code_intel/`)
- `parsing.py` — language-pluggable `LanguageIndexer` Protocol + registry. Shipped
  backbone is a **zero-dependency Python `ast` indexer** (precise for Python).
  tree-sitter / SCIP slot in behind the same Protocol (the `precision` column on
  `code_edges` already distinguishes `exact` vs `heuristic`).
- `graph.py` — `CodeGraphRepo`: snapshot lifecycle, bulk writes, and the
  **blast-radius** query (recursive reverse traversal over `imports`/`references`
  edges) + `search_code` (pgvector code-RAG).
- `indexer.py` — `index_working_copy`: walk a checkout → files/symbols/edges +
  pending embeddings → a `ready` `code_snapshots` row. Idempotent per commit sha.
- `embed.py` — best-effort pending-embedding fill via the shared Ollama embedder.
- `reindex.py` — drains `code_intel_index_triggers` to re-index at a new sha
  (the self-update loop), linking `parent_snapshot_id`.

### Part B — state + enrichment (`services/github_intel/`)
- `fsm.py` — `classify()` + PR-lifecycle / issue-status / CI-rollup transitions +
  the deterministic **rule reasoning** fast path (no LLM for the bulk).
- `state_store.py` — `read_state_snapshot` (inline read-only) + `apply_event`
  (authoritative ordered write with the `occurred_at >= last_event_at` guard).
- `code_client.py` — bridge to code_intel: changed-path extraction + blast radius
  + code-RAG.
- `enrichment.py` — assembles the `content["intelligence"]` dict + the structured
  enrichment record.
- `inline.py` — the **inline hook**: flag-gated, timeout-bounded, raw-on-failure.
- `reasoner.py` — optional flag-gated LLM causal step (`CausalExplanation`).
- `worker.py` — the **ordered worker**: feeder sweep + per-repo-advisory-locked
  drain → authoritative FSM + `github_signal_enrichment` + reindex emission.

### Wiring
- `services/ingestion/core.py` — inline enrichment hook in `ingest_from_draft`
  for `github:webhook` (raw-on-failure; never breaks ingest).
- `services/ingestion/handlers/github.py` — push `files`, PR `head_sha`/
  `head_ref`/`merge_commit_sha`/`changed_files` added to `content` (drive blast
  radius without the raw payload).
- `db/migrations/0063_code_intel.sql`, `0064_github_intel_state.sql`,
  `0065_github_intel_enrichment.sql`.
- `scripts/run_github_intel_worker.py` + `docker-compose.yml` service
  `github_intel_worker`.

## How to see it working

```bash
# infra already up: postgres:5434, ollama:11434
export DATABASE_URL=postgresql://company_os:company_os@localhost:5434/company_os
export COMPANY_OS_TENANT_ID=00000000-0000-0000-0000-000000000001
python scripts/demo_github_intel.py
```

The demo indexes a sample repo, injects a realistic webhook sequence
(issue → PR open → push → CI → review → merge → issue close → main pushes)
through the real ingestion path, drains the worker, drains reindex, then prints:
- **RESULT A** — every GitHub signal as an *enriched observation*
  (`content.intelligence`: state_change, cause/effect/why, blast radius, related).
- **RESULT B** — the current repo FSM state (PR merged/passing, issue closed,
  branch heads, repo HEAD).
- **RESULT C** — the `github_signal_enrichment` system-of-record.
- **RESULT D** — code-RAG semantic search.
- **RESULT E** — the raw-fallback guarantee (enrichment off → raw signal ingested).

A JSON artifact of the enriched observations + system-of-record is written to
`/tmp/ghintel_demo_result.json`.

### Verified behaviours (from a real run)
- A merge of `app/auth.py` → blast radius of 4 dependents (`app/api.py`,
  `app/main.py` + 2 symbols); a risky push to the high-fan-out `app/db.py` →
  **7 dependents** across api/auth/ratelimit/main. The layer reasons about the
  actual code graph, not just the event.
- PR #42 FSM: `open → approved → merged`, `ci_state → passing` from the check;
  issue #12: `open → closed`; repo HEAD advances only on default-branch pushes.
- Merge/push to `main` emits `code_intel_index_triggers` → reindex produces a new
  snapshot (the self-update loop).

## Tests
- `tests/unit/test_github_intel_fsm.py` — 22 pure-logic tests (FSM, reasoning,
  ast parsing, blast-radius wiring).
- `tests/integration/test_github_intel_pipeline.py` — 4 live-DB tests: full
  pipeline + enrichment + blast radius, ordering guard (no regression on late
  events), raw-on-disabled, raw-on-timeout.
- `pytest tests/unit/test_github_intel_fsm.py tests/integration/test_github_intel_pipeline.py`
  → **26 passed**.

## Deferred (per plan)
- Real `git clone` fetch via the App token (here the working copy is local;
  the indexer is identical regardless of how bytes arrive).
- SCIP / tree-sitter precise multi-language indexers (Protocol seam ready).
- LLM causal step is implemented + flag-gated (`github_intel.llm_enabled`) but
  off by default (rule path covers the dogfood scenarios; enabling needs an
  `LLM_API_KEY`).
- S3 working-copy cache, snapshot retention/GC, the standalone feeder LISTEN
  (the worker uses the periodic sweep; LISTEN is an additive optimization).
