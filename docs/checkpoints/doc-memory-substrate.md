# CHECKPOINT — Document Memory Substrate

**Branch:** `feat/document-memory-substrate`
**Date:** 2026-06-24
**Design doc:** [`docs/plans/document-memory-substrate.md`](../plans/document-memory-substrate.md)

Goal of the substrate: make large ingested documents recallable by reasoning,
shipped migration-free by mapping onto existing schema (claim roles, JSONB
structured fields, `born_from_event_id` provenance) rather than adding tables.

## Milestone summary

Phases 0, 1, and 2 are implemented:

- **Phase 0 — Persist structured summary + map-reduce.** The summarizer no
  longer discards its structured output; the durable structured summary
  (`list[ActionItem]{who, what, due}` plus narrative) is persisted, with a
  map-reduce pass over large documents.
- **Phase 1 — Distill into durable Models recalled by Pathways.** Document
  content is distilled into Models surfaced through the existing recall
  Pathways; the doc-memory-owned Think file (`prompt.py`) renders the mint
  prompt (rendering only — it performs no insert).
- **Phase 2 — Observability + scope resolution.** Doc-memory metrics, a
  `DocMemoryMintFailure` alert, a deployed `deadline_resolver` worker (for
  proactive deadline firing), and action-item owner/scope resolution.

Phases 1 (chunked-RAG) and 3 (agentic-recall) from the design remain DEFERRED.

## Remediation applied (Phase 2)

Four-part remediation landed in commits `514662e`, `ce130b8`, `5ddc5e4`:

1. **Metric-semantics fix (honest dispatch vs. true mint).** The misleading
   dispatch-time counter was renamed to `doc_memory_enriched_t1_total` — an
   honest "documents handed to Think" signal — across `metrics.py`, its
   increment site in `_enrich_t1_payload`, and all docstrings/comments. A TRUE
   `doc_memory_models_minted_total` (by source) plus a one-line,
   provenance-keyed helper `record_doc_memory_model_minted()` were added in the
   same EOF `doc_memory` block. The per-mint increment was **deliberately NOT
   wired**: the only real document-derived insert is
   `services/reasoning/think/applier.py::_apply_claim_insert` (via
   `ModelsRepo.insert`), which is owned by the parallel reasoning/BYOC track and
   was never touched by any doc-memory commit. Wiring it would create the exact
   cross-branch intersection the task forbids, so the documented escape hatch
   was used: the metric + ready-to-call helper are in place and tested, keyed on
   `born_from_event_id` provenance, for whichever track owns the applier. No
   misleadingly-named metric remains. The `DocMemoryMintFailure` alert
   (referencing `doc_memory_mint_failure_total`) was unaffected by the rename.
2. **Phase-2 tests.** Substantive pure-python coverage added for metrics
   (render-values, label collapse, dispatch/mint isolation), the Grafana
   alert-rules parse, the `deadline_resolver` `docker compose config` (run, not
   skipped, no `.env.production` leak), the batch nested-schema shape, and the
   new owner-resolution paths.
3. **Batch decision — KEEP nested schema.** The batch lane sends
   `DocumentSummarySchema.model_json_schema()` as a `json_schema` text format
   with `strict: False`; non-strict mode tolerates the nested
   `action_items -> $ref ActionItem` objects and `$defs`/`$ref`. Verified the
   request line carries a well-formed nested schema with all refs resolving and
   the body round-tripping through orjson — no real incompatibility, so no flag
   or guard was added. (If strict Structured-Outputs mode is ever enabled, it
   would restrict nesting and require an inlined schema — out of scope.)
4. **Owner decision — FIX (low-risk, testable).** Added
   `resolve_owner_actor()` in the doc-memory-owned `scope_resolution.py`: it
   tries the existing source-ref path, then falls back to matching the bare
   `who` against an active actor's `display_name`
   (case/whitespace-insensitive, exact-after-normalization, read-only via the
   existing `ActorRepo.list_active_actors`). It refuses ambiguous multi-actor
   matches and never invents IDs — only resolved UUIDs enter `scope_actors`;
   unmatched/ambiguous owners stay as text. Wired into
   `doc_memory.resolve_document_scope` (now passing `tenant_id`). The shared
   `services/domain/actors/repo.py` was NOT modified.

## Verify verdict

**`concerns`** — read-only verification of the Phase-2 remediation.

- Checks #2 (tests), #3 (batch), #4 (owner): CLEAN. 35 pure-python Phase-2
  tests pass on the specified venv + PYTHONPATH with substantive assertions
  across all four areas. Decisions #3 and #4 are both sound (nested schema
  genuinely tolerated under `strict: False`; owner-by-display-name resolution is
  read-only, exact-after-normalization, refuses ambiguity, never invents IDs,
  leaves `repo.py` untouched).
- Check #1 (metric semantics): **SOLE CONCERN**, medium severity. The rename is
  honest and the true `doc_memory_models_minted_total` counter has correct
  provenance semantics, but that counter is **never incremented in production**
  — it is called only from tests. The literal requirement (increment at an
  actual Think mint site) is unmet at runtime. The implementer's justification
  holds: the genuine insert site (`applier.py::_apply_claim_insert`) belongs to
  the parallel BYOC/reasoning track and was never touched by any doc-memory
  commit; wiring it would create the forbidden cross-branch intersection, so the
  documented escape hatch was used and a ready-to-call helper left in place. A
  low-severity note: a test docstring overstates that it exercises the real mint
  site (the assertion itself is real, not vacuous).

Net: `concerns`, not `fail` — the gap is a justified, migration- and
conflict-safe unwired metric, not wrong-semantics or a BYOC-file violation.

## Migration-free status

**Migration-free: YES.** No migration was added (`migration_added: false`); zero
DB/migration files in the diff. The substrate continues to ride on existing
schema (claim roles, JSONB structured fields, `born_from_event_id` provenance).

## Conflict safety

`metrics.py` changes are confined entirely to the EOF `doc_memory` block (after
`__all__` at line 353; hunks at lines 363+ and 473+) — the BYOC-owned file body
is untouched. No BYOC/forbidden files in the diff
(`forbidden_or_byoc_files_touched: []`); no `llm_reason.py` /
`circuit_breaker.py` / `oauth_refresh.py` / `repo.py` modifications.

## Merge-readiness

**Ready to merge.** Verify verdict is `concerns` (not `fail`): the single open
item is a deliberately-deferred, fully-justified one-line metric increment that
must be wired by the track owning `applier.py`, NOT a defect in doc-memory's own
code. The substrate is migration-free, conflict-safe, and test-covered. The
follow-up (one-line `record_doc_memory_model_minted()` call at
`applier.py::_apply_claim_insert`, keyed on `born_from_event_id`) is owned by
the parallel reasoning/BYOC track and is the only thing standing between the
true mint counter and a non-zero conversion-rate numerator.

## Commits in this milestone

- `514662e` — fix(obs): honest doc-memory mint metrics — DISPATCH vs TRUE mint (Phase 2)
- `ce130b8` — fix(ingest): resolve action-item owners by display-name (doc-memory Layer 2)
- `5ddc5e4` — test(doc-memory): Phase-2 observability + batch nested-schema coverage
