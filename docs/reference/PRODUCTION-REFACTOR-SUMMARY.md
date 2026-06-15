# Production Refactor Goal Summary

Status: Paused after incremental implementation work  
Last measured from this checkout: 2026-06-13  
Primary plan: `docs/reference/PRODUCTION-REFACTOR-PLAN.md`

## Executive Summary

This refactor goal made the codebase more production-shaped by adding
mechanical guardrails and reducing large, hard-to-review functions. It did not
finish the broader production-readiness objective, and it did not reduce total
repository LOC. The main measurable improvement was a reduction in long-function
hotspots, plus new ratchets that prevent those hotspot counts and architectural
violations from quietly growing again.

The most important caveat: many edits extracted long functions into named
helpers. That improves readability, ownership, and reviewability, but it can add
net lines in the short term. The next phase should explicitly target
duplication, helper sprawl, and net-LOC control rather than continuing only to
split long functions.

## Current Measured State

Metrics from `scripts/report_tech_debt_metrics.py --format markdown --top 12`:

| Metric | Current value |
| --- | ---: |
| Python files | 1,770 |
| Python lines | 495,247 |
| Test files | 787 |
| Non-test files | 983 |
| Files above threshold | 29 |
| Functions above threshold | 16 |
| Classes above threshold | 21 |
| Import-linter ignored imports | 71 |
| Raw Think trigger insert violations | 0 |
| Raw model re-eval insert violations | 0 |
| Raw pending post-commit action insert violations | 0 |
| Raw Think obligation insert violations | 0 |
| Parse errors | 0 |

## Production Guardrails Added Or Tightened

- Added `scripts/check_tech_debt_budget.py` as the main technical-debt ratchet.
- Added `scripts/report_tech_debt_metrics.py` as the static debt dashboard.
- Added or strengthened architecture ratchets for raw queue ownership.
- Added production environment contract checks.
- Kept `lint-imports` green and capped import-linter allowlist debt.
- Ratcheted raw production writes for:
  - `think_trigger_queue`
  - `model_reeval_queue`
  - `pending_post_commit_actions`
  - `think_obligations`
- Added production fail-closed checks around env/config paths, including
  production tenant fallback rejection and explicit production embedder backend
  selection.

## Major Refactor Results

### Platform Execution Split

`services/platform/execution/inquiry.py` was split into focused modules for:

- config
- DTOs/types
- routing
- runtime metrics
- language signals
- lexical terms
- motif utilities
- evidence/context packet helpers
- question policy and planning
- retrieval planning/actions/learning
- action execution/cache
- answer evaluation
- persistence
- SAGE reader execution/notes
- inquiry bootstrap, rounds, and finalization

The legacy import path was kept compatible while canonical homes were created.
The file is now capped in the technical-debt budget.

### Gateway And Product Router Splits

Several route factories were reduced to small registration functions with
module-level endpoint handlers:

- `services/app/gateway/recommendations_router.py`
- `services/app/gateway/debug_router.py`
- `services/app/gateway/structure_router.py`
- `services/app/gateway/finance_router.py`
- `services/app/gateway/today_routes.py`
- `services/product/decision_deltas/router.py`
- `services/product/forecasts/router.py`
- `services/product/resolution_threads/router.py`
- `services/app/gateway/slack_router.py`
- `services/app/gateway/ceo_view_wiring.py`

### Reasoning And Retrieval Splits

Large reasoning and retrieval coordinators were broken into named phases while
preserving public behavior:

- `services/reasoning/think/reason.py:think`
- `services/reasoning/think/reason.py:_run_once`
- `services/reasoning/think/applier.py:apply_diff`
- `services/reasoning/think/applier.py:_apply_claim_op`
- `services/reasoning/think/applier.py:_apply_act_op`
- `services/reasoning/think/reconciler.py:_reconcile_inner`
- `services/reasoning/think/validator.py:validate`
- `services/reasoning/retrieval/primary.py:primary_retrieve`
- `services/reasoning/retrieval/pathways.py:pathway_a_structural`
- `services/reasoning/retrieval/pathways.py:pathway_b_semantic`
- `services/reasoning/retrieval/pathways.py:pathway_g_model_edges`
- `services/reasoning/retrieval/assembler.py:assemble_context`
- `services/reasoning/sage/reader.py:read`
- `services/reasoning/sage/outcome_evaluator.py:_evaluate`
- `services/reasoning/sage/evidence_projection.py:_rank_for_model`
- `services/reasoning/sage/health.py:build_sage_health_report`
- `services/reasoning/sage/cue_extractor.py:_extract_sync`
- `services/reasoning/contestability/service.py:contest_model`

### Ingestion And Workflow Splits

Ingestion-related hotspots were split while preserving workflow semantics:

- `services/ingest/ingestion/core.py:ingest_from_draft`
- `services/ingest/integrations/gmail/fetcher.py:drain_mailbox_history`
- `services/ingest/ingestion/workflows/shard_fetch.py:_run_fetch_loop`
- `services/ingest/ingestion/writers/observation_writer.py:_handle_message`
- `services/ingest/ingestion/feature_flags/circuit_breaker.py:_process_tick`
- `services/ingest/synthetic/backfill_harness/harness.py:_write_install_and_trigger`
- `services/ingest/synthetic/live_generators/hmac_webhook.py:_build_payload`
- `services/ingest/synthetic/mock_servers/google_workspace.py:_make_handler`
- `services/ingest/synthetic/validation_runs/composition.py:build_live_drivers`
- `services/ingest/synthetic/validation_runs/run4_concurrent.py:run4`

### Benchmark, Probe, And Real-LLM Harness Splits

Operational and benchmark scripts were reduced into named phases:

- `scripts/run_storyline_batch_benchmark.py:run_benchmark`
- `scripts/run_storyline_batch_benchmark.py:score_storylines`
- `scripts/run_storyline_batch_benchmark.py:_product_value_evals`
- `scripts/run_storyline_batch_benchmark.py:_company_intelligence_scorecard`
- `scripts/run_1000_signal_model_layer_probe.py:collect_model_layer_report`
- `scripts/run_1000_signal_model_layer_probe.py:main`
- `benchmarks/run_benchmark.py:main`
- `scripts/run_100x_5000_model_e2e_stress.py:_build_case_models`
- `tests/real_llm/infrastructure/scenario_loader.py:materialize`

### Latest Test-Hotspot Splits

The final stretch focused on long test functions and quality gates:

| Function | Before | After |
| --- | ---: | ---: |
| `test_oauth_trigger_to_source_completion_end_to_end` | 420 | 53 |
| `test_oauth_trigger_to_tenant_completion_with_reconciler_reshare_path` | 337 | 35 |
| `test_shard_fetch_resumes_from_persisted_cursor_after_restart` | 303 | 56 |
| `test_quality_eval_corpus_mixed_entrypoints_regression_gate` | 292 | 19 |

The extraction preserved the original assertions and moved fixture setup,
worker/process handling, polling loops, and suite-level summaries into named
helpers.

## Verification Performed

The latest slices were verified with:

- `py_compile` for touched Python files
- `ruff check --select E9,F63,F7,F82,F821,F811,F401` on touched files
- repo-wide `ruff check --select E9,F63,F7,F82,F821,F811,F401 .`
- `scripts/check_tech_debt_budget.py`
- `scripts/tests/test_check_tech_debt_budget.py`
- `services/reasoning/retrieval/tests/test_retrieval_quality_harness.py`
- `scripts/check_architecture_ratchets.py`
- `scripts/check_production_env_contract.py`
- `lint-imports`
- `git diff --check`

Known caveat: subprocess E2E tests that depend on `moto_s3_server` could not run
in this local environment because `moto` is not installed. They failed in the
shared fixture before test bodies executed.

## What Is Still Remaining

This goal is not complete. Remaining measured debt includes:

- 16 functions above the long-function threshold.
- 29 files above the file-size threshold.
- 21 classes above the class-size/method-count threshold.
- 71 import-linter ignored imports still allowed by current contracts.
- Total Python LOC has increased, not decreased.
- Several large modules still need structural cleanup:
  - `scripts/run_storyline_batch_benchmark.py`
  - `benchmarks/fyralis_eval/fyralis_db.py`
  - `services/reasoning/think/applier.py`
  - `services/reasoning/sage/reader.py`
  - `services/domain/models/repo.py`
- Several large classes remain:
  - `services/reasoning/think/worker.py:ThinkWorker`
  - `services/reasoning/sage/topology_optimizer/optimizer.py:TopologyOptimizer`
  - `services/domain/models/repo.py:ModelsRepo`
  - `services/reasoning/sage/reader.py:SynthesisReader`

Current top long-function targets:

| Lines | Function | Location |
| ---: | --- | --- |
| 287 | `build_fixture` | `services/reasoning/retrieval/tests/_fixtures.py` |
| 267 | `test_oauth_trigger_to_tenant_completion_end_to_end` | `services/ingest/ingestion/workflows/tests/test_oauth_to_tenant_completion_end_to_end.py` |
| 251 | `_seed_stress_corpus` | `tests/unit/sage/test_sage_100_large_e2e_stress.py` |
| 249 | `test_oauth_trigger_to_gmail_completion_with_reshare` | `services/ingest/ingestion/workflows/tests/test_oauth_to_gmail_completion_with_reshare.py` |
| 245 | `test_oauth_trigger_to_gmail_completion_end_to_end` | `services/ingest/ingestion/workflows/tests/test_oauth_to_gmail_completion_end_to_end.py` |
| 233 | `_run_e2e` | `tests/e2e/test_google_workspace_e2e.py` |
| 222 | `run` | `scripts/sandbox_carta.py` |
| 220 | `_insert_scaffold` | `scripts/run_100x_5000_model_e2e_stress.py` |

## Recommended Next Step

Stop open-ended hotspot splitting and run a stabilization pass:

1. Group the current changes into reviewable commits or PRs.
2. Add an explicit net-LOC and duplication budget so helper extraction does not
   keep growing the repository.
3. Consolidate repeated test subprocess helpers introduced by the latest E2E
   refactors.
4. Install or pin the missing `moto` dependency for subprocess E2E verification,
   or mark those tests with a clear optional dependency gate.
5. Continue with smaller, targeted PRs against the remaining top hotspots only
   after the current branch is reviewable.
