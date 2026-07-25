# Fyralis Core Production Refactor Plan

Status: In progress  
Last reviewed from this checkout: 2026-06-13  
Scope: Fyralis Core backend, workers, database, tests, scripts, and docs. The UI
overlay repo is out of scope except where API contracts are affected.

> Historical plan: references to
> `services/ingest/synthetic/mock_servers/` record the implementation that
> existed when this plan was measured. Those servers are retired and deleted;
> `services/ingest/synthetic/provider_lab/` is now canonical.

## 0. Executive Summary

Fyralis Core does not need a ground-up rewrite. The existing architecture has
good bones: layered packages, real Postgres tests, import-linter, migrations,
observability records, background workers, and substantial documentation. The
problem is that too many important architectural rules are still enforced by
convention instead of by code, CI, schema constraints, single owning modules, or
runtime health checks.

The production-grade refactor should therefore follow one rule:

> Make the intended design mechanically true, then shrink the modules that hide
> that design.

The work should be shipped as a sequence of small, reversible PRs. Each PR must
either reduce a measurable hotspot, add a guardrail that prevents future debt, or
make a documented runtime path actually operate in production.

This plan deliberately avoids another broad directory reorganization. The code
already went through a major layering pass. Another rename-first refactor would
create churn without fixing the deeper issues: mutation paths, worker wiring,
queue entry points, embedding lifecycle, ingestion split-brain, and oversized
modules.

### Implementation Checkpoint - 2026-06-12

First implementation loop completed the production-readiness ratchets before
attempting large extractions:

- conservative ruff gate is green
- import-linter is green, with allowlist growth now capped by CI
- raw production writes to `think_trigger_queue` and `model_reeval_queue` are
  ratcheted to their owning modules and documented exceptions
- technical-debt dashboard reports large files/functions/classes, import-linter
  allowlists, queue-write violations, and parse errors
- technical-debt budget gate caps hotspot counts, import-linter ignores,
  queue-owner violations, and parse errors
- runtime manifest, compose services, healthchecks, and Python entrypoints have
  bidirectional tests
- `housekeeper_worker` is represented in compose/runtime surfaces
- Query/Ask rendering and cache adapters fail closed in production
- greeting scheduler construction routes through the fail-closed rendering
  adapter factory instead of defaulting directly to the mock
- embedder backend selection fails closed in production unless
  `EMBEDDER_BACKEND` is explicit
- tenant env fallbacks (`DEFAULT_TENANT_ID`, `COMPANY_OS_TENANT_ID`) are
  rejected in production startup/template checks
- `.env.production.example` is checked by a production env contract gate
- operational readiness harness includes the env contract preflight and reuses
  the current Python interpreter for nested Python gates
- benchmark cache/raw/tmp/generated report roots, local truss probe outputs, and
  agent run logs are ignored as generated artifacts

### Implementation Checkpoint - 2026-06-13

Second implementation loop widened the queue-owner ratchets without changing
runtime behavior:

- raw production writes to `pending_post_commit_actions` are ratcheted to
  `services/reasoning/think/post_commit.py`
- raw production writes to `think_obligations` are ratcheted to
  `services/domain/obligations.py`
- architecture ratchet code now uses one shared raw-insert scanner for the
  queue ownership checks
- technical-debt dashboard and budget gate now track four queue-owner violation
  counts: Think triggers, model re-eval, pending post-commit actions, and Think
  obligations
- focused ratchet/dashboard/budget tests pass with the new counters
- `InquiryConfig` now has a canonical home in
  `services/platform/execution/config.py`, while the legacy
  `services.platform.execution.inquiry` import path remains compatible
- the config extraction dropped the long-function dashboard count from 80 to 79
- public inquiry DTOs now have a canonical home in
  `services/platform/execution/types.py`, while both
  `services.platform.execution` and `services.platform.execution.inquiry`
  imports remain compatible
- LLM question-planning schemas now have a canonical home in
  `services/platform/execution/question_planning_schemas.py`, while legacy
  `services.platform.execution.inquiry` imports remain compatible
- runtime timing helpers now have a canonical home in
  `services/platform/execution/runtime_metrics.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- duplicated inquiry env parsing now reuses `services/platform/execution/config.py`
- route/budget helpers now have a canonical home in
  `services/platform/execution/routing.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- language-signal predicates now have a canonical home in
  `services/platform/execution/language_signals.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- evidence/context-packet utility helpers now have a canonical home in
  `services/platform/execution/evidence_utils.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- lexical term extraction helpers now have a canonical home in
  `services/platform/execution/lexical_terms.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- retrieval motif utility helpers now have a canonical home in
  `services/platform/execution/motif_utils.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- question policy and selection helpers now have a canonical home in
  `services/platform/execution/question_policy.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- question-planning runtime option helpers now have a canonical home in
  `services/platform/execution/question_planning_runtime.py`, while legacy
  private `services.platform.execution.inquiry` helper imports remain
  compatible
- retrieval action cache and scope-binding helpers now have a canonical home in
  `services/platform/execution/action_cache.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- SAGE reader diagnostics, learned-route gates, note recording, persistence
  compaction, and action timing summaries now have a canonical home in
  `services/platform/execution/sage_reader_notes.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- question text anchoring, focus cleanup, UUID filtering, and deterministic
  question phrasing helpers now have a canonical home in
  `services/platform/execution/question_text.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- deterministic inquiry hypotheses, initial unknowns, fallback candidate
  questions, and unknown deduplication now have a canonical home in
  `services/platform/execution/question_generation.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- retrieval action plan compilation, focused-index action planning, and motif
  action overlays now have a canonical home in
  `services/platform/execution/retrieval_plan.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- LLM-backed inquiry question-planning orchestration, belief-delta
  normalization, question quality repair, and LLM/deterministic safety merging
  now have a canonical home in
  `services/platform/execution/question_planning.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- learned question policy loading, retrieval motif matching, motif success
  learning, motif failure penalties, and low-value model-noise classification
  now have a canonical home in
  `services/platform/execution/retrieval_learning.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- evidence minimization, evidence scoring, state-contract packet assembly, and
  candidate state-change hints now have a canonical home in
  `services/platform/execution/context_packet.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- question answer classification, premise/owner challenge detection,
  resolved-unknown accounting, and inquiry sufficiency gates now have a
  canonical home in `services/platform/execution/answer_evaluation.py`, while
  legacy private `services.platform.execution.inquiry` helper imports remain
  compatible
- focused-index action execution, semantic-hybrid lexical fallback, bounded
  lookup timeout guards, and pathway model caps now have a canonical home in
  `services/platform/execution/retrieval_actions.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- pathway result wrapping, retrieval-result merging, relevance gating,
  structural-link packing, coverage-aware compaction, and reservoir upsert now
  have a canonical home in
  `services/platform/execution/result_composition.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- inquiry session/question/evidence persistence, Sage reader activation and
  decision-attribution persistence, Phase 1 trace emission, omission
  classification, and reader-attribution caps now have a canonical home in
  `services/platform/execution/inquiry_persistence.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- Sage reader runtime construction, single-question Sage reader execution, and
  per-round serial/parallel Sage reader execution now have a canonical home in
  `services/platform/execution/sage_reader_execution.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- retrieval action execution, staged motif action binding, serial/parallel
  question-action scheduling, action timing notes, and question retrieval plan
  records now have a canonical home in
  `services/platform/execution/action_execution.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- final evidence ranking/minimization, final result merge, context packet
  compilation, runtime note assembly, and `InquiryResult` construction now
  have a canonical home in
  `services/platform/execution/inquiry_finalization.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- final sufficiency-verdict selection and runtime-note assembly are split out
  inside `services/platform/execution/inquiry_finalization.py`, removing the
  execution layer's last >200-line function hotspot
- inquiry startup, route/budget resolution, baseline retrieval/no-op handling,
  initial evidence reservoir seeding, question-policy loading, Sage reader
  startup notes, and shared-substrate preparation now have a canonical home in
  `services/platform/execution/inquiry_bootstrap.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- per-round question selection, Sage reader batch execution, gated retrieval
  plan compilation, action-result application, question-result merging, answer
  evaluation, and interim sufficiency checks now have a canonical home in
  `services/platform/execution/inquiry_rounds.py`, while legacy private
  `services.platform.execution.inquiry` helper imports remain compatible
- `services/platform/execution/inquiry.py` is down to 493 lines after the
  config, DTO, question-planning schema, runtime metrics, routing, and
  answer/context/action-execution/composition/persistence/Sage-reader/bootstrap/round/finalization/learning/planning/helper
  extractions
- the technical-debt budget now ratchets
  `services/platform/execution/inquiry.py` at 493 lines and long-function
  hotspots at 76 so the hotspot set cannot grow while the split continues
- structural retrieval scope-filter expansion and scoped-model lookup now have
  canonical helpers in `services/reasoning/retrieval/pathways.py`, reducing
  `pathway_a_structural` from 701 to 402 lines
- the technical-debt budget now also caps
  `services/reasoning/retrieval/pathways.py:pathway_a_structural` at 402 lines
  with a function-specific ratchet; this intermediate cap is superseded below
  by the later 129-line Pathway A graph-walk split
- company-intelligence scorecard dimensions now have named helpers in
  `scripts/run_storyline_batch_benchmark.py`, reducing
  `_company_intelligence_scorecard` from 572 to 468 lines
- the technical-debt budget now also caps
  `scripts/run_storyline_batch_benchmark.py:_company_intelligence_scorecard`
  at 468 lines with a function-specific ratchet; this intermediate cap is
  superseded below by the later 78-line company-intelligence scorecard split
- product-value proof-gap rules now have a named helper in
  `scripts/run_storyline_batch_benchmark.py`, reducing `_product_value_evals`
  from 554 to 493 lines
- the technical-debt budget now also caps
  `scripts/run_storyline_batch_benchmark.py:_product_value_evals` at 493 lines
  with a function-specific ratchet; this intermediate cap is superseded below
  by the later 110-line Product-value scorecard split
- Stress10 benchmark fixture construction now delegates observation, query, and
  gold-label assembly to named builders in
  `benchmarks/adapters/stress10_adapter.py`, reducing `__init__` from 229 to
  6 lines while preserving the ten-case adapter contract
- the technical-debt budget now caps
  `benchmarks/adapters/stress10_adapter.py:__init__` at 6 lines while lowering
  the current long-function ratchet to 44
- Think context-plan debug capture, Sage inquiry trace setup, and cascade seed
  handling now have named helpers in `services/reasoning/think/reason.py`,
  reducing `_run_once` from 683 to 510 lines
- the technical-debt budget now also caps
  `services/reasoning/think/reason.py:_run_once` at 510 lines with a
  function-specific ratchet; this intermediate cap is superseded below by the
  later 143-line Think-run-orchestration split
- Think apply transaction preparation, claim-op splitter expansion, edge-op
  application, ontology-gap-op application, and T2 belief-updated enqueueing now
  have named helpers in `services/reasoning/think/applier.py`, reducing
  `apply_diff` from 643 to 497 lines
- the technical-debt budget now also caps
  `services/reasoning/think/applier.py:apply_diff` at 497 lines with a
  function-specific ratchet; this intermediate cap is superseded below by the
  later 149-line Think apply-phase split
- webhook ingress routing now uses named helpers for tenant resolution,
  signature verification, pre-tenant verified handshakes, provider-specific
  handling, Kafka cutover, and inline ingest responses, reducing
  `build_webhooks_router` from 543 to 30 lines and the nested `receive` handler
  from 519 lines to an 85-line `_receive_webhook` coordinator
- the technical-debt budget now caps the webhook router shape at
  `build_webhooks_router` 30 lines, `_receive_webhook` 85 lines, and
  `_inline_ingest_response` 108 lines while lowering the long-function ratchet
  to 76
- outcome evidence accounting now lives in
  `services/reasoning/sage/outcome_evidence.py`, reducing
  `services/reasoning/sage/outcome_evaluator.py:_evaluate` from 526 to 459 lines
  without adding a file-size hotspot
- the technical-debt budget now also caps
  `services/reasoning/sage/outcome_evaluator.py:_evaluate` at 459 lines with a
  function-specific ratchet; this intermediate cap is superseded below by the
  later 194-line outcome-phase split
- recommendation action routes now register module-level endpoint handlers from
  `services/app/gateway/recommendations_router.py`, reducing
  `build_recommendations_router` from 514 to 38 lines
- the technical-debt budget now caps
  `services/app/gateway/recommendations_router.py:build_recommendations_router`
  at 38 lines while lowering the long-function ratchet to 75
- debug inspector routes now register module-level endpoint handlers from
  `services/app/gateway/debug_router.py`, reducing `build_debug_router` from
  518 to 78 lines
- the technical-debt budget now caps
  `services/app/gateway/debug_router.py:build_debug_router` at 78 lines while
  lowering the long-function ratchet to 74
- structure overlay/resource routes now register module-level endpoint handlers
  from `services/app/gateway/structure_router.py`, reducing
  `build_structure_router` from 433 to 25 lines and adding dedicated router
  smoke coverage
- the technical-debt budget now caps
  `services/app/gateway/structure_router.py:build_structure_router` at 25 lines
  while lowering the long-function ratchet to 73
- finance source control routes now register module-level endpoint handlers from
  `services/app/gateway/finance_router.py`, reducing `build_finance_router`
  from 429 to 8 lines and adding DB-free smoke coverage for route registration
  and early validation
- the technical-debt budget now caps
  `services/app/gateway/finance_router.py:build_finance_router` at 8 lines
  while lowering the long-function ratchet to 72
- primary retrieval now delegates scope preparation, pathway execution,
  merge/rank assembly, and reconsolidation to named helpers in
  `services/reasoning/retrieval/primary.py`, reducing `primary_retrieve` from
  399 to 152 lines while preserving the public retrieval API
- the technical-debt budget now caps
  `services/reasoning/retrieval/primary.py:primary_retrieve` at 152 lines while
  lowering the long-function ratchet to 71
- Today page routes now register module-level endpoint handlers from
  `services/app/gateway/today_routes.py`, reducing `register_today_routes` from
  418 to 21 lines while preserving the `/today` apply/delegate/correction flows
- the technical-debt budget now caps
  `services/app/gateway/today_routes.py:register_today_routes` at 21 lines while
  lowering the long-function ratchet to 70
- Map snapshot assembly now delegates projection, model loading, edge loading,
  node capping, synthetic hierarchy edges, and neighborhood assembly to named
  helpers in `services/app/gateway/map_routes.py`, reducing `_build_snapshot`
  from 400 to 66 lines while preserving the snapshot response shape
- the technical-debt budget now caps
  `services/app/gateway/map_routes.py:_build_snapshot` at 66 lines while
  lowering the long-function ratchet to 69
- the synthetic backfill harness now delegates direct install/onboarding-trigger
  writes to module-level source writer helpers, reducing
  `services/ingest/synthetic/backfill_harness/harness.py:_write_install_and_trigger`
  from 1,078 to 43 lines while keeping tenant-scoped RLS writes explicit
- the technical-debt budget now caps
  `services/ingest/synthetic/backfill_harness/harness.py:_write_install_and_trigger`
  at 43 lines while lowering the long-function ratchet to 68
- Think run orchestration now delegates context planning, raw diff generation,
  validation observability, and cascade seeding to
  `services/reasoning/think/run_pipeline.py`, reducing
  `services/reasoning/think/reason.py:_run_once` from 510 to 143 lines without
  pushing `reason.py` over the file-size threshold
- the technical-debt budget now caps
  `services/reasoning/think/reason.py:_run_once` at 143 lines while lowering
  the long-function ratchet to 67
- Think diff application now delegates claim-op expansion/reconciliation,
  act-op application, and resource-op application to named phase helpers in
  `services/reasoning/think/applier.py`, reducing
  `services/reasoning/think/applier.py:apply_diff` from 497 to 149 lines
  without adding a new long-function helper
- the technical-debt budget now caps
  `services/reasoning/think/applier.py:apply_diff` at 149 lines while lowering
  the long-function ratchet to 66
- SAGE outcome evaluation now delegates session/context loading, valid-diff
  event emission, low-value reader emission, validation-failure emission, and
  reward-feature assembly to named helpers plus
  `services/reasoning/sage/outcome_reward.py`, reducing
  `services/reasoning/sage/outcome_evaluator.py:_evaluate` from 459 to 194 lines
  while keeping `outcome_evaluator.py` under the file-size threshold
- the technical-debt budget now caps
  `services/reasoning/sage/outcome_evaluator.py` at 1,476 lines and
  `services/reasoning/sage/outcome_evaluator.py:_evaluate` at 194 lines while
  lowering the long-function ratchet to 65
- Pathway A structural retrieval now delegates seed parsing, graph frontier
  expansion, graph-walk state tracking, and act/resource hydration to named
  helpers in `services/reasoning/retrieval/pathways.py`, reducing
  `pathway_a_structural` from 402 to 129 lines without adding a new
  long-function helper
- the technical-debt budget now caps
  `services/reasoning/retrieval/pathways.py:pathway_a_structural` at 129 lines
  while lowering the long-function ratchet to 64
- SAGE reader orchestration now delegates graph row loading, structural gate
  propagation, activation scoring, projection, trace construction, and debug
  payload assembly to named helpers in `services/reasoning/sage/reader.py`,
  reducing `SynthesisReader.read` from 378 to 179 lines without adding a new
  long-function helper
- the technical-debt budget now caps
  `services/reasoning/sage/reader.py:read` at 179 lines while lowering the
  long-function ratchet to 63
- Structure artifact commitment overlays now delegate row fetching, state-change
  activity assembly, customer/goal/people/decision/resource payload building,
  learned-pattern evidence assembly, and final payload construction to named
  helpers in `services/app/gateway/artifact_drawers.py`, reducing
  `fetch_commitment_overlay` from 375 to 43 lines
- the technical-debt budget now caps
  `services/app/gateway/artifact_drawers.py:fetch_commitment_overlay` at 43
  lines while lowering the long-function ratchet to 62
- Think claim-op application now delegates insert preparation/materialization,
  update preparation, supporting-evidence merging, resolution-field guarding,
  confidence updates, column updates, relation dual-write sync, prediction
  resolution sync, situation-merge updates, and archive handling to named
  helpers in `services/reasoning/think/applier.py`, reducing
  `_apply_claim_op` from 330 to 37 lines
- the technical-debt budget now caps
  `services/reasoning/think/applier.py:_apply_claim_op` at 37 lines while
  lowering the long-function ratchet to 61
- Decision Delta routes now register module-level endpoint handlers from
  `services/product/decision_deltas/router.py`, reducing `build_router` from
  317 to 18 lines while preserving the request-level route surface
- the technical-debt budget now caps
  `services/product/decision_deltas/router.py:build_router` at 18 lines while
  lowering the long-function ratchet to 60
- Forecasts routes now register module-level endpoint handlers from
  `services/product/forecasts/router.py`, reducing `build_router` from 308 to
  18 lines while preserving the static route ordering before the prediction
  detail catch-all
- the technical-debt budget now caps
  `services/product/forecasts/router.py:build_router` at 18 lines while
  lowering the long-function ratchet to 59
- Resolution Thread routes now register module-level endpoint handlers from
  `services/product/resolution_threads/router.py`, reducing `build_router`
  from 229 to 16 lines while preserving the `/v1/resolution_threads` request
  surface and integration-tested evaluate/step-completion flows
- the technical-debt budget now caps
  `services/product/resolution_threads/router.py:build_router` at 16 lines
  while lowering the current long-function ratchet to 45
- SAGE health reporting now delegates active-model, structural-feature,
  topology-optimizer, relationship-candidate, and ontology-proposal collection
  to named helpers in `services/reasoning/sage/health.py`, reducing
  `build_sage_health_report` from 312 to 46 lines while preserving the
  `/debug/sage-health` report shape
- the technical-debt budget now caps
  `services/reasoning/sage/health.py:build_sage_health_report` at 46 lines
  while lowering the long-function ratchet to 58
- SAGE cue extraction now delegates alias/entity matching, system matching,
  mention categorization, raw-text hint promotion, relationship clues,
  constraints, and decision-trigger extraction to named helpers in
  `services/reasoning/sage/cue_extractor.py`, reducing `_extract_sync` from
  223 to 32 lines while preserving deterministic cue semantics
- the technical-debt budget now caps
  `services/reasoning/sage/cue_extractor.py:_extract_sync` at 32 lines while
  lowering the current long-function ratchet to 43
- SAGE evidence projection now delegates counterevidence ranking,
  falsification-relevant selection, support ranking, freshest-confirmation
  selection, confidence-explanation selection, and per-node cap enforcement to
  module-level named helpers in `services/reasoning/sage/evidence_projection.py`,
  reducing `_rank_for_model` from 232 to 65 lines while dropping
  `EvidenceProjector` from 697 to 443 lines and preserving projection ordering
  and omission semantics
- the technical-debt budget now caps
  `services/reasoning/sage/evidence_projection.py:_rank_for_model` at 65 lines
  while lowering the current long-function ratchet to 42 and the oversized-class
  ratchet to 21
- Think validation now delegates region containment, claim-op validation,
  pending-basis confidence collection, edge-op validation, ontology-gap
  validation, act-op validation, resource-op validation, and partial-accept
  failure handling to named helpers in `services/reasoning/think/validator.py`,
  reducing `validate` from 291 to 88 lines while preserving dropped-op logging,
  feedback stats, new-prediction handling, and region strictness
- the technical-debt budget now caps
  `services/reasoning/think/validator.py:validate` at 88 lines while lowering
  the long-function ratchet to 57
- Model insert now delegates insert preparation, scope-actor validation,
  SQL row insertion, sidecar/relation sync, topology effects, audit/state-change
  emission, and recommendation publication to named helpers in
  `services/domain/models/repo.py`, reducing `_insert_core` from 290 to 35
  lines while preserving insert-time side effects and topology behavior
- the technical-debt budget now caps
  `services/domain/models/repo.py:_insert_core` at 35 lines while lowering the
  long-function ratchet to 56
- Think's public entrypoint now delegates run-record startup, early idempotency
  skip, usage aggregation setup/teardown, transaction attempt execution,
  out-of-region expansion, transaction-retry backoff, successful outcome
  finalization, region-lock release logging, and failed-outcome recording to
  named helpers in `services/reasoning/think/reason.py`, reducing `think` from
  289 to 148 lines while preserving retry, cost, and trace cleanup semantics
- the technical-debt budget now caps `services/reasoning/think/reason.py:think`
  at 148 lines while lowering the long-function ratchet to 55
- Retrieval context assembly now delegates budget resolution, model
  tenant/access filtering, MMR/ranked model selection, act capping,
  resource capping, customer-linked resource lookup, and notes construction to
  named helpers in `services/reasoning/retrieval/assembler.py`, reducing
  `assemble_context` from 277 to 88 lines while preserving bundle shape,
  observation-selection notes, access-redaction notes, and MMR metadata
- the technical-debt budget now caps
  `services/reasoning/retrieval/assembler.py:assemble_context` at 88 lines
  while lowering the long-function ratchet to 54
- Slack DM debug routing now lifts install, backfill, live emit, status, secret
  storage, live-webhook delivery, and status row assembly out of the nested
  router factory in `services/app/gateway/slack_router.py`, reducing
  `build_slack_router` from 256 to 9 lines while preserving the four public
  `/slack/{user_id}` controls
- the technical-debt budget now caps
  `services/app/gateway/slack_router.py:build_slack_router` at 9 lines while
  lowering the long-function ratchet to 53
- Commitment creation now delegates input normalization, create-time invariant
  prechecks, tenant/reference validation, auto-block resolution, row insertion,
  contributor writes, edge writes, and JSON serialization to named helpers in
  `services/domain/acts/commitments.py`, reducing `create` from 283 to 58
  lines while preserving C1/C5/C6/C9/C10/G4 enforcement and birth state-change
  emission
- the technical-debt budget now caps
  `services/domain/acts/commitments.py:create` at 58 lines while lowering the
  long-function ratchet to 52
- Think reconciliation now delegates candidate context construction, candidate
  loading, best-match scoring, no-match recording, auto-merge recording,
  human-review recording, and situation merge payload assembly to named helpers
  in `services/reasoning/think/reconciler.py` and
  `services/reasoning/think/reconciler_situation_merge.py`, reducing
  `_reconcile_inner` from 286 to 45 lines while preserving graph boosts,
  kind-specific thresholds, same-issue candidate emission, situation-member
  auto-merge, and best-effort failure isolation
- the technical-debt budget now caps
  `services/reasoning/think/reconciler.py:_reconcile_inner` at 45 lines and
  `services/reasoning/think/reconciler.py` at 1,387 lines while lowering the
  long-function ratchet to 51
- The now-retired Google Workspace synthetic mock moved the HTTP request handler
  class out of the `_make_handler` closure in
  `services/ingest/synthetic/mock_servers/google_workspace.py`, reducing
  `_make_handler` from 275 to 5 lines while preserving token binding,
  Directory, Gmail, Calendar, Drive, export/media, and request-hit behavior
- the historical technical-debt budget capped
  `services/ingest/synthetic/mock_servers/google_workspace.py:_make_handler` at
  5 lines while lowering the long-function ratchet to 50
- Domain bridge revenue-at-risk reporting now delegates the SQL fetch,
  fallback revenue split, zero-risk customer inclusion, and row-to-model
  projection to named helpers in `services/domain/bridge/queries.py`, reducing
  `revenue_at_risk` from 248 to 38 lines while preserving the dashboard report
  shape, fallback semantics, prediction-driven bucket, and linked zero-risk
  customer behavior
- the technical-debt budget now caps
  `services/domain/bridge/queries.py:revenue_at_risk` at 38 lines while
  lowering the long-function ratchet to 49
- HMAC synthetic live webhook payload generation now uses a provider dispatch
  table of named payload builders in
  `services/ingest/synthetic/live_generators/hmac_webhook.py`, reducing
  `_build_payload` from 255 to 6 lines while preserving byte-exact production
  verifier compatibility and tenant-resolution payload keys across all HMAC
  providers
- the technical-debt budget now caps
  `services/ingest/synthetic/live_generators/hmac_webhook.py:_build_payload`
  at 6 lines while lowering the long-function ratchet to 48
- Observation writer message handling now delegates parse-failure DLQ
  publishing, full-mode permanent-failure DLQ publishing, shared DLQ metrics,
  and partition guardrail DLQ handling to named helpers in
  `services/ingest/ingestion/writers/observation_writer.py`, reducing
  `_handle_message` from 210 to 136 lines while preserving shadow/full-mode,
  self-heal, out-of-bounds, DLQ, and metric semantics
- the technical-debt budget now caps
  `services/ingest/ingestion/writers/observation_writer.py:_handle_message`
  at 136 lines while lowering the current long-function ratchet to 41
- Contestation service execution now delegates input validation, standing
  enforcement, model snapshot loading, contestation observation insertion,
  first-person override, reading-contestation mutation, and T3 enqueueing to
  named helpers in `services/reasoning/contestability/service.py`, reducing
  `contest_model` from 210 to 44 lines while preserving standing errors,
  tenant validation, authoritative observation writes, contested-count updates,
  override multipliers, reading notes, and trigger payload semantics
- the technical-debt budget now caps
  `services/reasoning/contestability/service.py:contest_model` at 44 lines
  while lowering the current long-function ratchet to 40
- Circuit-breaker tick processing now delegates tick input reads,
  flag-disabled handling, operator re-enable bookkeeping reset, per-tenant
  state creation, worst-lane selection, breach-counter updates, and trip
  execution to named helpers in
  `services/ingest/ingestion/feature_flags/circuit_breaker.py`, reducing
  `_process_tick` from 202 to 42 lines while preserving lag measurement
  failure skips, active-tenant sampling, operator-disabled audit preservation,
  no-auto-recovery behavior, worst-source-lane trip semantics, flip-before-trip
  ordering, pinned-counter retry behavior, and alert payload shape
- the technical-debt budget now caps
  `services/ingest/ingestion/feature_flags/circuit_breaker.py:_process_tick`
  at 42 lines while lowering the current long-function ratchet to 39
- Greeting scheduler tenant refresh now delegates snapshot composition,
  bounded section rendering, card rendering, card-reasoning rendering,
  cache payload construction, stale-cache warning emission, cache writes,
  and stream publishing to named helpers in
  `services/product/greeting/scheduler.py`, reducing
  `_refresh_tenant_inner` from 264 to 27 lines while preserving cache key
  shapes, close-line storage, card expanded reasoning/evidence fallback,
  staleness WARN logging, rendering fallback behavior, and websocket update
  payloads
- the technical-debt budget now caps
  `services/product/greeting/scheduler.py:_refresh_tenant_inner` at 27 lines
  while lowering the current long-function ratchet to 38
- Ingestion core draft finalization now delegates GitHub inline enrichment,
  actor resolution, entity fast-path resolution, embedding computation,
  observation construction, transactional insert/dedup/trigger enqueue, and
  post-commit embedding request publishing to named helpers in
  `services/ingest/ingestion/core.py`, reducing `ingest_from_draft` from 249
  to 69 lines while preserving advisory dedup locking, unresolved actor/entity
  markers, `_cause_event_id` lifting, precomputed embedding fallback,
  post-commit observation notifications, shared T1 trigger enqueue semantics,
  best-effort embedding request publishing, and writer full-mode parity
- the technical-debt budget now caps
  `services/ingest/ingestion/core.py:ingest_from_draft` at 69 lines while
  lowering the current long-function ratchet to 37
- Gmail mailbox history draining now delegates read-path validation, Kafka
  cutover resolution, watch-row loading, history pagination, per-message fetch,
  cutover publish, inline fallback dispatch, read-audit writes, and bookmark
  advancement to named helpers in
  `services/ingest/integrations/gmail/fetcher.py`, reducing
  `drain_mailbox_history` from 219 to 86 lines while preserving inactive-watch
  skips, missing-bookmark skips, Gmail per-message failure skips, Kafka-success
  audit behavior, Kafka-failure inline fallback, dedup accounting, and
  push/poll timestamp updates
- the technical-debt budget now caps
  `services/ingest/integrations/gmail/fetcher.py:drain_mailbox_history` at 86
  lines while lowering the current long-function ratchet to 36
- Live validation-run driver composition now delegates shared app setup, Gmail
  app setup, cutover dependency threading, target grouping, mailbox/guild mock
  construction, Discord dispatch dependency creation, core generator entry, HMAC
  generator entry, Google push generator entry, Notion generator entry, and
  direct gateway/poll generator entry to named helpers in
  `services/ingest/synthetic/validation_runs/composition.py`, reducing
  `build_live_drivers` from 265 to 65 lines while preserving shared Slack/GitHub
  app wiring, Gmail's separate app, Run-4 Kafka/S3/flag cutover threading,
  per-source mock identity, optional-source construction, and teardown stack
  ownership; the Gmail live generator now also accepts mailbox-to-tenant hints
  so composition can reuse existing watched mailboxes under enforced RLS without
  falling back to unbound tenant-scoped Gmail reads
- the technical-debt budget now caps
  `services/ingest/synthetic/validation_runs/composition.py:build_live_drivers`
  at 65 lines while lowering the current long-function ratchet to 35
- Pathway G model-edge retrieval now delegates scoped seed discovery,
  candidate ranking, composition-edge expansion, graph frontier advancement,
  and final hydration to named helpers in
  `services/reasoning/retrieval/pathways.py`, reducing
  `pathway_g_model_edges` from 266 to 87 lines while preserving tenant filters,
  active-model filtering, accepted/candidate/disputed edge semantics,
  composition parent/member traversal, deterministic ranking, and notes payloads
- the technical-debt budget now caps
  `services/reasoning/retrieval/pathways.py:pathway_g_model_edges` at 87 lines
  while lowering the current long-function ratchet to 34
- Pathway B semantic retrieval now delegates vector resolution, optional HNSW
  tuning, pgvector bind preparation, actor/entity scope normalization, ANN
  query execution, exact-fallback ranking, and fallback note construction to
  named helpers in `services/reasoning/retrieval/pathways.py`, reducing
  `pathway_b_semantic` from 249 to 88 lines while preserving precomputed and
  Ollama vector paths, pgvector codec registration, RA-1 actor/entity OR-scope
  semantics, scoped exact fallback thresholds, unscoped exact fallback behavior,
  and notes payloads
- the technical-debt budget now caps
  `services/reasoning/retrieval/pathways.py:pathway_b_semantic` at 88 lines
  while lowering the current long-function ratchet to 33
- Shard fetch now delegates shard context parsing, N1 state bootstrap/resume,
  per-page cursor reload, rate-limited fetch dispatch, raw-tier S3 message
  construction, cursor publish/advance, and transient parking logs to named
  module helpers in `services/ingest/ingestion/workflows/shard_fetch.py`,
  reducing `_run_fetch_loop` from 258 to 99 lines while preserving
  install-unavailable parking, N1 publish-before-advance semantics,
  S3-write-before-publish ordering, Kafka/S3 transient retry behavior,
  recoverable source-error parking, NotImplemented terminal failure, and
  `shard.fetched` progress metrics
- the technical-debt budget now caps
  `services/ingest/ingestion/workflows/shard_fetch.py:_run_fetch_loop` at 99
  lines while lowering the current long-function ratchet to 32
- Think act-op application now delegates goal, commitment, decision, and
  commitment-edge mutation branches to domain-specific helpers in
  `services/reasoning/think/applier.py`, reducing `_apply_act_op` from 231 to
  24 lines while preserving summary payloads, state-change counts,
  confidence-basis threading, transition cause-event handling, and validation
  errors for unknown act ops
- the technical-debt budget now caps
  `services/reasoning/think/applier.py:_apply_act_op` at 24 lines while
  lowering the current long-function ratchet to 31
- CEO-view gateway wiring now delegates settings resolution, rendering router
  setup, greeting runtime construction, query router setup, conversation router
  setup, push ingress mounts, Google admin mounts, debug routing, and final
  state publication to focused helpers in
  `services/app/gateway/ceo_view_wiring.py`, reducing `configure_ceo_view` from
  201 to 28 lines while preserving router inclusion order, rendering dependency
  overrides, default-tenant token mapping, optional scheduler startup, and the
  `app_.state.ceo_view` contract
- the technical-debt budget now caps
  `services/app/gateway/ceo_view_wiring.py:configure_ceo_view` at 28 lines
  while lowering the current long-function ratchet to 30
- Run 4 concurrent validation now delegates report identity, live-target
  construction, live Kafka-cutover runtime setup, concurrent backfill/live
  driving, combined observation totals, per-source result aggregation,
  post-drain assertions, and report notes to focused helpers in
  `services/ingest/synthetic/validation_runs/run4_concurrent.py`, reducing
  `run4` from 215 to 112 lines while preserving migration/truncation,
  preflight, setup, live-via-Kafka dependency threading, service startup,
  drain, collect, assertion, and teardown ordering
- `services/ingest/synthetic/validation_runs/run4_concurrent.py` now imports
  optional `moto` infrastructure lazily inside `run4`, so pure helper imports
  do not require the synthetic-run dependency to be installed
- the technical-debt budget now caps
  `services/ingest/synthetic/validation_runs/run4_concurrent.py:run4` at 112
  lines while lowering the current long-function ratchet to 29
- Storyline benchmark execution now delegates run-id/build setup, runtime
  opening, tenant preparation, optional seed-model loading, worker
  construction, wave processing, model-summary collection, and output rendering
  to focused helpers in `scripts/run_storyline_batch_benchmark.py`, reducing
  `run_benchmark` from 290 to 101 lines while preserving build-only behavior,
  migrations, append-run handling, seed-model semantics, wave checkpoint
  writing, post-commit and topology drains, scoring output, cleanup, and pool
  closure
- the technical-debt budget now caps
  `scripts/run_storyline_batch_benchmark.py:run_benchmark` at 101 lines while
  lowering the current long-function ratchet to 28
- Storyline benchmark scoring now delegates DB fetches, observation indexing,
  latent-pattern scoring, latent-bridge scoring, edge/review scoring, note
  construction, and optional thesis judging to focused helpers in
  `scripts/run_storyline_batch_benchmark.py`, reducing `score_storylines` from
  373 to 34 lines while preserving model/edge/candidate/observation inputs,
  per-story evidence matching, latent bridge scoring, thesis judge limits,
  calibration samples, and the `StorylineScore` output contract
- the technical-debt budget now caps
  `scripts/run_storyline_batch_benchmark.py:score_storylines` at 34 lines while
  lowering the current long-function ratchet to 27
- Product-value scorecard evaluation now delegates derived metric collection,
  decision/memory/prediction dimensions, counterfactual/bridge/compression
  dimensions, and learning/customer-value dimensions to focused helpers in
  `scripts/run_storyline_batch_benchmark.py`, reducing `_product_value_evals`
  from 493 to 110 lines while preserving product-value eval keys, dimension
  formulas, proof-gap inputs, interpretation, and overall-score aggregation
- the technical-debt budget now caps
  `scripts/run_storyline_batch_benchmark.py:_product_value_evals` at 110 lines
  while lowering the current long-function ratchet to 26
- Company-intelligence scorecard assembly now delegates base run metrics,
  storyline/edge intelligence metrics, memory/context metrics, reasoning,
  temporal, robustness, efficiency, product-value eval wiring, weights, and
  proof coverage to focused helpers in `scripts/run_storyline_batch_benchmark.py`,
  reducing `_company_intelligence_scorecard` from 468 to 78 lines while
  preserving scorecard keys, dimension formulas, product-value eval integration,
  proof coverage, and proof-gap inputs
- the technical-debt budget now caps
  `scripts/run_storyline_batch_benchmark.py:_company_intelligence_scorecard` at
  78 lines while lowering the current long-function ratchet to 25
- Model-layer probe report collection now delegates static summary assembly,
  count queries, distribution queries, context-relation-contract aggregation,
  export-row loading, and artifact writing to focused helpers in
  `scripts/run_1000_signal_model_layer_probe.py`, reducing
  `collect_model_layer_report` from 415 to 46 lines while preserving
  `run_summary.json`, `models.jsonl`, `model_edges.jsonl`,
  `signal_manifest.jsonl`, markdown summary output, and graph-health
  computation
- the technical-debt budget now caps
  `scripts/run_1000_signal_model_layer_probe.py:collect_model_layer_report` at
  46 lines while lowering the current long-function ratchet to 24
- Model-layer probe orchestration now delegates argument validation, runtime
  opening, migration/materialization setup, optional seed-model insertion, wave
  injection and Think draining, post-commit/topology draining, report collection,
  and terminal summary printing to focused helpers in
  `scripts/run_1000_signal_model_layer_probe.py`, reducing `main` from 270 to
  55 lines while preserving run-id/report-dir construction, migration skipping,
  alias insertion, wave break-on-timeout behavior, post-report artifact writes,
  and final JSON summary output
- the technical-debt budget now caps
  `scripts/run_1000_signal_model_layer_probe.py:main` at 55 lines while
  lowering the current long-function ratchet to 23
- the technical-debt budget checker now builds file/function line budgets via
  module-level override maps and tiny builder helpers, reducing its own `main`
  from 202 to 25 lines so the ratchet tool does not become a hotspot
- Benchmark CLI orchestration now delegates parser construction, adapter/system
  resolution, metadata assembly, run-config construction, artifact writing, and
  terminal summary output to focused helpers in `benchmarks/run_benchmark.py`,
  reducing `main` from 268 to 23 lines while preserving benchmark choices,
  required-data validation, Fyralis embedding metadata, report artifact output,
  and the existing summary lines
- the technical-debt budget now caps `benchmarks/run_benchmark.py:main` at
  23 lines while lowering the current long-function ratchet to 22
- Model E2E stress-case construction now delegates target memory, counterevidence,
  optional graph, optional situation, noise-fill, and trigger assembly to focused
  helpers in `scripts/run_100x_5000_model_e2e_stress.py`, reducing
  `_build_case_models` from 274 to 86 lines while preserving generated draft
  count, expected model/member IDs, trigger metadata, semantic collision noise,
  and UUID append order
- the technical-debt budget now caps
  `scripts/run_100x_5000_model_e2e_stress.py:_build_case_models` at 86 lines
  while lowering the current long-function ratchet to 21
- Real-LLM scenario materialization now delegates foundation parsing,
  tenant/bootstrap creation, actor/customer/goal/commitment/decision creation,
  explicit and inferred customer-commitment linking, and alias insertion to
  focused helpers in `tests/real_llm/infrastructure/scenario_loader.py`,
  reducing `materialize` from 318 to 88 lines while preserving committed tenant
  setup before transactional foundation writes, bootstrap observation semantics,
  inferred customer links, maintenance-capacity defaults, and alias metadata
- the technical-debt budget now caps
  `tests/real_llm/infrastructure/scenario_loader.py:materialize` at 88 lines
  while lowering the current long-function ratchet to 20
- OAuth source-completion E2E coverage now delegates fixture seeding, worker
  launch, milestone polling, terminal chain assertions, shard-state validation,
  and subprocess cleanup to focused helpers in
  `services/ingest/ingestion/workflows/tests/test_oauth_to_source_completion_end_to_end.py`,
  reducing `test_oauth_trigger_to_source_completion_end_to_end` from 420 to 53
  lines while preserving the five-worker OAuth-to-source-completion chain
- the technical-debt budget now caps
  `services/ingest/ingestion/workflows/tests/test_oauth_to_source_completion_end_to_end.py:test_oauth_trigger_to_source_completion_end_to_end`
  at 53 lines while lowering the current long-function ratchet to 19
- OAuth tenant-completion re-share E2E coverage now delegates fixture seeding,
  worker launch, re-share-pass polling, mid-cycle checks, final state
  assertions, cross-service idempotency checks, and subprocess cleanup to
  focused helpers in
  `services/ingest/ingestion/workflows/tests/test_oauth_to_tenant_completion_with_reconciler_reshare.py`,
  reducing
  `test_oauth_trigger_to_tenant_completion_with_reconciler_reshare_path` from
  337 to 35 lines while preserving the gappy-pass, clean-pass, re-shard, and
  Bridge-completion assertions
- the technical-debt budget now caps
  `services/ingest/ingestion/workflows/tests/test_oauth_to_tenant_completion_with_reconciler_reshare.py:test_oauth_trigger_to_tenant_completion_with_reconciler_reshare_path`
  at 35 lines while lowering the current long-function ratchet to 18
- ShardFetch restart/resume E2E coverage now delegates fixture seeding,
  subprocess bootstrap/env construction, page-zero waiting, pre-restart
  non-vacuous-state checks, Process B completion polling, final cursor
  assertions, and subprocess cleanup to focused helpers in
  `services/ingest/ingestion/workflows/tests/test_shard_fetch_subprocess.py`,
  reducing `test_shard_fetch_resumes_from_persisted_cursor_after_restart`
  from 303 to 56 lines while preserving the persisted-cursor resume and
  `end_of_data` assertions
- the technical-debt budget now caps
  `services/ingest/ingestion/workflows/tests/test_shard_fetch_subprocess.py:test_shard_fetch_resumes_from_persisted_cursor_after_restart`
  at 56 lines while lowering the current long-function ratchet to 17
- Retrieval quality mixed-entrypoint regression coverage now delegates corpus
  construction, graph-positive/negative setup, expected-set derivation,
  case construction, per-case tenant isolation checks, and suite-level
  recall/negative-hit/latency assertions to focused helpers in
  `services/reasoning/retrieval/tests/test_retrieval_quality_harness.py`,
  reducing `test_quality_eval_corpus_mixed_entrypoints_regression_gate` from
  292 to 19 lines while preserving all eight entrypoint cases and quality
  thresholds
- the technical-debt budget now caps
  `services/reasoning/retrieval/tests/test_retrieval_quality_harness.py:test_quality_eval_corpus_mixed_entrypoints_regression_gate`
  at 19 lines while lowering the current long-function ratchet to 16
- Discord gateway worker launch now delegates configuration loading,
  health runtime setup, optional Kafka/S3 data-plane wiring, leader-lease
  acquire, crash-RESUME wiring, lease-refresh worker execution, and resource
  cleanup to named helpers in `scripts/run_discord_gateway_worker.py`, reducing
  `_main` from 244 to 72 lines while preserving fail-loud Redis validation,
  acquire-before-connect ordering, RESUME hook threading, lease-loss exit `3`,
  and cleanup order
- the technical-debt budget now caps
  `scripts/run_discord_gateway_worker.py:_main` at 72 lines while lowering the
  long-function ratchet to 47
- Think context-use telemetry now delegates relation-op counting,
  context-use grade selection, selected-context accounting, graph relation
  contract basis, UUID serialization, and ratio formatting to named helpers in
  `services/reasoning/think/context_use.py`, reducing `summarize_context_use`
  from 233 to 198 lines while preserving the persisted telemetry keys and
  graph/no-op/context-accounting semantics
- the technical-debt budget now caps
  `services/reasoning/think/context_use.py:summarize_context_use` at 198 lines
  while lowering the long-function ratchet to 46

## 1. Current Evidence Baseline

These measurements came from a repository scan on 2026-06-13.

| Area | Current signal |
| --- | --- |
| Python files | 1,770 files under `services`, `lib`, `scripts`, `benchmarks`, and `tests` in the current dashboard scope. |
| Python LOC | About 495.2k total lines in those Python files. |
| SQL migrations | 135 migration files in `db/migrations`. |
| Docs | 93 Markdown files under `docs`. |
| Test files | 787 test files versus 983 non-test Python files in the current dashboard scope. |
| Files above threshold | 29 files, down from 30 after `services/platform/execution/inquiry.py` crossed below the 1,500-line threshold. |
| Functions above threshold | 16 functions, down after the inquiry, retrieval, Think, applier, benchmark, Stress10-adapter, webhook-router, recommendations-router, debug-router, structure-router, finance-router, Slack-router, CEO-view wiring, Run-4 concurrent validation, Storyline benchmark orchestration, Storyline benchmark scoring, Product-value scorecard evaluation, Company-intelligence scorecard assembly, Model-layer report collection, Model-layer probe orchestration, Benchmark CLI orchestration, Model E2E stress-case construction, Real-LLM scenario materialization, OAuth source-completion E2E split, OAuth tenant re-share E2E split, ShardFetch resume E2E split, Retrieval quality mixed-entrypoint split, Commitment-create, Think-reconciler, Google-Workspace-mock, Domain-bridge revenue-at-risk, HMAC-webhook payload, Observation-writer message handling, Contestation-service entrypoint, Circuit-breaker tick processing, Greeting-scheduler refresh, Ingestion-core draft finalization, Gmail history-drain, live-driver composition, Pathway-G edge traversal, Pathway-B semantic retrieval, ShardFetch loop, Think act-op application, Discord-gateway launcher, Think-context-use telemetry, Decision Delta-router, Forecasts-router, Resolution Thread-router, SAGE-health-report, SAGE-cue-extractor, SAGE-evidence-projection, Think-validator, Model-insert, Think-entrypoint, Retrieval-assembler, primary-retrieval, Today-route, Map-snapshot, synthetic-backfill-install, Think-run-orchestration, Think-apply-phase, SAGE-outcome-phase, Pathway-A graph-walk, SAGE-reader orchestration, Structure artifact-overlay, and Think claim-op splits. |
| Classes above threshold | 21 classes, down after `services/reasoning/sage/evidence_projection.py:EvidenceProjector` dropped below the 600-line class threshold. |
| Existing architecture gate | `lint-imports` is present and green in this checkout. |
| Current lint issue | Conservative ruff gate is green. |
| Queue-owner ratchets | Raw Think trigger insert violations: 0; raw model re-eval insert violations: 0; raw pending post-commit action insert violations: 0; raw Think obligation insert violations: 0. |
| Import-linter allowlist debt | 71 ignored imports, now capped by a ratchet. |
| File-specific line budget | `services/platform/execution/inquiry.py` capped at 493 lines; `services/reasoning/sage/outcome_evaluator.py` capped at 1,476 lines; `services/reasoning/think/reconciler.py` capped at 1,387 lines. |
| Function-specific line budget | `services/app/gateway/debug_router.py:build_debug_router` capped at 78 lines; `services/app/gateway/finance_router.py:build_finance_router` capped at 8 lines; `services/app/gateway/map_routes.py:_build_snapshot` capped at 66 lines; `services/app/gateway/slack_router.py:build_slack_router` capped at 9 lines; `services/app/gateway/structure_router.py:build_structure_router` capped at 25 lines; `services/app/gateway/today_routes.py:register_today_routes` capped at 21 lines; `services/app/gateway/ceo_view_wiring.py:configure_ceo_view` capped at 28 lines; `services/app/gateway/recommendations_router.py:build_recommendations_router` capped at 38 lines; `services/app/gateway/artifact_drawers.py:fetch_commitment_overlay` capped at 43 lines; `services/domain/acts/commitments.py:create` capped at 58 lines; `services/domain/bridge/queries.py:revenue_at_risk` capped at 38 lines; `services/product/decision_deltas/router.py:build_router` capped at 18 lines; `services/product/forecasts/router.py:build_router` capped at 18 lines; `services/product/resolution_threads/router.py:build_router` capped at 16 lines; `services/product/greeting/scheduler.py:_refresh_tenant_inner` capped at 27 lines; `services/reasoning/sage/health.py:build_sage_health_report` capped at 46 lines; `services/reasoning/sage/cue_extractor.py:_extract_sync` capped at 32 lines; `services/reasoning/sage/evidence_projection.py:_rank_for_model` capped at 65 lines; `services/reasoning/contestability/service.py:contest_model` capped at 44 lines; `services/reasoning/think/context_use.py:summarize_context_use` capped at 198 lines; `services/reasoning/think/validator.py:validate` capped at 88 lines; `services/domain/models/repo.py:_insert_core` capped at 35 lines; `services/reasoning/think/reason.py:think` capped at 148 lines; `services/reasoning/think/reconciler.py:_reconcile_inner` capped at 45 lines; `services/reasoning/retrieval/assembler.py:assemble_context` capped at 88 lines; `services/app/webhooks/router.py:build_webhooks_router` capped at 30 lines; `services/app/webhooks/router.py:_receive_webhook` capped at 85 lines; `services/app/webhooks/router.py:_inline_ingest_response` capped at 108 lines; `services/ingest/synthetic/backfill_harness/harness.py:_write_install_and_trigger` capped at 43 lines; `services/ingest/synthetic/live_generators/hmac_webhook.py:_build_payload` capped at 6 lines; `services/ingest/synthetic/mock_servers/google_workspace.py:_make_handler` capped at 5 lines; `services/ingest/ingestion/writers/observation_writer.py:_handle_message` capped at 136 lines; `services/ingest/ingestion/feature_flags/circuit_breaker.py:_process_tick` capped at 42 lines; `services/ingest/ingestion/core.py:ingest_from_draft` capped at 69 lines; `services/ingest/ingestion/workflows/shard_fetch.py:_run_fetch_loop` capped at 99 lines; `services/ingest/integrations/gmail/fetcher.py:drain_mailbox_history` capped at 86 lines; `services/ingest/synthetic/validation_runs/composition.py:build_live_drivers` capped at 65 lines; `services/ingest/synthetic/validation_runs/run4_concurrent.py:run4` capped at 112 lines; `services/reasoning/sage/outcome_evaluator.py:_evaluate` capped at 194 lines; `services/reasoning/sage/reader.py:read` capped at 179 lines; `services/reasoning/think/applier.py:_apply_act_op` capped at 24 lines; `services/reasoning/think/applier.py:_apply_claim_op` capped at 37 lines; `services/reasoning/think/applier.py:apply_diff` capped at 149 lines; `services/reasoning/think/reason.py:_run_once` capped at 143 lines; `services/reasoning/retrieval/primary.py:primary_retrieve` capped at 152 lines; `services/reasoning/retrieval/pathways.py:pathway_a_structural` capped at 129 lines; `services/reasoning/retrieval/pathways.py:pathway_b_semantic` capped at 88 lines; `services/reasoning/retrieval/pathways.py:pathway_g_model_edges` capped at 87 lines; `benchmarks/adapters/stress10_adapter.py:__init__` capped at 6 lines; `scripts/run_discord_gateway_worker.py:_main` capped at 72 lines; `scripts/run_storyline_batch_benchmark.py:run_benchmark` capped at 101 lines; `scripts/run_storyline_batch_benchmark.py:score_storylines` capped at 34 lines; `scripts/run_storyline_batch_benchmark.py:_company_intelligence_scorecard` capped at 78 lines; `scripts/run_storyline_batch_benchmark.py:_product_value_evals` capped at 110 lines. |
| Newest function-specific line budget | `tests/real_llm/infrastructure/scenario_loader.py:materialize` capped at 88 lines, `services/ingest/ingestion/workflows/tests/test_oauth_to_source_completion_end_to_end.py:test_oauth_trigger_to_source_completion_end_to_end` capped at 53 lines, `services/ingest/ingestion/workflows/tests/test_oauth_to_tenant_completion_with_reconciler_reshare.py:test_oauth_trigger_to_tenant_completion_with_reconciler_reshare_path` capped at 35 lines, `services/ingest/ingestion/workflows/tests/test_shard_fetch_subprocess.py:test_shard_fetch_resumes_from_persisted_cursor_after_restart` capped at 56 lines, and `services/reasoning/retrieval/tests/test_retrieval_quality_harness.py:test_quality_eval_corpus_mixed_entrypoints_regression_gate` capped at 19 lines; the complete enforceable list lives in `scripts/check_tech_debt_budget.py`. |
| Dirty tree | There is substantial in-progress work across docs, scripts, reasoning, domain, ingest, and workers. |

Largest code areas by LOC:

| Area | Approx LOC | Notes |
| --- | ---: | --- |
| `services/ingest` | 155k | Largest surface; many source integrations and workflows. |
| `services/reasoning` | 89k | Think, retrieval, SAGE, topology, validation, workers. |
| `tests` | 44k | Good test investment, but large and unevenly distributed. |
| `services/product` | 38k | Ask, recommendations, Today, rendering, forecasts. |
| `scripts` | 36k | Many operational and benchmark scripts, several large enough to deserve package structure. |
| `services/app` | 33k | Gateway, webhooks, realtime, route surfaces. |
| `services/domain` | 29k | Persisted substrate and repositories. |

Largest hotspots:

| File | Approx LOC | Refactor concern |
| --- | ---: | --- |
| `scripts/run_storyline_batch_benchmark.py` | 5.9k | Benchmark runner still mixes scorecard construction, reporting, and data setup, though execution and storyline scoring are now below the long-function threshold. |
| `benchmarks/fyralis_eval/fyralis_db.py` | 4.0k | Reader/eval code should be split by data access and scoring concerns. |
| `services/reasoning/sage/reader.py` | 3.7k | SAGE policy composition and helper ownership need clearer boundaries, though the public read coordinator is now below the long-function threshold. |
| `services/reasoning/think/applier.py` | 3.6k | Diff application still owns many op-family helpers, though the public apply coordinator is now small. |
| `services/domain/models/repo.py` | 3.1k | Repository still performs downstream reasoning/product side effects, though the core insert pipeline is now split into named phases. |

Longest functions worth targeting:

| Function | Location | Approx lines |
| --- | --- | ---: |
| `build_fixture` | `services/reasoning/retrieval/tests/_fixtures.py` | 287 |
| `test_oauth_trigger_to_tenant_completion_end_to_end` | `services/ingest/ingestion/workflows/tests/test_oauth_to_tenant_completion_end_to_end.py` | 267 |
| `_seed_stress_corpus` | `tests/unit/sage/test_sage_100_large_e2e_stress.py` | 251 |
| `test_oauth_trigger_to_gmail_completion_with_reshare` | `services/ingest/ingestion/workflows/tests/test_oauth_to_gmail_completion_with_reshare.py` | 249 |

`run_inquiry_retrieval` is now about 83 lines and no longer counts as a
long-function hotspot. `_company_intelligence_scorecard` is down to about 78
lines, `collect_model_layer_report` is down to about 46 lines,
`scripts/run_1000_signal_model_layer_probe.py:main` is down to about 55 lines,
`benchmarks/run_benchmark.py:main` is down to about 23 lines,
`scripts/run_100x_5000_model_e2e_stress.py:_build_case_models` is down to
about 86 lines,
`tests/real_llm/infrastructure/scenario_loader.py:materialize` is down to about
88 lines,
`test_oauth_trigger_to_source_completion_end_to_end` is down to about 53 lines,
`test_oauth_trigger_to_tenant_completion_with_reconciler_reshare_path` is down
to about 35 lines,
`test_shard_fetch_resumes_from_persisted_cursor_after_restart` is down to about
56 lines,
`test_quality_eval_corpus_mixed_entrypoints_regression_gate` is down to about
19 lines,
`_product_value_evals` is down to about 110 lines, `score_storylines` is
down to about 34 lines, `_run_once` is down
to about 143 lines, `apply_diff` is down to about 149 lines, and
`pathway_a_structural` is down to about 129 lines. `primary_retrieve` is down
to about 152 lines after the pathway-orchestration split. `_evaluate` is down to about
194 lines after the outcome-evidence and outcome-phase splits. `build_webhooks_router` and the old
nested `receive` hotspot have moved below threshold; the webhook router's largest
helper is now `_inline_ingest_response` at about 108 lines.
`build_recommendations_router` is down to about 38 lines after lifting endpoint
handlers to module scope. `build_debug_router` is down to about 78 lines after
the same registration split. `build_structure_router` is down to about 25 lines
after the same registration split. `build_finance_router` is down to about 8
lines after the same registration split. `register_today_routes` is down to
about 21 lines after the same registration split. `_build_snapshot` is down to
about 66 lines after the Map snapshot assembly split.
`_write_install_and_trigger` is down to about 43 lines after the synthetic
backfill install-writer split. `apply_diff` is now below the long-function
threshold after the Think apply-phase split. `_evaluate` is now below the
long-function threshold after the SAGE outcome-phase split. `pathway_a_structural`
is now below the long-function threshold after the Pathway A graph-walk split.
The remaining top targets have moved back to benchmark, workflow-test, Think
reasoning/reconciliation, retrieval pathways, and ingestion surfaces.
`SynthesisReader.read` is now about 179 lines after the
graph/projection/result-assembly split.
`fetch_commitment_overlay` is now about 43 lines after the Structure
artifact-overlay split, with focused drawer assembly coverage added.
`_apply_claim_op` is now about 37 lines after the Think claim-op operation
split.
`build_sage_health_report` is now about 46 lines after the SAGE health-report
split. `validate` is now about 88 lines after the Think validator phase split.
`_insert_core` is now about 35 lines after the Model insert pipeline split.
`think` is now about 148 lines after the Think entrypoint retry/cost
orchestration split.
`assemble_context` is now about 88 lines after the retrieval context assembly
split.
`build_slack_router` is now about 9 lines after the Slack DM route-registration
split.
`commitments.create` is now about 58 lines after the Commitment create pipeline
split.
`_reconcile_inner` is now about 45 lines after the Think reconciler decision
pipeline split, and situation merge payload assembly now lives in
`reconciler_situation_merge.py`.
`_make_handler` is now about 5 lines after the Google Workspace mock request
handler extraction.

## 2. Desired End State

A production-ready Fyralis Core should have these properties:

1. The runtime process set matches the source tree.
2. Every load-bearing architectural boundary has a CI guard or single owning
   module.
3. Domain memory has one sanctioned mutation grammar, with auditability and
   replay hooks.
4. Queue insertion contracts are centralized and observable.
5. Worker loops that product behavior depends on are deployed, health-checked,
   and bounded.
6. Semantic vector columns only contain semantic embeddings, with explicit
   pending/provenance state.
7. Ingestion has a canonical replay cursor and shared normalization semantics.
8. Large modules are split by business responsibility, not cosmetic file size.
9. Local and CI gates are boring: lint, import-linter, migration prefix checks,
   schema drift, targeted tests, and readiness harnesses.
10. Documentation describes what is actually true today.

## 3. Non-Goals

Do not do these during this refactor:

- Do not split the repository into multiple repos.
- Do not introduce a new job system unless Postgres-based leasing demonstrably
  fails.
- Do not replace the layered directory structure with hexagonal or vertical
  slices.
- Do not rewrite all integrations behind a new connector SDK.
- Do not merge Ask, Query, and Conversations until the product surface itself is
  ready to retire one of them.
- Do not drop legacy model relationship arrays in the same tranche as broad
  architecture cleanup.
- Do not make formatting-only churn across large files.

## 4. Refactor Principles

Use these rules for every PR:

1. Prefer behavior-neutral extraction before semantic change.
2. Add a test or gate before moving a risky boundary.
3. Make one architectural invariant stricter per PR.
4. Delete dead paths only when grep, docs, and tests agree.
5. Keep public runtime behavior stable unless a migration note says otherwise.
6. Avoid new abstractions unless they remove an existing repeated contract.
7. No large file moves mixed with behavior changes.
8. Every worker or queue added to the tree must appear in the process manifest,
   compose/script launch surfaces, or a documented "library only" exemption.

## 5. Workstream A: Stabilize The Current Branch

Goal: turn the current dirty checkout into a set of reviewable, green slices.

### A1. Inventory Current Work

Create a short local inventory of modified and untracked work:

- existing source edits
- new source files
- generated artifacts
- benchmark caches
- docs that are plans versus docs that describe shipped behavior
- migration and schema-lock changes

The current checkout includes useful in-progress work such as:

- `services/domain/triggers.py`
- `services/workers/housekeeper/`
- `scripts/run_housekeeper_worker.py`
- `services/platform/runtime/process_manifest.py`
- `scripts/run_operational_readiness_gates.py`
- `scripts/run_production_readiness_gap_harness.py`
- learning-loop and Think changes across `services/reasoning/think`

It also includes generated/cache-like paths such as:

- `benchmarks/.cache/`
- `benchmarks/datasets/raw/`
- `benchmarks/datasets/tmp/`
- `truss_run/`
- `truss_run_2/`
- `site/`
- `.codex-run-logs/`

### A2. Restore Green Mechanical Gates

Before deeper refactoring, make these pass:

```bash
ruff check --select E9,F63,F7,F82,F821,F811,F401 .
lint-imports
pytest services/domain/tests/test_triggers.py services/workers/housekeeper/tests/test_worker.py -q
```

Known immediate lint cleanup from the scan:

- remove unused `traceback` import in `scripts/sandbox_ingest.py`
- remove unused `timedelta` import in `services/ingest/synthetic/fixtures/linkedin_generator.py`

### A3. Commit Slices

Land the current work in reviewable slices:

1. Trigger helper and queue call-site migration.
2. Obligation/feedback stats schema and domain helpers.
3. Think learning-loop behavior changes and tests.
4. Housekeeper worker and launcher.
5. Process manifest and runtime parity tests.
6. Readiness harnesses and docs.
7. Benchmark baselines/scripts, only if intentional.

Acceptance criteria:

- Each commit has one purpose.
- No generated cache files are committed unless explicitly intended.
- CI-relevant gates pass after each slice.
- Docs introduced in a slice match the behavior shipped in that slice.

## 6. Workstream B: Mechanical Architecture Ratchets

Goal: convert architectural conventions into mechanical checks.

### B1. Import-Linter Ratchets

Keep the existing import-linter approach: enforce only what is true, allowlist
known debt, and shrink allowlists over time.

Required contracts:

- `lib` does not import `services`.
- core does not import demo/simulation overlays.
- `services.reasoning` does not directly import app/product/ingest layers.
- `services.domain` does not add new imports of reasoning internals.
- `services.domain` does not add new imports of product code.
- `services.ingest` does not add new imports of app code.

Acceptance criteria:

- `lint-imports` passes.
- Any allowlist addition requires a linked debt item.
- Every refactor PR removes at least one allowlist entry when practical.

### B2. Queue Entry Points

Use single owning modules for durable queue inserts:

- `services/domain/triggers.py` owns `think_trigger_queue`
- `services/domain/triggers.py` owns compatibility writes to
  `model_reeval_queue`
- `services/reasoning/think/post_commit.py` owns
  `pending_post_commit_actions`
- `services/domain/obligations.py` owns `think_obligations`

Ban raw queue `INSERT` statements outside:

- the queue's owning module
- tests that intentionally seed rows
- migration files
- explicitly exempted benchmark/probe scripts

Acceptance criteria:

- production code calls the owning helper, such as `enqueue_trigger(...)`,
  `enqueue_model_reeval(...)`, `enqueue_post_commit_actions(...)`, or
  `open_obligation(...)`.
- raw queue insert ratchets fail CI outside the allowlists.
- queue helper tests cover payload JSON, scheduling, pre-locking, and model
  re-eval compatibility.

### B3. Runtime Manifest Parity

The process manifest should become the source of truth for expected runtime
processes.

Add checks that compare:

- manifest process names
- compose service names
- script launchers
- healthcheck expectations

Acceptance criteria:

- every production process has either a compose service or an explicit
  exemption
- every compose worker has a process-manifest entry
- every long-running worker has a health surface or documented reason

### B4. Size And Complexity Dashboard

Add a lightweight script that reports:

- files above 1,500 LOC
- functions above 200 LOC
- classes above 600 LOC or 15 methods
- raw SQL writes to domain tables outside domain/reasoning write paths
- import-linter allowlist counts
- queue-depth helper coverage

Do not block CI on the first version. Publish the report as an artifact and
ratchet thresholds down later.

Current command:

```bash
python scripts/report_tech_debt_metrics.py --top 20
python scripts/check_tech_debt_budget.py
```

Acceptance criteria:

- report runs locally without infrastructure
- top 20 hotspots are stable and linkable
- thresholds are documented
- hotspot and violation counts cannot grow without an intentional budget reset

## 7. Workstream C: Runtime And Operations Maturity

Goal: make implemented behavior actually run and be observable.

### C1. Housekeeper Worker

Complete `services/workers/housekeeper` as the scheduler for low-frequency jobs.

Default enabled jobs:

- deadline resolver
- obligation due sweep
- hourly decay
- archive decayed models
- relationship maintenance
- calibration updater
- edge drift

Opt-in expensive jobs:

- topology sweeper
- precipitation
- relationship ontology proposals
- SAGE structural features

Acceptance criteria:

- `scripts/run_housekeeper_worker.py` runs in once mode and forever mode.
- housekeeper has a compose service.
- failures are isolated per job.
- every job has an interval env var and initial delay env var.
- expensive jobs are disabled by default.
- logs include job name, duration, success/failure, and error class.

### C2. Move Background Loops Out Of Gateway

The gateway should be a composition root for HTTP/WS, not the owner of durable
periodic work.

Move or isolate:

- greeting scheduler loops
- cache refresh loops
- post-commit polling loops that can run under a singleton worker

Acceptance criteria:

- gateway startup gets simpler.
- duplicate render risk does not grow with gateway replicas.
- realtime update behavior is preserved through NOTIFY or another explicit
  bridge.
- gateway can restart without killing durable background progress.

### C3. Compose Profiles And Memory Limits

Make the default production process set small and explicit.

Default profile should include:

- Postgres
- gateway
- think worker
- post-commit worker
- embedding backlog if needed
- housekeeper
- active live-source workers for configured sources

Profiles should hold:

- Kafka data plane
- observability stack
- expensive SAGE/topology jobs
- source families not used in the deployment

Acceptance criteria:

- every worker has a memory limit or documented exemption
- `docker compose config` succeeds
- default compose startup does not launch unused source families
- docs show the exact profile commands

### C4. Operational Readiness Gates

Promote the readiness harnesses into a standard pre-release step.

Minimum gates:

- schema drift
- migration prefix uniqueness
- trigger/obligation drain checks
- pending post-commit actions
- model re-eval queue depth
- selected pytest slices
- benchmark smoke where applicable

Acceptance criteria:

- harness writes JSON and Markdown reports
- report paths are documented
- fail/warn/manual statuses are clear
- release docs say which statuses block deploy

## 8. Workstream D: De-Fang God Modules

Goal: split large modules along stable business responsibilities.

### D1. `services/platform/execution/inquiry.py`

Problem: this file is too large and appears misfiled. It owns retrieval
orchestration that belongs closer to reasoning.

Target structure:

```text
services/reasoning/execution/
  __init__.py
  inquiry.py              # compatibility facade, small
  planner.py              # question/path planning
  retrieval_loop.py       # loop and stop conditions
  evidence.py             # evidence item assembly and ranking
  persistence.py          # inquiry session/question/evidence writes
  scoring.py              # sufficiency and quality scoring
  types.py                # shared dataclasses/protocols
```

Migration sequence:

1. Add `services/reasoning/execution` package.
2. Move pure types first.
3. Extract pure scoring/evidence helpers.
4. Extract persistence.
5. Move orchestration.
6. Leave a re-export shim at the old import path.
7. Update imports gradually.
8. Add import-linter rule forbidding new imports from the old path.
9. Delete shim when imports are gone.

Acceptance criteria:

- public function signatures remain compatible during migration.
- Think and Query retrieval tests pass.
- old path has no new call sites.
- no single new file exceeds 1,500 LOC.

### D2. `services/reasoning/think/applier.py`

Problem: one module applies many unrelated operation families and owns complex
idempotency behavior.

Target structure:

```text
services/reasoning/think/apply/
  __init__.py
  orchestrator.py
  claims.py
  edges.py
  acts.py
  resources.py
  predictions.py
  topology.py
  audit.py
  idempotency.py
  results.py
```

Migration sequence:

1. Extract result/state dataclasses.
2. Extract idempotency helpers around `applied_triggers`.
3. Extract claim op handlers.
4. Extract edge op handlers.
5. Extract act/resource/prediction handlers.
6. Keep `apply_diff` as the public facade.
7. Add tests per op family.

Acceptance criteria:

- `apply_diff` behavior remains stable.
- existing applier tests pass.
- op-family modules do not import product or app code.
- error reporting remains visible to validator/retry flows.

### D3. `services/domain/models/repo.py`

Problem: repository code triggers reasoning/product side effects and does too
much inside insert transactions.

Target responsibilities:

- validate row inputs
- persist model rows
- maintain domain-owned sidecars
- emit state-change observations
- enqueue explicit post-commit work, not execute it inline

Move out:

- topology dispatch
- affordance profile updates
- auto-accept recommendation behavior
- network embedding calls inside transactions
- reasoning/product imports

Acceptance criteria:

- `services.domain.models.repo` no longer imports `services.reasoning` or
  `services.product`.
- model insert transaction does not perform outbound network calls.
- downstream side effects run through outbox/post-commit jobs.
- import-linter allowlist entries shrink.
- tests cover eventual consistency windows.

### D4. Gateway Route Modules

Do not re-split `gateway/main.py`; that work has already happened. Instead,
target remaining large route modules only when route contracts are pinned.

Before splitting any route file:

1. Capture `/openapi.json` from a running gateway.
2. Split by product surface or route family.
3. Capture `/openapi.json` again.
4. Diff route paths, methods, request bodies, and response models.

Acceptance criteria:

- route set is unchanged unless explicitly documented.
- no route module owns unrelated product logic.
- auth and public-path behavior stay covered by tests.

## 9. Workstream E: One Mutation Grammar

Goal: make domain memory changes audit-friendly and consistent.

### E1. Applied Diff Ledger

Add an `applied_diffs` table that stores the materialized validated diff applied
by Think.

Fields:

- `id`
- `tenant_id`
- `trigger_id`
- `think_run_id`
- `schema_version`
- `diff_hash`
- `diff_json`
- `applied_at`

Acceptance criteria:

- write happens in the same transaction as `applied_triggers` success.
- diff can be replayed into a scratch schema in dry-run mode.
- schema version is explicit.
- failure to write the ledger fails the apply transaction.

### E2. Domain Command Seam For Product Writes

Product code should not write directly to domain tables with ad hoc SQL.

Move product mutation paths into domain functions for:

- resource current value updates
- act state transitions
- recommendation ratify/reject effects
- decision delta application
- topology/state-change emission where still required

Acceptance criteria:

- product calls named domain commands.
- commands emit state changes consistently.
- commands enqueue follow-up triggers through `services/domain/triggers.py`.
- raw product SQL writes to `models`, `resources`, `goals`, `commitments`, and
  `decisions` are either gone or explicitly allowlisted.

### E3. Post-Commit Outbox Design

Before moving model side effects out of transactions, make the outbox contract
fit non-Think actions.

Required design decisions:

- action kind registry
- dedup key shape
- trigger relationship when there is no Think trigger
- retry policy
- dead-letter semantics
- observability fields
- ordering expectations

Acceptance criteria:

- migration is explicit and backward compatible.
- post-commit worker can process old and new action shapes.
- dead-lettered actions are visible in readiness gates.
- side-effect timing is documented as eventual.

## 10. Workstream F: Embedding Lifecycle

Goal: semantic vector columns should only contain semantic vectors.

### F1. Add Model Embedding State

Add columns to `models`:

- `embedding_pending boolean`
- `embedding_provider text`
- `embedding_model text`
- `embedding_version text`
- `embedding_updated_at timestamptz`

Exact names can change during design, but the state must distinguish real,
pending, stale, and failed embeddings.

Acceptance criteria:

- no deterministic lexical placeholder is treated as semantic.
- inserts without embeddings mark `embedding_pending`.
- retrieval can filter or degrade pending rows intentionally.
- schema drift lock is updated.

### F2. Model Embedding Backfill

Build a backfill/rebuild command:

```bash
python scripts/rebuild_model_embeddings.py --tenant-id ... --limit ...
```

It should:

- lease rows safely
- batch embedding calls
- update provenance
- retry transient failures
- emit metrics

Acceptance criteria:

- can run in dry-run mode.
- can target one tenant.
- can resume after interruption.
- tests cover pending, stale, and already-current rows.

### F3. Retrieval Guard

Make retrieval behavior explicit for pending embeddings:

- exclude pending semantic vectors from vector search, or
- include them only through non-vector paths with a clear score penalty.

Acceptance criteria:

- tests prove fake/pending vectors cannot dominate semantic retrieval.
- metrics report pending ratio per tenant.

## 11. Workstream G: Ingestion Coherence

Goal: every source event is replayable and normalization semantics are shared.

### G1. Canonical Raw Event Capture

Introduce a canonical `raw_events` record before downstream ingestion work.

Fields should include:

- `id`
- `tenant_id`
- `source`
- `ingress_kind`
- `external_id`
- `content_hash`
- `raw_uri` or `s3_key`
- `received_at`
- `status`
- `error`

Acceptance criteria:

- every webhook, poller, backfill, and live-source path captures raw first.
- raw capture has a health/readiness check.
- replay can select raw events by tenant/source/time/status.

### G2. Shared Draft Normalization

Move source-specific canonicalization into the shared draft path so inline and
Kafka paths are set-equal.

Known target:

- Gmail thread canonicalization should not only run on one path.

Acceptance criteria:

- one test runs the same fixture through both paths and compares observation
  fields.
- `unresolved_phrases` and raw payload metadata survive reconstruction.

### G3. Kafka As A Scaling Profile

Kafka should be a scaling profile, not a second source of truth.

Acceptance criteria:

- inline path remains the default for small deployment.
- Kafka profile can be enabled per deployment.
- `raw_events` remains canonical even when Kafka transports bytes.
- circuit breaker and tenant flags are documented or retired consistently.

## 12. Workstream H: Tests, Benchmarks, And CI

Goal: tests should make refactoring safer without making every PR expensive.

### H1. Test Pyramid By Risk

Use these tiers:

1. Static gates: ruff, import-linter, migration filename checks.
2. Fast unit tests for pure helpers and validators.
3. Postgres integration tests for repositories, queues, and workers.
4. Route contract tests for gateway surfaces.
5. Real-LLM and benchmark tests as nightly or promotion gates.

Acceptance criteria:

- default PR CI stays bounded.
- heavy tests are named and documented.
- real-LLM tests remain opt-in.

### H2. Contract Tests For Critical Seams

Add or strengthen tests for:

- queue helper behavior
- post-commit action processing
- model insert side effects
- embedding pending lifecycle
- raw event replay
- gateway route set
- runtime manifest versus compose

### H3. Benchmark Hygiene

Benchmarks should not pollute the repo with generated cache files.

Acceptance criteria:

- generated embeddings/cache paths are ignored unless intentionally committed.
- retained baselines are small and reviewed.
- benchmark scripts write to predictable output roots.

## 13. Workstream I: Documentation And Developer Experience

Goal: documentation should be accurate, short, and operational.

### I1. Docs Taxonomy

Keep docs in these categories:

- Architecture: what is true now.
- Plans: proposed or in-progress work.
- ADRs: accepted decisions.
- Runbooks: operational commands and response steps.
- Validation reports: dated evidence from test/production runs.

Every doc should declare status and date near the top.

### I2. Remove Stale Contradictions

Docs should not say a worker is deployed if compose does not run it. Docs should
not say a table is active if no code writes it.

Acceptance criteria:

- feature-status docs reflect current process manifest.
- architecture docs link to this refactor plan for proposed work.
- stale generated `site/` output is not treated as source documentation.

### I3. Onboarding Runbook

Create a short production maintainer runbook:

- how to boot local stack
- how to run migrations
- how to run core gates
- how to inspect queues
- how to restart workers
- how to read readiness reports
- how to roll back a bad worker/deploy

## 14. Sequenced Roadmap

### Phase 0: Branch Stabilization

Duration: 1 to 3 days

Deliverables:

- green ruff conservative gate
- green import-linter
- targeted trigger/housekeeper tests passing
- generated artifacts separated from source changes
- current work split into reviewable commits

Exit criteria:

- `git status` only shows intentional source/doc changes
- every new source file has a clear owning workstream
- current branch can be reviewed without cache noise

### Phase 1: Guardrails First

Duration: 1 week

Deliverables:

- raw trigger insert ban
- process manifest parity test
- import-linter allowlist ratchet documented
- size/complexity dashboard report
- technical-debt budget ratchet
- header redaction helper and tests
- loud rendering-backend guard for production config

Exit criteria:

- new architectural violations fail mechanically
- top debt metrics are visible in one command
- obvious production footguns are closed

### Phase 2: Runtime Honesty

Duration: 1 to 2 weeks

Deliverables:

- housekeeper compose service
- low-frequency jobs wired
- expensive jobs opt-in
- worker health/logging improvements
- readiness harness documented
- compose profiles and memory limits drafted

Exit criteria:

- implemented maintenance loops actually run
- queue and obligation depth are visible
- production process set is explicit

### Phase 3: Domain Mutation Cleanup

Duration: 2 weeks

Deliverables:

- post-commit outbox design
- `applied_diffs` ledger
- product domain-command seam
- ModelsRepo side-effect inversion design
- first ModelsRepo side-effect moved out of transaction

Exit criteria:

- domain/product/reasoning coupling shrinks
- no new product raw writes to domain tables
- model insert transaction is simpler and safer

### Phase 4: Embedding Lifecycle

Duration: 1 to 2 weeks

Deliverables:

- model embedding state migration
- applier stops storing fake semantic vectors
- embedding backfill command
- retrieval guard for pending embeddings
- metrics for embedding pending/stale ratios

Exit criteria:

- semantic vector search is no longer polluted by placeholders
- embeddings are rebuildable
- retrieval quality tests pin the behavior

### Phase 5: God Module Extraction

Duration: 2 to 4 weeks

Deliverables:

- `inquiry.py` split behind compatibility facade
- `applier.py` split by op family
- `run_storyline_batch_benchmark.py` split if it blocks daily work
- large route modules split only with route contract tests

Exit criteria:

- no critical runtime file exceeds agreed threshold without exemption
- public facades stay stable
- tests cover each extracted responsibility

### Phase 6: Ingestion Coherence

Duration: 2 weeks

Deliverables:

- raw event capture design and migration
- raw capture in one source path as proof
- shared draft normalization fixes
- inline/Kafka equivalence tests
- Kafka profile ADR update

Exit criteria:

- every source event can become replayable
- split-brain normalization risks are shrinking
- Kafka is a transport choice, not a second truth path

## 15. Metrics Dashboard

Track these weekly:

| Metric | Current baseline | Target |
| --- | ---: | ---: |
| Import-linter allowlist entries | Use `pyproject.toml` count | Down every milestone |
| Raw production inserts into `think_trigger_queue` | Near zero outside helper | Zero outside helper |
| Files above 1,500 LOC | Many | Down 50 percent |
| Functions above 200 LOC | Many | Down 50 percent |
| Domain imports of product/reasoning | Allowlisted | Zero runtime imports |
| Pending post-commit actions | Deployment-specific | Alert threshold documented |
| Open obligations | Deployment-specific | Alert threshold documented |
| Model embedding pending ratio | Unknown | Visible, bounded |
| Worker manifest/compose drift | Unknown | Zero |
| Ruff conservative errors | 0 in conservative gate | Zero |

## 16. Risk Register

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Dirty branch mixes unrelated work | Review becomes impossible | Stabilize and commit in slices before deeper refactor. |
| Allowlists grow instead of shrink | Architecture slowly decays | Require linked debt item for any new allowlist row. |
| Housekeeper concentrates too many jobs | One bad job starves others | Per-job try/except, intervals, LLM caps, memory limits, and health checks. |
| Moving side effects post-commit changes timing | Product sees brief eventual-consistency windows | Add explicit tests and UI/API tolerance for lag. |
| Applied diff replay overpromises determinism | False confidence in rebuilds | Document replay as deterministic diff replay, not LLM replay. |
| Embedding migration hurts retrieval temporarily | Search quality changes | Roll out with pending/stale metrics and backfill before strict filtering. |
| Ingestion raw capture adds latency | Webhook timeouts | Capture efficiently, measure latency, support async continuation. |
| Large file extraction breaks imports | Runtime failure | Use compatibility facades and update imports gradually. |

## 17. First Ten PRs

1. Fix current ruff failures and update generated artifact ignores if needed.
2. Land `services/domain/triggers.py` plus migrated production call sites.
3. Add CI grep banning raw `think_trigger_queue` inserts outside helper/tests.
4. Land housekeeper worker and launcher tests.
5. Add housekeeper compose service in a conservative default configuration.
6. Add process manifest parity tests.
7. Add operational readiness gate docs and make the harness command stable.
8. Add size/complexity dashboard script.
9. Add header redaction helper everywhere request headers are captured.
10. Write the post-commit outbox design for ModelsRepo side-effect inversion.

## 18. Definition Of Done

The deep refactor is done when:

- default CI is green and includes architectural ratchets
- production process manifest matches compose/script reality
- all required maintenance loops run or have explicit exemptions
- trigger enqueueing is centralized
- product write paths go through sanctioned domain commands
- ModelsRepo no longer imports product or reasoning internals
- model embedding lifecycle is explicit and rebuildable
- ingestion has canonical raw capture and shared normalization semantics
- largest modules have been split behind stable facades
- docs accurately distinguish shipped behavior from plans
- readiness reports give a maintainer enough signal to deploy or stop

This is the path from "fast-moving impressive prototype" to "production-grade
system a small team can safely keep evolving."
