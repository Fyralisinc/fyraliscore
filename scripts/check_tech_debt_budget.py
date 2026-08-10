#!/usr/bin/env python3
"""Enforce the current technical-debt budget.

The dashboard in ``report_tech_debt_metrics.py`` shows where refactor work
should go. This checker is the ratchet: counts may go down, but they should not
grow casually during unrelated PRs.
"""

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.report_tech_debt_metrics import TechDebtReport, build_report  # noqa: E402


DEFAULT_FILE_LINE_BUDGETS = {
    "services/reasoning/think/reconciler.py": 1387,
    "services/reasoning/sage/outcome_evaluator.py": 1476,
    "services/platform/execution/inquiry.py": 493,
}

DEFAULT_FUNCTION_LINE_BUDGETS = {
    "services/app/gateway/debug_router.py:build_debug_router": 78,
    "services/app/gateway/map_routes.py:_build_snapshot": 75,
    "services/app/gateway/structure_router.py:build_structure_router": 25,
    "services/app/gateway/today_routes.py:register_today_routes": 21,
    "services/app/gateway/ceo_view_wiring.py:configure_ceo_view": 28,
    "services/ingest/ingestion/writers/observation_writer.py:_handle_message": 136,
    "services/ingest/ingestion/core.py:ingest_from_draft": 69,
    "services/ingest/ingestion/workflows/shard_fetch.py:_run_fetch_loop": 99,
    "services/domain/acts/commitments.py:create": 58,
    "services/domain/bridge/queries.py:revenue_at_risk": 38,
    "services/domain/models/repo.py:_insert_core": 35,
    "services/app/webhooks/router.py:_inline_ingest_response": 108,
    "services/app/webhooks/router.py:_receive_webhook": 85,
    "services/app/webhooks/router.py:build_webhooks_router": 30,
    "services/app/gateway/recommendations_router.py:build_recommendations_router": 38,
    "services/app/gateway/artifact_drawers.py:fetch_commitment_overlay": 56,
    "services/product/decision_deltas/router.py:build_router": 18,
    "services/product/forecasts/router.py:build_router": 18,
    "services/product/resolution_threads/router.py:build_router": 16,
    "services/product/greeting/scheduler.py:_refresh_tenant_inner": 27,
    "services/reasoning/sage/health.py:build_sage_health_report": 46,
    "services/reasoning/sage/cue_extractor.py:_extract_sync": 32,
    "services/reasoning/sage/evidence_projection.py:_rank_for_model": 65,
    "services/reasoning/contestability/service.py:contest_model": 44,
    "services/reasoning/sage/outcome_evaluator.py:_evaluate": 194,
    "services/reasoning/sage/reader.py:read": 179,
    "services/reasoning/think/applier.py:_apply_act_op": 24,
    "services/reasoning/think/applier.py:_apply_claim_op": 37,
    "services/reasoning/think/applier.py:apply_diff": 149,
    "services/reasoning/think/reason.py:_run_once": 143,
    "services/reasoning/think/reconciler.py:_reconcile_inner": 45,
    "services/reasoning/think/reason.py:think": 148,
    "services/reasoning/think/context_use.py:summarize_context_use": 198,
    "services/reasoning/think/validator.py:validate": 88,
    "services/reasoning/retrieval/assembler.py:assemble_context": 88,
    "services/reasoning/retrieval/primary.py:primary_retrieve": 152,
    "services/reasoning/retrieval/pathways.py:pathway_b_semantic": 88,
    "services/reasoning/retrieval/pathways.py:pathway_a_structural": 129,
    "services/reasoning/retrieval/pathways.py:pathway_g_model_edges": 87,
    "benchmarks/adapters/stress10_adapter.py:__init__": 6,
    "scripts/run_storyline_batch_benchmark.py:run_benchmark": 101,
    "scripts/run_storyline_batch_benchmark.py:score_storylines": 34,
    "scripts/run_storyline_batch_benchmark.py:_company_intelligence_scorecard": 78,
    "scripts/run_storyline_batch_benchmark.py:_product_value_evals": 110,
    "scripts/run_1000_signal_model_layer_probe.py:collect_model_layer_report": 46,
    "scripts/run_1000_signal_model_layer_probe.py:main": 55,
    "benchmarks/run_benchmark.py:main": 23,
    "scripts/run_100x_5000_model_e2e_stress.py:_build_case_models": 86,
    "tests/real_llm/infrastructure/scenario_loader.py:materialize": 88,
    "services/ingest/ingestion/workflows/tests/test_oauth_to_source_completion_end_to_end.py:test_oauth_trigger_to_source_completion_end_to_end": 53,
    "services/ingest/ingestion/workflows/tests/test_oauth_to_tenant_completion_with_reconciler_reshare.py:test_oauth_trigger_to_tenant_completion_with_reconciler_reshare_path": 35,
    "services/ingest/ingestion/workflows/tests/test_shard_fetch_subprocess.py:test_shard_fetch_resumes_from_persisted_cursor_after_restart": 56,
    "services/reasoning/retrieval/tests/test_retrieval_quality_harness.py:test_quality_eval_corpus_mixed_entrypoints_regression_gate": 19,
}

FILE_LINE_BUDGET_ARG_NAMES = {
    "services/reasoning/sage/outcome_evaluator.py": (
        "max_outcome_evaluator_file_lines"
    ),
    "services/reasoning/think/reconciler.py": "max_think_reconciler_file_lines",
    "services/platform/execution/inquiry.py": "max_platform_inquiry_lines",
}

FUNCTION_LINE_BUDGET_ARG_NAMES = {
    "services/app/gateway/debug_router.py:build_debug_router": (
        "max_debug_router_factory_lines"
    ),
    "services/app/gateway/map_routes.py:_build_snapshot": "max_map_snapshot_lines",
    "services/app/gateway/structure_router.py:build_structure_router": (
        "max_structure_router_factory_lines"
    ),
    "services/app/gateway/today_routes.py:register_today_routes": (
        "max_today_routes_registration_lines"
    ),
    "services/app/gateway/ceo_view_wiring.py:configure_ceo_view": (
        "max_ceo_view_wiring_lines"
    ),
    "services/ingest/ingestion/writers/observation_writer.py:_handle_message": (
        "max_observation_writer_handle_message_lines"
    ),
    "services/ingest/ingestion/core.py:ingest_from_draft": (
        "max_ingest_from_draft_lines"
    ),
    "services/ingest/ingestion/workflows/shard_fetch.py:_run_fetch_loop": (
        "max_shard_fetch_loop_lines"
    ),
    "services/domain/acts/commitments.py:create": "max_commitment_create_lines",
    "services/domain/bridge/queries.py:revenue_at_risk": "max_revenue_at_risk_lines",
    "services/domain/models/repo.py:_insert_core": "max_model_insert_core_lines",
    "services/app/webhooks/router.py:_inline_ingest_response": (
        "max_webhook_inline_ingest_response_lines"
    ),
    "services/app/webhooks/router.py:_receive_webhook": "max_webhook_receive_lines",
    "services/app/webhooks/router.py:build_webhooks_router": (
        "max_webhooks_router_factory_lines"
    ),
    "services/app/gateway/recommendations_router.py:build_recommendations_router": (
        "max_recommendations_router_factory_lines"
    ),
    "services/app/gateway/artifact_drawers.py:fetch_commitment_overlay": (
        "max_artifact_commitment_overlay_lines"
    ),
    "services/product/decision_deltas/router.py:build_router": (
        "max_decision_deltas_router_lines"
    ),
    "services/product/forecasts/router.py:build_router": "max_forecasts_router_lines",
    "services/product/resolution_threads/router.py:build_router": (
        "max_resolution_threads_router_lines"
    ),
    "services/product/greeting/scheduler.py:_refresh_tenant_inner": (
        "max_greeting_refresh_inner_lines"
    ),
    "services/reasoning/sage/health.py:build_sage_health_report": (
        "max_sage_health_report_lines"
    ),
    "services/reasoning/sage/cue_extractor.py:_extract_sync": (
        "max_sage_cue_extractor_lines"
    ),
    "services/reasoning/sage/evidence_projection.py:_rank_for_model": (
        "max_sage_evidence_rank_lines"
    ),
    "services/reasoning/contestability/service.py:contest_model": (
        "max_contestation_service_entrypoint_lines"
    ),
    "services/reasoning/sage/outcome_evaluator.py:_evaluate": (
        "max_outcome_evaluator_lines"
    ),
    "services/reasoning/sage/reader.py:read": "max_sage_reader_read_lines",
    "services/reasoning/think/applier.py:_apply_act_op": (
        "max_think_apply_act_op_lines"
    ),
    "services/reasoning/think/applier.py:_apply_claim_op": (
        "max_think_apply_claim_op_lines"
    ),
    "services/reasoning/think/applier.py:apply_diff": "max_think_apply_diff_lines",
    "services/reasoning/think/reason.py:_run_once": "max_think_run_once_lines",
    "services/reasoning/think/reconciler.py:_reconcile_inner": (
        "max_think_reconcile_inner_lines"
    ),
    "services/reasoning/think/reason.py:think": "max_think_entrypoint_lines",
    "services/reasoning/think/context_use.py:summarize_context_use": (
        "max_think_context_use_lines"
    ),
    "services/reasoning/think/validator.py:validate": "max_think_validate_lines",
    "services/reasoning/retrieval/assembler.py:assemble_context": (
        "max_assemble_context_lines"
    ),
    "services/reasoning/retrieval/primary.py:primary_retrieve": (
        "max_primary_retrieve_lines"
    ),
    "services/reasoning/retrieval/pathways.py:pathway_b_semantic": (
        "max_pathway_b_semantic_lines"
    ),
    "services/reasoning/retrieval/pathways.py:pathway_a_structural": (
        "max_pathway_a_structural_lines"
    ),
    "services/reasoning/retrieval/pathways.py:pathway_g_model_edges": (
        "max_pathway_g_model_edges_lines"
    ),
    "benchmarks/adapters/stress10_adapter.py:__init__": (
        "max_stress10_adapter_init_lines"
    ),
    "scripts/run_storyline_batch_benchmark.py:run_benchmark": (
        "max_storyline_run_benchmark_lines"
    ),
    "scripts/run_storyline_batch_benchmark.py:score_storylines": (
        "max_storyline_score_lines"
    ),
    "scripts/run_storyline_batch_benchmark.py:_company_intelligence_scorecard": (
        "max_company_intelligence_scorecard_lines"
    ),
    "scripts/run_storyline_batch_benchmark.py:_product_value_evals": (
        "max_product_value_evals_lines"
    ),
    "scripts/run_1000_signal_model_layer_probe.py:collect_model_layer_report": (
        "max_model_layer_report_lines"
    ),
    "scripts/run_1000_signal_model_layer_probe.py:main": (
        "max_model_layer_probe_main_lines"
    ),
    "benchmarks/run_benchmark.py:main": "max_benchmark_runner_main_lines",
    "scripts/run_100x_5000_model_e2e_stress.py:_build_case_models": (
        "max_model_e2e_stress_case_models_lines"
    ),
    "tests/real_llm/infrastructure/scenario_loader.py:materialize": (
        "max_scenario_loader_materialize_lines"
    ),
    "services/ingest/ingestion/workflows/tests/test_oauth_to_source_completion_end_to_end.py:test_oauth_trigger_to_source_completion_end_to_end": (
        "max_oauth_source_completion_e2e_lines"
    ),
    "services/ingest/ingestion/workflows/tests/test_oauth_to_tenant_completion_with_reconciler_reshare.py:test_oauth_trigger_to_tenant_completion_with_reconciler_reshare_path": (
        "max_oauth_tenant_reshare_e2e_lines"
    ),
    "services/ingest/ingestion/workflows/tests/test_shard_fetch_subprocess.py:test_shard_fetch_resumes_from_persisted_cursor_after_restart": (
        "max_shard_fetch_resume_e2e_lines"
    ),
    "services/reasoning/retrieval/tests/test_retrieval_quality_harness.py:test_quality_eval_corpus_mixed_entrypoints_regression_gate": (
        "max_retrieval_quality_mixed_entrypoints_lines"
    ),
}


@dataclass(frozen=True)
class TechDebtBudget:
    files_over_threshold: int = 37
    functions_over_threshold: int = 36
    classes_over_threshold: int = 23
    import_linter_ignored_imports_total: int = 71
    file_line_budgets: Mapping[str, int] = field(
        default_factory=lambda: dict(DEFAULT_FILE_LINE_BUDGETS)
    )
    function_line_budgets: Mapping[str, int] = field(
        default_factory=lambda: dict(DEFAULT_FUNCTION_LINE_BUDGETS)
    )
    raw_think_trigger_insert_violations: int = 0
    raw_model_reeval_insert_violations: int = 0
    raw_pending_post_commit_action_insert_violations: int = 0
    raw_think_obligation_insert_violations: int = 0
    parse_errors: int = 0


@dataclass(frozen=True)
class BudgetViolation:
    metric: str
    actual: int
    limit: int

    def render(self) -> str:
        return f"{self.metric}: {self.actual} exceeds budget {self.limit}"


def _file_line_count(repo_root: Path, rel_path: str) -> int:
    path = repo_root / rel_path
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        return sum(1 for _ in fh)


def _function_line_count(repo_root: Path, budget_key: str) -> int:
    rel_path, sep, function_name = budget_key.rpartition(":")
    if not sep or not rel_path or not function_name:
        return 0
    path = repo_root / rel_path
    if not path.exists():
        return 0
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except SyntaxError:
        return 0
    matches: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != function_name:
            continue
        lineno = getattr(node, "lineno", None)
        end_lineno = getattr(node, "end_lineno", None)
        if isinstance(lineno, int) and isinstance(end_lineno, int):
            matches.append(end_lineno - lineno + 1)
    return max(matches, default=0)


def check_budget(
    report: TechDebtReport,
    budget: TechDebtBudget = TechDebtBudget(),
    *,
    repo_root: Path = REPO_ROOT,
) -> list[BudgetViolation]:
    measurements = {
        "files_over_threshold": len(report.files_over_threshold),
        "functions_over_threshold": len(report.functions_over_threshold),
        "classes_over_threshold": len(report.classes_over_threshold),
        "import_linter_ignored_imports_total": (
            report.import_linter_ignored_imports_total
        ),
        "raw_think_trigger_insert_violations": (
            report.raw_think_trigger_insert_violations
        ),
        "raw_model_reeval_insert_violations": (
            report.raw_model_reeval_insert_violations
        ),
        "raw_pending_post_commit_action_insert_violations": (
            report.raw_pending_post_commit_action_insert_violations
        ),
        "raw_think_obligation_insert_violations": (
            report.raw_think_obligation_insert_violations
        ),
        "parse_errors": len(report.parse_errors),
    }
    limits = budget.__dict__
    violations = [
        BudgetViolation(metric=metric, actual=actual, limit=limits[metric])
        for metric, actual in measurements.items()
        if actual > limits[metric]
    ]
    for path, limit in sorted(budget.file_line_budgets.items()):
        actual = _file_line_count(repo_root, path)
        if actual > limit:
            violations.append(
                BudgetViolation(
                    metric=f"file_line_budget:{path}",
                    actual=actual,
                    limit=limit,
                )
            )
    for key, limit in sorted(budget.function_line_budgets.items()):
        actual = _function_line_count(repo_root, key)
        if actual > limit:
            violations.append(
                BudgetViolation(
                    metric=f"function_line_budget:{key}",
                    actual=actual,
                    limit=limit,
                )
            )
    return violations


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--max-files-over-threshold", type=int, default=37)
    parser.add_argument("--max-functions-over-threshold", type=int, default=36)
    parser.add_argument("--max-classes-over-threshold", type=int, default=23)
    parser.add_argument("--max-import-linter-ignored-imports", type=int, default=71)
    parser.add_argument("--max-platform-inquiry-lines", type=int, default=493)
    parser.add_argument("--max-outcome-evaluator-file-lines", type=int, default=1476)
    parser.add_argument("--max-think-reconciler-file-lines", type=int, default=1387)
    parser.add_argument("--max-debug-router-factory-lines", type=int, default=78)
    parser.add_argument("--max-finance-router-factory-lines", type=int, default=8)
    parser.add_argument("--max-map-snapshot-lines", type=int, default=75)
    parser.add_argument("--max-slack-router-factory-lines", type=int, default=9)
    parser.add_argument("--max-structure-router-factory-lines", type=int, default=25)
    parser.add_argument("--max-today-routes-registration-lines", type=int, default=21)
    parser.add_argument("--max-ceo-view-wiring-lines", type=int, default=28)
    parser.add_argument("--max-backfill-install-dispatch-lines", type=int, default=43)
    parser.add_argument("--max-hmac-webhook-payload-lines", type=int, default=6)
    parser.add_argument("--max-google-workspace-handler-lines", type=int, default=5)
    parser.add_argument("--max-observation-writer-handle-message-lines", type=int, default=136)
    parser.add_argument("--max-circuit-breaker-process-tick-lines", type=int, default=42)
    parser.add_argument("--max-ingest-from-draft-lines", type=int, default=69)
    parser.add_argument("--max-shard-fetch-loop-lines", type=int, default=99)
    parser.add_argument("--max-gmail-drain-history-lines", type=int, default=86)
    parser.add_argument("--max-live-driver-composition-lines", type=int, default=65)
    parser.add_argument("--max-run4-concurrent-lines", type=int, default=112)
    parser.add_argument("--max-discord-gateway-main-lines", type=int, default=74)
    parser.add_argument("--max-commitment-create-lines", type=int, default=58)
    parser.add_argument("--max-revenue-at-risk-lines", type=int, default=38)
    parser.add_argument("--max-model-insert-core-lines", type=int, default=35)
    parser.add_argument(
        "--max-webhook-inline-ingest-response-lines", type=int, default=108
    )
    parser.add_argument("--max-webhook-receive-lines", type=int, default=85)
    parser.add_argument("--max-webhooks-router-factory-lines", type=int, default=30)
    parser.add_argument(
        "--max-recommendations-router-factory-lines", type=int, default=38
    )
    parser.add_argument("--max-artifact-commitment-overlay-lines", type=int, default=56)
    parser.add_argument("--max-decision-deltas-router-lines", type=int, default=18)
    parser.add_argument("--max-forecasts-router-lines", type=int, default=18)
    parser.add_argument("--max-resolution-threads-router-lines", type=int, default=16)
    parser.add_argument("--max-greeting-refresh-inner-lines", type=int, default=27)
    parser.add_argument("--max-sage-health-report-lines", type=int, default=46)
    parser.add_argument("--max-sage-cue-extractor-lines", type=int, default=32)
    parser.add_argument("--max-sage-evidence-rank-lines", type=int, default=65)
    parser.add_argument("--max-contestation-service-entrypoint-lines", type=int, default=44)
    parser.add_argument("--max-outcome-evaluator-lines", type=int, default=194)
    parser.add_argument("--max-sage-reader-read-lines", type=int, default=179)
    parser.add_argument("--max-think-apply-act-op-lines", type=int, default=24)
    parser.add_argument("--max-think-apply-claim-op-lines", type=int, default=37)
    parser.add_argument("--max-think-apply-diff-lines", type=int, default=149)
    parser.add_argument("--max-think-run-once-lines", type=int, default=143)
    parser.add_argument("--max-think-reconcile-inner-lines", type=int, default=45)
    parser.add_argument("--max-think-entrypoint-lines", type=int, default=148)
    parser.add_argument("--max-think-context-use-lines", type=int, default=198)
    parser.add_argument("--max-think-validate-lines", type=int, default=88)
    parser.add_argument("--max-assemble-context-lines", type=int, default=88)
    parser.add_argument("--max-primary-retrieve-lines", type=int, default=152)
    parser.add_argument("--max-pathway-b-semantic-lines", type=int, default=88)
    parser.add_argument("--max-pathway-a-structural-lines", type=int, default=129)
    parser.add_argument("--max-pathway-g-model-edges-lines", type=int, default=87)
    parser.add_argument("--max-stress10-adapter-init-lines", type=int, default=6)
    parser.add_argument("--max-storyline-run-benchmark-lines", type=int, default=101)
    parser.add_argument("--max-storyline-score-lines", type=int, default=34)
    parser.add_argument("--max-company-intelligence-scorecard-lines", type=int, default=78)
    parser.add_argument("--max-product-value-evals-lines", type=int, default=110)
    parser.add_argument("--max-model-layer-report-lines", type=int, default=46)
    parser.add_argument("--max-model-layer-probe-main-lines", type=int, default=55)
    parser.add_argument("--max-benchmark-runner-main-lines", type=int, default=23)
    parser.add_argument("--max-model-e2e-stress-case-models-lines", type=int, default=86)
    parser.add_argument("--max-scenario-loader-materialize-lines", type=int, default=88)
    parser.add_argument("--max-oauth-source-completion-e2e-lines", type=int, default=53)
    parser.add_argument("--max-oauth-tenant-reshare-e2e-lines", type=int, default=35)
    parser.add_argument("--max-shard-fetch-resume-e2e-lines", type=int, default=56)
    parser.add_argument(
        "--max-retrieval-quality-mixed-entrypoints-lines", type=int, default=19
    )
    parser.add_argument("--file-line-threshold", type=int, default=1500)
    parser.add_argument("--function-line-threshold", type=int, default=200)
    parser.add_argument("--class-line-threshold", type=int, default=600)
    parser.add_argument("--class-method-threshold", type=int, default=15)
    return parser.parse_args(argv)


def _build_file_line_budgets(args: argparse.Namespace) -> dict[str, int]:
    budgets = dict(DEFAULT_FILE_LINE_BUDGETS)
    for path, attr_name in FILE_LINE_BUDGET_ARG_NAMES.items():
        budgets[path] = getattr(args, attr_name)
    return budgets


def _build_function_line_budgets(args: argparse.Namespace) -> dict[str, int]:
    budgets = dict(DEFAULT_FUNCTION_LINE_BUDGETS)
    for key, attr_name in FUNCTION_LINE_BUDGET_ARG_NAMES.items():
        budgets[key] = getattr(args, attr_name)
    return budgets


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    report = build_report(
        repo_root=args.repo_root.resolve(),
        file_line_threshold=args.file_line_threshold,
        function_line_threshold=args.function_line_threshold,
        class_line_threshold=args.class_line_threshold,
        class_method_threshold=args.class_method_threshold,
    )
    budget = TechDebtBudget(
        files_over_threshold=args.max_files_over_threshold,
        functions_over_threshold=args.max_functions_over_threshold,
        classes_over_threshold=args.max_classes_over_threshold,
        import_linter_ignored_imports_total=args.max_import_linter_ignored_imports,
        file_line_budgets=_build_file_line_budgets(args),
        function_line_budgets=_build_function_line_budgets(args),
    )
    violations = check_budget(report, budget, repo_root=args.repo_root.resolve())
    if violations:
        print("Technical-debt budget violations:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation.render()}", file=sys.stderr)
        return 1
    print("Technical-debt budget passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
