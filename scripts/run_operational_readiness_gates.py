#!/usr/bin/env python3
"""Run operational production-readiness gates.

This is the umbrella harness for the broader readiness checks that sit above
the learning-loop proof harness:

* focused learning-loop gap probes
* 50-batch storyline report thresholds
* schema drift / migration rehearsal guard
* shadow-write and cutover safety tests
* synthetic load generator smoke tests
* calibration monitoring tests
* tenant/privacy isolation tests

The script writes JSON and Markdown artifacts under
tests/real_llm/reports/runs/<run-id>/.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT_ROOT = REPO_ROOT / "tests" / "real_llm" / "reports" / "runs"

PASS = "pass"
WARN = "warn"
FAIL = "fail"
MANUAL_REQUIRED = "manual_required"
SKIPPED = "skipped"
BLOCKING_STATUSES = {FAIL}
INCOMPLETE_STATUSES = {MANUAL_REQUIRED, SKIPPED}


@dataclass
class GateResult:
    name: str
    status: str
    details: str
    metrics: dict[str, Any] = field(default_factory=dict)
    command: list[str] | None = None
    elapsed_seconds: float | None = None
    stdout_tail: str = ""
    stderr_tail: str = ""
    artifacts: dict[str, str] = field(default_factory=dict)


@dataclass
class OperationalReadinessReport:
    run_id: str
    status: str
    automated_gates_passed: bool
    production_ready: bool
    started_at: str
    elapsed_seconds: float
    report_dir: str
    gates: list[GateResult]


def _utc_run_id(prefix: str) -> str:
    return f"{prefix}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=_json_default))


def _tail(text: str, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def _load_json(path: Path) -> dict[str, Any] | list[Any]:
    return json.loads(path.read_text())


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _latest_run_dir(report_root: Path, prefix: str) -> Path | None:
    candidates = [p for p in report_root.glob(f"{prefix}*") if p.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _base_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("COMPANY_OS_ENV", "test")
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def _python_command(*args: str) -> list[str]:
    return [sys.executable, *args]


def _run_command_gate(
    name: str,
    command: list[str],
    *,
    details: str,
    timeout_s: int,
    env: dict[str, str] | None = None,
    artifacts: dict[str, str] | None = None,
) -> GateResult:
    print(f"[gate] {name}: running {' '.join(command)}", flush=True)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=env or _base_env(),
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        elapsed = round(time.monotonic() - started, 3)
        return GateResult(
            name=name,
            status=FAIL,
            details=f"Command timed out after {timeout_s}s.",
            metrics={"timeout_seconds": timeout_s},
            command=command,
            elapsed_seconds=elapsed,
            stdout_tail=_tail(exc.stdout or ""),
            stderr_tail=_tail(exc.stderr or ""),
            artifacts=artifacts or {},
        )
    elapsed = round(time.monotonic() - started, 3)
    status = PASS if completed.returncode == 0 else FAIL
    return GateResult(
        name=name,
        status=status,
        details=details if status == PASS else "Command exited non-zero.",
        metrics={"returncode": completed.returncode},
        command=command,
        elapsed_seconds=elapsed,
        stdout_tail=_tail(completed.stdout or ""),
        stderr_tail=_tail(completed.stderr or ""),
        artifacts=artifacts or {},
    )


def _manual_gate(
    name: str,
    details: str,
    *,
    metrics: dict[str, Any] | None = None,
    artifacts: dict[str, str] | None = None,
) -> GateResult:
    return GateResult(
        name=name,
        status=MANUAL_REQUIRED,
        details=details,
        metrics=metrics or {},
        artifacts=artifacts or {},
    )


def _query_live_queue_state(database_url: str, tenant_id: str) -> dict[str, int]:
    import psycopg2

    query = """
    SELECT
      (SELECT COUNT(*)::bigint
         FROM think_trigger_queue
        WHERE tenant_id = %s::uuid AND completed_at IS NULL) AS pending_triggers,
      (SELECT COUNT(*)::bigint
         FROM pending_post_commit_actions
        WHERE tenant_id = %s::uuid
          AND processed_at IS NULL
          AND dead_lettered_at IS NULL) AS pending_post_commit_actions,
      (SELECT COUNT(*)::bigint
         FROM pending_post_commit_actions
        WHERE tenant_id = %s::uuid AND dead_lettered_at IS NOT NULL)
        AS dead_lettered_post_commit_actions,
      (SELECT COUNT(*)::bigint
         FROM model_reeval_queue
        WHERE tenant_id = %s::uuid AND processed_at IS NULL)
        AS pending_model_reeval,
      (SELECT COUNT(*)::bigint
         FROM think_obligations
        WHERE tenant_id = %s::uuid AND status = 'open') AS open_obligations
    """
    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, tenant_id, tenant_id, tenant_id, tenant_id))
            row = cur.fetchone()
    keys = [
        "pending_triggers",
        "pending_post_commit_actions",
        "dead_lettered_post_commit_actions",
        "pending_model_reeval",
        "open_obligations",
    ]
    return {key: int(value or 0) for key, value in zip(keys, row, strict=True)}


def _feedback_gap_gate(args: argparse.Namespace) -> GateResult:
    if args.skip_gap_harness:
        return GateResult(
            name="feedback_loop_gap_harness",
            status=SKIPPED,
            details="Skipped by --skip-gap-harness.",
        )
    gap_run_id = f"{args.run_id}-gap"
    command = _python_command(
        "scripts/run_production_readiness_gap_harness.py",
        "--run-id",
        gap_run_id,
    )
    result = _run_command_gate(
        "feedback_loop_gap_harness",
        command,
        details="Focused learning-loop gap probes passed.",
        timeout_s=args.command_timeout_s,
    )
    report_path = args.report_root / gap_run_id / "production_readiness_gap_report.json"
    if report_path.exists():
        result.artifacts["gap_report"] = _relative(report_path)
        data = _load_json(report_path)
        if isinstance(data, dict):
            result.metrics.update({
                "gap_harness_passed": bool(data.get("passed")),
                "gap_count": len(data.get("gates") or []),
            })
    return result


def _storyline_report_gate(args: argparse.Namespace) -> GateResult:
    run_dir = args.storyline_run_dir or _latest_run_dir(
        args.report_root,
        "learning-loop-50-batch-",
    )
    if run_dir is None:
        return GateResult(
            name="storyline_50_batch_report",
            status=FAIL,
            details="No learning-loop-50-batch report directory found.",
        )

    summary_path = run_dir / "storyline_scores.json"
    run_summary_path = run_dir / "run_summary.json"
    if not summary_path.exists() or not run_summary_path.exists():
        return GateResult(
            name="storyline_50_batch_report",
            status=FAIL,
            details="Report is missing storyline_scores.json or run_summary.json.",
            artifacts={"run_dir": _relative(run_dir)},
        )

    summary = _load_json(summary_path)
    run_summary = _load_json(run_summary_path)
    if not isinstance(summary, dict) or not isinstance(run_summary, dict):
        return GateResult(
            name="storyline_50_batch_report",
            status=FAIL,
            details="Report JSON has an unexpected shape.",
            artifacts={"run_dir": _relative(run_dir)},
        )

    scorecard = summary.get("company_intelligence_scorecard") or {}
    product_value = scorecard.get("product_value_evals") or {}
    calibration = summary.get("calibration") or {}
    amplification = summary.get("run_amplification") or {}
    waves = summary.get("waves") or []
    tenant_id = str(summary.get("tenant_id") or run_summary.get("tenant_id") or "")

    metrics: dict[str, Any] = {
        "run_id": summary.get("run_id") or run_summary.get("run_id"),
        "tenant_id": tenant_id,
        "waves": len(waves),
        "signals": _safe_int(summary.get("signals") or run_summary.get("signal_count")),
        "storyline_count": _safe_int(summary.get("storyline_count")),
        "average_storyline_score": _safe_float(summary.get("average_storyline_score")),
        "company_intelligence_overall": _safe_float(scorecard.get("overall_score")),
        "product_value_overall": _safe_float(product_value.get("overall_score")),
        "calibration_ece": _safe_float(calibration.get("expected_calibration_error"), 1.0),
        "calibration_n": _safe_int(calibration.get("n")),
        "think_runs_failed": _safe_int(amplification.get("think_runs_failed")),
        "validation_error_count": _safe_int(
            amplification.get("validation_error_count")
        ),
        "snapshot_pending_triggers": _safe_int(amplification.get("pending_triggers")),
    }

    failures: list[str] = []
    if metrics["waves"] < 50:
        failures.append("expected at least 50 waves")
    if metrics["signals"] < 1000:
        failures.append("expected at least 1000 signals")
    if metrics["storyline_count"] < 9:
        failures.append("expected at least 9 storylines")
    if metrics["average_storyline_score"] < args.min_storyline_score:
        failures.append("average storyline score below threshold")
    if metrics["company_intelligence_overall"] < args.min_company_score:
        failures.append("company intelligence score below threshold")
    if metrics["product_value_overall"] < args.min_product_value_score:
        failures.append("product value score below threshold")
    if metrics["calibration_ece"] > args.max_calibration_ece:
        failures.append("calibration ECE above threshold")
    if metrics["calibration_n"] < args.min_calibration_n:
        failures.append("too few calibration samples")
    if metrics["think_runs_failed"] != 0:
        failures.append("Think failures present")
    if metrics["validation_error_count"] != 0:
        failures.append("validation errors present")

    pending_snapshot = metrics["snapshot_pending_triggers"]
    if pending_snapshot:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url or not tenant_id:
            failures.append("pending trigger snapshot lacks live drain proof")
        else:
            try:
                live = _query_live_queue_state(database_url, tenant_id)
            except Exception as exc:  # pragma: no cover - operational failure path.
                failures.append(f"live queue drain check failed: {exc}")
            else:
                metrics.update({f"live_{key}": value for key, value in live.items()})
                live_blockers = {
                    key: value for key, value in live.items() if value != 0
                }
                if live_blockers:
                    failures.append(f"live queue blockers remain: {live_blockers}")
                else:
                    metrics["live_queue_drain_overrode_snapshot"] = True

    status = FAIL if failures else PASS
    details = (
        "50-batch report meets beta production thresholds."
        if status == PASS else "; ".join(failures)
    )
    return GateResult(
        name="storyline_50_batch_report",
        status=status,
        details=details,
        metrics=metrics,
        artifacts={
            "run_dir": _relative(run_dir),
            "storyline_scores": _relative(summary_path),
            "run_summary": _relative(run_summary_path),
        },
    )


def _schema_drift_gate(args: argparse.Namespace) -> GateResult:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        return _manual_gate(
            "schema_drift_migration_rehearsal",
            "DATABASE_URL is not set; run against the migrated staging clone.",
            artifacts={"tool": "scripts/check_schema_drift.py"},
        )
    return _run_command_gate(
        "schema_drift_migration_rehearsal",
        _python_command("scripts/check_schema_drift.py"),
        details="Live schema matches the drift lock expectations.",
        timeout_s=args.command_timeout_s,
        env=_base_env(),
    )


def _pytest_gate(
    name: str,
    paths: list[str],
    *,
    details: str,
    args: argparse.Namespace,
    timeout_s: int | None = None,
) -> GateResult:
    if args.skip_pytest:
        return GateResult(name=name, status=SKIPPED, details="Skipped by --skip-pytest.")
    return _run_command_gate(
        name,
        _python_command("-m", "pytest", "-q", *paths),
        details=details,
        timeout_s=timeout_s or args.command_timeout_s,
        env=_base_env(),
    )


def _artifact_gate() -> GateResult:
    required = {
        "operational_runbook": REPO_ROOT / "docs/operations/production-readiness-gates.md",
        "observability_review": (
            REPO_ROOT / "docs/architecture/observability_production_readiness.md"
        ),
    }
    missing = [name for name, path in required.items() if not path.exists()]
    metrics = {name: path.exists() for name, path in required.items()}
    artifacts = {name: _relative(path) for name, path in required.items()}
    if missing:
        return GateResult(
            name="rollback_alerting_slo_artifacts",
            status=FAIL,
            details=f"Missing readiness artifacts: {', '.join(missing)}.",
            metrics=metrics,
            artifacts=artifacts,
        )
    runbook = required["operational_runbook"].read_text().lower()
    keywords = [
        "rollback",
        "slo",
        "alert",
        "migration rehearsal",
        "privacy",
    ]
    absent = [keyword for keyword in keywords if keyword not in runbook]
    if absent:
        return GateResult(
            name="rollback_alerting_slo_artifacts",
            status=FAIL,
            details=f"Operational runbook lacks sections: {', '.join(absent)}.",
            metrics=metrics,
            artifacts=artifacts,
        )
    return GateResult(
        name="rollback_alerting_slo_artifacts",
        status=PASS,
        details="Rollback, alert/SLO, migration, and privacy artifacts exist.",
        metrics=metrics,
        artifacts=artifacts,
    )


def _production_env_contract_gate(args: argparse.Namespace) -> GateResult:
    return _run_command_gate(
        "production_env_contract",
        _python_command("scripts/check_production_env_contract.py"),
        details=".env.production.example includes required fail-closed production keys.",
        timeout_s=min(args.command_timeout_s, 30),
        env=_base_env(),
    )


def _github_required_checks_gate(args: argparse.Namespace) -> GateResult:
    token_present = bool(os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN"))
    repo_present = bool(os.environ.get("GITHUB_REPOSITORY"))
    if not token_present or not repo_present:
        return _manual_gate(
            "github_main_required_checks",
            (
                "Run scripts/check_github_required_checks.py --live with "
                "GITHUB_REPOSITORY and a GitHub token that can read branch "
                "protection/rulesets."
            ),
            metrics={
                "github_token_present": token_present,
                "github_repository_present": repo_present,
            },
            artifacts={"policy": ".github/main-required-checks.json"},
        )
    return _run_command_gate(
        "github_main_required_checks",
        _python_command("scripts/check_github_required_checks.py", "--live"),
        details="GitHub main branch protection requires all checked-in CI gates.",
        timeout_s=min(args.command_timeout_s, 60),
        env=_base_env(),
        artifacts={"policy": ".github/main-required-checks.json"},
    )


def _collect_gates(args: argparse.Namespace) -> list[GateResult]:
    gates: list[GateResult] = [
        _artifact_gate(),
        _production_env_contract_gate(args),
        _github_required_checks_gate(args),
        _feedback_gap_gate(args),
        _storyline_report_gate(args),
        _schema_drift_gate(args),
        _pytest_gate(
            "shadow_cutover_unit_suite",
            [
                "services/app/webhooks/tests/test_router_shadow.py",
                "services/app/webhooks/tests/test_router_m5_cutover.py",
            ],
            details="Shadow and webhook cutover safety tests passed.",
            args=args,
            timeout_s=max(args.command_timeout_s, 360),
        ),
        _pytest_gate(
            "synthetic_load_generator_smoke",
            ["services/ingest/synthetic/tests/test_cutover_load.py"],
            details="Synthetic load generator smoke tests passed.",
            args=args,
        ),
        _pytest_gate(
            "calibration_monitoring_suite",
            ["services/workers/calibration_updater/tests/test_calibration.py"],
            details="Calibration updater and read-path tests passed.",
            args=args,
            timeout_s=max(args.command_timeout_s, 360),
        ),
        _pytest_gate(
            "permission_privacy_tenant_isolation_suite",
            [
                "tests/unit/sage/test_sage_edge_cases.py::"
                "test_affordance_writes_isolated_between_tenants",
                "tests/unit/sage/test_sage_edge_cases.py::"
                "test_discovery_shortcut_isolated_between_tenants",
                "tests/unit/sage/test_sage_edge_cases.py::"
                "test_negative_memory_isolated_between_tenants",
                "tests/unit/sage/test_sage_edge_cases.py::"
                "test_region_summary_isolated_between_tenants",
                "tests/unit/sage/test_sage_edge_cases.py::"
                "test_model_predictions_isolated_between_tenants",
                "services/domain/observations/tests/test_repo.py::"
                "test_same_external_id_is_deduped_per_tenant",
                "services/domain/observations/tests/test_repo.py::"
                "test_cascade_trace_respects_tenant",
                "services/domain/observations/tests/test_repo.py::"
                "test_tenant_isolation_get_by_id",
                "services/domain/observations/tests/test_repo.py::"
                "test_tenant_isolation_list_queries",
                "services/app/webhooks/tests/test_tenant_resolver_security.py",
            ],
            details="Tenant isolation and resolver security probes passed.",
            args=args,
            timeout_s=max(args.command_timeout_s, 420),
        ),
    ]
    return gates


def _overall_status(gates: list[GateResult]) -> tuple[str, bool, bool]:
    automated_gates = [
        gate for gate in gates
        if gate.status not in INCOMPLETE_STATUSES
    ]
    automated_passed = all(
        gate.status not in BLOCKING_STATUSES for gate in automated_gates
    )
    has_failures = any(gate.status in BLOCKING_STATUSES for gate in gates)
    has_incomplete = any(gate.status in INCOMPLETE_STATUSES for gate in gates)
    production_ready = not has_failures and not has_incomplete
    if has_failures:
        return "failed", automated_passed, production_ready
    if has_incomplete:
        return "incomplete_gates", automated_passed, production_ready
    if any(gate.status == WARN for gate in gates):
        return "passed_with_warnings", automated_passed, production_ready
    return "passed", automated_passed, production_ready


def _render_markdown(report: OperationalReadinessReport) -> str:
    lines = [
        "# Operational Production Readiness Gates",
        "",
        f"- Run: `{report.run_id}`",
        f"- Status: `{report.status}`",
        f"- Automated gates passed: `{str(report.automated_gates_passed).lower()}`",
        f"- Production ready: `{str(report.production_ready).lower()}`",
        f"- Elapsed seconds: `{report.elapsed_seconds:.3f}`",
        "",
        "| Gate | Status | Details |",
        "| --- | --- | --- |",
    ]
    for gate in report.gates:
        lines.append(
            f"| {gate.name} | {gate.status.upper()} | "
            f"{gate.details.replace('|', '/')[:240]} |"
        )
    blockers = [
        gate for gate in report.gates
        if gate.status in BLOCKING_STATUSES or gate.status in INCOMPLETE_STATUSES
    ]
    if blockers:
        lines.extend(["", "## Blockers"])
        for gate in blockers:
            lines.append(f"- **{gate.name}** ({gate.status}): {gate.details}")
    lines.extend(["", "## Metrics"])
    for gate in report.gates:
        lines.extend([
            "",
            f"### {gate.name}",
            "```json",
            json.dumps(gate.metrics, indent=2, sort_keys=True),
            "```",
        ])
    return "\n".join(lines) + "\n"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-id",
        default=_utc_run_id("operational-readiness"),
    )
    parser.add_argument(
        "--report-root",
        type=Path,
        default=DEFAULT_REPORT_ROOT,
    )
    parser.add_argument("--storyline-run-dir", type=Path)
    parser.add_argument("--command-timeout-s", type=int, default=240)
    parser.add_argument("--skip-gap-harness", action="store_true")
    parser.add_argument("--skip-pytest", action="store_true")
    parser.add_argument("--min-storyline-score", type=float, default=0.70)
    parser.add_argument("--min-company-score", type=float, default=0.85)
    parser.add_argument("--min-product-value-score", type=float, default=0.70)
    parser.add_argument("--max-calibration-ece", type=float, default=0.25)
    parser.add_argument("--min-calibration-n", type=int, default=100)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    load_dotenv(REPO_ROOT / ".env", override=False)
    args.report_root = args.report_root.resolve()
    if args.storyline_run_dir is not None:
        args.storyline_run_dir = args.storyline_run_dir.resolve()

    report_dir = args.report_root / args.run_id
    report_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    started_at = datetime.now(UTC).isoformat()

    gates = _collect_gates(args)
    status, automated_passed, production_ready = _overall_status(gates)
    elapsed = round(time.monotonic() - started, 3)
    report = OperationalReadinessReport(
        run_id=args.run_id,
        status=status,
        automated_gates_passed=automated_passed,
        production_ready=production_ready,
        started_at=started_at,
        elapsed_seconds=elapsed,
        report_dir=str(report_dir),
        gates=gates,
    )
    json_path = report_dir / "operational_readiness_report.json"
    md_path = report_dir / "operational_readiness_summary.md"
    _write_json(json_path, asdict(report))
    md_path.write_text(_render_markdown(report))
    print(f"[report] {json_path}", flush=True)
    print(f"[report] {md_path}", flush=True)
    print(
        json.dumps(
            {
                "run_id": report.run_id,
                "status": report.status,
                "automated_gates_passed": report.automated_gates_passed,
                "production_ready": report.production_ready,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    if any(gate.status == FAIL for gate in gates):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
