"""Markdown report generator for real-LLM test runs and the flake-rate dashboard."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from tests.real_llm.infrastructure import flake_tracker

_REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"
_RUNS_DIR = _REPORTS_DIR / "runs"
_DASHBOARD_FILE = _REPORTS_DIR / "dashboard.md"


def _ensure_dir(path: Path) -> None:
    """Create a directory if it does not exist."""
    path.mkdir(parents=True, exist_ok=True)


def _fmt(value, default="-"):
    """Render a value for table cells, falling back to a placeholder when missing."""
    if value is None:
        return default
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def generate_run_report(run_dir: Path, results: list[dict]) -> Path:
    """Write a markdown report for one suite run and return the report path."""
    run_dir = Path(run_dir)
    _ensure_dir(run_dir)
    report_path = run_dir / "report.md"

    total = len(results)
    passed = sum(1 for r in results if r.get("outcome") == "pass")
    failed = total - passed
    flaky_runs = sum(
        1 for r in results
        if r.get("passes") is not None and r.get("total") is not None and r["passes"] < r["total"]
    )
    flake_rate = (flaky_runs / total) if total else 0.0

    lines: list[str] = []
    lines.append("# Real-LLM Run Report")
    lines.append("")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total tests: {total}")
    lines.append(f"- Passed: {passed}")
    lines.append(f"- Failed: {failed}")
    lines.append(f"- Flake rate (this run): {flake_rate:.2%}")
    lines.append("")
    lines.append("## Per-test results")
    lines.append("")
    lines.append("| Test | Outcome | Passes/Total | Time (s) | Cost (USD) |")
    lines.append("| --- | --- | --- | --- | --- |")
    for r in results:
        passes = r.get("passes")
        total_attempts = r.get("total")
        ratio = f"{passes}/{total_attempts}" if passes is not None and total_attempts is not None else "-"
        lines.append(
            f"| {_fmt(r.get('name'))} "
            f"| {_fmt(r.get('outcome'))} "
            f"| {ratio} "
            f"| {_fmt(r.get('time_seconds'))} "
            f"| {_fmt(r.get('cost_usd'))} |"
        )
    lines.append("")
    lines.append("## Flake-rate trend (top 5)")
    lines.append("")
    lines.extend(_flakiest_table(top=5))
    lines.append("")

    report_path.write_text("\n".join(lines))
    return report_path


def update_flake_dashboard() -> Path:
    """Refresh tests/real_llm/reports/dashboard.md with the latest flake-rate trend."""
    _ensure_dir(_REPORTS_DIR)
    lines: list[str] = []
    lines.append("# Real-LLM Flake Dashboard")
    lines.append("")
    lines.append(f"Updated: {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append("## Flake-rate trend (top 5)")
    lines.append("")
    lines.extend(_flakiest_table(top=5))
    lines.append("")
    _DASHBOARD_FILE.write_text("\n".join(lines))
    return _DASHBOARD_FILE


def _flakiest_table(top: int) -> list[str]:
    """Render a markdown table of the flakiest tests from history."""
    summary = flake_tracker.summary()
    if not summary:
        return ["(no history yet)"]
    ordered = sorted(summary.items(), key=lambda kv: kv[1].get("flake_rate", 0.0), reverse=True)
    rows = ordered[:top]
    out = [
        "| Test | Flake rate | Recent outcomes |",
        "| --- | --- | --- |",
    ]
    for name, stats in rows:
        recent = ",".join(stats.get("recent_outcomes", [])) or "-"
        out.append(f"| {name} | {stats.get('flake_rate', 0.0):.2%} | {recent} |")
    return out
