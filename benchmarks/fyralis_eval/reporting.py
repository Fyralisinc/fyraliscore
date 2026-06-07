"""Report generation for benchmark runs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from benchmarks.fyralis_eval.evaluator import EvaluationResult


@dataclass(frozen=True)
class BenchmarkArtifacts:
    output_dir: Path
    results_jsonl: Path
    metrics_summary_json: Path
    run_config_json: Path
    trace_sample_jsonl: Path
    benchmark_report_md: Path


def write_run_artifacts(
    *,
    output_dir: Path,
    run_config: dict[str, Any],
    results: list[EvaluationResult],
    retrieval_traces: list[dict[str, Any]],
    metrics_summary: dict[str, Any],
) -> BenchmarkArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    results_path = output_dir / "results.jsonl"
    metrics_path = output_dir / "metrics_summary.json"
    config_path = output_dir / "run_config.json"
    trace_path = output_dir / "trace_sample.jsonl"
    report_path = output_dir / "benchmark_report.md"

    results_path.write_text(
        "".join(json.dumps(result.to_json(), sort_keys=True) + "\n" for result in results),
        encoding="utf-8",
    )
    metrics_path.write_text(json.dumps(metrics_summary, indent=2, sort_keys=True), encoding="utf-8")
    config_path.write_text(json.dumps(run_config, indent=2, sort_keys=True), encoding="utf-8")
    trace_path.write_text(
        "".join(json.dumps(trace, sort_keys=True) + "\n" for trace in retrieval_traces[:20]),
        encoding="utf-8",
    )
    report_path.write_text(
        render_markdown_report(run_config=run_config, results=results, metrics_summary=metrics_summary),
        encoding="utf-8",
    )
    return BenchmarkArtifacts(
        output_dir=output_dir,
        results_jsonl=results_path,
        metrics_summary_json=metrics_path,
        run_config_json=config_path,
        trace_sample_jsonl=trace_path,
        benchmark_report_md=report_path,
    )


def render_markdown_report(
    *,
    run_config: dict[str, Any],
    results: list[EvaluationResult],
    metrics_summary: dict[str, Any],
) -> str:
    lines = [
        "# Fyralis Benchmark Report",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Summary",
        "",
        f"- Benchmark: {run_config.get('benchmark')}",
        f"- System: {run_config.get('system_name')}",
        f"- Queries: {len(results)}",
        f"- Accuracy: {_fmt(metrics_summary.get('accuracy'))}",
        f"- Evidence Recall: {_fmt(_first_prefixed_metric(metrics_summary, 'evidence_recall_at_'))}",
        f"- Evidence Precision: {_fmt(_first_prefixed_metric(metrics_summary, 'evidence_precision_at_'))}",
        f"- Mean token cost: {_fmt(metrics_summary.get('token_cost'))}",
        f"- Mean latency ms: {_fmt(metrics_summary.get('latency_ms'))}",
        "",
        "## Query Results",
        "",
        "| Query | Answer | Accuracy | Evidence Recall | Tokens | Latency ms |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for result in results:
        metrics = result.metrics
        answer = result.answer.replace("|", "\\|")
        lines.append(
            f"| {result.query_id} | {answer} | "
            f"{_fmt(metrics.get('accuracy'))} | "
            f"{_fmt(_first_prefixed_metric(metrics, 'evidence_recall_at_'))} | "
            f"{_fmt(metrics.get('token_cost'))} | "
            f"{_fmt(metrics.get('latency_ms'))} |"
        )
    lines.append("")
    return "\n".join(lines)


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _first_prefixed_metric(metrics: dict[str, Any], prefix: str) -> Any:
    for key in sorted(metrics):
        if key.startswith(prefix):
            return metrics[key]
    return None
