#!/usr/bin/env python3
"""Run the CAPABILITY-PLAN B3 Truss memory-compounding probe.

The probe compares:
  * truss_full: run 1 + run 2 in one memory stream,
  * truss_r2: run 2 only in a fresh stream,
  * an optional scoped RAG baseline.

The report highlights facts marked ``requires_run1_memory`` in the frozen fact
filter so a null result is interpretable.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_BENCHMARK = REPO_ROOT / "benchmarks" / "run_benchmark.py"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_root = args.out_root.resolve()
    arms = [
        ("full_memory", "truss_full", args.system),
        ("run2_only", "truss_r2", args.system),
    ]
    if args.include_rag_baseline:
        arms.append(("rag_baseline", "truss_r2", args.rag_system))

    report: dict[str, Any] = {
        "report_kind": "truss_memory_compounding_probe",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "arms": [],
    }
    for arm_name, benchmark, system in arms:
        arm_out = out_root / f"{run_id}-{arm_name}"
        cmd = _command(args, benchmark=benchmark, system=system, out=arm_out)
        print("$ " + " ".join(cmd), flush=True)
        if not args.dry_run:
            subprocess.run(cmd, cwd=REPO_ROOT, check=True)
        report["arms"].append({
            "name": arm_name,
            "benchmark": benchmark,
            "system": system,
            "out": str(arm_out),
            "metrics": _read_json(arm_out / "metrics_summary.json"),
            "run1_required": _run1_required_slice(
                arm_out / "results.jsonl",
                evidence_k=args.evidence_k,
            ),
        })
    report["comparison"] = _comparison(report["arms"], args.evidence_k)
    report_dir = out_root / f"{run_id}-memory-probe"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "truss_memory_probe.json").write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (report_dir / "truss_memory_probe.md").write_text(
        render_markdown(report),
        encoding="utf-8",
    )
    print(f"wrote {report_dir / 'truss_memory_probe.md'}")
    return 0


def _command(
    args: argparse.Namespace,
    *,
    benchmark: str,
    system: str,
    out: Path,
) -> list[str]:
    cmd = [
        sys.executable,
        str(RUN_BENCHMARK),
        "--benchmark",
        benchmark,
        "--data",
        str(args.data),
        "--truss-facts",
        str(args.truss_facts),
        "--system",
        system,
        "--top-k",
        str(args.top_k),
        "--evidence-k",
        str(args.evidence_k),
        "--out",
        str(out),
    ]
    if args.max_cases is not None:
        cmd.extend(["--max-cases", str(args.max_cases)])
    if args.embedding_mode:
        cmd.extend(["--embedding-mode", args.embedding_mode])
    if args.score_answers:
        cmd.append("--score-answers")
    if args.judge_answers:
        cmd.append("--judge-answers")
    if args.apply_migrations:
        cmd.append("--apply-migrations")
    if args.progress:
        cmd.append("--progress")
    return cmd


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _run1_required_slice(path: Path, *, evidence_k: int) -> dict[str, Any]:
    if not path.exists():
        return {"n": 0, "results": []}
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    selected = [
        row for row in rows
        if ((row.get("debug") or {}).get("query_metadata") or {}).get(
            "requires_run1_memory"
        )
        or _query_id_requires_run1(str(row.get("query_id") or ""))
    ]
    return {
        "n": len(selected),
        "evidence_recall_mean": _mean_metric(
            selected,
            f"evidence_recall_at_{evidence_k}",
        ),
        "accuracy_mean": _mean_metric(selected, "accuracy"),
        "results": selected,
    }


def _query_id_requires_run1(query_id: str) -> bool:
    return query_id in {
        "truss_fact_enterprise_checklist",
        "truss_fact_feature_flywheel",
    }


def _mean_metric(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [
        float((row.get("metrics") or {}).get(key))
        for row in rows
        if isinstance((row.get("metrics") or {}).get(key), (int, float))
    ]
    return round(sum(values) / len(values), 4) if values else None


def _comparison(arms: list[dict[str, Any]], evidence_k: int) -> dict[str, Any]:
    by_name = {arm["name"]: arm for arm in arms}
    full = by_name.get("full_memory", {})
    r2 = by_name.get("run2_only", {})
    key = f"evidence_recall_at_{evidence_k}"
    return {
        "overall_recall_delta_full_minus_run2": _delta(
            (r2.get("metrics") or {}).get(key),
            (full.get("metrics") or {}).get(key),
        ),
        "run1_required_recall_delta_full_minus_run2": _delta(
            (r2.get("run1_required") or {}).get("evidence_recall_mean"),
            (full.get("run1_required") or {}).get("evidence_recall_mean"),
        ),
    }


def _delta(left: Any, right: Any) -> float | None:
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        return None
    return round(float(right) - float(left), 6)


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Truss Memory-Compounding Probe",
        "",
        f"- Run: `{report.get('run_id')}`",
        "",
        "| Arm | Benchmark | System | Queries | Recall | Run1-required n | Run1-required recall |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for arm in report.get("arms") or []:
        metrics = arm.get("metrics") or {}
        recall_key = next((key for key in metrics if key.startswith("evidence_recall_at_")), "")
        run1 = arm.get("run1_required") or {}
        lines.append(
            "| {name} | {benchmark} | {system} | {queries} | {recall} | {n} | {run1_recall} |".format(
                name=arm.get("name"),
                benchmark=arm.get("benchmark"),
                system=arm.get("system"),
                queries=metrics.get("queries"),
                recall=_fmt(metrics.get(recall_key)),
                n=run1.get("n"),
                run1_recall=_fmt(run1.get("evidence_recall_mean")),
            )
        )
    lines.extend(["", "## Comparison", "```json"])
    lines.append(json.dumps(report.get("comparison") or {}, indent=2, sort_keys=True))
    lines.extend(["```", ""])
    return "\n".join(lines)


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=REPO_ROOT)
    parser.add_argument(
        "--truss-facts",
        type=Path,
        default=REPO_ROOT / "benchmarks" / "truss_signal_derivable_facts.json",
    )
    parser.add_argument("--system", default="bm25_session")
    parser.add_argument("--rag-system", default="bm25_session")
    parser.add_argument("--include-rag-baseline", action="store_true")
    parser.add_argument("--embedding-mode", choices=["hash", "provider", "ollama", "openai"], default=None)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--evidence-k", type=int, default=10)
    parser.add_argument("--max-cases", type=int, default=None)
    parser.add_argument("--score-answers", action="store_true")
    parser.add_argument("--judge-answers", action="store_true")
    parser.add_argument("--apply-migrations", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--out-root", type=Path, default=REPO_ROOT / "benchmarks" / "reports" / "generated" / "truss_memory")
    parser.add_argument("--run-id")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
