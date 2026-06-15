#!/usr/bin/env python3
"""Run the CAPABILITY-PLAN A5 benchmark A/B matrix.

Two decisions are priced:
  1. Fyralis DB retrieval with hash vs Ollama embeddings on LongMemEval-V2.
  2. Fyralis base DB reader vs SAGE reader on the same lane.

The script writes a compact matrix report next to the benchmark artifacts. It
does not interpret a winner beyond reporting deltas; confidence intervals and
cost should be considered by the human release owner.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_BENCHMARK = REPO_ROOT / "benchmarks" / "run_benchmark.py"


@dataclass(frozen=True)
class Arm:
    name: str
    system: str
    embedding_mode: str | None = None
    graph_enrichment: bool = False


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out_root = args.out_root.resolve()
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    arms: list[Arm] = []
    if not args.skip_embedding_ab:
        arms.extend([
            Arm("embedding_hash", "fyralis_current", "hash"),
            Arm("embedding_ollama", "fyralis_current", "ollama"),
        ])
    if not args.skip_sage_ab:
        arms.extend([
            Arm("base_reader", "fyralis_current", args.embedding_mode),
            Arm("sage_reader", "fyralis_sage_reader", args.embedding_mode),
        ])

    report: dict[str, Any] = {
        "report_kind": "capability_ab_matrix",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "benchmark": "longmemeval_v2",
        "data": str(args.lme_v2_data),
        "max_cases": args.max_cases,
        "arms": [],
    }

    for arm in arms:
        arm_out = out_root / f"{run_id}-{arm.name}"
        cmd = _command(args, arm, arm_out)
        print("$ " + " ".join(cmd), flush=True)
        if not args.dry_run:
            subprocess.run(cmd, cwd=REPO_ROOT, check=True)
        metrics = _read_metrics(arm_out / "metrics_summary.json")
        report["arms"].append({
            "name": arm.name,
            "system": arm.system,
            "embedding_mode": arm.embedding_mode,
            "out": str(arm_out),
            "metrics": metrics,
        })

    report["comparisons"] = _comparisons(report["arms"], evidence_k=args.evidence_k)
    report_dir = out_root / f"{run_id}-matrix"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "capability_ab_matrix.json").write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (report_dir / "capability_ab_matrix.md").write_text(
        _render_markdown(report),
        encoding="utf-8",
    )
    print(f"wrote {report_dir / 'capability_ab_matrix.md'}")
    return 0


def _command(args: argparse.Namespace, arm: Arm, out: Path) -> list[str]:
    cmd = [
        sys.executable,
        str(RUN_BENCHMARK),
        "--benchmark",
        "lme_v2",
        "--data",
        str(args.lme_v2_data),
        "--system",
        arm.system,
        "--top-k",
        str(args.top_k),
        "--evidence-k",
        str(args.evidence_k),
        "--out",
        str(out),
    ]
    if args.max_cases is not None:
        cmd.extend(["--max-cases", str(args.max_cases)])
    if arm.embedding_mode:
        cmd.extend(["--embedding-mode", arm.embedding_mode])
    if arm.graph_enrichment:
        cmd.append("--graph-enrichment")
    if args.apply_migrations:
        cmd.append("--apply-migrations")
    if args.progress:
        cmd.append("--progress")
    return cmd


def _read_metrics(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _comparisons(arms: list[dict[str, Any]], *, evidence_k: int) -> dict[str, Any]:
    by_name = {arm["name"]: arm for arm in arms}
    recall_key = f"evidence_recall_at_{evidence_k}"
    precision_key = f"evidence_precision_at_{evidence_k}"
    specs = {
        "hash_vs_ollama": ("embedding_hash", "embedding_ollama"),
        "base_vs_sage_reader": ("base_reader", "sage_reader"),
    }
    out: dict[str, Any] = {}
    for name, (left, right) in specs.items():
        if left not in by_name or right not in by_name:
            continue
        left_metrics = by_name[left].get("metrics") or {}
        right_metrics = by_name[right].get("metrics") or {}
        out[name] = {
            "left": left,
            "right": right,
            "metrics": {
                key: {
                    "left": left_metrics.get(key),
                    "right": right_metrics.get(key),
                    "delta": _delta(left_metrics.get(key), right_metrics.get(key)),
                }
                for key in (recall_key, precision_key, "latency_ms", "queries")
            },
        }
    return out


def _delta(left: Any, right: Any) -> float | None:
    if not isinstance(left, (int, float)) or not isinstance(right, (int, float)):
        return None
    return round(float(right) - float(left), 6)


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Capability A/B Matrix",
        "",
        f"- Run: `{report.get('run_id')}`",
        f"- Benchmark: {report.get('benchmark')}",
        f"- Max cases: {report.get('max_cases')}",
        "",
        "| Arm | System | Embeddings | Queries | Recall | Precision | Latency ms |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for arm in report.get("arms") or []:
        metrics = arm.get("metrics") or {}
        recall_key = next((k for k in metrics if k.startswith("evidence_recall_at_")), "")
        precision_key = next((k for k in metrics if k.startswith("evidence_precision_at_")), "")
        lines.append(
            "| {name} | {system} | {embed} | {queries} | {recall} | {precision} | {latency} |".format(
                name=arm.get("name"),
                system=arm.get("system"),
                embed=arm.get("embedding_mode") or "-",
                queries=metrics.get("queries"),
                recall=_fmt(metrics.get(recall_key)),
                precision=_fmt(metrics.get(precision_key)),
                latency=_fmt(metrics.get("latency_ms")),
            )
        )
    lines.extend(["", "## Comparisons", "```json"])
    lines.append(json.dumps(report.get("comparisons") or {}, indent=2, sort_keys=True))
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
    parser.add_argument("--lme-v2-data", type=Path, required=True)
    parser.add_argument("--max-cases", type=int, default=200)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--evidence-k", type=int, default=10)
    parser.add_argument("--embedding-mode", choices=["hash", "ollama", "provider", "openai"], default="hash")
    parser.add_argument(
        "--out-root",
        type=Path,
        default=REPO_ROOT / "benchmarks" / "reports" / "generated" / "capability_ab",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--apply-migrations", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--skip-embedding-ab", action="store_true")
    parser.add_argument("--skip-sage-ab", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if not args.lme_v2_data.exists() and not args.dry_run:
        raise SystemExit(f"--lme-v2-data does not exist: {args.lme_v2_data}")
    return args


if __name__ == "__main__":
    raise SystemExit(main())
