#!/usr/bin/env python3
"""Offline retrieval tuning grid for CAPABILITY-PLAN C6.

Runs benchmark arms while varying RETRIEVAL_RRF_K,
RETRIEVAL_TRIGGER_WEIGHTS_JSON, and RETRIEVAL_RECENCY_DECAY_HALF_LIFE_DAYS.
Use with a Fyralis DB-backed system; bm25/lexical systems do not consume these
runtime knobs.
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_BENCHMARK = REPO_ROOT / "benchmarks" / "run_benchmark.py"


WEIGHT_PRESETS: dict[str, str] = {
    "default": "",
    "semantic_heavy_t1": json.dumps({"T1": {"A": 0.18, "B": 0.56, "C": 0.12, "G": 0.14}}),
    "graph_heavy_t2": json.dumps({"T2": {"A": 0.12, "B": 0.12, "D": 0.10, "G": 0.66}}),
    "temporal_heavy_t1": json.dumps({"T1": {"A": 0.24, "B": 0.24, "C": 0.34, "G": 0.18}}),
}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_root = args.out_root.resolve()
    arms: list[dict[str, Any]] = []
    for rrf_k, half_life, preset in itertools.product(
        args.rrf_k,
        args.recency_half_life_days,
        args.weight_preset,
    ):
        arm_name = f"k{rrf_k}-hl{_label_num(half_life)}-{preset}"
        arm_out = out_root / f"{run_id}-{arm_name}"
        env = os.environ.copy()
        env["RETRIEVAL_RRF_K"] = str(rrf_k)
        env["RETRIEVAL_RECENCY_DECAY_HALF_LIFE_DAYS"] = str(half_life)
        preset_json = WEIGHT_PRESETS[preset]
        if preset_json:
            env["RETRIEVAL_TRIGGER_WEIGHTS_JSON"] = preset_json
        else:
            env.pop("RETRIEVAL_TRIGGER_WEIGHTS_JSON", None)
        cmd = _command(args, arm_out)
        print("$ " + " ".join(cmd), flush=True)
        if not args.dry_run:
            subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=True)
        arms.append({
            "name": arm_name,
            "rrf_k": rrf_k,
            "recency_half_life_days": half_life,
            "weight_preset": preset,
            "weight_override": preset_json,
            "out": str(arm_out),
            "metrics": _read_metrics(arm_out / "metrics_summary.json"),
        })
    report = {
        "report_kind": "retrieval_tuning_grid",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "benchmark": args.benchmark,
        "system": args.system,
        "arms": arms,
        "leaderboard": _leaderboard(arms, evidence_k=args.evidence_k),
    }
    report_dir = out_root / f"{run_id}-grid"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "retrieval_tuning_grid.json").write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (report_dir / "retrieval_tuning_grid.md").write_text(
        render_markdown(report),
        encoding="utf-8",
    )
    print(f"wrote {report_dir / 'retrieval_tuning_grid.md'}")
    return 0


def _command(args: argparse.Namespace, out: Path) -> list[str]:
    cmd = [
        sys.executable,
        str(RUN_BENCHMARK),
        "--benchmark",
        args.benchmark,
        "--system",
        args.system,
        "--top-k",
        str(args.top_k),
        "--evidence-k",
        str(args.evidence_k),
        "--out",
        str(out),
    ]
    if args.data is not None:
        cmd.extend(["--data", str(args.data)])
    if args.max_cases is not None:
        cmd.extend(["--max-cases", str(args.max_cases)])
    if args.embedding_mode:
        cmd.extend(["--embedding-mode", args.embedding_mode])
    if args.truss_facts:
        cmd.extend(["--truss-facts", str(args.truss_facts)])
    if args.apply_migrations:
        cmd.append("--apply-migrations")
    if args.progress:
        cmd.append("--progress")
    return cmd


def _read_metrics(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _leaderboard(arms: list[dict[str, Any]], *, evidence_k: int) -> list[dict[str, Any]]:
    recall_key = f"evidence_recall_at_{evidence_k}"
    precision_key = f"evidence_precision_at_{evidence_k}"
    rows = []
    for arm in arms:
        metrics = arm.get("metrics") or {}
        rows.append({
            "name": arm["name"],
            "recall": metrics.get(recall_key),
            "precision": metrics.get(precision_key),
            "latency_ms": metrics.get("latency_ms"),
            "queries": metrics.get("queries"),
        })
    return sorted(
        rows,
        key=lambda row: (
            row["recall"] if isinstance(row["recall"], (int, float)) else -1,
            row["precision"] if isinstance(row["precision"], (int, float)) else -1,
            -(row["latency_ms"] if isinstance(row["latency_ms"], (int, float)) else 0),
        ),
        reverse=True,
    )


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Retrieval Tuning Grid",
        "",
        f"- Run: `{report.get('run_id')}`",
        f"- Benchmark: {report.get('benchmark')}",
        f"- System: {report.get('system')}",
        "",
        "| Arm | Recall | Precision | Latency ms | Queries |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in report.get("leaderboard") or []:
        lines.append(
            f"| {row.get('name')} | {_fmt(row.get('recall'))} | "
            f"{_fmt(row.get('precision'))} | {_fmt(row.get('latency_ms'))} | "
            f"{row.get('queries')} |"
        )
    lines.append("")
    return "\n".join(lines)


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def _label_num(value: float) -> str:
    return str(value).replace(".", "p")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", default="truss_full")
    parser.add_argument("--data", type=Path, default=REPO_ROOT)
    parser.add_argument("--truss-facts", type=Path, default=REPO_ROOT / "benchmarks" / "truss_signal_derivable_facts.json")
    parser.add_argument("--system", default="fyralis_current")
    parser.add_argument("--embedding-mode", choices=["hash", "provider", "ollama", "openai"], default="hash")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--evidence-k", type=int, default=10)
    parser.add_argument("--max-cases", type=int, default=None)
    parser.add_argument("--rrf-k", type=int, nargs="+", default=[40, 60, 90])
    parser.add_argument("--recency-half-life-days", type=float, nargs="+", default=[0.0, 14.0, 45.0])
    parser.add_argument("--weight-preset", choices=sorted(WEIGHT_PRESETS), nargs="+", default=["default"])
    parser.add_argument("--apply-migrations", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--out-root", type=Path, default=REPO_ROOT / "benchmarks" / "reports" / "generated" / "retrieval_tuning")
    parser.add_argument("--run-id")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
