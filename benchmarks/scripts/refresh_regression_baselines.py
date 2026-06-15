#!/usr/bin/env python3
"""Refresh retained benchmark baselines for CAPABILITY-PLAN A4.

Runs offline retrieval lanes for datasets that are present locally and copies
their ``metrics_summary.json`` into ``benchmarks/baselines/<name>/``. Missing
datasets are reported as skipped so a partial local checkout remains usable.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Target:
    name: str
    benchmark: str
    data: Path | None
    system: str = "bm25_session"
    max_cases: int | None = None


DEFAULT_TARGETS = [
    Target("stress10_bm25", "stress10", None),
    Target(
        "longmemeval_s_bm25",
        "longmemeval",
        REPO_ROOT / "benchmarks" / "datasets" / "raw" / "longmemeval_s_cleaned.json",
        max_cases=500,
    ),
    Target(
        "lme_v2_small_bm25",
        "lme_v2",
        REPO_ROOT / "benchmarks" / "datasets" / "raw" / "longmemeval-v2",
        # LME-v2 preprocessing renders very large UI trajectories. Keep this
        # retained baseline as a smoke canary until the adapter is lazy-indexed.
        max_cases=5,
    ),
    Target(
        "hotpotqa_bm25",
        "hotpotqa",
        REPO_ROOT / "benchmarks" / "datasets" / "raw" / "hotpotqa_distractor_validation.json",
        max_cases=500,
    ),
    Target(
        "halumem_bm25",
        "halumem",
        REPO_ROOT / "benchmarks" / "datasets" / "raw" / "HaluMem-Medium.jsonl",
        max_cases=500,
    ),
]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report: dict[str, Any] = {
        "report_kind": "regression_baseline_refresh",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "targets": [],
    }
    for target in DEFAULT_TARGETS:
        if args.only and target.name not in args.only:
            continue
        if target.data is not None and not target.data.exists():
            report["targets"].append({
                "name": target.name,
                "status": "skipped",
                "reason": f"missing data: {target.data}",
            })
            continue
        out_dir = args.generated_root / target.name
        cmd = _command(target, out_dir)
        print("$ " + " ".join(cmd), flush=True)
        if not args.dry_run:
            subprocess.run(cmd, cwd=REPO_ROOT, check=True)
            baseline_dir = args.baseline_root / target.name
            baseline_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(out_dir / "metrics_summary.json", baseline_dir / "metrics_summary.json")
            shutil.copy2(out_dir / "run_config.json", baseline_dir / "run_config.json")
        report["targets"].append({
            "name": target.name,
            "status": "refreshed" if not args.dry_run else "dry_run",
            "generated_out": str(out_dir),
            "baseline": str(args.baseline_root / target.name),
        })
    args.generated_root.mkdir(parents=True, exist_ok=True)
    report_path = args.generated_root / "baseline_refresh_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(f"wrote {report_path}")
    return 0


def _command(target: Target, out_dir: Path) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "benchmarks.run_benchmark",
        "--benchmark",
        target.benchmark,
        "--system",
        target.system,
        "--out",
        str(out_dir),
    ]
    if target.data is not None:
        cmd.extend(["--data", str(target.data)])
    if target.max_cases is not None:
        cmd.extend(["--max-cases", str(target.max_cases)])
    return cmd


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline-root",
        type=Path,
        default=REPO_ROOT / "benchmarks" / "baselines",
    )
    parser.add_argument(
        "--generated-root",
        type=Path,
        default=REPO_ROOT / "benchmarks" / "reports" / "generated" / "baseline_refresh",
    )
    parser.add_argument("--only", action="append")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
