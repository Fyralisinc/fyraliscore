#!/usr/bin/env python3
"""Run the storyline variance-band instrument from CAPABILITY-PLAN A3.

The script produces N identical 225-signal storyline runs, with cache bypass
enabled by default, then invokes the benchmark's built-in variance-report mode.
It is intentionally a thin orchestration layer around
``scripts/run_storyline_batch_benchmark.py`` so the scored artifact format stays
single-sourced.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
STORYLINE_SCRIPT = REPO_ROOT / "scripts" / "run_storyline_batch_benchmark.py"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report_root = args.report_root.resolve()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_prefix = args.run_prefix or f"storyline-variance-{stamp}"
    arms = ["cache_off"]
    if args.include_cache_on:
        arms.append("cache_on")

    manifest: dict[str, Any] = {
        "report_kind": "storyline_variance_band_runner",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_prefix": run_prefix,
        "report_root": str(report_root),
        "runs_per_arm": args.runs,
        "arms": {},
    }

    for arm in arms:
        run_ids: list[str] = []
        for index in range(1, args.runs + 1):
            run_id = f"{run_prefix}-{arm}-{index:02d}"
            run_ids.append(run_id)
            cmd = _storyline_run_command(args, run_id)
            env = os.environ.copy()
            if arm == "cache_off":
                env["LLM_CACHE_BYPASS"] = "1"
            else:
                env.pop("LLM_CACHE_BYPASS", None)
            _run(cmd, env=env, dry_run=args.dry_run)

        report_id = f"{run_prefix}-{arm}-variance"
        report_cmd = [
            sys.executable,
            str(STORYLINE_SCRIPT),
            "--mode",
            "variance-report",
            "--report-root",
            str(report_root),
            "--run-id",
            report_id,
            "--variance-run-ids",
            *run_ids,
        ]
        _run(report_cmd, env=os.environ.copy(), dry_run=args.dry_run)
        manifest["arms"][arm] = {
            "run_ids": run_ids,
            "variance_report_id": report_id,
            "cache_bypass": arm == "cache_off",
        }

    output_dir = report_root / f"{run_prefix}-runner"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "variance_band_runner_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"wrote {manifest_path}")
    return 0


def _storyline_run_command(args: argparse.Namespace, run_id: str) -> list[str]:
    cmd = [
        sys.executable,
        str(STORYLINE_SCRIPT),
        "--mode",
        "run",
        "--run-id",
        run_id,
        "--report-root",
        str(args.report_root),
        "--signals-per-storyline",
        str(args.signals_per_storyline),
        "--future-validation-signals-per-storyline",
        str(args.future_validation_signals_per_storyline),
        "--noise-signals",
        str(args.noise_signals),
        "--target-t1-batches",
        str(args.target_t1_batches),
        "--run-timeout",
        str(args.run_timeout),
        "--post-commit-timeout",
        str(args.post_commit_timeout),
    ]
    if args.cleanup:
        cmd.append("--cleanup")
    if args.skip_topology_optimizer:
        cmd.append("--skip-topology-optimizer")
    if args.enable_thesis_judge:
        cmd.append("--enable-thesis-judge")
        cmd.extend(["--thesis-judge-limit", str(args.thesis_judge_limit)])
    for extra in args.extra_arg or []:
        cmd.append(extra)
    return cmd


def _run(cmd: list[str], *, env: dict[str, str], dry_run: bool) -> None:
    print("$ " + " ".join(cmd), flush=True)
    if dry_run:
        return
    subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--run-prefix")
    parser.add_argument(
        "--report-root",
        type=Path,
        default=REPO_ROOT / "tests" / "real_llm" / "reports" / "runs",
    )
    parser.add_argument("--signals-per-storyline", type=int, default=25)
    parser.add_argument("--future-validation-signals-per-storyline", type=int, default=0)
    parser.add_argument("--noise-signals", type=int, default=0)
    parser.add_argument(
        "--target-t1-batches",
        type=int,
        default=0,
        help="0 means the classic 225-signal scorecard layout.",
    )
    parser.add_argument("--run-timeout", type=float, default=900.0)
    parser.add_argument("--post-commit-timeout", type=int, default=600)
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--skip-topology-optimizer", action="store_true")
    parser.add_argument("--include-cache-on", action="store_true")
    parser.add_argument("--enable-thesis-judge", action="store_true")
    parser.add_argument("--thesis-judge-limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--extra-arg",
        action="append",
        help="Extra raw argument forwarded to run_storyline_batch_benchmark.py.",
    )
    args = parser.parse_args(argv)
    if args.runs < 2:
        raise SystemExit("--runs must be >= 2")
    return args


if __name__ == "__main__":
    raise SystemExit(main())
