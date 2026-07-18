#!/usr/bin/env python3
"""Run the rollback-only PostgreSQL P4 online-learning evaluator."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
from pathlib import Path

import asyncpg

from services.evaluation.epistemic_repair.p4_runner import run_p4_online_loop
from lib.evaluation.epistemic_repair.p4_p9 import build_p4_p9_sidecar


async def _run(dsn: str) -> dict:
    conn = await asyncpg.connect(dsn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        return await run_p4_online_loop(conn)
    finally:
        await transaction.rollback()
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/plans/epistemic-repair/p4/epistemic-repair-p4-online-learning-v1.json"),
    )
    parser.add_argument("--p9-output", type=Path)
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or DATABASE_URL is required")
    report = asyncio.run(_run(args.dsn))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n")
    if args.p9_output is not None:
        root = Path(__file__).resolve().parents[1]
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        clean = not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True).strip()
        sidecar = build_p4_p9_sidecar(
            report_path=args.output, commit=commit, worktree_clean=clean,
        )
        args.p9_output.parent.mkdir(parents=True, exist_ok=True)
        args.p9_output.write_text(json.dumps(sidecar, indent=2, sort_keys=True, default=str) + "\n")
    print(args.output)
    return 0 if report["phase_exit_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
