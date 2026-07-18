#!/usr/bin/env python3
"""Run the sealed P2 truth-kernel proof against PostgreSQL and write JSON."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import sys

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p2_exit import write_p2_exit_artifact  # noqa: E402
from lib.evaluation.epistemic_repair.p2_runner import run_p2_truth_kernel  # noqa: E402


async def _run(dsn: str, output: Path) -> None:
    conn = await asyncpg.connect(dsn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        report = await run_p2_truth_kernel(conn, concurrency_dsn=dsn)
        write_p2_exit_artifact(report, output)
    finally:
        await transaction.rollback()
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument(
        "--output", type=Path,
        default=Path("artifacts/epistemic-repair/p9/p2.normalized.json"),
    )
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or DATABASE_URL is required")
    asyncio.run(_run(args.dsn, args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
