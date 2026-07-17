#!/usr/bin/env python3
"""Run the sealed P6 stream through production T1 Think in 12 intact batches."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.evaluation.epistemic_repair.p6_think_runner import run_p6_production_think


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    artifact = await run_p6_production_think(
        database_url=args.database_url, population=build_p6_population(),
        checkpoint_path=args.output,
        per_batch_timeout_s=args.batch_timeout,
        total_timeout_s=args.total_timeout,
        max_batches=args.max_batches,
    )
    print(f"complete={str(artifact['complete']).lower()} batches={artifact['completed_batches']} terminal_reason={artifact['terminal_reason']} output={args.output}")
    return 0 if artifact["complete"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, default=Path("/tmp/p6-think.json"))
    parser.add_argument("--batch-timeout", type=float, default=180.0)
    parser.add_argument("--total-timeout", type=float, default=1800.0)
    parser.add_argument("--max-batches", type=int, default=12)
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
