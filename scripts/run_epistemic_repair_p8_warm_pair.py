#!/usr/bin/env python3
"""Run repeated warm P8 concurrency-1 versus concurrency-20 diagnostics."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p8_scale_runner import run_warm_pair_diagnostic


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    result = await run_warm_pair_diagnostic(
        args.database_url, batch_size=args.batch_size, memory_horizon_batches=args.horizon,
        repetitions=args.repetitions, pool_size=args.pool_size,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(asdict(result), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"repetitions={result.repetitions} barrier_ratio={result.barrier_ratio_p95:.3f} "
        f"end_to_end_ratio={result.end_to_end_ratio_p95:.3f} diagnosis={result.diagnosis} output={args.output}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--horizon", type=int, default=50)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--pool-size", type=int, default=20)
    parser.add_argument("--output", type=Path, default=Path("docs/plans/epistemic-repair/p8/p8-warm-pair.json"))
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
