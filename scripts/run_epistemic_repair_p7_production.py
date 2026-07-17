#!/usr/bin/env python3
"""Run P7's five isolated arms through production Think in 12 batches."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p6_think_runner import _write_checkpoint
from lib.evaluation.epistemic_repair.p7_production_runner import (
    run_p7_production_worlds,
    seal_execution_stream,
)
from lib.evaluation.epistemic_repair.p7_population import (
    P7_INITIAL_WORLD_COUNT,
    build_p7_population,
)
from lib.evaluation.epistemic_repair.p7_real_runner import _variant_population


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    preregistered = build_p7_population()
    variants = tuple(
        (world.world_id, _variant_population(world, index))
        for index, world in enumerate(preregistered.worlds[:P7_INITIAL_WORLD_COUNT])
    )
    streams = tuple(
        (world_id, seal_execution_stream(population))
        for world_id, population in variants
    )
    del variants, preregistered
    artifact = await run_p7_production_worlds(
        database_url=args.database_url,
        worlds=streams,
        per_batch_timeout_s=args.batch_timeout,
    )
    _write_checkpoint(args.output, artifact)
    print(
        f"complete={str(artifact['complete']).lower()} "
        f"worlds={artifact['world_count']} arms={artifact['arm_execution_count']} "
        f"output={args.output}"
    )
    return 0 if artifact["complete"] else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, default=Path("/tmp/p7-production.json"))
    parser.add_argument("--batch-timeout", type=float, default=180.0)
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
