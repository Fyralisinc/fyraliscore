#!/usr/bin/env python3
"""Run the partial, genuine PostgreSQL P8 restart/replay fault slice."""

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

from services.evaluation.epistemic_repair.p8_postgres_runner import run_postgres_fault_slice


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    result = await run_postgres_fault_slice(args.database_url)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(asdict(result), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"covered_boundaries={len(result.covered_boundaries)}/12 "
        f"executions={len(result.receipts)} exact_required_fault_coverage=false "
        f"output={args.output}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, default=Path("docs/plans/epistemic-repair/p8/p8-postgres-fault-slice.json"))
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
