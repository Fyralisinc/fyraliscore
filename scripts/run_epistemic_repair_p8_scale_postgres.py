#!/usr/bin/env python3
"""Run the measured 27-cell PostgreSQL P8 semantic scale matrix."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p8_scale_runner import (
    evaluate_scale_execution,
    run_scale_matrix,
    run_shared_contention,
)


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    execution = await run_scale_matrix(args.database_url)
    contention = await run_shared_contention(args.database_url)
    execution = replace(execution, shared_contention=contention)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evaluation = evaluate_scale_execution(execution)
    artifact = {"execution": asdict(execution), "evaluation": evaluation}
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"scale_cells={len(execution.cells)}/27 exact_matrix_coverage={str(execution.exact_matrix_coverage).lower()} "
        f"rollback_isolated=true physically_isolated_databases=false "
        f"shared_contention_cells={contention.concurrent_cells} "
        f"scale_execution_ready={str(evaluation['scale_execution_ready']).lower()} output={args.output}"
    )
    return 0 if execution.exact_matrix_coverage else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, default=Path("docs/plans/epistemic-repair/p8/p8-postgres-scale-matrix.json"))
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
