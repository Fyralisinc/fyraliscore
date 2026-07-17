#!/usr/bin/env python3
"""Run the rollback-scoped PostgreSQL P5 zero-seed vertical evaluator."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import sys
from uuid import uuid4

import asyncpg


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p5_runner import (
    run_p5_vertical,
    write_p5_artifact,
    write_p5_schema,
)


DEFAULT_OUTPUT = Path(
    "docs/plans/epistemic-repair/p5/epistemic-repair-p5-vertical-v1.json"
)


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    conn = await asyncpg.connect(args.database_url)
    transaction = conn.transaction()
    await transaction.start()
    try:
        artifact = await run_p5_vertical(conn, tenant_id=uuid4())
    finally:
        await transaction.rollback()
        await conn.close()
    write_p5_artifact(artifact, args.output)
    if args.schema_output is not None:
        write_p5_schema(args.schema_output)
    print(
        f"phase_exit_ready={str(artifact.phase_exit_ready).lower()} "
        f"signals={artifact.normalized_signal_count} "
        f"gates_passed={sum(g.status == 'pass' for g in artifact.hard_gates.values())}/"
        f"{len(artifact.hard_gates)} output={args.output}"
    )
    return 0 if artifact.phase_exit_ready else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL DSN; defaults to DATABASE_URL.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--schema-output", type=Path)
    return parser


def main() -> int:
    return asyncio.run(_run(_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
