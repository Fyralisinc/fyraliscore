#!/usr/bin/env python3
"""Run the rollback-scoped PostgreSQL P6 12-batch mixed-stream evaluator."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import asyncpg

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.evaluation.epistemic_repair.p6_runner import (
    run_p6_mixed_stream, write_p6_artifact, write_p6_markdown,
)
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from services.evaluation.epistemic_repair.p6_provider import (
    run_p6_provider_entity_evaluation, write_p6_provider_report,
)

DEFAULT_OUTPUT = Path("docs/plans/epistemic-repair/p6/epistemic-repair-p6-mixed-stream-v1.json")


def _commit_sha() -> str:
    result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                            text=True, check=False)
    return result.stdout.strip() or "working-tree"


async def _run(args: argparse.Namespace) -> int:
    if not args.database_url:
        raise SystemExit("DATABASE_URL or --database-url is required")
    conn = await asyncpg.connect(args.database_url)
    tx = conn.transaction()
    await tx.start()
    try:
        artifact = await run_p6_mixed_stream(conn, tenant_id=uuid4(),
                                             commit_sha=_commit_sha())
    finally:
        await tx.rollback()
        await conn.close()
    write_p6_artifact(artifact, args.output)
    write_p6_markdown(artifact, args.markdown_output or args.output.with_suffix(".md"))
    if args.provider_entity_output is not None:
        provider_report = await run_p6_provider_entity_evaluation(
            build_p6_population(),
            checkpoint_path=args.provider_entity_output.with_suffix(".checkpoint.json"),
            per_batch_timeout_s=args.provider_batch_timeout,
            total_timeout_s=args.provider_total_timeout,
        )
        write_p6_provider_report(provider_report, args.provider_entity_output)
    print(f"phase_exit_ready={str(artifact.phase_exit_ready).lower()} signals={len(artifact.signal_fates)} batches={len(artifact.batch_snapshots)} gates={sum(g.status == 'pass' for g in artifact.hard_gates.values())}/{len(artifact.hard_gates)} output={args.output}")
    return 0 if artifact.phase_exit_ready else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--provider-entity-output", type=Path,
                        help="Opt in to 12 production learned-extractor provider calls.")
    parser.add_argument("--provider-batch-timeout", type=float, default=120.0)
    parser.add_argument("--provider-total-timeout", type=float, default=600.0)
    return asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
