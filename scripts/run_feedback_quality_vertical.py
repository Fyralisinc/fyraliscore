#!/usr/bin/env python3
"""Run the bounded matched adaptive-vs-frozen feedback-quality DB proof."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg

from lib.shared.migrations import apply_migrations_dir
from services.feedback_quality_vertical import run_feedback_quality_vertical


async def _run(*, dsn: str, output: Path):
    conn = await asyncpg.connect(dsn)
    try:
        await apply_migrations_dir(conn, ROOT / "db" / "migrations")
    finally:
        await conn.close()
    return await run_feedback_quality_vertical(dsn=dsn, output_path=output)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or DATABASE_URL is required")
    if args.output.exists() or args.receipt.exists():
        raise SystemExit("output and receipt must not already exist")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt = {
        "schema_version": "feedback-quality-matched-db-receipt-v1",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "completed_at": None, "status": "running", "run_attempts": 1,
    }
    args.receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    try:
        artifact = asyncio.run(_run(dsn=args.dsn, output=args.output))
        receipt.update({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "completed", "verdict": artifact["verdict"],
            "continuous_score": artifact["continuous_score"],
            "objective_sha256": artifact["objective_sha256"],
            "artifact_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        })
    except Exception as exc:
        receipt.update({
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "status": "failed", "error_type": type(exc).__name__,
            "error_message": str(exc),
        })
        raise
    finally:
        args.receipt.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "output": str(args.output), "receipt": str(args.receipt),
        "verdict": artifact["verdict"], "continuous_score": artifact["continuous_score"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
