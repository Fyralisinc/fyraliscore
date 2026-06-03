#!/usr/bin/env python
"""Export flagged Think quality cases into replay fixtures.

Example:
  DATABASE_URL=postgres://... \
    .venv/bin/python scripts/promote_think_quality_cases.py \
      --tenant-id <uuid> --limit 10
"""
from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from uuid import UUID

import asyncpg

from services.domain.models.repo import pgvector_pool_init
from services.reasoning.think.quality_promoter import promote_quality_cases
from services.reasoning.think.quality_report import build_think_quality_cases


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote /debug/think-quality/cases into JSON fixtures."
    )
    parser.add_argument("--tenant-id", required=True, type=UUID)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to DATABASE_URL.",
    )
    parser.add_argument("--since-hours", type=int, default=24)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--low-context-ratio", type=float, default=0.20)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("tests/quality_replay/cases"),
    )
    parser.add_argument(
        "--expectation-mode",
        choices=("known_failure", "must_pass"),
        default="known_failure",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--no-artifacts",
        action="store_true",
        help="Do not include think_run_artifacts payloads in promoted cases.",
    )
    return parser


async def _main() -> int:
    args = _parser().parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL is required via --database-url or env")

    pool = await asyncpg.create_pool(
        args.database_url,
        min_size=1,
        max_size=3,
        init=pgvector_pool_init,
    )
    try:
        async with pool.acquire() as conn:
            payload = await build_think_quality_cases(
                conn,
                tenant_id=args.tenant_id,
                since_hours=args.since_hours,
                limit=args.limit,
                low_context_ratio=args.low_context_ratio,
                include_artifacts=not args.no_artifacts,
            )
        paths = promote_quality_cases(
            payload["cases"],
            output_dir=args.output_dir,
            source={
                "tenant_id": str(args.tenant_id),
                "window": payload["window"],
                "source": "scripts/promote_think_quality_cases.py",
            },
            expectation_mode=args.expectation_mode,
            overwrite=args.overwrite,
        )
    finally:
        await pool.close()

    print(f"promoted {len(paths)} case(s) into {args.output_dir}")
    for path in paths:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
