"""Command-line runner for rebuildable Model projections.

Usage:
  python -m services.domain.projections.run --projection constraints --tenant-id <uuid>
  python -m services.domain.projections.run --projection resources --tenant-id <uuid>
  python -m services.domain.projections.run --projection all --watch
"""
from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from pathlib import Path
from uuid import UUID

import asyncpg

from services.domain.projections.catalog import (
    build_projection_registry,
    projection_choices,
    projectors_for,
)
from services.domain.projections.runtime import ProjectionRunner
from services.domain.projections.types import Projector


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:  # pragma: no cover - dependency exists in normal envs.
        return
    repo_root = Path(__file__).resolve().parents[3]
    load_dotenv(repo_root / ".env", override=False)


def _projectors_for(names: Sequence[str]) -> list[Projector]:
    return projectors_for(names)


async def _tenant_ids(
    conn: asyncpg.Connection,
    tenant_id: UUID | None,
) -> list[UUID]:
    if tenant_id is not None:
        return [tenant_id]
    rows = await conn.fetch(
        """
        SELECT DISTINCT tenant_id
        FROM model_events
        ORDER BY tenant_id
        """
    )
    return [row["tenant_id"] for row in rows]


async def run_projection_pass(
    *,
    dsn: str,
    projection_names: Sequence[str],
    tenant_id: UUID | None = None,
    limit: int = 500,
) -> int:
    """Run one projection pass and return the number of events checkpointed."""
    registry = build_projection_registry(projection_names)
    runner = ProjectionRunner(registry)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        total = 0
        async with pool.acquire() as conn:
            for tid in await _tenant_ids(conn, tenant_id):
                total += await runner.run_once(conn, tenant_id=tid, limit=limit)
        return total
    finally:
        await pool.close()


async def _run(args: argparse.Namespace) -> int:
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        print("ERROR: no DSN provided. Set DATABASE_URL or pass --dsn.")
        return 2

    tenant_id = UUID(args.tenant_id) if args.tenant_id else None
    iterations = 0
    while True:
        processed = await run_projection_pass(
            dsn=dsn,
            projection_names=args.projection,
            tenant_id=tenant_id,
            limit=args.limit,
        )
        print(f"projection_pass processed_events={processed}")
        iterations += 1
        if not args.watch:
            return 0
        if args.max_iterations is not None and iterations >= args.max_iterations:
            return 0
        await asyncio.sleep(args.interval_seconds)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=None, help="Postgres DSN; default $DATABASE_URL.")
    parser.add_argument(
        "--projection",
        action="append",
        choices=projection_choices(),
        default=None,
        help="Projection to run. Repeatable. Default: constraints.",
    )
    parser.add_argument("--tenant-id", default=None, help="Optional tenant UUID.")
    parser.add_argument("--limit", type=int, default=500, help="Events per projector pass.")
    parser.add_argument("--watch", action="store_true", help="Run continuously.")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=2.0,
        help="Sleep between watch passes.",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Testing/debug guard for --watch.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    _load_dotenv()
    return asyncio.run(_run(parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
