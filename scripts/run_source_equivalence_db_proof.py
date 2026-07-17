#!/usr/bin/env python3
"""Run the fresh persisted-signal source-equivalence DB proof."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg

from services.source_equivalence_db_vertical import run_source_equivalence_db_vertical


async def _run(dsn: str, tenant_id: UUID, output: Path) -> None:
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO tenants (id,name) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                tenant_id, f"source-equivalence-db-{tenant_id}",
            )
        result = await run_source_equivalence_db_vertical(
            pool=pool, tenant_id=tenant_id, output_path=output,
        )
        print(result["objective_sha256"])
        print(result["evaluation"])
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--tenant-id", type=UUID, default=uuid4())
    parser.add_argument(
        "--output", type=Path,
        default=Path("/tmp/source-equivalence-db-proof.json"),
    )
    args = parser.parse_args()
    if not args.dsn:
        raise SystemExit("--dsn or DATABASE_URL is required")
    asyncio.run(_run(args.dsn, args.tenant_id, args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
