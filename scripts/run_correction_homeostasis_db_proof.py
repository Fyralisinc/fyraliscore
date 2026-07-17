#!/usr/bin/env python3
"""Run the bounded correction-homeostasis proof against an initialized test DB."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

import asyncpg

from services.correction_homeostasis_db_vertical import run_correction_homeostasis_db_vertical


async def _run(database_url: str, output: Path) -> dict:
    async def install_json_codec(conn):
        for type_name in ("json", "jsonb"):
            await conn.set_type_codec(
                type_name,
                encoder=lambda value: json.dumps(value) if not isinstance(value, str) else value,
                decoder=json.loads,
                schema="pg_catalog",
            )

    pool = await asyncpg.create_pool(
        database_url, min_size=1, max_size=3, init=install_json_codec,
    )
    tenant_id = uuid4()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO tenants (id,name) VALUES ($1,$2)",
                tenant_id, f"correction-homeostasis-proof-{tenant_id}",
            )
        return await run_correction_homeostasis_db_vertical(
            pool=pool, tenant_id=tenant_id, output_path=output,
        )
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    result = asyncio.run(_run(args.database_url, args.output))
    print(json.dumps({
        "output": str(args.output),
        "objective_sha256": result["objective_sha256"],
        "verdict": result["evaluation"]["verdict"],
        "continuous_score": result["evaluation"]["continuous_score"],
    }, sort_keys=True))
    return 0 if result["evaluation"]["verdict"] == "meets_policy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
