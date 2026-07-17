#!/usr/bin/env python3
"""Run the bounded sealed company-physics vertical on a migrated database."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg

from services.company_physics_vertical import run_company_physics_vertical


async def _install_json_codec(conn: asyncpg.Connection) -> None:
    for type_name in ("json", "jsonb"):
        await conn.set_type_codec(
            type_name,
            encoder=lambda value: json.dumps(value) if not isinstance(value, str) else value,
            decoder=json.loads,
            schema="pg_catalog",
        )


async def _run(*, dsn: str, tenant_id: UUID, output: Path) -> None:
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=4, init=_install_json_codec
    )
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO tenants (id,name) VALUES ($1,$2) ON CONFLICT DO NOTHING",
                tenant_id, f"sealed-company-physics-{tenant_id}",
            )
        result = await run_company_physics_vertical(
            pool=pool, tenant_id=tenant_id, output_path=output
        )
        print(result["objective_sha256"])
    finally:
        await pool.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--tenant-id", type=UUID, default=uuid4())
    parser.add_argument(
        "--output", type=Path,
        default=Path("/tmp/sealed_company_physics_vertical.json"),
    )
    args = parser.parse_args()
    if not args.dsn:
        raise SystemExit("--dsn or DATABASE_URL is required")
    asyncio.run(_run(dsn=args.dsn, tenant_id=args.tenant_id, output=args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
