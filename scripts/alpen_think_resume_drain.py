"""Resume draining the existing Alpen Think queue without enqueueing triggers."""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from uuid import UUID

import asyncpg
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("COMPANY_OS_ENV", "test")

load_dotenv(REPO_ROOT / ".env")

import lib.shared.db as _db
from lib.llm.provider import build_provider
from services.app.gateway.db_bootstrap import _register_codecs
from tests.real_llm.infrastructure.durability_flow import run_think_until_drain

TENANT = UUID(
    os.environ.get(
        "ALPEN_TENANT_ID",
        "90864cdd-731b-44b3-96c5-78f0004af3e2",
    )
)


async def main() -> None:
    pool = await asyncpg.create_pool(
        os.environ["DATABASE_URL"],
        min_size=1,
        max_size=8,
        init=_register_codecs,
    )
    _db._pool = pool
    provider = build_provider()
    print(f"resume provider: {type(provider).__name__} model={provider.config.model}", flush=True)
    await run_think_until_drain(
        TENANT,
        pool=pool,
        provider=provider,
        timeout_seconds=int(os.environ.get("THINK_TIMEOUT_S", "86400")),
    )
    async with pool.acquire() as conn:
        q = await conn.fetchrow(
            """
            SELECT count(*) filter (where completed_at is null) pending,
                   count(*) filter (where completed_at is not null) completed,
                   count(*) total
            FROM think_trigger_queue
            WHERE tenant_id=$1
            """,
            TENANT,
        )
        print(f"drain complete: {dict(q)}", flush=True)
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
