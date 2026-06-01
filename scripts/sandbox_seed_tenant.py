#!/usr/bin/env python3
"""scripts/sandbox_seed_tenant.py — minimal tenant + CEO actor seed for the
real-API sandbox.

Unlike scripts/seed_dogfood_tenant.py, this does NOT import the
simulation / synthetic personas (that module hard-refuses to load under
COMPANY_OS_ENV=prod, which the sandbox runs as). It seeds only what the
OAuth-install flow needs: a tenants row + the CEO actor, so
POST /auth/session can mint a session for that (actor, tenant).

Reads COMPANY_OS_TENANT_ID, COMPANY_OS_CEO_ACTOR_ID, DATABASE_URL from
env. Idempotent. Run inside the stack:

    docker compose -f docker-compose.yml -f docker-compose.sandbox.yml \\
        exec gateway python scripts/sandbox_seed_tenant.py
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
from uuid import UUID

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import asyncpg

from services.gateway.db_bootstrap import _register_codecs


async def _main() -> int:
    dsn = os.environ.get("DATABASE_URL")
    tid = os.environ.get("COMPANY_OS_TENANT_ID")
    aid = os.environ.get("COMPANY_OS_CEO_ACTOR_ID")
    if not (dsn and tid and aid):
        print("ERROR: DATABASE_URL, COMPANY_OS_TENANT_ID, COMPANY_OS_CEO_ACTOR_ID required", file=sys.stderr)
        return 1
    tenant_id, ceo_id = UUID(tid), UUID(aid)

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2, init=_register_codecs)
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO tenants (id, name)
                    VALUES ($1, 'sandbox')
                    ON CONFLICT (id) DO NOTHING
                    """,
                    tenant_id,
                )
                await conn.execute(
                    """
                    INSERT INTO actors
                        (id, tenant_id, type, display_name, email, status, metadata, created_at)
                    VALUES ($1, $2, 'human_internal', 'Rachin',
                            'rachin@fyralis.internal', 'active', $3::jsonb, now())
                    ON CONFLICT (id) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        email = EXCLUDED.email
                    """,
                    ceo_id, tenant_id,
                    json.dumps({"role": "ceo", "title": "CEO", "synthetic_persona": False}),
                )
        print(f"OK: seeded tenant {tenant_id} + CEO actor {ceo_id}")
        return 0
    finally:
        await pool.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
