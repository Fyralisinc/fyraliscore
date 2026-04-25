"""Seeds the dogfood tenant with:
- CEO actor (Rachin), stable UUID from $COMPANY_OS_CEO_ACTOR_ID
- All simulation personas via ensure_personas_seeded (12 actors)

Idempotent via ON CONFLICT. Safe to re-run.
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
from simulation.workers._common import ensure_personas_seeded


async def _seed_ceo(conn: asyncpg.Connection, tenant_id: UUID, ceo_id: UUID) -> None:
    await conn.execute(
        """
        INSERT INTO actors
            (id, tenant_id, type, display_name, email,
             status, metadata, created_at)
        VALUES ($1, $2, 'human_internal', 'Rachin',
                'rachin@fyralis.internal', 'active', $3::jsonb, now())
        ON CONFLICT (id) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            email = EXCLUDED.email
        """,
        ceo_id,
        tenant_id,
        json.dumps(
            {"role": "ceo", "title": "CEO", "synthetic_persona": False}
        ),
    )
    # Aliases so CEO-authored messages on slack / github resolve back.
    for channel, ref in [
        ("slack", "rachin"),
        ("github", "rachin"),
        ("email", "rachin@fyralis.internal"),
    ]:
        await conn.execute(
            """
            INSERT INTO actor_identity_mappings
                (actor_id, source_channel, source_actor_ref,
                 confidence, created_at)
            VALUES ($1, $2, $3, 1.0, now())
            ON CONFLICT (source_channel, source_actor_ref) DO NOTHING
            """,
            ceo_id, channel, ref,
        )


async def main() -> None:
    dsn = os.environ["DATABASE_URL"]
    tenant_id = UUID(os.environ["COMPANY_OS_TENANT_ID"])
    ceo_id = UUID(os.environ["COMPANY_OS_CEO_ACTOR_ID"])

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2, init=_register_codecs)
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await _seed_ceo(conn, tenant_id, ceo_id)
        await ensure_personas_seeded(pool, tenant_id)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT count(*)::int as n FROM actors WHERE tenant_id = $1",
                tenant_id,
            )
        print(f"Seeded tenant {tenant_id}: {row['n']} actors (CEO + personas).")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
