"""Gateway startup helper for the built-in Pelago demo config."""
from __future__ import annotations

import asyncpg

from services.app.gateway.logging_config import get_logger


log = get_logger("gateway")


async def ensure_demo_seed(pool: asyncpg.Pool) -> None:
    """Re-apply the Pelago demo_configs row if it is missing."""
    have = await pool.fetchval(
        "SELECT 1 FROM demo_configs WHERE company_id = 'pelago' LIMIT 1"
    )
    if have:
        return
    await pool.execute(
        """
        INSERT INTO demo_configs (
            id, company_id, name, description, tagline, snapshot_uri,
            model_routing, cost_cap_usd_per_session, determinism_seed
        ) VALUES (
            '00000000-0000-7d23-8000-000000000004'::uuid,
            'pelago',
            'Pelago',
            $1,
            'Series A, multi-shock year, founder running on signals',
            'demo/snapshots/pelago-v1.sql.zst',
            $2::jsonb,
            5.00,
            42
        )
        ON CONFLICT (company_id) DO UPDATE
          SET name = EXCLUDED.name,
              description = EXCLUDED.description,
              tagline = EXCLUDED.tagline,
              snapshot_uri = EXCLUDED.snapshot_uri,
              model_routing = EXCLUDED.model_routing,
              cost_cap_usd_per_session = EXCLUDED.cost_cap_usd_per_session,
              determinism_seed = EXCLUDED.determinism_seed
        """,
        (
            "Series A B2B SaaS revenue-intelligence platform. 35 people, "
            "$5.8M ARR, 28 customers. Just closed a $14M Series A. The "
            "company is 9 months in: an anchor design partner has churned, "
            "the VP Eng departed mid-year, and the org has just "
            "reorganized around integration surfaces."
        ),
        '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}',
    )
    log.info("demo_seed_inserted", extra={"company_id": "pelago"})
