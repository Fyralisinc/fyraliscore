#!/usr/bin/env python
"""scripts/github_intel_dev_session.py — mint a browser token for the GitHub
Intelligence UI.

The browser demo picker provisions a FRESH per-session tenant, so it never sees
GitHub-intel data seeded under the dogfood tenant. This script mints a bearer
token bound to the tenant that actually holds the data (default: the dogfood
COMPANY_OS_TENANT_ID, which scripts/demo_github_intel.py seeds), and prints a
ready-to-paste token + the /github URL.

Usage:
  # 1. seed data (once):
  DATABASE_URL=postgresql://company_os:company_os@localhost:5434/company_os \
    COMPANY_OS_TENANT_ID=00000000-0000-0000-0000-000000000001 \
    python scripts/demo_github_intel.py
  # 2. mint a token:
  DATABASE_URL=... COMPANY_OS_TENANT_ID=... python scripts/github_intel_dev_session.py
  # 3. start the gateway (:8000) + UI (npm run dev, :5173), open the URL, paste the token.
"""
from __future__ import annotations

import asyncio
import os
from uuid import UUID

import asyncpg

from services.gateway.auth import create_session
from lib.shared.ids import uuid7

DSN = os.environ.get("DATABASE_URL", "postgresql://company_os:company_os@localhost:5434/company_os")
TENANT = UUID(os.environ.get("COMPANY_OS_TENANT_ID", "00000000-0000-0000-0000-000000000001"))
UI_BASE = os.environ.get("GITHUB_INTEL_UI_BASE", "http://localhost:5173")


async def main() -> None:
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    try:
        await pool.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, 'ghintel-dev') ON CONFLICT (id) DO NOTHING",
            TENANT,
        )
        # Reuse any existing actor for the tenant; else create a lightweight one.
        actor_id = await pool.fetchval(
            "SELECT id FROM actors WHERE tenant_id = $1 ORDER BY created_at LIMIT 1", TENANT
        )
        if actor_id is None:
            actor_id = uuid7()
            await pool.execute(
                "INSERT INTO actors (id, tenant_id, type, display_name, status) "
                "VALUES ($1, $2, 'human_internal', 'GH Intel Dev', 'active')",
                actor_id, TENANT,
            )
        token, ctx = await create_session(pool, actor_id=actor_id, tenant_id=TENANT)

        # How many repos does this tenant actually have intel for? (a sanity hint)
        repo_count = await pool.fetchval(
            "SELECT count(DISTINCT repo) FROM github_signal_enrichment WHERE tenant_id = $1",
            TENANT,
        )
    finally:
        await pool.close()

    bar = "=" * 72
    print(bar)
    print("  GitHub Intelligence — browser dev session")
    print(bar)
    print(f"  tenant_id : {TENANT}")
    print(f"  actor_id  : {actor_id}")
    print(f"  repos with intel for this tenant: {repo_count}")
    if not repo_count:
        print("  ⚠  no intel yet — run scripts/demo_github_intel.py first.")
    print()
    print(f"  Open:  {UI_BASE}/github")
    print("  Paste this token into the page's token bar, click Connect:")
    print()
    print(f"  {token}")
    print()
    print(f"  expires: {ctx.expires_at.isoformat()}")
    print(bar)


if __name__ == "__main__":
    asyncio.run(main())
