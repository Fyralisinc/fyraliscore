#!/usr/bin/env python
"""apply_extension_migrations.py — apply every installed extension's own schema.

Run AFTER the core migration set (scripts/docker-migrate.sh / lib.shared.migrations).
Discovers each extension's ``company_os.migrations`` directory and applies it under
a per-extension ledger (``schema_migrations_ext_<id>``), so extensions own their
tables without colliding with the host's numbering. Idempotent + ledger-tracked.

    DATABASE_URL=postgresql://… python scripts/apply_extension_migrations.py
"""
from __future__ import annotations

import asyncio
import json
import os

import asyncpg

from lib.extensions.migrations import apply_extension_migrations, discover_migration_dirs


async def _main() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is required")
    dirs = discover_migration_dirs()
    if not dirs:
        print(json.dumps({"extensions": [], "note": "no company_os.migrations contributors"}))
        return
    conn = await asyncpg.connect(dsn)
    try:
        results = await apply_extension_migrations(conn, on_error="stop")
    finally:
        await conn.close()
    print(json.dumps({"applied": {k: v for k, v in results.items()}}, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
