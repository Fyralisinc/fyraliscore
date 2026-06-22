"""Shared helpers for extension integration tests."""
from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg
import pytest

from lib.shared.migrations import apply_migrations_dir, schema_bootstrap_lock


_PGVECTOR_PRIVILEGE_MARKERS = (
    'permission denied to create extension "vector"',
    "Must be superuser to create this extension",
    'extension "vector" is not available',
)


async def require_pgvector_server_privilege_or_skip(
    server_dsn: str,
    *,
    feature: str,
) -> None:
    if "://" not in server_dsn:
        pytest.skip("no DATABASE_URL")
    try:
        admin = await asyncio.wait_for(asyncpg.connect(server_dsn), timeout=5.0)
    except TimeoutError:
        pytest.skip(f"{feature} integration timed out while connecting to Postgres")
    try:
        is_superuser = await asyncio.wait_for(
            admin.fetchval(
                "SELECT COALESCE(rolsuper, false) "
                "FROM pg_roles WHERE rolname = current_user"
            ),
            timeout=5.0,
        )
        if not is_superuser:
            pytest.skip(
                f"{feature} integration needs a DB user that can create "
                "the pgvector extension in the throwaway database"
            )
    finally:
        await admin.close()


async def _ensure_pgvector_or_skip(conn: asyncpg.Connection, *, feature: str) -> None:
    try:
        await asyncio.wait_for(
            conn.execute("CREATE EXTENSION IF NOT EXISTS vector"),
            timeout=5.0,
        )
    except TimeoutError:
        pytest.skip(f"{feature} integration timed out while checking pgvector")
    except Exception as exc:  # noqa: BLE001 - classify optional local DB support
        if any(marker in str(exc) for marker in _PGVECTOR_PRIVILEGE_MARKERS):
            pytest.skip(
                f"{feature} integration needs a DB user that can create "
                "the pgvector extension in the throwaway database"
            )
        raise


async def apply_core_migrations_or_skip(
    conn: asyncpg.Connection,
    migrations_dir: Path,
    *,
    feature: str,
) -> None:
    """Apply core migrations, skipping when local Postgres lacks pgvector DDL."""
    await _ensure_pgvector_or_skip(conn, feature=feature)
    async with schema_bootstrap_lock(conn):
        try:
            await apply_migrations_dir(conn, migrations_dir)
        except Exception as exc:  # noqa: BLE001 - preserve original failure otherwise
            if any(marker in str(exc) for marker in _PGVECTOR_PRIVILEGE_MARKERS):
                pytest.skip(
                    f"{feature} integration needs a DB user that can create "
                    "the pgvector extension in the throwaway database"
                )
            raise
