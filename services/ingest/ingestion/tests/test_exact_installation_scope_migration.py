"""Database gates for migration 0196 exact installation identity."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migration


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_MIGRATION = (
    Path(__file__).resolve().parents[4]
    / "db/migrations/0196_exact_installation_scope_uniqueness.sql"
)


@dataclass(frozen=True)
class _ScopedTable:
    table: str
    scope_column: str
    index: str


_SCOPED_TABLES = (
    _ScopedTable(
        "mercury_installations",
        "organization_id",
        "mercury_installations_exact_scope_unique",
    ),
    _ScopedTable(
        "brex_installations",
        "organization_id",
        "brex_installations_exact_scope_unique",
    ),
    _ScopedTable(
        "deel_installations",
        "organization_id",
        "deel_installations_exact_scope_unique",
    ),
    _ScopedTable(
        "fireflies_installations",
        "workspace_id",
        "fireflies_installations_exact_scope_unique",
    ),
    _ScopedTable(
        "miro_installations",
        "org_id",
        "miro_installations_exact_scope_unique",
    ),
    _ScopedTable(
        "figma_installations",
        "team_id",
        "figma_installations_exact_scope_unique",
    ),
)


async def _seed_tenant(pool: asyncpg.Pool, label: str) -> UUID:
    tenant_id = uuid7()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"exact-scope-{label}-{tenant_id}",
    )
    return tenant_id


async def test_exact_scope_migration_is_idempotent(
    fresh_db: asyncpg.Pool,
) -> None:
    sql = _MIGRATION.read_text()
    async with fresh_db.acquire() as conn:
        await apply_migration(conn, sql, name=_MIGRATION.name)
        await apply_migration(conn, sql, name=_MIGRATION.name)


@pytest.mark.parametrize("scoped", _SCOPED_TABLES, ids=lambda item: item.table)
async def test_exact_scope_unique_and_unresolved_fallback_semantics(
    fresh_db: asyncpg.Pool,
    scoped: _ScopedTable,
) -> None:
    tenant_id = await _seed_tenant(fresh_db, scoped.table)

    old_constraint = await fresh_db.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
              FROM pg_constraint
             WHERE conrelid = $1::regclass
               AND conname = $2
        )
        """,
        scoped.table,
        f"{scoped.table}_tenant_id_base_url_key",
    )
    assert old_constraint is False

    index_definition = await fresh_db.fetchval(
        "SELECT indexdef FROM pg_indexes WHERE indexname = $1",
        scoped.index,
    )
    assert index_definition is not None
    assert scoped.scope_column in index_definition
    assert "COALESCE" in index_definition
    assert "UNIQUE INDEX" in index_definition

    insert_sql = (
        f"INSERT INTO {scoped.table} "
        f"(id, tenant_id, base_url, {scoped.scope_column}) "
        "VALUES ($1, $2, $3, $4)"
    )
    canonical_base = "https://api.provider.test"

    # Two exact installations may share the provider's canonical API host.
    await fresh_db.execute(
        insert_sql,
        uuid7(),
        tenant_id,
        canonical_base,
        "scope-a",
    )
    await fresh_db.execute(
        insert_sql,
        uuid7(),
        tenant_id,
        canonical_base,
        "scope-b",
    )

    # A provider scope is the identity even if its endpoint later changes.
    with pytest.raises(asyncpg.UniqueViolationError):
        await fresh_db.execute(
            insert_sql,
            uuid7(),
            tenant_id,
            "https://alternate.provider.test",
            "scope-a",
        )

    # While scope discovery is unavailable, base URL remains the legacy
    # idempotency fallback. It is kept in a separate discriminator namespace.
    await fresh_db.execute(
        insert_sql,
        uuid7(),
        tenant_id,
        "https://unresolved-a.provider.test",
        None,
    )
    await fresh_db.execute(
        insert_sql,
        uuid7(),
        tenant_id,
        "https://unresolved-b.provider.test",
        None,
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        await fresh_db.execute(
            insert_sql,
            uuid7(),
            tenant_id,
            "https://unresolved-a.provider.test",
            None,
        )
