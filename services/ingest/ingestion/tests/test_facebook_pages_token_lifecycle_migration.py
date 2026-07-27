"""Database gates for migration 0198 Facebook Page token lifecycle."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.migrations import apply_migration


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]
_MIGRATION = (
    Path(__file__).resolve().parents[4]
    / "db/migrations/0198_facebook_pages_token_lifecycle.sql"
)


async def test_facebook_token_lifecycle_migration_is_idempotent(
    fresh_db: asyncpg.Pool,
) -> None:
    sql = _MIGRATION.read_text()
    async with fresh_db.acquire() as conn:
        await apply_migration(conn, sql, name=_MIGRATION.name)
        await apply_migration(conn, sql, name=_MIGRATION.name)


async def test_legacy_page_secret_is_preserved_but_reauthorization_is_required(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    installation_id = uuid7()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"facebook-token-lifecycle-{tenant_id}",
    )
    await fresh_db.execute(
        """
        INSERT INTO facebook_page_installations (
            id, tenant_id, page_id, page_access_token_ref
        ) VALUES ($1, $2, $3, $4)
        """,
        installation_id,
        tenant_id,
        f"page-{installation_id}",
        "secret://existing-page-token",
    )

    row = await fresh_db.fetchrow(
        """
        SELECT page_access_token_ref, user_access_token_ref,
               connection_state, reauthorization_required_at,
               page_token_recovery_next_attempt_at
          FROM facebook_page_installations
         WHERE id = $1 AND tenant_id = $2
        """,
        installation_id,
        tenant_id,
    )
    assert row is not None
    assert row["page_access_token_ref"] == "secret://existing-page-token"
    assert row["user_access_token_ref"] is None
    assert row["connection_state"] == "reauthorization_required"
    assert row["page_token_recovery_next_attempt_at"] is None


async def test_connected_install_requires_explicit_supported_user_token_data(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    installation_id = uuid7()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"facebook-connected-token-{tenant_id}",
    )
    expires_at = datetime.now(timezone.utc) + timedelta(days=30)
    await fresh_db.execute(
        """
        INSERT INTO facebook_page_installations (
            id, tenant_id, page_id, page_access_token_ref,
            user_access_token_ref, user_token_expires_at, connection_state
        ) VALUES ($1, $2, $3, $4, $5, $6, 'connected')
        """,
        installation_id,
        tenant_id,
        f"page-{installation_id}",
        "secret://page-token",
        "secret://long-user-token",
        expires_at,
    )

    row = await fresh_db.fetchrow(
        """
        SELECT user_access_token_ref, user_token_expires_at, connection_state
          FROM facebook_page_installations
         WHERE id = $1 AND tenant_id = $2
        """,
        installation_id,
        tenant_id,
    )
    assert row["user_access_token_ref"] == "secret://long-user-token"
    assert row["user_token_expires_at"] == expires_at
    assert row["connection_state"] == "connected"

    with pytest.raises(asyncpg.CheckViolationError):
        await fresh_db.execute(
            """
            UPDATE facebook_page_installations
               SET connection_state = 'invented_refresh'
             WHERE id = $1 AND tenant_id = $2
            """,
            installation_id,
            tenant_id,
        )

    with pytest.raises(asyncpg.CheckViolationError):
        await fresh_db.execute(
            """
            UPDATE facebook_page_installations
               SET connection_state = 'connected',
                   user_access_token_ref = NULL
             WHERE id = $1 AND tenant_id = $2
            """,
            installation_id,
            tenant_id,
        )
