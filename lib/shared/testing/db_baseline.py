"""Shared integration-test database baseline helpers."""
from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import UUID

import asyncpg


_SOURCE_CATALOG_ARTIFACT = Path(__file__).with_name(
    "source_catalog.generated.json"
)
_SOURCE_CATALOG_SCHEMA = "fyralis.test-source-catalog.v1"


def _test_source_catalog_rows() -> list[tuple[object, ...]]:
    """Load the checked-in catalog derivative without importing services."""

    payload = json.loads(_SOURCE_CATALOG_ARTIFACT.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schemaVersion") != (
        _SOURCE_CATALOG_SCHEMA
    ):
        raise RuntimeError("test source catalog artifact has an invalid schema")
    raw_rows = payload.get("rows")
    if not isinstance(raw_rows, list) or not raw_rows:
        raise RuntimeError("test source catalog artifact must contain rows")

    rows: list[tuple[object, ...]] = []
    source_ids: set[str] = set()
    for raw in raw_rows:
        if not isinstance(raw, dict) or set(raw) != {
            "aliases",
            "dataPlane",
            "historicalSupported",
            "id",
            "uiSlug",
        }:
            raise RuntimeError("test source catalog artifact row is malformed")
        source_id = raw["id"]
        ui_slug = raw["uiSlug"]
        aliases = raw["aliases"]
        historical_supported = raw["historicalSupported"]
        data_plane = raw["dataPlane"]
        if (
            not isinstance(source_id, str)
            or not source_id
            or source_id in source_ids
            or not isinstance(ui_slug, str)
            or not ui_slug
            or not isinstance(aliases, list)
            or not all(isinstance(alias, str) and alias for alias in aliases)
            or not isinstance(historical_supported, bool)
            or not isinstance(data_plane, bool)
        ):
            raise RuntimeError("test source catalog artifact row is invalid")
        source_ids.add(source_id)
        rows.append(
            (
                source_id,
                ui_slug,
                aliases,
                historical_supported,
                data_plane,
            )
        )
    return rows


async def seed_test_source_catalog(conn: asyncpg.Connection) -> None:
    """Restore migration-owned source reference rows after legacy test cleanup.

    Production treats catalog/database drift as a startup error; tests are
    different because older cleanup fixtures may have truncated every public
    table while leaving the catalog foreign keys installed.  Re-seeding here
    makes a long-lived local test database recoverable without weakening the
    production parity gate.
    """

    await conn.executemany(
        """
        INSERT INTO ingestion_source_catalog (
            id, ui_slug, aliases, historical_supported, data_plane
        ) VALUES ($1, $2, $3::text[], $4, $5)
        ON CONFLICT (id) DO UPDATE SET
            ui_slug = EXCLUDED.ui_slug,
            aliases = EXCLUDED.aliases,
            historical_supported = EXCLUDED.historical_supported,
            data_plane = EXCLUDED.data_plane,
            updated_at = now()
        """,
        _test_source_catalog_rows(),
    )


async def install_test_tenant_auto_register(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE OR REPLACE FUNCTION _test_auto_register_tenant()
        RETURNS trigger AS $$
        BEGIN
          IF NEW.tenant_id IS NOT NULL THEN
            INSERT INTO tenants (id, name, is_demo)
            VALUES (
              NEW.tenant_id,
              'test_auto_' || NEW.tenant_id::text,
              FALSE
            )
            ON CONFLICT (id) DO NOTHING;
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    rows = await conn.fetch(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relispartition = FALSE
          AND a.attname = 'tenant_id'
          AND NOT a.attisdropped
          AND c.relname <> 'tenants'
        """
    )
    for row in rows:
        table = row["relname"]
        await conn.execute(
            f'DROP TRIGGER IF EXISTS _test_auto_register_tenant ON "{table}"'
        )
        await conn.execute(
            f"""
            CREATE TRIGGER _test_auto_register_tenant
            BEFORE INSERT ON "{table}"
            FOR EACH ROW
            EXECUTE FUNCTION _test_auto_register_tenant()
            """
        )


async def seed_test_baseline(conn: asyncpg.Connection) -> None:
    await seed_test_source_catalog(conn)
    default_tenant = os.environ.get("DEFAULT_TENANT_ID")
    tenant_ids: set[UUID] = {
        UUID("11111111-1111-1111-1111-111111111111"),
        UUID("22222222-2222-2222-2222-222222222222"),
    }
    if default_tenant:
        try:
            tenant_ids.add(UUID(default_tenant))
        except ValueError:
            pass
    for tenant_id in tenant_ids:
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, $2, FALSE)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
            f"test_baseline_{tenant_id}",
        )
