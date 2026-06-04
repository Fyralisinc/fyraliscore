"""Shared integration-test database baseline helpers."""
from __future__ import annotations

import os
from uuid import UUID

import asyncpg


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


async def seed_demo_configs(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        INSERT INTO demo_configs (
          id, company_id, name, description, tagline, snapshot_uri,
          model_routing, cost_cap_usd_per_session, determinism_seed
        ) VALUES
          (
            '00000000-0000-7d23-8000-000000000001'::uuid,
            'truss',
            'Truss',
            '40-person AI-native developer infrastructure company.',
            'Series A, founder at full cognitive load',
            'demo/snapshots/truss-v1.sql.zst',
            '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
            5.00,
            42
          ),
          (
            '00000000-0000-7d23-8000-000000000002'::uuid,
            'northwind',
            'Northwind Software',
            'Series B SaaS, 180 employees, $14M ARR, growing 80% YoY.',
            'Series B, healthy growth, normal Tuesday',
            'demo/snapshots/northwind-v1.sql.zst',
            '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
            5.00,
            43
          ),
          (
            '00000000-0000-7d23-8000-000000000003'::uuid,
            'meridian',
            'Meridian Industrial',
            'Series C enterprise software, 1100 employees, $85M ARR.',
            'Series C, $4.2M ARR customer escalating',
            'demo/snapshots/meridian-v1.sql.zst',
            '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
            7.50,
            44
          ),
          (
            '00000000-0000-7d23-8000-000000000004'::uuid,
            'pelago',
            'Pelago',
            'Series A B2B SaaS revenue-intelligence platform.',
            'Series A, multi-shock year, founder running on signals',
            'demo/snapshots/pelago-v1.sql.zst',
            '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
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
              determinism_seed = EXCLUDED.determinism_seed;
        """
    )


async def seed_test_baseline(conn: asyncpg.Connection) -> None:
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
    await seed_demo_configs(conn)

