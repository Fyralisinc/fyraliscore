"""Integration: extension capability enforcement is STRUCTURAL (live Postgres).

The production-critical proofs of E2:
  * the ``fyralis_ext_readonly`` role can read but a substrate write is denied by
    Postgres itself (not the app layer);
  * ``extension_grants`` round-trips and is tenant-isolated;
  * ``CapabilityScopedReader`` returns only granted channels and refuses ungranted
    ones;
  * ``reader_for`` is first-party-fully-granted but third-party-needs-grant.
"""
from __future__ import annotations

import uuid

import asyncpg
import pytest

from lib.extensions.host_api.v1 import Capabilities, CapabilityError
from lib.extensions.manifest import ExtensionManifest
from services.platform.extensions.access import is_enabled, reader_for
from services.platform.extensions.grants import ExtensionGrantsRepo
from services.platform.extensions.substrate_reader import CapabilityScopedReader

from .conftest import insert_observation

pytestmark = pytest.mark.integration


async def _seed_tenant(conn, tenant: uuid.UUID) -> None:
    await conn.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        tenant, f"ext-test-{tenant}",
    )


async def test_role_and_table_exist(db_pool):
    async with db_pool.acquire() as conn:
        role = await conn.fetchval(
            "SELECT 1 FROM pg_roles WHERE rolname = 'fyralis_ext_readonly'"
        )
        tbl = await conn.fetchval(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'extension_grants'"
        )
    assert role == 1
    assert tbl == 1


async def test_fyralis_ext_readonly_can_read_but_not_write(db_pool, committed_conn, tenant):
    # Seed a committed observation for the tenant.
    await insert_observation(committed_conn, tenant, source_channel="github:webhook")

    # READ under the restricted role: works.
    async with db_pool.acquire() as conn:
        await conn.execute("BEGIN")
        try:
            await conn.execute("SELECT set_config('app.current_tenant', $1, true)", str(tenant))
            await conn.execute("SET LOCAL ROLE fyralis_ext_readonly")
            cnt = await conn.fetchval("SELECT count(*) FROM observations")
            assert cnt >= 1
        finally:
            await conn.execute("ROLLBACK")

    # WRITE under the restricted role: denied by Postgres (structural).
    async with db_pool.acquire() as conn:
        await conn.execute("BEGIN")
        try:
            await conn.execute("SELECT set_config('app.current_tenant', $1, true)", str(tenant))
            await conn.execute("SET LOCAL ROLE fyralis_ext_readonly")
            with pytest.raises(asyncpg.exceptions.InsufficientPrivilegeError):
                await conn.execute(
                    """
                    INSERT INTO observations
                      (id, tenant_id, occurred_at, kind, source_channel,
                       content, content_text, embedding_pending, trust_tier)
                    VALUES (gen_random_uuid(), $1, now(), 'signal', 'github:webhook',
                            '{}'::jsonb, 'should fail', TRUE, 'authoritative')
                    """,
                    tenant,
                )
        finally:
            await conn.execute("ROLLBACK")


async def test_extension_grants_roundtrip_and_isolation(db_pool, committed_conn, tenant, other_tenant):
    await _seed_tenant(committed_conn, tenant)
    await _seed_tenant(committed_conn, other_tenant)
    repo = ExtensionGrantsRepo(db_pool)

    caps = Capabilities.from_dict(
        {"read_channels": ["github:webhook"], "substrate_read": ["observation"]}
    )
    await repo.grant(
        tenant_id=tenant, extension_id="acme.intel", granted_version="1.0.0",
        capabilities=caps, trust_ceiling="attested_agent", granted_by="operator@test",
    )

    got = await repo.get(tenant_id=tenant, extension_id="acme.intel")
    assert got is not None
    assert got.granted_version == "1.0.0"
    assert got.trust_ceiling == "attested_agent"
    assert got.capabilities.allows_channel("github:webhook")
    assert got.active

    # tenant isolation: other_tenant sees no grant.
    assert await repo.get(tenant_id=other_tenant, extension_id="acme.intel") is None

    # revoke → get returns None.
    await repo.revoke(tenant_id=tenant, extension_id="acme.intel")
    assert await repo.get(tenant_id=tenant, extension_id="acme.intel") is None

    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM extension_grants WHERE tenant_id = $1", tenant
            )
    except Exception:  # noqa: BLE001
        pass


async def test_capability_scoped_reader_channel_filtering(db_pool, committed_conn, tenant):
    await insert_observation(committed_conn, tenant, source_channel="github:webhook")
    await insert_observation(committed_conn, tenant, source_channel="github:webhook")
    await insert_observation(committed_conn, tenant, source_channel="slack:message")

    caps = Capabilities.from_dict(
        {"read_channels": ["github:webhook"], "substrate_read": ["observation"]}
    )
    reader = CapabilityScopedReader(pool=db_pool, tenant_id=tenant, capabilities=caps)

    rows = await reader.query_observations(limit=50)
    assert len(rows) == 2
    assert all(r.source_channel == "github:webhook" for r in rows)
    # raw payload never leaks through a view
    assert all("_raw" not in r.content for r in rows)

    # requesting an ungranted channel is refused.
    with pytest.raises(CapabilityError):
        await reader.query_observations(channel="slack:message")

    # a kind not granted is refused.
    with pytest.raises(CapabilityError):
        await reader.get_model(uuid.uuid4())


async def test_reader_for_first_party_vs_third_party(db_pool, committed_conn, tenant):
    await _seed_tenant(committed_conn, tenant)

    fp = ExtensionManifest(
        id=f"fp-{tenant.hex[:8]}",
        trust_tier="first_party",
        feature_flag=None,
        capabilities={"read_channels": ["github:webhook"], "substrate_read": ["observation"]},
    )
    tp = ExtensionManifest(
        id=f"tp-{tenant.hex[:8]}",
        trust_tier="third_party",
        feature_flag=f"tp-{tenant.hex[:8]}.enabled",
        capabilities={"read_channels": ["github:webhook"], "substrate_read": ["observation"]},
    )

    # First-party with no grant → fully granted (auditable, no consent migration).
    assert await reader_for(db_pool, tenant_id=tenant, manifest=fp) is not None

    # Third-party with no grant + flag off → nothing.
    assert await is_enabled(db_pool, tenant_id=tenant, manifest=tp) is False
    assert await reader_for(db_pool, tenant_id=tenant, manifest=tp) is None

    # Enable the flag AND grant → now usable (needs BOTH).
    from services.ingest.ingestion.feature_flags.client import TenantFlags

    await TenantFlags(db_pool).set_bool(tenant, tp.feature_flag, True, set_by="test")
    await ExtensionGrantsRepo(db_pool).grant(
        tenant_id=tenant, extension_id=tp.id, granted_version="1.0.0",
        capabilities=Capabilities.from_dict(tp.capabilities), granted_by="operator@test",
    )
    assert await reader_for(db_pool, tenant_id=tenant, manifest=tp) is not None

    # cleanup
    try:
        await ExtensionGrantsRepo(db_pool).revoke(tenant_id=tenant, extension_id=tp.id)
    except Exception:  # noqa: BLE001
        pass
