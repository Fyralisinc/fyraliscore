"""Deel install-path regression (Phase-2 hardening finding #4).

`finalize_install` — the REAL install surface (deel OAuth finalize +
finance connect wizard) — UPSERTs `contract_name` / `contract_type` into
`deel_contracts`. Migration 0098 never created those columns, so every real
install with >= 1 contract raised UndefinedColumnError at install time. The
all-25 synthetic gate masked it (it seeds deel via the fetcher path, not
`finalize_install`). Migration 0122 adds the columns; this test exercises the
exact path that crashed.
"""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import pytest

from services.ingest.integrations.deel.onboarding import finalize_install

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'deel-onboarding-test')", tid,
    )
    return tid


async def test_finalize_install_persists_contract_metadata(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant = await _seed_tenant(fresh_db)
    install_id = await finalize_install(
        fresh_db,
        tenant_id=tenant,
        base_url="https://api.letsdeel.com",
        contracts=[
            {"contract_id": "ctr_1", "contract_name": "Eng Contractor", "contract_type": "eor"},
            {"contract_id": "ctr_2"},  # name/type absent -> NULL, must not crash
        ],
    )
    assert isinstance(install_id, UUID)

    rows = await fresh_db.fetch(
        "SELECT contract_id, contract_name, contract_type "
        "FROM deel_contracts WHERE deel_installation_id = $1 ORDER BY contract_id",
        install_id,
    )
    by_id = {r["contract_id"]: r for r in rows}
    assert set(by_id) == {"ctr_1", "ctr_2"}
    assert by_id["ctr_1"]["contract_name"] == "Eng Contractor"
    assert by_id["ctr_1"]["contract_type"] == "eor"
    assert by_id["ctr_2"]["contract_name"] is None


async def test_finalize_install_is_idempotent(fresh_db: asyncpg.Pool) -> None:
    tenant = await _seed_tenant(fresh_db)
    contracts = [{"contract_id": "ctr_x", "contract_name": "X", "contract_type": "contractor"}]
    first = await finalize_install(
        fresh_db, tenant_id=tenant, base_url="https://api.letsdeel.com",
        contracts=contracts,
    )
    # Re-run with an updated name — UPSERT must COALESCE without error.
    second = await finalize_install(
        fresh_db, tenant_id=tenant, base_url="https://api.letsdeel.com",
        contracts=[{"contract_id": "ctr_x", "contract_name": "X renamed"}],
    )
    assert first == second  # idempotent on (tenant, base_url)
    row = await fresh_db.fetchrow(
        "SELECT contract_name, contract_type FROM deel_contracts "
        "WHERE deel_installation_id = $1 AND contract_id = 'ctr_x'",
        first,
    )
    assert row["contract_name"] == "X renamed"
    assert row["contract_type"] == "contractor"  # COALESCE kept the prior value
